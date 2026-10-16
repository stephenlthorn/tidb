// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// These tests record every Get a cached prepared point get sends. A hint to skip a transaction's locks is only valid
// while that transaction is uncommitted; if a reused executor carried it into a later execution, that execution would
// skip the leftover secondary lock of a transaction that has committed since and return a value older than an
// acknowledged write.

const lockHintFakeTxn = uint64(7)

type lockHintGet struct {
	handle    int64
	version   uint64
	resolved  []uint64
	committed []uint64
}

type lockHintRecorder struct {
	tikv.Client
	mu                    sync.Mutex
	gets                  []lockHintGet
	maxTSCheckTxnStatuses int
	holdPrimaryCommit     atomic.Bool
	primaryCommitReached  chan struct{}
	releasePrimaryCommit  chan struct{}
	fakeFirstLockHandle   atomic.Int64
}

func (r *lockHintRecorder) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	switch req.Type {
	case tikvrpc.CmdGet:
		g := req.Get()
		if _, handle, err := tablecodec.DecodeRecordKey(g.Key); err == nil {
			r.mu.Lock()
			r.gets = append(r.gets, lockHintGet{
				handle:    handle.IntValue(),
				version:   g.Version,
				resolved:  slices.Clone(req.ResolvedLocks),
				committed: slices.Clone(req.CommittedLocks),
			})
			r.mu.Unlock()
			if r.fakeFirstLockHandle.CompareAndSwap(handle.IntValue(), 0) {
				lock := &kvrpcpb.LockInfo{
					Key: g.Key, PrimaryLock: g.Key, LockVersion: lockHintFakeTxn, LockTtl: 3000, TxnSize: 1, LockType: kvrpcpb.Op_Put,
				}
				return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{Error: &kvrpcpb.KeyError{Locked: lock}}}, nil
			}
		}
	case tikvrpc.CmdCheckTxnStatus:
		cts := req.CheckTxnStatus()
		if cts.CallerStartTs == math.MaxUint64 {
			r.mu.Lock()
			r.maxTSCheckTxnStatuses++
			r.mu.Unlock()
		}
		if cts.LockTs == lockHintFakeTxn {
			return &tikvrpc.Response{Resp: &kvrpcpb.CheckTxnStatusResponse{}}, nil // rolled back
		}
	case tikvrpc.CmdResolveLock:
		if req.ResolveLock().StartVersion == lockHintFakeTxn {
			return &tikvrpc.Response{Resp: &kvrpcpb.ResolveLockResponse{}}, nil
		}
	case tikvrpc.CmdCommit:
		if r.holdPrimaryCommit.CompareAndSwap(true, false) {
			close(r.primaryCommitReached)
			<-r.releasePrimaryCommit
		}
	}
	return r.Client.SendRequest(ctx, addr, req, timeout)
}

func (r *lockHintRecorder) getsFor(handle int64, from int) []lockHintGet {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []lockHintGet
	for _, g := range r.gets[from:] {
		if g.handle == handle {
			out = append(out, g)
		}
	}
	return out
}

func (r *lockHintRecorder) counters() (gets, maxTSStatuses int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.gets), r.maxTSCheckTxnStatuses
}

type lockHintScenario struct {
	t        *testing.T
	rec      *lockHintRecorder
	reader   *testkit.TestKit
	writer   *testkit.TestKit
	fresh    *testkit.TestKit
	commitCh chan error
}

func newLockHintScenario(t *testing.T) *lockHintScenario {
	rec := &lockHintRecorder{
		primaryCommitReached: make(chan struct{}),
		releasePrimaryCommit: make(chan struct{}),
	}
	store := testkit.CreateMockStore(t,
		mockstore.WithStoreType(mockstore.EmbedUnistore),
		mockstore.WithClientHijacker(func(base tikv.Client) tikv.Client {
			rec.Client = base
			return rec
		}),
	)
	s := &lockHintScenario{t: t, rec: rec, commitCh: make(chan error, 1)}
	s.reader = testkit.NewTestKit(t, store)
	s.writer = testkit.NewTestKit(t, store)
	s.fresh = testkit.NewTestKit(t, store)
	for _, tk := range []*testkit.TestKit{s.reader, s.writer, s.fresh} {
		tk.MustExec("use test")
	}
	s.reader.MustExec("create table t (id int primary key, v int)")
	s.reader.MustExec("insert into t values (1, 10), (2, 20), (3, 30)")
	// Defaults, nothing tuned: pessimistic txn mode, autocommit on, async commit and 1PC off.
	s.reader.MustQuery("select @@tidb_txn_mode, @@autocommit, @@tidb_enable_async_commit, @@tidb_enable_1pc").
		Check(testkit.Rows("pessimistic 1 0 0"))
	s.reader.MustExec("prepare p from 'select v from t where id = ?'")
	s.reader.MustExec("set @id = 2")
	// The first execution builds the PointGetExecutor and caches it, with its snapshot, in the PlanCacheStmt.
	s.reader.MustQuery("execute p using @id").Check(lockHintRows(20))
	s.reader.MustQuery("execute p using @id").Check(lockHintRows(20))
	warm := rec.getsFor(2, 0)
	require.NotEmpty(t, warm)
	for _, g := range warm {
		require.Equal(t, uint64(math.MaxUint64), g.version, "autocommit prepared point get reads at MaxTS with default settings")
		require.Empty(t, g.resolved)
		require.Empty(t, g.committed)
	}
	return s
}

// startWriterUpToPrimaryCommit opens a classic (non async-commit, non 1PC) 2PC transaction C that updates ids 1, 2, 3,
// lets it prewrite everything, then parks it right before the primary commit request.
func (s *lockHintScenario) startWriterUpToPrimaryCommit() {
	// One key per 2PC batch: the primary then commits alone and the other keys follow asynchronously, which is what
	// happens when the secondary keys live in other regions. "skip" leaves them unresolved after the ack.
	require.NoError(s.t, failpoint.Enable("tikvclient/twoPCRequestBatchSizeLimit", `return(true)`))
	s.t.Cleanup(func() { require.NoError(s.t, failpoint.Disable("tikvclient/twoPCRequestBatchSizeLimit")) })
	require.NoError(s.t, failpoint.Enable("tikvclient/beforeCommitSecondaries", `return("skip")`))
	s.t.Cleanup(func() { require.NoError(s.t, failpoint.Disable("tikvclient/beforeCommitSecondaries")) })
	s.writer.MustExec("set @@tidb_enable_async_commit = 0")
	s.writer.MustExec("set @@tidb_enable_1pc = 0")
	s.writer.MustExec("begin optimistic")
	s.writer.MustExec("update t set v = v + 1 where id in (1, 2, 3)")
	s.rec.holdPrimaryCommit.Store(true)
	go func() {
		_, err := s.writer.Exec("commit")
		s.commitCh <- err
	}()
	select {
	case <-s.rec.primaryCommitReached:
	case <-time.After(10 * time.Second):
		s.t.Fatal("writer never reached the primary commit")
	}
}

// releaseWriter lets C commit its primary; C is acknowledged when this returns. Its secondaries stay locked.
func (s *lockHintScenario) releaseWriter() {
	close(s.rec.releasePrimaryCommit)
	select {
	case err := <-s.commitCh:
		require.NoError(s.t, err)
	case <-time.After(10 * time.Second):
		s.t.Fatal("writer commit did not finish")
	}
}

func lockHintRows(vals ...int) [][]any {
	out := make([][]any, 0, len(vals))
	for _, v := range vals {
		out = append(out, []any{fmt.Sprint(v)})
	}
	return out
}

func (s *lockHintScenario) readCached(handle int64) ([][]any, []lockHintGet) {
	s.reader.MustExec(fmt.Sprintf("set @id = %d", handle))
	before, _ := s.rec.counters()
	got := s.reader.MustQuery("execute p using @id").Rows()
	return got, s.rec.getsFor(handle, before)
}

func (s *lockHintScenario) assertHintLearned(execN []lockHintGet) (cTxn uint64) {
	require.GreaterOrEqual(s.t, len(execN), 2)
	require.Empty(s.t, execN[0].resolved, "the first request of execution N carries no hint")
	last := execN[len(execN)-1]
	require.NotEmpty(s.t, last.resolved, "the retry bypasses the in-flight txn through a resolved-lock hint")
	return last.resolved[len(last.resolved)-1]
}

// Entry point 1: the first lock a MaxTS read meets. CheckTxnStatus with caller_start_ts = MaxUint64 answers
// MinCommitTSPushed for an uncommitted classic 2PC transaction, and client-go stores it in the snapshot's hints.
func TestReusedPointGetExecutorDropsPushedLockHint(t *testing.T) {
	s := newLockHintScenario(t)
	s.startWriterUpToPrimaryCommit()

	gotN, execN := s.readCached(2)
	require.Equal(t, lockHintRows(20), gotN, "the pre-commit value is correct while the writer is in flight")
	cTxn := s.assertHintLearned(execN)

	s.releaseWriter()
	s.fresh.MustQuery("select v from t where id = 1").Check(lockHintRows(11))

	gotN1, execN1 := s.readCached(2)
	require.NotContains(t, execN1[0].resolved, cTxn)
	require.Equal(t, lockHintRows(21), gotN1)

	gotN2, execN2 := s.readCached(3)
	require.NotContains(t, execN2[0].resolved, cTxn)
	require.Equal(t, lockHintRows(31), gotN2)
}

// Entry point 2: the second lock from a different transaction inside one Get, which client-go adds to the hints
// without checking its status.
func TestReusedPointGetExecutorDropsSecondLockHint(t *testing.T) {
	s := newLockHintScenario(t)
	s.startWriterUpToPrimaryCommit()

	s.rec.fakeFirstLockHandle.Store(2)
	gotN, execN := s.readCached(2)
	require.Equal(t, lockHintRows(20), gotN)
	require.Len(t, execN[len(execN)-1].resolved, 2)

	s.releaseWriter()

	gotN1, _ := s.readCached(2)
	require.Equal(t, lockHintRows(21), gotN1)
}

// Committed-transaction hints may outlive a statement: "transaction T committed" never becomes false, so reading
// through T's leftover lock stays correct, and a later write to the key still wins.
func TestReusedPointGetExecutorKeepsCommittedLockHint(t *testing.T) {
	s := newLockHintScenario(t)
	s.startWriterUpToPrimaryCommit()
	s.releaseWriter()

	gotN, execN := s.readCached(2)
	require.Equal(t, lockHintRows(21), gotN)
	last := execN[len(execN)-1]
	require.Empty(t, last.resolved)
	require.NotEmpty(t, last.committed)

	gotN1, _ := s.readCached(3)
	require.Equal(t, lockHintRows(31), gotN1)

	s.writer.MustExec("update t set v = 99 where id = 3")
	gotN2, _ := s.readCached(3)
	require.Equal(t, lockHintRows(99), gotN2)
	gotN3, _ := s.readCached(2)
	require.Equal(t, lockHintRows(21), gotN3)
}
