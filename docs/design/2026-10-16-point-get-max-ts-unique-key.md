# Proposal: per-statement lock hints for reused point gets, and MaxTS reads for unique-key point gets

- Author(s): [Stephen Thorn](https://github.com/stephenlthorn), drafted and tested with Claude (Anthropic)
- Discussion PR: not opened yet (this branch lives on a fork)
- Tracking Issue: none yet

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

Two changes to the autocommit point-get path, both aimed at sub-millisecond p99 point lookups without weakening consistency:

1. **Bug fix (default behaviour).** A prepared point get that reuses its cached `PointGetExecutor` also reused the executor's
   `KVSnapshot`, and with it the snapshot's lock-resolution hints. A hint that says "skip transaction T's locks" is only valid while T
   is uncommitted. Carried into a later execution, it skips T's leftover secondary lock after T committed and returns a value older
   than an acknowledged write. `Recreated` now resets the snapshot (`SetSnapshotTS(MaxUint64)`), which drops those hints.
2. **Opt-in feature.** With `tidb_enable_unique_key_point_get_max_ts = ON`, an autocommit point get through a unique secondary index
   (or a non-clustered primary key) reads at MaxTS like a clustered-PK point get already does, instead of first fetching a TSO from
   PD. Because the index entry and the row are read at two different moments, the executor re-encodes the unique key from the row it
   read and only accepts the row if the key still matches; otherwise it repeats the lookup on a fresh TSO snapshot.

## Motivation or Background

With default settings an autocommit clustered-PK point get never talks to PD: the optimistic provider picks `start_ts = MaxUint64`
(`sessiontxn/isolation/optimistic.go`), so the critical path is one TiDB to TiKV RPC. A unique-key point get is excluded
(`IsPointGetWithPKOrUniqueKeyByAutoCommit`) because its two reads must see one snapshot, so it pays a TSO round trip and then two
sequential Gets. A source-level latency model of both paths (RTT 100 us, 16-32 vCPU, about 50% CPU) puts the clustered-PK prepared
path near 1 ms p99 client-observed and the unique-key path near 2 ms; removing the TSO is worth about 0.35 ms at p99 for the unique-key shape.

While proving the MaxTS double read safe, a harness that checks linearizability of prepared point gets against a 2PC writer found
that today's clustered-PK path already returns stale data: on a three-node AWS cluster (c6i/m6id, one AZ) upstream TiDB returned
2,372 stale and 2,049 non-monotonic reads out of 118,348 prepared PK point gets. Text-protocol reads were never affected, because they
build a fresh executor and snapshot per statement.

## Detailed Design

### 1. Lock hints on reused executors

A MaxTS read that meets a lock asks for the owner's status with `caller_start_ts = MaxUint64`. TiKV answers `MinCommitTSPushed` for an
uncommitted classic 2PC transaction, and client-go records the transaction in the snapshot's `resolvedLocks`; a second lock from a
different transaction inside one Get is recorded the same way (`txnsnapshot/snapshot.go`). Every later request from that snapshot sends
the set as `bypass_locks`, and TiKV checks `bypass_locks` before anything else. `KVSnapshot.SetSnapshotTS` is the only reset.

`PsStmt.PointGet.Executor` is the only object that keeps a snapshot across statements on the MaxTS path (BatchPointGet and covering reads
are never cached; COM_QUERY builds a new executor). `PointGetExecutor.Recreated` now calls
`e.snapshot.SetOption(kv.SnapshotTS, MaxUint64)`, which clears `resolvedLocks` and the value cache without an allocation.

`committedLocks` is kept on purpose: it only lets a read return the value of a lock whose transaction has already committed, and that
stays true forever; a newer write cannot prewrite the key while the old lock exists. A first version created a fresh snapshot through the
transaction manager instead; it was correct but activated a `KVTxn` per execution (+32 allocations and +2.3 KB per prepared point get in
`BenchmarkPreparedPointGet`), so it was replaced by the reset.

### 2. Unique-key point gets at MaxTS

Eligibility (`isMaxTSSafeUniqueIndexRead`, planner): the sysvar is ON, the plan is a `PointGetPlan` through a unique, non-global index
with no prefix columns and no generated (including expression-index) columns, on a non-partitioned, non-temporary table. Cache tables
stay excluded as before. `FOR UPDATE` in autocommit mode does not lock (MySQL semantics) and is eligible like the PK path.

Execution (`PointGetExecutor.Next`):

1. Read the index entry at MaxTS (time t1). If absent, return empty: no committed row had the key at t1.
2. Read the row at MaxTS (time t2).
3. Decode only the index columns of the row (a row decoder cached per table, index and session time zone), re-encode the unique key
   with `EncodeUniqueIndexKey`, and compare bytes with the key looked up. A NULL column is a mismatch: a point lookup never matches NULL,
   and the encoder would otherwise turn a NULL string into `''`.
4. On a match, return the row. Committed states keep a unique index and its rows in step, so at t2 the index maps the key to this row
   and the row is current: the result is the state at t2, inside the statement's execution interval.
5. On a mismatch or a missing row, fetch a TSO on the statement context and repeat steps 1-2 on a snapshot at that TSO, which is the
   upstream behaviour. The fallback snapshot gets the same read options as the main one, and
   `tidb_executor_point_get_unique_key_max_ts_fallback_total` counts it.

The result can differ from upstream only when a write commits between the two reads; both answers are linearizable (upstream
linearizes at statement start, this design at the row read). `TestPointGetRepeatableRead` pins the upstream answer and keeps passing
because the sysvar defaults to OFF.

Unique-key executors are not cached for reuse (`ExecStmt.PointGet`), because a cached executor skips the privilege and table-lock
checks of later executions. The caching rule is therefore exactly upstream's.

Compatibility: default OFF, GLOBAL and SESSION scope. Partitioned tables, global indexes, temporary tables, cache tables, prefix and
generated-column indexes keep the TSO path. Collations are handled because the key is re-encoded with the column collation. Async commit
and 1PC locks are never ignored at MaxTS (TiKV and client-go wait or resolve them), so lookups of hot keys under async-commit writers gain
nothing at p99. No changes to PD, TiKV, client-go or tooling.

## Test Design

### Functional Tests

- `TestReusedPointGetExecutorDropsPushedLockHint`, `TestReusedPointGetExecutorDropsSecondLockHint`,
  `TestReusedPointGetExecutorKeepsCommittedLockHint` (`pkg/executor/point_get_lock_hint_test.go`): a hijacked client records every Get of
  a cached prepared point get while a classic 2PC writer is parked before its primary commit. The first two fail on the base commit
  (stale value `20` instead of `21`) and pass with the reset.
- `TestUniqueKeyPointGetAtMaxTSRechecksRow`: failpoints pause between the index read and the row read while a writer moves the key away,
  changes another column, deletes the row, moves the key to another row, or sets it to NULL; asserts the result and exactly one fallback
  where expected.
- `TestUniqueKeyPointGetAtMaxTSMatchesUnderCollation`, `TestUniqueKeyPointGetAtMaxTSIsNotCached`,
  `TestIsPointGetUniqueKeyMaxTSEligibility`, `TestOptimisticProviderUniqueKeyPointGetReadTS`.

### Scenario Tests

A linearizability harness (prepared and text protocol, 2PC, async-commit and replace-style writers, assertions for stale,
non-monotonic and false-empty reads) run against real TiKV:

| Target | Prepared PK reads | Stale | Non-monotonic |
|---|---|---|---|
| Upstream TiDB, AWS cluster | 118,348 | 2,372 | 2,049 |
| This branch, AWS cluster | 114,531 | 0 | 0 |
| This branch, unique key ON, plain 2PC / async commit / swap / replace writers | about 340,000 in total | 0 | 0 |

### Benchmark Tests

See [Benchmark results](#benchmark-results).

## Benchmark results

Setup: AWS us-east-1a, one PD plus the benchmark client on c6i.xlarge, TiDB on c6i.2xlarge (8 vCPU), one TiKV on m6id.xlarge
(4 vCPU, local NVMe), nightly PD and TiKV. Upstream TiDB (base commit) and this branch ran side by side against the same PD and TiKV.
Prepared statements, 1M rows, random keys, 20 s per run after 5 s warmup, three interleaved rounds, medians shown. "Server <= 1 ms" is the
share of statements in the `le=0.001` bucket of `tidb_server_handle_query_duration_seconds` (TiDB receives the statement to TiDB
writes the result); p99 is under 1 ms when it is at least 99%. Client p99 includes the client to TiDB network hop.

| Shape | Clients | Build | QPS | Client p50 (us) | Client p99 (us) | Server <= 1 ms | p99 < 1 ms |
|---|---|---|---|---|---|---|---|
| Clustered PK | 4 | upstream | 8,245 | 478 | 610 | 99.999% | yes |
| Clustered PK | 4 | this branch | 8,218 | 480 | 612 | 99.995% | yes |
| Clustered PK | 8 | upstream | 15,376 | 513 | 674 | 99.998% | yes |
| Clustered PK | 8 | this branch | 15,309 | 515 | 678 | 99.997% | yes |
| Clustered PK | 16 | upstream | 26,817 | 585 | 837 | 99.964% | yes |
| Clustered PK | 16 | this branch | 26,697 | 587 | 840 | 99.960% | yes |
| Clustered PK | 32 | upstream | 40,860 | 742 | 1,514 | 97.416% | no |
| Unique key | 4 | upstream | 3,876 | 1,031 | 1,203 | 97.474% | no |
| Unique key | 4 | this branch, ON | 5,316 | 746 | 912 | 99.956% | yes |
| Unique key | 8 | upstream | 6,886 | 1,146 | 1,455 | 56.890% | no |
| Unique key | 8 | this branch, ON | 9,736 | 815 | 1,026 | 99.834% | yes |
| Unique key | 16 | upstream | 12,211 | 1,302 | 1,696 | 17.404% | no |
| Unique key | 16 | this branch, ON | 16,371 | 964 | 1,322 | 95.298% | no |
| Unique key | 32 | upstream | 20,474 | 1,540 | 2,201 | 1.057% | no |
| Unique key | 32 | this branch, ON | 24,785 | 1,257 | 2,007 | 33.607% | no |

With the switch OFF this branch matched upstream within noise on the unique-key shape (for example 97.61% vs 97.47% at 4 clients).
The lock-hint reset costs nothing measurable on the clustered-PK path.

Reading: the clustered-PK path already meets the target up to about 27k QPS on this hardware, and this branch keeps it there while
fixing the stale reads. The unique-key path misses the target at every load upstream and meets it with the switch ON up to about
10k QPS. Above that, both shapes miss as the single 4-vCPU TiKV saturates (roughly 30k Gets per second, and a unique-key lookup is two
Gets); a standard three-TiKV deployment was not tested because of the account's vCPU quota.

`BenchmarkPreparedPointGet` (in-process mock store, sysvar OFF, three runs each on an M3 laptop): upstream 8.73-8.81 us/op, 130
allocs/op, about 9.4 KB/op; this branch 8.85-8.96 us/op, 130 allocs/op, about 9.4 KB/op. The first version of the fix, which built a new
snapshot through the transaction manager, measured 162 allocs/op and 11.7 KB/op, which is why it was replaced by the reset.

## Impacts & Risks

- Under heavy key churn the fallback rate is about the owner-change rate times the gap between the two reads (measured 1-4% at 100-265
  changes per second on one hot key); each fallback costs one TSO plus two Gets. Watch the fallback counter before enabling it for
  write-heavy unique keys.
- `@@tidb_last_txn_info` reports `start_ts = MaxUint64` for MaxTS unique-key reads, including ones that fell back; the fallback TSO wait
  is not added to `Wait_TS` in the slow log.
- The lock-hint reset changes default behaviour for every prepared point get, but only by removing hints that were already invalid.

## Investigation & Alternatives

The same source map ranked other levers for the clustered-PK path, each worth roughly 90-135 us of p99 in the model and left as follow-ups:
running lease-valid, cache-resident gets inline on TiKV's gRPC thread; a caller-side direct send in client-go's batch client (or
`tikv-client.batch-policy = "basic"`); and GC/allocation reduction in tidb-server. For unique keys, resolving the index entry and the row in
one storage request (building on index lookup pushdown) would remove the second Get as well. A proxy in front of TiDB costs about as
much p99 as all three structural changes recover.

## Unresolved Questions

- Tests for lock interplay during the MaxTS double read (prewrite locks on the index or row key with async commit and 1PC), fallback
  TSO errors, and a SQL integration test for the sysvar.
- Whether the fallback TSO should be reflected in `tidb_last_txn_info` and `Wait_TS`.
- `make bazel_prepare` was not run; BUILD.bazel was updated by hand.
