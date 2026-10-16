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
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func preparedLookup(t *testing.T, tk *testkit.TestKit, query string) (func(arg any) [][]any, func() any) {
	stmtID, _, _, err := tk.Session().PrepareStmt(query)
	require.NoError(t, err)
	lookup := func(arg any) [][]any {
		rs, err := tk.Session().ExecutePreparedStmt(context.Background(), stmtID, expression.Args2Expressions4Test(arg))
		require.NoError(t, err)
		return tk.ResultSetToResult(rs, query).Rows()
	}
	cachedExecutor := func() any {
		return tk.Session().GetSessionVars().PreparedStmts[stmtID].(*plannercore.PlanCacheStmt).PointGet.Executor
	}
	return lookup, cachedExecutor
}

// A cached executor skips the privilege and table lock checks of later executions, so unique index reads at MaxTS
// are executed with a fresh executor every time.
func TestUniqueKeyPointGetAtMaxTSIsNotCached(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_unique_key_point_get_max_ts = ON")
	tk.MustExec("create table t (id int primary key, uk int, unique key uk_idx (uk))")
	tk.MustExec("insert into t values (1, 10)")
	lookup, cachedExecutor := preparedLookup(t, tk, "select id from t where uk = ?")

	require.Equal(t, testkit.Rows("1"), lookup(10))
	require.Equal(t, testkit.Rows("1"), lookup(10))
	tk.MustQuery("select json_extract(@@tidb_last_txn_info, '$.start_ts')").Check(testkit.Rows("18446744073709551615"))
	require.Nil(t, cachedExecutor())
}
