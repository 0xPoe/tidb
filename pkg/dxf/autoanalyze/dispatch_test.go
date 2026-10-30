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

package autoanalyze_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/dxf/autoanalyze"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	statstestutil "github.com/pingcap/tidb/pkg/statistics/handle/ddl/testutil"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestAutoAnalyzeThroughDXF(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, index idx(a))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("set global tidb_enable_dist_auto_analyze = ON")
	defer tk.MustExec("set global tidb_enable_dist_auto_analyze = OFF")

	h := dom.StatsHandle()
	require.NoError(t, statstestutil.HandleNextDDLEventWithTxn(h))
	tk.MustExec("flush stats_delta *.*")
	require.NoError(t, h.Update(context.Background(), dom.InfoSchema()))
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	require.True(t, h.HandleAutoAnalyze())

	// The statement went through DXF and succeeded there.
	tk.MustQuery("select type, state from mysql.tidb_global_task union all " +
		"select type, state from mysql.tidb_global_task_history").Check(testkit.Rows("AutoAnalyze succeed"))
	// It is still recorded as an auto analyze job, and the stats are there.
	tk.MustQuery("select table_name, job_info, state from mysql.analyze_jobs").Check(
		testkit.Rows("t auto analyze table all indexes, all columns with 256 buckets, 100 topn, 1 samplerate finished"))
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	require.NoError(t, h.Update(context.Background(), dom.InfoSchema()))
	require.Equal(t, int64(3), h.GetPhysicalTableStats(tblInfo.ID, tblInfo).RealtimeCount)
	require.True(t, h.GetPhysicalTableStats(tblInfo.ID, tblInfo).IsAnalyzed())
}

// TestKillDispatchedAutoAnalyze kills the auto-analyze process of the stats
// owner while the statement runs as a DXF task, the way KILL,
// tidb_max_auto_analyze_time, or the shutdown of the node does. The task must be
// cancelled and the statement must stop on the node that runs it.
func TestKillDispatchedAutoAnalyze(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, index idx(a))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("set global tidb_enable_dist_auto_analyze = ON")
	defer tk.MustExec("set global tidb_enable_dist_auto_analyze = OFF")
	h := dom.StatsHandle()
	require.NoError(t, statstestutil.HandleNextDDLEventWithTxn(h))
	tk.MustExec("flush stats_delta *.*")
	require.NoError(t, h.Update(context.Background(), dom.InfoSchema()))
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()

	// The statement blocks until it is killed.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockSlowAnalyzeV2", "return")
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.HandleAutoAnalyze()
	}()
	// There are two auto-analyze processes showing the statement: the one that
	// waits for the task on the stats owner, which started first, and the one
	// that runs the statement.
	var waiting uint64
	require.Eventually(t, func() bool {
		running := dom.SysProcTracker().GetSysProcessList()
		if len(statsutil.GlobalAutoAnalyzeProcessList.All()) != 2 || len(running) != 2 {
			return false
		}
		var first time.Time
		for id, info := range running {
			if info.Info != "analyze table `test`.`t`" {
				return false
			}
			if first.IsZero() || info.Time.Before(first) {
				waiting, first = id, info.Time
			}
		}
		return true
	}, 30*time.Second, 50*time.Millisecond)
	waitingStartTS := dom.SysProcTracker().GetSysProcessList()[waiting].CurTxnStartTS
	dom.SysProcTracker().KillSysProcess(waiting)

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		require.FailNow(t, "the auto analyze did not stop after it was killed")
	}
	// The waiting process held no transaction, so it did not hold back GC.
	require.Zero(t, waitingStartTS)
	require.Empty(t, statsutil.GlobalAutoAnalyzeProcessList.All())
	require.Empty(t, dom.SysProcTracker().GetSysProcessList())
	tk.MustQuery("select type, state from mysql.tidb_global_task union all " +
		"select type, state from mysql.tidb_global_task_history").Check(testkit.Rows("AutoAnalyze reverted"))
	tk.MustQuery("select table_name, state from mysql.analyze_jobs").Check(testkit.Rows("t failed"))
}

// TestStatementPoolWaitsForUsers checks that the session pool of a statement
// is only idle after everybody who works in one of its sessions is done.
func TestStatementPoolWaitsForUsers(t *testing.T) {
	store := testkit.CreateMockStore(t)
	pool := autoanalyze.NewStatementPool(syssession.NewAdvancedSessionPool(4, func() (syssession.SessionContext, error) {
		return testkit.NewTestKit(t, store).Session(), nil
	}))
	defer pool.Close()
	working, finish := make(chan struct{}), make(chan struct{})
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- pool.WithSession(func(*syssession.Session) error {
			close(working)
			<-finish
			return nil
		})
	}()
	<-working
	idle := make(chan struct{})
	go func() {
		pool.WaitIdle()
		close(idle)
	}()
	// Nobody else gets a session any more, and the pool is not idle yet.
	require.Eventually(t, func() bool {
		return pool.WithSession(func(*syssession.Session) error { return nil }) != nil
	}, 10*time.Second, 10*time.Millisecond)
	select {
	case <-idle:
		require.FailNow(t, "the pool is idle while a session is in use")
	case <-time.After(100 * time.Millisecond):
	}
	close(finish)
	require.NoError(t, <-workerDone)
	<-idle
}

func TestLimitAnalyzeConcurrency(t *testing.T) {
	// The slots of a statement are capped at the CPU count of a node. With 16
	// CPUs, that cap is above all the slots checked below.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(16)")
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	ctx := context.Background()
	tk.MustExec("set global tidb_auto_build_stats_concurrency = 6")
	tk.MustExec("set global tidb_build_sampling_stats_concurrency = 3")
	tk.MustExec("set global tidb_sysproc_scan_concurrency = 2")
	// A session of the stats handle runs the statement. It has not loaded any of
	// the settings yet.
	require.NoError(t, statsutil.CallWithSCtx(dom.StatsHandle().SPool(), func(sctx sessionctx.Context) error {
		// What an auto-analyze statement in the session works with.
		settings := func() []string {
			res := make([]string, 0, 3)
			for _, name := range []string{
				vardef.TiDBAutoBuildStatsConcurrency, vardef.TiDBBuildSamplingStatsConcurrency, vardef.TiDBSysProcScanConcurrency,
			} {
				val, err := sctx.GetSessionVars().GetSessionOrGlobalSystemVar(ctx, name)
				require.NoError(t, err)
				res = append(res, val)
			}
			return res
		}
		limit := func(slots int64) (restore func()) {
			return autoanalyze.LimitAnalyzeConcurrency(sctx, &proto.StepResource{CPU: proto.NewAllocatable(slots)})
		}

		// Only the build concurrency is lowered to the slots, and is back afterwards.
		restore := limit(2)
		require.Equal(t, []string{"2", "3", "2"}, settings())
		restore()
		require.Equal(t, []string{"6", "3", "2"}, settings())
		for _, slots := range []int64{6, 8} {
			restore = limit(slots)
			require.Equal(t, []string{"6", "3", "2"}, settings())
			restore()
			require.Equal(t, []string{"6", "3", "2"}, settings())
		}
		return nil
	}))

	// The statement asks for the tables or partitions it analyzes at once as its
	// slots. These are the statement forms of the priority queue.
	dxfCtx := kv.WithInternalSourceType(ctx, kv.InternalDistTask)
	for _, c := range []struct {
		sql    string
		params []string
		slots  int
	}{
		{"analyze table %n.%n", []string{"test", "t"}, 1},
		{"analyze table %n.%n index %n", []string{"test", "t", "idx"}, 1},
		{"analyze table %n.%n partition %n", []string{"test", "t", "p0"}, 1},
		{"analyze table %n.%n partition %n index %n", []string{"test", "t", "p0", "idx"}, 1},
		{"analyze table %n.%n partition %n, %n, %n", []string{"test", "t", "p0", "p1", "p2"}, 3},
		{"analyze table %n.%n partition %n, %n, %n index %n", []string{"test", "t", "p0", "p1", "p2", "idx"}, 3},
		// The build concurrency caps the partitions analyzed at once.
		{"analyze table %n.%n partition %n, %n, %n, %n, %n, %n, %n, %n",
			[]string{"test", "t", "p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7"}, 6},
	} {
		slots, err := autoanalyze.RequiredSlots(dxfCtx, tk.Session(), &autoanalyze.TaskMeta{SQL: c.sql, Params: c.params})
		require.NoError(t, err)
		require.Equal(t, c.slots, slots, c.sql)
	}

	// The statement goes to the node with the fewest used slots. With equal used
	// slots, tasks with consecutive IDs start at consecutive nodes in host order.
	nodes := []string{"a", "b", "c"}
	require.Equal(t, []string{"a", "b", "c"}, autoanalyze.OrderByUsedSlots(nodes, nil, 3))
	require.Equal(t, []string{"b", "c", "a"}, autoanalyze.OrderByUsedSlots(nodes, nil, 4))
	// Only the nodes with equal used slots rotate. A node without subtasks has
	// none, and the used slots of other nodes do not matter.
	nodes = []string{"a", "b", "c", "d"}
	usedSlots := map[string]int{"a": 3, "c": 5, "d": 0, "other-scope": 1}
	require.Equal(t, []string{"b", "d", "a", "c"}, autoanalyze.OrderByUsedSlots(nodes, usedSlots, 4))
	require.Equal(t, []string{"d", "b", "a", "c"}, autoanalyze.OrderByUsedSlots(nodes, usedSlots, 7))
}
