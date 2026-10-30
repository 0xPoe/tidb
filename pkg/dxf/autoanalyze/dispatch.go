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

package autoanalyze

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"go.uber.org/zap"
)

const (
	// cancelRetryInterval is how long to wait before trying to cancel a task again.
	cancelRetryInterval = time.Second
	// cancelWaitTimeout is how long a killed statement waits for its task to
	// stop. A cancelled task stops within seconds. The wait must end even if it
	// does not, e.g. when the node shuts down and can no longer read the task.
	cancelWaitTimeout = 10 * time.Second
)

// Register registers the auto-analyze task type to DXF and hooks the stats
// owner so that it dispatches auto-analyze statements through DXF when
// tidb_enable_dist_auto_analyze is ON.
func Register() {
	scheduler.RegisterSchedulerFactory(proto.AutoAnalyze,
		func(ctx context.Context, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
			return newScheduler(ctx, task, param)
		},
	)
	taskexecutor.RegisterTaskType(proto.AutoAnalyze,
		func(ctx context.Context, task *proto.Task, param taskexecutor.Param) taskexecutor.TaskExecutor {
			return newTaskExecutor(ctx, task, param)
		},
	)
	exec.SetDistRunner(dispatch)
}

// dispatch runs one analyze statement as a DXF task and waits for it, so the slot of
// tidb_auto_analyze_concurrency stays taken. The statement stays an auto-analyze
// process here, so whatever kills such a process also cancels the task.
func dispatch(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
	req *exec.DistRequest,
) error {
	taskMeta, err := newTaskMeta(sctx, req)
	if err != nil {
		return err
	}
	if _, err := storage.GetDXFSvcTaskMgr(); err != nil {
		// DXF is not ready on this node yet. The priority queue analyzes the table again later.
		return errors.Trace(err)
	}
	meta, err := json.Marshal(taskMeta)
	if err != nil {
		return errors.Trace(err)
	}

	sessVars := sctx.GetSessionVars()
	procID := statsHandle.AutoAnalyzeProcID()
	defer statsHandle.ReleaseAutoAnalyzeProcID(procID)
	tracker := statsutil.NewAutoAnalyzeTracker(sysProcTracker.Track, sysProcTracker.UnTrack)
	if err := tracker.Track(procID, sctx); err != nil {
		return errors.Trace(err)
	}
	// Untracking also resets the killer, so no kill signal is left to the next
	// user of the session.
	defer tracker.UnTrack(procID)
	// Tracking resets the killer, so the channel must be taken after it.
	killed := sessVars.SQLKiller.GetKillEventChan()
	defer showAsRunning(sctx, taskMeta)()

	keyspace := sctx.GetStore().GetKeyspace()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	if kv.IsUserKS(sctx.GetStore()) {
		// A DXF service node cannot ask the nodes of this keyspace to flush their
		// stats deltas, so flush them before submitting.
		if _, _, err := statsutil.ExecWithOpts(sctx, nil, "flush stats_delta %n.%n cluster", taskMeta.Params[0], taskMeta.Params[1]); err != nil {
			return errors.Annotate(err, "flush stats delta before dispatching auto analyze")
		}
	}
	slots, err := requiredSlots(ctx, sctx, taskMeta)
	if err != nil {
		return err
	}
	taskKey := fmt.Sprintf("%s/%s/%s", keyspace, proto.AutoAnalyze, uuid.NewString())
	start := time.Now()
	task, err := handle.SubmitTask(ctx, taskKey, proto.AutoAnalyze, keyspace, slots, handle.GetTargetScope(), 1, meta)
	if err != nil {
		// The task may have been created, do not leave it running unobserved.
		if cancelErr := handle.CancelTask(ctx, taskKey); cancelErr != nil {
			statslogutil.StatsLogger().Warn("cancel auto analyze task failed", zap.String("taskKey", taskKey), zap.Error(cancelErr))
		}
		return errors.Trace(err)
	}
	err = waitTask(ctx, task, killed)
	if err == nil {
		// A statement that runs here ends with loading the new stats into the cache
		// of this node. The priority queue relies on that when a job finishes,
		// otherwise it may see the old stats and queue the table again.
		err = loadTableStats(ctx, sctx, statsHandle, taskMeta.TableID)
	}
	if killErr := sessVars.SQLKiller.HandleSignal(); killErr != nil {
		err = killErr
	}
	statslogutil.StatsLogger().Info("auto analyze dispatched through DXF",
		zap.Int64("taskID", task.ID), zap.String("taskKey", taskKey), zap.Int("slots", slots),
		zap.String("sql", taskMeta.SQL), zap.Strings("params", taskMeta.Params),
		zap.Duration("cost", time.Since(start)), zap.Error(err))
	return err
}

// loadTableStats loads the stats of the table and its partitions from the
// storage into the cache of the stats handle.
func loadTableStats(ctx context.Context, sctx sessionctx.Context, statsHandle statstypes.StatsHandle, tableID int64) error {
	is := sctx.GetLatestInfoSchema().(infoschema.InfoSchema)
	tblInfo, ok := is.TableInfoByID(tableID)
	if !ok {
		// Nothing to load for a table that is gone.
		return nil
	}
	ids := []int64{tblInfo.ID}
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		for _, def := range pi.Definitions {
			ids = append(ids, def.ID)
		}
	}
	return errors.Trace(statsHandle.Update(ctx, is, ids...))
}

// processInfoSetter is implemented by the sessions that show what they run.
type processInfoSetter interface {
	SetProcessInfo(sql string, t time.Time, command byte, maxExecutionTime uint64)
}

// showAsRunning shows the statement as the running query of the session until done
// is called, so it is visible here and tidb_max_auto_analyze_time applies to it.
func showAsRunning(sctx sessionctx.Context, meta *TaskMeta) (done func()) {
	setter, ok := sctx.(processInfoSetter)
	if !ok {
		return func() {}
	}
	sql, err := sqlescape.EscapeSQL(meta.SQL, meta.args()...)
	if err != nil {
		sql = meta.SQL
	}
	// The statements of an internal session show as ComSleep, as such a session
	// has no command. With another command the process would also show the start
	// TS of the last transaction of the session, and hold back GC while it waits.
	setter.SetProcessInfo(sql, time.Now(), mysql.ComSleep, 0)
	return func() {
		setter.SetProcessInfo("", time.Now(), mysql.ComSleep, 0)
	}
}

// newTaskMeta builds the task meta of the statement.
func newTaskMeta(sctx sessionctx.Context, req *exec.DistRequest) (*TaskMeta, error) {
	meta := &TaskMeta{
		SQL:                    req.SQL,
		Params:                 make([]string, 0, len(req.Params)),
		NeedVersionRewriteWarn: req.NeedVersionRewriteWarn,
	}
	for _, p := range req.Params {
		s, ok := p.(string)
		if !ok {
			return nil, errors.Errorf("unexpected parameter %v of the auto analyze statement", p)
		}
		meta.Params = append(meta.Params, s)
	}
	// Every auto-analyze statement starts with the schema and the table name.
	if len(meta.Params) < 2 {
		return nil, errors.Errorf("the auto analyze statement %q names no table", req.SQL)
	}
	tblInfo, err := sctx.GetLatestInfoSchema().TableInfoByName(ast.NewCIStr(meta.Params[0]), ast.NewCIStr(meta.Params[1]))
	if err != nil {
		return nil, errors.Trace(err)
	}
	meta.DBID, meta.TableID = tblInfo.DBID, tblInfo.ID
	return meta, nil
}

// requiredSlots returns the tables or partitions the statement analyzes at
// once, capped at one node: a worker of ANALYZE builds one of them at a time.
// The scan and the sampling, most of its work, run in TiKV and take no slots.
func requiredSlots(ctx context.Context, sctx sessionctx.Context, meta *TaskMeta) (int, error) {
	concurrency, err := analyzeConcurrency(ctx, sctx)
	if err != nil {
		return 0, err
	}
	nodeCPU, err := handle.GetCPUCountOfNode(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return min(concurrency, analyzeUnits(meta), nodeCPU), nil
}

// analyzeUnits returns the tables or partitions the statement analyzes. Its
// parameters are the schema, the table, any partitions, and any index.
func analyzeUnits(meta *TaskMeta) int {
	units := len(meta.Params) - 2
	if strings.Contains(meta.SQL, " index ") {
		units--
	}
	return max(units, 1)
}

// analyzeConcurrency returns tidb_auto_build_stats_concurrency of the session:
// the tables or partitions an auto-analyze statement analyzes at once.
func analyzeConcurrency(ctx context.Context, sctx sessionctx.Context) (int, error) {
	val, err := sctx.GetSessionVars().GetSessionOrGlobalSystemVar(ctx, vardef.TiDBAutoBuildStatsConcurrency)
	if err != nil {
		return 0, errors.Trace(err)
	}
	c, err := strconv.Atoi(val)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return max(c, 1), nil
}

// waitTask waits until the task is done. It cancels the task once killed is
// closed and then waits for the task to stop, so that the statement no longer
// runs anywhere when waitTask returns, but for cancelWaitTimeout at most.
func waitTask(ctx context.Context, task *proto.Task, killed <-chan struct{}) error {
	waitCtx, stopWaiting := context.WithCancel(ctx)
	defer stopWaiting()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-killed:
		case <-waitCtx.Done():
			return
		}
		timer := time.AfterFunc(cancelWaitTimeout, stopWaiting)
		defer timer.Stop()
		cancelTask(waitCtx, task.Key)
		<-waitCtx.Done()
	}()
	found, err := handle.WaitTaskDoneOrPausedWithResult(waitCtx, task.ID)
	stopWaiting()
	wg.Wait()
	if err != nil {
		return errors.Trace(err)
	}
	if found.State != proto.TaskStateSucceed {
		return errors.Errorf("auto analyze task %d stopped with state %s", task.ID, found.State)
	}
	return nil
}

// cancelTask cancels the task, and tries again on failure until ctx is done.
func cancelTask(ctx context.Context, taskKey string) {
	for {
		err := handle.CancelTask(ctx, taskKey)
		if err == nil {
			return
		}
		statslogutil.StatsLogger().Warn("cancel auto analyze task failed", zap.String("taskKey", taskKey), zap.Error(err))
		select {
		case <-ctx.Done():
			return
		case <-time.After(cancelRetryInterval):
		}
	}
}
