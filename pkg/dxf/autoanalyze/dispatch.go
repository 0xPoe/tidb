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
	goerrors "errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain"
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
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"go.uber.org/zap"
)

const (
	// distMinRowCount is the row count from which a statement goes to DXF. A task
	// adds seconds of scheduling, which pays off only for a long statement. The
	// value is a conservative round number, not a measured break-even point.
	distMinRowCount = 1_000_000
	// cancelRetryInterval is how long to wait before trying to cancel a task again.
	cancelRetryInterval = time.Second
	// cancelWaitTimeout is how long a killed statement waits for its task to
	// stop. A cancelled task stops within seconds. The wait must end even if it
	// does not, e.g. when this node cannot reach the storage to read the task.
	cancelWaitTimeout = 10 * time.Second
)

// Register registers the auto-analyze task type to DXF and hooks the stats
// owner so that it dispatches the auto-analyze statements of big tables
// through DXF when tidb_enable_dist_auto_analyze is ON.
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

// dispatch runs the analyze statement of a big table through DXF. It returns
// false, before it changes anything, for a statement that it cannot describe or
// whose table is below the row limit: the caller runs it as it does without DXF.
func dispatch(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
	req *exec.DistRequest,
) (bool, error) {
	taskMeta, ok := newTaskMeta(sctx, req)
	if !ok || !isBigTable(sctx, statsHandle, taskMeta.TableID) {
		return false, nil
	}
	return true, runAsTask(sctx, statsHandle, sysProcTracker, taskMeta)
}

// isBigTable reports whether the stats cache of this node has at least
// distMinRowCount rows for the table. A partitioned table counts the rows of its
// partitions: in static prune mode ANALYZE does not correct the table-level count.
func isBigTable(sctx sessionctx.Context, statsHandle statstypes.StatsHandle, tableID int64) bool {
	tblInfo, ok := sctx.GetLatestInfoSchema().TableInfoByID(tableID)
	if !ok {
		return false
	}
	physicalIDs := []int64{tableID}
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		physicalIDs = make([]int64, 0, len(pi.Definitions))
		for _, def := range pi.Definitions {
			physicalIDs = append(physicalIDs, def.ID)
		}
	}
	var rows int64
	for _, id := range physicalIDs {
		if tblStats, ok := statsHandle.GetNonPseudoPhysicalTableStats(id); ok && !tblStats.Pseudo {
			rows += tblStats.RealtimeCount
		}
	}
	return rows >= distMinRowCount
}

// runAsTask runs the statement as a DXF task and waits for it, so the slot of
// tidb_auto_analyze_concurrency stays taken. The statement stays an auto-analyze
// process here: a kill of it, except by a shutdown, also cancels the task.
func runAsTask(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
	taskMeta *TaskMeta,
) error {
	taskMgr, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
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
	// It runs before the session shows as a process, so nothing of the last
	// statement shows even for a moment.
	forgetLastStatement(sctx)
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
	// The task keys of a table share a prefix, by which activeTask finds them,
	// and end in a UUID: DXF refuses a key that an earlier task has used.
	keyPrefix := fmt.Sprintf("%s/%s/%d/", keyspace, proto.AutoAnalyze, taskMeta.TableID)
	shuttingDown := domain.GetDomain(sctx).AutoAnalyzeStopped
	start := time.Now()
	// A stats owner that shut down or failed can have left a task that runs
	// this statement. If that task succeeds, the statement is done.
	task, err := activeTask(ctx, taskMgr, keyPrefix, taskMeta)
	if err != nil {
		return errors.Trace(err)
	}
	failpoint.InjectCall("afterFindActiveTask")
	adopted := task != nil
	if adopted {
		adopted = waitTask(ctx, task, killed, shuttingDown) == nil
	}
	if !adopted {
		// A kill up to here ends the statement before it has a task of its own.
		if killErr := sessVars.SQLKiller.HandleSignal(); killErr != nil {
			return killErr
		}
		task, err = submitTask(ctx, keyPrefix+uuid.NewString(), keyspace, meta)
		if err != nil {
			return err
		}
		err = waitTask(ctx, task, killed, shuttingDown)
	}
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
		zap.Int64("taskID", task.ID), zap.String("taskKey", task.Key), zap.Bool("adopted", adopted),
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
	ClearProcessInfo()
}

// forgetLastStatement drops the statement context and the process info that the
// session has of its last statement. Shown for the process that waits for a
// task, the runaway checker in them would kill that process.
func forgetLastStatement(sctx sessionctx.Context) {
	sessVars := sctx.GetSessionVars()
	sessVars.StmtCtx = sessVars.InitStatementContext()
	if setter, ok := sctx.(processInfoSetter); ok {
		setter.ClearProcessInfo()
	}
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

// newTaskMeta builds the task meta of the statement. It returns false for a
// statement that the meta cannot describe, which then runs on this node: a
// missing table fails there as it does without DXF.
func newTaskMeta(sctx sessionctx.Context, req *exec.DistRequest) (*TaskMeta, bool) {
	meta := &TaskMeta{
		SQL:                    req.SQL,
		Params:                 make([]string, 0, len(req.Params)),
		NeedVersionRewriteWarn: req.NeedVersionRewriteWarn,
	}
	for _, p := range req.Params {
		s, ok := p.(string)
		if !ok {
			return nil, false
		}
		meta.Params = append(meta.Params, s)
	}
	// The meta needs the schema and the table name, which come first in every
	// statement of the priority queue.
	if len(meta.Params) < 2 {
		return nil, false
	}
	tblInfo, err := sctx.GetLatestInfoSchema().TableInfoByName(ast.NewCIStr(meta.Params[0]), ast.NewCIStr(meta.Params[1]))
	if err != nil {
		return nil, false
	}
	meta.TableID = tblInfo.ID
	return meta, true
}

// activeTask returns a pending or running task that runs the statement of
// meta, or nil if there is none. The tasks of a table can run different
// statements at the same time, e.g. one for each partition in static prune mode.
func activeTask(ctx context.Context, taskMgr *storage.TaskManager, keyPrefix string, meta *TaskMeta) (*proto.TaskBase, error) {
	candidates, err := taskMgr.GetTaskBasesInStates(ctx, proto.TaskStatePending, proto.TaskStateRunning)
	if err != nil {
		return nil, err
	}
	for _, candidate := range candidates {
		if !strings.HasPrefix(candidate.Key, keyPrefix) {
			continue
		}
		task, err := taskMgr.GetTaskByID(ctx, candidate.ID)
		if goerrors.Is(err, storage.ErrTaskNotFound) {
			// The task is done meanwhile, and has moved to the history.
			continue
		}
		if err != nil {
			return nil, err
		}
		if meta.sameStatement(task.Meta) {
			return candidate, nil
		}
	}
	return nil, nil
}

// submitTask submits the statement as a task that runs on one node.
func submitTask(ctx context.Context, taskKey, keyspace string, meta []byte) (*proto.TaskBase, error) {
	// The task asks for one slot whatever the build concurrency of its statement
	// is: the scan and the sampling of ANALYZE, most of its work, run in TiKV.
	task, err := handle.SubmitTask(ctx, taskKey, proto.AutoAnalyze, keyspace, 1, targetScope(), 1, meta)
	if err != nil {
		// The task may have been created, do not leave it running unobserved.
		if cancelErr := handle.CancelTask(ctx, taskKey); cancelErr != nil {
			statslogutil.StatsLogger().Warn("cancel auto analyze task failed", zap.String("taskKey", taskKey), zap.Error(cancelErr))
		}
		return nil, errors.Trace(err)
	}
	return &task.TaskBase, nil
}

// targetScope returns the service scope of the nodes that run the tasks. On the
// classic kernel it is empty, with which DXF prefers the background nodes: the
// scope of the stats owner would limit the tasks to the nodes of that scope.
func targetScope() string {
	if kerneltype.IsNextGen() {
		return handle.NextGenTargetScope
	}
	return ""
}

// waitTask waits until the task succeeds. A kill cancels the task and waits
// until the statement runs nowhere, for cancelWaitTimeout at most. A kill by the
// shutdown of this node only ends the wait, so a task on another node runs on.
func waitTask(ctx context.Context, task *proto.TaskBase, killed <-chan struct{}, shuttingDown func() bool) error {
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
		if shuttingDown() {
			// If this node runs the subtask, the shutdown also kills the
			// statement, and the task fails.
			stopWaiting()
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
