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
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"go.uber.org/zap"
)

const (
	// distMinRowCount is the row count from which a statement goes to DXF. A task
	// adds seconds of scheduling, which pays off only for a long statement. The
	// value is a conservative round number, not a measured break-even point.
	distMinRowCount = 1_000_000
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
	_ sysproctrack.Tracker,
	req *exec.DistRequest,
) (bool, error) {
	taskMeta, ok := newTaskMeta(sctx, req)
	if !ok || !isBigTable(sctx, statsHandle, taskMeta.TableID) {
		return false, nil
	}
	return true, runAsTask(sctx, statsHandle, taskMeta)
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
// tidb_auto_analyze_concurrency stays taken.
func runAsTask(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
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

	keyspace := sctx.GetStore().GetKeyspace()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	// The task keys of a table share a prefix, by which activeTask finds them,
	// and end in a UUID: DXF refuses a key that an earlier task has used.
	keyPrefix := fmt.Sprintf("%s/%s/%d/", keyspace, proto.AutoAnalyze, taskMeta.TableID)
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
		adopted = waitTask(ctx, task) == nil
	}
	if !adopted {
		task, err = submitTask(ctx, keyPrefix+uuid.NewString(), keyspace, meta)
		if err != nil {
			return err
		}
		err = waitTask(ctx, task)
	}
	if err == nil {
		// A statement that runs here ends with loading the new stats into the cache
		// of this node. The priority queue relies on that when a job finishes,
		// otherwise it may see the old stats and queue the table again.
		err = loadTableStats(ctx, sctx, statsHandle, taskMeta.TableID)
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

// waitTask waits until the task succeeds.
func waitTask(ctx context.Context, task *proto.TaskBase) error {
	found, err := handle.WaitTaskDoneOrPausedWithResult(ctx, task.ID)
	if err != nil {
		return errors.Trace(err)
	}
	if found.State != proto.TaskStateSucceed {
		return errors.Errorf("auto analyze task %d stopped with state %s", task.ID, found.State)
	}
	return nil
}
