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
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type taskExecutor struct {
	*taskexecutor.BaseTaskExecutor
	runtime sqlsvrapi.Runtime
}

var _ taskexecutor.TaskExecutor = (*taskExecutor)(nil)

func newTaskExecutor(ctx context.Context, task *proto.Task, param taskexecutor.Param) *taskExecutor {
	e := &taskExecutor{
		BaseTaskExecutor: taskexecutor.NewBaseTaskExecutor(ctx, task, param),
		runtime:          param.TaskRuntime,
	}
	e.BaseTaskExecutor.Extension = e
	return e
}

// IsIdempotent returns true: re-running ANALYZE only refreshes the statistics again.
func (*taskExecutor) IsIdempotent(*proto.Subtask) bool {
	return true
}

func (e *taskExecutor) GetStepExecutor(task *proto.Task) (execute.StepExecutor, error) {
	return &stepExecutor{runtime: e.runtime, keyspace: task.Keyspace, taskTable: e.GetTaskTable()}, nil
}

// IsRetryableError returns false: the priority queue of the stats owner
// analyzes the table again later if the statement fails.
func (*taskExecutor) IsRetryableError(error) bool {
	return false
}

type stepExecutor struct {
	taskexecutor.BaseStepExecutor
	runtime   sqlsvrapi.Runtime
	keyspace  string
	taskTable taskexecutor.TaskTable
}

// RunSubtask runs the analyze statement in the task keyspace.
func (s *stepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	meta := &TaskMeta{}
	if err := json.Unmarshal(subtask.Meta, meta); err != nil {
		return errors.Trace(err)
	}
	// The domain of the current node. It serves the task keyspace on the classic
	// kernel, and the SYSTEM keyspace on a DXF service node of the next-gen kernel.
	var localDom *domain.Domain
	if err := s.taskTable.WithNewSession(func(se sessionctx.Context) error {
		localDom = domain.GetDomain(se)
		return nil
	}); err != nil {
		return errors.Trace(err)
	}
	if localDom == nil {
		return errors.New("the current node has no domain")
	}

	start := time.Now()
	var err error
	onServiceNode := localDom.Store().GetKeyspace() != s.keyspace
	if onServiceNode {
		err = runOnServiceNode(ctx, s.runtime, localDom, s.GetResource(), meta)
	} else {
		err = runWithDomain(ctx, localDom, s.GetResource(), meta)
	}
	logutil.Logger(ctx).Info("dist auto analyze subtask finished",
		zap.String("taskKeyspace", s.keyspace), zap.String("sql", meta.SQL), zap.Strings("params", meta.Params),
		zap.Bool("onServiceNode", onServiceNode), zap.Duration("cost", time.Since(start)), zap.Error(err))
	return err
}

// runWithDomain runs the statement on a node of the task keyspace.
func runWithDomain(ctx context.Context, dom *domain.Domain, resource *proto.StepResource, meta *TaskMeta) error {
	statsHandle := dom.StatsHandle()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnStatsForegroundPriority)
	return withStatsSession(statsHandle.SPool(), func(sctx sessionctx.Context) error {
		return runAnalyze(ctx, sctx, statsHandle, dom.SysProcTracker(), resource, meta)
	})
}

// withStatsSession is statsutil.CallWithSCtx without its panic recovery: a panic
// must fail the subtask, not turn an unfinished statement into a success.
func withStatsSession(pool syssession.Pool, f func(sctx sessionctx.Context) error) error {
	return pool.WithSession(func(se *syssession.Session) error {
		return se.WithSessionContext(func(sctx sessionctx.Context) error {
			if err := statsutil.UpdateSCtxVarsForStats(sctx); err != nil {
				return errors.Trace(err)
			}
			return f(sctx)
		})
	})
}

// runAnalyze runs the statement of meta in the session, within the slots of
// the subtask.
func runAnalyze(
	ctx context.Context,
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
	resource *proto.StepResource,
	meta *TaskMeta,
) error {
	restore := limitAnalyzeConcurrency(sctx, resource)
	// The session may go back to a pool.
	defer restore()
	_, _, err := exec.RunAnalyzeStmtWithCtx(ctx, sctx, statsHandle, sysProcTracker,
		statistics.Version2, meta.NeedVersionRewriteWarn, meta.SQL, meta.args()...)
	return err
}

// slotLimitedGlobalVars lowers the global tidb_auto_build_stats_concurrency to
// the slots of a subtask. An internal ANALYZE statement reads that global
// variable rather than the session one, see ExecStmt.Exec.
type slotLimitedGlobalVars struct {
	variable.GlobalVarAccessor
	slots int
}

func (v *slotLimitedGlobalVars) GetGlobalSysVar(name string) (string, error) {
	val, err := v.GlobalVarAccessor.GetGlobalSysVar(name)
	if err != nil || name != vardef.TiDBAutoBuildStatsConcurrency {
		return val, err
	}
	c, err := strconv.Atoi(val)
	if err != nil {
		return "", errors.Trace(err)
	}
	return strconv.Itoa(min(c, v.slots)), nil
}

// limitAnalyzeConcurrency keeps the build concurrency of the statement within
// the slots of the subtask. The returned function undoes that.
func limitAnalyzeConcurrency(sctx sessionctx.Context, resource *proto.StepResource) (restore func()) {
	sessVars := sctx.GetSessionVars()
	globalVars := sessVars.GlobalVarsAccessor
	sessVars.GlobalVarsAccessor = &slotLimitedGlobalVars{
		GlobalVarAccessor: globalVars,
		slots:             int(max(resource.CPU.Capacity(), 1)),
	}
	return func() {
		sessVars.GlobalVarsAccessor = globalVars
	}
}
