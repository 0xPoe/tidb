// Copyright 2024 PingCAP, Inc.
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

package exec

import (
	"context"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

func execOptionForAnalyzeVersion(statsVer int, needVersionRewriteWarn bool, sql string, params ...any) sqlexec.OptionFuncAlias {
	if needVersionRewriteWarn {
		escapedSQL, err := sqlescape.EscapeSQL(sql, params...)
		intest.Assert(err == nil, "sql escaping should not fail")
		// No need to fail the analyze task if the SQL escaping fails, just log the original SQL.
		if err != nil {
			escapedSQL = sql
		}
		statslogutil.StatsLogger().Warn(
			"auto analyze rewrites legacy statistics version 1 to version 2",
			zap.String("sql", escapedSQL),
		)
	}
	intest.Assert(statsVer == statistics.Version2, "auto analyze should use stats version 2")
	return sqlexec.ExecOptionAnalyzeVer2
}

// DistRequest is an auto-analyze statement that may run on another TiDB node.
type DistRequest struct {
	// SQL is the statement template, such as "analyze table %n.%n".
	SQL string
	// Params are the arguments of SQL.
	Params []any
	// NeedVersionRewriteWarn tells the node that runs the statement to log that
	// it rewrites version 1 statistics.
	NeedVersionRewriteWarn bool
}

// DistRunner runs an auto-analyze statement on some TiDB node and waits for its
// result. sctx is an idle session of the current node, whose keyspace owns the
// statement.
type DistRunner func(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
	req *DistRequest,
) error

var distRunner atomic.Pointer[DistRunner]

// SetDistRunner sets the runner used when tidb_enable_dist_auto_analyze is ON.
func SetDistRunner(r DistRunner) {
	distRunner.Store(&r)
}

// AutoAnalyze executes the auto analyze task.
func AutoAnalyze(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
	statsVer int,
	needVersionRewriteWarn bool,
	sql string,
	params ...any,
) bool {
	startTime := time.Now()
	var err error
	if r := distRunner.Load(); r != nil && vardef.EnableDistAutoAnalyze.Load() {
		req := &DistRequest{SQL: sql, Params: params, NeedVersionRewriteWarn: needVersionRewriteWarn}
		err = (*r)(sctx, statsHandle, sysProcTracker, req)
	} else {
		_, _, err = RunAnalyzeStmt(sctx, statsHandle, sysProcTracker, statsVer, needVersionRewriteWarn, sql, params...)
	}
	dur := time.Since(startTime)
	metrics.AutoAnalyzeHistogram.Observe(dur.Seconds())
	if err != nil {
		escaped, err1 := sqlescape.EscapeSQL(sql, params...)
		if err1 != nil {
			escaped = ""
		}
		statslogutil.StatsErrVerboseSampleLogger().Error(
			"auto analyze failed",
			zap.String("sql", escaped),
			zap.Duration("cost_time", dur),
			zap.Error(err),
		)
		metrics.AutoAnalyzeCounter.WithLabelValues("failed").Inc()
		return false
	}
	metrics.AutoAnalyzeCounter.WithLabelValues("succ").Inc()
	return true
}

// RunAnalyzeStmt executes the analyze statement.
func RunAnalyzeStmt(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
	statsVer int,
	needVersionRewriteWarn bool,
	sql string,
	params ...any,
) ([]chunk.Row, []*resolve.ResultField, error) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Warn("panic in execAnalyzeStmt", zap.Any("error", r), zap.Stack("stack"))
		}
	}()
	return RunAnalyzeStmtWithCtx(statsutil.StatsCtx, sctx, statsHandle, sysProcTracker, statsVer, needVersionRewriteWarn, sql, params...)
}

// RunAnalyzeStmtWithCtx is RunAnalyzeStmt that stops when ctx, which must carry an
// internal source type, is cancelled. It does not recover from a panic, so the caller
// cannot take an unfinished statement as finished.
func RunAnalyzeStmtWithCtx(
	ctx context.Context,
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
	statsVer int,
	needVersionRewriteWarn bool,
	sql string,
	params ...any,
) ([]chunk.Row, []*resolve.ResultField, error) {
	pruneMode := sctx.GetSessionVars().PartitionPruneMode.Load()
	analyzeSnapshot := sctx.GetSessionVars().EnableAnalyzeSnapshot
	autoAnalyzeTracker := statsutil.NewAutoAnalyzeTracker(sysProcTracker.Track, sysProcTracker.UnTrack)
	autoAnalyzeProcID := statsHandle.AutoAnalyzeProcID()
	optFuncs := []sqlexec.OptionFuncAlias{
		execOptionForAnalyzeVersion(statsVer, needVersionRewriteWarn, sql, params...),
		sqlexec.GetAnalyzeSnapshotOption(analyzeSnapshot),
		sqlexec.GetPartitionPruneModeOption(pruneMode),
		sqlexec.ExecOptionUseCurSession,
		sqlexec.ExecOptionWithSysProcTrack(autoAnalyzeProcID, autoAnalyzeTracker.Track, autoAnalyzeTracker.UnTrack),
	}
	// Some steps of an auto-analyze statement, like merging the global stats,
	// only watch the SQL killer, so a cancellation of ctx is also delivered as a
	// kill of the process. The session itself handles a cancellation that comes
	// before the statement starts: it checks ctx after it has reset the killer.
	killed := make(chan struct{})
	stopKill := context.AfterFunc(ctx, func() {
		defer close(killed)
		sysProcTracker.KillSysProcess(autoAnalyzeProcID)
	})
	defer func() {
		// The kill must be over before the ID is released: a released ID can be
		// given to another process.
		if !stopKill() {
			<-killed
		}
		statsHandle.ReleaseAutoAnalyzeProcID(autoAnalyzeProcID)
	}()
	return statsutil.ExecWithOptsWithCtx(ctx, sctx, optFuncs, sql, params...)
}

// GetAutoAnalyzeParameters gets the auto analyze parameters from mysql.global_variables.
func GetAutoAnalyzeParameters(sctx sessionctx.Context) map[string]string {
	sql := "select variable_name, variable_value from mysql.global_variables where variable_name in (%?, %?, %?)"
	rows, _, err := statsutil.ExecWithOpts(sctx, nil, sql, vardef.TiDBAutoAnalyzeRatio, vardef.TiDBAutoAnalyzeStartTime, vardef.TiDBAutoAnalyzeEndTime)
	if err != nil {
		return map[string]string{}
	}
	parameters := make(map[string]string, len(rows))
	for _, row := range rows {
		parameters[row.GetString(0)] = row.GetString(1)
	}
	return parameters
}

// ParseAutoAnalyzeRatio parses the auto analyze ratio from the string.
func ParseAutoAnalyzeRatio(ratio string) float64 {
	autoAnalyzeRatio, err := strconv.ParseFloat(ratio, 64)
	if err != nil {
		return vardef.DefAutoAnalyzeRatio
	}
	return math.Max(autoAnalyzeRatio, 0)
}

// ParseAutoAnalysisWindow parses the time window for auto analysis.
// It parses the times in UTC location.
func ParseAutoAnalysisWindow(start, end string) (_, _ time.Time, err error) {
	if start == "" {
		start = vardef.DefAutoAnalyzeStartTime
	}
	if end == "" {
		end = vardef.DefAutoAnalyzeEndTime
	}
	s, err := time.ParseInLocation(vardef.FullDayTimeFormat, start, time.UTC)
	if err != nil {
		return s, s, errors.Trace(err)
	}
	e, err := time.ParseInLocation(vardef.FullDayTimeFormat, end, time.UTC)
	return s, e, err
}
