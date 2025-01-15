// Copyright 2023 PingCAP, Inc.
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

package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"go.uber.org/zap"
)

// UpdateStatsVersion will set statistics version to the newest TS, then
// tidb-server will reload automatic.
func UpdateStatsVersion(ctx context.Context, sctx sessionctx.Context) error {
	startTS, err := statsutil.GetStartTS(sctx)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err = statsutil.ExecWithCtx(
		ctx, sctx, "update mysql.stats_meta set version = %?", startTS,
	); err != nil {
		return err
	}
	if _, err = statsutil.ExecWithCtx(
		ctx, sctx, "update mysql.stats_extended set version = %?", startTS,
	); err != nil {
		return err
	}
	if _, err = statsutil.ExecWithCtx(
		ctx, sctx, "update mysql.stats_histograms set version = %?", startTS,
	); err != nil {
		return err
	}
	return nil
}

// DeltaUpdate is the delta update for stats meta.
type DeltaUpdate struct {
	Delta    variable.TableDelta
	TableID  int64
	IsLocked bool
}

// NewDeltaUpdate creates a new DeltaUpdate.
func NewDeltaUpdate(tableID int64, delta variable.TableDelta, isLocked bool) *DeltaUpdate {
	return &DeltaUpdate{
		Delta:    delta,
		TableID:  tableID,
		IsLocked: isLocked,
	}
}

// UpdateStatsMeta updates the stats meta for multiple tables.
// It uses the INSERT INTO ... ON DUPLICATE KEY UPDATE syntax to fill the missing records.
// Note: Make sure call this function in a transaction.
func UpdateStatsMeta(
	ctx context.Context,
	sctx sessionctx.Context,
	startTS uint64,
	updates ...*DeltaUpdate,
) (err error) {
	if len(updates) == 0 {
		return nil
	}

	// Separate locked and unlocked updates
	lockedUpdates := make([]*DeltaUpdate, 0, 20)
	unlockedUpdates := make([]*DeltaUpdate, 0, len(updates))
	cacheInvalidateIDs := make([]int64, 0, len(updates))

	for _, update := range updates {
		if update.IsLocked {
			lockedUpdates = append(lockedUpdates, update)
		} else {
			unlockedUpdates = append(unlockedUpdates, update)
			cacheInvalidateIDs = append(cacheInvalidateIDs, update.TableID)
		}
	}

	// Enable tidb_enable_prepared_plan_cache to speed up the prepared statement execution
	if _, err = statsutil.ExecWithCtx(ctx, sctx, "set tidb_enable_prepared_plan_cache = true"); err != nil {
		return err
	}
	defer func() {
		_, err = statsutil.ExecWithCtx(ctx, sctx, "set tidb_enable_prepared_plan_cache = false")
		if err != nil {
			statslogutil.StatsLogger().Warn(
				"Failed to disable the prepared plan cache",
				zap.Error(err),
			)
		}
	}()

	// Prepare statements for different update types
	prepareLocked := `
		PREPARE locked_stmt FROM '
		insert into mysql.stats_table_locked (version, table_id, modify_count, count)
		values (?, ?, ?, ?)
		on duplicate key update
			version = values(version),
			modify_count = modify_count + values(modify_count),
			count = count + values(count)'
	`
	if _, err = statsutil.ExecWithCtx(ctx, sctx, prepareLocked); err != nil {
		return err
	}
	defer func() {
		_, err = statsutil.ExecWithCtx(ctx, sctx, "DEALLOCATE PREPARE locked_stmt")
		if err != nil {
			statslogutil.StatsLogger().Warn(
				"Failed to deallocate the locked_stmt prepared statement",
				zap.Error(err),
			)
		}
	}()

	prepareUnlockedPos := `
		PREPARE unlocked_pos_stmt FROM '
		insert into mysql.stats_meta (version, table_id, modify_count, count)
		values (?, ?, ?, ?)
		on duplicate key update
			version = values(version),
			modify_count = modify_count + values(modify_count),
			count = count + values(count)'
	`
	if _, err = statsutil.ExecWithCtx(ctx, sctx, prepareUnlockedPos); err != nil {
		return err
	}
	defer func() {
		_, err = statsutil.ExecWithCtx(ctx, sctx, "DEALLOCATE PREPARE unlocked_pos_stmt")
		if err != nil {
			statslogutil.StatsLogger().Warn(
				"Failed to deallocate the unlocked_pos_stmt prepared statement",
				zap.Error(err),
			)
		}
	}()

	prepareUnlockedNeg := `
		PREPARE unlocked_neg_stmt FROM '
		insert into mysql.stats_meta (version, table_id, modify_count, count)
		values (?, ?, ?, ?)
		on duplicate key update
			version = values(version),
			modify_count = modify_count + values(modify_count),
			count = if(count > values(count), count - values(count), 0)'
	`
	if _, err = statsutil.ExecWithCtx(ctx, sctx, prepareUnlockedNeg); err != nil {
		return err
	}
	defer func() {
		_, err = statsutil.ExecWithCtx(ctx, sctx, "DEALLOCATE PREPARE unlocked_neg_stmt")
		if err != nil {
			statslogutil.StatsLogger().Warn(
				"Failed to deallocate the unlocked_neg_stmt prepared statement",
				zap.Error(err),
			)
		}
	}()

	// Execute locked updates one by one
	for _, update := range lockedUpdates {
		setVars := fmt.Sprintf("SET @v1=%d, @v2=%d, @v3=%d, @v4=%d",
			startTS, update.TableID, update.Delta.Count, update.Delta.Delta)
		if _, err = statsutil.ExecWithCtx(ctx, sctx, setVars); err != nil {
			return err
		}

		if _, err = statsutil.ExecWithCtx(ctx, sctx,
			"EXECUTE locked_stmt USING @v1, @v2, @v3, @v4"); err != nil {
			return err
		}
	}

	// Execute unlocked updates one by one
	for _, update := range unlockedUpdates {
		var setVars string
		var execStmt string

		if update.Delta.Delta < 0 {
			setVars = fmt.Sprintf("SET @v1=%d, @v2=%d, @v3=%d, @v4=%d",
				startTS, update.TableID, update.Delta.Count, -update.Delta.Delta)
			execStmt = "EXECUTE unlocked_neg_stmt USING @v1, @v2, @v3, @v4"
		} else {
			setVars = fmt.Sprintf("SET @v1=%d, @v2=%d, @v3=%d, @v4=%d",
				startTS, update.TableID, update.Delta.Count, update.Delta.Delta)
			execStmt = "EXECUTE unlocked_pos_stmt USING @v1, @v2, @v3, @v4"
		}

		if _, err = statsutil.ExecWithCtx(ctx, sctx, setVars); err != nil {
			return err
		}

		if _, err = statsutil.ExecWithCtx(ctx, sctx, execStmt); err != nil {
			return err
		}
	}

	// Invalidate cache for all unlocked tables
	for _, id := range cacheInvalidateIDs {
		cache.TableRowStatsCache.Invalidate(id)
	}

	return nil
}

// InsertExtendedStats inserts a record into mysql.stats_extended and update version in mysql.stats_meta.
func InsertExtendedStats(sctx sessionctx.Context,
	statsCache types.StatsCache,
	statsName string, colIDs []int64, tp int, tableID int64, ifNotExists bool) (statsVer uint64, err error) {
	slices.Sort(colIDs)
	bytes, err := json.Marshal(colIDs)
	if err != nil {
		return 0, errors.Trace(err)
	}
	strColIDs := string(bytes)

	// No need to use `exec.ExecuteInternal` since we have acquired the lock.
	rows, _, err := statsutil.ExecRows(sctx, "SELECT name, type, column_ids FROM mysql.stats_extended WHERE table_id = %? and status in (%?, %?)", tableID, statistics.ExtendedStatsInited, statistics.ExtendedStatsAnalyzed)
	if err != nil {
		return 0, errors.Trace(err)
	}
	for _, row := range rows {
		currStatsName := row.GetString(0)
		currTp := row.GetInt64(1)
		currStrColIDs := row.GetString(2)
		if currStatsName == statsName {
			if ifNotExists {
				return 0, nil
			}
			return 0, errors.Errorf("extended statistics '%s' for the specified table already exists", statsName)
		}
		if tp == int(currTp) && currStrColIDs == strColIDs {
			return 0, errors.Errorf("extended statistics '%s' with same type on same columns already exists", statsName)
		}
	}
	version, err := statsutil.GetStartTS(sctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	// Bump version in `mysql.stats_meta` to trigger stats cache refresh.
	if _, err = statsutil.Exec(sctx, "UPDATE mysql.stats_meta SET version = %? WHERE table_id = %?", version, tableID); err != nil {
		return 0, err
	}
	statsVer = version
	// Remove the existing 'deleted' records.
	if _, err = statsutil.Exec(sctx, "DELETE FROM mysql.stats_extended WHERE name = %? and table_id = %?", statsName, tableID); err != nil {
		return 0, err
	}
	// Remove the cache item, which is necessary for cases like a cluster with 3 tidb instances, e.g, a, b and c.
	// If tidb-a executes `alter table drop stats_extended` to mark the record as 'deleted', and before this operation
	// is synchronized to other tidb instances, tidb-b executes `alter table add stats_extended`, which would delete
	// the record from the table, tidb-b should delete the cached item synchronously. While for tidb-c, it has to wait for
	// next `Update()` to remove the cached item then.
	removeExtendedStatsItem(statsCache, tableID, statsName)
	const sql = "INSERT INTO mysql.stats_extended(name, type, table_id, column_ids, version, status) VALUES (%?, %?, %?, %?, %?, %?)"
	if _, err = statsutil.Exec(sctx, sql, statsName, tp, tableID, strColIDs, version, statistics.ExtendedStatsInited); err != nil {
		return 0, err
	}
	return
}

// SaveExtendedStatsToStorage writes extended stats of a table into mysql.stats_extended.
func SaveExtendedStatsToStorage(sctx sessionctx.Context,
	tableID int64, extStats *statistics.ExtendedStatsColl, isLoad bool) (statsVer uint64, err error) {
	if extStats == nil || len(extStats.Stats) == 0 {
		return 0, nil
	}

	version, err := statsutil.GetStartTS(sctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	for name, item := range extStats.Stats {
		bytes, err := json.Marshal(item.ColIDs)
		if err != nil {
			return 0, errors.Trace(err)
		}
		strColIDs := string(bytes)
		var statsStr string
		switch item.Tp {
		case ast.StatsTypeCardinality, ast.StatsTypeCorrelation:
			statsStr = fmt.Sprintf("%f", item.ScalarVals)
		case ast.StatsTypeDependency:
			statsStr = item.StringVals
		}
		// If isLoad is true, it's INSERT; otherwise, it's UPDATE.
		if _, err := statsutil.Exec(sctx, "replace into mysql.stats_extended values (%?, %?, %?, %?, %?, %?, %?)", name, item.Tp, tableID, strColIDs, statsStr, version, statistics.ExtendedStatsAnalyzed); err != nil {
			return 0, err
		}
	}
	if !isLoad {
		if _, err := statsutil.Exec(sctx, "UPDATE mysql.stats_meta SET version = %? WHERE table_id = %?", version, tableID); err != nil {
			return 0, err
		}
		statsVer = version
	}
	return statsVer, nil
}

func removeExtendedStatsItem(statsCache types.StatsCache,
	tableID int64, statsName string) {
	tbl, ok := statsCache.Get(tableID)
	if !ok || tbl.ExtendedStats == nil || len(tbl.ExtendedStats.Stats) == 0 {
		return
	}
	newTbl := tbl.Copy()
	delete(newTbl.ExtendedStats.Stats, statsName)
	statsCache.UpdateStatsCache(types.CacheUpdate{
		Updated: []*statistics.Table{newTbl},
	})
}

var changeGlobalStatsTables = []string{
	"stats_meta", "stats_top_n", "stats_fm_sketch", "stats_buckets",
	"stats_histograms", "column_stats_usage",
}

// ChangeGlobalStatsID changes the table ID in global-stats to the new table ID.
func ChangeGlobalStatsID(
	ctx context.Context,
	sctx sessionctx.Context,
	from, to int64,
) error {
	for _, table := range changeGlobalStatsTables {
		_, err := statsutil.ExecWithCtx(
			ctx, sctx,
			"update mysql."+table+" set table_id = %? where table_id = %?",
			to, from,
		)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// UpdateStatsMetaVersionForGC updates the version of stats_meta to be deleted
// soon.
func UpdateStatsMetaVersionForGC(
	ctx context.Context,
	sctx sessionctx.Context,
	physicalID int64,
) (uint64, error) {
	startTS, err := statsutil.GetStartTS(sctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if _, err = statsutil.ExecWithCtx(
		ctx,
		sctx,
		"update mysql.stats_meta set version=%? where table_id =%?",
		startTS, physicalID,
	); err != nil {
		return 0, errors.Trace(err)
	}
	return startTS, nil
}
