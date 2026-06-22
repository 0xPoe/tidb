# Statistics Variable Default Audit - release-8.5

Audit date: 2026-06-22

Scope: TiDB `release-8.5` in `/Users/poe/code/tidb` compared with docs `release-8.5` in `/Users/poe/code/docs`. This file only tracks stats-related system-variable default values. Non-default behavior, deprecation, SQL statement, feature, and config findings are in [stats-doc-audit-release-8.5-other-findings.md](/Users/poe/code/tidb/stats-doc-audit-release-8.5-other-findings.md:1).

## Mismatched Defaults

| Variable | Code default | Docs default | Finding | Suggested docs update |
|---|---|---|---|---|
| `tidb_auto_analyze_concurrency` | `3`; `release-8.5:pkg/sessionctx/variable/tidb_vars.go:1555`, used in `release-8.5:pkg/sessionctx/variable/sysvar.go:1481`; fresh-bootstrap test expects it at `release-8.5:pkg/session/bootstraptest/bootstrap_upgrade_test.go:1163`. | `system-variables.md:1217` says default `1`. | Documented default is stale. | Change the default to `3`; mention that existing clusters can retain an explicitly persisted value. |
| `tidb_auto_build_stats_concurrency` | `2`; `release-8.5:pkg/sessionctx/variable/tidb_vars.go:1335` sets `DefBuildStatsConcurrency = 2`, `:1594` maps auto-build stats to it, and `release-8.5:pkg/sessionctx/variable/sysvar.go:1557` uses it. | `system-variables.md:1280` says default `1`. | Documented default is stale. | Change the default to `2`. |
| `tidb_sysproc_scan_concurrency` | `4`; `release-8.5:pkg/sessionctx/variable/tidb_vars.go:1334` sets `DefAnalyzeDistSQLScanConcurrency = 4`, `:1595` maps sysproc scan to it, and `release-8.5:pkg/sessionctx/variable/sysvar.go:1558` uses it. | `system-variables.md:6347` says default `1`. | Documented default is stale. | Change the default to `4`. |

## Checked Defaults Matching Code

| Variable | Code/docs default | Notes |
|---|---|---|
| `tidb_auto_analyze_ratio` | `0.5` | Matches code and docs. |
| `tidb_auto_analyze_start_time` | `00:00 +0000` | Matches code and docs. |
| `tidb_auto_analyze_end_time` | `23:59 +0000` | Matches code and docs. |
| `tidb_build_stats_concurrency` | `2` | Matches code and docs. |
| `tidb_build_sampling_stats_concurrency` | `2` | Matches code and docs. |
| `tidb_analyze_distsql_scan_concurrency` | `4` | Matches code and docs. |
| `tidb_analyze_version` | `2` | Matches code and docs. |
| `tidb_analyze_skip_column_types` | `json,blob,mediumblob,longblob,mediumtext,longtext` | Matches code and docs. |
| `tidb_analyze_column_options` | `ALL` | Matches code and docs. |
| `tidb_auto_analyze_partition_batch_size` | `8192` | Default matches; deprecation wording issue is in the other findings file. |
| `tidb_enable_auto_analyze` | `ON` | Matches code and docs. |
| `tidb_enable_auto_analyze_priority_queue` | `ON` | Default matches; deprecation issue is in the other findings file. |
| `tidb_enable_fast_analyze` | `OFF` | Default matches; no-effect behavior issue is in the other findings file. |
| `tidb_enable_extended_stats` | `OFF` | Matches code and docs. |
| `tidb_enable_pseudo_for_outdated_stats` | `OFF` | Matches code and docs. |
| `tidb_stats_load_sync_wait` | `100` | Matches code and docs. |
| `tidb_stats_load_pseudo_timeout` | `ON` | Matches code and docs. |
| `tidb_enable_analyze_snapshot` | `OFF` | Matches code and docs. |
| `tidb_enable_stats_owner` | `ON` | Matches config-backed sysvar default and docs. |
| `tidb_enable_column_tracking` | `ON` | Default matches; no-op behavior issue is in the other findings file. |
| `tidb_persist_analyze_options` | `ON` | Matches code and docs. |
| `tidb_enable_historical_stats` | `OFF` | Matches code and docs. |
| `tidb_historical_stats_duration` | `168h` | Matches code and docs. |
| `tidb_enable_historical_stats_for_capture` | `OFF` | Matches code and docs. |
| `tidb_stats_cache_mem_quota` | `0` | Matches code and docs. |
| `tidb_mem_quota_analyze` | `-1` | Matches code and docs. |
| `tidb_max_auto_analyze_time` | `43200` | Matches code and docs. |
| `tidb_analyze_partition_concurrency` | `2` | Matches code and docs. |
| `tidb_merge_partition_stats_concurrency` | `1` | Matches code and docs. |
| `tidb_enable_async_merge_global_stats` | `ON` | Matches code and docs. |
| `tidb_skip_missing_partition_stats` | `ON` | Matches code and docs. |
| `tidb_stats_update_during_ddl` | `OFF` | Default matches; scope/prerequisite issue is in the other findings file. |
