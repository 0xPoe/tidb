# Statistics Variable Default Audit - master

Audit date: 2026-06-22

Scope: TiDB `master` in `/Users/poe/code/tidb` compared with docs `master` in `/Users/poe/code/docs`. This file only tracks stats-related system-variable default values. Non-default behavior, deprecation, SQL statement, feature, and config findings are in [stats-doc-audit-master-other-findings.md](/Users/poe/code/tidb/stats-doc-audit-master-other-findings.md:1).

## Mismatched Or Missing Defaults

| Variable | Code default | Docs default | Finding | Suggested docs update |
|---|---|---|---|---|
| `tidb_auto_analyze_concurrency` | `3`; `master:pkg/sessionctx/vardef/tidb_vars.go:1676`, used in `master:pkg/sessionctx/variable/sysvar.go:1682`; fresh-bootstrap test expects it at `master:pkg/session/test/bootstraptest/bootstrap_upgrade_test.go:1399`. | `system-variables.md:1265` says default `1`. | Documented default is stale. | Change the default to `3`; mention that existing clusters can retain an explicitly persisted value. |
| `tidb_auto_build_stats_concurrency` | `2`; `master:pkg/sessionctx/vardef/tidb_vars.go:1442` sets `DefBuildStatsConcurrency = 2`, `:1719` maps auto-build stats to it, and `master:pkg/sessionctx/variable/sysvar.go:1758` uses it. | `system-variables.md:1328` says default `1`. | Documented default is stale. | Change the default to `2`. |
| `tidb_sysproc_scan_concurrency` | `4`; `master:pkg/sessionctx/vardef/tidb_vars.go:1441` sets `DefAnalyzeDistSQLScanConcurrency = 4`, `:1720` maps sysproc scan to it, and `master:pkg/sessionctx/variable/sysvar.go:1759` uses it. | `system-variables.md:6491` says default `1`. | Documented default is stale. | Change the default to `4`. |

## Checked Defaults Matching Code

| Variable | Code/docs default | Notes |
|---|---|---|
| `tidb_auto_analyze_ratio` | `0.5` | Matches code and docs. |
| `tidb_auto_analyze_start_time` | `00:00 +0000` | Matches code and docs. |
| `tidb_auto_analyze_end_time` | `23:59 +0000` | Matches code and docs. |
| `tidb_build_stats_concurrency` | `2` | Matches code and docs. |
| `tidb_build_sampling_stats_concurrency` | `2` | Matches code and docs. |
| `tidb_analyze_distsql_scan_concurrency` | `4` | Matches code and docs. |
| `tidb_analyze_version` | `2` | Default matches; non-default support/range issue is in the other findings file. |
| `tidb_analyze_skip_column_types` | `json,blob,mediumblob,longblob,mediumtext,longtext` | Matches code and docs. |
| `tidb_analyze_column_options` | `ALL` | Matches code and docs. |
| `tidb_auto_analyze_partition_batch_size` | `8192` | Default matches; deprecation wording issue is in the other findings file. |
| `tidb_enable_auto_analyze` | `ON` | Matches code and docs. |
| `tidb_enable_auto_analyze_priority_queue` | `ON` | Default matches; behavior/deprecation issue is in the other findings file. |
| `tidb_enable_fast_analyze` | `OFF` | Default matches; no-effect behavior issue is in the other findings file. |
| `tidb_enable_extended_stats` | `OFF` | Default matches; removed-feature issue is in the other findings file. |
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
| `tidb_stats_update_during_ddl` | `OFF` | Default matches; scope/reference issue is in the other findings file. |
