# DDL Event Handler 审计报告（三遍审计）

> **审计对象**：TiDB 的 DDL 事件通知（notifier）框架、发布方（DDL 模块）、全部订阅方（统计信息子系统的两个 handler），以及与之相邻的统计写入路径（IMPORT INTO、BR/PITR 恢复、RECOVER、re-partition）。
> **方法**：三遍多智能体审计——①建立基线、全量扫；②专攻原子性/多 schema/回滚/并发/bootstrap/分区数值/幂等性等"新地面"并强化证伪；③验证第二遍明确标"未验证"的盲点（IMPORT INTO、ChangeGlobalStatsID、RECOVER）并打开 notifier 表无界增长、滚动升级、stats-owner 竞态、BR/PITR、跨节点一致性、next-gen 等新面。每条候选结论由独立 agent 重读真实代码确认/驳回；可复现的高影响项用 **TiUP playground（本地编译 master `tidb-server` + 真实 TiKV）实测**。
> **基线**：`master` @ `222da210ca`。三遍合计 ~129 个 agent、~720 万 token。

## 证据等级
🧪 真实测试（附实测数据） · 🔬 代码验证（独立 agent 逐行确认） · 📖 仅代码阅读（含一定不确定性） · ⚪ 非缺陷 / 已证伪

---

## 一、框架速览
事务性 outbox + 轮询订阅：DDL job 收尾时把 `SchemaChangeEvent` INSERT 进 `mysql.tidb_ddl_notifier`，与 job 状态在同一事务提交；`DDLNotifier` 只在 stats owner 节点每 **1s** keyset 分页轮询，分发给两个 handler（`StatsMetaHandlerID` 维护 stats 系统表、`PriorityQueueHandlerID` 维护 auto-analyze 内存队列）；每事件用 `processed_by_flag` 位图记账，handler 的 SQL 与 flag 的 CAS 更新在**同一悲观事务**原子提交。**核心框架经三遍反复攻击被确认健全**（见 §三）；问题集中在 handler 实现、发布/订阅覆盖面、多语句步骤的事务边界，以及相邻统计写路径。

---

## 二、缺陷清单（按严重程度，🧪 表示已真实复现）

### 🔴 HIGH-1：`DROP DATABASE` 永久泄漏分区级统计信息
`subscriber.go:253-265` · 数据泄漏 · 🧪+🔬
`ActionDropSchema` 只遍历 `miniDBInfo.Tables` 对 `table.ID` 调一次 `delayedDeleteStats4PhysicalID`，**不遍历 `table.Partitions`**（事件里其实带了分区 ID）。GC 只按 `stats_meta.version` 窗口回收，分区行 version 从不被 bump → 散落 8 张 stats 表的分区行**永久回收不到**。同事件的 PQ handler 与本文件 `DROP TABLE` 分支都正确处理了分区，唯独这里漏。
**🧪** DROP DATABASE 后全局行 version 被 bump、3 个分区 version 全不变；对照组 DROP TABLE 全部 bump。static/dynamic 都只漏分区。未 analyze 的建表也有占位行，故空表分区表同样泄漏。
**修复** 遍历 `table.Partitions` 逐个 bump。

### 🔴 HIGH-2：`ALTER TABLE … REMOVE PARTITIONING / PARTITION BY` 静默丢失 `LOCK STATS`（用户锁被绕过）
`storage/update.go:154-176`（`changeGlobalStatsTables` / `ChangeGlobalStatsID`） · 正确性 / 语义违反 · 🧪+🔬
re-partition 会改变表的物理 ID，`ChangeGlobalStatsID` 把统计行从旧 ID 迁到新 ID——但它**只迁移 6 张表**（meta/top_n/fm_sketch/buckets/histograms/column_stats_usage），**漏了 `stats_table_locked` 和 `analyze_options`**。后果：(1) 用户 `LOCK STATS` 的表 re-partition 后，锁行还指向旧 ID，新 ID **未锁** → 该表**重新变得可被 auto-analyze / 手动 ANALYZE 覆盖**，违反用户"冻结统计"的显式意图；(2) 自定义 ANALYZE 选项（采样率/列/桶/topn）**回退默认**；(3) 旧 ID 的锁行/选项行成**永久孤儿**（GC 不扫这两张表）。
**🧪** `LOCK STATS lk.pt`（id 118 等）→ `REMOVE PARTITIONING`（id→122）→ `SHOW STATS_LOCKED` **变空**、`ANALYZE` **照常执行**；`PARTITION BY HASH` 方向同样丢锁；孤儿清扫查到 lock/opts 各 2 条死 ID 行。
**修复** `changeGlobalStatsTables` 加入 `stats_table_locked` 与 `analyze_options`（或 re-partition 后对原本锁定的表重新加锁）。

### 🟠 MEDIUM 级

**M-1 解码缓冲复用 + `Analyzed,omitempty` 串味** `store.go:197-223` · 🔬+Go复现
`unmarshalSchemaChanges` 复用槽位不清零，`Analyzed` 在两条同类型 AddIndex/ModifyColumn 间 `true→false` 串味 → 跳过新索引/列统计初始化。修复：解码前 `*inner = jsonSchemaChangeEvent{}`。

**M-2 静态分区循环 `return nil` 应为 `continue`** `queue_ddl_handler.go:162,219` · 🔬
首个无统计分区让后续分区 auto-analyze job 全漏建（至下次 rebuild 自愈）。

**M-3 PQ 从内部池 session 读 prune 模式，恒 "dynamic"** `queue_ddl_handler.go:151,207` · 🔬
notifier 池 session `CommonGlobalLoaded` 被硬置 true 且永不刷新 → static 集群 PQ 走错分支，与 stats 订阅方/PQ 初始化不一致。

**M-4 多事件 DDL 步骤的 `Reset()` 丢不掉已 staged 的 notifier INSERT → 幽灵事件** `job_worker.go:665-693` + `session.go:84-86` · 🔬
`DROP DATABASE`(>100 表)/批量建表一步循环发多条事件；`w.sess.Reset()` 只 `StmtRollback`，已 `StmtCommit` 的前几条 INSERT 留在基础事务缓冲，中途失败后仍 fall-through `Commit` → 为"整步未完成"的变更发幽灵事件，重试还撞主键。打破 `job_worker.go:956-958` 明文不变式。修复：多事件步骤错误窗口改 `Rollback`，或 Insert 幂等 + 订阅方加完成性守卫，或合并成单条 multi-VALUES INSERT。

**M-5 多 schema `ALTER` phase-2 在后续 sub-job 回滚时仍提交前一个 sub-job 的事件** `multi_schema_change.go:94-126` · 🔬（failpoint 可复现）
后面 sub-job 失败 → 返回 `nil`（非 error）绕过 reset 门控 → 已 staged 的 `AddColumn/AddIndex` 事件随 `Commit` 落库，但列/索引被回滚 → 幽灵事件。stats 订阅方靠 GC 自愈，非 stats 订阅方无兜底。与 M-4 同根（notifier 事件事务边界）。

**M-6 `IMPORT INTO` 把统计增量全记到全局表 ID，分区计数器不动** `import_into.go:332`、`dxf/importinto/scheduler.go:833` · 🧪+🔬
本地路径与 DXF 路径都用**逻辑表 ID** 调 `FlushTableStats`/`UpdateStatsMeta`，不按分区拆。而 auto-analyze 在 **static 和 dynamic 两种模式** 都按 per-partition `modify_count` 判断是否触发 → 导入的分区**永远不会被自动 analyze**，直到手动 ANALYZE。普通 DML 不受影响（delta collector 会同时更新分区+父表）。
**🧪** static 模式 IMPORT 100 行：全局 100/100，分区 0/0；对照普通 INSERT：全局+分区都更新。
**修复** 导入完成后按分区 ID 拆分并写 per-partition delta（或触发一次按分区的 analyze 检查）。

**M-7 `RECOVER` / `FLASHBACK TABLE` 会丢统计（GC 窗口竞态）+ 不刷新缓存** `ddl/table.go onRecoverTable`、`storage/gc.go:84-122` · 🧪+🔬
RECOVER 不发事件、不 re-bump `stats_meta.version`。stats GC 的硬删窗口（`10*lease`，约 7.5min）与 TiKV GC safe point（10min）解耦，存在**重叠窗口**：表数据仍可恢复，但其直方图行已被 stats GC 硬删 → 恢复后表有数据无统计，直到手动 re-analyze。即使直方图存活，因 version 未 re-bump，内存缓存的版本窗口扫描可能跳过它 → 恢复后短暂走 pseudo 统计。
**🧪** RECOVER 后表 id 不变、3 行直方图存活、`EXPLAIN` 估算正确，但 `stats_meta.version` **未变**、`SHOW STATS_HISTOGRAMS` 暂空（坐实"不发事件/不 re-bump/缓存未刷新"前提）。
**修复** RECOVER 路径发一个恢复事件或 re-bump version 触发缓存重载；并让 RECOVER 与 stats GC 的窗口对齐。

**M-8 `tidb_enable_stats_owner` 开关竞态 → 重复 campaign + 关闭死锁（使 L13 变为真实可达）** `domain.go:2007-2013` + `owner/manager.go:288-301` + `sysvar.go:647-657` · 🔬
`enableStatsOwner` 用 `IsOwner()` 做门控，但 `IsOwner()` 在赢得选举后才为真，且 SetGlobal 无锁。两个并发 `SET GLOBAL tidb_enable_stats_owner=ON` 都看到 `!IsOwner()` → 各自 `CampaignOwner`（非幂等：`wg.Add(1)` + 覆盖 `campaignCancel` + 起第二个 campaignLoop）→ 泄漏第一个 campaignLoop **和第二个 `DDLNotifier.start` 轮询循环**（一个节点两个 notifier）；随后 `SET ...=OFF` / 优雅关闭在 `wg.Wait()` **永久阻塞**。这把第二遍判为"latent"的 L13（`OnBecomeOwner` 无幂等守卫）变成**真实可达**。
**修复** `CampaignOwner` 幂等化，或用互斥串行化 stats-owner 开关。

**M-9 跨节点 stats delta 扫描可能永久漏掉新物理 ID（schema 传播滞后时）** `statscache.go:122-273` · 🔬/📖
非 owner 节点的统计刷新只有"按 version 窗口 + 15s 偏移"的 delta 扫描、无运行时全量对账。若该节点临时丢失 etcd schema watch（可恢复分区）退化到慢轮询，期间发生 TRUNCATE / re-partition，新物理 ID 可能在偏移窗口外被跳过 → 该节点对新 ID **持续走 pseudo 统计**，直到下次 analyze/DML 提升其 version。计划质量下降，可自愈、非数据错误。

**M-10 滚动升级：旧版本 owner 静默丢弃新版本发布的新事件类型** `subscriber.go:266-271` · 🔬
stats owner（跑 notifier 消费）与 DDL owner 是**两个独立 owner**，升级期可在不同版本节点。新版本 DDL owner 发布带**新 `ActionType`** 的事件，旧版本 owner 消费时 switch 无对应 case → 命中 `default`（生产：静默标记已处理并删行）→ 该 DDL 的统计更新**被无声丢弃**。配合 L16（无穷尽性断言）使每次新增事件类型都成潜在风险。

**M-11 BR 逻辑统计恢复丢失锁状态与 analyze_options** `br/.../systable_restore.go`、`pkg/statistics/util/json_objects.go` · 🔬
BR 统计 JSON 只含 count/直方图等，**不含 `IsLocked` 与 analyze 选项**；物理快恢复回退到逻辑恢复时，源端 `LOCK STATS` 的表**恢复后变未锁**（且无告警）。`analyze_options` 即使物理路径也丢（在 `unRecoverableTable` 白名单）。与 HIGH-2 同主题、不同代码路径。

### 🟡 LOW 级

| # | 问题 | 位置 | 验证 |
|---|---|---|---|
| L1 | `HandlerID.String()` 漏 PQ → 日志显示 `HandlerID(2)` | `subscribe.go:77-86` | 🔬 |
| L2 | 两 handler 吞错返回 nil，瞬时失败事件被静默丢（已知取舍 #59474） | stats `ddl.go:54-73`+PQ | 🔬 |
| L3 | PQ 内存堆改动不受事务保护（影响有界：按 tableID 幂等、owner 切换重建） | `subscribe.go:287-322` | 🔬 |
| L4 | 测试路径成功发送后仍打 "fail to notify DDL event" | `ddl.go:702-715` | 🔬 |
| L5 | `OpenTableStore` PK 注释写了不存在的列 `multi_schema_change_id` | `store.go:229-232` | 🔬 |
| L6 | `String()` 不打印 `MiniDBInfo`，DropSchema 出错日志近空 | `events.go:36-77` | 🔬 |
| L7/L8 | `DROP INDEX`/`DROP COLUMN` 不发事件，孤儿直方图行等懒 GC（非正确性：优化器只加载活列/索引） | `index.go`/`column.go` | 🧪+🔬 |
| L9 | PQ 不处理 `ActionFlashbackCluster`（不借此 rebuild） | `queue_ddl_handler.go:58-81` | 🔬 |
| L10 | 批量/BR 建表对 sequence、view 也发 `CreateTableEvent` → 孤儿统计（单建不发） | `create_table.go:348-353` | 🔬（单建🧪） |
| L11 | 截断/删分区把被删行数**加进** modify_count，注释写"keep"误导 | `subscriber.go:600-613` | 🧪+🔬 |
| L12 | unlock 写回 `modify_count` 无 `>0` 下限（`count` 有）→ 负值可落库，破坏 `GetStatsHealthy`/`NeedAnalyzeTable` | `lockstats/unlock_stats.go:29-30` | 🔬 |
| L13 | `OnBecomeOwner` 无幂等守卫（**经 M-8 已变真实可达**，见上） | `subscribe.go:328-351` | 🔬 |
| L14 | notifier `start()` 的 `intest.Assert` 错误白名单脆弱，良性瞬时错误在内部检查构建下 panic | `subscribe.go:167-175` | 🔬 |
| L15 | 全局临时表建表也产生占位 `stats_meta`+直方图 → 永久孤儿 | `storage/save.go:446-505` | 🧪+🔬 |
| L16 | `default` 分支对未来新事件类型"静默吞掉"，无穷尽性断言（建议加 `ActionType` 穷尽单测） | `subscriber.go:266-271`+PQ | 🔬 |
| L17 | `List()` 钉死一个读快照覆盖整轮 drain → 大积压+慢 handler 下单次 tick 超 `gc_life_time` 会 `ErrTxnAbortedByGC`（可自愈） | `store.go:143-195` | 🔬 |

---

## 三、已验证的强项（"证明没问题"，避免误改）
- **恰好一次 / 幂等性**（🔬，且有 `TestCommitFailed`/`Test2OwnerForAShortTime` 回归）：handler 写 + `processed_by_flag` CAS 同悲观事务原子提交 → 重投递被 bit 短路、两 owner 由行锁+CAS 串行化败者整体回滚；所有 **delta 更新不会双扣**，绝对/INSERT IGNORE 类本身幂等。
- **单事件发布与 job 提交原子**（🔬）：同一事务提交；commit 真失败一起回滚干净重试，假失败走 `IsDone()` 短路不重发。
- **分区统计数值正确**（🧪）：exchange 80→50、truncate/drop 80→30 且 modify_count 0→50。
- **多 schema 子事件正确**（🧪）：`sub_job_id` 唯一、无主键冲突。
- **bootstrap/升级生命周期、exchange modifyCount、RENAME（含跨库）、owner 退位收尾** —— 全部证伪为非问题。

---

## 四、看着像 bug、实则不是（已验证排除）
- **notifier 表因 PQ 卡死而无界增长** —— ⚪ **真实测试证伪**：触发条件 `tidb_enable_auto_analyze_priority_queue=OFF` **不可达**——该变量默认 ON 且 `SET ... =OFF` 被硬拒（"已废弃，TiDB 总是用优先队列"）。*结构性残留*：框架确无 TTL/重试上限，若将来有 handler 永久返回 `ErrNotReadyRetryLater` 仍会无界增长，值得加 backstop（仅作 info 记录）。
- **delta 类更新重投递双扣 / 两 owner 双写 / exchange modifyCount 误清零** —— ⚪ 证伪（见 §三）。
- **空 handler 删事件 / `RECOVER`+`ADD COLUMNAR INDEX` 无事件本身错 / `RENAME` 漏事件 / bootstrap 期发事件 / `%d` 注入 / keyset 哨兵** —— ⚪ 全部证伪。
- 注：RECOVER 在 §二 M-7 是"丢统计"的真实问题，但"RECOVER 不发事件"这件事本身（同 ID 恢复）在窗口外是正确的——M-7 限定在 GC 重叠窗口内。

---

## 五、可复现配方（给开发者）
- **M-4 / M-5（幽灵事件）**：用 `failpoint`（`asyncNotifyEventError` 计数触发在第 2 条 INSERT / 第 2 个 sub-job 的 public 步）+ 多事件 DDL（`CREATE TABLES` / 多 schema `ALTER`），断言 `mysql.tidb_ddl_notifier` 留下"未完成步骤"的行 / 出现指向已回滚列的孤儿 `stats_histograms` 行。可作 `pkg/ddl/notifier` 或 `pkg/ddl` 单测；扩展现有 `TestPublishEventError`。
- **M-1（Analyzed 串味）**：调小 `ProcessEventsBatchSize`，构造两条同槽位 AddIndex（true 后 false），断言第二条按 `analyzed=false` 处理。
- **HIGH-2 / M-6 / M-7（真实 SQL，无需 failpoint）**：见 §六附录，均已实测。

---

## 六、优先级与真实测试附录
| 优先级 | 问题 | 验证 |
|---|---|---|
| **P0** | HIGH-1 DROP DATABASE 分区统计泄漏 | 🧪 |
| **P0** | HIGH-2 re-partition 丢 LOCK STATS（语义违反） | 🧪 |
| **P1** | M-4/M-5 notifier 事件事务边界（幽灵事件） | 🔬 |
| **P1** | M-6 IMPORT INTO 分区统计饿死 | 🧪 |
| **P1** | M-2 静态分区 `return nil`→`continue`；M-1 Analyzed 串味 | 🔬 |
| **P2** | M-7 RECOVER 丢统计；M-8 stats-owner 竞态死锁；M-3 PQ prune 模式 | 🧪/🔬 |
| **P2** | M-11 BR 逻辑恢复丢锁；M-10 滚动升级丢新事件；M-9 跨节点漏新 ID | 🔬 |
| **P3** | L1/L4/L5/L6/L11/L16（日志、文档、穷尽性）；L2/L3/L12（语义/一致性） | 🔬/🧪 |
| **P4** | L7/L8/L9/L10/L13/L14/L15/L17 + ChangeGlobalStatsID 漏 analyze_options（孤儿） | 🧪/🔬 |

**真实测试数据（三遍累计，本地 master 二进制 `222da210ca` + 真实 TiKV，测后均已停 playground + 清数据）**

| 测试 | 结果 |
|---|---|
| DROP DATABASE 分区表（dynamic/static） | 全局 version bump、分区 version 全不变 ❌（HIGH-1） |
| DROP TABLE 分区表（对照） | 全局+分区全 bump ✅ |
| **REMOVE PARTITIONING / PARTITION BY 锁表** | `SHOW STATS_LOCKED` 变空、`ANALYZE` 照跑、孤儿 lock/opts 各 2 行 ❌（HIGH-2，两方向） |
| **IMPORT INTO static 分区表** | 全局 100/100，分区 0/0 ❌；对照普通 INSERT 全局+分区都更新 ✅（M-6） |
| **RECOVER TABLE** | 表 id 不变、直方图存活、估算正确，但 version 未 re-bump、`SHOW STATS_HISTOGRAMS` 暂空（M-7 前提） |
| `tidb_enable_auto_analyze_priority_queue=OFF` | **被硬拒**（已废弃）→ 无界增长不可达（证伪） |
| EXCHANGE/TRUNCATE/DROP 分区 count | 80→50 / 80→30 / 80→30，modify_count 0→50 ✅（强项 + L11） |
| 多 schema `ADD COLUMN,ADD INDEX` | 一 job 两条 `sub_job_id` 0/1 子事件 ✅ |
| 未 analyze 建表 / 全局临时表 | 均有占位 `stats_meta`+直方图（H1 前提 / L15） |
| DROP INDEX / 单建 SEQUENCE | 索引直方图残留无 version bump（L7）/ 单建无 stats 行（L10 界定） |

---

## 七、方法与未验证项
- 三遍共 ~129 个 agent；每条候选结论独立证伪（①51 ✓/2 ✗ ②16 ✓/3 ✗ ③24 ✓/2 ✗）。
- **真实测试**坐实：HIGH-1、HIGH-2、M-6、M-7（前提）、L7/L10/L11/L15、各分区数值、多 schema 子事件，并以真实测试**证伪**了"无界增长"误报。
- **仅代码阅读/未实跑**：M-4/M-5 的 failpoint 复现（已给配方）、M-9 跨节点（需传播滞后场景）、M-10 滚动升级（需混合版本集群）、M-11 BR（需 BR 恢复环境）、next-gen/keyspace（默认未启用，未发现可达问题）。
