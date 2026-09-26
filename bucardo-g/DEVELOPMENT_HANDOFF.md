# Bucardo-G 开发交接归档

归档时间：2026-09-24

## 项目位置

本项目位于仓库的 `bucardo-g` 子目录，模块路径为：

```text
github.com/bucardo-g
```

目标是将 Bucardo 重构为仅支持 PostgreSQL 源端和目标端、同时支持 Windows/Linux 的 Go 项目。

## 已完成内容

- `ARCHITECTURE.md`
  - 中文总体架构、控制平面、数据平面、组件依赖、模块边界和 Mermaid 图。
- `DOMAIN_MODEL.md`
  - Database、Table、Sync、Trigger、Delta、ConflictRule、Job、Worker 领域模型。
  - Job 启动和 terminal 状态迁移。
- `internal/control/store.go`
  - 使用 `pgxpool` 连接 Bucardo 控制库。
  - `Open` 自动执行当前 MVP 控制元表 schema migration。
  - 读取 active sync、source、target、herd、goat。
  - 写入 `bucardo.syncrun`。
- `internal/replication/worker.go`
  - 单源单目标 PostgreSQL 复制。
  - 每轮复制前自动确保 source 的 delta/track 表和 delta trigger 存在。
  - 读取 `bucardo.delta_<schema>_<table>`。
  - 按主键回读源表当前行。
  - 目标端使用 `INSERT ... ON CONFLICT DO UPDATE` 或 DELETE。
  - 目标事务提交后写入源端 `track_<schema>_<table>`。
  - 目标事务失败时保留 delta，修复后可重复执行。
  - 零变更轮次记录为 `empty`，并设置 `syncrun.lastempty`。
- `cmd/bucardo-g/main.go`
  - Cobra CLI、YAML apply、run 和全局 slog 日志参数。
- `internal/logging/logging.go`
  - 统一配置 JSON/text handler、日志级别和 stderr 输出。
- `internal/logging/file.go`
  - 按本地日期滚动日志文件，并按保留天数清理旧文件。

## 当前 MVP 运行方式

在 `bucardo-g` 目录执行：

```powershell
go run .\cmd\bucardo-g `
  run bucardo.yaml --sync "example_sync"
```

当前命令每次执行一个 sync，读取一个 active source 和目标组中优先级最高的 active target。

## 当前明确边界

已支持：

- PostgreSQL 到 PostgreSQL；
- 单源、单目标；
- 普通表；
- 主键驱动的 INSERT、UPDATE、DELETE；
- 自动管理当前 MVP 所需的 Bucardo delta/track 表；
- 自动管理 `stage_*`，支持单目标失败恢复和多目标确认；
- `syncrun` 记录；
- 通过 session advisory lock 防止同一 Sync 并发执行；
- Windows/Linux 代码路径不依赖 fork、setsid 或 Unix-only daemon。

尚未支持：

- 完整常驻 daemon、MCP/CTL/KID 进程模型和平台 Service 包装；
- 多源冲突和 ConflictRule；
- TRUNCATE；
- sequence；
- fullcopy/初始全量复制；
- replication origin、全局变更排序和冲突回溯；
- custom code；
- 完整 triggerkick/autokick、通知断线重连和 worker 崩溃恢复；
- 完整 dbrun、bucardo_rate、连接占用和复制速率观测；
- 完整 PostgreSQL 集成测试 harness（已建立 `regress/` 黑盒用例）；
- 完整旧版 Bucardo schema 的历史过程函数和兼容对象。

## 验证状态
当前工作状态（2026-09-26）：

- 已完成：配置 migration、YAML init/apply、Cobra run/kick/serve、slog 日志和按日期
  文件滚动；
- 已完成：单 source、多 target、stage/track、VAC、过期 stage 恢复和 LISTEN/NOTIFY；
- 已完成：多 source worker、复制写入 bypass 和 A/B 无冲突双向同步回归；
- 已推进：domain/database 接入配置/运行 topology，domain/job 接入 CLI run 和
  syncrun 持久化；domain/table 已作为复制主链路关系对象；
- 已推进：`Store.LoadTopology` 已接入 CLI，worker 核心现在接收
  `domain/topology.Topology`；旧 control DTO/兼容 wrapper 仅待 regress fixture 迁移后删除。
- 尚未完成：ConflictRule、并发写入冲突、全局冲突排序、fullcopy、sequence、
  TRUNCATE、完整服务托管、断线/崩溃恢复、完整观测和 Perl/Bucardo-G 差分测试。

许可说明：`LICENSE` 使用 BSD 2-Clause License，`NOTICE` 明确本项目是 Bucardo 的
Go 语言重构，不代表原 Bucardo 官方发行版，并记录原 Bucardo 和第三方依赖的版权边界。

2026-09-26 后续验证：advisory lock 的互斥/释放集成测试分别通过本机 15432 和
25432 PostgreSQL 实例；`go test ./...`、`go vet ./...` 以及 Windows/Linux amd64
交叉构建均通过。

2026-09-26 复制 worker 增量：真实 PostgreSQL 集成测试已覆盖单表 INSERT、UPDATE、
DELETE、目标端 track 写入和重复轮次跳过；15432 -> 25432 及反向 25432 -> 15432
均通过。worker 现在按目标端已确认的 `track_*` 排除 delta，并准确统计三类 DML。

2026-09-26 回归用例 `003_target_failure_retry` 已通过 15432 -> 25432：目标表缺失
时复制失败且不写 track，目标恢复后重试成功。

2026-09-26 回归用例 `004_metadata_bootstrap` 已通过 15432 -> 25432：首次运行自动
创建控制 schema、delta/track 表和 delta trigger，业务 INSERT 被捕获并复制成功。

2026-09-26 回归用例 `005_trigger_events` 已通过 15432：自动 trigger 对 UPDATE、
DELETE 正确写入 delta，事务 rollback 不产生 delta。

2026-09-26 回归用例 `006_cli_syncrun_status` 已通过 15432 -> 25432：CLI apply/run
完整验证 empty、good、bad 输出、退出码和 `syncrun` 标志位。

2026-09-26 回归用例 `007_multi_target_confirmation` 和 `008_stage_recovery_vacuum`
已通过 15432 -> 25432：部分 target 成功时不清理 delta，全部确认后清理；过期 stage
可恢复，已确认 delta 可由 VAC 清理。

2026-09-26 回归用例 `009_kick_notification` 已通过 15432：控制库 LISTEN/NOTIFY
能够发布并接收 `kick_sync_<sync>` 手工 kick payload。

2026-09-26 双向同步推进：YAML/Apply 已支持 `sources: [...]` 多 source 拓扑并为每个
source 建立 goat/herdmap 映射；回归用例 `010_bidirectional_no_loop` 已通过 15432/25432，
worker 可将 A 的变更复制到 B，并通过事务级 bypass 避免 B 的 trigger 产生回流 delta。
当前双向链路已实现 `bucardo_abort`，冲突时保留 delta 并避免部分写入；`bucardo_latest`、
`bucardo_latest_all_tables`、来源优先级和完整并发冲突处理尚未实现。

2026-09-27 回归用例 `011_conflict_abort` 已通过 15432/25432：同一主键的多 source
delta 会在 target 写入前失败，双方 delta 保留且 track 不写入。

`regress/` 已建立 PostgreSQL regress 风格的黑盒回归入口：

```sh
BUCARDO_TEST_SOURCE_DSN='postgres://...:15432/postgres?sslmode=disable' \
BUCARDO_TEST_TARGET_DSN='postgres://...:25432/postgres?sslmode=disable' \
go test ./regress -count=1 -v
```

最近一次已通过：

```text
go fmt ./...
go vet ./...
go test ./...
```

Linux amd64 交叉构建也已验证。

已使用本机 15432 和 25432 两个 PostgreSQL 实例完成 advisory lock、单表 DML、track
去重和目标失败重试验证；不依赖本地 `pg_isready.exe`。

## 继续开发前的准备

在新机器上：

1. 获取仓库完整内容，确保 `bucardo-g` 目录及其未提交文件一并保留。
2. 确认 Go 版本满足 `go.mod` 中的 `go 1.26.1`。
3. 在 `bucardo-g` 执行：

   ```powershell
   go mod download
   go test ./...
   go vet ./...
   ```

4. 准备两个可连接的 PostgreSQL 数据库：
   - Bucardo 控制库；
   - source 和 target 数据库。
5. 准备控制库、source 和 target 的业务连接；Bucardo-G 首次运行会创建当前 MVP 所需
  的控制元表、source delta/track 表和 delta trigger。
6. 创建一个 active、单源单目标、带主键普通表的 sync。
7. 先手工向 source 表写入 INSERT/UPDATE/DELETE，再运行一次 CLI 验证 MVP。

## 推荐下一步

按小步增量继续：

1. 为控制库和复制 worker 增加可注入的接口，便于测试。
2. 扩展 `serve` 回归，覆盖通知触发复制、取消和重连。
3. 增加 TRUNCATE、sequence 和多源冲突策略及对应回归场景。

## 工作区状态说明

当前 Go 重构代码已纳入 Git，并保留在连续的 WIP/功能提交中；现有 Perl Bucardo 文件
未被修改。工作区状态应以 `git status --short` 为准。

如果要在其他机器通过 Git 继续开发，需要先将 `bucardo-g` 纳入版本控制并提交，或使用外部文件同步方式传递整个目录。
