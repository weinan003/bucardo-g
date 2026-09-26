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
- `syncrun` 记录；
- 通过 session advisory lock 防止同一 Sync 并发执行；
- Windows/Linux 代码路径不依赖 fork、setsid 或 Unix-only daemon。

尚未支持：

- LISTEN/NOTIFY 自动调度；
- 常驻 daemon、MCP/CTL/KID 进程模型；
- 多源冲突和 ConflictRule；
- TRUNCATE；
- sequence；
- VAC；
- custom code；
- 完整 PostgreSQL 集成测试 harness（已建立 `regress/` 黑盒用例）；
- 完整旧版 Bucardo schema 的历史过程函数和兼容对象。

## 验证状态
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
2. 扩展 `regress/` fixture，覆盖主键变化、多目标确认和清理。
3. 增加 LISTEN/NOTIFY 监听器和手工 kick 调度，并将行为加入 regress 用例。
4. 增加 TRUNCATE、sequence 和多源冲突策略及对应回归场景。
5. 最后实现常驻服务和 Windows/Linux 服务包装。

## 工作区状态说明

本归档生成时，`bucardo-g` 目录在 Git 中仍显示为未跟踪目录，未创建 Git commit，也未修改现有 Perl Bucardo 文件。

如果要在其他机器通过 Git 继续开发，需要先将 `bucardo-g` 纳入版本控制并提交，或使用外部文件同步方式传递整个目录。
