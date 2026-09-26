# Bucardo-G 开发手册

## 1. 代码结构

```text
cmd/bucardo-g/              单次 Sync CLI
internal/domain/            无数据库领域模型和状态规则
internal/control/            控制库读取、syncrun 和 Sync 锁
internal/replication/       PostgreSQL 单源单目标复制 worker
internal/control/schema.go   控制库 MVP 元数据 migration
internal/replication/metadata.go  source delta/track/trigger 部署
regress/                    面向公开行为的 PostgreSQL 黑盒回归
```

模块路径为 `github.com/bucardo-g`。当前实现使用 `pgx/v5` 和 `pgxpool`，不引入
Perl DBI，也不依赖 Unix 专有进程 API。

日志统一使用 `log/slog`。CLI 根命令通过 `--log-level` 和 `--log-format` 配置全局
handler，默认 JSON/info 输出到 stderr。控制层和复制层使用结构化字段记录 sync、
数据库逻辑名、schema/table、批次计数和错误；禁止把 DSN、密码或业务行写入日志。
`--log-file` 使用 `DailyFileWriter` 按本地日期生成 `basename-YYYY-MM-DD.ext` 文件，
在写入时跨日期切换，并按 `--log-retention-days` 清理过期文件。writer 使用 mutex
保护跨 goroutine 写入，进程退出时由 CLI 关闭当前文件。

## 2. 运行边界

配置入口是 `bucardo-g apply bucardo.yaml`；运行 Sync 使用
`bucardo-g run bucardo.yaml --sync example_sync`；使用
`bucardo-g init bucardo.yaml` 可以生成不可覆盖的初始模板。
`internal/config` 解析 YAML，
`control.ApplyConfig` 在事务中 upsert `db`、`dbmap`、`goat`、`herdmap` 和 `sync`；
apply 前会 ping 每个 source/target。配置文件描述用户意图，控制表属于程序生成的
持久化投影，不应要求用户直接写 SQL。

单目标可以使用 `target`；多目标使用 `targets`，两者不能同时出现：

```yaml
syncs:
   - name: fanout_sync
      source: source
      targets: [target_a, target_b]
      tables:
         - schema: public
            name: items
            primaryKey: [id]
```

程序内部会按 Sync 名称生成 target group，并为每个 target 独立写入 stage/track；只有
所有 target 都确认后才清理 delta。

控制库由 `internal/control.Store` 访问。`control.Open` 首先执行
`internal/control/schema.go` 中的幂等 migration，然后 `LoadSync` 解析：

- active Sync；
- source herd 对应的 active source database；
- target group 中优先级最高的 active target；
- source goat 对应的普通表、主键和关系类型。

复制入口是 `replication.RunOnce`。它接收已经解析好的 `control.Sync`，并在 source
端调用 `internal/replication/metadata.go` 管理 delta/track/trigger。因此领域
模型和数据库访问可以独立测试。CLI 负责连接控制库、获取锁、调用 worker、记录
`syncrun` 并释放资源。

## 3. 当前复制算法

对每个普通表：

0. 确保 `bucardo` schema、`delta_*`、`track_*` 和 delta trigger 存在；业务表本身
   必须已经存在且有主键。
1. 查询 `bucardo.delta_<schema>_<table>`，通过 `track_<schema>_<table>` 排除
   当前 target 已确认的 `txntime`。
2. 对每个 delta 主键查询源表当前行。
3. 当前行不存在时在目标端执行 DELETE。
4. 当前行存在时先判断目标端是否已有该主键，再执行
   `INSERT ... ON CONFLICT DO UPDATE`，据此统计 insert/update。
5. 所有目标 DML 在同一个目标事务中提交。
6. 目标事务成功后才写源端 track。

目标事务失败时会 rollback，track 写入不会发生。track 写入目前发生在目标事务提交
之后但不与目标事务组成跨数据库事务；这是 Bucardo 兼容语义下的可重试设计，后续需
加入更严格的 stage/track 和多目标确认模型。

## 4. Sync advisory lock

`Store.TryLockSync` 为 sync 名称计算稳定的 SHA-256 前缀，并使用 PostgreSQL
`pg_try_advisory_lock(bigint, bigint)` 的两个 `int4` key。锁连接独立于连接池，锁的
生命周期覆盖整个 `RunOnce`，连接关闭时 PostgreSQL 自动释放锁。

不要改成连接池上的普通查询锁：连接归还池后不能保证 session 生命周期。任何新的
Controller 或服务入口都必须复用 `TryLockSync`。

## 5. 测试分层

### 5.1 领域和包内测试

用于名称、状态、参数校验和局部 helper：

```sh
go test ./internal/...
go test -race ./internal/...
go vet ./...
```

### 5.2 黑盒 regress 测试

`regress/` 只调用公开 API，并连接真实 PostgreSQL。用例采用稳定编号：

- `001`：Sync advisory lock 互斥和释放；
- `002`：单源单目标 DML、track 去重、空轮次；
- `003`：目标失败不写 track，修复后重试收敛；已在 15432/25432 验证；
- `004`：自动部署元表、trigger delta 捕获和 rollback；已在 15432/25432 验证；
- `005`：UPDATE、DELETE、主键变化和 rollback 的 trigger delta；已在 15432 验证；
- `006`：CLI、syncrun 的 empty/good/bad 和错误结果；已在 15432/25432 验证；
- `007`：多目标确认和清理；已在 15432/25432 验证；
- `008`：过期 stage 恢复和 VAC delta 清理；已在 15432/25432 验证；
- `009`：LISTEN/NOTIFY 手工 kick；已在 15432 验证；
- `010`：取消、通知和跨平台运行时。

执行：

```sh
BUCARDO_TEST_SOURCE_DSN='postgres://...:15432/postgres?sslmode=disable' \
BUCARDO_TEST_TARGET_DSN='postgres://...:25432/postgres?sslmode=disable' \
go test ./regress -count=1 -v
```

每个 case 必须：

1. 创建唯一 fixture；
2. 只使用可清理的临时对象；
3. 断言业务行、delta、track、计数和错误状态；
4. 不断言 PID、日志顺序、绝对时间或连接池内部细节；
5. 在 DSN 缺失时明确 skip，而不是伪造通过。

### 5.3 Perl 行为 oracle

现有 `t/` TAP 测试仍是兼容性 oracle。Go regress 先验证 PostgreSQL-only P0
行为，再对同一 fixture 比较源/目标表、delta、track、stage、syncrun 和 dbrun。
不应删除旧测试来让 Go 测试变绿。

## 6. 新增功能的方法

新增复制行为时按以下顺序：

1. 先在 `BUCARDO_G_REFACTORING.md` 确认规约和完成条件；
2. 为 public boundary 写 regress case；
3. 为领域约束或 SQL helper 写包内测试；
4. 实现最小接口，不让 CLI 直接拼接业务 SQL；
5. 在 15432/25432 上运行定向 regress；
6. 运行 `go test ./...`、`go test -race ./...`、`go vet ./...`；
7. 更新 `USER_GUIDE.md`、本文件和 `DEVELOPMENT_HANDOFF.md`。

## 7. 待实现顺序

P0：

1. LISTEN/NOTIFY 与手工 kick；
2. 常驻 `serve`、优雅取消和跨平台服务包装。

P1：

1. TRUNCATE 和 sequence；
2. 多源 ConflictRule；
3. VAC 和 stalled/reconnect；
4. 常驻 `serve`、优雅取消和 Windows Service 包装；
5. Perl/Bucardo-G 差分测试 harness。

## 8. 提交前门禁

```sh
gofmt -w cmd internal regress
go test ./...
go test -race ./...
go vet ./...
git diff --check
```

如果使用两个本地实例，还必须运行：

```sh
BUCARDO_TEST_SOURCE_DSN='postgres://wwn@127.0.0.1:15432/postgres?sslmode=disable' \
BUCARDO_TEST_TARGET_DSN='postgres://wwn@127.0.0.1:25432/postgres?sslmode=disable' \
go test ./regress -count=1 -v
```
