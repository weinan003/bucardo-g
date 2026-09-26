# Bucardo-G 用户手册

## 1. 产品范围

Bucardo-G 是 Bucardo 的 Go 重构版本。当前可运行版本只支持：

- PostgreSQL 源端到 PostgreSQL 目标端；
- 单源、单目标；
- 具有主键的普通表；
- `INSERT`、`UPDATE`、`DELETE`；
- 手工执行一次 Sync；
- 自动管理当前 MVP 所需的 `bucardo` schema、控制元表、`delta_*`、`track_*` 和
  delta trigger。

当前版本不支持自动 kick、多目标、多源冲突、TRUNCATE、sequence、VAC、custom
code，也不会自动部署完整旧版 Bucardo 的所有历史过程函数。

## 2. 环境要求

- Go 1.26.1 或兼容版本；
- PostgreSQL 源库、目标库和 Bucardo 控制库；
- 源端和目标端已经存在业务表，且业务表具有主键；
- 运行账户可以连接控制库、源库和目标库，并对控制元表、源端元表、源业务表和目标
  业务表拥有所需权限；
- 运行账户允许在控制库和源库执行 `CREATE SCHEMA`、`CREATE TABLE`、`CREATE FUNCTION`
  和 `CREATE TRIGGER`。

源库和目标库可以是同一 PostgreSQL 实例上的不同数据库，也可以是不同实例。Windows
和 Linux 使用同一套 Go 命令，不依赖 Unix daemon、`fork` 或 `/tmp`。

## 3. 编译和检查

在本目录执行：

```sh
go mod download
go test ./...
go vet ./...
go build -o bucardo-g ./cmd/bucardo-g
```

Windows PowerShell：

```powershell
go build -o bucardo-g.exe .\cmd\bucardo-g
go test ./...
go vet ./...
```

## 4. 通过 YAML 管理配置

用户不需要手工向 `bucardo.db`、`dbmap`、`goat`、`herdmap` 或 `sync` 写 SQL。创建
配置文件 `bucardo.yaml`。也可以先让程序生成模板：

```sh
./bucardo-g init bucardo.yaml
```

`init` 只创建新文件，不会覆盖已有配置；然后按实际环境修改模板中的 DSN、Sync
名称和业务表。配置文件示例：

```yaml
controlDatabase:
  dsn: postgres://wwn@127.0.0.1:15432/postgres?sslmode=disable

databases:
  - name: source
    role: source
    dsn: postgres://wwn@127.0.0.1:15432/postgres?sslmode=disable
  - name: target
    role: target
    dsn: postgres://wwn@127.0.0.1:25432/postgres?sslmode=disable

syncs:
  - name: example_sync
    source: source
    target: target
    deleteMethod: delete
    tables:
      - schema: public
        name: items
        primaryKey: [id]
```

执行：

```sh
./bucardo-g apply bucardo.yaml
```

`apply` 会校验配置、检查 source/target 连通性，并事务化创建或更新控制库配置。
重复执行是幂等的。首次运行 Sync 时，Bucardo-G 再自动创建 source 端的 delta、track
和 trigger。

## 5. 运行一次 Sync

```sh
./bucardo-g run bucardo.yaml --sync example_sync
```

PowerShell：

```powershell
.\bucardo-g.exe `
  run bucardo.yaml --sync example_sync
```

`example_sync` 必须是控制库中状态为 `active` 的 Sync。程序会读取该 Sync 的 active
source、目标组中优先级最高的 active target 和普通表配置，然后执行一轮复制。

成功输出示例：

```text
sync=example_sync status=good inserts=1 updates=2 deletes=1
```

## 6. 一轮复制的行为

1. 连接控制库并自动执行幂等 schema migration。
2. 加载 active Sync 并获取 PostgreSQL session advisory lock。
3. 在 source 为每张普通表自动创建 `bucardo.delta_*`、`track_*` 和 delta trigger。
4. 从源端 `bucardo.delta_<schema>_<table>` 读取当前目标尚未确认的 delta。
5. 按 delta 主键回读源表当前行。
6. 源行存在时对目标执行 upsert，源行不存在时删除目标行。
7. 目标事务提交成功后，在源端写入 `track_<schema>_<table>`。
8. 写入 `bucardo.syncrun` 并释放连接和锁。

如果目标事务失败，事务会回滚，track 不会被写入；修复目标问题后再次执行同一 Sync
即可重试。已经针对该目标写入 track 的 delta 不会在下一轮重复消费。

## 7. 两个本地 PostgreSQL 实例验证

本项目测试约定：

```sh
export BUCARDO_TEST_SOURCE_DSN='postgres://wwn@127.0.0.1:15432/postgres?sslmode=disable'
export BUCARDO_TEST_TARGET_DSN='postgres://wwn@127.0.0.1:25432/postgres?sslmode=disable'
export BUCARDO_TEST_CONTROL_DSN="$BUCARDO_TEST_SOURCE_DSN"
```

运行黑盒回归：

```sh
go test ./regress -count=1 -v
```

未设置 DSN 时，数据库回归测试会跳过；`go test ./...` 仍然可以作为离线单元测试
入口。设置 DSN 后，测试会创建带时间后缀的临时表，并在结束时删除。

首次运行时，控制库会创建 `bucardo.bucardo_g_schema_version` 并记录迁移版本；后续
版本只增加迁移，不修改或删除用户已有数据。source 上的 delta/track/trigger 会按
业务表幂等创建。Bucardo-G 不会创建、删除或修改目标业务表。

## 8. 排障

### Sync 已在运行

错误通常为：

```text
sync "example_sync" is already running
```

确认没有其他 Bucardo-G 进程持有该 Sync。进程异常退出或连接断开后，PostgreSQL 会
自动释放 session advisory lock。

### 找不到 active target 或 source

检查控制库中的 Sync、herd、goat、dbmap 和 db 状态。当前版本只选一个 active
source 和目标组中优先级最高的 active target。

### 目标端写入失败

保留源端 delta，不要手工删除 delta 或 track。修复目标表结构、权限或连接问题后，
再次运行相同 Sync。

### DSN 含密码

不要把密码提交到代码、文档或日志。生产环境应使用 PostgreSQL service 文件、环境
变量或受控 secret store，并限制 DSN 的读取权限。

## 9. 当前限制

常驻服务、LISTEN/NOTIFY、自动 kick、多源冲突、TRUNCATE、sequence、VAC、custom
code、完整旧版 schema 兼容和完整 Perl/Bucardo-G 差分迁移仍在后续开发计划中。生产迁移
前应先使用 `regress/` 和现有 Perl TAP 测试验证同一 fixture 的结果。

## 10. 日志

Bucardo-G 使用 Go 标准库 `log/slog` 输出结构化日志，默认写到 stderr，默认格式为
JSON、级别为 `info`：

```bash
./bucardo-g run bucardo.yaml --sync example_sync \
  --log-level debug \
  --log-format text
```

支持的级别为 `debug`、`info`、`warn`、`error`；支持的格式为 `json` 和 `text`。
日志会记录命令生命周期、schema/apply、Sync 锁、每张表的 delta 数量、DML 统计和
失败原因。日志不会记录 DSN、密码或业务行内容。

写入按日期滚动的文件：

```bash
./bucardo-g run bucardo.yaml --sync example_sync \
  --log-file /var/log/bucardo-g/bucardo-g.log \
  --log-retention-days 14
```

实际文件名会是：

```text
/var/log/bucardo-g/bucardo-g-2026-09-26.log
```

程序第一次写日志和跨日期后的第一次写日志时切换文件。默认文件权限为 owner
读写、group 只读（`0640`），目录权限为 owner 读写执行、group 读执行（`0750`）。
`--log-retention-days 0` 表示不自动清理旧日志。文件无法创建或写入时命令直接失败，
不会静默退回 stderr。

## 11. 手工 kick 和 serve

发送一个 Sync 的 PostgreSQL 通知：

```bash
./bucardo-g kick bucardo.yaml --sync example_sync
```

启动常驻监听器：

```bash
./bucardo-g serve bucardo.yaml
```

`serve` 监听控制库的 `bucardo` channel，接收 `kick_sync_<sync>` payload 后执行对应
Sync。使用 `Ctrl-C` 或 SIGINT 停止监听器；日志会记录通知和执行失败。当前服务按配置
文件中的 Sync 名称接受 kick，完整服务管理器、自动重连和 Windows Service 托管仍在后续
阶段完善。
