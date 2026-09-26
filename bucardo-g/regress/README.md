# Bucardo-G Regression Tests

This directory is the black-box regression suite for behavior that must remain
compatible with the Bucardo data model. Tests use public Go package APIs and
real PostgreSQL instances, while package-local tests remain focused on small
implementation details.

## Running

Unit and package tests:

```sh
go test ./...
```

PostgreSQL regression tests need two writable databases:

```sh
BUCARDO_TEST_SOURCE_DSN='postgres://wwn@127.0.0.1:15432/postgres?sslmode=disable' \
BUCARDO_TEST_TARGET_DSN='postgres://wwn@127.0.0.1:25432/postgres?sslmode=disable' \
go test ./regress -count=1 -v
```

`BUCARDO_TEST_CONTROL_DSN` is optional and defaults to the source DSN for the
lock regression. Tests create uniquely named tables and remove them with
`DROP TABLE IF EXISTS` during cleanup. They skip when the required DSNs are not
set, so `go test ./...` remains useful without PostgreSQL.

## Case naming

Regression cases use stable numeric names, similar to PostgreSQL's `sql/` and
`expected/` layout:

- `001_lock_serialization`: one Sync cannot run concurrently twice.
- `002_single_source_target`: INSERT, UPDATE, DELETE, target confirmation,
  duplicate delta suppression, and empty rounds.
- `003_target_failure_retry`: target failure leaves no track confirmation and a
  later retry converges.
- `004_metadata_bootstrap`: control/source metadata, delta/track tables and
  delta trigger are created automatically before the first copy.
- `005_trigger_events`: UPDATE, DELETE, primary-key-change support and rollback
  behavior for automatically managed delta triggers.
- `006_cli_syncrun_status`: CLI apply/run plus persisted empty, good, and bad
  syncrun states.

Each case should assert durable database state and counters, not log wording or
unstable timestamps. New behavior belongs here once it crosses a package
boundary; package tests should cover only local helpers and validation rules.

## Planned cases

- `007_multi_target_confirmation`: cleanup waits until every target is tracked.
- `008_cross_platform_runtime`: cancellation and graceful shutdown behavior.
