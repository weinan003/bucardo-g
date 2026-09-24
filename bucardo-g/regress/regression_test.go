package regress

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bucardo-g/internal/control"
	"github.com/bucardo-g/internal/domain/table"
	"github.com/bucardo-g/internal/replication"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRegression001LockSerialization(t *testing.T) {
	dsn := os.Getenv("BUCARDO_TEST_CONTROL_DSN")
	if dsn == "" {
		dsn = os.Getenv("BUCARDO_TEST_SOURCE_DSN")
	}
	if dsn == "" {
		t.Skip("BUCARDO_TEST_CONTROL_DSN or BUCARDO_TEST_SOURCE_DSN is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, err := control.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := control.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	syncName := fmt.Sprintf("regress_lock_%d", time.Now().UnixNano())
	firstLock, err := first.TryLockSync(ctx, syncName)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.TryLockSync(ctx, syncName); err == nil {
		t.Fatal("second controller acquired an active Sync lock")
	}
	if err := firstLock.Close(); err != nil {
		t.Fatal(err)
	}
	secondLock, err := second.TryLockSync(ctx, syncName)
	if err != nil {
		t.Fatalf("lock was not released after connection close: %v", err)
	}
	if err := secondLock.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRegression002SingleSourceTarget(t *testing.T) {
	sourceDSN := os.Getenv("BUCARDO_TEST_SOURCE_DSN")
	targetDSN := os.Getenv("BUCARDO_TEST_TARGET_DSN")
	if sourceDSN == "" || targetDSN == "" {
		t.Skip("BUCARDO_TEST_SOURCE_DSN and BUCARDO_TEST_TARGET_DSN are not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	source := openPool(t, ctx, sourceDSN)
	defer source.Close()
	target := openPool(t, ctx, targetDSN)
	defer target.Close()

	tableName := fmt.Sprintf("bucardo_g_regress_%d", time.Now().UnixNano())
	deltaName := "delta_public_" + tableName
	trackTableName := "track_public_" + tableName
	relation := table.Table{Schema: "public", Name: tableName, Relation: table.RelationTable, PrimaryKey: []string{"id"}}
	defer dropTables(source, target, tableName, deltaName, trackTableName)

	for _, statement := range []string{
		"CREATE SCHEMA IF NOT EXISTS " + quoteIdent("bucardo"),
		"CREATE TABLE " + qualified("public", tableName) + " (id integer PRIMARY KEY, value text NOT NULL)",
		"CREATE TABLE " + qualified("bucardo", deltaName) + " (id integer NOT NULL, txntime bigint NOT NULL)",
		"CREATE TABLE " + qualified("bucardo", trackTableName) + " (txntime bigint NOT NULL, target text NOT NULL, PRIMARY KEY (txntime, target))",
	} {
		if _, err := source.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	for _, statement := range []string{
		"CREATE TABLE " + qualified("public", tableName) + " (id integer PRIMARY KEY, value text NOT NULL)",
		"INSERT INTO " + qualified("public", tableName) + " (id, value) VALUES (1, 'old'), (3, 'stale')",
	} {
		if _, err := target.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	for _, statement := range []string{
		"INSERT INTO " + qualified("public", tableName) + " (id, value) VALUES (1, 'updated'), (2, 'inserted')",
		"INSERT INTO " + qualified("bucardo", deltaName) + " (id, txntime) VALUES (1, 101), (2, 102), (3, 103)",
	} {
		if _, err := source.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	topology := control.Sync{
		Name:       "regress_single_source_target",
		Source:     control.Database{Name: "source", DSN: sourceDSN},
		Target:     control.Database{Name: "target", DSN: targetDSN},
		TargetName: "db-target",
		Tables:     []table.Table{relation},
	}
	stats, err := replication.RunOnce(ctx, topology)
	if err != nil {
		t.Fatal(err)
	}
	if stats != (replication.Stats{Inserts: 1, Updates: 1, Deletes: 1}) {
		t.Fatalf("unexpected first run stats: %+v", stats)
	}
	assertRows(t, ctx, target, qualified("public", tableName), []string{"1:updated", "2:inserted"})
	assertTrackCount(t, ctx, source, qualified("bucardo", trackTableName), "db-target", 3)

	stats, err = replication.RunOnce(ctx, topology)
	if err != nil {
		t.Fatal(err)
	}
	if stats != (replication.Stats{}) {
		t.Fatalf("expected an empty second run, got %+v", stats)
	}
}

func TestRegression003TargetFailureRetry(t *testing.T) {
	sourceDSN := os.Getenv("BUCARDO_TEST_SOURCE_DSN")
	targetDSN := os.Getenv("BUCARDO_TEST_TARGET_DSN")
	if sourceDSN == "" || targetDSN == "" {
		t.Skip("BUCARDO_TEST_SOURCE_DSN and BUCARDO_TEST_TARGET_DSN are not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	source := openPool(t, ctx, sourceDSN)
	defer source.Close()
	target := openPool(t, ctx, targetDSN)
	defer target.Close()

	tableName := fmt.Sprintf("bucardo_g_retry_%d", time.Now().UnixNano())
	deltaName := "delta_public_" + tableName
	trackTableName := "track_public_" + tableName
	relation := table.Table{Schema: "public", Name: tableName, Relation: table.RelationTable, PrimaryKey: []string{"id"}}
	defer dropTables(source, target, tableName, deltaName, trackTableName)

	for _, statement := range []string{
		"CREATE SCHEMA IF NOT EXISTS " + quoteIdent("bucardo"),
		"CREATE TABLE " + qualified("public", tableName) + " (id integer PRIMARY KEY, value text NOT NULL)",
		"CREATE TABLE " + qualified("bucardo", deltaName) + " (id integer NOT NULL, txntime bigint NOT NULL)",
		"CREATE TABLE " + qualified("bucardo", trackTableName) + " (txntime bigint NOT NULL, target text NOT NULL, PRIMARY KEY (txntime, target))",
		"INSERT INTO " + qualified("public", tableName) + " (id, value) VALUES (1, 'retry')",
		"INSERT INTO " + qualified("bucardo", deltaName) + " (id, txntime) VALUES (1, 201)",
	} {
		if _, err := source.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	topology := control.Sync{
		Name:       "regress_target_failure_retry",
		Source:     control.Database{Name: "source", DSN: sourceDSN},
		Target:     control.Database{Name: "target", DSN: targetDSN},
		TargetName: "db-target",
		Tables:     []table.Table{relation},
	}
	if _, err := replication.RunOnce(ctx, topology); err == nil {
		t.Fatal("expected replication to fail while target table is absent")
	}
	assertTrackCount(t, ctx, source, qualified("bucardo", trackTableName), "db-target", 0)

	if _, err := target.Exec(ctx, "CREATE TABLE "+qualified("public", tableName)+" (id integer PRIMARY KEY, value text NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	stats, err := replication.RunOnce(ctx, topology)
	if err != nil {
		t.Fatal(err)
	}
	if stats != (replication.Stats{Inserts: 1}) {
		t.Fatalf("unexpected retry stats: %+v", stats)
	}
	assertRows(t, ctx, target, qualified("public", tableName), []string{"1:retry"})
	assertTrackCount(t, ctx, source, qualified("bucardo", trackTableName), "db-target", 1)
}

func TestRegression004MetadataBootstrap(t *testing.T) {
	sourceDSN := os.Getenv("BUCARDO_TEST_SOURCE_DSN")
	targetDSN := os.Getenv("BUCARDO_TEST_TARGET_DSN")
	if sourceDSN == "" || targetDSN == "" {
		t.Skip("BUCARDO_TEST_SOURCE_DSN and BUCARDO_TEST_TARGET_DSN are not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	source := openPool(t, ctx, sourceDSN)
	defer source.Close()
	target := openPool(t, ctx, targetDSN)
	defer target.Close()

	tableName := fmt.Sprintf("bucardo_g_bootstrap_%d", time.Now().UnixNano())
	deltaName := "delta_public_" + tableName
	trackTableName := "track_public_" + tableName
	relation := table.Table{Schema: "public", Name: tableName, Relation: table.RelationTable, PrimaryKey: []string{"id"}}
	defer dropTables(source, target, tableName, deltaName, trackTableName)
	for _, statement := range []string{
		"CREATE TABLE " + qualified("public", tableName) + " (id integer PRIMARY KEY, value text NOT NULL)",
	} {
		if _, err := source.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := target.Exec(ctx, "CREATE TABLE "+qualified("public", tableName)+" (id integer PRIMARY KEY, value text NOT NULL)"); err != nil {
		t.Fatal(err)
	}

	topology := control.Sync{
		Name:       "regress_metadata_bootstrap",
		Source:     control.Database{Name: "source", DSN: sourceDSN},
		Target:     control.Database{Name: "target", DSN: targetDSN},
		TargetName: "db-target",
		Tables:     []table.Table{relation},
	}
	if _, err := replication.RunOnce(ctx, topology); err != nil {
		t.Fatal(err)
	}
	var objectCount int
	if err := source.QueryRow(ctx, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'bucardo' AND c.relname IN ($1, $2)`, deltaName, trackTableName).Scan(&objectCount); err != nil {
		t.Fatal(err)
	}
	if objectCount != 2 {
		t.Fatalf("expected automatically managed delta and track tables, got %d", objectCount)
	}
	if _, err := source.Exec(ctx, "INSERT INTO "+qualified("public", tableName)+" (id, value) VALUES (1, 'triggered')"); err != nil {
		t.Fatal(err)
	}
	var deltaCount int
	if err := source.QueryRow(ctx, "SELECT count(*) FROM "+qualified("bucardo", deltaName)).Scan(&deltaCount); err != nil {
		t.Fatal(err)
	}
	if deltaCount != 1 {
		t.Fatalf("expected trigger-created delta, got %d", deltaCount)
	}
	stats, err := replication.RunOnce(ctx, topology)
	if err != nil {
		t.Fatal(err)
	}
	if stats != (replication.Stats{Inserts: 1}) {
		t.Fatalf("unexpected bootstrap replication stats: %+v", stats)
	}
	assertRows(t, ctx, target, qualified("public", tableName), []string{"1:triggered"})
}

func openPool(t *testing.T, ctx context.Context, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	return pool
}

func dropTables(source, target *pgxpool.Pool, tableName, deltaName, trackName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, statement := range []string{
		"DROP TABLE IF EXISTS " + qualified("bucardo", trackName),
		"DROP TABLE IF EXISTS " + qualified("bucardo", deltaName),
		"DROP TABLE IF EXISTS " + qualified("public", tableName),
	} {
		if _, err := source.Exec(ctx, statement); err != nil {
			fmt.Fprintf(os.Stderr, "regression cleanup failed: %v\n", err)
		}
	}
	if _, err := target.Exec(ctx, "DROP TABLE IF EXISTS "+qualified("public", tableName)); err != nil {
		fmt.Fprintf(os.Stderr, "regression cleanup failed: %v\n", err)
	}
}

func assertRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, relation string, expected []string) {
	t.Helper()
	rows, err := pool.Query(ctx, "SELECT id || ':' || value FROM "+relation+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var actual []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		actual = append(actual, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(actual) != fmt.Sprint(expected) {
		t.Fatalf("unexpected rows: got %v want %v", actual, expected)
	}
}

func assertTrackCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, relation, target string, expected int) {
	t.Helper()
	var actual int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+relation+" WHERE target = $1", target).Scan(&actual); err != nil {
		t.Fatal(err)
	}
	if actual != expected {
		t.Fatalf("unexpected track count: got %d want %d", actual, expected)
	}
}

func qualified(schema, name string) string {
	return quoteIdent(schema) + "." + quoteIdent(name)
}

func quoteIdent(value string) string {
	result := `"`
	for _, character := range value {
		if character == '"' {
			result += `""`
		} else {
			result += string(character)
		}
	}
	return result + `"`
}
