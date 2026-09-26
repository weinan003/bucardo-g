package regress

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/bucardo-g/internal/control"
	domainDatabase "github.com/bucardo-g/internal/domain/database"
	"github.com/bucardo-g/internal/domain/table"
	domainTopology "github.com/bucardo-g/internal/domain/topology"
	"github.com/bucardo-g/internal/replication"
	"github.com/jackc/pgx/v5"
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

	topology := domainTopology.Topology{
		Name:    "regress_single_source_target",
		Sources: []domainDatabase.Database{{Name: "source", DSN: sourceDSN}},
		Targets: []domainDatabase.Database{{Name: "target", DSN: targetDSN}},
		Tables:  []table.Table{relation},
	}
	stats, err := replication.RunTopology(ctx, topology)
	if err != nil {
		t.Fatal(err)
	}
	if stats != (replication.Stats{Inserts: 1, Updates: 1, Deletes: 1}) {
		t.Fatalf("unexpected first run stats: %+v", stats)
	}
	assertRows(t, ctx, target, qualified("public", tableName), []string{"1:updated", "2:inserted"})
	assertTrackCount(t, ctx, source, qualified("bucardo", trackTableName), "target", 3)

	stats, err = replication.RunTopology(ctx, topology)
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

	topology := domainTopology.Topology{
		Name:    "regress_target_failure_retry",
		Sources: []domainDatabase.Database{{Name: "source", DSN: sourceDSN}},
		Targets: []domainDatabase.Database{{Name: "target", DSN: targetDSN}},
		Tables:  []table.Table{relation},
	}
	if _, err := replication.RunTopology(ctx, topology); err == nil {
		t.Fatal("expected replication to fail while target table is absent")
	}
	assertTrackCount(t, ctx, source, qualified("bucardo", trackTableName), "target", 0)
	assertTrackCount(t, ctx, source, qualified("bucardo", "stage_public_"+tableName), "target", 1)

	if _, err := target.Exec(ctx, "CREATE TABLE "+qualified("public", tableName)+" (id integer PRIMARY KEY, value text NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	stats, err := replication.RunTopology(ctx, topology)
	if err != nil {
		t.Fatal(err)
	}
	if stats != (replication.Stats{Inserts: 1}) {
		t.Fatalf("unexpected retry stats: %+v", stats)
	}
	assertRows(t, ctx, target, qualified("public", tableName), []string{"1:retry"})
	assertTrackCount(t, ctx, source, qualified("bucardo", trackTableName), "target", 1)
	assertTrackCount(t, ctx, source, qualified("bucardo", "stage_public_"+tableName), "target", 0)
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

	topology := domainTopology.Topology{
		Name:    "regress_metadata_bootstrap",
		Sources: []domainDatabase.Database{{Name: "source", DSN: sourceDSN}},
		Targets: []domainDatabase.Database{{Name: "target", DSN: targetDSN}},
		Tables:  []table.Table{relation},
	}
	if _, err := replication.RunTopology(ctx, topology); err != nil {
		t.Fatal(err)
	}
	var objectCount int
	if err := source.QueryRow(ctx, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'bucardo' AND c.relname IN ($1, $2, $3)`, deltaName, trackTableName, "stage_public_"+tableName).Scan(&objectCount); err != nil {
		t.Fatal(err)
	}
	if objectCount != 3 {
		t.Fatalf("expected automatically managed delta, stage, and track tables, got %d", objectCount)
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
	stats, err := replication.RunTopology(ctx, topology)
	if err != nil {
		t.Fatal(err)
	}
	if stats != (replication.Stats{Inserts: 1}) {
		t.Fatalf("unexpected bootstrap replication stats: %+v", stats)
	}
	assertRows(t, ctx, target, qualified("public", tableName), []string{"1:triggered"})
}

func TestRegression005TriggerUpdateDeleteRollback(t *testing.T) {
	sourceDSN := os.Getenv("BUCARDO_TEST_SOURCE_DSN")
	if sourceDSN == "" {
		t.Skip("BUCARDO_TEST_SOURCE_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	source := openPool(t, ctx, sourceDSN)
	defer source.Close()
	tableName := fmt.Sprintf("bucardo_g_trigger_%d", time.Now().UnixNano())
	deltaName := "delta_public_" + tableName
	trackTableName := "track_public_" + tableName
	relation := table.Table{Schema: "public", Name: tableName, Relation: table.RelationTable, PrimaryKey: []string{"id"}}
	defer dropTables(source, source, tableName, deltaName, trackTableName)
	if _, err := source.Exec(ctx, "CREATE TABLE "+qualified("public", tableName)+" (id integer PRIMARY KEY, value text NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if err := replication.EnsureReplicationObjects(ctx, source, relation); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(ctx, "INSERT INTO "+qualified("public", tableName)+" (id, value) VALUES (1, 'before')"); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(ctx, "TRUNCATE "+qualified("bucardo", deltaName)); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(ctx, "UPDATE "+qualified("public", tableName)+" SET value = 'after' WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	assertDeltaCount(t, ctx, source, qualified("bucardo", deltaName), 1)
	if _, err := source.Exec(ctx, "UPDATE "+qualified("public", tableName)+" SET id = 2 WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	assertDeltaCount(t, ctx, source, qualified("bucardo", deltaName), 3)
	if _, err := source.Exec(ctx, "DELETE FROM "+qualified("public", tableName)+" WHERE id = 2"); err != nil {
		t.Fatal(err)
	}
	assertDeltaCount(t, ctx, source, qualified("bucardo", deltaName), 4)
	if _, err := source.Exec(ctx, "TRUNCATE "+qualified("bucardo", deltaName)); err != nil {
		t.Fatal(err)
	}
	tx, err := source.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO "+qualified("public", tableName)+" (id, value) VALUES (2, 'rolled back')"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	assertDeltaCount(t, ctx, source, qualified("bucardo", deltaName), 0)
}

func TestRegression006CLIAndSyncrunStatuses(t *testing.T) {
	sourceDSN := os.Getenv("BUCARDO_TEST_SOURCE_DSN")
	targetDSN := os.Getenv("BUCARDO_TEST_TARGET_DSN")
	if sourceDSN == "" || targetDSN == "" {
		t.Skip("BUCARDO_TEST_SOURCE_DSN and BUCARDO_TEST_TARGET_DSN are not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	source := openPool(t, ctx, sourceDSN)
	defer source.Close()
	target := openPool(t, ctx, targetDSN)
	defer target.Close()

	tableName := fmt.Sprintf("bucardo_g_cli_%d", time.Now().UnixNano())
	syncName := fmt.Sprintf("cli_sync_%d", time.Now().UnixNano())
	deltaName := "delta_public_" + tableName
	trackTableName := "track_public_" + tableName
	defer dropTables(source, target, tableName, deltaName, trackTableName)
	for _, pool := range []*pgxpool.Pool{source, target} {
		if _, err := pool.Exec(ctx, "CREATE TABLE "+qualified("public", tableName)+" (id integer PRIMARY KEY, value text NOT NULL)"); err != nil {
			t.Fatal(err)
		}
	}

	configFile := writeCLIConfig(t, sourceDSN, targetDSN, syncName, tableName)
	defer os.Remove(configFile)
	applyOutput := runCLI(t, ctx, "apply", configFile)
	if !strings.Contains(applyOutput, "configuration applied") {
		t.Fatalf("unexpected apply output: %s", applyOutput)
	}
	runOutput := runCLI(t, ctx, "run", configFile, "--sync", syncName)
	if !strings.Contains(runOutput, "status=empty") {
		t.Fatalf("expected empty CLI output, got: %s", runOutput)
	}
	assertSyncrunStatus(t, ctx, source, syncName, "empty", "false", "false", "true")

	if _, err := source.Exec(ctx, "INSERT INTO "+qualified("public", tableName)+" (id, value) VALUES (1, 'cli-good')"); err != nil {
		t.Fatal(err)
	}
	runOutput = runCLI(t, ctx, "run", configFile, "--sync", syncName)
	if !strings.Contains(runOutput, "status=good") {
		t.Fatalf("expected good CLI output, got: %s", runOutput)
	}
	assertSyncrunStatus(t, ctx, source, syncName, "good", "true", "false", "false")

	if _, err := source.Exec(ctx, "INSERT INTO "+qualified("public", tableName)+" (id, value) VALUES (2, 'cli-bad')"); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Exec(ctx, "DROP TABLE "+qualified("public", tableName)); err != nil {
		t.Fatal(err)
	}
	if output, err := runCLIAllowFailure(ctx, "run", configFile, "--sync", syncName); err == nil || !strings.Contains(output, "status=bad") {
		t.Fatalf("expected bad CLI run, output=%s err=%v", output, err)
	}
	assertSyncrunStatus(t, ctx, source, syncName, "bad", "false", "true", "false")
}

func TestRegression007MultiTargetConfirmation(t *testing.T) {
	sourceDSN := os.Getenv("BUCARDO_TEST_SOURCE_DSN")
	targetDSN := os.Getenv("BUCARDO_TEST_TARGET_DSN")
	if sourceDSN == "" || targetDSN == "" {
		t.Skip("BUCARDO_TEST_SOURCE_DSN and BUCARDO_TEST_TARGET_DSN are not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	source := openPool(t, ctx, sourceDSN)
	defer source.Close()
	target := openPool(t, ctx, targetDSN)
	defer target.Close()
	tableName := fmt.Sprintf("bucardo_g_multi_%d", time.Now().UnixNano())
	deltaName := "delta_public_" + tableName
	trackTableName := "track_public_" + tableName
	defer dropTables(source, target, tableName, deltaName, trackTableName)
	for _, pool := range []*pgxpool.Pool{source, target} {
		if _, err := pool.Exec(ctx, "CREATE TABLE "+qualified("public", tableName)+" (id integer PRIMARY KEY, value text NOT NULL)"); err != nil {
			t.Fatal(err)
		}
	}
	topology := domainTopology.Topology{
		Name:    "regress_multi_target",
		Sources: []domainDatabase.Database{{Name: "source", DSN: sourceDSN}},
		Targets: []domainDatabase.Database{
			{Name: "target_a", DSN: targetDSN},
			{Name: "target_b", DSN: "postgres://wwn@127.0.0.1:29999/postgres?sslmode=disable"},
		},
		Tables: []table.Table{{ID: 1, Database: "source", Schema: "public", Name: tableName, Relation: table.RelationTable, PrimaryKey: []string{"id"}}},
	}
	topology.Targets = topology.Targets[:1]
	if _, err := replication.RunTopology(ctx, topology); err != nil {
		t.Fatal(err)
	}
	topology.Targets = []domainDatabase.Database{
		{Name: "target_a", DSN: targetDSN},
		{Name: "target_b", DSN: "postgres://wwn@127.0.0.1:29999/postgres?sslmode=disable"},
	}
	if _, err := source.Exec(ctx, "INSERT INTO "+qualified("public", tableName)+" (id, value) VALUES (1, 'multi')"); err != nil {
		t.Fatal(err)
	}
	if _, err := replication.RunTopology(ctx, topology); err == nil {
		t.Fatal("expected second target connection to fail")
	}
	assertTrackCount(t, ctx, source, qualified("bucardo", trackTableName), "target_a", 1)
	assertTrackCount(t, ctx, source, qualified("bucardo", trackTableName), "target_b", 0)
	assertDeltaCount(t, ctx, source, qualified("bucardo", deltaName), 1)

	topology.Targets[1].DSN = targetDSN
	stats, err := replication.RunTopology(ctx, topology)
	if err != nil {
		t.Fatal(err)
	}
	if stats != (replication.Stats{Updates: 1}) {
		t.Fatalf("unexpected recovery stats: %+v", stats)
	}
	assertTrackCount(t, ctx, source, qualified("bucardo", trackTableName), "target_a", 1)
	assertTrackCount(t, ctx, source, qualified("bucardo", trackTableName), "target_b", 1)
	assertDeltaCount(t, ctx, source, qualified("bucardo", deltaName), 0)
}

func TestRegression008StageRecoveryAndVacuum(t *testing.T) {
	sourceDSN := os.Getenv("BUCARDO_TEST_SOURCE_DSN")
	targetDSN := os.Getenv("BUCARDO_TEST_TARGET_DSN")
	if sourceDSN == "" || targetDSN == "" {
		t.Skip("BUCARDO_TEST_SOURCE_DSN and BUCARDO_TEST_TARGET_DSN are not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	source := openPool(t, ctx, sourceDSN)
	defer source.Close()
	target := openPool(t, ctx, targetDSN)
	defer target.Close()
	tableName := fmt.Sprintf("bucardo_g_vac_%d", time.Now().UnixNano())
	deltaName := "delta_public_" + tableName
	trackTableName := "track_public_" + tableName
	stageTableName := "stage_public_" + tableName
	defer dropTables(source, target, tableName, deltaName, trackTableName)
	for _, pool := range []*pgxpool.Pool{source, target} {
		if _, err := pool.Exec(ctx, "CREATE TABLE "+qualified("public", tableName)+" (id integer PRIMARY KEY, value text NOT NULL)"); err != nil {
			t.Fatal(err)
		}
	}
	relation := table.Table{ID: 1, Database: "source", Schema: "public", Name: tableName, Relation: table.RelationTable, PrimaryKey: []string{"id"}}
	if err := replication.EnsureReplicationObjects(ctx, source, relation); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(ctx, "INSERT INTO "+qualified("bucardo", stageTableName)+" (txntime, target, started) VALUES (998, 'target', now() - interval '2 hours')"); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(ctx, "INSERT INTO "+qualified("bucardo", deltaName)+" (id, txntime) VALUES (1, 999)"); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(ctx, "INSERT INTO "+qualified("bucardo", trackTableName)+" (txntime, target) VALUES (999, 'target')"); err != nil {
		t.Fatal(err)
	}
	topology := domainTopology.Topology{
		Name:    "regress_stage_recovery",
		Sources: []domainDatabase.Database{{Name: "source", DSN: sourceDSN}},
		Targets: []domainDatabase.Database{{Name: "target", DSN: targetDSN}},
		Tables:  []table.Table{relation},
	}
	if _, err := replication.RunTopology(ctx, topology); err != nil {
		t.Fatal(err)
	}
	assertTrackCount(t, ctx, source, qualified("bucardo", stageTableName), "target", 0)
	assertDeltaCount(t, ctx, source, qualified("bucardo", deltaName), 0)
}

func TestRegression009KickNotification(t *testing.T) {
	dsn := os.Getenv("BUCARDO_TEST_CONTROL_DSN")
	if dsn == "" {
		dsn = os.Getenv("BUCARDO_TEST_SOURCE_DSN")
	}
	if dsn == "" {
		t.Skip("BUCARDO_TEST_CONTROL_DSN or BUCARDO_TEST_SOURCE_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, `LISTEN bucardo`); err != nil {
		t.Fatal(err)
	}
	if err := control.NotifyKick(ctx, dsn, "regress_notify_sync"); err != nil {
		t.Fatal(err)
	}
	notification, err := conn.WaitForNotification(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if notification.Channel != "bucardo" || notification.Payload != "kick_sync_regress_notify_sync" {
		t.Fatalf("unexpected notification: channel=%s payload=%s", notification.Channel, notification.Payload)
	}
}

func TestRegression010BidirectionalReplicationWithoutLoop(t *testing.T) {
	sourceDSN := os.Getenv("BUCARDO_TEST_SOURCE_DSN")
	targetDSN := os.Getenv("BUCARDO_TEST_TARGET_DSN")
	if sourceDSN == "" || targetDSN == "" {
		t.Skip("BUCARDO_TEST_SOURCE_DSN and BUCARDO_TEST_TARGET_DSN are not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a := openPool(t, ctx, sourceDSN)
	defer a.Close()
	b := openPool(t, ctx, targetDSN)
	defer b.Close()
	tableName := fmt.Sprintf("bucardo_g_bidir_%d", time.Now().UnixNano())
	deltaName := "delta_public_" + tableName
	trackTableName := "track_public_" + tableName
	defer dropTables(a, b, tableName, deltaName, trackTableName)
	for _, pool := range []*pgxpool.Pool{a, b} {
		if _, err := pool.Exec(ctx, "CREATE TABLE "+qualified("public", tableName)+" (id integer PRIMARY KEY, value text NOT NULL)"); err != nil {
			t.Fatal(err)
		}
	}
	relationA := table.Table{ID: 1, Database: "source_a", Schema: "public", Name: tableName, Relation: table.RelationTable, PrimaryKey: []string{"id"}}
	relationB := relationA
	relationB.ID = 2
	relationB.Database = "source_b"
	topology := domainTopology.Topology{
		Name: "regress_bidirectional",
		Sources: []domainDatabase.Database{
			{Name: "source_a", DSN: sourceDSN},
			{Name: "source_b", DSN: targetDSN},
		},
		Targets: []domainDatabase.Database{
			{Name: "target_a", DSN: sourceDSN},
			{Name: "target_b", DSN: targetDSN},
		},
		Tables: []table.Table{relationA, relationB},
	}
	if _, err := replication.RunTopology(ctx, topology); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Exec(ctx, "INSERT INTO "+qualified("public", tableName)+" (id, value) VALUES (1, 'from-a')"); err != nil {
		t.Fatal(err)
	}
	stats, err := replication.RunTopology(ctx, topology)
	if err != nil {
		t.Fatal(err)
	}
	if stats != (replication.Stats{Inserts: 1, Updates: 1}) {
		t.Fatalf("unexpected bidirectional stats: %+v", stats)
	}
	assertRows(t, ctx, b, qualified("public", tableName), []string{"1:from-a"})
	assertDeltaCount(t, ctx, b, qualified("bucardo", deltaName), 0)
	assertDeltaCount(t, ctx, a, qualified("bucardo", deltaName), 0)
	assertTrackCount(t, ctx, a, qualified("bucardo", trackTableName), "target_a", 1)
	assertTrackCount(t, ctx, a, qualified("bucardo", trackTableName), "target_b", 1)
}

func TestRegression011ConflictAbort(t *testing.T) {
	sourceDSN := os.Getenv("BUCARDO_TEST_SOURCE_DSN")
	targetDSN := os.Getenv("BUCARDO_TEST_TARGET_DSN")
	if sourceDSN == "" || targetDSN == "" {
		t.Skip("BUCARDO_TEST_SOURCE_DSN and BUCARDO_TEST_TARGET_DSN are not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a := openPool(t, ctx, sourceDSN)
	defer a.Close()
	b := openPool(t, ctx, targetDSN)
	defer b.Close()
	tableName := fmt.Sprintf("bucardo_g_conflict_%d", time.Now().UnixNano())
	deltaName := "delta_public_" + tableName
	trackTableName := "track_public_" + tableName
	defer dropTables(a, b, tableName, deltaName, trackTableName)
	for _, pool := range []*pgxpool.Pool{a, b} {
		if _, err := pool.Exec(ctx, "CREATE TABLE "+qualified("public", tableName)+" (id integer PRIMARY KEY, value text NOT NULL)"); err != nil {
			t.Fatal(err)
		}
	}
	relationA := table.Table{ID: 1, Database: "source_a", Schema: "public", Name: tableName, Relation: table.RelationTable, PrimaryKey: []string{"id"}}
	relationB := relationA
	relationB.ID = 2
	relationB.Database = "source_b"
	topology := domainTopology.Topology{
		Name: "regress_conflict_abort",
		Sources: []domainDatabase.Database{
			{Name: "source_a", DSN: sourceDSN},
			{Name: "source_b", DSN: targetDSN},
		},
		Targets: []domainDatabase.Database{
			{Name: "target_a", DSN: sourceDSN},
			{Name: "target_b", DSN: targetDSN},
		},
		ConflictStrategy: "abort",
		Tables:           []table.Table{relationA, relationB},
	}
	if _, err := replication.RunTopology(ctx, topology); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Exec(ctx, "INSERT INTO "+qualified("public", tableName)+" (id, value) VALUES (1, 'from-a')"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Exec(ctx, "INSERT INTO "+qualified("public", tableName)+" (id, value) VALUES (1, 'from-b')"); err != nil {
		t.Fatal(err)
	}
	if _, err := replication.RunTopology(ctx, topology); err == nil || !strings.Contains(err.Error(), "source conflict") {
		t.Fatalf("expected source conflict abort, got %v", err)
	}
	assertDeltaCount(t, ctx, a, qualified("bucardo", deltaName), 1)
	assertDeltaCount(t, ctx, b, qualified("bucardo", deltaName), 1)
	assertTrackCount(t, ctx, a, qualified("bucardo", trackTableName), "target_a", 0)
	assertTrackCount(t, ctx, a, qualified("bucardo", trackTableName), "target_b", 0)
}

func TestRegression012ConflictLatest(t *testing.T) {
	runConflictResolutionCase(t, "latest", nil, "from-b")
}

func TestRegression013ConflictSourcePriority(t *testing.T) {
	runConflictResolutionCase(t, "source_priority", []string{"source_a", "source_b"}, "from-a")
}

func runConflictResolutionCase(t *testing.T, strategy string, priority []string, expected string) {
	t.Helper()
	sourceDSN := os.Getenv("BUCARDO_TEST_SOURCE_DSN")
	targetDSN := os.Getenv("BUCARDO_TEST_TARGET_DSN")
	if sourceDSN == "" || targetDSN == "" {
		t.Skip("BUCARDO_TEST_SOURCE_DSN and BUCARDO_TEST_TARGET_DSN are not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a := openPool(t, ctx, sourceDSN)
	defer a.Close()
	b := openPool(t, ctx, targetDSN)
	defer b.Close()
	tableName := fmt.Sprintf("bucardo_g_conflict_%s_%d", strategy, time.Now().UnixNano())
	deltaName := "delta_public_" + tableName
	trackTableName := "track_public_" + tableName
	defer dropTables(a, b, tableName, deltaName, trackTableName)
	for _, pool := range []*pgxpool.Pool{a, b} {
		if _, err := pool.Exec(ctx, "CREATE TABLE "+qualified("public", tableName)+" (id integer PRIMARY KEY, value text NOT NULL)"); err != nil {
			t.Fatal(err)
		}
	}
	relationA := table.Table{ID: 1, Database: "source_a", Schema: "public", Name: tableName, Relation: table.RelationTable, PrimaryKey: []string{"id"}}
	relationB := relationA
	relationB.ID = 2
	relationB.Database = "source_b"
	topology := domainTopology.Topology{
		Name:             "regress_conflict_" + strategy,
		Sources:          []domainDatabase.Database{{Name: "source_a", DSN: sourceDSN}, {Name: "source_b", DSN: targetDSN}},
		Targets:          []domainDatabase.Database{{Name: "target_a", DSN: sourceDSN}, {Name: "target_b", DSN: targetDSN}},
		ConflictStrategy: strategy,
		SourcePriority:   priority,
		Tables:           []table.Table{relationA, relationB},
	}
	if _, err := replication.RunTopology(ctx, topology); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Exec(ctx, "INSERT INTO "+qualified("public", tableName)+" (id, value) VALUES (1, 'from-a')"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if _, err := b.Exec(ctx, "INSERT INTO "+qualified("public", tableName)+" (id, value) VALUES (1, 'from-b')"); err != nil {
		t.Fatal(err)
	}
	if _, err := replication.RunTopology(ctx, topology); err != nil {
		t.Fatal(err)
	}
	assertRows(t, ctx, a, qualified("public", tableName), []string{"1:" + expected})
	assertRows(t, ctx, b, qualified("public", tableName), []string{"1:" + expected})
	assertDeltaCount(t, ctx, a, qualified("bucardo", deltaName), 0)
	assertDeltaCount(t, ctx, b, qualified("bucardo", deltaName), 0)
}

func writeCLIConfig(t *testing.T, sourceDSN, targetDSN, syncName, tableName string) string {
	t.Helper()
	file, err := os.CreateTemp("", "bucardo-g-cli-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	path := file.Name()
	defer file.Close()
	content := fmt.Sprintf("controlDatabase:\n  dsn: %s\ndatabases:\n  - name: source\n    role: source\n    dsn: %s\n  - name: target\n    role: target\n    dsn: %s\nsyncs:\n  - name: %s\n    source: source\n    target: target\n    conflictStrategy: abort\n    tables:\n      - schema: public\n        name: %s\n        primaryKey: [id]\n", sourceDSN, sourceDSN, targetDSN, syncName, tableName)
	if _, err := file.WriteString(content); err != nil {
		t.Fatal(err)
	}
	return path
}

func runCLI(t *testing.T, ctx context.Context, args ...string) string {
	t.Helper()
	output, err := runCLIAllowFailure(ctx, args...)
	if err != nil {
		t.Fatalf("CLI %v failed: %v\n%s", args, err, output)
	}
	return output
}

func runCLIAllowFailure(ctx context.Context, args ...string) (string, error) {
	command := exec.CommandContext(ctx, "go", append([]string{"run", "../cmd/bucardo-g", "--log-format", "text", "--log-level", "error"}, args...)...)
	output, err := command.CombinedOutput()
	return string(output), err
}

func assertSyncrunStatus(t *testing.T, ctx context.Context, source *pgxpool.Pool, syncName, status, lastGood, lastBad, lastEmpty string) {
	t.Helper()
	var actualStatus string
	var actualGood, actualBad, actualEmpty bool
	err := source.QueryRow(ctx, `SELECT status, lastgood, lastbad, lastempty FROM bucardo.syncrun WHERE sync = $1 ORDER BY started DESC LIMIT 1`, syncName).Scan(&actualStatus, &actualGood, &actualBad, &actualEmpty)
	if err != nil {
		t.Fatal(err)
	}
	if actualStatus != status || fmt.Sprint(actualGood) != lastGood || fmt.Sprint(actualBad) != lastBad || fmt.Sprint(actualEmpty) != lastEmpty {
		t.Fatalf("unexpected syncrun status: status=%s good=%t bad=%t empty=%t", actualStatus, actualGood, actualBad, actualEmpty)
	}
}

func assertDeltaCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, relation string, expected int) {
	t.Helper()
	var actual int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+relation).Scan(&actual); err != nil {
		t.Fatal(err)
	}
	if actual != expected {
		t.Fatalf("unexpected delta count: got %d want %d", actual, expected)
	}
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
		"DROP TABLE IF EXISTS " + qualified("bucardo", strings.Replace(trackName, "track_", "stage_", 1)),
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
