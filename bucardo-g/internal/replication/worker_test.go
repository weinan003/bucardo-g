package replication

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bucardo-g/internal/domain/table"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCopyTableAppliesDeltaAndSkipsTrackedRows(t *testing.T) {
	sourceDSN := os.Getenv("BUCARDO_TEST_SOURCE_DSN")
	targetDSN := os.Getenv("BUCARDO_TEST_TARGET_DSN")
	if sourceDSN == "" || targetDSN == "" {
		t.Skip("BUCARDO_TEST_SOURCE_DSN and BUCARDO_TEST_TARGET_DSN are not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	source, err := pgxpool.New(ctx, sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	target, err := pgxpool.New(ctx, targetDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if err := source.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := target.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	tableName := fmt.Sprintf("bucardo_g_copy_%d", time.Now().UnixNano())
	relation := table.Table{Schema: "public", Name: tableName, Relation: table.RelationTable, PrimaryKey: []string{"id"}}
	deltaName := "delta_public_" + tableName
	trackTableName := "track_public_" + tableName
	cleanup := func(pool *pgxpool.Pool, statements ...string) {
		for _, statement := range statements {
			if _, err := pool.Exec(context.Background(), statement); err != nil {
				t.Logf("cleanup %q: %v", statement, err)
			}
		}
	}
	defer cleanup(source,
		"DROP TABLE IF EXISTS "+qualified("bucardo", trackTableName),
		"DROP TABLE IF EXISTS "+qualified("bucardo", deltaName),
		"DROP TABLE IF EXISTS "+qualified("public", tableName),
	)
	defer cleanup(target, "DROP TABLE IF EXISTS "+qualified("public", tableName))

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

	stats, err := copyTable(ctx, source, target, "db-target", relation)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Inserts != 1 || stats.Updates != 1 || stats.Deletes != 1 {
		t.Fatalf("unexpected first-run stats: %+v", stats)
	}
	assertRows(t, ctx, target, qualified("public", tableName), []string{"1:updated", "2:inserted"})
	var trackCount int
	if err := source.QueryRow(ctx, "SELECT count(*) FROM "+qualified("bucardo", trackTableName)+" WHERE target = $1", "db-target").Scan(&trackCount); err != nil {
		t.Fatal(err)
	}
	if trackCount != 3 {
		t.Fatalf("expected 3 track rows, got %d", trackCount)
	}

	stats, err = copyTable(ctx, source, target, "db-target", relation)
	if err != nil {
		t.Fatal(err)
	}
	if stats != (Stats{}) {
		t.Fatalf("expected empty second run, got %+v", stats)
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

func qualified(schema, name string) string {
	return quoteIdent(schema) + "." + quoteIdent(name)
}
