package replication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"

	"github.com/bucardo-g/internal/domain/table"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ensureReplicationObjects(ctx context.Context, source *pgxpool.Pool, relation table.Table) error {
	logger := slog.Default()
	if relation.Relation != table.RelationTable || len(relation.PrimaryKey) == 0 {
		return nil
	}
	if _, err := source.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS "bucardo"`); err != nil {
		return fmt.Errorf("create Bucardo schema: %w", err)
	}

	delta := "delta_" + relation.Schema + "_" + relation.Name
	stage := stageName(relation)
	track := trackName(relation)
	columns := joinQuoted(relation.PrimaryKey)
	if _, err := source.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+qualifiedName("bucardo", delta)+
		` AS SELECT `+columns+`, NULL::bigint AS "txntime", NULL::timestamptz AS "changed_at" FROM `+qualifiedName(relation.Schema, relation.Name)+` WHERE false`); err != nil {
		return fmt.Errorf("create delta table %s: %w", delta, err)
	}
	if _, err := source.Exec(ctx, `CREATE INDEX IF NOT EXISTS `+quoteIdent(delta+"_txntime_idx")+
		` ON `+qualifiedName("bucardo", delta)+` ("txntime")`); err != nil {
		return fmt.Errorf("index delta table %s: %w", delta, err)
	}
	if _, err := source.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+qualifiedName("bucardo", track)+
		` ("txntime" bigint NOT NULL, "target" text NOT NULL, PRIMARY KEY ("txntime", "target"))`); err != nil {
		return fmt.Errorf("create track table %s: %w", track, err)
	}
	if _, err := source.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+qualifiedName("bucardo", stage)+
		` ("txntime" bigint NOT NULL, "target" text NOT NULL, "started" timestamptz NOT NULL DEFAULT now(), PRIMARY KEY ("txntime", "target"))`); err != nil {
		return fmt.Errorf("create stage table %s: %w", stage, err)
	}

	functionName := deltaFunctionName(relation)
	newValues := make([]string, len(relation.PrimaryKey))
	oldValues := make([]string, len(relation.PrimaryKey))
	keyChanged := make([]string, len(relation.PrimaryKey))
	for i, key := range relation.PrimaryKey {
		newValues[i] = "NEW." + quoteIdent(key)
		oldValues[i] = "OLD." + quoteIdent(key)
		keyChanged[i] = "OLD." + quoteIdent(key) + " IS DISTINCT FROM NEW." + quoteIdent(key)
	}
	// The trigger bypass is transaction-local. It prevents writes made by this
	// worker on a peer database from becoming a new delta and forming a loop.
	functionSQL := `CREATE OR REPLACE FUNCTION ` + qualifiedName("bucardo", functionName) + `()
RETURNS trigger LANGUAGE plpgsql AS $bucardo_g$
BEGIN
	IF current_setting('bucardo_g.replication_write', true) = 'on' THEN
		IF TG_OP = 'DELETE' THEN
			RETURN OLD;
		END IF;
		RETURN NEW;
	END IF;
	IF TG_OP = 'DELETE' THEN
		INSERT INTO ` + qualifiedName("bucardo", delta) + ` (` + columns + `, "txntime", "changed_at") VALUES (` + strings.Join(oldValues, ", ") + `, txid_current(), clock_timestamp());
		RETURN OLD;
	END IF;
	IF TG_OP = 'UPDATE' AND (` + strings.Join(keyChanged, " OR ") + `) THEN
		INSERT INTO ` + qualifiedName("bucardo", delta) + ` (` + columns + `, "txntime", "changed_at") VALUES (` + strings.Join(oldValues, ", ") + `, txid_current(), clock_timestamp());
	END IF;
	INSERT INTO ` + qualifiedName("bucardo", delta) + ` (` + columns + `, "txntime", "changed_at") VALUES (` + strings.Join(newValues, ", ") + `, txid_current(), clock_timestamp());
	RETURN NEW;
END;
$bucardo_g$`
	if _, err := source.Exec(ctx, functionSQL); err != nil {
		return fmt.Errorf("create delta trigger function %s: %w", functionName, err)
	}
	triggerSQL := `DROP TRIGGER IF EXISTS "bucardo_g_delta" ON ` + qualifiedName(relation.Schema, relation.Name) + `;
CREATE TRIGGER "bucardo_g_delta" AFTER INSERT OR UPDATE OR DELETE ON ` + qualifiedName(relation.Schema, relation.Name) + `
FOR EACH ROW EXECUTE FUNCTION ` + qualifiedName("bucardo", functionName) + `()`
	if _, err := source.Exec(ctx, triggerSQL); err != nil {
		return fmt.Errorf("create delta trigger on %s.%s: %w", relation.Schema, relation.Name, err)
	}
	logger.Debug("replication metadata ready", "schema", relation.Schema, "table", relation.Name, "delta_table", delta, "stage_table", stage, "track_table", track)
	return nil
}

// EnsureReplicationObjects installs the delta/track metadata for one source table.
func EnsureReplicationObjects(ctx context.Context, source *pgxpool.Pool, relation table.Table) error {
	return ensureReplicationObjects(ctx, source, relation)
}

func deltaFunctionName(relation table.Table) string {
	hash := sha256.Sum256([]byte(relation.Schema + "." + relation.Name))
	return "bucardo_g_delta_" + hex.EncodeToString(hash[:])[:16]
}

func stageName(relation table.Table) string {
	return "stage_" + relation.Schema + "_" + relation.Name
}

func qualifiedName(schema, name string) string {
	return quoteIdent(schema) + "." + quoteIdent(name)
}
