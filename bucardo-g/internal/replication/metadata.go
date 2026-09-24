package replication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/bucardo-g/internal/domain/table"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ensureReplicationObjects(ctx context.Context, source *pgxpool.Pool, relation table.Table) error {
	if relation.Relation != table.RelationTable || len(relation.PrimaryKey) == 0 {
		return nil
	}
	if _, err := source.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS "bucardo"`); err != nil {
		return fmt.Errorf("create Bucardo schema: %w", err)
	}

	delta := "delta_" + relation.Schema + "_" + relation.Name
	track := trackName(relation)
	columns := joinQuoted(relation.PrimaryKey)
	if _, err := source.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+qualifiedName("bucardo", delta)+
		` AS SELECT `+columns+`, NULL::bigint AS "txntime" FROM `+qualifiedName(relation.Schema, relation.Name)+` WHERE false`); err != nil {
		return fmt.Errorf("create delta table %s: %w", delta, err)
	}
	if _, err := source.Exec(ctx, `ALTER TABLE `+qualifiedName("bucardo", delta)+` ADD COLUMN IF NOT EXISTS "txntime" bigint`); err != nil {
		return fmt.Errorf("add delta timestamp %s: %w", delta, err)
	}
	if _, err := source.Exec(ctx, `CREATE INDEX IF NOT EXISTS `+quoteIdent(delta+"_txntime_idx")+
		` ON `+qualifiedName("bucardo", delta)+` ("txntime")`); err != nil {
		return fmt.Errorf("index delta table %s: %w", delta, err)
	}
	if _, err := source.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+qualifiedName("bucardo", track)+
		` ("txntime" bigint NOT NULL, "target" text NOT NULL, PRIMARY KEY ("txntime", "target"))`); err != nil {
		return fmt.Errorf("create track table %s: %w", track, err)
	}

	functionName := deltaFunctionName(relation)
	newValues := make([]string, len(relation.PrimaryKey))
	oldValues := make([]string, len(relation.PrimaryKey))
	for i, key := range relation.PrimaryKey {
		newValues[i] = "NEW." + quoteIdent(key)
		oldValues[i] = "OLD." + quoteIdent(key)
	}
	functionSQL := `CREATE OR REPLACE FUNCTION ` + qualifiedName("bucardo", functionName) + `()
RETURNS trigger LANGUAGE plpgsql AS $bucardo_g$
BEGIN
	IF TG_OP = 'DELETE' THEN
		INSERT INTO ` + qualifiedName("bucardo", delta) + ` (` + columns + `, "txntime") VALUES (` + strings.Join(oldValues, ", ") + `, txid_current());
		RETURN OLD;
	END IF;
	INSERT INTO ` + qualifiedName("bucardo", delta) + ` (` + columns + `, "txntime") VALUES (` + strings.Join(newValues, ", ") + `, txid_current());
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
	return nil
}

func deltaFunctionName(relation table.Table) string {
	hash := sha256.Sum256([]byte(relation.Schema + "." + relation.Name))
	return "bucardo_g_delta_" + hex.EncodeToString(hash[:])[:16]
}

func qualifiedName(schema, name string) string {
	return quoteIdent(schema) + "." + quoteIdent(name)
}
