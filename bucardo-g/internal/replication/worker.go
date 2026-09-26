// Package replication implements the PostgreSQL data plane. A run reads source
// delta queues, applies rows in target transactions, confirms track entries, and
// cleans only changes confirmed by every required target.
package replication

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/bucardo-g/internal/control"
	"github.com/bucardo-g/internal/domain/table"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Stats struct {
	Inserts int64
	Updates int64
	Deletes int64
}

func RunOnce(ctx context.Context, sync control.Sync) (Stats, error) {
	logger := slog.Default()
	logger.Debug("replication worker started", "sync", sync.Name, "tables", len(sync.Tables))
	sources := sync.Sources
	if len(sources) == 0 {
		sources = []control.Database{sync.Source}
	}
	sourcePools := make(map[string]*pgxpool.Pool, len(sources))
	for _, sourceConfig := range sources {
		pool, err := pgxpool.New(ctx, sourceConfig.DSN)
		if err != nil {
			return Stats{}, fmt.Errorf("open source database %q: %w", sourceConfig.Name, err)
		}
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			return Stats{}, fmt.Errorf("ping source database %q: %w", sourceConfig.Name, err)
		}
		sourcePools[sourceConfig.Name] = pool
	}
	defer func() {
		for _, pool := range sourcePools {
			pool.Close()
		}
	}()
	targets := sync.Targets
	if len(targets) == 0 {
		targets = []control.Database{sync.Target}
	}
	targetNames := make([]string, len(targets))
	for i, target := range targets {
		if len(targets) == 1 {
			targetNames[i] = sync.TargetName
		} else {
			targetNames[i] = target.Name
		}
	}

	var stats Stats
	// Targets are processed serially so a failed later target cannot prevent an
	// earlier target from recording its confirmation.
	for i, targetConfig := range targets {
		target, err := pgxpool.New(ctx, targetConfig.DSN)
		if err != nil {
			return Stats{}, fmt.Errorf("open target database %q: %w", targetConfig.Name, err)
		}
		if err := target.Ping(ctx); err != nil {
			target.Close()
			return Stats{}, fmt.Errorf("ping target database %q: %w", targetConfig.Name, err)
		}
		for _, sourceConfig := range sources {
			source := sourcePools[sourceConfig.Name]
			for _, relation := range sync.Tables {
				if relation.Relation != table.RelationTable || (relation.Database != "" && relation.Database != sourceConfig.Name) {
					continue
				}
				if err := ensureReplicationObjects(ctx, source, relation); err != nil {
					return Stats{}, fmt.Errorf("prepare replication metadata for %s.%s on %s: %w", relation.Schema, relation.Name, sourceConfig.Name, err)
				}
				if err := recoverStaleStages(ctx, source, relation, targetNames); err != nil {
					return Stats{}, err
				}
				tableStats, err := copyTable(ctx, source, target, targetNames[i], relation)
				if err != nil {
					target.Close()
					return Stats{}, fmt.Errorf("copy %s.%s from %s to %s: %w", relation.Schema, relation.Name, sourceConfig.Name, targetConfig.Name, err)
				}
				stats.Inserts += tableStats.Inserts
				stats.Updates += tableStats.Updates
				stats.Deletes += tableStats.Deletes
				logger.Debug("table replication completed", "sync", sync.Name, "source", sourceConfig.Name, "target", targetConfig.Name, "schema", relation.Schema, "table", relation.Name, "inserts", tableStats.Inserts, "updates", tableStats.Updates, "deletes", tableStats.Deletes)
			}
		}
		target.Close()
	}
	for _, sourceConfig := range sources {
		source := sourcePools[sourceConfig.Name]
		for _, relation := range sync.Tables {
			if relation.Relation != table.RelationTable || (relation.Database != "" && relation.Database != sourceConfig.Name) {
				continue
			}
			if err := cleanupDelta(ctx, source, relation, targetNames); err != nil {
				return Stats{}, err
			}
		}
	}
	logger.Debug("replication worker completed", "sync", sync.Name, "inserts", stats.Inserts, "updates", stats.Updates, "deletes", stats.Deletes)
	return stats, nil
}

func copyTable(ctx context.Context, source, target *pgxpool.Pool, targetName string, relation table.Table) (Stats, error) {
	deltaName := "delta_" + relation.Schema + "_" + relation.Name
	deltaRows, err := source.Query(ctx, `SELECT d.* FROM `+quoteIdent("bucardo")+`.`+quoteIdent(deltaName)+` d
		LEFT JOIN `+quoteIdent("bucardo")+`.`+quoteIdent(trackName(relation))+` t
			ON t.txntime = d.txntime AND t.target = $1
		WHERE t.txntime IS NULL
		ORDER BY d.txntime`, targetName)
	if err != nil {
		return Stats{}, fmt.Errorf("read delta table %s: %w", deltaName, err)
	}
	defer deltaRows.Close()

	tx, err := target.Begin(ctx)
	if err != nil {
		return Stats{}, fmt.Errorf("begin target transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT set_config('bucardo_g.replication_write', 'on', true)`); err != nil {
		return Stats{}, fmt.Errorf("set replication write mode: %w", err)
	}
	var stats Stats
	fields := deltaRows.FieldDescriptions()
	if len(fields) < len(relation.PrimaryKey)+1 {
		return Stats{}, fmt.Errorf("delta table %s has unexpected columns", deltaName)
	}
	var txntimes []any
	for deltaRows.Next() {
		values, err := deltaRows.Values()
		if err != nil {
			return Stats{}, fmt.Errorf("read delta row: %w", err)
		}
		keys := values[:len(relation.PrimaryKey)]
		txntime := values[len(relation.PrimaryKey)]
		if err := markStage(ctx, source, relation, targetName, txntime); err != nil {
			return Stats{}, err
		}
		sourceRow, columns, err := readCurrentRow(ctx, source, relation, keys)
		if err != nil {
			return Stats{}, err
		}
		if sourceRow == nil {
			if err := deleteTargetRow(ctx, tx, relation, keys); err != nil {
				return Stats{}, err
			}
			stats.Deletes++
		} else {
			exists, err := targetRowExists(ctx, tx, relation, keys)
			if err != nil {
				return Stats{}, err
			}
			if err := upsertTargetRow(ctx, tx, relation, columns, sourceRow); err != nil {
				return Stats{}, err
			}
			if exists {
				stats.Updates++
			} else {
				stats.Inserts++
			}
		}
		txntimes = append(txntimes, txntime)
	}
	if err := deltaRows.Err(); err != nil {
		return Stats{}, fmt.Errorf("iterate delta table: %w", err)
	}
	slog.Default().Debug("delta rows loaded", "schema", relation.Schema, "table", relation.Name, "rows", len(txntimes), "target", targetName)
	if err := tx.Commit(ctx); err != nil {
		return Stats{}, fmt.Errorf("commit target transaction: %w", err)
	}
	for _, txntime := range txntimes {
		if err := markTrack(ctx, source, relation, targetName, txntime); err != nil {
			return Stats{}, err
		}
		if err := clearStage(ctx, source, relation, targetName, txntime); err != nil {
			return Stats{}, err
		}
	}
	return stats, nil
}

func readCurrentRow(ctx context.Context, source *pgxpool.Pool, relation table.Table, keys []any) ([]any, []string, error) {
	conditions := make([]string, len(relation.PrimaryKey))
	for i, key := range relation.PrimaryKey {
		conditions[i] = quoteIdent(key) + "=$" + fmt.Sprint(i+1)
	}
	rows, err := source.Query(ctx, `SELECT * FROM `+quoteIdent(relation.Schema)+`.`+quoteIdent(relation.Name)+` WHERE `+strings.Join(conditions, " AND "), keys...)
	if err != nil {
		return nil, nil, fmt.Errorf("read current row: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, nil, nil
	}
	values, err := rows.Values()
	if err != nil {
		return nil, nil, fmt.Errorf("read current values: %w", err)
	}
	fields := rows.FieldDescriptions()
	columns := make([]string, len(fields))
	for i, field := range fields {
		columns[i] = string(field.Name)
	}
	return values, columns, nil
}

func upsertTargetRow(ctx context.Context, tx pgx.Tx, relation table.Table, columns []string, values []any) error {
	placeholders := make([]string, len(columns))
	quotedColumns := make([]string, len(columns))
	updates := make([]string, 0, len(columns))
	for i, column := range columns {
		placeholders[i] = "$" + fmt.Sprint(i+1)
		quotedColumns[i] = quoteIdent(column)
		if !contains(relation.PrimaryKey, column) {
			updates = append(updates, quoteIdent(column)+"=EXCLUDED."+quoteIdent(column))
		}
	}
	action := "DO NOTHING"
	if len(updates) > 0 {
		action = "DO UPDATE SET " + strings.Join(updates, ", ")
	}
	_, err := tx.Exec(ctx, `INSERT INTO `+quoteIdent(relation.Schema)+`.`+quoteIdent(relation.Name)+
		` (`+strings.Join(quotedColumns, ", ")+`) VALUES (`+strings.Join(placeholders, ", ")+`) ON CONFLICT (`+
		joinQuoted(relation.PrimaryKey)+`) `+action, values...)
	if err != nil {
		return fmt.Errorf("upsert target row: %w", err)
	}
	return nil
}

func deleteTargetRow(ctx context.Context, tx pgx.Tx, relation table.Table, keys []any) error {
	conditions := make([]string, len(relation.PrimaryKey))
	for i, key := range relation.PrimaryKey {
		conditions[i] = quoteIdent(key) + "=$" + fmt.Sprint(i+1)
	}
	_, err := tx.Exec(ctx, `DELETE FROM `+quoteIdent(relation.Schema)+`.`+quoteIdent(relation.Name)+` WHERE `+strings.Join(conditions, " AND "), keys...)
	if err != nil {
		return fmt.Errorf("delete target row: %w", err)
	}
	return nil
}

func targetRowExists(ctx context.Context, tx pgx.Tx, relation table.Table, keys []any) (bool, error) {
	conditions := make([]string, len(relation.PrimaryKey))
	for i, key := range relation.PrimaryKey {
		conditions[i] = quoteIdent(key) + "=$" + fmt.Sprint(i+1)
	}
	var exists bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+quoteIdent(relation.Schema)+`.`+quoteIdent(relation.Name)+` WHERE `+strings.Join(conditions, " AND ")+`)`, keys...).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check target row: %w", err)
	}
	return exists, nil
}

func markTrack(ctx context.Context, source *pgxpool.Pool, relation table.Table, target string, txntime any) error {
	_, err := source.Exec(ctx, `INSERT INTO `+quoteIdent("bucardo")+`.`+quoteIdent(trackName(relation))+` (txntime,target) VALUES ($1,$2)`, txntime, target)
	if err != nil {
		return fmt.Errorf("mark track table: %w", err)
	}
	return nil
}

func markStage(ctx context.Context, source *pgxpool.Pool, relation table.Table, target string, txntime any) error {
	_, err := source.Exec(ctx, `INSERT INTO `+quoteIdent("bucardo")+`.`+quoteIdent(stageName(relation))+` (txntime,target) VALUES ($1,$2) ON CONFLICT (txntime,target) DO UPDATE SET started = now()`, txntime, target)
	if err != nil {
		return fmt.Errorf("mark stage table: %w", err)
	}
	return nil
}

func clearStage(ctx context.Context, source *pgxpool.Pool, relation table.Table, target string, txntime any) error {
	_, err := source.Exec(ctx, `DELETE FROM `+quoteIdent("bucardo")+`.`+quoteIdent(stageName(relation))+` WHERE txntime = $1 AND target = $2`, txntime, target)
	if err != nil {
		return fmt.Errorf("clear stage table: %w", err)
	}
	return nil
}

func cleanupDelta(ctx context.Context, source *pgxpool.Pool, relation table.Table, targets []string) error {
	if len(targets) == 0 {
		return nil
	}
	delta := "delta_" + relation.Schema + "_" + relation.Name
	_, err := source.Exec(ctx, `DELETE FROM `+quoteIdent("bucardo")+`.`+quoteIdent(delta)+` d
		WHERE NOT EXISTS (
			SELECT 1 FROM unnest($1::text[]) required(target)
			WHERE NOT EXISTS (
				SELECT 1 FROM `+quoteIdent("bucardo")+`.`+quoteIdent(trackName(relation))+` t
				WHERE t.txntime = d.txntime AND t.target = required.target
			)
		)`, targets)
	if err != nil {
		return fmt.Errorf("clean delta table %s: %w", delta, err)
	}
	return nil
}

func recoverStaleStages(ctx context.Context, source *pgxpool.Pool, relation table.Table, targets []string) error {
	if len(targets) == 0 {
		return nil
	}
	_, err := source.Exec(ctx, `DELETE FROM `+quoteIdent("bucardo")+`.`+quoteIdent(stageName(relation))+` s
		WHERE s.started < now() - interval '1 hour'
		AND s.target = ANY($1::text[])
		AND NOT EXISTS (
			SELECT 1 FROM `+quoteIdent("bucardo")+`.`+quoteIdent(trackName(relation))+` t
			WHERE t.txntime = s.txntime AND t.target = s.target
		)`, targets)
	if err != nil {
		return fmt.Errorf("recover stale stage table %s: %w", stageName(relation), err)
	}
	return nil
}

func trackName(relation table.Table) string {
	return "track_" + relation.Schema + "_" + relation.Name
}

func quoteIdent(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }

func joinQuoted(values []string) string {
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = quoteIdent(value)
	}
	return strings.Join(quoted, ", ")
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
