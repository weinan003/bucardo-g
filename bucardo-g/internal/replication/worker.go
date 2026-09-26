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
	source, err := pgxpool.New(ctx, sync.Source.DSN)
	if err != nil {
		return Stats{}, fmt.Errorf("open source database: %w", err)
	}
	defer source.Close()
	target, err := pgxpool.New(ctx, sync.Target.DSN)
	if err != nil {
		return Stats{}, fmt.Errorf("open target database: %w", err)
	}
	defer target.Close()
	if err := source.Ping(ctx); err != nil {
		return Stats{}, fmt.Errorf("ping source database: %w", err)
	}
	if err := target.Ping(ctx); err != nil {
		return Stats{}, fmt.Errorf("ping target database: %w", err)
	}

	var stats Stats
	for _, relation := range sync.Tables {
		if relation.Relation != table.RelationTable {
			continue
		}
		if err := ensureReplicationObjects(ctx, source, relation); err != nil {
			return stats, fmt.Errorf("prepare replication metadata for %s.%s: %w", relation.Schema, relation.Name, err)
		}
		tableStats, err := copyTable(ctx, source, target, sync.TargetName, relation)
		if err != nil {
			return stats, fmt.Errorf("copy %s.%s: %w", relation.Schema, relation.Name, err)
		}
		stats.Inserts += tableStats.Inserts
		stats.Updates += tableStats.Updates
		stats.Deletes += tableStats.Deletes
		logger.Debug("table replication completed", "sync", sync.Name, "schema", relation.Schema, "table", relation.Name, "inserts", tableStats.Inserts, "updates", tableStats.Updates, "deletes", tableStats.Deletes)
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
		txntimes = append(txntimes, values[len(relation.PrimaryKey)])
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
