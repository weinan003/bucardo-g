package control

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/bucardo-g/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func (s *Store) ApplyConfig(ctx context.Context, cfg config.Config) error {
	// Validate and ping first so a failed configuration never partially reaches
	// the control database transaction.
	logger := slog.Default()
	if err := cfg.Validate(); err != nil {
		return err
	}
	for _, db := range cfg.Databases {
		pool, err := pgxpool.New(ctx, db.DSN)
		if err != nil {
			return fmt.Errorf("open configured database %q: %w", db.Name, err)
		}
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			return fmt.Errorf("ping configured database %q: %w", db.Name, err)
		}
		pool.Close()
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin config apply: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, db := range cfg.Databases {
		if _, err := tx.Exec(ctx, `
			INSERT INTO bucardo.db (name, dbtype, dbdsn, dbconn, status)
			VALUES ($1, 'postgres', $2, '', 'active')
			ON CONFLICT (name) DO UPDATE SET dbtype = 'postgres', dbdsn = EXCLUDED.dbdsn, status = 'active'`, db.Name, db.DSN); err != nil {
			return fmt.Errorf("apply database %q: %w", db.Name, err)
		}
	}
	for _, syncConfig := range cfg.Syncs {
		targetGroup := syncConfig.TargetGroupName()
		herdName := syncConfig.Name + "_herd"
		if _, err := tx.Exec(ctx, `INSERT INTO bucardo.dbgroup(name) VALUES ($1) ON CONFLICT (name) DO NOTHING`, targetGroup); err != nil {
			return fmt.Errorf("apply target group %q: %w", targetGroup, err)
		}
		for priority, target := range syncConfig.TargetNames() {
			if _, err := tx.Exec(ctx, `
				INSERT INTO bucardo.dbmap (db, dbgroup, role, priority)
				VALUES ($1, $2, 'target', $3)
				ON CONFLICT (db, dbgroup, role) DO UPDATE SET priority = EXCLUDED.priority`, target, targetGroup, len(syncConfig.TargetNames())-priority); err != nil {
				return fmt.Errorf("apply target mapping for %q: %w", syncConfig.Name, err)
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO bucardo.herd(name) VALUES ($1) ON CONFLICT (name) DO NOTHING`, herdName); err != nil {
			return fmt.Errorf("apply herd %q: %w", herdName, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO bucardo.sync (name, herd, dbs, status, deletemethod, autokick)
			VALUES ($1, $2, $3, 'active', $4, false)
			ON CONFLICT (name) DO UPDATE SET herd = EXCLUDED.herd, dbs = EXCLUDED.dbs, status = 'active', deletemethod = EXCLUDED.deletemethod`, syncConfig.Name, herdName, targetGroup, defaultDeleteMethod(syncConfig.DeleteMethod)); err != nil {
			return fmt.Errorf("apply sync %q: %w", syncConfig.Name, err)
		}
		for _, sourceDatabase := range syncConfig.SourceNames() {
			for _, item := range syncConfig.Tables {
				var goatID int64
				err := tx.QueryRow(ctx, `SELECT id FROM bucardo.goat WHERE db = $1 AND schemaname = $2 AND tablename = $3`, sourceDatabase, item.Schema, item.Name).Scan(&goatID)
				if err == pgx.ErrNoRows {
					err = tx.QueryRow(ctx, `
					INSERT INTO bucardo.goat (db, schemaname, tablename, reltype, pkey, ghost)
					VALUES ($1, $2, $3, 'table', $4, false) RETURNING id`, sourceDatabase, item.Schema, item.Name, strings.Join(item.PrimaryKey, "|")).Scan(&goatID)
				}
				if err != nil {
					return fmt.Errorf("apply table %s.%s: %w", item.Schema, item.Name, err)
				}
				if _, err := tx.Exec(ctx, `
				INSERT INTO bucardo.herdmap (herd, goat, priority)
				VALUES ($1, $2, 100)
				ON CONFLICT (herd, goat) DO UPDATE SET priority = EXCLUDED.priority`, herdName, goatID); err != nil {
					return fmt.Errorf("apply herd mapping for %s.%s: %w", item.Schema, item.Name, err)
				}
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit config apply: %w", err)
	}
	logger.Info("control configuration committed", "databases", len(cfg.Databases), "syncs", len(cfg.Syncs))
	return nil
}

func defaultDeleteMethod(value string) string {
	if value == "" {
		return "delete"
	}
	return value
}
