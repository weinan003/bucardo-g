package control

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const controlSchemaVersion = 1

// ensureSchema installs the control metadata required by the current Go MVP.
// Each migration is idempotent and recorded for future upgrades.
func ensureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	statements := []string{
		`CREATE SCHEMA IF NOT EXISTS bucardo`,
		`CREATE TABLE IF NOT EXISTS bucardo.bucardo_g_schema_version (
			version integer PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS bucardo.db (
			name text PRIMARY KEY,
			dbtype text NOT NULL DEFAULT 'postgres',
			dbhost text,
			dbport text,
			dbname text,
			dbuser text,
			dbpass text,
			dbservice text,
			dbconn text NOT NULL DEFAULT '',
			dbdsn text NOT NULL DEFAULT '',
			status text NOT NULL DEFAULT 'active'
		)`,
		`CREATE TABLE IF NOT EXISTS bucardo.dbgroup (
			name text PRIMARY KEY
		)`,
		`CREATE TABLE IF NOT EXISTS bucardo.dbmap (
			db text NOT NULL,
			dbgroup text NOT NULL,
			role text NOT NULL,
			priority integer NOT NULL DEFAULT 0,
			PRIMARY KEY (db, dbgroup, role)
		)`,
		`CREATE TABLE IF NOT EXISTS bucardo.goat (
			id bigserial PRIMARY KEY,
			db text NOT NULL,
			schemaname text NOT NULL,
			tablename text NOT NULL,
			reltype text NOT NULL DEFAULT 'table',
			pkey text NOT NULL DEFAULT '',
			ghost boolean NOT NULL DEFAULT false
		)`,
		`CREATE TABLE IF NOT EXISTS bucardo.herd (
			name text PRIMARY KEY
		)`,
		`CREATE TABLE IF NOT EXISTS bucardo.herdmap (
			herd text NOT NULL,
			goat bigint NOT NULL,
			priority integer NOT NULL DEFAULT 0,
			PRIMARY KEY (herd, goat)
		)`,
		`CREATE TABLE IF NOT EXISTS bucardo.sync (
			name text PRIMARY KEY,
			herd text NOT NULL,
			dbs text NOT NULL,
			status text NOT NULL DEFAULT 'active',
			deletemethod text NOT NULL DEFAULT 'delete',
			autokick boolean NOT NULL DEFAULT false
		)`,
		`CREATE TABLE IF NOT EXISTS bucardo.syncrun (
			sync text,
			truncates integer NOT NULL DEFAULT 0,
			deletes bigint NOT NULL DEFAULT 0,
			inserts bigint NOT NULL DEFAULT 0,
			conflicts bigint NOT NULL DEFAULT 0,
			started timestamptz NOT NULL DEFAULT now(),
			ended timestamptz,
			lastgood boolean NOT NULL DEFAULT false,
			lastbad boolean NOT NULL DEFAULT false,
			lastempty boolean NOT NULL DEFAULT false,
			details text,
			status text
		)`,
		`CREATE INDEX IF NOT EXISTS syncrun_sync_started ON bucardo.syncrun(sync) WHERE ended IS NULL`,
		`CREATE TABLE IF NOT EXISTS bucardo.bucardo_delta_names (
			sync text NOT NULL,
			tablename text NOT NULL,
			deltaname text NOT NULL,
			trackname text NOT NULL,
			cdate timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (sync, tablename)
		)`,
		`CREATE TABLE IF NOT EXISTS bucardo.bucardo_delta_targets (
			tablename oid NOT NULL,
			target text NOT NULL,
			cdate timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (tablename, target)
		)`,
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			return fmt.Errorf("apply control schema: %w", err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO bucardo.bucardo_g_schema_version(version) VALUES ($1) ON CONFLICT (version) DO NOTHING`, controlSchemaVersion); err != nil {
		return fmt.Errorf("record control schema version: %w", err)
	}
	return nil
}
