// Package control owns control-database access, configuration projection,
// run history, advisory locks, and PostgreSQL notifications.
package control

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/bucardo-g/internal/domain/database"
	"github.com/bucardo-g/internal/domain/job"
	"github.com/bucardo-g/internal/domain/table"
	"github.com/bucardo-g/internal/domain/topology"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct{ pool *pgxpool.Pool }
type SyncLock struct{ conn *pgx.Conn }

func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open control database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping control database: %w", err)
	}
	if err := ensureSchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	slog.Default().Debug("control database ready")
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// LoadTopology reads persisted control state and returns the runtime domain topology.
func (s *Store) LoadTopology(ctx context.Context, name string) (topology.Topology, error) {
	var result topology.Topology
	var herdName, targetGroup, sourcePriority string
	if err := s.pool.QueryRow(ctx, `
		SELECT name, herd, dbs, COALESCE(conflict, 'abort'), COALESCE(source_priority, ''), COALESCE(deletemethod, 'delete')
		FROM bucardo.sync WHERE name = $1 AND status = 'active'`, name,
	).Scan(&result.Name, &herdName, &targetGroup, &result.ConflictStrategy, &sourcePriority, &result.DeleteMethod); err != nil {
		return topology.Topology{}, fmt.Errorf("load topology %q: %w", name, err)
	}
	result.SourcePriority = splitPrimaryKey(sourcePriority)

	targetRows, err := s.pool.Query(ctx, `
		SELECT d.name, COALESCE(d.dbdsn, ''), COALESCE(d.dbhost, ''), COALESCE(d.dbport, ''), COALESCE(d.dbname, ''), COALESCE(d.dbuser, ''), COALESCE(d.dbpass, ''), COALESCE(d.dbconn, '')
		FROM bucardo.db d JOIN bucardo.dbmap m ON m.db = d.name
		WHERE m.dbgroup = $1 AND m.role = 'target' AND d.status = 'active'
		ORDER BY m.priority DESC, d.name`, targetGroup)
	if err != nil {
		return topology.Topology{}, fmt.Errorf("load target databases: %w", err)
	}
	defer targetRows.Close()
	for targetRows.Next() {
		value, err := scanDatabase(targetRows)
		if err != nil {
			return topology.Topology{}, fmt.Errorf("read target database: %w", err)
		}
		result.Targets = append(result.Targets, value)
	}
	if err := targetRows.Err(); err != nil {
		return topology.Topology{}, fmt.Errorf("iterate target databases: %w", err)
	}
	if len(result.Targets) == 0 {
		return topology.Topology{}, fmt.Errorf("sync %q has no active target database", name)
	}

	sourceRows, err := s.pool.Query(ctx, `
		SELECT DISTINCT d.name, COALESCE(d.dbdsn, ''), COALESCE(d.dbhost, ''), COALESCE(d.dbport, ''), COALESCE(d.dbname, ''), COALESCE(d.dbuser, ''), COALESCE(d.dbpass, ''), COALESCE(d.dbconn, '')
		FROM bucardo.db d JOIN bucardo.goat g ON g.db = d.name JOIN bucardo.herdmap hm ON hm.goat = g.id
		WHERE hm.herd = $1 AND d.status = 'active' ORDER BY d.name`, herdName)
	if err != nil {
		return topology.Topology{}, fmt.Errorf("load source databases: %w", err)
	}
	defer sourceRows.Close()
	for sourceRows.Next() {
		value, err := scanDatabase(sourceRows)
		if err != nil {
			return topology.Topology{}, fmt.Errorf("read source database: %w", err)
		}
		result.Sources = append(result.Sources, value)
	}
	if err := sourceRows.Err(); err != nil {
		return topology.Topology{}, fmt.Errorf("iterate source databases: %w", err)
	}
	if len(result.Sources) == 0 {
		return topology.Topology{}, fmt.Errorf("sync %q has no active source database", name)
	}

	tableRows, err := s.pool.Query(ctx, `
		SELECT g.id, g.db, g.schemaname, g.tablename, g.reltype, COALESCE(g.pkey, '')
		FROM bucardo.goat g JOIN bucardo.herdmap hm ON hm.goat = g.id
		WHERE hm.herd = $1 AND g.ghost = false ORDER BY g.id`, herdName)
	if err != nil {
		return topology.Topology{}, fmt.Errorf("load sync tables: %w", err)
	}
	defer tableRows.Close()
	for tableRows.Next() {
		var id int
		var db, schema, relationName, relationType, pkey string
		if err := tableRows.Scan(&id, &db, &schema, &relationName, &relationType, &pkey); err != nil {
			return topology.Topology{}, fmt.Errorf("read sync table: %w", err)
		}
		relation, err := table.New(id, db, schema, relationName, table.RelationType(relationType), splitPrimaryKey(pkey))
		if err != nil {
			return topology.Topology{}, err
		}
		result.Tables = append(result.Tables, relation)
	}
	if err := tableRows.Err(); err != nil {
		return topology.Topology{}, fmt.Errorf("iterate sync tables: %w", err)
	}
	if len(result.Tables) == 0 {
		return topology.Topology{}, fmt.Errorf("sync %q has no tables", name)
	}
	return result, nil
}

func scanDatabase(rows pgx.Rows) (database.Database, error) {
	var name, dsn, host, port, dbName, user, pass, conn string
	if err := rows.Scan(&name, &dsn, &host, &port, &dbName, &user, &pass, &conn); err != nil {
		return database.Database{}, err
	}
	connection := buildDSN(dsn, host, port, dbName, user, pass, conn)
	return database.Database{Name: name, Type: database.TypePostgreSQL, Connection: connection, DSN: connection, Status: database.StatusActive, MakeDelta: true}, nil
}

func (s *Store) TryLockSync(ctx context.Context, syncName string) (*SyncLock, error) {
	conn, err := pgx.ConnectConfig(ctx, s.pool.Config().ConnConfig.Copy())
	if err != nil {
		return nil, fmt.Errorf("connect for sync lock: %w", err)
	}
	key1, key2 := syncLockKeys(syncName)
	var acquired bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1, $2)`, key1, key2).Scan(&acquired); err != nil {
		_ = conn.Close(context.Background())
		return nil, fmt.Errorf("acquire sync lock %q: %w", syncName, err)
	}
	if !acquired {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("sync %q is already running", syncName)
	}
	return &SyncLock{conn: conn}, nil
}

func (l *SyncLock) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return l.conn.Close(ctx)
}

func syncLockKeys(syncName string) (int32, int32) {
	hash := sha256.Sum256([]byte("bucardo-g/sync/" + syncName))
	return int32(binary.BigEndian.Uint32(hash[:4])), int32(binary.BigEndian.Uint32(hash[4:8]))
}

func (s *Store) RecordRun(ctx context.Context, run job.Job, details string) error {
	status := string(run.Status)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO bucardo.syncrun
			(sync, inserts, deletes, started, ended, lastgood, lastbad, lastempty, details, status)
		VALUES ($1, $2, $3, $4, $5, $6 = 'good', $6 = 'bad', $6 = 'empty', $7, $6)`,
		run.Sync, run.Inserts, run.Deletes, run.StartedAt, run.EndedAt, status, details)
	if err != nil {
		return fmt.Errorf("record sync run: %w", err)
	}
	return nil
}

func buildDSN(dsn, host, port, databaseName, user, pass, conn string) string {
	if dsn != "" {
		return dsn + conn
	}
	u := &url.URL{Scheme: "postgres", Host: host, Path: "/" + databaseName}
	q := u.Query()
	if port != "" {
		q.Set("port", port)
	}
	u.RawQuery = q.Encode()
	u.User = url.UserPassword(user, pass)
	return u.String() + conn
}

func splitPrimaryKey(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, "|")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}
