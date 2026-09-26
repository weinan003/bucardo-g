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

	"github.com/bucardo-g/internal/domain/table"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Database struct {
	Name string
	DSN  string
}

type Sync struct {
	Name       string
	Source     Database
	Target     Database
	TargetName string
	Tables     []table.Table
}

type Store struct{ pool *pgxpool.Pool }

type SyncLock struct{ conn *pgx.Conn }

func Open(ctx context.Context, dsn string) (*Store, error) {
	logger := slog.Default()
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
	logger.Debug("control database ready")
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) TryLockSync(ctx context.Context, syncName string) (*SyncLock, error) {
	logger := slog.Default()
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
		logger.Warn("sync lock is already held", "sync", syncName)
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("sync %q is already running", syncName)
	}
	logger.Debug("sync lock acquired", "sync", syncName)
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

func (s *Store) RecordRun(ctx context.Context, syncName, status, details string, inserts, deletes int64, started time.Time) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO bucardo.syncrun
			(sync, inserts, deletes, started, ended, lastgood, lastbad, lastempty, details, status)
		VALUES ($1, $2, $3, $4, now(), $5 = 'good', $5 = 'bad', $5 = 'empty', $6, $5)`,
		syncName, inserts, deletes, started, status, details)
	if err != nil {
		return fmt.Errorf("record sync run: %w", err)
	}
	return nil
}

func (s *Store) LoadSync(ctx context.Context, name string) (Sync, error) {
	var result Sync
	var sourceName, targetGroup string
	err := s.pool.QueryRow(ctx, `
		SELECT name, herd, dbs
		FROM bucardo.sync
		WHERE name = $1 AND status = 'active'`, name,
	).Scan(&result.Name, &sourceName, &targetGroup)
	if err != nil {
		return Sync{}, fmt.Errorf("load sync %q: %w", name, err)
	}

	rows, err := s.pool.Query(ctx, `
			SELECT d.name, COALESCE(d.dbdsn, ''), COALESCE(d.dbhost, ''), COALESCE(d.dbport, ''), COALESCE(d.dbname, ''), COALESCE(d.dbuser, ''), COALESCE(d.dbpass, ''), COALESCE(d.dbconn, '')
		FROM bucardo.db d
		JOIN bucardo.dbmap m ON m.db = d.name
		WHERE m.dbgroup = $1 AND m.role = 'target' AND d.status = 'active'
		ORDER BY m.priority DESC, d.name`, targetGroup)
	if err != nil {
		return Sync{}, fmt.Errorf("load target databases: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return Sync{}, fmt.Errorf("sync %q has no active target database", name)
	}
	var dbName, dbDSN, host, port, dbNameValue, user, pass, conn string
	if err := rows.Scan(&dbName, &dbDSN, &host, &port, &dbNameValue, &user, &pass, &conn); err != nil {
		return Sync{}, fmt.Errorf("read target database: %w", err)
	}
	result.TargetName, result.Target = "dbgroup "+targetGroup, Database{Name: dbName, DSN: buildDSN(dbDSN, host, port, dbNameValue, user, pass, conn)}

	var sourceDB Database
	err = s.pool.QueryRow(ctx, `
				SELECT d.name, COALESCE(d.dbdsn, ''), COALESCE(d.dbhost, ''), COALESCE(d.dbport, ''), COALESCE(d.dbname, ''), COALESCE(d.dbuser, ''), COALESCE(d.dbpass, ''), COALESCE(d.dbconn, '')
		FROM bucardo.db d
		WHERE d.name = (
			SELECT g.db
			FROM bucardo.goat g
			JOIN bucardo.herdmap hm ON hm.goat = g.id
			WHERE hm.herd = $1
			ORDER BY hm.priority DESC, g.id
			LIMIT 1
		) AND d.status = 'active'
		LIMIT 1`, sourceName,
	).Scan(&dbName, &dbDSN, &host, &port, &dbNameValue, &user, &pass, &conn)
	if err != nil {
		return Sync{}, fmt.Errorf("load source database: %w", err)
	}
	sourceDB = Database{Name: dbName, DSN: buildDSN(dbDSN, host, port, dbNameValue, user, pass, conn)}
	result.Source = sourceDB

	tableRows, err := s.pool.Query(ctx, `
		SELECT g.id, g.db, g.schemaname, g.tablename, g.reltype, COALESCE(g.pkey, '')
		FROM bucardo.goat g
		JOIN bucardo.herdmap hm ON hm.goat = g.id
		WHERE hm.herd = $1 AND g.db = $2 AND g.ghost = false
		ORDER BY g.id`, sourceName, sourceDB.Name)
	if err != nil {
		return Sync{}, fmt.Errorf("load sync tables: %w", err)
	}
	defer tableRows.Close()
	for tableRows.Next() {
		var id int
		var db, schema, relationName, relationType, pkey string
		if err := tableRows.Scan(&id, &db, &schema, &relationName, &relationType, &pkey); err != nil {
			return Sync{}, fmt.Errorf("read sync table: %w", err)
		}
		keys := splitPrimaryKey(pkey)
		t, err := table.New(id, db, schema, relationName, table.RelationType(relationType), keys)
		if err != nil {
			return Sync{}, err
		}
		result.Tables = append(result.Tables, t)
	}
	if err := tableRows.Err(); err != nil {
		return Sync{}, fmt.Errorf("iterate sync tables: %w", err)
	}
	if len(result.Tables) == 0 {
		return Sync{}, fmt.Errorf("sync %q has no tables", name)
	}
	return result, nil
}

func buildDSN(dsn, host, port, database, user, pass, conn string) string {
	if dsn != "" {
		return dsn + conn
	}
	u := &url.URL{Scheme: "postgres", Host: host, Path: "/" + database}
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
