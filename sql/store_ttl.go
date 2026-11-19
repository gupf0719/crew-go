package sql

import (
	"context"
	dbsql "database/sql"
	"errors"
	"fmt"

	"crew-go/store"
)

const (
	postgresStoreClock = "(floor(EXTRACT(EPOCH FROM statement_timestamp()) * 1000)::bigint)"
	mysqlStoreClock    = "(TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP(6)) DIV 1000)"
	sqliteStoreClock   = "(CAST(strftime('%s','now') AS INTEGER) * 1000 + CAST(substr(strftime('%f','now'),4,3) AS INTEGER))"
)

// sqlStoreBackend 让 TTLStore 复用 SQL Store 的操作，由构造器独占底层配置。
type sqlStoreBackend = Store

// TTLStore 是支持 [store.WithTTL] 的 SQL Store。首次启用前调用 Setup。
type TTLStore struct {
	*sqlStoreBackend
}

// NewTTLStore 用内置 PostgreSQL、MySQL 或 SQLite 方言构造支持 TTL 的 Store。
// 同一张启用 TTL 的表用该入口读写。正时长按毫秒向上取整，缺省或非正时长表示永久。
func NewTTLStore(db *dbsql.DB, dialect Dialect, opts ...StoreOption) (*TTLStore, error) {
	if db == nil {
		return nil, errors.New("sql store ttl requires a database")
	}
	switch d := dialect.(type) {
	case *Postgres:
		if d != nil {
			dialect = *d
		}
	case *MySQL:
		if d != nil {
			dialect = *d
		}
	case *SQLite:
		if d != nil {
			dialect = *d
		}
	}

	var clock string
	switch dialect.(type) {
	case Postgres:
		clock = postgresStoreClock
	case MySQL:
		clock = mysqlStoreClock
	case SQLite:
		clock = sqliteStoreClock
	default:
		return nil, errors.New("sql store ttl requires postgres, mysql or sqlite")
	}
	s := NewStore(db, dialect, opts...)
	s.ttlClock = clock
	return &TTLStore{sqlStoreBackend: s}, nil
}

// SupportsTTL 实现 [store.TTLAware]。
func (*TTLStore) SupportsTTL() bool { return true }

// SweepExpired 用一条 DELETE 清理本 Store 表中到期的条目，返回实际删除条数。
// 读取在清扫前就会隐藏过期数据。清扫周期由调用方决定。
func (s *TTLStore) SweepExpired(ctx context.Context) (int64, error) {
	q := "DELETE FROM " + s.table + " WHERE expires_at_ms <= " + s.ttlClock
	result, err := s.db.ExecContext(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("sql store sweep expired: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("sql store sweep expired rows: %w", err)
	}
	return n, nil
}

// withLiveRows 在各读取入口分组、排序和分页之前使用同一可见性谓词。
func (s *Store) withLiveRows(where string) string {
	if s.ttlClock == "" {
		return where
	}
	if where != "" {
		where += " AND "
	}
	return where + "(expires_at_ms IS NULL OR expires_at_ms > " + s.ttlClock + ")"
}

func (s *Store) ttlPutSQL() string {
	ph := s.dialect.Placeholder
	target := s.table + " AS stored"
	// 按写入锁保护的旧期限决定创建时间，与新的值和期限一起提交。
	upsert := "ON CONFLICT (ns, data_key) DO UPDATE SET " +
		"created_at=CASE WHEN stored.expires_at_ms <= " + s.ttlClock + " THEN excluded.created_at ELSE stored.created_at END, " +
		"value=excluded.value, updated_at=excluded.updated_at, expires_at_ms=excluded.expires_at_ms"
	if _, mysql := s.dialect.(MySQL); mysql {
		target = s.table
		// MySQL 按顺序求值，必须在更新 expires_at_ms 之前读取旧值。
		upsert = "ON DUPLICATE KEY UPDATE " +
			"created_at=CASE WHEN expires_at_ms <= " + s.ttlClock + " THEN VALUES(created_at) ELSE created_at END, " +
			"value=VALUES(value), updated_at=VALUES(updated_at), expires_at_ms=VALUES(expires_at_ms)"
	}
	return fmt.Sprintf(
		`INSERT INTO %s (ns, data_key, value, created_at, updated_at, expires_at_ms) VALUES (%s, %s, %s, %s, %s, %s + %s) %s`,
		target, ph(1), ph(2), ph(3), ph(4), ph(5), s.ttlClock, ph(6), upsert,
	)
}

var (
	_ store.Store    = (*TTLStore)(nil)
	_ store.TTLAware = (*TTLStore)(nil)
)
