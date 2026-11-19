package sql

import (
	"context"
	"crypto/sha256"
	dbsql "database/sql"
	"errors"
	"fmt"
	"strings"
)

func (s *Store) setupTTL(ctx context.Context, createTable string) error {
	if err := s.ensureTTLStoreTable(ctx, createTable); err != nil {
		return err
	}
	exists, err := s.checkTTLColumn(ctx)
	if err != nil {
		return err
	}
	if !exists {
		_, err := s.db.ExecContext(ctx, "ALTER TABLE "+s.table+" ADD COLUMN expires_at_ms BIGINT NULL")
		if err != nil {
			// 同时迁移的进程可能已建好列，按实际结构确认结果。
			exists, inspectErr := s.checkTTLColumn(ctx)
			if !exists || inspectErr != nil {
				return fmt.Errorf("sql store setup expiry column: %w", errors.Join(err, inspectErr))
			}
		}
	}
	if err := s.ensureTTLStoreIndex(ctx, "_ns_idx", "ns"); err != nil {
		return err
	}
	return s.ensureTTLStoreIndex(ctx, "_ttl_idx", "expires_at_ms")
}

func (s *Store) ensureTTLStoreTable(ctx context.Context, createTable string) error {
	_, postgres := s.dialect.(Postgres)
	if postgres {
		// CREATE TABLE 选 search_path 的首个 schema。先确认普通 Store 实际读写的表，
		// 避免在靠前的 schema 创建同名空表，遮住后面已有的表。
		exists, err := s.postgresStoreTableExists(ctx)
		if err != nil || exists {
			return err
		}
	}
	if _, err := s.db.ExecContext(ctx, createTable); err != nil {
		if postgres {
			// 并发的 CREATE TABLE IF NOT EXISTS 也可能收到系统目录的唯一性错误。
			if exists, readErr := s.postgresStoreTableExists(ctx); readErr == nil && exists {
				return nil
			}
		}
		return fmt.Errorf("sql store setup ttl table: %w", err)
	}
	return nil
}

func (s *Store) postgresStoreTableExists(ctx context.Context) (bool, error) {
	var exists bool
	const query = `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_class
		WHERE oid = pg_catalog.to_regclass($1) AND relkind IN ('r', 'p'))`
	if err := s.db.QueryRowContext(ctx, query, s.table).Scan(&exists); err != nil {
		return false, fmt.Errorf("sql store inspect table: %w", err)
	}
	return exists, nil
}

func (s *Store) checkTTLColumn(ctx context.Context) (bool, error) {
	var kind string
	var nullable, writable bool
	var defaultValue dbsql.NullString
	var err error
	switch s.dialect.(type) {
	case Postgres:
		// 以实际解析到的表为准。转成 JSON 后也能查询尚无 attgenerated 的 PostgreSQL 版本。
		const query = `SELECT pg_catalog.format_type(a.atttypid,a.atttypmod), NOT a.attnotnull,
			pg_catalog.pg_get_expr(d.adbin,d.adrelid), COALESCE(pg_catalog.row_to_json(a)->>'attgenerated', '') = ''
			FROM pg_catalog.pg_attribute a
			LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
			WHERE a.attrelid = pg_catalog.to_regclass($1) AND a.attname = 'expires_at_ms' AND NOT a.attisdropped`
		err = s.db.QueryRowContext(ctx, query, s.table).Scan(&kind, &nullable, &defaultValue, &writable)
	case MySQL:
		var field, nullFlag, key, extra string
		query := fmt.Sprintf("SHOW COLUMNS FROM %s WHERE Field = 'expires_at_ms'", s.table)
		err = s.db.QueryRowContext(ctx, query).Scan(&field, &kind, &nullFlag, &key, &defaultValue, &extra)
		nullable = nullFlag == "YES"
		extra = strings.ToLower(extra)
		// DEFAULT_GENERATED 来自默认表达式，列本身仍然可写。
		writable = !strings.Contains(extra, "virtual generated") && !strings.Contains(extra, "stored generated")
		// 旧版本 SHOW COLUMNS 会带 BIGINT 的显示宽度，存储类型相同。
		if strings.HasPrefix(kind, "bigint(") && strings.HasSuffix(kind, ")") {
			kind = "bigint"
		}
	case SQLite:
		const query = `SELECT type, "notnull" = 0, dflt_value, hidden = 0
			FROM pragma_table_xinfo(?) WHERE lower(name) = 'expires_at_ms'`
		err = s.db.QueryRowContext(ctx, query, s.table).Scan(&kind, &nullable, &defaultValue, &writable)
	}
	if errors.Is(err, dbsql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("sql store inspect expiry column: %w", err)
	}
	def := strings.ToLower(strings.TrimSpace(defaultValue.String))
	if !strings.EqualFold(kind, "bigint") || !nullable || !writable ||
		(defaultValue.Valid && def != "null" && def != "null::bigint") {
		return false, errors.New("sql store expires_at_ms must be a writable nullable bigint with a null default")
	}
	return true, nil
}

func (s *Store) ensureTTLStoreIndex(ctx context.Context, suffix, column string) error {
	name := s.table + suffix
	if len(name) > 63 {
		// 避免长表名在 PostgreSQL 截断后让两个索引重名，也适配 MySQL 的长度上限。
		digest := sha256.Sum256([]byte(strings.ToLower(s.table)))
		name = fmt.Sprintf("crew_store_%x%s", digest[:8], suffix)
	}
	exists, err := s.ttlIndexExists(ctx, name, column)
	if err != nil || exists {
		return err
	}
	ddl := fmt.Sprintf("CREATE INDEX %s ON %s (%s)", name, s.table, column)
	_, migrationErr := s.db.ExecContext(ctx, ddl)
	exists, err = s.ttlIndexExists(ctx, name, column)
	if err == nil && exists {
		return nil
	}
	return fmt.Errorf("sql store setup index %s on %s: %w", name, s.table,
		errors.Join(migrationErr, err, errors.New("required index is unavailable")))
}

func (s *Store) ttlIndexExists(ctx context.Context, name, column string) (bool, error) {
	var exists bool
	var err error
	switch s.dialect.(type) {
	case Postgres:
		const query = `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_index i
			JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
			JOIN pg_catalog.pg_am am ON am.oid = c.relam
			JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
			WHERE i.indrelid = pg_catalog.to_regclass($1) AND c.relname = $2 AND a.attname = $3
			AND i.indisvalid AND i.indisready AND i.indpred IS NULL AND am.amname = 'btree')`
		err = s.db.QueryRowContext(ctx, query, s.table, strings.ToLower(name), column).Scan(&exists)
	case SQLite:
		const query = `SELECT EXISTS(SELECT 1 FROM pragma_index_list(?) l JOIN pragma_index_info(l.name) i
			WHERE l.name = ? COLLATE NOCASE AND l.partial = 0 AND i.seqno = 0 AND i.name = ? COLLATE NOCASE)`
		err = s.db.QueryRowContext(ctx, query, s.table, name, column).Scan(&exists)
	case MySQL:
		query := fmt.Sprintf(`SHOW INDEX FROM %s WHERE Key_name = '%s' AND Seq_in_index = 1
			AND Column_name = '%s' AND Sub_part IS NULL AND Index_type = 'BTREE'`, s.table, name, column)
		var rows *dbsql.Rows
		rows, err = s.db.QueryContext(ctx, query)
		if err == nil {
			defer rows.Close()
			exists = rows.Next()
			err = rows.Err()
		}
	}
	if err != nil {
		return false, fmt.Errorf("sql store inspect index: %w", err)
	}
	return exists, nil
}
