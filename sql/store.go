package sql

import (
	"context"
	dbsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"crew-go/store"
)

// DefaultStoreTable 是 Store 默认表名. 业务想跟自己 schema 隔离时 `WithStoreTable`
// option 改名.
const DefaultStoreTable = "crew_store"

// Store 是 store.Store 接口的 SQL 共享实现. 通过 Dialect 注入支持 PG/MySQL.
//
// **业务接入**:
//
//	import (
//	    _ "github.com/jackc/pgx/v5/stdlib"
//	    sqlstd "database/sql"
//	    agentSQL "crew-go/sql"
//	)
//
//	db, _ := sqlstd.Open("pgx", "postgres://...")
//	st := agentSQL.NewStore(db, agentSQL.Postgres{})
//	st.Setup(ctx) // 创建表 + 索引 (PG/MySQL 自动选)
//
// **业务自管**: driver / 连接池 / DSN. crew-go/sql 不 close db, 业务负责.
//
// **conformance 验证**: 业务 `_test.go` 调
// `store/conformance.RunStoreTests(t, factory)` 验语义合规.
type Store struct {
	db      *dbsql.DB
	dialect Dialect
	table   string
}

// StoreOption 配置 Store 行为.
type StoreOption func(*Store)

// WithStoreTable 自定义 Store 表名 (默认 `crew_store`).
func WithStoreTable(name string) StoreOption {
	return func(s *Store) { s.table = name }
}

// NewStore 创建 SQL Store. db 业务自管 lifecycle (crew-go/sql 不 close).
//
// **panic on invalid table name** (audit iter-4 视角 2 M3): WithStoreTable
// 传非法 identifier 时 NewStore panic — 业务 init 时 fail-fast, 防 SQL injection.
func NewStore(db *dbsql.DB, dialect Dialect, opts ...StoreOption) *Store {
	s := &Store{
		db:      db,
		dialect: dialect,
		table:   DefaultStoreTable,
	}
	for _, opt := range opts {
		opt(s)
	}
	if err := validateTableName(s.table); err != nil {
		panic("crew-go/sql NewStore: " + err.Error())
	}
	return s
}

// ── store.Store interface 实现 ──

// Get 实现 store.Store.Get. 不存在返 (nil, nil) — 跟 InMemoryStore 协议一致.
func (s *Store) Get(ctx context.Context, namespace store.Namespace, key string) (*store.Item, error) {
	if err := namespace.Validate(); err != nil {
		return nil, err
	}

	ph := s.dialect.Placeholder
	q := fmt.Sprintf(
		`SELECT value, created_at, updated_at FROM %s WHERE ns = %s AND data_key = %s`,
		s.table, ph(1), ph(2),
	)

	var (
		valueJSON            []byte
		createdAt, updatedAt time.Time
	)
	err := s.db.QueryRowContext(ctx, q, joinNS(namespace), key).Scan(&valueJSON, &createdAt, &updatedAt)
	if errors.Is(err, dbsql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sql store get: %w", err)
	}

	value, err := unmarshalValue(valueJSON)
	if err != nil {
		return nil, fmt.Errorf("sql store get: %w", err)
	}

	return &store.Item{
		Namespace: namespace, // 业务传入的 namespace 不重 split (避免 PG 反序列化 ns 字符串歧义)
		Key:       key,
		Value:     value,
		CreatedAt: createdAt.UTC(), // audit iter-3 H2: 强制 UTC 防业务 PG TIMESTAMP 无 TZ 错时区
		UpdatedAt: updatedAt.UTC(),
	}, nil
}

// Put 实现 store.Store.Put. value=nil 等价 Delete.
func (s *Store) Put(ctx context.Context, namespace store.Namespace, key string, value any, opts ...store.PutOption) error {
	if err := namespace.Validate(); err != nil {
		return err
	}

	// value=nil → Delete
	if value == nil {
		return s.Delete(ctx, namespace, key)
	}

	// PoC: SQL backend 暂不支持 TTL (Phase 2 加 expires_at 列后再开启).
	// 业务调 WithTTL 时返 ErrTTLNotSupported (跟 InMemoryStore 一致).
	cfg := store.ParsePutOptions(opts...)
	if cfg.TTL > 0 {
		return store.ErrTTLNotSupported
	}

	// 落盘的是值的 JSON 形状（store.NormalizeValue）再编码一遍：编码 map 时键已排序，SQLite 按 JSON 文本比的
	// 整对象相等因此不受 struct 字段声明序影响（PG / MySQL 原生无序相等，本来就不受）
	norm, err := store.NormalizeValue(value)
	if err != nil {
		return fmt.Errorf("sql store put: marshal value: %w", err)
	}
	valueJSON, err := json.Marshal(norm)
	if err != nil {
		return fmt.Errorf("sql store put: marshal value: %w", err)
	}

	now := time.Now().UTC()
	ph := s.dialect.Placeholder
	upsert := s.dialect.UpsertSuffix(
		[]string{"ns", "data_key"},
		[]string{"value", "updated_at"},
	)

	q := fmt.Sprintf(
		`INSERT INTO %s (ns, data_key, value, created_at, updated_at) VALUES (%s, %s, %s, %s, %s) %s`,
		s.table, ph(1), ph(2), ph(3), ph(4), ph(5), upsert,
	)

	if _, err := s.db.ExecContext(ctx, q, joinNS(namespace), key, bindJSON(valueJSON), now, now); err != nil {
		return fmt.Errorf("sql store put: %w", err)
	}
	return nil
}

// Delete 实现 store.Store.Delete. 不存在 key 不报错 (幂等).
func (s *Store) Delete(ctx context.Context, namespace store.Namespace, key string) error {
	if err := namespace.Validate(); err != nil {
		return err
	}

	ph := s.dialect.Placeholder
	q := fmt.Sprintf(
		`DELETE FROM %s WHERE ns = %s AND data_key = %s`,
		s.table, ph(1), ph(2),
	)
	if _, err := s.db.ExecContext(ctx, q, joinNS(namespace), key); err != nil {
		return fmt.Errorf("sql store delete: %w", err)
	}
	return nil
}

// **audit iter-4 视角 1 H2 修**: 不实现 `store.TTLAware` 接口 —
// `store/conformance/ttl.go:22-25` contract 明示 "实现了 TTLAware 必须返 true,
// false 没意义". sql.Store PoC 不支持 TTL → **不实现接口**, 业务 type-assert
// `(store.TTLAware)` 返 (_, false) → conformance TTL 子测试走"不支持"路径 skip.
// Phase 2 加 expires_at 列时再加 SupportsTTL() bool { return true } 方法.

// 编译期 interface 实现检查.
var _ store.Store = (*Store)(nil)

// **不实现 store.TTLAware** — audit iter-4 视角 1 H2 修 (见上 SupportsTTL godoc).
