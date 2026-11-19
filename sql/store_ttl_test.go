package sql_test

import (
	"database/sql"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/schema"
	crewsql "crew-go/sql"
	"crew-go/store"
	"crew-go/store/conformance"
)

func TestSQLiteTTLStoreConformance(t *testing.T) {
	conformance.RunStoreTests(t, func() store.Store {
		st := newTTLTestStore(t, newSQLiteDB(t), crewsql.SQLite{}, crewsql.DefaultStoreTable)
		if err := st.Setup(t.Context()); err != nil {
			t.Fatal(err)
		}
		return st
	})
}

func TestSQLiteTTLStoreReadsMigratesAndSweeps(t *testing.T) {
	checkSQLStoreTTL(t, newSQLiteDB(t), &crewsql.SQLite{}, "crew_store_ttl_test", "crew_history_ttl_test")
}

func TestSQLiteTTLStoreRejectsIncompatibleExpiryColumn(t *testing.T) {
	for _, declaration := range []string{
		"BIGINT NOT NULL DEFAULT 0",
		"TEXT NULL",
		"BIGINT NULL DEFAULT 0",
		"BIGINT GENERATED ALWAYS AS (NULL) VIRTUAL",
	} {
		t.Run(declaration, func(t *testing.T) {
			db := newSQLiteDB(t)
			base := crewsql.NewStore(db, crewsql.SQLite{})
			if err := base.Setup(t.Context()); err != nil {
				t.Fatal(err)
			}
			ttlExec(t, db, "ALTER TABLE "+crewsql.DefaultStoreTable+" ADD COLUMN expires_at_ms "+declaration)
			st := newTTLTestStore(t, db, crewsql.SQLite{}, crewsql.DefaultStoreTable)
			if err := st.Setup(t.Context()); err == nil {
				t.Fatal("Setup 应拒绝会改变过期语义的既有列")
			}
		})
	}
}

func TestSQLiteTTLStoreReportsIndexCollisionAndAllowsRetry(t *testing.T) {
	db := newSQLiteDB(t)
	ctx := t.Context()
	ns := store.NS("migration")
	base := crewsql.NewStore(db, crewsql.SQLite{})
	if err := base.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := base.Put(ctx, ns, "key", "迁移前"); err != nil {
		t.Fatal(err)
	}
	ttlExec(t, db, "CREATE TABLE other_store (expires_at_ms BIGINT)")
	ttlExec(t, db, "CREATE INDEX crew_store_ttl_idx ON other_store (expires_at_ms)")
	st := newTTLTestStore(t, db, crewsql.SQLite{}, crewsql.DefaultStoreTable)
	if err := st.Setup(ctx); err == nil {
		t.Fatal("另一张表的同名索引不能满足本 Store 的迁移")
	}

	ttlExec(t, db, "DROP INDEX crew_store_ttl_idx")
	for range 2 {
		if err := st.Setup(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var indexed int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pragma_index_list('crew_store')
		WHERE name = 'crew_store_ttl_idx'`).Scan(&indexed); err != nil || indexed != 1 {
		t.Fatalf("重试应在正确表上建立过期索引，得到 %d，err=%v", indexed, err)
	}
	if got := requireTTLItem(t, st, ns, "key"); got.Value != "迁移前" {
		t.Fatalf("部分迁移与重试期间应保留旧值，得到 %+v", got)
	}
}

func TestSQLTTLStoreChecksConstructorArguments(t *testing.T) {
	var absent *crewsql.Postgres
	type custom struct{ crewsql.SQLite }
	db := newSQLiteDB(t)
	for _, dialect := range []crewsql.Dialect{nil, absent, custom{}} {
		if st, err := crewsql.NewTTLStore(db, dialect); st != nil || err == nil {
			t.Errorf("无时钟契约的方言 %T 应拒绝，得到 %v，err=%v", dialect, st, err)
		}
	}
	if st, err := crewsql.NewTTLStore(nil, crewsql.SQLite{}); st != nil || err == nil {
		t.Errorf("缺少数据库连接池应拒绝，得到 %v，err=%v", st, err)
	}
}

func newTTLTestStore(t *testing.T, db *sql.DB, dialect crewsql.Dialect, table string) *crewsql.TTLStore {
	t.Helper()
	st, err := crewsql.NewTTLStore(db, dialect, crewsql.WithStoreTable(table))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// checkSQLStoreTTL 用真实 Store、HistoryStore 和引擎验证同一业务路径。直接调整存档期限使分页与清扫断言可重复。
func checkSQLStoreTTL(t *testing.T, db *sql.DB, dialect crewsql.Dialect, table, historyTable string) {
	t.Helper()
	ctx := t.Context()
	// TTL 构造器接受内置方言指针。创建迁移前的 SQLite 基线时用普通 Store 支持的值形式。
	legacyDialect := dialect
	if sqlite, ok := dialect.(*crewsql.SQLite); ok {
		legacyDialect = *sqlite
	}
	legacy := crewsql.NewStore(db, legacyDialect, crewsql.WithStoreTable(table))
	if err := legacy.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	root := store.NS("tenant")
	oldNS := store.NS("tenant", "b", "legacy")
	deadNS := store.NS("tenant", "a")
	mixedNS := store.NS("tenant", "b", "deep")
	otherNS := store.NS("tenant", "c")

	oldValue := map[string]string{"tag": "match", "description": "迁移前写入"}
	if err := legacy.Put(ctx, oldNS, "old", oldValue); err != nil {
		t.Fatal(err)
	}
	original := requireTTLItem(t, legacy, oldNS, "old")

	historyStore := crewsql.NewHistoryStore(db, legacyDialect, crewsql.WithHistoryTable(historyTable))
	if err := historyStore.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	historyKey := agent_v2.HistoryKey{Owner: "tenant", ConvID: deadNS.String()}
	msg := schema.AssistantMessage("旧会话说明")
	if err := historyStore.Append(ctx, historyKey, []*schema.Message{msg}); err != nil {
		t.Fatal(err)
	}

	st := newTTLTestStore(t, db, dialect, table)
	for range 2 {
		if err := st.Setup(ctx); err != nil {
			t.Fatal(err)
		}
	}
	migrated := requireTTLItem(t, st, oldNS, "old")
	if migrated.Value.(map[string]any)["description"] != "迁移前写入" || !migrated.CreatedAt.Equal(original.CreatedAt) ||
		!migrated.UpdatedAt.Equal(original.UpdatedAt) {
		t.Fatalf("迁移应保留旧值和时间，得到 %+v", migrated)
	}

	value := map[string]string{"tag": "match"}
	const lifetime = time.Hour
	before, precision := readTTLClock(t, db, legacyDialect)
	for _, ns := range []store.Namespace{deadNS, mixedNS, otherNS, store.NS("tenant2", "outside")} {
		if err := st.Put(ctx, ns, "key", value, store.WithTTL(lifetime)); err != nil {
			t.Fatal(err)
		}
	}
	after, _ := readTTLClock(t, db, legacyDialect)
	ph := dialect.Placeholder
	readExpiry := func(ns store.Namespace, key string) sql.NullInt64 {
		t.Helper()
		var expires sql.NullInt64
		q := "SELECT expires_at_ms FROM " + table + " WHERE ns = " + ph(1) + " AND data_key = " + ph(2)
		if err := db.QueryRowContext(ctx, q, ns.String(), key).Scan(&expires); err != nil {
			t.Fatal(err)
		}
		return expires
	}
	expiry := readExpiry(mixedNS, "key")
	if !expiry.Valid || expiry.Int64 < before+lifetime.Milliseconds() ||
		expiry.Int64 > after+lifetime.Milliseconds()+precision {
		t.Fatalf("应按数据库时钟计算截止毫秒，数据库窗口 %d–%d，写入 %+v", before, after, expiry)
	}
	if expires := readExpiry(oldNS, "old"); expires.Valid {
		t.Fatalf("迁移前的数据应保持永久，得到 %+v", expires)
	}
	live := requireTTLItem(t, st, mixedNS, "key")
	if err := st.Put(ctx, mixedNS, "key", value); err != nil {
		t.Fatal(err)
	}
	if got := requireTTLItem(t, st, mixedNS, "key"); !got.CreatedAt.Equal(live.CreatedAt) || readExpiry(mixedNS, "key").Valid {
		t.Fatalf("普通覆盖应清除 TTL 并保留当前生命周期的创建时间，得到 %+v", got)
	}
	if err := st.Put(ctx, mixedNS, "short", value, store.WithTTL(lifetime)); err != nil {
		t.Fatal(err)
	}
	prior := requireTTLItem(t, st, deadNS, "key")
	// 过期值排到最新。错误地在后端分页之后过滤会少返当页有效项。
	for _, target := range []struct {
		ns  store.Namespace
		key string
	}{{deadNS, "key"}, {mixedNS, "short"}} {
		q := "UPDATE " + table + " SET expires_at_ms = -1, updated_at = '2030-01-01 00:00:00'" +
			" WHERE ns = " + ph(1) + " AND data_key = " + ph(2)
		ttlExec(t, db, q, target.ns.String(), target.key)
	}
	if got, err := st.Get(ctx, deadNS, "key"); err != nil || got != nil {
		t.Fatalf("过期条目应视同缺失，得到 %+v，err=%v", got, err)
	}
	// 行仍在，直到显式扫除或重写。
	if expiry := readExpiry(deadNS, "key"); !expiry.Valid || expiry.Int64 != -1 {
		t.Fatalf("读取不应物理删除过期行，得到 %+v", expiry)
	}

	items, err := st.Search(ctx, root, store.WithFilter(map[string]any{"tag": "match"}), store.WithLimit(2))
	if err != nil || len(items) != 2 {
		t.Fatalf("过期过滤应先于搜索分页，得到 %v，err=%v", items, err)
	}
	for _, item := range items {
		if item.Key != "key" || (item.Namespace.String() != mixedNS.String() && item.Namespace.String() != otherNS.String()) {
			t.Fatalf("搜索应只返回作用域内较新的存活条目，得到 %+v", item)
		}
	}

	flat, err := st.ListNamespaces(ctx, store.WithListOffset(1), store.WithListLimit(1))
	if err != nil || len(flat) != 1 || flat[0].String() != oldNS.String() {
		t.Fatalf("展开枚举应在分页前隐藏只含过期项的命名空间，得到 %v，err=%v", flat, err)
	}
	deep, err := st.ListNamespaces(ctx, store.WithListPrefix(root), store.WithListMaxDepth(2), store.WithListOffset(1), store.WithListLimit(1))
	if err != nil || len(deep) != 1 || deep[0].String() != otherNS.String() {
		t.Fatalf("深度截断、去重和分页应以存活条目为输入，得到 %v，err=%v", deep, err)
	}
	results, err := st.Batch(ctx, []store.Op{
		store.GetOp{Namespace: deadNS, Key: "key"},
		store.ListNamespacesOp{Prefix: root, MaxDepth: 2},
		store.PutOp{Namespace: deadNS, Key: "key", Value: "新生命周期", TTL: lifetime},
	})
	if err != nil || len(results) != 3 || results[0].Err != nil || results[0].Item != nil ||
		results[1].Err != nil || len(results[1].Namespaces) != 2 || results[2].Err != nil {
		t.Fatalf("批量入口应复用同一过期契约，得到 %+v，err=%v", results, err)
	}
	recreated := requireTTLItem(t, st, deadNS, "key")
	if recreated.Value != "新生命周期" || !recreated.CreatedAt.After(prior.CreatedAt) || !readExpiry(deadNS, "key").Valid {
		t.Fatalf("重写过期键应开新生命周期，得到 %+v", recreated)
	}

	if n, err := st.SweepExpired(ctx); err != nil || n != 1 {
		t.Fatalf("清扫应只物理移除仍过期的行，得到 %d，err=%v", n, err)
	}
	if n, err := st.SweepExpired(ctx); err != nil || n != 0 {
		t.Fatalf("重试清扫应幂等，得到 %d，err=%v", n, err)
	}
	requireTTLItem(t, st, oldNS, "old")
	requireTTLItem(t, st, deadNS, "key")
	history, err := agent_v2.LoadAllHistory(ctx, historyStore, historyKey)
	if err != nil || len(history) != 1 || history[0].ID != msg.ID || history[0].Content != msg.Content {
		t.Fatalf("Store 清扫应保留独立历史表，得到 %+v，err=%v", history, err)
	}
}

func readTTLClock(t *testing.T, db *sql.DB, dialect crewsql.Dialect) (now, uncertainty int64) {
	t.Helper()
	query := "SELECT CAST(round((julianday('now') - 2440587.5) * 86400000) AS INTEGER)"
	switch dialect.(type) {
	case crewsql.Postgres:
		query = "SELECT floor(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint"
	case crewsql.MySQL:
		query = "SELECT UNIX_TIMESTAMP() * 1000"
		uncertainty = 999 // 无参数 UNIX_TIMESTAMP 的精度为秒。
	}
	if err := db.QueryRowContext(t.Context(), query).Scan(&now); err != nil {
		t.Fatal(err)
	}
	return now, uncertainty
}

func requireTTLItem(t *testing.T, st store.Store, ns store.Namespace, key string) *store.Item {
	t.Helper()
	item, err := st.Get(t.Context(), ns, key)
	if err != nil || item == nil {
		t.Fatalf("应读到 %s/%s，得到 %+v，err=%v", ns.String(), key, item, err)
	}
	return item
}

func ttlExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), query, args...); err != nil {
		t.Fatal(err)
	}
}
