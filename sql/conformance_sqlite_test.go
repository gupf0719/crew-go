package sql_test

// 让本模块的 SQL 在**真 SQL 引擎**上跑上游的 conformance 套件。
//
// 此前本模块只有 sqlmock——钉的是「发出去的 SQL 长什么样」而不是「跑出来对不对」。
// 代价实测过三次：ns 子树匹配先后漏掉「重访实例段 @」与「前缀里的 LIKE 元字符」，
// 以及 filter 的类型不匹配语义（字符串行被当成「大于 10」返回、`$eq` 直值 10 不命中
// 10.0）——都对 sqlmock 全程隐形，接上真引擎才变红。唯一的变量就是「backend 能不能真跑
// 起来」。
//
// PostgreSQL 和 MySQL 的方言与驱动行为由带 integration 标签的真库测试覆盖。

import (
	databasesql "database/sql"
	"encoding/json"
	"strings"
	"testing"

	_ "modernc.org/sqlite" // 纯 Go SQLite 驱动（无 cgo、无外部进程）

	"crew-go/compose_v2"
	"crew-go/compose_v2/checkpointer_conformance"
	agentSQL "crew-go/sql"
	"crew-go/store"
	"crew-go/store/conformance"
)

// newSQLiteDB 开一个隔离的库并建表。每个 factory 调用拿自己的库，避免用例互相看见。
func newSQLiteDB(t *testing.T) *databasesql.DB {
	t.Helper()
	db, err := agentSQL.OpenSQLiteForTest()
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestSQLiteCheckpointer_Conformance(t *testing.T) {
	checkpointer_conformance.RunCheckpointerTests(t, func() compose_v2.Checkpointer {
		db := newSQLiteDB(t)
		cp := agentSQL.NewCheckpointer(db, agentSQL.SQLite{})
		if err := cp.Setup(t.Context()); err != nil {
			t.Fatalf("cp Setup: %v", err)
		}
		return cp
	})
}

func TestSQLiteCheckpointer_TaskLeases(t *testing.T) {
	db := newSQLiteDB(t)
	cp := agentSQL.NewCheckpointer(db, agentSQL.SQLite{})
	if err := cp.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	// SQLite 将未加引号的表名按大小写无关的规则解析。
	peer := agentSQL.NewCheckpointer(db, agentSQL.SQLite{},
		agentSQL.WithCheckpointerTable(strings.ToUpper(agentSQL.DefaultCheckpointTable)))
	checkpointer_conformance.RunTaskLeaseTests(t, cp, peer, func(sid string) {
		if _, err := db.ExecContext(t.Context(), "UPDATE crew_checkpoint_task_lease SET expires_at = 0 WHERE sid = ?", sid); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSQLiteStore_Conformance(t *testing.T) {
	conformance.RunStoreTests(t, func() store.Store {
		db := newSQLiteDB(t)
		st := agentSQL.NewStore(db, agentSQL.SQLite{})
		if err := st.Setup(t.Context()); err != nil {
			t.Fatalf("store Setup: %v", err)
		}
		return st
	})
}

func TestSQLiteStore_DecimalFilterPrecision(t *testing.T) {
	st := agentSQL.NewStore(newSQLiteDB(t), agentSQL.SQLite{})
	if err := st.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertDecimalFilterPrecision(t, st, false)
}

func assertDecimalFilterPrecision(t *testing.T, st store.Store, exact bool) {
	t.Helper()
	ctx := t.Context()
	ns := store.NS("decimals")
	baseline, nearby := json.Number("0.1"), json.Number("0.100000000000000001")
	for key, value := range map[string]json.Number{"baseline": baseline, "nearby": nearby} {
		if err := st.Put(ctx, ns, key, map[string]any{"price": value}); err != nil {
			t.Fatal(err)
		}
	}
	wantEqual, wantGreater := 2, 0
	if exact {
		wantEqual, wantGreater = 1, 1
	}
	for op, want := range map[string]int{"$eq": wantEqual, "$gt": wantGreater} {
		items, err := st.Search(ctx, ns, store.WithFilter(map[string]any{"price": map[string]any{op: baseline}}))
		if err != nil || len(items) != want {
			t.Errorf("小数比较 %s 得到 %d 条，预期 %d，错误 %v", op, len(items), want, err)
			continue
		}
		if exact && ((op == "$eq" && items[0].Key != "baseline") || (op == "$gt" && items[0].Key != "nearby")) {
			t.Errorf("精确小数比较 %s 命中了错误的值 %q", op, items[0].Key)
		}
	}
}
