package sql_test

// 让本模块的 SQL 在**真 SQL 引擎**上跑上游的 conformance 套件。
//
// 此前本模块只有 sqlmock——钉的是「发出去的 SQL 长什么样」而不是「跑出来对不对」。
// 代价实测过三次：ns 子树匹配先后漏掉「重访实例段 @」与「前缀里的 LIKE 元字符」，
// 以及 filter 的类型不匹配语义（字符串行被当成「大于 10」返回、`$eq` 直值 10 不命中
// 10.0）——都对 sqlmock 全程隐形，接上真引擎才变红。唯一的变量就是「backend 能不能真跑
// 起来」。
//
// ⚠️ **它不等于 PG/MySQL 覆盖**——覆盖边界见 sql.OpenSQLiteForTest 的 godoc；那两家
// 2026-09-03 用 embedded-postgres 与官方 MySQL tarball 各跑过一遍同一套 conformance
// （PG 16.9 / MySQL 8.0.35 全绿），改 filter 语义或 DDL 时照 store 对照文档里的配方再跑。

import (
	databasesql "database/sql"
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
