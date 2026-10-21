package sql_test

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"crew-go/compose_v2"
	agentSQL "crew-go/sql"
)

// TestCheckpointer_SaveNS_PG_SQL 钉 SaveNS 的语句顺序：一个事务里先占住这一行、读它的
// graph_id、再整行写入（graph_id 与 payload 一起写）、提交。判冲突读的是锁住之后的那一行，
// 见 SaveNS godoc。
func TestCheckpointer_SaveNS_PG_SQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	cp := agentSQL.NewCheckpointer(db, agentSQL.Postgres{})

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`ON CONFLICT (sid, ns) DO UPDATE SET sid=EXCLUDED.sid`)).
		WithArgs("sid_1", "ns_a").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT graph_id FROM crew_checkpoint WHERE sid = $1 AND ns = $2`)).
		WithArgs("sid_1", "ns_a").
		WillReturnRows(sqlmock.NewRows([]string{"graph_id"}).AddRow(""))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE crew_checkpoint SET parent_ns = $1, status = $2, schema_ver = $3, graph_id = $4, created_at = $5, payload = $6`)).
		WithArgs(
			"", sqlmock.AnyArg(), sqlmock.AnyArg(),
			"g_a", int64(100), sqlmock.AnyArg(),
			"sid_1", "ns_a",
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	c := &compose_v2.Checkpoint{
		GraphID:   "g_a",
		Status:    compose_v2.CPStatusPending,
		CreatedAt: 100,
	}
	if err := cp.SaveNS(context.Background(), "sid_1", "ns_a", c); err != nil {
		t.Errorf("SaveNS: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

// TestCheckpointer_SaveNS_GraphIDConflict 钉冲突路径：读到的 graph_id 与本次的都非空且不同 ⟹
// 回滚、返 ErrGraphIDConflict，不发 UPDATE（这一行不动）。
func TestCheckpointer_SaveNS_GraphIDConflict(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	cp := agentSQL.NewCheckpointer(db, agentSQL.Postgres{})

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO crew_checkpoint").
		WithArgs("sid_1", "ns_a").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT graph_id FROM crew_checkpoint WHERE sid = $1 AND ns = $2`)).
		WithArgs("sid_1", "ns_a").
		WillReturnRows(sqlmock.NewRows([]string{"graph_id"}).AddRow("g_a"))
	mock.ExpectRollback()

	c := &compose_v2.Checkpoint{GraphID: "g_b"}
	err = cp.SaveNS(context.Background(), "sid_1", "ns_a", c)
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) {
		t.Errorf("expected ErrGraphIDConflict, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestCheckpointer_SaveNS_AutoFills_ParentNS(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	cp := agentSQL.NewCheckpointer(db, agentSQL.Postgres{})

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO crew_checkpoint").
		WithArgs("sid_1", "parent/child").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT graph_id FROM crew_checkpoint").
		WillReturnRows(sqlmock.NewRows([]string{"graph_id"}).AddRow(""))
	mock.ExpectExec("UPDATE crew_checkpoint").
		WithArgs(
			"parent", // parent_ns auto-fill
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(),
			"sid_1", "parent/child",
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	c := &compose_v2.Checkpoint{}
	if err := cp.SaveNS(context.Background(), "sid_1", "parent/child", c); err != nil {
		t.Errorf("SaveNS: %v", err)
	}
	if c.ParentNS != "parent" {
		t.Errorf("expected ParentNS='parent' auto-fill, got %q", c.ParentNS)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestCheckpointer_LoadNS_NotFound_Returns_Nil_Nil(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	cp := agentSQL.NewCheckpointer(db, agentSQL.Postgres{})

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT payload FROM crew_checkpoint WHERE sid = $1 AND ns = $2`)).
		WithArgs("noexist", "noexist").
		WillReturnRows(sqlmock.NewRows([]string{"payload"})) // 空 rows

	loaded, err := cp.LoadNS(context.Background(), "noexist", "noexist")
	if err != nil {
		t.Errorf("LoadNS non-existent should return (nil, nil), got err: %v", err)
	}
	if loaded != nil {
		t.Errorf("expected nil, got %+v", loaded)
	}
}

func TestCheckpointer_DeleteSubtree_SQL_PG(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	cp := agentSQL.NewCheckpointer(db, agentSQL.Postgres{})

	// 本条只承一件事：**PG 方言的占位符渲染**（$1, $2, ...）——那是 SQLite 上的
	// conformance 覆盖不到的部分。子树匹配的**语义**由真引擎上的 conformance 负责
	// （conformance_sqlite_test.go），不再由这里的字符串形状负责。
	//
	// 判据本身已不在 SQL 里：先 `SELECT ns WHERE sid = ?` 枚举，再在 Go 里调
	// compose_v2.IsNSInSubtree 过滤，最后按 ns 精确 DELETE。这样「是不是子树」只剩
	// 权威函数一份实现——此前的 LIKE 复刻先后漏过分隔符 `@` 与前缀元字符两次，
	// 两次都静默。
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT ns FROM crew_checkpoint WHERE sid = $1`)).
		WithArgs("sid_1").
		WillReturnRows(sqlmock.NewRows([]string{"ns"}).
			AddRow("ns_a").       // 自身
			AddRow("ns_a/child"). // 嵌套子图段
			AddRow("ns_a@1").     // 重访实例段 — 同一个节点的另一次执行，不该删
			AddRow("ns_ab").      // sibling — 不该删
			AddRow("nsXa/child")) // `_` 若被当通配符才会命中 — 不该删

	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM crew_checkpoint WHERE sid = $1 AND ns IN ($2, $3)`)).
		WithArgs("sid_1", "ns_a", "ns_a/child").
		WillReturnResult(sqlmock.NewResult(0, 2))

	n, err := cp.DeleteSubtree(context.Background(), "sid_1", "ns_a")
	if err != nil {
		t.Errorf("DeleteSubtree: %v", err)
	}
	if n != 2 {
		t.Errorf("expected 2, got %d", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestCheckpointer_DeleteSubtree_EmptyPrefix_DeleteWholeSession(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	cp := agentSQL.NewCheckpointer(db, agentSQL.Postgres{})

	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM crew_checkpoint WHERE sid = $1`)).
		WithArgs("sid_1").
		WillReturnResult(sqlmock.NewResult(0, 5))

	n, err := cp.DeleteSubtree(context.Background(), "sid_1", "")
	if err != nil {
		t.Errorf("DeleteSubtree: %v", err)
	}
	if n != 5 {
		t.Errorf("expected 5, got %d", n)
	}
}

func TestCheckpointer_Setup_PG_DDL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	cp := agentSQL.NewCheckpointer(db, agentSQL.Postgres{})

	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS crew_checkpoint")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("CREATE INDEX IF NOT EXISTS crew_checkpoint_sid_status_idx")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := cp.Setup(context.Background()); err != nil {
		t.Errorf("Setup: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestCheckpointer_Setup_MySQL_DDL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	cp := agentSQL.NewCheckpointer(db, agentSQL.MySQL{})

	// audit iter-5: 回滚 iter-4 stored procedure — 简单 CREATE TABLE + 2 索引
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS crew_checkpoint")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("CREATE INDEX crew_checkpoint_sid_status_idx")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := cp.Setup(context.Background()); err != nil {
		t.Errorf("Setup: %v", err)
	}
}

func TestCheckpointer_NilCheckpoint_Returns_Error(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	cp := agentSQL.NewCheckpointer(db, agentSQL.Postgres{})

	if err := cp.SaveNS(context.Background(), "sid", "ns", nil); err == nil {
		t.Error("SaveNS nil cp should return error")
	}
}

func TestCheckpointer_EmptySessionID_Returns_Error(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	cp := agentSQL.NewCheckpointer(db, agentSQL.Postgres{})

	c := &compose_v2.Checkpoint{}
	if err := cp.SaveNS(context.Background(), "", "ns", c); err == nil {
		t.Error("SaveNS empty sid should return error")
	}
}
