package sql_test

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	agentSQL "crew-go/sql"
)

// TestCheckpointer_DeleteSubtree_MySQL_Instance_Segment_Patterns 验 MySQL 方言下
// DeleteSubtree 删的范围：前缀自身 + 经 `/` 往下的后代（判据见 compose_v2.IsNSInSubtree），
// 同一个节点的 `#`（fan-out 实例）与 `@`（重访实例）兄弟执行**不删**。
//
// PG 与 MySQL 共用同一段拼 SQL 的代码,两方言只差占位符渲染 —— 但**两边各钉
// 一条**才拦得住"只改了一个方言的期望"这种半吊子改动。
//
// 连兄弟一起删的后果不报错：一次执行成功清理时，把兄弟执行还挂着的存档删了，兄弟
// 续跑时只能从头来（副作用重放；模型每次换 tool_call ID 时永不收敛）。
func TestCheckpointer_DeleteSubtree_MySQL_Instance_Segment_Patterns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	cp := agentSQL.NewCheckpointer(db, agentSQL.MySQL{})

	// 同 PG 那条：本条只承 **MySQL 方言的占位符渲染**（全 `?`）。子树语义由真引擎上的
	// conformance 负责，判据已收回 Go（compose_v2.IsNSInSubtree 唯一一份）。
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT ns FROM crew_checkpoint WHERE sid = ?`)).
		WithArgs("sid_1").
		WillReturnRows(sqlmock.NewRows([]string{"ns"}).
			AddRow("parent").
			AddRow("parent/sub").
			AddRow("parent#0"). // 兄弟执行 — 不该删
			AddRow("parent@2"). // 兄弟执行 — 不该删
			AddRow("parentx"))  // sibling — 不该删

	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM crew_checkpoint WHERE sid = ? AND ns IN (?, ?)`)).
		WithArgs("sid_1", "parent", "parent/sub").
		WillReturnResult(sqlmock.NewResult(0, 2))

	n, err := cp.DeleteSubtree(context.Background(), "sid_1", "parent")
	if err != nil {
		t.Errorf("DeleteSubtree: %v", err)
	}
	if n != 2 {
		t.Errorf("expected 2 deleted, got %d", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}
