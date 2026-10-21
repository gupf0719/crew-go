package sql_test

// GraphID 在 sql 后端只有一把尺：存档（payload）里那份。graph_id 列是它的副本——下面的用例在
// 真 SQLite 上直接读列，钉「每次写入之后列与 LoadNS 读出的 GraphID 相同」。conformance 只看得到
// 接口、看不到列；列与存档分叉时，Run / Resume 入口按存档放行的图写回时会被按列拒掉（节点带着
// 用户的答复已经跑过，进度存不下来，下次续跑副作用重放）。

import (
	databasesql "database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"

	"crew-go/compose_v2"
	agentSQL "crew-go/sql"
)

// openSQLiteCheckpointer 在一个隔离的真 SQLite 库上建表，返回 Checkpointer 与底层连接（读列用）。
func openSQLiteCheckpointer(t *testing.T) (*agentSQL.Checkpointer, *databasesql.DB) {
	t.Helper()
	db := newSQLiteDB(t)
	cp := agentSQL.NewCheckpointer(db, agentSQL.SQLite{})
	if err := cp.Setup(t.Context()); err != nil {
		t.Fatalf("cp Setup: %v", err)
	}
	return cp, db
}

// assertGraphIDCopies 断言这一行列里的 graph_id 与存档里的 GraphID 都是 want。
func assertGraphIDCopies(t *testing.T, cp *agentSQL.Checkpointer, db *databasesql.DB, ns, want string) {
	t.Helper()
	var col string
	if err := db.QueryRowContext(t.Context(),
		`SELECT graph_id FROM crew_checkpoint WHERE sid = ? AND ns = ?`, "sid", ns,
	).Scan(&col); err != nil {
		t.Fatalf("read graph_id column: %v", err)
	}
	got, err := cp.LoadNS(t.Context(), "sid", ns)
	if err != nil || got == nil {
		t.Fatalf("LoadNS: cp=%v err=%v", got, err)
	}
	if col != want || got.GraphID != want {
		t.Fatalf("graph_id column = %q, stored GraphID = %q, want both %q", col, got.GraphID, want)
	}
}

// 串行写入时列跟着每次放行的写入走：空 → A → B 拒掉 B、两份都还是 A；A → 空 → B 三次都放行、
// 两份最后都是 B。旧实现把 graph_id 从更新列里排除，列在第一次写入后就冻结——前一个序列三次都
// 成功（列是空、存档是 B），后一个序列拒掉 B（列是 A、存档是空）。
func TestCheckpointer_GraphIDColumnFollowsStoredCheckpoint(t *testing.T) {
	t.Run("empty_then_A_then_B_rejects_B", func(t *testing.T) {
		cp, db := openSQLiteCheckpointer(t)
		if err := cp.SaveNS(t.Context(), "sid", "ns", &compose_v2.Checkpoint{}); err != nil {
			t.Fatalf("SaveNS empty: %v", err)
		}
		assertGraphIDCopies(t, cp, db, "ns", "")
		if err := cp.SaveNS(t.Context(), "sid", "ns", &compose_v2.Checkpoint{GraphID: "graph_a"}); err != nil {
			t.Fatalf("SaveNS graph_a: %v", err)
		}
		assertGraphIDCopies(t, cp, db, "ns", "graph_a")
		err := cp.SaveNS(t.Context(), "sid", "ns", &compose_v2.Checkpoint{GraphID: "graph_b"})
		if !errors.Is(err, compose_v2.ErrGraphIDConflict) {
			t.Fatalf("SaveNS graph_b over graph_a: want ErrGraphIDConflict, got %v", err)
		}
		assertGraphIDCopies(t, cp, db, "ns", "graph_a")
	})

	t.Run("A_then_empty_then_B_accepts_B", func(t *testing.T) {
		cp, db := openSQLiteCheckpointer(t)
		for _, g := range []string{"graph_a", "", "graph_b"} {
			if err := cp.SaveNS(t.Context(), "sid", "ns", &compose_v2.Checkpoint{GraphID: g}); err != nil {
				t.Fatalf("SaveNS GraphID=%q: %v", g, err)
			}
			assertGraphIDCopies(t, cp, db, "ns", g)
		}
	})
}

// 两个写入者带不同 GraphID 同时写同一行：恰好一个成功，留下的那一行列与存档都是成功者的。
// 旧实现（先查后写、两条语句之间不持锁）在这里 50 轮里 48~50 轮两个都成功，留下列与存档分属
// 两张图的行。
func TestCheckpointer_ConcurrentConflictingWriters_LeaveOneConsistentRow(t *testing.T) {
	cp, db := openSQLiteCheckpointer(t)
	graphs := [2]string{"graph_a", "graph_b"}
	for r := 0; r < 50; r++ {
		ns := fmt.Sprintf("race_%d", r)
		start := make(chan struct{})
		var wg sync.WaitGroup
		var errs [2]error
		for i := range graphs {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				errs[i] = cp.SaveNS(t.Context(), "sid", ns, &compose_v2.Checkpoint{GraphID: graphs[i]})
			}(i)
		}
		close(start)
		wg.Wait()

		winner := -1
		conflicts := 0
		for i, err := range errs {
			switch {
			case err == nil:
				winner = i
			case errors.Is(err, compose_v2.ErrGraphIDConflict):
				conflicts++
			default:
				t.Fatalf("round %d: SaveNS %s: %v", r, graphs[i], err)
			}
		}
		if winner < 0 || conflicts != 1 {
			t.Fatalf("round %d: want exactly one success and one ErrGraphIDConflict, got %v / %v", r, errs[0], errs[1])
		}
		assertGraphIDCopies(t, cp, db, ns, graphs[winner])
	}
}
