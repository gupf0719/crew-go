package checkpointer_conformance

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
)

// testConcurrent 验证并发安全 — N goroutine 同时 SaveNS/LoadNS/DeleteSubtree 无 race.
func testConcurrent(t *testing.T, factory CheckpointerFactory) {
	t.Run("Concurrent_SaveNS_Different_NS_All_Persisted", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()
		const N = 30

		// audit iter-2 视角 3 H1: CLAUDE.md "WaitGroup 屏障 + 短超时强制真并发"
		// 修前实测 0.00s 完成 = 100% 伪并发. 加 barrier 让 N goroutine 同瞬间释放.
		start := make(chan struct{})

		var wg sync.WaitGroup
		var errCount atomic.Int64
		for i := 0; i < N; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				<-start
				ns := fmt.Sprintf("ns_%d", idx)
				c := newCheckpoint("g", compose_v2.CPStatusUnset)
				if err := cp.SaveNS(ctx, "shared_sid", ns, c); err != nil {
					errCount.Add(1)
					t.Errorf("SaveNS ns_%d: %v", idx, err)
				}
			}(i)
		}
		close(start)
		wg.Wait()

		if errCount.Load() > 0 {
			t.Fatalf("concurrent SaveNS encountered %d errors", errCount.Load())
		}

		all, _ := cp.LoadAllInSession(ctx, "shared_sid")
		if len(all) != N {
			t.Errorf("expected %d cps in shared_sid, got %d", N, len(all))
		}
	})

	t.Run("Concurrent_SaveNS_LoadNS_NoRace", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()
		const N = 20

		// 预置数据 — Status=Unset
		for i := 0; i < N; i++ {
			_ = cp.SaveNS(ctx, "sid", fmt.Sprintf("ns_%d", i),
				newCheckpoint("g", compose_v2.CPStatusUnset))
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < N; i++ {
			wg.Add(2)
			go func(idx int) {
				defer wg.Done()
				<-start
				_, _ = cp.LoadNS(ctx, "sid", fmt.Sprintf("ns_%d", idx))
			}(i)
			go func(idx int) {
				defer wg.Done()
				<-start
				// 并发 SaveNS 改 Status=Pending
				_ = cp.SaveNS(ctx, "sid", fmt.Sprintf("ns_%d", idx),
					newCheckpoint("g", compose_v2.CPStatusPending))
			}(i)
		}
		close(start)
		wg.Wait()

		// audit 视角 2 H7: 仅靠 -race detector 不够 — 业务 backend SaveNS silent
		// noop (return nil 不写) 时 -race 不报但 LoadNS 拿不到最新 status. 加 final
		// 一致性验证: 全部 N 个 cp Status 都已被 Concurrent SaveNS 改为 Pending.
		for i := 0; i < N; i++ {
			ns := fmt.Sprintf("ns_%d", i)
			loaded, err := cp.LoadNS(ctx, "sid", ns)
			if err != nil {
				t.Errorf("final LoadNS %s: %v", ns, err)
				continue
			}
			if loaded == nil {
				t.Errorf("final LoadNS %s: nil cp (业务 backend SaveNS 可能 silent noop)", ns)
				continue
			}
			if loaded.Status != compose_v2.CPStatusPending {
				t.Errorf("final LoadNS %s: expected Status=Pending after concurrent SaveNS, got %v",
					ns, loaded.Status)
			}
		}
	})

	// 两个写入者带不同 GraphID 同时写同一 (sid, ns)：判冲突与写入必须是一个原子步骤——恰好一个
	// 成功、另一个报 ErrGraphIDConflict，留下的是成功者那份。先读再写的实现两个都成功，后写的
	// 静默顶掉先写的（sql 后端曾在真 SQLite 上 50 轮里 48~50 轮如此）。
	//
	// 新键与已有的行要分开测：新键上「插入」本身就把两个写入者排了队（唯一键冲突时后到的等先到的
	// 提交），已有的行上没有这一层，全靠实现自己先锁住那一行再判。只测新键时，一个判定前不加锁的
	// 实现照样全绿（真 PG 实测：占行改成不加锁之后，已有行上 40 轮 39 轮两个都成功，而新键这条照过）。
	t.Run("Concurrent_SaveNS_Conflicting_GraphID_Exactly_One_Wins", func(t *testing.T) {
		cp := factory()
		for r := 0; r < 20; r++ {
			raceConflictingGraphIDs(t, cp, fmt.Sprintf("race_%d", r))
		}
	})

	t.Run("Concurrent_SaveNS_Conflicting_GraphID_On_Existing_Row_Exactly_One_Wins", func(t *testing.T) {
		cp := factory()
		ctx := context.Background()
		for r := 0; r < 20; r++ {
			ns := fmt.Sprintf("existing_%d", r)
			// 已有一份 GraphID 为空的档：两个写入者跟它都不冲突，只跟彼此冲突
			if err := cp.SaveNS(ctx, "sid", ns, newCheckpoint("", compose_v2.CPStatusUnset)); err != nil {
				t.Fatalf("round %d: seed: %v", r, err)
			}
			raceConflictingGraphIDs(t, cp, ns)
		}
	})
}

// raceConflictingGraphIDs 让两个写入者带不同 GraphID 同时写 (sid, ns)，断言恰好一个成功、存档是赢家那份；
// 之后再各写一次：输家的 GraphID 被拒、赢家的放行——写入侧判冲突用的与 LoadNS 读出的是同一份 GraphID。
func raceConflictingGraphIDs(t *testing.T, cp compose_v2.Checkpointer, ns string) {
	t.Helper()
	ctx := context.Background()
	graphs := [2]string{"graph_a", "graph_b"}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var errs [2]error
	for i := range graphs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = cp.SaveNS(ctx, "sid", ns, newCheckpoint(graphs[i], compose_v2.CPStatusPending))
		}(i)
	}
	close(start)
	wg.Wait()

	winner, loser := -1, -1
	for i, err := range errs {
		switch {
		case err == nil:
			winner = i
		case errors.Is(err, compose_v2.ErrGraphIDConflict):
			loser = i
		default:
			t.Fatalf("ns %s: SaveNS %s: unexpected error %v", ns, graphs[i], err)
		}
	}
	if winner < 0 || loser < 0 {
		t.Fatalf("ns %s: want exactly one success and one ErrGraphIDConflict, got %v / %v", ns, errs[0], errs[1])
	}

	got, err := cp.LoadNS(ctx, "sid", ns)
	if err != nil || got == nil {
		t.Fatalf("ns %s: LoadNS: cp=%v err=%v", ns, got, err)
	}
	if got.GraphID != graphs[winner] {
		t.Fatalf("ns %s: stored GraphID = %q, want the winner %q", ns, got.GraphID, graphs[winner])
	}
	if err := cp.SaveNS(ctx, "sid", ns, newCheckpoint(graphs[loser], compose_v2.CPStatusUnset)); !errors.Is(err, compose_v2.ErrGraphIDConflict) {
		t.Fatalf("ns %s: later write by the loser %q: want ErrGraphIDConflict, got %v", ns, graphs[loser], err)
	}
	if err := cp.SaveNS(ctx, "sid", ns, newCheckpoint(graphs[winner], compose_v2.CPStatusUnset)); err != nil {
		t.Fatalf("ns %s: later write by the winner %q: %v", ns, graphs[winner], err)
	}
}
