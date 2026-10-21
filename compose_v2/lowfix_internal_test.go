package compose_v2

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// blockingCheckpointer 的 SaveNS 进入时 signal entered（确认 async worker 已卡住）、然后
// 阻塞在 release 上，用于把 async cp writer 的单 worker 卡死、让 buffer 填满。其余
// Checkpointer 方法不会被调用（embed nil 接口即可满足类型）。
type blockingCheckpointer struct {
	Checkpointer
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingCheckpointer) SaveNS(_ context.Context, _, _ string, _ *Checkpoint) error {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return nil
}

// TestAsyncCpWriter_SubmitEscapesOnCtxCancel 验证 LOW-1 fix：async durability writer 的
// submit 在 buffer 满阻塞时能经 ctx.Done() 逃出（防业务 backend SaveNS 忽略 ctx 卡死 →
// buffer 填满 → submit 永久阻塞 → 整 Run 挂死）。
//
// sanity-revert：把 submit 的 `select { case w.tasks <- cp: ; case <-w.ctx.Done(): ... }`
// 换回裸 `w.tasks <- cp` 后，buffer 满时 submit 永久阻塞、cancel ctx 也救不了 → 下面 2s
// 超时 fail，精确指向 "submit 无 ctx 逃逸 → 死锁"。
func TestAsyncCpWriter_SubmitEscapesOnCtxCancel(t *testing.T) {
	bc := &blockingCheckpointer{entered: make(chan struct{}), release: make(chan struct{})}
	defer close(bc.release) // 测试结束放行 worker（drain 后退出）

	ctx, cancel := context.WithCancel(context.Background())
	cfg := &runOptions{checkpointer: bc, sessionID: "s"}
	w := newAsyncCpWriter(ctx, cfg, NewChannelManager())

	// submit #1 → worker 取走 → SaveNS 阻塞；等 entered 确认 worker 已卡在 SaveNS 里
	if err := w.submit(&Checkpoint{}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	<-bc.entered

	// 填满 buffer（asyncCpWriterBuffer 个）——worker 卡死不消费，w.tasks 满
	for i := 0; i < asyncCpWriterBuffer; i++ {
		if err := w.submit(&Checkpoint{}); err != nil {
			t.Fatalf("submit fill %d: %v", i, err)
		}
	}

	// 下一个 submit 必阻塞（buffer 满 + worker 卡死）。goroutine 里发，cancel ctx 后应返
	// context.Canceled 逃出；revert（无 select）会永久阻塞 → 2s 超时 fail。
	done := make(chan error, 1)
	go func() { done <- w.submit(&Checkpoint{}) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("submit 应在 ctx 取消时返 context.Canceled，got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("submit 未在 ctx 取消时逃出（死锁 —— LOW-1 ctx.Done() select 缺失）")
	}
}

// TestCleanup_DeterministicSidTieBreakOnEqualTimestamp 验证 LOW-2 fix：Cleanup 在多个 sid
// 的 latest（CreatedAt）相同时，按 sid 字典序二级 tie-break 确定保哪个 / 删哪个，不受
// c.data map 迭代随机序影响。
//
// 构造：5 个 sid 的 cp CreatedAt 全相同 → latest 全等 → 只有 sid tie-break 能定序。
// keepLastN=2 → 应稳定保留 sid 字典序 top-2 = {d, e}。跑 20 轮（每轮 fresh checkpointer，
// map 迭代序每轮随机），fix 下每轮都保 {d,e}；revert（删 `stamps[j-1].sid < stamps[j].sid`
// 二级比较）下保留集随 map 序漂移 → 至少一轮保留非 {d,e} 的 sid → fail。
func TestCleanup_DeterministicSidTieBreakOnEqualTimestamp(t *testing.T) {
	sids := []string{"a", "b", "c", "d", "e"}
	const keepN = 2
	const iters = 20
	want := map[string]bool{"d": true, "e": true} // sid 字典序 top-2

	for it := 0; it < iters; it++ {
		cper := NewInMemoryCheckpointer()
		for _, sid := range sids {
			// 所有 cp CreatedAt 相同 → latest 全等 → 只有 sid tie-break 能定序
			if err := cper.SaveNS(context.Background(), sid, "", &Checkpoint{CreatedAt: 1000}); err != nil {
				t.Fatalf("save %s: %v", sid, err)
			}
		}
		deleted, err := cper.Cleanup(context.Background(), keepN)
		if err != nil {
			t.Fatalf("cleanup: %v", err)
		}
		if deleted != int32(len(sids)-keepN) {
			t.Fatalf("deleted=%d want %d", deleted, len(sids)-keepN)
		}
		if len(cper.data) != keepN {
			t.Fatalf("kept %d sids, want %d", len(cper.data), keepN)
		}
		for sid := range cper.data {
			if !want[sid] {
				t.Fatalf("iter %d: Cleanup 保留 sid=%q（不在 sid 字典序 top-2 {d,e}）—— latest 全等时"+
					"非确定 tie-break（LOW-2 revert：删 sid 二级比较 → map 迭代序随机）", it, sid)
			}
		}
	}
}
