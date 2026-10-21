package compose_v2_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
)

// Phase 1.0.b 端到端测试：真正"中断后从断点继续"的 Resume 语义。
//
// 撞 audit F3 [HIGH] cp/Resume 序列化先决问题——本组测试验证 frontier 持久化
// + ResumeFrom 让 graph 真能从中断点继续，不重跑已完成节点。

// ── Resume 端到端：中断 + cp + 跨进程模拟 + 继续 ──

// TestResume_FromCheckpoint_ContinuesFromInterruption 是 Phase 1.0.b 旗舰测试：
//
// **场景**：
//  1. Run 1 启动一个 5 轮循环（每轮 step → check 是否 done → 继续 / EndNode）
//  2. ctx 在第 3 轮触发 cancel——cm 自动记录当前 frontier
//  3. 拿到 snapshot（含 channel state + frontier）
//  4. JSON marshal + unmarshal 模拟跨进程
//  5. 新 cm + 注册同 channels + Restore（值 + frontier）
//  6. Run 2 用 WithResumeFrom 从 snapshot.CurrentFrontier 起跑
//  7. 验证 counter 最终值正确（5 轮全跑完，不重复计数）
//
// 关键验证：
//   - Run 2 不重跑已完成的前 3 轮（counter 不会变成 8 而是正确的 5）
//   - frontier 从 snapshot 取，不是从 StartNode 重算
//   - cm state 完整恢复（counter 起点正确）
func TestResume_FromCheckpoint_ContinuesFromInterruption(t *testing.T) {
	counterCh := compose_v2.Sum("counter")
	maxRoundsCh := compose_v2.LastValue[int]("max_rounds")

	// Run 1 的 ctx 在 step 第 3 次执行时取消——确定性的，不靠超时与调度。曾经是 70ms 超时配每步
	// 20ms 的睡眠：第 4、5 轮之间只有十几毫秒余量，机器负载高时超时回调的 goroutine 调度晚了、
	// 取消没被看见，第 5 轮跑完直接结束（实测一次「Run 1 应被 ctx cancel 中断」）。
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	var steps atomic.Int32

	makeGraph := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder().
			AddNode("step", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				if steps.Add(1) == 3 {
					cancel1()
				}
				return []compose_v2.ChannelWrite{compose_v2.Write(counterCh, 1)}, nil
			}).
			AddEdge(compose_v2.StartNode, "step").
			AddConditionalEdge("step", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
				count, _ := compose_v2.Get(chs, counterCh)
				maxR := compose_v2.MustGet(chs, maxRoundsCh)
				if count >= maxR {
					return compose_v2.EndNode, nil
				}
				return "step", nil
			}).
			WithMaxVisit(20)
		b = compose_v2.WithChannel(b, counterCh)
		b = compose_v2.WithChannel(b, maxRoundsCh)
		g, err := b.Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		return g
	}

	// === Run 1：跑半截后被 ctx cancel ===
	g1 := makeGraph()
	cm1 := compose_v2.NewChannelManager()

	runErr := compose_v2.Run(ctx1, g1, cm1,
		compose_v2.WithInitialWrites(compose_v2.Write(maxRoundsCh, 5)),
	)
	if runErr == nil {
		t.Fatal("Run 1 应被 ctx cancel 中断")
	}
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("应是 context canceled，实际 %v", runErr)
	}

	// 此时 counter 是部分进度（取消发生在第 3 轮里）
	partialCount := compose_v2.MustGet(cm1.Channels(), counterCh)
	if partialCount == 0 {
		t.Fatal("Run 1 至少应跑 1 轮再 cancel")
	}
	if partialCount >= 5 {
		t.Fatalf("Run 1 不应跑完，实际 counter=%d", partialCount)
	}
	t.Logf("Run 1 中断时 counter=%d", partialCount)

	// === Snapshot + 序列化 + 反序列化（模拟跨进程）===
	snap1, err := cm1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap1.CurrentFrontier) == 0 {
		t.Fatal("snapshot 应含 CurrentFrontier（中断时尚未执行的节点）")
	}
	t.Logf("snapshot frontier: %v", snap1.CurrentFrontier)

	data, err := compose_v2.MarshalCheckpoint(snap1)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// === Run 2：恢复并从断点继续 ===
	snap2, err := compose_v2.UnmarshalCheckpoint(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	g2 := makeGraph() // 编译同样的 graph
	cm2 := compose_v2.NewChannelManager()
	if err := compose_v2.RegisterChannel(cm2, counterCh); err != nil {
		t.Fatalf("cm2 register counter: %v", err)
	}
	if err := compose_v2.RegisterChannel(cm2, maxRoundsCh); err != nil {
		t.Fatalf("cm2 register maxRounds: %v", err)
	}
	if err := cm2.Restore(snap2); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// 验证 Restore 后状态等于 Run 1 中断时
	if v := compose_v2.MustGet(cm2.Channels(), counterCh); v != partialCount {
		t.Fatalf("Restore 后 counter 应 = Run 1 partial（%d），实际 %d", partialCount, v)
	}

	// Run 2 从断点继续——给足够时间跑完
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if err := compose_v2.Run(ctx2, g2, cm2, compose_v2.WithResumeFrom(snap2)); err != nil {
		t.Fatalf("Run 2 (resume) 应成功跑完，实际 %v", err)
	}

	// 验证：counter 最终 = 5（不是 partial + 5 = 重复跑前面 partial 轮）
	final := compose_v2.MustGet(cm2.Channels(), counterCh)
	if final != 5 {
		t.Fatalf("counter 最终应 5（resume 不重跑已完成轮），实际 %d", final)
	}
	t.Logf("Run 2 完成后 counter=%d（正确：从中断点继续，未重跑前 %d 轮）", final, partialCount)
}

// TestResume_FromAlreadyCompletedSnapshot 验证 Resume 一个已经跑完的 snapshot
// 立即返成功——CurrentFrontier 为空意味着上次已 isEnd。
func TestResume_FromAlreadyCompletedSnapshot(t *testing.T) {
	counterCh := compose_v2.Sum("counter_resume")

	b := compose_v2.NewBuilder().
		AddNode("inc", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(counterCh, 1)}, nil
		}).
		AddEdge(compose_v2.StartNode, "inc").
		AddEdge("inc", compose_v2.EndNode)
	b = compose_v2.WithChannel(b, counterCh)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// Run 1 跑完
	cm1 := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm1); err != nil {
		t.Fatalf("Run 1: %v", err)
	}

	// Snapshot——CurrentFrontier 应空
	snap, err := cm1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap.CurrentFrontier) != 0 {
		t.Fatalf("跑完的 graph snapshot 不应含 frontier，实际 %v", snap.CurrentFrontier)
	}

	// Run 2 用 ResumeFrom——应直接返成功，不重跑
	g2 := g // 同 graph
	cm2 := compose_v2.NewChannelManager()
	if err := compose_v2.RegisterChannel(cm2, counterCh); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := cm2.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if err := compose_v2.Run(context.Background(), g2, cm2, compose_v2.WithResumeFrom(snap)); err != nil {
		t.Fatalf("Resume from completed snapshot 应直接 success，实际 %v", err)
	}

	// 验证 counter 没重复增加（仍是 1）
	v := compose_v2.MustGet(cm2.Channels(), counterCh)
	if v != 1 {
		t.Fatalf("Resume 跑完的 graph 不应重跑，counter 应保持 1，实际 %d", v)
	}
}
