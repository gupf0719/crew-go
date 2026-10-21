package compose_v2_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestDeferredSend_ReplayPreservesInstanceIdentity 守 deferred send 跨 cp 重放的
// **目标实例身份**。
//
// PA-H1 路径：超步内 split 成功且 SendFrom 同 Target 投 2 次、兄弟 gate 中断 →
// split 的 sends 存进 cp.DeferredSends 等 Resume 重放。身份必须**随 directive 一起
// 持久化**（这正是 mint 点选在 drainSendSink 而非"target 进 frontier"的理由）。
//
// 掉了身份不报错，只是 N 个重放实例全拿空串、彼此无法区分 —— HITL 的 InterruptID
// 撞车被 dedupe 吞掉（用户永远看不到第一个问题）、子 cp 撞同一 ns 互覆、观测 span
// 共用一个槽。三个已修的侧面一起复发。
//
// sanity-revert：去掉 SerializedSend.InstanceID 的 marshal/unmarshal → ids 变 ["" ""]。
func TestDeferredSend_ReplayPreservesInstanceIdentity(t *testing.T) {
	// 节点 "w" 有多个实例**并发**跑，收集身份必须加锁——否则 append 撞在一起，
	// race detector 会报在这里，把真正该看的信号淹掉。
	var idsMu sync.Mutex
	var ids []string
	var interrupted atomic.Bool

	b := compose_v2.NewBuilder().WithName("h1")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "w"},
			compose_v2.SendDirective{Target: "w"},
		)
		return nil, nil
	})
	b.AddNode("gate", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// 首跑中断，逼 split 的 sends 进 cp.DeferredSends 等 Resume 重放
		if _, answered := compose_v2.GetResumeAnswer(ctx, "__h1__"); !answered {
			if interrupted.CompareAndSwap(false, true) {
				return nil, &compose_v2.InterruptError{Reason: "h1",
					Query: &compose_v2.AskQuery{InterruptID: "__h1__", Question: "?"}}
			}
		}
		return nil, nil
	})
	b.AddNode("w", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		idsMu.Lock()
		ids = append(ids, compose_v2.GetCurrentInstanceID(ctx))
		idsMu.Unlock()
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge(compose_v2.StartNode, "gate")
	b.AddEdge("w", compose_v2.EndNode)
	b.AddEdge("gate", compose_v2.EndNode)

	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("h1"))); !ok {
		t.Fatal("Run1 应中断")
	}
	cp, _ := cper.LoadNS(ctx, "h1", "")
	if len(cp.DeferredSends) != 2 {
		t.Fatalf("split 的 2 条 send 应进 cp.DeferredSends, 实际 %d 条", len(cp.DeferredSends))
	}
	for i, sd := range cp.DeferredSends {
		if sd.InstanceID == "" {
			t.Errorf("cp.DeferredSends[%d].InstanceID 为空——目标实例身份没随 directive 持久化", i)
		}
	}

	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("h1"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{"__h1__": schema.UserMessage("go")})); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	distinct := map[string]bool{}
	for _, id := range ids {
		distinct[id] = true
	}
	if len(ids) != 2 || len(distinct) != 2 {
		t.Errorf("2 个重放实例应各有独立身份，实际 ids=%q distinct=%d\n"+
			"  全空 = SerializedSend.InstanceID 没上 wire，身份在 cp round-trip 掉了\n"+
			"  ⟹ HITL InterruptID 撞车 / 子 cp 互覆 / 观测 span 共槽（侧面 1/2/6 一起复发）",
			ids, len(distinct))
	}
}
