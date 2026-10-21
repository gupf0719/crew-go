package compose_v2_test

// F-B 回归：SendDirective 的 payload 在 ApplyAll 整批没提交之后不能就这么丢。
//
// 裂缝来自 R27a 的取舍：dirWrites（本超步全部 directive 的 payload 汇总）ApplyAll
// 失败时，**target 已进 frontier 并被持久化**，而**产 send 的节点已成功、不重跑**
// （R27a 明确拒绝重跑：visit 漂移 + 副作用双跑）⟹ payload 无从再生。修复前 Resume
// 时 target 作普通节点执行、静默读到零值——最坏的一类 bug（不报错、结果错）。
//
// 修法：把被回滚的整批 writes 存进 cp.PendingSendWrites，Resume 在跑任何节点之前
// 补 apply。target 仍留 frontier（保住 R27a 决策）。
//
// sanity-revert 锚点：
//   - 删 graph_loop 失败分支的 cm.setPendingSendWrites(dirWrites) → 两个测试都 fail
//   - 删 Resume 入口的 peekPendingSendWrites 补 apply 段 → 两个测试都 fail
//   - 删 Snapshot/Restore 的 PendingSendWrites 段 → 两个测试都 fail（跨 Run 丢）

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestPendingSendWrites_RolledBackPayloadReappliedOnResume 是 F-B 主证。
//
// 拓扑 split → SendFrom(Y, goodCh="DATA_FOR_Y") + SendFrom(Z, badCh=1)。badCh 的
// 业务 reducer **首次** panic（瞬时故障，如下游抖动 / 偶发 type assert 失败）：
//
//	Run 1   split 成功产两条 directive → dirWrites=[goodCh 写, badCh 写] → ApplyAll
//	        撞 panic → **整批一条都不提交**（Y 的好 payload 被株连）→ frontier=[Y,Z] 存档
//	Resume  补 apply 整批 → badCh reducer 这次好了 → Y/Z 跑起来看到各自 payload
//
// 修复前：Resume 时 Y 读到零值 ""，且 Run 返 nil —— 静默出错。
func TestPendingSendWrites_RolledBackPayloadReappliedOnResume(t *testing.T) {
	var reducerCalls atomic.Int32
	var gotY, gotZ atomic.Value
	gotY.Store("")
	gotZ.Store(int(-1))

	goodCh := compose_v2.LastValue[string]("__fb_good__")
	// 业务自定义 reducer：首次 panic（瞬时），之后正常
	badCh := compose_v2.Custom("__fb_bad__", func(_, next int) int {
		if reducerCalls.Add(1) == 1 {
			panic("transient biz reducer panic")
		}
		return next
	}, 0)

	b := compose_v2.NewBuilder().WithName("fb_transient")
	compose_v2.WithChannel(b, goodCh)
	compose_v2.WithChannel(b, badCh)

	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{
				Target:  "Y",
				Payload: []compose_v2.ChannelWrite{compose_v2.Write(goodCh, "DATA_FOR_Y")},
			},
			compose_v2.SendDirective{
				Target:  "Z",
				Payload: []compose_v2.ChannelWrite{compose_v2.Write(badCh, 7)},
			},
		)
		return nil, nil
	})
	b.AddNode("Y", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		v, _ := compose_v2.Get(chs, goodCh)
		gotY.Store(v)
		return nil, nil
	})
	b.AddNode("Z", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		v, _ := compose_v2.Get(chs, badCh)
		gotZ.Store(v)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("Y", compose_v2.EndNode)
	b.AddEdge("Z", compose_v2.EndNode)

	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// Run 1：dirWrites ApplyAll 撞 reducer panic → 整批回滚
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("fb_sid"))
	if err == nil {
		t.Fatal("Run1 应因 dirWrites ApplyAll panic 失败")
	}
	cp1, _ := cper.LoadNS(ctx, "fb_sid", "")
	if cp1 == nil {
		t.Fatal("Run1 应存下 cp（frontier 含 SendFrom target）")
	}
	if len(cp1.PendingSendWrites) != 2 {
		t.Fatalf("被回滚的 payload 应整批存进 cp.PendingSendWrites（2 条），实际 %d 条: %+v\n"+
			"  不存 = Y/Z 的 payload 永久丢失（产 send 的 split 已成功、R27a 不让它重跑）",
			len(cp1.PendingSendWrites), cp1.PendingSendWrites)
	}

	// Resume：补 apply → reducer 这次好了 → Y/Z 看到各自 payload
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("fb_sid"))
	if err != nil {
		t.Fatalf("Resume 应成功, got %v", err)
	}
	if got := gotY.Load().(string); got != "DATA_FOR_Y" {
		t.Errorf("F-B：Y 应看到 split 投递的 payload %q, 实际 %q\n"+
			"  payload 因整批没提交而消失、又没存档 → Resume 时 Y 读到零值（静默出错）",
			"DATA_FOR_Y", got)
	}
	if got := gotZ.Load().(int); got != 7 {
		t.Errorf("F-B：Z 应看到 payload 7, 实际 %d", got)
	}
}

// TestPendingSendWrites_DeferredReplayPayloadPreserved 守**另一条来源路径**：撞回滚的
// payload 不只来自本超步新产的 SendFrom，也可能来自 **cp 里重放回来的 deferred send**
// （PA-H1：部分失败超步成功成员的 SendFrom 存 cp，Resume 时前置合并进本超步 sends 流）。
//
// 时序：A 成功产 sendZ + B 中断 → sendZ 进 cp.DeferredSends；Resume 1 时 B 成功、sendZ
// 重放进 dirWrites → ApplyAll 撞 reducer panic → 整批回滚。此刻 cm.deferredSends 已被
// 待办记录已清空、Z 已进 frontier —— **payload 只剩 PendingSendWrites 一根
// 救命稻草**（A 早已成功，永远不会重跑重产）。
func TestPendingSendWrites_DeferredReplayPayloadPreserved(t *testing.T) {
	var boomCalls atomic.Int32
	var zSaw atomic.Value
	zSaw.Store("")

	boomCh := compose_v2.Custom("__fb3_boom__", func(_, next string) string {
		if boomCalls.Add(1) == 1 {
			panic("transient reducer boom")
		}
		return next
	}, "")

	b := compose_v2.NewBuilder().WithName("fb_deferred_replay")
	compose_v2.WithChannel(b, boomCh)
	b.AddNode("split", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddNode("A", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{
			Target:  "Z",
			Payload: []compose_v2.ChannelWrite{compose_v2.Write(boomCh, "PAYLOAD_FROM_A")},
		})
		return nil, nil
	})
	b.AddNode("B", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, ok := compose_v2.GetResumeAnswer(ctx, "__fb3_q__"); !ok {
			return nil, &compose_v2.InterruptError{
				Reason: "fb3",
				Query:  &compose_v2.AskQuery{InterruptID: "__fb3_q__", Question: "?"},
			}
		}
		return nil, nil
	})
	b.AddNode("Z", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		v, _ := compose_v2.Get(chs, boomCh)
		zSaw.Store(v)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("split", "A")
	b.AddEdge("split", "B")
	b.AddEdge("A", compose_v2.EndNode)
	b.AddEdge("B", compose_v2.EndNode)
	b.AddEdge("Z", compose_v2.EndNode)

	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	ans := map[string]*schema.Message{"__fb3_q__": schema.UserMessage("go")}

	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("fb3_sid"))); !ok {
		t.Fatal("Run1 应中断（B）")
	}
	// Resume 1：deferred sendZ 重放 → ApplyAll 撞 panic → 整批回滚
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("fb3_sid"),
		compose_v2.WithResumeAnswers(ans)); err == nil {
		t.Fatal("Resume1 应因 dirWrites ApplyAll panic 失败")
	}
	cp2, _ := cper.LoadNS(ctx, "fb3_sid", "")
	if cp2 == nil || len(cp2.PendingSendWrites) != 1 {
		t.Fatalf("重放的 deferred payload 被回滚后应存进 cp.PendingSendWrites，实际 cp=%+v\n"+
			"  此刻 cm.deferredSends 已清空、A 已成功不会重跑 —— 不存就永久丢", cp2)
	}
	// Resume 2：补 apply → Z 看到 A 当初投的 payload
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("fb3_sid"),
		compose_v2.WithResumeAnswers(ans)); err != nil {
		t.Fatalf("Resume2 应成功, got %v", err)
	}
	if got := zSaw.Load().(string); got != "PAYLOAD_FROM_A" {
		t.Errorf("Z 应看到 A 当初投的 payload %q, 实际 %q（deferred 重放的 payload 被回滚后丢失）",
			"PAYLOAD_FROM_A", got)
	}
}

// TestPendingSendWrites_DeterministicFailureStaysLoud 守另一半：业务 reducer 若是
// **确定性**坏的，补 apply 每次都会撞同样的 panic —— 此时框架该**持续报错**，不该
// 退化成"跳过补 apply、让 target 读零值继续跑"（那就绕回 F-B 的静默出错了）。
//
// 修复前的行为正是后者：Resume 静默成功 + Y 读到零值。
func TestPendingSendWrites_DeterministicFailureStaysLoud(t *testing.T) {
	var yRan atomic.Int32
	goodCh := compose_v2.LastValue[string]("__fb2_good__")
	badCh := compose_v2.Custom("__fb2_bad__", func(_, _ int) int {
		panic("permanent biz reducer panic")
	}, 0)

	b := compose_v2.NewBuilder().WithName("fb_deterministic")
	compose_v2.WithChannel(b, goodCh)
	compose_v2.WithChannel(b, badCh)

	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{
				Target:  "Y",
				Payload: []compose_v2.ChannelWrite{compose_v2.Write(goodCh, "DATA_FOR_Y")},
			},
			compose_v2.SendDirective{
				Target:  "Z",
				Payload: []compose_v2.ChannelWrite{compose_v2.Write(badCh, 1)},
			},
		)
		return nil, nil
	})
	b.AddNode("Y", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		yRan.Add(1)
		return nil, nil
	})
	b.AddNode("Z", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("Y", compose_v2.EndNode)
	b.AddEdge("Z", compose_v2.EndNode)

	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("fb2_sid")); err == nil {
		t.Fatal("Run1 应因 reducer panic 失败")
	}

	// Resume：补 apply 再次撞 panic → 必须报错，不能静默跑 Y。
	//
	// 用 WithResumeFrom 显式恢复——上次是业务错退出、存档标了 CPStatusFailed；
	// 这里显式指名是为了让这条用例只测「补 apply 再撞 panic」这一件事，不依赖入口的捡档行为。
	failedCp, _ := cper.LoadNS(ctx, "fb2_sid", "")
	if failedCp == nil {
		t.Fatal("失败存档应当保留（只是不进 ListPending）")
	}
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("fb2_sid"),
		compose_v2.WithResumeFrom(failedCp))
	if err == nil {
		t.Fatalf("确定性坏 reducer：Resume 应持续报错，实际静默成功（Y 跑了 %d 次、读的是零值）", yRan.Load())
	}
	if !strings.Contains(err.Error(), "pending send writes") {
		t.Errorf("错误应指向补 apply 失败（含 \"pending send writes\"），实际: %v", err)
	}
	if n := yRan.Load(); n != 0 {
		t.Errorf("补 apply 没成功时 Y 不该跑（会读到零值），实际跑了 %d 次", n)
	}

	// cp 得继续带着 pendingSendWrites —— 业务修完 reducer 再 Resume 应能续跑
	cp, _ := cper.LoadNS(ctx, "fb2_sid", "")
	if cp == nil || len(cp.PendingSendWrites) != 2 {
		t.Errorf("补 apply 失败后 cp 应仍带着这批 writes（业务修完 reducer 可续跑），实际 cp=%+v", cp)
	}
}
