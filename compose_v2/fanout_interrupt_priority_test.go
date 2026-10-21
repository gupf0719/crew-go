package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crew-go/compose_v2"
)

// 同一超步里既有分支中断、又有分支业务失败时，返回值必须是**中断**。
//
// 改之前的判据是「≥2 个中断才合并，否则返第一个错」，于是 1 个分支中断 + 兄弟分支业务
// 失败时返的是业务错、`IsInterrupt` 为 false——调用方按「Run 失败了」处理不会去 Resume，
// 而 cp 里正挂着那个等回答的问题，永远没人答。中断个数不同丢的东西还相反（1 个丢中断、
// 2 个走合并那条路反过来丢业务错），那不是取舍是漏想。
//
// **fixture 要点**：三个分支用三个**不同的节点**而不是同一节点的三个实例——SendFrom
// 派生的 frontier 成员按 directive 声明序排（确定），所以「谁排在索引 0」是可控的。
// 用同一节点的三个实例则要靠运行时抢到的序号来决定谁中断，索引 0 落在谁头上不确定：
// 一旦恰好是中断分支，`FirstErr` 本身就是中断，改不改都过（假绿）。这里让索引 0 的
// bizA 必定业务失败，中断排在中间。
func TestFanout_InterruptWinsOverSiblingBranchError(t *testing.T) {
	bizFail := errors.New("下游 500")

	b := compose_v2.NewBuilder().WithName("fanout_int_priority")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "bizA"}, // 索引 0——FirstErr 必定取它
			compose_v2.SendDirective{Target: "asks"},
			compose_v2.SendDirective{Target: "bizB"})
		return nil, nil
	})
	fail := func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, bizFail
	}
	b.AddNode("bizA", fail)
	b.AddNode("bizB", fail)
	b.AddNode("asks", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{InterruptID: "lone_branch", Question: "唯一一个等人回答的分支"},
		}
	})
	b.AddEdge(compose_v2.StartNode, "split")
	for _, n := range []compose_v2.NodeID{"bizA", "bizB", "asks"} {
		b.AddEdge(n, compose_v2.EndNode)
	}

	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	runErr := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("fip_sid"))

	// cp 侧当对照组：框架其实知道有一个问题挂着。
	cp, cpErr := cper.LoadNS(ctx, "fip_sid", "")
	if cpErr != nil {
		t.Fatalf("LoadNS: %v", cpErr)
	}
	inCp := 0
	if cp != nil {
		inCp = len(cp.PendingInterrupts)
	}
	if inCp != 1 {
		t.Fatalf("fixture 没造出预期状态：cp 里应有 1 个 pending interrupt，实际 %d", inCp)
	}

	ie, isInt := compose_v2.IsInterrupt(runErr)
	if !isInt {
		t.Fatalf("cp 里挂着 %d 个待回答的问题，返回值却不是 InterruptError（%v）——"+
			"调用方按「Run 失败了」处理不会 Resume，那个问题永远没人答", inCp, runErr)
	}
	if got := len(ie.AllQueries()); got != inCp {
		t.Errorf("caller 拿到 %d 个 query，cp 里记了 %d 个", got, inCp)
	}
}

// 只有一个分支中断时，返出去的必须是那条分支的**原始 error**，不是从链上抠出来的裸
// `*InterruptError`。
//
// 中断错常常并列挂着第二个 sentinel（子 agent 撞上陈旧写回时抛
// `errors.Join(interrupt, ErrCheckpointStale)`），而 a2a/server、agui、observability
// 三处都把 stale 判在 IsInterrupt 之前。剥掉之后它们退化成正常 HITL：留 entry 等续发、
// 导出 status=blocked 的绿 span，而那个中断的 cp 根本没落盘，用户被反复问同一个问题。
//
// `agent_v2` 侧的 `TestSentinel_SurvivesToolBoundary` 覆盖的是同一条不变量在 tool
// 边界的表现，这里钉的是引擎出口这一处——中间隔着几层，坏在哪一层要能分得出来。
func TestFanout_SingleInterruptKeepsCompanionSentinel(t *testing.T) {
	joined := errors.Join(
		&compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "single_sentinel", Question: "问一句"}},
		compose_v2.ErrCheckpointStale,
	)

	b := compose_v2.NewBuilder().WithName("fanout_int_sentinel")
	b.AddNode("n", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, joined
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)

	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	runErr := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()),
		compose_v2.WithSessionID("fis_sid"))

	if _, isInt := compose_v2.IsInterrupt(runErr); !isInt {
		t.Fatalf("应当仍是中断，实际 %v", runErr)
	}
	if !errors.Is(runErr, compose_v2.ErrCheckpointStale) {
		t.Errorf("原始链上并列挂的 ErrCheckpointStale 在引擎出口被剥掉了（err=%v）——"+
			"下游把 stale 判在 IsInterrupt 之前的三处会退化成正常 HITL，"+
			"而这次的 cp 根本没落盘", runErr)
	}
}

// 多个分支同时中断时，各分支原始链上并列挂的第二 sentinel 同样不能丢。
//
// 这是上一条（单中断）的另一半：`MergeInterrupts` 在 ≥2 条时新建一个 `InterruptError` 装
// 合并后的 query 列表，而 `*InterruptError` **没有 Unwrap**——只返它等于把 N 条原始链全
// 扔了。于是同一个不变量在 1 个中断时守住、2 个中断时守不住，而下游那三处（a2a/server
// 的 keepPending、agui 的 handleOutcome、observability 的 span 状态）判的都是同一个东西。
//
// **sanity-revert**：把 exitErr 的 default 分支改回裸 `MergeInterrupts(r.AllInterrupts)`，
// 本测试 FAIL 并报 sentinel 被剥。
func TestFanout_MultiInterruptKeepsCompanionSentinels(t *testing.T) {
	mkErr := func(q string) error {
		return fmt.Errorf("%w (cp 未落盘: %w)",
			&compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "sentinel_" + q, Question: q}},
			compose_v2.ErrCheckpointStale)
	}

	b := compose_v2.NewBuilder().WithName("fanout_multi_sentinel")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "askA"},
			compose_v2.SendDirective{Target: "askB"})
		return nil, nil
	})
	b.AddNode("askA", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, mkErr("问题 A")
	})
	b.AddNode("askB", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, mkErr("问题 B")
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("askA", compose_v2.EndNode)
	b.AddEdge("askB", compose_v2.EndNode)

	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	runErr := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()),
		compose_v2.WithSessionID("fms_sid"))

	ie, isInt := compose_v2.IsInterrupt(runErr)
	if !isInt {
		t.Fatalf("应当是 InterruptError，实际 %v", runErr)
	}
	if got := len(ie.AllQueries()); got != 2 {
		t.Errorf("两个分支中断，caller 拿到 %d 个 query", got)
	}
	if !errors.Is(runErr, compose_v2.ErrCheckpointStale) {
		t.Errorf("两个分支的原始链上都挂着 ErrCheckpointStale，合并后全被剥掉了（err=%v）——"+
			"同一个不变量单中断时守住、多中断时守不住；下游把 stale 判在 IsInterrupt 之前的"+
			"三处会退化成正常 HITL，而这次的 cp 根本没落盘", runErr)
	}
}
