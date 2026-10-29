package compose_v2_test

// 节点里发起、落进所在那张图自己存档槽（同一个 sid、同一个 ns）的嵌套 Run 在入口被拒（ErrRunAborted）：它会把外层图正挂着
// 的存档当成自己的读进来接着跑，成功时把那个槽清掉。被拒的这次 Run 什么都不执行，也不清那个槽（槽是外层图的）。没配
// checkpointer 时没有槽可撞、给了自己的 ns 时不撞，都照常跑。

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

func innerGraph(t *testing.T, ran *int) *compose_v2.Graph {
	t.Helper()
	b := compose_v2.NewBuilder().WithName("own_slot_inner")
	b.AddNode("x", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		*ran++
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "x")
	b.AddEdge("x", compose_v2.EndNode)
	return rsCompile(t, b)
}

// 外层 a 问人、续跑；b 里裸跑嵌套图（继承 sid 与 ns）：被拒、嵌套图的节点没跑、外层那份挂起档还在。
func TestNestedRun_OnOwnSlotRejectedWithoutPurging(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	ran := 0
	var innerErr error
	slotKept := false
	b := compose_v2.NewBuilder().WithName("own_slot_outer")
	b.AddNode("a", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续吗"})
		return nil, err
	})
	b.AddNode("b", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		innerErr = compose_v2.Run(ctx, innerGraph(t, &ran), compose_v2.NewChannelManager())
		slotKept, _ = cp.ExistsNS(ctx, "s", "")
		return nil, nil // 吞掉，看外层的档在嵌套 Run 返回之后还在不在
	})
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddEdge("a", "b")
	b.AddEdge("b", compose_v2.EndNode)
	g := rsCompile(t, b)
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s")}
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture 没搭对：a 应停在问人上，实际 %v", err)
	}
	answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("是")}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), append(opts, compose_v2.WithResumeAnswers(answers))...); err != nil {
		t.Fatalf("续跑：%v", err)
	}
	if !errors.Is(innerErr, compose_v2.ErrRunAborted) || !strings.Contains(innerErr.Error(), "RunSubAgent") || !strings.Contains(innerErr.Error(), `node "b"`) {
		t.Fatalf("落进外层图自己存档槽的嵌套 Run 应在入口被拒（ErrRunAborted、指出节点、指向 RunSubAgent），实际 %v", innerErr)
	}
	if ran != 0 {
		t.Fatalf("被拒的嵌套 Run 不该执行任何节点，实际执行了 %d 次", ran)
	}
	if !slotKept {
		t.Fatal("被拒的嵌套 Run 不该清掉外层图的存档槽")
	}
}

// 没配 checkpointer、或给了自己的 ns：照常跑。
func TestNestedRun_NoCheckpointerOrOwnNSRuns(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []compose_v2.RunOption
		ns   bool
	}{
		{"没配 checkpointer", nil, false},
		{"自己的 ns", []compose_v2.RunOption{compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()), compose_v2.WithSessionID("s")}, true},
		// 有 checkpointer 没有 sid：引擎不存档、不 auto-load，没有槽可撞（审计实测曾被误拒）
		{"有 checkpointer 没有 sid", []compose_v2.RunOption{compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer())}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ran := 0
			var innerErr error
			b := compose_v2.NewBuilder().WithName("own_slot_ok")
			b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				var o []compose_v2.RunOption
				if tc.ns {
					o = append(o, compose_v2.WithRunNS(compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), "inner")))
				}
				innerErr = compose_v2.Run(ctx, innerGraph(t, &ran), compose_v2.NewChannelManager(), o...)
				return nil, innerErr
			})
			b.AddEdge(compose_v2.StartNode, "n")
			b.AddEdge("n", compose_v2.EndNode)
			if err := compose_v2.Run(context.Background(), rsCompile(t, b), compose_v2.NewChannelManager(), tc.opts...); err != nil || ran != 1 {
				t.Fatalf("应照常跑完、嵌套图的节点执行一次，实际 err=%v inner=%v ran=%d", err, innerErr, ran)
			}
		})
	}
}

// graphStartRunsInner 在 OnGraphStart 里裸跑一张嵌套图（继承 sid 与 ns），记下它的结果；only 非空时只对那个 ns 上的图这么做。
type graphStartRunsInner struct {
	compose_v2.NoOpOrchestrationHooks
	t    *testing.T
	only string
	ran  *int
	err  *error
}

func (h graphStartRunsInner) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	if compose_v2.GetGraphNS(ctx) == h.only {
		*h.err = compose_v2.Run(ctx, innerGraph(h.t, h.ran), compose_v2.NewChannelManager())
	}
	return ctx, nil
}

// 图级钩子里裸跑嵌套图同样落进正在跑的那张图的槽：顶层图的 OnGraphStart（不在任何节点里）、嵌套图的 OnGraphStart（节点范围
// 的章是外层节点的、ns 却是嵌套图自己的）都拒。只看节点范围的章的话这两处都漏。
func TestNestedRun_FromGraphHookOnOwnSlotRejected(t *testing.T) {
	for _, nested := range []bool{false, true} {
		name := map[bool]string{false: "顶层图的 OnGraphStart", true: "嵌套图的 OnGraphStart"}[nested]
		t.Run(name, func(t *testing.T) {
			ran := 0
			var innerErr error
			hooks := graphStartRunsInner{t: t, ran: &ran, err: &innerErr}
			b := compose_v2.NewBuilder().WithName("hook_slot_outer")
			if nested {
				hooks.only = "mid"
				mid := compose_v2.NewBuilder().WithName("hook_slot_mid")
				mid.AddNode("m", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
				mid.AddEdge(compose_v2.StartNode, "m")
				mid.AddEdge("m", compose_v2.EndNode)
				mg := rsCompile(t, mid)
				b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					return nil, compose_v2.Run(ctx, mg, compose_v2.NewChannelManager(), compose_v2.WithHooks(hooks), compose_v2.WithRunNS("mid"))
				})
			} else {
				b.AddNode("n", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
			}
			b.AddEdge(compose_v2.StartNode, "n")
			b.AddEdge("n", compose_v2.EndNode)
			opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()), compose_v2.WithSessionID("s")}
			if !nested {
				opts = append(opts, compose_v2.WithHooks(hooks))
			}
			_ = compose_v2.Run(context.Background(), rsCompile(t, b), compose_v2.NewChannelManager(), opts...)
			if !errors.Is(innerErr, compose_v2.ErrRunAborted) || !strings.Contains(innerErr.Error(), "RunSubAgent") {
				t.Fatalf("落进正在跑的那张图自己存档槽的嵌套 Run 应在入口被拒，实际 %v", innerErr)
			}
			if ran != 0 {
				t.Fatalf("被拒的嵌套 Run 不该执行任何节点，实际执行了 %d 次", ran)
			}
		})
	}
}

// 外层没配 checkpointer（不占槽），节点里的嵌套图自己带了 checkpointer：照常跑。
func TestNestedRun_OuterWithoutSlotInnerWithCheckpointerRuns(t *testing.T) {
	ran := 0
	b := compose_v2.NewBuilder().WithName("own_slot_outer_nocp")
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, compose_v2.Run(ctx, innerGraph(t, &ran), compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()), compose_v2.WithSessionID("s"))
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	if err := compose_v2.Run(context.Background(), rsCompile(t, b), compose_v2.NewChannelManager(), compose_v2.WithSessionID("s")); err != nil || ran != 1 {
		t.Fatalf("外层不占槽时嵌套图应照常跑，实际 err=%v ran=%d", err, ran)
	}
}
