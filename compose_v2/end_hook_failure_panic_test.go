package compose_v2_test

// 节点体失败之后的 OnNodeEnd、图失败之后的 OnGraphEnd panic，不改变这次执行的结局：照旧交出原来那个错误，panic 只记日志。
// 曾经节点那一处把原错误整个换成 panic 的错误：节点体问的问题认不出来（IsInterrupt 为假、存档里没有挂起项）、终局失败的
// 标记丢了被当成可重试、存档没清（审计实测）；图那一处把 panic 并进错误，错误文本多出一行钩子的 panic。

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

type failEndPanicHooks struct {
	compose_v2.NoOpOrchestrationHooks
	node, graph bool
}

func (h failEndPanicHooks) OnNodeEnd(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Channels, err error) context.Context {
	if h.node && err != nil {
		panic("boom-fail-node-end")
	}
	return ctx
}

func (h failEndPanicHooks) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, err error) context.Context {
	if h.graph && err != nil {
		panic("boom-fail-graph-end")
	}
	return ctx
}

var endHookPanicCases = []struct {
	name string
	h    failEndPanicHooks
}{{"OnNodeEnd", failEndPanicHooks{node: true}}, {"OnGraphEnd", failEndPanicHooks{graph: true}}}

// 节点体问人（中断）之后钩子 panic：照旧交出中断、存档里有这一问；答了续跑，节点体拿到答案、图跑完。
func TestEndHookFailurePanic_KeepsInterrupt(t *testing.T) {
	for _, tc := range endHookPanicCases {
		t.Run(tc.name, func(t *testing.T) {
			var got string
			b := compose_v2.NewBuilder().WithName("end_fail_int_" + tc.name)
			b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续吗"})
				if err != nil {
					return nil, err
				}
				got = ans.Content
				return nil, nil
			})
			b.AddEdge(compose_v2.StartNode, "ask")
			b.AddEdge("ask", compose_v2.EndNode)
			g := rsCompile(t, b)
			cp := compose_v2.NewInMemoryCheckpointer()
			opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s"), compose_v2.WithHooks(tc.h)}
			err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...)
			ie, ok := compose_v2.IsInterrupt(err)
			var pe *compose_v2.PanicError
			if !ok || errors.As(err, &pe) {
				t.Fatalf("应照旧交出中断、不挂 PanicError，实际 %v", err)
			}
			answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("是")}
			if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
				append(opts, compose_v2.WithResumeAnswers(answers))...); err != nil || got != "是" {
				t.Fatalf("答了续跑应跑完、节点体拿到答案，实际 err=%v got=%q", err, got)
			}
		})
	}
}

// 节点体以终局失败（ErrRunAborted）结束之后钩子 panic：照旧是终局失败——不可重试、存档清掉。
func TestEndHookFailurePanic_KeepsTerminalFailure(t *testing.T) {
	for _, tc := range endHookPanicCases {
		t.Run(tc.name, func(t *testing.T) {
			b := compose_v2.NewBuilder().WithName("end_fail_abort_" + tc.name)
			b.AddNode("first", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
			b.AddNode("n", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				return nil, errors.Join(errors.New("确定性的协议违规"), compose_v2.ErrRunAborted)
			})
			b.AddEdge(compose_v2.StartNode, "first")
			b.AddEdge("first", "n")
			b.AddEdge("n", compose_v2.EndNode)
			g := rsCompile(t, b)
			cp := compose_v2.NewInMemoryCheckpointer()
			err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
				compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s"), compose_v2.WithHooks(tc.h))
			if !errors.Is(err, compose_v2.ErrRunAborted) || compose_v2.IsRetryable(err) {
				t.Fatalf("应照旧是终局失败、不可重试，实际 err=%v retryable=%v", err, compose_v2.IsRetryable(err))
			}
			if ok, lerr := cp.ExistsNS(context.Background(), "s", ""); lerr != nil || ok {
				t.Fatalf("终局失败应清掉存档，实际还在（err=%v）", lerr)
			}
		})
	}
}
