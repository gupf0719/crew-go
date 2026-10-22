package agent_v2_test

import (
	"context"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// agentEndPanicOnce 在这次调用成功收尾时的 OnAgentEnd 里 panic 一次。
type agentEndPanicOnce struct {
	hook.NoOpHooks
	done atomic.Bool
}

func (h *agentEndPanicOnce) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	if agent_v2.GetEndError(ctx) == nil && h.done.CompareAndSwap(false, true) {
		panic("agent end boom")
	}
	return ctx
}

// TestObserverHookPanic_AgentEndDoesNotRedoMember：顺序编排的成员（调一次扣款工具再答复）成功跑完之后，它的 OnAgentEnd 钩子
// panic 一次。OnAgentEnd 只观测、返回值不影响执行，这次调用照旧算成功——编排照常往下走，扣款只做一次。曾经 panic 被转成
// 成员的错误：成员自己的存档已经按成功清掉，编排续跑 / 重试时成员从头再跑、扣款重放（实测）。
func TestObserverHookPanic_AgentEndDoesNotRedoMember(t *testing.T) {
	for _, mode := range []string{"续跑", "重试"} {
		t.Run(mode, func(t *testing.T) {
			var charges atomic.Int64
			tk := tool.NewToolkit()
			tk.Register(tool.NewFuncTool("charge", "扣款", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
				charges.Add(1)
				return tool.Success("已扣款"), nil
			}))
			h := &agentEndPanicOnce{}
			member := agent_v2.NewBasicAgent(agent_v2.WithName("payer"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4), agent_v2.WithHooks(h),
				agent_v2.WithModel(newStubModel([]*schema.Message{
					schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c1", Type: "function", Function: schema.FunctionCall{Name: "charge", Arguments: `{}`}}}),
					schema.AssistantMessage("扣好了"),
					schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c2", Type: "function", Function: schema.FunctionCall{Name: "charge", Arguments: `{}`}}}),
					schema.AssistantMessage("扣好了"),
				})))
			opts := []agent_v2.SequentialOption{agent_v2.WithSequentialName("seq")}
			if mode == "重试" {
				opts = append(opts, agent_v2.WithSequentialMaxRetries(2))
			}
			seq := agent_v2.NewSequentialAgent(opts...).AddStep("pay", member)
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			sess := agent_v2.NewRunSession()
			_, err := seq.Run(ctx, sess, schema.UserMessage("付款"))
			if err != nil && mode == "续跑" {
				_, err = seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess))
			}
			if err != nil {
				t.Fatalf("编排应成功：%v", err)
			}
			if !h.done.Load() {
				t.Fatal("fixture 没搭对：OnAgentEnd 没 panic")
			}
			if n := charges.Load(); n != 1 {
				t.Errorf("成员已经做完的扣款被重做了：扣了 %d 次", n)
			}
		})
	}
}
