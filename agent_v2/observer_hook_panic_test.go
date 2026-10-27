package agent_v2_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
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

// agentEndPanicAlways 在每次 OnAgentEnd 里都 panic。
type agentEndPanicAlways struct{ hook.NoOpHooks }

func (agentEndPanicAlways) OnAgentEnd(context.Context, *hook.AgentInfo, *hook.AgentOutput) context.Context {
	panic("agent end boom")
}

// 调用失败时 OnAgentEnd 钩子 panic：交出的仍是原来那个错误（同一个错误文本、认得出模型的 APIError、没有 PanicError）。曾经
// panic 交给外层兜底，原来的错误被整个换成 panic 的错误。
func TestObserverHookPanic_AgentEndKeepsFailure(t *testing.T) {
	run := func(hooks hook.Hooks) error {
		m := &scriptedStreamModel{
			openErr: func(int) error { return &model.APIError{Provider: "stub", StatusCode: 400, Body: "bad request"} },
			script:  func(int) ([]string, error) { return nil, nil },
		}
		opts := []agent_v2.Option{agent_v2.WithName("f"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(2)}
		if hooks != nil {
			opts = append(opts, agent_v2.WithHooks(hooks))
		}
		_, err := agent_v2.NewBasicAgent(opts...).Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
		return err
	}
	want := run(nil)
	got := run(agentEndPanicAlways{})
	var apiErr *model.APIError
	var pe *compose_v2.PanicError
	if got == nil || want == nil || got.Error() != want.Error() || !errors.As(got, &apiErr) || errors.As(got, &pe) {
		t.Fatalf("OnAgentEnd panic 不该改变失败的结局：期望 %v，实际 %v", want, got)
	}
}

// 调用停在中断上时 OnAgentEnd 钩子 panic：交出的仍是那个中断，续跑接得上（曾经被换成 panic 的错误，调用方认不出有问题要答）。
func TestObserverHookPanic_AgentEndKeepsInterrupt(t *testing.T) {
	m := &scriptedStreamModel{script: func(int) ([]string, error) { return []string{"draft"}, nil }}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("i"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(2),
		agent_v2.WithHooks(hook.NewChainHooks(askOnModelEnd{}, agentEndPanicAlways{})))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("OnAgentEnd panic 不该吞掉中断，实际 %v", err)
	}
	events, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("是")}))
	if err != nil || agent_v2.FinalMessage(events).Content != "draft" {
		t.Fatalf("续跑应接上并交出答复，实际 err=%v", err)
	}
}

// askOnModelEnd 在 OnModelEnd 里问一次人。
type askOnModelEnd struct{ hook.NoOpHooks }

func (askOnModelEnd) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
	_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "发出去吗"})
	return ctx, err
}
