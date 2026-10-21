package agent_v2_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// hitlHookCounter 记 OnAgentStart / OnAgentEnd 调用次数（HITL 闭环严格计数守护用）。
type hitlHookCounter struct {
	hook.NoOpHooks
	starts atomic.Int32
	ends   atomic.Int32
}

func (h *hitlHookCounter) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	h.starts.Add(1)
	return ctx, nil
}

func (h *hitlHookCounter) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	h.ends.Add(1)
	return ctx
}

// C1.3 守护测试：BasicAgent HITL Run+Resume 闭环 OnAgentStart==2 && OnAgentEnd==2 严格相等
// （CLAUDE.md "Hook 选择速查" + ISSUE-82 OnAgentEnd 单一所有权承诺）。
//
// 现有 hook 测试多 `if got == 0 { fail }` 只防 0 次——不防"漏触发"（Resume 漏 fire
// OnAgentStart）也不防"重复触发"（ISSUE-82 OnAgentEnd 双触发回归）。本测用**阶段性严格
// 相等**断言守护：
//   - Run 1 抛 Interrupt 后：starts==1 && ends==1（Run defer 兜底 Interrupt 退出路径）
//   - Resume 完成后：     starts==2 && ends==2（Resume 同步 fire OnAgentStart + Resume defer OnAgentEnd）
//
// 非 tautological：阶段断言内置增量验证——Run 后 ==1 排除"恒 0"，Resume 后 ==2 排除
// "Resume 没新增 fire"。ISSUE-82 双触发回归（ends 变 3）或 Resume 漏 fire（starts 仍 1）本测必 fail。
func TestBasicAgent_HITLResume_HookCountStrict(t *testing.T) {
	askTool := tool.NewFuncTool(
		"ask", "ask user",
		map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}, "required": []string{"q"}},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "ask_id"); ok {
				return tool.Success("user said: " + ans), nil
			}
			return nil, &compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "ask_id", Question: "what?"}}
		},
	)
	tk := tool.NewToolkit()
	tk.Register(askTool)

	tc := schema.ToolCall{ID: "tc_1", Function: schema.FunctionCall{Name: "ask", Arguments: `{"q":"test"}`}}
	m := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{tc}),
		schema.AssistantMessage("got it"),
	})

	counter := &hitlHookCounter{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("hitl_count"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(counter),
		agent_v2.WithMaxRounds(5),
	)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// ── Run 1：抛 Interrupt ──
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, isInt := agent_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("Run 1 应抛 InterruptError, got %v", err)
	}
	if got := counter.starts.Load(); got != 1 {
		t.Errorf("Run 1 后 OnAgentStart 应 ==1, got %d", got)
	}
	if got := counter.ends.Load(); got != 1 {
		t.Errorf("Run 1 后 OnAgentEnd 应 ==1（Run defer 兜底 Interrupt 退出），got %d —— "+
			"ISSUE-82 单一所有权回归则可能 >1", got)
	}

	// ── Resume：完成 ──
	events, err := ag.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("answer")}),
		agent_v2.WithResumeSession(sess),
	)
	if err != nil {
		t.Fatalf("Resume err: %v", err)
	}
	if final := agent_v2.FinalMessage(events); final == nil || !strings.Contains(final.Content, "got it") {
		t.Fatalf("Resume final 应含 'got it', got %v", final)
	}

	// ── 严格相等终态断言：HITL 闭环 start==2 && end==2 ──
	if got := counter.starts.Load(); got != 2 {
		t.Errorf("HITL 闭环 OnAgentStart 应严格 ==2 (Run 1 + Resume 1), got %d —— Resume 漏 fire 则 <2", got)
	}
	if got := counter.ends.Load(); got != 2 {
		t.Errorf("HITL 闭环 OnAgentEnd 应严格 ==2 (Run defer + Resume defer), got %d —— "+
			"ISSUE-82 双触发回归则 >2 / 漏触发则 <2", got)
	}
}
