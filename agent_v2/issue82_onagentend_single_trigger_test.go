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

// onAgentEndCounter 统计 OnAgentEnd 调用次数——用于 ISSUE-82 验证 hook
// **整个 Run 一次**协议（pre-fix 状态是双触发 = 2 次）。
type onAgentEndCounter struct {
	hook.NoOpHooks
	count atomic.Int32
}

func (h *onAgentEndCounter) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	h.count.Add(1)
	return ctx
}

// TestISSUE82_OnAgentEnd_TriggeredOnce_NormalExit 验 ISSUE-82 fix — 正常退出
// （model emit final message 无 tool_call）OnAgentEnd 仅触发 1 次（不是 2 次）。
//
// **bug 历史**: pre-ISSUE-82 双触发——
//   - agent_v2/basic_builder.go ConditionalEdge cond fn (model→terminal) fire 1 次
//   - agent_v2/basic_agent.go:139 Run defer fire 1 次
//     = 业务计费/SLO/审计算 2 次，错。
//
// **fix**: 删 ConditionalEdge cond fn 内 fire——agent_v2 Run defer 唯一源
// （学 eino flowAgent + ChatModelAgent 单一所有权）。
//
// **sanity-revert**: 还原 agent_v2/basic_builder.go-478 fire 块 → 测试
// fail "OnAgentEnd called 2 次, want 1"。
func TestISSUE82_OnAgentEnd_TriggeredOnce_NormalExit(t *testing.T) {
	counter := &onAgentEndCounter{}

	// stub model: r1 emit final message (no tool_calls)
	stub := newStubModel([]*schema.Message{
		schema.AssistantMessage("done"),
	})

	agent := agent_v2.NewBasicAgent(
		agent_v2.WithName("test"),
		agent_v2.WithModel(stub),
		agent_v2.WithHooks(counter),
		agent_v2.WithMaxRounds(3),
	)

	_, err := agent.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	got := counter.count.Load()
	if got != 1 {
		t.Errorf("ISSUE-82 fix: OnAgentEnd 应 fire 1 次 (单一所有权), got %d 次\n"+
			"sanity-revert: 还原 agent_v2/basic_builder.go-478 ConditionalEdge fire 块 → got = 2",
			got)
	}
}

// interruptingTool82 抛 InterruptError，用于验 HITL 退出路径 OnAgentEnd fire 1 次
type interruptingTool82 struct{}

func (interruptingTool82) Name() string                { return "ask" }
func (interruptingTool82) Description() string         { return "ask user" }
func (interruptingTool82) InputSchema() map[string]any { return map[string]any{} }
func (interruptingTool82) Call(_ context.Context, _ map[string]any) (*tool.Result, error) {
	return nil, &compose_v2.InterruptError{
		Reason: "ask user via tool",
		Query:  &compose_v2.AskQuery{InterruptID: "issue82_ask", Question: "Q?"},
	}
}

var _ tool.Tool = interruptingTool82{}

// TestISSUE82_OnAgentEnd_TriggeredOnce_HITLInterrupt 验 ISSUE-82 fix — HITL
// InterruptError 退出时 OnAgentEnd 仍触发 1 次（agent_v2 Run defer 兜底）。
//
// **协议明示**: 业务监控 OnAgentEnd 在 HITL 中断时也 fire（学 eino flowAgent
// wrapIterWithOnEnd iter 关闭即 fire 模式 + LangGraph GraphInterrupt swallow →
// on_chain_end 模式）—— Resume 后真 final 时再 fire 1 次。业务计费时 HITL
// 暂停算"退出事件"。
//
// **sanity-revert**: 删 agent_v2/basic_agent.go Run defer OnAgentEnd 块 → 测试 fail
// "OnAgentEnd called 0 次, want 1" (audit² 删 SafeOnAgentEnd wrapper 跟 eino 对齐 —
// framework outer recover 兜底 panic, defer 直接调 hooks.OnAgentEnd)。
func TestISSUE82_OnAgentEnd_TriggeredOnce_HITLInterrupt(t *testing.T) {
	counter := &onAgentEndCounter{}

	tk := agent_v2_newToolkit()
	tk.Register(interruptingTool82{})

	stub := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "tc_int", Type: "function", Function: schema.FunctionCall{Name: "ask", Arguments: `{}`}},
		}),
		schema.AssistantMessage("after resume"),
	})

	agent := agent_v2.NewBasicAgent(
		agent_v2.WithName("test"),
		agent_v2.WithModel(stub),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(counter),
		agent_v2.WithMaxRounds(3),
	)

	_, err := agent.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("Run 应抛 InterruptError, got nil")
	}
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("应返 InterruptError, got %v", err)
	}

	got := counter.count.Load()
	if got != 1 {
		t.Errorf("ISSUE-82 fix: HITL Interrupt 退出 OnAgentEnd 应 fire 1 次 (Run defer 兜底), got %d 次\n"+
			"pre-fix bug: ConditionalEdge cond fn 不跑 InterruptError 路径 → 漏 fire (got=0)\n"+
			"sanity-revert: 删 agent_v2/basic_agent.go Run defer OnAgentEnd 块 → got = 0",
			got)
	}
}

// TestISSUE82_OnAgentEnd_TriggeredOnce_MaxRounds 验 audit LOW-2 — maxRounds 退出
// 路径 OnAgentEnd 应 fire 1 次（Run defer 兜底）。
//
// 场景: model 永远返 tool_call → ReAct 循环 maxRounds 达到 → graph_loop 抛
// MaxIterationsError → Run defer 兜底 fire OnAgentEnd 1 次。
func TestISSUE82_OnAgentEnd_TriggeredOnce_MaxRounds(t *testing.T) {
	counter := &onAgentEndCounter{}

	// 简单 echo tool — 让 model emit tool_call 后能继续循环
	tk := agent_v2_newToolkit()
	tk.Register(&orderedTool{name: "noop", sleep: 0})

	// 一直 emit tool_call (cycle 5 次足够触 maxRounds=2)
	tcMsg := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tc_loop", Type: "function", Function: schema.FunctionCall{Name: "noop", Arguments: `{}`}},
	})
	stub := newStubModel([]*schema.Message{tcMsg, tcMsg, tcMsg, tcMsg, tcMsg})

	agent := agent_v2.NewBasicAgent(
		agent_v2.WithName("test"),
		agent_v2.WithModel(stub),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(counter),
		agent_v2.WithMaxRounds(2), // 故意小让 maxRounds 触发
	)

	_, _ = agent.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	// 不验 err — maxRounds 退出可能返 err 也可能 nil 取决 graph_loop 实现, 本测试只验 OnAgentEnd 触发次数

	got := counter.count.Load()
	if got != 1 {
		t.Errorf("ISSUE-82 audit LOW-2 fix: maxRounds 退出 OnAgentEnd 应 fire 1 次 (Run defer 兜底), got %d 次", got)
	}
}

// bizErrTool 抛真业务错 (非 Interrupt 非 ctx.Err) — 验业务错退出路径
type bizErrTool82 struct{}

func (bizErrTool82) Name() string                { return "biz_fail" }
func (bizErrTool82) Description() string         { return "biz fail" }
func (bizErrTool82) InputSchema() map[string]any { return map[string]any{} }
func (bizErrTool82) Call(_ context.Context, _ map[string]any) (*tool.Result, error) {
	return nil, &nonInterruptError{msg: "真业务错"}
}

type nonInterruptError struct{ msg string }

func (e *nonInterruptError) Error() string { return e.msg }

var _ tool.Tool = bizErrTool82{}

// TestISSUE82_OnAgentEnd_TriggeredOnce_BizError 验 audit LOW-2 — 业务错(非
// InterruptError 非 ctx.Err)退出路径 OnAgentEnd 应 fire 1 次。
func TestISSUE82_OnAgentEnd_TriggeredOnce_BizError(t *testing.T) {
	counter := &onAgentEndCounter{}

	tk := agent_v2_newToolkit()
	tk.Register(bizErrTool82{})

	stub := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "tc_err", Type: "function", Function: schema.FunctionCall{Name: "biz_fail", Arguments: `{}`}},
		}),
	})

	agent := agent_v2.NewBasicAgent(
		agent_v2.WithName("test"),
		agent_v2.WithModel(stub),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(counter),
		agent_v2.WithMaxRounds(3),
	)

	_, err := agent.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("Run 应抛业务错, got nil")
	}
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Fatalf("应返非 InterruptError 业务错, got InterruptError")
	}

	got := counter.count.Load()
	if got != 1 {
		t.Errorf("ISSUE-82 audit LOW-2 fix: 业务错退出 OnAgentEnd 应 fire 1 次 (Run defer 兜底), got %d 次", got)
	}
}
