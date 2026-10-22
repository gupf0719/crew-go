package agent_v2_test

import (
	"context"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// Phase 3 Step 1 测试：BasicAgent 业务级 hook.Hooks 集成。

// recordingAgentHooks 测试 helper——记录所有 hook 触发顺序。
type recordingAgentHooks struct {
	hook.NoOpHooks
	mu sync.Mutex
	ev []string

	// 控制 hook 行为：
	failOnModelStart error
	failOnToolStart  error
}

func (h *recordingAgentHooks) record(s string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ev = append(h.ev, s)
}

func (h *recordingAgentHooks) events() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.ev))
	copy(out, h.ev)
	return out
}

func (h *recordingAgentHooks) OnAgentStart(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	h.record("agent_start:" + info.Name + ":r" + intStr(info.Round))
	return ctx, nil
}

func (h *recordingAgentHooks) OnAgentEnd(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	h.record("agent_end:" + info.Name + ":r" + intStr(info.Round))
	return ctx
}

func (h *recordingAgentHooks) OnModelStart(ctx context.Context, info *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	h.record("model_start:r" + intStr(info.Round))
	return ctx, h.failOnModelStart
}

func (h *recordingAgentHooks) OnModelEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	tcCount := len(schema.GetToolCalls(out.Message))
	h.record("model_end:r" + intStr(info.Round) + ":tc=" + intStr(int32(tcCount)))
	return ctx, nil
}

func (h *recordingAgentHooks) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	h.record("tool_start:" + in.Name)
	return ctx, h.failOnToolStart
}

func (h *recordingAgentHooks) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	h.record("tool_end:" + out.CallId)
	return ctx, nil
}

func (h *recordingAgentHooks) OnError(ctx context.Context, _ *hook.AgentInfo, e *schema.CommError) context.Context {
	h.record("error:" + e.Code)
	return ctx
}

func intStr(n int32) string {
	if n == 0 {
		return "0"
	}
	if n < 10 {
		return string(rune('0' + n))
	}
	// 简化——PoC 测试 round < 100
	return string(rune('0'+n/10)) + string(rune('0'+n%10))
}

// ── 旗舰：完整 ReAct 循环触发 hook 时序 ──

func TestAgentHooks_FullReActLifecycle(t *testing.T) {
	llm := &mockLLM{} // 复用 basic_agent_test.go 的 mockLLM（2 轮：tool_call + final）
	hooks := &recordingAgentHooks{}

	model := func(_ context.Context, _, msgs []*schema.Message) (*schema.Message, error) {
		return llm.Generate(context.Background(), msgs)
	}
	tools := func(ctx context.Context, tcs []schema.ToolCall, _ []context.Context) ([]*schema.Message, error) {
		var out []*schema.Message
		for _, tc := range tcs {
			r, _ := mockTool(ctx, tc.Function.Name, tc.Function.Arguments)
			out = append(out, schema.ToolResultMessage(tc.ID, r))
		}
		return out, nil
	}

	g, err := agent_v2.NewBasicAgentBuilder(model, tools).
		WithName("test_agent").
		WithAgentHooks(hooks).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentInputWrites(schema.UserMessage("北京天气"))...),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 验证 hook 时序 (compose_v2 builder 直接跑, 非 agent_v2 wrapper):
	// 第 1 轮 (round=0): model_start:r0 → model_end:r0:tc=1 → tool_start:get_weather → tool_end:call_1
	// 第 2 轮 (round=1): model_start:r1 → model_end:r1:tc=0
	//
	// **ISSUE-82 fix**: 删 `agent_end` 期望项 — agent-level lifecycle 由 agent_v2 wrapper 单一所有权 fire.
	//
	// **8-method 协议**: 删 `agent_start` 期望项 — OnAgentStart 整个调用 1 次, 由 agent_v2
	// wrapper Run/Resume top-level fire (跟 OnAgentEnd 对称), compose_v2 builder NodeFunc 内不再 fire.
	// 业务直接用 agent_v2.NewBasicAgentBuilder 想要 agent-level hook 必须改用 agent_v2.BasicAgent.
	got := hooks.events()
	expected := []string{
		"model_start:r0",
		"model_end:r0:tc=1",
		"tool_start:get_weather",
		"tool_end:call_1",
		"model_start:r1",
		"model_end:r1:tc=0",
	}
	if !sliceEqual(got, expected) {
		t.Fatalf("hook 时序错\nexpected: %v\ngot:      %v", expected, got)
	}
}

// ── OnModelStart 修改 messages 反映到 model fn ──

func TestAgentHooks_OnModelStartCanModifyInput(t *testing.T) {
	hooks := &modifyHooks{}
	var receivedMsgs []*schema.Message

	model := func(_ context.Context, _, msgs []*schema.Message) (*schema.Message, error) {
		receivedMsgs = msgs
		return schema.AssistantMessage("ok"), nil
	}

	g, _ := agent_v2.NewBasicAgentBuilder(model, nil).WithAgentHooks(hooks).Compile()

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentInputWrites(schema.UserMessage("original"))...),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// hook 应在 OnModelStart 添加一条 system message
	if len(receivedMsgs) != 2 {
		t.Fatalf("model 应收到 2 条（user + hook 加的 system），实际 %d", len(receivedMsgs))
	}
	if receivedMsgs[0].Role != schema.RoleSystem {
		t.Fatalf("第 1 条应是 hook 加的 system，实际 %s", receivedMsgs[0].Role)
	}
	if receivedMsgs[0].Content != "INJECTED_BY_HOOK" {
		t.Fatalf("system content 应是 hook 注入，实际 %q", receivedMsgs[0].Content)
	}
}

type modifyHooks struct {
	hook.NoOpHooks
}

func (h *modifyHooks) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	// 在 user 消息前注入 system
	in.Messages = append(
		[]*schema.Message{schema.SystemMessage("INJECTED_BY_HOOK")},
		in.Messages...,
	)
	return ctx, nil
}

// ── nil hooks 安全 ──

func TestAgentHooks_NilHooksSafe(t *testing.T) {
	llm := &directAnswerLLM{}
	model := func(_ context.Context, _, _ []*schema.Message) (*schema.Message, error) {
		return llm.Generate(context.Background(), nil)
	}
	g, _ := agent_v2.NewBasicAgentBuilder(model, nil).Compile() // 无 WithAgentHooks
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentInputWrites(schema.UserMessage("hi"))...),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// ── OnToolStart 改 Name / ArgumentsJSON → toolsNode 消费后喂业务工具调用 ──
//
// 回归哨兵（sanity-revert 友好）：revert toolsNode 的 `toolCalls[i].Function.Name =
// ti.Name` / `Arguments = ti.ArgumentsJSON` 后，业务工具收到原始 "orig_tool" /
// "ORIGINAL"，断言 fail 精确指向"OnToolStart 改写未消费"。
func TestAgentHooks_OnToolStartConsumesArgRewrite(t *testing.T) {
	var capturedName, capturedArgs string
	round := 0
	model := func(_ context.Context, _, _ []*schema.Message) (*schema.Message, error) {
		round++
		if round == 1 {
			return schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID:       "call_1",
				Type:     "function",
				Function: schema.FunctionCall{Name: "orig_tool", Arguments: `{"q":"ORIGINAL"}`},
			}}), nil
		}
		return schema.AssistantMessage("done"), nil
	}
	// mock 读 input 做断言（CLAUDE.md：mock 必须读 input，否则抓不到上游吞改写）
	tools := func(_ context.Context, tcs []schema.ToolCall, _ []context.Context) ([]*schema.Message, error) {
		capturedName = tcs[0].Function.Name
		capturedArgs = tcs[0].Function.Arguments
		return []*schema.Message{schema.ToolResultMessage(tcs[0].ID, "raw")}, nil
	}

	g, _ := agent_v2.NewBasicAgentBuilder(model, tools).WithAgentHooks(&toolStartRewriteHooks{}).Compile()
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentInputWrites(schema.UserMessage("hi"))...),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if capturedName != "rewritten_tool" {
		t.Fatalf("OnToolStart 改 Name 未被消费：业务工具收到 %q，期望 rewritten_tool", capturedName)
	}
	if capturedArgs != `{"q":"REWRITTEN"}` {
		t.Fatalf("OnToolStart 改 ArgumentsJSON 未被消费：业务工具收到 %q，期望 REWRITTEN", capturedArgs)
	}
}

type toolStartRewriteHooks struct{ hook.NoOpHooks }

func (h *toolStartRewriteHooks) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	in.Name = "rewritten_tool"
	in.ArgumentsJSON = `{"q":"REWRITTEN"}`
	in.CallId = "TAMPERED_ID" // [只读] 配对锚点：改它不应影响配对，下方测试验证 tool_result 仍配原始 ID
	return ctx, nil
}

// ── OnToolEnd 改 Result.Output → toolsNode 消费后写回 LLM 历史；CallId 只读 ──
//
// 回归哨兵：revert toolsNode 的 `r.Content = content` + Toolresult.Content 写回后，
// model 第 2 轮看到原始 "secret_data"，断言 fail 精确指向"OnToolEnd 改写未消费"。
func TestAgentHooks_OnToolEndConsumesResultRewrite(t *testing.T) {
	var toolResultSeenByModel, toolCallIDSeenByModel string
	round := 0
	model := func(_ context.Context, _, msgs []*schema.Message) (*schema.Message, error) {
		round++
		if round == 1 {
			return schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID:       "call_1",
				Type:     "function",
				Function: schema.FunctionCall{Name: "t", Arguments: "{}"},
			}}), nil
		}
		// 第 2 轮：捕获 model 看到的 tool_result content + 配对 ID
		for _, m := range msgs {
			if m.Role == schema.RoleTool {
				toolResultSeenByModel = m.Content
				toolCallIDSeenByModel = schema.GetToolResultID(m)
			}
		}
		return schema.AssistantMessage("done"), nil
	}
	tools := func(_ context.Context, tcs []schema.ToolCall, _ []context.Context) ([]*schema.Message, error) {
		return []*schema.Message{schema.ToolResultMessage(tcs[0].ID, "secret_data")}, nil
	}

	g, _ := agent_v2.NewBasicAgentBuilder(model, tools).WithAgentHooks(&toolEndRewriteHooks{}).Compile()
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentInputWrites(schema.UserMessage("hi"))...),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if toolResultSeenByModel != "[REDACTED]" {
		t.Fatalf("OnToolEnd 改 Result.Output 未被消费：model 第 2 轮看到 tool_result content=%q，期望 [REDACTED]", toolResultSeenByModel)
	}
	// CallId [只读]：hook 改了 to.CallId 也不影响配对，tool_result 仍配原始 call_1
	if toolCallIDSeenByModel != "call_1" {
		t.Fatalf("CallId 应只读（配对锚点）：tool_result 配对 ID=%q，期望保持原始 call_1", toolCallIDSeenByModel)
	}
}

type toolEndRewriteHooks struct{ hook.NoOpHooks }

func (h *toolEndRewriteHooks) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	out.Result.Output = "[REDACTED]" // [消费] 结果脱敏
	out.CallId = "TAMPERED_ID"       // [只读] 配对锚点：改它不影响 tool_result 配对
	return ctx, nil
}
