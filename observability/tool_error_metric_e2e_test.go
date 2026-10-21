package observability_test

// gen_ai_tool_errors 的**真端到端**回归（issue-95 finding 3）。
//
// **为什么已有的 TestPhase2Sub5c_ToolCallDurationAndError 抓不到这个 bug（假绿范式）**：
// 那条测试直接调 `hooks.OnToolEnd(ctx, info, &schema.ToolOutput{Result: &schema.ToolCallResult{
// Success: false, Error: "boom"}})` —— **手工构造**了它想要的输入，从没经过 BasicAgent /
// makeToolsFn / toolsNode。于是它证明的是"消费方在 Success=false 时会计数"，而**生产方
// 到底会不会发出 false 从没被测过**。实际生产方硬编码 `Success: true`，这个 metric 对
// 工具失败恒为 0 —— bug 就这么在 440 个绿测试里活了下来。
//
// 教训同 CLAUDE.md 5 类假绿炸弹 #5（mock 不读 input 返固定响应）：**测试自己构造了被测
// 链路本该产出的东西**，就只剩自己测自己。故本文件坚持从 `ag.Run()` 入口跑真链路。
//
// sanity-revert：把 basic_builder.go 读 annotation 还原 Success/Error/ErrorCode 那段删掉
// （退回硬编码 Success: true）→ 本测试 fail（Counter 0，want 1）。

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/observability"
	"crew-go/schema"
	"crew-go/tool"
)

// mockChatToolErr：首轮发 tool_call，见到 tool_result 后收尾。
type mockChatToolErr struct{ toolName string }

func (m *mockChatToolErr) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *mockChatToolErr) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == schema.RoleUser {
			break
		}
		if msgs[i].Role == schema.RoleTool {
			return schema.AssistantMessage("done"), nil
		}
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "call_1", Type: "function",
		Function: schema.FunctionCall{Name: m.toolName, Arguments: `{}`},
	}}), nil
}

func (m *mockChatToolErr) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(resp)
	schema.CloseSend(sr)
	return sr, nil
}

var _ model.ChatModel = (*mockChatToolErr)(nil)

// runAgentWithTool 跑一轮真 BasicAgent（LLM 调 tool → tool 返 res → LLM 收尾），
// 全程接 observability hooks。
func runAgentWithTool(t *testing.T, obs *observability.Observability, agentName, sid string, tl tool.Tool) {
	t.Helper()
	tk := tool.NewToolkit()
	tk.Register(tl)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName(agentName),
		agent_v2.WithModel(&mockChatToolErr{toolName: tl.Name()}),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(obs.AgentHooks()),
		agent_v2.WithMaxRounds(5),
	)
	ctx := compose_v2.WithSessionIDCtx(context.Background(), sid)
	ctx = agent_v2.WithCtxOrchHooks(ctx, obs.OrchHooks())
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	obs.CleanupSession(sid)
}

type failingTool struct{}

func (failingTool) Name() string        { return "flaky_api" }
func (failingTool) Description() string { return "外部 API，会失败" }
func (failingTool) InputSchema() map[string]any {
	return map[string]any{"type": "object", "properties": map[string]any{}}
}
func (failingTool) Call(_ context.Context, _ map[string]any) (*tool.Result, error) {
	return tool.Failure("rate_limited", "upstream 429"), nil
}

type okTool struct{}

func (okTool) Name() string        { return "ok_api" }
func (okTool) Description() string { return "正常工具" }
func (okTool) InputSchema() map[string]any {
	return map[string]any{"type": "object", "properties": map[string]any{}}
}
func (okTool) Call(_ context.Context, _ map[string]any) (*tool.Result, error) {
	return tool.Success("fine"), nil
}

// TestToolErrorMetric_E2E_BizFailureCounted 是 finding 3 的终点断言：业务工具
// tool.Failure 时，跑完真 BasicAgent 后 gen_ai_tool_errors 必须真的 +1。
func TestToolErrorMetric_E2E_BizFailureCounted(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	runAgentWithTool(t, obs, "e2e_fail_agent", "e2e_fail_sid", failingTool{})

	c := obs.Counter(observability.MetricToolErrorTotal, map[string]string{
		"gen_ai_agent_name": "e2e_fail_agent",
		"gen_ai_tool_name":  "flaky_api",
	})
	if c == nil || c.Value() != 1 {
		got := "nil"
		if c != nil {
			got = string(rune('0' + c.Value()))
		}
		t.Errorf("业务工具 tool.Failure 后 %s{tool=flaky_api} 应 = 1, got %s\n"+
			"  这是 CLAUDE.md 承诺的 24 个 framework auto-fire metric 之一——恒 0 意味着业务"+
			"照 metric 协议建的工具告警永不响（工具挂了看不见）",
			observability.MetricToolErrorTotal, got)
	}
}

// TestToolErrorMetric_E2E_SuccessNotCounted 守另一半：成功工具不能误计（否则修完
// finding 3 变成"所有工具都算错误"，错误率恒 100%）。
func TestToolErrorMetric_E2E_SuccessNotCounted(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	runAgentWithTool(t, obs, "e2e_ok_agent", "e2e_ok_sid", okTool{})

	c := obs.Counter(observability.MetricToolErrorTotal, map[string]string{
		"gen_ai_agent_name": "e2e_ok_agent",
		"gen_ai_tool_name":  "ok_api",
	})
	if c != nil && c.Value() != 0 {
		t.Errorf("成功工具不该计 %s，got %d", observability.MetricToolErrorTotal, c.Value())
	}
}
