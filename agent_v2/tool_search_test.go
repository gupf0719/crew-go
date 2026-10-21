package agent_v2_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// captureToolsModel 捕获每轮 WithTools 的工具名 + 每轮 Generate 的输入 msgs，按 sequence 返响应。
// 验工具级渐进披露端到端接线：bound list 过滤 + 揭示流 + fail-closed tool_result。
type captureToolsModel struct {
	mu        sync.Mutex
	bound     [][]string          // 每次 WithTools 的工具名（按轮）
	inputs    [][]*schema.Message // 每次 Generate 的输入 msgs（按轮）
	responses []*schema.Message
	idx       int
}

func (m *captureToolsModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	m.inputs = append(m.inputs, msgs)
	out := schema.AssistantMessage("[exhausted]")
	if m.idx < len(m.responses) {
		out = m.responses[m.idx]
		m.idx++
	}
	m.mu.Unlock()
	return out, nil
}

func (m *captureToolsModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil // 退化到 Generate
}

func (m *captureToolsModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	names := make([]string, len(tools))
	for i, t := range tools {
		names[i] = t.Name
	}
	m.mu.Lock()
	m.bound = append(m.bound, names)
	m.mu.Unlock()
	return m, nil
}

var _ model.ChatModel = (*captureToolsModel)(nil)

func tsTestTool(name, desc string) tool.Tool {
	return tool.NewFuncTool(name, desc, map[string]any{"type": "object"},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Success("[" + name + " executed]"), nil
		})
}

func sliceHas(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func mcpStylePrefix(t tool.Tool) bool { return strings.HasPrefix(t.Name(), "mcp_") }

// TestToolSearch_EndToEnd：deferred 工具初始隐藏 → LLM 调 tool_search 检索 → 揭示的工具进 bind
// list 可调，未揭示的仍隐藏。验 makeModelFn 过滤 + 揭示流端到端接线。
func TestToolSearch_EndToEnd(t *testing.T) {
	tk := tool.NewToolkit()
	tk.RegisterAll(
		tsTestTool("echo", "回显输入"),           // static
		tsTestTool("mcp_x_weather", "查天气预报"), // deferred
		tsTestTool("mcp_x_email", "发送邮件"),    // deferred
	)
	m := &captureToolsModel{responses: []*schema.Message{
		// round1：调 tool_search 检索「天气」
		schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "tc1",
			Function: schema.FunctionCall{Name: "tool_search", Arguments: `{"query":"天气"}`}}}),
		// round2：调揭示出的 mcp_x_weather
		schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "tc2",
			Function: schema.FunctionCall{Name: "mcp_x_weather", Arguments: `{}`}}}),
		// round3：final
		schema.AssistantMessage("天气查到了"),
	}}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ts_e2e"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true, Deferred: mcpStylePrefix}),
		agent_v2.WithMaxRounds(5),
	)
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("查天气"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(m.bound) < 2 {
		t.Fatalf("应至少 2 轮 WithTools，实际 %d", len(m.bound))
	}

	// round1：static echo + tool_search 可见，deferred 全隐藏
	r1 := m.bound[0]
	if !sliceHas(r1, "echo") || !sliceHas(r1, "tool_search") {
		t.Errorf("round1 应含 echo + tool_search，实际 %v", r1)
	}
	if sliceHas(r1, "mcp_x_weather") || sliceHas(r1, "mcp_x_email") {
		t.Errorf("round1 deferred 工具应隐藏，实际 %v", r1)
	}

	// round2：tool_search 揭示了 mcp_x_weather → 可见；mcp_x_email 未揭示 → 仍隐藏
	r2 := m.bound[1]
	if !sliceHas(r2, "mcp_x_weather") {
		t.Errorf("round2 应揭示 mcp_x_weather 进 bind list，实际 %v", r2)
	}
	if sliceHas(r2, "mcp_x_email") {
		t.Errorf("round2 未揭示的 mcp_x_email 应仍隐藏，实际 %v", r2)
	}

	if final := agent_v2.FinalMessage(events); final == nil || !strings.Contains(final.Content, "天气") {
		t.Errorf("应有最终答案，实际 %v", final)
	}
}

// TestToolSearch_FailClosed：LLM 直接调未揭示的 deferred 工具（幻觉，不在 bind list）→ fail-closed
// 拒执行、返 [tool_not_revealed] 反馈（工具本身不被执行）。
func TestToolSearch_FailClosed(t *testing.T) {
	var executed bool
	weather := tool.NewFuncTool("mcp_x_weather", "查天气", map[string]any{"type": "object"},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			executed = true // fail-closed 应阻止它被调
			return tool.Success("天气数据"), nil
		})
	tk := tool.NewToolkit()
	tk.Register(weather)
	m := &captureToolsModel{responses: []*schema.Message{
		// round1：直接调未揭示的 mcp_x_weather（没先 tool_search）
		schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "tc1",
			Function: schema.FunctionCall{Name: "mcp_x_weather", Arguments: `{}`}}}),
		// round2：final（LLM 看到拒绝后收尾）
		schema.AssistantMessage("收到反馈"),
	}}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ts_failclosed"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true, Deferred: mcpStylePrefix}),
		agent_v2.WithMaxRounds(5),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("查天气")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if executed {
		t.Error("fail-closed 应阻止未揭示的 deferred 工具被执行")
	}
	// round2 的输入 msgs 应含 round1 那个 [tool_not_revealed] tool_result
	if len(m.inputs) < 2 {
		t.Fatalf("应至少 2 轮 Generate，实际 %d", len(m.inputs))
	}
	foundReject := false
	for _, msg := range m.inputs[1] {
		if strings.Contains(msg.Content, "tool_not_revealed") {
			foundReject = true
		}
	}
	if !foundReject {
		t.Error("round2 输入应含 [tool_not_revealed] 拒绝反馈")
	}
}

// TestToolSearch_NonCandidateMatchesExplained：自定义检索器只返回不需要解锁的工具（本来就可见的、不存在的）——
// 什么都不解锁，回执告诉模型这些工具不需要解锁、可见的直接调，不说「可能不存在」。曾经把这种情况一律回成
// 「没有匹配……所需能力可能不存在」，能力明明就在可用列表里，模型被诱导放弃（审计实测）。
func TestToolSearch_NonCandidateMatchesExplained(t *testing.T) {
	tk := tool.NewToolkit()
	for _, n := range []string{"mcp_x_lookup", "visible"} {
		tk.Register(tool.NewFuncTool(n, n, map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("ok"), nil }))
	}
	m := &captureToolsModel{responses: []*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "tc1",
			Function: schema.FunctionCall{Name: "tool_search", Arguments: `{"query":"查资料"}`}}}),
		schema.AssistantMessage("收到"),
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("ts_noncand"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true, Deferred: mcpStylePrefix,
			Retriever: func(context.Context, string, []tool.Tool) []string { return []string{"visible", "ghost"} }}),
		agent_v2.WithMaxRounds(5))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("查")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(m.inputs) < 2 {
		t.Fatalf("应至少 2 轮 Generate，实际 %d", len(m.inputs))
	}
	var result string
	for _, msg := range m.inputs[1] {
		if msg.Role == schema.RoleTool {
			result = msg.Content
		}
	}
	if !strings.Contains(result, "不需要解锁") || !strings.Contains(result, "visible") || strings.Contains(result, "可能不存在") {
		t.Errorf("回执应说明这些工具不需要解锁、列出它们，不说「可能不存在」，实际 %q", result)
	}
	for _, b := range m.bound[1] {
		if b == "mcp_x_lookup" {
			t.Error("检索器没返回候选里的工具，mcp_x_lookup 不该被解锁")
		}
	}
}
