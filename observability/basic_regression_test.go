// BasicAgent observability 回归测试 — 验 Phase 2 component 改动 (graph_loop consume +
// OnAgentStart 读 component) 对 BasicAgent 现有 span 拓扑零影响.
//
// ────────────────────────────────────────────────────────────────────────
// BasicAgent ReAct 路径外部能力统一 tool, ctx 无 component 声明. 验证 component 改动后:
//   - agent span operation.name = agent (不被 component 污染)
//   - node span (preamble/model/tools/postamble) operation.name = workflow (graph_loop
//     consume set "" → OnNodeStart 读 "" → isToolKind 校验 fallback workflow)
//   - tool span operation.name = tool (Phase 1 Categorizer 路径, get_weather 不实现 Categorizer)
//   - **无**意外的 retrieval/embedding/guardrail/evaluator span
//
// 真跑 BasicAgent (mock model + tool) + obs hooks + startCaptureServer 捕获真 span.
// ────────────────────────────────────────────────────────────────────────
package observability

import (
	"context"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// mockChatRegression: 返 tool_call 然后最终答复 (照搬 observability_demo mockChat).
type mockChatRegression struct{ tools []*schema.ToolInfo }

func (m *mockChatRegression) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	cp := *m
	cp.tools = tools
	return &cp, nil
}

func (m *mockChatRegression) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	// 倒扫: 本轮 user 之后有 tool_result → 最终答复; 否则 → tool_call.
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == schema.RoleUser {
			break
		}
		if msgs[i].Role == schema.RoleTool {
			return schema.AssistantMessage("done"), nil
		}
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:       "call_1",
		Type:     "function",
		Function: schema.FunctionCall{Name: "get_weather", Arguments: `{}`},
	}}), nil
}

func (m *mockChatRegression) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(resp)
	schema.CloseSend(sr)
	return sr, nil
}

var _ model.ChatModel = (*mockChatRegression)(nil)

// weatherToolRegression: 普通 tool 不实现 Categorizer → 默认 tool kind.
type weatherToolRegression struct{}

func (w *weatherToolRegression) Name() string        { return "get_weather" }
func (w *weatherToolRegression) Description() string { return "查天气" }
func (w *weatherToolRegression) InputSchema() map[string]any {
	return map[string]any{"type": "object", "properties": map[string]any{}}
}
func (w *weatherToolRegression) Call(_ context.Context, _ map[string]any) (*tool.Result, error) {
	return tool.Success("晴 25°C"), nil
}

func TestBasicAgent_ComponentChangeNoRegression(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	tk := tool.NewToolkit()
	tk.Register(&weatherToolRegression{})
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("weather_agent"),
		agent_v2.WithModel(&mockChatRegression{}),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(obs.AgentHooks()),
		agent_v2.WithMaxRounds(5),
	)

	ctx := compose_v2.WithSessionIDCtx(context.Background(), "regr_sid")
	ctx = agent_v2.WithCtxOrchHooks(ctx, obs.OrchHooks())
	_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("天气?"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	obs.CleanupSession("regr_sid")

	time.Sleep(500 * time.Millisecond)
	spans := srv.Spans()

	// agent span = agent (ctx 无 component → OnAgentStart 不覆盖 operation.name).
	if findSpanByOperation(spans, "agent") == nil {
		t.Errorf("BasicAgent agent span 应 operation.name=agent (不被 component 污染), got ops=%v", spanOps(spans))
	}
	// node span = workflow (preamble/model/tools/postamble 无 component 声明, graph_loop
	// consume set "" → OnNodeStart fallback workflow).
	if findSpanByOperation(spans, "workflow") == nil {
		t.Errorf("BasicAgent node span 应 operation.name=workflow (无 component), got ops=%v", spanOps(spans))
	}
	// tool span = tool (Phase 1 路径, get_weather 不实现 Categorizer).
	if findSpanByOperation(spans, "tool") == nil {
		t.Errorf("BasicAgent tool span 应 operation.name=tool, got ops=%v", spanOps(spans))
	}
	// 不应有意外 component span — BasicAgent 路径无 WithNodeComponent / ReflectAgent push.
	for _, op := range []string{"retrieval", "embedding", "guardrail"} {
		if findSpanByOperation(spans, op) != nil {
			t.Errorf("BasicAgent 不应有 %s span (无 component 声明), got ops=%v", op, spanOps(spans))
		}
	}
}
