package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// ────────────────────────────────────────────────────────────────────────
// audit fix eaa3fdf HIGH-1 (假绿炸弹) — 端到端 Categorizer 真路径测试.
//
// 之前 observability/tool_category_test.go 9 测试都手动填 ti.Extra["category"],
// 绕开 framework toolsNode 内的 categoryLookup 路径. sanity-revert
// `agent_v2/basic_builder.go::toolsNode` 内 categoryLookup 整段删 → 9 测试仍 PASS.
//
// 本测试走真路径: 业务 retrieval tool 实现 tool.Categorizer → BasicAgent.Run 入口
// 自动 pushToolCategorizerCtx → compose_v2.Run → toolsNode 内 fire OnToolStart 之前
// 查 ctx lookup 填 ti.Extra → 业务 hook 拦截看到 framework 填的 category.
//
// **sanity-revert 保证**:
//  1. 删 `agent_v2/basic_agent.go:197 pushToolCategorizerCtx` → 测试 FAIL (Extra nil)
//  2. 删 `agent_v2/basic_builder.go::toolsNode` 内 categoryLookup 填 Extra → FAIL
//  3. 业务 tool 不实现 Categorizer → Extra 不含 category (向后兼容验证)
// ────────────────────────────────────────────────────────────────────────

// retrievalTool 模拟业务自定义 RAG 检索 tool, 实现 tool.Categorizer.
type retrievalTool struct{}

func (r *retrievalTool) Name() string        { return "vector_search" }
func (r *retrievalTool) Description() string { return "向量库 RAG 检索" }
func (r *retrievalTool) InputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{"type": "string"},
		},
		"required": []string{"query"},
	}
}
func (r *retrievalTool) Call(_ context.Context, _ map[string]any) (*tool.Result, error) {
	return tool.Success("doc1"), nil
}
func (r *retrievalTool) Category() string { return tool.CategoryRetrieval }

// plainTool 不实现 Categorizer, 走默认 tool kind.
type plainTool struct{}

func (p *plainTool) Name() string        { return "echo" }
func (p *plainTool) Description() string { return "回显" }
func (p *plainTool) InputSchema() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{"msg": map[string]any{"type": "string"}},
		"required":   []string{"msg"},
	}
}
func (p *plainTool) Call(_ context.Context, _ map[string]any) (*tool.Result, error) {
	return tool.Success("echoed"), nil
}

// categoryInterceptor 业务 hook 拦截 OnToolStart, 看 framework 填的 ti.Extra.
type categoryInterceptor struct {
	hook.NoOpHooks // 嵌入默认实现, 仅覆写 OnToolStart
	trackAll       bool
	toolStartCount int
	lastExtra      map[string]any
	allExtras      []map[string]any
}

func (i *categoryInterceptor) OnToolStart(ctx context.Context, info *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	if in == nil {
		return ctx, nil
	}
	i.toolStartCount++
	// 深拷贝 Extra 避免后续修改污染
	extraCopy := make(map[string]any)
	for k, v := range in.Extra {
		extraCopy[k] = v
	}
	i.lastExtra = extraCopy
	if i.trackAll {
		i.allExtras = append(i.allExtras, extraCopy)
	}
	return ctx, nil
}

// ──────────────────── TEST 1: retrieval tool 端到端 ────────────────────

func TestCategorizer_E2E_RetrievalToolGetsCategory(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(&retrievalTool{})

	tc := schema.ToolCall{ID: "tc_1", Function: schema.FunctionCall{Name: "vector_search", Arguments: `{"query":"go"}`}}
	resp1 := schema.AssistantToolCallMessage([]schema.ToolCall{tc})
	resp2 := schema.AssistantMessage("done")
	m := newStubModel([]*schema.Message{resp1, resp2})

	interceptor := &categoryInterceptor{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("rag_agent"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(interceptor),
		agent_v2.WithMaxRounds(3),
	)

	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("search go"))
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if interceptor.toolStartCount == 0 {
		t.Fatal("OnToolStart 应被 fire 至少 1 次")
	}
	cat, ok := interceptor.lastExtra["category"].(string)
	if !ok {
		t.Fatalf("framework 应自动填 ti.Extra[category], 业务 tool 实现 Categorizer 接口. got Extra=%v", interceptor.lastExtra)
	}
	if cat != tool.CategoryRetrieval {
		t.Errorf("category 应=retrieval, got %q", cat)
	}
}

// ──────────────────── TEST 2: plain tool 不填 category ────────────────────

func TestCategorizer_E2E_PlainToolNoCategory(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(&plainTool{})

	tc := schema.ToolCall{ID: "tc_1", Function: schema.FunctionCall{Name: "echo", Arguments: `{"msg":"hi"}`}}
	resp1 := schema.AssistantToolCallMessage([]schema.ToolCall{tc})
	resp2 := schema.AssistantMessage("done")
	m := newStubModel([]*schema.Message{resp1, resp2})

	interceptor := &categoryInterceptor{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("plain_agent"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(interceptor),
		agent_v2.WithMaxRounds(3),
	)

	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("echo"))
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if interceptor.toolStartCount == 0 {
		t.Fatal("OnToolStart 应被 fire")
	}
	if cat, ok := interceptor.lastExtra["category"].(string); ok && cat != "" {
		t.Errorf("plain tool 不应有 category (未实现 Categorizer), got %q", cat)
	}
}

// ──────────────────── TEST 3: 混合 toolkit, 不同 tool 区分 ──────────

func TestCategorizer_E2E_MixedToolkit(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(&retrievalTool{}) // 实现 Categorizer
	tk.Register(&plainTool{})     // 不实现

	tc1 := schema.ToolCall{ID: "tc_1", Function: schema.FunctionCall{Name: "vector_search", Arguments: `{"query":"x"}`}}
	resp1 := schema.AssistantToolCallMessage([]schema.ToolCall{tc1})
	tc2 := schema.ToolCall{ID: "tc_2", Function: schema.FunctionCall{Name: "echo", Arguments: `{"msg":"y"}`}}
	resp2 := schema.AssistantToolCallMessage([]schema.ToolCall{tc2})
	resp3 := schema.AssistantMessage("done")
	m := newStubModel([]*schema.Message{resp1, resp2, resp3})

	interceptor := &categoryInterceptor{trackAll: true}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("mixed_agent"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(interceptor),
		agent_v2.WithMaxRounds(5),
	)

	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go"))
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if len(interceptor.allExtras) < 2 {
		t.Fatalf("应至少 fire 2 次 OnToolStart, got %d", len(interceptor.allExtras))
	}

	// 验第一次 (vector_search): category=retrieval
	if cat, _ := interceptor.allExtras[0]["category"].(string); cat != tool.CategoryRetrieval {
		t.Errorf("第 1 次 OnToolStart (retrieval tool) 应 category=retrieval, got %q", cat)
	}
	// 验第二次 (echo): 无 category
	if cat, _ := interceptor.allExtras[1]["category"].(string); cat != "" {
		t.Errorf("第 2 次 OnToolStart (plain tool) 不应有 category, got %q", cat)
	}
}
