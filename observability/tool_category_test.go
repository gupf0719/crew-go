// Phase 1 测试: tool category 区分 (Categorizer 接口 + activate_skill skill kind).
//
// 测试 OnToolStart 路由 spanKind 的 6 个场景:
//  1. 普通 tool (无 Extra category) → kind=tool, operation.name=tool
//  2. tool with Extra["category"]="retrieval" → kind=retrieval, operation.name=retrieval
//  3. tool with Extra["category"]="embedding" → kind=embedding
//  4. tool with Extra["category"]="guardrail" → kind=guardrail
//  5. tool with Extra["category"]="evaluator" → fallback tool (evaluator 已退化删除, 非合法 component)
//  6. activate_skill tool (in.Name==ActivateSkillToolName) → kind=skill, displayName=skill name (from args)
//
// 验证设计跟 langfuse OBSERVATION_TYPE 同款 — span kind 决定监控 UI 渲染图标/颜色.
package observability

import (
	"context"
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/modules/common"
	"crew-go/schema"
)

// runToolFlow 模拟一次 BasicAgent Run tool 调用完整序列, 返 mock collector 收到的 span.
//
// 时序: OnAgentStart → OnGraphStart → OnToolStart → OnToolEnd → OnAgentEnd → OnGraphEnd
func runToolFlow(t *testing.T, ti *schema.ToolInput) []*tracepb.Span {
	t.Helper()
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := context.Background()
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()

	info := &hook.AgentInfo{Name: "test_agent", Type: "base", Round: 0}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{Messages: []*schema.Message{schema.UserMessage("hi")}})
	_, _ = orch.OnGraphStart(ctx)

	_, _ = ah.OnToolStart(ctx, info, ti)
	_, _ = ah.OnToolEnd(ctx, info, &schema.ToolOutput{
		CallId: ti.CallId,
		Result: &common.ToolCallResult{Success: true, Output: "ok"},
	})

	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{Messages: []*schema.Message{schema.AssistantMessage("done")}})
	orch.OnGraphEnd(ctx, nil)

	time.Sleep(500 * time.Millisecond)
	return srv.Spans()
}

// findSpanByOperation 在 spans 内找 gen_ai.operation.name 匹配的第一个 span.
func findSpanByOperation(spans []*tracepb.Span, op string) *tracepb.Span {
	for _, s := range spans {
		for _, a := range s.Attributes {
			if a.Key == "gen_ai.operation.name" {
				if sv, ok := a.Value.Value.(*commonpb.AnyValue_StringValue); ok && sv.StringValue == op {
					return s
				}
			}
		}
	}
	return nil
}

// findAttrString 取 span 内指定 attr 的 string 值.
func findAttrString(s *tracepb.Span, key string) string {
	for _, a := range s.Attributes {
		if a.Key == key {
			if sv, ok := a.Value.Value.(*commonpb.AnyValue_StringValue); ok {
				return sv.StringValue
			}
		}
	}
	return ""
}

// ─────────────────────────────────────────────────────────────────────────
// 测试 1: 普通 tool (无 Categorizer) → 默认 tool kind
// ─────────────────────────────────────────────────────────────────────────

func TestToolCategory_Default_NoCategory(t *testing.T) {
	spans := runToolFlow(t, &schema.ToolInput{
		Name:          "calculator",
		CallId:        "call_1",
		ArgumentsJSON: `{"expr":"2+2"}`,
		// 无 Extra — 默认普通 tool
	})

	got := findSpanByOperation(spans, "tool")
	if got == nil {
		t.Fatalf("应有 operation.name=tool span (default fallback), got spans=%d", len(spans))
	}
	if name := findAttrString(got, "gen_ai.node.name"); name != "calculator" {
		t.Errorf("普通 tool node.name 应=calculator, got %q", name)
	}
	// 不应有 retrieval/embedding/guardrail/skill
	for _, op := range []string{"retrieval", "embedding", "guardrail", "skill"} {
		if s := findSpanByOperation(spans, op); s != nil {
			t.Errorf("普通 tool 不应 emit %s span", op)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────
// 测试 2-5: Extra["category"] 路由到对应 span kind
// ─────────────────────────────────────────────────────────────────────────

func TestToolCategory_Retrieval(t *testing.T) {
	spans := runToolFlow(t, &schema.ToolInput{
		Name:          "vector_search",
		CallId:        "call_2",
		ArgumentsJSON: `{"query":"go concurrency"}`,
		Extra:         map[string]any{"category": "retrieval"},
	})
	got := findSpanByOperation(spans, "retrieval")
	if got == nil {
		t.Fatalf("应有 operation.name=retrieval span")
	}
	if name := findAttrString(got, "gen_ai.node.name"); name != "vector_search" {
		t.Errorf("retrieval node.name 应=vector_search, got %q", name)
	}
}

func TestToolCategory_Embedding(t *testing.T) {
	spans := runToolFlow(t, &schema.ToolInput{
		Name:          "openai_embed",
		CallId:        "call_3",
		ArgumentsJSON: `{"text":"hello"}`,
		Extra:         map[string]any{"category": "embedding"},
	})
	if findSpanByOperation(spans, "embedding") == nil {
		t.Fatalf("应有 operation.name=embedding span")
	}
}

func TestToolCategory_Guardrail(t *testing.T) {
	spans := runToolFlow(t, &schema.ToolInput{
		Name:          "pii_check",
		CallId:        "call_4",
		ArgumentsJSON: `{"content":"..."}`,
		Extra:         map[string]any{"category": "guardrail"},
	})
	if findSpanByOperation(spans, "guardrail") == nil {
		t.Fatalf("应有 operation.name=guardrail span")
	}
}

// TestToolCategory_EvaluatorFallbackTool — evaluator 退化删除后, 业务给 Extra["category"]
// ="evaluator" 不再是合法 component, fallback tool kind (CS-OSS/OTel operation.name 无
// evaluator, LLM-as-judge 走默认 tool). 防回归: 若有人加回 evaluator 合法值, 此测 FAIL.
func TestToolCategory_EvaluatorFallbackTool(t *testing.T) {
	spans := runToolFlow(t, &schema.ToolInput{
		Name:          "llm_judge",
		CallId:        "call_5",
		ArgumentsJSON: `{"output":"x"}`,
		Extra:         map[string]any{"category": "evaluator"},
	})
	// evaluator 已退化 → 不创 evaluator span
	if findSpanByOperation(spans, "evaluator") != nil {
		t.Fatalf("evaluator 已退化删除, 不应创 operation.name=evaluator span (应 fallback tool)")
	}
	// fallback tool kind
	if findSpanByOperation(spans, "tool") == nil {
		t.Fatalf("evaluator category 应 fallback tool span, got ops=%v", spanOps(spans))
	}
}

// ─────────────────────────────────────────────────────────────────────────
// 测试 6: activate_skill 工具 → skill kind, displayName=skill name from args
// 重做 task #64 的 skill kind 区分 (前面 revert 时丢了).
// ─────────────────────────────────────────────────────────────────────────

func TestToolCategory_ActivateSkill_SkillKind(t *testing.T) {
	spans := runToolFlow(t, &schema.ToolInput{
		Name:          agent_v2.ActivateSkillToolName, // "activate_skill"
		CallId:        "call_6",
		ArgumentsJSON: `{"skill":"code-review","query":"..."}`,
	})

	got := findSpanByOperation(spans, "skill")
	if got == nil {
		t.Fatalf("activate_skill tool 应 emit operation.name=skill span")
	}
	// displayName 应是 args.skill 字段值, 不是 tool name "activate_skill"
	if name := findAttrString(got, "gen_ai.node.name"); name != "code-review" {
		t.Errorf("skill span node.name 应=code-review (from args), got %q", name)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// 测试 7: activate_skill 优先级高于 Extra category (theoretical edge)
// 即使业务给 activate_skill 加了 Extra["category"]="retrieval", 仍走 skill 路径.
// ─────────────────────────────────────────────────────────────────────────

func TestToolCategory_ActivateSkill_PriorityOverExtra(t *testing.T) {
	spans := runToolFlow(t, &schema.ToolInput{
		Name:          agent_v2.ActivateSkillToolName,
		CallId:        "call_7",
		ArgumentsJSON: `{"skill":"sql-query","query":"..."}`,
		Extra:         map[string]any{"category": "retrieval"}, // 业务即使错误打了 retrieval
	})
	if findSpanByOperation(spans, "skill") == nil {
		t.Fatalf("activate_skill 优先级应高于 Extra category, 期望 skill span")
	}
	if findSpanByOperation(spans, "retrieval") != nil {
		t.Errorf("activate_skill 不应被 Extra category=retrieval 覆盖")
	}
}

// ─────────────────────────────────────────────────────────────────────────
// 测试 8: 未知 category (e.g., 业务 typo "retrievalr") fallback 到 tool kind
// ─────────────────────────────────────────────────────────────────────────

func TestToolCategory_UnknownCategory_FallbackTool(t *testing.T) {
	spans := runToolFlow(t, &schema.ToolInput{
		Name:          "weird_tool",
		CallId:        "call_8",
		ArgumentsJSON: `{}`,
		Extra:         map[string]any{"category": "retrievalr"}, // typo
	})
	if findSpanByOperation(spans, "tool") == nil {
		t.Errorf("未知 category 应 fallback 到 tool kind")
	}
}

// ─────────────────────────────────────────────────────────────────────────
// 测试 9: WithToolCategoryLookup ctx helper 与 Categorizer 接口集成
// 验证: agent_v2/basic_agent.go 扫 toolkit 创 lookup → toolsNode 读 ctx 填 Extra
// 等同 Extra["category"] 路径 (端到端).
// ─────────────────────────────────────────────────────────────────────────

func TestToolCategory_CtxLookupHelper(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	// 模拟 framework 通过 WithToolCategoryLookup push 一个 lookup 函数
	ctx := context.Background()
	ctx = compose_v2.WithToolCategoryLookup(ctx, func(name string) string {
		if name == "my_retrieval" {
			return "retrieval"
		}
		return ""
	})

	// fire OnToolStart, framework 应在 fire 之前从 ctx 取 lookup + 填 Extra["category"]
	// (这部分由 agent_v2/basic_builder.go::toolsNode 实现, observability hook 不参与)
	// 测试模拟实际是 caller 提前填 Extra (跟 toolsNode 行为对齐)
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	info := &hook.AgentInfo{Name: "test_agent", Type: "base", Round: 0}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	_, _ = orch.OnGraphStart(ctx)

	ti := &schema.ToolInput{Name: "my_retrieval", CallId: "c", ArgumentsJSON: `{}`}
	// 模拟 toolsNode 用 ctx lookup 填 Extra
	if lookup := compose_v2.GetToolCategoryLookup(ctx); lookup != nil {
		if cat := lookup(ti.Name); cat != "" {
			if ti.Extra == nil {
				ti.Extra = make(map[string]any)
			}
			ti.Extra["category"] = cat
		}
	}

	_, _ = ah.OnToolStart(ctx, info, ti)
	_, _ = ah.OnToolEnd(ctx, info, &schema.ToolOutput{CallId: "c", Result: &common.ToolCallResult{Success: true}})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	orch.OnGraphEnd(ctx, nil)

	time.Sleep(500 * time.Millisecond)
	spans := srv.Spans()
	if findSpanByOperation(spans, "retrieval") == nil {
		t.Errorf("ctx lookup helper 应让 tool 走 retrieval kind")
	}
}
