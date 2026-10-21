// GraphAgent 显式编排路径 — node 组件类型 span kind 测试 (Phase 2 探索).
//
// ────────────────────────────────────────────────────────────────────────
// **缺口 (Phase 1 tool.Categorizer 够不着)**
// BasicAgent/ReAct 路径外部能力统一 tool, Phase 1 已让 tool span 标 retrieval/
// embedding/guardrail. 但 GraphAgent 显式编排路径下业务在 NodeFunc 里**直接**
// 调组件 (不经 LLM tool_call), 这些 node 现在统一 trace 成 "node" kind (operation.name
// =workflow) — 标不了组件类型.
//
// **方案 B (node 声明式标注)**: compose_v2 让 AddNode 声明组件类型, graph 执行引擎在
// fire OnNodeStart 前 WithComponentKind(ctx, kind) push, observability OnNodeStart/End
// 读它创对应 kind span 而非通用 node.
//
// **TDD 探索 (此测先看效果再决定全量实施)**: 模拟 graph 引擎已 push component=retrieval
// 的 ctx, 调 OnNodeStart/End. 实施前 FAIL (OnNodeStart 硬编码 "node" kind, operation.name
// =workflow, 找不到 retrieval span), 实施后 PASS.
// ────────────────────────────────────────────────────────────────────────
package observability

import (
	"context"
	"testing"
	"time"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// spanOps 列出 spans 所有 gen_ai.operation.name (FAIL 调试用).
func spanOps(spans []*tracepb.Span) []string {
	ops := make([]string, 0, len(spans))
	for _, s := range spans {
		ops = append(ops, findAttrString(s, "gen_ai.operation.name"))
	}
	return ops
}

// TestComponentNode_RetrievalKind — 声明 component=retrieval 的 node 应创 retrieval span.
func TestComponentNode_RetrievalKind(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := compose_v2.WithGraphNS(context.Background(), "ga_graph")
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	info := &hook.AgentInfo{Name: "rag_graph_agent", Type: "base", Round: 0}

	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{Messages: []*schema.Message{schema.UserMessage("q")}})
	_, _ = orch.OnGraphStart(ctx)

	// 模拟 compose_v2 graph 执行引擎在 fire OnNodeStart 前 push 该 node 声明的组件类型.
	nodeCtx := compose_v2.WithComponentKind(ctx, "retrieval")
	nctx, _ := orch.OnNodeStart(nodeCtx, "retrieve_docs")
	orch.OnNodeEnd(nctx, "retrieve_docs", nil)

	orch.OnGraphEnd(ctx, nil)
	_ = ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})

	time.Sleep(500 * time.Millisecond)
	spans := srv.Spans()
	if findSpanByOperation(spans, "retrieval") == nil {
		t.Fatalf("声明 component=retrieval 的 node 应创 retrieval kind span, got operations=%v\n"+
			"缺口: OnNodeStart 硬编码 \"node\" kind 没读 ctx component kind, 仍创 workflow node span", spanOps(spans))
	}
}

// TestComponentAgent_ComponentKindIgnored — agent 级 component 已退化: OnAgentStart **不**读
// ctx component kind, agent span operation.name 恒 "agent" (对齐 OTel invoke_agent).
//
// **退化背景**: 原 ReflectAgent 在调 evaluator/reflector sub-agent 前 push WithComponentKind
// (ctx, "evaluator"), OnAgentStart 读它把 agent span 标 evaluator. 但 CS-OSS/OTel
// operation.name 无 evaluator, sub-agent invocation 即 invoke_agent — 强造 evaluator
// operation.name 不对齐. evaluator 是唯一 agent 级 component 用例, 已删. component kind
// (retrieval/embedding/guardrail) 只标在 tool span 与 node span (OnNodeStart), agent 不标.
//
// **防回归**: 若有人加回 OnAgentStart 读 component 逻辑 / reflect_agent push component,
// agent span 被覆盖标 evaluator, 此测 FAIL.
func TestComponentAgent_ComponentKindIgnored(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := compose_v2.WithGraphNS(context.Background(), "reflect_graph")
	ah := obs.AgentHooks()
	orch := obs.OrchHooks()
	info := &hook.AgentInfo{Name: "my_evaluator", Type: "base", Round: 0}

	// 即使 push component=evaluator (模拟退化前 reflect_agent 行为), agent span 不被覆盖.
	evCtx := compose_v2.WithComponentKind(ctx, "evaluator")
	_, _ = ah.OnAgentStart(evCtx, info, &hook.AgentInput{Messages: []*schema.Message{schema.UserMessage("eval")}})
	_, _ = orch.OnGraphStart(evCtx)
	orch.OnGraphEnd(evCtx, nil)
	_ = ah.OnAgentEnd(evCtx, info, &hook.AgentOutput{})

	time.Sleep(500 * time.Millisecond)
	spans := srv.Spans()

	// agent span 恒标 "agent" — 不被 ctx component 覆盖 (OTel invoke_agent 对齐).
	if findSpanByOperation(spans, "agent") == nil {
		t.Fatalf("agent span 应 operation.name=agent (component 不覆盖 agent span), got ops=%v", spanOps(spans))
	}
	// 不应创 evaluator span (agent 级 component 已退化).
	if findSpanByOperation(spans, "evaluator") != nil {
		t.Errorf("agent 级 component 已退化, 不应有 evaluator operation span, got ops=%v", spanOps(spans))
	}
}
