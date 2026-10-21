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

// span topology 测试 — 验"agent 作 root, graph 不发独立 span (顶层 + 嵌套都不发)"
// 设计跟 eino ADK 对齐. 详见调研报告 task#67-#70.
//
// 5 个场景测:
//
//	Test 1: 顶层 agent_v2.Run — 1 个 agent root span 无 graph span
//	Test 2: 嵌套 sub-agent — parent = 父 agent span
//	Test 3: ParallelAgent 多分支 sub-agent — 都 parent = 父 agent
//	Test 4: 多层嵌套 (3 层) — 父子链正确
//	Test 5: 业务直接 compose_v2.Run 不走 agent_v2 — graph 作 root
//
// 验证方法: 模拟 hook fire 序列 → capture spans → 断 (1) span 数量 (2) parent-child
// 关系 (3) operation.name 类型分布.

// 共享 helper: capture spans + 拿指定 operation.name 的 spans.
func captureByOp(spans []*tracepb.Span, op string) []*tracepb.Span {
	out := []*tracepb.Span{}
	for _, s := range spans {
		if v, _ := findAttr(s, "gen_ai.operation.name"); v == op {
			out = append(out, s)
		}
	}
	return out
}

// findSpanByName 按 gen_ai.node.name 找 span.
func findSpanByName(spans []*tracepb.Span, name string) *tracepb.Span {
	for _, s := range spans {
		if v, _ := findAttr(s, "gen_ai.node.name"); v == name {
			return s
		}
	}
	return nil
}

// Test 1: 顶层 agent_v2.Run — agent 是 root, 无 graph span.
//
// 模拟时序:
//
//	OnAgentStart(ctx, name="chat_entry") [T0]
//	OnGraphStart(ctx)                    [T1, 应跳过创 graph span]
//	OnGraphEnd(ctx)                      [T2]
//	OnAgentEnd(ctx, name="chat_entry")   [T3]
//
// 期望:
//   - spans 内只有 1 个 agent span (operation.name=agent)
//   - 没 graph span (operation.name=workflow)
//   - agent span ParentSpanId 为空 (root)
//   - agent span EndTime > StartTime (正常时长)
func TestSpanTopology_TopLevelAgentIsRoot(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := context.Background()
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()

	info := &hook.AgentInfo{Name: "chat_entry", Type: "base", Round: 0}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{Messages: []*schema.Message{schema.UserMessage("hi")}})
	time.Sleep(2 * time.Millisecond) // 让 graph start time 晚于 agent
	_, _ = orch.OnGraphStart(ctx)
	orch.OnGraphEnd(ctx, nil)
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{Messages: []*schema.Message{schema.AssistantMessage("done")}})

	time.Sleep(500 * time.Millisecond)
	spans := srv.Spans()

	agentSpans := captureByOp(spans, "agent")
	graphSpans := captureByOp(spans, "workflow")

	if len(agentSpans) != 1 {
		t.Fatalf("应有 1 agent span, got %d", len(agentSpans))
	}
	if len(graphSpans) != 0 {
		t.Errorf("顶层 agent 已 fire 时 graph span 应跳过, got %d 个 workflow span", len(graphSpans))
	}
	agentSpan := agentSpans[0]
	if len(agentSpan.ParentSpanId) != 0 {
		t.Errorf("顶层 agent 应作 root (parent=空), got parent=%q", string(agentSpan.ParentSpanId))
	}
}

// Test 2: 嵌套 sub-agent — sub-agent parent = 父 agent span.
//
// 模拟时序 (父 agent 内 push graphNS 调 sub-agent.Run):
//
//	OnAgentStart(ctxRoot, name="parent")        [T0, graphNS=""]
//	OnGraphStart(ctxRoot)                       [T1, 跳过]
//	  ctxChild = WithGraphNS(ctxRoot, "sub")
//	  OnAgentStart(ctxChild, name="child")       [T2, graphNS="sub", parent=parent agent]
//	  OnGraphStart(ctxChild)                     [T3, 嵌套也跳过]
//	  OnGraphEnd(ctxChild)
//	  OnAgentEnd(ctxChild, name="child")
//	OnGraphEnd(ctxRoot)
//	OnAgentEnd(ctxRoot, name="parent")
//
// 期望:
//   - 2 个 agent span (parent + child)
//   - 0 个 graph span (顶层 + 嵌套都跳过)
//   - parent agent ParentSpanId="" (root)
//   - child agent ParentSpanId=parent agent SpanId
func TestSpanTopology_NestedSubAgent_ParentIsParentAgent(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctxRoot := context.Background()
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()

	parentInfo := &hook.AgentInfo{Name: "parent_agent", Type: "base"}
	childInfo := &hook.AgentInfo{Name: "child_agent", Type: "base"}

	_, _ = ah.OnAgentStart(ctxRoot, parentInfo, &hook.AgentInput{})
	_, _ = orch.OnGraphStart(ctxRoot)

	// 模拟父 push graphNS 调 sub-agent
	ctxChild := compose_v2.WithGraphNS(ctxRoot, "sub_ns")
	_, _ = ah.OnAgentStart(ctxChild, childInfo, &hook.AgentInput{})
	_, _ = orch.OnGraphStart(ctxChild)
	orch.OnGraphEnd(ctxChild, nil)
	ah.OnAgentEnd(ctxChild, childInfo, &hook.AgentOutput{})

	orch.OnGraphEnd(ctxRoot, nil)
	ah.OnAgentEnd(ctxRoot, parentInfo, &hook.AgentOutput{})

	time.Sleep(500 * time.Millisecond)
	spans := srv.Spans()

	agentSpans := captureByOp(spans, "agent")
	graphSpans := captureByOp(spans, "workflow")

	if len(agentSpans) != 2 {
		t.Fatalf("应有 2 agent span (parent+child), got %d", len(agentSpans))
	}
	if len(graphSpans) != 0 {
		t.Errorf("嵌套也跳过 graph span, got %d 个", len(graphSpans))
	}

	parent := findSpanByName(agentSpans, "parent_agent")
	child := findSpanByName(agentSpans, "child_agent")
	if parent == nil || child == nil {
		t.Fatalf("找不到 parent (%v) 或 child (%v) span by name", parent, child)
	}
	if len(parent.ParentSpanId) != 0 {
		t.Errorf("parent agent 应 root, got parent=%q", string(parent.ParentSpanId))
	}
	if string(child.ParentSpanId) != string(parent.SpanId) {
		t.Errorf("child agent ParentSpanId 应=parent agent SpanId, got child.parent=%q, parent.sid=%q",
			string(child.ParentSpanId), string(parent.SpanId))
	}
}

// Test 3: ParallelAgent 多分支 sub-agent — 都 parent = 父 agent.
//
// 模拟时序:
//
//	OnAgentStart(ctxRoot, "parallel") [graphNS=""]
//	OnGraphStart(ctxRoot)             [跳过]
//	  分支 a:
//	    ctxA = WithGraphNS(ctxRoot, "branch_a")
//	    OnAgentStart(ctxA, "sub_a")    [parent=parallel agent]
//	    OnAgentEnd(ctxA, "sub_a")
//	  分支 b:
//	    ctxB = WithGraphNS(ctxRoot, "branch_b")
//	    OnAgentStart(ctxB, "sub_b")    [parent=parallel agent]
//	    OnAgentEnd(ctxB, "sub_b")
//	OnGraphEnd(ctxRoot)
//	OnAgentEnd(ctxRoot, "parallel")
//
// 期望: 3 agent spans (parallel + sub_a + sub_b), sub_a/sub_b 都 parent=parallel.
func TestSpanTopology_ParallelMultipleSubAgents_AllParentToRoot(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctxRoot := context.Background()
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()

	root := &hook.AgentInfo{Name: "parallel", Type: "parallel"}
	subA := &hook.AgentInfo{Name: "sub_a", Type: "base"}
	subB := &hook.AgentInfo{Name: "sub_b", Type: "base"}

	_, _ = ah.OnAgentStart(ctxRoot, root, &hook.AgentInput{})
	_, _ = orch.OnGraphStart(ctxRoot)

	ctxA := compose_v2.WithGraphNS(ctxRoot, "branch_a")
	_, _ = ah.OnAgentStart(ctxA, subA, &hook.AgentInput{})
	ah.OnAgentEnd(ctxA, subA, &hook.AgentOutput{})

	ctxB := compose_v2.WithGraphNS(ctxRoot, "branch_b")
	_, _ = ah.OnAgentStart(ctxB, subB, &hook.AgentInput{})
	ah.OnAgentEnd(ctxB, subB, &hook.AgentOutput{})

	orch.OnGraphEnd(ctxRoot, nil)
	ah.OnAgentEnd(ctxRoot, root, &hook.AgentOutput{})

	time.Sleep(500 * time.Millisecond)
	spans := srv.Spans()

	agentSpans := captureByOp(spans, "agent")
	if len(agentSpans) != 3 {
		t.Fatalf("应有 3 agent span (parallel+sub_a+sub_b), got %d", len(agentSpans))
	}
	parent := findSpanByName(agentSpans, "parallel")
	a := findSpanByName(agentSpans, "sub_a")
	b := findSpanByName(agentSpans, "sub_b")
	if parent == nil || a == nil || b == nil {
		t.Fatalf("span 查找失败: parent=%v, a=%v, b=%v", parent, a, b)
	}
	if string(a.ParentSpanId) != string(parent.SpanId) {
		t.Errorf("sub_a parent 应=parallel SpanId, got %q vs %q", string(a.ParentSpanId), string(parent.SpanId))
	}
	if string(b.ParentSpanId) != string(parent.SpanId) {
		t.Errorf("sub_b parent 应=parallel SpanId, got %q vs %q", string(b.ParentSpanId), string(parent.SpanId))
	}
}

// Test 4: 3 层嵌套 — top → middle → leaf 父子链正确.
//
// 模拟 PlanAgent.Run → planner worker → BasicAgent → ask_translator skill fork 路径.
//
// 期望: 3 agent spans, leaf.parent=middle, middle.parent=top.
func TestSpanTopology_ThreeLevelNesting(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctxTop := context.Background()
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()

	top := &hook.AgentInfo{Name: "top", Type: "plan"}
	middle := &hook.AgentInfo{Name: "middle", Type: "base"}
	leaf := &hook.AgentInfo{Name: "leaf", Type: "base"}

	_, _ = ah.OnAgentStart(ctxTop, top, &hook.AgentInput{})
	_, _ = orch.OnGraphStart(ctxTop)

	// 真实业务用 JoinNS 拼 ns (WithGraphNS 是覆写完整 ns 不是 append, 测试模拟
	// 父 wrapper 用 JoinNS / JoinFanoutNS 算 child ns 再 WithGraphNS push).
	midNS := compose_v2.JoinNS("", "worker_step_0") // = "worker_step_0"
	ctxMid := compose_v2.WithGraphNS(ctxTop, midNS)
	_, _ = ah.OnAgentStart(ctxMid, middle, &hook.AgentInput{})
	_, _ = orch.OnGraphStart(ctxMid)

	leafNS := compose_v2.JoinNS(midNS, "skill_translate#abc") // = "worker_step_0/skill_translate#abc"
	ctxLeaf := compose_v2.WithGraphNS(ctxMid, leafNS)
	_, _ = ah.OnAgentStart(ctxLeaf, leaf, &hook.AgentInput{})
	_, _ = orch.OnGraphStart(ctxLeaf)
	orch.OnGraphEnd(ctxLeaf, nil)
	ah.OnAgentEnd(ctxLeaf, leaf, &hook.AgentOutput{})

	orch.OnGraphEnd(ctxMid, nil)
	ah.OnAgentEnd(ctxMid, middle, &hook.AgentOutput{})

	orch.OnGraphEnd(ctxTop, nil)
	ah.OnAgentEnd(ctxTop, top, &hook.AgentOutput{})

	time.Sleep(500 * time.Millisecond)
	spans := srv.Spans()

	agentSpans := captureByOp(spans, "agent")
	if len(agentSpans) != 3 {
		t.Fatalf("应有 3 agent span, got %d", len(agentSpans))
	}
	topSpan := findSpanByName(agentSpans, "top")
	midSpan := findSpanByName(agentSpans, "middle")
	leafSpan := findSpanByName(agentSpans, "leaf")
	if topSpan == nil || midSpan == nil || leafSpan == nil {
		t.Fatalf("span 查找失败")
	}
	if len(topSpan.ParentSpanId) != 0 {
		t.Errorf("top agent 应 root, got parent=%q", string(topSpan.ParentSpanId))
	}
	if string(midSpan.ParentSpanId) != string(topSpan.SpanId) {
		t.Errorf("middle parent 应=top SpanId, got %q vs %q",
			string(midSpan.ParentSpanId), string(topSpan.SpanId))
	}
	if string(leafSpan.ParentSpanId) != string(midSpan.SpanId) {
		t.Errorf("leaf parent 应=middle SpanId, got %q vs %q",
			string(leafSpan.ParentSpanId), string(midSpan.SpanId))
	}
}

// Test 5: 业务直接调 compose_v2.Run 不走 agent_v2 — graph 作 root.
//
// 模拟时序 (无 OnAgentStart fire):
//
//	OnGraphStart(ctx)        [创 graph span 作 root, 因为没 in-flight agent]
//	OnNodeStart(ctx, "node_a")
//	OnNodeEnd(ctx, "node_a")
//	OnGraphEnd(ctx)
//
// 期望: 1 graph span (root) + 1 node span (parent=graph).
func TestSpanTopology_DirectComposeRun_GraphIsRoot(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := context.Background()
	orch := obs.OrchHooks()

	_, _ = orch.OnGraphStart(ctx)
	_, _ = orch.OnNodeStart(ctx, "node_a")
	orch.OnNodeEnd(ctx, "node_a", nil)
	orch.OnGraphEnd(ctx, nil)

	time.Sleep(500 * time.Millisecond)
	spans := srv.Spans()

	graphSpans := captureByOp(spans, "workflow")
	// node span 也是 workflow operation (gen_ai.workflow.node.<id>)
	if len(graphSpans) < 1 {
		t.Fatalf("应有 graph span (root), got %d", len(graphSpans))
	}

	// 找 graph (gen_ai.node.name 是 "root" 或 graphNS) — 顶层 graphNS="" fallback "root"
	var graphSpan, nodeSpan *tracepb.Span
	for _, s := range graphSpans {
		nodeID, _ := findAttr(s, "gen_ai.node.id")
		if nodeID == "" || nodeID == "/" {
			// graph kind node.id = graphNS = ""
			graphSpan = s
		} else {
			// node kind node.id = graphNS + "/" + name = "/node_a"
			nodeSpan = s
		}
	}
	if graphSpan == nil {
		t.Fatalf("没找到 graph root span")
	}
	if nodeSpan == nil {
		t.Fatalf("没找到 node span")
	}
	if len(graphSpan.ParentSpanId) != 0 {
		t.Errorf("graph 应 root, got parent=%q", string(graphSpan.ParentSpanId))
	}
	if string(nodeSpan.ParentSpanId) != string(graphSpan.SpanId) {
		t.Errorf("node parent 应=graph SpanId, got %q vs %q",
			string(nodeSpan.ParentSpanId), string(graphSpan.SpanId))
	}
}
