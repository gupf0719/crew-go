package observability

import (
	"context"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/hook"
)

// a2a 跨语言 trace seed 路径白盒测（第 2 步）：验证 server 侧从 remote
// TraceContext seed 后，整棵 span 树用 remote traceID + 顶层挂 remote parent。
// 现有 observability 全套测试覆盖的是**非-seed**路径（证零影响），本测试专补 seed 路径。

// TestA2ASeed_RemoteParentTraceTree 端到端：push remote TraceContext → 跑 agent
// hook 序列 → 抓上报 span → 验 TraceId=remote + 顶层 agent span 挂 remote parent。
//
// sanity-revert：删 sessionByIDSeeded 的 seed 分支（恒 GenTraceID）→ TraceId 非
// remote → fail；或删 startSpan 的 rootParentSpanID 分支（恒 rootSpanID）→ 顶层
// ParentSpanId 非 remote SpanID → fail。
func TestA2ASeed_RemoteParentTraceTree(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	// 模拟 client / Java 传来的合法 remote trace（CS-OSS 格式）
	remoteTraceID := GenTraceID("remote-client")
	remoteSpanID := GenRootSpanID()
	remoteTC := TraceContext{
		TraceID: remoteTraceID, SpanID: remoteSpanID,
		Sampled: true, AppName: "remote-client", NextIdx: 1,
	}

	// a2a server 入口：提取 metadata 后 push remote TraceContext + 独立 sid
	ctx := compose_v2.WithSessionIDCtx(context.Background(), "a2a-seed-sid")
	ctx = WithTraceContext(ctx, remoteTC)

	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	info := &hook.AgentInfo{Name: "server_agent", Type: "base", Round: 0}
	// 真实 hook 时序：OnAgentStart → OnGraphStart → ... → OnGraphEnd → OnAgentEnd
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	_, _ = orch.OnGraphStart(ctx)
	orch.OnGraphEnd(ctx, nil)
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})

	time.Sleep(400 * time.Millisecond) // 等 export tick

	spans := srv.Spans()
	if len(spans) == 0 {
		t.Fatal("没抓到任何 span")
	}

	var agentSpan *spanLike
	for _, s := range spans {
		// 所有 span 必须用 remote traceID（不本地新生成）—— 跨进程串联核心
		if string(s.TraceId) != remoteTraceID {
			t.Errorf("seed 后 span TraceId 应=remote %q，got %q（本地 GenTraceID 没被 remote 覆盖）",
				remoteTraceID, string(s.TraceId))
		}
		if s.Name == "gen_ai.agent.server_agent" {
			agentSpan = &spanLike{spanID: string(s.SpanId), parentSpanID: string(s.ParentSpanId), name: s.Name}
		}
	}
	if agentSpan == nil {
		names := make([]string, 0, len(spans))
		for _, s := range spans {
			names = append(names, s.Name)
		}
		t.Fatalf("没找到顶层 agent span gen_ai.agent.server_agent，got spans %v", names)
	}
	// 顶层 agent span 必须挂到 remote parent（不是本地 rootSpanID、不是空 root）
	if agentSpan.parentSpanID != remoteSpanID {
		t.Errorf("顶层 agent span 应挂 remote parent %q，got ParentSpanId %q（没串到 client 的 trace）",
			remoteSpanID, agentSpan.parentSpanID)
	}
	// 顶层 span 自己的 SpanId 是派生的新 id（不能顶替 remote SpanID）
	if agentSpan.spanID == remoteSpanID {
		t.Errorf("顶层 agent span SpanId 不应=remote SpanID（应派生新 child id），got %q", agentSpan.spanID)
	}
}

// TestA2ASeed_NoSeedUsesLocalTrace 对照：不带 remote TraceContext（普通本地场景）
// 仍走原 GenTraceID 路径——证旁路对非-a2a 零影响（traceID 本地生成、顶层是 root）。
func TestA2ASeed_NoSeedUsesLocalTrace(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	remoteTraceID := GenTraceID("remote-client") // 仅用于对照，不 push
	ctx := compose_v2.WithSessionIDCtx(context.Background(), "no-seed-sid")
	// 注意：不 WithTraceContext —— 模拟普通本地 agent

	ah := obs.AgentHooks()
	info := &hook.AgentInfo{Name: "local_agent", Type: "base", Round: 0}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})

	time.Sleep(400 * time.Millisecond)

	spans := srv.Spans()
	if len(spans) == 0 {
		t.Fatal("没抓到 span")
	}
	for _, s := range spans {
		if string(s.TraceId) == remoteTraceID {
			t.Errorf("无 seed 时不应用 remote traceID（应本地 GenTraceID）")
		}
		if s.Name == "gen_ai.agent.local_agent" && string(s.ParentSpanId) != "" {
			// 本地顶层 agent span 是 root（ParentSpanId 空），不挂任何 remote
			t.Errorf("本地顶层 agent span 应是 root（ParentSpanId 空），got %q", string(s.ParentSpanId))
		}
	}
}

type spanLike struct {
	spanID       string
	parentSpanID string
	name         string
}

// TestA2AServerSpan_InternalAgentNests 第3步：server span 建后，内部 agent span
// 挂到 a2a_server span 下（用 remote traceID），符合协议拓扑 client→server→内部。
// sanity-revert：删 StartA2AServerSpan 里 rootParentSpanID 更新 → 内部 agent 挂
// remote 而非 a2a_server span → fail。
func TestA2AServerSpan_InternalAgentNests(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	remoteTraceID := GenTraceID("java-client")
	remoteSpanID := GenRootSpanID()
	remoteTC := TraceContext{TraceID: remoteTraceID, SpanID: remoteSpanID, Sampled: true, AppName: "java-client", NextIdx: 1}

	ctx := compose_v2.WithSessionIDCtx(context.Background(), "srv-span-sid")
	ctx = WithTraceContext(ctx, remoteTC)
	srvSpan := obs.StartA2AServerSpan(ctx, "message_stream", "ctx-1", "task-1")

	ah := obs.AgentHooks()
	info := &hook.AgentInfo{Name: "inner_agent", Type: "base", Round: 0}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	srvSpan.End("success", "", "")

	time.Sleep(400 * time.Millisecond)
	spans := srv.Spans()
	var serverSpanID, agentParentID string
	for _, s := range spans {
		if string(s.TraceId) != remoteTraceID {
			t.Errorf("span %q TraceId 应=remote %q，got %q", s.Name, remoteTraceID, string(s.TraceId))
		}
		switch s.Name {
		case "gen_ai.a2a_server.message_stream":
			serverSpanID = string(s.SpanId)
			if string(s.ParentSpanId) != remoteSpanID {
				t.Errorf("a2a_server span 应挂 remote parent %q，got %q", remoteSpanID, string(s.ParentSpanId))
			}
		case "gen_ai.agent.inner_agent":
			agentParentID = string(s.ParentSpanId)
		}
	}
	if serverSpanID == "" {
		t.Fatal("没找到 a2a_server span")
	}
	if agentParentID != serverSpanID {
		t.Errorf("内部 agent span 应挂 a2a_server span %q 下，got parent %q", serverSpanID, agentParentID)
	}
}

// TestA2AClientSpan_NestsUnderActiveAgent 第3步：有活跃 host agent span 时，
// client span 挂它下 + TraceContext valid（可注入 outbound metadata）。
func TestA2AClientSpan_NestsUnderActiveAgent(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := compose_v2.WithSessionIDCtx(context.Background(), "cli-span-sid")
	ah := obs.AgentHooks()
	info := &hook.AgentInfo{Name: "host_agent", Type: "base", Round: 0}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})

	cliSpan := obs.StartA2AClientSpan(ctx, "remote-svc", "message_stream", "jsonrpc", "ctx-2", "task-2")
	tc := cliSpan.TraceContext()
	if !tc.IsValid() {
		t.Fatal("有活跃 host agent span 时 client TraceContext 应 valid（可注入 metadata）")
	}
	if tc.SpanID == "" {
		t.Error("client TraceContext.SpanID 空（注入给 server 当 parent 用）")
	}
	cliSpan.End("success", "", "")
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})

	time.Sleep(400 * time.Millisecond)
	spans := srv.Spans()
	var hostSpanID, clientParentID string
	for _, s := range spans {
		switch s.Name {
		case "gen_ai.agent.host_agent":
			hostSpanID = string(s.SpanId)
		case "gen_ai.a2a_client.remote-svc":
			clientParentID = string(s.ParentSpanId)
		}
	}
	if hostSpanID == "" {
		t.Fatal("没找到 host agent span")
	}
	if clientParentID != hostSpanID {
		t.Errorf("client span 应挂 host agent span %q 下，got parent %q", hostSpanID, clientParentID)
	}
}

// TestA2AClientSpan_NoActiveAgentNoSpan 第3步：无活跃 agent span 时 client span
// no-op（协议 line 81：不建孤立 client span）+ TraceContext invalid（caller 不注入）。
func TestA2AClientSpan_NoActiveAgentNoSpan(t *testing.T) {
	_, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ctx := compose_v2.WithSessionIDCtx(context.Background(), "cli-noparent-sid")
	cliSpan := obs.StartA2AClientSpan(ctx, "remote-svc", "message_stream", "jsonrpc", "", "")
	tc := cliSpan.TraceContext()
	if tc.IsValid() {
		t.Error("无活跃父 agent span 时 client 应 no-op（TraceContext invalid，不注入避免孤立 span）")
	}
}

// TestA2AClientSpan_AsTool_ChildNS 验 RemoteAgent 经 AgentAsTool / RunSubAgent（主用法）
// 在 child ns 时，client span 沿 ParentNS 上溯命中 parent ns 的 host agent span——修
// R33 audit MED：findActiveAgentSpan 精确匹配 child ns 找不到父（host 在 parent ns）→
// client span no-op → 不注入 metadata → 远端自起 trace 断链。host BasicAgent 把
// RemoteAgent 当 tool 调时 RunSubAgent push child ns，这是 RemoteAgent 主用法。
func TestA2AClientSpan_AsTool_ChildNS(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	// host agent span 在 parent ns=""（OnAgentStart graphNS=""）
	ctx := compose_v2.WithSessionIDCtx(context.Background(), "astool-childns-sid")
	ah := obs.AgentHooks()
	info := &hook.AgentInfo{Name: "host_agent", Type: "base", Round: 0}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})

	// RemoteAgent 作 tool 被调：RunSubAgent push child ns（host span 在父 ns ""，本调用在子 ns）
	childCtx := compose_v2.WithGraphNS(ctx, "remotetool")
	cliSpan := obs.StartA2AClientSpan(childCtx, "remote-svc", "message_stream", "jsonrpc", "ctx-astool", "task-astool")
	tc := cliSpan.TraceContext()
	if !tc.IsValid() {
		t.Fatal("child ns 下 client span 应沿 ParentNS 上溯命中 parent ns 的 host agent span（修复前：精确匹配 child ns 找不到 → no-op → TraceContext invalid → 不注入 metadata → 断链）")
	}
	cliSpan.End("success", "", "")
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})

	time.Sleep(400 * time.Millisecond)
	var hostSpanID, clientParentID string
	for _, s := range srv.Spans() {
		switch s.Name {
		case "gen_ai.agent.host_agent":
			hostSpanID = string(s.SpanId)
		case "gen_ai.a2a_client.remote-svc":
			clientParentID = string(s.ParentSpanId)
		}
	}
	if hostSpanID == "" {
		t.Fatal("没找到 host agent span")
	}
	if clientParentID != hostSpanID {
		t.Errorf("child ns 的 client span 应上溯挂到 parent ns 的 host agent span %q 下，got parent %q（断链）", hostSpanID, clientParentID)
	}
}
