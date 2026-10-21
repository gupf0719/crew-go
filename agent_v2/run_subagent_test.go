package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// captureSubAgent 记录调用时 ctx 的 graphNS / sess / storeScope / input 让测试断言.
// **#7 audit follow-up H4 fix**: 加 gotInput 字段防 helper 漏传/吞 input silent bug.
type captureSubAgent struct {
	name          string
	gotGraphNS    string
	gotSess       *agent_v2.RunSession
	gotScope      bool
	gotScopeOK    bool
	gotInput      *schema.Message
	wantErr       error
	wantInterrupt bool
}

func (c *captureSubAgent) Name() string        { return c.name }
func (c *captureSubAgent) Description() string { return "capture sub-agent" }
func (c *captureSubAgent) Run(ctx context.Context, sess *agent_v2.RunSession, input *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	c.gotGraphNS = compose_v2.GetGraphNS(ctx)
	c.gotSess = sess
	c.gotScope, c.gotScopeOK = agent_v2.IsSubAgentStoreScope(ctx), true
	c.gotInput = input
	if c.wantInterrupt {
		return nil, &compose_v2.InterruptError{Reason: "test", Query: &compose_v2.AskQuery{Question: "?", InterruptID: "ask_1"}}
	}
	if c.wantErr != nil {
		return nil, c.wantErr
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(c.name, schema.AssistantMessage("sub-done"))}, nil
}
func (c *captureSubAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

// TestRunSubAgent_Push3CtxOnHappy 验证 happy path: 3 件 ctx 全 push (graphNS + childSess + storeScope)
// + input 透传 (H4 fix).
func TestRunSubAgent_Push3CtxOnHappy(t *testing.T) {
	cap := &captureSubAgent{name: "child"}
	parentSess := agent_v2.NewRunSession()
	ctx := agent_v2.WithSession(context.Background(), parentSess)
	ctx = compose_v2.WithGraphNS(ctx, "parent")

	inputMsg := schema.UserMessage("hi-unique-marker")
	_, err := agent_v2.RunSubAgent(ctx, "child", cap, inputMsg)
	if err != nil {
		t.Fatalf("RunSubAgent: %v", err)
	}
	// 子 ns 是 parent 下 child 的一个 `#` 实例（身份 = 位置 + 输入内容，见 RunSubAgent godoc）
	if segs, err := compose_v2.SplitNS(cap.gotGraphNS); err != nil || len(segs) != 2 ||
		segs[0].NodeID != "parent" || segs[1].NodeID != "child" || segs[1].Kind != "fanout" {
		t.Errorf("graphNS 应是 parent/child#<身份>，got %q（err=%v）", cap.gotGraphNS, err)
	}
	if cap.gotSess == nil {
		t.Fatal("childSess 应非 nil (父 sess 非 nil 时构造)")
	}
	if cap.gotSess == parentSess {
		t.Error("childSess 不应是 parent (NewSharedChildSession 应构造新实例)")
	}
	if cap.gotSess.RunID != parentSess.RunID {
		t.Errorf("childSess.RunID 应沿用父 (NewSharedChildSession 协议), parent=%q child=%q", parentSess.RunID, cap.gotSess.RunID)
	}
	if !cap.gotScope {
		t.Error("WithSubAgentStoreScope 应为 true (ISSUE-84)")
	}
	// H4 fix: 验 input 原样透传 (helper 漏传/吞/换 input silent bug 防御)
	if cap.gotInput == nil {
		t.Fatal("input 不应被 helper 吞掉")
	}
	if cap.gotInput.Content != "hi-unique-marker" {
		t.Errorf("input.Content 应原样透传, want 'hi-unique-marker', got %q", cap.gotInput.Content)
	}
}

// TestRunSubAgent_FanoutDetect 验证 ctx 含 tcID 时走 JoinFanoutNS (含 hash, ISSUE-78).
// **#7 audit follow-up H5 fix**: 跑两次不同 tcID 断言 NS 不等——防 helper 固定 idx
// 让两 tcID 产相同 NS 互覆 bug.
func TestRunSubAgent_FanoutDetect(t *testing.T) {
	parentSess := agent_v2.NewRunSession()
	base := agent_v2.WithSession(context.Background(), parentSess)
	base = compose_v2.WithGraphNS(base, "parent")

	// Run 1: tcID="tc_xyz"
	cap1 := &captureSubAgent{name: "child"}
	ctx1 := compose_v2.WithCurrentToolCallContext(base, "tc_xyz", 0)
	if _, err := agent_v2.RunSubAgent(ctx1, "child", cap1, schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if !strings.HasPrefix(cap1.gotGraphNS, "parent/child#") {
		t.Errorf("Run 1 fanout graphNS 应有 child#<int> 后缀, got %q", cap1.gotGraphNS)
	}
	suffix1 := strings.TrimPrefix(cap1.gotGraphNS, "parent/child#")
	if strings.HasPrefix(suffix1, "-") {
		t.Errorf("Run 1 fanout hash 不应 negative (audit B LOW-1 fix), got %q", cap1.gotGraphNS)
	}

	// Run 2: tcID="tc_abc" — 不同 tcID, NS hash 应不等
	cap2 := &captureSubAgent{name: "child"}
	ctx2 := compose_v2.WithCurrentToolCallContext(base, "tc_abc", 0)
	if _, err := agent_v2.RunSubAgent(ctx2, "child", cap2, schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run 2: %v", err)
	}

	// **核心断言** (H5 fix): 不同 tcID 必产不同 graphNS 让 cp 物理分离
	// sanity-revert: helper 改 `JoinFanoutNS(parent, nodeID, 0)` 固定 idx 时, 两 tcID
	// 产相同 NS, 本断言 fail 锁住协议
	if cap1.gotGraphNS == cap2.gotGraphNS {
		t.Errorf("不同 tcID 应产不同 graphNS (hash 由 tcID 派生), 实际 cap1=cap2=%q (helper 可能固定 idx 而非 hash)", cap1.gotGraphNS)
	}
}

// TestRunSubAgent_NilParentSess 验证父 sess nil 时 helper 不 panic, childSess 也 nil 让 sub.Run NewRunSession fallback.
func TestRunSubAgent_NilParentSess(t *testing.T) {
	cap := &captureSubAgent{name: "child"}
	// 故意不 push sess
	ctx := compose_v2.WithGraphNS(context.Background(), "parent")

	_, err := agent_v2.RunSubAgent(ctx, "child", cap, schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("RunSubAgent (no parent sess): %v", err)
	}
	// 调用方 ctx 里没有 session：给被调方一个。ctx 上没有 sid 时新铸 RunID
	if cap.gotSess == nil || cap.gotSess.RunID == "" {
		t.Fatalf("父 sess nil 时应给被调方一个带 RunID 的 session, got %+v", cap.gotSess)
	}
	// ctx 上有 sid、又不在任何正在执行的任务里（没有节点执行范围、没有任务章）：被调方沿用它——
	// 一律新铸的话每次都是新任务、存档落在随机 sid 下，续跑找不到。（在节点里时取的是那张图的 sid，
	// ctx 上另推的不算，见 TestRunSubAgent_BareGraphWithOtherSessionInCtx_FollowsGraphTask）
	cap2 := &captureSubAgent{name: "child"}
	if _, err := agent_v2.RunSubAgent(compose_v2.WithSessionIDCtx(ctx, "graph-sid"), "child", cap2, schema.UserMessage("hi")); err != nil {
		t.Fatalf("RunSubAgent: %v", err)
	}
	if cap2.gotSess == nil || cap2.gotSess.RunID != "graph-sid" {
		t.Errorf("被调方的 session 应沿用 ctx 上的 sid, got %+v", cap2.gotSess)
	}
}

// TestRunSubAgent_InvalidNodeID 验证 nodeID 含 NS 禁止字符 (/, #) fail-fast.
func TestRunSubAgent_InvalidNodeID(t *testing.T) {
	cap := &captureSubAgent{name: "child"}
	ctx := compose_v2.WithGraphNS(context.Background(), "parent")

	_, err := agent_v2.RunSubAgent(ctx, "bad/node", cap, schema.UserMessage("hi"))
	if err == nil || !strings.Contains(err.Error(), "invalid for ns") {
		t.Errorf("bad nodeID 应 fail-fast 含 'invalid for ns', got %v", err)
	}
}

// TestRunSubAgent_NilSubAgent 验证 sub nil 时 fail-fast (不 panic).
//
// **#7 audit follow-up M6 fix**: 加 typed nil interface case 让 isAgentNil reflect
// 路径被覆盖 (CLAUDE.md "Go interface typed nil 陷阱"). 之前只测 untyped nil literal,
// `var b *captureSubAgent; RunSubAgent(..., b, ...)` typed nil 路径漏 — 砍 isAgentNil
// 内 reflect.IsNil 让 `a == nil` 早出, 测试仍 PASS silent.
//
// sanity-revert: 砍 agent_helpers.go::isAgentNil 内 reflect 部分让 `return a == nil`,
// typed nil case 测试 fail.
func TestRunSubAgent_NilSubAgent(t *testing.T) {
	// Case 1: untyped nil literal — 早期出 (a == nil)
	_, err := agent_v2.RunSubAgent(context.Background(), "child", nil, schema.UserMessage("hi"))
	if err == nil || !strings.Contains(err.Error(), "nil sub-agent") {
		t.Errorf("untyped nil 应 fail-fast 含 'nil sub-agent', got %v", err)
	}

	// Case 2: typed nil interface — 必走 reflect.IsNil 路径才能抓到
	var typedNil *captureSubAgent // 持非 nil type tag + nil value
	var asIface agent_v2.Agent = typedNil
	_, err = agent_v2.RunSubAgent(context.Background(), "child", asIface, schema.UserMessage("hi"))
	if err == nil || !strings.Contains(err.Error(), "nil sub-agent") {
		t.Errorf("typed nil interface 应 fail-fast 含 'nil sub-agent' (isAgentNil reflect 路径), got %v", err)
	}
}

// ── #7 F1+F5: AgentAsTool 新协议测试 ──

// descCaptureAgent 含可控的 Description() 给测试用.
type descCaptureAgent struct {
	name string
	desc string
}

func (a *descCaptureAgent) Name() string        { return a.name }
func (a *descCaptureAgent) Description() string { return a.desc }
func (a *descCaptureAgent) Run(_ context.Context, _ *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, schema.AssistantMessage("ok"))}, nil
}
func (a *descCaptureAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

// TestAgentAsTool_DescriptionDefaultFromAgent 验证 F5: 不传 WithAgentToolDescription
// 时 default 取 agent.Description().
func TestAgentAsTool_DescriptionDefaultFromAgent(t *testing.T) {
	a := &descCaptureAgent{name: "sub", desc: "I am a sub-agent from .Description()"}
	tooly := agent_v2.AgentAsTool(a) // 不传 description option

	if got := tooly.Description(); got != "I am a sub-agent from .Description()" {
		t.Errorf("default desc 应取 agent.Description(), got %q", got)
	}
}

// TestAgentAsTool_DescriptionOverride 验证 F5: WithAgentToolDescription override 默认.
func TestAgentAsTool_DescriptionOverride(t *testing.T) {
	a := &descCaptureAgent{name: "sub", desc: "default"}
	tooly := agent_v2.AgentAsTool(a, agent_v2.WithAgentToolDescription("override"))

	if got := tooly.Description(); got != "override" {
		t.Errorf("WithAgentToolDescription 应 override, got %q", got)
	}
}

// TestAgentAsTool_EmptyDescPanic 验证 F5: agent.Description() 空 + 无 override
// → 构造期 panic (eino fail-fast 范式).
func TestAgentAsTool_EmptyDescPanic(t *testing.T) {
	a := &descCaptureAgent{name: "sub", desc: ""}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("空 desc 应 panic")
		} else if !strings.Contains(r.(string), "description") {
			t.Errorf("panic message 应含 'description', got %v", r)
		}
	}()
	_ = agent_v2.AgentAsTool(a) // expect panic
}

// TestAgentAsTool_CustomInputSchema 验证 F1: WithAgentToolInputSchema 让业务定制
// schema, Call 时 args 整体 marshal JSON 作 input.
func TestAgentAsTool_CustomInputSchema(t *testing.T) {
	customSchema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query":  map[string]any{"type": "string"},
			"top_k":  map[string]any{"type": "integer"},
			"format": map[string]any{"type": "string"},
		},
		"required": []string{"query"},
	}
	cap := &captureSubAgent{name: "search"}
	tooly := agent_v2.AgentAsTool(cap,
		agent_v2.WithAgentToolDescription("search"),
		agent_v2.WithAgentToolInputSchema(customSchema),
	)

	// 验证 InputSchema 返回的是自定义的
	if !reflectMapEq(tooly.InputSchema(), customSchema) {
		t.Errorf("InputSchema 应是自定义 schema, got %v", tooly.InputSchema())
	}

	// **#7 audit follow-up H3 fix**: 验 Call 路径真把 args 整体 marshal JSON 作
	// sub-agent input.Content. sanity-revert: agent_tool.go::Call 改 `json.Marshal(args)`
	// 为 `json.Marshal(map[string]any{})` 时本断言 fail.
	parentSess := agent_v2.NewRunSession()
	ctx := agent_v2.WithSession(context.Background(), parentSess)
	_, err := tooly.Call(ctx, map[string]any{"query": "go scheduler", "top_k": 5})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if cap.gotInput == nil {
		t.Fatal("sub-agent 应收到 input")
	}
	// args 整体 JSON marshal 应含 query / top_k 字段
	if !strings.Contains(cap.gotInput.Content, `"query"`) || !strings.Contains(cap.gotInput.Content, `"go scheduler"`) {
		t.Errorf("customSchema 模式应把 args 整体 marshal JSON 含 'query'/'go scheduler', got %q", cap.gotInput.Content)
	}
	if !strings.Contains(cap.gotInput.Content, `"top_k"`) {
		t.Errorf("customSchema 应含全部 args key, got %q", cap.gotInput.Content)
	}
}

// TestAgentAsTool_FullHistoryInput 验证 F1: WithAgentToolFullHistoryInput 启用时
// 从父 sess.HistoryFor("") 拼 messages 作 input.
func TestAgentAsTool_FullHistoryInput(t *testing.T) {
	// 父 sess 加 2 条 events
	parentSess := agent_v2.NewRunSession()
	parentSess.AddEvent(agent_v2.NewOutputEvent("user", schema.UserMessage("first turn")))
	parentSess.AddEvent(agent_v2.NewOutputEvent("user", schema.UserMessage("second turn")))

	var capturedInput *schema.Message
	inputCap := &runFnAgent{
		name: "history-reader",
		desc: "read history",
		runFn: func(_ context.Context, _ *agent_v2.RunSession, input *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
			capturedInput = input
			return []*agent_v2.AgentEvent{agent_v2.NewExitEvent("history-reader", schema.AssistantMessage("done"))}, nil
		},
	}
	tooly := agent_v2.AgentAsTool(inputCap,
		agent_v2.WithAgentToolDescription("test"),
		agent_v2.WithAgentToolFullHistoryInput(),
	)

	ctx := agent_v2.WithSession(context.Background(), parentSess)
	_, err := tooly.Call(ctx, map[string]any{})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if capturedInput == nil {
		t.Fatal("sub-agent 应收到 input")
	}
	// 拼成 single user message 含 "first turn" + "second turn"
	if !strings.Contains(capturedInput.Content, "first turn") || !strings.Contains(capturedInput.Content, "second turn") {
		t.Errorf("history input 应含父 messages, got %q", capturedInput.Content)
	}
	// 拼出来的是父对话的拷贝，必须标成派生输入、不进会话历史——否则每次调用都把整段
	// 对话当成一条新的用户输入记一遍，跨 Run 越积越大（MarkDerivedInput 返回的是
	// 拷贝，丢掉返回值等于没标）
	if v, _ := capturedInput.Annotations[agent_v2.DerivedInputAnnotation].(bool); !v {
		t.Errorf("fullHistory 拼出的输入没打 DerivedInputAnnotation，annotations=%v", capturedInput.Annotations)
	}
}

// runFnAgent 用户自定义 Run fn 让测试断言 input.
type runFnAgent struct {
	name  string
	desc  string
	runFn func(context.Context, *agent_v2.RunSession, *schema.Message, ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error)
}

func (a *runFnAgent) Name() string        { return a.name }
func (a *runFnAgent) Description() string { return a.desc }
func (a *runFnAgent) Run(ctx context.Context, sess *agent_v2.RunSession, input *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	return a.runFn(ctx, sess, input, opts...)
}
func (a *runFnAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

// reflectMapEq 简单 map 浅比较 (测试用).
func reflectMapEq(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok || !equalAny(va, vb) {
			return false
		}
	}
	return true
}

func equalAny(a, b any) bool {
	switch va := a.(type) {
	case map[string]any:
		vb, ok := b.(map[string]any)
		if !ok {
			return false
		}
		return reflectMapEq(va, vb)
	case []string:
		vb, ok := b.([]string)
		if !ok || len(va) != len(vb) {
			return false
		}
		for i := range va {
			if va[i] != vb[i] {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}

// ── #7 F6: 7 编排 Name() 空构造期 panic (BasicAgent + PlanAgent UUID 默认不 fail-fast) ──

// TestNewBasicAgent_NoNameUsesUUIDDefault 验证 BasicAgent 不传 WithName 时
// defaultConfig 给 UUID name (跟 PlanAgent 协议对齐 — 不 fail-fast 业务 init 无负担).
func TestNewBasicAgent_NoNameUsesUUIDDefault(t *testing.T) {
	a := agent_v2.NewBasicAgent() // no WithName — 不应 panic
	if a.Name() == "" {
		t.Error("BasicAgent 不传 WithName 时应有 UUID 默认 name")
	}
	if !strings.HasPrefix(a.Name(), "agent_v2_") {
		t.Errorf("默认 name 应是 'agent_v2_<uuid>' 形态, got %q", a.Name())
	}
}

// TestNewSequentialAgent_EmptyNamePanic 验证 F6: 7 编排 fail-fast.
// **#7 audit follow-up M5 fix**: 加 panic msg 断言锁死 fix 路径——
// sanity-revert 改 panic msg 为 "something else" 时本测试 fail.
func TestNewSequentialAgent_EmptyNamePanic(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("空 name 应 panic")
		}
		if !strings.Contains(fmt.Sprint(r), "WithSequentialName required") {
			t.Errorf("panic msg 应含 'WithSequentialName required', got %v", r)
		}
	}()
	_ = agent_v2.NewSequentialAgent()
}

// TestNewParallelAgent_EmptyNamePanic 同款.
func TestNewParallelAgent_EmptyNamePanic(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("空 name 应 panic")
		}
		if !strings.Contains(fmt.Sprint(r), "WithParallelName required") {
			t.Errorf("panic msg 应含 'WithParallelName required', got %v", r)
		}
	}()
	_ = agent_v2.NewParallelAgent()
}

// TestNewLoopAgent_EmptyNamePanic 同款.
func TestNewLoopAgent_EmptyNamePanic(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("空 name 应 panic")
		}
		if !strings.Contains(fmt.Sprint(r), "WithLoopName required") {
			t.Errorf("panic msg 应含 'WithLoopName required', got %v", r)
		}
	}()
	_ = agent_v2.NewLoopAgent()
}
