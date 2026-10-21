package agent_v2_test

import (
	"context"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// ISSUE-79 修复验证：AgentAsTool 默认走半共享 child sess——values 与父共享指针 +
// events 独立 + parentSess 反向指针 + RunID 沿用父。
//
// 设计参考 eino adk withSharedParentSession（runctx.go:386-391），但保留 crew-go
// 已有的 NS 物理分离 cp（比 eino bridgeStore 嵌入式 cp 更强）。

// captureSessionAgent 捕获 ctx 里的 sess，写一些 events + values 验证半共享语义。
type captureSessionAgent struct {
	name string
	// 测试用的 capture targets
	capturedSess     *agent_v2.RunSession
	capturedRunID    string
	capturedRoot     *agent_v2.RunSession
	capturedParentID string
	mu               sync.Mutex
	// 写入 child sess 的 KV——验证父 sess 是否能读
	writeKV string
}

func (c *captureSessionAgent) Run(ctx context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	if sess == nil {
		sess = agent_v2.NewRunSession()
	}
	c.mu.Lock()
	c.capturedSess = sess
	c.capturedRunID = sess.RunID
	c.capturedRoot = sess.RootSession()
	if p := sess.ParentSession(); p != nil {
		c.capturedParentID = p.RunID
	}
	c.mu.Unlock()

	// 写入 child sess.values + child sess.Events
	if c.writeKV != "" {
		sess.SetValue("child_key", c.writeKV)
	}
	sess.AddEvent(agent_v2.NewOutputEvent(c.name, schema.AssistantMessage("child wrote me")))
	return []*agent_v2.AgentEvent{
		agent_v2.NewExitEvent(c.name, schema.AssistantMessage("child final")),
	}, nil
}
func (c *captureSessionAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (c *captureSessionAgent) Name() string        { return c.name }
func (c *captureSessionAgent) Description() string { return "capture stub" }

// TestAgentAsTool_ISSUE79_SharedSession 验证 AgentAsTool 默认走半共享：
// child sess 与父共享 RunID + values 指针 + parentSess 链路，但 events 独立。
func TestAgentAsTool_ISSUE79_SharedSession(t *testing.T) {
	cap := &captureSessionAgent{name: "child", writeKV: "child_val"}
	tooly := agent_v2.AgentAsTool(cap, agent_v2.WithAgentToolDescription("child agent"))

	parentSess := agent_v2.NewRunSession()
	parentSess.SetValue("parent_key", "parent_val")
	ctx := agent_v2.WithSession(context.Background(), parentSess)

	_, err := tooly.Call(ctx, map[string]any{"input": "hi"})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}

	cap.mu.Lock()
	defer cap.mu.Unlock()

	// child RunID == parent RunID（cp 持久化按 conv 边界对齐）
	if cap.capturedRunID != parentSess.RunID {
		t.Errorf("child RunID 应沿用父；实际 child=%q parent=%q",
			cap.capturedRunID, parentSess.RunID)
	}
	// child sess.parentSess 指向 parent
	if cap.capturedParentID != parentSess.RunID {
		t.Errorf("child sess.ParentSession().RunID 应等于父；实际 %q",
			cap.capturedParentID)
	}
	// RootSession 沿父链上追到 parent
	if cap.capturedRoot != parentSess {
		t.Errorf("child sess.RootSession() 应等于父 sess；实际不同")
	}
	// values 共享指针：child 写 child_key 父能读到
	v, ok := parentSess.GetValue("child_key")
	if !ok || v != "child_val" {
		t.Errorf("ISSUE-79: 父应通过共享指针读到 child_key=child_val；实际 ok=%v v=%v\n"+
			"sanity-revert 删 NewSharedChildSession values 共享 → 父读不到 → 测试 fail", ok, v)
	}
	// child 也能反向读到父 values
	v2, ok := cap.capturedSess.GetValue("parent_key")
	if !ok || v2 != "parent_val" {
		t.Errorf("child 应通过共享指针读到 parent_key；实际 ok=%v v=%v", ok, v2)
	}
	// events 独立：父 sess.Events 不含 child events（半共享语义核心）
	for _, e := range parentSess.Events() {
		if e.AgentName == "child" {
			t.Errorf("events 应独立——父 sess.Events 不应含 child events；实际有 %v", e)
		}
	}
}

// TestAgentAsTool_ISSUE79_RootSessionTraversal 验证 RootSession 沿多层 parent 链上追。
// 业务场景：父 → 子 (shared) → 孙 (shared)，孙调 Memory.Read 沿链到父。
func TestAgentAsTool_ISSUE79_RootSessionTraversal(t *testing.T) {
	root := agent_v2.NewRunSession()
	root.RunID = "root_run"
	root.SetValue("level", "root")

	level1 := agent_v2.NewSharedChildSession(root)
	level2 := agent_v2.NewSharedChildSession(level1)
	level3 := agent_v2.NewSharedChildSession(level2)

	if got := level3.RootSession(); got != root {
		t.Errorf("level3.RootSession() 应 = root；实际 %v vs %v", got, root)
	}
	if got := level2.RootSession(); got != root {
		t.Errorf("level2.RootSession() 应 = root；实际 %v vs %v", got, root)
	}
	if got := level3.ParentSession(); got != level2 {
		t.Errorf("level3.ParentSession() 应 = level2")
	}
	if v, ok := level3.GetValue("level"); !ok || v != "root" {
		t.Errorf("跨层 values 应共享指针；level3 读 level 应 = root；实际 ok=%v v=%v", ok, v)
	}
	level3.SetValue("from_level3", "yes")
	if v, ok := root.GetValue("from_level3"); !ok || v != "yes" {
		t.Errorf("level3 写 from_level3，root 应读到；实际 ok=%v v=%v", ok, v)
	}
}

// **R32 audit 视角 D HIGH-1**: 原 TestAgentAsTool_ISSUE79_NilParent_Fallback 是
// 假绿炸弹（CLAUDE.md "5 类炸弹" #2 早退兜底盖过 fix 路径）—— captureSessionAgent.Run
// 内 `if sess == nil { sess = NewRunSession() }` 自防御让 sanity-revert 仍 PASS，
// 测的是 stub 自防御不是 fix 行为。已删除——业务"ctx 无 parent sess fallback" 路径
// 由 sub-agent.Run 入口的 NewRunSession() fallback 保证（不属于 ISSUE-79 fix 范畴）。

// TestAgentAsTool_ISSUE79_SkillListingAccumulation 验证业务核心场景：sub-agent
// 跨多次调用 sess.values 累积（典型 sub-agent 间共享 KV 场景），
// 不再每次 NewRunSession 重置——ISSUE-79 修复价值的本质。
func TestAgentAsTool_ISSUE79_SkillListingAccumulation(t *testing.T) {
	cap := &captureSessionAgent{name: "child", writeKV: "first_call"}
	tooly := agent_v2.AgentAsTool(cap, agent_v2.WithAgentToolDescription("child"))

	parentSess := agent_v2.NewRunSession()
	ctx := agent_v2.WithSession(context.Background(), parentSess)

	// 第一次 Call: child 写 child_key=first_call
	if _, err := tooly.Call(ctx, map[string]any{"input": "1"}); err != nil {
		t.Fatalf("Call 1: %v", err)
	}
	v, ok := parentSess.GetValue("child_key")
	if !ok || v != "first_call" {
		t.Fatalf("Call 1 后父 sess 应见 child_key=first_call；实际 ok=%v v=%v", ok, v)
	}

	// 第二次 Call: 第二个 sub-agent 应通过 GetSession(ctx).GetValue 读到 first_call
	cap2 := &captureReadAgent{name: "child"}
	tooly2 := agent_v2.AgentAsTool(cap2, agent_v2.WithAgentToolDescription("child"))
	if _, err := tooly2.Call(ctx, map[string]any{"input": "2"}); err != nil {
		t.Fatalf("Call 2: %v", err)
	}
	if cap2.readVal != "first_call" {
		t.Errorf("ISSUE-79 修复：第二个 sub-agent 应能读到上次 sub-agent 写入的 KV；"+
			"实际读到 %q（期望 'first_call'）\n"+
			"sanity-revert 改回 nil sess → 子 NewRunSession 全新 → 读不到 → 测试 fail",
			cap2.readVal)
	}
}

// captureReadAgent 读 child_key 验证跨调用累积。
type captureReadAgent struct {
	name    string
	readVal string
}

func (c *captureReadAgent) Run(_ context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	if sess == nil {
		sess = agent_v2.NewRunSession()
	}
	if v, ok := sess.GetValue("child_key"); ok {
		if s, ok2 := v.(string); ok2 {
			c.readVal = s
		}
	}
	return []*agent_v2.AgentEvent{
		agent_v2.NewExitEvent(c.name, schema.AssistantMessage("done")),
	}, nil
}
func (c *captureReadAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (c *captureReadAgent) Name() string        { return c.name }
func (c *captureReadAgent) Description() string { return "" }
