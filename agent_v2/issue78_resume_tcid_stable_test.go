package agent_v2_test

import (
	"context"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// nsCapturingResumeSubAgent 用 BasicAgent 包 ask_user_tool 的 sub-agent——
// 第一次 Run 内 LLM 调 ask_user 抛 InterruptError, Resume 后用户答 → 完成。
// 同时记录每次 Run 收到的 ctx graphNS——验证 Resume 路径 NS 跨 Run 稳定。
type nsCapturingResumeSubAgent struct {
	name     string
	mu       sync.Mutex
	visitNSs []string
	inner    agent_v2.Agent
}

func (a *nsCapturingResumeSubAgent) Run(ctx context.Context, sess *agent_v2.RunSession, input *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.mu.Lock()
	a.visitNSs = append(a.visitNSs, compose_v2.GetGraphNS(ctx))
	a.mu.Unlock()
	return a.inner.Run(ctx, sess, input, opts...)
}
func (a *nsCapturingResumeSubAgent) Resume(ctx context.Context, sid string, opts ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	a.mu.Lock()
	a.visitNSs = append(a.visitNSs, compose_v2.GetGraphNS(ctx))
	a.mu.Unlock()
	return a.inner.Resume(ctx, sid, opts...)
}
func (a *nsCapturingResumeSubAgent) Name() string        { return a.name }
func (a *nsCapturingResumeSubAgent) Description() string { return "" }

// resumeStableSubModel sub-agent LLM:
//   - 第 1 次调用: emit ask_user(question="?")
//   - 第 2 次调用 (Resume 后): 读 tool_result final
type resumeStableSubModel struct {
	mu    sync.Mutex
	calls int
}

func (m *resumeStableSubModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	m.calls++
	c := m.calls
	m.mu.Unlock()
	if c == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID:       "sub_ask_tc",
			Type:     "function",
			Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"q?"}`},
		}}), nil
	}
	last := ""
	for _, mm := range msgs {
		if mm == nil {
			continue
		}
		if mm.Role == schema.RoleTool {
			last = mm.Content
		}
	}
	return schema.AssistantMessage("sub final: " + last), nil
}
func (m *resumeStableSubModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *resumeStableSubModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

var _ model.ChatModel = (*resumeStableSubModel)(nil)

// parentResumeStableModel 父 LLM:
//   - 第 1 次: emit AgentAsTool(sub_agent, input="ask")
//   - 第 2 次: final
type parentResumeStableModel struct {
	mu    sync.Mutex
	calls int
}

func (m *parentResumeStableModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	m.calls++
	c := m.calls
	m.mu.Unlock()
	if c == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID:       "tc_resume_stable",
			Type:     "function",
			Function: schema.FunctionCall{Name: "sub_agent", Arguments: `{"input":"ask"}`},
		}}), nil
	}
	return schema.AssistantMessage("parent done"), nil
}
func (m *parentResumeStableModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *parentResumeStableModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

var _ model.ChatModel = (*parentResumeStableModel)(nil)

// TestISSUE78_AgentAsTool_ResumeTcIDStability 验证 audit C MED-2 — Resume 路径
// tc.ID 稳定性 (cp.Channels[basicMessagesCh] 末段 toolCall 持久化让 hash 跨
// Resume 稳定 → childNS 命中原 cp)。
//
// **bug 历史**: commit 5ff1384 跟踪 audit A HIGH-2: "Resume tc.ID 稳定性 实际 OK
// (cp 存 toolCalls 含原 tc.ID), 但缺测试"——本测试补此覆盖。
//
// **测试设计**:
//   - Run 1: 父 LLM emit tc.ID="tc_resume_stable" → AgentAsTool(sub_agent) → sub
//     LLM 调 ask_user 抛 InterruptError → 父 SaveNS cp
//   - Resume: 答 InterruptID → 父 toolsFn 重跑 → 父 LLM 不重调 (cp restore
//     basicMessagesCh) → 同 tc.ID → 同 hash → 同 childNS → cper.LoadNS 命中 sub cp
//     → sub 续跑 → 完成
//   - 断言: sub-agent 跨 Run 收到的 ctx graphNS 应**完全相同** (tc.ID hash 稳定)
//
// **sanity-revert**: 改 hash 计算混入 round number (e.g. `hash + round * P`)
// → Resume 后 NS 不同 → 测试 fail.
//
// **R34 加固**: 用 WithResumeSession 让 sess 跨 Run 1 → Resume 一致 (issue77 同问题)。
func TestISSUE78_AgentAsTool_ResumeTcIDStability(t *testing.T) {
	subModel := &resumeStableSubModel{}
	subInner := agent_v2.NewBasicAgent(
		agent_v2.WithName("sub_agent"),
		agent_v2.WithModel(subModel),
		agent_v2.WithAskUser(),
		agent_v2.WithMaxRounds(5),
	)
	subCap := &nsCapturingResumeSubAgent{name: "sub_agent", inner: subInner}

	tk := agent_v2_newToolkit()
	tk.Register(agent_v2.AgentAsTool(subCap, agent_v2.WithAgentToolDescription("sub agent for resume stability check")))

	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(&parentResumeStableModel{}),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5),
	)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// === Run 1: parent → AgentAsTool(sub) → sub.LLM 调 ask_user 抛 Interrupt ===
	_, err := parent.Run(ctx, sess, schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("Run 1 应抛 InterruptError, got nil")
	}
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("Run 1 应返 InterruptError, got %v (%T)", err, err)
	}
	if ie.Query == nil || ie.Query.InterruptID == "" {
		t.Fatalf("InterruptError.Query 应非空, got %+v", ie)
	}

	subCap.mu.Lock()
	if len(subCap.visitNSs) != 1 {
		t.Fatalf("Run 1 sub-agent 应被调 1 次, got %d (visitNSs=%v)", len(subCap.visitNSs), subCap.visitNSs)
	}
	nsRun1 := subCap.visitNSs[0]
	subCap.mu.Unlock()
	t.Logf("Run 1 sub childNS = %q, InterruptID = %s", nsRun1, ie.Query.InterruptID)

	// === Resume: 答 InterruptID → 父重跑 toolsFn → sub 续跑 → 完成 ===
	events, err := parent.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("answer_payload"),
		}),
		agent_v2.WithResumeSession(sess),
	)
	if err != nil {
		t.Fatalf("Resume 应成功, got %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatalf("Resume 应返 final message")
	}
	t.Logf("Resume final: %q", final.Content)

	subCap.mu.Lock()
	defer subCap.mu.Unlock()
	if len(subCap.visitNSs) < 2 {
		t.Fatalf("Resume sub-agent 应被再次调用, total visits = %d (visitNSs=%v)", len(subCap.visitNSs), subCap.visitNSs)
	}
	nsResume := subCap.visitNSs[len(subCap.visitNSs)-1]
	t.Logf("Resume sub childNS = %q (历次 %v)", nsResume, subCap.visitNSs)

	// **核心断言**: nsRun1 == nsResume (tc.ID hash 跨 Resume 稳定)
	if nsRun1 != nsResume {
		t.Errorf("ISSUE-78 audit C MED-2 fix: Resume 后 sub-agent ctx graphNS 应与 Run 1 相同 (tc.ID hash 稳定);\n"+
			"Run 1 NS=%q\nResume NS=%q\n"+
			"sanity-revert: 改 hash 函数混入 round (如 `hash + round * P`) → Resume NS 不同 → 测试 fail",
			nsRun1, nsResume)
	}
}

// 防 unused import: compose_v2 / tool 都已用
var _ = tool.NewToolkit
