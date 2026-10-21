package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/skill"
	"crew-go/tool"
)

// askInterruptModel 是 sub-agent 内置 ask_user 的 mock：
//   - 第一次 LLM 调用返 tool_call(ask_user, {"question":"need clarify?"})
//   - 第二次 LLM 调用返 final answer "sub-agent done with: <answer>"
//
// 用于 fork sub-agent + HITL 真实场景——sub LLM 主动抛 InterruptError，框架走
// 完整 cper SaveNS / Resume / cm.Restore 路径。
type askInterruptModel struct {
	calls int
}

func (m *askInterruptModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.calls++
	if m.calls == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID:       "sub_tc1",
			Type:     "function",
			Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"need clarify?"}`},
		}}), nil
	}
	// 第二次 LLM 看到 ask_user 的 tool_result 含用户答案
	last := ""
	for _, m := range msgs {
		if m == nil {
			continue
		}
		if m.Role == schema.RoleTool {
			last = m.Content
		}
	}
	return schema.AssistantMessage("sub-agent done with: " + last), nil
}
func (m *askInterruptModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *askInterruptModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*askInterruptModel)(nil)

// TestForkMode_E2E_HITLResumeWithCheckpointer 是 P2 收尾 e2e 端到端测试——
// 验证 fork sub-agent 内 ask_user 抛 InterruptError + 父 BasicAgent + cper 启用时
// 的完整 HITL Resume 闭环。
//
// **覆盖盲区（R15 audit + R16 audit 反复提到）**：
//   - 之前所有 fork 测试都用默认 BasicAgent **无 cper**——R15 HIGH-1 (ns 串台) 只在
//     cper 启用时才显现，单测掩盖。本测试启 cper 跑全链路。
//   - R14 HIGH-2 fix 让 fork sub-agent InterruptError propagate 到父——本测试验证
//     propagate 后父 SaveNS + Resume 重建 sub-agent + cm.Restore + GetResumeAnswer
//     命中的完整路径。
//   - R15 HIGH-1 fix push child graphNS "skill_<name>"——sub cp 写到独立 ns 与父
//     cp 不冲突，本测试验证 cper 启用下不抛 ErrGraphIDConflict / cleanup 不误清。
//
// **bomb 防御**：sanity-revert R15 HIGH-1 fix（去 ns push）→ sub cp 写到 (sid, parentNS)
// 与父 cp 同 key 抛 ErrGraphIDConflict → fail。R14 HIGH-2 fix（去 InterruptError
// propagate）→ Resume 后 ask_user 看不到答案 silent fail。
func TestForkMode_E2E_HITLResumeWithCheckpointer(t *testing.T) {
	box := &forkBox{
		skills: []skill.SkillDescriptor{
			{Name: "ask_skill", Description: "ask user something", Context: "fork", Agent: "ask_agent"},
		},
		activations: map[string]string{"ask_skill": "you ask user via ask_user tool"},
	}

	// **关键**：sub-agent 必须显式注 ask_user 工具（R16 MED-1 已知限制）
	subModel := &askInterruptModel{}
	subAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("ask_agent"),
		agent_v2.WithModel(subModel),
		agent_v2.WithAskUser(), // 必须显式——R16 audit MED-1 doc 已说
		agent_v2.WithMaxRounds(5),
	)
	resolver := func(name string) (agent_v2.Agent, error) {
		if name == "ask_agent" {
			return subAgent, nil
		}
		return nil, nil
	}

	// **R18 audit HIGH-1 fix**：父 LLM 必须**真读 input** 才能验证 Resume 链路完整。
	// 旧实现用 newStubModel 按 calls 索引返固定 parentR2 = "parent final integrating: "
	// → Resume 后 sub-agent 答案 "blue" 是否真传到父根本不验证（断言只 Contains
	// "parent final integrating" 字符串恒过 → 5 类炸弹·第 5 类）。
	//
	// 新实现：parentReplayLLM r1 返 tool_call(fork)，r2 **从 msgs 找 last tool_result**
	// 拼到 final 内容里——sub-agent 答案"blue"必须走完整链路才能进 final。
	parentLLM := &parentReplayLLM{}
	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentLLM),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSubAgentResolver(resolver),
		agent_v2.WithMaxRounds(10),
	)

	// 启 cper（关键：R14 HIGH-2 / R15 HIGH-1 修复都要 cper 启用才显现）
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// === Run 1: 父调 fork skill → sub-agent 内 ask_user 抛 InterruptError → 父 SaveNS ===
	_, err := parent.Run(ctx, sess, schema.UserMessage("ask user about preference"))
	if err == nil {
		t.Fatalf("Run 1 应抛 InterruptError（fork sub-agent 内 ask_user 抛中断 propagate 到父），实际 nil")
	}
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("Run 1 应返 InterruptError，实际 %v (%T)", err, err)
	}
	if ie.Query == nil || ie.Query.InterruptID == "" {
		t.Fatalf("InterruptError.Query 应非空 + InterruptID 非空，实际 %+v", ie)
	}
	t.Logf("Run 1 InterruptID = %s, question = %s, sub.calls = %d", ie.Query.InterruptID, ie.Query.Question, subModel.calls)

	// 验证 cper 真存了 cp（R15 HIGH-1 fix：sub-agent push child ns "skill_ask_skill"）
	exists, _ := cper.ExistsNS(ctx, sess.RunID, "")
	if !exists {
		t.Errorf("cper 应存了父 cp，实际 ExistsNS 返 false——R15 HIGH-1 ns 协议可能漏写")
	}

	// 关键断言：Run 1 sub-agent 应已被调 1 次（抛 Interrupt 之前）
	if subModel.calls != 1 {
		t.Errorf("Run 1 sub-agent LLM 应调 1 次（首次抛 ask_user Interrupt），实际 %d", subModel.calls)
	}

	// === Run 2: Resume + 答 InterruptID → sub-agent 重跑命中 ResumeAnswer → 完成 → 父收 final ===
	events, err := parent.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("blue"),
		}),
		agent_v2.WithResumeSession(sess),
	)
	if err != nil {
		t.Fatalf("Run 2 Resume 应成功，实际 %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatalf("Resume 应返 final message")
	}
	t.Logf("Run 2 final: %q, sub.calls = %d", final.Content, subModel.calls)

	// **R18 audit HIGH-1 关键断言**：final.Content 必须**真含 "blue"**（用户答案）。
	// 链路：Resume 答 InterruptID="blue" → sub-agent 重跑 ask_user 工具命中 GetResumeAnswer
	// → ask_user 返 "blue" → askInterruptModel 第 2 次调用读 tool_result 拼成
	// "sub-agent done with: blue" → parent activate_skill tool_result content
	// 包含 "blue" → parentReplayLLM r2 读 tool_result 拼到 final
	// → final.Content = "parent final integrating: ...sub-agent done with: blue..."
	if !strings.Contains(final.Content, "blue") {
		t.Errorf("R18 HIGH-1 回归：Resume 完整链路应让 final 含用户答案 'blue'，实际 %q\n"+
			"假绿模式：parent r2 是固定字符串不读 input → 无论 sub 是否真重跑都 PASS\n"+
			"真链路：Resume 答案 → sub ask_user → sub LLM r2 → parent tool_result → parent r2 final",
			final.Content)
	}

	// 关键断言：sub-agent 应被调 2 次（Run 1 一次抛 Interrupt + Run 2 重跑收答案）
	if subModel.calls != 2 {
		t.Errorf("R18 HIGH-1 回归：sub-agent LLM 应被调 2 次（Run 1 抛 Interrupt + Run 2 重跑），"+
			"实际 %d——可能 Resume 没真重建 sub-agent", subModel.calls)
	}

	// 验证 parent r2 真执行了（不是 r1 直接结束）——至少 2 次 LLM 调用
	if len(parentLLM.allCalls) < 2 {
		t.Errorf("parent LLM 应被调 ≥2 次（r1 fork + r2 final），实际 %d", len(parentLLM.allCalls))
	}

	// 验证 cleanup：Run 完成后 cper 应清掉 cp
	exists2, _ := cper.ExistsNS(ctx, sess.RunID, "")
	if exists2 {
		t.Errorf("Run 完成后 cper 应清掉 cp（cleanupOnSuccess），实际 ExistsNS 仍返 true")
	}
}

// parentReplayLLM 是 R18 audit HIGH-1 fix 用的 input-aware mock——
// r1 返 fork tool_call；r2 **真读 msgs 找 last tool_result** 拼到 final 内容里，
// 让"sub-agent 答案 → tool_result → parent final"完整链路可被断言。
//
// **why 不用 captureMsgsLLM**：captureMsgsLLM 也是按 calls 索引返固定 responses，
// 不读 input。本 mock 是 input-aware 真依赖 input 决定输出。
type parentReplayLLM struct {
	calls    int
	allCalls [][]*schema.Message
}

func (m *parentReplayLLM) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.allCalls = append(m.allCalls, msgs)
	defer func() { m.calls++ }()
	if m.calls == 0 {
		// r1: 调 fork skill
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "parent_tc1", Type: "function",
			Function: schema.FunctionCall{
				Name:      "activate_skill",
				Arguments: `{"skill":"ask_skill","query":"please ask user about color"}`,
			},
		}}), nil
	}
	// r2: 读 msgs 找 last tool_result（fork sub-agent 的 final answer）拼到 final
	last := ""
	for _, x := range msgs {
		if x != nil && x.Role == schema.RoleTool {
			last = x.Content
		}
	}
	return schema.AssistantMessage("parent final integrating: " + last), nil
}
func (m *parentReplayLLM) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *parentReplayLLM) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*parentReplayLLM)(nil)

// TestForkMode_E2E_NSIsolation_NoConflict 验证 fork sub-agent ns 物理分离
// 不与父 cp 冲突——R15 HIGH-1 关键回归。
//
// 用 cper.ListPending / DeleteSubtree 检查 cp key 路径——sub-agent 的 cp 应在
// child ns "skill_<name>" 下，不在 parent ns 直接覆盖。
func TestForkMode_E2E_NSIsolation_NoConflict(t *testing.T) {
	box := &forkBox{
		skills: []skill.SkillDescriptor{
			{Name: "research", Description: "research subtask", Context: "fork", Agent: "research_agent"},
		},
		activations: map[string]string{"research": "you do research"},
	}

	subModel := &askInterruptModel{}
	subAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("research_agent"),
		agent_v2.WithModel(subModel),
		agent_v2.WithAskUser(),
		agent_v2.WithMaxRounds(5),
	)
	resolver := func(_ string) (agent_v2.Agent, error) { return subAgent, nil }

	parentR1 := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "parent_tc1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      "activate_skill",
			Arguments: `{"skill":"research","query":"AI trends"}`,
		},
	}})
	parentR2 := schema.AssistantMessage("done")
	parentStub := newStubModel([]*schema.Message{parentR1, parentR2})

	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentStub),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSubAgentResolver(resolver),
		agent_v2.WithMaxRounds(10),
	)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := parent.Run(ctx, sess, schema.UserMessage("research"))
	if err == nil {
		t.Fatalf("Run 应抛 InterruptError")
	}
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("应返 InterruptError")
	}

	// 关键：扫所有 cp，验证 sub-agent 的 cp 写到 child ns "__skillfork_research" 下
	pending, err := cper.ListPending(ctx, sess.RunID)
	if err != nil {
		t.Fatalf("ListPending err: %v", err)
	}

	t.Logf("Found %d pending cps:", len(pending))
	hasParentCP := false
	hasChildCP := false
	for _, nscp := range pending {
		parentNS := ""
		if nscp.Checkpoint != nil {
			parentNS = nscp.Checkpoint.ParentNS
		}
		t.Logf("  cp NS=%q ParentNS=%q", nscp.NS, parentNS)
		if nscp.NS == "" {
			hasParentCP = true
		}
		if strings.Contains(nscp.NS, "__skillfork_research") {
			hasChildCP = true
		}
	}

	// R15 HIGH-1 关键断言：sub-agent cp 必须在 child ns，不能写到 parent ns（否则 conflict）
	if !hasChildCP {
		t.Errorf("R15 HIGH-1 回归：fork sub-agent 应在 child ns '__skillfork_research' 下有 cp，实际未找到。" +
			"sanity-revert: 去掉 runForkMode ns push → sub cp 写到 parent ns → 与父 cp 同 key 抛 ErrGraphIDConflict")
	}
	_ = hasParentCP // parent cp 也应有但具体路径取决于父 graph 状态
}

// 注：测试用 newStubModel 复用 basic_agent_test.go 的 helper（按 calls 索引返响应）。
// askInterruptModel 是 sub-agent 专用 mock，单独定义在本文件。

// 编译期断言 ToolInfo / Toolkit 存在（避免 unused import）
var _ tool.Tool = (tool.Tool)(nil)
