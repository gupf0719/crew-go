package agent_v2_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// perCallModel 是读 input 的 mock ChatModel——记录每次 Generate 收到的 messages
// 快照 + 调用次数（CLAUDE.md 要求 mock 读 input 做断言，否则抓不到"上游吞消息"类
// bug），按 callIdx 经 respFn 返回响应。Stream 返 nil reader 让 streamOrGenerate
// 降级 Generate（同 stubModel 模式）。
type perCallModel struct {
	mu     sync.Mutex
	calls  [][]*schema.Message
	respFn func(callIdx int) *schema.Message
}

func (m *perCallModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	snap := make([]*schema.Message, len(msgs))
	copy(snap, msgs)
	idx := len(m.calls)
	m.calls = append(m.calls, snap)
	return m.respFn(idx), nil
}

func (m *perCallModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *perCallModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

func (m *perCallModel) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *perCallModel) firstCall() []*schema.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.calls) == 0 {
		return nil
	}
	return m.calls[0]
}

var _ model.ChatModel = (*perCallModel)(nil)

func constReply(content string) func(int) *schema.Message {
	return func(int) *schema.Message { return schema.AssistantMessage(content) }
}

// TestPerCallOverride_SystemPrompt 验证 WithRunSystemPrompt 仅本次 Run 覆盖构造期 system prompt。
func TestPerCallOverride_SystemPrompt(t *testing.T) {
	// per-call 覆盖：LLM 收到的首条 system 是 OVERRIDE 而非构造期 BASE
	m := &perCallModel{respFn: constReply("done")}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("t"),
		agent_v2.WithModel(m),
		agent_v2.WithSystemPrompt("BASE_PROMPT"),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"),
		agent_v2.WithRunSystemPrompt("OVERRIDE_PROMPT")); err != nil {
		t.Fatalf("run: %v", err)
	}
	first := m.firstCall()
	if len(first) == 0 || first[0].Role != schema.RoleSystem {
		t.Fatalf("expected first message to be system, got %+v", first)
	}
	if first[0].Content != "OVERRIDE_PROMPT" {
		t.Fatalf("WithRunSystemPrompt not applied: want system content %q, got %q", "OVERRIDE_PROMPT", first[0].Content)
	}

	// sanity：不传 per-call 时用构造期 BASE_PROMPT（保证 old/new 行为分叉，非 tautological）
	m2 := &perCallModel{respFn: constReply("done")}
	ag2 := agent_v2.NewBasicAgent(
		agent_v2.WithName("t2"),
		agent_v2.WithModel(m2),
		agent_v2.WithSystemPrompt("BASE_PROMPT"),
	)
	if _, err := ag2.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("run2: %v", err)
	}
	if got := m2.firstCall(); len(got) == 0 || got[0].Content != "BASE_PROMPT" {
		t.Fatalf("baseline system prompt wrong (expected construct-time BASE_PROMPT): %+v", got)
	}
}

// TestPerCallOverride_Model 验证 WithRunModel 仅本次 Run 覆盖构造期 model（构造期 model
// 不被调用），且 per-call model 可单独满足 "model required"。
func TestPerCallOverride_Model(t *testing.T) {
	mA := &perCallModel{respFn: constReply("FROM_A")}
	mB := &perCallModel{respFn: constReply("FROM_B")}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("t"),
		agent_v2.WithModel(mA),
	)
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"),
		agent_v2.WithRunModel(mB))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "FROM_B" {
		t.Fatalf("WithRunModel not applied: want final FROM_B, got %v", final)
	}
	if mA.callCount() != 0 {
		t.Fatalf("construct-time model A must NOT be called when per-call model B given, got %d calls", mA.callCount())
	}
	if mB.callCount() != 1 {
		t.Fatalf("per-call model B should be called once, got %d", mB.callCount())
	}

	// 构造期无 model 必须 fail-fast——per-call WithRunModel 是"覆盖已有 model"而非"提供
	// 唯一 model"（构造期无 model 只靠 per-call 会在 HITL Resume 崩，故 Run 入口挡掉；
	// audit Finding 1 回归保护）。
	mOnly := &perCallModel{respFn: constReply("ok")}
	agNoModel := agent_v2.NewBasicAgent(agent_v2.WithName("nomodel"))
	_, err = agNoModel.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"),
		agent_v2.WithRunModel(mOnly))
	if err == nil {
		t.Fatalf("construct-time no model must fail-fast even with per-call WithRunModel")
	}
	if !strings.Contains(err.Error(), "model is required") {
		t.Fatalf("expected 'model is required' fail-fast, got: %v", err)
	}
}

// TestPerCallOverride_MaxRounds 验证 WithRunMaxRounds 仅本次 Run 覆盖构造期 maxRounds——
// model 永远返 tool_call 不终止，受 maxRounds→WithMaxVisit 约束触顶，per-call 小值时
// LLM 调用次数明显少于构造期大值（sanity：revert 后两者相等，断言 fail）。
func TestPerCallOverride_MaxRounds(t *testing.T) {
	loopTool := tool.NewFuncTool("noop", "noop",
		map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("ok"), nil })
	tk := tool.NewToolkit()
	tk.Register(loopTool)
	alwaysToolCall := func(int) *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "tc", Function: schema.FunctionCall{Name: "noop", Arguments: "{}"}},
		})
	}
	run := func(opts ...agent_v2.RunOption) int {
		m := &perCallModel{respFn: alwaysToolCall}
		ag := agent_v2.NewBasicAgent(
			agent_v2.WithName("t"),
			agent_v2.WithModel(m),
			agent_v2.WithToolkit(tk),
			agent_v2.WithMaxRounds(10), // 构造期大值
		)
		_, _ = ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"), opts...)
		return m.callCount()
	}

	perCall2 := run(agent_v2.WithRunMaxRounds(2))
	defaultRounds := run() // 用构造期 maxRounds=10

	if perCall2 >= defaultRounds {
		t.Fatalf("WithRunMaxRounds(2) should cap LLM calls below construct-time maxRounds=10: perCall2=%d default=%d", perCall2, defaultRounds)
	}
	if perCall2 == 0 || perCall2 > 6 {
		t.Fatalf("per-call maxRounds=2 expected small call count (~2-4), got %d", perCall2)
	}
}

// TestPerCallOverride_NoLeakToNestedAgent 验证父 BasicAgent 的 per-call 覆盖**不泄漏**
// 给嵌套子 agent（AgentAsTool）——per-call 语义是"仅本层本次 Run"，与 orchHooks 的
// ctx 继承相反。子 agent 必须用自己构造期的 model，而非父的 per-call override（override
// 走 ctx 透传，若不在子 Run 入口清除继承会泄漏）。
func TestPerCallOverride_NoLeakToNestedAgent(t *testing.T) {
	mChild := &perCallModel{respFn: constReply("CHILD_DONE")}
	child := agent_v2.NewBasicAgent(
		agent_v2.WithName("child"),
		agent_v2.WithDescription("child helper agent"),
		agent_v2.WithModel(mChild),
	)
	childTool := agent_v2.AgentAsTool(child)
	tk := tool.NewToolkit()
	tk.Register(childTool)

	// 父 per-call override model：第 1 轮调 child tool，第 2 轮 final
	callChildThenDone := func(idx int) *schema.Message {
		if idx == 0 {
			return schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "tc", Function: schema.FunctionCall{Name: "child", Arguments: "{}"}},
			})
		}
		return schema.AssistantMessage("PARENT_DONE")
	}
	mOverride := &perCallModel{respFn: callChildThenDone}
	mParentBase := &perCallModel{respFn: constReply("PARENT_BASE_SHOULD_NOT_RUN")}

	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(mParentBase),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5),
	)
	if _, err := parent.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"),
		agent_v2.WithRunModel(mOverride)); err != nil {
		t.Fatalf("run: %v", err)
	}
	// 父 per-call override 生效：构造期 base 不被调用
	if mParentBase.callCount() != 0 {
		t.Fatalf("parent construct-time model should not run (per-call override active), got %d calls", mParentBase.callCount())
	}
	// 关键：子 agent 必须用自己的 model——父 per-call override 不泄漏
	// （mChild calls=0 说明 override 泄漏到子，子错用了父的 mOverride）
	if mChild.callCount() != 1 {
		t.Fatalf("nested child must use its OWN model (parent per-call override must NOT leak into child): mChild calls=%d", mChild.callCount())
	}
}

// TestPerCallOverride_ResumeCanRetarget 验证 Resume 也能传 per-call 覆盖（对齐
// eino/langgraph：resume 是一次正常调用、可重传 per-call）。Run 用 WithRunModel、
// 中断后 Resume 用 WithResumeModel——续跑用 Resume 的 model，而非构造期 base、也非 Run 的。
func TestPerCallOverride_ResumeCanRetarget(t *testing.T) {
	// ask 工具：首次抛 InterruptError，Resume 命中答案后返结果（二阶段幂等）
	askTool := tool.NewFuncTool("ask", "ask user",
		map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "ask_id"); ok {
				return tool.Success("user: " + ans), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "ask_id", Question: "?"},
			}
		})
	tk := tool.NewToolkit()
	tk.Register(askTool)

	askCall := func(int) *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "tc", Function: schema.FunctionCall{Name: "ask", Arguments: "{}"}},
		})
	}
	mBase := &perCallModel{respFn: constReply("BASE_SHOULD_NOT_RUN_ON_RESUME")}
	mRun := &perCallModel{respFn: askCall}                            // Run：调 ask → 中断
	mResume := &perCallModel{respFn: constReply("RESUME_MODEL_DONE")} // Resume：续跑 final

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("t"),
		agent_v2.WithModel(mBase),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5),
	)
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// Run with WithRunModel(mRun)：mRun 返 ask tool_call → 中断
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"), agent_v2.WithRunModel(mRun))
	if _, isInt := agent_v2.IsInterrupt(err); !isInt {
		t.Fatalf("expected interrupt from Run, got %v", err)
	}

	// Resume with WithResumeModel(mResume)：ask 返答案 → mResume 返 final
	events, err := ag.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{"ask_id": schema.UserMessage("ok")}),
		agent_v2.WithResumeModel(mResume),
		agent_v2.WithResumeSession(sess),
	)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "RESUME_MODEL_DONE" {
		t.Fatalf("WithResumeModel not applied: want RESUME_MODEL_DONE, got %v", final)
	}
	// 构造期 base model 全程不该被调（Run 用 mRun、Resume 用 mResume）
	if mBase.callCount() != 0 {
		t.Fatalf("construct-time base model must not run when Run/Resume both override: got %d calls", mBase.callCount())
	}
	if mResume.callCount() < 1 {
		t.Fatalf("WithResumeModel model should be called on resume, got %d", mResume.callCount())
	}
}

// TestPerCallOverride_SystemPromptFixedInCheckpoint 锁定 systemPrompt 语义：它由 preamble
// 首轮生成进 cp.Channels，中断在 preamble 之后（tools 中断）时 Resume 不重跑 preamble，
// model 续轮收到 cp 固化的 system（= Run 时 WithRunSystemPrompt 的值）。这是**无
// WithResumeSystemPrompt** 的依据——Resume 改不动已固化的 system（区别于 model/maxRounds
// 每次 Resume 运行时/编译期重 resolve；三方亦无 resume 改 prompt 的 option）。
func TestPerCallOverride_SystemPromptFixedInCheckpoint(t *testing.T) {
	askTool := tool.NewFuncTool("ask", "ask user",
		map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "ask_id"); ok {
				return tool.Success("user: " + ans), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "ask_id", Question: "?"},
			}
		})
	tk := tool.NewToolkit()
	tk.Register(askTool)

	m := &perCallModel{respFn: func(idx int) *schema.Message {
		if idx == 0 {
			return schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "tc", Function: schema.FunctionCall{Name: "ask", Arguments: "{}"}},
			})
		}
		return schema.AssistantMessage("done")
	}}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("t"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithSystemPrompt("BASE"),
		agent_v2.WithMaxRounds(5),
	)
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"), agent_v2.WithRunSystemPrompt("RUN_PROMPT"))
	if _, isInt := agent_v2.IsInterrupt(err); !isInt {
		t.Fatalf("expected interrupt, got %v", err)
	}
	// Resume 续跑（无法传 WithResumeSystemPrompt——已删）→ model 第2轮收到 cp 固化的 RUN_PROMPT
	_, err = ag.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{"ask_id": schema.UserMessage("ok")}),
		agent_v2.WithResumeSession(sess),
	)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(m.calls) < 2 {
		t.Fatalf("expected >=2 model calls (run + resume), got %d", len(m.calls))
	}
	run1Sys := m.calls[0][0].Content
	resume2Sys := m.calls[1][0].Content
	if run1Sys != "RUN_PROMPT" {
		t.Fatalf("Run 第1轮 system 应是 WithRunSystemPrompt=RUN_PROMPT，got %q", run1Sys)
	}
	// 锁定：Resume 续跑用 cp 固化的 system（= Run 时的 RUN_PROMPT），systemPrompt 不重 resolve，
	// 故无 WithResumeSystemPrompt option。
	if resume2Sys != "RUN_PROMPT" {
		t.Fatalf("Resume 续跑 system 应为 cp 固化的 RUN_PROMPT（systemPrompt 不重 resolve），got %q", resume2Sys)
	}
}
