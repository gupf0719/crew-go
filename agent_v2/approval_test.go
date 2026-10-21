package agent_v2_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// recordTool 记录被执行次数（验证审批 gate 是否真放行/拦截）。
type recordTool struct {
	name  string
	calls atomic.Int32
}

func (t *recordTool) Name() string                { return t.name }
func (t *recordTool) Description() string         { return "record tool" }
func (t *recordTool) InputSchema() map[string]any { return map[string]any{} }
func (t *recordTool) Call(_ context.Context, _ map[string]any) (*tool.Result, error) {
	t.calls.Add(1)
	return tool.Success("ran_" + t.name), nil
}

var _ tool.Tool = (*recordTool)(nil)

func oneToolCall(name string) *schema.Message {
	return schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tc1", Type: "function", Function: schema.FunctionCall{Name: name, Arguments: `{"x":1}`}},
	})
}

// TestApproval_Auto_NoInterrupt：默认 Auto 模式——工具直接执行，无中断（零行为改动 sanity）。
func TestApproval_Auto_NoInterrupt(t *testing.T) {
	rt := &recordTool{name: "do_thing"}
	tk := agent_v2_newToolkit()
	tk.Register(rt)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithModel(newStubModel([]*schema.Message{oneToolCall("do_thing"), schema.AssistantMessage("done")})),
	)
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Auto 模式不应中断: %v", err)
	}
	if rt.calls.Load() != 1 {
		t.Errorf("Auto 模式工具应直接执行 1 次, 实际 %d", rt.calls.Load())
	}
}

// TestApproval_Ask_Approve：Ask 模式——工具执行前抛 tool_approval 中断 → Resume approve → 执行。
func TestApproval_Ask_Approve(t *testing.T) {
	rt := &recordTool{name: "do_thing"}
	tk := agent_v2_newToolkit()
	tk.Register(rt)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithModel(newStubModel([]*schema.Message{oneToolCall("do_thing"), schema.AssistantMessage("final")})),
	)
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("Ask 模式应抛中断, got err=%v", err)
	}
	if ie.Reason != "tool_approval" {
		t.Errorf("中断 Reason 应 tool_approval, 实际 %q", ie.Reason)
	}
	// 四档：这一次 / 这件事 / 这个工具 / 拒绝。
	// **「这件事」和「这个工具」必须是两个独立选项**——用户心里的「一直允许」通常指
	// 前者（"读这个项目的文件"）而不是后者（"读任何文件，永远"）。合成一个答案时框架
	// 只能猜，而猜宽了就是把用户没打算给的权限给出去。
	wantOpts := []string{
		agent_v2.ApprovalOptionApprove,
		agent_v2.ApprovalOptionApproveAlways,
		agent_v2.ApprovalOptionApproveAlwaysTool,
		agent_v2.ApprovalOptionDeny,
	}
	if ie.Query == nil || len(ie.Query.Options) != len(wantOpts) {
		t.Fatalf("审批 Query 选项不对: %+v", ie.Query)
	}
	for i, w := range wantOpts {
		if ie.Query.Options[i] != w {
			t.Fatalf("选项 [%d] = %q, want %q（顺序也是契约：调用方按下标渲染）",
				i, ie.Query.Options[i], w)
		}
	}
	if rt.calls.Load() != 0 {
		t.Errorf("中断时工具不应已执行, 实际 %d", rt.calls.Load())
	}

	events, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApprove)}))
	if err != nil {
		t.Fatalf("Resume approve 不应报错: %v", err)
	}
	if rt.calls.Load() != 1 {
		t.Errorf("approve 后工具应执行 1 次, 实际 %d", rt.calls.Load())
	}
	if final := agent_v2.FinalMessage(events); final == nil || !strings.Contains(final.Content, "final") {
		t.Errorf("approve 后应跑到 final, got %+v", final)
	}
}

// TestApproval_Ask_Deny：Resume deny → 工具不执行，返工具级 error 给 LLM 继续。
func TestApproval_Ask_Deny(t *testing.T) {
	rt := &recordTool{name: "do_thing"}
	tk := agent_v2_newToolkit()
	tk.Register(rt)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithModel(newStubModel([]*schema.Message{oneToolCall("do_thing"), schema.AssistantMessage("ok denied")})),
	)
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应抛中断, got %v", err)
	}
	_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionDeny)}))
	if err != nil {
		t.Fatalf("Resume deny 不应报错（拒绝是工具级失败非 Run 失败）: %v", err)
	}
	if rt.calls.Load() != 0 {
		t.Errorf("deny 后工具不应执行, 实际 %d", rt.calls.Load())
	}
}

// TestApproval_ApproveAlways_RuleSkips：approve_always 记 don't-ask-again 规则 → 同工具下次不再中断。
func TestApproval_ApproveAlways_RuleSkips(t *testing.T) {
	rt := &recordTool{name: "do_thing"}
	tk := agent_v2_newToolkit()
	tk.Register(rt)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		// 两轮 tool_call + done：approve_always 后第二轮同工具应直接跑（规则命中、不再中断）
		agent_v2.WithModel(newStubModel([]*schema.Message{
			oneToolCall("do_thing"), oneToolCall("do_thing"), schema.AssistantMessage("done"),
		})),
	)
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首轮应抛中断, got %v", err)
	}
	// approve_always → 第二轮同工具不应再抛中断
	_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApproveAlways)}))
	if _, stillInt := compose_v2.IsInterrupt(err); stillInt {
		t.Fatalf("approve_always 后第二轮同工具不应再中断（规则应命中）, 仍中断 err=%v", err)
	}
	if err != nil {
		t.Fatalf("Resume 不应报错: %v", err)
	}
	if rt.calls.Load() != 2 {
		t.Errorf("两轮工具都应执行（approve_always 跳过第二次询问）, 实际 %d", rt.calls.Load())
	}
}

// TestApproval_Fanout_MultiTool：N>=2 工具同轮都需审批 → fanout 收集 + MergeInterrupts 一次问完
// （多 query）→ Resume 全 approve → 都执行。配 -race 验 fanout 下审批 gate 并发安全。
func TestApproval_Fanout_MultiTool(t *testing.T) {
	rt1 := &recordTool{name: "t1"}
	rt2 := &recordTool{name: "t2"}
	tk := agent_v2_newToolkit()
	tk.Register(rt1)
	tk.Register(rt2)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "tc1", Type: "function", Function: schema.FunctionCall{Name: "t1", Arguments: `{}`}},
				{ID: "tc2", Type: "function", Function: schema.FunctionCall{Name: "t2", Arguments: `{}`}},
			}),
			schema.AssistantMessage("done"),
		})),
	)
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应抛中断, got %v", err)
	}
	queries := ie.AllQueries()
	if len(queries) != 2 {
		t.Fatalf("2 工具同轮应聚合 2 个审批 query, 实际 %d", len(queries))
	}
	inputs := map[string]*schema.Message{}
	for _, q := range queries {
		inputs[q.InterruptID] = schema.UserMessage(agent_v2.ApprovalOptionApprove)
	}
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(inputs)); err != nil {
		t.Fatalf("Resume 全 approve 不应报错: %v", err)
	}
	if rt1.calls.Load() != 1 || rt2.calls.Load() != 1 {
		t.Errorf("两工具都应执行 1 次, 实际 t1=%d t2=%d", rt1.calls.Load(), rt2.calls.Load())
	}
}

// TestApproval_SameNameTool_DenyNotOverridden（audit #4 race fix）：同轮同名工具 2 个 tc，一个
// approve_always 一个 deny → per-call deny 优先于 don't-ask-again 规则，被 deny 的不执行。
func TestApproval_SameNameTool_DenyNotOverridden(t *testing.T) {
	rt := &recordTool{name: "act"}
	tk := agent_v2_newToolkit()
	tk.Register(rt)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "c1", Type: "function", Function: schema.FunctionCall{Name: "act", Arguments: `{"i":1}`}},
				{ID: "c2", Type: "function", Function: schema.FunctionCall{Name: "act", Arguments: `{"i":2}`}},
			}),
			schema.AssistantMessage("done"),
		})),
	)
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应抛中断, got %v", err)
	}
	queries := ie.AllQueries()
	if len(queries) != 2 {
		t.Fatalf("同名 2 tc 应 2 个独立 query, 实际 %d", len(queries))
	}
	// 一个 approve_always 一个 deny——被 deny 的不应被 approve_always 规则抢跑（per-call 优先）
	_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		queries[0].InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApproveAlways),
		queries[1].InterruptID: schema.UserMessage(agent_v2.ApprovalOptionDeny),
	}))
	if err != nil {
		t.Fatalf("Resume 不应报错: %v", err)
	}
	if rt.calls.Load() != 1 {
		t.Errorf("approve_always 一个 + deny 一个 → 应只执行 1 次（deny 不被规则覆盖）, 实际 %d", rt.calls.Load())
	}
}

// TestApproval_Heuristic_AllowInAsk（audit #3 缺口）：Ask 模式 + 启发式 Allow 白名单工具 → 直接执行无中断。
func TestApproval_Heuristic_AllowInAsk(t *testing.T) {
	rt := &recordTool{name: "safe"}
	tk := agent_v2_newToolkit()
	tk.Register(rt)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithApprovalHeuristic(func(_ context.Context, name string, _ map[string]any) agent_v2.ApprovalDecision {
			if name == "safe" {
				return agent_v2.ApprovalDecisionAllow // 白名单：Ask 模式下也直接放行
			}
			return agent_v2.ApprovalDecisionDefault
		}),
		agent_v2.WithModel(newStubModel([]*schema.Message{oneToolCall("safe"), schema.AssistantMessage("done")})),
	)
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Ask 模式 + 启发式 Allow 应无中断直接放行: %v", err)
	}
	if rt.calls.Load() != 1 {
		t.Errorf("启发式 Allow 工具应直接执行 1 次, 实际 %d", rt.calls.Load())
	}
}

// TestApproval_ApproveAlways_CrossResume（audit #2/#5）：approve_always 规则跨**第二次独立 Resume**
// 持久（caller 传 WithResumeSession 复用 session）→ 同工具再调不再审批。
func TestApproval_ApproveAlways_CrossResume(t *testing.T) {
	rt := &recordTool{name: "act"}
	tk := agent_v2_newToolkit()
	tk.Register(rt)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(8),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), agent_v2.WithAskUser(),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			oneToolCall("act"), // round 1：业务工具 → 审批中断
			schema.AssistantToolCallMessage([]schema.ToolCall{ // round 2：ask_user → 第二个中断，强制第二次 Resume
				{ID: "au1", Type: "function", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"q?"}`}},
			}),
			oneToolCall("act"), // round 3：同业务工具 → approve_always 规则应跨 Resume 命中、不再审批
			schema.AssistantMessage("done"),
		})),
	)
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.Reason != "tool_approval" {
		t.Fatalf("round 1 应审批中断, got %v", err)
	}
	// Resume #1（WithResumeSession 复用 session 让规则持久）approve_always → round 2 ask_user 中断
	_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApproveAlways)}))
	ie2, ok := compose_v2.IsInterrupt(err)
	if !ok || ie2.Reason != "ask_user" {
		t.Fatalf("round 2 应 ask_user 中断（非审批，approve_always 已放行 round 1 工具）, got %v", err)
	}
	if rt.calls.Load() != 1 {
		t.Fatalf("round 1 工具应已执行 1 次, 实际 %d", rt.calls.Load())
	}
	// Resume #2（WithResumeSession）答 ask_user → round 3 同工具：规则应跨 Resume 命中、不再审批
	_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie2.Query.InterruptID: schema.UserMessage("ok")}))
	if _, stillInt := compose_v2.IsInterrupt(err); stillInt {
		t.Fatalf("round 3 同工具 approve_always 规则应跨 Resume 命中不再审批, 仍中断: %v", err)
	}
	if err != nil {
		t.Fatalf("Resume #2 不应报错: %v", err)
	}
	if rt.calls.Load() != 2 {
		t.Errorf("round 3 工具应规则命中执行（共 2 次）, 实际 %d", rt.calls.Load())
	}
}

// TestApproval_PlainApprove_NotCrossRound（audit G1）：plain approve 只批本次调用——下一轮同工具
// （新 visit → 新 InterruptID）应**重新审批**（区别于 approve_always）。守护 approve≠markToolApproved。
func TestApproval_PlainApprove_NotCrossRound(t *testing.T) {
	rt := &recordTool{name: "act"}
	tk := agent_v2_newToolkit()
	tk.Register(rt)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			oneToolCall("act"), oneToolCall("act"), schema.AssistantMessage("done"),
		})),
	)
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("round 1 应中断, got %v", err)
	}
	// plain approve（非 always）→ round 1 工具执行；round 2 同工具应**再次**审批
	_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApprove)}))
	ie2, stillInt := compose_v2.IsInterrupt(err)
	if !stillInt || ie2.Reason != "tool_approval" {
		t.Fatalf("plain approve 只批本调用，round 2 同工具应重新审批（非跨轮免审）, got %v", err)
	}
	if rt.calls.Load() != 1 {
		t.Errorf("round 1 工具应执行 1 次, 实际 %d", rt.calls.Load())
	}
}

// selfHITLTool 是一个**自身也抛 HITL 中断**的业务工具（仿 AgentAsTool / 业务自管 HITL）：首次调
// 抛 InterruptError，Resume 后返成功——验证 call-approved 让审批 approve 后工具自身再中断的重跑
// 不重复要审批（audit #1 fix 的目标场景）。
type selfHITLTool struct {
	name  string
	calls atomic.Int32
}

func (t *selfHITLTool) Name() string                { return t.name }
func (t *selfHITLTool) Description() string         { return "self HITL tool" }
func (t *selfHITLTool) InputSchema() map[string]any { return map[string]any{} }
func (t *selfHITLTool) Call(ctx context.Context, _ map[string]any) (*tool.Result, error) {
	t.calls.Add(1)
	id := compose_v2.CurrentInterruptSite(ctx).InterruptID()
	if ans, ok := compose_v2.GetResumeAnswer(ctx, id); ok && ans != nil {
		return tool.Success("self-done:" + ans.Content), nil
	}
	return nil, &compose_v2.InterruptError{Reason: "tool_self_ask", Query: &compose_v2.AskQuery{Question: "self?", InterruptID: id}}
}

var _ tool.Tool = (*selfHITLTool)(nil)

// TestApproval_SelfInterruptingTool_NoReapproval（audit G2 + #1 fix 端到端）：被 approve 的工具自身
// 再抛 HITL → 下次 Resume 重跑 runOne 时 call-approved 命中、不重复要审批。
func TestApproval_SelfInterruptingTool_NoReapproval(t *testing.T) {
	st := &selfHITLTool{name: "subagent"}
	tk := agent_v2_newToolkit()
	tk.Register(st)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithModel(newStubModel([]*schema.Message{oneToolCall("subagent"), schema.AssistantMessage("done")})),
	)
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	// Run → 审批中断（工具还没跑）
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.Reason != "tool_approval" {
		t.Fatalf("应先审批中断, got %v", err)
	}
	// Resume #1（WithResumeSession）approve → 工具跑 → 工具自身抛中断
	_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApprove)}))
	ie2, ok := compose_v2.IsInterrupt(err)
	if !ok || ie2.Reason != "tool_self_ask" {
		t.Fatalf("approve 后工具应执行并抛自身中断（非再次审批）, got %v", err)
	}
	// Resume #2（WithResumeSession）答工具自身的中断 → 重跑 runOne 时审批 gate 应 call-approved 命中、不再审批
	_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie2.Query.InterruptID: schema.UserMessage("go")}))
	if ie3, stillInt := compose_v2.IsInterrupt(err); stillInt {
		t.Fatalf("call-approved 应让重跑跳过再审批, 却又中断 reason=%q（#1 fix 失效）", ie3.Reason)
	}
	if err != nil {
		t.Fatalf("Resume #2 不应报错: %v", err)
	}
}

// TestApproval_Heuristic_Deny：Auto 模式 + 启发式 Deny 特定工具 → 该工具被拒（启发式覆盖 mode）。
func TestApproval_Heuristic_Deny(t *testing.T) {
	rt := &recordTool{name: "danger"}
	tk := agent_v2_newToolkit()
	tk.Register(rt)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithApprovalHeuristic(func(_ context.Context, name string, _ map[string]any) agent_v2.ApprovalDecision {
			if name == "danger" {
				return agent_v2.ApprovalDecisionDeny
			}
			return agent_v2.ApprovalDecisionDefault
		}),
		agent_v2.WithModel(newStubModel([]*schema.Message{oneToolCall("danger"), schema.AssistantMessage("ok")})),
	)
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("启发式 Deny 是工具级拒绝非 Run 失败, 不应报错: %v", err)
	}
	if rt.calls.Load() != 0 {
		t.Errorf("启发式 Deny 的工具不应执行, 实际 %d", rt.calls.Load())
	}
}
