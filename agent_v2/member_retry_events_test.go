package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// 编排的成员重试：一次尝试写进会话的事件，只在它被下一次从头再跑的尝试取代时作废（没有存档、从头再来），重来
// 那次的上文跟第一次一样；成功的、下一次从存档续的（配了存档）、不再有下一次尝试的（重试用完、终局失败、panic）
// 都照常留下。
//
// 曾经成员直接往编排的会话里写：没配存档时第一次尝试在 preamble 记下的输入事件留在会话里，重来那次从事件
// 读上文、又接一遍本次输入，模型看到同一个问题两次（实测 dev 上即如此）。

const retryQuestion = "总结一下"

// failOnce 是第一次调用失败（可重试的上游错）、之后答复的脚本。
func failOnce(answer string) []func([]*schema.Message) (*schema.Message, error) {
	return []func([]*schema.Message) (*schema.Message, error){
		func([]*schema.Message) (*schema.Message, error) { return nil, errors.New("upstream 503") },
		reply(schema.AssistantMessage(answer)),
	}
}

func countUserText(msgs []*schema.Message, text string) int {
	n := 0
	for _, m := range msgs {
		if m != nil && m.Role == schema.RoleUser && m.Content == text {
			n++
		}
	}
	return n
}

// eventTexts 数会话事件里某个 agent 名下、内容是 text 的消息有几条。
func eventTexts(sess *agent_v2.RunSession, agent, text string) int {
	n := 0
	for _, ev := range sess.Events() {
		if ev.AgentName == agent && ev.Output != nil && ev.Output.Message != nil && ev.Output.Message.Content == text {
			n++
		}
	}
	return n
}

// sanity-revert：成员直接写编排的会话（或尝试失败也提交）⟹ 第二次尝试的模型输入里问题出现两次，本用例红。
func TestMemberRetry_FailedAttemptInputNotReread(t *testing.T) {
	m := &scriptModel{steps: failOnce("ok")}
	step := agent_v2.NewBasicAgent(agent_v2.WithName("retry_step"), agent_v2.WithModel(m))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("retry_seq"), agent_v2.WithSequentialMaxRetries(1)).
		AddStep("only", step)
	sess := agent_v2.NewRunSession()
	if _, err := seq.Run(context.Background(), sess, schema.UserMessage(retryQuestion)); err != nil {
		t.Fatalf("重试之后该成功，实际 %v", err)
	}
	if m.count() != 2 {
		t.Fatalf("该调两次模型，实际 %d 次", m.count())
	}
	if got := countUserText(m.call(1), retryQuestion); got != 1 {
		t.Errorf("重来那次的上文该跟第一次一样、问题只出现一次，实际 %d 次：%+v", got, m.call(1))
	}
	if got := eventTexts(sess, "retry_step", retryQuestion); got != 1 {
		t.Errorf("会话事件里成员的输入该只记一次（失败那次作废），实际 %d 次", got)
	}
	if got := eventTexts(sess, "retry_step", "ok"); got != 1 {
		t.Errorf("成功那次的答复该提交进会话事件，实际 %d 次", got)
	}
}

// 配了存档：第一次尝试失败留下可续的存档，第二次尝试从存档续（preamble 不重跑）——第一次写下的输入事件属于
// 留下来的进度，照常提交。
//
// sanity-revert：尝试一失败就丢掉它的事件 ⟹ 续跑不会再写输入，会话里这一问没了，本用例红。
func TestMemberRetry_CheckpointedAttemptKeepsEvents(t *testing.T) {
	m := &scriptModel{steps: failOnce("ok")}
	step := agent_v2.NewBasicAgent(agent_v2.WithName("retry_cp_step"), agent_v2.WithModel(m))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("retry_cp_seq"), agent_v2.WithSequentialMaxRetries(1)).
		AddStep("only", step)
	sess := agent_v2.NewRunSession()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	if _, err := seq.Run(ctx, sess, schema.UserMessage(retryQuestion)); err != nil {
		t.Fatalf("重试之后该成功，实际 %v", err)
	}
	if got := countUserText(m.call(1), retryQuestion); got != 1 {
		t.Errorf("续跑那次的模型输入里问题该只出现一次，实际 %d 次：%+v", got, m.call(1))
	}
	if got := eventTexts(sess, "retry_cp_step", retryQuestion); got != 1 {
		t.Errorf("会话事件里成员的输入该恰好一次（进度留着，事件也留着），实际 %d 次", got)
	}
}

// 成员本身是个编排：它里面的第一步成功了、第二步失败，没有存档时整个成员从头重来——第一步那次成功写下的事件
// 跟着这次尝试一起作废，第一步重跑时的上文跟第一次一样。
//
// sanity-revert：只在 BasicAgent 自己那次调用上按结局作废（不在成员尝试这一层）⟹ 第一步重跑时上文里有上次
// 的问答，本用例红。
func TestMemberRetry_NestedDiscardDropsSucceededInnerEvents(t *testing.T) {
	m1 := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		reply(schema.AssistantMessage("step1 done")), reply(schema.AssistantMessage("step1 done again")),
	}}
	m2 := &scriptModel{steps: failOnce("step2 done")}
	s1 := agent_v2.NewBasicAgent(agent_v2.WithName("nested_s1"), agent_v2.WithModel(m1))
	s2 := agent_v2.NewBasicAgent(agent_v2.WithName("nested_s2"), agent_v2.WithModel(m2))
	inner := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("nested_inner")).AddStep("s1", s1).AddStep("s2", s2)
	outer := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("nested_outer"), agent_v2.WithSequentialMaxRetries(1)).
		AddStep("inner", inner)
	if _, err := outer.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage(retryQuestion)); err != nil {
		t.Fatalf("重试之后该成功，实际 %v", err)
	}
	if m1.count() != 2 {
		t.Fatalf("第一步该跑两次（成员从头重来），实际 %d 次", m1.count())
	}
	second := m1.call(1)
	if got := countUserText(second, retryQuestion); got != 1 {
		t.Errorf("第一步重跑时问题该只出现一次，实际 %d 次：%+v", got, second)
	}
	for _, msg := range second {
		if msg.Role == schema.RoleAssistant && msg.Content == "step1 done" {
			t.Errorf("作废那次尝试里第一步的答复不该进重跑时的上文：%+v", second)
		}
	}
}

// 转交的候选重试（各看各的历史视图，候选自己记输入）：同上。
func TestHandoffRetry_FailedAttemptInputNotReread(t *testing.T) {
	m := &scriptModel{steps: failOnce("ok")}
	cand := agent_v2.NewBasicAgent(agent_v2.WithName("cand"), agent_v2.WithModel(m))
	h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("retry_handoff"), agent_v2.WithHandoffInitial("cand"),
		agent_v2.WithHandoffHistoryMode(agent_v2.HandoffHistorySelfOnly), agent_v2.WithHandoffMaxRetries(1)).
		AddAgent("cand", cand)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage(retryQuestion)); err != nil {
		t.Fatalf("重试之后该成功，实际 %v", err)
	}
	if got := countUserText(m.call(1), retryQuestion); got != 1 {
		t.Errorf("候选重来那次的上文里问题该只出现一次，实际 %d 次：%+v", got, m.call(1))
	}
}

// 并行分支的成员重试：分支本来就在自己的 lane 上，重试那一层再套一层，同上。
func TestParallelRetry_FailedAttemptInputNotReread(t *testing.T) {
	m := &scriptModel{steps: failOnce("ok")}
	branch := agent_v2.NewBasicAgent(agent_v2.WithName("pbranch"), agent_v2.WithModel(m))
	p := agent_v2.NewParallelAgent(agent_v2.WithParallelName("retry_parallel"), agent_v2.WithParallelMaxRetries(1)).
		AddBranch("b", branch)
	if _, err := p.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage(retryQuestion)); err != nil {
		t.Fatalf("重试之后该成功，实际 %v", err)
	}
	if got := countUserText(m.call(1), retryQuestion); got != 1 {
		t.Errorf("分支重来那次的上文里问题该只出现一次，实际 %d 次：%+v", got, m.call(1))
	}
}

// 最后那次尝试的事件照常留下（不论成败）：只有被下一次从头再跑的尝试取代的那次才作废。配了存档但成员终局失败
// （ErrRunAborted）时没有下一次尝试，它写下的输入事件照常留在会话里。
//
// sanity-revert：最后那次失败尝试的事件也丢掉 ⟹ 会话里这一问没了，本用例红。
func TestMemberRetry_FinalFailedAttemptKeepsEvents(t *testing.T) {
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		func([]*schema.Message) (*schema.Message, error) {
			return nil, fmt.Errorf("bad request: %w", compose_v2.ErrRunAborted)
		},
	}}
	step := agent_v2.NewBasicAgent(agent_v2.WithName("terminal_step"), agent_v2.WithModel(m))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("terminal_seq")).AddStep("only", step)
	sess := agent_v2.NewRunSession()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	if _, err := seq.Run(ctx, sess, schema.UserMessage(retryQuestion)); !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("该终局失败，实际 %v", err)
	}
	if got := eventTexts(sess, "terminal_step", retryQuestion); got != 1 {
		t.Errorf("最后那次尝试写下的输入事件该照常留下，实际 %d 条", got)
	}
}

// 没配存档、成员失败、没有重试：最后那次尝试的事件照常留在会话里、交给调用方（跟配了存档时一样）。
//
// sanity-revert：没配存档的失败一律丢事件 ⟹ 会话里这一问没了，本用例红。
func TestMemberRetry_FinalFailureWithoutCheckpointerKeepsEvents(t *testing.T) {
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		func([]*schema.Message) (*schema.Message, error) { return nil, errors.New("upstream 503") },
	}}
	step := agent_v2.NewBasicAgent(agent_v2.WithName("final_fail_step"), agent_v2.WithModel(m))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("final_fail_seq")).AddStep("only", step)
	sess := agent_v2.NewRunSession()
	if _, err := seq.Run(context.Background(), sess, schema.UserMessage(retryQuestion)); err == nil {
		t.Fatal("成员该失败")
	}
	if got := eventTexts(sess, "final_fail_step", retryQuestion); got != 1 {
		t.Errorf("没有下一次尝试，这次写下的输入事件该照常留下，实际 %d 条", got)
	}
}

// 成员轮次用完走软降落：返回带收尾答复的 events + MaxIterationsError（不可重试、没有下一次尝试）。没配存档时这条
// 答复照样要交给调用方，用 FinalMessage 取得到。
//
// sanity-revert：把「报错且没配存档」一律当作作废 ⟹ 编排返回的 events 里没有这条答复，本用例红（审计实测）。
func TestMemberRetry_SoftLandingAnswerKeptWithoutCheckpointer(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("search", "search", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("results"), nil }))
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		reply(toolCallsMessage("search")),
		reply(schema.AssistantMessage("收尾答复")), // 最后一轮摘掉了工具，只能答
	}}
	step := agent_v2.NewBasicAgent(agent_v2.WithName("soft_step"), agent_v2.WithToolkit(tk), agent_v2.WithModel(m),
		agent_v2.WithMaxRounds(1))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("soft_seq")).AddStep("only", step)
	events, err := seq.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage(retryQuestion))
	var maxIter *compose_v2.MaxIterationsError
	if !errors.As(err, &maxIter) {
		t.Fatalf("轮次用完该返回 MaxIterationsError，实际 %v", err)
	}
	if f := agent_v2.FinalMessage(events); f == nil || f.Content != "收尾答复" {
		t.Errorf("软降落的答复该在编排返回的 events 里，实际 %+v", f)
	}
}

// 转交的候选失败、没有重试（各看各的历史视图，候选自己记输入）：最后那次尝试的事件照常留下。
//
// sanity-revert：Handoff 那处重试结束后不结账 ⟹ 候选写下的输入事件没了，本用例红。
func TestHandoffRetry_FinalFailureKeepsEvents(t *testing.T) {
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		func([]*schema.Message) (*schema.Message, error) { return nil, errors.New("upstream 503") },
	}}
	cand := agent_v2.NewBasicAgent(agent_v2.WithName("final_cand"), agent_v2.WithModel(m))
	h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("final_handoff"), agent_v2.WithHandoffInitial("cand"),
		agent_v2.WithHandoffHistoryMode(agent_v2.HandoffHistorySelfOnly)).AddAgent("cand", cand)
	sess := agent_v2.NewRunSession()
	if _, err := h.Run(context.Background(), sess, schema.UserMessage(retryQuestion)); err == nil {
		t.Fatal("候选该失败")
	}
	if got := eventTexts(sess, "final_cand", retryQuestion); got != 1 {
		t.Errorf("没有下一次尝试，候选写下的输入事件该照常留下，实际 %d 条", got)
	}
}

// panicShell 是没兜住 panic 的业务自研 agent：往会话里写一条事件之后 panic。
type panicShell struct{ name string }

func (a *panicShell) Name() string        { return a.name }
func (a *panicShell) Description() string { return "" }
func (a *panicShell) Run(_ context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	sess.AddEvent(agent_v2.NewOutputEvent(a.name, schema.AssistantMessage(panicShellText)))
	panic("boom")
}
func (a *panicShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

const panicShellText = "panic 之前写下的"

// 成员 panic：panic 越过重试往上走，不会再有下一次尝试取代这一次——它写下的事件照常留下（同重试用完时的最后一次）。
//
// sanity-revert：只在尝试正常返回时处置它的 lane ⟹ panic 那次写下的事件没了，本用例红。
func TestMemberRetry_PanickedAttemptKeepsEvents(t *testing.T) {
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("panic_seq"), agent_v2.WithSequentialMaxRetries(1)).
		AddStep("only", &panicShell{name: "panic_shell"})
	sess := agent_v2.NewRunSession()
	_, err := seq.Run(context.Background(), sess, schema.UserMessage(retryQuestion))
	var pe *compose_v2.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("成员 panic 该以 PanicError 收尾，实际 %v", err)
	}
	if got := eventTexts(sess, "panic_shell", panicShellText); got != 1 {
		t.Errorf("panic 那次尝试写下的事件该照常留下，实际 %d 条", got)
	}
}

// 转交的候选 panic：同上（Handoff 那处在重试结束后结账，panic 时走不到那一行）。
//
// sanity-revert：同上。
func TestHandoffRetry_PanickedAttemptKeepsEvents(t *testing.T) {
	h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("panic_handoff"), agent_v2.WithHandoffInitial("cand"),
		agent_v2.WithHandoffHistoryMode(agent_v2.HandoffHistorySelfOnly), agent_v2.WithHandoffMaxRetries(1)).
		AddAgent("cand", &panicShell{name: "panic_cand"})
	sess := agent_v2.NewRunSession()
	_, err := h.Run(context.Background(), sess, schema.UserMessage(retryQuestion))
	var pe *compose_v2.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("候选 panic 该以 PanicError 收尾，实际 %v", err)
	}
	if got := eventTexts(sess, "panic_cand", panicShellText); got != 1 {
		t.Errorf("panic 那次尝试写下的事件该照常留下，实际 %d 条", got)
	}
}

// 并行分支里的成员 panic：同上——panic 越过分支往上走，分支写下的事件照常提交进编排会话。
//
// sanity-revert：分支 lane 只在分支正常返回时提交 ⟹ 会话里一条也没有，本用例红（审计实测）。
func TestParallelBranch_PanickedMemberKeepsEvents(t *testing.T) {
	p := agent_v2.NewParallelAgent(agent_v2.WithParallelName("panic_par")).AddBranch("p", &panicShell{name: "panic_branch"})
	sess := agent_v2.NewRunSession()
	_, err := p.Run(context.Background(), sess, schema.UserMessage(retryQuestion))
	var pe *compose_v2.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("分支成员 panic 该以 PanicError 收尾，实际 %v", err)
	}
	if got := eventTexts(sess, "panic_branch", panicShellText); got != 1 {
		t.Errorf("panic 那次尝试写下的事件该照常留下，实际 %d 条", got)
	}
}

// 转交候选第一次尝试失败之后，业务的重试判据 panic：panic 越过重试往上走、不会再有下一次尝试，失败那次的事件照常提交。
//
// sanity-revert：Handoff 那处在重试结束后顺序调用结账（不放进 defer）⟹ 失败那次写下的输入事件没了，本用例红。
func TestHandoffRetry_PredicatePanicKeepsFailedAttemptEvents(t *testing.T) {
	m := &scriptModel{steps: failOnce("ok")}
	cand := agent_v2.NewBasicAgent(agent_v2.WithName("pred_cand"), agent_v2.WithModel(m))
	h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("pred_handoff"), agent_v2.WithHandoffInitial("cand"),
		agent_v2.WithHandoffHistoryMode(agent_v2.HandoffHistorySelfOnly),
		agent_v2.WithHandoffRetryConfig(compose_v2.RetryConfig{
			MaxRetries:  1,
			ShouldRetry: func(error) bool { panic("predicate boom") },
		})).
		AddAgent("cand", cand)
	sess := agent_v2.NewRunSession()
	_, err := h.Run(context.Background(), sess, schema.UserMessage(retryQuestion))
	var pe *compose_v2.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("重试判据 panic 该以 PanicError 收尾，实际 %v", err)
	}
	if got := eventTexts(sess, "pred_cand", retryQuestion); got != 1 {
		t.Errorf("失败那次尝试写下的输入事件该照常留下，实际 %d 条", got)
	}
}
