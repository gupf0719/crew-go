package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// 这组哨兵钉的不变量：**一次工具调用的审批结论只形成一次**——启发式在这次调用的整个生命周期
// （首跑 + 中断续跑 + 失败续跑）里至多被调一次，续跑重放第一次的结论。
//
// 每条用例里的启发式都按调用次数给出不同的决定，模拟模型判断在拿不准区间里的翻转（按时间、
// 额度判断的规则隔一段时间续跑也会翻）。每条都断言启发式只被调一次——那是「重放了」的正向
// 证据；只断言「没出事」的话，判定碰巧没翻的实现也能过。

// seqHeuristic 让启发式对每个工具名分别按序返回 decisions[name]，用完后重复最后一个。
// fanout 下会被并发调用，所以计数按工具名分开、带锁。
type seqHeuristic struct {
	mu        sync.Mutex
	decisions map[string][]agent_v2.ApprovalDecision
	calls     map[string]int
}

func newSeqHeuristic(decisions map[string][]agent_v2.ApprovalDecision) *seqHeuristic {
	return &seqHeuristic{decisions: decisions, calls: map[string]int{}}
}

func (h *seqHeuristic) fn(_ context.Context, name string, _ map[string]any) agent_v2.ApprovalDecision {
	h.mu.Lock()
	defer h.mu.Unlock()
	seq := h.decisions[name]
	i := h.calls[name]
	h.calls[name]++
	if i >= len(seq) {
		i = len(seq) - 1
	}
	return seq[i]
}

func (h *seqHeuristic) count(name string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls[name]
}

// askingTool 执行时自己用 compose_v2.Ask 问一句，记下拿到的答复。
type askingTool struct {
	name     string
	question string
	runs     atomic.Int32
	mu       sync.Mutex
	answers  []string
}

func (t *askingTool) Name() string                { return t.name }
func (t *askingTool) Description() string         { return "asking tool" }
func (t *askingTool) InputSchema() map[string]any { return map[string]any{} }
func (t *askingTool) Call(ctx context.Context, _ map[string]any) (*tool.Result, error) {
	t.runs.Add(1)
	ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: t.question})
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.answers = append(t.answers, ans.Content)
	t.mu.Unlock()
	return tool.Success("done:" + ans.Content), nil
}

func (t *askingTool) gotAnswers() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.answers...)
}

func newHeuristicAgent(h *seqHeuristic, model *stubModel, tools ...tool.Tool) agent_v2.Agent {
	tk := agent_v2_newToolkit()
	for _, tl := range tools {
		tk.Register(tl)
	}
	return agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalHeuristic(h.fn),
		agent_v2.WithModel(model),
	)
}

func answer(id, content string) agent_v2.ResumeOption {
	return agent_v2.WithResumeUserInputs(map[string]*schema.Message{id: schema.UserMessage(content)})
}

// 首次判 Ask、用户答 deny；续跑时启发式若再判一次会给 Allow。用户的拒绝必须作数。
func TestApprovalHeuristic_DenyAnswerNotOverriddenOnResume(t *testing.T) {
	rt := &recordTool{name: "refund"}
	h := newSeqHeuristic(map[string][]agent_v2.ApprovalDecision{
		"refund": {agent_v2.ApprovalDecisionAsk, agent_v2.ApprovalDecisionAllow},
	})
	ag := newHeuristicAgent(h, newStubModel([]*schema.Message{oneToolCall("refund"), schema.AssistantMessage("final")}), rt)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.Query.Reason != "tool_approval" {
		t.Fatalf("首跑应为审批中断, got %v", err)
	}
	if _, err := ag.Resume(ctx, sess.RunID, answer(ie.Query.InterruptID, agent_v2.ApprovalOptionDeny)); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if got := rt.calls.Load(); got != 0 {
		t.Errorf("用户明确拒绝，工具却执行了 %d 次——续跑时重新调了启发式，新结论推翻了用户的答复", got)
	}
	if got := h.count("refund"); got != 1 {
		t.Errorf("启发式应只在首次形成结论时调用 1 次，实际 %d 次（续跑没有重放第一次的结论）", got)
	}
}

// 首次判 Ask、用户答 approve；续跑时启发式若再判一次会给 Deny。用户的批准必须作数。
func TestApprovalHeuristic_ApproveAnswerNotOverriddenOnResume(t *testing.T) {
	rt := &recordTool{name: "refund"}
	h := newSeqHeuristic(map[string][]agent_v2.ApprovalDecision{
		"refund": {agent_v2.ApprovalDecisionAsk, agent_v2.ApprovalDecisionDeny},
	})
	ag := newHeuristicAgent(h, newStubModel([]*schema.Message{oneToolCall("refund"), schema.AssistantMessage("final")}), rt)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.Query.Reason != "tool_approval" {
		t.Fatalf("首跑应为审批中断, got %v", err)
	}
	if _, err := ag.Resume(ctx, sess.RunID, answer(ie.Query.InterruptID, agent_v2.ApprovalOptionApprove)); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if got := rt.calls.Load(); got != 1 {
		t.Errorf("用户明确批准，工具应执行 1 次，实际 %d 次——续跑时重新调了启发式，新结论推翻了用户的答复", got)
	}
	if got := h.count("refund"); got != 1 {
		t.Errorf("启发式应只调用 1 次，实际 %d 次", got)
	}
}

// 首次判 Ask、用户只答了审批；续跑时启发式若再判一次会给 Allow——审批层这次不再占提问序号，
// 工具体自己的「确认金额?」就算出了审批问题的 ID、拿到 "approve" 当答案，用户根本没被问到。
func TestApprovalHeuristic_ApprovalAnswerNotHandedToToolQuestion(t *testing.T) {
	at := &askingTool{name: "refund", question: "确认金额?"}
	h := newSeqHeuristic(map[string][]agent_v2.ApprovalDecision{
		"refund": {agent_v2.ApprovalDecisionAsk, agent_v2.ApprovalDecisionAllow},
	})
	ag := newHeuristicAgent(h, newStubModel([]*schema.Message{oneToolCall("refund"), schema.AssistantMessage("final")}), at)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie1, ok := compose_v2.IsInterrupt(err)
	if !ok || ie1.Query.Reason != "tool_approval" {
		t.Fatalf("首跑应为审批中断, got %v", err)
	}
	_, err = ag.Resume(ctx, sess.RunID, answer(ie1.Query.InterruptID, agent_v2.ApprovalOptionApprove))
	ie2, ok := compose_v2.IsInterrupt(err)
	if !ok || ie2.Query.Question != "确认金额?" {
		t.Fatalf("批准之后工具自己的「确认金额?」应当被问到；实际 err=%v，工具拿到的答复=%q"+
			"（审批的答复被交给了工具的问题）", err, at.gotAnswers())
	}
	if ie2.Query.InterruptID == ie1.Query.InterruptID {
		t.Fatalf("工具的问题与审批问题算出了同一个 InterruptID %q", ie1.Query.InterruptID)
	}
	if _, err := ag.Resume(ctx, sess.RunID, answer(ie2.Query.InterruptID, "100")); err != nil {
		t.Fatalf("Resume #2: %v", err)
	}
	if got := at.gotAnswers(); len(got) != 1 || got[0] != "100" {
		t.Errorf("工具应拿到用户对「确认金额?」的答复 \"100\"，实际 %q", got)
	}
	if got := h.count("refund"); got != 1 {
		t.Errorf("首跑加两次续跑，启发式应只调用 1 次，实际 %d 次", got)
	}
}

// 首次判 Allow、工具自己问「确认金额?」、用户答 yes；续跑时启发式若再判一次会给 Ask——审批层这次
// 多占一个提问序号，用户的 "yes" 被当成了审批放行（那个审批问题用户从没见过），金额被重问。
func TestApprovalHeuristic_ToolAnswerNotTakenAsApproval(t *testing.T) {
	at := &askingTool{name: "refund", question: "确认金额?"}
	h := newSeqHeuristic(map[string][]agent_v2.ApprovalDecision{
		"refund": {agent_v2.ApprovalDecisionAllow, agent_v2.ApprovalDecisionAsk},
	})
	ag := newHeuristicAgent(h, newStubModel([]*schema.Message{oneToolCall("refund"), schema.AssistantMessage("final")}), at)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.Query.Question != "确认金额?" {
		t.Fatalf("首跑应为工具自己的提问, got %v", err)
	}
	_, err = ag.Resume(ctx, sess.RunID, answer(ie.Query.InterruptID, "yes"))
	if ie2, again := compose_v2.IsInterrupt(err); again {
		t.Fatalf("用户已答「确认金额?」，续跑不该再中断；实际又问了 %q（reason=%q）——用户的答复被审批层拿走了",
			ie2.Query.Question, ie2.Query.Reason)
	}
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if got := at.gotAnswers(); len(got) != 1 || got[0] != "yes" {
		t.Errorf("工具应拿到 \"yes\"，实际 %q", got)
	}
	if got := h.count("refund"); got != 1 {
		t.Errorf("启发式应只调用 1 次，实际 %d 次", got)
	}
}

// failOnceTool 第一次执行返回硬错误（整节点失败），之后成功。
type failOnceTool struct {
	name string
	runs atomic.Int32
}

func (t *failOnceTool) Name() string                { return t.name }
func (t *failOnceTool) Description() string         { return "fail once" }
func (t *failOnceTool) InputSchema() map[string]any { return map[string]any{} }
func (t *failOnceTool) Call(context.Context, map[string]any) (*tool.Result, error) {
	if t.runs.Add(1) == 1 {
		return nil, errors.New("transient backend error")
	}
	return tool.Success("ok"), nil
}

// 失败续跑也是同一次调用的生命周期：首跑放行后工具硬失败，续跑重试这次调用时沿用「放行」，
// 不因为启发式这回给了 Deny 就把已放行的调用拒掉。
func TestApprovalHeuristic_DecisionReplayedOnFailureResume(t *testing.T) {
	ft := &failOnceTool{name: "refund"}
	h := newSeqHeuristic(map[string][]agent_v2.ApprovalDecision{
		"refund": {agent_v2.ApprovalDecisionAllow, agent_v2.ApprovalDecisionDeny},
	})
	ag := newHeuristicAgent(h, newStubModel([]*schema.Message{oneToolCall("refund"), schema.AssistantMessage("final")}), ft)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("首跑工具硬失败，Run 应报错")
	}
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Fatalf("首跑应是失败不是中断: %v", err)
	}
	if _, err := ag.Resume(ctx, sess.RunID); err != nil {
		t.Fatalf("失败续跑: %v", err)
	}
	if got := ft.runs.Load(); got != 2 {
		t.Errorf("续跑应重试这次已放行的调用（共执行 2 次），实际 %d 次——续跑时重新判定把它拒了", got)
	}
	if got := h.count("refund"); got != 1 {
		t.Errorf("启发式应只调用 1 次，实际 %d 次", got)
	}
}

// 一批两个工具并发：各自的结论各自重放，互不串位。refund 首次判 Ask（用户续跑时 deny），
// notify 首次判 Allow、自己问一句（用户答 yes）；续跑时两者的启发式都会翻转。
func TestApprovalHeuristic_FanoutEachCallReplaysOwnDecision(t *testing.T) {
	rt := &recordTool{name: "refund"}
	at := &askingTool{name: "notify", question: "发给谁?"}
	h := newSeqHeuristic(map[string][]agent_v2.ApprovalDecision{
		"refund": {agent_v2.ApprovalDecisionAsk, agent_v2.ApprovalDecisionAllow},
		"notify": {agent_v2.ApprovalDecisionAllow, agent_v2.ApprovalDecisionAsk},
	})
	twoCalls := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tc1", Type: "function", Function: schema.FunctionCall{Name: "refund", Arguments: `{"x":1}`}},
		{ID: "tc2", Type: "function", Function: schema.FunctionCall{Name: "notify", Arguments: `{"x":2}`}},
	})
	ag := newHeuristicAgent(h, newStubModel([]*schema.Message{twoCalls, schema.AssistantMessage("final")}), rt, at)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应中断, got %v", err)
	}
	answers := map[string]*schema.Message{}
	for _, q := range ie.AllQueries() {
		switch {
		case q.Reason == "tool_approval" && q.ToolName == "refund":
			answers[q.InterruptID] = schema.UserMessage(agent_v2.ApprovalOptionDeny)
		case q.Question == "发给谁?":
			answers[q.InterruptID] = schema.UserMessage("yes")
		}
	}
	if len(answers) != 2 {
		t.Fatalf("首跑应同时挂起 refund 的审批与 notify 自己的提问，实际 %d 条: %+v", len(ie.AllQueries()), ie.AllQueries())
	}
	_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
	if ie2, again := compose_v2.IsInterrupt(err); again {
		var asked []string
		for _, q := range ie2.AllQueries() {
			asked = append(asked, q.Question)
		}
		t.Fatalf("两条都答了，续跑不该再中断；实际又问了 %q", asked)
	}
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if got := rt.calls.Load(); got != 0 {
		t.Errorf("refund 被用户拒绝，却执行了 %d 次", got)
	}
	if got := at.gotAnswers(); len(got) != 1 || got[0] != "yes" {
		t.Errorf("notify 应拿到 \"yes\"，实际 %q", got)
	}
	for _, name := range []string{"refund", "notify"} {
		if got := h.count(name); got != 1 {
			t.Errorf("%s 的启发式应只调用 1 次，实际 %d 次", name, got)
		}
	}
}

// amountHeuristic 按金额判：<=500 放行，否则拒绝。记下每次看到的金额。
type amountHeuristic struct {
	mu   sync.Mutex
	seen []float64
}

func (h *amountHeuristic) fn(_ context.Context, _ string, args map[string]any) agent_v2.ApprovalDecision {
	amt, _ := args["amount"].(float64) // 测试输入恒为数字，取不到即 0、走放行，下面的断言会暴露
	h.mu.Lock()
	h.seen = append(h.seen, amt)
	h.mu.Unlock()
	if amt <= 500 {
		return agent_v2.ApprovalDecisionAllow
	}
	return agent_v2.ApprovalDecisionDeny
}

// rewriteFromSecond 从第二次执行起把参数改写成 {"amount":1000}（模拟每次执行给出不同改写的钩子）。
type rewriteFromSecond struct {
	hook.NoOpHooks
	n atomic.Int32
}

func (h *rewriteFromSecond) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	if h.n.Add(1) > 1 {
		in.ArgumentsJSON = `{"amount":1000}`
	}
	return ctx, nil
}

// amountTool 记下执行时收到的金额；ask 为真时先问一句，failFirst 为真时第一次执行返回硬错误。
type amountTool struct {
	ask, failFirst bool
	runs           atomic.Int32
	mu             sync.Mutex
	executed       []float64
}

func (t *amountTool) Name() string                { return "refund" }
func (t *amountTool) Description() string         { return "refund" }
func (t *amountTool) InputSchema() map[string]any { return map[string]any{} }
func (t *amountTool) Call(ctx context.Context, args map[string]any) (*tool.Result, error) {
	if t.runs.Add(1) == 1 && t.failFirst {
		return nil, errors.New("transient backend error")
	}
	if t.ask {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认退款?"}); err != nil {
			return nil, err
		}
	}
	amt, _ := args["amount"].(float64) // 同上
	t.mu.Lock()
	t.executed = append(t.executed, amt)
	t.mu.Unlock()
	return tool.Success("refunded"), nil
}

func newAmountAgent(h *amountHeuristic, tl *amountTool, hooks hook.Hooks) agent_v2.Agent {
	tk := agent_v2_newToolkit()
	tk.Register(tl)
	refund100 := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tc1", Type: "function", Function: schema.FunctionCall{Name: "refund", Arguments: `{"amount":100}`}},
	})
	return agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalHeuristic(h.fn),
		agent_v2.WithHooks(hooks),
		agent_v2.WithModel(newStubModel([]*schema.Message{refund100, schema.AssistantMessage("final")})),
	)
}

// 审批生效时调用内容在第一次形成结论时固定：续跑时钩子把参数从 100 改写成 1000，执行的仍是启发式判过的 100，
// 启发式也不再被调。中断续跑这条路径。
func TestApprovalHeuristic_PinnedContentExecutedOnResume(t *testing.T) {
	h, tl := &amountHeuristic{}, &amountTool{ask: true}
	ag := newAmountAgent(h, tl, &rewriteFromSecond{})
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.Query.Question != "确认退款?" {
		t.Fatalf("首跑应放行并停在工具自己的提问, got %v", err)
	}
	if _, err := ag.Resume(ctx, sess.RunID, answer(ie.Query.InterruptID, "yes")); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	h.mu.Lock()
	seen := append([]float64(nil), h.seen...)
	h.mu.Unlock()
	if len(tl.executed) != 1 || tl.executed[0] != 100 {
		t.Errorf("执行的应是启发式判过的 100，实际 %v（启发式见过 %v）", tl.executed, seen)
	}
	if len(seen) != 1 || seen[0] != 100 {
		t.Errorf("启发式应只对 100 判过一次，实际 %v", seen)
	}
}

// 同上，失败续跑这条路径：没有人参与，执行的照样是判过的那一份。
func TestApprovalHeuristic_PinnedContentExecutedOnFailureResume(t *testing.T) {
	h, tl := &amountHeuristic{}, &amountTool{failFirst: true}
	ag := newAmountAgent(h, tl, &rewriteFromSecond{})
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	if _, err := ag.Run(ctx, sess, schema.UserMessage("hi")); err == nil {
		t.Fatalf("首跑工具硬失败，Run 应报错")
	}
	if _, err := ag.Resume(ctx, sess.RunID); err != nil {
		t.Fatalf("失败续跑: %v", err)
	}
	h.mu.Lock()
	seen := append([]float64(nil), h.seen...)
	h.mu.Unlock()
	if len(tl.executed) != 1 || tl.executed[0] != 100 || len(seen) != 1 {
		t.Errorf("执行的应是判过的 100、启发式只判一次：执行=%v 启发式见过=%v", tl.executed, seen)
	}
}

// nonceHooks 每次执行都给参数注入一个新的请求号（典型的「每次都变」的改写）。
type nonceHooks struct {
	hook.NoOpHooks
	n atomic.Int32
}

func (h *nonceHooks) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	in.ArgumentsJSON = fmt.Sprintf(`{"amount":100,"nonce":%d}`, h.n.Add(1))
	return ctx, nil
}

// 钩子每次注入新值、启发式首次判 Ask 续跑时会改口 Allow：用户答 deny 必须作数。
func TestApprovalHeuristic_NonceRewriteDoesNotOverrideDeny(t *testing.T) {
	rt := &recordTool{name: "refund"}
	h := newSeqHeuristic(map[string][]agent_v2.ApprovalDecision{
		"refund": {agent_v2.ApprovalDecisionAsk, agent_v2.ApprovalDecisionAllow},
	})
	tk := agent_v2_newToolkit()
	tk.Register(rt)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalHeuristic(h.fn), agent_v2.WithHooks(&nonceHooks{}),
		agent_v2.WithModel(newStubModel([]*schema.Message{oneToolCall("refund"), schema.AssistantMessage("final")})),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.Query.Reason != "tool_approval" {
		t.Fatalf("首跑应为审批中断, got %v", err)
	}
	if _, err := ag.Resume(ctx, sess.RunID, answer(ie.Query.InterruptID, agent_v2.ApprovalOptionDeny)); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if got := rt.calls.Load(); got != 0 {
		t.Errorf("用户明确拒绝，工具却执行了 %d 次——钩子注入了新值，续跑按新内容重判、推翻了用户的答复", got)
	}
	if got := h.count("refund"); got != 1 {
		t.Errorf("启发式应只调用 1 次，实际 %d 次", got)
	}
}

// 纯 ApprovalAsk、没有启发式：用户在审批问题里批准的是 100，续跑时钩子改写成 1000，执行的必须是 100。
func TestApproval_UserApprovesExactlyWhatIsExecuted(t *testing.T) {
	tl := &amountTool{}
	tk := agent_v2_newToolkit()
	tk.Register(tl)
	refund100 := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tc1", Type: "function", Function: schema.FunctionCall{Name: "refund", Arguments: `{"amount":100}`}},
	})
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), agent_v2.WithHooks(&rewriteFromSecond{}),
		agent_v2.WithModel(newStubModel([]*schema.Message{refund100, schema.AssistantMessage("final")})),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || !strings.Contains(ie.Query.Question, `"amount":100`) {
		t.Fatalf("审批问题里应是 100, got %v", err)
	}
	if _, err := ag.Resume(ctx, sess.RunID, answer(ie.Query.InterruptID, agent_v2.ApprovalOptionApprove)); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if len(tl.executed) != 1 || tl.executed[0] != 100 {
		t.Errorf("用户批准的是 100，执行的却是 %v", tl.executed)
	}
}

// renameFromSecond 从第二次执行起把工具改名成 wipe_all。
type renameFromSecond struct {
	hook.NoOpHooks
	n atomic.Int32
}

func (h *renameFromSecond) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	if h.n.Add(1) > 1 {
		in.Name = "wipe_all"
	}
	return ctx, nil
}

// 用户批准的是 refund，续跑时钩子把它改名成 wipe_all：执行的必须是 refund。
func TestApproval_RenameAfterApprovalExecutesApprovedTool(t *testing.T) {
	refund, wipe := &recordTool{name: "refund"}, &recordTool{name: "wipe_all"}
	tk := agent_v2_newToolkit()
	tk.Register(refund)
	tk.Register(wipe)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), agent_v2.WithHooks(&renameFromSecond{}),
		agent_v2.WithModel(newStubModel([]*schema.Message{oneToolCall("refund"), schema.AssistantMessage("final")})),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应审批中断, got %v", err)
	}
	if _, err := ag.Resume(ctx, sess.RunID, answer(ie.Query.InterruptID, agent_v2.ApprovalOptionApprove)); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if refund.calls.Load() != 1 || wipe.calls.Load() != 0 {
		t.Errorf("用户批准的是 refund：refund 应执行 1 次、wipe_all 0 次，实际 %d / %d", refund.calls.Load(), wipe.calls.Load())
	}
}

// constRewrite 每次都把金额改写成 200（确定性的改写）。
type constRewrite struct{ hook.NoOpHooks }

func (constRewrite) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	in.ArgumentsJSON = `{"amount":200}`
	return ctx, nil
}

// 确定性的改写不受固定内容影响：首次执行就按改写后的 200 判、批、执行。
func TestApproval_DeterministicRewriteStillApplies(t *testing.T) {
	h, tl := &amountHeuristic{}, &amountTool{ask: true}
	ag := newAmountAgent(h, tl, constRewrite{})
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应停在工具自己的提问, got %v", err)
	}
	if _, err := ag.Resume(ctx, sess.RunID, answer(ie.Query.InterruptID, "yes")); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if len(tl.executed) != 1 || tl.executed[0] != 200 || len(h.seen) != 1 || h.seen[0] != 200 {
		t.Errorf("确定性改写应照常生效：执行=%v 启发式见过=%v", tl.executed, h.seen)
	}
}

// 启发式里调了 compose_v2.Ask（它的签名交不出中断，这样用本身就问不到人，但框架不该因此串位）：
// 启发式要跑在自己的相位里，不占工具调用的提问序号——否则续跑时它不再被调，序号挪位，审批问题换 ID
// 重问、工具自己的问题拿到审批的答复。
func TestApprovalHeuristic_AskInsideHeuristicDoesNotShiftSlots(t *testing.T) {
	at := &askingTool{name: "refund", question: "金额?"}
	calls := 0
	heur := func(ctx context.Context, _ string, _ map[string]any) agent_v2.ApprovalDecision {
		calls++
		if ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "预授权?"}); err == nil && ans.Content == "yes" {
			return agent_v2.ApprovalDecisionAllow
		}
		return agent_v2.ApprovalDecisionAsk
	}
	tk := agent_v2_newToolkit()
	tk.Register(at)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalHeuristic(heur),
		agent_v2.WithModel(newStubModel([]*schema.Message{oneToolCall("refund"), schema.AssistantMessage("final")})),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie1, ok := compose_v2.IsInterrupt(err)
	if !ok || ie1.Query.Reason != "tool_approval" {
		t.Fatalf("首跑应为审批中断, got %v", err)
	}
	_, err = ag.Resume(ctx, sess.RunID, answer(ie1.Query.InterruptID, agent_v2.ApprovalOptionApprove))
	ie2, ok := compose_v2.IsInterrupt(err)
	if !ok || ie2.Query.Question != "金额?" {
		var reason string
		if ok {
			reason = ie2.Query.Reason
		}
		t.Fatalf("批准之后应轮到工具自己的「金额?」；实际 err=%v reason=%q 工具拿到=%q（审批问题换 ID 重问 / 工具拿到审批的答复）",
			err, reason, at.gotAnswers())
	}
	if _, err := ag.Resume(ctx, sess.RunID, answer(ie2.Query.InterruptID, "100元")); err != nil {
		t.Fatalf("Resume #2: %v", err)
	}
	if got := at.gotAnswers(); len(got) != 1 || got[0] != "100元" {
		t.Errorf("工具应拿到「金额?」的答复 \"100元\"，实际 %q", got)
	}
	if calls != 1 {
		t.Errorf("启发式应只调用 1 次，实际 %d 次", calls)
	}
}

// twoQuestionTool 连问两句才完成。
type twoQuestionTool struct{ name string }

func (t *twoQuestionTool) Name() string                { return t.name }
func (t *twoQuestionTool) Description() string         { return "two questions" }
func (t *twoQuestionTool) InputSchema() map[string]any { return map[string]any{} }
func (t *twoQuestionTool) Call(ctx context.Context, _ map[string]any) (*tool.Result, error) {
	for _, q := range []string{"第一问?", "第二问?"} {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: q}); err != nil {
			return nil, err
		}
	}
	return tool.Success("ok"), nil
}

// failNTool 前 failTimes 次执行返回硬错误，之后成功；okRuns 记成功执行（副作用）的次数。
type failNTool struct {
	name      string
	failTimes int32
	runs      atomic.Int32
	okRuns    atomic.Int32
}

func (t *failNTool) Name() string                { return t.name }
func (t *failNTool) Description() string         { return "fail n times" }
func (t *failNTool) InputSchema() map[string]any { return map[string]any{} }
func (t *failNTool) Call(context.Context, map[string]any) (*tool.Result, error) {
	if t.runs.Add(1) <= t.failTimes {
		return nil, errors.New("transient backend error")
	}
	t.okRuns.Add(1)
	return tool.Success("ok"), nil
}

func twoCalls(a, b string) *schema.Message {
	return schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tc1", Type: "function", Function: schema.FunctionCall{Name: a, Arguments: `{"x":1}`}},
		{ID: "tc2", Type: "function", Function: schema.FunctionCall{Name: b, Arguments: `{"x":2}`}},
	})
}

// answerAll 按问题文本作答（累积传入：每次 Resume 都带上全部答案）。
func answerAll(ie *compose_v2.InterruptError, by map[string]string, acc map[string]*schema.Message) map[string]*schema.Message {
	for _, q := range ie.AllQueries() {
		key := q.Question
		if q.Reason == "tool_approval" {
			key = "approval:" + q.ToolName
		}
		if a, ok := by[key]; ok {
			acc[q.InterruptID] = schema.UserMessage(a)
		}
	}
	return acc
}

// 批过且已成功的工具，续跑时钩子改了名，也不能再执行一遍（工具账的写入与读取要按同一份固定内容）。中断续跑。
func TestApproval_PinnedCompletedToolNotReplayedAfterRename(t *testing.T) {
	refund, wipe := &recordTool{name: "refund"}, &recordTool{name: "wipe_all"}
	tk := agent_v2_newToolkit()
	tk.Register(refund)
	tk.Register(wipe)
	tk.Register(&twoQuestionTool{name: "survey"})
	hooks := &renameRefundFromSecond{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithApprovalHeuristic(func(_ context.Context, name string, _ map[string]any) agent_v2.ApprovalDecision {
			if name == "survey" {
				return agent_v2.ApprovalDecisionAllow
			}
			return agent_v2.ApprovalDecisionDefault
		}),
		agent_v2.WithHooks(hooks),
		agent_v2.WithModel(newStubModel([]*schema.Message{twoCalls("refund", "survey"), schema.AssistantMessage("final")})),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	by := map[string]string{"approval:refund": agent_v2.ApprovalOptionApprove, "第一问?": "a1", "第二问?": "a2"}
	acc := map[string]*schema.Message{}
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	for i := 0; i < 4; i++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			break
		}
		_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answerAll(ie, by, acc)))
	}
	if err != nil {
		t.Fatalf("应跑完: %v", err)
	}
	if refund.calls.Load() != 1 || wipe.calls.Load() != 0 {
		t.Errorf("refund 批过且已成功，续跑时被改名也只应执行 1 次；实际 refund=%d wipe_all=%d", refund.calls.Load(), wipe.calls.Load())
	}
}

// renameRefundFromSecond 从第二次执行起把 refund 改名成 wipe_all（别的工具不动）。
type renameRefundFromSecond struct {
	hook.NoOpHooks
	n atomic.Int32
}

func (h *renameRefundFromSecond) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	if in.Name == "refund" && h.n.Add(1) > 1 {
		in.Name = "wipe_all"
	}
	return ctx, nil
}

// 同上，失败续跑：没有人参与，已成功的副作用也不能重放。
func TestApproval_PinnedCompletedToolNotReplayedAfterRenameOnFailureResume(t *testing.T) {
	refund := &failNTool{name: "refund", failTimes: 1}
	sibling := &failNTool{name: "sync", failTimes: 2}
	tk := agent_v2_newToolkit()
	tk.Register(refund)
	tk.Register(sibling)
	tk.Register(&recordTool{name: "wipe_all"})
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalHeuristic(func(context.Context, string, map[string]any) agent_v2.ApprovalDecision {
			return agent_v2.ApprovalDecisionAllow
		}),
		agent_v2.WithHooks(&renameRefundFromSecond{}),
		agent_v2.WithModel(newStubModel([]*schema.Message{twoCalls("refund", "sync"), schema.AssistantMessage("final")})),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	for i := 0; i < 3 && err != nil; i++ {
		_, err = ag.Resume(ctx, sess.RunID)
	}
	if err != nil {
		t.Fatalf("几次失败续跑后应跑完: %v", err)
	}
	if got := refund.okRuns.Load(); got != 1 {
		t.Errorf("refund 的副作用应只发生 1 次，实际 %d 次", got)
	}
}

// 配置在 Run 与 Resume 之间变化的几个方向：每个方向都用 A 配置跑到中断、B 配置续跑。工具在批准之后自己问一句
// 「金额?」。要守的是：审批问题与工具自己的问题各占各的序号，答复不串；已经形成的结论不因配置变化重判。
// approvalAgent 用给定的模型建 agent。A、B 两个配置要共用同一个模型实例：桩模型按调用次数出剧本，新建一个的话
// 续跑后的下一轮会从头再发一次 refund 调用，那个新调用问的「金额?」会冒充成被测的那一问（曾因此假绿）。
func approvalAgent(m *stubModel, tl tool.Tool, opts ...agent_v2.Option) agent_v2.Agent {
	tk := agent_v2_newToolkit()
	tk.Register(tl)
	base := []agent_v2.Option{agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6), agent_v2.WithModel(m)}
	return agent_v2.NewBasicAgent(append(base, opts...)...)
}

func refundOnce() *stubModel {
	return newStubModel([]*schema.Message{oneToolCall("refund"), schema.AssistantMessage("final")})
}

func allowAll(context.Context, string, map[string]any) agent_v2.ApprovalDecision {
	return agent_v2.ApprovalDecisionAllow
}

// 启发式判 Allow 之后去掉了启发式（换成纯 ApprovalAsk）：这次调用已经放行，用户对「金额?」的回答给工具。
func TestApproval_ConfigChange_HeuristicRemoved(t *testing.T) {
	at := &askingTool{name: "refund", question: "金额?"}
	m := refundOnce()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := approvalAgent(m, at, agent_v2.WithApprovalHeuristic(allowAll)).Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.Query.Question != "金额?" {
		t.Fatalf("首跑应放行并停在「金额?」, got %v", err)
	}
	_, err = approvalAgent(m, at, agent_v2.WithApprovalMode(agent_v2.ApprovalAsk)).
		Resume(ctx, sess.RunID, answer(ie.Query.InterruptID, "yes"))
	if err != nil {
		t.Fatalf("已放行的调用续跑时不该再拦，也不该把「金额?」的回答当成审批：%v（工具拿到 %q）", err, at.gotAnswers())
	}
	if got := at.gotAnswers(); len(got) != 1 || got[0] != "yes" {
		t.Errorf("工具应拿到 \"yes\"，实际 %q", got)
	}
}

// 纯 ApprovalAsk 问过审批之后加了判 Allow 的启发式：用户的批准由审批层用掉，「金额?」照常问。
func TestApproval_ConfigChange_HeuristicAdded(t *testing.T) {
	at := &askingTool{name: "refund", question: "金额?"}
	m := refundOnce()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := approvalAgent(m, at, agent_v2.WithApprovalMode(agent_v2.ApprovalAsk)).Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.Query.Reason != "tool_approval" {
		t.Fatalf("首跑应为审批中断, got %v", err)
	}
	_, err = approvalAgent(m, at, agent_v2.WithApprovalHeuristic(allowAll)).
		Resume(ctx, sess.RunID, answer(ie.Query.InterruptID, agent_v2.ApprovalOptionApprove))
	ie2, ok := compose_v2.IsInterrupt(err)
	if !ok || ie2.Query.Question != "金额?" || len(at.gotAnswers()) != 0 {
		t.Fatalf("批准之后应轮到「金额?」、工具还没拿到任何答复；实际 err=%v，工具拿到 %q（审批的答复被交给了工具的问题）", err, at.gotAnswers())
	}
}

// 审批从关闭到开启：「金额?」已经被问过、用户答了，续跑时新开启的审批另问一句，不拿「金额?」的回答当审批答复。
func TestApproval_ConfigChange_ApprovalTurnedOn(t *testing.T) {
	at := &askingTool{name: "refund", question: "金额?"}
	m := refundOnce()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := approvalAgent(m, at).Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.Query.Question != "金额?" {
		t.Fatalf("首跑应停在「金额?」, got %v", err)
	}
	b := approvalAgent(m, at, agent_v2.WithApprovalMode(agent_v2.ApprovalAsk))
	acc := map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("100元")}
	_, err = b.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(acc))
	ie2, ok := compose_v2.IsInterrupt(err)
	if !ok || ie2.Query.Reason != "tool_approval" {
		t.Fatalf("新开启的审批应另问一句；实际 err=%v（「100元」被当成了审批答复）", err)
	}
	acc[ie2.Query.InterruptID] = schema.UserMessage(agent_v2.ApprovalOptionApprove)
	if _, err := b.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(acc)); err != nil {
		t.Fatalf("Resume #2: %v", err)
	}
	if got := at.gotAnswers(); len(got) != 1 || got[0] != "100元" {
		t.Errorf("工具应拿到 \"100元\"，实际 %q", got)
	}
}

// 审批从开启到关闭：用户批准的答复不能交给工具自己的「金额?」。
func TestApproval_ConfigChange_ApprovalTurnedOff(t *testing.T) {
	at := &askingTool{name: "refund", question: "金额?"}
	m := refundOnce()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := approvalAgent(m, at, agent_v2.WithApprovalMode(agent_v2.ApprovalAsk)).Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.Query.Reason != "tool_approval" {
		t.Fatalf("首跑应为审批中断, got %v", err)
	}
	_, err = approvalAgent(m, at).Resume(ctx, sess.RunID, answer(ie.Query.InterruptID, agent_v2.ApprovalOptionApprove))
	ie2, ok := compose_v2.IsInterrupt(err)
	if !ok || ie2.Query.Question != "金额?" || len(at.gotAnswers()) != 0 {
		t.Fatalf("应轮到「金额?」、工具还没拿到任何答复；实际 err=%v，工具拿到 %q（审批的答复被交给了工具的问题）", err, at.gotAnswers())
	}
}

// 纯 ApprovalAsk 问过审批、用户答 deny 之后加了判 Allow 的启发式：这次调用的结论已经形成（Ask），用户的拒绝作数，
// 新加的启发式不重判它。只断言「答复不串」分不出这一点——不记结论、续跑时让新启发式判 Allow 的做法同样不串，只是
// 把用户的拒绝推翻了。
func TestApproval_ConfigChange_HeuristicAddedKeepsDeny(t *testing.T) {
	at := &askingTool{name: "refund", question: "金额?"}
	m := refundOnce()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := approvalAgent(m, at, agent_v2.WithApprovalMode(agent_v2.ApprovalAsk)).Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.Query.Reason != "tool_approval" {
		t.Fatalf("首跑应为审批中断, got %v", err)
	}
	_, err = approvalAgent(m, at, agent_v2.WithApprovalHeuristic(allowAll)).
		Resume(ctx, sess.RunID, answer(ie.Query.InterruptID, agent_v2.ApprovalOptionDeny))
	if err != nil {
		t.Fatalf("拒绝之后应跑完: %v", err)
	}
	if n := at.runs.Load(); n != 0 {
		t.Errorf("用户拒绝了，工具不该执行；实际执行 %d 次（新加的启发式推翻了拒绝）", n)
	}
}

// 审批从开启到关闭、用户答了 deny：这次调用的结论已经形成，拒绝照样作数；关掉审批只管还没形成结论的调用。
func TestApproval_ConfigChange_ApprovalTurnedOffKeepsDeny(t *testing.T) {
	at := &askingTool{name: "refund", question: "金额?"}
	m := refundOnce()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := approvalAgent(m, at, agent_v2.WithApprovalMode(agent_v2.ApprovalAsk)).Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.Query.Reason != "tool_approval" {
		t.Fatalf("首跑应为审批中断, got %v", err)
	}
	_, err = approvalAgent(m, at).Resume(ctx, sess.RunID, answer(ie.Query.InterruptID, agent_v2.ApprovalOptionDeny))
	if err != nil {
		t.Fatalf("拒绝之后应跑完: %v", err)
	}
	if n := at.runs.Load(); n != 0 {
		t.Errorf("用户拒绝了，工具不该执行；实际执行 %d 次（关掉审批推翻了已经形成的拒绝）", n)
	}
}

// renameAskUserToRefund 从第二次执行起把 ask_user 改名成 refund。
type renameAskUserToRefund struct {
	hook.NoOpHooks
	n atomic.Int32
}

func (h *renameAskUserToRefund) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	if in.Name == agent_v2.AskUserToolName && h.n.Add(1) > 1 {
		in.Name = "refund"
	}
	return ctx, nil
}

// 用户回答了 ask_user，续跑时钩子把它改名成要审批的业务工具：这次调用的内容在第一次执行时就定下了，改名不生效——
// 回答交给 ask_user、交回模型，refund 不执行，这个回答也不会被当成审批放行。
func TestApproval_AskUserAnswerNotTakenAsApprovalAfterRename(t *testing.T) {
	refund := &recordTool{name: "refund"}
	tk := agent_v2_newToolkit()
	tk.Register(refund)
	askUser := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tc1", Type: "function", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"继续吗?"}`}},
	})
	var seen []*schema.Message
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6), agent_v2.WithAskUser(),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), agent_v2.WithHooks(&renameAskUserToRefund{}),
		agent_v2.WithModel(newStubModelCapturing([]*schema.Message{askUser, schema.AssistantMessage("final")}, &seen)),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在 ask_user, got %v", err)
	}
	_, err = ag.Resume(ctx, sess.RunID, answer(ie.Query.InterruptID, "yes"))
	if refund.calls.Load() != 0 {
		t.Fatalf("refund 被执行了（续跑时的改名生效了，ask_user 的 \"yes\" 被当成了审批放行）")
	}
	if err != nil {
		t.Fatalf("改名不生效、回答交给 ask_user，应跑完：%v", err)
	}
	for _, m := range seen {
		if m.Role == schema.RoleTool && m.Content == "yes" {
			return
		}
	}
	t.Errorf("ask_user 应拿到回答 \"yes\" 并交回模型，模型没看到")
}

// 没有审批时同样只按第一次的内容执行：工具自己问「确认退款?」，用户答 yes 时看到的是退 100 的那次调用；续跑时钩子把
// 参数改成 1000，那个 yes 按位置交给了新内容的同一问。执行的必须仍是 100。
func TestToolCall_ContentFixedWithoutApproval(t *testing.T) {
	tl := &amountTool{ask: true}
	tk := agent_v2_newToolkit()
	tk.Register(tl)
	refund100 := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tc1", Type: "function", Function: schema.FunctionCall{Name: "refund", Arguments: `{"amount":100}`}},
	})
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithHooks(&rewriteFromSecond{}),
		agent_v2.WithModel(newStubModel([]*schema.Message{refund100, schema.AssistantMessage("final")})),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.Query.Question != "确认退款?" {
		t.Fatalf("首跑应停在「确认退款?」, got %v", err)
	}
	if _, err := ag.Resume(ctx, sess.RunID, answer(ie.Query.InterruptID, "yes")); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	tl.mu.Lock()
	defer tl.mu.Unlock()
	if len(tl.executed) != 1 || tl.executed[0] != 100 {
		t.Errorf("用户确认的是退 100，执行的应是 [100]，实际 %v", tl.executed)
	}
}

// confirmAmountTool 先问「确认退款 <amount>?」再执行，记下执行时的金额。
type confirmAmountTool struct {
	mu       sync.Mutex
	executed []float64
}

func (t *confirmAmountTool) Name() string                { return "refund" }
func (t *confirmAmountTool) Description() string         { return "refund" }
func (t *confirmAmountTool) InputSchema() map[string]any { return map[string]any{} }
func (t *confirmAmountTool) Call(ctx context.Context, args map[string]any) (*tool.Result, error) {
	amt, _ := args["amount"].(float64) // 测试里参数恒是数字
	if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: fmt.Sprintf("确认退款 %v?", amt)}); err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.executed = append(t.executed, amt)
	t.mu.Unlock()
	return tool.Success("refunded"), nil
}

// 启发式只做判断：它就地改了 args（把金额截到 500），这个改动不能进到工具里。改动只在首跑生效时，用户确认的是
// 「退 500」，续跑时结论重放、启发式不再被调，工具拿到原样的 1000，退了 1000（审计实测）。
func TestApprovalHeuristic_MutatingArgsDoesNotReachTool(t *testing.T) {
	tl := &confirmAmountTool{}
	clamp := func(_ context.Context, _ string, args map[string]any) agent_v2.ApprovalDecision {
		if a, _ := args["amount"].(float64); a > 500 {
			args["amount"] = 500.0
		}
		return agent_v2.ApprovalDecisionAllow
	}
	tk := agent_v2_newToolkit()
	tk.Register(tl)
	refund1000 := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tc1", Type: "function", Function: schema.FunctionCall{Name: "refund", Arguments: `{"amount":1000}`}},
	})
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalHeuristic(clamp),
		agent_v2.WithModel(newStubModel([]*schema.Message{refund1000, schema.AssistantMessage("final")})))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在退款确认, got %v", err)
	}
	if _, err := ag.Resume(ctx, sess.RunID, answer(ie.Query.InterruptID, "yes")); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	tl.mu.Lock()
	defer tl.mu.Unlock()
	if len(tl.executed) != 1 || fmt.Sprintf("确认退款 %v?", tl.executed[0]) != ie.Query.Question || tl.executed[0] != 1000 {
		t.Errorf("用户确认的是 %q，执行的金额是 %v——应是同一份，且启发式的改动不进工具（执行 1000）", ie.Query.Question, tl.executed)
	}
}

// approve_always 规则放行的调用同样只准入一次：第二次同参数的调用经规则放行、工具自己问了一句；续跑时没带原
// session（规则存在 session 上、跟着丢了），这次调用不该再要审批——再问一次、用户答 deny，工具就只执行了一半。
func TestApproval_RuleAdmittedCallNotReapprovedWithoutSession(t *testing.T) {
	at := &askingTool{name: "refund", question: "金额?"}
	m := newStubModel([]*schema.Message{oneToolCall("refund"), oneToolCall("refund"), schema.AssistantMessage("final")})
	ag := approvalAgent(m, at, agent_v2.WithApprovalMode(agent_v2.ApprovalAsk))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.Query.Reason != "tool_approval" {
		t.Fatalf("首跑应为审批中断, got %v", err)
	}
	acc := map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApproveAlways)}
	_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(acc))
	if ie, ok = compose_v2.IsInterrupt(err); !ok || ie.Query.Question != "金额?" {
		t.Fatalf("批准之后应轮到第一次调用的「金额?」, got %v", err)
	}
	acc[ie.Query.InterruptID] = schema.UserMessage("1")
	_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(acc))
	if ie, ok = compose_v2.IsInterrupt(err); !ok || ie.Query.Question != "金额?" {
		t.Fatalf("第二次同参数的调用应经规则放行、停在它自己的「金额?」, got %v", err)
	}
	acc[ie.Query.InterruptID] = schema.UserMessage("2")
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(acc)); err != nil {
		t.Fatalf("已经准入的调用续跑时不该再要审批（续跑没带原 session）：%v", err)
	}
	if got := at.gotAnswers(); len(got) != 2 || got[0] != "1" || got[1] != "2" {
		t.Errorf("两次调用都应拿到自己的答复，实际 %q", got)
	}
}

// 批准之后这次调用就准入了：工具已经开始执行、问了自己的问题，调用方续跑时对同一个审批 ID 改答 deny，不能把准入
// 推翻——推翻的话工具只执行了一半（拒绝记进工具账之后同样改不回来，两边要对称）。
func TestApproval_ApprovalNotRetractedAfterToolStarted(t *testing.T) {
	at := &askingTool{name: "refund", question: "金额?"}
	ag := approvalAgent(refundOnce(), at, agent_v2.WithApprovalMode(agent_v2.ApprovalAsk))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.Query.Reason != "tool_approval" {
		t.Fatalf("首跑应为审批中断, got %v", err)
	}
	approvalID := ie.Query.InterruptID
	_, err = ag.Resume(ctx, sess.RunID, answer(approvalID, agent_v2.ApprovalOptionApprove))
	if ie, ok = compose_v2.IsInterrupt(err); !ok || ie.Query.Question != "金额?" {
		t.Fatalf("批准之后应轮到「金额?」, got %v", err)
	}
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		approvalID:           schema.UserMessage(agent_v2.ApprovalOptionDeny),
		ie.Query.InterruptID: schema.UserMessage("100"),
	})); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if got := at.gotAnswers(); len(got) != 1 || got[0] != "100" {
		t.Errorf("已经准入、开始执行的调用应跑完，拿到 \"100\"；实际 %q（批准被改答的 deny 推翻了）", got)
	}
}

// 启发式 panic：不形成结论（失败续跑时再调一次），报出来的错要指明是审批启发式，不能算到工具头上（曾报成
// `tool "refund" panic recovered: heuristic boom`，排查时以为是工具体崩了）。
func TestApprovalHeuristic_PanicReportedAsHeuristic(t *testing.T) {
	rt := &recordTool{name: "refund"}
	var calls atomic.Int32
	h := func(context.Context, string, map[string]any) agent_v2.ApprovalDecision {
		if calls.Add(1) == 1 {
			panic("heuristic boom")
		}
		return agent_v2.ApprovalDecisionAllow
	}
	ag := approvalAgent(refundOnce(), rt, agent_v2.WithApprovalHeuristic(h))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	if err == nil || !strings.Contains(err.Error(), "approval heuristic") || strings.Contains(err.Error(), `tool "refund" panic recovered`) {
		t.Fatalf("错误应指明是审批启发式 panic，实际 %v", err)
	}
	if n := rt.calls.Load(); n != 0 {
		t.Fatalf("启发式 panic 时工具不该执行，实际执行 %d 次", n)
	}
	if _, err := ag.Resume(ctx, sess.RunID); err != nil {
		t.Fatalf("失败续跑时应再调启发式并放行: %v", err)
	}
	if rt.calls.Load() != 1 || calls.Load() != 2 {
		t.Errorf("panic 不形成结论：续跑时启发式再调一次（共 2 次）、工具执行 1 次；实际启发式 %d 次、工具 %d 次", calls.Load(), rt.calls.Load())
	}
}
