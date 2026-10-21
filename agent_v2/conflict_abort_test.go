package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
)

// 「对同一件事做了两个决定」分两种，处置不同：
//
//   - 同一批 tool_call（模型一条回复里同时发了两个 transfer）：顺序来自那条已提交的消息，第一个生效，
//     后面那个换成一条失败结果（TestHandoffBatch_FirstTransferWins）。曾经报冲突、整个 Run 失败，而
//     续跑时这批 tool_call 从工具账原样重放、必然再撞——任务永远过不去。
//   - 同一超步的并发成员（没有先后可言）：一个收件人只能有一个成员做决定，两个都做了就是图的设计错误
//     ——SignalConflictError（类型本身带着 ErrRunAborted），不重试、清档，值相同也一样（下面几条）。ParallelAgent
//     的分支拿不到决定权，并发的决定方只能是业务显式转授出来的（图里几个并发节点各自 RegrantFromNode）。

// TestHandoffBatch_FirstTransferWins：同一批里两个目标不同的 transfer，第一个（billing）生效，第二个
// 的 tool_result 换成「没有生效」的失败结果；Run 不失败、不重试，模型的副作用只有一次。
func TestHandoffBatch_FirstTransferWins(t *testing.T) {
	var charges atomic.Int32
	end := &endCapture{}
	front := agent_v2.NewBasicAgent(agent_v2.WithName("front"),
		agent_v2.WithModel(&twoTransferStub{onCall: func() { charges.Add(1) }}),
		agent_v2.WithHandoffTransfer(), agent_v2.WithHooks(end))
	billing, billingModel := scopeLeaf("billing", "BILLING 已处理")
	tech, techModel := scopeLeaf("tech", "TECH 已处理")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("desk"), agent_v2.WithHandoffInitial("front"),
		agent_v2.WithHandoffRetryConfig(compose_v2.RetryConfig{MaxRetries: 3, ShouldRetry: compose_v2.IsRetryable}),
	).AddAgent("front", front).AddAgent("billing", billing).AddAgent("tech", tech)

	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("同一批两个转交不该让 Run 失败：%v", err)
	}
	if b, tc := billingModel.count(), techModel.count(); b != 1 || tc != 0 {
		t.Errorf("排在前面的转交（billing）应生效：billing 被调 %d 次、tech %d 次", b, tc)
	}
	if n := charges.Load(); n != 1 {
		t.Errorf("前台模型（带副作用）应只调 1 次，实际 %d", n)
	}
	var second *schema.Message
	for _, m := range end.msgs {
		if m != nil && m.Role == schema.RoleTool && len(m.ContentBlocks) > 0 &&
			m.ContentBlocks[0].Toolresult != nil && m.ContentBlocks[0].Toolresult.ToolCallId == "t2" {
			second = m
		}
	}
	if second == nil || !strings.Contains(second.Content, agent_v2.SupersededDecisionErrCode) {
		t.Errorf("第二个 transfer 的结果应换成「没有生效」的失败结果，实际 %+v", second)
	}
}

// endCapture 记下 agent 结束时的完整消息流。
type endCapture struct {
	hook.NoOpHooks
	msgs []*schema.Message
}

func (h *endCapture) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, out *hook.AgentOutput) context.Context {
	if out != nil {
		h.msgs = out.Messages
	}
	return ctx
}

// countingTransfer 是程序化转交的成员：每次 Run 记一次（模拟副作用），经 HandoffTo 转给 target。
type countingTransfer struct {
	name, target string
	runs         *atomic.Int32
}

func (a *countingTransfer) Run(ctx context.Context, _ *agent_v2.RunSession, _ *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.runs.Add(1)
	if err := agent_v2.HandoffTo(ctx, opts, a, a.target); err != nil {
		return nil, err
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, schema.AssistantMessage(a.name+" 转给 "+a.target))}, nil
}
func (a *countingTransfer) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *countingTransfer) Name() string        { return a.name }
func (a *countingTransfer) Description() string { return "程序化转交" }

// concurrentDecidersGraph 是一个 GraphAgent：每个成员一个节点、同一超步并发，节点里经 RunSubAgent 调成员，
// 并用 RegrantFromNode 把这张图收到的决定权显式转授给它——业务亲手造出了几个并发的决定方。
func concurrentDecidersGraph(t *testing.T, name string, members ...agent_v2.Agent) *agent_v2.GraphAgent {
	t.Helper()
	b := compose_v2.NewBuilder().WithName(name)
	inCh := compose_v2.LastValue[*schema.Message]("in")
	compose_v2.WithChannel(b, inCh)
	for _, m := range members {
		m := m
		id := compose_v2.NodeID("n_" + m.Name())
		b.AddNode(id, func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			in, _ := compose_v2.Get(chs, inCh)
			_, err := agent_v2.RunSubAgent(ctx, m.Name(), m, in, agent_v2.RegrantFromNode(ctx, m)...)
			return nil, err
		})
		b.AddEdge(compose_v2.StartNode, id)
		b.AddEdge(id, compose_v2.EndNode)
	}
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName(name),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message {
			return schema.AssistantMessage("前台作答")
		}))
}

// conflictingDesk 是一个 HandoffAgent，前台候选是 concurrentDecidersGraph：两个并发节点各自转授给一个
// 程序化转交的成员（目标 ta / tb）——同一超步两个成员都对同一个收件人做了决定，在合并点按设计错误终局。
func conflictingDesk(t *testing.T, runs *atomic.Int32, retry *compose_v2.RetryConfig, ta, tb string) *agent_v2.HandoffAgent {
	front := concurrentDecidersGraph(t, "front",
		&countingTransfer{name: "a", target: ta, runs: runs},
		&countingTransfer{name: "b", target: tb, runs: runs})
	mk := func(n string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName(n),
			agent_v2.WithModel(newScopeStub(schema.AssistantMessage(n+" 接待"))))
	}
	if retry != nil {
		return agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("desk"), agent_v2.WithHandoffInitial("front"),
			agent_v2.WithHandoffRetryConfig(*retry)).
			AddAgent("front", front).AddAgent("billing", mk("billing")).AddAgent("tech", mk("tech"))
	}
	return agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("desk"), agent_v2.WithHandoffInitial("front")).
		AddAgent("front", front).AddAgent("billing", mk("billing")).AddAgent("tech", mk("tech"))
}

// TestHandoffConflict_DoesNotReplaySideEffects 钉住冲突不会被重试重放。
//
// 冲突在候选**内部**报出来（并发的两个节点在合并点），错从候选的 Run 返回，经过 HandoffAgent 包在候选
// 调用外面的重试——要是重试，两个成员连同它们的副作用重新跑一遍、每次都撞同一个冲突、不收敛。冲突带
// ErrRunAborted，重试层的框架守卫在业务谓词之前就把它排除了（谓词一律返回 true 也不重试），默认谓词
// IsRetryable 同样不重试它。
//
// ⚠️ **断言落在副作用跑了几次**，不落在「有没有报错」上：冲突类型自己不声明不可重试，不重试全靠带着
// ErrRunAborted——标记丢了照样报错（最后一次重试的错原样返回），但副作用已经跑了 N 遍。次数那条用
// Errorf 放在前面，不让后面那条 Fatalf 把它截掉。
func TestHandoffConflict_DoesNotReplaySideEffects(t *testing.T) {
	var runs atomic.Int32
	desk := conflictingDesk(t, &runs, &compose_v2.RetryConfig{MaxRetries: 3, ShouldRetry: compose_v2.IsRetryable},
		"billing", "tech")
	_, err := desk.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if n := runs.Load(); n != 2 {
		t.Errorf("两个成员各该跑 1 次（共 2 次），实际 %d——冲突被当成可重试的错重放了", n)
	}
	var conflict *compose_v2.SignalConflictError
	if !errors.As(err, &conflict) || !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("两个并发节点各自转交，应报冲突并带 ErrRunAborted：%v", err)
	}
}

// TestHandoffConflict_AgreeingDecidersStillTerminal：两个并发节点转给同一个人，照样是设计错误、谁也不
// 接手。一个收件人在同一超步里只能有一个决定方，跟值同不同无关——只在值不同时才报的话，两个并发的
// 决定方平时一致、偶尔分歧，设计错误要等到分歧那一次才暴露。
func TestHandoffConflict_AgreeingDecidersStillTerminal(t *testing.T) {
	var runs atomic.Int32
	desk := conflictingDesk(t, &runs, nil, "billing", "billing")
	_, err := desk.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	var conflict *compose_v2.SignalConflictError
	if !errors.As(err, &conflict) || !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("两个并发节点都做了决定（值相同），应报冲突并带 ErrRunAborted：%v", err)
	}
	if conflict.First.Value != "billing" || conflict.Second.Value != "billing" {
		t.Errorf("冲突应报出两个成员各自的决定，实际 %q 与 %q", conflict.First.Value, conflict.Second.Value)
	}
}

// TestHandoffConflict_IsTerminal_BeatsSiblingQuestion：Parallel 的一支是会冲突的转交台、另一支正问着人。
// 冲突是终局：整个 Run 报冲突（带 ErrRunAborted）而不是报中断，存档清掉——那个问题不会问出去。
//
// 终局失败压过中断：这个 Run 已经结束，问题答了也用不上。曾经冲突不清档，成员内部冒上来的冲突在扇出
// 仲裁里排在兄弟的中断之后：人答完续跑才撞上冲突，冲突的两方要是从记录里拿回的就原样再撞，那一问白问。
func TestHandoffConflict_IsTerminal_BeatsSiblingQuestion(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	var runs atomic.Int32
	asker := agent_v2.NewBasicAgent(agent_v2.WithName("asker"),
		agent_v2.WithModel(&askOnceStub{}), agent_v2.WithAskUser())
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("fan")).
		AddBranch("sibling", asker).AddBranch("conflict", conflictingDesk(t, &runs, nil, "billing", "tech"))

	sess := agent_v2.NewRunSession()
	_, err := par.Run(ctx, sess, schema.UserMessage("hi"))
	var conflict *compose_v2.SignalConflictError
	if !errors.As(err, &conflict) || !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("应报冲突并带 ErrRunAborted：%v", err)
	}
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Errorf("终局失败压过中断，不该报成中断：%v", err)
	}
	pend, perr := cp.ListPending(context.Background(), sess.RunID)
	if perr != nil {
		t.Fatalf("ListPending: %v", perr)
	}
	if len(pend) != 0 {
		t.Errorf("冲突是终局，存档应清掉，实际还剩 %d 份挂起档", len(pend))
	}
	if _, err := par.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess)); !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
		t.Errorf("终局之后续跑应返回 ErrNoPendingCheckpoint，实际 %v", err)
	}
}

// TestHandoffConflict_AfterAnsweredStep_IsTerminal：第 1 步问过人、人答了，第 2 步的候选冲突。冲突是终局：
// 存档清掉，再续跑返回 ErrNoPendingCheckpoint，已经答过的那一步随档消失，任务只能重开。这是「按设计
// 错误处理」要付的代价：留档的话，续跑把冲突那一步的成员连同副作用重做一遍，决定从记录里拿回时次次再撞。
func TestHandoffConflict_AfterAnsweredStep_IsTerminal(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)

	askerModel := newScopeStub(
		schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "q1", Type: "function",
			Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"确认转账？"}`}}}),
		schema.AssistantMessage("已确认"))
	asker := agent_v2.NewBasicAgent(agent_v2.WithName("asker"), agent_v2.WithModel(askerModel),
		agent_v2.WithAskUser(), agent_v2.WithMaxRounds(3))
	var runs atomic.Int32
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
		AddStep("confirm", asker).AddStep("desk", conflictingDesk(t, &runs, nil, "billing", "tech"))

	sess := agent_v2.NewRunSession()
	_, err := seq.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("第 1 步应先问人：%v", err)
	}
	answers := map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("是")}
	_, err = seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	var conflict *compose_v2.SignalConflictError
	if !errors.As(err, &conflict) || !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("第 2 步应报冲突并带 ErrRunAborted：%v", err)
	}
	if n := askerModel.count(); n != 2 {
		t.Fatalf("前置：第 1 步问一次、答完再作答一次，模型应被调 2 次，实际 %d", n)
	}
	if n := runs.Load(); n != 2 {
		t.Fatalf("前置：冲突的两个成员各跑 1 次，实际共 %d 次", n)
	}

	_, err = seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	if !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
		t.Fatalf("冲突是终局，存档应已清掉、再续跑返回 ErrNoPendingCheckpoint，实际 %v", err)
	}
	if n, m := askerModel.count(), runs.Load(); n != 2 || m != 2 {
		t.Errorf("终局之后什么都不该再跑：第 1 步模型被调 %d 次（应 2），冲突成员共跑 %d 次（应 2）", n, m)
	}
}

// askOnceStub：问一次 ask_user 就挂起。
type askOnceStub struct{}

func (m *askOnceStub) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "q0", Type: "function",
		Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"要继续吗"}`},
	}}), nil
}

func (m *askOnceStub) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(msg)
	schema.CloseSend(sr)
	return sr, nil
}

func (m *askOnceStub) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// twoTransferStub：每次调用吐两个**目标不同**的 transfer tool_call。
type twoTransferStub struct{ onCall func() }

func (m *twoTransferStub) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if m.onCall != nil {
		m.onCall() // 模拟业务副作用（扣款 / 发消息 / 写外部系统）
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "t1", Type: "function", Function: schema.FunctionCall{
			Name: agent_v2.HandoffTransferToolName, Arguments: `{"agent_name":"billing"}`}},
		{ID: "t2", Type: "function", Function: schema.FunctionCall{
			Name: agent_v2.HandoffTransferToolName, Arguments: `{"agent_name":"tech"}`}},
	}), nil
}

func (m *twoTransferStub) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(msg)
	schema.CloseSend(sr)
	return sr, nil
}

func (m *twoTransferStub) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }
