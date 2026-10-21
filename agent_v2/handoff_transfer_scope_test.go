package agent_v2_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/skill"
	"crew-go/tool"
)

// ─────────────────────────────────────────────────────────────────────────────
// 转交信号的作用域：一次 sub-agent 调用，不多不少。
//
// 本文件的每条用例断言的都是**billing 的模型有没有被真调**，不是最终文本——
// 泄漏发生时最终文本「看着也对」（就是 billing 说的那句），只有调用次数会变。
// ─────────────────────────────────────────────────────────────────────────────

// scopeStub 是线程安全的按序 stub，额外记录被调次数（断言点）。
type scopeStub struct {
	mu    sync.Mutex
	msgs  []*schema.Message
	calls int
}

func newScopeStub(msgs ...*schema.Message) *scopeStub { return &scopeStub{msgs: msgs} }

func (s *scopeStub) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.calls
	s.calls++
	if i >= len(s.msgs) {
		i = len(s.msgs) - 1
	}
	m := *s.msgs[i]
	return &m, nil
}

func (s *scopeStub) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	m, err := s.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(m)
	schema.CloseSend(sr)
	return sr, nil
}

func (s *scopeStub) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return s, nil }

func (s *scopeStub) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

var _ model.ChatModel = (*scopeStub)(nil)

// scopeLeaf 造一个「被调用就留痕」的候选 agent。
func scopeLeaf(name, say string) (agent_v2.Agent, *scopeStub) {
	m := newScopeStub(schema.AssistantMessage(say))
	return agent_v2.NewBasicAgent(
		agent_v2.WithName(name),
		agent_v2.WithModel(m),
		agent_v2.WithMaxRounds(3),
	), m
}

// scopeGoexitOnce 第一次 Run 让内层跑完（transfer 因此写下目标），随后 runtime.Goexit
// ——模拟「业务塞进来的执行体在信号写完之后被掐断」。Goexit 会跑 defer，但**不会走任何
// `if err != nil` 分支**，所以任何写在错误路径上的手工清理都指望不上。
type scopeGoexitOnce struct {
	inner agent_v2.Agent
	mu    sync.Mutex
	n     int
}

func (g *scopeGoexitOnce) Run(ctx context.Context, s *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	g.mu.Lock()
	g.n++
	first := g.n == 1
	g.mu.Unlock()
	evs, err := g.inner.Run(ctx, s, in, agent_v2.RegrantHandoffTo(opts, g, g.inner)...)
	if first {
		runtime.Goexit()
	}
	return evs, err
}

func (g *scopeGoexitOnce) Resume(ctx context.Context, sid string, opts ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return g.inner.Resume(ctx, sid, opts...)
}
func (g *scopeGoexitOnce) Name() string        { return "front" }
func (g *scopeGoexitOnce) Description() string { return "前台" }

func scopeTransferCall(id, target string) *schema.Message {
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: id, Type: "function",
		Function: schema.FunctionCall{
			Name:      agent_v2.HandoffTransferToolName,
			Arguments: `{"agent_name":"` + target + `"}`,
		},
	}})
}

// scopeResidueFixture：front 第一次转交 billing 然后被掐断，之后只答普通文本。
func scopeResidueFixture() (*agent_v2.HandoffAgent, *scopeStub, *scopeStub) {
	frontModel := newScopeStub(
		scopeTransferCall("t1", "billing"),
		schema.AssistantMessage("FRONT 自己答了，没有转交"),
	)
	frontInner := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithModel(frontModel),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(3),
	)
	billing, billingModel := scopeLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", &scopeGoexitOnce{inner: frontInner}).AddAgent("billing", billing)
	return h, billingModel, frontModel
}

// assertResiduePremise 验**前提搭起来了**：front 恰好被调两次——Run 1 消耗转交那条、
// Run 2 消耗文本那条。少于 2 说明某个 Run 压根没走到模型；多于 2 说明转交那轮没被
// 接受（候选改名 / 工具没注册 ⟹ 软失败回灌让 LLM 重试），两种腐化下「billing 没被
// 调用」都会在「什么都没发生」的状态上恒真。
func assertResiduePremise(t *testing.T, frontModel *scopeStub) {
	t.Helper()
	if n := frontModel.count(); n != 2 {
		t.Fatalf("fixture 没搭对：front 模型被调 %d 次（期望 2——Run 1 转交 + Run 2 自答）,"+
			"下面那条「billing 没被转交」的断言在这种状态下恒真", n)
	}
}

// runDetached 在独立 goroutine 里跑——Goexit 掐掉的是那条 goroutine，测试线程要活着。
func runDetached(t *testing.T, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("被掐断的那次 Run 没能收尾（超时）")
	}
}

// TestHandoffTransfer_NoResidueAcrossRuns（缺陷①a）：同一个 sess 连跑两次，
// 第一次的转交被掐断在半路，第二次 front 明确不转交。
func TestHandoffTransfer_NoResidueAcrossRuns(t *testing.T) {
	h, billingModel, frontModel := scopeResidueFixture()
	sess := agent_v2.NewRunSession()
	ctx := context.Background()

	runDetached(t, func() { _, _ = h.Run(ctx, sess, schema.UserMessage("第一个问题")) })

	if _, err := h.Run(ctx, sess, schema.UserMessage("完全不相干的第二个问题")); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	assertResiduePremise(t, frontModel)
	if n := billingModel.count(); n != 0 {
		t.Errorf("跨 Run 幽灵转交：Run 2 里 front 没转交，billing 却被真调 %d 次（err 全程 nil）", n)
	}
}

// TestHandoffTransfer_NoResidueAcrossTasks（缺陷①b）：两次之间调 NextTask 换了
// RunID——位置维度里没有 RunID，所以换它救不了。
func TestHandoffTransfer_NoResidueAcrossTasks(t *testing.T) {
	h, billingModel, frontModel := scopeResidueFixture()
	sess := agent_v2.NewRunSession()
	ctx := context.Background()

	runDetached(t, func() { _, _ = h.Run(ctx, sess, schema.UserMessage("第一个问题")) })
	sess.NextTask()

	if _, err := h.Run(ctx, sess, schema.UserMessage("完全不相干的第二个问题")); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	assertResiduePremise(t, frontModel)
	if n := billingModel.count(); n != 0 {
		t.Errorf("跨任务幽灵转交：NextTask 换了 RunID 仍被上个任务的信号劫持（billing 被调 %d 次）", n)
	}
}

// askThenAnswerStub：第 1 轮调一个会中断的工具；之后直接作答**不转交**。
type askThenAnswerStub struct {
	mu sync.Mutex
	n  int
}

func (m *askThenAnswerStub) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.n++
	if m.n == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc_ask", Type: "function",
			Function: schema.FunctionCall{Name: "ask_confirm", Arguments: `{}`},
		}}), nil
	}
	return schema.AssistantMessage("FRONT 答完了，没有转交"), nil
}
func (m *askThenAnswerStub) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *askThenAnswerStub) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestHandoffTransfer_NoResidueAcrossResume（缺陷①c）：**同一个任务**里，Run 停在
// 中断（同一枚 RunID 的 Pending 存档），业务在两次调用之间用同一个 sess 跑了别的
// 东西——那个东西调了 transfer；续跑时 front 明确不转交。
//
// 为什么这一版不用 Goexit 造现场：Goexit **确实**被框架接住并转成业务错（成功即清那道
// 清理没跑，实测 `DeleteSubtree` 0 次），但那个错不是控制流信号 ⟹ 出口把存档盖成 **Failed 档**，
// 而 `ListPending` 只列 Pending ⟹ 拿不到「等着答的那份挂起档」，于是没有「同任务续跑」
// 这回事可测。（Failed 档本身仍 `HasResumableWork`、compose_v2 层续得上，只是走不到
// 这条用例要验的 agent_v2.Resume 路径。）
func TestHandoffTransfer_NoResidueAcrossResume(t *testing.T) {
	askTK := tool.NewToolkit()
	askTK.Register(tool.NewFuncTool("ask_confirm", "问一句", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认吗"})
			if err != nil {
				return nil, err
			}
			return tool.Success("confirmed:" + ans.Content), nil
		}))

	front := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithModel(&askThenAnswerStub{}),
		agent_v2.WithToolkit(askTK),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(5),
	)
	billing, billingModel := scopeLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)

	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	_, err := h.Run(ctx, sess, schema.UserMessage("第一个问题"))
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("前提不成立：Run 应停在中断，实际 err=%v", err)
	}

	// 两次调用之间，业务用同一个 sess 跑了个别的 agent（它也配了 transfer）。
	other := agent_v2.NewBasicAgent(
		agent_v2.WithName("other"),
		agent_v2.WithModel(newScopeStub(
			scopeTransferCall("o1", "billing"),
			schema.AssistantMessage("OTHER 说完了"),
		)),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(3),
	)
	if _, oerr := other.Run(context.Background(), sess, schema.UserMessage("顺手问一句")); oerr != nil {
		t.Fatalf("other run: %v", oerr)
	}

	qs := ie.AllQueries()
	if _, err := h.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{qs[0].InterruptID: schema.UserMessage("确认")}),
		agent_v2.WithResumeSession(sess),
	); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if n := billingModel.count(); n != 0 {
		t.Errorf("同任务续跑被残留信号劫持：front 答完就该收尾，billing 却被真调 %d 次", n)
	}
}

// TestHandoffTransfer_StrayCallHasNowhereToGo（缺陷②）：**头顶没有 HandoffAgent**
// 的 BasicAgent 装了 transfer 工具并调了一次，共用同一个 RunSession 的顶层
// HandoffAgent 不该把它当自己的信号。
//
// 这条同时钉住工具那侧的回灌语义：没人接转交时返 HandoffNoCoordinatorErrCode
// 软失败，LLM 改成自己作答，**不是**整个 Run 报错。
func TestHandoffTransfer_StrayCallHasNowhereToGo(t *testing.T) {
	strayModel := newScopeStub(
		scopeTransferCall("s1", "billing"),
		schema.AssistantMessage("STRAY 说完了"),
	)
	stray := agent_v2.NewBasicAgent(
		agent_v2.WithName("stray"),
		agent_v2.WithModel(strayModel),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(3),
	)
	sess := agent_v2.NewRunSession()
	ctx := context.Background()
	evs, err := stray.Run(ctx, sess, schema.UserMessage("跟 handoff 毫无关系的一句话"))
	if err != nil {
		t.Fatalf("stray run 不该整体失败（软失败回灌）：%v", err)
	}
	// 正向断言：软失败确实回灌给了 LLM，它接着答了第二轮。
	if strayModel.count() < 2 {
		t.Errorf("没人接转交时应软失败回灌让 LLM 续答，实际模型只被调 %d 次", strayModel.count())
	}
	if fm := agent_v2.FinalMessage(evs); fm == nil || fm.Content != "STRAY 说完了" {
		t.Errorf("stray 应自己作答收尾，final=%v", fm)
	}

	front, _ := scopeLeaf("front", "FRONT 自己答了，没有转交")
	billing, billingModel := scopeLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)

	if _, err := h.Run(ctx, sess, schema.UserMessage("请问营业时间")); err != nil {
		t.Fatalf("handoff run: %v", err)
	}
	if n := billingModel.count(); n != 0 {
		t.Errorf("一个跟 handoff 无关的 transfer 调用劫持了路由（billing 被真调 %d 次，err 全程 nil）", n)
	}
}

// TestHandoffTransfer_GrandchildCannotHijack（缺陷③）：候选 frontdesk 把另一个 agent
// 当**工具**用（AgentAsTool），那个孙辈也配了 transfer 并调了一次；frontdesk 自己
// 明确不转交。孙辈继承的是同一个 ctx（ns / hop / 候选集全一样），按位置算键那套
// 天然拦不住——摘收件人才拦得住（AgentAsTool 摘掉它，见 withoutHandoffDest）。
func TestHandoffTransfer_GrandchildCannotHijack(t *testing.T) {
	innerModel := newScopeStub(scopeTransferCall("g1", "billing"))
	inner := agent_v2.NewBasicAgent(
		agent_v2.WithName("inner"),
		agent_v2.WithDescription("内部资料查询"),
		agent_v2.WithModel(innerModel),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(3),
	)
	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(inner))

	frontdesk := agent_v2.NewBasicAgent(
		agent_v2.WithName("frontdesk"),
		agent_v2.WithModel(newScopeStub(
			schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID: "tc_consult", Type: "function",
				Function: schema.FunctionCall{Name: "inner", Arguments: `{"input":"查一下"}`},
			}}),
			schema.AssistantMessage("FRONTDESK 自己答了，不转交"),
		)),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(5),
	)
	billing, billingModel := scopeLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("frontdesk"),
	).AddAgent("frontdesk", frontdesk).AddAgent("billing", billing)

	sess := agent_v2.NewRunSession()
	if _, err := h.Run(context.Background(), sess, schema.UserMessage("请问营业时间")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if innerModel.count() == 0 {
		t.Fatal("前提不成立：孙辈根本没跑，这条用例测不到东西")
	}
	if n := billingModel.count(); n != 0 {
		t.Errorf("孙辈越权：frontdesk 明确没转交，被它当工具用的孙辈却把路由夺走了"+
			"（billing 被真调 %d 次，err 全程 nil）", n)
	}
	if st := h.State(sess); st.Hops != 1 {
		t.Errorf("孙辈越权：hops=%d 期望 1（frontdesk 一跳收尾）", st.Hops)
	}
}

// TestHandoffTransfer_NestedHandoffStillWorksThroughAgentTool 摘收件人的反向哨兵：
// `AgentAsTool(HandoffAgent)` 是支持模式（§12），内层 HandoffAgent 进去会 push
// 自己的收件人，**内层转交必须照常发生**。
//
// 没有这条，「摘收件人」可以退化成「把 transfer 一刀切禁掉」而全套仍绿。
func TestHandoffTransfer_NestedHandoffStillWorksThroughAgentTool(t *testing.T) {
	innerFront := agent_v2.NewBasicAgent(
		agent_v2.WithName("inner_front"),
		agent_v2.WithModel(newScopeStub(scopeTransferCall("i1", "inner_target"))),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(3),
	)
	innerTarget, innerTargetModel := scopeLeaf("inner_target", "INNER_TARGET 接手了")
	innerHandoff := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("inner_router"),
		agent_v2.WithHandoffDescription("内部转交小组"),
		agent_v2.WithHandoffInitial("inner_front"),
	).AddAgent("inner_front", innerFront).AddAgent("inner_target", innerTarget)

	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(innerHandoff))

	outerFront := agent_v2.NewBasicAgent(
		agent_v2.WithName("outer_front"),
		agent_v2.WithModel(newScopeStub(
			schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID: "tc_inner", Type: "function",
				Function: schema.FunctionCall{Name: "inner_router", Arguments: `{"input":"处理一下"}`},
			}}),
			schema.AssistantMessage("OUTER_FRONT 自己答了"),
		)),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(5),
	)
	outerOther, outerOtherModel := scopeLeaf("outer_other", "OUTER_OTHER 接手了")
	// 外层也注册一个同名候选——内层转交若泄漏出来，外层会把它当自己的目标
	outerLeak, outerLeakModel := scopeLeaf("inner_target", "OUTER 被内层信号劫持了")
	outer := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("outer_router"),
		agent_v2.WithHandoffInitial("outer_front"),
	).AddAgent("outer_front", outerFront).
		AddAgent("outer_other", outerOther).
		AddAgent("inner_target", outerLeak)

	if _, err := outer.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("run: %v", err)
	}
	// 正向：内层转交真发生了（不是被一刀切禁掉）
	if n := innerTargetModel.count(); n != 1 {
		t.Errorf("内层转交没发生：inner_target 被调 %d 次，期望 1（摘收件人不该把嵌套 handoff 一起废掉）", n)
	}
	// 反向：没泄漏到外层
	if n := outerLeakModel.count(); n != 0 {
		t.Errorf("内层转交泄漏到外层：外层的 inner_target 被调 %d 次", n)
	}
	if n := outerOtherModel.count(); n != 0 {
		t.Errorf("外层被莫名转交到 outer_other %d 次", n)
	}
}

// transferThenDirectStub：第 1 次吐 [transfer(billing), boom]（同轮 N=2 触发 fanout，
// transfer 写下目标、boom 报错让整次尝试失败）；第 2 次起直接作答**不转交**。
type transferThenDirectStub struct {
	mu sync.Mutex
	n  int
}

func (m *transferThenDirectStub) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.n++
	if m.n == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "tc0", Type: "function", Function: schema.FunctionCall{
				Name: agent_v2.HandoffTransferToolName, Arguments: `{"agent_name":"billing"}`}},
			{ID: "tc1", Type: "function", Function: schema.FunctionCall{
				Name: "boom", Arguments: `{}`}},
		}), nil
	}
	return schema.AssistantMessage("FRONT 第二次尝试直接答了，没有转交"), nil
}
func (m *transferThenDirectStub) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *transferThenDirectStub) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}
func (m *transferThenDirectStub) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.n
}

// TestHandoffTransfer_NotCarriedAcrossRetryAttempts（缺陷④）：配了重试策略，
// 尝试 1 转交 + 兄弟工具报错整体失败，尝试 2 直接作答不转交。
//
// 尝试 1 的转交只在那次尝试成功时才交出——失败那次做出的随它作废。⚠️ 这里有两层都在作废它：
// stepFn 每次尝试一次 CaptureSignals，RunWithRetry 自己也按尝试收；挪掉其中任一层这条都不红。
// 单独钉 RunWithRetry 那一层的是 TestSideSignal_RetryFailedAttemptDirectTransferDiscarded。
func TestHandoffTransfer_NotCarriedAcrossRetryAttempts(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("boom", "会炸的工具", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			return nil, fmt.Errorf("boom: 一次性故障")
		}))

	frontModel := &transferThenDirectStub{}
	front := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithModel(frontModel),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(5),
	)
	billing, billingModel := scopeLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
		agent_v2.WithHandoffRetryConfig(compose_v2.SimpleRetry(1)),
	).AddAgent("front", front).AddAgent("billing", billing)

	evs, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("run: %v（尝试 2 应该成功）", err)
	}
	// **承重断言先跑**：泄漏发生时 final 会变成 billing 说的那句，前提断言就会先炸、
	// 把真正的失败原因盖住（这条实测踩过）。
	if n := billingModel.count(); n != 0 {
		t.Errorf("失败的那次尝试写下的转交活到了下一次尝试（billing 被真调 %d 次，err 全程 nil）——"+
			"失败那次尝试做出的决定必须随它作废", n)
	}
	// 正向断言：重试确实发生且第二次自己作答了（否则这条用例在「什么都没发生」上恒真）。
	// 用**模型被调次数**判，不用 final 内容——后者在泄漏发生时会变。
	if n := frontModel.count(); n < 2 {
		t.Fatalf("前提不成立：front 的模型只被调 %d 次，第二次尝试没跑起来", n)
	}
	if fm := agent_v2.FinalMessage(evs); fm == nil || fm.Content != "FRONT 第二次尝试直接答了，没有转交" {
		t.Errorf("第二次尝试的答复没成为最终产出，final=%v", fm)
	}
}

// TestHandoffTransfer_SkillForkCannotHijack（缺陷③的第二条入口）：候选 frontdesk
// 激活一个 `context: fork` 的 skill，业务给这个 skill 配的 sub-agent 也带着 transfer
// 并调了一次；frontdesk 自己明确不转交。
//
// 「转交决定不许从被当工具用的那一层逃出去」这条规矩有**两个**入口——AgentAsTool 和
// skill fork。守卫只长在其中一个上等于没长（本仓栽过两次），故两条各配一条哨兵。
func TestHandoffTransfer_SkillForkCannotHijack(t *testing.T) {
	box := &forkBox{
		skills: []skill.SkillDescriptor{
			{Name: "research", Description: "调研", Context: "fork", Agent: "biz_research_agent"},
		},
		activations: map[string]string{"research": "skill body"},
	}
	forkModel := newScopeStub(scopeTransferCall("f1", "billing"))
	forkAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("biz_research_agent"),
		agent_v2.WithModel(forkModel),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(3),
	)
	resolver := func(name string) (agent_v2.Agent, error) {
		if name == "biz_research_agent" {
			return forkAgent, nil
		}
		return nil, fmt.Errorf("unknown agent: %s", name)
	}

	frontdesk := agent_v2.NewBasicAgent(
		agent_v2.WithName("frontdesk"),
		agent_v2.WithModel(newScopeStub(
			schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID: "tc_skill", Type: "function",
				Function: schema.FunctionCall{
					Name:      "activate_skill",
					Arguments: `{"skill":"research","query":"查一下"}`,
				},
			}}),
			schema.AssistantMessage("FRONTDESK 自己答了，不转交"),
		)),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSubAgentResolver(resolver),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(5),
	)
	billing, billingModel := scopeLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("frontdesk"),
	).AddAgent("frontdesk", frontdesk).AddAgent("billing", billing)

	sess := agent_v2.NewRunSession()
	if _, err := h.Run(context.Background(), sess, schema.UserMessage("请问营业时间")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if forkModel.count() == 0 {
		t.Fatal("前提不成立：fork 出来的 sub-agent 根本没跑，这条用例测不到东西")
	}
	if n := billingModel.count(); n != 0 {
		t.Errorf("skill fork 越权：frontdesk 明确没转交，被 fork 出来的 sub-agent 把路由夺走了"+
			"（billing 被真调 %d 次，err 全程 nil）", n)
	}
}
