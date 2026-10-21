// nested_smoke 是嵌套 HITL 端到端 binary 级冒烟测试（v2 channel-based）。
//
// **目的**：验证 BasicAgent + Sequential + Loop + Reflect + Handoff + Parallel +
// Routing **7 编排**（9 编排中除 PlanAgent + GraphAgent 外，含 BasicAgent 顶层）
// 能在嵌入运行时正确 `Run → InterruptError → Resume` 闭环。CLAUDE.md 强制
// commit 前必跑之一（另一个是 plan_chat_web binary smoke）。
//
// **为什么不含 PlanAgent / GraphAgent**：PlanAgent 嵌套 HITL 走 plan_chat_web
// binary smoke + `agent_v2/plan_nested_resume_test.go`；GraphAgent 嵌套 Resume
// 走 `agent_v2/r30_graph_agent_hook_order_test.go` + `r31_graph_resume_test.go`
// 独立单测。本 smoke 聚焦 7 个"高频嵌入路径" + ISSUE-50 GraphID 防串台。
//
// **不依赖真 LLM**——内部 `askAnswerModel` 按"最近一条消息是否含 tool_result"
// 路由：
//   - 没有 tool_result（Run 第一次）→ 返 ask_user tool_call 触发 InterruptError
//   - 有 tool_result（Resume 后）→ 返最终文本
//
// 跑法：
//
//	go run ./example/orchestration/nested_smoke
//
// 全部场景跑通 + 退出码 0 表示冒烟通过。
//
// **覆盖矩阵**（12 场景，7 编排）：
//   - 5 基础：BasicAgent / Sequential / Loop / Reflect / Handoff 各跑一次 Run + Resume
//   - 4 高级：cleanup 不污染 / Resume Abort / loop 多轮 ns 重入 / reflect 跨轮
//   - 2 补全：Parallel / Routing 嵌入路径（ISSUE-71 接口合并后首次覆盖）
//   - 1 ISSUE-50：GraphID 防串台
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

func main() {
	scenarios := []struct {
		name string
		fn   func() error
	}{
		// 基础场景（5 编排各跑一次 Run + Resume Continue）
		{"BasicAgent 顶层 ask_user (chat_entry / plan_chat_web 核心路径)", scenarioBasicAgentTopLevel},
		{"sequential 内嵌 BasicAgent ask_user", scenarioSequential},
		{"loop body=BasicAgent ask_user", scenarioLoop},
		{"reflect actor=BasicAgent ask_user", scenarioReflect},
		{"handoff initial=BasicAgent ask_user", scenarioHandoff},
		// 高级场景（cleanup + Abort + 多次 Run 同 sid + 多轮 HITL）
		{"sequential 多次 Run 同 sessionID（验证 cleanup 不污染）", scenarioMultiRunSameSession},
		{"sequential Resume Abort（验证 cp.DeleteSubtree 清子树）", scenarioResumeAbort},
		{"loop 多轮 HITL（验证 ns round_<N> 隔离不冲突）", scenarioLoopMultiRoundHITL},
		{"reflect actor + reflector 跨轮 HITL", scenarioReflectCrossRound},
		// ISSUE-71 接口合并后 7 编排全覆盖：补 Parallel + Routing (Plan/Graph 走单测)
		{"parallel 内嵌 BasicAgent ask_user (一分支中断)", scenarioParallel},
		{"routing classifier→BasicAgent ask_user", scenarioRouting},
		// ISSUE-50 P0/P1 端到端冒烟
		{"GraphID mismatch fail-fast (ISSUE-50 P0 防串台)", scenarioGraphIDMismatch},
	}

	failed := 0
	for _, sc := range scenarios {
		fmt.Printf("=== 场景: %s ===\n", sc.name)
		if err := sc.fn(); err != nil {
			log.Printf("FAIL: %v\n", err)
			failed++
			continue
		}
		fmt.Println("PASS")
	}
	fmt.Printf("\n=== 总结：%d/%d 通过 ===\n", len(scenarios)-failed, len(scenarios))
	if failed > 0 {
		os.Exit(1)
	}
}

// ── 共享 stub model ───────────────────────────────────────────────

// askAnswerModel 按"最近一条消息是否含 tool_result"路由：
//   - 没有 tool_result（Run 第一次）→ 返 ask_user tool_call 触发 InterruptError
//   - 有 tool_result（Resume 后）→ 返最终文本
type askAnswerModel struct {
	question    string
	finalAnswer string
}

func newAskAnswerModel(question, finalAnswer string) *askAnswerModel {
	return &askAnswerModel{question: question, finalAnswer: finalAnswer}
}

func (m *askAnswerModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	hasToolResult := false
	for _, msg := range msgs {
		if msg.Role == schema.RoleTool {
			hasToolResult = true
			break
		}
	}
	if hasToolResult {
		return schema.AssistantMessage(m.finalAnswer), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc_smoke",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      agent_v2.AskUserToolName,
			Arguments: fmt.Sprintf(`{"question":"%s"}`, m.question),
		},
	}}), nil
}
func (m *askAnswerModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *askAnswerModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// recordingAskModel 记下每次被调用时**最后一条 user 消息**的内容，并在第 askAtCall 次
// 调用时才提问（其余轮直接答）。
//
// # 为什么 smoke 需要它
//
// 这个 binary 是 CLAUDE.md 指定的必跑闸门，可它原先对「续跑时把已被改写的输入回退成
// 原始种子」这一整类退化**完全不承重**——审计实测：把引擎改回「两条路径都 apply」的
// 旧语义，它照样 12/12 全过。原因是两个场景各自没搭对：反思场景的评估器首轮就判通过
// ⟹ 反馈节点根本不跑 ⟹ actor 输入从没被改写过，重复 apply 只是同值覆盖。
//
// 而这类 bug 的表现恰恰是「最终文本看着对、中间那轮白跑了」——断最终文本是断在
// **不承重的维度**上。所以这里改成记录 actor 每次收到什么，断言重跑那次**仍带着评审反馈**。
type recordingAskModel struct {
	question  string
	askAtCall int
	answers   []string
	mu        sync.Mutex
	seen      []string
	calls     int
}

func (m *recordingAskModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	hasToolResult := false
	lastUser := ""
	for _, msg := range msgs {
		if msg.Role == schema.RoleTool {
			hasToolResult = true
		}
		if msg.Role == schema.RoleUser {
			lastUser = msg.Content
		}
	}
	m.mu.Lock()
	m.calls++
	n := m.calls
	m.seen = append(m.seen, lastUser)
	m.mu.Unlock()

	if hasToolResult {
		return schema.AssistantMessage(m.answers[len(m.answers)-1]), nil
	}
	if n == m.askAtCall {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc_rec", Type: "function",
			Function: schema.FunctionCall{
				Name:      agent_v2.AskUserToolName,
				Arguments: fmt.Sprintf(`{"question":"%s"}`, m.question),
			},
		}}), nil
	}
	idx := n - 1
	if idx >= len(m.answers) {
		idx = len(m.answers) - 1
	}
	return schema.AssistantMessage(m.answers[idx]), nil
}

func (m *recordingAskModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *recordingAskModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *recordingAskModel) inputs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.seen...)
}

// boundaryRecorder 包在子 agent 外面，记下**上层编排每次传给它什么**。
//
// # 为什么必须断在这个边界上
//
// 「续跑时把已被改写的输入回退成原始种子」这个退化发生在**编排的 channel 上**。若断在
// 更下游（比如子 agent 内部模型看到什么），会被**子 agent 自己的存档**遮住：真 BasicAgent
// 续跑时从自己的 cp 恢复自己的消息历史，上游 channel 被改回原值它照样看得见反馈——
// 实测就是这么过的，变异注入后 actor 的模型仍收到评审反馈、smoke 照常 12/12。
//
// 这是本仓「双层防护遮蔽」陷阱的又一例：**下游有独立恢复能力时，上游的退化观测不到**。
// 真正的暴露面是**没有自己存档的子 agent**（远程 agent / 业务自研 agent / 被包成 tool 的），
// 那些没有第二层兜底，上游一回退它就真的重做一遍。
type boundaryRecorder struct {
	inner agent_v2.Agent
	mu    sync.Mutex
	seen  []string
}

func (r *boundaryRecorder) Name() string        { return r.inner.Name() }
func (r *boundaryRecorder) Description() string { return r.inner.Description() }

func (r *boundaryRecorder) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	r.mu.Lock()
	r.seen = append(r.seen, in.Content)
	r.mu.Unlock()
	return r.inner.Run(ctx, sess, in, opts...)
}

func (r *boundaryRecorder) Resume(ctx context.Context, sid string, opts ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return r.inner.Resume(ctx, sid, opts...)
}

func (r *boundaryRecorder) inputs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.seen...)
}

// transferModel 调 transfer_to_agent 工具自主转交——**生产路径**（模型 B）。
// 不用文本 fallback：那条只在内容恰好等于目标名时才命中，smoke 里靠它不稳。
type transferModel struct{ target string }

func (m *transferModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "tc_ho", Type: "function",
		Function: schema.FunctionCall{
			Name:      agent_v2.HandoffTransferToolName,
			Arguments: fmt.Sprintf(`{"agent_name":"%s"}`, m.target),
		},
	}}), nil
}
func (m *transferModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *transferModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// echoModel 直接返回固定文本，用于辅助节点（不触发中断）。
type echoModel struct{ text string }

func (m *echoModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(m.text), nil
}
func (m *echoModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *echoModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// multiRoundAskModel 每次都返 ask_user tool_call（无 done 状态），让 LoopAgent
// 每轮都触发中断。tool_result 出现后返简短 final。
type multiRoundAskModel struct{ question string }

func (m *multiRoundAskModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	hasToolResult := false
	for _, msg := range msgs {
		if msg.Role == schema.RoleTool {
			hasToolResult = true
			break
		}
	}
	if hasToolResult {
		return schema.AssistantMessage("body done"), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc_round",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      agent_v2.AskUserToolName,
			Arguments: fmt.Sprintf(`{"question":"%s"}`, m.question),
		},
	}}), nil
}
func (m *multiRoundAskModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *multiRoundAskModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

// ── 共享 helper ──────────────────────────────────────────────────

// runRoundtrip 跑 ag.Run → 收 InterruptError → ag.Resume → 验证 final。
//
// 对所有 v2 编排通用（同 Resume API 签名 `Resume(ctx, sid, opts...)`）。
func runRoundtrip(ag agent_v2.Agent, input, resumeAnswer string) (*schema.Message, error) {
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage(input))
	if err == nil {
		return nil, errors.New("expected InterruptError on first Run, got nil")
	}
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		return nil, fmt.Errorf("expected *InterruptError, got %T: %w", err, err)
	}
	// 单 query 场景直接读 ie.Query 可——本 helper 跑的 7 编排每次只有一个 ask_user
	// 挂起（单分支单 tool）。**多 query（fanout）场景必须改用 ie.AllQueries() 遍历**：
	// ParallelAgent 多 branch 并发 / ISSUE-81 toolsFn 多 tc 同时抛 Interrupt 时，本 ie
	// 只含单一 query，直接读 ie.Query 会漏处理兄弟分支的 query（对应 branch Resume 续
	// 不上）。完整多 branch 聚合视图见 example/orchestration/parallel_hitl_smoke + doc/v2/hitl-resume.md §2.4。
	if ie.Query == nil || ie.Query.InterruptID == "" {
		return nil, fmt.Errorf("InterruptError missing Query.InterruptID")
	}
	fmt.Printf("  Run 1: 触发中断 interrupt_id=%q question=%q\n", ie.Query.InterruptID, ie.Query.Question)

	events, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage(resumeAnswer)},
	))
	if err != nil {
		return nil, fmt.Errorf("resume: %w", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		return nil, errors.New("resume returned nil final message")
	}
	fmt.Printf("  Resume: final=%q\n", final.Content)
	return final, nil
}

// ── 基础场景（5 编排各跑一次 Run + Resume）────────────────────────

func scenarioBasicAgentTopLevel() error {
	a := agent_v2.NewBasicAgent(
		agent_v2.WithName("chat"),
		agent_v2.WithModel(newAskAnswerModel("color?", "got it: red")),
		agent_v2.WithAskUser(),
	)
	final, err := runRoundtrip(a, "what color?", "red")
	if err != nil {
		return err
	}
	if final.Content != "got it: red" {
		return fmt.Errorf("expected 'got it: red', got %q", final.Content)
	}
	return nil
}

func scenarioSequential() error {
	step1 := agent_v2.NewBasicAgent(
		agent_v2.WithName("ask_step"),
		agent_v2.WithModel(newAskAnswerModel("color?", "step1 done with red")),
		agent_v2.WithAskUser(),
	)
	step2 := agent_v2.NewBasicAgent(
		agent_v2.WithName("echo_step"),
		agent_v2.WithModel(&echoModel{text: "step2 echo"}),
	)
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
		AddStep("ask_step", step1).
		AddStep("echo_step", step2)
	final, err := runRoundtrip(seq, "start", "red")
	if err != nil {
		return err
	}
	if final.Content != "step2 echo" {
		return fmt.Errorf("expected final from echo_step, got %q", final.Content)
	}
	return nil
}

func scenarioLoop() error {
	body := agent_v2.NewBasicAgent(
		agent_v2.WithName("loop_body"),
		agent_v2.WithModel(newAskAnswerModel("ready?", "loop done")),
		agent_v2.WithAskUser(),
	)
	loop := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("loop"),
		agent_v2.WithLoopMaxIterations(3),
		agent_v2.WithLoopExitCondition(func(_ context.Context, last *schema.Message, _ int) (bool, error) {
			return last != nil && last.Content == "loop done", nil
		}),
	).WithBody(body)
	final, err := runRoundtrip(loop, "start", "ready")
	if err != nil {
		return err
	}
	if final.Content != "loop done" {
		return fmt.Errorf("expected loop done, got %q", final.Content)
	}
	return nil
}

func scenarioReflect() error {
	// ⚠️ **首轮必须判不通过**——原先用 echoModel{"yes"} 首轮就 GoalMatched=true，反馈节点
	// 根本不跑，actor 输入从没被改写过，于是这个场景对「续跑回退输入」完全不承重
	// （实测：退回旧语义它照样过）。现在首轮给评审意见、次轮才通过。
	const critique = "太浅了，展开讲"
	mdl := &recordingAskModel{
		question:  "draft?",
		askAtCall: 2, // 第 2 轮（= 拿到评审反馈那轮）才提问，中断落在反馈之后
		answers:   []string{"draft v1", "draft v2"},
	}
	actor := agent_v2.NewBasicAgent(
		agent_v2.WithName("actor"),
		agent_v2.WithModel(mdl),
		agent_v2.WithAskUser(),
	)
	rec := &boundaryRecorder{inner: actor}
	round := 0
	r := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("reflect"),
		agent_v2.WithReflectMaxRounds(3),
		agent_v2.WithReflector(func(_ context.Context, _, _ *schema.Message) (*agent_v2.ReflectOpinion, error) {
			round++
			if round == 1 {
				return &agent_v2.ReflectOpinion{GoalMatched: false, Critique: critique}, nil
			}
			return &agent_v2.ReflectOpinion{GoalMatched: true}, nil
		}),
	).WithActor(rec)

	// ⚠️ **必须嵌套**——顶层 Run 恒声明「这是新任务」跳过 auto-load，续跑那条路根本走不到，
	// 于是这个场景对「续跑回退输入」再怎么搭都测不到（实测：不包这层时，把引擎改回旧语义
	// 它照样过）。包一层 Sequential 让 reflect 处在 graphNS 非空的位置，外层 Resume 会
	// 重新调它的 Run、它再 auto-load 自己的存档——那才是这类退化真正发生的形态。
	outer := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("outer")).AddStep("r", r)
	final, err := runRoundtrip(outer, "topic", "draft me")
	if err != nil {
		return err
	}
	if final.Content == "" {
		return errors.New("expected non-empty final from reflect agent")
	}

	// **承重断言**：编排重跑那次传给 actor 的输入必须仍带着评审反馈。
	// 断的不是最终文本（种子回退时最终文本照样「看着对」，只是那一轮白跑了），
	// 也不是 actor 模型看到什么（那会被 actor 自己的存档遮住，见 boundaryRecorder）。
	seen := rec.inputs()
	fmt.Printf("  actor 历次收到：%q\n", seen)
	if len(seen) < 3 {
		return fmt.Errorf("actor 应至少被调 3 次（首轮 + 反馈轮 + Resume 重跑），实际 %d 次：%q",
			len(seen), seen)
	}
	if !strings.Contains(seen[len(seen)-1], critique) {
		return fmt.Errorf("Resume 重跑时 actor 的输入被回退成原始种子了——评审反馈没了。\n"+
			"实际收到：%q", seen[len(seen)-1])
	}
	return nil
}

func scenarioHandoff() error {
	// ⚠️ **必须真转交**——原先只注册了 agent_a、从不转交，于是 active channel 从没被节点
	// 改写过，这个场景对「转交后控制权被回退」完全不承重。现在 a 立刻转交给 b，b 提问。
	//
	// 注：转交后的 active **当前**由 store 兜着（转交当场就落盘，续跑时种子值已等于演化
	// 值），所以这一格并不能单独钉住播种语义——它钉的是「转交 + HITL + 续跑」这条链本身。
	// 哪天转交改成不当场落 store，播种那条依赖就会裸出来。
	a := agent_v2.NewBasicAgent(
		agent_v2.WithName("agent_a"),
		agent_v2.WithModel(&transferModel{target: "agent_b"}),
		agent_v2.WithHandoffTransfer(),
	)
	b := agent_v2.NewBasicAgent(
		agent_v2.WithName("agent_b"),
		agent_v2.WithModel(newAskAnswerModel("clarify?", "agent_b done")),
		agent_v2.WithAskUser(),
	)
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("h"),
		agent_v2.WithHandoffInitial("agent_a"),
		agent_v2.WithHandoffMaxHops(4),
	).AddAgent("agent_a", a).AddAgent("agent_b", b)
	final, err := runRoundtrip(h, "start", "ok clarify")
	if err != nil {
		return err
	}
	// **承重**：中断的是 b，Resume 后必须由 b 收尾——控制权弹回 a 的话最终文本是 a 的。
	if !strings.Contains(final.Content, "agent_b") {
		return fmt.Errorf("Resume 后控制权没留在转交目标手上，final=%q", final.Content)
	}
	return nil
}

// ── 高级场景 ──────────────────────────────────────────────────────

// scenarioMultiRunSameSession 验证 cleanupOnSuccess：第一次 Run 完成后 cp 应被
// 清；第二次同 sessionID Run 不应被旧 Pending cp 污染。
func scenarioMultiRunSameSession() error {
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	step1 := agent_v2.NewBasicAgent(
		agent_v2.WithName("ask_step"),
		agent_v2.WithModel(newAskAnswerModel("color?", "step1 done")),
		agent_v2.WithAskUser(),
	)
	step2 := agent_v2.NewBasicAgent(
		agent_v2.WithName("echo_step"),
		agent_v2.WithModel(&echoModel{text: "step2 echo"}),
	)
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
		AddStep("ask_step", step1).
		AddStep("echo_step", step2)

	// Run 1：触发中断 → Resume → 完成
	_, err := seq.Run(ctx, sess, schema.UserMessage("first"))
	if err == nil {
		return errors.New("expected ie on first Run")
	}
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		return fmt.Errorf("expected *InterruptError, got %T", err)
	}
	// 单 query 场景直接读 ie.Query 可（Sequential 内单 ask_user step）；多 query
	// （fanout）场景必须用 ie.AllQueries() 遍历——同 runRoundtrip 处说明。
	_, err = seq.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("red")},
	))
	if err != nil {
		return fmt.Errorf("first Resume: %w", err)
	}
	fmt.Printf("  Run 1: 完成\n")

	// 验证 cp 已清
	all, _ := cp.LoadAllInSession(ctx, sess.RunID)
	if len(all) > 0 {
		return fmt.Errorf("cleanupOnSuccess 失败：sessionID 子树应空，实有 %d cp", len(all))
	}
	fmt.Printf("  cp 已清（cleanupOnSuccess work）\n")

	// Run 2：同 sessionID 再跑（如果旧 cp 残留会污染 PendingExact）
	sess2 := &agent_v2.RunSession{RunID: sess.RunID} // 强制复用 sessionID
	_, err = seq.Run(ctx, sess2, schema.UserMessage("second"))
	if err == nil {
		return errors.New("expected ie on second Run too")
	}
	ie, isInt = compose_v2.IsInterrupt(err)
	if !isInt {
		return fmt.Errorf("second Run expected *InterruptError, got %T", err)
	}
	// 同上：单 query 直接读 ie.Query；多 query（fanout）须用 ie.AllQueries() 遍历。
	_, err = seq.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("blue")},
	))
	if err != nil {
		return fmt.Errorf("second Resume: %w", err)
	}
	fmt.Printf("  Run 2: 完成（同 sessionID 不被旧 cp 污染）\n")
	return nil
}

// scenarioResumeAbort 验证 ResumeActionAbort 调 cp.DeleteSubtree 清整 sessionID 子树。
func scenarioResumeAbort() error {
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	step1 := agent_v2.NewBasicAgent(
		agent_v2.WithName("ask_step"),
		agent_v2.WithModel(newAskAnswerModel("ok?", "step1 done")),
		agent_v2.WithAskUser(),
	)
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
		AddStep("ask_step", step1)

	_, err := seq.Run(ctx, sess, schema.UserMessage("start"))
	if err == nil {
		return errors.New("expected ie")
	}
	fmt.Printf("  Run 1: 触发中断\n")

	_, err = seq.Resume(ctx, sess.RunID, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort))
	if err != nil {
		return fmt.Errorf("abort resume: %w", err)
	}
	all, _ := cp.LoadAllInSession(ctx, sess.RunID)
	if len(all) > 0 {
		return fmt.Errorf("Abort 应清 cp，实有 %d cp", len(all))
	}
	fmt.Printf("  Resume Abort: cp 已删\n")
	return nil
}

// scenarioLoopMultiRoundHITL 验证 LoopAgent body 触发 ask_user → Resume 后
// loop 正常推进（验证 LoopAgent 用 `round_<N>` ns 隔离，多轮 cp 不互覆）。
func scenarioLoopMultiRoundHITL() error {
	body := agent_v2.NewBasicAgent(
		agent_v2.WithName("body"),
		agent_v2.WithModel(&multiRoundAskModel{question: "round?"}),
		agent_v2.WithAskUser(),
	)
	loop := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("loop"),
		agent_v2.WithLoopMaxIterations(3),
		agent_v2.WithLoopExitCondition(func(_ context.Context, last *schema.Message, _ int) (bool, error) {
			return last != nil && last.Content == "body done", nil
		}),
	).WithBody(body)

	final, err := runRoundtrip(loop, "start", "ack")
	if err != nil {
		return err
	}
	if final.Content != "body done" {
		return fmt.Errorf("expected body done, got %q", final.Content)
	}
	return nil
}

// scenarioReflectCrossRound 验证 ReflectAgent actor 触发 ie，Resume 后 actor
// 完成 → reflector 评估 → 决策继续/接受。
func scenarioReflectCrossRound() error {
	actor := agent_v2.NewBasicAgent(
		agent_v2.WithName("actor"),
		agent_v2.WithModel(newAskAnswerModel("draft?", "actor done v1")),
		agent_v2.WithAskUser(),
	)
	// audit⁴ LOW fix：原 evaluator 恒 "yes" 单轮即过、"跨轮"名不副实——改第 1 轮
	// "不行"（真进轮 1）第 2 轮 "yes"，覆盖 Resume 矩阵 (a) 轮 1+ 的 binary 冒烟。
	evaluator := agent_v2.NewBasicAgent(
		agent_v2.WithName("evaluator"),
		agent_v2.WithModel(&seqEchoModel{texts: []string{"不行，缺细节", "yes"}}),
	)
	r := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("reflect"),
		agent_v2.WithEvaluator(evaluator),
		agent_v2.WithReflectMaxRounds(3),
	).WithActor(actor)

	// 真跨轮 = 跨轮二次中断（Resume 矩阵 (d)）：轮 0 actor ask 中断 → 答 →
	// evaluator 不过 → 轮 1 actor 又 ask 中断（新 InterruptID，round_N ns 隔离）→
	// 再答 → evaluator 过。循环 Resume 直到完成。
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("topic"))
	answers := map[string]*schema.Message{}
	seenIDs := map[string]bool{}
	hops := 0
	var final *schema.Message
	for {
		ie, isInt := compose_v2.IsInterrupt(err)
		if !isInt {
			break
		}
		hops++
		if hops > 4 {
			return fmt.Errorf("跨轮中断次数异常（>4），最后 err=%v", err)
		}
		if seenIDs[ie.Query.InterruptID] {
			return fmt.Errorf("跨轮 InterruptID 重复 %q（round_N ns 隔离失效）", ie.Query.InterruptID)
		}
		seenIDs[ie.Query.InterruptID] = true
		fmt.Printf("  中断 #%d id=%q question=%q\n", hops, ie.Query.InterruptID, ie.Query.Question)
		answers[ie.Query.InterruptID] = schema.UserMessage(fmt.Sprintf("draft me v%d", hops))
		var events []*agent_v2.AgentEvent
		events, err = r.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
		if err == nil {
			final = agent_v2.FinalMessage(events)
		}
	}
	if err != nil {
		return fmt.Errorf("resume: %w", err)
	}
	if hops != 2 {
		return fmt.Errorf("应恰 2 次中断（轮 0 + 轮 1 各一次 ask），got %d", hops)
	}
	if final == nil || final.Content == "" {
		return errors.New("reflect cross-round 应有非空 final")
	}
	if matched, _ := final.Annotations["reflect_goal_matched"].(bool); !matched {
		return errors.New("跨轮后应达标退出（reflect_goal_matched=true）")
	}
	if rounds, _ := final.Annotations["reflect_rounds"].(int); rounds != 2 {
		return fmt.Errorf("应真跨 2 轮（轮 0 不过 + 轮 1 过），got %v", final.Annotations["reflect_rounds"])
	}
	fmt.Printf("  跨轮 final=%q rounds=%v 中断=%d\n", final.Content, final.Annotations["reflect_rounds"], hops)
	return nil
}

// seqEchoModel 按调用次序返回固定文本序列（跨轮场景 evaluator 用）。
type seqEchoModel struct {
	texts []string
	idx   int
}

func (m *seqEchoModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	t := m.texts[len(m.texts)-1]
	if m.idx < len(m.texts) {
		t = m.texts[m.idx]
	}
	m.idx++
	return schema.AssistantMessage(t), nil
}

func (m *seqEchoModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *seqEchoModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// ── ISSUE-71 接口合并补全：Parallel + Routing 嵌入 ─────────────────

// scenarioParallel 验证 ParallelAgent 嵌入 BasicAgent ask_user：一分支抛
// InterruptError，框架按 superstep_snapshot 保留成功兄弟 + Resume 只重跑失败分支。
func scenarioParallel() error {
	asker := agent_v2.NewBasicAgent(
		agent_v2.WithName("asker"),
		agent_v2.WithModel(newAskAnswerModel("clarify?", "asker done")),
		agent_v2.WithAskUser(),
	)
	echoer := agent_v2.NewBasicAgent(
		agent_v2.WithName("echoer"),
		agent_v2.WithModel(&echoModel{text: "echoer done"}),
	)
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("ask_branch", asker).
		AddBranch("echo_branch", echoer)
	final, err := runRoundtrip(par, "start parallel", "yes")
	if err != nil {
		return err
	}
	if final.Content == "" {
		return errors.New("parallel final 应非空（merger 拼两分支结果）")
	}
	return nil
}

// scenarioRouting 验证 RoutingAgent WithRouter（显式 Router 函数）路径 + 选中 worker 嵌入 ask_user。
func scenarioRouting() error {
	asker := agent_v2.NewBasicAgent(
		agent_v2.WithName("ask_branch"),
		agent_v2.WithModel(newAskAnswerModel("which?", "branch done")),
		agent_v2.WithAskUser(),
	)
	router := func(_ context.Context, _ *schema.Message) (compose_v2.NodeID, error) {
		return "ask_branch", nil
	}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("router"),
		agent_v2.WithRouter(router),
	).AddBranch("ask_branch", asker)
	final, err := runRoundtrip(r, "route me", "answer")
	if err != nil {
		return err
	}
	if final.Content != "branch done" {
		return fmt.Errorf("expected 'branch done', got %q", final.Content)
	}
	return nil
}

// ── ISSUE-50 P0/P1 端到端冒烟 ────────────────────────────────────

// scenarioGraphIDMismatch 验证 ISSUE-50 P0：两个不同 Graph 共享同 sessionID 时，
// SaveNS / Run 应 fail-fast `ErrGraphIDConflict`，不静默串台。
func scenarioGraphIDMismatch() error {
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// 直接用底层 InMemoryCheckpointer.SaveNS 验证 GraphID 冲突守卫
	c1 := &compose_v2.Checkpoint{
		ID:      "shared_sid",
		NS:      "",
		Status:  compose_v2.CPStatusPending,
		GraphID: "graph-A",
	}
	if err := cp.SaveNS(ctx, "shared_sid", "", c1); err != nil {
		return fmt.Errorf("first save: %w", err)
	}
	fmt.Printf("  graph-A 写入 cp.GraphID=%q\n", c1.GraphID)

	// 第二个不同 GraphID 同 (sid, ns) → fail-fast
	c2 := &compose_v2.Checkpoint{
		ID:      "shared_sid",
		NS:      "",
		Status:  compose_v2.CPStatusPending,
		GraphID: "graph-B",
	}
	err := cp.SaveNS(ctx, "shared_sid", "", c2)
	if err == nil {
		return errors.New("graph-B 写同 (sid, ns) 应 fail-fast 但成功了（P0 防御失效）")
	}
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) {
		return fmt.Errorf("err 应满足 errors.Is(err, ErrGraphIDConflict), got: %v", err)
	}
	fmt.Printf("  graph-B 写同 sid 正确 fail-fast: %v\n", err)
	return nil
}
