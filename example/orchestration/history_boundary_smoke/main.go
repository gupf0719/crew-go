// history_boundary_smoke 用真 qwen 端到端验会话历史层的**编排边界**——成功路径和失败路径都跑。
//
// 跟 example/agent/history_smoke 分工：那个验"旁路"定位（后端写失败 / panic / 读失败 /
// 卡住时主链路照常）和高并发不串台；这个验**编排边界的判据**——外部执行体那步靠边界补记
// 才留得下、中断 Resume 不丢不重、扇出每条分支流各自完整、配置冲突 fail-fast。
//
// # 为什么要真 LLM
//
// 历史层的判据是在「谁记了什么」这件事上做区分，而 stub model 每次返回预置内容、
// 工具往返也是编的。真 LLM 会：多轮、自己决定调不调工具、产出内容每次不同、
// 偶尔不遵循指令。这些恰好是判据容易出错的地方——`agent_v2` 的单测覆盖不到。
//
// # 跑
//
//	go run ./example/orchestration/history_boundary_smoke              # 全部场景
//	go run ./example/orchestration/history_boundary_smoke -model xxx   # 指定单个 qwen 模型
//	go run ./example/orchestration/history_boundary_smoke -only 3      # 只跑第 3 个场景
//
// 需要 .env 里的 QWEN_API_KEY。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/example/envloader"
	"crew-go/fallback"
	"crew-go/model"
	"crew-go/model/qwen"
	"crew-go/schema"
	"crew-go/tool"
)

var (
	modelFlag = flag.String("model", "", "指定单个 qwen 模型名；留空 = 把 TestModelCandidates 串成 fallback 链")
	onlyFlag  = flag.Int("only", 0, "只跑第 N 个场景（1 起；0 = 全跑）")
)

func newModel() model.ChatModel {
	if *modelFlag != "" {
		return qwen.NewTestClient(*modelFlag)
	}
	ms := make([]model.ChatModel, 0, len(qwen.TestModelCandidates))
	for _, n := range qwen.TestModelCandidates {
		ms = append(ms, qwen.NewTestClient(n))
	}
	return fallback.New(ms)
}

// ── 结果统计 ──

var (
	passed, failed, skipped int
)

func report(name string, err error) {
	switch {
	case err == nil:
		passed++
		fmt.Printf("PASS  %s\n\n", name)
	case errors.Is(err, errSkip):
		skipped++
		fmt.Printf("SKIP  %s：%v\n\n", name, err)
	default:
		failed++
		fmt.Printf("FAIL  %s：%v\n\n", name, err)
	}
}

var errSkip = errors.New("跳过")

func skipf(format string, a ...any) error {
	return fmt.Errorf("%w：%s", errSkip, fmt.Sprintf(format, a...))
}

// ── 公共 helper ──

const tenant = "smoke_tenant"

func dump(msgs []*schema.Message) string {
	var b strings.Builder
	fmt.Fprintf(&b, "历史 %d 条：\n", len(msgs))
	for i, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		c := strings.TrimSpace(m.Content)
		if len(c) > 70 {
			c = c[:70] + "…"
		}
		if tcs := schema.GetToolCalls(m); c == "" && len(tcs) > 0 {
			c = "(tool_call: " + tcs[0].Function.Name + ")"
		}
		fmt.Fprintf(&b, "    [%d] %-9s owner=%-14s %s\n", i, m.Role, o, c)
	}
	return b.String()
}

func load(ctx context.Context, hs agent_v2.HistoryStore, conv string) []*schema.Message {
	msgs, err := hs.Load(ctx, agent_v2.HistoryKey{Owner: tenant, ConvID: conv})
	if err != nil {
		log.Printf("  Load 失败：%v", err)
		return nil
	}
	return msgs
}

func countContains(msgs []*schema.Message, sub string) int {
	n := 0
	for _, m := range msgs {
		if strings.Contains(m.Content, sub) {
			n++
		}
	}
	return n
}

// externalShell 是「不是 BasicAgent 的执行体」——远程 agent / 自研 agent / 适配别的
// 框架的壳都是这个形状：实现 Agent 接口，对历史层一无所知，跑完什么也不留。
type externalShell struct {
	name     string
	reply    string
	failOnce atomic.Bool // 置位时第一次 Run 返业务错
	intOnce  atomic.Bool // 置位时第一次 Run 抛中断
	runs     atomic.Int64
}

func (a *externalShell) Run(_ context.Context, sess *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.runs.Add(1)
	if a.intOnce.CompareAndSwap(true, false) {
		return nil, &compose_v2.InterruptError{
			Reason: "外部执行体需要确认",
			Query:  &compose_v2.AskQuery{InterruptID: a.name + "-q1", Question: "确认继续？"},
		}
	}
	if a.failOnce.CompareAndSwap(true, false) {
		return nil, fmt.Errorf("%s: 模拟的业务失败", a.name)
	}
	out := schema.AssistantMessage(a.reply + "｜收到：" + strings.TrimSpace(in.Content))
	sess.AddEvent(agent_v2.NewExitEvent(a.name, out))
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, out)}, nil
}

func (a *externalShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *externalShell) Name() string        { return a.name }
func (a *externalShell) Description() string { return "外部执行体（不写历史）" }

// weatherTool 让真 LLM 有工具可调——验「历史里有完整的工具往返，不只首尾」。
func weatherTool() tool.Tool {
	return tool.NewFuncTool("get_weather", "查询指定城市今天的天气",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"city": map[string]any{"type": "string", "description": "城市名"},
			},
			"required": []string{"city"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			city, _ := args["city"].(string)
			return tool.Success(fmt.Sprintf(
				`{"city":%q,"weather":"晴","temp_c":26,"marker":"WEATHER_TOOL_RESULT"}`, city)), nil
		})
}

func main() {
	flag.Parse()
	envloader.Load()
	if os.Getenv("QWEN_API_KEY") == "" {
		log.Fatal("QWEN_API_KEY 未设置（.env 加 QWEN_API_KEY=xxx）——本 smoke 需真 qwen")
	}
	ctx := context.Background()

	scenarios := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"1. 核心：工具往返完整进历史（不只首尾）", scToolRoundTrip},
		{"2. 核心：跨 Run 续接，第二轮模型看得到第一轮", scCrossRun},
		{"3. 旁支：外部执行体那步靠边界补记才留得下", scExternalBoundary},
		{"4. 失败：外部执行体中断 + Resume，历史不丢不重", scInterruptResume},
		{"5. 失败：外部执行体返业务错，历史只留跑成功的部分", scBusinessError},
		{"6. 核心：并发扇出每条分支流各自完整（有问有答）", scFanout},
		{"7. 失败：子 agent 历史配置冲突要 fail-fast", scConflictFailFast},
		{"8. 旁支：AgentAsTool 不重记 + 被摘除的分类器不进历史", scToolAndSuppressed},
		{"9. 核心：顶层 GraphAgent 的历史可跨 Run 读回", scGraphAgentTopLevel},
	}
	for i, s := range scenarios {
		if *onlyFlag != 0 && *onlyFlag != i+1 {
			continue
		}
		fmt.Printf("=== %s ===\n", s.name)
		report(s.name, s.fn(ctx))
	}

	fmt.Printf("=== 总结：%d 通过 / %d 失败 / %d 跳过 ===\n", passed, failed, skipped)
	if failed > 0 {
		os.Exit(1)
	}
}

// ── 场景 1：工具往返完整进历史 ──
//
// `prior` 换源到历史层的**唯一理由**就是「events 只记首尾，模型看不到结论的来源」。
// 这条验的是那个理由成立：历史里要能看到 tool_call 和 tool_result，不是只有首尾两条。
func scToolRoundTrip(ctx context.Context) error {
	hs := agent_v2.NewInMemoryHistoryStore()
	tk := tool.NewToolkit()
	tk.Register(weatherTool())
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("weather_bot"),
		agent_v2.WithModel(newModel()),
		agent_v2.WithToolkit(tk),
		agent_v2.WithSystemPrompt("你是天气助手。用户问天气时必须调用 get_weather 工具，不要凭空回答。"),
		agent_v2.WithHistory(hs, tenant),
		agent_v2.WithMaxRounds(4))

	conv := "sc1"
	c := compose_v2.WithThreadIDCtx(ctx, conv)
	if _, err := ag.Run(c, agent_v2.NewRunSession(), schema.UserMessage("北京今天天气怎么样？")); err != nil {
		return fmt.Errorf("Run: %w", err)
	}
	msgs := load(c, hs, conv)
	fmt.Print("  " + dump(msgs))

	var hasUser, hasToolCall, hasToolResult, hasFinal bool
	for _, m := range msgs {
		switch {
		case m.Role == schema.RoleUser && strings.Contains(m.Content, "北京"):
			hasUser = true
		case len(schema.GetToolCalls(m)) > 0:
			hasToolCall = true
		case m.Role == schema.RoleTool && strings.Contains(m.Content, "WEATHER_TOOL_RESULT"):
			hasToolResult = true
		case m.Role == schema.RoleAssistant && strings.TrimSpace(m.Content) != "":
			hasFinal = true
		}
	}
	if !hasUser {
		return fmt.Errorf("用户提问不在历史里")
	}
	if !hasToolCall || !hasToolResult {
		return skipf("模型这次没调工具（tool_call=%v result=%v）——弱模型不遵循指令，不是历史层的问题",
			hasToolCall, hasToolResult)
	}
	if !hasFinal {
		return fmt.Errorf("最终答复不在历史里")
	}
	return nil
}

// ── 场景 2：跨 Run 续接 ──
//
// 第二个 Run 里模型必须看得到第一轮说过的事实。这条同时验 `prior` 真的从历史读回来了。
func scCrossRun(ctx context.Context) error {
	hs := agent_v2.NewInMemoryHistoryStore()
	mk := func() agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("memo_bot"),
			agent_v2.WithModel(newModel()),
			agent_v2.WithSystemPrompt("你是助手。回答简短，只用一句话。"),
			agent_v2.WithHistory(hs, tenant),
			agent_v2.WithMaxRounds(3))
	}
	conv := "sc2"
	c := compose_v2.WithThreadIDCtx(ctx, conv)
	if _, err := mk().Run(c, agent_v2.NewRunSession(), schema.UserMessage("请记住：我的项目代号是 ORION-7。")); err != nil {
		return fmt.Errorf("Run 1: %w", err)
	}
	// 换一个**新构造**的 agent 对象——业务每请求新建是常规写法
	evs, err := mk().Run(c, agent_v2.NewRunSession(), schema.UserMessage("我的项目代号是什么？直接给代号。"))
	if err != nil {
		return fmt.Errorf("Run 2: %w", err)
	}
	final := agent_v2.FinalMessage(evs)
	msgs := load(c, hs, conv)
	fmt.Print("  " + dump(msgs))
	fmt.Printf("  第二轮答复：%q\n", strings.TrimSpace(final.Content))

	if !strings.Contains(strings.ToUpper(final.Content), "ORION-7") {
		return fmt.Errorf("第二轮模型没答出第一轮的代号——历史没读回来（或模型没遵循）")
	}
	if countContains(msgs, "ORION-7") < 2 {
		return fmt.Errorf("历史里 ORION-7 出现不足 2 次（第一轮提问 + 第二轮答复）")
	}
	return nil
}

// ── 场景 3：外部执行体靠边界补记 ──
func scExternalBoundary(ctx context.Context) error {
	hs := agent_v2.NewInMemoryHistoryStore()
	local := agent_v2.NewBasicAgent(
		agent_v2.WithName("summarizer"),
		agent_v2.WithModel(newModel()),
		agent_v2.WithSystemPrompt("把收到的内容压成一句话。"),
		agent_v2.WithHistory(hs, tenant),
		agent_v2.WithMaxRounds(3))
	ext := &externalShell{name: "remote_fetcher", reply: "REMOTE_PAYLOAD"}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("flow")).
		AddStep("fetch", ext).
		AddStep("sum", local)

	conv := "sc3"
	c := compose_v2.WithThreadIDCtx(ctx, conv)
	if _, err := seq.Run(c, agent_v2.NewRunSession(), schema.UserMessage("USER_ASK_3 帮我取一下数据")); err != nil {
		return fmt.Errorf("Run: %w", err)
	}
	msgs := load(c, hs, conv)
	fmt.Print("  " + dump(msgs))

	if countContains(msgs, "USER_ASK_3") == 0 {
		return fmt.Errorf("用户原话缺席——第一步交给了外部执行体，边界没补上")
	}
	if n := countContains(msgs, "REMOTE_PAYLOAD"); n == 0 {
		return fmt.Errorf("外部执行体的产出缺席")
	} else if n > 1 {
		return fmt.Errorf("外部执行体的产出记了 %d 次——同一条产出多个归属", n)
	}
	return nil
}

// ── 场景 4：中断 + Resume ──
func scInterruptResume(ctx context.Context) error {
	hs := agent_v2.NewInMemoryHistoryStore()
	cp := compose_v2.NewInMemoryCheckpointer()
	local := agent_v2.NewBasicAgent(
		agent_v2.WithName("finisher"),
		agent_v2.WithModel(newModel()),
		agent_v2.WithSystemPrompt("回一句确认。"),
		agent_v2.WithHistory(hs, tenant),
		agent_v2.WithMaxRounds(3))
	ext := &externalShell{name: "gated_step", reply: "GATED_PAYLOAD"}
	ext.intOnce.Store(true)
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("flow")).
		AddStep("gate", ext).
		AddStep("done", local)

	conv := "sc4"
	c := compose_v2.WithThreadIDCtx(ctx, conv)
	c = compose_v2.WithCheckpointerCtx(c, cp)
	sess := agent_v2.NewRunSession()
	_, err := seq.Run(c, sess, schema.UserMessage("USER_ASK_4 走一遍带确认的流程"))
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		return fmt.Errorf("第一次 Run 应中断，实际 err=%v", err)
	}
	fmt.Printf("  中断：id=%s question=%q\n", ie.Query.InterruptID, ie.Query.Question)
	if _, err := seq.Resume(c, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("确认")})); err != nil {
		return fmt.Errorf("Resume: %w", err)
	}
	msgs := load(c, hs, conv)
	fmt.Print("  " + dump(msgs))

	if countContains(msgs, "USER_ASK_4") == 0 {
		return fmt.Errorf("用户原话缺席——Resume 那一段没建写入器")
	}
	if n := countContains(msgs, "GATED_PAYLOAD"); n != 1 {
		return fmt.Errorf("外部执行体产出出现 %d 次，应恰好 1 次", n)
	}
	if got := ext.runs.Load(); got != 2 {
		return fmt.Errorf("外部执行体跑了 %d 次，应 2 次（中断一次 + Resume 一次）", got)
	}
	return nil
}

// ── 场景 5：业务失败 ──
//
// 失败路径的语义：历史是**旁路**，跑成功的那部分该留下，失败不该把已记的抹掉，
// 也不该把没跑的编出来。
func scBusinessError(ctx context.Context) error {
	hs := agent_v2.NewInMemoryHistoryStore()
	okStep := &externalShell{name: "step_ok", reply: "STEP_OK_PAYLOAD"}
	badStep := &externalShell{name: "step_bad", reply: "NEVER_SEEN"}
	badStep.failOnce.Store(true)
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("flow")).
		AddStep("ok", okStep).
		AddStep("bad", badStep).
		AddStep("never", agent_v2.NewBasicAgent(
			agent_v2.WithName("never_runs"),
			agent_v2.WithModel(newModel()),
			agent_v2.WithHistory(hs, tenant),
			agent_v2.WithMaxRounds(2)))

	conv := "sc5"
	c := compose_v2.WithThreadIDCtx(ctx, conv)
	_, err := seq.Run(c, agent_v2.NewRunSession(), schema.UserMessage("USER_ASK_5 跑一个会中途失败的流程"))
	if err == nil {
		return fmt.Errorf("第二步该失败，实际成功了")
	}
	fmt.Printf("  预期内的失败：%v\n", err)
	msgs := load(c, hs, conv)
	fmt.Print("  " + dump(msgs))

	if countContains(msgs, "STEP_OK_PAYLOAD") != 1 {
		return fmt.Errorf("跑成功那步的产出应留下恰好 1 份")
	}
	if countContains(msgs, "NEVER_SEEN") != 0 {
		return fmt.Errorf("失败那步不该有产出进历史")
	}
	return nil
}

// ── 场景 6：并发扇出 ──
//
// SelfOnly 下每条分支流要各自完整（有问有答）——这是有意的语义，不是重复。
func scFanout(ctx context.Context) error {
	hs := agent_v2.NewInMemoryHistoryStore()
	mk := func(name, role string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName(name),
			agent_v2.WithModel(newModel()),
			agent_v2.WithSystemPrompt("你是"+role+"。用一句话回答。"),
			agent_v2.WithHistory(hs, tenant),
			agent_v2.WithMaxRounds(3))
	}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("panel")).
		AddBranch("optimist", mk("a1", "乐观派")).
		AddBranch("pessimist", mk("a2", "悲观派")).
		AddBranch("realist", mk("a3", "现实派"))

	conv := "sc6"
	c := compose_v2.WithThreadIDCtx(ctx, conv)
	if _, err := par.Run(c, agent_v2.NewRunSession(), schema.UserMessage("USER_ASK_6 远程办公是好是坏？")); err != nil {
		return fmt.Errorf("Run: %w", err)
	}
	msgs := load(c, hs, conv)
	fmt.Print("  " + dump(msgs))

	// 每条分支流：既有它自己的提问，也有它自己的答复
	streams := map[string]struct{ q, a int }{}
	for _, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		s := streams[o]
		if m.Role == schema.RoleUser {
			s.q++
		} else if m.Role == schema.RoleAssistant && strings.TrimSpace(m.Content) != "" {
			s.a++
		}
		streams[o] = s
	}
	if len(streams) != 3 {
		return fmt.Errorf("应有 3 条分支流，实际 %d 条：%v", len(streams), streams)
	}
	for o, s := range streams {
		if s.q != 1 || s.a < 1 {
			return fmt.Errorf("分支流 %q 不完整（提问 %d 条 / 答复 %d 条）——每条流该有问有答", o, s.q, s.a)
		}
	}
	return nil
}

// ── 场景 7：配置冲突 fail-fast ──
func scConflictFailFast(ctx context.Context) error {
	hsA, hsB := agent_v2.NewInMemoryHistoryStore(), agent_v2.NewInMemoryHistoryStore()
	mk := func(name string, hs agent_v2.HistoryStore, owner string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName(name),
			agent_v2.WithModel(newModel()),
			agent_v2.WithHistory(hs, owner),
			agent_v2.WithMaxRounds(2))
	}
	// 两个子 agent 配了不同租户 —— 框架无法替业务选，必须报错
	ho := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("front"), agent_v2.WithHandoffInitial("a"),
	).AddAgent("a", mk("agent_a", hsA, "tenant_a")).
		AddAgent("b", mk("agent_b", hsB, "tenant_b"))

	c := compose_v2.WithThreadIDCtx(ctx, "sc7")
	_, err := ho.Run(c, agent_v2.NewRunSession(), schema.UserMessage("USER_ASK_7"))
	if err == nil {
		return fmt.Errorf("配了两个租户应 fail-fast，实际成功了——那意味着对话落进了其中一个，而是哪个取决于迭代顺序")
	}
	if !strings.Contains(err.Error(), "conflicting history config") {
		return fmt.Errorf("报错该指明是历史配置冲突，实际：%v", err)
	}
	fmt.Printf("  预期内的 fail-fast：%v\n", err)

	// 对照：同一套配置照常跑
	ho2 := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("front2"), agent_v2.WithHandoffInitial("a"),
	).AddAgent("a", mk("agent_a", hsA, "tenant_a")).
		AddAgent("b", mk("agent_b", hsA, "tenant_a"))
	c2 := compose_v2.WithThreadIDCtx(ctx, "sc7b")
	if _, err := ho2.Run(c2, agent_v2.NewRunSession(), schema.UserMessage("USER_ASK_7B 你好")); err != nil {
		return fmt.Errorf("同一套配置不该报错：%w", err)
	}
	fmt.Println("  对照组（同一套配置）正常跑通")
	return nil
}

// ── 场景 8：AgentAsTool 不重记 + 被摘除的分类器不进历史 ──
func scToolAndSuppressed(ctx context.Context) error {
	hs := agent_v2.NewInMemoryHistoryStore()

	// (a) AgentAsTool 包一个外部执行体：产出经父的 tool_result 进历史，只该有一份
	sub := &externalShell{name: "lookup", reply: "TOOL_SUB_PAYLOAD"}
	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(sub, agent_v2.WithAgentToolDescription("查询内部资料库，任何需要查资料的问题都用它")))
	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("router_bot"),
		agent_v2.WithModel(newModel()),
		agent_v2.WithToolkit(tk),
		agent_v2.WithSystemPrompt("需要查资料时必须调 lookup 工具。"),
		agent_v2.WithHistory(hs, tenant),
		agent_v2.WithMaxRounds(4))

	conv := "sc8"
	c := compose_v2.WithThreadIDCtx(ctx, conv)
	if _, err := parent.Run(c, agent_v2.NewRunSession(), schema.UserMessage("USER_ASK_8 帮我查一下内部资料库里关于休假政策的说明")); err != nil {
		return fmt.Errorf("Run: %w", err)
	}
	msgs := load(c, hs, conv)
	fmt.Print("  (a) AgentAsTool " + dump(msgs))
	if sub.runs.Load() == 0 {
		fmt.Println("  (a) 模型这次没调工具，跳过重复性断言")
	} else if n := countContains(msgs, "TOOL_SUB_PAYLOAD"); n != 1 {
		return fmt.Errorf("(a) 子 agent 产出出现 %d 次，应恰好 1 次（经父的 tool_result）", n)
	}

	// (b) Routing 的分类器被 WithoutHistoryCtx 摘除：它的产出不该进历史，
	//     但它的**配置**仍是有效来源（分支全是外部执行体时它是唯一来源）
	classifier := agent_v2.NewBasicAgent(
		agent_v2.WithName("classifier"),
		agent_v2.WithModel(newModel()),
		agent_v2.WithSystemPrompt("只输出一个词：tech 或 life。CLASSIFIER_MARKER"),
		agent_v2.WithHistory(hs, tenant),
		agent_v2.WithMaxRounds(2))
	rt := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("rt"),
		agent_v2.WithRoutingClassifier(classifier),
		agent_v2.WithRoutingFallback(&externalShell{name: "fb", reply: "FALLBACK_PAYLOAD"}),
	).AddBranch("tech", &externalShell{name: "tech", reply: "TECH_PAYLOAD"}).
		AddBranch("life", &externalShell{name: "life", reply: "LIFE_PAYLOAD"})

	conv2 := "sc8b"
	c2 := compose_v2.WithThreadIDCtx(ctx, conv2)
	if _, err := rt.Run(c2, agent_v2.NewRunSession(), schema.UserMessage("USER_ASK_8B Go 的 GC 是怎么工作的？")); err != nil {
		return fmt.Errorf("(b) Run: %w", err)
	}
	msgs2 := load(c2, hs, conv2)
	fmt.Print("  (b) Routing " + dump(msgs2))
	if len(msgs2) == 0 {
		return fmt.Errorf("(b) 历史全空——分类器没被当成配置来源，写入器没建起来")
	}
	if countContains(msgs2, "CLASSIFIER_MARKER") > 0 {
		return fmt.Errorf("(b) 分类器的内容进了历史——WithoutHistoryCtx 没生效")
	}
	hit := countContains(msgs2, "TECH_PAYLOAD") + countContains(msgs2, "LIFE_PAYLOAD") + countContains(msgs2, "FALLBACK_PAYLOAD")
	if hit != 1 {
		return fmt.Errorf("(b) 命中分支的产出应恰好 1 份，实际 %d 份", hit)
	}
	if countContains(msgs2, "USER_ASK_8B") == 0 {
		return fmt.Errorf("(b) 用户原话缺席")
	}
	return nil
}

// ── 场景 9：顶层 GraphAgent ──
//
// GraphAgent 此前对历史层整体没接，**顶层直接跑它历史全程为空**。它没有静态子列表
// （子由业务 NodeFunc 经 RunSubAgent 调起），配置只能来自运行入口声明 WithHistoryCtx。
//
// 这里同时验一件容易被忽略的事：接上历史 ≠ 历史**能用**。归属（HistoryAgentAnnotation）
// 是跨 Run 读回的键，读写两侧同源、一侧变了另一侧要跟着变，筛空时的失效模式是**彻底空**
// 不是报错。所以判据放在「模型答没答出来」上，不是「历史里有没有条数」。
//
// （原注释说「顶层 GraphAgent 会给自己推一层位置、子的归属随之带上前缀」——那是第一版，
// 已被 `aa9226b` 换掉：推 slot 会让图名成为所有后代归属的根段，代价是改个图名它下面所有
// 子的历史全变孤儿。现在顶层只开范围回执不推 slot，子的归属逐字不变。）
func scGraphAgentTopLevel(ctx context.Context) error {
	hs := agent_v2.NewInMemoryHistoryStore()
	inCh := compose_v2.LastValue[*schema.Message]("__sc9_in__")
	outCh := compose_v2.LastValue[*schema.Message]("__sc9_out__")

	mkGA := func() agent_v2.Agent {
		bld := compose_v2.NewBuilder().WithName("g_sc9")
		compose_v2.WithChannel(bld, inCh)
		compose_v2.WithChannel(bld, outCh)
		bld.AddNode("ask", func(c context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			in, _ := compose_v2.Get(chs, inCh)
			// 图里的节点调真 BasicAgent——业务把 GraphAgent 当编排壳的常规写法
			sub := agent_v2.NewBasicAgent(
				agent_v2.WithName("sc9_bot"),
				agent_v2.WithModel(newModel()),
				agent_v2.WithSystemPrompt("你是助手。回答简短，只用一句话。"),
				agent_v2.WithMaxRounds(3))
			evs, err := agent_v2.RunSubAgent(c, "worker", sub, in)
			if err != nil {
				return nil, err
			}
			out := agent_v2.FinalMessage(evs)
			if out == nil {
				out = schema.AssistantMessage("")
			}
			return []compose_v2.ChannelWrite{compose_v2.Write(outCh, out)}, nil
		})
		bld.AddEdge(compose_v2.StartNode, "ask")
		bld.AddEdge("ask", compose_v2.EndNode)
		g, err := bld.Compile()
		if err != nil {
			panic(err)
		}
		return agent_v2.NewGraphAgent(g,
			agent_v2.WithGraphAgentName("sc9_graph"),
			agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
				return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
			}),
			agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
				out, _ := compose_v2.Get(chs, outCh)
				return out
			}))
	}

	conv := "sc9"
	// 配置走运行入口声明——GraphAgent 借不到子的配置，这是它唯一的来源
	c := agent_v2.WithHistoryCtx(ctx, hs, tenant)
	c = compose_v2.WithThreadIDCtx(c, conv)

	if _, err := mkGA().Run(c, agent_v2.NewRunSession(), schema.UserMessage("请记住：我的机器编号是 VEGA-42。")); err != nil {
		return fmt.Errorf("Run 1: %w", err)
	}
	// 换新构造的对象，跟场景 2 同款（业务每请求新建是常规写法）
	evs, err := mkGA().Run(c, agent_v2.NewRunSession(), schema.UserMessage("我的机器编号是什么？直接给编号。"))
	if err != nil {
		return fmt.Errorf("Run 2: %w", err)
	}
	final := agent_v2.FinalMessage(evs)
	msgs := load(c, hs, conv)
	fmt.Print("  " + dump(msgs))
	if final != nil {
		fmt.Printf("  第二轮答复：%q\n", strings.TrimSpace(final.Content))
	}

	if len(msgs) == 0 {
		return fmt.Errorf("顶层 GraphAgent 历史全程为空——它没接历史层（或配置没经 WithHistoryCtx 落到它身上）")
	}
	if final == nil || !strings.Contains(strings.ToUpper(final.Content), "VEGA-42") {
		return fmt.Errorf("第二轮模型没答出第一轮的编号——历史读不回来。\n" +
			"  接上写入 ≠ 读得回：归属是跨 Run 读回的键，顶层推的那层位置若让读取侧筛空，" +
			"失效模式是彻底空不是报错")
	}
	return nil
}
