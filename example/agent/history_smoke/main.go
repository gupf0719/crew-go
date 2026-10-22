// history_smoke 验会话历史层（`WithHistory`）——成功路径 + 失败路径 + 并发。
//
// 会话历史层被定位成**旁路**：它记不下来是它自己的事，绝不能反过来影响主链路。
// 这个定位原先只写在文档里，本冒烟把它变成可执行的断言——后端写失败 / 后端 panic /
// 后端读失败 / 后端卡住不返回，四种违约下 agent 都得照常给出答案。
//
// 场景：
//  1. 真模型跨 RunSession 的完整流（含工具往返）—— 需要 QWEN_API_KEY
//     1b. 真模型多 agent 编排：归属按位置分开、显示名是 agent 名 —— 需要 QWEN_API_KEY
//  2. 后端写失败      → 主链路不受影响
//  3. 后端 panic      → 吞掉，主链路不受影响
//  4. 后端读失败      → 回落，不崩
//  5. 后端卡住不返回  → 主链路不被旁路挂死
//  6. 高并发多会话    → 互不串台、每轮输入不丢
//
// 跑法：QWEN_API_KEY 放项目根 .env 或环境变量。`go run ./example/agent/history_smoke`。
// 无 key 时自动跳过场景 1，其余照跑。退出码非 0 表示有场景失败。
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/example/envloader"
	"crew-go/fallback"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/model/qwen"
	"crew-go/schema"
	"crew-go/tool"
)

// newModel 把全部候选串成一条 fallback 链。
//
// **不要写 `newModel()`**：候选表第一项是带日期的
// 固定快照，它的免费额度会耗尽（实测 403 `AllocationQuota.FreeTierOnly`）——那时冒烟
// 报的是「场景失败」，读的人第一反应是去查代码，而实际上一行代码都没问题。
// fallback 认得这个 403，会自动切下一个候选。
func newModel() model.ChatModel {
	ms := make([]model.ChatModel, 0, len(qwen.TestModelCandidates))
	for _, n := range qwen.TestModelCandidates {
		ms = append(ms, qwen.NewTestClient(n))
	}
	return fallback.New(ms)
}

// ── 观测 hook：记录每轮喂给模型的消息流 ──

type capture struct {
	hook.NoOpHooks
	mu    sync.Mutex
	turns [][]*schema.Message
}

func (c *capture) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	snap := make([]*schema.Message, len(in.Messages))
	copy(snap, in.Messages)
	c.turns = append(c.turns, snap)
	return ctx, nil
}

// ── mock 模型：读 input 做回显（mock 必须读 input，否则抓不到上游吞消息类问题）──

type echoModel struct{}

func (m *echoModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	var last string
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == schema.RoleUser {
			last = msgs[i].Content
			break
		}
	}
	return schema.AssistantMessage("ack:" + last), nil
}

func (m *echoModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	out, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(out)
	schema.CloseSend(sr)
	return sr, nil
}

func (m *echoModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// ── 违约后端：按配置以各种方式失败 ──

type faultyStore struct {
	failAppend bool
	panicOnAdd bool
	failLoad   bool
	blockAdd   chan struct{} // 非 nil 时 Append 阻塞直到它被关闭
}

func (s *faultyStore) Append(ctx context.Context, _ agent_v2.HistoryKey, _ []*schema.Message) error {
	if s.blockAdd != nil {
		select {
		case <-s.blockAdd:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.panicOnAdd {
		panic("faulty backend: boom on append")
	}
	if s.failAppend {
		return errors.New("faulty backend: append refused")
	}
	return nil
}

func (s *faultyStore) Load(_ context.Context, _ agent_v2.HistoryKey, _ agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	if s.failLoad {
		return agent_v2.HistoryPage{}, errors.New("faulty backend: load refused")
	}
	return agent_v2.HistoryPage{}, nil
}

func main() {
	envloader.Load()
	fails := 0

	if !scenarioRealModel() {
		fails++
	}
	if !scenarioRealModelMultiAgent() {
		fails++
	}
	if !scenarioRealModelNestedOrchestration() {
		fails++
	}
	for _, sc := range []struct {
		name string
		run  func() bool
	}{
		{"后端写失败", func() bool { return runBypassCase(&faultyStore{failAppend: true}, "写失败") }},
		{"后端 panic", func() bool { return runBypassCase(&faultyStore{panicOnAdd: true}, "panic") }},
		{"后端读失败", func() bool { return runBypassCase(&faultyStore{failLoad: true}, "读失败") }},
		{"后端卡住不返回", scenarioAppendBlocks},
		{"高并发多会话", scenarioConcurrent},
	} {
		fmt.Printf("\n┌─ %s ─\n", sc.name)
		if !sc.run() {
			fails++
		}
	}

	fmt.Println()
	if fails > 0 {
		fmt.Printf("=== 有 %d 个场景失败 ===\n", fails)
		os.Exit(1)
	}
	fmt.Println("=== ALL SCENARIOS PASSED ===")
}

// 场景 1：真模型跨 RunSession——第二轮要能看到第一轮的完整流（含工具往返）。
//
// 为什么值得真模型跑：多轮 prior 一旦残缺（工具往返丢失），模型无法判断上一轮结论
// 的来源，追问「你怎么查到的」就开始编造。这类退化不报错，单测也容易因为 mock 不读
// input 而假绿。
func scenarioRealModel() bool {
	fmt.Println("┌─ 真模型跨 RunSession 的完整流 ─")
	if os.Getenv("QWEN_API_KEY") == "" {
		fmt.Println("  跳过：QWEN_API_KEY 未设置")
		return true
	}

	stock := tool.NewFuncTool("query_stock", "查询指定 SKU 的库存数量",
		map[string]any{
			"type":       "object",
			"properties": map[string]any{"sku": map[string]any{"type": "string"}},
			"required":   []string{"sku"},
		},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			return tool.Success(fmt.Sprintf("SKU %v 库存 4271 件", args["sku"])), nil
		})
	tk := tool.NewToolkit()
	tk.Register(stock)

	cap := &capture{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("stock_bot"), // 历史层要求显式命名
		agent_v2.WithModel(newModel()),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(cap),
		agent_v2.WithHistory(agent_v2.NewInMemoryHistoryStore(), "tenant-A"), // Owner 必填
		agent_v2.WithMaxRounds(4),
	)
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-real-1")

	ev1, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("SKU-9001 还有多少库存？"))
	if err != nil {
		fmt.Println("  ✗ 第 1 轮失败:", err)
		return false
	}
	fmt.Println("  轮 1 答:", oneLine(agent_v2.FinalMessage(ev1).Content))

	before := len(cap.turns)
	ev2, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("你刚才是怎么查到这个数字的？"))
	if err != nil {
		fmt.Println("  ✗ 第 2 轮失败:", err)
		return false
	}
	fmt.Println("  轮 2 答:", oneLine(agent_v2.FinalMessage(ev2).Content))

	if before >= len(cap.turns) {
		fmt.Println("  ✗ 第 2 轮没捕获到模型调用")
		return false
	}
	var sawCall, sawResult bool
	for _, m := range cap.turns[before] {
		if len(schema.GetToolCalls(m)) > 0 {
			sawCall = true
		}
		if m.Role == schema.RoleTool && strings.Contains(m.Content, "4271") {
			sawResult = true
		}
	}
	fmt.Printf("  第 2 轮开局 %d 条消息；含轮 1 tool_call=%v tool_result=%v\n",
		len(cap.turns[before]), sawCall, sawResult)
	if !sawCall || !sawResult {
		fmt.Println("  ✗ 第 2 轮看不到第 1 轮的工具往返——历史层没起作用")
		return false
	}
	fmt.Println("  ✓ 跨 RunSession 拿到完整流")
	return true
}

// runBypassCase 跑两轮，断言违约后端不影响主链路产出（场景 2/3/4 共用）。
func runBypassCase(st agent_v2.HistoryStore, what string) bool {
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("bypass_bot"),
		agent_v2.WithModel(&echoModel{}),
		agent_v2.WithHistory(st, "tenant-B"),
		agent_v2.WithMaxRounds(2),
	)
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-bypass-"+what)
	for i := 0; i < 2; i++ {
		want := fmt.Sprintf("hello-%d", i)
		ev, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(want))
		if err != nil {
			fmt.Printf("  ✗ 后端%s 把主链路带崩了: %v\n", what, err)
			return false
		}
		got := agent_v2.FinalMessage(ev)
		if got == nil || !strings.Contains(got.Content, want) {
			fmt.Printf("  ✗ 答案不对（期望含 %q，实际 %v）\n", want, got)
			return false
		}
	}
	fmt.Printf("  ✓ 后端%s 被吞掉，两轮都正常出答案\n", what)
	return true
}

// 场景 5：后端卡住不返回——主链路不能被旁路挂死。
func scenarioAppendBlocks() bool {
	release := make(chan struct{})
	defer close(release) // 收尾放行，别泄漏 worker

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("block_bot"),
		agent_v2.WithModel(&echoModel{}),
		agent_v2.WithHistory(&faultyStore{blockAdd: release}, "tenant-C"),
		agent_v2.WithMaxRounds(2),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx = compose_v2.WithThreadIDCtx(ctx, "conv-block")

	done := make(chan error, 1)
	go func() {
		_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("ping"))
		done <- err
	}()

	select {
	case err := <-done:
		// 卡住的是旁路：主链路要么正常出答案，要么因 ctx 超时干净返回——
		// 两者都可接受，唯独不能挂在那里不回来。
		fmt.Printf("  ✓ 后端卡住时主链路仍返回（err=%s）\n", errBrief(err))
		return true
	case <-time.After(10 * time.Second):
		fmt.Println("  ✗ 后端卡住把主链路一起挂死了（10s 未返回）")
		return false
	}
}

// 场景 6：高并发多会话——互不串台、每轮输入不丢。
//
// 每个 goroutine 用**独立 ConvID**：同 ConvID 并发写是已立项的开放问题（并发分支各
// 记一份原始输入），此处避开，专测隔离性与完整性。
func scenarioConcurrent() bool {
	const convs, turns = 40, 3
	hs := agent_v2.NewInMemoryHistoryStore()
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("stress_bot"),
		agent_v2.WithModel(&echoModel{}),
		agent_v2.WithHistory(hs, "tenant-S"),
		agent_v2.WithMaxRounds(2),
	)
	root, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var start, done sync.WaitGroup
	start.Add(1)
	errCh := make(chan error, convs)
	for c := 0; c < convs; c++ {
		done.Add(1)
		go func(c int) {
			defer done.Done()
			start.Wait() // 屏障：强制真并发，防 scheduler 自然串行化
			ctx := compose_v2.WithThreadIDCtx(root, fmt.Sprintf("stress-%d", c))
			for t := 0; t < turns; t++ {
				in := schema.UserMessage(fmt.Sprintf("c%d-t%d", c, t))
				if _, err := ag.Run(ctx, agent_v2.NewRunSession(), in); err != nil {
					errCh <- err
					return
				}
			}
		}(c)
	}
	start.Done()

	fin := make(chan struct{})
	go func() { done.Wait(); close(fin) }()
	select {
	case <-fin:
	case <-root.Done():
		fmt.Println("  ✗ 超时：并发跑不完（疑似挂死）")
		return false
	}
	close(errCh)
	for err := range errCh {
		fmt.Println("  ✗ 并发 Run 出错:", err)
		return false
	}

	crossTalk, incomplete := 0, 0
	for c := 0; c < convs; c++ {
		msgs, err := agent_v2.LoadAllHistory(context.Background(), hs,
			agent_v2.HistoryKey{Owner: "tenant-S", ConvID: fmt.Sprintf("stress-%d", c)})
		if err != nil {
			fmt.Printf("  ✗ Load conv%d 失败: %v\n", c, err)
			return false
		}
		var sb strings.Builder
		for _, m := range msgs {
			sb.WriteString(m.Content)
			sb.WriteString("\n")
		}
		s := sb.String()
		for other := 0; other < convs; other++ {
			if other != c && strings.Contains(s, fmt.Sprintf("c%d-t", other)) {
				crossTalk++
				break
			}
		}
		for t := 0; t < turns; t++ {
			if !strings.Contains(s, fmt.Sprintf("c%d-t%d", c, t)) {
				incomplete++
				break
			}
		}
	}
	fmt.Printf("  %d 会话 × %d 轮：串台 %d，不完整 %d（都应为 0）\n", convs, turns, crossTalk, incomplete)
	if crossTalk != 0 || incomplete != 0 {
		return false
	}
	fmt.Println("  ✓ 互不串台、每轮输入无丢失")
	return true
}

func oneLine(s string) string {
	s = strings.TrimSpace(strings.ReplaceAll(s, "\n", " "))
	if len(s) > 70 {
		return s[:70] + "…"
	}
	return s
}

func errBrief(err error) string {
	if err == nil {
		return "nil"
	}
	return oneLine(err.Error())
}

// 场景 1b：真模型 + 多 agent 编排，验归属机制在真实链路上成立。
//
// 单测能钉住机制，但钉不住「真模型跑起来之后这套东西还对不对」——尤其两件事：
// 归属按**编排位置**分开（两个分支的 agent 故意同名，只可能靠位置区分），以及模型
// 在跨归属改写里看到的是**agent 名**而不是编排内部的槽位名（`br1` 那种模型看不懂）。
func scenarioRealModelMultiAgent() bool {
	fmt.Println("\n┌─ 真模型多 agent：归属按位置分开 + 显示名可读 ─")
	if os.Getenv("QWEN_API_KEY") == "" {
		fmt.Println("  跳过：QWEN_API_KEY 未设置")
		return true
	}

	hs := agent_v2.NewInMemoryHistoryStore()
	mk := func(role, sys string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			// **两个分支的 agent 故意同名**：归属若还用 agent 名就会合并成一条流
			agent_v2.WithName("analyst"),
			agent_v2.WithSystemPrompt(sys),
			agent_v2.WithModel(newModel()),
			agent_v2.WithHistory(hs, "tenant-M"),
			agent_v2.WithMaxRounds(3),
		)
	}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("panel")).
		AddBranch("optimist", mk("optimist", "你是乐观派分析师。用一句话给出偏乐观的判断。")).
		AddBranch("skeptic", mk("skeptic", "你是审慎派分析师。用一句话给出偏谨慎的判断。"))

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-panel")
	if _, err := par.Run(ctx, agent_v2.NewRunSession(),
		schema.UserMessage("这个季度要不要加大投放？")); err != nil {
		fmt.Println("  ✗ Run 失败:", err)
		return false
	}

	msgs, err := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "tenant-M", ConvID: "conv-panel"})
	if err != nil {
		fmt.Println("  ✗ Load 失败:", err)
		return false
	}
	byOwner := map[string]int{}
	displays := map[string]bool{}
	for _, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		d, _ := m.Annotations[agent_v2.HistoryAgentDisplayAnnotation].(string)
		byOwner[o]++
		if d != "" {
			displays[d] = true
		}
		if m.Role == schema.RoleAssistant && strings.TrimSpace(m.Content) != "" {
			fmt.Printf("  %-9s(显示 %s): %s\n", o, d, oneLine(m.Content))
		}
	}
	fmt.Printf("  归属分布: %v；显示名: %v\n", byOwner, keysOf(displays))

	ok := true
	if len(byOwner) != 2 {
		fmt.Printf("  ✗ 归属 %d 种, want 2——两个同名 agent 的历史合并了\n", len(byOwner))
		ok = false
	}
	for _, want := range []string{"optimist", "skeptic"} {
		if byOwner[want] == 0 {
			fmt.Printf("  ✗ 归属里没有 %q——该用分支注册名\n", want)
			ok = false
		}
	}
	if byOwner["analyst"] > 0 {
		fmt.Println("  ✗ 归属里出现了 agent 名 analyst——两个 agent 同名，用它做归属会合并")
		ok = false
	}
	if !displays["analyst"] {
		fmt.Println("  ✗ 显示名不是 agent 名——跨归属改写会把槽位名塞给模型")
		ok = false
	}
	if ok {
		fmt.Println("  ✓ 归属按位置分开、显示名是 agent 名")
	}
	return ok
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// 场景 1c：真模型 + 嵌套编排，验位置路径的**层级拼接**。
//
// 12 个 slot push 点里，单测覆盖了机制、场景 1b 用真模型验了 Parallel 一层。这条补的是
// **嵌套**：Sequential 的一步是个 Parallel，最内层 agent 的归属该是 `步骤名/分支名` 而
// 不是只有一段——路径拼错（丢父段 / 重复拼）在单层场景下根本看不出来。
func scenarioRealModelNestedOrchestration() bool {
	fmt.Println("\n┌─ 真模型嵌套编排：归属拼成层级路径 ─")
	if os.Getenv("QWEN_API_KEY") == "" {
		fmt.Println("  跳过：QWEN_API_KEY 未设置")
		return true
	}

	hs := agent_v2.NewInMemoryHistoryStore()
	mk := func(sys string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("worker"), // 三个 agent 全同名，只可能靠位置区分
			agent_v2.WithSystemPrompt(sys),
			agent_v2.WithModel(newModel()),
			agent_v2.WithHistory(hs, "tenant-N"),
			agent_v2.WithMaxRounds(3),
		)
	}
	inner := agent_v2.NewParallelAgent(agent_v2.WithParallelName("pair")).
		AddBranch("left", mk("用一句话说出这件事的一个好处。")).
		AddBranch("right", mk("用一句话说出这件事的一个风险。"))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("flow")).
		AddStep("collect", inner).
		AddStep("summarize", mk("把收到的内容压缩成一句结论。"))

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-nested")
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("远程办公")); err != nil {
		fmt.Println("  ✗ Run 失败:", err)
		return false
	}

	msgs, err := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "tenant-N", ConvID: "conv-nested"})
	if err != nil {
		fmt.Println("  ✗ Load 失败:", err)
		return false
	}
	byOwner := map[string]int{}
	for _, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		byOwner[o]++
	}
	fmt.Printf("  归属分布: %v\n", byOwner)

	ok := true
	for _, want := range []string{"collect/left", "collect/right", "summarize"} {
		if byOwner[want] == 0 {
			fmt.Printf("  ✗ 归属里没有 %q——路径拼接可能丢了父段或层级错\n", want)
			ok = false
		}
	}
	if byOwner["worker"] > 0 {
		fmt.Println("  ✗ 归属里出现 agent 名 worker——三个 agent 同名，用它做归属会全部合并")
		ok = false
	}
	for _, bad := range []string{"left", "right"} {
		if byOwner[bad] > 0 {
			fmt.Printf("  ✗ 归属 %q 缺父段——嵌套时该拼成 collect/%s\n", bad, bad)
			ok = false
		}
	}
	if ok {
		fmt.Println("  ✓ 嵌套路径正确拼接（collect/left、collect/right、summarize）")
	}
	return ok
}
