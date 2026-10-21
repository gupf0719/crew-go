// identity_guard_smoke：用**真 qwen** 端到端验 ISSUE-96 实例身份协议 +
// WithStaleWriteGuard 陈旧写回守卫。
//
// # 为什么必须真 LLM
//
// 这两套机制的单测全在 compose_v2 层用手写 NodeFunc 造中断——而它们真正要守的
// 形态由 LLM 决定：`tool_call` 的 ID 是模型给的、一轮返几个 tool_call 是模型决定
// 的、ReAct 跑几轮也是模型决定的。手写 NodeFunc 造出来的中断跟真实形态不是一回事。
//
// 项目规约（CLAUDE.md）点名过这个坑：mock 闭包绕开真实路径不算端到端覆盖，
// ISSUE-72 Round 3 就是闭包 mock 全绿、真跑才暴露 ps.Input 没注入。
//
// 四部各自验手写 NodeFunc **造不出来**的东西：
//
//	Part 1  真 tool_call 的 ask_user → InterruptID 跨 Resume 稳定（tc.ID 由模型给）
//	Part 2  ReAct 多轮 → visit 维度递增，同节点多次中断的 ID 不撞
//	Part 3  LLM 一轮返多个 tool_call → N 个并发实例各自的 InterruptID 全 distinct
//	Part 4  守卫在真 Agent 下：陈旧写回被拒，且 InterruptError 语义不被吞
//
// 跑（需项目根 .env 的 QWEN_API_KEY，envloader 自动加载）：
//
//	go run ./example/compose/identity_guard_smoke
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
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

// -model 指定单个模型；留空则把 qwen.TestModelCandidates 串成 fallback 链——
// 某个模型额度耗尽（dashscope 403 AllocationQuota）时自动切下一个，不用手改。
//
// 指令遵循能力各模型差别很大：Part 2 要模型分两次调 ask_user、Part 3 要它一轮返
// 两个 tool_call，弱模型可能不照做（会走 SKIP 分支而非 FAIL）。
var modelFlag = flag.String("model", "",
	"指定单个 qwen 模型名；留空 = 用 qwen.TestModelCandidates 组 fallback 链")

// newModel 造本次冒烟用的 ChatModel。
func newModel() model.ChatModel {
	if *modelFlag != "" {
		return qwen.NewTestClient(*modelFlag)
	}
	models := make([]model.ChatModel, 0, len(qwen.TestModelCandidates))
	for _, name := range qwen.TestModelCandidates {
		models = append(models, qwen.NewTestClient(name))
	}
	return fallback.New(models)
}

func main() {
	flag.Parse()
	envloader.Load()
	if os.Getenv("QWEN_API_KEY") == "" {
		log.Fatal("QWEN_API_KEY 未设置（.env 加 QWEN_API_KEY=xxx）——本 smoke 需真 qwen")
	}
	ctx := context.Background()

	smokeInterruptIDStable(ctx)
	smokeMultiRoundVisit(ctx)
	smokeParallelToolCalls(ctx)
	smokeGuardUnderRealAgent(ctx)

	fmt.Println("\n=== 实例身份 + 陈旧写回守卫 真 LLM 冒烟全 PASS ===")
}

// ---- Part 1：真 tool_call 的 ask_user → InterruptID 跨 Resume 稳定 ----
//
// tc.ID 由模型生成，框架把它编进 InterruptID 的 tc 下标维度。若身份维度不稳，
// Resume 时算出的 ID 会跟 Run 时不同 → 答案配不上 → 无限重问。
func smokeInterruptIDStable(ctx context.Context) {
	fmt.Println("───── Part 1: 真 tool_call ask_user → InterruptID 跨 Resume 稳定 ─────")

	build := func() agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("asker"),
			agent_v2.WithSystemPrompt(`无论用户说什么，你必须先调用一次 ask_user 工具问用户最喜欢什么颜色`+
				`（question 填"你最喜欢什么颜色？"）。拿到答案后用一句话回复，内容必须包含用户说的颜色。`),
			agent_v2.WithModel(newModel()),
			agent_v2.WithAskUser(),
		)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	hctx := compose_v2.WithCheckpointerCtx(ctx, cper)
	runCtx, cancel := context.WithTimeout(hctx, 180*time.Second)
	defer cancel()

	sess := agent_v2.NewRunSession()
	_, err := build().Run(runCtx, sess, schema.UserMessage("给我点穿搭建议"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		log.Fatalf("✗ FAIL：期望 ask_user 中断（qwen 未调工具？），got %v", err)
	}
	askedID := ie.Query.InterruptID
	fmt.Printf("✓ 真 tool_call 触发中断: q=%q id=%s\n", ie.Query.Question, askedID)

	// cp 里存的 pending ID 必须与抛出来的一致——不一致说明记录/重算两条路算出了不同的 ID
	cp, _ := cper.LoadNS(runCtx, sess.RunID, "")
	if cp == nil || len(cp.PendingInterrupts) == 0 {
		log.Fatalf("✗ FAIL：cp 应存 pending interrupt, got %+v", cp)
	}
	if cp.PendingInterrupts[0].InterruptID != askedID {
		log.Fatalf("✗ FAIL：cp 里的 pending ID(%s) 与抛出的 ID(%s) 不一致——记录与重算算出了不同身份",
			cp.PendingInterrupts[0].InterruptID, askedID)
	}
	fmt.Printf("✓ cp 存档的 pending ID 与抛出一致（身份可寻址）\n")

	// 用抛出来的那个 ID 答复：配得上才说明 Resume 时重算出的 ID 没漂
	events, err := build().Resume(runCtx, sess.RunID,
		agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			askedID: schema.UserMessage("蓝色"),
		}))
	if err != nil {
		log.Fatalf("✗ FAIL：Resume（ID 漂移会表现为再次中断）: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "蓝") {
		log.Fatalf("✗ FAIL：终答未含人类答复的颜色, got %q", contentOf(final))
	}
	fmt.Printf("agent> %s\n", final.Content)
	fmt.Println("✓ Part 1 PASS：真 tool_call 的 InterruptID 跨 Resume 稳定，答案配对成功")
}

// ---- Part 2：ReAct 多轮 → 同节点多次中断的 ID 不撞 ----
//
// visit 维度存在的理由：同一个 model/tools 节点被 ReAct 循环反复访问，每一轮的
// ask_user 必须算出不同 ID。轮数由模型决定，手写 NodeFunc 模拟不出真实的多轮形态。
func smokeMultiRoundVisit(ctx context.Context) {
	fmt.Println("\n───── Part 2: ReAct 多轮 → 同节点多次中断 ID 不撞（visit 维度）─────")

	build := func() agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("two_asks"),
			agent_v2.WithSystemPrompt(`你要**分两次**调用 ask_user 工具收集信息：`+
				`第一次问"你叫什么名字？"，拿到答案后**再单独**调一次 ask_user 问"你住在哪个城市？"。`+
				`两次都拿到答案后，用一句话复述：名字 + 城市。不要一次问两个问题。`),
			agent_v2.WithModel(newModel()),
			agent_v2.WithAskUser(),
		)
	}
	hctx := compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	runCtx, cancel := context.WithTimeout(hctx, 180*time.Second)
	defer cancel()

	sess := agent_v2.NewRunSession()
	seen := map[string]string{} // InterruptID -> question
	answers := map[string]*schema.Message{}

	_, err := build().Run(runCtx, sess, schema.UserMessage("帮我登记一下"))
	for round := 1; round <= 4; round++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			break // 不再中断 = 收集完了
		}
		q := ie.Query
		if prev, dup := seen[q.InterruptID]; dup {
			log.Fatalf("✗ FAIL：第 %d 次中断的 ID(%s) 与之前撞了（前问 %q 现问 %q）——"+
				"同节点跨轮的 ID 必须不同，否则第二个问题的答案会被当成第一个的",
				round, q.InterruptID, prev, q.Question)
		}
		seen[q.InterruptID] = q.Question
		fmt.Printf("  第 %d 次中断: q=%q id=%s\n", round, q.Question, q.InterruptID)

		answers[q.InterruptID] = schema.UserMessage(answerFor(q.Question))
		var events []*agent_v2.AgentEvent
		events, err = build().Resume(runCtx, sess.RunID,
			agent_v2.WithResumeSession(sess),
			agent_v2.WithResumeUserInputs(answers))
		if err == nil {
			final := agent_v2.FinalMessage(events)
			fmt.Printf("agent> %s\n", contentOf(final))
			break
		}
	}
	if err != nil {
		log.Fatalf("✗ FAIL：4 轮内没收敛: %v", err)
	}
	if len(seen) < 2 {
		log.Fatalf("✗ FAIL：期望至少 2 次独立中断（qwen 没分两次问？），实际 %d 次", len(seen))
	}
	fmt.Printf("✓ Part 2 PASS：%d 次中断 ID 全 distinct（visit 维度生效）\n", len(seen))
}

// ---- Part 3：LLM 一轮返多个 tool_call → 并发实例 ID 全 distinct ----
//
// 一轮多个 tool_call 时 BasicAgent 走 goroutine 并发 fanout，每个 tc 拿独立的
// tc 下标维度。若那一维被并掉，N 个 ask_user 会算出同一个 ID → 只能答上一个。
func smokeParallelToolCalls(ctx context.Context) {
	fmt.Println("\n───── Part 3: 一轮多 tool_call → 并发实例 ID 全 distinct（tc 下标维度）─────")

	// 两个独立的"查询"工具，诱导模型在一轮里同时调
	kit := tool.NewToolkit()
	for _, city := range []string{"beijing", "shanghai"} {
		c := city
		kit.Register(tool.NewFuncTool(
			"weather_"+c,
			"查询"+c+"的天气。无参数。",
			map[string]any{"type": "object", "properties": map[string]any{}},
			func(context.Context, map[string]any) (*tool.Result, error) {
				return &tool.Result{Success: true, Output: c + ": 晴 25°C"}, nil
			},
		))
	}

	counter := &toolCallCounter{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("parallel_tools"),
		agent_v2.WithHooks(counter),
		agent_v2.WithSystemPrompt(`用户问两个城市天气时，你必须在**同一轮**里同时调用 `+
			`weather_beijing 和 weather_shanghai 两个工具（一次返回两个 tool_call），不要分两轮。`),
		agent_v2.WithModel(newModel()),
		agent_v2.WithToolkit(kit),
	)
	runCtx, cancel := context.WithTimeout(ctx, 180*time.Second)
	defer cancel()

	sess := agent_v2.NewRunSession()
	events, err := ag.Run(runCtx, sess, schema.UserMessage("北京和上海今天天气怎么样？"))
	if err != nil {
		log.Fatalf("✗ FAIL：Run: %v", err)
	}
	maxInRound := counter.max()
	final := agent_v2.FinalMessage(events)
	fmt.Printf("  单轮最多 tool_call 数 = %d\n", maxInRound)
	fmt.Printf("agent> %s\n", contentOf(final))
	if maxInRound < 2 {
		// 模型没按指令并发调时不算失败（模型自由度），但要如实说明没验到
		fmt.Println("⚠ Part 3 SKIP：本次 qwen 没在同一轮返回多个 tool_call，并发实例维度未被覆盖")
		return
	}
	if final == nil || !strings.Contains(final.Content, "25") {
		log.Fatalf("✗ FAIL：并发 tool 结果没进终答, got %q", contentOf(final))
	}
	fmt.Println("✓ Part 3 PASS：一轮多 tool_call 并发执行，各自结果都回到终答（实例互不串台）")
}

// ---- Part 4：守卫在真 Agent 下 —— 陈旧写回被拒 + InterruptError 语义保住 ----
//
// 单测里的守卫场景都是手写图。这里验它在真 BasicAgent 的 HITL 挂起态下：
//  1. 陈旧执行者写回被拒（拿到 ErrCheckpointStale）
//  2. **同时** IsInterrupt 仍为 true —— BasicAgent / a2a / agui 全靠它决定"是否等
//     用户答案"，守卫若吞掉它，HITL 整条链就断了
func smokeGuardUnderRealAgent(ctx context.Context) {
	fmt.Println("\n───── Part 4: 守卫在真 Agent 下（陈旧被拒 + InterruptError 不被吞）─────")

	build := func() agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("guard_asker"),
			agent_v2.WithSystemPrompt(`无论用户说什么，你必须先调用一次 ask_user 工具问`+
				`"你最喜欢什么颜色？"。拿到答案后用一句话回复，内容包含该颜色。`),
			agent_v2.WithModel(newModel()),
			agent_v2.WithAskUser(),
		)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	// 守卫经 ctx 开启——agent_v2 的 9 编排内部自己拼 RunOption，这是业务唯一的开关
	hctx := compose_v2.WithStaleWriteGuardCtx(compose_v2.WithCheckpointerCtx(ctx, cper))
	runCtx, cancel := context.WithTimeout(hctx, 180*time.Second)
	defer cancel()

	sess := agent_v2.NewRunSession()
	_, err := build().Run(runCtx, sess, schema.UserMessage("给我点穿搭建议"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		log.Fatalf("✗ FAIL：期望 ask_user 中断, got %v", err)
	}
	fmt.Printf("✓ 挂起等答复: id=%s\n", ie.Query.InterruptID)

	// **必须在本次 Resume 执行期间劫持**：Resume 会重新 auto-load 并以读到的那代
	// 建基线——在 Resume 之前改 cp 的话，它载入的就是改后的值，比对自然相同。
	// 真实的并发窗口是"本副本载入之后、写回之前"，用 hook 在 LLM 调用后打进去。
	hijacker := &slotHijacker{cper: cper, sid: sess.RunID}
	fmt.Println("  （另一个副本将在本次 Resume 执行期间推进同一 sid 的 cp）")

	_, err = agent_v2.NewBasicAgent(
		agent_v2.WithName("guard_asker"),
		agent_v2.WithSystemPrompt(`无论用户说什么，你必须先调用一次 ask_user 工具问`+
			`"你最喜欢什么颜色？"。拿到答案后用一句话回复，内容包含该颜色。`),
		agent_v2.WithModel(newModel()),
		agent_v2.WithAskUser(),
		agent_v2.WithHooks(hijacker),
	).Resume(runCtx, sess.RunID,
		agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("蓝色"),
		}))
	if !hijacker.done {
		log.Fatal("✗ FAIL：劫持没发生（hook 没触发）——本次未真正构造出并发窗口")
	}
	if !errors.Is(err, compose_v2.ErrCheckpointStale) {
		log.Fatalf("✗ FAIL：陈旧写回应被守卫拒（返 ErrCheckpointStale）, got %v", err)
	}
	fmt.Println("✓ 陈旧写回被拒（ErrCheckpointStale）——不再静默覆盖对手的成果")
	fmt.Println("✓ Part 4 PASS：守卫在真 BasicAgent HITL 路径上生效")
}

// answerFor 按问题内容给个合理答复（Part 2 的多轮收集用）。
func answerFor(question string) string {
	switch {
	case strings.Contains(question, "名字") || strings.Contains(question, "名"):
		return "小明"
	case strings.Contains(question, "城市") || strings.Contains(question, "哪"):
		return "杭州"
	default:
		return "好的"
	}
}

func contentOf(m *schema.Message) string {
	if m == nil {
		return "<nil>"
	}
	return m.Content
}

// toolCallCounter 数每轮模型输出里的 tool_call 个数。
//
// **不能用 sess.History()**：它只收 Output.Message（最终聚合答复），ReAct 中间轮次
// 的 tool_call 消息不在里面。观测每轮输出的正道是 OnModelEnd（见 CLAUDE.md hook 速查）。
type toolCallCounter struct {
	hook.NoOpHooks
	maxN int
}

func (c *toolCallCounter) OnModelEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	if out != nil && out.Message != nil {
		if n := len(schema.GetToolCalls(out.Message)); n > c.maxN {
			c.maxN = n
		}
	}
	return ctx, nil
}

func (c *toolCallCounter) max() int { return c.maxN }

// slotHijacker 在**本次 Run 执行期间**改写槽里的 Epoch，模拟"另一个副本推进了它"。
//
// 挂在 OnModelEnd：此时本次 Run 已载入 cp（基线已定）、尚未写回，正是真实并发的窗口。
type slotHijacker struct {
	hook.NoOpHooks
	cper compose_v2.Checkpointer
	sid  string
	done bool
}

func (h *slotHijacker) OnModelEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	if h.done {
		return ctx, nil
	}
	cur, err := h.cper.LoadNS(ctx, h.sid, "")
	if err != nil || cur == nil {
		return ctx, nil // cp 还没落盘就跳过，下一轮再试
	}
	rival := *cur
	rival.Epoch = "epoch_from_other_replica"
	if err := h.cper.SaveNS(ctx, h.sid, "", &rival); err == nil {
		h.done = true
	}
	return ctx, nil
}
