// channel_smoke 是 compose_v2 channel-based PoC 的端到端冒烟测试——验证 PoC
// 在真业务场景下完整可用。对照 example/orchestration/nested_smoke（main 分支 production
// binary），本 binary 跑同款"嵌套 HITL 闭环"场景但用 compose_v2 模型。
//
// **目的**（Phase 4.1）：
// PoC 已通过 116 单测，但单测之间相互隔离——本 binary 把多个能力（cp marshal
// + Resume + HITL + 嵌套子图 + per-branch delta + hooks 集成）**串联**在 1 个
// 真实 demo 里跑，证明这些能力**组合**起来仍然正确。
//
// 不依赖真 LLM——mock 函数按调用次数返回脚本响应：
//   - 第 1 次调：抛 ask_user InterruptError 触发挂起
//   - 第 2 次调（Resume 后）：返回最终答复
//
// 跑法：
//
//	go run ./example/orchestration/channel_smoke
//
// 全部场景跑通 + 退出码 0 表示冒烟通过。
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

func init() {
	// 注册 cp marshal 用到的 channel value 类型。
	// PlanAgent 内部 channel 类型（*planExecStep / []ExecutedStep）由 agent_v2 自身
	// init 注册——业务用高层 NewPlanAgent 不需手动注册 plan 内部类型。
	compose_v2.RegisterValueType[[]*schema.Message]("[]*schema.Message")
	compose_v2.RegisterValueType[*schema.Message]("*schema.Message")
	compose_v2.RegisterValueType[string]("string")
	compose_v2.RegisterValueType[int]("int")
	compose_v2.RegisterValueType[bool]("bool")
	compose_v2.RegisterValueType[[]string]("[]string")
}

func main() {
	scenarios := []struct {
		name string
		fn   func() error
	}{
		{"BasicAgent ReAct + ask_user + Resume", scenarioBasicAgentHITL},
		{"Sequential 嵌入 BasicAgent + 中间步 ask_user", scenarioSequentialNestedHITL},
		{"Plan 编排 + worker ask_user + Resume", scenarioPlanWorkerHITL},
		{"嵌套子图独立 cp + 跨进程 marshal", scenarioNestedSubgraphMarshal},
		{"Parallel fan-out per-branch delta + Resume failed only", scenarioPerBranchDelta},
	}

	failed := 0
	for i, s := range scenarios {
		fmt.Printf("\n=== [%d/%d] 场景：%s ===\n", i+1, len(scenarios), s.name)
		if err := s.fn(); err != nil {
			fmt.Printf("  ✗ FAIL: %v\n", err)
			failed++
		} else {
			fmt.Println("  ✓ PASS")
		}
	}

	fmt.Printf("\n=== 总结：%d/%d 通过 ===\n", len(scenarios)-failed, len(scenarios))
	if failed > 0 {
		os.Exit(1)
	}
}

// ── 场景 1: BasicAgent ReAct + ask_user + Resume ──

func scenarioBasicAgentHITL() error {
	llmRound := 0
	llm := func(_ context.Context, _, msgs []*schema.Message) (*schema.Message, error) {
		llmRound++
		if llmRound == 1 {
			// 第 1 轮：返 tool_call 触发 ask_user
			return schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "call_1", Function: schema.FunctionCall{Name: "ask_user", Arguments: `{"q":"确认?"}`}},
			}), nil
		}
		// 第 2+ 轮：用 user answer 给最终答复
		var answer string
		if len(msgs) > 0 {
			answer = msgs[len(msgs)-1].Content
		}
		return schema.AssistantMessage("user said: " + answer), nil
	}
	// 第 i 个调用用 callCtxs[i] 执行（OnToolStart(i) 采纳之后的 ctx，钩子按调用放的值只在这里）。
	tools := func(_ context.Context, tcs []schema.ToolCall, callCtxs []context.Context) ([]*schema.Message, error) {
		var out []*schema.Message
		for i, tc := range tcs {
			if tc.Function.Name == "ask_user" {
				// Ask 一步到位：有答案就返，没有就抛中断。身份由框架按执行位置派生
				// （同一 tools 节点里 N 个 tool_call 各自不同），业务不用也不能自己编。
				ans, err := compose_v2.Ask(callCtxs[i], &compose_v2.AskQuery{
					Reason:   "ask_user invoked",
					Question: "确认操作？",
				})
				if err != nil {
					return nil, err
				}
				out = append(out, schema.ToolResultMessage(tc.ID, "ask_user", ans.Content))
				continue
			}
		}
		return out, nil
	}

	makeGraph := func() *compose_v2.Graph {
		g, err := agent_v2.NewBasicAgentBuilder(llm, tools).Compile()
		if err != nil {
			panic(err)
		}
		return g
	}

	// Run 1: 触发 ask_user
	cm1 := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), makeGraph(), cm1,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentInputWrites(schema.UserMessage("hello"))...),
	)
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		return fmt.Errorf("Run 1 应中断，实际 %v", err)
	}
	fmt.Println("  · Run 1 触发 ask_user 中断 ✓")

	snap, _ := cm1.Snapshot()
	if len(snap.PendingInterrupts) != 1 {
		return fmt.Errorf("snapshot 应含 1 个 pending，实际 %d", len(snap.PendingInterrupts))
	}
	askID := snap.PendingInterrupts[0].InterruptID

	// Run 2: 注入 answer
	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(context.Background(), makeGraph(), cm2,
		compose_v2.WithResumeFrom(snap),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{askID: schema.UserMessage("approved")}),
	)
	if err != nil {
		return fmt.Errorf("Run 2: %v", err)
	}

	final := agent_v2.BasicAgentFinalMessage(cm2.Channels())
	if final == nil {
		return errors.New("Run 2 应有 final message")
	}
	fmt.Printf("  · Run 2 final: %q ✓\n", final.Content)
	return nil
}

// ── 场景 2: Sequential 嵌入 BasicAgent（用子图） + 中间步 ask_user ──

func scenarioSequentialNestedHITL() error {
	resultCh := compose_v2.LastValue[string]("seq_nested_result")

	step1 := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(resultCh, "step1_done")}, nil
	}
	step2WithAsk := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续 step3？"})
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(resultCh, "step2_with:"+ans.Content)}, nil
	}
	step3 := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(resultCh, "step3_done")}, nil
	}

	makeSeq := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder()
		compose_v2.WithChannel(b, resultCh)
		b.AddNode("s1", step1)
		b.AddNode("s2_ask", step2WithAsk)
		b.AddNode("s3", step3)
		b.AddEdge(compose_v2.StartNode, "s1")
		b.AddEdge("s1", "s2_ask")
		b.AddEdge("s2_ask", "s3")
		b.AddEdge("s3", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			panic(err)
		}
		return g
	}

	// Run 1: s1 → s2 抛中断
	cm1 := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), makeSeq(), cm1)
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		return fmt.Errorf("Run 1 应中断，实际 %v", err)
	}
	if v, _ := compose_v2.Get(cm1.Channels(), resultCh); v != "step1_done" {
		return fmt.Errorf("中断时 result 应是 step1_done，实际 %q", v)
	}
	fmt.Println("  · Run 1: s1 完成 + s2 ask_user 中断 ✓")

	snap, _ := cm1.Snapshot()
	if len(snap.CurrentFrontier) != 1 || snap.CurrentFrontier[0].NodeID != "s2_ask" {
		return fmt.Errorf("frontier 应是 [s2_ask]，实际 %v", snap.CurrentFrontier)
	}

	// Run 2: 注入 answer，s2 重跑续 s3
	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(context.Background(), makeSeq(), cm2,
		compose_v2.WithResumeFrom(snap),
		// **答案的 ID 从抛出来的那条中断上读**——身份由框架派生，业务这边不该有字面量
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("yes")}),
	)
	if err != nil {
		return fmt.Errorf("Run 2: %v", err)
	}
	if v, _ := compose_v2.Get(cm2.Channels(), resultCh); v != "step3_done" {
		return fmt.Errorf("最终 result 应是 step3_done，实际 %q", v)
	}
	fmt.Println("  · Run 2: s2 重跑 + s3 跑完 ✓")
	return nil
}

// ── 场景 3: Plan 编排（高层 PlanAgent）+ sub-agent ask_user + Resume ──
//
// 迁移自原低层 compose_v2.NewPlan/Worker（plan builder 已移到 agent_v2，
// compose_v2 不再有低层 plan builder）。语义保留：2 步 plan（auto / ask_worker），
// 第 2 步 sub-agent 抛 ask_user 中断，Resume 续跑。

func scenarioPlanWorkerHITL() error {
	// auto sub-agent：BasicAgent，model 直接返结果（无 tool 调用即最终答复）。
	autoAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("auto"),
		agent_v2.WithModel(&fnChatModel{fn: func(_ context.Context, _ []*schema.Message) (*schema.Message, error) {
			return schema.AssistantMessage("auto_result"), nil
		}}),
	)
	// ask_worker sub-agent：BasicAgent，首次抛 ask_user InterruptError，Resume 后取答案返。
	askAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("ask_worker"),
		agent_v2.WithModel(&fnChatModel{fn: func(ctx context.Context, _ []*schema.Message) (*schema.Message, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{
				Reason: "ask_worker needs confirmation", Question: "继续？",
			})
			if err != nil {
				return nil, err
			}
			return schema.AssistantMessage("answered:" + ans.Content), nil
		}}),
	)

	// 高层 Planner：第 1 步 auto，第 2 步 ask_worker，之后终止。
	planner := agent_v2.Planner(func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		switch len(executed) {
		case 0:
			return []agent_v2.PlanStep{{Description: "auto", SubAgentName: "auto"}}, "", nil
		case 1:
			return []agent_v2.PlanStep{{Description: "needs_ask", SubAgentName: "ask_worker"}}, "", nil
		default:
			return nil, "all done", nil
		}
	})

	makePlanAgent := func() *agent_v2.PlanAgent {
		return agent_v2.NewPlanAgent(planner, agent_v2.PlanName("plan_smoke")).
			AddSubAgent("auto", autoAgent).
			AddSubAgent("ask_worker", askAgent)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// Run 1: auto step 完成 → ask_worker sub-agent 抛 ask_user 中断
	_, err := makePlanAgent().Run(ctx, sess, schema.UserMessage("test"))
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		return fmt.Errorf("Run 1 应中断，实际 %v", err)
	}
	if ie.Query == nil {
		return fmt.Errorf("Run 1 中断应带 Query")
	}
	fmt.Println("  · Run 1: auto step 完成 + ask_worker 中断 ✓")

	// Run 2: 注入 answer，ask_worker 续跑 + plan 收尾
	events, err := makePlanAgent().Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("ok"),
		}),
		agent_v2.WithResumeSession(sess),
	)
	if err != nil {
		return fmt.Errorf("Run 2: %v", err)
	}

	final := agent_v2.FinalMessage(events)
	if final == nil {
		return fmt.Errorf("Run 2 应有 final message")
	}
	fmt.Printf("  · Run 2: ask_worker 续跑，最终 final=%q ✓\n", final.Content)
	return nil
}

// fnChatModel 是 closure-based model.ChatModel——让 sub-agent 用 ctx 检查 ResumeContext。
type fnChatModel struct {
	fn func(ctx context.Context, msgs []*schema.Message) (*schema.Message, error)
}

func (m *fnChatModel) Generate(ctx context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return m.fn(ctx, msgs)
}

func (m *fnChatModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *fnChatModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

// ── 场景 4: 嵌套子图独立 cp + 跨进程 marshal ──

func scenarioNestedSubgraphMarshal() error {
	subResultCh := compose_v2.LastValue[string]("nested_result")

	makeSub := func() *compose_v2.Graph {
		subB := compose_v2.NewBuilder().
			AddNode("inner_step", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "子图询问？"})
				if err != nil {
					return nil, err
				}
				return []compose_v2.ChannelWrite{compose_v2.Write(subResultCh, "inner:"+ans.Content)}, nil
			}).
			AddEdge(compose_v2.StartNode, "inner_step").
			AddEdge("inner_step", compose_v2.EndNode)
		subB = compose_v2.WithChannel(subB, subResultCh)
		sub, _ := subB.Compile()
		return sub
	}

	makeParent := func() *compose_v2.Graph {
		parentB := compose_v2.NewBuilder().
			AddSubgraphNode("sub_node", makeSub(), compose_v2.WithExport(subResultCh)).
			AddEdge(compose_v2.StartNode, "sub_node").
			AddEdge("sub_node", compose_v2.EndNode)
		parentB = compose_v2.WithChannel(parentB, subResultCh)
		parent, _ := parentB.Compile()
		return parent
	}

	// Phase 5.1.a Step 4：嵌套 cp 物理分离——业务必须注入 Checkpointer + sessionID
	cper := compose_v2.NewInMemoryCheckpointer()
	const sid = "smoke_nested_sid"

	cm1 := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), makeParent(), cm1,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sid),
	)
	subIE, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		return fmt.Errorf("应中断，实际 %v", err)
	}

	// 验证子 cp 写到 (sid, "sub_node") 物理分离 ns
	subCp, err := cper.LoadNS(context.Background(), sid, "sub_node")
	if err != nil {
		return fmt.Errorf("LoadNS sub_node: %w", err)
	}
	if subCp == nil {
		return fmt.Errorf("子图中断后子 cp 应写到 (sid, sub_node) ns，实际未找到")
	}
	if subCp.Status != compose_v2.CPStatusPending {
		return fmt.Errorf("子 cp Status 应 Pending，实际 %v", subCp.Status)
	}
	fmt.Printf("  · Run 1: 子图中断 + 子 cp 写到 (sid, sub_node) Pending 状态 ✓\n")
	fmt.Printf("  · 子 cp frontier: %v\n", subCp.CurrentFrontier)

	// Run 2: 同 cper 让 LoadNS 找回子 cp
	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(context.Background(), makeParent(), cm2,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{subIE.AllQueries()[0].InterruptID: schema.UserMessage("from_user")}),
	)
	if err != nil {
		return fmt.Errorf("Run 2: %w", err)
	}

	v := compose_v2.MustGet(cm2.Channels(), subResultCh)
	if v != "inner:from_user" {
		return fmt.Errorf("应是 'inner:from_user'，实际 %q", v)
	}

	// 验证子 cp 已被 DeleteSubtree 清掉（子图成功完成）
	subCp2, _ := cper.LoadNS(context.Background(), sid, "sub_node")
	if subCp2 != nil {
		return fmt.Errorf("子图成功完成后子 cp 应被清掉，实际仍存")
	}
	fmt.Printf("  · Run 2: 子图续跑 + 完成后清子 cp，result=%q ✓\n", v)
	return nil
}

// ── 场景 5: Parallel fan-out per-branch delta + Resume failed only ──

func scenarioPerBranchDelta() error {
	resultsCh := compose_v2.Append[string]("pbd_smoke_results")

	flakyCount := 0
	makePar := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder()
		compose_v2.WithChannel(b, resultsCh)

		// split 节点：超步 SendFrom 三个 worker 并发执行
		b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			compose_v2.SendFrom(ctx,
				compose_v2.SendDirective{Target: "ok1"},
				compose_v2.SendDirective{Target: "ok2"},
				compose_v2.SendDirective{Target: "flaky"},
			)
			return nil, nil
		})
		b.AddNode("ok1", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(resultsCh, []string{"ok1"})}, nil
		})
		b.AddNode("ok2", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(resultsCh, []string{"ok2"})}, nil
		})
		b.AddNode("flaky", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			flakyCount++
			if flakyCount == 1 {
				return nil, errors.New("transient")
			}
			return []compose_v2.ChannelWrite{compose_v2.Write(resultsCh, []string{"flaky_ok"})}, nil
		})
		b.AddEdge(compose_v2.StartNode, "split")
		b.AddEdge("ok1", compose_v2.EndNode)
		b.AddEdge("ok2", compose_v2.EndNode)
		b.AddEdge("flaky", compose_v2.EndNode)
		g, _ := b.Compile()
		return g
	}

	cm1 := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), makePar(), cm1)
	if err == nil {
		return errors.New("Run 1 应失败（flaky 第 1 次）")
	}

	res1, _ := compose_v2.Get(cm1.Channels(), resultsCh)
	if len(res1) != 2 {
		return fmt.Errorf("Run 1 应保留 2 个成功 worker writes（per-branch delta），实际 %v", res1)
	}
	fmt.Printf("  · Run 1: ok1/ok2 writes 保留 (per-branch delta) + flaky 失败标记 ✓\n")

	snap, _ := cm1.Snapshot()
	if len(snap.CurrentFrontier) != 1 || snap.CurrentFrontier[0].NodeID != "flaky" {
		return fmt.Errorf("frontier 应只含 flaky，实际 %v", snap.CurrentFrontier)
	}

	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(context.Background(), makePar(), cm2,
		compose_v2.WithResumeFrom(snap),
	)
	if err != nil {
		return fmt.Errorf("Run 2: %v", err)
	}

	res2 := compose_v2.MustGet(cm2.Channels(), resultsCh)
	if len(res2) != 3 {
		return fmt.Errorf("Resume 后应共 3 个 results（ok1/ok2 不重跑 + flaky 恢复），实际 %v", res2)
	}
	fmt.Printf("  · Run 2: 只 flaky 重跑（ok1/ok2 不动），final=%v ✓\n", res2)
	return nil
}

// suppress unused import warning
var _ = log.Print
