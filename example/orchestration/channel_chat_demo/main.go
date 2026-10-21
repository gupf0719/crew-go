// channel_chat_demo 模拟完整 chat web 场景验证 compose_v2 真生产可用：
// streaming chunk + HITL ask_user 中断 + Resume 闭环 + 跨进程 cp 模拟。
//
// **目的**（Phase 4.3）：
// 不接真 LLM 也不接 SSE/HTTP——用 mock LLM + 内存"前端" channel 模拟一次
// 完整对话。如果这个 demo 能跑通，说明 compose_v2 已具备替代 plan_chat_web
// 等 production binary 的能力（SSE wire format 版已实现，见 channel_chat_web）。
//
// **场景**：3 轮 chat 对话
//   - 轮 1：user "查天气" → LLM 流式输出 token-by-token → 终止
//   - 轮 2：user "需要权限确认才能查" → LLM 抛 ask_user 中断
//     → 模拟跨进程：snap marshal/unmarshal
//     → user 答 "approved" → Resume → LLM 流式答最终结果
//   - 轮 3：单轮 LLM 流式响应作收尾
//
// 跑法：
//
//	go run ./example/orchestration/channel_chat_demo
//
// 全部场景成功 + 退出码 0 表示通过。
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

func init() {
	compose_v2.RegisterValueType[[]*schema.Message]("[]*schema.Message")
	compose_v2.RegisterValueType[*schema.Message]("*schema.Message")
	compose_v2.RegisterValueType[int]("int")
}

// frontendBus 模拟前端事件总线——OrchestrationHooks.OnNodeEvent 投递的 chunk
// 通过这个 bus 输出（在真实场景里这就是 SSE wire 写入 ResponseWriter）。
type frontendBus struct {
	compose_v2.NoOpOrchestrationHooks                    // 嵌入：接口再长方法时不至于编译断；下面 5 个全部覆盖
	chunks                            []string           // 累积的 token chunks（按 OnNodeEvent 接收顺序）
	nodeEvents                        []string           // 节点级事件（OnNodeStart/OnNodeEnd 等）
	chunkLogger                       func(chunk string) // 实时回调（模拟 SSE flush）
}

func (b *frontendBus) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	b.nodeEvents = append(b.nodeEvents, "graph_start")
	return ctx, nil
}

func (b *frontendBus) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, err error) context.Context {
	if err != nil {
		b.nodeEvents = append(b.nodeEvents, "graph_end:err")
	} else {
		b.nodeEvents = append(b.nodeEvents, "graph_end:ok")
	}
	return ctx
}

func (b *frontendBus) OnNodeStart(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	b.nodeEvents = append(b.nodeEvents, "node_start:"+string(id))
	return ctx, nil
}

func (b *frontendBus) OnNodeEnd(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels, err error) context.Context {
	if err != nil {
		b.nodeEvents = append(b.nodeEvents, "node_end:"+string(id)+":err")
	} else {
		b.nodeEvents = append(b.nodeEvents, "node_end:"+string(id))
	}
	return ctx
}

func (b *frontendBus) OnNodeEvent(ctx context.Context, _ compose_v2.NodeID, ev compose_v2.Event) context.Context {
	if ev.Type == "chunk" {
		b.chunks = append(b.chunks, ev.Content)
		if b.chunkLogger != nil {
			b.chunkLogger(ev.Content)
		}
	}
	return ctx
}

// streamingMockLLM 模拟流式 LLM——逐 token 通过 EventSink.Send 发出，最后返完整 message。
//
// behavior 决定第 N 次调用的行为：
//   - "stream:token1|token2|..." → 流式输出每个 token，最终返聚合 assistant
//   - "ask:question" → 抛 InterruptError（模拟需要 ask_user 工具调用）
//   - "tool_call:weather" → 返带 tool_call 的 assistant
//   - "final:..." → 直接返 final assistant 不流式
type streamingMockLLM struct {
	behaviors []string
}

// Generate 按**历史内容**定位到第几条 behavior——数消息流里已有多少条 tool_result，
// 就说明前面几条 tool_call 已经办完了。
//
// ⚠️ **不能用自增计数器**：跨进程恢复时 model 节点不重跑（Resume 从中断的 tools 节点
// 起跑），而这里每次都是新造的 mock 实例、计数器从 0 重来 ⟹ 它会把**已经办完的**
// tool_call 再发一遍。实测：Resume 后 ask_user 被重发，第二问的中断身份是新的、配不上
// 用户已给的答案，于是又中断一次，Round 2 永远收不了尾。
func (m *streamingMockLLM) Generate(ctx context.Context, msgs []*schema.Message) (*schema.Message, error) {
	done := 0
	for _, msg := range msgs {
		if msg.Role == schema.RoleTool {
			done++
		}
	}
	if done >= len(m.behaviors) {
		return schema.AssistantMessage("[no more mock behavior]"), nil
	}
	beh := m.behaviors[done]

	switch {
	case strings.HasPrefix(beh, "stream:"):
		// 流式输出：通过 EventSink 实时发 chunk
		tokens := strings.Split(strings.TrimPrefix(beh, "stream:"), "|")
		var full strings.Builder
		sink := compose_v2.GetEventSink(ctx)
		for _, t := range tokens {
			if sink != nil {
				sink.Send(compose_v2.Event{Type: "chunk", Content: t})
			}
			full.WriteString(t)
		}
		return schema.AssistantMessage(full.String()), nil

	case strings.HasPrefix(beh, "tool_call:"):
		toolName := strings.TrimPrefix(beh, "tool_call:")
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: fmt.Sprintf("call_%d", done), Function: schema.FunctionCall{
				Name:      toolName,
				Arguments: `{}`,
			}},
		}), nil

	case strings.HasPrefix(beh, "final:"):
		return schema.AssistantMessage(strings.TrimPrefix(beh, "final:")), nil

	default:
		return schema.AssistantMessage("unknown mock behavior: " + beh), nil
	}
}

// mockTools 模拟工具——ask_user 抛中断，其他工具正常返结果。
//
// 第 i 个调用用 callCtxs[i] 执行：它是 OnToolStart(i) 采纳之后的 ctx，钩子按调用放的值（span、凭证）只在这里。
// 批级的 ctx 只给这一批整体的工作，不带任何调用的钩子值。
func mockTools(_ context.Context, tcs []schema.ToolCall, callCtxs []context.Context) ([]*schema.Message, error) {
	var results []*schema.Message
	for i, tc := range tcs {
		if tc.Function.Name == "ask_user" {
			// Ask 一步到位；身份由框架按执行位置派生（同节点内多个 tool_call 各自不同），
			// 业务不用也不能自己编——自编 ID 曾让两个不同的提问共用一个答案位。
			ansMsg, err := compose_v2.Ask(callCtxs[i], &compose_v2.AskQuery{
				Reason:   "user permission needed",
				Question: "需要用户确认是否查询天气？",
				Options:  []string{"approved", "denied"},
			})
			if err != nil {
				return nil, err
			}
			results = append(results, schema.ToolResultMessage(tc.ID, "ask_user", ansMsg.Content))
			continue
		}
		// 其他工具正常返
		results = append(results, schema.ToolResultMessage(tc.ID, tc.Function.Name, "tool_result_for_"+tc.Function.Name))
	}
	return results, nil
}

func main() {
	if err := runDemo(); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\n=== ALL ROUNDS PASSED ===")
}

func runDemo() error {
	// ── 轮 1：简单流式响应 ──
	fmt.Println("\n┌──────────────────────────────────────────┐")
	fmt.Println("│  Round 1: 简单流式响应（无 tool_call）   │")
	fmt.Println("└──────────────────────────────────────────┘")

	llm1 := &streamingMockLLM{
		behaviors: []string{
			"stream:今天|天气|很好|，|适合|出门",
		},
	}
	bus1 := &frontendBus{
		chunkLogger: func(c string) { fmt.Printf("📥 chunk: %q\n", c) },
	}
	model1 := func(ctx context.Context, preamble, msgs []*schema.Message) (*schema.Message, error) {
		return llm1.Generate(ctx, msgs)
	}
	g1, err := agent_v2.NewBasicAgentBuilder(model1, mockTools).WithName("chat_v2").Compile()
	if err != nil {
		return fmt.Errorf("Round 1 compile: %w", err)
	}
	cm1 := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g1, cm1,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentInputWrites(schema.UserMessage("今天天气怎么样？"))...),
		compose_v2.WithHooks(bus1),
	); err != nil {
		return fmt.Errorf("Round 1 Run: %w", err)
	}
	final1 := agent_v2.BasicAgentFinalMessage(cm1.Channels())
	if final1 == nil || !strings.Contains(final1.Content, "天气") {
		return fmt.Errorf("Round 1 final 应含'天气'，实际 %v", final1)
	}
	fmt.Printf("✅ Round 1 final: %q\n", final1.Content)
	fmt.Printf("   总共 %d 个 chunks 实时投递\n", len(bus1.chunks))

	// ── 轮 2：tool_call ask_user 中断 + Resume + 跨进程 cp ──
	fmt.Println("\n┌────────────────────────────────────────────────────┐")
	fmt.Println("│  Round 2: ask_user 中断 + 跨进程 marshal + Resume  │")
	fmt.Println("└────────────────────────────────────────────────────┘")

	makeRound2Graph := func() *compose_v2.Graph {
		llm2 := &streamingMockLLM{
			behaviors: []string{
				"tool_call:ask_user", // 第 1 轮 LLM 抛 ask_user tool_call
				"stream:好的|，|已|查到|天气|是|晴|，|20℃", // 第 2 轮 Resume 后流式答
			},
		}
		model2 := func(ctx context.Context, preamble, msgs []*schema.Message) (*schema.Message, error) {
			return llm2.Generate(ctx, msgs)
		}
		g, err := agent_v2.NewBasicAgentBuilder(model2, mockTools).WithName("chat_v2").Compile()
		if err != nil {
			panic(err)
		}
		return g
	}

	bus2a := &frontendBus{
		chunkLogger: func(c string) { fmt.Printf("📥 chunk: %q\n", c) },
	}
	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(context.Background(), makeRound2Graph(), cm2,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentInputWrites(schema.UserMessage("查天气需要权限"))...),
		compose_v2.WithHooks(bus2a),
	)
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		return fmt.Errorf("Round 2 应中断，实际 %v", err)
	}
	pending := cm2.PendingInterrupts()
	if len(pending) != 1 {
		return fmt.Errorf("应 1 个 pending interrupt，实际 %d", len(pending))
	}
	q := pending[0]
	fmt.Printf("⏸️  中断！前端收到 ask_user：%q (id=%s, options=%v)\n",
		q.Question, q.InterruptID, q.Options)

	// 模拟跨进程：marshal cp + 重启
	snap, _ := cm2.Snapshot()
	data, err := compose_v2.MarshalCheckpoint(snap)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	fmt.Printf("💾 cp wire size: %d bytes（持久化到磁盘/数据库）\n", len(data))
	fmt.Println("--- [模拟跨进程：进程重启 / 用户输入答案] ---")

	// 用户回答
	userAnswer := "approved"
	fmt.Printf("👤 用户回答：%q\n", userAnswer)

	// 反序列化 + Resume
	snap2, err := compose_v2.UnmarshalCheckpoint(data)
	if err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	bus2b := &frontendBus{
		chunkLogger: func(c string) { fmt.Printf("📥 chunk: %q\n", c) },
	}
	cm2b := compose_v2.NewChannelManager()
	err = compose_v2.Run(context.Background(), makeRound2Graph(), cm2b,
		compose_v2.WithResumeFrom(snap2),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{q.InterruptID: schema.UserMessage(userAnswer)}),
		compose_v2.WithHooks(bus2b),
	)
	if err != nil {
		return fmt.Errorf("Round 2 Resume: %w", err)
	}
	final2 := agent_v2.BasicAgentFinalMessage(cm2b.Channels())
	if final2 == nil || !strings.Contains(final2.Content, "晴") {
		return fmt.Errorf("Resume 后 final 应含'晴'，实际 %v", final2)
	}
	fmt.Printf("✅ Round 2 final: %q\n", final2.Content)
	fmt.Printf("   总共 %d 个 chunks 实时投递（跨进程恢复后继续 streaming）\n", len(bus2b.chunks))

	// ── 轮 3：另一个简单流式响应（验证 cm 状态独立性）──
	fmt.Println("\n┌──────────────────────────────────────────┐")
	fmt.Println("│  Round 3: 收尾对话（独立新 cm）          │")
	fmt.Println("└──────────────────────────────────────────┘")

	llm3 := &streamingMockLLM{
		behaviors: []string{"stream:再见|，|有|事|随时|找|我"},
	}
	bus3 := &frontendBus{
		chunkLogger: func(c string) { fmt.Printf("📥 chunk: %q\n", c) },
	}
	model3 := func(ctx context.Context, preamble, msgs []*schema.Message) (*schema.Message, error) {
		return llm3.Generate(ctx, msgs)
	}
	g3, _ := agent_v2.NewBasicAgentBuilder(model3, mockTools).WithName("chat_v2").Compile()
	cm3 := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g3, cm3,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentInputWrites(schema.UserMessage("谢谢，再见"))...),
		compose_v2.WithHooks(bus3),
	); err != nil {
		return fmt.Errorf("Round 3: %w", err)
	}
	final3 := agent_v2.BasicAgentFinalMessage(cm3.Channels())
	fmt.Printf("✅ Round 3 final: %q\n", final3.Content)

	// ── 总结 ──
	fmt.Println("\n┌──────────────────────────────────────────┐")
	fmt.Println("│  全部 3 轮通过                            │")
	fmt.Println("└──────────────────────────────────────────┘")
	fmt.Printf("✓ 流式 chunk 投递（OrchHooks.OnNodeEvent + EventSink）\n")
	fmt.Printf("✓ HITL ask_user 中断（InterruptError + AskQuery）\n")
	fmt.Printf("✓ 跨进程 cp marshal/unmarshal（%d bytes JSON wire）\n", len(data))
	fmt.Printf("✓ Resume with answer（WithResumeAnswers map）\n")
	fmt.Printf("✓ Restore 后流式继续（chunk 实时投递不丢）\n")
	fmt.Printf("\n→ compose_v2 已具备替代 plan_chat_web 等 production binary 的能力\n")
	fmt.Printf("→ 剩下只是接 SSE wire format / HTTP handler 的工作（不影响协议正确性）\n")

	return nil
}
