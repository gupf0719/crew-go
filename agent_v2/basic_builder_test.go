package agent_v2_test

import (
	"context"
	"fmt"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// PoC §4.1 场景 A：BasicAgent ReAct 等价实现 + audit F2 实测对照。
//
// **目标**：验证 channel-based 模型实现 ReAct 循环（model ↔ tools 两节点 + 循环边）
// 的 API 复杂度——PoC 设计文档原估 "≤ 现状 60%" 但 audit F2 指出现状基线失实
// （buildBasicAgentGraph 14 行 + modelNodeFunc 137 行 + toolsNodeFunc 139 行 = 290 行
// 含 hook/skill/Resume/streaming）。本 PoC 不接 hook/skill/Resume/streaming，只验证
// channel 模型本身的 ReAct 表达力——对等比较留 Phase 1 完整迁移。
//
// **撞 audit F8 hook 集成路径未说明**：本实现**故意不接 hook**——验证 channel
// 模型基础 API 是否能干净表达 ReAct，hook 集成路径留 Phase 1 设计。

// ── Mock LLM ──

// mockLLM 模拟 ReAct LLM：第一轮返带 tool_call，第二轮返 final answer。
type mockLLM struct {
	round int
}

func (m *mockLLM) Generate(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
	m.round++
	if m.round == 1 {
		// 第一轮：根据 user query 决定调哪个 tool
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "call_1", Function: schema.FunctionCall{Name: "get_weather", Arguments: `{"city":"Beijing"}`}},
		}), nil
	}
	// 第二轮+：基于 tool_result 返 final answer
	return schema.AssistantMessage("北京今天 20℃ 晴。"), nil
}

// mockTool 模拟 tool 执行。
func mockTool(_ context.Context, name, _ string) (string, error) {
	switch name {
	case "get_weather":
		return `{"city":"Beijing","temp":20,"condition":"sunny"}`, nil
	default:
		return "", fmt.Errorf("unknown tool: %s", name)
	}
}

// ── BasicAgent ReAct 等价实现 ──

// 全局 channel 定义（业务一次声明，节点共享引用）
var basicAgentMessagesCh = compose_v2.Append[*schema.Message]("messages")

// buildBasicAgentReAct 构造 BasicAgent 等价的 ReAct graph。
//
// **行数实测**：本函数（含节点闭包）共约 50 行——但**不含** hook/skill/Resume/
// streaming/4 层压缩集成。完整对等版（含上述）需 Phase 1 评估，PoC 不强求。
//
// 对比现状（agent/basicagent_graph.go）：
//   - buildBasicAgentGraph: 14 行（builder 拼装）
//   - modelNodeFunc: 137 行（含 hook 触发 + skill 注入 + 流式聚合 + Resume 配对）
//   - toolsNodeFunc: 139 行（含 tool 执行 + hook + Resume + skill activation）
//   - 总 290 行
//
// PoC 版（不含 hook/skill/Resume/streaming）50 行，证明 channel 模型本身表达
// ReAct **核心循环**至少不比 frontier 模型重。
func buildBasicAgentReAct(llm *mockLLM) (*compose_v2.Graph, error) {
	modelNode := func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		msgs, _ := compose_v2.Get(chs, basicAgentMessagesCh)
		out, err := llm.Generate(context.Background(), msgs)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{
			compose_v2.Write(basicAgentMessagesCh, []*schema.Message{out}),
		}, nil
	}

	toolsNode := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		msgs, _ := compose_v2.Get(chs, basicAgentMessagesCh)
		if len(msgs) == 0 {
			return nil, fmt.Errorf("tools node: no messages")
		}
		last := msgs[len(msgs)-1]
		var results []*schema.Message
		for _, tc := range schema.GetToolCalls(last) {
			out, err := mockTool(ctx, tc.Function.Name, tc.Function.Arguments)
			if err != nil {
				return nil, fmt.Errorf("tool %q: %w", tc.Function.Name, err)
			}
			toolMsg := schema.ToolResultMessage(tc.ID, out)
			results = append(results, toolMsg)
		}
		return []compose_v2.ChannelWrite{
			compose_v2.Write(basicAgentMessagesCh, results),
		}, nil
	}

	b := compose_v2.NewBuilder().
		AddNode("model", modelNode).
		AddNode("tools", toolsNode).
		AddEdge(compose_v2.StartNode, "model").
		AddConditionalEdge("model", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
			msgs, _ := compose_v2.Get(chs, basicAgentMessagesCh)
			if len(msgs) == 0 {
				return compose_v2.EndNode, nil
			}
			last := msgs[len(msgs)-1]
			if len(schema.GetToolCalls(last)) == 0 {
				// 无 tool_call → 终止
				return compose_v2.EndNode, nil
			}
			return "tools", nil
		}).
		AddEdge("tools", "model").
		WithMaxVisit(20)
	b = compose_v2.WithChannel(b, basicAgentMessagesCh)
	return b.Compile()
}

// ── 端到端测试 ──

// TestBasicAgent_ReActLoop 验证 ReAct 循环：user → model(tool_call) → tools →
// model(final) → end。
func TestBasicAgent_ReActLoop(t *testing.T) {
	llm := &mockLLM{}
	g, err := buildBasicAgentReAct(llm)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	userMsg := schema.UserMessage("北京天气怎么样？")

	if err := compose_v2.Run(
		context.Background(), g, cm,
		compose_v2.WithInitialWrites(compose_v2.Write(basicAgentMessagesCh, []*schema.Message{userMsg})),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 验证消息累积顺序：user → assistant(tool_call) → tool_result → assistant(final)
	msgs := compose_v2.MustGet(cm.Channels(), basicAgentMessagesCh)
	if len(msgs) != 4 {
		t.Fatalf("应有 4 条 messages（user/assistant_tc/tool/assistant_final），实际 %d", len(msgs))
	}

	if msgs[0].Role != schema.RoleUser {
		t.Fatalf("msgs[0] 应是 user 实际 %s", msgs[0].Role)
	}
	if msgs[1].Role != schema.RoleAssistant || len(schema.GetToolCalls(msgs[1])) != 1 {
		t.Fatalf("msgs[1] 应是 assistant + tool_call，实际 role=%s tcs=%d",
			msgs[1].Role, len(schema.GetToolCalls(msgs[1])))
	}
	if msgs[2].Role != schema.RoleTool {
		t.Fatalf("msgs[2] 应是 tool，实际 %s", msgs[2].Role)
	}
	if msgs[3].Role != schema.RoleAssistant || len(schema.GetToolCalls(msgs[3])) != 0 {
		t.Fatalf("msgs[3] 应是 assistant + final（无 tool_call），实际 role=%s tcs=%d content=%q",
			msgs[3].Role, len(schema.GetToolCalls(msgs[3])), msgs[3].Content)
	}
	if msgs[3].Content != "北京今天 20℃ 晴。" {
		t.Fatalf("msgs[3] content 应是 final answer，实际 %q", msgs[3].Content)
	}

	// 验证 LLM 被调 2 次（first round tool_call + second round final）
	if llm.round != 2 {
		t.Fatalf("LLM 应被调 2 次，实际 %d", llm.round)
	}
}

// TestBasicAgent_NoToolCall_DirectFinal 验证 LLM 第一轮直接返 final（无 tool_call）
// 时立即终止，不调 tools 节点。
func TestBasicAgent_NoToolCall_DirectFinal(t *testing.T) {
	// 自定义 mockLLM 第一轮就返无 tool_call
	directLLM := &directAnswerLLM{}
	modelNode := func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		msgs, _ := compose_v2.Get(chs, basicAgentMessagesCh)
		out, _ := directLLM.Generate(context.Background(), msgs)
		return []compose_v2.ChannelWrite{
			compose_v2.Write(basicAgentMessagesCh, []*schema.Message{out}),
		}, nil
	}
	toolsNode := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		t.Fatalf("无 tool_call 时不应调 tools 节点")
		return nil, nil
	}

	b := compose_v2.NewBuilder().
		AddNode("model", modelNode).
		AddNode("tools", toolsNode).
		AddEdge(compose_v2.StartNode, "model").
		AddConditionalEdge("model", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
			msgs, _ := compose_v2.Get(chs, basicAgentMessagesCh)
			last := msgs[len(msgs)-1]
			if len(schema.GetToolCalls(last)) == 0 {
				return compose_v2.EndNode, nil
			}
			return "tools", nil
		}).
		AddEdge("tools", "model")
	b = compose_v2.WithChannel(b, basicAgentMessagesCh)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(
		context.Background(), g, cm,
		compose_v2.WithInitialWrites(compose_v2.Write(basicAgentMessagesCh, []*schema.Message{schema.UserMessage("hi")})),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs := compose_v2.MustGet(cm.Channels(), basicAgentMessagesCh)
	if len(msgs) != 2 {
		t.Fatalf("应 2 条（user + assistant_final），实际 %d", len(msgs))
	}
	if directLLM.round != 1 {
		t.Fatalf("直接 final 时 LLM 应只调 1 次，实际 %d", directLLM.round)
	}
}

type directAnswerLLM struct {
	round int
}

func (m *directAnswerLLM) Generate(_ context.Context, _ []*schema.Message) (*schema.Message, error) {
	m.round++
	return schema.AssistantMessage("Hello!"), nil
}

// TestBasicAgent_MultiRoundReAct 验证多轮 tool 调用循环（model→tools→model→tools→model→final）。
func TestBasicAgent_MultiRoundReAct(t *testing.T) {
	llm := &multiRoundLLM{maxToolRounds: 3}
	modelNode := func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		msgs, _ := compose_v2.Get(chs, basicAgentMessagesCh)
		out, err := llm.Generate(context.Background(), msgs)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{
			compose_v2.Write(basicAgentMessagesCh, []*schema.Message{out}),
		}, nil
	}
	toolsNode := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		msgs, _ := compose_v2.Get(chs, basicAgentMessagesCh)
		last := msgs[len(msgs)-1]
		var results []*schema.Message
		for _, tc := range schema.GetToolCalls(last) {
			out, _ := mockTool(ctx, tc.Function.Name, tc.Function.Arguments)
			results = append(results, schema.ToolResultMessage(tc.ID, out))
		}
		return []compose_v2.ChannelWrite{
			compose_v2.Write(basicAgentMessagesCh, results),
		}, nil
	}

	b := compose_v2.NewBuilder().
		AddNode("model", modelNode).
		AddNode("tools", toolsNode).
		AddEdge(compose_v2.StartNode, "model").
		AddConditionalEdge("model", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
			msgs, _ := compose_v2.Get(chs, basicAgentMessagesCh)
			last := msgs[len(msgs)-1]
			if len(schema.GetToolCalls(last)) == 0 {
				return compose_v2.EndNode, nil
			}
			return "tools", nil
		}).
		AddEdge("tools", "model").
		WithMaxVisit(20)
	b = compose_v2.WithChannel(b, basicAgentMessagesCh)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(
		context.Background(), g, cm,
		compose_v2.WithInitialWrites(compose_v2.Write(basicAgentMessagesCh, []*schema.Message{schema.UserMessage("查 3 次天气")})),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 验证 LLM 调 4 次（3 轮 tool + 1 轮 final），共 8 条消息（user + (assistant_tc + tool)×3 + assistant_final）
	if llm.round != 4 {
		t.Fatalf("LLM 应调 4 次，实际 %d", llm.round)
	}
	msgs := compose_v2.MustGet(cm.Channels(), basicAgentMessagesCh)
	if len(msgs) != 8 {
		t.Fatalf("应 8 条 messages，实际 %d", len(msgs))
	}
}

type multiRoundLLM struct {
	round         int
	maxToolRounds int
}

func (m *multiRoundLLM) Generate(_ context.Context, _ []*schema.Message) (*schema.Message, error) {
	m.round++
	if m.round <= m.maxToolRounds {
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: fmt.Sprintf("call_%d", m.round), Function: schema.FunctionCall{
				Name:      "get_weather",
				Arguments: `{"city":"Beijing"}`,
			}},
		}), nil
	}
	return schema.AssistantMessage("3 次都晴"), nil
}
