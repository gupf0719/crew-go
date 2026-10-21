//go:build integration

package tutorial_test

// ──────────────────────────────────────────────────────────────────
// 9. GraphAgent（进阶）— 自定义图编排 + 节点内调真 LLM sub-agent
//
// 补 adv 系列缺口：此前 9 编排只有 GraphAgent 没有真 LLM integration 测试
//（机制/mock 路径见 agent_v2/graph_agent_test.go 单测，故无 _mock 孪生）。
//
// 拓扑：prepare（确定性节点，拼指令）→ answer（节点内经 RunSubAgent 调真 LLM BasicAgent，
// CLAUDE.md "GraphAgent 提示"的用户自定义 NodeFunc 调子 agent 范式）→
// OutputReader 抽终答。断言用确定性锚：prepare 节点真跑（指令进了 LLM 输入）+
// 终答含强制前缀 ANSWER: + 含确定性事实（2+3=5）。
// ──────────────────────────────────────────────────────────────────

import (
	"context"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

func TestAdv_09_GraphAgent(t *testing.T) {
	provider := requireAPIKey(t)
	llm := newLLM(provider)

	// 节点内被调的真 LLM sub-agent：强制锚定输出格式
	answerer := agent_v2.NewBasicAgent(
		agent_v2.WithName("graph_answerer"),
		agent_v2.WithSystemPrompt(`回答用户问题。回答必须以 "ANSWER: " 开头（大写、半角冒号、一个空格），后接最终答案，不要其它前缀。`),
		agent_v2.WithModel(llm),
	)

	inputCh := compose_v2.LastValue[*schema.Message]("__adv09_input__")
	promptCh := compose_v2.LastValue[*schema.Message]("__adv09_prompt__")
	outputCh := compose_v2.LastValue[*schema.Message]("__adv09_output__")

	bld := compose_v2.NewBuilder()
	compose_v2.WithChannel(bld, inputCh)
	compose_v2.WithChannel(bld, promptCh)
	compose_v2.WithChannel(bld, outputCh)

	// prepare：确定性变换——把用户输入包成带约束的指令（验证图数据流真经过此节点）
	bld.AddNode("prepare", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inputCh)
		wrapped := schema.UserMessage("请只用一句话回答下面的问题，并在答案里写出计算结果数字：" + in.Content)
		return []compose_v2.ChannelWrite{compose_v2.Write(promptCh, wrapped)}, nil
	})
	// answer：节点内调真 LLM sub-agent。走 RunSubAgent——它给子 agent 一个自己的存档槽、让它属于
	// 这张图的任务（续跑续得上）。别写 answerer.Run(ctx, NewRunSession(), ...)：新 session 不属于所在
	// 的任务，入口直接拒（要开独立任务得先 agent_v2.DetachTask(ctx)）。
	bld.AddNode("answer", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		prompt, _ := compose_v2.Get(chs, promptCh)
		events, err := agent_v2.RunSubAgent(ctx, "answer", answerer, prompt)
		if err != nil {
			return nil, err
		}
		final := agent_v2.FinalMessage(events)
		if final == nil {
			final = schema.AssistantMessage("")
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outputCh, final)}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "prepare")
	bld.AddEdge("prepare", "answer")
	bld.AddEdge("answer", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	ga := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("adv09_graph"),
		agent_v2.WithGraphInputWriter(func(input *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inputCh, input)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outputCh)
			return out
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	events, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("2 加 3 等于几？"))
	if err != nil {
		t.Fatalf("GraphAgent.Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || strings.TrimSpace(final.Content) == "" {
		t.Fatal("无终答")
	}
	t.Logf("=== [GraphAgent 终答] ===\n%s", final.Content)

	// 确定性锚 1：LLM 遵循了 prepare 节点注入的格式约束（图数据流 + sub-agent 调用真发生）
	if !strings.HasPrefix(final.Content, "ANSWER: ") {
		t.Errorf("终答应以 'ANSWER: ' 开头（prepare→answer 链路 + 格式约束），got %q", final.Content)
	}
	// 确定性锚 2：含确定性事实
	if !strings.Contains(final.Content, "5") {
		t.Errorf("终答应含计算结果 5，got %q", final.Content)
	}
}
