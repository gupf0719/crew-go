// v2_graphagent_demo 演示 agent_v2.NewGraphAgent — 把任意 compose_v2.Graph 包装成
// 满足 agent_v2.Agent 接口的对象, 能被 SequentialAgent / ParallelAgent / 任意编排
// 嵌入.
//
// **场景**: 自定义 3 节点 graph (preprocess → analyze → format), 用 channel 串数据.
// 包成 GraphAgent 后可单独 Run + 嵌入 SequentialAgent 作 step 跑.
//
// **关键 API**:
//   - `compose_v2.NewBuilder()` + `WithChannel(bld, ch)` — 声明业务 channel
//   - `compose_v2.LastValue[T](name)` — 类型安全 channel (LastValue reducer)
//   - `bld.AddNode(id, NodeFunc)` + `AddEdge(from, to)` — 拼接 graph 拓扑
//   - `compose_v2.WithNodeComponent("retrieval")` — Phase 2: 声明 node 组件类型, 接
//     observability 后 trace span 标对应 kind (retrieval/embedding/guardrail)
//   - `compose_v2.StartNode` / `EndNode` — graph 进出标记
//   - `compose_v2.Get(chs, ch)` / `compose_v2.Write(ch, val)` — 类型安全 channel 读写
//   - `agent_v2.NewGraphAgent(g, opts...)` — 包装 Graph 为 Agent
//   - `WithGraphInputWriter` / `WithGraphOutputReader` — 业务定义 Agent.Run input
//     `*schema.Message` 怎么进 channel, output 怎么从 channel 取
//
// **不依赖真 LLM** — 全用纯函数节点, 演示 channel 数据流 + Agent 接口包装.
//
// 跑法:
//
//	go run ./example/graph/v2_graphagent_demo
//
// 全场景 PASS + 退出码 0 表示通过.
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

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("v2_graphagent_demo PASS")
}

func run() error {
	ctx := context.Background()

	// === 场景 1: 自定义 3 节点 graph 包成 GraphAgent 单独跑 ===
	ga, err := buildGraphAgent("text_analyzer")
	if err != nil {
		return fmt.Errorf("build GraphAgent: %w", err)
	}

	sess := agent_v2.NewRunSession()
	events, err := ga.Run(ctx, sess, schema.UserMessage("Hello World"))
	if err != nil {
		return fmt.Errorf("场景 1 Run: %w", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		return fmt.Errorf("场景 1: expect final message, got nil")
	}
	expected := "ANALYZED: hello world (chars=11)"
	if final.Content != expected {
		return fmt.Errorf("场景 1: expect %q, got %q", expected, final.Content)
	}
	fmt.Printf("场景 1 OK: GraphAgent 独立跑 — final=%q\n", final.Content)

	// === 场景 2: 同 GraphAgent 作为 SequentialAgent step 嵌入 ===
	ga2, err := buildGraphAgent("text_analyzer_2")
	if err != nil {
		return fmt.Errorf("build GraphAgent 2: %w", err)
	}

	seq := agent_v2.NewSequentialAgent(
		agent_v2.WithSequentialName("seq_with_graph"),
	)
	seq.AddStep("analyze", ga2)

	sess2 := agent_v2.NewRunSession()
	events2, err := seq.Run(ctx, sess2, schema.UserMessage("Channel Based"))
	if err != nil {
		return fmt.Errorf("场景 2 Run: %w", err)
	}
	final2 := agent_v2.FinalMessage(events2)
	if final2 == nil {
		return fmt.Errorf("场景 2: expect final message, got nil")
	}
	if !strings.Contains(final2.Content, "ANALYZED") {
		return fmt.Errorf("场景 2: expect 'ANALYZED' in final, got %q", final2.Content)
	}
	fmt.Printf("场景 2 OK: GraphAgent 嵌入 SequentialAgent — final=%q\n", final2.Content)

	return nil
}

// buildGraphAgent 拼装 3 节点 graph:
//
//	START → preprocess (lower-case) → analyze (count chars) → format (prepend "ANALYZED:") → END
//
// 用 3 个 LastValue channel 串数据, 节点纯函数.
func buildGraphAgent(name string) (*agent_v2.GraphAgent, error) {
	// 声明业务 channel — 类型参数 *schema.Message 让编译器查类型
	inCh := compose_v2.LastValue[*schema.Message]("text_input")
	loweredCh := compose_v2.LastValue[string]("text_lowered")
	analyzedCh := compose_v2.LastValue[string]("text_analyzed")
	outCh := compose_v2.LastValue[*schema.Message]("text_output")

	bld := compose_v2.NewBuilder()
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, loweredCh)
	compose_v2.WithChannel(bld, analyzedCh)
	compose_v2.WithChannel(bld, outCh)

	// 节点 1: preprocess — 读 inCh, 转小写, 写 loweredCh
	bld.AddNode("preprocess", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, ok := compose_v2.Get(chs, inCh)
		if !ok || in == nil {
			return nil, fmt.Errorf("preprocess: 输入 channel 空")
		}
		lowered := strings.ToLower(in.Content)
		return []compose_v2.ChannelWrite{compose_v2.Write(loweredCh, lowered)}, nil
	})

	// 节点 2: analyze — 读 loweredCh, 算字符数, 写 analyzedCh
	//
	// **Phase 2 演示 — WithNodeComponent**: GraphAgent 显式编排路径下, 业务在 NodeFunc 里
	// 直接调组件 (retrieval/embedding/guardrail) 不经 LLM tool_call. 用
	// WithNodeComponent 声明 node 组件类型后, 接 observability (obs.OrchHooks()) 时此 node
	// 的 trace span operation.name 标对应 kind (这里 retrieval) 而非通用 "workflow" — 让
	// trace 树区分组件调用. 此 demo 是纯函数 (analyze 只计字数), 声明 retrieval 仅演示
	// API; 实际业务里 retrieval node 通常是 RAG 向量检索 / 全文检索. 无声明的 node (preprocess
	// /format) 仍是默认 workflow kind.
	bld.AddNode("analyze", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		text, ok := compose_v2.Get(chs, loweredCh)
		if !ok {
			return nil, fmt.Errorf("analyze: lowered channel 空")
		}
		analyzed := fmt.Sprintf("%s (chars=%d)", text, len(text))
		return []compose_v2.ChannelWrite{compose_v2.Write(analyzedCh, analyzed)}, nil
	}, compose_v2.WithNodeComponent("retrieval"))

	// 节点 3: format — 读 analyzedCh, 加 prefix, 写 outCh (用 assistant message 包装)
	bld.AddNode("format", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		analyzed, ok := compose_v2.Get(chs, analyzedCh)
		if !ok {
			return nil, fmt.Errorf("format: analyzed channel 空")
		}
		final := schema.AssistantMessage("ANALYZED: " + analyzed)
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, final)}, nil
	})

	bld.AddEdge(compose_v2.StartNode, "preprocess")
	bld.AddEdge("preprocess", "analyze")
	bld.AddEdge("analyze", "format")
	bld.AddEdge("format", compose_v2.EndNode)

	g, err := bld.Compile()
	if err != nil {
		return nil, fmt.Errorf("Compile graph: %w", err)
	}

	// 包成 GraphAgent — 业务负责定义 input message 怎么进图, output 怎么取
	return agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName(name),
		agent_v2.WithGraphAgentDescription("把文本转小写+计字数+格式化"),
		agent_v2.WithGraphInputWriter(func(input *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, input)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}),
	), nil
}
