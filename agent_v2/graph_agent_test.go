package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestGraphAgent_BasicRun 验证 GraphAgent 包装一个简单单节点 graph 跑通。
func TestGraphAgent_BasicRun(t *testing.T) {
	// 自定义 channel
	inputCh := compose_v2.LastValue[*schema.Message]("__test_input__")
	outputCh := compose_v2.LastValue[*schema.Message]("__test_output__")

	// 单节点：echo input.Content + " processed"
	bld := compose_v2.NewBuilder()
	compose_v2.WithChannel(bld, inputCh)
	compose_v2.WithChannel(bld, outputCh)

	bld.AddNode("echo", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inputCh)
		out := schema.AssistantMessage(in.Content + " processed")
		return []compose_v2.ChannelWrite{compose_v2.Write(outputCh, out)}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "echo")
	bld.AddEdge("echo", compose_v2.EndNode)

	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	ga := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("echo_agent"),
		agent_v2.WithGraphInputWriter(func(input *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inputCh, input)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outputCh)
			return out
		}),
	)

	events, err := ga.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hello"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "hello processed") {
		t.Errorf("expect 'hello processed', got %v", final)
	}
}

// TestGraphAgent_AsStepInSequential 验证 GraphAgent 可作 SequentialAgent step 嵌入。
func TestGraphAgent_AsStepInSequential(t *testing.T) {
	// 用同一 echo graph 作 step
	inputCh := compose_v2.LastValue[*schema.Message]("__test_input2__")
	outputCh := compose_v2.LastValue[*schema.Message]("__test_output2__")

	bld := compose_v2.NewBuilder()
	compose_v2.WithChannel(bld, inputCh)
	compose_v2.WithChannel(bld, outputCh)
	bld.AddNode("echo", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inputCh)
		out := schema.AssistantMessage("[echo] " + in.Content)
		return []compose_v2.ChannelWrite{compose_v2.Write(outputCh, out)}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "echo")
	bld.AddEdge("echo", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	ga := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("echo_agent"),
		agent_v2.WithGraphInputWriter(func(input *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inputCh, input)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outputCh)
			return out
		}),
	)

	llmAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("llm"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("processed")})),
	)

	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
		AddStep("graph_step", ga).
		AddStep("llm_step", llmAgent)

	events, err := seq.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "processed") {
		t.Errorf("expect 'processed' from LLM step, got %v", final)
	}
}

// TestGraphAgent_NoGraphFails 验证 graph 必填。
func TestGraphAgent_NoGraphFails(t *testing.T) {
	ga := agent_v2.NewGraphAgent(nil, agent_v2.WithGraphAgentName("nograph"))
	_, err := ga.Run(context.Background(), nil, schema.UserMessage("hi"))
	if err == nil || !strings.Contains(err.Error(), "graph required") {
		t.Fatalf("expect 'graph required' err, got %v", err)
	}
}

// TestGraphAgent_NoInputWriterFails 验证 InputWriter 必填。
func TestGraphAgent_NoInputWriterFails(t *testing.T) {
	bld := compose_v2.NewBuilder()
	bld.AddNode("noop", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	bld.AddEdge(compose_v2.StartNode, "noop")
	bld.AddEdge("noop", compose_v2.EndNode)
	g, _ := bld.Compile()

	ga := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("noinput"))
	_, err := ga.Run(context.Background(), nil, schema.UserMessage("hi"))
	if err == nil || !strings.Contains(err.Error(), "input writer required") {
		t.Fatalf("expect 'input writer required' err, got %v", err)
	}
}
