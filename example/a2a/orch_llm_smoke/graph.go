package main

import (
	"context"
	"log"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// buildCodeGraph 构造最小自定义图：单节点 NodeFunc 内经 agent_v2.RunSubAgent 调 RemoteAgent
// （一站式 push graphNS + child session + storeScope，对齐 CLAUDE.md "GraphAgent 提示" 推荐
// 范式；直接 ra.Run 不 push 父 ns、a2a client span 不落 child ns），包成 GraphAgent。
func buildCodeGraph(ra *a2a.RemoteAgent) (*compose_v2.Graph, *agent_v2.GraphAgent) {
	inputCh := compose_v2.LastValue[*schema.Message]("__a2a_orch_input__")
	outputCh := compose_v2.LastValue[*schema.Message]("__a2a_orch_output__")

	bld := compose_v2.NewBuilder()
	compose_v2.WithChannel(bld, inputCh)
	compose_v2.WithChannel(bld, outputCh)
	bld.AddNode("call_remote", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inputCh)
		events, err := agent_v2.RunSubAgent(ctx, "call_remote", ra, in)
		if err != nil {
			return nil, err
		}
		final := agent_v2.FinalMessage(events)
		if final == nil {
			final = schema.AssistantMessage("")
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outputCh, final)}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "call_remote")
	bld.AddEdge("call_remote", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		log.Fatalf("graph Compile: %v", err)
	}

	ga := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("a2a_code_graph"),
		agent_v2.WithGraphInputWriter(func(input *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inputCh, input)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outputCh)
			return out
		}),
	)
	return g, ga
}
