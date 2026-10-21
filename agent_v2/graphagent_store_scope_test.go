package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// C1.2 守护测试：GraphAgent 的 NodeFunc 内 framework **不自动** push store scope flag
// （doc/v2/store.md §10.5 #4 承诺"GraphAgent 业务自管图边界"——不像 7 编排 / AgentAsTool /
// SkillTool fork 那样自动 WithSubAgentStoreScope(ctx, true)）。
//
// 守护意义：业务在 GraphAgent NodeFunc 内调子 agent.Run 且想避免 store 双重持久化到父 ns
// 时，必须 **自己** push WithSubAgentStoreScope——framework 不替业务做（图边界业务自定义，
// framework 不知道 NodeFunc 里有没有调子 agent）。若未来 framework 改成自动 push，本测
// fail 提醒同步 store.md §10.5 #4 文档。
//
// 非 tautological 设计：同一 NodeFunc 内双向断言——默认 ctx IsSubAgentStoreScope==false
// （反例：framework 不 push）+ 业务自 push 后 ==true（正例：证明 IsSubAgentStoreScope 真在
// 工作，scopeDefault 的 false 是因为没 push 而非函数恒返 false）。
func TestGraphAgent_NodeFunc_NoAutoStoreScope(t *testing.T) {
	inputCh := compose_v2.LastValue[*schema.Message]("__c12_in__")
	outputCh := compose_v2.LastValue[*schema.Message]("__c12_out__")

	var scopeDefault, scopeAfterSelfPush, nodeRan bool

	bld := compose_v2.NewBuilder()
	compose_v2.WithChannel(bld, inputCh)
	compose_v2.WithChannel(bld, outputCh)
	bld.AddNode("probe", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		nodeRan = true
		// 反例：GraphAgent NodeFunc 内 framework 未自动 push scope → 应 false
		scopeDefault = agent_v2.IsSubAgentStoreScope(ctx)
		// 正例：业务自 push 后 IsSubAgentStoreScope 能 detect → 应 true（证明断言非恒 false）
		scopeAfterSelfPush = agent_v2.IsSubAgentStoreScope(agent_v2.WithSubAgentStoreScope(ctx, true))
		in, _ := compose_v2.Get(chs, inputCh)
		return []compose_v2.ChannelWrite{compose_v2.Write(outputCh, schema.AssistantMessage(in.Content))}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "probe")
	bld.AddEdge("probe", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	ga := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("c12_probe"),
		agent_v2.WithGraphInputWriter(func(input *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inputCh, input)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outputCh)
			return out
		}),
	)

	if _, err := ga.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("x")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !nodeRan {
		t.Fatal("probe node 没跑——测试无效")
	}
	if scopeDefault {
		t.Error("GraphAgent NodeFunc 内 IsSubAgentStoreScope 应 = false（framework 不自动 push，" +
			"业务自管图边界 store.md §10.5 #4），got true —— framework 行为变了需同步文档")
	}
	if !scopeAfterSelfPush {
		t.Error("业务自 push WithSubAgentStoreScope(ctx,true) 后应 = true，got false —— " +
			"IsSubAgentStoreScope 失效（scopeDefault 断言会退化为 tautological）")
	}
}
