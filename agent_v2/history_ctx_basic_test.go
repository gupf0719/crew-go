package agent_v2_test

// 顶层 BasicAgent 用运行入口声明的会话历史配置（WithHistoryCtx）：声明了就以它为准，否则用自己的 WithHistory——
// 同编排。只看自己的配置时，只在 ctx 上声明历史的调用一条不写、两者都配时写进自己那个（WithHistoryCtx 的约定是
// 以 ctx 为准），DetachTask 开的独立任务里跑没配 WithHistory 的 BasicAgent 也一条不写（DetachTask 留下了历史配置）。

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

func historyCount(t *testing.T, ctx context.Context, hs agent_v2.HistoryStore, owner, conv string) int {
	t.Helper()
	msgs, err := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: owner, ConvID: conv})
	if err != nil {
		t.Fatal(err)
	}
	return len(msgs)
}

// TestHistory_BasicAgent_HistoryCtxOnly_Records：顶层 BasicAgent 没配 WithHistory，运行入口用 WithHistoryCtx 声明了历史——照样写。
func TestHistory_BasicAgent_HistoryCtxOnly_Records(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ctx := compose_v2.WithThreadIDCtx(agent_v2.WithHistoryCtx(context.Background(), hs, "o"), "cv")
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("bare"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("答")})))
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("问")); err != nil {
		t.Fatal(err)
	}
	if n := historyCount(t, ctx, hs, "o", "cv"); n != 2 {
		t.Fatalf("运行入口声明了会话历史，顶层 BasicAgent 应记下一问一答 2 条，实际 %d 条", n)
	}
}

// TestHistory_BasicAgent_HistoryCtxWinsOverOption：两者都配时以 ctx 为准——写进 WithHistoryCtx 的那个，WithHistory 的那个不写。
func TestHistory_BasicAgent_HistoryCtxWinsOverOption(t *testing.T) {
	ctxHS, optHS := agent_v2.NewInMemoryHistoryStore(), agent_v2.NewInMemoryHistoryStore()
	ctx := compose_v2.WithThreadIDCtx(agent_v2.WithHistoryCtx(context.Background(), ctxHS, "o"), "cv")
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("both"), agent_v2.WithHistory(optHS, "o"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("答")})))
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("问")); err != nil {
		t.Fatal(err)
	}
	if n := historyCount(t, ctx, ctxHS, "o", "cv"); n != 2 {
		t.Errorf("运行入口声明的那个后端应记下 2 条，实际 %d 条", n)
	}
	if n := historyCount(t, ctx, optHS, "o", "cv"); n != 0 {
		t.Errorf("两者都配时以 ctx 为准，WithHistory 那个不该写，实际 %d 条", n)
	}
}

// TestHistory_DetachTask_BareBasicAgent_RecordsIntoPushedConv：节点里经 DetachTask 开独立任务、推一个会话 ID，跑一个没配
// WithHistory 的 BasicAgent——DetachTask 留下了历史配置，它照样写进那个会话。
func TestHistory_DetachTask_BareBasicAgent_RecordsIntoPushedConv(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	side := agent_v2.NewBasicAgent(agent_v2.WithName("side"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("独立任务的答复")})))
	inCh := compose_v2.LastValue[*schema.Message]("__hctx_in__")
	outCh := compose_v2.LastValue[*schema.Message]("__hctx_out__")
	bld := compose_v2.NewBuilder().WithName("hctxg")
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, outCh)
	bld.AddNode("n1", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		dctx := compose_v2.WithThreadIDCtx(agent_v2.DetachTask(ctx), "c_side")
		ev, err := side.Run(dctx, agent_v2.NewRunSession(), schema.UserMessage("独立任务的问题"))
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, agent_v2.FinalMessage(ev))}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "n1")
	bld.AddEdge("n1", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ga := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("p"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
	ctx := compose_v2.WithThreadIDCtx(agent_v2.WithHistoryCtx(context.Background(), hs, "o"), "c_main")
	if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("主任务")); err != nil {
		t.Fatal(err)
	}
	if n := historyCount(t, ctx, hs, "o", "c_side"); n != 2 {
		t.Fatalf("独立任务推的会话里应记下一问一答 2 条，实际 %d 条", n)
	}
}
