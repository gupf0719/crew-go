package compose_v2_test

// 子图重访身份哨兵（ns.go JoinVisitNS + subgraph.go 静态成员 visit 维）。
//
// 钉的缺陷形态：父循环第 2 轮重进同一个 subgraph 节点时，childNS 不带轮次维 ⟹
// 子图内部从头执行、全部内部维度与第 1 轮相同 ⟹ 第 2 轮的 Ask 算出与第 1 轮
// **完全相同**的 InterruptID ⟹ 撞上已兑付的答案位、终局 ErrRunAborted（2026-09-23
// 实测）。答案账本跨子图边界之前的后果更糟：沿 ctx 活着的旧答案被静默复用，第 2 轮的提问
// 从未呈现给用户（当时实测 gotAnswers=2 而 asked=1，err==nil）。
//
// sanity-revert：把 subgraph.go 静态分支的 JoinVisitNS 换回 JoinNS，
// TestSubgraphRevisit_DistinctInterruptIdentity 在「第 2 轮必须重新提问」处红（红成
// 「答案已经被兑付过了」）。

import (
	"context"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// buildLoopOverAskingSub 造「父循环（tick→sub→cond 回边）+ 子图（ask 节点 Ask）」
// fixture：父图进 sub 两轮，每轮子图都该向用户新问一次。
func buildLoopOverAskingSub(t *testing.T) (g *compose_v2.Graph, asked *[]string, gotAnswers *[]string) {
	t.Helper()
	askedIDs := []string{}
	answers := []string{}

	subBld := compose_v2.NewBuilder()
	subBld.AddNode("ask", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗?"})
		if err != nil {
			if ie, ok := compose_v2.IsInterrupt(err); ok && ie.Query != nil {
				askedIDs = append(askedIDs, ie.Query.InterruptID)
			}
			return nil, err
		}
		answers = append(answers, ans.Content)
		return nil, nil
	})
	subBld.AddEdge(compose_v2.StartNode, "ask")
	sub, err := subBld.Compile()
	if err != nil {
		t.Fatalf("sub compile: %v", err)
	}

	bld := compose_v2.NewBuilder()
	round := compose_v2.Sum("round")
	bld = compose_v2.WithChannel(bld, round)
	bld.AddNode("tick", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(round, 1)}, nil
	})
	bld.AddSubgraphNode("sub", sub)
	bld.AddEdge(compose_v2.StartNode, "tick")
	bld.AddEdge("tick", "sub")
	bld.AddConditionalEdge("sub", func(ctx context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		r, _ := compose_v2.Get(chs, round)
		if r < 2 {
			return "tick", nil
		}
		return compose_v2.EndNode, nil
	})
	g, err = bld.Compile()
	if err != nil {
		t.Fatalf("parent compile: %v", err)
	}
	return g, &askedIDs, &answers
}

// TestSubgraphRevisit_DistinctInterruptIdentity：循环两轮各进一次子图，两轮的
// 中断身份必须不同——第 2 轮必须**重新向用户提问**，而不是静默吃掉第 1 轮的答案；
// 三次 Run 闭环后两轮答案按序兑付。
func TestSubgraphRevisit_DistinctInterruptIdentity(t *testing.T) {
	g, asked, gotAnswers := buildLoopOverAskingSub(t)

	cper := compose_v2.NewInMemoryCheckpointer()
	sid := "revisit-identity-sid"
	base := context.Background()

	e1 := compose_v2.Run(base, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	ie1, ok := compose_v2.IsInterrupt(e1)
	if !ok {
		t.Fatalf("Run1 该中断, got %v", e1)
	}
	id1 := ie1.Query.InterruptID

	// Run2：答第 1 轮的问题。第 1 轮子图续跑兑付，第 2 轮子图必须**重新中断**。
	e2 := compose_v2.Run(base, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			id1: schema.UserMessage("批准A"),
		}))
	ie2, ok := compose_v2.IsInterrupt(e2)
	if !ok {
		t.Fatalf("Run2 第 2 轮该重新提问（中断），got err=%v gotAnswers=%v", e2, *gotAnswers)
	}
	id2 := ie2.Query.InterruptID
	if id2 == id1 {
		t.Fatalf("两轮中断身份该不同, 都是 %s", id1)
	}
	if len(*gotAnswers) != 1 || (*gotAnswers)[0] != "批准A" {
		t.Fatalf("Run2 该只兑付第 1 轮答案, got %v", *gotAnswers)
	}

	// 第 2 轮子图的挂起档该落在带重访序数的 ns 下（visit=1 ⟹ "sub@1"）
	subCp2, err := cper.LoadNS(base, sid, "sub@1")
	if err != nil {
		t.Fatalf("LoadNS sub@1: %v", err)
	}
	if subCp2 == nil || len(subCp2.PendingInterrupts) != 1 {
		t.Fatalf("第 2 轮子档该挂在 sub@1 且带 1 条挂起, got %+v", subCp2)
	}

	// Run3：累积传两轮答案（文档化的预期形态），闭环。
	e3 := compose_v2.Run(base, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			id1: schema.UserMessage("批准A"),
			id2: schema.UserMessage("批准B"),
		}))
	if e3 != nil {
		t.Fatalf("Run3 该闭环, got %v", e3)
	}
	if len(*gotAnswers) != 2 || (*gotAnswers)[1] != "批准B" {
		t.Fatalf("两轮答案该按序各兑付一次, got %v", *gotAnswers)
	}
	if len(*asked) != 2 {
		t.Fatalf("全程该恰好提问 2 次, got %d (%v)", len(*asked), *asked)
	}
}
