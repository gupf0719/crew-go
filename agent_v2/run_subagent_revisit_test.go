package agent_v2_test

// RunSubAgent 静态路径重访身份哨兵（run_subagent.go 的 JoinVisitNS 维）。
//
// 钉的缺陷形态：业务图里一个循环节点的 NodeFunc 经 RunSubAgent 调外部执行体，
// 执行体自带内部图（内部位置每轮从头算）——第 2 轮 childNS 不带重访序数则内部
// 提问算出与第 1 轮相同的 InterruptID，撞上已兑付的答案位、终局 ErrRunAborted
// （2026-09-23 实测；答案账本跨子图边界之前是沿 ctx 活着的旧答案被静默复用）。
//
// fixture 的 sub 特意用「内部跑独立 compose 图」的壳（而不是直接在父节点位置
// Ask）——直接 Ask 会借到父节点自己的 visit 维，revert 后仍绿（假绿）；带内部图
// 才还原「内部维度每轮全同、ns 是唯一区分维」的真实形态（BasicAgent 同构）。
//
// sanity-revert：把 run_subagent.go 静态分支的 JoinVisitNS 换回 JoinNS，本测试
// 在「Run2 第 2 轮该重新提问」处红。

import (
	"context"
	"fmt"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// askingShellAgent 是「自带内部图的外部执行体」最小壳：Run 内部裸跑一张只有
// ask 节点的 compose 图（配置全靠 ctx 回落——checkpointer/sid/ns 均不显式传）。
type askingShellAgent struct {
	asked      *[]string
	gotAnswers *[]string
}

func (a *askingShellAgent) Run(ctx context.Context, _ *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	bld := compose_v2.NewBuilder()
	bld.AddNode("ask", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗?"})
		if err != nil {
			if ie, ok := compose_v2.IsInterrupt(err); ok && ie.Query != nil {
				*a.asked = append(*a.asked, ie.Query.InterruptID)
			}
			return nil, err
		}
		*a.gotAnswers = append(*a.gotAnswers, ans.Content)
		return nil, nil
	})
	bld.AddEdge(compose_v2.StartNode, "ask")
	g, err := bld.Compile()
	if err != nil {
		return nil, fmt.Errorf("shell compile: %w", err)
	}
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager()); err != nil {
		return nil, err
	}
	return nil, nil
}

func (a *askingShellAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, fmt.Errorf("askingShellAgent: resume not supported")
}
func (a *askingShellAgent) Name() string        { return "asking_shell" }
func (a *askingShellAgent) Description() string { return "内部图提问壳" }

// TestRunSubAgentRevisit_DistinctInterruptIdentity：父循环两轮各经 RunSubAgent
// 调一次执行体，两轮中断身份必须不同——第 2 轮必须重新提问，三次 Run 闭环。
func TestRunSubAgentRevisit_DistinctInterruptIdentity(t *testing.T) {
	asked := []string{}
	gotAnswers := []string{}
	shell := &askingShellAgent{asked: &asked, gotAnswers: &gotAnswers}

	bld := compose_v2.NewBuilder()
	round := compose_v2.Sum("round")
	bld = compose_v2.WithChannel(bld, round)
	bld.AddNode("worker", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, err := agent_v2.RunSubAgent(ctx, "sub", shell, schema.UserMessage("go")); err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(round, 1)}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "worker")
	bld.AddConditionalEdge("worker", func(ctx context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		r, _ := compose_v2.Get(chs, round)
		if r < 2 {
			return "worker", nil
		}
		return compose_v2.EndNode, nil
	})
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	sid := "runsub-revisit-sid"
	base := context.Background()

	e1 := compose_v2.Run(base, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	ie1, ok := compose_v2.IsInterrupt(e1)
	if !ok {
		t.Fatalf("Run1 该中断, got %v", e1)
	}
	id1 := ie1.Query.InterruptID

	e2 := compose_v2.Run(base, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			id1: schema.UserMessage("批准A"),
		}))
	ie2, ok := compose_v2.IsInterrupt(e2)
	if !ok {
		t.Fatalf("Run2 第 2 轮该重新提问（中断），got err=%v gotAnswers=%v", e2, gotAnswers)
	}
	id2 := ie2.Query.InterruptID
	if id2 == id1 {
		t.Fatalf("两轮中断身份该不同, 都是 %s", id1)
	}
	if len(gotAnswers) != 1 || gotAnswers[0] != "批准A" {
		t.Fatalf("Run2 该只兑付第 1 轮答案, got %v", gotAnswers)
	}

	e3 := compose_v2.Run(base, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			id1: schema.UserMessage("批准A"),
			id2: schema.UserMessage("批准B"),
		}))
	if e3 != nil {
		t.Fatalf("Run3 该闭环, got %v", e3)
	}
	if len(gotAnswers) != 2 || gotAnswers[1] != "批准B" {
		t.Fatalf("两轮答案该按序各兑付一次, got %v", gotAnswers)
	}
	if len(asked) != 2 {
		t.Fatalf("全程该恰好提问 2 次, got %d (%v)", len(asked), asked)
	}
}
