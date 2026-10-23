package compose_v2_test

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// 节点里直接跑的嵌套图跑完之后节点又失败（不是条件边否决）：续跑时节点重跑，嵌套图的存档在它成功时已清，于是它从头
// 再跑——一次重生，里面的提问是新内容上的新问题、换新 ID。调用方累积传的旧答案（对 v1 的批准）不兑付给 v2。
//
// 修之前重生的图在同一个位置、同一个 ID 提问，旧答案被直接兑付：问过「批准 v1 吗」，产出「v2 已批准(答:批准 v1 吗)」，
// err == nil（实测）。续跑嵌套图自己存档的那一遍不算重生：答过的问题不重问。

// regenDrive 跑图：中断就把问题记下、按问题作答（累积传答案），普通失败就再跑一次（失败续跑），最多几轮。
func regenDrive(t *testing.T, g *compose_v2.Graph) (asked []string, err error) {
	t.Helper()
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()), compose_v2.WithSessionID("regen")}
	answers := map[string]*schema.Message{}
	for round := 0; round < 8; round++ {
		err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), append(opts, compose_v2.WithResumeAnswers(answers))...)
		if err == nil {
			return asked, nil
		}
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			continue // 普通失败：续跑
		}
		for _, q := range ie.AllQueries() {
			asked = append(asked, q.Question)
			answers[q.InterruptID] = schema.UserMessage("答:" + q.Question)
		}
	}
	return asked, err
}

func TestNestedGraph_RegeneratedAfterHostFailure_AsksNewQuestion(t *testing.T) {
	var drafts atomic.Int32
	inner, outCh := retryAskDraftGraph(t, &drafts)
	var failed atomic.Bool
	var final string
	g := retryAskGraph(t, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		cm := compose_v2.NewChannelManager()
		if err := compose_v2.Run(ctx, inner, cm, compose_v2.WithRunNS("inner")); err != nil {
			return nil, err
		}
		if failed.CompareAndSwap(false, true) {
			return nil, errors.New("下游错误") // 嵌套图跑完之后节点失败一次
		}
		final, _ = compose_v2.Get(cm.Channels(), outCh)
		return nil, nil
	})
	asked, err := regenDrive(t, g)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %q）", err, asked)
	}
	if drafts.Load() != 2 {
		t.Fatalf("fixture：节点续跑应从头再跑嵌套图（draft 2 次），实际 %d", drafts.Load())
	}
	if !slices.Equal(asked, []string{"批准 v1 吗", "批准 v2 吗"}) || final != "v2 已批准(答:批准 v2 吗)" {
		t.Errorf("重生的嵌套图应问新问题、批准用在 v2 上；问过 %q，final=%q", asked, final)
	}
}

// 两层嵌套：外层节点里跑图 A，A 的节点里再跑起草 / 批准图 B。A 跑完之后外层节点失败、续跑时 A 重生——B 的提问
// 在 A 的重生次数之下换新身份（重生次数沿外层链往里传）。
func TestNestedGraph_RegenerationPropagatesInward(t *testing.T) {
	var drafts atomic.Int32
	inner, outCh := retryAskDraftGraph(t, &drafts)
	midOut := compose_v2.LastValue[string]("mid_out")
	mb := compose_v2.NewBuilder().WithName("regen_mid")
	compose_v2.WithChannel(mb, midOut)
	mb.AddNode("m", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		cm := compose_v2.NewChannelManager()
		if err := compose_v2.Run(ctx, inner, cm, compose_v2.WithRunNS("inner")); err != nil {
			return nil, err
		}
		o, _ := compose_v2.Get(cm.Channels(), outCh)
		return []compose_v2.ChannelWrite{compose_v2.Write(midOut, o)}, nil
	})
	mb.AddEdge(compose_v2.StartNode, "m")
	mb.AddEdge("m", compose_v2.EndNode)
	mid, err := mb.Compile()
	if err != nil {
		t.Fatal(err)
	}
	var failed atomic.Bool
	var final string
	g := retryAskGraph(t, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		cm := compose_v2.NewChannelManager()
		if err := compose_v2.Run(ctx, mid, cm, compose_v2.WithRunNS("mid")); err != nil {
			return nil, err
		}
		if failed.CompareAndSwap(false, true) {
			return nil, errors.New("下游错误")
		}
		final, _ = compose_v2.Get(cm.Channels(), midOut)
		return nil, nil
	})
	asked, err := regenDrive(t, g)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %q）", err, asked)
	}
	if !slices.Equal(asked, []string{"批准 v1 吗", "批准 v2 吗"}) || final != "v2 已批准(答:批准 v2 吗)" {
		t.Errorf("外层重生应传到里层的提问：问过 %q，final=%q", asked, final)
	}
}

// 嵌套图问完人还没跑完（挂起）时外层续跑：它续它自己的存档，不是重生——答过的问题不重问、ID 不变。
func TestNestedGraph_ResumedFromOwnCheckpoint_NotRegenerated(t *testing.T) {
	var drafts atomic.Int32
	inner, outCh := retryAskDraftGraph(t, &drafts)
	var final string
	g := retryAskGraph(t, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		cm := compose_v2.NewChannelManager()
		if err := compose_v2.Run(ctx, inner, cm, compose_v2.WithRunNS("inner")); err != nil {
			return nil, err
		}
		final, _ = compose_v2.Get(cm.Channels(), outCh)
		return nil, nil
	})
	asked, err := regenDrive(t, g)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %q）", err, asked)
	}
	if !slices.Equal(asked, []string{"批准 v1 吗"}) || final != "v1 已批准(答:批准 v1 吗)" || drafts.Load() != 1 {
		t.Errorf("续跑自己存档的嵌套图不该重生：问过 %q，final=%q，draft %d 次", asked, final, drafts.Load())
	}
}
