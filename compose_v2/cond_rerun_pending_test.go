package compose_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
)

// 声明了 `CondFailRerunFrom` 的图，cond 报错后存档必须留在「等自动恢复」那一档。
//
// 这个档的语义就写在名字里：存着，等 Resume 重跑 from。但 cond 返出去的是**业务错**
// （cond 报错的原因原样带着），而出口原先只按 `IsControlFlowSignal` 判档——业务错一律
// 标成失败现场。
//
// 写这条时（2026-08-13 撤 Status 闸门之前）后果是**静默退化**：失败现场不被 auto-load 捡 ⟹
// 下一次 Resume 变成 fresh run 从头再跑一遍，policy 承诺的「重跑 from」没发生，err 一路是 nil。
// 今天闸门按内容判、续跑不受影响，但档位仍要标对——`ListPending` 是按它过滤的。这条不好抓——`multi_instance_test.go` 那个
// Resume 断言就被它骗过：从头跑一遍恰好让计数对上，测试照绿。
//
// **sanity-revert**：把出口判据从 `shouldStayPending` 改回 `IsControlFlowSignal`，
// 本测试 FAIL 并报存档档位是 failed。
func TestCondRerunFrom_CheckpointStaysPending(t *testing.T) {
	bizErr := errors.New("上游数据还没备齐")
	var condCalls, preRuns int

	b := compose_v2.NewBuilder().WithName("cond_rerun_pending")
	// **前置节点是判据**：正常 Resume 的 frontier 是 [from]，pre 不该重跑；退化成
	// fresh run 则从 StartNode 走一遍，pre 会再跑一次。只数 cond 或 from 的次数区分
	// 不了两者——它们在两种路径下都恰好各跑一次。
	b.AddNode("pre", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		preRuns++
		return nil, nil
	})
	b.AddNode("from", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddNode("to", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "pre")
	b.AddEdge("pre", "from")
	b.AddConditionalEdge("from", func(context.Context, compose_v2.Channels) (compose_v2.NodeID, error) {
		condCalls++
		if condCalls == 1 {
			return "", bizErr // 首跑失败 → 按 policy 存档等重跑
		}
		return "to", nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	b.AddEdge("to", compose_v2.EndNode)

	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	runErr := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("crp_sid"))
	if runErr == nil {
		t.Fatal("首跑应当带着 cond 的错退出")
	}
	if !errors.Is(runErr, bizErr) {
		t.Errorf("cond 报错的原因该原样带出来，实际 %v", runErr)
	}

	cp, loadErr := cper.LoadNS(ctx, "crp_sid", "")
	if loadErr != nil {
		t.Fatalf("LoadNS: %v", loadErr)
	}
	if cp == nil {
		t.Fatal("CondFailRerunFrom 该留存档")
	}
	if cp.Status != compose_v2.CPStatusPending {
		t.Errorf("存档档位是 %v，want pending——这一档的语义是「存着等 Resume 重跑 from」，"+
			"标成失败现场就从「谁在等答案」的清单里消失了",
			cp.Status)
	}

	if preRuns != 1 {
		t.Fatalf("fixture 没造出预期状态：首跑 pre 应当执行 1 次，实际 %d", preRuns)
	}

	// 反面：存档得真能被自动捡起，而且是**从 from 续跑**不是从头再来。
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("crp_sid")); err != nil {
		t.Fatalf("Resume 应当成功，实际 %v", err)
	}
	if preRuns != 1 {
		t.Errorf("Resume 之后 pre 累计跑了 %d 次，want 1——多跑说明没从 from 续跑，"+
			"而是当 fresh run 从头走了一遍（副作用会整条重放）", preRuns)
	}
}
