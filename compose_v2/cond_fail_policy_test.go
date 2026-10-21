package compose_v2_test

// CondFailPolicy 的端到端回归：验两档策略对 **cp 是否可 Resume** 的实际影响。
//
// 背景：cond 是纯判断，它报错只可能是「判断的前提不成立」——上游 node 产出的数据缺失/非法。
// 该重试还是该放弃，取决于上游是不是确定性的，只有业务知道，故交业务声明：
//   - CondFailAbort（默认）：终局失败，不存可 Resume 的 cp
//   - CondFailRerunFrom：存 Pending cp，业务 Resume 重跑 from（让它重新产出、cond 重评）
//
// 上面的 issue86_cond_path_internal_test.go 测的是 frontier 语义（白盒）；本文件测
// **业务可观测的最终效果**（黑盒）：cp 存不存、Resume 能不能续跑。

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
)

// buildCondFailGraph 造 Start → up → cond → down；up 是非确定节点（首跑产坏数据、
// 重跑产好数据），cond 读 up 的产出判断：坏数据就报错。
//
// 这模拟真实场景：up 是调 LLM 的 planner，首次吐了个非法 target，cond 判出来报错；
// 重跑 up 让 LLM 重新生成 → 好数据 → cond 重评通过。
func buildCondFailGraph(t *testing.T, name string, upRuns *int, opts ...compose_v2.CondOption) *compose_v2.Graph {
	t.Helper()
	dataCh := compose_v2.LastValue[string]("__cfp_data__") // LastValue：重跑覆盖，幂等
	downRan := false

	bld := compose_v2.NewBuilder().WithName(name)
	compose_v2.WithChannel(bld, dataCh)
	bld.AddNode("up", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		*upRuns++
		// 首跑产"坏数据"，重跑产"好数据"——模拟 LLM 这类非确定上游
		val := "bad"
		if *upRuns > 1 {
			val = "good"
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(dataCh, val)}, nil
	})
	bld.AddNode("down", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		downRan = true
		return nil, nil
	})
	bld.AddEdge(compose_v2.StartNode, "up")
	bld.AddEdge("down", compose_v2.EndNode)
	// cond 纯判断：读 up 产出，坏数据就报错（判断的前提不成立）
	bld.AddConditionalEdge("up", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		v, _ := compose_v2.Get(chs, dataCh)
		if v == "bad" {
			return "", errors.New("cond: upstream produced invalid data")
		}
		return "down", nil
	}, opts...)
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_ = downRan
	return g
}

// TestCondFailPolicy_DefaultAbort_NoResumableCheckpoint：默认档 → cond 报错 = 终局失败，
// **不存可 Resume 的 cp**，业务 Resume 拿到 ErrNoPendingCheckpoint 而非静默重跑。
//
// sanity-revert：graph_loop.go cond 报错 default 分支改回 `updateFrontier([]NodeID{from})`
// → cp 被存下 → 本测试 fail（cp != nil）。
func TestCondFailPolicy_DefaultAbort_NoResumableCheckpoint(t *testing.T) {
	upRuns := 0
	g := buildCondFailGraph(t, "cfp_abort", &upRuns) // 不传 opts = 默认 CondFailAbort
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(ctx, g, cm,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("cfp_abort_sid"))
	if err == nil {
		t.Fatal("cond 报错应让 Run 失败")
	}
	// cond 报错不是 HITL
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Errorf("cond 报错不该是 InterruptError, got %v", err)
	}

	// **核心断言**：默认 Abort → 不存可 Resume 的 cp（终局失败，业务修完重新跑）
	cp, loadErr := cper.LoadNS(ctx, "cfp_abort_sid", "")
	if loadErr != nil {
		t.Fatalf("LoadNS: %v", loadErr)
	}
	if cp != nil {
		t.Errorf("CondFailAbort（默认）应不存可 Resume 的 cp（cond 报错=上游数据坏，多为确定性，"+
			"重跑白烧上游）；实际存下了 cp: CurrentFrontier=%v\n"+
			"sanity-revert: graph_loop.go cond 报错 default 分支改回 updateFrontier([from]) → 本测试 fail",
			cp.CurrentFrontier)
	}
	if upRuns != 1 {
		t.Errorf("up 应只跑 1 次（Abort 不给 Resume）, got %d", upRuns)
	}
}

// TestCondFailPolicy_RerunFrom_ResumeRerunsUpstream：显式 RerunFrom → 存 Pending cp，
// 业务 Resume **重跑 from**（up 重新产出好数据）→ cond 重评通过 → 下游执行。
//
// 这验证「非确定上游（LLM planner/classifier）报错后重跑是有意义的」这条设计前提。
//
// sanity-revert：删 graph_loop.go 的 CondFailRerunFrom 分支（让它落到 default Abort）
// → 不存 cp → Resume 拿不到进度 → 本测试 fail。
func TestCondFailPolicy_RerunFrom_ResumeRerunsUpstream(t *testing.T) {
	upRuns := 0
	g := buildCondFailGraph(t, "cfp_rerun", &upRuns,
		compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// Run 1：up 产坏数据 → cond 报错 → 存 Pending cp（frontier=[up]）
	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(ctx, g, cm,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("cfp_rerun_sid"))
	if err == nil {
		t.Fatal("Run1: cond 报错应让 Run 失败")
	}

	cp, _ := cper.LoadNS(ctx, "cfp_rerun_sid", "")
	if cp == nil {
		t.Fatal("CondFailRerunFrom 应存 Pending cp 让业务 Resume 重跑 from")
	}
	if len(cp.CurrentFrontier) != 1 || cp.CurrentFrontier[0].NodeID != "up" {
		t.Fatalf("cp.CurrentFrontier 应=[up]（只重跑 cond 的 from，不牵连他人）, got %v", cp.CurrentFrontier)
	}

	// Run 2（Resume）：up 重跑 → 产好数据 → cond 重评通过 → down 执行
	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, g, cm2,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("cfp_rerun_sid"))
	if err != nil {
		t.Fatalf("Run2 (Resume) 应成功（up 重跑产好数据 → cond 重评通过）, got %v", err)
	}
	if upRuns != 2 {
		t.Errorf("up 应跑 2 次（首跑坏数据 + Resume 重跑好数据）, got %d", upRuns)
	}
}
