package agent_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// GA-M2（四编排 review 衍生协议修复）回归：9 编排对"显式 Resume 意图 + cp 不
// 存在"统一 fail-fast 返 compose_v2.ErrNoPendingCheckpoint——sid 拼错 /
// cleanupOnSuccess 已清 / 从未跑过都属调用方错误，修复前静默 fresh-run
// （GraphAgent 空 channels 跑出垃圾 final 且 err=nil）。
//
// sanity-revert：删 commonResumePrep 的 ExistsNS 校验块 → GraphAgent 用例显形为
// err=nil + 空跑（其余编排报各自下游错但 errors.Is 不命中哨兵）→ 全表 fail。
func TestGAM2_ResumeNoCp_AllOrchestrationsFailFast(t *testing.T) {
	echo := &fixedReplyAgent{name: "noop_sub", reply: "ok"}

	graphForGA := func() *agent_v2.GraphAgent { return buildR33EchoGraph(t, nil) }

	cases := []struct {
		name  string
		agent agent_v2.Agent
	}{
		{"basic", agent_v2.NewBasicAgent(
			agent_v2.WithName("gam2_basic"),
			agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("ok")})),
		)},
		{"sequential", agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("gam2_seq")).AddStep("s1", echo)},
		{"parallel", agent_v2.NewParallelAgent(agent_v2.WithParallelName("gam2_par")).AddBranch("b1", echo)},
		{"loop", agent_v2.NewLoopAgent(
			agent_v2.WithLoopName("gam2_loop"),
			agent_v2.WithLoopMaxIterations(1),
		).WithBody(echo)},
		{"routing", agent_v2.NewRoutingAgent(
			agent_v2.WithRoutingName("gam2_route"),
			agent_v2.WithRouter(func(_ context.Context, _ *schema.Message) (compose_v2.NodeID, error) { return "b1", nil }),
		).AddBranch("b1", echo)},
		{"reflect", agent_v2.NewReflectAgent(agent_v2.WithReflectName("gam2_reflect")).WithActor(echo)},
		{"handoff", agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("gam2_handoff")).
			AddAgent("a1", echo)},
		{"plan", agent_v2.NewPlanAgent(
			func(_ context.Context, _ *agent_v2.RunSession, _ string, _ []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
				return nil, "done", nil
			},
			agent_v2.PlanName("gam2_plan"),
		)},
		{"graph", graphForGA()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cper := compose_v2.NewInMemoryCheckpointer()
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
			events, err := tc.agent.Resume(ctx, "no_such_sid_gam2")
			if err == nil {
				t.Fatalf("GA-M2: %s.Resume 对不存在 cp 应 fail-fast，got err=nil events=%d（静默空跑）", tc.name, len(events))
			}
			if !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
				t.Fatalf("GA-M2: %s.Resume 应返 ErrNoPendingCheckpoint 哨兵（errors.Is 可判），got %v", tc.name, err)
			}
		})
	}
}

// TestGAM2_ResumeAbort_ExemptFromCpCheck 验证 Abort 豁免：DeleteSubtree 是幂等
// 清理（a2a/server Cancel 协调依赖"框架编排 abort 恒返 nil"）——cp 不存在时
// Abort 仍成功返 nil，不受 ExistsNS fail-fast 影响。
func TestGAM2_ResumeAbort_ExemptFromCpCheck(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	ag := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("gam2_abort")).
		AddStep("s1", &fixedReplyAgent{name: "noop2", reply: "ok"})
	events, err := ag.Resume(ctx, "no_such_sid_abort", agent_v2.WithResumeAction(agent_v2.ResumeActionAbort))
	if err != nil {
		t.Fatalf("GA-M2: Abort 对不存在 cp 应豁免（幂等清理），got %v", err)
	}
	if events != nil {
		t.Fatalf("Abort 路径应返 nil events，got %d", len(events))
	}
}

// TestGAM2_ResumeAfterSuccess_FailsFast 验证灰色场景：Run 成功完成
// （cleanupOnSuccess 删 cp）后再 Resume 同 sid → 确定性报 ErrNoPendingCheckpoint
// （对照 LangGraph 静默 no-op / eino stale 重放——我们的语义最干净）。
func TestGAM2_ResumeAfterSuccess_FailsFast(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	ag := buildR33EchoGraph(t, nil)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(ctx, sess, schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	_, err := ag.Resume(ctx, sess.RunID)
	if !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
		t.Fatalf("GA-M2: 成功完成后再 Resume 应返 ErrNoPendingCheckpoint（cleanupOnSuccess 已清），got %v", err)
	}
}
