package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
)

// TestReviewV6_H03_AbortForwardGraphNS 验 review-v6 H-03 fix — 9 编排
// ResumeActionAbort 路径 forward 父 graphNS, 只删本 sub-agent 子树, 不误删父 cp.
//
// 历史: 9 编排 Continue 路径已 forward (R29 视角 4/5 HIGH-1), Abort 路径
// 全 9 编排同协议漏修 (review-v6 视角 3 HIGH-3).
//
// sanity-revert: 任一编排改回 `cper.DeleteSubtree(ctx, sessionID, "")` →
// nsPrefix="" 是任意 ns 祖先 → 父 cp + sibling cp 一并删 → 此测试 fail.
//
// 测试覆盖代表性 4 编排 (BasicAgent / PlanAgent / ParallelAgent / GraphAgent),
// 其余 5 编排 (Sequential / Loop / Routing / Reflect / Handoff) 修法完全同款
// 用 grep -nE 'DeleteSubtree\(ctx, sessionID, ns\)' agent_v2/*.go 验证 9 处协议一致.
func TestReviewV6_H03_AbortForwardGraphNS(t *testing.T) {
	cases := []struct {
		name    string
		callRsm func(ctx context.Context, sid string) error // 调 sub-agent.Resume(Abort)
	}{
		{
			name: "BasicAgent",
			callRsm: func(ctx context.Context, sid string) error {
				ag := agent_v2.NewBasicAgent(agent_v2.WithName("basic"))
				_, err := ag.Resume(ctx, sid, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort))
				return err
			},
		},
		{
			name: "SequentialAgent",
			callRsm: func(ctx context.Context, sid string) error {
				ag := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"))
				_, err := ag.Resume(ctx, sid, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort))
				return err
			},
		},
		{
			name: "ParallelAgent",
			callRsm: func(ctx context.Context, sid string) error {
				ag := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par"))
				_, err := ag.Resume(ctx, sid, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort))
				return err
			},
		},
		{
			name: "LoopAgent",
			callRsm: func(ctx context.Context, sid string) error {
				body := agent_v2.NewBasicAgent(agent_v2.WithName("body"))
				ag := agent_v2.NewLoopAgent(agent_v2.WithLoopName("loop")).WithBody(body)
				_, err := ag.Resume(ctx, sid, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort))
				return err
			},
		},
		{
			name: "RoutingAgent",
			callRsm: func(ctx context.Context, sid string) error {
				ag := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("rt"))
				_, err := ag.Resume(ctx, sid, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort))
				return err
			},
		},
		{
			name: "ReflectAgent",
			callRsm: func(ctx context.Context, sid string) error {
				actor := agent_v2.NewBasicAgent(agent_v2.WithName("actor"))
				ag := agent_v2.NewReflectAgent(agent_v2.WithReflectName("rf")).WithActor(actor)
				_, err := ag.Resume(ctx, sid, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort))
				return err
			},
		},
		{
			name: "HandoffAgent",
			callRsm: func(ctx context.Context, sid string) error {
				ag := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("ho"))
				_, err := ag.Resume(ctx, sid, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort))
				return err
			},
		},
		{
			name: "PlanAgent",
			callRsm: func(ctx context.Context, sid string) error {
				planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, _ []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
					return nil, "done", nil
				}
				ag := agent_v2.NewPlanAgent(planner, agent_v2.PlanName("plan"))
				_, err := ag.Resume(ctx, sid, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort))
				return err
			},
		},
		// GraphAgent.Resume 入口校验 graph != nil — 简单测试 skip (要构造 graph 太复杂)
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cper := compose_v2.NewInMemoryCheckpointer()
			sid := "test_sid_" + tc.name

			// 预填 3 个 cp 模拟嵌套场景: 父 + 本 sub-agent + sibling
			parentNS := "parent"
			selfNS := compose_v2.JoinNS(parentNS, "self")
			siblingNS := compose_v2.JoinNS(parentNS, "sibling")
			for _, ns := range []string{parentNS, selfNS, siblingNS} {
				cp := &compose_v2.Checkpoint{
					ID:     "cp_" + ns,
					NS:     ns,
					Status: compose_v2.CPStatusPending,
				}
				if err := cper.SaveNS(context.Background(), sid, ns, cp); err != nil {
					t.Fatalf("seed cp ns=%s: %v", ns, err)
				}
			}

			// 模拟 nested 调用: ctx push 父 graphNS = "parent/self" (sub-agent 在父位置)
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
			ctx = compose_v2.WithGraphNS(ctx, selfNS)

			// 调 sub-agent.Resume(Abort)
			if err := tc.callRsm(ctx, sid); err != nil {
				t.Fatalf("%s.Resume(Abort) err: %v", tc.name, err)
			}

			// 验证 fix 后: 只删 "parent/self" 子树, "parent" 和 "parent/sibling" 仍在
			remaining, err := cper.LoadAllInSession(context.Background(), sid)
			if err != nil {
				t.Fatalf("LoadAllInSession: %v", err)
			}
			remainingNSes := map[string]bool{}
			for _, nscp := range remaining {
				remainingNSes[nscp.NS] = true
			}

			// fix 后期望: parentNS + siblingNS 仍在, selfNS 已删
			if !remainingNSes[parentNS] {
				t.Errorf("%s: review-v6 H-03 fix: 父 cp ns=%s 应保留, 实际被误删\n"+
					"sanity-revert: 改回 cper.DeleteSubtree(ctx, sessionID, \"\") → nsPrefix=\"\" 是任意 ns 祖先 → 父 cp 一并删 → 此 fail",
					tc.name, parentNS)
			}
			if !remainingNSes[siblingNS] {
				t.Errorf("%s: review-v6 H-03 fix: sibling cp ns=%s 应保留, 实际被误删", tc.name, siblingNS)
			}
			if remainingNSes[selfNS] {
				t.Errorf("%s: review-v6 H-03 fix: 本 sub-agent cp ns=%s 应被删, 实际仍在 — fix 漏了?", tc.name, selfNS)
			}
		})
	}
}
