package agent_v2_test

// 守「开了陈旧写回检测之后，正常路径还得能用」。
//
// **为什么是 agent_v2 这一层**：`enterRunTask` 让每个顶层 Run 恒 `WithFreshRun`
// （那是任务隔离要的），而 fresh run 刻意不载入 ⟹ 恒无基线。守卫的「无基线就拒绝删除」
// 于是对着一个**空槽**触发保护——把每一个跑成功的顶层 Run 降级成 `ErrCheckpointStale`。
// 一个 opt-in 的观测功能，开了之后正常路径全废。
//
// ⚠️ **2026-08-12 之后这条守的是另一件事**：无基线拒删那条分支已整个撤掉（`RunSession.RunID`
// 的契约把「这个槽归谁」定死了，那条极性的立论没了），所以它不再是「机制哨兵的症状层」，
// 而是**「顶层 Run 开着守卫必须照常跑通」这条不变量的唯一守门人**。compose_v2 那半是
// `TestStaleWriteGuard_NoBaselineDoesNotBlockCleanup`。这条守的是**症状**：
// 业务实际会怎么撞上它。两层都要有——只有机制那条的话，将来谁把顶层的 fresh 语义改了、
// 或者在 agent_v2 侧另加一条无基线路径，机制那条照样绿。

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestStaleGuard_SuccessfulTopLevelRunNotDowngraded 开着守卫跑几个编排的成功路径，
// 一个都不该返错。
//
// sanity-revert：删 `compose_v2/durability.go::checkStale` 里
// `if probeErr == nil && empty { return nil }` 那三行 → 每个子用例都 fail，
// err 是 `checkpoint stale: ... purge-success 无基线可比`。
func TestStaleGuard_SuccessfulTopLevelRunNotDowngraded(t *testing.T) {
	mk := func(name, reply string) *agent_v2.BasicAgent {
		return agent_v2.NewBasicAgent(agent_v2.WithName(name),
			agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage(reply)})))
	}

	cases := []struct {
		name  string
		build func() agent_v2.Agent
	}{
		{"BasicAgent", func() agent_v2.Agent { return mk("basic", "done") }},
		{"SequentialAgent", func() agent_v2.Agent {
			return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
				AddStep("s1", mk("a1", "X")).AddStep("s2", mk("a2", "Y"))
		}},
		{"ParallelAgent", func() agent_v2.Agent {
			return agent_v2.NewParallelAgent(agent_v2.WithParallelName("par"),
				agent_v2.WithAggregator(func(_ context.Context, m map[string]string) (*schema.Message, error) {
					return schema.AssistantMessage(m["b1"] + m["b2"]), nil
				})).AddBranch("b1", mk("a1", "X")).AddBranch("b2", mk("a2", "Y"))
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cper := compose_v2.NewInMemoryCheckpointer()
			ctx := compose_v2.WithStaleWriteGuardCtx(
				compose_v2.WithCheckpointerCtx(context.Background(), cper))

			if _, err := tc.build().Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
				t.Fatalf("开了 WithStaleWriteGuardCtx 之后，一个**完全成功**的顶层 Run 返错了: %v\n"+
					"顶层恒 WithFreshRun ⟹ 恒无基线；无基线的拒绝必须先看子树里有没有东西需要保护", err)
			}
		})
	}
}
