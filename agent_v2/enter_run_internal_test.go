package agent_v2

import (
	"context"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestEnterRun_SameEntryAsOrchestrations：EnterRun 做 9 个编排 Run 入口做的同一件事——盖任务章、不在任务里时
// 宣告新任务（「新开」信号 + 清掉这个 RunID 名下的 approve_always 规则）、遮住调用方的节点授予。回归用例
// TestRawCalledAgent_AgentLevelHookGetsNoCallerGrant 只钉住「遮授予」那一半：EnterRun 改成只遮授予时全套测试
// 照绿（审计实测），这里把另一半钉住。
func TestEnterRun_SameEntryAsOrchestrations(t *testing.T) {
	approve := func(s *RunSession) {
		markToolApproved(s, &Config{}, schema.ToolCall{Function: schema.FunctionCall{Name: "pay"}}, true)
	}
	approved := func(s *RunSession) bool {
		return isToolApproved(s, &Config{}, schema.ToolCall{Function: schema.FunctionCall{Name: "pay"}})
	}
	callerGrants := withNodeGrants(context.Background(), memberGrants{route: &routeGrant{}})

	t.Run("不在任务里：盖章、宣告新任务、清规则、遮授予", func(t *testing.T) {
		sess := NewRunSession()
		approve(sess)
		ctx, got, err := EnterRun(callerGrants, sess, "x")
		if err != nil || got != sess {
			t.Fatalf("EnterRun: %v（session 应原样返回）", err)
		}
		if task, ok := enclosingTask(ctx); !ok || task != sess.RunID {
			t.Errorf("应盖上这个任务的章：task=%q ok=%v", task, ok)
		}
		if !compose_v2.IsFreshRunCtx(ctx) {
			t.Error("不在任务里应宣告新任务（「新开」信号）")
		}
		if approved(sess) {
			t.Error("新任务开始时应清掉这个 RunID 名下的 approve_always 规则")
		}
		if g, _ := ctx.Value(nodeGrantsKey{}).(*memberGrants); g == nil || g.route != nil || g.handoff != nil {
			t.Error("应遮住调用方的节点授予")
		}
	})

	t.Run("在任务里：只盖章，不宣告新任务、不清规则", func(t *testing.T) {
		sess := NewRunSession()
		approve(sess)
		inTask := stampTask(context.Background(), sess.RunID, false)
		ctx, _, err := EnterRun(inTask, sess, "x")
		if err != nil {
			t.Fatal(err)
		}
		if task, ok := enclosingTask(ctx); !ok || task != sess.RunID {
			t.Errorf("应盖上这个任务的章：task=%q ok=%v", task, ok)
		}
		if compose_v2.IsFreshRunCtx(ctx) {
			t.Error("在任务里不该宣告新任务")
		}
		if !approved(sess) {
			t.Error("在任务里不该清掉规则")
		}
	})
}
