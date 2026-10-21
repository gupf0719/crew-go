package agent_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// PlanAgent 的图身份必须跨**对象重建**稳定。
//
// `Graph.ID()` 第一行是「有 name 就返回 name」，而 `NewPlanAgent` 不设名时自动生成
// `plan_agent_<uuid8>`——**每次构造都不同**。它被交给 graph 当身份之后，cp 里存的
// GraphID 就是那个随机串：业务重建 PlanAgent 对象再续同一个会话，必撞
// `ErrGraphIDConflict`。而「每请求新建 agent 对象」「进程重启后重建」都是常规写法。
//
// 修法：只有业务显式 `PlanName(...)` 的名字才交给 graph；自动名回落 fingerprint
// ——跟 BasicAgent 一致（它的自动名同样不进 graph）。
//
// ⚠️ **必须让它中断**：Run 跑完会 cleanupOnSuccess 把 cp 清掉，没有 cp 就无从冲突，
// 用例会变成空的（第一版就是这么写的，带着 bug 也绿）。
func TestPlanAgent_GraphIDStableAcrossRebuild(t *testing.T) {
	build := func() *agent_v2.PlanAgent {
		p := agent_v2.NewPlanAgent(agent_v2.Planner(gidPlanner)) // 有意不设名
		p.AddSubAgent("w", &gidInterruptAgent{name: "worker"})
		return p
	}

	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	_, err := build().Run(ctx, sess, schema.UserMessage("原话"))
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("第一次 Run 应中断（要留下 cp 才谈得上身份冲突）；实际 err=%v", err)
	}
	// 换一个**新构造**的对象接着这个会话——业务重启 / 每请求新建都是这个形状。
	_, err = build().Resume(ctx, sess.RunID)
	if errors.Is(err, compose_v2.ErrGraphIDConflict) {
		t.Fatalf("重建 PlanAgent 对象后撞 GraphID 冲突——自动名（每次构造都不同）被当成了" +
			"图的持久身份，业务每请求新建 agent 对象就会踩")
	}
}

// 对照组：显式设名时那个名字仍是图身份。
//
// 没有这一条，上面那个用例可以靠「干脆永远不把名字给 graph」通过，等于把显式命名的
// 作用一并删掉——而显式名的用处正是让 sub-agent 集合变更不改指纹。
func TestPlanAgent_ExplicitNameIsGraphIdentity(t *testing.T) {
	mk := func(name string, extraSub bool) *agent_v2.PlanAgent {
		p := agent_v2.NewPlanAgent(agent_v2.Planner(gidPlanner), agent_v2.PlanName(name))
		p.AddSubAgent("w", &gidInterruptAgent{name: "worker"})
		if extraSub {
			// 改 sub-agent 集合 ⟹ 节点集变 ⟹ fingerprint 会变。显式名让身份不受影响。
			p.AddSubAgent("w2", &gidInterruptAgent{name: "worker2"})
		}
		return p
	}

	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)

	sess := agent_v2.NewRunSession()
	_, err := mk("myplan", false).Run(ctx, sess, schema.UserMessage("原话"))
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("第一次 Run 应中断；实际 err=%v", err)
	}
	if _, err := mk("myplan", true).Resume(ctx, sess.RunID); errors.Is(err, compose_v2.ErrGraphIDConflict) {
		t.Errorf("同名但 sub-agent 集合变了，显式名该让身份不变，实际撞了冲突：%v", err)
	}

	// 不同名字必须是不同身份——冲突守卫要能报出来。
	sess2 := agent_v2.NewRunSession()
	_, err = mk("planA", false).Run(ctx, sess2, schema.UserMessage("原话"))
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("planA Run 应中断；实际 err=%v", err)
	}
	_, err = mk("planB", false).Resume(ctx, sess2.RunID)
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) {
		t.Errorf("两个不同名的 PlanAgent 共用一个 sid 应报 GraphID 冲突，实际：%v", err)
	}
}

// gidPlanner 出一步固定计划，执行过一步就收尾。
func gidPlanner(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
	if len(executed) > 0 {
		return nil, "完成", nil
	}
	return []agent_v2.PlanStep{{Description: "干一件事", SubAgentName: "w"}}, "", nil
}

// gidInterruptAgent 恒中断——用来留下 cp。
type gidInterruptAgent struct{ name string }

func (a *gidInterruptAgent) Run(context.Context, *agent_v2.RunSession, *schema.Message, ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	return nil, &compose_v2.InterruptError{
		Reason: "need input",
		Query:  &compose_v2.AskQuery{InterruptID: "gid-q1", Question: "确认？"},
	}
}

func (a *gidInterruptAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *gidInterruptAgent) Name() string        { return a.name }
func (a *gidInterruptAgent) Description() string { return "恒中断的执行体" }
