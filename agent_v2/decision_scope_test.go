package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// 内部决策点——回合结束闸门、评审 / 评估函数、路由函数、路由决策方、规划器——里请教的 agent：它的对话不进
// 用户的会话历史，它也不取用户的插话。框架自带的决策 agent（分类器、评审 agent）走 runDecisionAgent，早就
// 这样；这里钉的是业务自己写的决策函数在里面请教另一个 agent 的那条路，每个决策点各一条（它们各自给 ctx，
// 漏掉哪一处只有那一处的用例会红）。

const consultReq = "CONSULT_REQ 请判断"
const consultAnswer = "CONSULT_ANSWER 合格"

// consult 经 RunSubAgent 请教一个 helper agent，返回它的答复。
func consult(ctx context.Context, t *testing.T) string {
	t.Helper()
	helper := agent_v2.NewBasicAgent(agent_v2.WithName("helper"),
		agent_v2.WithModel(&scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage(consultAnswer))}}))
	events, err := agent_v2.RunSubAgent(ctx, "consult", helper, schema.UserMessage(consultReq))
	if err != nil {
		t.Errorf("RunSubAgent: %v", err)
		return ""
	}
	if f := agent_v2.FinalMessage(events); f != nil {
		return f.Content
	}
	return ""
}

// assertConsultNotInHistory：历史里有用户的输入（正向断言，防「什么都没记」也算过），没有决策点里请教的对话。
func assertConsultNotInHistory(t *testing.T, hs agent_v2.HistoryStore, ctx context.Context, conv, userInput string) {
	t.Helper()
	msgs := loadHist(t, hs, ctx, conv)
	sawInput := false
	for _, m := range msgs {
		if m.Content == userInput {
			sawInput = true
		}
		if strings.Contains(m.Content, "CONSULT_REQ") || strings.Contains(m.Content, "CONSULT_ANSWER") {
			t.Errorf("决策点里请教的对话不该进会话历史，实际有 %s %q", m.Role, m.Content)
		}
	}
	if !sawInput {
		t.Fatalf("历史里该有用户的输入 %q（否则本用例测不到什么），实际 %d 条", userInput, len(msgs))
	}
}

// historyCtx 是编排顶层用的历史配置（WithHistoryCtx）；BasicAgent 顶层用 basicHistory。
func historyCtx(conv string) (context.Context, agent_v2.HistoryStore) {
	hs := agent_v2.NewInMemoryHistoryStore()
	return agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(context.Background(), conv), hs, "t"), hs
}

// basicHistory 给顶层 BasicAgent 配历史：WithHistory 选项 + 会话 ID。
func basicHistory(conv string) (context.Context, agent_v2.HistoryStore, agent_v2.Option) {
	hs := agent_v2.NewInMemoryHistoryStore()
	return compose_v2.WithThreadIDCtx(context.Background(), conv), hs, agent_v2.WithHistory(hs, "t")
}

// 回合结束闸门里请评审 agent 判断：评审的对话不进会话历史。
//
// sanity-revert：闸门的 ctx 不摘历史 ⟹ 评审的请求与答复写进了历史，本用例红。
func TestDecisionScope_TurnGateConsultNotInHistory(t *testing.T) {
	ctx, hs, withHist := basicHistory("conv-gate")
	consulted := ""
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("gate_host"), withHist,
		agent_v2.WithModel(&scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage("answer"))}}),
		agent_v2.WithTurnGate(func(gctx context.Context, _ agent_v2.TurnGateInput) (string, error) {
			consulted = consult(gctx, t)
			return "", nil
		}))
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatal(err)
	}
	if consulted != consultAnswer {
		t.Fatalf("闸门该请教过 helper，实际答复 %q", consulted)
	}
	assertConsultNotInHistory(t, hs, ctx, "conv-gate", "原话")
}

// ReflectAgent 的 Reflector 函数里请教另一个 agent：那段对话不进会话历史（同评审 agent 那一档）。
//
// sanity-revert：评审函数的 ctx 不摘历史 ⟹ 请教的对话写进了历史，本用例红。
func TestDecisionScope_ReflectorConsultNotInHistory(t *testing.T) {
	ctx, hs := historyCtx("conv-reflector")
	actor := agent_v2.NewBasicAgent(agent_v2.WithName("actor"),
		agent_v2.WithModel(&scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage("draft"))}}))
	r := agent_v2.NewReflectAgent(agent_v2.WithReflectName("reflect_fn"), agent_v2.WithReflectMaxRounds(1),
		agent_v2.WithReflector(func(rctx context.Context, _, _ *schema.Message) (*agent_v2.ReflectOpinion, error) {
			consult(rctx, t)
			return &agent_v2.ReflectOpinion{GoalMatched: true}, nil
		})).WithActor(actor)
	if _, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatal(err)
	}
	assertConsultNotInHistory(t, hs, ctx, "conv-reflector", "原话")
}

// ReflectAgent 的 EvalFunc 里请教另一个 agent：同上。
func TestDecisionScope_EvalFuncConsultNotInHistory(t *testing.T) {
	ctx, hs := historyCtx("conv-evalfn")
	actor := agent_v2.NewBasicAgent(agent_v2.WithName("actor"),
		agent_v2.WithModel(&scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage("draft"))}}))
	r := agent_v2.NewReflectAgent(agent_v2.WithReflectName("reflect_eval"), agent_v2.WithReflectMaxRounds(1),
		agent_v2.WithEvalFunc(func(ectx context.Context, _ *schema.Message) (bool, error) {
			consult(ectx, t)
			return true, nil
		})).WithActor(actor)
	if _, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatal(err)
	}
	assertConsultNotInHistory(t, hs, ctx, "conv-evalfn", "原话")
}

// RoutingAgent 的路由函数（WithRouter）里请教另一个 agent：同分类器那一档，不进会话历史。
//
// sanity-revert：路由函数的 ctx 不摘历史 ⟹ 请教的对话写进了历史，本用例红。
func TestDecisionScope_RouterConsultNotInHistory(t *testing.T) {
	ctx, hs := historyCtx("conv-router")
	branch := agent_v2.NewBasicAgent(agent_v2.WithName("branch"),
		agent_v2.WithModel(&scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage("done"))}}))
	rt := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("rt_fn"),
		agent_v2.WithRouter(func(rctx context.Context, _ *schema.Message) (compose_v2.NodeID, error) {
			consult(rctx, t)
			return "b", nil
		})).AddBranch("b", branch)
	if _, err := rt.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatal(err)
	}
	assertConsultNotInHistory(t, hs, ctx, "conv-router", "原话")
}

// probeAgent 是决策函数里直接（不经 RunSubAgent）用这个任务的 session 跑的 BasicAgent：它的模型看到了什么。
func probeAgent(name string) (agent_v2.Agent, *scriptModel) {
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage("probe"))}}
	return agent_v2.NewBasicAgent(agent_v2.WithName(name), agent_v2.WithModel(m)), m
}

// 路由决策方里直接跑一个 BasicAgent：它不取用户的插话，插话留给被路由到的那个分支。
//
// sanity-revert：决策方的 ctx 不摘收件箱 ⟹ 决策方里的 agent 把插话取走了、分支看不到，本用例红。
func TestDecisionScope_RouteDeciderDoesNotTakeInterjection(t *testing.T) {
	inbox := agent_v2.NewInbox()
	mustPush(t, inbox, "用英文")
	sess := agent_v2.NewRunSession()
	probe, probeModel := probeAgent("decider_probe")
	branchModel := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage("done"))}}
	branch := agent_v2.NewBasicAgent(agent_v2.WithName("branch"), agent_v2.WithModel(branchModel))
	rt := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("rt_decider"),
		agent_v2.WithRouteDecider(func(dctx context.Context, req agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
			if _, err := probe.Run(dctx, sess, req.Input); err != nil {
				return agent_v2.RouteDecision{}, err
			}
			return agent_v2.RouteDecision{Target: "b", Confidence: 1}, nil
		}, 0)).AddBranch("b", branch)
	if _, err := rt.Run(agent_v2.WithInboxCtx(context.Background(), inbox), sess, schema.UserMessage("原话")); err != nil {
		t.Fatal(err)
	}
	if probeModel.count() != 1 {
		t.Fatalf("决策方里的 agent 该跑过一次，实际 %d 次", probeModel.count())
	}
	if hasInterjection(probeModel.call(0), "用英文") {
		t.Errorf("决策方里的 agent 不该取走用户的插话，实际 %+v", probeModel.call(0))
	}
	if !hasInterjection(branchModel.call(0), "用英文") {
		t.Errorf("插话该留给被路由到的分支，实际 %+v", branchModel.call(0))
	}
}

// 规划器里直接跑一个 BasicAgent：它不取用户的插话，插话留给执行步骤的那个成员。
//
// sanity-revert：规划器的 ctx 不摘收件箱 ⟹ 规划器里的 agent 把插话取走了，本用例红。
func TestDecisionScope_PlannerDoesNotTakeInterjection(t *testing.T) {
	inbox := agent_v2.NewInbox()
	mustPush(t, inbox, "用英文")
	probe, probeModel := probeAgent("planner_probe")
	workerModel := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage("did it"))}}
	worker := agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithModel(workerModel))
	planner := func(pctx context.Context, sess *agent_v2.RunSession, input string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) > 0 {
			return nil, "all done", nil
		}
		if _, err := probe.Run(pctx, sess, schema.UserMessage("plan: "+input)); err != nil {
			return nil, "", err
		}
		return []agent_v2.PlanStep{{Description: "do it", SubAgentName: "worker"}}, "", nil
	}
	plan := agent_v2.NewPlanAgent(planner, agent_v2.WithSubAgents(map[string]agent_v2.Agent{"worker": worker}))
	if _, err := plan.Run(agent_v2.WithInboxCtx(context.Background(), inbox), agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatal(err)
	}
	if probeModel.count() != 1 {
		t.Fatalf("规划器里的 agent 该跑过一次，实际 %d 次", probeModel.count())
	}
	if hasInterjection(probeModel.call(0), "用英文") {
		t.Errorf("规划器里的 agent 不该取走用户的插话，实际 %+v", probeModel.call(0))
	}
	if !hasInterjection(workerModel.call(0), "用英文") {
		t.Errorf("插话该留给执行步骤的成员，实际 %+v", workerModel.call(0))
	}
}

// 影子决策档（WithShadow）里直接跑一个 BasicAgent：同真实的决策档，不取用户的插话。
//
// sanity-revert：影子档的 ctx 不摘收件箱 ⟹ 影子档里的 agent 把插话取走了，本用例红。
func TestDecisionScope_ShadowDeciderDoesNotTakeInterjection(t *testing.T) {
	inbox := agent_v2.NewInbox()
	mustPush(t, inbox, "用英文")
	sess := agent_v2.NewRunSession()
	probe, probeModel := probeAgent("shadow_probe")
	branchModel := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage("done"))}}
	branch := agent_v2.NewBasicAgent(agent_v2.WithName("branch"), agent_v2.WithModel(branchModel))
	rt := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("rt_shadow"),
		agent_v2.WithRouteDecider(func(dctx context.Context, req agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
			if _, err := probe.Run(dctx, sess, req.Input); err != nil {
				return agent_v2.RouteDecision{}, err
			}
			return agent_v2.RouteDecision{Target: "b", Confidence: 1}, nil
		}, 0, agent_v2.WithShadow()),
		agent_v2.WithRouteDecider(func(context.Context, agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
			return agent_v2.RouteDecision{Target: "b", Confidence: 1}, nil
		}, 0)).AddBranch("b", branch)
	if _, err := rt.Run(agent_v2.WithInboxCtx(context.Background(), inbox), sess, schema.UserMessage("原话")); err != nil {
		t.Fatal(err)
	}
	if probeModel.count() != 1 {
		t.Fatalf("影子档里的 agent 该跑过一次，实际 %d 次", probeModel.count())
	}
	if hasInterjection(probeModel.call(0), "用英文") {
		t.Errorf("影子档里的 agent 不该取走用户的插话，实际 %+v", probeModel.call(0))
	}
	if !hasInterjection(branchModel.call(0), "用英文") {
		t.Errorf("插话该留给被路由到的分支，实际 %+v", branchModel.call(0))
	}
}

// LoopAgent 的退出判断函数（WithLoopExitCondition）里请教另一个 agent：它是内部决策，同评审函数，不进会话历史。
//
// sanity-revert：退出判断的 ctx 不摘历史 ⟹ 请教的对话写进了历史，本用例红。
func TestDecisionScope_LoopExitConditionConsultNotInHistory(t *testing.T) {
	ctx, hs := historyCtx("conv-loop-exit")
	body := agent_v2.NewBasicAgent(agent_v2.WithName("loop_body"),
		agent_v2.WithModel(&scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage("draft"))}}))
	loop := agent_v2.NewLoopAgent(agent_v2.WithLoopName("loop_exit"), agent_v2.WithLoopMaxIterations(2),
		agent_v2.WithLoopExitCondition(func(ectx context.Context, _ *schema.Message, _ int) (bool, error) {
			consult(ectx, t)
			return true, nil
		})).WithBody(body)
	if _, err := loop.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatal(err)
	}
	assertConsultNotInHistory(t, hs, ctx, "conv-loop-exit", "原话")
}
