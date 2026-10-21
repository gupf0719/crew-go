package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// R34（audit⁴ 探针回灌）：Reflect 拆图 Resume 矩阵补仓内零覆盖的两格——
// (a) actor 轮 1+ 中断（轮 0 evaluate 已 commit 不重评）
// (b) evaluate 内中断（actor 不重跑、roundCh 不双加、critique fallback 流转）。
// 矩阵 (c) feedback 中断 = r33_reflect_split_test.go；(d) 跨轮二次中断 =
// nested_smoke "reflect actor + reflector 跨轮 HITL" binary 冒烟。

// buildAskEvaluator 真 BasicAgent evaluator + ask tool：轮 0 评估前先问人
// （中断），拿到答案后产 "不行，缺 X"（critique）；轮 1 直接 "yes"。
func buildAskEvaluator() *agent_v2.BasicAgent {
	askTool := tool.NewFuncTool(
		"ev_ask",
		"ask user before judging",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "ev_ask_id"); ok {
				return tool.Success("评审依据：" + ans), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "ev_ask_id", Question: "评审标准？"},
			}
		},
	)
	tk := tool.NewToolkit()
	tk.Register(askTool)
	tc := schema.ToolCall{ID: "ev_tc", Function: schema.FunctionCall{Name: "ev_ask", Arguments: `{}`}}
	return agent_v2.NewBasicAgent(
		agent_v2.WithName("ask_judge"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{tc}), // 轮 0：先问人 → 中断
			schema.AssistantMessage("不行，缺 X"),                      // 拿到答案 → 不过 + critique
			schema.AssistantMessage("yes"),                         // 轮 1：过
		})),
		agent_v2.WithToolkit(tk),
	)
}

// TestR34_ReflectMatrix_EvaluateInterrupt_ActorNotRerun 矩阵 (b)：
// evaluator 内 ask_user 中断 → Resume 重跑 evaluate 节点——actor **不重跑**
// （actorOut 已 commit）、evaluator 从子 cp 续跑、roundCh 不双加；轮 1 actor
// input 含 critique fallback（无 reflectorAgent → critique 全文作评审反馈）。
func TestR34_ReflectMatrix_EvaluateInterrupt_ActorNotRerun(t *testing.T) {
	actor := &countingAgent{inner: &seqReplyAgent{name: "w_b", replies: []string{"DRAFT_1", "DRAFT_2"}}}
	evaluator := buildAskEvaluator()

	ra := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("r34_b"),
		agent_v2.WithEvaluator(evaluator),
		agent_v2.WithReflectMaxRounds(3),
	).WithActor(actor)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := ra.Run(ctx, sess, schema.UserMessage("GOAL_B"))
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("expect evaluator gate interrupt, got %v", err)
	}
	if actor.calls != 1 {
		t.Fatalf("中断时 actor 应已跑 1 次, got %d", actor.calls)
	}

	events, err := ra.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("按完整性")}))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	// 矩阵 (b) 核心：Resume 重跑 evaluate 节点不重跑 actor——actor 总计恰 2 次
	//（轮 0 一次 + 轮 1 一次；若 actor 被重跑会是 3 次）。
	if actor.calls != 2 {
		t.Fatalf("矩阵 b: actor 轮 0 不应重跑（actorOut 已 commit），总计应 2 次 got %d", actor.calls)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "DRAFT_2") {
		t.Fatalf("final 应是轮 1 产物 DRAFT_2, got %v", final)
	}
	// roundCh 不双加（evaluate 中断时 writes 不 commit；重跑成功后 +1 一次/轮）
	if v, _ := final.Annotations["reflect_rounds"].(int); v != 2 {
		t.Fatalf("reflect_rounds 应 2（roundCh 无双加），got %v", final.Annotations["reflect_rounds"])
	}
	// 轮 1 actor input 含 critique fallback（RF-M2：无 reflectorAgent 时
	// evaluator 批评全文作评审反馈——原实现恒留空）
	if actor.last == nil || !strings.Contains(actor.last.Content, "不行，缺 X") {
		t.Fatalf("轮 1 actor input 应含 evaluator critique fallback，got %v", actor.last)
	}
}

// TestR34_ReflectMatrix_ActorRound1Interrupt 矩阵 (a) 轮 1+（compose 黑盒）：
// 轮 0 完整走完（evaluate 不过 + feedback 合成 nextInput 已 commit）→ 轮 1
// actor 内中断 → Resume 重跑 actor 轮 1——input 正确（goal+上轮输出+反馈）、
// evaluate 轮 0 不重评、cp.CurrentFrontier=[actor]。
func TestR34_ReflectMatrix_ActorRound1Interrupt(t *testing.T) {
	actorCalls := 0
	evalCalls := 0
	round1Input := ""

	actor := func(ctx context.Context, input *schema.Message) (*schema.Message, error) {
		actorCalls++
		if strings.Contains(input.Content, "ADD_X") { // 轮 1（feedback 合成 input）
			round1Input = input.Content
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "r34a_id"); ok {
				return schema.AssistantMessage("DRAFT_2_with_" + ans), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "r34a_id", Question: "round1 详情?"},
			}
		}
		return schema.AssistantMessage("DRAFT_1"), nil
	}
	reflector := func(_ context.Context, goal, out *schema.Message) (*agent_v2.ReflectOpinion, error) {
		evalCalls++
		if goal == nil || !strings.Contains(goal.Content, "GOAL_A") {
			t.Errorf("R33 Reflector 应收到 goal, got %v", goal)
		}
		if out.Content == "DRAFT_1" {
			return &agent_v2.ReflectOpinion{GoalMatched: false, Opinion: "ADD_X"}, nil
		}
		return &agent_v2.ReflectOpinion{GoalMatched: true}, nil
	}

	g, err := agent_v2.NewReflectAgentBuilder().WithName("r34_a").
		WithActor(actor).WithReflector(reflector).WithMaxRounds(3).Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	cm := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, g, cm,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("r34a_sid"),
		compose_v2.WithInitialWrites(
			compose_v2.Write(agent_v2.ReflectAgentOriginalChannel(), schema.UserMessage("GOAL_A")),
			compose_v2.Write(agent_v2.ReflectAgentInputChannel(), schema.UserMessage("GOAL_A")),
		))
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("expect actor 轮 1 interrupt, got %v", err)
	}
	if actorCalls != 2 || evalCalls != 1 {
		t.Fatalf("中断时 actor=2（轮0+轮1尝试） eval=1, got actor=%d eval=%d", actorCalls, evalCalls)
	}
	if got := agent_v2.GetReflectAgentRound(cm); got != 1 {
		t.Fatalf("轮 0 evaluate 已 commit → round=1, got %d", got)
	}
	if !strings.Contains(round1Input, "ADD_X") || !strings.Contains(round1Input, "DRAFT_1") || !strings.Contains(round1Input, "GOAL_A") {
		t.Fatalf("轮 1 input 应含 goal+上轮输出+评审反馈, got %q", round1Input)
	}
	cp, _ := cper.LoadNS(ctx, "r34a_sid", "")
	if cp == nil || len(cp.CurrentFrontier) != 1 || cp.CurrentFrontier[0].NodeID != "actor" {
		t.Fatalf("cp.CurrentFrontier 应 [actor], got %+v", cp)
	}

	// Resume：只重跑 actor 轮 1，evaluate 轮 0 不重评
	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, g, cm2,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("r34a_sid"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{"r34a_id": schema.UserMessage("DETAIL")}))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if actorCalls != 3 {
		t.Fatalf("Resume 重跑 actor 轮 1 一次（总 3），got %d", actorCalls)
	}
	if evalCalls != 2 {
		t.Fatalf("矩阵 a: evaluate 轮 0 不重评（总 2 = 轮0 + 轮1 正常评估），got %d", evalCalls)
	}
	final := agent_v2.ReflectAgentFinalMessage(cm2.Channels())
	if final == nil || !strings.Contains(final.Content, "DRAFT_2_with_DETAIL") {
		t.Fatalf("final 应含用户答案, got %v", final)
	}
}
