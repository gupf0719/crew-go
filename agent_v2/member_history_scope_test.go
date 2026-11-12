package agent_v2_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

type branchStartQuestion struct{ hook.NoOpHooks }

func (branchStartQuestion) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准 B 开始"})
	return ctx, err
}

func TestParallel_ResumeIsolatesCompletedSiblingEvents(t *testing.T) {
	for _, resumeMode := range []string{"fresh", "original", "restored_events"} {
		t.Run(resumeMode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
			// Raw 会保留其他成员的消息，确保本用例实测并行隔离，而非仅靠 SelfOnly 的归属过滤。
			ctx = agent_v2.WithHandoffHistoryModeCtx(ctx, agent_v2.HandoffHistoryRaw)
			a, b, mb := sameNameWorkers(branchStartQuestion{})
			par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("parallel")).AddBranch("a", a).AddBranch("b", b)
			sess := agent_v2.NewRunSession()
			sess.AddEvent(agent_v2.NewOutputEvent("worker", schema.UserMessage("EARLIER-CALLER-EVENT")))
			_, err := par.Run(ctx, sess, schema.UserMessage(laneQ))
			id := compactionQuestionID(t, err, "批准 B 开始")
			if !strings.Contains(inputText(sess.History()), "A-ans") {
				t.Fatal("已成功分支的事件应保留给父会话")
			}
			opts := []agent_v2.ResumeOption{agent_v2.WithResumeUserInputs(map[string]*schema.Message{id: schema.UserMessage("yes")})}
			resumeSession := sess
			if resumeMode == "restored_events" {
				wire, err := json.Marshal(sess.Events())
				if err != nil {
					t.Fatal(err)
				}
				var events []*agent_v2.AgentEvent
				if err := json.Unmarshal(wire, &events); err != nil {
					t.Fatal(err)
				}
				resumeSession = &agent_v2.RunSession{RunID: sess.RunID}
				resumeSession.RestoreEvents(events)
			}
			if resumeMode != "fresh" {
				opts = append(opts, agent_v2.WithResumeSession(resumeSession))
			}
			events, err := par.Resume(ctx, sess.RunID, opts...)
			if err != nil {
				t.Fatal(err)
			}
			final := agent_v2.FinalMessage(events)
			if mb.count() != 1 || final == nil || !strings.Contains(final.Content, "A-ans") || !strings.Contains(final.Content, "B-ans") {
				t.Fatalf("预期两支合并且 B 只执行一次，final=%+v, B=%d", final, mb.count())
			}
			in := inputText(mb.call(0))
			if strings.Contains(in, "A-ans") {
				t.Fatalf("恢复的 B 看到了兄弟 A 的事件，输入=%q", in)
			}
			if resumeMode != "fresh" && !strings.Contains(in, "EARLIER-CALLER-EVENT") {
				t.Fatalf("正常的父会话前文被过滤，输入=%q", in)
			}
		})
	}
}

func TestReflect_SameNameEvaluatorDoesNotWriteActorHistory(t *testing.T) {
	ma := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		reply(schema.AssistantMessage("draft")), reply(schema.AssistantMessage("final")),
	}}
	me := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		reply(schema.AssistantMessage("PRIVATE-CRITIQUE")), reply(schema.AssistantMessage("yes")),
	}}
	actor := agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithModel(ma))
	eval := agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithModel(me))
	r := agent_v2.NewReflectAgent(agent_v2.WithReflectName("reflect"), agent_v2.WithReflectMaxRounds(2),
		agent_v2.WithEvaluator(eval)).WithActor(actor)
	events, err := r.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("用户请求"))
	if err != nil {
		t.Fatal(err)
	}
	final := agent_v2.FinalMessage(events)
	if ma.count() != 2 || me.count() != 2 || final == nil || final.Content != "final" {
		t.Fatalf("预期两轮评估后产出最终答复，actor=%d eval=%d final=%+v", ma.count(), me.count(), final)
	}
	var feedback bool
	for _, msg := range ma.call(1) {
		if msg.Role == schema.RoleAssistant && msg.Content == "PRIVATE-CRITIQUE" {
			t.Error("评估器的答复被当成 actor 的历史")
		}
		feedback = feedback || msg.Role == schema.RoleUser && strings.Contains(msg.Content, "评审反馈：\nPRIVATE-CRITIQUE")
	}
	if !feedback {
		t.Fatal("actor 未收到显式反馈")
	}
}

func TestPlan_TemporaryWorkerHistoryHasOwnTaskAndSlot(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		for _, slot := range []string{"tmp_step_0", fmt.Sprintf("%%tmp-%x-0", "first-task")} {
			t.Run(fmt.Sprintf("persistent=%v/slot=%s", persistent, slot), func(t *testing.T) {
				ctx := t.Context()
				if persistent {
					ctx, _ = historyCtx("plan-history")
				}
				testTemporaryWorkerHistory(t, ctx, slot)
			})
		}
	}
}

func testTemporaryWorkerHistory(t *testing.T, ctx context.Context, registeredName string) {
	t.Helper()
	tmpModel := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		reply(schema.AssistantMessage("TMP-FIRST")), reply(schema.AssistantMessage("TMP-SECOND")),
	}}
	workerModel := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		reply(schema.AssistantMessage("REGISTERED-FIRST")), reply(schema.AssistantMessage("REGISTERED-SECOND")),
	}}
	worker := agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithModel(workerModel))
	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, done []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		switch len(done) {
		case 0:
			return []agent_v2.PlanStep{{Description: "临时任务", TmpSubAgent: &agent_v2.TmpSubAgentConfig{Name: "worker"}}}, "", nil
		case 1:
			return []agent_v2.PlanStep{{Description: "独立任务", SubAgentName: registeredName}}, "", nil
		default:
			return nil, "done", nil
		}
	}
	p := agent_v2.NewPlanAgent(planner, agent_v2.PlanName("plan"), agent_v2.WithPlanModel(tmpModel),
		agent_v2.WithSubAgents(map[string]agent_v2.Agent{registeredName: worker}),
		agent_v2.WithExecInputBuilder(func(_ context.Context, _ string, _ []agent_v2.ExecutedStep, current agent_v2.PlanStep, _ []int) (*schema.Message, error) {
			return schema.UserMessage(current.Description), nil
		}))
	sess := &agent_v2.RunSession{RunID: "first-task"}
	for i, want := range []string{"TMP-FIRST", "TMP-SECOND"} {
		events, err := p.Run(ctx, sess, schema.UserMessage(fmt.Sprintf("请求 %d", i)))
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, ev := range events {
			if ev.Output != nil && ev.Output.Message != nil && ev.Output.Message.Content == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("第 %d 个任务丢失临时成员执行事件", i+1)
		}
		sess.NextTask()
	}
	if tmpModel.count() != 2 || workerModel.count() != 2 {
		t.Fatalf("临时和注册成员应各执行两次，tmp=%d worker=%d", tmpModel.count(), workerModel.count())
	}
	for i := range 2 {
		if in := inputText(workerModel.call(i)); !strings.Contains(in, "独立任务") || strings.Contains(in, "TMP-") {
			t.Fatalf("注册成员读到了其他成员的历史，输入=%q", in)
		}
	}
	if in := inputText(tmpModel.call(1)); !strings.Contains(in, "临时任务") || strings.Contains(in, "TMP-FIRST") || strings.Contains(in, "REGISTERED-FIRST") {
		t.Fatalf("临时成员跨任务复用了其他步骤的历史，输入=%q", in)
	}
	if in := inputText(workerModel.call(1)); !strings.Contains(in, "REGISTERED-FIRST") {
		t.Fatalf("注册成员应看到自己的上一任务历史，输入=%q", in)
	}
}
