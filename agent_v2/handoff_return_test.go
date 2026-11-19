package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

func TestHandoffDeterministicReturnResumesSupervisor(t *testing.T) {
	const group, owner, thread = "supervised", "inventory-team", "stock-review"
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx = compose_v2.WithCheckpointerCtx(compose_v2.WithThreadIDCtx(compose_v2.WithOwner(ctx, owner), thread), cp)
	activeStore := store.NewInMemoryStore()
	ctx = agent_v2.WithHistoryCtx(store.WithContext(ctx, activeStore), agent_v2.NewInMemoryHistoryStore(), owner)

	var workerCalls, writes atomic.Int32
	workerTools := tool.NewToolkit()
	workerTools.Register(tool.NewFuncTool("record_inventory", "记录库存", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			writes.Add(1)
			return tool.Success("inventory written"), nil
		}))
	worker := agent_v2.NewBasicAgent(agent_v2.WithName("inventory-worker"), agent_v2.WithToolkit(workerTools),
		agent_v2.WithModel(&fnModel{fn: func(_ context.Context, messages []*schema.Message) (*schema.Message, error) {
			workerCalls.Add(1)
			for _, message := range messages {
				if schema.GetToolResultID(message) == "write" && strings.Contains(message.Content, "inventory written") {
					return schema.AssistantMessage("worker recorded inventory"), nil
				}
			}
			return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "write", Type: "function",
				Function: schema.FunctionCall{Name: "record_inventory", Arguments: `{}`}}}), nil
		}}))
	var supervisorCalls atomic.Int32
	supervisor := agent_v2.NewBasicAgent(agent_v2.WithName("inventory-supervisor"), agent_v2.WithHandoffTransfer(),
		agent_v2.WithModel(&fnModel{fn: func(ctx context.Context, messages []*schema.Message) (*schema.Message, error) {
			if supervisorCalls.Add(1) == 1 {
				return transferToolCall("dispatch", "worker"), nil
			}
			for _, message := range messages {
				if strings.Contains(message.Content, "worker recorded inventory") {
					answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认库存记录？"})
					if err != nil {
						return nil, err
					}
					return schema.AssistantMessage("supervisor: " + answer.Content), nil
				}
			}
			return nil, errors.New("supervisor did not receive worker history")
		}}))
	build := func(returnTo string) *agent_v2.HandoffAgent {
		return agent_v2.NewHandoffAgent(agent_v2.WithHandoffName(group), agent_v2.WithHandoffInitial("supervisor"),
			agent_v2.WithDeterministicReturnTo(returnTo)).AddAgent("supervisor", supervisor).AddAgent("worker", worker)
	}
	h := build("supervisor")
	session := agent_v2.NewRunSession()
	_, err := h.Run(ctx, session, schema.UserMessage("盘点"))
	interrupt, ok := compose_v2.IsInterrupt(err)
	if !ok || len(interrupt.AllQueries()) != 1 || interrupt.AllQueries()[0].Question != "确认库存记录？" {
		t.Fatalf("worker 应退回到等待确认的 supervisor: %v", err)
	}
	if writes.Load() != 1 || workerCalls.Load() != 2 || supervisorCalls.Load() != 2 {
		t.Fatalf("挂起前写库存 %d 次，调 worker 模型 %d 次，调 supervisor 模型 %d 次",
			writes.Load(), workerCalls.Load(), supervisorCalls.Load())
	}
	activeNS := store.NS("__handoff_active__", group, owner, thread)
	if item, err := activeStore.Get(ctx, activeNS, "active"); err != nil || item != nil {
		t.Fatalf("挂起任务提前提交了 active: item=%v err=%v", item, err)
	}
	if state := h.State(session); state.Current != "supervisor" || state.Hops != 2 || state.Done || state.Exhausted {
		t.Fatalf("挂起时退回状态不正确: %+v", state)
	}
	if _, err := build("").Resume(ctx, session.RunID); !errors.Is(err, compose_v2.ErrGraphIDConflict) {
		t.Fatalf("更换退回策略应拒绝续跑: %v", err)
	}
	resumed := build("supervisor")
	events, err := resumed.Resume(ctx, session.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		interrupt.AllQueries()[0].InterruptID: schema.UserMessage("确认"),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "supervisor: 确认" || writes.Load() != 1 || workerCalls.Load() != 2 {
		t.Fatalf("续跑结果=%v，库存写入次数=%d，worker 模型调用次数=%d", final, writes.Load(), workerCalls.Load())
	}
	if item, err := activeStore.Get(ctx, activeNS, "active"); err != nil || item == nil || item.Value != "supervisor" {
		t.Fatalf("完成后应提交 supervisor active: item=%v err=%v", item, err)
	}
	if left, err := cp.LoadAllInSession(ctx, session.RunID); err != nil || len(left) != 0 {
		t.Fatalf("续跑完成后剩余存档=%d err=%v", len(left), err)
	}
}

func TestHandoffDeterministicReturnFollowsExplicitTransfer(t *testing.T) {
	worker := newProgAgent("worker", "delegated", "specialist")
	specialist := newProgAgent("specialist", "specialist evidence", "")
	supervisor := newProgAgent("supervisor", "supervised", "")
	h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("explicit-then-return"), agent_v2.WithHandoffInitial("worker"),
		agent_v2.WithDeterministicReturnTo("supervisor")).
		AddAgent("worker", worker).AddAgent("specialist", specialist).AddAgent("supervisor", supervisor)
	session := agent_v2.NewRunSession()
	events, err := h.Run(t.Context(), session, schema.UserMessage("review"))
	if err != nil {
		t.Fatal(err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || final.Content != "supervised" || specialist.runs != 1 || supervisor.runs != 1 || worker.lastErr != nil || !h.State(session).Done {
		t.Fatalf("final=%v state=%+v specialist=%d supervisor=%d transfer err=%v", final, h.State(session),
			specialist.runs, supervisor.runs, worker.lastErr)
	}
}

func TestHandoffDeterministicReturnRespectsHopLimit(t *testing.T) {
	worker := newProgAgent("worker", "worker output", "")
	supervisor := newProgAgent("supervisor", "supervised", "")
	h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("return-limit"), agent_v2.WithHandoffInitial("worker"),
		agent_v2.WithDeterministicReturnTo("supervisor"), agent_v2.WithHandoffMaxHops(1)).
		AddAgent("worker", worker).AddAgent("supervisor", supervisor)
	session := agent_v2.NewRunSession()
	events, err := h.Run(t.Context(), session, schema.UserMessage("review"))
	if err != nil {
		t.Fatal(err)
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "worker output" ||
		supervisor.runs != 0 || !h.State(session).Exhausted {
		t.Fatalf("final=%v state=%+v supervisor=%d", final, h.State(session), supervisor.runs)
	}
}

func TestHandoffTransferOptionsRejectUnregisteredNames(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  agent_v2.HandoffOption
		want string
	}{
		{"退回目标", agent_v2.WithDeterministicReturnTo("missing"), `return target "missing" not registered`},
		{"工具期望", agent_v2.WithExpectedHandoffTransfer("missing"), `expected transfer candidate "missing" not registered`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			worker := newProgAgent("worker", "worker output", "")
			h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("unregistered"), agent_v2.WithHandoffInitial("worker"), tc.opt).
				AddAgent("worker", worker)
			_, err := h.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("review"))
			if err == nil || !strings.Contains(err.Error(), tc.want) || worker.runs != 0 {
				t.Fatalf("err=%v, worker=%d", err, worker.runs)
			}
		})
	}
}
