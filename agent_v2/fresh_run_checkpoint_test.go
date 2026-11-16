package agent_v2_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

type freshRunStartProbe struct {
	hook.NoOpHooks
	calls int
}

func (p *freshRunStartProbe) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	p.calls++
	return ctx, nil
}

// 让 A 留下实际可续跑的提问，再用真实 Run 入口尝试发起 B。
func TestFreshRunCheckpoint_ProtectsSuspendedTask(t *testing.T) {
	for _, tc := range []struct {
		name    string
		oldNS   string
		newID   bool
		abandon bool
	}{
		{name: "同 RunID 拒绝新开"},
		{name: "默认 namespace 拒绝覆盖其他 namespace 的任务", oldNS: "explicit/first"},
		{name: "NextTask 保留旧档", newID: true},
		{name: "显式放弃后新开", newID: true, abandon: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const (
				owner    = "tenant"
				thread   = "fresh-run"
				question = "批准任务 A？"
				answer   = "批准 A"
				newInput = "执行独立任务 B"
			)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cp := compose_v2.NewInMemoryCheckpointer()
			ctx = compose_v2.WithCheckpointerCtx(compose_v2.WithThreadIDCtx(ctx, thread), cp)
			oldCtx := compose_v2.WithGraphNS(ctx, tc.oldNS)
			hs := agent_v2.NewInMemoryHistoryStore()
			history := func() []*schema.Message {
				t.Helper()
				messages, err := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: owner, ConvID: thread})
				if err != nil {
					t.Fatal(err)
				}
				return messages
			}
			sess := agent_v2.NewRunSession()
			oldID := sess.RunID
			loadCheckpoint := func() *compose_v2.Checkpoint {
				t.Helper()
				checkpoint, err := cp.LoadNS(ctx, oldID, tc.oldNS)
				if err != nil {
					t.Fatal(err)
				}
				return checkpoint
			}

			var toolAnswer string
			var newToolCalls int
			tk := tool.NewToolkit()
			tk.Register(tool.NewFuncTool("ask_a", "暂停任务 A", map[string]any{"type": "object"},
				func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
					response, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: question})
					if err != nil {
						return nil, err
					}
					toolAnswer = response.Content
					return tool.Success(response.Content), nil
				}))
			tk.Register(tool.NewFuncTool("mark_b", "执行任务 B", map[string]any{
				"type": "object", "properties": map[string]any{"tag": map[string]any{"type": "string"}},
			}, func(_ context.Context, args map[string]any) (*tool.Result, error) {
				if args["tag"] != "B" {
					t.Errorf("任务 B 工具参数为 %v，预期 tag=B", args)
				}
				newToolCalls++
				return tool.Success("B 工具已执行"), nil
			}))
			call := func(id, name, args string) *schema.Message {
				return schema.AssistantToolCallMessage([]schema.ToolCall{{
					ID: id, Function: schema.FunctionCall{Name: name, Arguments: args},
				}})
			}
			oldAgent := agent_v2.NewBasicAgent(agent_v2.WithName("fresh-run"), agent_v2.WithToolkit(tk),
				agent_v2.WithHistory(hs, owner), agent_v2.WithModel(newStubModel([]*schema.Message{
					call("a", "ask_a", "{}"), schema.AssistantMessage("A 完成"),
				})))
			_, err := oldAgent.Run(oldCtx, sess, schema.UserMessage("开始任务 A"))
			interruption, ok := compose_v2.IsInterrupt(err)
			if !ok || interruption.Query == nil || interruption.Query.Question != question || interruption.Query.InterruptID == "" {
				t.Fatalf("任务 A 应返回一条待答问题，实际 %v", err)
			}
			interruptID := interruption.Query.InterruptID
			before := loadCheckpoint()
			if before == nil || !before.HasResumableWork() || len(before.PendingInterrupts) != 1 || before.PendingInterrupts[0].InterruptID != interruptID {
				t.Fatalf("任务 A 应留下同一问题的可续存档，实际 %v", before)
			}
			beforeJSON, err := json.Marshal(before)
			if err != nil {
				t.Fatal(err)
			}
			beforeHistory := history()
			beforeHistoryJSON, err := json.Marshal(beforeHistory)
			if err != nil {
				t.Fatal(err)
			}
			var newModelInputs []*schema.Message
			newModel := newStubModelCapturing([]*schema.Message{
				call("b", "mark_b", `{"tag":"B"}`), schema.AssistantMessage("B 完成"),
			}, &newModelInputs)
			newHooks := &freshRunStartProbe{}
			newAgent := agent_v2.NewBasicAgent(agent_v2.WithName("fresh-run"), agent_v2.WithToolkit(tk),
				agent_v2.WithHistory(hs, owner), agent_v2.WithHooks(newHooks), agent_v2.WithModel(newModel))
			var opts []agent_v2.RunOption
			if tc.newID {
				sess.NextTask()
				if sess.RunID == oldID {
					t.Fatal("NextTask 应更换 RunID")
				}
			}
			if tc.abandon {
				opts = append(opts, agent_v2.WithAbandonedTasks(oldID))
			}
			events, err := newAgent.Run(ctx, sess, schema.UserMessage(newInput), opts...)
			if !tc.newID {
				var nonRetryable compose_v2.NonRetryable
				if !errors.Is(err, agent_v2.ErrRunIDHasResumableCheckpoint) || !errors.As(err, &nonRetryable) || errors.Is(err, compose_v2.ErrRunAborted) {
					t.Errorf("任务 B 应在执行前不可重试地拒绝，且不触发终局清档，实际 %v", err)
				}
				if newHooks.calls != 0 || newModel.idx != 0 || newToolCalls != 0 || len(events) != 0 {
					t.Errorf("拒绝后任务 B 不应产生副作用，Start=%d、模型=%d、工具=%d、事件=%d",
						newHooks.calls, newModel.idx, newToolCalls, len(events))
				}
				after := history()
				afterHistoryJSON, err := json.Marshal(after)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(afterHistoryJSON, beforeHistoryJSON) {
					t.Errorf("拒绝不应改写历史，拒绝前 %d 条，拒绝后 %d 条", len(beforeHistory), len(after))
				}
			} else {
				if final := agent_v2.FinalMessage(events); err != nil || final == nil || final.Content != "B 完成" || newHooks.calls != 1 || newToolCalls != 1 {
					t.Fatalf("更换 RunID 后任务 B 应执行并结束，答复=%v、错误=%v、Start=%d、工具=%d", final, err, newHooks.calls, newToolCalls)
				}
				if !slices.ContainsFunc(newModelInputs, func(m *schema.Message) bool { return m.Role == schema.RoleUser && m.Content == newInput }) {
					t.Fatal("任务 B 的新输入应抵达模型")
				}
			}
			remaining := loadCheckpoint()
			if tc.abandon {
				if remaining != nil {
					t.Error("显式放弃应删除任务 A 的旧档")
				}
				if !slices.ContainsFunc(history(), func(m *schema.Message) bool { return schema.GetToolResultID(m) == "a" }) {
					t.Error("显式放弃应为任务 A 的工具调用补历史结果")
				}
			} else {
				afterJSON, err := json.Marshal(remaining)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(afterJSON, beforeJSON) {
					t.Error("任务 B 不应改写任务 A 的待答存档")
				}
			}
			events, err = oldAgent.Resume(oldCtx, oldID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
				interruptID: schema.UserMessage(answer),
			}))
			if tc.abandon {
				if !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) || toolAnswer != "" {
					t.Errorf("已放弃的任务 A 不应兑付答案，错误=%v、工具收到=%q", err, toolAnswer)
				}
				return
			}
			if final := agent_v2.FinalMessage(events); err != nil || final == nil || final.Content != "A 完成" || toolAnswer != answer {
				t.Errorf("任务 A 应从旧档收到原题答案并完成，答复=%v、错误=%v、工具收到=%q", final, err, toolAnswer)
			}
			if loadCheckpoint() != nil {
				t.Error("任务 A 成功结束后应清档")
			}
		})
	}
}

// 失败档、旧档和已终结快照都以存档里的剩余工作为准。
func TestFreshRunCheckpoint_UsesResumableContent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status compose_v2.CheckpointStatus
		work   bool
	}{
		{name: "失败但可续", status: compose_v2.CPStatusFailed, work: true},
		{name: "旧档有待执行节点", status: compose_v2.CPStatusUnset, work: true},
		{name: "已终结快照", status: compose_v2.CPStatusUnset},
		{name: "旧标记为挂起但无剩余工作", status: compose_v2.CPStatusPending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cp := compose_v2.NewInMemoryCheckpointer()
			ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
			sess := agent_v2.NewRunSession()
			before := &compose_v2.Checkpoint{Status: tc.status, Epoch: "旧档"}
			if tc.work {
				before.CurrentFrontier = []compose_v2.FrontierMember{{NodeID: "model"}}
			}
			if err := cp.SaveNS(ctx, sess.RunID, "", before); err != nil {
				t.Fatal(err)
			}
			stub := newStubModel([]*schema.Message{schema.AssistantMessage("新任务完成")})
			agent := agent_v2.NewBasicAgent(agent_v2.WithModel(stub))
			events, err := agent.Run(ctx, sess, schema.UserMessage("开新任务"))
			after, loadErr := cp.LoadNS(ctx, sess.RunID, "")
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			if tc.work {
				if !errors.Is(err, agent_v2.ErrRunIDHasResumableCheckpoint) || stub.idx != 0 || after == nil || after.Epoch != "旧档" {
					t.Fatalf("有可续工作时应在模型前拒绝并保留旧档，错误=%v、模型=%d、存档=%v", err, stub.idx, after)
				}
				return
			}
			if final := agent_v2.FinalMessage(events); err != nil || final == nil || final.Content != "新任务完成" || stub.idx != 1 || after != nil {
				t.Errorf("无可续工作应允许新任务并在完成后清档，答复=%v、错误=%v、模型=%d、存档=%v", final, err, stub.idx, after)
			}
		})
	}
}

func TestFreshRunCheckpoint_ThroughCustomEntry(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(t.Context(), cp)
	sess := agent_v2.NewRunSession()
	old := &compose_v2.Checkpoint{CurrentFrontier: []compose_v2.FrontierMember{{NodeID: "old"}}, Epoch: "old"}
	if err := cp.SaveNS(ctx, sess.RunID, "", old); err != nil {
		t.Fatal(err)
	}
	ctx, sess, err := agent_v2.EnterRun(ctx, sess, "wrapper")
	if err != nil {
		t.Fatal(err)
	}
	model := newStubModel([]*schema.Message{schema.AssistantMessage("独立任务")})
	worker := agent_v2.NewBasicAgent(agent_v2.WithModel(model))
	if _, err := worker.Run(ctx, sess, schema.UserMessage("新输入")); !errors.Is(err, agent_v2.ErrRunIDHasResumableCheckpoint) {
		t.Fatalf("透传的内建入口应拒绝覆盖挂起任务，实际 %v", err)
	}
	after, err := cp.LoadNS(ctx, sess.RunID, "")
	if err != nil || after == nil || after.Epoch != "old" || model.idx != 0 {
		t.Fatalf("模型不应调用，旧档应保留，模型=%d 存档=%v 错误=%v", model.idx, after, err)
	}
}

type freshBackendOnStart struct {
	hook.NoOpHooks
	checkpointer compose_v2.Checkpointer
	starts       int
	ends         int
	endErr       error
}

func (h *freshBackendOnStart) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	h.starts++
	return compose_v2.WithCheckpointerCtx(ctx, h.checkpointer), nil
}

func (h *freshBackendOnStart) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	h.ends++
	h.endErr = agent_v2.GetEndError(ctx)
	return ctx
}

func TestFreshRunCheckpoint_BackendSelectedAtStart(t *testing.T) {
	for _, occupied := range []bool{true, false} {
		name := "空后端"
		if occupied {
			name = "换库遇到可续档"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cp := compose_v2.NewInMemoryCheckpointer()
			sess := agent_v2.NewRunSession()
			if occupied {
				old := &compose_v2.Checkpoint{CurrentFrontier: []compose_v2.FrontierMember{{NodeID: "old"}}, Epoch: "old"}
				if err := cp.SaveNS(ctx, sess.RunID, "child", old); err != nil {
					t.Fatal(err)
				}
			}
			hooks := &freshBackendOnStart{checkpointer: cp}
			model := newStubModel([]*schema.Message{schema.AssistantMessage("B 完成")})
			worker := agent_v2.NewBasicAgent(agent_v2.WithModel(model), agent_v2.WithHooks(hooks))
			events, err := worker.Run(ctx, sess, schema.UserMessage("任务 B"))
			if hooks.starts != 1 || hooks.ends != 1 {
				t.Fatalf("换库应保持钩子配对，实际 %d/%d", hooks.starts, hooks.ends)
			}
			if !occupied {
				if err != nil || agent_v2.FinalMessage(events).GetContent() != "B 完成" || model.idx != 1 {
					t.Fatalf("空后端应允许新任务执行，事件=%v 错误=%v", events, err)
				}
				return
			}
			if !errors.Is(err, agent_v2.ErrRunIDHasResumableCheckpoint) ||
				!errors.Is(hooks.endErr, agent_v2.ErrRunIDHasResumableCheckpoint) || model.idx != 0 || len(events) != 0 {
				t.Fatalf("新后端有旧档时应拒绝执行，模型=%d 事件=%d 错误=%v End=%v", model.idx, len(events), err, hooks.endErr)
			}
			after, err := cp.LoadNS(ctx, sess.RunID, "child")
			if err != nil || after == nil || after.Epoch != "old" {
				t.Fatalf("换库拒绝应保留旧档，实际 %v，%v", after, err)
			}
		})
	}
}

type holdFreshAdmission struct {
	hook.NoOpHooks
	entered chan struct{}
	proceed chan struct{}
}

func (h *holdFreshAdmission) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	close(h.entered)
	select {
	case <-h.proceed:
		return ctx, nil
	case <-ctx.Done():
		return ctx, ctx.Err()
	}
}

func TestFreshRunCheckpoint_ConcurrentEntryWithHostActivity(t *testing.T) {
	parent, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	parent = compose_v2.WithCheckpointerCtx(parent, cp)
	sess := agent_v2.NewRunSession()
	ctx, host, err := compose_v2.BeginTaskActivity(parent, sess.RunID, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Release()
	hooks := &holdFreshAdmission{entered: make(chan struct{}), proceed: make(chan struct{})}
	worker := agent_v2.NewBasicAgent(agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("任务 A 完成")})),
		agent_v2.WithHooks(hooks))
	done := make(chan error, 1)
	go func() {
		_, err := worker.Run(ctx, sess, schema.UserMessage("任务 A"))
		done <- err
	}()
	defer func() {
		close(hooks.proceed)
		if err := <-done; err != nil {
			t.Errorf("同一宿主持有的任务 A 应完成，实际 %v", err)
		}
	}()
	select {
	case <-hooks.entered:
	case <-ctx.Done():
		t.Fatal("没有进入任务 A 的启动钩子")
	}
	otherModel := newStubModel([]*schema.Message{schema.AssistantMessage("任务 B")})
	other := agent_v2.NewBasicAgent(agent_v2.WithModel(otherModel))
	_, err = other.Run(ctx, &agent_v2.RunSession{RunID: sess.RunID}, schema.UserMessage("任务 B"))
	if !errors.Is(err, compose_v2.ErrTaskBusy) || otherModel.idx != 0 {
		t.Fatalf("同一宿主 ctx 的第二次顶层调用应在模型前被拒，实际 %v，模型 %d 次", err, otherModel.idx)
	}
}
