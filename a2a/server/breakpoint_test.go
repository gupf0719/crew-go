package server_test

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
)

type breakpointProgress struct {
	prepared, applied atomic.Int32
	replyText         atomic.Value
}

func breakpointWorkflow(t *testing.T) (*agent_v2.GraphAgent, *breakpointProgress) {
	t.Helper()
	progress := &breakpointProgress{}
	child := compose_v2.NewBuilder().WithName("debug_child").
		WithInterruptBefore("prepare", "apply").WithInterruptAfter("prepare")
	child.AddNode("prepare", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		progress.prepared.Add(1)
		return nil, nil
	})
	child.AddNode("apply", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		progress.applied.Add(1)
		return nil, nil
	})
	child.AddEdge(compose_v2.StartNode, "prepare")
	child.AddEdge("prepare", "apply")
	child.AddEdge("apply", compose_v2.EndNode)
	sub, err := child.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parent := compose_v2.NewBuilder().WithName("debug_parent").AddSubgraphNode("stage", sub)
	parent.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "Which account?"})
		if err != nil {
			return nil, err
		}
		progress.replyText.Store(answer.Content)
		return nil, nil
	})
	for _, name := range []compose_v2.NodeID{"ask", "stage"} {
		parent.AddEdge(compose_v2.StartNode, name)
		parent.AddEdge(name, compose_v2.EndNode)
	}
	graph, err := parent.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ag := agent_v2.NewGraphAgent(graph,
		agent_v2.WithGraphAgentName("debug_parent"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message {
			return schema.AssistantMessage("done")
		}))
	return ag, progress
}

func TestServerBreakpoint_StreamsAndResumesOnlyAddressedBoundary(t *testing.T) {
	ag, progress := breakpointWorkflow(t)
	client := clientFor(t, ag)
	first := send(t, client, a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("go")))
	before := a2aBreakpoint(t, first, "prepare", "")
	if progress.prepared.Load() != 0 || len(first.pending) != 2 {
		t.Fatalf("应同时挂起子图断点与普通提问，实际 %+v，prepare=%d", first, progress.prepared.Load())
	}
	var question string
	for _, item := range first.pending {
		if item.Breakpoint == nil && item.Question == "Which account?" {
			question = item.InterruptID
		}
	}
	if question == "" {
		t.Fatalf("未收到普通提问，实际 %+v", first.pending)
	}
	partial := send(t, client, reply(first, a2a.AnswerPart(question, nil, "acct-a")))
	if current := a2aBreakpoint(t, partial, "prepare", ""); current != before || progress.prepared.Load() != 0 || progress.replyText.Load() != nil {
		t.Fatalf("仅答普通提问不能让断点或提问继续执行，实际 %+v，prepare=%d，答复=%v", partial, progress.prepared.Load(), progress.replyText.Load())
	}

	next := send(t, client, reply(first, a2a.AnswerPart(before, nil, ""), a2a.AnswerPart(question, nil, "acct-a")))
	after := a2aBreakpoint(t, next, "apply", "prepare")
	if next.taskID != first.taskID || next.ctxID != first.ctxID || before == after || len(next.pending) != 1 ||
		progress.prepared.Load() != 1 || progress.applied.Load() != 0 || progress.replyText.Load() != "acct-a" {
		t.Fatalf("应在原任务进入下一断点，实际 %+v，prepare/apply=%d/%d，答复=%v",
			next, progress.prepared.Load(), progress.applied.Load(), progress.replyText.Load())
	}
	stale := send(t, client, reply(next, a2a.AnswerPart(before, nil, "")))
	if current := a2aBreakpoint(t, stale, "apply", "prepare"); current != after || progress.applied.Load() != 0 {
		t.Fatalf("迟到的旧 ID 不能放行下一断点，实际 %+v", stale)
	}
	unbound := send(t, client, reply(next, a2atypes.NewTextPart("continue")))
	if current := a2aBreakpoint(t, unbound, "apply", "prepare"); current != after || progress.applied.Load() != 0 || !strings.Contains(unbound.text, after) {
		t.Fatalf("只有 taskId 而无断点 ID 的续发应仍停在原处，实际 %+v", unbound)
	}

	task, err := client.GetTask(t.Context(), &a2atypes.GetTaskRequest{ID: next.taskID})
	if err != nil || task == nil || task.Status.Message == nil {
		t.Fatalf("查询断点任务失败，task=%+v err=%v", task, err)
	}
	read := turn{state: task.Status.State, taskID: task.ID, ctxID: task.ContextID, pending: a2a.PendingQuestionsFromParts(task.Status.Message.Parts)}
	if current := a2aBreakpoint(t, read, "apply", "prepare"); current != after {
		t.Fatalf("任务查询和流必须给出同一个地址，实际 %q 与 %q", current, after)
	}
	// 带挂起清单的单项续发由 RemoteAgent 使用。正文仅用于兼容通用 A2A 消息格式。
	resume := reply(next, a2atypes.NewTextPart(""))
	resume.Metadata = map[string]any{a2a.ReplyToInterruptIDsKey: []any{after}}
	done := send(t, client, resume)
	if done.state != a2atypes.TaskStateCompleted || done.taskID != first.taskID || progress.prepared.Load() != 1 || progress.applied.Load() != 1 {
		t.Fatalf("带当轮身份的续发应完成原任务，实际 %+v，prepare/apply=%d/%d", done, progress.prepared.Load(), progress.applied.Load())
	}
}

func TestRemoteBreakpoint_MissingConfirmationKeepsAllHostAnswersAvailable(t *testing.T) {
	ag, progress := breakpointWorkflow(t)
	remote, err := a2a.NewRemoteAgent(t.Context(), serveAgent(t, ag))
	if err != nil {
		t.Fatal(err)
	}
	host := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("debug_host")).AddStep("remote", remote)
	ctx := compose_v2.WithCheckpointerCtx(t.Context(), compose_v2.NewInMemoryCheckpointer())
	session := agent_v2.NewRunSession()
	_, err = host.Run(ctx, session, schema.UserMessage("go"))
	paused, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("宿主应挂起等待确认，实际 %v", err)
	}
	open := paused.AllQueries()
	if len(open) != 2 || open[0].Question != "Which account?" || open[1].Breakpoint == nil || !open[1].Breakpoint.Delegated {
		t.Fatalf("回归需要普通提问排在转述断点前面，实际 %+v", paused)
	}

	inputs := map[string]*schema.Message{open[0].InterruptID: schema.UserMessage("acct-a")}
	_, err = host.Resume(ctx, session.RunID, agent_v2.WithResumeSession(session), agent_v2.WithResumeUserInputs(inputs))
	paused, ok = compose_v2.IsInterrupt(err)
	if !ok || progress.prepared.Load() != 0 || progress.replyText.Load() != nil {
		t.Fatalf("仅答普通问题应保持宿主挂起且远端零进展，实际 %v，执行 %d 次，答复 %v",
			err, progress.prepared.Load(), progress.replyText.Load())
	}
	stillOpen := paused.AllQueries()
	if len(stillOpen) != 2 || stillOpen[0].InterruptID != open[0].InterruptID || stillOpen[1].InterruptID != open[1].InterruptID {
		t.Fatalf("待确认时应保留原来两个宿主 ID，实际 %+v", paused)
	}

	inputs[open[1].InterruptID] = schema.UserMessage("")
	_, err = host.Resume(ctx, session.RunID, agent_v2.WithResumeSession(session), agent_v2.WithResumeUserInputs(inputs))
	paused, ok = compose_v2.IsInterrupt(err)
	if !ok || progress.prepared.Load() != 1 || progress.applied.Load() != 0 || progress.replyText.Load() != "acct-a" {
		t.Fatalf("补齐确认应交付普通答复并推进到下一断点，实际 %v，执行 %d/%d 次，答复 %v",
			err, progress.prepared.Load(), progress.applied.Load(), progress.replyText.Load())
	}
	next := paused.AllQueries()
	if len(next) != 1 || next[0].Breakpoint == nil || !next[0].Breakpoint.Delegated ||
		!slices.Equal(next[0].Breakpoint.Before, []compose_v2.NodeID{"apply"}) || next[0].InterruptID == open[1].InterruptID {
		t.Fatalf("下一断点应保留自己的位置和宿主地址，实际 %+v", paused)
	}
	events, err := host.Resume(ctx, session.RunID, agent_v2.WithResumeSession(session),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{next[0].InterruptID: schema.UserMessage("")}))
	if err != nil || progress.prepared.Load() != 1 || progress.applied.Load() != 1 || agent_v2.FinalMessage(events).GetContent() != "done" {
		t.Fatalf("确认下一断点后应完成原任务且只写一次，实际 %v，执行 %d/%d 次",
			err, progress.prepared.Load(), progress.applied.Load())
	}
}

func a2aBreakpoint(t *testing.T, turn turn, before, after compose_v2.NodeID) string {
	t.Helper()
	if turn.state != a2atypes.TaskStateInputRequired || turn.taskID == "" || turn.ctxID == "" {
		t.Fatalf("断点对应原任务的 input-required 状态，实际 %+v", turn)
	}
	var wantAfter []compose_v2.NodeID
	if after != "" {
		wantAfter = []compose_v2.NodeID{after}
	}
	for _, item := range turn.pending {
		bp := item.Breakpoint
		if bp == nil {
			continue
		}
		if item.InterruptID == "" || item.Approval || bp.NS != "stage" || !slices.Equal(bp.Before, []compose_v2.NodeID{before}) || !slices.Equal(bp.After, wantAfter) {
			t.Fatalf("断点字段与节点边界不匹配，实际 %+v，位置 %+v", item, bp)
		}
		return item.InterruptID
	}
	t.Fatalf("缺少断点位置，实际 %+v", turn.pending)
	return ""
}
