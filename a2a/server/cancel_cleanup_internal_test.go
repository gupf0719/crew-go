package server

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

type cancelFailCP struct {
	compose_v2.Checkpointer
	fail       atomic.Bool
	calls      atomic.Int32
	thread     atomic.Value
	badContext atomic.Bool
}

func (c *cancelFailCP) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	c.calls.Add(1)
	deadline, bounded := ctx.Deadline()
	if ctx.Err() != nil || compose_v2.GetOwner(ctx) != "owner" || compose_v2.GetSessionID(ctx) != sid || !bounded || time.Until(deadline) < 9*time.Second || time.Until(deadline) > 11*time.Second {
		c.badContext.Store(true)
	}
	if thread := c.thread.Load(); thread != nil && compose_v2.GetThreadID(ctx) != thread.(string) {
		c.badContext.Store(true)
	}
	if c.fail.Load() {
		return 0, errors.New("injected delete failure")
	}
	return c.Checkpointer.DeleteSubtree(ctx, sid, ns)
}

func TestCancelTask_RetriesFailedCleanupAfterTerminalStatus(t *testing.T) {
	b := compose_v2.NewBuilder().WithName("cancel-retry")
	b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "approve"})
		return nil, err
	})
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ag := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("cancel-retry"), agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }), agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return schema.AssistantMessage("done") }))
	e := NewExecutor(ag, WithOwnerResolver(func(context.Context) string { return "owner" }))
	cp := &cancelFailCP{Checkpointer: compose_v2.NewInMemoryCheckpointer()}
	e.cp = cp
	h := a2asrv.NewHandler(e)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result, err := h.SendMessage(ctx, &a2atypes.SendMessageRequest{Message: a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("run"))})
	if err != nil {
		t.Fatal(err)
	}
	task, ok := result.(*a2atypes.Task)
	if !ok || task.Status.State != a2atypes.TaskStateInputRequired {
		t.Fatalf("initial=%+v", result)
	}
	e.mu.Lock()
	st := e.tasks[taskKey{owner: "owner", taskID: task.ID}]
	sid := st.sess.RunID
	e.mu.Unlock()
	cp.thread.Store(task.ContextID)
	cp.fail.Store(true)
	cancelReq := &a2atypes.CancelTaskRequest{ID: task.ID}
	canceled, err := h.CancelTask(ctx, cancelReq)
	if err != nil || canceled == nil || canceled.Status.State != a2atypes.TaskStateCanceled {
		t.Fatalf("expected canceled task, got %v err=%v", canceled, err)
	}
	if calls := cp.calls.Load(); calls != 2 {
		t.Fatalf("cancel and SDK completion should each attempt cleanup, got %d deletes", calls)
	}
	if exists, err := cp.ExistsNS(ctx, sid, ""); err != nil || !exists {
		t.Fatalf("fixture should retain the checkpoint during outage, exists=%v err=%v", exists, err)
	}
	canceledCtx, cancelRequest := context.WithCancel(ctx)
	cancelRequest()
	e.Cleanup(canceledCtx, &a2asrv.ExecutorContext{TaskID: task.ID, ContextID: task.ContextID}, canceled, nil)
	if calls := cp.calls.Load(); calls != 3 {
		t.Fatalf("SDK cleanup should retry after request cancellation, got %d deletes", calls)
	}
	_, err = h.SendMessage(ctx, &a2atypes.SendMessageRequest{
		Message: a2atypes.NewMessageForTask(a2atypes.MessageRoleUser,
			a2atypes.TaskInfo{TaskID: task.ID, ContextID: task.ContextID}, a2atypes.NewTextPart("yes")),
	})
	if !errors.Is(err, a2atypes.ErrUnsupportedOperation) {
		t.Fatalf("canceled task accepted continuation: %v", err)
	}

	cp.fail.Store(false)
	canceled, retryErr := h.CancelTask(ctx, cancelReq)
	exists, existsErr := cp.ExistsNS(ctx, sid, "")
	if retryErr != nil || canceled == nil || canceled.Status.State != a2atypes.TaskStateCanceled ||
		existsErr != nil || exists || cp.calls.Load() != 4 {
		t.Fatalf("cancellation cleanup became unreachable: retry=%v exists=%v existsErr=%v calls=%d",
			retryErr, exists, existsErr, cp.calls.Load())
	}
	if cp.badContext.Load() {
		t.Fatal("cleanup lost owner/thread/session identity or independent deadline")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.tasks) != 0 {
		t.Fatal("successful cleanup retained task mapping")
	}
}
