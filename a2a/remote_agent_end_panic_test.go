package a2a_test

import (
	"context"
	"sync/atomic"
	"testing"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// endPanicOnSuccess 在这次调用成功收尾时的 OnAgentEnd 里 panic 一次。
type endPanicOnSuccess struct {
	hook.NoOpHooks
	done atomic.Bool
}

func (h *endPanicOnSuccess) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	if agent_v2.GetEndError(ctx) == nil && h.done.CompareAndSwap(false, true) {
		panic("remote agent end boom")
	}
	return ctx
}

// TestRemoteAgent_AgentEndPanicOnSuccessKeepsSuccess：远端调用已经成功，RemoteAgent 的 OnAgentEnd 钩子 panic 一次——它只观测，
// 这次调用照旧算成功。放进带重试的顺序编排里，远端请求只发一次；曾经 panic 被转成错误，编排重试时远端请求再发一遍。
func TestRemoteAgent_AgentEndPanicOnSuccessKeepsSuccess(t *testing.T) {
	var calls atomic.Int64
	base := startServer(t, fnExec(func(execCtx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
		calls.Add(1)
		completedExec(execCtx, yield)
	}))
	h := &endPanicOnSuccess{}
	ra, err := a2a.NewRemoteAgent(context.Background(), base, a2a.WithHooks(h))
	if err != nil {
		t.Fatalf("NewRemoteAgent: %v", err)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"), agent_v2.WithSequentialMaxRetries(2)).AddStep("remote", ra)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("远端调用已经成功，编排不该失败：%v", err)
	}
	if !h.done.Load() {
		t.Fatal("fixture 没搭对：OnAgentEnd 没 panic")
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("远端请求发了 %d 次，want 1", n)
	}
}
