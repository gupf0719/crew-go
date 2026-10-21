package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
)

// simpleStartHooks 简易 hooks 实现——只计 OnGraphStart 次数。
type simpleStartHooks struct {
	agent_v2.NoOpOrchestrationHooks
	starts int
}

func (h *simpleStartHooks) OnGraphStart(ctx context.Context) (context.Context, error) {
	h.starts++
	return ctx, nil
}

// TestC1_AgentV2_CtxOrchHooks_RoundTrip 验证 agent_v2.WithCtxOrchHooks / GetCtxOrchHooks。
func TestC1_AgentV2_CtxOrchHooks_RoundTrip(t *testing.T) {
	h := &simpleStartHooks{}
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), h)
	if got := agent_v2.GetCtxOrchHooks(ctx); got != h {
		t.Errorf("ctx round-trip failed")
	}
	// 未注入返 nil
	if got := agent_v2.GetCtxOrchHooks(context.Background()); got != nil {
		t.Errorf("expect nil, got %v", got)
	}
}

// TestC1_AgentV2_CtxOrchHooks_NilSafe 验证 nil hook ctx 不污染。
func TestC1_AgentV2_CtxOrchHooks_NilSafe(t *testing.T) {
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), nil)
	if got := agent_v2.GetCtxOrchHooks(ctx); got != nil {
		t.Errorf("nil hooks should not be stored, got %v", got)
	}
}
