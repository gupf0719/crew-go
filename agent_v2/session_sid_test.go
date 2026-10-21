package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
)

// TestWithSession_PushesSessionRunID：WithSession 把 ctx 上的 sid 推成 session 的 RunID——任务的身份是
// RunID，ctx 上的 sid 只是它的副本（观测层的顶层 agent span 靠它在 OnAgentStart 阶段就带上
// request.id）。
//   - 空 ctx → push RunID；
//   - ctx 上已有别的 sid → 覆盖：长寿的 ctx 上钉着上一个任务的 sid、业务 `NextTask` 换了 RunID 时，
//     「没有才补」会让框架照旧认那个旧 sid，新任务接着跑被放弃任务的存档（实测）；
//   - 嵌套的半共享子 session RunID 跟父相同 → sid 不变（嵌套沿用父 sid）；
//   - 字面量 session（RunID 为空）→ 不推。
func TestWithSession_PushesSessionRunID(t *testing.T) {
	ctx := agent_v2.WithSession(context.Background(), &agent_v2.RunSession{RunID: "run_top"})
	if got := compose_v2.GetSessionID(ctx); got != "run_top" {
		t.Errorf("WithSession 应 push sid=RunID; got %q want run_top", got)
	}
	stale := compose_v2.WithSessionIDCtx(context.Background(), "上个任务")
	if got := compose_v2.GetSessionID(agent_v2.WithSession(stale, &agent_v2.RunSession{RunID: "本任务"})); got != "本任务" {
		t.Errorf("ctx 上别的 sid 应被 session 的 RunID 覆盖; got %q", got)
	}
	parent := agent_v2.NewRunSession()
	pctx := agent_v2.WithSession(context.Background(), parent)
	if got := compose_v2.GetSessionID(agent_v2.WithSession(pctx, agent_v2.NewSharedChildSession(parent))); got != parent.RunID {
		t.Errorf("嵌套的子 session 应沿用父 sid; got %q want %q", got, parent.RunID)
	}
	if got := compose_v2.GetSessionID(agent_v2.WithSession(stale, &agent_v2.RunSession{})); got != "上个任务" {
		t.Errorf("RunID 为空的 session 不该推 sid; got %q", got)
	}
}
