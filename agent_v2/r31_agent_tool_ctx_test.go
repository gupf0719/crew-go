package agent_v2_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// slowFailingAgent 模拟 sub-agent 在 ctx canceled 时返 ctx.Err。
type slowFailingAgent struct{ name string }

func (s *slowFailingAgent) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	// 等 ctx done（典型 deadline / cancel 场景）
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(5 * time.Second):
		return nil, nil
	}
}
func (s *slowFailingAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (s *slowFailingAgent) Name() string        { return s.name }
func (s *slowFailingAgent) Description() string { return "" }

// TestR31_AgentAsTool_PropagateCtxErr 复现 R31 视角 B MED-2：
//
// AgentAsTool.Call 仅判 InterruptError 转 propagate，对 ctx.Err()（cancel/deadline）
// 静默转 tool.Failure → 父 LLM 看到工具失败继续 ReAct → 父 ctx 也 deadline 时新
// LLM 调用立即失败浪费 token + 用户看到错乱"超时但还在重试"反馈。
//
// fix 后：ctx.Err 同 InterruptError 一样 propagate as error。
func TestR31_AgentAsTool_PropagateCtxErr(t *testing.T) {
	sa := &slowFailingAgent{name: "slow"}
	tooly := agent_v2.AgentAsTool(sa, agent_v2.WithAgentToolDescription("slow agent"))

	// 100ms 后 cancel——sub-agent.Run 收到 ctx.Done 返 context.Canceled
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	res, err := tooly.Call(ctx, map[string]any{"input": "go"})

	// fix 后：err != nil 且是 ctx.Err（context.Canceled / DeadlineExceeded）
	// fix 前：err == nil + res = tool.Failure（错位静默吞 ctx.Err）
	if err == nil {
		t.Fatalf("expected ctx.Err propagated, got nil + res=%+v", res)
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.DeadlineExceeded / Canceled, got %v", err)
	}
	// 验证 fix 后 res 应为 nil（propagate 路径）；fix 前 res 是 tool.Failure 的 *tool.Result
	if res != nil {
		t.Fatalf("fix 前路径触发：res 非 nil（Success=%v），ctx.Err 被静默吞包成 tool.Failure", res.Success)
	}
}
