package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// retryFlakyModel 前 N 次返 error，第 N+1 次返 success——模拟瞬时故障。
type retryFlakyModel struct {
	failTimes int32
	count     atomic.Int32
	resp      *schema.Message
}

func newFlakyModel(failTimes int, resp *schema.Message) *retryFlakyModel {
	return &retryFlakyModel{failTimes: int32(failTimes), resp: resp}
}

func (m *retryFlakyModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	n := m.count.Add(1)
	if n <= m.failTimes {
		return nil, errors.New("transient flaky error")
	}
	return m.resp, nil
}

func (m *retryFlakyModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *retryFlakyModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

var _ model.ChatModel = (*retryFlakyModel)(nil)

// TestRetry_SequentialAgent 验证 SequentialAgent 接入 retry：sub-agent 前 2 次失败
// + WithSequentialMaxRetries(3) → 第 3 次成功。
func TestRetry_SequentialAgent(t *testing.T) {
	flaky := newFlakyModel(2, schema.AssistantMessage("recovered"))
	sub := agent_v2.NewBasicAgent(agent_v2.WithName("sub"), agent_v2.WithModel(flaky))

	seq := agent_v2.NewSequentialAgent(
		agent_v2.WithSequentialName("seq"),
		agent_v2.WithSequentialMaxRetries(3),
	).AddStep("only", sub)

	events, err := seq.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "recovered") {
		t.Errorf("expect 'recovered' after retry, got %v", final)
	}
	if got := flaky.count.Load(); got != 3 {
		t.Errorf("model called %d times, want 3 (2 fail + 1 success)", got)
	}
}

// TestRetry_LoopAgent 验证 LoopAgent 接入 retry。
func TestRetry_LoopAgent(t *testing.T) {
	flaky := newFlakyModel(1, schema.AssistantMessage("ok"))
	body := agent_v2.NewBasicAgent(agent_v2.WithName("body"), agent_v2.WithModel(flaky))

	loop := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("loop"),
		agent_v2.WithLoopMaxIterations(1),
		agent_v2.WithLoopMaxRetries(2),
	).WithBody(body)

	events, err := loop.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "ok") {
		t.Errorf("expect 'ok' after retry, got %v", final)
	}
}

// TestRetry_ExhaustedReturnsError 验证 retries 用尽仍失败时返原 error。
func TestRetry_ExhaustedReturnsError(t *testing.T) {
	// 5 次都失败但只允许 retry 2 次 → 共调 3 次仍失败 → Run 报错
	flaky := newFlakyModel(5, schema.AssistantMessage("never"))
	sub := agent_v2.NewBasicAgent(agent_v2.WithName("sub"), agent_v2.WithModel(flaky))

	seq := agent_v2.NewSequentialAgent(
		agent_v2.WithSequentialName("seq"),
		agent_v2.WithSequentialMaxRetries(2),
	).AddStep("only", sub)

	_, err := seq.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil {
		t.Fatal("expect retry exhausted err")
	}
	if !strings.Contains(err.Error(), "transient") {
		t.Errorf("expect 'transient' err, got %v", err)
	}
	if got := flaky.count.Load(); got != 3 {
		t.Errorf("model called %d times, want 3 (1 + 2 retries)", got)
	}
}

// TestRetry_UsesExponentialBackoff 验证 ExponentialBackoff config 也被接受（不验时延，只验功能性）。
func TestRetry_UsesExponentialBackoff(t *testing.T) {
	flaky := newFlakyModel(1, schema.AssistantMessage("ok"))
	sub := agent_v2.NewBasicAgent(agent_v2.WithName("sub"), agent_v2.WithModel(flaky))

	cfg := compose_v2.ExponentialBackoff(2, 1, 5) // 1ns init, 5ns max — 立刻重试
	seq := agent_v2.NewSequentialAgent(
		agent_v2.WithSequentialName("seq"),
		agent_v2.WithSequentialRetryConfig(cfg),
	).AddStep("only", sub)

	_, err := seq.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
}
