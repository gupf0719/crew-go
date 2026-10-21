package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// compact:reactive:retry EventSink event 守护测试(sink-gated——无独立开关).
//
// 测覆盖:
//  1. 接了 OrchHooks + LLM 报 prompt_too_long → retry 循环每 attempt 之间 fire 1 个
//     compact:reactive:retry event
//  2. LLM 不报 prompt_too_long(无 retry)→ 0 retry event
//
// sanity-revert: 删 basic_agent.go::if hasEventSink 块内 retry EmitCompactionEvent 调用 →
// 测试 fail.

// promptTooLongThenOk 模拟 LLM 前 N call 报 prompt too long, 第 N+1 成功.
// 让 retry 循环跑满 N attempt 测试 fire 多个 event.
type promptTooLongThenOk struct {
	mu        sync.Mutex
	calls     int
	failFirst int // 前 N call 返 prompt too long
}

func (m *promptTooLongThenOk) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.calls <= m.failFirst {
		// 业务 error 字符串含 "prompt is too long" 走 IsPromptTooLongError true
		return nil, errors.New("prompt is too long: 200000 tokens > 128000 maximum")
	}
	return schema.AssistantMessage("ok answer"), nil
}
func (m *promptTooLongThenOk) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *promptTooLongThenOk) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// captureRetrySink 接 framework 转发 compaction event.
type captureRetrySink struct {
	compose_v2.NoOpOrchestrationHooks
	mu     sync.Mutex
	events []compose_v2.Event
}

func (s *captureRetrySink) OnNodeEvent(ctx context.Context, _ compose_v2.NodeID, ev compose_v2.Event) context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
	return ctx
}

func (s *captureRetrySink) retryActions() []*agent_v2.CompactionAction {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*agent_v2.CompactionAction
	for _, ev := range s.events {
		if ev.Type != agent_v2.CompactionEventType {
			continue
		}
		if a, ok := ev.Metadata["action"].(*agent_v2.CompactionAction); ok {
			if a.Type == agent_v2.CompactionActionRetry {
				out = append(out, a)
			}
		}
	}
	return out
}

func TestPhase2Sub4_ReactiveRetry_EmitsEvent(t *testing.T) {
	// Mock model 前 2 call 返 prompt too long, 第 3 call 成功 → retry 循环跑 2 attempt
	stub := &promptTooLongThenOk{failFirst: 2}
	sink := &captureRetrySink{}
	ctx := compose_v2.WithOrchHooks(context.Background(), sink)

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("test_retry"),
		agent_v2.WithModel(stub),
		agent_v2.WithReactiveCompact(&agent_v2.ReactiveCompactConfig{
			Enabled:              true,
			MaxAttempts:          3,
			EmergencyDropPercent: 50,
		}),
	)

	// 构造长 history 让 emergencyDrop 真删 (至少 2 组)
	sess := agent_v2.NewRunSession()
	for i := 0; i < 5; i++ {
		sess.AddEvent(agent_v2.NewOutputEvent("test_retry", schema.UserMessage(fmt.Sprintf("u%d %s", i, strings.Repeat("p", 100)))))
		sess.AddEvent(agent_v2.NewOutputEvent("test_retry", schema.AssistantMessage(fmt.Sprintf("a%d %s", i, strings.Repeat("p", 100)))))
	}

	_, _ = ag.Run(ctx, sess, schema.UserMessage("query"))

	actions := sink.retryActions()
	if len(actions) == 0 {
		t.Fatalf("Phase 2 Sub-4 regression: ReactiveCompact retry 循环应 fire compact:reactive:retry event\n"+
			"sanity-revert: 删 basic_agent.go 内 retry EmitCompactionEvent 调用 → 此测 fail\n"+
			"mock LLM call count=%d", stub.calls)
	}
	// audit MED-3 fix: 验多 attempt 都 fire (failFirst=2 应 fire 2 个 retry event,
	// 之前只验首个漏验多 attempt 模式)
	if len(actions) != 2 {
		t.Errorf("audit MED-3: failFirst=2 应 fire 2 个 retry event (attempt 1 + 2), got %d\n"+
			"mock call count=%d", len(actions), stub.calls)
	}
	// 验首 retry event 含正确字段
	a := actions[0]
	if a.Layer != agent_v2.CompactionLayerReactive {
		t.Errorf("Layer 应 = reactive, got %s", a.Layer)
	}
	if a.Retry == nil {
		t.Fatal("Retry 字段 nil")
	}
	if a.Retry.Attempt != 1 {
		t.Errorf("第 1 retry event Attempt 应 = 1, got %d", a.Retry.Attempt)
	}
	if a.Retry.MaxAttempts != 3 {
		t.Errorf("MaxAttempts 应 = 3, got %d", a.Retry.MaxAttempts)
	}
	if a.Retry.Reason != "prompt_too_long" {
		t.Errorf("Reason 应 = prompt_too_long, got %s", a.Retry.Reason)
	}
	if a.Retry.OrigMsgs == 0 || a.Retry.CompactedMsgs == 0 {
		t.Errorf("OrigMsgs/CompactedMsgs 应 > 0, got %+v", a.Retry)
	}
	// audit MED-3: 第 2 retry event Attempt 应 = 2 (跨 attempt 递增)
	if len(actions) >= 2 {
		a2 := actions[1]
		if a2.Retry == nil || a2.Retry.Attempt != 2 {
			t.Errorf("第 2 retry event Attempt 应 = 2, got %+v", a2.Retry)
		}
	}
}

func TestPhase2Sub4_ReactiveRetry_NoRetryNoEvent(t *testing.T) {
	// sink-gated 新语义:LLM 不报 prompt_too_long → 无 retry → 不 fire retry event
	// (接了 OrchHooks 也一样)。证明 retry event 只在真 retry 时发。
	stub := &promptTooLongThenOk{failFirst: 0} // 第一次就成功,不触发 retry
	sink := &captureRetrySink{}
	ctx := compose_v2.WithOrchHooks(context.Background(), sink)

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("test_retry_noretry"),
		agent_v2.WithModel(stub),
		agent_v2.WithReactiveCompact(&agent_v2.ReactiveCompactConfig{
			Enabled:              true,
			MaxAttempts:          3,
			EmergencyDropPercent: 50,
		}),
	)

	sess := agent_v2.NewRunSession()
	sess.AddEvent(agent_v2.NewOutputEvent("test_retry_noretry", schema.UserMessage("q")))

	_, _ = ag.Run(ctx, sess, schema.UserMessage("query"))

	if got := len(sink.retryActions()); got != 0 {
		t.Errorf("LLM 不报 prompt_too_long 时应 0 retry event, got %d", got)
	}
}
