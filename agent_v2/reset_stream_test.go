package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// flakyModel 第 1 次 Generate 抛 prompt-too-long，第 2 次返正常输出。
// 验证 ReactiveCompact 在第 1 次失败后触发 retry，emit reset_stream event。
type flakyModel struct {
	calls int
}

func (m *flakyModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.calls++
	if m.calls == 1 {
		return nil, errors.New(`{"error":{"code":"context_length_exceeded","message":"prompt is too long: 50000 tokens > 8192 maximum"}}`)
	}
	return schema.AssistantMessage("retry succeeded"), nil
}
func (m *flakyModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *flakyModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*flakyModel)(nil)

// captureHooks 收集 OnNodeEvent 触发的 events 让测试断言。
type captureHooks struct {
	agent_v2.NoOpOrchestrationHooks
	mu     sync.Mutex
	events []agent_v2.Event
}

func (h *captureHooks) OnNodeEvent(ctx context.Context, _ string, ev agent_v2.Event) context.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, ev)
	return ctx
}

// TestResetStream_EmitOnReactiveCompactRetry 验证：
//   - flakyModel 第 1 次抛 prompt-too-long
//   - ReactiveCompact 删旧消息组 + 第 2 次 retry 成功
//   - retry 前框架通过 EventSink 投递 type=reset_stream 事件
//   - captureHooks.OnNodeEvent 应收到 reset_stream + reason=prompt_too_long
func TestResetStream_EmitOnReactiveCompactRetry(t *testing.T) {
	stub := &flakyModel{}
	hooks := &captureHooks{}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("test_reset_stream"),
		agent_v2.WithModel(stub),
		agent_v2.WithReactiveCompact(&agent_v2.ReactiveCompactConfig{Enabled: true, MaxAttempts: 3}),
		agent_v2.WithMaxRounds(3),
	)

	// 给 BasicAgent 喂多轮历史让 ReactiveCompact 有东西删
	sess := &agent_v2.RunSession{RunID: "test_rs"}
	for i := 0; i < 3; i++ {
		sess.AddEvent(agent_v2.NewOutputEvent("test_reset_stream", schema.UserMessage(fmt.Sprintf("prior %d", i))))
		sess.AddEvent(agent_v2.NewOutputEvent("test_reset_stream", schema.AssistantMessage(fmt.Sprintf("ans %d", i))))
	}

	ctx := context.Background()
	_, err := ag.Run(ctx, sess, schema.UserMessage("trigger compact"),
		agent_v2.WithOrchHooks(hooks))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 验证 model 被调 2 次（第 1 次 fail，第 2 次 retry 成功）
	if stub.calls != 2 {
		t.Fatalf("expected model.Generate called 2 times (1 fail + 1 retry), got %d", stub.calls)
	}

	// 验证 hooks 收到 reset_stream event
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	gotResetStream := false
	for _, ev := range hooks.events {
		if ev.Type != "reset_stream" {
			continue
		}
		gotResetStream = true
		// 验证 metadata 含 reason
		if reason, _ := ev.Metadata["reason"].(string); reason != "prompt_too_long" {
			t.Fatalf("reset_stream event metadata.reason 应是 'prompt_too_long'，实际 %q", reason)
		}
		// audit Round 5 Bug #2 修复：reset_stream 必须附 run_id 让前端精确清此
		// stream group buffer——之前缺 run_id 前端要么全清要么漏清。
		runID, _ := ev.Metadata["run_id"].(string)
		if runID == "" {
			t.Fatalf("Bug #2 reproduced: reset_stream event metadata 缺 run_id（前端无法精确清此 stream buffer）\n"+
				"修复方向：modelFn 管理 runID 生命周期，emit reset_stream 时附即将作废的 run_id\n"+
				"实际 metadata: %+v", ev.Metadata)
		}
		// agent name 也应附（多 sub-agent 场景前端按 agent 分流）
		if agName, _ := ev.Metadata["agent"].(string); agName != "test_reset_stream" {
			t.Fatalf("reset_stream event metadata.agent 应是 'test_reset_stream'，实际 %q", agName)
		}
	}
	if !gotResetStream {
		evTypes := make([]string, len(hooks.events))
		for i, e := range hooks.events {
			evTypes[i] = e.Type
		}
		t.Fatalf("ReactiveCompact retry 应 emit reset_stream event，实际收到 events: %v", evTypes)
	}
}

// 防 unused
var _ = compose_v2.Event{}
