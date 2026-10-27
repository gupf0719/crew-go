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

// TestResetStream_ReactiveCompactRetryWithoutChunks：第 1 次调用（Generate，没交出过片段）报 prompt-too-long，压缩后
// 重调成功。没有可撤的片段，不发 reset_stream——撤回只在失败点、只在真交出过片段时发（streamOrGenerate）。这里曾经
// 无条件再发一次，没片段时是空发、有片段时跟失败点那次重复。
func TestResetStream_ReactiveCompactRetryWithoutChunks(t *testing.T) {
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

	_, err := ag.Run(context.Background(), sess, schema.UserMessage("trigger compact"), agent_v2.WithOrchHooks(hooks))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stub.calls != 2 {
		t.Fatalf("expected model.Generate called 2 times (1 fail + 1 retry), got %d", stub.calls)
	}
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	for _, ev := range hooks.events {
		if ev.Type == "reset_stream" {
			t.Fatalf("第 1 次调用没交出过片段，不该发 reset_stream：%+v", ev.Metadata)
		}
	}
}

// chunkThenTooLongModel 第 1 次流出一个片段后以 prompt-too-long 结束（provider 在流中途报上下文超窗），之后正常。
type chunkThenTooLongModel struct{ calls int }

func (m *chunkThenTooLongModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, errors.New("unused")
}
func (m *chunkThenTooLongModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	m.calls++
	sr, send := schema.NewStreamReader[*schema.Message](2)
	if m.calls == 1 {
		send(&schema.Message{Role: schema.RoleAssistant, Content: "half"})
		schema.SendError(sr, errors.New(`{"error":{"code":"context_length_exceeded","message":"prompt is too long: 50000 tokens > 8192 maximum"}}`))
	} else {
		send(&schema.Message{Role: schema.RoleAssistant, Content: "ok"})
	}
	schema.CloseSend(sr)
	return sr, nil
}
func (m *chunkThenTooLongModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestResetStream_MidStreamTooLongResetOnce：交出过片段之后报上下文超窗、压缩后重调成功。作废那次的片段恰好撤回一次，
// 带它自己的 run_id。
func TestResetStream_MidStreamTooLongResetOnce(t *testing.T) {
	stub := &chunkThenTooLongModel{}
	hooks := &captureHooks{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("rs2"), agent_v2.WithModel(stub),
		agent_v2.WithReactiveCompact(&agent_v2.ReactiveCompactConfig{Enabled: true, MaxAttempts: 3}), agent_v2.WithMaxRounds(3))
	sess := &agent_v2.RunSession{RunID: "test_rs2"}
	for i := 0; i < 3; i++ {
		sess.AddEvent(agent_v2.NewOutputEvent("rs2", schema.UserMessage(fmt.Sprintf("prior %d", i))))
		sess.AddEvent(agent_v2.NewOutputEvent("rs2", schema.AssistantMessage(fmt.Sprintf("ans %d", i))))
	}
	if _, err := ag.Run(context.Background(), sess, schema.UserMessage("go"), agent_v2.WithOrchHooks(hooks)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	var halfRun string
	resets := map[string]int{}
	for _, ev := range hooks.events {
		runID, _ := ev.Metadata["run_id"].(string)
		switch {
		case ev.Type == "chunk" && ev.Content == "half":
			halfRun = runID
		case ev.Type == "reset_stream":
			resets[runID]++
		}
	}
	if halfRun == "" || resets[halfRun] != 1 || len(resets) != 1 {
		t.Fatalf("作废那次调用（run_id %q）的片段应恰好撤回一次，实际 %v", halfRun, resets)
	}
}

// 防 unused
var _ = compose_v2.Event{}
