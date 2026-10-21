package agent_v2_test

import (
	"context"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/model"
	specModel "crew-go/modules/model"
	"crew-go/schema"
)

// streamingModel 模拟真流式 LLM——Stream() 返 reader 投递 N 个 chunk 后 EOF。
// 验证 BasicAgent.streamOrGenerate：
//   - chunk 通过 EventSink 实时投递（type=chunk + run_id metadata）
//   - 最终聚合 message 含累积 content
type streamingModel struct {
	chunks []string // 按顺序投递的 chunk content
}

func (m *streamingModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	// 流模式专用 stub——caller 不应调 Generate（除非 Stream 失败 fallback）
	full := ""
	for _, c := range m.chunks {
		full += c
	}
	return schema.AssistantMessage(full), nil
}
func (m *streamingModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	sr, send := schema.NewStreamReader[*schema.Message](len(m.chunks))
	for _, c := range m.chunks {
		send(&schema.Message{Role: schema.RoleAssistant, Content: c})
	}
	schema.CloseSend(sr)
	return sr, nil
}
func (m *streamingModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*streamingModel)(nil)

// chunkCaptureHooks 收 OnNodeEvent 触发的 events——验证 chunk emit + run_id 一致性。
type chunkCaptureHooks struct {
	agent_v2.NoOpOrchestrationHooks
	mu     sync.Mutex
	events []agent_v2.Event
}

func (h *chunkCaptureHooks) OnNodeEvent(ctx context.Context, _ string, ev agent_v2.Event) context.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, ev)
	return ctx
}

// TestChunkStreaming_EmitsRealtimeChunks 验证 streamOrGenerate 真走 Stream API 时
// 通过 EventSink emit chunk events——前端按 type=chunk + run_id 累积渲染。
func TestChunkStreaming_EmitsRealtimeChunks(t *testing.T) {
	stub := &streamingModel{chunks: []string{"hello ", "world", "!"}}
	hooks := &chunkCaptureHooks{}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("stream_test"),
		agent_v2.WithModel(stub),
		agent_v2.WithMaxRounds(2),
	)

	events, err := ag.Run(context.Background(), nil, schema.UserMessage("hi"),
		agent_v2.WithOrchHooks(hooks))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 验证 final message 含完整聚合内容
	final := agent_v2.FinalMessage(events)
	if final == nil || final.Content != "hello world!" {
		t.Fatalf("expected final content 'hello world!', got %+v", final)
	}

	// 验证 hooks 收到 3 个 chunk events（每个 chunk 一个）
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	chunkEvs := []agent_v2.Event{}
	for _, ev := range hooks.events {
		if ev.Type == "chunk" {
			chunkEvs = append(chunkEvs, ev)
		}
	}
	if len(chunkEvs) != 3 {
		evTypes := make([]string, len(hooks.events))
		for i, e := range hooks.events {
			evTypes[i] = e.Type
		}
		t.Fatalf("expected 3 chunk events, got %d (all events: %v)", len(chunkEvs), evTypes)
	}
	// 验证 chunks content 顺序正确
	if chunkEvs[0].Content != "hello " || chunkEvs[1].Content != "world" || chunkEvs[2].Content != "!" {
		for i, e := range chunkEvs {
			t.Logf("chunk[%d]: %q", i, e.Content)
		}
		t.Fatalf("chunk content order wrong")
	}
	// 验证所有 chunks 共享同一 run_id（单次 LLM 调用 stream group）
	runID0, _ := chunkEvs[0].Metadata["run_id"].(string)
	if runID0 == "" {
		t.Fatal("chunk[0].Metadata.run_id should be non-empty")
	}
	for i := 1; i < len(chunkEvs); i++ {
		runIDi, _ := chunkEvs[i].Metadata["run_id"].(string)
		if runIDi != runID0 {
			t.Fatalf("chunks should share same run_id; got chunk[0]=%q chunk[%d]=%q", runID0, i, runIDi)
		}
	}
	// 验证 run_id 与 final message annotation 一致
	finalRunID, _ := final.Annotations["run_id"].(string)
	if finalRunID != runID0 {
		t.Fatalf("final message run_id should match chunk run_id; got final=%q chunks=%q", finalRunID, runID0)
	}
}

// 注：incremental tool_call accumulation 测试见 streaming_internal_test.go
// （white-box 直接验 streamOrGenerate）

// TestChunkStreaming_FallbackToGenerateWhenStreamUnsupported 验证模型不支持流式（Stream 返回
// nil reader）时 streamOrGenerate 改调 Generate（无 chunk emit）。Stream 报错时不改调，见
// stream_fallback_test.go。
func TestChunkStreaming_FallbackToGenerateWhenStreamUnsupported(t *testing.T) {
	stub := &fallbackStreamModel{generateResp: schema.AssistantMessage("fallback works")}
	hooks := &chunkCaptureHooks{}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("fallback_test"),
		agent_v2.WithModel(stub),
		agent_v2.WithMaxRounds(2),
	)

	events, err := ag.Run(context.Background(), nil, schema.UserMessage("hi"),
		agent_v2.WithOrchHooks(hooks))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	final := agent_v2.FinalMessage(events)
	if final == nil || final.Content != "fallback works" {
		t.Fatalf("expected fallback content, got %+v", final)
	}

	// 不支持流式 → 不应有 chunk events
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	for _, ev := range hooks.events {
		if ev.Type == "chunk" {
			t.Fatalf("不支持流式时不该有 chunk 事件，实际 %+v", ev)
		}
	}
}

// fallbackStreamModel: Stream() 总返 nil reader（视为不支持），triggers Generate fallback
type fallbackStreamModel struct {
	generateResp *schema.Message
}

func (m *fallbackStreamModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return m.generateResp, nil
}
func (m *fallbackStreamModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil // nil reader → streamOrGenerate 走 fallback
}
func (m *fallbackStreamModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*fallbackStreamModel)(nil)

// ──────────────────────────────────────────────────────────
// OnModelEndStream hook 测试（ISSUE-73 P1 接入）
// ──────────────────────────────────────────────────────────

// streamObserverHook 是个 hook.Hooks 实现——OnModelEndStream wrap reader 观测
// 每个 chunk（不修改 chunks，验证 hook 真被调用 + chunks 真透传）。
type streamObserverHook struct {
	hook.NoOpHooks
	mu        sync.Mutex
	chunks    []string // 观测到的 chunk content
	wrapCalls int      // OnModelEndStream 被调次数
}

func (h *streamObserverHook) OnModelEndStream(ctx context.Context, info *hook.AgentInfo, sr specModel.MessageStreamReader) (context.Context, specModel.MessageStreamReader, error) {
	h.mu.Lock()
	h.wrapCalls++
	h.mu.Unlock()
	// 返 wrapper 让业务能观测 chunks
	return ctx, &observingReader{inner: sr, parent: h}, nil
}

type observingReader struct {
	inner  specModel.MessageStreamReader
	parent *streamObserverHook
}

func (r *observingReader) Recv(ctx context.Context) (*schema.Message, error) {
	msg, err := r.inner.Recv(ctx)
	if err == nil && msg != nil && msg.Content != "" {
		r.parent.mu.Lock()
		r.parent.chunks = append(r.parent.chunks, msg.Content)
		r.parent.mu.Unlock()
	}
	return msg, err
}

func (r *observingReader) Close() error { return r.inner.Close() }

// TestOnModelEndStream_HookWrapsReader 验证 ISSUE-73 P1 接入：
// 业务用 hook.Hooks.OnModelEndStream wrap reader 后，能观测每个 chunk +
// final message 仍正确聚合（hook 透传不破坏聚合）。
func TestOnModelEndStream_HookWrapsReader(t *testing.T) {
	stub := &streamingModel{chunks: []string{"foo ", "bar ", "baz"}}
	observer := &streamObserverHook{}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("hook_stream_test"),
		agent_v2.WithModel(stub),
		agent_v2.WithMaxRounds(2),
		agent_v2.WithHooks(observer),
	)

	events, err := ag.Run(context.Background(), nil, schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 1. hook OnModelEndStream 被调 1 次（单 LLM 调用）
	observer.mu.Lock()
	wrapCalls := observer.wrapCalls
	chunks := append([]string(nil), observer.chunks...)
	observer.mu.Unlock()

	if wrapCalls != 1 {
		t.Errorf("OnModelEndStream 应被调 1 次，实际 %d", wrapCalls)
	}

	// 2. hook 观测到所有 chunks（透传不丢）
	if len(chunks) != 3 {
		t.Errorf("hook 应观测 3 chunks，实际 %d: %v", len(chunks), chunks)
	}
	expected := []string{"foo ", "bar ", "baz"}
	for i, c := range chunks {
		if i < len(expected) && c != expected[i] {
			t.Errorf("chunk[%d]=%q, want %q", i, c, expected[i])
		}
	}

	// 3. final message 仍正确聚合（hook wrap 不影响聚合）
	final := agent_v2.FinalMessage(events)
	if final == nil || final.Content != "foo bar baz" {
		t.Fatalf("final aggregate broken; got %+v", final)
	}
}

// TestOnModelEndStream_NoHook_Passthrough 验证 cfg.Hooks == nil 时 streaming
// 走原始 reader（无 wrap 开销）—— 行为与之前完全一致。
func TestOnModelEndStream_NoHook_Passthrough(t *testing.T) {
	stub := &streamingModel{chunks: []string{"a", "b"}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("nohook"),
		agent_v2.WithModel(stub),
		agent_v2.WithMaxRounds(2),
		// 不传 WithHooks
	)

	events, err := ag.Run(context.Background(), nil, schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	final := agent_v2.FinalMessage(events)
	if final == nil || final.Content != "ab" {
		t.Fatalf("nil hook should still aggregate; got %+v", final)
	}
}

// **R32+ 视角 4 audit 删**: TestAgentEvent_NSAutoFillFromCtx 测的是 AddEventInGraph
// + AgentEvent.NS 字段, 两者都已删除 (生产 0 caller / 0 reader, 设计被 ParallelAgent
// ForkLane lane 物理隔离替代)。
