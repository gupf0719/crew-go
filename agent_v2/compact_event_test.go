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

// compact event 协议守护测试(sink-gated——无独立 EmitEvents 开关).
//
// 测覆盖:
//  1. MicroCompact 有 EventSink 时 fire Before + After 2 event (含 Dropped/Kept)
//  2. AutoCompact 有 EventSink 时 fire Before + Generate + After 3 event (含失败 Generate)
//  3. ReactiveCompact 有 EventSink 时 fire Before + After 2 event
//  4. 无 EventSink 时压缩核心逻辑正常执行(event 是纯观测旁路,门 = sink 有无)
//
// sanity-revert: 删 microcompact/autocompact/reactive_compact 内 EmitCompactionEvent
// 调用 → assertion 拿到 0 event → 此测 fail (验 fix 真在 hot path 执行).

// capturingSink 实现 compose_v2.EventSink, 记录所有 fire 的 event.
type capturingSink struct {
	mu     sync.Mutex
	events []compose_v2.Event
}

func (s *capturingSink) Send(ev compose_v2.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
}

func (s *capturingSink) compactionEvents() []*agent_v2.CompactionAction {
	s.mu.Lock()
	defer s.mu.Unlock()
	var actions []*agent_v2.CompactionAction
	for _, ev := range s.events {
		if ev.Type != agent_v2.CompactionEventType {
			continue
		}
		if action, ok := ev.Metadata["action"].(*agent_v2.CompactionAction); ok {
			actions = append(actions, action)
		}
	}
	return actions
}

// ── MicroCompact ──

func TestAudit5_HIGH1_MicroCompact_EmitsBeforeAfter(t *testing.T) {
	cfg := &agent_v2.MicroCompactConfig{
		Enabled:    true,
		KeepRecent: 2,
	}
	// 构造 5 个 Read tool_result 触发清 3 个 (keep recent 2)
	msgs := []*schema.Message{}
	for i := 0; i < 5; i++ {
		callID := fmt.Sprintf("tc_%d", i)
		tc := schema.ToolCall{ID: callID, Function: schema.FunctionCall{Name: "Read", Arguments: `{"path":"/x"}`}}
		msgs = append(msgs, schema.AssistantToolCallMessage([]schema.ToolCall{tc}))
		msgs = append(msgs, schema.ToolResultMessage(callID, "Read", "long content..."))
	}

	sink := &capturingSink{}
	ctx := compose_v2.WithEventSink(context.Background(), sink)
	_ = agent_v2.MicroCompactRun(ctx, msgs, cfg)

	actions := sink.compactionEvents()
	if len(actions) != 2 {
		t.Fatalf("compact event regression: MicroCompact 有 sink 应 fire 2 event (Before+After), got %d\n"+
			"sanity-revert: 删 microcompact.go EmitCompactionEvent 调用 → 此测 fail", len(actions))
	}
	if actions[0].Layer != agent_v2.CompactionLayerMicro || actions[0].Type != agent_v2.CompactionActionBefore {
		t.Errorf("第 1 event 应 Layer=micro Type=before, got Layer=%s Type=%s", actions[0].Layer, actions[0].Type)
	}
	if actions[1].Layer != agent_v2.CompactionLayerMicro || actions[1].Type != agent_v2.CompactionActionAfter {
		t.Errorf("第 2 event 应 Layer=micro Type=after, got Layer=%s Type=%s", actions[1].Layer, actions[1].Type)
	}
	// After.Dropped 应是 3 (5 - keep 2)
	if actions[1].After == nil || actions[1].After.Dropped != 3 {
		t.Errorf("After.Dropped 应 = 3, got %+v", actions[1].After)
	}
}

func TestMicroCompact_NoSinkSafe(t *testing.T) {
	// sink-gated 新语义:无 EventSink(业务未接 observability)时压缩核心逻辑正常执行、
	// 不 fire event、不 panic——event fire 是纯观测旁路。
	cfg := &agent_v2.MicroCompactConfig{Enabled: true, KeepRecent: 2}
	msgs := []*schema.Message{}
	for i := 0; i < 5; i++ {
		callID := fmt.Sprintf("tc_%d", i)
		tc := schema.ToolCall{ID: callID, Function: schema.FunctionCall{Name: "Read"}}
		msgs = append(msgs, schema.AssistantToolCallMessage([]schema.ToolCall{tc}))
		msgs = append(msgs, schema.ToolResultMessage(callID, "Read", "x"))
	}
	// 无 sink ctx(context.Background 未 push EventSink）
	out := agent_v2.MicroCompactRun(context.Background(), msgs, cfg)
	// MicroCompact 清内容不删消息——消息数不变即证明核心逻辑跑了且未 panic。
	if len(out) != len(msgs) {
		t.Fatalf("无 sink 时 MicroCompact 应正常执行(清内容不删消息), in=%d out=%d", len(msgs), len(out))
	}
}

// ── AutoCompact ──

// alwaysSummaryModel 返回带 <summary> 标签的固定 response, 触发 AutoCompact 成功路径.
type alwaysSummaryModel struct{}

func (m *alwaysSummaryModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage("<summary>compacted</summary>"), nil
}
func (m *alwaysSummaryModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *alwaysSummaryModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func TestAudit5_HIGH1_AutoCompact_EmitsBeforeGenerateAfter(t *testing.T) {
	cfg := &agent_v2.AutoCompactConfig{
		Enabled: true,
		Model:   &alwaysSummaryModel{},
		// threshold = ModelContextWindow - SummaryMaxTokens - SummaryBufferTokens
		// 设 200/10/50 → threshold=140 token. estimateToken = (len+3)/4. 测试 msgs
		// 总长 > 560 chars 触发.
		ModelContextWindow:  200,
		SummaryMaxTokens:    10,
		SummaryBufferTokens: 50,
		// 微缩配置（阈值仅 ~140 token）——按同比例关掉可压缩区尺寸守卫，
		// 否则 early 达不到 MinCompactableTokens 默认 5000 会被静默跳过。
		MinCompactableTokens: 1,
	}
	// 构造足够多 messages 让 token 超阈值 (window=100, SummaryMaxTokens=10, buffer=13 → 阈值~77)
	msgs := []*schema.Message{schema.UserMessage("first question")}
	// 多轮 assistant + user 让 GroupMessagesByAPIRound 分多组
	for i := 0; i < 6; i++ {
		msgs = append(msgs, schema.AssistantMessage(fmt.Sprintf("answer round %d with enough content to push token over the threshold of seventy-seven tokens we set", i)))
		msgs = append(msgs, schema.UserMessage(fmt.Sprintf("follow up %d also containing extra padding text", i)))
	}

	sink := &capturingSink{}
	ctx := compose_v2.WithEventSink(context.Background(), sink)
	_, err := agent_v2.AutoCompactRun(ctx, cfg, msgs)
	if err != nil {
		t.Fatalf("AutoCompactRun err: %v", err)
	}

	actions := sink.compactionEvents()
	// 应至少 3 event: Before + Generate (1 attempt) + After
	if len(actions) < 3 {
		t.Fatalf("compact event regression: AutoCompact 有 sink 应 fire >=3 event (Before+Generate+After), got %d events: %+v\n"+
			"sanity-revert: 删 autocompact.go 内 EmitCompactionEvent 调用 → 此测 fail", len(actions), actions)
	}
	// 验类型序: 第一个必 Before, 最后必 After, 中间至少 1 个 Generate
	if actions[0].Type != agent_v2.CompactionActionBefore || actions[0].Layer != agent_v2.CompactionLayerAuto {
		t.Errorf("第 1 event 应 Layer=auto Type=before, got %+v", actions[0])
	}
	lastIdx := len(actions) - 1
	if actions[lastIdx].Type != agent_v2.CompactionActionAfter {
		t.Errorf("最后 event 应 Type=after, got %s", actions[lastIdx].Type)
	}
	var sawGenerate bool
	for i := 1; i < lastIdx; i++ {
		if actions[i].Type == agent_v2.CompactionActionGenerate && actions[i].Layer == agent_v2.CompactionLayerAuto {
			sawGenerate = true
			if actions[i].Generate == nil || actions[i].Generate.Attempt != 1 {
				t.Errorf("Generate.Attempt 应 = 1, got %+v", actions[i].Generate)
			}
		}
	}
	if !sawGenerate {
		t.Errorf("应看到 Generate event (LLM 调用之后), got actions: %+v", actions)
	}
}

// failModel 让 AutoCompact LLM 调用失败, 验失败 Generate event 仍 fire + After.Err 填.
type failModel struct{ err error }

func (m *failModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return nil, m.err
}
func (m *failModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *failModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func TestAudit5_HIGH1_AutoCompact_EmitsGenerateOnFailure(t *testing.T) {
	cfg := &agent_v2.AutoCompactConfig{
		Enabled:             true,
		Model:               &failModel{err: errors.New("simulated LLM 500")},
		ModelContextWindow:  200,
		SummaryMaxTokens:    10,
		SummaryBufferTokens: 50,
		// 微缩配置（阈值仅 ~140 token）——按同比例关掉可压缩区尺寸守卫，
		// 否则 early 达不到 MinCompactableTokens 默认 5000 会被静默跳过。
		MinCompactableTokens: 1,
	}
	msgs := []*schema.Message{schema.UserMessage("q")}
	for i := 0; i < 6; i++ {
		msgs = append(msgs, schema.AssistantMessage(fmt.Sprintf("answer round %d with enough content to exceed threshold a few thousand chars total", i)))
		msgs = append(msgs, schema.UserMessage(fmt.Sprintf("follow %d more pad", i)))
	}
	sink := &capturingSink{}
	ctx := compose_v2.WithEventSink(context.Background(), sink)
	_, _ = agent_v2.AutoCompactRun(ctx, cfg, msgs)

	actions := sink.compactionEvents()
	var generateErrSeen, afterErrSeen bool
	for _, a := range actions {
		if a.Type == agent_v2.CompactionActionGenerate && a.Generate != nil && a.Generate.Err != "" {
			generateErrSeen = true
		}
		if a.Type == agent_v2.CompactionActionAfter && a.After != nil && a.After.Err != "" {
			afterErrSeen = true
		}
	}
	if !generateErrSeen {
		t.Errorf("LLM 失败时 Generate.Err 应填, got actions: %+v", actions)
	}
	if !afterErrSeen {
		t.Errorf("AutoCompact 整体失败时 After.Err 应填 (defer 内 emit 拿到 err), got: %+v", actions)
	}
}

// ── ReactiveCompact ──

func TestAudit5_HIGH1_ReactiveCompact_EmitsBeforeAfter(t *testing.T) {
	cfg := &agent_v2.ReactiveCompactConfig{
		Enabled:              true,
		EmergencyDropPercent: 50,
	}
	// 构造足够多 messages 让 emergencyDrop 真删
	var msgs []*schema.Message
	for i := 0; i < 10; i++ {
		msgs = append(msgs, schema.UserMessage(fmt.Sprintf("u%d", i)))
		msgs = append(msgs, schema.AssistantMessage(fmt.Sprintf("a%d", i)))
	}
	sink := &capturingSink{}
	ctx := compose_v2.WithEventSink(context.Background(), sink)
	origLen := len(msgs)
	out, err := agent_v2.ReactiveCompactRun(ctx, cfg, msgs, errors.New("prompt is too long"))
	if err != nil {
		t.Fatalf("ReactiveCompactRun err: %v", err)
	}
	// audit HIGH-C3 anchor: 验 ReactiveCompact 真删 messages 不只是 fire event
	// (之前测试只验 event count 2, emergencyDrop 替为 noop 测试仍 PASS — 假绿).
	if len(out) >= origLen {
		t.Fatalf("audit HIGH-C3 regression: ReactiveCompact 应真删 messages (in=%d out=%d), emergencyDrop 失效?\n"+
			"sanity-revert: emergencyDropOldestGroups 替为 `return messages` noop → 此测 fail", origLen, len(out))
	}

	actions := sink.compactionEvents()
	if len(actions) != 2 {
		t.Fatalf("compact event regression: ReactiveCompact 有 sink 应 fire 2 event (Before+After), got %d\n"+
			"sanity-revert: 删 reactive_compact.go EmitCompactionEvent 调用 → 此测 fail", len(actions))
	}
	if actions[0].Layer != agent_v2.CompactionLayerReactive || actions[0].Type != agent_v2.CompactionActionBefore {
		t.Errorf("第 1 event 应 Layer=reactive Type=before, got %+v", actions[0])
	}
	if actions[1].Layer != agent_v2.CompactionLayerReactive || actions[1].Type != agent_v2.CompactionActionAfter {
		t.Errorf("第 2 event 应 Layer=reactive Type=after, got %+v", actions[1])
	}
	// Before.Trigger 应 = "prompt_too_long"
	if actions[0].Before == nil || actions[0].Before.Trigger != "prompt_too_long" {
		t.Errorf("Before.Trigger 应 = prompt_too_long, got %+v", actions[0].Before)
	}
}

func TestReactiveCompact_NoSinkSafe(t *testing.T) {
	// sink-gated 新语义:无 EventSink 时 emergencyDrop 仍真删 messages(功能不依赖 event)。
	cfg := &agent_v2.ReactiveCompactConfig{Enabled: true, EmergencyDropPercent: 50}
	var msgs []*schema.Message
	for i := 0; i < 10; i++ {
		msgs = append(msgs, schema.UserMessage(fmt.Sprintf("u%d", i)))
		msgs = append(msgs, schema.AssistantMessage(fmt.Sprintf("a%d", i)))
	}
	out, err := agent_v2.ReactiveCompactRun(context.Background(), cfg, msgs, errors.New("prompt is too long"))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(out) >= len(msgs) {
		t.Fatalf("无 sink 时 emergencyDrop 仍应真删 messages, in=%d out=%d", len(msgs), len(out))
	}
}

// TestEmitCompactionEvent_SinkGating 守护 sink-gated 核心不变量:无 sink → nil-safe
// 不 Send;有 sink → Send。改造后的 NoSinkSafe/NoCompactor/NoRetry 等只验压缩功能 /
// 路径不可达,对 sink 门 tautological(门恒开/恒关都 PASS),不覆盖此核心语义,故单独守护。
func TestEmitCompactionEvent_SinkGating(t *testing.T) {
	action := &agent_v2.CompactionAction{
		Layer:  agent_v2.CompactionLayerMicro,
		Type:   agent_v2.CompactionActionBefore,
		Before: &agent_v2.CompactionBeforeAction{},
	}
	// 无 sink(context.Background 未挂 EventSink):内 guard nil-safe,不 panic 不 Send
	agent_v2.EmitCompactionEvent(context.Background(), action)

	// 有 sink:Send 到 sink
	sink := &capturingSink{}
	ctx := compose_v2.WithEventSink(context.Background(), sink)
	agent_v2.EmitCompactionEvent(ctx, action)
	if got := len(sink.compactionEvents()); got != 1 {
		t.Fatalf("有 sink 时 EmitCompactionEvent 应 Send 1 event, got %d\n"+
			"sanity-revert: 删 EmitCompactionEvent 内 sink.Send 调用 → 有 sink 得 0 → 此测 fail; "+
			"删内 `if sink == nil` guard → 无 sink 段 nil.Send panic → 此测 fail", got)
	}
}
