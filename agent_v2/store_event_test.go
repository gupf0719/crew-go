package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
)

// finalAnswerForSizeCheck 是 stub 模型的最终答复；ValueSizeBytes 的期望值直接由它算，
// 免得两处各写一份长度、改一处漏一处。
const finalAnswerForSizeCheck = "ok answer with some content for value size"

// store.search / store.put EventSink event 守护测试(sink-gated——无独立开关).
//
// 测覆盖:
//  1. 配 StoreMemory + 触发 preamble Search → fire 1 search event
//  2. commitFinal Put → fire 1 put event (含 NS/Key/ValueSize/Ok)
//  3. Search 失败 → fire event 含 Err 字段 + Ok=false
//  4. Put 失败 → fire event 含 Err 字段 + Ok=false
//  5. 没配 StoreMemory → 0 store event(Search/Put 根本没调)
//
// sanity-revert: 删 basic_agent.go::if hasEventSink 块内 EmitStoreEvent 调用 → 测试 fail.

// captureStoreSink 实现 compose_v2.OrchestrationHooks, 通过 OnNodeEvent 接 framework
// 转发的 store event (compose_v2.Run 在 NodeFunc 入口用 nodeEventSink 覆盖 ctx 的
// EventSink + 转发到 OrchestrationHooks.OnNodeEvent — 业务真集成路径).
type captureStoreSink struct {
	compose_v2.NoOpOrchestrationHooks
	events []compose_v2.Event
}

func (s *captureStoreSink) OnNodeEvent(ctx context.Context, _ compose_v2.NodeID, ev compose_v2.Event) context.Context {
	s.events = append(s.events, ev)
	return ctx
}

func (s *captureStoreSink) storeActions() []*agent_v2.StoreAction {
	var actions []*agent_v2.StoreAction
	for _, ev := range s.events {
		if ev.Type != agent_v2.StoreEventType {
			continue
		}
		if a, ok := ev.Metadata["action"].(*agent_v2.StoreAction); ok {
			actions = append(actions, a)
		}
	}
	return actions
}

// stubAssistModel 返固定 assistant response 让 commitFinal 触发 store.Put.
type stubAssistModel struct{}

func (m *stubAssistModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(finalAnswerForSizeCheck), nil
}
func (m *stubAssistModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *stubAssistModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func TestPhase2Sub1_StoreSearch_EmitsEvent(t *testing.T) {
	st := store.NewInMemoryStore()
	// 预 Put 几条让 Search 真返 items
	for _, k := range []string{"k1", "k2", "k3"} {
		if err := st.Put(context.Background(), store.NS("test_ns"), k, schema.UserMessage("data-"+k)); err != nil {
			t.Fatal(err)
		}
	}

	sink := &captureStoreSink{}
	ctx := compose_v2.WithOrchHooks(context.Background(), sink)
	ctx = store.WithContext(ctx, st)

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("test_store_search"),
		agent_v2.WithModel(&stubAssistModel{}),
		agent_v2.WithStore(st),
		agent_v2.WithStoreMemory(store.NS("test_ns"), &agent_v2.StoreMemoryConfig{
			SearchLimit: 5,
		}),
	)

	_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("data-k1 是什么"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	actions := sink.storeActions()
	// 应至少 1 search + 1 put event
	var searchSeen, putSeen bool
	for _, a := range actions {
		if a.Type == agent_v2.StoreOperationSearch {
			searchSeen = true
			if a.Search == nil {
				t.Errorf("Search action 字段 nil")
				continue
			}
			if a.Search.NS != "test_ns" {
				t.Errorf("Search.NS 应 = test_ns, got %q", a.Search.NS)
			}
			if !a.Search.Ok {
				t.Errorf("Search.Ok 应 true, got false")
			}
			if a.Search.DocsCount < 1 {
				t.Errorf("Search.DocsCount 应 >= 1 (预 Put 3 条), got %d", a.Search.DocsCount)
			}
		}
		if a.Type == agent_v2.StoreOperationPut {
			putSeen = true
			if a.Put == nil {
				t.Errorf("Put action 字段 nil")
				continue
			}
			if a.Put.NS != "test_ns" {
				t.Errorf("Put.NS 应 = test_ns, got %q", a.Put.NS)
			}
			if a.Put.Key == "" {
				t.Errorf("Put.Key 应非空")
			}
			if !a.Put.Ok {
				t.Errorf("Put.Ok 应 true")
			}
			// ⚠️ 断言**具体数值**而不只是「非 0」：这个字段的口径是「实际交给 Put 的那个
			// 值的大小」，而 commitFinal 交给 Put 的是 `last.Content`（答复文本）。
			// 只断言非 0 的话，把它改回 `json.Marshal(整条消息)`（信封固定多出约 70 字节、
			// 短答复上高估数倍）照样全绿——那正是这个字段之前一直报错数的原因。
			if want := len(finalAnswerForSizeCheck); a.Put.ValueSizeBytes != want {
				t.Errorf("Put.ValueSizeBytes = %d, want %d（= len(答复文本)）——"+
					"报的必须是真正存进去的那个值的大小，不是整条消息序列化后的大小",
					a.Put.ValueSizeBytes, want)
			}
		}
	}
	if !searchSeen {
		t.Fatalf("store event regression: 有 sink 应 fire store.search event\n"+
			"sanity-revert: 删 basic_agent.go 内 store.Search EmitStoreEvent block → 此测 fail\n"+
			"got %d actions: %+v", len(actions), actions)
	}
	if !putSeen {
		t.Fatalf("store event regression: 有 sink 应 fire store.put event\n" +
			"sanity-revert: 删 basic_agent.go 内 store.Put EmitStoreEvent block → 此测 fail")
	}
}

func TestPhase2Sub1_Store_NoStoreMemoryNoEvent(t *testing.T) {
	// sink-gated 新语义:没配 WithStoreMemory → 不 Search/Put → 0 store event
	// (接了 OrchHooks 也一样)。证明 store event 只在 StoreMemory 集成真跑时发。
	st := store.NewInMemoryStore()
	sink := &captureStoreSink{}
	ctx := compose_v2.WithOrchHooks(context.Background(), sink)
	ctx = store.WithContext(ctx, st)

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("test_no_storemem"),
		agent_v2.WithModel(&stubAssistModel{}),
		agent_v2.WithStore(st),
		// 不配 WithStoreMemory
	)

	_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("data-k1 是什么"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}
	if got := len(sink.storeActions()); got != 0 {
		t.Errorf("没配 WithStoreMemory 应 0 store event, got %d", got)
	}
}

// failSearchStore 模拟 store.Search 失败.
type failSearchStore struct {
	store.Store
}

func (f *failSearchStore) Search(_ context.Context, _ store.Namespace, _ ...store.SearchOption) ([]*store.SearchItem, error) {
	return nil, errors.New("simulated search backend error")
}

// failPutStore 模拟 store.Put 失败 (audit HIGH-2 fix 用)
type failPutStore struct {
	store.Store
}

func (f *failPutStore) Put(_ context.Context, _ store.Namespace, _ string, _ any, _ ...store.PutOption) error {
	return errors.New("simulated put backend error")
}

// TestPhase2Sub1_StorePutFailure_EmitsErrInEvent 验 audit HIGH-2: Put 失败路径
// 也 fire event 含 Ok=false + Err 字段. 之前漏测试让 Put 失败 fire 代码 dead.
func TestPhase2Sub1_StorePutFailure_EmitsErrInEvent(t *testing.T) {
	innerSt := store.NewInMemoryStore()
	st := &failPutStore{Store: innerSt}
	sink := &captureStoreSink{}
	ctx := compose_v2.WithOrchHooks(context.Background(), sink)
	ctx = store.WithContext(ctx, st)

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("test_put_fail"),
		agent_v2.WithModel(&stubAssistModel{}),
		agent_v2.WithStore(st),
		agent_v2.WithStoreMemory(store.NS("test_ns"), &agent_v2.StoreMemoryConfig{
			SearchLimit: 5,
		}),
	)

	_, _ = ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("data-k1 是什么"))

	actions := sink.storeActions()
	var sawPutFail bool
	for _, a := range actions {
		if a.Type == agent_v2.StoreOperationPut && a.Put != nil && !a.Put.Ok {
			sawPutFail = true
			if !strings.Contains(a.Put.Err, "simulated put backend error") {
				t.Errorf("Put.Err 应含真实 err msg, got %q", a.Put.Err)
			}
		}
	}
	if !sawPutFail {
		t.Errorf("Put 失败时应 fire event 含 Ok=false + Err 字段\n"+
			"sanity-revert: 删 basic_agent.go 内 commitFinal store.Put EmitStoreEvent block → 此测 fail\n"+
			"got actions %+v", actions)
	}
}

// TestPhase2Sub1_AuditHIGH1_MicroCompactOffloadPutEmitsEvent 验 audit HIGH-1:
// microcompact.go offload 路径 (第 3 个 framework store.Put callsite) 也 fire
// store.put event. 之前漏 fire 让业务订阅 observability 拿不到这条最高频路径.
//
// sanity-revert: 删 microcompact.go::offloadStore.Put EmitStoreEvent block → 此测 fail
func TestPhase2Sub1_AuditHIGH1_MicroCompactOffloadPutEmitsEvent(t *testing.T) {
	st := store.NewInMemoryStore()
	sink := &captureStoreSink{}
	// 直接调 MicroCompactRun 验 offload fire (不走 BasicAgent.Run — 直接调时可控制
	// ctx 内是否挂 EventSink, 精确验 sink-gated 行为).
	// MicroCompactRun 接受 ctx 内 EventSink 直接 fire (不经 framework nodeEventSink
	// 覆盖, 因为 MicroCompactRun 不在 NodeFunc 内独立运行 — 实际生产路径会被
	// nodeEventSink 覆盖, 但 unit test 直接调时 ctx EventSink 直接 visible).
	ctx := compose_v2.WithEventSink(context.Background(), &eventSinkFromOrch{captureStoreSink: sink})
	ctx = store.WithContext(ctx, st)

	cfg := &agent_v2.MicroCompactConfig{
		Enabled:        true,
		KeepRecent:     2,
		OffloadEnabled: true,
	}
	msgs := []*schema.Message{}
	for i := 0; i < 5; i++ {
		callID := "off_tc_" + string(rune('0'+i))
		tc := schema.ToolCall{ID: callID, Function: schema.FunctionCall{Name: "Read"}}
		msgs = append(msgs, schema.AssistantToolCallMessage([]schema.ToolCall{tc}))
		msgs = append(msgs, schema.ToolResultMessage(callID, "long original content offloaded"))
	}
	_ = agent_v2.MicroCompactRun(ctx, msgs, cfg)

	// 早 3 个被 offload — 应 fire 3 store.put event
	actions := sink.storeActions()
	putCount := 0
	for _, a := range actions {
		if a.Type == agent_v2.StoreOperationPut && a.Put != nil && a.Put.Ok {
			putCount++
		}
	}
	if putCount != 3 {
		t.Errorf("audit HIGH-1: microcompact offload 应 fire 3 store.put event, got %d\n"+
			"sanity-revert: 删 microcompact.go::offloadStore.Put EmitStoreEvent block → 此测 fail",
			putCount)
	}
}

// eventSinkFromOrch 让 captureStoreSink (本来是 OrchHooks) 也实现 EventSink
// 接 MicroCompactRun 直接调用时 ctx EventSink 路径 (MicroCompactRun unit test
// 直接调不走 BasicAgent.Run 所以 nodeEventSink 不会覆盖).
type eventSinkFromOrch struct {
	*captureStoreSink
}

func (a *eventSinkFromOrch) Send(ev compose_v2.Event) {
	a.captureStoreSink.events = append(a.captureStoreSink.events, ev)
}

func TestPhase2Sub1_StoreSearchFailure_EmitsErrInEvent(t *testing.T) {
	innerSt := store.NewInMemoryStore()
	st := &failSearchStore{Store: innerSt}
	sink := &captureStoreSink{}
	ctx := compose_v2.WithOrchHooks(context.Background(), sink)
	ctx = store.WithContext(ctx, st)

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("test_search_fail"),
		agent_v2.WithModel(&stubAssistModel{}),
		agent_v2.WithStore(st),
		agent_v2.WithStoreMemory(store.NS("test_ns"), &agent_v2.StoreMemoryConfig{
			SearchLimit: 5,
		}),
	)

	_, _ = ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("data-k1 是什么"))

	actions := sink.storeActions()
	var sawSearchFail bool
	for _, a := range actions {
		if a.Type == agent_v2.StoreOperationSearch && a.Search != nil && !a.Search.Ok {
			sawSearchFail = true
			if !strings.Contains(a.Search.Err, "simulated search backend error") {
				t.Errorf("Search.Err 应含真实 err msg, got %q", a.Search.Err)
			}
		}
	}
	if !sawSearchFail {
		t.Errorf("Search 失败时应 fire event 含 Ok=false + Err 字段, got actions %+v", actions)
	}
}
