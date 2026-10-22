package agent_v2

import (
	"context"

	"crew-go/compose_v2"
)

// ────────────────────────────────────────────────────────────
// store operation event 协议 (Phase 2 D2 sub-1)
//
// 设计参考:
//   - 语义参考 OTel GenAI 约定的 retrieval 操作（gen_ai.operation.name=retrieval）
//   - 通过 EventSink 让 framework 内调 store 操作 (search / put) 时自动 fire event
//
// 设计核心:
//   - **sink-gated (无独立开关)**: caller 用 hasEventSink(ctx) 门 fire, 无独立
//     StoreMemoryConfig.EmitEvents 布尔——接了 observability 就自动观测
//   - **走 compose_v2.EventSink.Send** (跟现有 chunk / reset_stream / compaction 同协议)
//   - **零开销 sink nil**: ctx 内无 EventSink 时 EmitStoreEvent 立刻 return
//   - **结构化字段**: action 用 union 类型 (Type=search/put), 业务订阅 OnNodeEvent
//     按 Type 分发处理 Search/Put 字段
//
// ⚠️ **框架内没有结构化消费者**: observability 的 OnNodeEvent 只把它记成一个 span 事件
// （*StoreAction 不是标量，不展开成属性），也不转指标。这条事件今天只服务业务自己在
// OrchHooks 里做的 type assert.
// ────────────────────────────────────────────────────────────

// StoreEventType 是 compose_v2.Event.Type 字符串.
const StoreEventType = "store"

// StoreOperationType 区分 store 操作类型.
type StoreOperationType string

const (
	// StoreOperationSearch 对应 store.Store.Search 调用.
	StoreOperationSearch StoreOperationType = "search"
	// StoreOperationPut 对应 store.Store.Put 调用.
	StoreOperationPut StoreOperationType = "put"
)

// StoreAction 是 compose_v2.Event.Metadata["action"] 内的结构化字段——业务订阅
// OnNodeEvent 按 Type 分发处理 Search/Put 字段.
//
// union 模式: 用单 Action 结构含 Type 字段 (跟 CompactionAction 设计统一).
type StoreAction struct {
	// Type 操作类型 (search / put).
	Type StoreOperationType `json:"type"`

	// Search 仅 Type=search 时非 nil.
	Search *StoreSearchAction `json:"search,omitempty"`
	// Put 仅 Type=put 时非 nil.
	Put *StorePutAction `json:"put,omitempty"`
}

// StoreSearchAction 对应 store.Search 调用，语义上对应 OTel GenAI 约定的 retrieval 操作.
type StoreSearchAction struct {
	// NS 是 search 的 namespace prefix (store.Namespace.String() 字符串形式).
	NS string `json:"ns"`
	// DocsCount 是 Search 返回的 SearchItem 数 (失败时 0).
	DocsCount int `json:"docs_count"`
	// DurationMs Search 调用耗时 (毫秒).
	DurationMs int64 `json:"duration_ms"`
	// Ok 调用是否成功 (err == nil).
	Ok bool `json:"ok"`
	// Err 失败原因 (err.Error()), 成功时空字符串.
	Err string `json:"err,omitempty"`
	// OutOfScopeDropped 是被丢弃的**越界条目**数——后端返了不在本 agent namespace 之下的
	// 记录。非 0 说明那个 Store 实现有 bug（归属过滤是它的义务）。
	//
	// 单独一个字段而不是并进 Err：它不是「这次调用失败了」，Ok 仍是 true、结果仍可用。
	// 但也不能不报——静默丢弃会让观测撒谎，而这里丢掉的恰恰是「后端把别人的数据递过来了」
	// 这种最该被看见的事。
	OutOfScopeDropped int `json:"out_of_scope_dropped,omitempty"`
}

// StorePutAction 对应 store.Put 调用.
type StorePutAction struct {
	// NS 是 put 的 namespace.
	NS string `json:"ns"`
	// Key 是 put 的 key.
	Key string `json:"key"`
	// ValueSizeBytes 是**实际交给 Put 的那个值**的字节数估算 (业务后端实际可能不同).
	// ⚠️ 口径是「存进去的那个值」不是「序列化后」——两个生产者据此各按各的:
	// commitFinal 存的是答复文本 (裸 string 长度), microcompact offload 存的是整条
	// 消息 (json.Marshal 长度). 0 表示业务侧未估算或 value 是 nil/复杂类型无法估.
	ValueSizeBytes int `json:"value_size_bytes"`
	// DurationMs Put 调用耗时 (毫秒).
	DurationMs int64 `json:"duration_ms"`
	// Ok 调用是否成功.
	Ok bool `json:"ok"`
	// Err 失败原因 (err.Error()), 成功时空字符串.
	Err string `json:"err,omitempty"`
}

// EmitStoreEvent 投递 store event 到 compose_v2.EventSink.
//
// **sink-gated**: caller 用 hasEventSink(ctx) 门决定是否调本函数 (跟
// EmitCompactionEvent 设计一致, 无独立 EmitEvents 开关).
//
// **零开销路径**: ctx 内无 EventSink 时立刻 return (业务不订阅 event 流时不付任何代价).
//
// ⚠️ 框架内无消费者 (见上面 StoreAction 的说明), 业务自己在 OrchHooks 里
// type assert Metadata["action"].(*StoreAction) 取用.
func EmitStoreEvent(ctx context.Context, action *StoreAction) {
	if action == nil {
		return
	}
	sink := compose_v2.GetEventSink(ctx)
	if sink == nil {
		return
	}
	sink.Send(compose_v2.Event{
		Type:    StoreEventType,
		Content: string(action.Type),
		Metadata: map[string]any{
			"action": action,
		},
	})
}
