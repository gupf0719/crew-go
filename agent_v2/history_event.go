package agent_v2

import (
	"context"

	"crew-go/compose_v2"
)

// ────────────────────────────────────────────────────────────
// history.compact event 协议 (Phase 2 D2 sub-2)
//
// 设计源头:
//   - crew-go 独有 HistoryCompactor 协议 (config.go::HistoryCompactor func)
//   - OTel GenAI 约定没有对应语义 — 我们走 framework 自定义 event.Type=history.compact
//
// 设计核心 (跟 StoreAction / CompactionAction 协议一致):
//   - **sink-gated (无独立开关)**: caller 用 hasEventSink(ctx) 门 fire, 无独立
//     EmitHistoryCompactEvent 布尔——接了 observability 就自动观测
//   - **走 compose_v2.EventSink.Send**: 跟 chunk / reset_stream / compaction /
//     store 同协议
//   - **零开销 sink nil**: ctx 内无 EventSink 时立刻 return
//   - **结构化字段**: 业务订阅 OnNodeEvent 按 event.Type 分发处理 *HistoryCompactAction
//
// observability 的 OnNodeEvent 把它记成节点 span 上的 crew.history.compact 事件（结构化字段不展开）.
// ────────────────────────────────────────────────────────────

// HistoryCompactEventType 是 compose_v2.Event.Type 字符串.
const HistoryCompactEventType = "history.compact"

// HistoryCompactAction 是 compose_v2.Event.Metadata["action"] 内的结构化字段.
type HistoryCompactAction struct {
	// InputMsgs HistoryCompactor 入参 messages 数 (压缩前 prior 长度).
	InputMsgs int `json:"input_msgs"`
	// OutputMsgs HistoryCompactor 返 messages 数 (压缩后 prior 长度).
	// 失败时为 0.
	OutputMsgs int `json:"output_msgs"`
	// KeepHint caller 期望保留的 keepPairs 提示 (Config.MaxHistoryPairs 或默认 20).
	KeepHint int `json:"keep_hint"`
	// DurationMs HistoryCompactor 调用耗时 (毫秒).
	DurationMs int64 `json:"duration_ms"`
	// Ok 调用是否成功 (err == nil).
	Ok bool `json:"ok"`
	// Err 失败原因 (err.Error()), 成功时空字符串.
	Err string `json:"err,omitempty"`
}

// EmitHistoryCompactEvent 投递 history.compact event 到 compose_v2.EventSink.
//
// **sink-gated**: caller 用 hasEventSink(ctx) 门决定是否调本函数(无独立开关).
// **零开销路径**: ctx 内无 EventSink 时立刻 return.
//
// 跟 EmitStoreEvent / EmitCompactionEvent 设计完全一致.
func EmitHistoryCompactEvent(ctx context.Context, action *HistoryCompactAction) {
	if action == nil {
		return
	}
	sink := compose_v2.GetEventSink(ctx)
	if sink == nil {
		return
	}
	sink.Send(compose_v2.Event{
		Type: HistoryCompactEventType,
		Metadata: map[string]any{
			"action": action,
		},
	})
}
