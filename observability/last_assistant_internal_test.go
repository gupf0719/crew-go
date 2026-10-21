package observability

import (
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// TestLastAssistantMessage_SkipsUnresolvedCallsRecord 钉住「上报的 agent 最终答复不能是
// 框架自己的记账文本」。
//
// `lastAssistantMessage` 喂的是 `agent_output` event 的 `gen_ai_content`，那个 event 的
// 契约写明是「本次 agent 调用的**真 final 答复**」。而 `agent_v2` 的记账文本
// （见 `agent_v2.UnresolvedCallsRecordAnnotation`）是「assistant + 零 tool_call」，
// 正好命中朴素的倒扫判据。
//
// **可达路径**：上一轮 return-direct 留下悬空 tool_call → 这一轮 preamble 把它洗成记账
// 文本 → 本轮 LLM 在产出任何 assistant 之前失败/中断（API 500 / ctx cancel / OnModelStart
// 里做 HITL gate）⟹ 记账文本就是最后一条 assistant。**出事的恰好是 error 路径**——
// 也就是有人正在看 trace 排障的时候，看到的「agent 的回答」是一句中文框架记账。
//
// ⚠️ 这条**必须单独存在**：它跟 agent_v2 那两处共用同一个谓词，但「共用谓词」这个事实
// 不产生断言——谓词写对了不等于调用点调了它。实测过：把这里的跳过 revert 掉，
// 全仓 `go test ./...` 照样全绿。
func TestLastAssistantMessage_SkipsUnresolvedCallsRecord(t *testing.T) {
	rec := schema.AssistantMessage("[历史记录] 此前发起过以下工具调用…")
	rec.Annotations = map[string]any{agent_v2.UnresolvedCallsRecordAnnotation: true}
	real := schema.AssistantMessage("这是真答复")

	t.Run("记账文本排最后时跳过它", func(t *testing.T) {
		got := lastAssistantMessage([]*schema.Message{schema.UserMessage("q"), real, rec})
		if got != real {
			t.Fatalf("应拿到真答复，实际 %v", got)
		}
	})
	t.Run("只有记账文本时返 nil 而不是上报它", func(t *testing.T) {
		got := lastAssistantMessage([]*schema.Message{schema.UserMessage("q"), rec})
		if got != nil {
			t.Fatalf("宁可不填 gen_ai_content，也不能把框架记账当成 agent 的回答上报：%q",
				got.Content)
		}
	})
	t.Run("nil 元素不 panic", func(t *testing.T) {
		if got := lastAssistantMessage([]*schema.Message{real, nil}); got != real {
			t.Fatalf("应跨过 nil，实际 %v", got)
		}
	})
}
