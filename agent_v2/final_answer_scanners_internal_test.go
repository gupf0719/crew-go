package agent_v2

import (
	"strings"
	"testing"

	"crew-go/schema"
)

// TestUnresolvedCallsRecord_StillRecognized 钉住记账文本的识别谓词。
//
// 记账文本（`dropDanglingToolCalls` 给零结果工具轮补的那条）是「assistant + 零 tool_call」，
// **正好长得像一条最终答复**。凡是框架**挑一条消息代表某件事**的地方都得跳过它。
//
// # 这条哨兵守的范围变小了，如实记下来
//
// 它原先登记三处扫描（`lastAssistant` / `BasicAgentFinalMessage` / observability 的
// `lastAssistantMessage`）。前两处现在**不扫消息列表了**——最终产出改由决策点写进专用
// channel，而记账文本在 preamble 侧合成、进不了那条 channel。所以那两处的防护从
// 「靠谓词跳过」变成了「结构上不可能」，谓词对它们不再承重（两个 helper 已作为死代码删除）。
//
// 仍在扫、仍需要谓词的只剩 **`observability.lastAssistantMessage`**（它扫的是业务 hook
// 入参 `out.Messages`，那份含 prior）和 ToolFilter 的 lastInput。**那一处有自己的用例**
// （`observability/last_assistant_internal_test.go`），本用例只守谓词本身。
//
// ⚠️ 别把这条当成「三处一致」的保证——那个保证已经不成立，因为只剩一处在扫。
func TestUnresolvedCallsRecord_StillRecognized(t *testing.T) {
	// 走真实产出路径造记账文本，不手搓 annotation——手搓的话 marker key 改了本用例还绿。
	var rec *schema.Message
	for _, m := range dropDanglingToolCalls([]*schema.Message{
		schema.UserMessage("转 5000"),
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "c1", Type: "function",
			Function: schema.FunctionCall{Name: "transfer_money", Arguments: `{"amount":5000}`},
		}}),
	}) {
		if IsUnresolvedCallsRecord(m) {
			rec = m
		}
	}
	if rec == nil {
		t.Fatal("没造出记账文本——本用例的前提没了")
	}
	if !strings.Contains(rec.Content, "transfer_money") {
		t.Fatalf("记账文本内容不对：%q", rec.Content)
	}
	if !IsUnresolvedCallsRecord(rec) {
		t.Fatal("谓词不认自己产出的记账文本")
	}
	// 承重：普通 assistant 不能被误判，否则真答复会被跳过丢掉
	if IsUnresolvedCallsRecord(schema.AssistantMessage("这是真答复")) {
		t.Fatal("普通 assistant 被误判成记账文本")
	}
	if IsUnresolvedCallsRecord(nil) {
		t.Fatal("nil 应返 false")
	}
}
