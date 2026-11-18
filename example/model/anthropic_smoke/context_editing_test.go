//go:build integration

package anthropic_smoke_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"crew-go/model"
	"crew-go/model/anthropic"
	"crew-go/schema"
)

func TestSmoke_ContextEditing(t *testing.T) {
	keep := int64(1)
	c, rec := newClient(t,
		anthropic.WithEffort(anthropic.EffortHigh),
		anthropic.WithThinkingDisplay(anthropic.DisplaySummarized),
		anthropic.WithContextEditing(anthropic.ContextEditing{
			ToolUses: &anthropic.ToolUseClearing{TriggerToolUses: 2, KeepToolUses: &keep},
			Thinking: &anthropic.ThinkingClearing{KeepTurns: 2},
		}),
		anthropic.WithCompaction(anthropic.Compaction{TriggerTokens: 50000}),
	)
	llm, err := c.WithTools([]*schema.ToolInfo{{Name: "lookup", Desc: "查记录", Params: strictSchema("id")}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	noTools := model.WithToolChoice(schema.ToolChoiceNone)
	history := []*schema.Message{schema.UserMessage("查三条历史记录。")}
	var calls []schema.ToolCall
	var results []*schema.Message
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("toolu_context_%d", i)
		calls = append(calls, schema.ToolCall{ID: id, Type: "function", Function: schema.FunctionCall{
			Name: "lookup", Arguments: fmt.Sprintf(`{"id":"%d"}`, i),
		}})
		results = append(results, schema.ToolResultMessage(id, strings.Repeat("旧记录的详细说明。", 80)))
	}
	history = append(history, schema.AssistantToolCallMessage(calls))
	history = append(history, results...)
	history = append(history, schema.AssistantMessage("三条记录已收到。"),
		schema.UserMessage("用中文简要解释为什么长度为 7 的二进制串中，恰有 3 个 1 且任意两个 1 不相邻的串有 10 个。"))
	first, err := llm.Generate(ctx, history, noTools)
	if err != nil {
		t.Fatalf("清理旧工具结果: %v", err)
	}
	checkNoPrefixMismatch(t, first)
	if edits := anthropic.AppliedContextEdits(first); !slices.ContainsFunc(edits, func(e anthropic.AppliedContextEdit) bool {
		return e.ClearedToolUses > 0 && e.ClearedInputTokens > 0
	}) {
		t.Fatalf("服务端未报告旧工具结果清理: %+v", edits)
	}

	history = append(history, first, schema.UserMessage("同样的规则下，如果长度为 12，恰有 5 个 1，计算数量并简要解释。"))
	stream, err := llm.Stream(ctx, history, noTools)
	if err != nil {
		t.Fatalf("流式回放签名: %v", err)
	}
	second := aggregate(t, stream)
	checkNoPrefixMismatch(t, second)
	if replayedThinking(rec.requests(t)) == 0 {
		t.Fatal("第二轮未实际发送签名 thinking 块，无法验证严格前缀绑定")
	}

	history = append(history, second, schema.UserMessage("把上面的计数写成长度 n、恰有 k 个 1 的通式。"))
	third, err := llm.Generate(ctx, history, noTools)
	if err != nil {
		t.Fatalf("清理旧 thinking: %v", err)
	}
	checkNoPrefixMismatch(t, third)
	if edits := anthropic.AppliedContextEdits(third); !slices.ContainsFunc(edits, func(e anthropic.AppliedContextEdit) bool {
		return e.ClearedThinkingTurns > 0 && e.ClearedInputTokens > 0
	}) {
		t.Fatalf("服务端未报告旧 thinking 清理: %+v", edits)
	}
}
