package anthropic_test

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"

	"crew-go/model/anthropic"
	"crew-go/schema"
)

func TestContextEditingRequest(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  anthropic.ContextEditing
		want string
	}{
		{name: "off"},
		{
			name: "provider defaults",
			cfg:  anthropic.ContextEditing{Thinking: &anthropic.ThinkingClearing{}, ToolUses: &anthropic.ToolUseClearing{}},
			want: `{"edits":[{"type":"clear_thinking_20251015"},{"type":"clear_tool_uses_20250919"}]}`,
		},
		{
			name: "token threshold and explicit zero retention",
			cfg: anthropic.ContextEditing{ToolUses: &anthropic.ToolUseClearing{
				TriggerTokens: 30000, KeepToolUses: new(int64(0)), ClearAtLeastTokens: 5000,
				ExcludeTools: []string{"memory"}, ClearToolInputs: true,
			}},
			want: `{"edits":[{"clear_at_least":{"type":"input_tokens","value":5000},"clear_tool_inputs":true,"exclude_tools":["memory"],"keep":{"type":"tool_uses","value":0},"trigger":{"type":"input_tokens","value":30000},"type":"clear_tool_uses_20250919"}]}`,
		},
		{
			name: "tool threshold and selected inputs",
			cfg: anthropic.ContextEditing{ToolUses: &anthropic.ToolUseClearing{
				TriggerToolUses: 9, KeepToolUses: new(int64(4)), ClearToolInputsFor: []string{"lookup", "scan"},
			}},
			want: `{"edits":[{"clear_tool_inputs":["lookup","scan"],"keep":{"type":"tool_uses","value":4},"trigger":{"type":"tool_uses","value":9},"type":"clear_tool_uses_20250919"}]}`,
		},
		{
			name: "preserve all thinking",
			cfg:  anthropic.ContextEditing{Thinking: &anthropic.ThinkingClearing{KeepAll: true}},
			want: `{"edits":[{"keep":"all","type":"clear_thinking_20251015"}]}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, rec := newServer(t, textReply)
			c := newClient(srv, anthropic.WithContextEditing(tc.cfg))
			_, err := c.Generate(t.Context(), []*schema.Message{schema.UserMessage("q")})
			if err != nil {
				t.Fatal(err)
			}
			req := rec.at(t, 0)
			on := tc.want != ""
			if hasBeta := slices.Contains(betas(req.header), "context-management-2025-06-27"); hasBeta != on {
				t.Fatalf("上下文编辑 beta 应随策略启用，实际 %v", betas(req.header))
			}
			got, exists := req.body["context_management"]
			if exists != on || on && canon(t, got) != tc.want {
				t.Fatalf("context_management = %v，期望 %s", got, tc.want)
			}
		})
	}
}

func TestContextEditingRejectsInvalidOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  anthropic.ContextEditing
	}{
		{"two triggers", anthropic.ContextEditing{ToolUses: &anthropic.ToolUseClearing{TriggerTokens: 50000, TriggerToolUses: 3}}},
		{"negative trigger", anthropic.ContextEditing{ToolUses: &anthropic.ToolUseClearing{TriggerToolUses: -1}}},
		{"negative tool retention", anthropic.ContextEditing{ToolUses: &anthropic.ToolUseClearing{KeepToolUses: new(int64(-1))}}},
		{"negative minimum", anthropic.ContextEditing{ToolUses: &anthropic.ToolUseClearing{ClearAtLeastTokens: -1}}},
		{"two input policies", anthropic.ContextEditing{ToolUses: &anthropic.ToolUseClearing{ClearToolInputs: true, ClearToolInputsFor: []string{"lookup"}}}},
		{"negative thinking retention", anthropic.ContextEditing{Thinking: &anthropic.ThinkingClearing{KeepTurns: -1}}},
		{"two thinking retention policies", anthropic.ContextEditing{Thinking: &anthropic.ThinkingClearing{KeepTurns: 2, KeepAll: true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, rec := newServer(t, textReply)
			c := newClient(srv, anthropic.WithContextEditing(tc.cfg))
			_, err := c.Generate(t.Context(), []*schema.Message{schema.UserMessage("q")})
			if err == nil || !strings.Contains(err.Error(), "context editing") || rec.count() != 0 {
				t.Fatalf("非法编辑配置应在发送前拒绝，错误 %v，已发送 %d 次", err, rec.count())
			}
		})
	}
}

func TestContextEditingSnapshotsCallerOptions(t *testing.T) {
	keep := int64(4)
	tools := &anthropic.ToolUseClearing{KeepToolUses: &keep, ExcludeTools: []string{"memory"}, ClearToolInputsFor: []string{"lookup"}}
	thinking := &anthropic.ThinkingClearing{KeepTurns: 2}
	srv, rec := newServer(t, textReply)
	c := newClient(srv, anthropic.WithContextEditing(anthropic.ContextEditing{ToolUses: tools, Thinking: thinking}))
	keep, thinking.KeepTurns = 1, 3
	tools.ExcludeTools[0], tools.ClearToolInputsFor[0] = "other", "changed"
	callee, err := c.WithTools([]*schema.ToolInfo{{Name: "lookup"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := callee.Generate(t.Context(), []*schema.Message{schema.UserMessage("q")}); err != nil {
		t.Fatal(err)
	}
	want := `{"edits":[{"keep":{"type":"thinking_turns","value":2},"type":"clear_thinking_20251015"},{"clear_tool_inputs":["lookup"],"exclude_tools":["memory"],"keep":{"type":"tool_uses","value":4},"type":"clear_tool_uses_20250919"}]}`
	if got := canon(t, rec.at(t, 0).body["context_management"]); got != want {
		t.Fatalf("派生客户端读到了调用方改动的配置，实际 %s", got)
	}
}

// 两种返回路径把实际编辑记入标注，序列化后仍能读取。下一轮的历史前缀、压缩块和签名按原形状发回。
func TestContextEditingPreservesReplayAndCacheOptions(t *testing.T) {
	for _, mode := range []string{"generate", "stream"} {
		t.Run(mode, func(t *testing.T) {
			compaction := map[string]any{"type": "compaction", "content": "summary", "encrypted_content": "encrypted", "signature": "compact-signature"}
			thinking := map[string]any{"type": "thinking", "thinking": "", "signature": "thinking-signature"}
			answer := map[string]any{"type": "text", "text": "answer"}
			usage := map[string]any{"input_tokens": 7, "cache_read_input_tokens": 800, "cache_creation_input_tokens": 100, "output_tokens": 5}
			management := map[string]any{"applied_edits": []any{
				map[string]any{"type": "clear_thinking_20251015", "cleared_input_tokens": 120, "cleared_thinking_turns": 2},
				map[string]any{"type": "clear_tool_uses_20250919", "cleared_input_tokens": 8000, "cleared_tool_uses": 3},
			}}
			srv, rec := newServer(t, func(w http.ResponseWriter, r *http.Request, n int) {
				switch {
				case n > 1:
					textReply(w, r, n)
				case mode == "generate":
					response := message([]any{compaction, thinking, answer}, "end_turn", usage)
					response["context_management"] = management
					replyJSON(w, http.StatusOK, response)
				default:
					end := evMessageDelta("end_turn", usage)
					end.data.(map[string]any)["context_management"] = management
					replySSE(w, evMessageStart(nil),
						evBlockStart(0, compaction), evBlockStop(0),
						evBlockStart(1, thinking), evBlockStop(1),
						evBlockStart(2, map[string]any{"type": "text", "text": ""}),
						evDelta(2, map[string]any{"type": "text_delta", "text": "answer"}), evBlockStop(2),
						end, evMessageStop())
				}
			})
			c := newClient(srv,
				anthropic.WithCompaction(anthropic.Compaction{TriggerTokens: 50000}),
				anthropic.WithContextEditing(anthropic.ContextEditing{
					Thinking: &anthropic.ThinkingClearing{KeepTurns: 2},
					ToolUses: &anthropic.ToolUseClearing{TriggerTokens: 30000},
				}),
				anthropic.WithPrefixMismatch(anthropic.PrefixMismatchError),
				anthropic.WithCacheTTL(anthropic.CacheTTL1h),
				anthropic.WithBetas("context-management-2025-06-27", "compact-2026-01-12"),
			)
			llm, err := c.WithTools([]*schema.ToolInfo{{Name: "lookup"}})
			if err != nil {
				t.Fatal(err)
			}
			in := []*schema.Message{
				schema.SystemMessage("instructions"),
				schema.UserMessage("request"),
				schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "toolu_old", Type: "function",
					Function: schema.FunctionCall{Name: "lookup", Arguments: `{"id":9007199254740993}`}}}),
				schema.ToolResultMessage("toolu_old", "original tool result"),
			}

			var first *schema.Message
			if mode == "stream" {
				stream, err := llm.Stream(t.Context(), in)
				if err != nil {
					t.Fatal(err)
				}
				chunks, err := collect(t, stream)
				if !errors.Is(err, io.EOF) {
					t.Fatalf("流结束于 %v", err)
				}
				first = aggregate(chunks)
			} else {
				first, err = llm.Generate(t.Context(), in)
				if err != nil {
					t.Fatal(err)
				}
			}
			if first.Content != "answer" || first.Metadata == nil || first.Metadata.Usage.InputTokens != 907 {
				t.Fatalf("答复与用量应直接来自服务端，实际 %+v", first)
			}
			wantEdits := []anthropic.AppliedContextEdit{
				{Type: "clear_thinking_20251015", ClearedInputTokens: 120, ClearedThinkingTurns: 2},
				{Type: "clear_tool_uses_20250919", ClearedInputTokens: 8000, ClearedToolUses: 3},
			}
			if got := anthropic.AppliedContextEdits(first); !slices.Equal(got, wantEdits) {
				t.Fatalf("AppliedContextEdits = %+v，期望 %+v", got, wantEdits)
			}
			raw, err := json.Marshal(first)
			if err != nil {
				t.Fatal(err)
			}
			var restored schema.Message
			if err := json.Unmarshal(raw, &restored); err != nil {
				t.Fatal(err)
			}
			if got := anthropic.AppliedContextEdits(&restored); !slices.Equal(got, wantEdits) {
				t.Fatalf("存档往返后的编辑统计为 %+v", got)
			}

			next, err := llm.Generate(t.Context(), append(in, &restored, schema.UserMessage("continue")))
			if err != nil {
				t.Fatal(err)
			}
			if got := anthropic.AppliedContextEdits(next); got != nil {
				t.Fatalf("未报告编辑的响应带了统计 %+v", got)
			}

			firstReq, nextReq := rec.at(t, 0), rec.at(t, 1)
			wantManagement := `{"edits":[{"keep":{"type":"thinking_turns","value":2},"type":"clear_thinking_20251015"},{"trigger":{"type":"input_tokens","value":30000},"type":"clear_tool_uses_20250919"},{"trigger":{"type":"input_tokens","value":50000},"type":"compact_20260112"}]}`
			for _, req := range []captured{firstReq, nextReq} {
				if got := canon(t, req.body["context_management"]); got != wantManagement {
					t.Errorf("清理与压缩策略的实际请求为 %s", got)
				}
				for _, want := range []string{"context-management-2025-06-27", "compact-2026-01-12"} {
					count := 0
					for _, beta := range betas(req.header) {
						if beta == want {
							count++
						}
					}
					if count != 1 {
						t.Errorf("beta %q 应出现一次，实际 %v", want, betas(req.header))
					}
				}
				if str(req.body, "cache_control", "ttl") != "1h" ||
					str(req.body, "thinking", "block_binding", "prefix_mismatch_behavior") != "error" {
					t.Errorf("未保留调用方的缓存 TTL 或严格签名校验，实际 cache=%v thinking=%v", req.body["cache_control"], req.body["thinking"])
				}
			}
			for _, key := range []string{"system", "tools"} {
				if canon(t, firstReq.body[key]) != canon(t, nextReq.body[key]) {
					t.Errorf("下一轮改变了前缀字段 %s", key)
				}
			}
			prior, ok := firstReq.body["messages"].([]any)
			if !ok || str(prior, 2, "content", 0, "tool_use_id") != "toolu_old" ||
				str(prior, 2, "content", 0, "content", 0, "text") != "original tool result" {
				t.Fatalf("发出的旧工具结果及配对应保留，实际 %v", prior)
			}
			sent, ok := nextReq.body["messages"].([]any)
			if !ok || len(sent) != len(prior)+2 || canon(t, sent[:len(prior)]) != canon(t, prior) {
				t.Fatalf("下一轮改写了历史，实际发送 %v", sent)
			}
			blocks := at(sent, len(prior), "content")
			if got, want := canon(t, blocks), canon(t, []any{compaction, thinking, answer}); got != want {
				t.Errorf("重放块与原响应不一致，实际 %s，期望 %s", got, want)
			}
		})
	}
}
