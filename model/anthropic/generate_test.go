package anthropic_test

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/model/anthropic"
	"crew-go/schema"
)

// toolResult 构造一条 role=tool 消息（不经 schema.ToolResultMessage，免得受它的签名变化影响）。
func toolResult(callID, content string, isError bool) *schema.Message {
	return &schema.Message{Role: schema.RoleTool, Content: content, ContentBlocks: []*schema.ContentBlock{{
		Type:       "tool_result",
		Toolresult: &schema.ToolresultContentBlock{ToolCallId: callID, Content: content, IsError: isError},
	}}}
}

// toolCall 构造一个工具调用块。
func toolCall(id, name, args string) *schema.ContentBlock {
	return &schema.ContentBlock{Type: "tool_call", Toolcall: &schema.ToolcallContentBlock{Id: id, Name: name, Arguments: args}}
}

func textReply(w http.ResponseWriter, _ *http.Request, _ int) {
	replyJSON(w, http.StatusOK, message([]any{map[string]any{"type": "text", "text": "好"}}, "end_turn", nil))
}

// TestGenerate_DefaultRequestShape 钉住默认请求：模型、max_tokens、adaptive thinking + drop_block、
// 自动 cache、服务端回退、对应的 beta 头、凭证头，以及没有不该有的字段。
func TestGenerate_DefaultRequestShape(t *testing.T) {
	srv, rec := newServer(t, textReply)
	_, err := newClient(srv).Generate(context.Background(), []*schema.Message{
		schema.SystemMessage("你是助手"),
		schema.UserMessage("你好"),
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	req := rec.at(t, 0)
	if got := str(req.body, "model"); got != anthropic.DefaultModel {
		t.Errorf("model = %q，期望 %q", got, anthropic.DefaultModel)
	}
	if got := num(t, req.body, "max_tokens"); got != anthropic.DefaultMaxTokens {
		t.Errorf("max_tokens = %d，期望 %d", got, anthropic.DefaultMaxTokens)
	}
	if got := str(req.body, "system", 0, "text"); got != "你是助手" {
		t.Errorf("system[0].text = %q", got)
	}
	if got := canon(t, req.body["messages"]); got != `[{"content":[{"text":"你好","type":"text"}],"role":"user"}]` {
		t.Errorf("messages = %s", got)
	}
	if got := canon(t, req.body["thinking"]); got != `{"block_binding":{"prefix_mismatch_behavior":"drop_block"},"type":"adaptive"}` {
		t.Errorf("thinking = %s", got)
	}
	if got := canon(t, req.body["cache_control"]); got != `{"type":"ephemeral"}` {
		t.Errorf("cache_control = %s", got)
	}
	if got := str(req.body, "fallbacks"); got != "default" {
		t.Errorf("fallbacks = %q，期望 default", got)
	}
	for _, key := range []string{"tools", "tool_choice", "temperature", "top_p", "output_config", "stop_sequences"} {
		if _, ok := req.body[key]; ok {
			t.Errorf("默认请求不该带 %s：%s", key, canon(t, req.body[key]))
		}
	}
	gotBetas := betas(req.header)
	for _, want := range []string{"thinking-binding-controls-2026-08-01", "server-side-fallback-2026-07-01"} {
		if !slices.Contains(gotBetas, want) {
			t.Errorf("anthropic-beta = %v，缺 %s", gotBetas, want)
		}
	}
	if got := req.header.Get("X-Api-Key"); got != "test-key" {
		t.Errorf("x-api-key = %q", got)
	}
	if got := req.header.Get("Authorization"); got != "" {
		t.Errorf("显式传了 key，不该再带 Authorization：%q", got)
	}
}

// TestNewClient_ExplicitKeyWinsOverEnvToken：环境里有 ANTHROPIC_AUTH_TOKEN 时 SDK 会把它设成
// Authorization 头；显式传了 key 就只能带 key（两个凭证头同时出现 API 直接拒绝）。
func TestNewClient_ExplicitKeyWinsOverEnvToken(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "env-token")
	srv, rec := newServer(t, textReply)
	if _, err := newClient(srv).Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")}); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	h := rec.at(t, 0).header
	if got := h.Get("Authorization"); got != "" {
		t.Errorf("Authorization = %q，期望没有", got)
	}
	if got := h.Get("X-Api-Key"); got != "test-key" {
		t.Errorf("x-api-key = %q", got)
	}
}

// TestGenerate_ResponseMapping 钉住响应到 Message 的映射：文本、thinking 文本、工具调用、结束原因、
// 模型、provider，以及用量的口径（InputTokens 含 cache 读写，OutputTokens 不含 thinking）。
func TestGenerate_ResponseMapping(t *testing.T) {
	srv, _ := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		replyJSON(w, http.StatusOK, message([]any{
			map[string]any{"type": "thinking", "thinking": "先想一下", "signature": "sig-a"},
			map[string]any{"type": "text", "text": "我查一下"},
			map[string]any{"type": "tool_use", "id": "toolu_1", "name": "get_weather", "input": map[string]any{"city": "北京"}},
		}, "tool_use", map[string]any{
			"input_tokens": 100, "cache_read_input_tokens": 300, "cache_creation_input_tokens": 50,
			"output_tokens": 80, "output_tokens_details": map[string]any{"thinking_tokens": 30},
		}))
	})
	msg, err := newClient(srv).Generate(context.Background(), []*schema.Message{schema.UserMessage("北京天气")})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if msg.Role != schema.RoleAssistant || msg.Content != "我查一下" || msg.ReasoningContent != "先想一下" {
		t.Errorf("Role/Content/ReasoningContent = %q / %q / %q", msg.Role, msg.Content, msg.ReasoningContent)
	}
	calls := schema.GetToolCalls(msg)
	if len(calls) != 1 || calls[0].ID != "toolu_1" || calls[0].Function.Name != "get_weather" ||
		canon(t, jsonOf(t, calls[0].Function.Arguments)) != `{"city":"北京"}` {
		t.Errorf("工具调用 = %+v", calls)
	}
	md := msg.Metadata
	if md == nil || md.FinishReason != "tool_calls" || md.Model != "claude-opus-5-5" || md.System != anthropic.ProviderName {
		t.Fatalf("Metadata = %+v", md)
	}
	want := schema.MessageUsage{InputTokens: 450, CachedInputTokens: 300, CacheCreationInputTokens: 50,
		OutputTokens: 50, ThinkTokens: 30, TotalTokens: 530}
	if md.Usage == nil || *md.Usage != want {
		t.Errorf("Usage = %+v，期望 %+v", md.Usage, want)
	}
	if s, _ := msg.Annotations[anthropic.AssistantContentAnnotation].(string); s == "" {
		t.Errorf("缺回放快照标注 %s", anthropic.AssistantContentAnnotation)
	}
}

// TestGenerate_TextJoin 钉住多个 text 块拼成 Content 的规则：相邻 text 块直接相接；中间隔着别的块时
// 补一个空行，已有空白时不补。
func TestGenerate_TextJoin(t *testing.T) {
	text := func(s string) map[string]any { return map[string]any{"type": "text", "text": s} }
	tool := func(id string) map[string]any {
		return map[string]any{"type": "tool_use", "id": id, "name": "f", "input": map[string]any{}}
	}
	think := map[string]any{"type": "thinking", "thinking": "", "signature": "s"}
	cases := []struct {
		name    string
		content []any
		want    string
	}{
		{"相邻 text 块直接相接", []any{text("前半"), text("后半")}, "前半后半"},
		{"隔着工具调用补空行", []any{text("先查 A。"), tool("t1"), text("再查 B。"), tool("t2")}, "先查 A。\n\n再查 B。"},
		{"隔着 thinking 补空行", []any{text("一"), think, text("二")}, "一\n\n二"},
		{"前一段以空白结尾不补", []any{text("一\n"), tool("t1"), text("二")}, "一\n二"},
		{"后一段以空白开头不补", []any{text("一"), tool("t1"), text(" 二")}, "一 二"},
		{"开头的非 text 块不补", []any{think, tool("t1"), text("答案")}, "答案"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
				replyJSON(w, http.StatusOK, message(tc.content, "end_turn", nil))
			})
			msg, err := newClient(srv).Generate(context.Background(), []*schema.Message{schema.UserMessage("q")})
			if err != nil {
				t.Fatalf("Generate: %v", err)
			}
			if msg.Content != tc.want {
				t.Errorf("Content = %q，期望 %q", msg.Content, tc.want)
			}
			// 流式路径拼出的必须一模一样（回放指纹按 Content 算）
			if got := streamedContent(t, tc.content); got != tc.want {
				t.Errorf("流式拼出 %q，Generate 拼出 %q", got, tc.want)
			}
		})
	}
}

// TestGenerate_HistoryConversion 钉住历史转换：前导 system 进顶层 system；相邻的 tool 消息与非前导
// system 合并成一条 user，tool_result 在前、is_error 带上；没有快照的 assistant 按领域字段重建，
// 工具参数原样、空参数发 {}。
func TestGenerate_HistoryConversion(t *testing.T) {
	srv, rec := newServer(t, textReply)
	history := []*schema.Message{
		schema.SystemMessage("前言一"),
		schema.SystemMessage("前言二"),
		schema.UserMessage("查天气和时间"),
		{Role: schema.RoleAssistant, Content: "我查一下", ContentBlocks: []*schema.ContentBlock{
			toolCall("call_a", "get_weather", `{"city":"北京"}`),
			toolCall("call_b", "get_time", ""),
		}},
		toolResult("call_a", "晴", false),
		schema.SystemMessage("中途提醒"),
		toolResult("call_b", "[timeout] 工具超时", true),
	}
	if _, err := newClient(srv).Generate(context.Background(), history); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	body := rec.at(t, 0).body
	if got := canon(t, body["system"]); got != `[{"text":"前言一","type":"text"},{"text":"前言二","type":"text"}]` {
		t.Errorf("system = %s", got)
	}
	want := `[` +
		`{"content":[{"text":"查天气和时间","type":"text"}],"role":"user"},` +
		`{"content":[{"text":"我查一下","type":"text"},` +
		`{"id":"call_a","input":{"city":"北京"},"name":"get_weather","type":"tool_use"},` +
		`{"id":"call_b","input":{},"name":"get_time","type":"tool_use"}],"role":"assistant"},` +
		`{"content":[` +
		`{"content":[{"text":"晴","type":"text"}],"tool_use_id":"call_a","type":"tool_result"},` +
		`{"content":[{"text":"[timeout] 工具超时","type":"text"}],"is_error":true,"tool_use_id":"call_b","type":"tool_result"},` +
		`{"text":"<system-reminder>\n中途提醒\n</system-reminder>","type":"text"}` +
		`],"role":"user"}]`
	if got := canon(t, body["messages"]); got != want {
		t.Errorf("messages =\n%s\n期望\n%s", got, want)
	}
}

// TestGenerate_Refusal：stop_reason=refusal 是 HTTP 200，要转成错误——认得出是内容过滤、带拒答
// 类别、不可重试。
func TestGenerate_Refusal(t *testing.T) {
	srv, _ := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		body := message([]any{}, "refusal", nil)
		body["stop_details"] = map[string]any{"type": "refusal", "category": "cyber", "explanation": "policy"}
		replyJSON(w, http.StatusOK, body)
	})
	_, err := newClient(srv).Generate(context.Background(), []*schema.Message{schema.UserMessage("q")})
	if !errors.Is(err, model.ErrContentFiltered) {
		t.Fatalf("err = %v，期望包着 model.ErrContentFiltered", err)
	}
	var refusal *anthropic.RefusalError
	if !errors.As(err, &refusal) || refusal.Category != "cyber" || refusal.Explanation != "policy" {
		t.Errorf("RefusalError = %+v", refusal)
	}
	if compose_v2.IsRetryable(err) {
		t.Errorf("拒答不该重试：%v", err)
	}
}

// TestGenerate_ContextWindowExceeded：stop_reason=model_context_window_exceeded 转成
// model.ErrContextWindowExceeded（上下文压缩据它压缩后重试）。
func TestGenerate_ContextWindowExceeded(t *testing.T) {
	srv, _ := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		replyJSON(w, http.StatusOK, message([]any{map[string]any{"type": "text", "text": "半"}}, "model_context_window_exceeded", nil))
	})
	_, err := newClient(srv).Generate(context.Background(), []*schema.Message{schema.UserMessage("q")})
	if !errors.Is(err, model.ErrContextWindowExceeded) {
		t.Fatalf("err = %v，期望包着 model.ErrContextWindowExceeded", err)
	}
}

// TestGenerate_HTTPError：HTTP 错误转成带状态码、响应体与 Retry-After 的 *model.APIError，且只发一次
// （SDK 自己的重试默认关掉，重试交给 crew-go 的重试层）。
func TestGenerate_HTTPError(t *testing.T) {
	srv, rec := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.Header().Set("Retry-After", "7")
		replyJSON(w, http.StatusTooManyRequests, map[string]any{"type": "error",
			"error": map[string]any{"type": "rate_limit_error", "message": "slow down"}})
	})
	_, err := newClient(srv).Generate(context.Background(), []*schema.Message{schema.UserMessage("q")})
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %T %v，期望 *model.APIError", err, err)
	}
	if apiErr.Provider != anthropic.ProviderName || apiErr.StatusCode != http.StatusTooManyRequests ||
		apiErr.RetryAfter != 7*time.Second || !strings.Contains(apiErr.Body, "rate_limit_error") {
		t.Errorf("APIError = %+v", apiErr)
	}
	if n := rec.count(); n != 1 {
		t.Errorf("发了 %d 次请求，期望 1 次（SDK 不该自己重试）", n)
	}
}

// TestGenerate_PromptTooLong：上下文超长的 400 保留原文，上层按文案认出它（ReactiveCompact），且不重试。
func TestGenerate_PromptTooLong(t *testing.T) {
	srv, _ := newServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		replyJSON(w, http.StatusBadRequest, map[string]any{"type": "error", "error": map[string]any{
			"type": "invalid_request_error", "message": "prompt is too long: 250000 tokens > 200000 maximum"}})
	})
	_, err := newClient(srv).Generate(context.Background(), []*schema.Message{schema.UserMessage("q")})
	if err == nil || !strings.Contains(err.Error(), "prompt is too long") {
		t.Fatalf("err = %v，期望错误文本含 prompt is too long", err)
	}
	if compose_v2.IsRetryable(err) {
		t.Errorf("400 不该重试：%v", err)
	}
}
