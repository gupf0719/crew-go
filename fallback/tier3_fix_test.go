package fallback_test

import (
	"context"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/fallback"
	"crew-go/model"
	"crew-go/schema"
)

// TestFailoverReason_429Quota 验证 OpenAI 429 `insufficient_quota` body 标 quota_exhausted
// 而非 rate_limit（配额耗尽需充值/换账号 vs 限流退避自愈，runbook 不同）。
//
// sanity：revert failoverReason 429 case 的先判 quota 后，429 quota body 落 rate_limit → FAIL。
func TestFailoverReason_429Quota(t *testing.T) {
	m0 := &stubModel{name: "m0", respond: func(n int, in []*schema.Message) (*schema.Message, error) {
		return nil, &model.APIError{StatusCode: 429, Body: `{"error":{"type":"insufficient_quota","message":"You exceeded your current quota"}}`}
	}}
	m1 := &stubModel{name: "m1"} // 恒成功
	fb := fallback.New([]model.ChatModel{m0, m1})
	sink := &captureSink{}
	ctx := compose_v2.WithEventSink(context.Background(), sink)
	if _, err := fb.Generate(ctx, []*schema.Message{schema.UserMessage("q")}); err != nil {
		t.Fatalf("m0 429 应切 m1 成功, got: %v", err)
	}
	var reason string
	for i := range sink.events {
		if sink.events[i].Type == fallback.FailoverEventType {
			if a, ok := sink.events[i].Metadata["action"].(*fallback.FailoverAction); ok {
				reason = a.Reason
			}
		}
	}
	if reason != "quota_exhausted" {
		t.Fatalf("429 insufficient_quota 应标 quota_exhausted, got %q", reason)
	}
}

// TestShouldFailover_AnthropicCreditBalance 验证 Anthropic 400 "credit balance is too low"
// （配额耗尽复用 400）触发切换到健康候选（同 LiteLLM#24320）。
//
// sanity：revert isQuotaExhausted 加的 "credit balance is too low" 关键词后，400 不切 → FAIL。
func TestShouldFailover_AnthropicCreditBalance(t *testing.T) {
	p := fallback.DefaultFailoverPolicy{}
	quotaErr := &model.APIError{StatusCode: 400, Body: "Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."}
	if !p.ShouldFailover(context.Background(), quotaErr) {
		t.Fatal("Anthropic 400 credit balance too low（配额耗尽）应触发切换到健康候选")
	}
	// 对照：普通 400 参数错不切（换模型也白搭）——防误判把所有 400 都切
	badReq := &model.APIError{StatusCode: 400, Body: "invalid request: missing required field 'model'"}
	if p.ShouldFailover(context.Background(), badReq) {
		t.Fatal("普通 400 参数错不应切换")
	}
}

// TestFallback_ProbingCleared_OnNonFailover 验证 half-open 探测遇非切换错误后 probing 被清，
// 候选可立即重探——fallback probing 泄漏回归（我改 circuit_breaker half-open 时引入）。
//
// sanity：revert fallback.go 非切换路径的 ClearProbe 调用后，请求 3 因 probing 泄漏被
// ErrAllMembersUnavailable（候选被额外排除到 probeAt+cooldown）→ FAIL。
func TestFallback_ProbingCleared_OnNonFailover(t *testing.T) {
	calls := 0
	m := &stubModel{name: "m", respond: func(n int, in []*schema.Message) (*schema.Message, error) {
		calls++
		switch calls {
		case 1:
			return nil, &model.APIError{StatusCode: 503} // trip（单候选无下一个 → ErrAllMembersUnavailable）
		case 2:
			return nil, &model.APIError{StatusCode: 400, Body: "bad request"} // half-open 探测非切换 → ClearProbe
		default:
			return &schema.Message{Role: schema.RoleAssistant, Content: "ok"}, nil // 重探成功
		}
	}}
	fb := fallback.New([]model.ChatModel{m}, fallback.WithBreaker(1, 10*time.Millisecond))
	ctx := context.Background()

	// 请求 1：503 → RecordFailure 熔断 breaker + return firstErr(503)（Allow 初始 closed=true →
	// triedAny，故返 503 而非 ErrAllMembersUnavailable——后者只在所有候选 Allow=false 时）
	if _, err := fb.Generate(ctx, []*schema.Message{schema.UserMessage("q")}); err == nil {
		t.Fatal("503 应返 error（breaker 首次熔断，单候选无处可切）")
	}
	time.Sleep(20 * time.Millisecond) // cooldown 过

	// 请求 2：half-open 探测 → 400 非切换 → ClearProbe 释放 probing
	if _, err := fb.Generate(ctx, []*schema.Message{schema.UserMessage("q")}); err == nil {
		t.Fatal("探测 400 应返 error（非切换）")
	}

	// 请求 3：probing 已清 + cooldown 仍过 → 可重探成功（修复前 probing 泄漏会 ErrAllMembersUnavailable）
	out, err := fb.Generate(ctx, []*schema.Message{schema.UserMessage("q")})
	if err != nil {
		t.Fatalf("probing 已清应可重探成功（修复前泄漏会 ErrAllMembersUnavailable）, got %v", err)
	}
	if out == nil || out.Content != "ok" {
		t.Fatalf("重探应成功返 ok, got %+v", out)
	}
}
