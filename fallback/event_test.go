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

// captureSink 捕获 emit 的 event（mock compose_v2.EventSink）。
type captureSink struct{ events []compose_v2.Event }

func (s *captureSink) Send(ev compose_v2.Event) { s.events = append(s.events, ev) }

// TestFailoverEvent_SinkGated 验 failover 切换时 sink-gated emit failover event（业务接
// observability 自动采集）——有 EventSink 时 emit 且 payload from/to/reason 正确；无 sink
// 时零开销不 emit（不 panic、切换照常成功）。跟压缩 compact event 同款协议。
func TestFailoverEvent_SinkGated(t *testing.T) {
	// 每个 subtest 用 fresh fb（独立 breaker），确保都真从 m0 切到 m1（不被上一 subtest
	// 的 breaker 熔断跳过 m0 而不触发切换）。
	newFB := func() *fallback.FallbackChatModel {
		m0 := &stubModel{name: "m0", respond: func(n int, in []*schema.Message) (*schema.Message, error) {
			return nil, &model.APIError{StatusCode: 429, Body: "rate limit exceeded"} // 触发切换
		}}
		m1 := &stubModel{name: "m1"} // 恒成功
		return fallback.New([]model.ChatModel{m0, m1})
	}

	t.Run("有sink自动emit failover event", func(t *testing.T) {
		fb := newFB()
		sink := &captureSink{}
		ctx := compose_v2.WithEventSink(context.Background(), sink)
		out, err := fb.Generate(ctx, []*schema.Message{schema.UserMessage("q")})
		if err != nil {
			t.Fatalf("m0 fail 应切 m1 成功, got err: %v", err)
		}
		if out == nil || out.Content != "m1 ok" {
			t.Fatalf("应返 m1 响应, got %+v", out)
		}
		// 捕获 failover event
		var fe *compose_v2.Event
		for i := range sink.events {
			if sink.events[i].Type == fallback.FailoverEventType {
				fe = &sink.events[i]
			}
		}
		if fe == nil {
			t.Fatal("切换应 emit failover event（sink-gated）")
		}
		action, ok := fe.Metadata["action"].(*fallback.FailoverAction)
		if !ok || action == nil {
			t.Fatalf("Metadata[action] 应是 *FailoverAction, got %T", fe.Metadata["action"])
		}
		if action.FromIdx != 0 || action.ToIdx != 1 {
			t.Errorf("from/to = %d/%d; want 0/1", action.FromIdx, action.ToIdx)
		}
		if action.Reason != "rate_limit" {
			t.Errorf("reason = %q; want rate_limit（429 分类）", action.Reason)
		}
	})

	t.Run("无sink零开销不panic切换正常", func(t *testing.T) {
		fb := newFB()
		// ctx 无 EventSink → emitFailoverEvent 内 GetEventSink 返 nil → 零开销 return
		out, err := fb.Generate(context.Background(), []*schema.Message{schema.UserMessage("q")})
		if err != nil {
			t.Fatalf("无 sink 也应正常切换成功, got: %v", err)
		}
		if out == nil || out.Content != "m1 ok" {
			t.Fatalf("应返 m1 响应, got %+v", out)
		}
	})
}

// findTrippedAction 从捕获的 events 里找 member.tripped 的 action（nil=未 emit）。
func findTrippedAction(events []compose_v2.Event) *fallback.MemberTrippedAction {
	for i := range events {
		if events[i].Type == fallback.MemberTrippedEventType {
			if a, ok := events[i].Metadata["action"].(*fallback.MemberTrippedAction); ok {
				return a
			}
		}
	}
	return nil
}

// TestMemberTrippedEvent_SinkGated 验断路器熔断（closed→open 边缘）sink-gated emit
// member.tripped event。核心验 member.tripped 区别 failover 的独立价值：
//   - 熔断边缘 emit（member idx + reason 正确）
//   - **最后一个候选熔断也 emit tripped 但不 emit failover**（failover 看不到全员挂光）
//   - 无 sink 零开销不 panic
func TestMemberTrippedEvent_SinkGated(t *testing.T) {
	// maxFailures=1：候选一次失败即熔断（单次 Generate 内即触发 trip 边缘）。
	trip1 := fallback.WithBreaker(1, time.Minute)

	t.Run("熔断边缘emit tripped(member+reason)", func(t *testing.T) {
		m0 := &stubModel{name: "m0", respond: func(n int, in []*schema.Message) (*schema.Message, error) {
			return nil, &model.APIError{StatusCode: 429, Body: "rate limit exceeded"}
		}}
		m1 := &stubModel{name: "m1"}
		fb := fallback.New([]model.ChatModel{m0, m1}, trip1)
		sink := &captureSink{}
		ctx := compose_v2.WithEventSink(context.Background(), sink)
		if _, err := fb.Generate(ctx, []*schema.Message{schema.UserMessage("q")}); err != nil {
			t.Fatalf("m0 熔断应切 m1 成功: %v", err)
		}
		te := findTrippedAction(sink.events)
		if te == nil {
			t.Fatal("m0 断路器熔断应 emit member.tripped event")
		}
		if te.MemberIdx != 0 {
			t.Errorf("MemberIdx = %d; want 0", te.MemberIdx)
		}
		if te.Reason != "rate_limit" {
			t.Errorf("Reason = %q; want rate_limit（429 分类）", te.Reason)
		}
	})

	t.Run("最后候选熔断也emit tripped但无failover", func(t *testing.T) {
		// 单成员 m0 失败熔断：emit tripped(member=0)，无下一候选故无 failover——这正是
		// member.tripped 区别 failover 的价值（failover 看不到全员挂光）。
		m0 := &stubModel{name: "m0", respond: func(n int, in []*schema.Message) (*schema.Message, error) {
			return nil, &model.APIError{StatusCode: 500, Body: "internal server error"}
		}}
		fb := fallback.New([]model.ChatModel{m0}, trip1)
		sink := &captureSink{}
		ctx := compose_v2.WithEventSink(context.Background(), sink)
		if _, err := fb.Generate(ctx, []*schema.Message{schema.UserMessage("q")}); err == nil {
			t.Fatal("单成员失败应返错")
		}
		te := findTrippedAction(sink.events)
		if te == nil {
			t.Fatal("最后候选熔断也应 emit member.tripped（failover 抓不到全员挂光）")
		}
		if te.Reason != "server_error" {
			t.Errorf("Reason = %q; want server_error（500 分类）", te.Reason)
		}
		for _, ev := range sink.events {
			if ev.Type == fallback.FailoverEventType {
				t.Error("单成员无下一候选，不应 emit failover event")
			}
		}
	})

	t.Run("无sink零开销不panic", func(t *testing.T) {
		m0 := &stubModel{name: "m0", respond: func(n int, in []*schema.Message) (*schema.Message, error) {
			return nil, &model.APIError{StatusCode: 429, Body: "rate limit exceeded"}
		}}
		m1 := &stubModel{name: "m1"}
		fb := fallback.New([]model.ChatModel{m0, m1}, trip1)
		// ctx 无 EventSink → emitMemberTrippedEvent 内 GetEventSink 返 nil → 零开销 return
		if _, err := fb.Generate(context.Background(), []*schema.Message{schema.UserMessage("q")}); err != nil {
			t.Fatalf("无 sink 也应正常切换成功: %v", err)
		}
	})
}
