package observability

import (
	"testing"

	"crew-go/agent_v2"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// compactionSpanAttrs 白盒守护测试:sink-gated 后 compaction event 恒发,但 span 上
// "压缩了什么"(dropped/kept/token before→after/耗时/触发原因)全靠本 helper 结构化
// 展开——否则 OnNodeEvent 默认走 toString 把 *CompactionAction 吃成空字符串,span 一片空白。

func kvLookup(attrs []*commonpb.KeyValue, key string) (*commonpb.AnyValue, bool) {
	for _, kv := range attrs {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return nil, false
}

func TestCompactionSpanAttrs_AfterExpandsDetail(t *testing.T) {
	action := &agent_v2.CompactionAction{
		Layer: agent_v2.CompactionLayerAuto,
		Type:  agent_v2.CompactionActionAfter,
		After: &agent_v2.CompactionAfterAction{
			Dropped: 12, Kept: 30, InputTokens: 8500, OutputTokens: 3200, DurationMs: 240,
		},
	}
	attrs := compactionSpanAttrs(action)

	if v, ok := kvLookup(attrs, "crew.compact.layer"); !ok || v.GetStringValue() != "auto" {
		t.Errorf("crew.compact.layer 应 = auto, got %v (ok=%v)", v, ok)
	}
	// 这些数值就是"压缩了什么"——修复前 span 上完全看不到(toString 吃成空)
	for key, want := range map[string]int64{
		"crew.compact.dropped":       12,
		"crew.compact.kept":          30,
		"crew.compact.input_tokens":  8500,
		"crew.compact.output_tokens": 3200,
		"crew.compact.duration_ms":   240,
	} {
		v, ok := kvLookup(attrs, key)
		if !ok {
			t.Errorf("%s 缺失——span 上看不到该压缩细节\n"+
				"sanity-revert: 删 compactionSpanAttrs 的 After 展开分支 → 此测 fail", key)
			continue
		}
		if v.GetIntValue() != want {
			t.Errorf("%s 应 = %d, got %d", key, want, v.GetIntValue())
		}
	}
}

func TestCompactionSpanAttrs_PhaseSpecific(t *testing.T) {
	// Before: 展开 trigger + input_tokens, 不含 After 专属字段
	before := compactionSpanAttrs(&agent_v2.CompactionAction{
		Layer: agent_v2.CompactionLayerReactive, Type: agent_v2.CompactionActionBefore,
		Before: &agent_v2.CompactionBeforeAction{Trigger: "prompt_too_long", InputTokens: 5000},
	})
	if v, ok := kvLookup(before, "crew.compact.trigger"); !ok || v.GetStringValue() != "prompt_too_long" {
		t.Errorf("Before 应展开 trigger, got %v", v)
	}
	if _, ok := kvLookup(before, "crew.compact.dropped"); ok {
		t.Error("Before phase 不应展开 dropped(After 专属)——phase 隔离失效")
	}

	// Retry: 展开 attempt/max_attempts/orig_msgs/compacted_msgs/reason
	retry := compactionSpanAttrs(&agent_v2.CompactionAction{
		Layer: agent_v2.CompactionLayerReactive, Type: agent_v2.CompactionActionRetry,
		Retry: &agent_v2.CompactionRetryAction{Attempt: 2, MaxAttempts: 3, OrigMsgs: 20, CompactedMsgs: 8, Reason: "prompt_too_long"},
	})
	if v, ok := kvLookup(retry, "crew.compact.attempt"); !ok || v.GetIntValue() != 2 {
		t.Errorf("Retry 应展开 attempt=2, got %v", v)
	}
	if v, ok := kvLookup(retry, "crew.compact.orig_msgs"); !ok || v.GetIntValue() != 20 {
		t.Errorf("Retry 应展开 orig_msgs=20, got %v", v)
	}

	// nil action 安全
	if compactionSpanAttrs(nil) != nil {
		t.Error("nil action 应返 nil")
	}
	// MicroCompact 不计 token:After.InputTokens=0 时 input_tokens attr 不出现(避免噪声)
	micro := compactionSpanAttrs(&agent_v2.CompactionAction{
		Layer: agent_v2.CompactionLayerMicro, Type: agent_v2.CompactionActionAfter,
		After: &agent_v2.CompactionAfterAction{Dropped: 3, Kept: 7, InputTokens: 0},
	})
	if _, ok := kvLookup(micro, "crew.compact.input_tokens"); ok {
		t.Error("MicroCompact InputTokens=0 时不应出现 input_tokens attr")
	}
	if _, ok := kvLookup(micro, "crew.compact.dropped"); !ok {
		t.Error("Micro After 仍应展开 dropped/kept")
	}
}
