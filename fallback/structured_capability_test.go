package fallback_test

// FallbackChatModel 必须转发结构化输出能力画像，否则 `agent_v2` 的类型断言探不到，
// `WithOutputSchema` 会静默丢掉 `tool_choice=required` 那层强制——退化成靠模型自觉。

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crew-go/compose_v2"
	"crew-go/fallback"
	"crew-go/model"
)

// capStub 复用包内 stubModel（省得再实现一遍 ChatModel 三方法），只加能力自报。
type capStub struct {
	*stubModel
	cap model.StructuredCapability
}

func (s *capStub) StructuredCapability(string) model.StructuredCapability { return s.cap }

func newCapStub(name string, c model.StructuredCapability) *capStub {
	return &capStub{stubModel: &stubModel{name: name}, cap: c}
}

func TestFallback_StructuredCapabilityIsIntersection(t *testing.T) {
	full := model.StructuredCapability{
		SupportsJSONSchema: true, SupportsJSONObject: true,
		SupportsToolChoiceRequired: true, SupportsStrictTool: true,
	}
	// 只支持 tool_choice=required + json_object（典型 qwen/deepseek 画像）
	partial := model.StructuredCapability{
		SupportsJSONObject: true, SupportsToolChoiceRequired: true,
	}

	// 注：本文件其它子测试直接调 f.StructuredCapability，所以"删掉该方法"的
	// sanity-revert 表现为**编译失败**而非断言失败——同样是有效信号（方法没了立刻炸），
	// 只是不如断言精确。真正端到端的验证在 example/harness/structured_smoke：
	// 它用 WithOutputSchema + 本装饰器，能力探不到时会静默丢掉 tool_choice=required。
	t.Run("装饰器满足 StructuredCapable 接口", func(t *testing.T) {
		f := fallback.New([]model.ChatModel{newCapStub("full", full)})
		if _, ok := any(f).(model.StructuredCapable); !ok {
			t.Fatal("FallbackChatModel 必须实现 model.StructuredCapable——" +
				"agent_v2 靠类型断言探它，不实现就静默降级、丢掉 tool_choice=required")
		}
	})

	t.Run("全员支持则支持", func(t *testing.T) {
		f := fallback.New([]model.ChatModel{newCapStub("full", full), newCapStub("full", full)})
		got := f.StructuredCapability("m")
		if !got.SupportsToolChoiceRequired || !got.SupportsStrictTool {
			t.Errorf("全员支持时不该降级, got %+v", got)
		}
	})

	t.Run("取交集而非首成员——切过去才不会带它不认的参数", func(t *testing.T) {
		f := fallback.New([]model.ChatModel{newCapStub("full", full), newCapStub("partial", partial)})
		got := f.StructuredCapability("m")
		if got.SupportsStrictTool {
			t.Error("备选不支持 strict tool，交集就不能报支持——切过去会带它不认的参数")
		}
		if got.SupportsJSONSchema {
			t.Error("备选不支持 json_schema，交集不能报支持")
		}
		// 两者都支持的那些要保住
		if !got.SupportsToolChoiceRequired || !got.SupportsJSONObject {
			t.Errorf("两者都支持的能力不该被抹掉, got %+v", got)
		}
	})

	t.Run("成员能力未知则整体保守全 false", func(t *testing.T) {
		f := fallback.New([]model.ChatModel{newCapStub("full", full), &stubModel{name: "plain"}})
		got := f.StructuredCapability("m")
		if got.SupportsToolChoiceRequired || got.SupportsJSONObject ||
			got.SupportsJSONSchema || got.SupportsStrictTool {
			t.Errorf("有成员不自报能力时不能猜它支持, got %+v", got)
		}
	})
}

// TestFallback_StaleIsNotFailover：`ErrCheckpointStale` **不该触发候选切换**。
//
// `DefaultFailoverPolicy` 判「再做一次有没有可能有用」用的是 `compose_v2.IsRetryFutile`，
// `ErrCheckpointStale` 是它的哨兵之一（陈旧写回重试有害——副作用会再跑一遍）。⚠️ 它同时
// 也在 `IsControlFlowSignal` 里，但那是 retry 层的判据、**不是这条的传导链**。语义上也对：陈旧写回
// 跟 model 好不好用**毫无关系**，换个模型重跑只会把副作用再造一次，还会污染断路器
// 统计（把一个健康的 model 记成失败）。
//
// 这条是把 stale 加进 `IsControlFlowSignal` 的**连带影响**，单独钉住免得日后被
// "优化"掉。
func TestFallback_StaleIsNotFailover(t *testing.T) {
	stale := fmt.Errorf("%w (probe)", compose_v2.ErrCheckpointStale)
	var p fallback.DefaultFailoverPolicy

	if p.ShouldFailover(context.Background(), stale) {
		t.Error("陈旧写回不该切候选——它跟 model 无关，换模型重跑只会重造副作用 + 污染断路器统计")
	}
	// 对照：普通 model 故障仍要切
	if !p.ShouldFailover(context.Background(), errors.New("connection reset")) {
		t.Error("普通故障仍应切换——stale 的收紧不能波及正常的 failover 路径")
	}
}
