//go:build integration

package qwen_test

// TestModelCandidates 的**入列判据守卫**：列表里每个候选都必须真的能调通。
//
// # 为什么需要这条
//
// 候选列表被 `testDefaultModel`（所有 `NewTestClient("")`）+ `llmtrace` 的 fallback
// 链 + tutorial 全套 integration 共用。放一个调不通的进去，影响面是全仓级的。
//
// 而且**"调不通"分两类，后一类更毒**：
//
//   - 额度耗尽（403 AllocationQuota）：`fallback` 认得它、会切下一个，只是浪费一次往返
//   - 参数不兼容（普通 400）：`DefaultFailoverPolicy` **不切**——链在这里硬停，后面的
//     健康候选一个都够不着。实测过 `qwen3.7-max-2026-05-17` / `qwen3.7-max-preview`
//     强制要求 `enable_thinking=true`，与 `presetDashscope` 写死的 `false` 冲突恒 400
//
// 跑：`go test -tags integration ./model/qwen/ -run TestTestModelCandidates_AllReachable`
// （需 QWEN_API_KEY；默认 `go test` 不带 tag 跑不到，不拖慢日常）

import (
	"context"
	"os"
	"strings"
	"testing"

	"crew-go/model/qwen"
	"crew-go/schema"
)

func TestTestModelCandidates_AllReachable(t *testing.T) {
	if os.Getenv("QWEN_API_KEY") == "" {
		t.Skip("需 QWEN_API_KEY")
	}
	if len(qwen.TestModelCandidates) == 0 {
		t.Fatal("候选列表为空——testDefaultModel 会 index out of range panic")
	}

	var broken []string
	for _, name := range qwen.TestModelCandidates {
		m := qwen.NewTestClient(name)
		_, err := m.Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")})
		if err == nil {
			t.Logf("  %-26s OK", name)
			continue
		}
		msg := err.Error()
		switch {
		case strings.Contains(msg, "AllocationQuota"):
			// 额度耗尽：fallback 会切下一个，但候选列表不该留着它白占位置
			t.Errorf("  %-26s ✗ 额度耗尽（403 AllocationQuota）——从列表挪走", name)
		default:
			// 普通 4xx：DefaultFailoverPolicy 不切 → 链在此硬停
			broken = append(broken, name)
			t.Errorf("  %-26s ✗ 调不通且 fallback 不会切过去：%s", name, truncate(msg, 120))
		}
	}
	if len(broken) > 0 {
		t.Errorf("\n上述 %d 个候选会让 fallback 链**硬停**（DefaultFailoverPolicy 只对额度类 4xx 切换），\n"+
			"排在它们后面的健康候选永远够不着。要么从 TestModelCandidates 删掉，\n"+
			"要么给它们单独配兼容的 client（如 qwen.WithEnableThinking(true)）。", len(broken))
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
