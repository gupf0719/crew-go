//go:build integration

package tutorial_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// ──────────────────────────────────────────────────────────────────
// 3. ParallelAgent（进阶）— 多语言翻译 + 自定义 Merger + superstep 快照重跑
// ──────────────────────────────────────────────────────────────────

func TestAdv_03_ParallelAgent(t *testing.T) {
	provider := requireAPIKey(t)

	toEN := newBasicAgentV2(provider, "translate_en", "你是专业翻译。将用户输入的中文翻译为流畅的英文，只输出译文，不加任何解释。")
	toJA := newBasicAgentV2(provider, "translate_ja", "你是专业翻译。将用户输入的中文翻译为流畅的日文，只输出译文，不加任何解释。")
	toDE := newBasicAgentV2(provider, "translate_de", "你是专业翻译。将用户输入的中文翻译为流畅的德文，只输出译文，不加任何解释。")

	multiLangMerger := agent_v2.ParallelMerger(func(_ context.Context, results []agent_v2.ParallelBranchResult) (*schema.Message, error) {
		langMap := map[string]string{
			"translate_en": "🇬🇧 英文",
			"translate_ja": "🇯🇵 日文",
			"translate_de": "🇩🇪 德文",
		}
		var sb strings.Builder
		sb.WriteString("## 多语言版本\n\n")
		for _, r := range results {
			lang := langMap[r.Name]
			if lang == "" {
				lang = r.Name
			}
			// 注意：分支失败（重试耗尽）时整 Run 直接返错、merger 不跑——results
			// 里不会出现失败分支（OrigErr 恒 nil，"部分失败降级出报告"当前不支持）。
			sb.WriteString(fmt.Sprintf("**%s**：\n%s\n\n", lang, r.Output.Content))
		}
		return schema.AssistantMessage(sb.String()), nil
	})

	pa := agent_v2.NewParallelAgent(
		agent_v2.WithParallelName("multilang_translator"),
		agent_v2.WithParallelMerger(multiLangMerger),
		agent_v2.WithParallelRetryConfig(compose_v2.ExponentialBackoff(2, 200*time.Millisecond, 2*time.Second)),
	).
		AddBranch("translate_en", toEN).
		AddBranch("translate_ja", toJA).
		AddBranch("translate_de", toDE)

	events, err := pa.Run(ctx300s(t), nil,
		schema.UserMessage("人工智能正在深刻地改变软件工程的工作方式，自动化工具将替代大量重复性编码工作。"))
	if err != nil {
		t.Fatalf("ParallelAgent 运行失败: %v", err)
	}
	out := mustFinalMessageV2(t, events)
	t.Logf("多语言翻译结果:\n%s", out.Content)

	if strings.Count(out.Content, "**") < 2 {
		t.Errorf("期望至少 2 个语言版本，实际内容: %q", out.Content[:minInt(300, len(out.Content))])
	}
}
