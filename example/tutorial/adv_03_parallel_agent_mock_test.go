package tutorial_test

// adv_03 mock 镜像：验证 ParallelAgent 多 sub-agent fan-out + Merger + 重试配置
// 不依赖真 LLM。原 adv_03_parallel_agent_test.go 走真千问保留 //go:build integration。
//
// 迁移到 agent_v2：
//   - constructor 改链式：NewParallelAgent + AddBranch
//   - ParallelName / WithMerger / WithParallelRetryConfig → 同名 agent_v2 options
//   - compose.Merger / compose.ParallelResult → agent_v2.ParallelMerger / ParallelBranchResult

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

func TestAdv_03_ParallelAgent_Mock(t *testing.T) {
	makeTranslator := func(lang, translation string) agent_v2.Agent {
		stub := &stubModel{
			reply: func(_, _ string, _ []*schema.Message) *schema.Message {
				return schema.AssistantMessage(translation)
			},
		}
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("translate_"+lang),
			agent_v2.WithSystemPrompt("translate to "+lang),
			agent_v2.WithModel(stub),
		)
	}
	toEN := makeTranslator("en", "AI is profoundly transforming software engineering.")
	toJA := makeTranslator("ja", "AIはソフトウェア工学を深く変えつつあります。")
	toDE := makeTranslator("de", "KI verändert die Softwareentwicklung tiefgreifend.")

	merger := agent_v2.ParallelMerger(func(_ context.Context, results []agent_v2.ParallelBranchResult) (*schema.Message, error) {
		labelOf := func(name string) string {
			switch {
			case strings.Contains(name, "translate_en"):
				return "🇬🇧 英文"
			case strings.Contains(name, "translate_ja"):
				return "🇯🇵 日文"
			case strings.Contains(name, "translate_de"):
				return "🇩🇪 德文"
			}
			return name
		}
		var sb strings.Builder
		sb.WriteString("## 多语言版本\n\n")
		for _, r := range results {
			lang := labelOf(r.Name)
			// 注意：分支失败（重试耗尽）时整 Run 直接返错、merger 不跑——results
			// 里不会出现失败分支（OrigErr 恒 nil，"部分失败降级出报告"当前不支持）。
			sb.WriteString(fmt.Sprintf("**%s**：\n%s\n\n", lang, r.Output.Content))
		}
		return schema.AssistantMessage(sb.String()), nil
	})

	pa := agent_v2.NewParallelAgent(
		agent_v2.WithParallelName("multilang_translator"),
		agent_v2.WithParallelMerger(merger),
		agent_v2.WithParallelRetryConfig(compose_v2.ExponentialBackoff(2, 200*time.Millisecond, 2*time.Second)),
	).
		AddBranch("translate_en", toEN).
		AddBranch("translate_ja", toJA).
		AddBranch("translate_de", toDE)

	events, err := pa.Run(ctx60sMock(t), nil,
		schema.UserMessage("AI 改变软工"))
	if err != nil {
		t.Fatalf("ParallelAgent 运行失败: %v", err)
	}
	out := agent_v2.FinalMessage(events)
	if out == nil {
		t.Fatal("FinalMessage 为 nil")
	}
	t.Logf("多语言翻译结果:\n%s", out.Content)

	for _, want := range []string{"🇬🇧 英文", "🇯🇵 日文", "🇩🇪 德文"} {
		if !strings.Contains(out.Content, want) {
			t.Errorf("缺 %s 版本，实际内容: %q", want, out.Content)
		}
	}
	if strings.Count(out.Content, "**") < 6 {
		t.Errorf("期望 3 个语言 × 2 ** marker，实际 %d", strings.Count(out.Content, "**"))
	}
}
