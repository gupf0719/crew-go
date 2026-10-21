//go:build integration

package tutorial_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// ──────────────────────────────────────────────────────────────────
// 4. LoopAgent（进阶）— 迭代代码优化（RunSession 追踪质量评分）
// ──────────────────────────────────────────────────────────────────

func TestAdv_04_LoopAgent(t *testing.T) {
	provider := requireAPIKey(t)

	optimizer := agent_v2.NewBasicAgent(
		agent_v2.WithName("code_optimizer"),
		agent_v2.WithSystemPrompt(
			"你是 Go 代码优化专家。每轮收到代码后，找出一个最重要的性能或可读性问题并修复它。\n"+
				"输出改进后的代码块，然后在最后一行输出当前代码质量评分：\n"+
				"QUALITY:n（n 为 1~10 的整数，10 表示完美）\n"+
				"第一轮评分通常在 3~5，每轮改进后适当提高。",
		),
		agent_v2.WithModel(newLLM(provider)),
		agent_v2.WithMaxRounds(3),
	)

	const qualityKey = "last_quality"
	var roundLog []string
	session := agent_v2.NewRunSession()

	la := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("code_quality_loop"),
		agent_v2.WithLoopMaxIterations(5),
		agent_v2.WithLoopExitCondition(func(_ context.Context, output *schema.Message, round int) (bool, error) {
			score := 0
			if output != nil {
				if idx := strings.LastIndex(output.Content, "QUALITY:"); idx >= 0 {
					fields := strings.Fields(output.Content[idx:])
					if len(fields) > 0 {
						fmt.Sscanf(strings.TrimPrefix(fields[0], "QUALITY:"), "%d", &score)
					}
				}
			}
			session.SetValue(qualityKey, score)
			roundLog = append(roundLog, fmt.Sprintf("第 %d 轮评分: %d", round, score))
			return score >= 7 || round >= 3, nil
		}),
	).WithBody(optimizer)

	badCode := `func sum(nums []int) int {
    total := 0
    for i := 0; i < len(nums); i++ {
        total = total + nums[i]
    }
    return total
}`

	events, err := la.Run(ctx300s(t), session,
		schema.UserMessage("请优化以下 Go 代码：\n```go\n"+badCode+"\n```"))
	if err != nil {
		t.Fatalf("LoopAgent 运行失败: %v", err)
	}
	out := mustFinalMessageV2(t, events)

	finalScore, _ := session.GetValue(qualityKey)
	t.Logf("迭代记录: %v", roundLog)
	t.Logf("最终质量分: %v", finalScore)
	t.Logf("最终代码（前 400 字符）: %.400s", out.Content)

	if len(roundLog) == 0 {
		t.Error("期望至少执行一轮迭代")
	}
}
