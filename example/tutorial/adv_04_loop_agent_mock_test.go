package tutorial_test

// adv_04 mock 镜像：LoopAgent 迭代 + ExitCondition 解析 QUALITY 分
//
// 迁移到 agent_v2：
//   - constructor 改链式：NewLoopAgent + WithBody
//   - LoopName/WithMaxIterations → WithLoopName/WithLoopMaxIterations
//   - WithExitCondition 签名改：(sess, round, msg) → (ctx, lastOut, round)
//   - sess KV API 不变 (SetValue/GetValue)

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

func TestAdv_04_LoopAgent_Mock(t *testing.T) {
	round := int32(0)
	stub := &stubModel{
		reply: func(_, _ string, _ []*schema.Message) *schema.Message {
			n := atomic.AddInt32(&round, 1)
			score := 2 + 2*int(n) // 4, 6, 8
			return schema.AssistantMessage(fmt.Sprintf("优化后代码 v%d\nQUALITY:%d", n, score))
		},
	}
	optimizer := agent_v2.NewBasicAgent(
		agent_v2.WithName("code_optimizer"),
		agent_v2.WithSystemPrompt("代码优化"),
		agent_v2.WithModel(stub),
		agent_v2.WithMaxRounds(3),
	)
	const qualityKey = "last_quality"
	var roundLog []string

	sess := agent_v2.NewRunSession()
	la := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("code_quality_loop"),
		agent_v2.WithLoopMaxIterations(5),
		agent_v2.WithLoopExitCondition(func(_ context.Context, output *schema.Message, r int) (bool, error) {
			score := 0
			if output != nil {
				if idx := strings.LastIndex(output.Content, "QUALITY:"); idx >= 0 {
					fields := strings.Fields(output.Content[idx:])
					if len(fields) > 0 {
						fmt.Sscanf(strings.TrimPrefix(fields[0], "QUALITY:"), "%d", &score)
					}
				}
			}
			sess.SetValue(qualityKey, score)
			roundLog = append(roundLog, fmt.Sprintf("第 %d 轮: %d", r, score))
			return score >= 7 || r >= 3, nil
		}),
	).WithBody(optimizer)

	events, err := la.Run(ctx60sMock(t), sess, schema.UserMessage("优化代码"))
	if err != nil {
		t.Fatalf("Run 失败: %v", err)
	}
	out := agent_v2.FinalMessage(events)
	finalScore, _ := sess.GetValue(qualityKey)
	t.Logf("迭代: %v 最终分: %v 最终输出: %.100s", roundLog, finalScore, out.Content)
	if len(roundLog) < 2 {
		t.Errorf("期望至少 2 轮（直到 score≥7），实际 %d", len(roundLog))
	}
	if score, _ := finalScore.(int); score < 7 {
		t.Errorf("最终分应 ≥7（达成 ExitCondition），实际 %v", finalScore)
	}
}
