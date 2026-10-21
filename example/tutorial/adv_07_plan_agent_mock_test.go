package tutorial_test

// adv_07 mock：PlanAgent 自定义 Planner + 4 角色 sub-agent
//
// 迁移到 agent_v2：
//   - constructor 签名: NewPlanAgent(planner, opts...) （v1 用 (planner, executor, opts...)，v2 用 SetExecutor 链式）
//   - WithSubAgents map 不变
//   - PlanName → PlanName 同名
//   - Planner 签名 (ctx, sess, input, executed) → ([]PlanStep, finalAnswer, err) 不变

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

func TestAdv_07_PlanAgent_Mock(t *testing.T) {
	plannerStub := &stubModel{reply: func(_, _ string, _ []*schema.Message) *schema.Message {
		return schema.AssistantMessage(
			"[architect] 设计 JWT 认证模块\n" +
				"[developer] 实现 token 生成验证\n" +
				"[tester] 编写测试用例\n" +
				"[devops] Docker + K8s 部署",
		)
	}}
	plannerAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("planner"), agent_v2.WithSystemPrompt("plan"), agent_v2.WithModel(plannerStub),
	)
	makeExpert := func(name, output string) agent_v2.Agent {
		s := &stubModel{reply: func(_, _ string, _ []*schema.Message) *schema.Message {
			return schema.AssistantMessage(output)
		}}
		return agent_v2.NewBasicAgent(agent_v2.WithName(name), agent_v2.WithSystemPrompt(name), agent_v2.WithModel(s))
	}
	architect := makeExpert("architect", "[arch] 接口定义 + 数据模型")
	developer := makeExpert("developer", "[dev] 核心代码 + 注释")
	tester := makeExpert("tester", "[test] 测试用例覆盖")
	devops := makeExpert("devops", "[ops] Dockerfile + Deployment yaml")

	var cachedSteps []agent_v2.PlanStep
	customPlanner := agent_v2.Planner(func(ctx context.Context, sess *agent_v2.RunSession, input string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) == 0 {
			events, err := plannerAgent.Run(ctx, sess, schema.UserMessage(input))
			if err != nil {
				return nil, "", fmt.Errorf("planner: %w", err)
			}
			msg := agent_v2.FinalMessage(events)
			if msg == nil {
				return nil, "", errors.New("planner nil")
			}
			cachedSteps = cachedSteps[:0]
			for _, line := range strings.Split(msg.Content, "\n") {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				if strings.HasPrefix(line, "[") {
					end := strings.Index(line, "]")
					if end > 0 {
						role := strings.TrimSpace(line[1:end])
						desc := strings.TrimSpace(line[end+1:])
						if desc != "" {
							cachedSteps = append(cachedSteps, agent_v2.PlanStep{Description: desc, SubAgentName: role})
							continue
						}
					}
				}
				cachedSteps = append(cachedSteps, agent_v2.PlanStep{Description: line})
			}
		}
		if len(executed) >= len(cachedSteps) {
			return nil, "", nil
		}
		return cachedSteps[len(executed):], "", nil
	})
	pa := agent_v2.NewPlanAgent(
		customPlanner,
		agent_v2.PlanName("feature_dev_pipeline"),
		agent_v2.WithSubAgents(map[string]agent_v2.Agent{
			"architect": architect, "developer": developer,
			"tester": tester, "devops": devops,
		}),
	).SetExecutor(developer)

	events, err := pa.Run(ctx60sMock(t), nil,
		schema.UserMessage("开发 JWT 认证功能"))
	if err != nil {
		t.Fatalf("Run 失败: %v", err)
	}
	out := agent_v2.FinalMessage(events)
	if out == nil {
		t.Fatal("nil")
	}
	t.Logf("最终: %.500s", out.Content)
	for _, want := range []string{"[arch]", "[dev]", "[test]", "[ops]"} {
		if !strings.Contains(out.Content, want) {
			t.Errorf("缺 %s 阶段产出，实际 %q", want, out.Content)
		}
	}
}
