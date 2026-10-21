//go:build integration

package tutorial_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// ──────────────────────────────────────────────────────────────────
// 7. PlanAgent（进阶）— 功能开发（多专家子 Agent + 自定义 Planner）
//
// **v2 迁移说明**：
//   - PlanAgent constructor: NewPlanAgent(planner, opts...) + .SetExecutor(developer)
//   - Planner 闭包签名 (ctx, sess, input, executed) → 不变
//   - StepResultMapper：v2 暂未提供等价 API（默认拼步骤内容），定制需走 ExecInputBuilder 路径
// ──────────────────────────────────────────────────────────────────

func TestAdv_07_PlanAgent(t *testing.T) {
	provider := requireAPIKey(t)

	plannerAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("planner"),
		agent_v2.WithSystemPrompt(
			"你是软件项目规划师。将用户的功能需求拆解为 4~6 个开发步骤，每步骤格式为：\n"+
				"[角色] 描述\n\n"+
				"角色只能是：architect（架构）、developer（开发）、tester（测试）、devops（部署）\n"+
				"示例：\n"+
				"[architect] 设计用户认证模块的数据模型和接口\n"+
				"[developer] 实现 JWT Token 生成和验证逻辑\n"+
				"只输出步骤列表，一步一行。",
		),
		agent_v2.WithModel(newLLM(provider)),
	)

	architect := newBasicAgentV2(provider, "architect",
		"你是软件架构师。为给定任务设计清晰的技术架构，给出接口定义和数据模型。")
	developer := newBasicAgentV2(provider, "developer",
		"你是 Go 高级工程师。根据架构设计编写核心代码，附详细注释。")
	tester := newBasicAgentV2(provider, "tester",
		"你是 QA 工程师。为给定功能设计测试用例，包含正常流程和边界条件。")
	devops := newBasicAgentV2(provider, "devops",
		"你是 DevOps 工程师。提供 Docker 构建和 Kubernetes 部署配置。")

	var cachedSteps []agent_v2.PlanStep
	customPlanner := agent_v2.Planner(func(ctx context.Context, session *agent_v2.RunSession, input string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) == 0 {
			events, err := plannerAgent.Run(ctx, session, schema.UserMessage(input))
			if err != nil {
				return nil, "", fmt.Errorf("planner agent: %w", err)
			}
			msg := agent_v2.FinalMessage(events)
			if msg == nil {
				return nil, "", errors.New("planner returned nil output")
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

	execInputBuilder := agent_v2.ExecInputBuilder(func(
		_ context.Context,
		original string,
		executed []agent_v2.ExecutedStep,
		current agent_v2.PlanStep,
		_ []int,
	) (*schema.Message, error) {
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("项目需求：%s\n\n", original))
		if len(executed) > 0 {
			sb.WriteString("已完成工作摘要：\n")
			for _, e := range executed {
				result := e.Result
				if len(result) > 200 {
					result = result[:200] + "..."
				}
				sb.WriteString(fmt.Sprintf("- %s：%s\n", e.Step, result))
			}
			sb.WriteString("\n")
		}
		sb.WriteString(fmt.Sprintf("当前任务（%s）：%s", current.SubAgentName, current.Description))
		return schema.UserMessage(sb.String()), nil
	})

	pa := agent_v2.NewPlanAgent(
		customPlanner,
		agent_v2.PlanName("feature_dev_pipeline"),
		agent_v2.WithSubAgents(map[string]agent_v2.Agent{
			"architect": architect,
			"developer": developer,
			"tester":    tester,
			"devops":    devops,
		}),
		agent_v2.WithExecInputBuilder(execInputBuilder),
	).SetExecutor(developer)

	events, err := pa.Run(ctx300s(t), nil,
		schema.UserMessage("为电商平台开发用户登录和 JWT 认证功能（支持短信验证码和密码两种方式）"))
	if err != nil {
		t.Fatalf("PlanAgent 运行失败: %v", err)
	}
	out := mustFinalMessageV2(t, events)
	t.Logf("开发计划执行结果（前 600 字符）: %.600s", out.Content)
}
