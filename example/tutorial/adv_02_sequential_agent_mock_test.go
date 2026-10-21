package tutorial_test

// adv_02 mock 镜像：SequentialAgent 三阶段流水线 + stage hook 改写
//
// 迁移到 agent_v2：
//   - constructor 改链式：NewSequentialAgent + AddStep
//   - SequentialName → WithSequentialName
//   - SequentialAgent.Run 第二参数 *RunSession 改为 agent_v2.RunSession

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/schema"
)

func TestAdv_02_SequentialAgent_Mock(t *testing.T) {
	makeAgent := func(name, output string, prevNode string) agent_v2.Agent {
		stub := &stubModel{
			reply: func(_, _ string, _ []*schema.Message) *schema.Message {
				return schema.AssistantMessage(output)
			},
		}
		hooks := []hook.Hooks{}
		if prevNode != "" {
			hooks = append(hooks, &stageInputHookMock{prevNode: prevNode})
		}
		hooks = append(hooks, &stageOutputHookMock{t: t})
		return agent_v2.NewBasicAgent(
			agent_v2.WithName(name),
			agent_v2.WithSystemPrompt(name),
			agent_v2.WithModel(stub),
			agent_v2.WithHooks(hook.NewChainHooks(hooks...)),
		)
	}
	analyzer := makeAgent("static_analyzer", "[BUG] SQL 拼接\n[SEC] 硬编码密码", "")
	scanner := makeAgent("security_scanner", "[SEC-HIGH] SQL 注入 CWE-89\n[SEC-MED] 硬编码凭证 CWE-798", "static_analyzer")
	writer := makeAgent("report_writer", "# 代码审查报告\n## 执行摘要\nSQL 注入 + 硬编码风险\n## 优先修复建议\n1. 参数化查询\n2. 移走密码到 env", "security_scanner")

	seq := agent_v2.NewSequentialAgent(
		agent_v2.WithSequentialName("code_review_pipeline"),
	).
		AddStep("static_analyzer", analyzer).
		AddStep("security_scanner", scanner).
		AddStep("report_writer", writer)

	events, err := seq.Run(ctx60sMock(t), nil, schema.UserMessage("审查代码"))
	if err != nil {
		t.Fatalf("Run 失败: %v", err)
	}
	out := agent_v2.FinalMessage(events)
	if out == nil || !strings.Contains(out.Content, "代码审查报告") {
		t.Errorf("期望含 '代码审查报告'，实际 %q", out.Content)
	}
}

// stage hooks（mock 版）
type stageOutputHookMock struct {
	hook.NoOpHooks
	t *testing.T
}

func (h *stageOutputHookMock) OnAgentEnd(ctx context.Context, info *hook.AgentInfo, output *hook.AgentOutput) context.Context {
	if len(output.Messages) > 0 {
		last := output.Messages[len(output.Messages)-1]
		h.t.Logf("=== [%s 输出] ===\n%s", info.Name, last.Content)
	}
	return ctx
}

type stageInputHookMock struct {
	hook.NoOpHooks
	prevNode string
}

func (h *stageInputHookMock) OnAgentStart(ctx context.Context, info *hook.AgentInfo, input *hook.AgentInput) (context.Context, error) {
	if info.Round != 1 || len(input.Messages) == 0 {
		return ctx, nil
	}
	last := input.Messages[len(input.Messages)-1]
	header := fmt.Sprintf("【前序阶段 %q】\n\n", h.prevNode)
	input.Messages[len(input.Messages)-1] = schema.UserMessage(header + last.Content)
	return ctx, nil
}
