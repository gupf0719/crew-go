//go:build integration

package tutorial_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// ──────────────────────────────────────────────────────────────────
// 2. SequentialAgent（进阶）— 三阶段代码审查流水线
// 阶段间输出→输入改写通过子 agent 的 OnAgentStart hook 完成。
// ──────────────────────────────────────────────────────────────────

// stageOutputHook 在 agent 每轮结束后打印本阶段输出。
// 用 OnAgentEnd 而非 OnAgentStart：End 触发时拿到的是本轮真实输出，
// 且每轮只触发一次，不受 ReAct 多轮影响。
type stageOutputHook struct {
	hook.NoOpHooks
	t *testing.T
}

func (h *stageOutputHook) OnAgentEnd(ctx context.Context, info *hook.AgentInfo, output *hook.AgentOutput) context.Context {
	if len(output.Messages) == 0 {
		return ctx
	}
	last := output.Messages[len(output.Messages)-1]
	h.t.Logf("=== [%s 输出] ===\n%s", info.Name, last.Content)
	return ctx
}

// stageInputHook 在 agent 第一轮启动前，将输入消息包上前序阶段标注。
// 只在 Round==1 时改写，避免多轮 ReAct 重复处理。
type stageInputHook struct {
	hook.NoOpHooks
	prevNode string
}

func (h *stageInputHook) OnAgentStart(ctx context.Context, info *hook.AgentInfo, input *hook.AgentInput) (context.Context, error) {
	if info.Round != 1 || len(input.Messages) == 0 {
		return ctx, nil
	}
	last := input.Messages[len(input.Messages)-1]
	header := fmt.Sprintf("【前序阶段 %q 的分析结果】\n\n", h.prevNode)
	input.Messages[len(input.Messages)-1] = schema.UserMessage(header + last.Content)
	return ctx, nil
}

func TestAdv_02_SequentialAgent(t *testing.T) {
	provider := requireAPIKey(t)

	outputHook := &stageOutputHook{t: t}

	// 阶段 1：静态分析——识别潜在 Bug 和代码异味
	analyzer := agent_v2.NewBasicAgent(
		agent_v2.WithName("static_analyzer"),
		agent_v2.WithSystemPrompt(
			"你是代码静态分析专家。分析给定代码，以列表形式输出：\n"+
				"[BUG] 潜在错误\n[SMELL] 代码异味\n[PERF] 性能问题\n"+
				"每条一行，附简短说明。只输出问题列表，不输出修复建议。",
		),
		agent_v2.WithModel(newLLM(provider)),
		agent_v2.WithHooks(outputHook),
	)

	// 阶段 2：安全扫描——OnAgentStart 插入前序标注，OnAgentEnd 打印本阶段输出
	securityScanner := agent_v2.NewBasicAgent(
		agent_v2.WithName("security_scanner"),
		agent_v2.WithSystemPrompt(
			"你是代码安全审计专家，只分析安全漏洞。根据收到的代码及前序分析报告，输出：\n"+
				"[SEC-HIGH] 高危漏洞\n[SEC-MED] 中危漏洞\n[SEC-LOW] 低危风险\n"+
				"每条一行，附 CVE/CWE 编号（如有）。若无安全问题，输出 [PASS] 无安全漏洞。",
		),
		agent_v2.WithModel(newLLM(provider)),
		agent_v2.WithHooks(hook.NewChainHooks(
			&stageInputHook{prevNode: "static_analyzer"},
			outputHook,
		)),
	)

	// 阶段 3：综合报告——OnAgentStart 插入前序标注，OnAgentEnd 打印本阶段输出
	reportWriter := agent_v2.NewBasicAgent(
		agent_v2.WithName("report_writer"),
		agent_v2.WithSystemPrompt(
			"你是技术文档专家。根据收到的静态分析和安全扫描结果，生成标准 Markdown 格式的代码审查报告。\n"+
				"报告结构：\n"+
				"# 代码审查报告\n"+
				"## 执行摘要\n"+
				"## 静态分析结果\n"+
				"## 安全审计结果\n"+
				"## 优先修复建议（按严重程度排序）",
		),
		agent_v2.WithModel(newLLM(provider)),
		agent_v2.WithHooks(hook.NewChainHooks(
			&stageInputHook{prevNode: "security_scanner"},
			outputHook,
		)),
	)

	seq := agent_v2.NewSequentialAgent(
		agent_v2.WithSequentialName("code_review_pipeline"),
	).
		AddStep("static_analyzer", analyzer).
		AddStep("security_scanner", securityScanner).
		AddStep("report_writer", reportWriter)

	// 含明显问题的代码片段
	codeSnippet := `
func processPayment(userInput string) string {
    query := "SELECT * FROM orders WHERE id = " + userInput
    db.Exec(query)
    password := "admin123"
    for i := 0; i < 1000000; i++ {
        fmt.Println(i)
    }
    return nil
}
`
	events, err := seq.Run(ctx300s(t), nil,
		schema.UserMessage("请审查以下 Go 代码：\n```go"+codeSnippet+"```"))
	if err != nil {
		t.Fatalf("SequentialAgent 运行失败: %v", err)
	}
	out := mustFinalMessageV2(t, events)

	if !strings.Contains(out.Content, "代码审查报告") {
		t.Errorf("期望报告包含 '代码审查报告' 标题，实际: %q", out.Content[:minInt(200, len(out.Content))])
	}
}
