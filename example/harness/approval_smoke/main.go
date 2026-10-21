// approval_smoke 用真实 qwen 验工具审批层（harness P1）端到端：
//
//	Part 1 approve：真 LLM 被要求删数据 → 调 delete_records → 框架审批 gate 拦截抛
//	         tool_approval 中断（工具**未**执行）→ caller approve → Resume → 工具执行 →
//	         LLM 汇报影响行数（88 是 LLM 编不出的标记，证明工具真跑了）。
//	Part 2 deny：同流程但 caller deny → 工具**不**执行（executed=false）→ 工具级
//	         [tool_denied] 回灌 LLM 继续。
//
// 模型走 fallback 链 flash → flash-2026-04-16 → plus → plus37（额度满/限流自动切）。
// QWEN_API_KEY 走项目根 .env。跑：QWEN_API_KEY=xxx go run ./example/harness/approval_smoke
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/example/envloader"
	"crew-go/example/llmtrace"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// buildAgent 建一个"删数据需审批"的真 qwen BasicAgent；executed 在 delete_records 真执行时置真。
func buildAgent(m model.ChatModel, tr *llmtrace.Tracer, executed *bool) agent_v2.Agent {
	del := tool.NewFuncTool("delete_records", "删除指定表的旧记录。参数 table=表名。",
		map[string]any{"type": "object", "properties": map[string]any{
			"table": map[string]any{"type": "string", "description": "表名"}}, "required": []any{"table"}},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			*executed = true
			table, _ := args["table"].(string)
			// 88 是 LLM 编不出的"真实数据"——只有工具真跑了才拿得到。
			return tool.Success(fmt.Sprintf("已删除 %s 表旧记录：影响 88 行，释放 1.2GB", table)), nil
		})
	tk := tool.NewToolkit()
	tk.Register(del)
	return agent_v2.NewBasicAgent(
		agent_v2.WithName("ops"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithSystemPrompt("你是运维助手。用户要删数据时调 delete_records 工具（传 table 参数）。拿到结果后用自然语言汇报影响了多少行。"),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithHooks(tr.Hooks()),
		agent_v2.WithMaxRounds(6),
	)
}

func finalText(events []*agent_v2.AgentEvent) string {
	if f := agent_v2.FinalMessage(events); f != nil {
		return f.Content
	}
	return ""
}

func main() {
	envloader.Load()
	if os.Getenv("QWEN_API_KEY") == "" {
		fmt.Println("✗ QWEN_API_KEY 未设置（项目根 .env 或环境变量）")
		os.Exit(1)
	}
	m, tr := llmtrace.NewQwenFallback()
	pass := true
	check := func(cond bool, name string) {
		if cond {
			fmt.Printf("  ✓ PASS %s\n", name)
		} else {
			fmt.Printf("  ✗ FAIL %s\n", name)
			pass = false
		}
	}

	// ── Part 1：approve ──────────────────────────────────────────────────
	fmt.Println("=== Part 1：审批中断 → approve → 工具执行 ===")
	executed1 := false
	ag1 := buildAgent(m, tr, &executed1)
	cp1 := compose_v2.NewInMemoryCheckpointer()
	sess1 := agent_v2.NewRunSession()
	ctx1, cancel1 := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel1()
	ctx1 = compose_v2.WithCheckpointerCtx(ctx1, cp1)

	_, err := ag1.Run(ctx1, sess1, schema.UserMessage("帮我删除 logs 表的旧数据"))
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		fmt.Printf("✗ 期望审批中断，实际 err=%v（疑似 LLM 没调工具/额度问题）\n", err)
		os.Exit(1)
	}
	fmt.Printf("  中断 Reason=%q Query=%q Options=%v\n", ie.Reason, ie.Query.Question, ie.Query.Options)
	check(ie.Reason == "tool_approval", "抛 tool_approval 中断")
	check(!executed1, "中断时工具未执行（被 gate 拦在 Call 前）")

	events, err := ag1.Resume(ctx1, sess1.RunID, agent_v2.WithResumeSession(sess1),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApprove)}))
	if err != nil {
		fmt.Printf("✗ Resume approve 报错：%v\n", err)
		os.Exit(1)
	}
	ans1 := finalText(events)
	fmt.Printf("  approve 后最终答复：%s\n", truncate(ans1, 90))
	check(executed1, "approve 后工具真执行")
	check(strings.Contains(ans1, "88"), "LLM 汇报含工具返回的 88 行（证明回灌链通、非编造）")

	// ── Part 2：deny ─────────────────────────────────────────────────────
	fmt.Println("=== Part 2：审批中断 → deny → 工具不执行 ===")
	executed2 := false
	ag2 := buildAgent(m, tr, &executed2)
	cp2 := compose_v2.NewInMemoryCheckpointer()
	sess2 := agent_v2.NewRunSession()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel2()
	ctx2 = compose_v2.WithCheckpointerCtx(ctx2, cp2)

	_, err = ag2.Run(ctx2, sess2, schema.UserMessage("帮我删除 events 表的旧数据"))
	ie2, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		fmt.Printf("✗ 期望审批中断，实际 err=%v\n", err)
		os.Exit(1)
	}
	events2, err := ag2.Resume(ctx2, sess2.RunID, agent_v2.WithResumeSession(sess2),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie2.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionDeny)}))
	if err != nil {
		fmt.Printf("✗ Resume deny 报错（拒绝应是工具级失败非 Run 失败）：%v\n", err)
		os.Exit(1)
	}
	fmt.Printf("  deny 后最终答复：%s\n", truncate(finalText(events2), 90))
	check(!executed2, "deny 后工具未执行（被拒）")

	fmt.Println("\n" + tr.Report())
	if !pass {
		fmt.Println("\n✗ 有场景 FAIL")
		os.Exit(1)
	}
	fmt.Println("\n=== 全部 PASS：工具审批层经真 qwen 端到端正确（approve 执行 / deny 拦截）===")
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len([]rune(s)) <= n {
		return s
	}
	return string([]rune(s)[:n]) + "…"
}
