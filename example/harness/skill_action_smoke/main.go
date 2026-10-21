// skill_action_smoke 用真实 qwen 验 skill action 端到端：
//
//	skill "inventory" 暴露一个 go-func action get_stock（确定性返一个 LLM 编不出来的库存数字
//	237）。真 LLM 被要求查库存 → 自主调 run_skill_action(inventory, get_stock, {sku}) → 框架调
//	SkillBox.RunSkillAction → go-func 跑 → 结果回灌 → LLM 报出 237。
//
// 关键：237 只有 action 跑了才拿得到——若最终答案含 237 且 action.executed 标记为真，证明
// 「LLM → run_skill_action → go-func action → 结果回灌」整条链通了（非 LLM 自己编）。
//
// 模型走 fallback 链 flash → flash-2026-04-16 → plus → plus37（额度满/限流/欠费自动切）。
// QWEN_API_KEY 走项目根 .env。跑：QWEN_API_KEY=xxx go run ./example/harness/skill_action_smoke
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"crew-go/agent_v2"
	"crew-go/example/envloader"
	"crew-go/example/llmtrace"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/skill"
)

// captureHook 记录 LLM 各轮 emit 的 tool_call 名。
type captureHook struct {
	hook.NoOpHooks
	mu      sync.Mutex
	emitted []string
}

func (h *captureHook) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, tc := range schema.GetToolCalls(out.Message) {
		h.emitted = append(h.emitted, tc.Function.Name)
	}
	return ctx, nil
}

func (h *captureHook) called(name string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, n := range h.emitted {
		if n == name {
			return true
		}
	}
	return false
}

// buildBox 建含 inventory skill 的 box，注册 go-func action get_stock；executed 在 action 跑时置真。
func buildBox(root string, executed *bool) (skill.SkillBox, error) {
	dir := root + "/inventory"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	md := "---\nname: inventory\ndescription: 商品库存查询\n---\n库存查询助手。\n"
	if err := os.WriteFile(dir+"/SKILL.md", []byte(md), 0o644); err != nil {
		return nil, err
	}
	return skill.NewDefaultSkillBox(root,
		skill.WithSkillActionFunc("inventory",
			skill.SkillActionDescriptor{
				Name:        "get_stock",
				Description: "按 SKU 查询当前库存件数",
				ArgumentsSchema: map[string]any{
					"type":       "object",
					"properties": map[string]any{"sku": map[string]any{"type": "string", "description": "商品 SKU"}},
					"required":   []any{"sku"},
				},
			},
			func(_ context.Context, args map[string]any) (*skill.SkillExecutionResult, error) {
				*executed = true
				sku, _ := args["sku"].(string)
				// 237 是 LLM 编不出来的「真实数据」——只有 action 跑了才拿得到。
				return &skill.SkillExecutionResult{
					Success: true,
					Output:  fmt.Sprintf("【库存系统】%s 当前库存 237 件", sku),
				}, nil
			}))
}

func main() {
	envloader.Load()
	if os.Getenv("QWEN_API_KEY") == "" {
		fmt.Println("✗ QWEN_API_KEY 未设置（项目根 .env 或环境变量）")
		os.Exit(1)
	}
	root, err := os.MkdirTemp("", "skill-action-smoke")
	if err != nil {
		fmt.Printf("✗ temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(root)

	executed := false
	box, err := buildBox(root, &executed)
	if err != nil {
		fmt.Printf("✗ build box: %v\n", err)
		os.Exit(1)
	}

	h := &captureHook{}
	m, tr := llmtrace.NewQwenFallback()
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("inv_assistant"),
		agent_v2.WithModel(m),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSystemPrompt("你是库存助手。用户查商品库存时，**必须调 run_skill_action 工具调 inventory skill 的 get_stock action**（传 sku 参数）拿真实库存数字，**不要自己编造数字**。拿到结果后用自然语言回答用户。"),
		agent_v2.WithHooks(hook.NewChainHooks(h, tr.Hooks())),
		agent_v2.WithMaxRounds(6),
	)

	fmt.Println("=== skill action 真 qwen 端到端：LLM → run_skill_action → go-func action → 结果回灌 ===")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	events, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("帮我查一下 SKU-7788 现在有多少库存？"))
	if err != nil {
		fmt.Printf("✗ Run 失败（疑似全模型额度/网络问题，非 skill-action bug）：%v\n", err)
		os.Exit(1)
	}

	answer := ""
	if final := agent_v2.FinalMessage(events); final != nil {
		answer = final.Content
	}
	calledTool := h.called(agent_v2.RunSkillActionToolName)

	fmt.Printf("  LLM 调了 run_skill_action: %v\n", calledTool)
	fmt.Printf("  go-func action 真执行: %v\n", executed)
	fmt.Printf("  最终答案: %s\n", truncate(answer, 100))
	fmt.Println("  " + tr.Report())

	// PASS 判定：action 真跑了（executed）+ 结果数字 237 进了最终答案（证明回灌链通、非 LLM 编造）。
	pass := executed && strings.Contains(answer, "237")
	fmt.Println()
	if pass {
		fmt.Println("=== skill action 真 LLM 端到端通过：action 执行 + 结果 237 回灌进答案 ===")
		return
	}
	fmt.Printf("✗ FAIL：executed=%v / 答案含237=%v / 调run_skill_action=%v\n",
		executed, strings.Contains(answer, "237"), calledTool)
	os.Exit(1)
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len([]rune(s)) <= n {
		return s
	}
	return string([]rune(s)[:n]) + "…"
}
