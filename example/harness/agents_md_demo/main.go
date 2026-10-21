// agents_md_demo 演示 WithAgentsMD（mock 模型，不需要 API key）：
//
//	[1] 分层：AGENTS.md 是 harness 指令层，排在 WithSystemPrompt 任务层之前——LLM 输入首条
//	[2] 多文件按传入顺序拼接
//	[3] transient：每轮 LLM 调用都重新注入一次，但不写进会话历史（sess.History()）——
//	    所以它不会被 4 层压缩摘要掉，Resume 也不用业务自己补
//	[4] 缺文件静默跳过（只打 slog warn），其余文件照注入
//
// 跑：go run ./example/harness/agents_md_demo
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// capturingModel 记录每次 Generate 收到的完整输入；按内容路由：本轮已有 tool_result 就收尾，
// 否则发一次 noop 工具调用（凑出第二轮，验「每轮都注入」）。
type capturingModel struct {
	mu       sync.Mutex
	captured [][]*schema.Message
	twoRound bool
}

func (m *capturingModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	m.captured = append(m.captured, append([]*schema.Message(nil), msgs...))
	m.mu.Unlock()
	if m.twoRound && (len(msgs) == 0 || msgs[len(msgs)-1].Role != schema.RoleTool) {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc_1", Type: "function",
			Function: schema.FunctionCall{Name: "noop", Arguments: `{}`},
		}}), nil
	}
	return schema.AssistantMessage("[mock] ok"), nil
}

func (m *capturingModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *capturingModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func writeFile(dir, name, content string) string {
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		fmt.Println("写文件失败:", err)
		os.Exit(1)
	}
	return p
}

// countIn 数一份 LLM 输入里含 needle 的消息条数。
func countIn(msgs []*schema.Message, needle string) int {
	n := 0
	for _, m := range msgs {
		if strings.Contains(m.Content, needle) {
			n++
		}
	}
	return n
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "agents_md_demo")
	if err != nil {
		fmt.Println("MkdirTemp:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)

	pass := true
	check := func(cond bool, name string) {
		if cond {
			fmt.Printf("  ✓ %s\n", name)
		} else {
			fmt.Printf("  ✗ %s\n", name)
			pass = false
		}
	}

	rulesMD := writeFile(dir, "AGENTS.md", "PROJECT RULE: 回答一律用中文。")
	styleMD := writeFile(dir, "STYLE.md", "STYLE RULE: 结论放第一句。")

	// ── [1]+[2] 分层 + 多文件顺序 ──
	fmt.Println("=== [1]+[2] AGENTS.md 是首条 system（harness 层），任务 system 排它后面；多文件按序拼 ===")
	{
		cm := &capturingModel{}
		ag := agent_v2.NewBasicAgent(
			agent_v2.WithName("md_demo"),
			agent_v2.WithModel(cm),
			agent_v2.WithSystemPrompt("你是客服助手。"),
			agent_v2.WithAgentsMD(rulesMD, styleMD),
		)
		if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
			fmt.Println("Run:", err)
			os.Exit(1)
		}
		in := cm.captured[0]
		for i, m := range in {
			fmt.Printf("  input[%d] %-9s %q\n", i, m.Role, m.Content)
		}
		harness := in[0]
		check(harness.Role == schema.RoleSystem && strings.Contains(harness.Content, "PROJECT RULE"), "input[0] 是 AGENTS.md（system）")
		check(strings.Index(harness.Content, "PROJECT RULE") < strings.Index(harness.Content, "STYLE RULE"), "两个文件按传入顺序拼接在同一条里")
		check(in[1].Role == schema.RoleSystem && strings.Contains(in[1].Content, "客服助手"), "input[1] 是 WithSystemPrompt 任务层")
	}

	// ── [3] transient：每轮注入、不进历史 ──
	fmt.Println("\n=== [3] transient：两轮各注入一次，会话历史里一条都没有 ===")
	{
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("noop", "什么都不做", map[string]any{"type": "object"},
			func(_ context.Context, _ map[string]any) (*tool.Result, error) { return tool.Success("done"), nil }))
		cm := &capturingModel{twoRound: true}
		ag := agent_v2.NewBasicAgent(
			agent_v2.WithName("md_demo"),
			agent_v2.WithModel(cm),
			agent_v2.WithToolkit(tk),
			agent_v2.WithSystemPrompt("你是客服助手。"),
			agent_v2.WithAgentsMD(rulesMD),
			agent_v2.WithMaxRounds(3),
		)
		sess := agent_v2.NewRunSession()
		if _, err := ag.Run(ctx, sess, schema.UserMessage("hi")); err != nil {
			fmt.Println("Run:", err)
			os.Exit(1)
		}
		fmt.Printf("  LLM 调了 %d 轮；各轮输入里 AGENTS.md 条数: ", len(cm.captured))
		perRound := make([]int, 0, len(cm.captured))
		for _, in := range cm.captured {
			perRound = append(perRound, countIn(in, "PROJECT RULE"))
		}
		fmt.Println(perRound)
		allOnce := len(cm.captured) == 2
		for _, n := range perRound {
			allOnce = allOnce && n == 1
		}
		check(allOnce, "每轮恰好注入 1 条（不是累积、也不是只在首轮）")
		check(countIn(sess.History(), "PROJECT RULE") == 0, "sess.History() 里 0 条——没写进持久历史")
	}

	// ── [4] 缺文件 ──
	fmt.Println("\n=== [4] 缺文件静默跳过：其余文件照注入 ===")
	{
		cm := &capturingModel{}
		ag := agent_v2.NewBasicAgent(
			agent_v2.WithName("md_demo"),
			agent_v2.WithModel(cm),
			agent_v2.WithSystemPrompt("你是客服助手。"),
			agent_v2.WithAgentsMD(filepath.Join(dir, "MISSING.md"), styleMD),
		)
		if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
			fmt.Println("Run:", err)
			os.Exit(1)
		}
		in := cm.captured[0]
		fmt.Printf("  input[0] %-9s %q\n", in[0].Role, in[0].Content)
		check(strings.Contains(in[0].Content, "STYLE RULE") && !strings.Contains(in[0].Content, "MISSING"), "缺的那份被跳过，STYLE.md 仍是首条 system")
	}

	fmt.Println()
	if !pass {
		fmt.Println("agents_md_demo FAIL")
		os.Exit(1)
	}
	fmt.Println("agents_md_demo PASS")
}
