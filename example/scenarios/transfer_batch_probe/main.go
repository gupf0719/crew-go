//go:build integration

// transfer_batch_probe 实测：真实 LLM 会不会把 return-direct 的 transfer_to_agent
// 跟别的业务工具塞进**同一批** tool_call。
//
// # 为什么要测这个
//
// 框架对「return-direct 工具与别的工具同批」**没有任何约束**（basic_builder.go:630
// 的语义是「任一 tool_result 带 return-direct 标记就短路」——同批工具**全部执行完**
// 之后才不回 model）。于是同批那些业务工具：副作用真的发生了，产出却没有任何人看。
//
// ISSUE-99 位 6（转交信号跨存档存活 + 框架工具豁免名单）整个建立在这个组合之上：
// transfer 成功 + 兄弟工具中断 → 存档 → 信号要活下来。而那个场景此前**只在 mock 里
// 构造过**——让假模型同时返回两个 tool_call。真实 LLM 会不会这么调，从没量过。
//
// 结果决定位 6 的修法方向：
//   - 若真实 LLM 会同批 → 场景真实，位 6 需要「信号进持久载体」的结构解；
//   - 若基本不会 → 更根治的解是**规范这个组合本身**（同批出现 return-direct 时其余
//     不执行 / 整批打回），问题类直接消失一大半。
//
// 用法：go run -tags integration ./example/scenarios/transfer_batch_probe
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"crew-go/agent_v2"
	"crew-go/example/envloader"
	"crew-go/fallback"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/model/qwen"
	"crew-go/schema"
	"crew-go/tool"
)

// batchRecord 记一轮 LLM 输出的 tool_call 组成。
type batchRecord struct {
	agent string
	names []string
}

func (b batchRecord) hasTransfer() bool {
	for _, n := range b.names {
		if n == agent_v2.HandoffTransferToolName {
			return true
		}
	}
	return false
}

// mixed 报告「transfer 与业务工具同批」——即本次实测要量的那个量。
func (b batchRecord) mixed() bool {
	return b.hasTransfer() && len(b.names) > 1
}

// probeHooks 抓每轮 LLM 产出的 tool_call 名单。用 OnModelEnd 而不是 OnToolStart：
// 要的是「模型一次决定了调哪些」，而不是「框架执行了哪些」。
type probeHooks struct {
	hook.NoOpHooks
	records *[]batchRecord
}

func (p probeHooks) OnModelEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	if out == nil || out.Message == nil {
		return ctx, nil
	}
	tcs := schema.GetToolCalls(out.Message)
	if len(tcs) == 0 {
		return ctx, nil
	}
	names := make([]string, 0, len(tcs))
	for _, tc := range tcs {
		names = append(names, tc.Function.Name)
	}
	agentName := ""
	if info != nil {
		agentName = info.Name
	}
	*p.records = append(*p.records, batchRecord{agent: agentName, names: names})
	return ctx, nil
}

func bizTools() *tool.Toolkit {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("query_order", "查询订单状态与金额",
		map[string]any{
			"type":       "object",
			"properties": map[string]any{"order_id": map[string]any{"type": "string", "description": "订单号"}},
			"required":   []string{"order_id"},
		},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: fmt.Sprintf("订单 %v：状态=已支付，金额=￥238.00", args["order_id"])}, nil
		}))
	tk.Register(tool.NewFuncTool("get_refund_policy", "查询退款政策条款",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: "7 天内无理由退款，超期需人工审核。"}, nil
		}))
	return tk
}

func runOnce(ctx context.Context, m model.ChatModel, prompt string) ([]batchRecord, string, error) {
	var records []batchRecord
	h := probeHooks{records: &records}

	front := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithDescription("前台客服，可查询订单与退款政策；账单争议需转给 billing"),
		agent_v2.WithSystemPrompt("你是前台客服。你可以查询订单和退款政策。遇到账单争议类问题，请转交给 billing 专员处理。"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(bizTools()),
		agent_v2.WithHandoffTransfer(), // 授予转交能力
		agent_v2.WithHooks(h),
		agent_v2.WithMaxRounds(4),
	)
	billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithDescription("账单专员，处理账单争议与退款"),
		agent_v2.WithSystemPrompt("你是账单专员。直接给出处理结论，不要再转交。"),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(h),
		agent_v2.WithMaxRounds(3),
	)

	ha := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("support"),
		agent_v2.WithHandoffDescription("客服团队"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)

	events, err := ha.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(prompt))
	if err != nil {
		return records, "", err
	}
	final := ""
	if fm := agent_v2.FinalMessage(events); fm != nil {
		final = fm.Content
	}
	return records, final, nil
}

func main() {
	envloader.Load()
	if os.Getenv("QWEN_API_KEY") == "" {
		fmt.Println("需要 .env 里的 QWEN_API_KEY")
		os.Exit(1)
	}

	models := make([]model.ChatModel, 0, len(qwen.TestModelCandidates))
	for _, name := range qwen.TestModelCandidates {
		models = append(models, qwen.NewTestClient(name))
	}
	m := fallback.New(models)

	// 三档诱导强度。若连最强诱导都不同批，这个组合基本不会自然发生。
	scenarios := []struct{ name, prompt string }{
		{"A 中性", "我的订单 A123 账单金额好像不对，能帮我看看吗？"},
		{"B 暗示先查后转", "帮我查一下订单 A123 的金额，然后把我转给账单专员处理这个争议。"},
		{"C 强诱导同批", "请同时做两件事：查询订单 A123 的状态，并且立刻把我转交给账单专员。两件事一起做，不要分先后。"},
	}
	const rounds = 3

	fmt.Println("=== 实测：真实 LLM 会不会把 transfer 与业务工具塞进同一批 tool_call ===")
	fmt.Printf("模型候选：%v\n\n", qwen.TestModelCandidates)

	totalBatches, transferBatches, mixedBatches := 0, 0, 0
	for _, sc := range scenarios {
		fmt.Printf("── 场景 %s ──\n   prompt: %s\n", sc.name, sc.prompt)
		for i := 1; i <= rounds; i++ {
			records, final, err := runOnce(context.Background(), m, sc.prompt)
			if err != nil {
				fmt.Printf("   #%d 失败: %v\n", i, err)
				continue
			}
			for _, r := range records {
				totalBatches++
				if r.hasTransfer() {
					transferBatches++
				}
				mark := ""
				if r.mixed() {
					mixedBatches++
					mark = "  ← transfer 与业务工具同批！"
				}
				fmt.Printf("   #%d [%s] 一批 %d 个: %s%s\n", i, r.agent, len(r.names), strings.Join(r.names, ", "), mark)
			}
			if len(records) == 0 {
				fmt.Printf("   #%d 无 tool_call（模型直接作答）\n", i)
			}
			fmt.Printf("   #%d final: %s\n", i, truncate(final, 60))
		}
		fmt.Println()
	}

	fmt.Println("=== 统计 ===")
	fmt.Printf("总 tool_call 批次:            %d\n", totalBatches)
	fmt.Printf("其中含 transfer 的批次:       %d\n", transferBatches)
	fmt.Printf("transfer 与业务工具同批:      %d   ← 位 6 场景的真实性\n", mixedBatches)
	if mixedBatches == 0 {
		fmt.Println("\n结论：未观测到同批。位 6 的场景在真实 LLM 下不自然发生 ⟹ 更根治的解是规范这个组合本身。")
	} else {
		fmt.Printf("\n结论：观测到 %d 次同批。场景真实 ⟹ 位 6 需要「信号进持久载体」的结构解。\n", mixedBatches)
	}
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len([]rune(s)) <= n {
		return s
	}
	return string([]rune(s)[:n]) + "…"
}
