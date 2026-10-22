// spill_smoke 用**真 qwen**验证超大工具输出转存（WithToolResultSpill）端到端：工具吐出二十多万字节的
// 订单清单，问其中某一单的备注——模型只看得到说明 + 预览，要读懂说明、调 read_tool_output 取回（按行
// 或按正则）才答得上来。
//
// 用法：项目根 .env 配 QWEN_API_KEY，然后 go run ./example/harness/spill_smoke
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/example/envloader"
	"crew-go/example/llmtrace"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

// callRecorder 记下模型调了哪些工具、模型收到的工具结果最大有多大。
//
// 大小在 OnModelStart 里量（模型实际收到的 tool_result），不在 OnToolEnd 里量：转存在 OnToolEnd 之后做，钩子看到的
// 是全文（结果脱敏这类改写要作用在全文上）。
type callRecorder struct {
	hook.NoOpHooks
	mu      sync.Mutex
	names   []string
	maxSeen int
}

func (r *callRecorder) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	r.mu.Lock()
	r.names = append(r.names, in.Name)
	r.mu.Unlock()
	return ctx, nil
}

func (r *callRecorder) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, msg := range in.Messages {
		if msg != nil && msg.Role == schema.RoleTool && len(msg.Content) > r.maxSeen {
			r.maxSeen = len(msg.Content)
		}
	}
	return ctx, nil
}

func orders() string {
	var b strings.Builder
	statuses := []string{"已发货", "待付款", "已签收", "已取消"}
	for i := 1; i <= 4000; i++ {
		note := fmt.Sprintf("NOTE-%05d", i*7%10000)
		if i == 2718 {
			note = "VEGA-42 优先处理"
		}
		fmt.Fprintf(&b, "订单 #%05d：客户 C%05d，金额 %d 元，状态 %s，备注 %s\n", i, i, 100+i%900, statuses[i%4], note)
	}
	return b.String()
}

func main() {
	envloader.Load()
	if os.Getenv("QWEN_API_KEY") == "" {
		fmt.Println("QWEN_API_KEY 未设置（.env 加 QWEN_API_KEY=xxx）——本 smoke 需真 qwen")
		os.Exit(1)
	}
	m, tr := llmtrace.NewQwenFallback()
	data := orders()
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("export_orders", "导出全部订单清单（每行一单）", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: data}, nil
		}))
	rec := &callRecorder{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ops"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithStore(store.NewInMemoryStore()),
		agent_v2.WithToolResultSpill(nil),
		agent_v2.WithHooks(hook.NewChainHooks(rec, tr.Hooks())),
		agent_v2.WithMaxRounds(8),
		agent_v2.WithSystemPrompt("你是订单助手。需要数据时调用工具，不要编造。"),
	)
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "spill-smoke")
	events, err := ag.Run(ctx, agent_v2.NewRunSession(),
		schema.UserMessage("先调用 export_orders 导出全部订单，然后告诉我订单 #02718 的备注是什么。只回答备注内容。"))
	if err != nil {
		fmt.Println("FAIL Run:", err)
		os.Exit(1)
	}
	final := agent_v2.FinalMessage(events)
	answer := ""
	if final != nil {
		answer = final.Content
	}
	fmt.Printf("工具输出原文 %d 字节；模型看到的工具结果最大 %d 字节\n", len(data), rec.maxSeen)
	fmt.Printf("调用过的工具：%v\n", rec.names)
	fmt.Printf("答复：%s\n", answer)
	fmt.Println(tr.Report())

	ok := true
	if rec.maxSeen > 20_000 {
		fmt.Println("FAIL 模型看到的工具结果没被换成预览")
		ok = false
	}
	read := 0
	for _, n := range rec.names {
		if n == agent_v2.ReadToolOutputToolName {
			read++
		}
	}
	if read == 0 {
		fmt.Println("FAIL 模型没有调 read_tool_output 取回")
		ok = false
	}
	if !strings.Contains(answer, "VEGA-42") {
		fmt.Println("FAIL 答复里没有正确的备注 VEGA-42")
		ok = false
	}
	if !ok {
		os.Exit(1)
	}
	fmt.Println("=== spill_smoke PASS（超大输出转存 → 模型读说明 → read_tool_output 取回 → 答对）===")
}
