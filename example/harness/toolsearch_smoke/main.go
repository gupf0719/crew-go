// toolsearch_smoke 用真实 qwen 验工具级渐进披露（toolsearch）端到端：
//
//	Part 1 渐进披露闭环：deferred 工具初始不进 bind list → LLM 自主调 tool_search 检索 →
//	         揭示的工具进 bind list → LLM 调它拿答案。
//	Part 2 fail-closed 执行拦截：强提示 LLM「直接调 get_weather」（该工具 deferred 未揭示、
//	         不在 bind list）→ 若 LLM 仍 emit 该 call（越过 bind list）→ 框架拒执行返
//	         [tool_not_revealed] → 捕获 hook 验拦截发生。
//
// 模型走 llmtrace 的 qwen fallback 链（候选取 qwen.TestModelCandidates）
// （flash 额度满/限流/欠费都自动切下一个）。QWEN_API_KEY 走项目根 .env（envloader 自动加载）。
//
// 跑：QWEN_API_KEY=xxx go run ./example/harness/toolsearch_smoke
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
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// record 记录哪些 deferred 工具被真正执行（tool.Call 触发）。
type record struct {
	mu       sync.Mutex
	executed []string
}

func (r *record) exec(name string) {
	r.mu.Lock()
	r.executed = append(r.executed, name)
	r.mu.Unlock()
}

func (r *record) has(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range r.executed {
		if n == name {
			return true
		}
	}
	return false
}

func (r *record) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.executed...)
}

// captureHook 捕获 LLM 每轮 emit 的 tool_call 名、是否见到 fail-closed 的 [tool_not_revealed] 反馈，以及 get_weather
// 有没有在被 tool_search 解锁之前就被调。
type captureHook struct {
	hook.NoOpHooks
	mu            sync.Mutex
	rejectionSeen bool       // 见过 [tool_not_revealed] tool_result（= fail-closed 触发过）
	weatherShown  bool       // 见过 tool_search 解锁 get_weather 的结果
	directWeather bool       // get_weather 在解锁之前就被 emit（越过 bind list）
	rounds        [][]string // LLM 各轮 emit 的 tool_call 名（含越过 bind list 的幻觉调用）
}

func (h *captureHook) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, msg := range in.Messages {
		if strings.Contains(msg.Content, "tool_not_revealed") {
			h.rejectionSeen = true
		}
		if msg.Role == schema.RoleTool && strings.Contains(msg.Content, "已解锁") && strings.Contains(msg.Content, "get_weather") {
			h.weatherShown = true
		}
	}
	return ctx, nil
}

func (h *captureHook) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var names []string
	for _, tc := range schema.GetToolCalls(out.Message) {
		names = append(names, tc.Function.Name)
		if tc.Function.Name == "get_weather" && !h.weatherShown {
			h.directWeather = true
		}
	}
	h.rounds = append(h.rounds, names)
	return ctx, nil
}

func (h *captureHook) snapshot() (rejection, direct bool, rounds [][]string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.rejectionSeen, h.directWeather, append([][]string(nil), h.rounds...)
}

var deferredSet = map[string]bool{
	"get_weather": true, "send_email": true, "search_flights": true,
	"book_hotel": true, "translate_text": true, "calculate": true,
}

func makeToolkit(rec *record) *tool.Toolkit {
	mk := func(name, desc, result string) tool.Tool {
		return tool.NewFuncTool(name, desc,
			map[string]any{"type": "object", "properties": map[string]any{
				"city": map[string]any{"type": "string", "description": "参数"},
			}},
			func(_ context.Context, _ map[string]any) (*tool.Result, error) {
				rec.exec(name)
				return tool.Success(result), nil
			})
	}
	tk := tool.NewToolkit()
	tk.RegisterAll(
		mk("get_weather", "查询指定城市的实时天气", "北京：晴，气温 25°C，东南风 2 级"),
		mk("send_email", "发送电子邮件给指定收件人", "邮件已发送"),
		mk("search_flights", "搜索两地之间的航班", "找到 3 个航班"),
		mk("book_hotel", "预订指定城市的酒店", "已预订"),
		mk("translate_text", "把文本翻译成目标语言", "（译文）"),
		mk("calculate", "执行数学计算", "42"),
	)
	return tk
}

func deferred(t tool.Tool) bool { return deferredSet[t.Name()] }

func runScenario(m model.ChatModel, tr *llmtrace.Tracer, system, user string) (*record, *captureHook, *schema.Message, error) {
	rec := &record{}
	h := &captureHook{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ts_smoke"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(makeToolkit(rec)),
		agent_v2.WithSystemPrompt(system),
		agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true, Deferred: deferred}),
		agent_v2.WithHooks(hook.NewChainHooks(h, tr.Hooks())),
		agent_v2.WithMaxRounds(8),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	events, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(user))
	if err != nil {
		return rec, h, nil, err
	}
	return rec, h, agent_v2.FinalMessage(events), nil
}

func main() {
	envloader.Load()
	if os.Getenv("QWEN_API_KEY") == "" {
		fmt.Println("✗ QWEN_API_KEY 未设置（项目根 .env 或环境变量）")
		os.Exit(1)
	}
	m, tr := llmtrace.NewQwenFallback()
	pass := true

	// ── Part 1：渐进披露闭环 ──────────────────────────────────────────────
	fmt.Println("=== Part 1：渐进披露闭环（deferred 隐藏 → tool_search → 揭示 → 调用）===")
	rec1, h1, final1, err1 := runScenario(m, tr,
		"你是助手。当前可见工具有限——你需要的工具如果不在可见列表里，必须先调 tool_search（传 query 描述要做的事）检索解锁，拿到工具后再调用它。",
		"北京今天天气怎么样？")
	if err1 != nil {
		fmt.Printf("✗ Run 失败（疑似全模型额度/网络问题，非 toolsearch bug）：%v\n", err1)
		os.Exit(1)
	}
	_, _, rounds1 := h1.snapshot()
	calledSearch := false
	for _, r := range rounds1 {
		for _, n := range r {
			if n == "tool_search" {
				calledSearch = true
			}
		}
	}
	fmt.Printf("  LLM 各轮 emit 的 tool_call: %v\n", rounds1)
	fmt.Printf("  实际执行的 deferred 工具: %v\n", rec1.list())
	answer1 := ""
	if final1 != nil {
		answer1 = final1.Content
	}
	fmt.Printf("  最终答案: %s\n", truncate(answer1, 80))
	// 判定：LLM 调了 tool_search 解锁 + get_weather 被揭示后执行 + 答案含天气信息
	p1 := calledSearch && rec1.has("get_weather") && (strings.Contains(answer1, "晴") || strings.Contains(answer1, "25") || strings.Contains(answer1, "天气") || strings.Contains(answer1, "气温"))
	if p1 {
		fmt.Println("  ✓ PASS：tool_search 解锁 → get_weather 执行 → 答案落地")
	} else {
		fmt.Printf("  ✗ FAIL：calledSearch=%v get_weather执行=%v\n", calledSearch, rec1.has("get_weather"))
		pass = false
	}

	// ── Part 2：fail-closed 执行拦截 ──────────────────────────────────────
	fmt.Println("\n=== Part 2：fail-closed 执行拦截（强提示直调未揭示工具 → 拒执行）===")
	rec2, h2, final2, err2 := runScenario(m, tr,
		"你是天气助手。用户问天气时**直接调用 get_weather 工具**（参数 city=城市名），不要调 tool_search 也不要调别的工具——get_weather 一定可用。",
		"查一下北京的天气。")
	if err2 != nil {
		fmt.Printf("✗ Run 失败：%v\n", err2)
		os.Exit(1)
	}
	// 「越界直调」= get_weather 在 tool_search 把它解锁之前就被 emit（含跟 tool_search 同一批）。先检索、下一轮再调
	// 是合规调用，不该被拦；曾经只看「调没调过 get_weather」，模型守规矩先检索再调时被误判成漏防。
	rejection2, emittedWeatherDirect, rounds2 := h2.snapshot()
	fmt.Printf("  LLM 各轮 emit 的 tool_call: %v\n", rounds2)
	fmt.Printf("  解锁之前就直 emit get_weather: %v\n", emittedWeatherDirect)
	fmt.Printf("  框架返 [tool_not_revealed] 拦截: %v\n", rejection2)
	fmt.Printf("  实际执行的 deferred 工具: %v\n", rec2.list())
	answer2 := ""
	if final2 != nil {
		answer2 = final2.Content
	}
	fmt.Printf("  最终答案: %s\n", truncate(answer2, 80))
	switch {
	case rejection2:
		// LLM 越过 bind list 调了未揭示工具 → 框架拒执行返反馈 → fail-closed 真触发
		fmt.Println("  ✓ PASS：fail-closed 真实触发——未揭示工具被拒执行、返反馈引导重走 tool_search")
	case !emittedWeatherDirect:
		// LLM 守规矩没越界（用了 tool_search）——fail-closed 没机会触发，但披露机制正常
		fmt.Println("  ⊘ 未触发：qwen 守规矩走了 tool_search 没越界直调（fail-closed 由 mock 单测 TestToolSearch_FailClosed 确定性覆盖）")
	default:
		fmt.Println("  ✗ FAIL：LLM 直 emit 了 get_weather 但框架没拦截（fail-closed 漏防！）")
		pass = false
	}

	fmt.Println()
	fmt.Println(tr.Report())
	if pass {
		fmt.Println("=== 真 LLM toolsearch smoke 通过 ===")
		return
	}
	fmt.Println("=== 有 FAIL，见上 ===")
	os.Exit(1)
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len([]rune(s)) <= n {
		return s
	}
	return string([]rune(s)[:n]) + "…"
}
