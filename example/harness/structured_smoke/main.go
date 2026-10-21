// structured_smoke 用**真 qwen**多场景验证结构化输出（WithOutputSchema）端到端：业务
// 声明一次 schema → 框架注册 final_output 工具（tool_choice=required 逼模型经它收尾）→ 校验
// → 最终答复即结构化 JSON。断言用确定性锚（数字 / 枚举成员 / 数组长度，不赌 LLM 措辞）+
// json.Unmarshal 成功。
//
// 4 场景：① 简单抽取 ② 嵌套 object + 数组 ③ 枚举 + 布尔 + 可选字段 ④ 带业务 toolkit（模型
// 先调工具查数据、再 final_output 结构化收尾——验证场景 A 多轮 + tool_choice=required 兼容）。
//
// 跑（项目根 .env 需 QWEN_API_KEY；~8-12 次调用，约 1-2 分钟）：
//
//	go run ./example/harness/structured_smoke
//
// model 用 llmtrace.NewQwenFallback() 的 qwen 备选链（候选取 qwen.TestModelCandidates，
// 带日期的固定快照在前）——某个候选免费额度耗尽时 alwaysFailover 自动切下一个，结束
// 打印 tracer.Report() 透明核实哪个模型真服务 + token 消耗。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"crew-go/agent_v2"
	"crew-go/example/envloader"
	"crew-go/example/llmtrace"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// tracer 是 llmtrace fallback 链的追踪器（main 赋值）——runStructured 把它 chain 进 WithHooks，
// 让 tracer.Report() 能打印哪个 qwen 模型真服务 + token 消耗（fallback 静默切模型时透明可核）。
var tracer *llmtrace.Tracer

// finalOutputCounter 观测 final_output 是否真被调（验证结构化经工具收尾而非自由文本）。
type finalOutputCounter struct {
	hook.NoOpHooks
	calls int
}

func (c *finalOutputCounter) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	if in != nil && in.Name == agent_v2.FinalOutputToolName {
		c.calls++
	}
	return ctx, nil
}

// runStructured 跑一个结构化输出 agent，返回最终 Content + final_output 调用次数。tk 非 nil
// 时挂上业务 toolkit（跟 WithOutputSchema 的先后无关：final_output 由框架每次 Run 注入）。
func runStructured(m model.ChatModel, name string, sc map[string]any, userMsg string, tk *tool.Toolkit) (string, int) {
	counter := &finalOutputCounter{}
	opts := []agent_v2.Option{
		agent_v2.WithName(name),
		agent_v2.WithModel(m),
		agent_v2.WithSystemPrompt("你是信息抽取助手，从用户文本中抽取结构化信息。需要外部数据时先调工具查。"),
		agent_v2.WithHooks(hook.NewChainHooks(counter, tracer.Hooks())), // counter + llmtrace 追踪
		agent_v2.WithMaxRounds(6),
	}
	if tk != nil {
		opts = append(opts, agent_v2.WithToolkit(tk))
	}
	opts = append(opts, agent_v2.WithOutputSchema(name, sc))

	ag := agent_v2.NewBasicAgent(opts...)
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage(userMsg))
	if err != nil {
		log.Fatalf("[%s] Run 出错: %v", name, err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		log.Fatalf("[%s] 无最终消息", name)
	}
	if counter.calls == 0 {
		log.Fatalf("[%s] final_output 未被调用（结构化应经工具收尾）", name)
	}
	if !json.Valid([]byte(final.Content)) {
		log.Fatalf("[%s] 最终答复非合法 JSON: %q", name, final.Content)
	}
	return final.Content, counter.calls
}

func main() {
	envloader.Load()
	// llmtrace qwen fallback 链（候选取 qwen.TestModelCandidates）——额度
	// 耗尽/故障 alwaysFailover 自动切；Tracer 透明追踪哪个模型真服务 + token 消耗（见 llmtrace）。
	m, tr := llmtrace.NewQwenFallback()
	tracer = tr

	part1Simple(m)
	part2NestedArray(m)
	part3EnumBoolOptional(m)
	part4WithToolkit(m)

	fmt.Println("\n=== 全部 4 场景通过 ===")
	fmt.Println(tracer.Report()) // 透明追踪：哪个 qwen 模型真服务 + token 消耗
}

// Part 1：简单抽取——name/age/city，验证 A1 合成 + 基础类型。
func part1Simple(m model.ChatModel) {
	fmt.Println("=== Part 1：简单抽取 ===")
	sc := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name": map[string]any{"type": "string", "description": "人名"},
			"age":  map[string]any{"type": "integer", "description": "年龄"},
			"city": map[string]any{"type": "string", "description": "所在城市"},
		},
		"required": []string{"name", "age", "city"},
	}
	content, calls := runStructured(m, "person", sc, "张三今年 30 岁，住在北京。", nil)
	fmt.Printf("final_output×%d  Content: %s\n", calls, content)

	var p struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
		City string `json:"city"`
	}
	mustUnmarshal(content, &p, "person")
	assert(p.Age == 30, "age=%d want 30", p.Age)
	assert(strings.Contains(p.Name, "张三"), "name=%q 应含 张三", p.Name)
	assert(strings.Contains(p.City, "北京"), "city=%q 应含 北京", p.City)
	fmt.Println("PASS: age/name/city 锚点命中")
}

// Part 2：嵌套 object + 数组——验证结构感知的深层抽取。
func part2NestedArray(m model.ChatModel) {
	fmt.Println("\n=== Part 2：嵌套 object + 数组 ===")
	sc := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"customer": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name": map[string]any{"type": "string", "description": "客户名"},
				},
				"required": []string{"name"},
			},
			"items": map[string]any{
				"type":        "array",
				"description": "商品清单",
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"name":     map[string]any{"type": "string", "description": "商品名"},
						"quantity": map[string]any{"type": "integer", "description": "数量"},
					},
					"required": []string{"name", "quantity"},
				},
			},
			"total": map[string]any{"type": "integer", "description": "总价（元）"},
		},
		"required": []string{"customer", "items", "total"},
	}
	content, calls := runStructured(m, "order", sc,
		"李四订购了 3 台 iPhone 和 2 个耳机，总价 30000 元。", nil)
	fmt.Printf("final_output×%d  Content: %s\n", calls, content)

	var o struct {
		Customer struct {
			Name string `json:"name"`
		} `json:"customer"`
		Items []struct {
			Name     string `json:"name"`
			Quantity int    `json:"quantity"`
		} `json:"items"`
		Total int `json:"total"`
	}
	mustUnmarshal(content, &o, "order")
	assert(strings.Contains(o.Customer.Name, "李四"), "customer.name=%q 应含 李四", o.Customer.Name)
	assert(len(o.Items) == 2, "items 长度=%d want 2", len(o.Items))
	assert(o.Total == 30000, "total=%d want 30000", o.Total)
	fmt.Println("PASS: 嵌套 customer + 数组 items(2) + total 锚点命中")
}

// Part 3：枚举 + 布尔 + 可选字段——验证 enum/bool 类型 + required 外的可选字段。
func part3EnumBoolOptional(m model.ChatModel) {
	fmt.Println("\n=== Part 3：枚举 + 布尔 + 可选字段 ===")
	sc := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"title":    map[string]any{"type": "string", "description": "任务标题"},
			"priority": map[string]any{"type": "string", "enum": []any{"low", "medium", "high"}, "description": "优先级"},
			"done":     map[string]any{"type": "boolean", "description": "是否完成"},
			"assignee": map[string]any{"type": "string", "description": "负责人（可选）"},
		},
		"required": []string{"title", "priority", "done"},
	}
	content, calls := runStructured(m, "task", sc,
		"高优先级任务：修复登录 bug，已经完成了，负责人是王五。", nil)
	fmt.Printf("final_output×%d  Content: %s\n", calls, content)

	var t struct {
		Title    string `json:"title"`
		Priority string `json:"priority"`
		Done     bool   `json:"done"`
		Assignee string `json:"assignee"`
	}
	mustUnmarshal(content, &t, "task")
	assert(t.Priority == "high", "priority=%q want high", t.Priority)
	assert(t.Done, "done=%v want true", t.Done)
	assert(t.Title != "", "title 不应为空")
	fmt.Printf("PASS: priority=high(枚举) + done=true(布尔) + assignee=%q(可选) 命中\n", t.Assignee)
}

// Part 4：带业务 toolkit（场景 A）——模型先调 lookup_price 查价、再 final_output 结构化收尾。
// 验证 tool_choice=required 下业务工具多轮 + final_output 收尾共存；结构化结果引用工具返回值。
func part4WithToolkit(m model.ChatModel) {
	fmt.Println("\n=== Part 4：带业务 toolkit（场景 A 多轮）===")
	const fixedPrice = 8888
	lookup := tool.NewFuncTool("lookup_price", "查询指定商品的当前单价（元）",
		map[string]any{
			"type":       "object",
			"properties": map[string]any{"product": map[string]any{"type": "string", "description": "商品名"}},
			"required":   []string{"product"},
		},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			return tool.Success(fmt.Sprintf(`{"price": %d}`, fixedPrice)), nil
		})
	tk := tool.NewToolkit()
	tk.Register(lookup)

	sc := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"product": map[string]any{"type": "string", "description": "商品名"},
			"price":   map[string]any{"type": "integer", "description": "从 lookup_price 查到的单价"},
		},
		"required": []string{"product", "price"},
	}
	content, calls := runStructured(m, "quote", sc,
		"帮我查一下 iPhone 的价格，然后给我结构化报价。", tk)
	fmt.Printf("final_output×%d  Content: %s\n", calls, content)

	var q struct {
		Product string `json:"product"`
		Price   int    `json:"price"`
	}
	mustUnmarshal(content, &q, "quote")
	assert(strings.Contains(q.Product, "iPhone"), "product=%q 应含 iPhone", q.Product)
	assert(q.Price == fixedPrice, "price=%d want %d（应引用工具返回值）", q.Price, fixedPrice)
	fmt.Printf("PASS: 模型调 lookup_price 查价 → final_output 结构化 price=%d（工具值）命中\n", q.Price)
}

func mustUnmarshal(content string, v any, name string) {
	if err := json.Unmarshal([]byte(content), v); err != nil {
		log.Fatalf("[%s] unmarshal 失败: %v (content=%q)", name, err, content)
	}
}

func assert(ok bool, format string, args ...any) {
	if !ok {
		log.Fatalf("FAIL: "+format, args...)
	}
}
