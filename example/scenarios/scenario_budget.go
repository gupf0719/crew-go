package scenarios

import (
	"context"
	"fmt"
	"strings"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/tool"
)

// scenario_budget（方向 D）：约束求解 / 预算回退。
// replan 价值：超预算后回退/替换已规划步骤，diff 戏剧化（- 砍 + ~ 改 + + 加 替代）。
func init() {
	Register("budget", buildBudgetScenario)
}

func buildBudgetScenario(m model.ChatModel, hooks hook.Hooks, extraOpts ...agent_v2.Option) Scenario {
	// 三个 mock 工具：返回固定的"价格选项"，让 LLM 必须做权衡。
	flightSearch := tool.NewFuncTool(
		"flight_search",
		"按城市搜索机票，返回价格选项。",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"city":  map[string]any{"type": "string"},
				"class": map[string]any{"type": "string", "description": "economy / business"},
			},
			"required": []string{"city"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			class, _ := args["class"].(string)
			if class == "business" {
				return tool.Success(`[mock flight] 上海往返机票（商务舱）：
  - 国航 CA1500：3200 元
  - 东航 MU5800：2900 元`), nil
			}
			return tool.Success(`[mock flight] 上海往返机票（经济舱）：
  - 春秋 9C8800：800 元
  - 东航 MU5805：950 元`), nil
		},
	)
	hotelSearch := tool.NewFuncTool(
		"hotel_search",
		"按城市和等级搜索酒店，返回价格选项。",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"city":  map[string]any{"type": "string"},
				"level": map[string]any{"type": "string", "description": "luxury / mid / budget"},
				"days":  map[string]any{"type": "integer"},
			},
			"required": []string{"city", "level"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			level, _ := args["level"].(string)
			days := 3
			if d, ok := args["days"].(float64); ok && d > 0 {
				days = int(d)
			}
			switch strings.ToLower(level) {
			case "luxury":
				return tool.Success(fmt.Sprintf("[mock hotel] 五星：上海外滩茂悦 1500 元/晚 × %d = %d 元", days, 1500*days)), nil
			case "budget":
				return tool.Success(fmt.Sprintf("[mock hotel] 经济：如家精选 280 元/晚 × %d = %d 元", days, 280*days)), nil
			default:
				return tool.Success(fmt.Sprintf("[mock hotel] 中端：全季酒店 550 元/晚 × %d = %d 元", days, 550*days)), nil
			}
		},
	)
	activitySearch := tool.NewFuncTool(
		"activity_search",
		"按城市搜索活动/景点，返回价格选项。",
		map[string]any{
			"type":       "object",
			"properties": map[string]any{"city": map[string]any{"type": "string"}},
			"required":   []string{"city"},
		},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Success(`[mock activity] 上海推荐：
  - 迪士尼一日游：600 元
  - 黄浦江夜游：180 元
  - 豫园+城隍庙（免费）`), nil
		},
	)

	tk := tool.NewToolkit()
	tk.Register(flightSearch)
	tk.Register(hotelSearch)
	tk.Register(activitySearch)

	flight := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("flight"),
		agent_v2.WithDescription("搜机票（economy/business）并选定一个方案给出价格"),
		agent_v2.WithSystemPrompt(`你是机票搜索员。调用 flight_search 工具拿价格选项，
明确选定一个方案并给出价格（仅一个数字 + 单位"元"）。`),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	hotel := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("hotel"),
		agent_v2.WithDescription("搜酒店（luxury/mid/budget × 天数）并选定一个方案给出总价"),
		agent_v2.WithSystemPrompt(`你是酒店搜索员。调用 hotel_search 工具拿价格选项，
明确选定一个方案并给出价格（仅一个数字 + 单位"元"）。`),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	activity := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("activity"),
		agent_v2.WithDescription("搜活动/景点，选 1-2 个并给出总价"),
		agent_v2.WithSystemPrompt(`你是活动规划员。调用 activity_search 工具拿活动选项，
选 1-2 个并给出总价格（数字 + "元"）。`),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	budgeter := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("budgeter"),
		agent_v2.WithDescription("加总报价判断是否超预算（首行 TOTAL/BUDGET，次行 STATUS: WITHIN/OVER）"),
		agent_v2.WithSystemPrompt(`你是预算审计。基于已完成步骤的报价，加总判断是否超预算。
输出格式：
- 第一行 "TOTAL: <数字>元 / BUDGET: <数字>元"
- 第二行 "STATUS: WITHIN" 或 "STATUS: OVER by <差额>元"
- 若 OVER，给出建议（砍哪个 / 换便宜替代）`),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)

	planner := `你是动态重规划者。任务是在用户预算内规划一次出差/旅行。

【输出格式】严格 JSON 数组，字段名 "desc" / "agent" / "refs"。

可选 agent 列表（含各自职责）由 user 消息动态注入。

规划规则（每次只输出 1 步；replan-每步 范式让搜索→审计自然展开）：
- 没有 executed → 输出 [{flight, refs:[]}]
- 上一步 flight → 输出 [{hotel, refs:[]}]
- 上一步 hotel → 输出 [{activity, refs:[]}]
- 上一步 activity → 输出 [{budgeter 审计, refs:["flight","hotel","activity"]}]
  （budgeter 必须看到三者原文价格才能加总——refs 都指向已完成步骤）
- 上一步 budgeter 产出包含 "STATUS: WITHIN" → 输出 []（收尾）
- 上一步 budgeter 产出包含 "STATUS: OVER" → 必须重规划砍掉/降级：
    输出 [{hotel 砍预算后重搜, refs:[]}]，下一轮再 budgeter 用 string 自动取最新版本

不要一次规划多个 budgeter 调用。` + RefsPromptHelp

	return Scenario{
		Name:          "budget",
		Description:   "预算约束：超支后回退/替换已规划步骤，diff 出现 - / ~ / +",
		PlannerPrompt: planner,
		SubAgents: map[string]agent_v2.Agent{
			"flight":   flight,
			"hotel":    hotel,
			"activity": activity,
			"budgeter": budgeter,
		},
		Fallback: budgeter,
		DialogueScript: []string{
			`下周去上海出差 3 天，预算 2000 元，帮我规划机票+酒店+1 个活动`,
			`如果预算降到 1500 元呢？`,
		},
		ReplanHint: "首版超预算 → budgeter 提示 OVER → replan 改用 budget 酒店或砍 activity",
	}
}
