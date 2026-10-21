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

// scenario_arbiter（方向 F）：多源对比 + 分歧仲裁。
// replan 价值：分歧出现时追加查证步骤；一致时直接 arbiter 收尾。
func init() {
	Register("arbiter", buildArbiterScenario)
}

func buildArbiterScenario(m model.ChatModel, hooks hook.Hooks, extraOpts ...agent_v2.Option) Scenario {
	// mock 三个"信息源"，对同一个问题给不同结论。
	sources := map[string]map[string]string{
		"go_routine_stack": {
			"source_a": "[mock A·官方 blog] Go 1.4 起 goroutine 初始栈 8KB",
			"source_b": "[mock B·StackOverflow] goroutine 初始栈是 2KB（这个是 1.4 之前的旧数据）",
			"source_c": "[mock C·runtime 源码] runtime/stack.go: _StackMin = 2048 但 newproc 实际分配 8192 起步",
		},
		"channel_close": {
			"source_a": "[mock A·Effective Go] 关闭已关闭 channel 会 panic",
			"source_b": "[mock B·中文社区] 关闭已关闭 channel 会 panic",
			"source_c": "[mock C·Go spec] 关闭已关闭 channel 会 panic",
		},
	}

	pickKey := func(query string) string {
		q := strings.ToLower(query)
		switch {
		case strings.Contains(q, "栈") || strings.Contains(q, "stack") || strings.Contains(q, "8kb") || strings.Contains(q, "2kb"):
			return "go_routine_stack"
		case strings.Contains(q, "channel") || strings.Contains(q, "关闭"):
			return "channel_close"
		default:
			return "go_routine_stack"
		}
	}

	mkSearchTool := func(name, sourceID string) *tool.FuncTool {
		return tool.NewFuncTool(
			name,
			fmt.Sprintf("从信息源 %s 检索关于 query 的资料。", sourceID),
			map[string]any{
				"type":       "object",
				"properties": map[string]any{"query": map[string]any{"type": "string"}},
				"required":   []string{"query"},
			},
			func(_ context.Context, args map[string]any) (*tool.Result, error) {
				q, _ := args["query"].(string)
				key := pickKey(q)
				if v, ok := sources[key][sourceID]; ok {
					return tool.Success(v), nil
				}
				return tool.Success(fmt.Sprintf("[mock %s] 未检索到 %q 的资料", sourceID, q)), nil
			},
		)
	}

	tkA := tool.NewToolkit()
	tkA.Register(mkSearchTool("search_a", "source_a"))
	tkB := tool.NewToolkit()
	tkB.Register(mkSearchTool("search_b", "source_b"))
	tkC := tool.NewToolkit()
	tkC.Register(mkSearchTool("search_c", "source_c"))

	searcherA := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("searcher_a"),
		agent_v2.WithDescription("信息源 A（官方文档）检索员，输出以 \"结论：\" 开头的 1-2 行结论"),
		agent_v2.WithSystemPrompt(`你是信息源 A（官方文档）的检索员。调用 search_a 工具拿数据，
输出 1-2 行结论，结论行必须以 "结论：" 开头。`),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tkA),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	searcherB := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("searcher_b"),
		agent_v2.WithDescription("信息源 B（社区资料）检索员，输出以 \"结论：\" 开头的 1-2 行结论"),
		agent_v2.WithSystemPrompt(`你是信息源 B（社区资料）的检索员。调用 search_b 工具拿数据，
输出 1-2 行结论，结论行必须以 "结论：" 开头。`),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tkB),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	searcherC := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("searcher_c"),
		agent_v2.WithDescription("信息源 C（源码 / spec）检索员，输出以 \"结论：\" 开头的 1-2 行结论"),
		agent_v2.WithSystemPrompt(`你是信息源 C（源码 / spec）的检索员。调用 search_c 工具拿数据，
输出 1-2 行结论，结论行必须以 "结论：" 开头。`),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tkC),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	arbiter := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("arbiter"),
		agent_v2.WithDescription("综合多信息源结论，开头一行必须 AGREE/DISAGREE，给最终答案"),
		agent_v2.WithSystemPrompt(`你是仲裁者。基于已查询的多个信息源结论：
- 若结论一致：直接给最终答案。
- 若有分歧：明确指出分歧点、说明哪一方更可信（一般是源码/spec > 官方文档 > 社区），给最终答案。
开头一行必须是 "AGREE" 或 "DISAGREE"。`),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)

	planner := `你是动态重规划者。任务是用多信息源核实事实，必要时追加第三源仲裁。

【输出格式】严格 JSON 数组，字段名 "desc" / "agent" / "refs"。

可选 agent 列表（含各自职责）由 user 消息动态注入。

规划规则（每次只输出 1 步，让 replan-每步 范式逐步展开 fan-in；searcher_a/b/c 名字唯一可直接用 string）：
- 没有 executed → 输出 [{searcher_a 查, refs:[]}]
- 上一步 searcher_a → 输出 [{searcher_b 查, refs:[]}]
- 上一步 searcher_b → 输出 [{arbiter 综合, refs:["searcher_a","searcher_b"]}]
- 上一步是 arbiter 且产出以 "AGREE" 开头 → 输出 []（收尾）
- 上一步是 arbiter 且产出以 "DISAGREE" 开头 → 输出 [{searcher_c 查源码, refs:["arbiter"]}]
- 上一步 searcher_c → 输出 [{arbiter 重新综合, refs:["searcher_a","searcher_b","searcher_c"]}]
- 已经做过 searcher_c 仍 DISAGREE → 输出 []（强制收尾，避免无限）

⚠️ arbiter 是 fan-in 节点，必须把所有相关 searcher 都列进 refs，否则 arbiter 看不到对应源的结论。` + RefsPromptHelp

	return Scenario{
		Name:          "arbiter",
		Description:   "多源对比：fan-in 节点靠 Planner 声明 refs 把所有 searcher 原文拉给 arbiter",
		PlannerPrompt: planner,
		SubAgents: map[string]agent_v2.Agent{
			"searcher_a": searcherA,
			"searcher_b": searcherB,
			"searcher_c": searcherC,
			"arbiter":    arbiter,
		},
		Fallback: arbiter,
		DialogueScript: []string{
			`Go goroutine 的初始栈大小到底是 2KB 还是 8KB？`,
			`关闭一个已经关闭的 channel 会发生什么？`,
		},
		ReplanHint: "源 A/B 分歧 → arbiter 输出 DISAGREE → replan 追加 source_c → 重新 arbiter",
	}
}
