package scenarios

import (
	"context"
	"fmt"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/tool"
)

// scenario_writing_team（ISSUE-72 R7 演示）：7 个写作类专家组成的扩展写作团队。
//
// 与 default 场景的区别：default 是 researcher/writer/critic 三人小组；本场景额外加
// coder / translator / summarizer / comparator，演示 description 自动注入后 planner
// 在更大池子里也能正确派发——加 / 删专家只动本文件，不动 PlannerPrompt。
//
// 适合演示：写技术博客（researcher + writer + critic 配合）、写代码教程（+ coder）、
// 中英对照（+ translator）、压缩长文（+ summarizer）、并排对比方案（+ comparator）。
func init() {
	Register("writing_team", buildWritingTeamScenario)
}

func buildWritingTeamScenario(m model.ChatModel, hooks hook.Hooks, extraOpts ...agent_v2.Option) Scenario {
	searchTool := tool.NewFuncTool(
		"search",
		"根据关键词检索资料",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"query": map[string]any{"type": "string"},
			},
			"required": []string{"query"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			q, _ := args["query"].(string)
			return tool.Success(fmt.Sprintf("[mock search] %q 的要点：A/B/C", q)), nil
		},
	)
	tk := tool.NewToolkit()
	tk.Register(searchTool)

	researcher := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("researcher"),
		agent_v2.WithDescription("查资料、列事实——必要时调用 search 工具检索"),
		agent_v2.WithSystemPrompt("你是资料调研员。收到子任务后，必要时调用 search 工具，最终输出结构化的资料要点。"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	writer := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("writer"),
		agent_v2.WithDescription("整理资料、写流畅的段落或教程章节"),
		agent_v2.WithSystemPrompt("你是文案作者。基于已有资料要点，写出流畅的段落或教程章节。"),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	critic := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("critic"),
		agent_v2.WithDescription("审查前序产出在事实/逻辑/结构上的问题，给出改进建议"),
		agent_v2.WithSystemPrompt("你是审稿人。指出前序产出在事实、逻辑、结构上的问题，给出改进建议。"),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	coder := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("coder"),
		agent_v2.WithDescription("写代码示例——基于资料/需求生成可运行的代码片段并简要说明"),
		agent_v2.WithSystemPrompt("你是代码工程师。基于上下文生成简短可运行的示例代码并配少量注释，不要过度解释。"),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	translator := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("translator"),
		agent_v2.WithDescription("中英互译——把段落或术语在中英之间高质量转换"),
		agent_v2.WithSystemPrompt("你是翻译员。把输入文本翻译为目标语言，保留术语原貌。无目标语言时默认中英互译。"),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	summarizer := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("summarizer"),
		agent_v2.WithDescription("摘要/提纯——把冗长资料压缩成 3-5 个关键 bullet"),
		agent_v2.WithSystemPrompt("你是摘要专家。把上文压成 3-5 条 bullet，保留关键事实和决策点，去掉冗余。"),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	comparator := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("comparator"),
		agent_v2.WithDescription("对比分析——把两份/多份内容并排对照，输出差异表"),
		agent_v2.WithSystemPrompt("你是对比分析员。把上文里的多份内容并排对照，按维度列差异和共性，输出表格或要点。"),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)

	planner := `你是动态重规划者。每次被调用时，会拿到原始目标和已完成步骤摘要，
负责输出"下一步要执行什么"的 JSON 数组（每次只 1 步，replan-每步 范式）。已完成的步骤不要重复输出；
如果目标已达成，输出空数组 []。

【输出格式】严格 JSON 数组，字段名 "desc" / "agent" / "refs"，不要 markdown 代码块、不要解释。

可选 agent 列表（含各自职责描述）会在 user 消息里给出。请按目标和已完成步骤从中选最合适的一个；
不要选列表外的名字。

规划规则（每次只输出 1 步）：
- 没有 executed → 给开局步：通常先 researcher 查资料，但若目标本身不需要查资料（如纯翻译/纯摘要）
  则可直接选对应专家
- 后续每步基于上一步产出，选最匹配的下一个 agent；可串接多个不同专家完成复杂目标
- 目标达成 → 输出 []

⚠️ refs 只引用**已完成步骤**（user 消息里看到的 [N] 列表）。不要在多步输出里让 step[1:] 引用同批
sibling step[0] —— PlanAgent 只跑 remaining[0]，多步纯属 token 浪费。每次输出 1 步即可。` + RefsPromptHelp

	return Scenario{
		Name:          "writing_team",
		Description:   "扩展写作团队（ISSUE-72 R7 演示）：7 个专家池，演示 description 自动注入派发",
		PlannerPrompt: planner,
		SubAgents: map[string]agent_v2.Agent{
			"researcher": researcher,
			"writer":     writer,
			"critic":     critic,
			"coder":      coder,
			"translator": translator,
			"summarizer": summarizer,
			"comparator": comparator,
		},
		Fallback: writer,
		DialogueScript: []string{
			`写一篇 Go goroutine 调度器入门博客`,
			`帮我把这段中文翻译成英文：xxx`,
			`帮我对比 sync.Map 和 RWMutex+map 的优劣`,
		},
		ReplanHint: "Planner 看到 7 个 agent 描述后按目标自动选最合适的链路",
	}
}
