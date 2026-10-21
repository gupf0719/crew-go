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

// scenario_drill（方向 E）：codebase 递归钻取，深度事前不定。
// replan 价值：根据当前理解决定下一个钻取点，可能是更深一层 dir 也可能切换文件。
func init() {
	Register("drill", buildDrillScenario)
}

func buildDrillScenario(m model.ChatModel, hooks hook.Hooks, extraOpts ...agent_v2.Option) Scenario {
	// mock 一个微型 repo 结构。
	tree := map[string]string{
		"/":              "README.md  service/  pkg/  cmd/",
		"/service":       "order/  user/  payment/",
		"/service/order": "handler.go  repo.go  README.md",
		"/pkg":           "log/  cache/  middleware/",
		"/cmd":           "server/  worker/",
	}
	files := map[string]string{
		"/README.md":                `# AcmeShop 后端\n核心业务在 service/，工具库在 pkg/。order 模块最复杂，建议优先看。`,
		"/service/order/README.md":  `# Order 模块\nhandler.go: HTTP 路由层；repo.go: 数据库层。\n注意 repo.go 里 Insert 方法有性能争议（N+1 查询）。`,
		"/service/order/handler.go": `package order\nfunc (h *Handler) Create(c *gin.Context) { ... 调用 repo.Insert ... }`,
		"/service/order/repo.go":    `package order\nfunc (r *Repo) Insert(ctx, o *Order) {\n  for _, item := range o.Items { db.Exec("INSERT ...") }  // N+1\n}`,
	}

	listDir := tool.NewFuncTool(
		"list_dir",
		"列出指定目录下的子项。",
		map[string]any{
			"type":       "object",
			"properties": map[string]any{"path": map[string]any{"type": "string"}},
			"required":   []string{"path"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			p, _ := args["path"].(string)
			p = "/" + strings.TrimPrefix(strings.TrimSuffix(p, "/"), "/")
			if p == "//" {
				p = "/"
			}
			if v, ok := tree[p]; ok {
				return tool.Success(fmt.Sprintf("[mock dir] %s:\n  %s", p, v)), nil
			}
			return tool.Success(fmt.Sprintf("[mock dir] %s 不存在或未被 mock", p)), nil
		},
	)
	readFile := tool.NewFuncTool(
		"read_file",
		"读取指定文件内容。",
		map[string]any{
			"type":       "object",
			"properties": map[string]any{"path": map[string]any{"type": "string"}},
			"required":   []string{"path"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			p, _ := args["path"].(string)
			if v, ok := files[p]; ok {
				return tool.Success(fmt.Sprintf("[mock file] %s:\n%s", p, v)), nil
			}
			return tool.Success(fmt.Sprintf("[mock file] %s 不存在或未被 mock", p)), nil
		},
	)

	tk := tool.NewToolkit()
	tk.Register(listDir)
	tk.Register(readFile)

	overview := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("overview"),
		agent_v2.WithDescription("看 / 和 README 给整体概览并指明下一个钻取目标路径"),
		agent_v2.WithSystemPrompt(`你是 codebase 概览员。调用 list_dir + read_file 工具看根目录和 README，
输出 1-3 行总结，并明确指出"哪个子模块最值得深入"（精确路径）。`),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	driller := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("driller"),
		agent_v2.WithDescription("钻取指定子模块（任务里指定路径），输出总结并指出是否还要继续"),
		agent_v2.WithSystemPrompt(`你是模块钻取员。你的任务里会指定一个目标路径，
调用 list_dir 看目录、调用 read_file 看 README/关键文件，
输出 2-4 行总结，并明确指出"还有哪个文件/子目录值得继续钻取"或 "已足够，不需要继续"。`),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	summarizer := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("summarizer"),
		agent_v2.WithDescription("基于 overview + 所有 driller 输出，写最终 3-5 行 repo 总结"),
		agent_v2.WithSystemPrompt(`你是最终总结者。基于已有的 overview 和 driller 输出，
写一段 3-5 行的总结：repo 主要做什么、核心模块、值得注意的设计/问题。`),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)

	planner := `你是动态重规划者。任务是带用户递归钻取 codebase，深度根据每步发现动态决定。

【输出格式】严格 JSON 数组，字段名 "desc" / "agent" / "refs"。

可选 agent 列表（含各自职责）由 user 消息动态注入。

规划规则：
- 没有 executed → 输出 [{overview 概览整个 repo, refs:[]}]
- 上一步 overview → 输出 [{driller 钻取 /xxx, refs:["overview"]}]
- 上一步是 driller 还要继续 → 输出 [{driller 钻取 /yyy, refs:["driller"]}]（string 取最近 driller）
- 上一步 driller 产出包含"已足够" 或 已经钻取了 3 次以上 → 输出 [{summarizer 写总结, refs:[0,1,2,3,...]}]
  （summarizer 要看 overview + 所有 driller 的全部历史，**必须用 int 列出所有 [N]**，不能用 string）
- 上一步是 summarizer → 输出 []

每次只规划接下来 1 步，钻取深度由 driller 自己判断。` + RefsPromptHelp

	return Scenario{
		Name:          "drill",
		Description:   "codebase 递归钻取：根据每步发现决定下个钻取点，深度事前不知",
		PlannerPrompt: planner,
		SubAgents: map[string]agent_v2.Agent{
			"overview":   overview,
			"driller":    driller,
			"summarizer": summarizer,
		},
		Fallback: summarizer,
		DialogueScript: []string{
			`帮我看下这个 repo 主要在干嘛，重点说复杂度高的模块`,
		},
		ReplanHint: "overview 指 service/ → driller 进 service/ → 指 order/ → driller 进 order/ → 总结",
	}
}
