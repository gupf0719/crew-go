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

// scenario_fault（方向 C）：故障诊断，每步发现决定下一步查哪个组件。
// replan 价值：诊断路径事前不可预测——日志说 DB 错才查表，表错才查 schema 等。
func init() {
	Register("fault", buildFaultScenario)
}

func buildFaultScenario(m model.ChatModel, hooks hook.Hooks, extraOpts ...agent_v2.Option) Scenario {
	// 三个 mock 工具：根据 endpoint/table 名返回事先编排好的"故障线索"。
	readLog := tool.NewFuncTool(
		"read_log",
		"读取指定 endpoint 最近的错误日志，返回 stack trace 摘要。",
		map[string]any{
			"type":       "object",
			"properties": map[string]any{"endpoint": map[string]any{"type": "string"}},
			"required":   []string{"endpoint"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			ep, _ := args["endpoint"].(string)
			if strings.Contains(ep, "order") {
				return tool.Success(`[mock log] 2026-04-18 10:23:15 ERROR /api/order
panic: ERROR 1054 (42S22): Unknown column 'shipping_addr' in 'field list'
at: db/orders.go:88 (*OrderRepo).Insert
建议：检查 orders 表 schema`), nil
			}
			return tool.Success(fmt.Sprintf("[mock log] %s 最近无错误日志", ep)), nil
		},
	)
	queryDB := tool.NewFuncTool(
		"query_db",
		"对指定表执行 DESCRIBE，返回字段列表。",
		map[string]any{
			"type":       "object",
			"properties": map[string]any{"table": map[string]any{"type": "string"}},
			"required":   []string{"table"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			t, _ := args["table"].(string)
			if t == "orders" {
				return tool.Success(`[mock db] DESCRIBE orders:
  id           bigint    PRIMARY KEY
  user_id      bigint    NOT NULL
  total_cents  bigint    NOT NULL
  created_at   datetime  NOT NULL
（未发现 shipping_addr 字段——schema 与代码期望不一致）`), nil
			}
			return tool.Success(fmt.Sprintf("[mock db] 表 %s 不存在", t)), nil
		},
	)
	readCode := tool.NewFuncTool(
		"read_code",
		"读取代码文件并返回相关片段。",
		map[string]any{
			"type":       "object",
			"properties": map[string]any{"path": map[string]any{"type": "string"}},
			"required":   []string{"path"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			p, _ := args["path"].(string)
			if strings.Contains(p, "orders.go") {
				return tool.Success(`[mock code] db/orders.go:80-95
func (r *OrderRepo) Insert(ctx, o *Order) error {
    return r.db.ExecContext(ctx,
      "INSERT INTO orders (user_id, total_cents, shipping_addr, created_at) VALUES (?,?,?,?)",
      o.UserID, o.TotalCents, o.ShippingAddr, o.CreatedAt)
}
（代码引用了 shipping_addr 但 schema 没有该字段）`), nil
			}
			return tool.Success(fmt.Sprintf("[mock code] 文件 %s 内容略", p)), nil
		},
	)

	tk := tool.NewToolkit()
	tk.Register(readLog)
	tk.Register(queryDB)
	tk.Register(readCode)

	logInspector := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("log_inspector"),
		agent_v2.WithDescription("读日志，定位故障类型为 DB / 代码 / 配置 之一"),
		agent_v2.WithSystemPrompt(`你是日志检查员。收到任务后调用 read_log 工具拿日志，
最后明确说出"故障定位线索"——是 DB / 代码 / 配置 哪一类。`),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	dbInspector := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("db_inspector"),
		agent_v2.WithDescription("DESCRIBE 表，检查 schema 是否符合代码期望"),
		agent_v2.WithSystemPrompt(`你是 DBA。收到任务后调用 query_db 工具检查表结构，
明确说出 "schema 是否匹配代码期望"。`),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	codeInspector := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("code_inspector"),
		agent_v2.WithDescription("读代码，找代码与 DB schema 不一致的具体位置"),
		agent_v2.WithSystemPrompt(`你是代码检查员。调用 read_code 读取相关文件，
找出代码与 DB schema 不一致的具体位置。`),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)
	fixer := agent_v2.NewBasicAgent(withExtra([]agent_v2.Option{
		agent_v2.WithName("fixer"),
		agent_v2.WithDescription("基于已知线索写最小修复方案（migration 或代码 patch）"),
		agent_v2.WithSystemPrompt(`你是修复方案撰写者。基于已有诊断信息，给出最小修复方案
（如：写一段 ALTER TABLE migration、或修改代码删除字段引用）。要给出可执行的具体步骤。`),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(hooks),
	}, extraOpts)...)

	planner := `你是动态重规划者。任务是协调诊断 agent 排查线上故障。

【输出格式】严格 JSON 数组，字段名 "desc" / "agent" / "refs"，不要 markdown、不要解释。

可选 agent 列表（含各自职责）由 user 消息动态注入。

规划规则：
- 没有 executed → 仅输出 [{log_inspector 看日志, refs:[]}]
- 上一步 log_inspector 暗示"DB" → 输出 [{db_inspector 查相关表, refs:["log_inspector"]}]
- 上一步 db_inspector 发现 schema 不一致 → 输出 [{code_inspector 看代码, refs:["db_inspector"]}]
- 上一步 code_inspector → 输出 [{fixer 给修复方案, refs:["log_inspector","db_inspector","code_inspector"]}]
- 上一步 db_inspector 一切正常 → 输出 [{code_inspector 看代码, refs:["log_inspector"]}]
- 上一步 fixer 已给方案 → 输出 []

每次只规划接下来 1-2 步，等结果再 replan。` + RefsPromptHelp

	return Scenario{
		Name:          "fault",
		Description:   "故障诊断：每步发现决定下一步查哪个组件，路径事前不可知",
		PlannerPrompt: planner,
		SubAgents: map[string]agent_v2.Agent{
			"log_inspector":  logInspector,
			"db_inspector":   dbInspector,
			"code_inspector": codeInspector,
			"fixer":          fixer,
		},
		Fallback: fixer,
		DialogueScript: []string{
			`接口 /api/order POST 返回 500，帮我定位并给修复方案`,
		},
		ReplanHint: "log → 提示 DB → 追加 db_inspector → 发现 schema 不匹配 → 追加 code_inspector + fixer",
	}
}
