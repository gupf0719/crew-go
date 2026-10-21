package agent_v2

import (
	"context"

	"crew-go/compose_v2"
	"crew-go/tool"
)

// ────────────────────────────────────────────────────────────────────────
// Phase 1: tool Categorizer 透传 helper.
//
// agent_v2.BasicAgent 在 Run / Resume 建图之后，把「这次 Run 派发的那份工具表」的类型查询经
// compose_v2.WithToolCategoryLookup 推进 ctx；agent_v2/basic_builder.go::toolsNode 在 fire OnToolStart 前
// 查它填 ti.Extra["category"]，observability 据此创 retrieval/embedding/guardrail span kind。
//
// 业务**不**直接调本 helper。业务工具实现 tool.Categorizer 即可，框架自动接管 trace。
// ────────────────────────────────────────────────────────────────────────

// pushToolCategorizerCtx 把派发工具表 tk 的类型查询推进 ctx：按工具名查到的是 tk 里这个名字下挂的那件
// **实例**，类型跟着实际派发的那件走。框架工具顶掉了业务同名工具时（WithAskUser / WithOutputSchema 等），
// 类型不套业务那件的（框架工具不标类型）。曾经在 Run 入口扫业务的 toolkit 建「名字 → 类型」表，框架
// 那件被派发时 trace 照样标成业务同名工具的类型（实测）。
//
// **恒推**（哪怕没有任何工具实现 Categorizer、tk 为 nil）：嵌套时子 agent 的 ctx 从调用方那里继承，
// 不推的话子 agent 的工具会按名字查到调用方的类型表。
//
// 查的是 OnToolStart 改名**之前**的名字：类型要在钩子之前就交给观测层（span 在 OnToolStart 里建）。钩子把
// 工具改名到另一件时，trace 上标的仍是原来那件的类型。
func pushToolCategorizerCtx(ctx context.Context, tk *tool.Toolkit) context.Context {
	return compose_v2.WithToolCategoryLookup(ctx, func(name string) string {
		if tk == nil {
			return ""
		}
		if c, ok := tk.Get(name).(tool.Categorizer); ok {
			return c.Category()
		}
		return ""
	})
}
