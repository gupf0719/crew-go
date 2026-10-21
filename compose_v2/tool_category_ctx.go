package compose_v2

import "context"

// ────────────────────────────────────────────────────────────────────────
// Phase 1: tool category ctx 透传协议
//
// 业务自定义 tool 实现 tool.Categorizer 接口声明自己是 retrieval/embedding/
// guardrail 哪一类. framework 把 name→category lookup 函数 push 到
// ctx, toolsNode fire OnToolStart 之前用 lookup 填 ti.Extra["category"],
// observability hook 读 Extra 决定 span kind.
//
// 机制: tool.Categorizer 接口 + framework 自动透传 ctx → ti.Extra
//
// 协议:
//   - business 自定义 tool 实现 tool.Categorizer 接口 (可选, 不实现走默认 tool kind)
//   - agent_v2.BasicAgent.Run/Resume 建图之后按这次 Run 派发的那份工具表推 lookup（查到的是实际
//     派发的那件实例），WithToolCategoryLookup push ctx（恒推，免得嵌套子 agent 继承调用方的）
//   - agent_v2/basic_builder.go::toolsNode 在 OnToolStart 之前 GetToolCategoryLookup
//     查 ti.Name 对应 category 填 ti.Extra["category"]
//   - observability/hooks.go::OnToolStart 读 ti.Extra["category"] 路由 spanKind
// ────────────────────────────────────────────────────────────────────────

// toolCategoryLookupKey ctx 键.
type toolCategoryLookupKey struct{}

// WithToolCategoryLookup 注入 tool name → category lookup 函数到 ctx.
//
// 业务**不**直接调本 helper, 由 agent_v2.BasicAgent.Run/Resume 建图之后按派发工具表
// 自动 push. lookup 函数对未知 tool 应返空字符串.
//
// fn==nil 时 no-op 返原 ctx (业务无 Categorizer tool 的零开销路径).
func WithToolCategoryLookup(ctx context.Context, fn func(toolName string) string) context.Context {
	if fn == nil {
		return ctx
	}
	return context.WithValue(ctx, toolCategoryLookupKey{}, fn)
}

// GetToolCategoryLookup 从 ctx 取 lookup 函数. 未注入返 nil.
//
// caller (agent_v2/basic_builder.go::toolsNode) 调用前应 nil check:
//
//	if lookup := compose_v2.GetToolCategoryLookup(ctx); lookup != nil {
//	    if cat := lookup(ti.Name); cat != "" { ti.Extra["category"] = cat }
//	}
func GetToolCategoryLookup(ctx context.Context) func(toolName string) string {
	if fn, ok := ctx.Value(toolCategoryLookupKey{}).(func(toolName string) string); ok {
		return fn
	}
	return nil
}
