package compose_v2

import "context"

// GraphAgent 显式编排路径的 node 组件类型透传 (Phase 2 探索).
//
// **背景**: BasicAgent/ReAct 路径外部能力统一是 tool, Phase 1 tool.Categorizer 已让
// tool span 标 retrieval/embedding/guardrail kind. 但 GraphAgent 显式编排
// 路径下业务在自定义 NodeFunc 里**直接**调组件 (不经 LLM tool_call), 框架默认把这些
// node 统一 trace 成 "node" kind — 标不了组件类型.
//
// WithComponentKind 让 graph 执行引擎在 fire OnNodeStart **之前** push node 声明的组件
// 类型 (来自 AddNode 的组件 option), observability OnNodeStart/OnNodeEnd 读它创对应 kind
// 的 node span (retrieval/embedding/guardrail) 而非通用 node — 把组件类型显式标到
// node span，对齐 langfuse OBSERVATION_TYPE.
//
// **跟 WithGraphNS 同款 ctx 透传范式** (见 ns_ctx.go): NodeFunc 签名不带组件类型参数,
// 走 ctx 透传避免改 NodeFunc / OrchHooks 签名破坏所有现有节点. OnNodeStart 在 NodeFunc
// 之前 fire (graph_loop.go), 所以组件类型必须在 fire 前由引擎 push (业务在 NodeFunc 内
// push 已太晚, node span 那时已创).

type componentKindKey struct{}

// WithComponentKind 注入当前 node 的组件类型到 ctx (retrieval/embedding/guardrail).
// 由 graph 执行引擎在 fire OnNodeStart 前调用 (基于 AddNode 的组件声明); 同一 node 执行
// 周期内 OnNodeStart / OnNodeEnd 共享此 ctx, 保 start/end span kind 一致.
func WithComponentKind(ctx context.Context, kind string) context.Context {
	return context.WithValue(ctx, componentKindKey{}, kind)
}

// GetComponentKind 返回当前 node 声明的组件类型. 未声明时返 "" (observability 走默认 node kind).
func GetComponentKind(ctx context.Context) string {
	if v, ok := ctx.Value(componentKindKey{}).(string); ok {
		return v
	}
	return ""
}
