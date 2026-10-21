package agent_v2

import (
	"context"
)

// subAgentStoreScopeKey 是「这次调用发生在成员位置上」标记的 ctx key（见
// [WithSubAgentStoreScope]；框架自己不读，留给业务的 store 后端）。
type subAgentStoreScopeKey struct{}

// WithSubAgentStoreScope 标记 ctx 为"sub-agent 调用 scope"。
//
// **push 点收敛在 4 处**，业务和框架都不必再记一张清单：
//   - runMember（**6 个**编排调自己的成员——不含 Handoff，它的铸造点是下面单列的那条）
//   - runDecisionAgent（分类器 / 评估器 / 反馈教练）
//   - runNestedCall（嵌套调用的执行器：RunSubAgent 公开 helper + AgentAsTool + SkillTool fork）
//   - HandoffAgent 的转交铸造点
//
// 这个数曾经是 15，由注释里一句人肉数出来的「协议覆盖 15/15」维持——而 `5afb8eb`
// 那次加协议就漏了 ParallelAgent 和 Routing 的 classifier。哨兵
// TestMemberDispatch_AllAgentRunsGoThroughHelper 现在机械盯着「有没有第 5 处派发」。
//
// ⚠️ **框架自己不再读这个 flag**（2026-08）。它曾经守在 `commitFinal` 的 store.Put
// 前面——那道闸的本意是「让顶层唯一写入」，实际效果是**成员配了 WithStoreMemory 也一条
// 写不进去**。真正的撞键根因是键里没有写入者维，补上那一维之后闸就没有存在理由了
// （现在顶层和成员写的是**不同 ns**，本来就不重复）。
//
// flag 仍然 push（4 处派发点无条件 push），业务可以继续读它做自己的判断——但**别再照
// 下面那个「skip 让顶层唯一写入」的老 pattern 写**：那样做的净效果是把框架刚修好的
// 「成员的记忆能落盘」又抵消掉（实测：Parallel 两分支各写一条 → Put 调用 2 次、丢弃
// 2 次、落盘 0 条、Run.err=nil）。
//
// **协议层级局限** (ISSUE-84 audit MED-1 文档化): 当前协议是 boolean 信号, 多层
// 嵌套时 (顶层 → AgentAsTool sub1 → sub1 内 AgentAsTool sub2) 所有 sub-agent
// 都看到 `IsSubAgentStoreScope=true`, 业务无法区分"我是第几层 sub-agent"。
// 业务需"分层归类"时可用 `agent_v2.GetSession(ctx).ParentSession()` 数链长度判层级。
//
// **还有什么正当用法**：flag 说的是「这次调用发生在成员位置上」，业务可以据此给 sub-agent
// 的记忆挑一个不同的后端 / 不同的 TTL / 打个标记。**不建议**用它整个 skip——防重复写这件
// 事框架已经在 ns 那一维上解决了。
//
//	func (m *MyMem0Store) Put(ctx context.Context, ns store.Namespace, key string, value any, opts ...store.PutOption) error {
//	    if agent_v2.IsSubAgentStoreScope(ctx) {
//	        return m.client.AddWithTTL(ns, key, value, 24*time.Hour) // 成员的记忆留短一点
//	    }
//	    return m.client.Add(ns, key, value)
//	}
func WithSubAgentStoreScope(ctx context.Context, isSubAgent bool) context.Context {
	return context.WithValue(ctx, subAgentStoreScopeKey{}, isSubAgent)
}

// IsSubAgentStoreScope 查 ctx 是否标记为 sub-agent 调用 scope。
//
// ⚠️ **框架自己不读它**（见 [WithSubAgentStoreScope]）——曾经守在 commitFinal 的
// store.Put 前面，那道闸让成员的 WithStoreMemory 完全失效，已删。业务可以继续读。
//
// 返 false 当 ctx 未标记 (顶层 BasicAgent 调用 / 业务直接调 store.Put) 或显式标
// false。
func IsSubAgentStoreScope(ctx context.Context) bool {
	v, _ := ctx.Value(subAgentStoreScopeKey{}).(bool)
	return v
}
