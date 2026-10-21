package compose_v2

import "context"

// ISSUE-70 Phase 2: ctx 上的 ns 透传机制。
//
// **Graph NS**（WithGraphNS / GetGraphNS）—— 当前 graph 整体所在的 ns 位置。
// 由 RunGraph / runGraphFromSuperstep / RunSubgraph 入口注入；节点闭包派生子
// ns 时取此值作 parent。业务自定义 NodeFunc 嵌套 sub-agent.Run 时也用此值算子 ns。
//
// 节点签名 NodeFunc(ctx, Channels) → ([]ChannelWrite, error) 不带 nodeID
// 参数——节点想知道自己 ns 必须通过 ctx，避免改 NodeFunc 签名破坏所有现有节点。
//
// **audit follow-up #6 fix**: 之前还有第二维度 `WithCurrentNS / GetCurrentNS /
// EnterChildNS`——godoc 承诺"业务 tool 算 interrupt_id 用 current ns", 但生产
// 0 caller (grep verify), ask_user_tool 等业务 tool 实际只用 GetGraphNS. 协议
// 从未真正启用, 删整套 currentNS 维度 + EnterChildNS helper 让 ns_ctx 真正
// 收敛到单维度. 业务想算 InterruptID 走 CurrentInterruptSite(ctx).InterruptID()
// 5 维 helper (compose_v2/interrupt.go).

type graphNSKey struct{}

// WithGraphNS 注入当前 graph 整体 ns 到 ctx。
//
// 由 RunGraph / runGraphFromSuperstep / RunSubgraph 入口调用，覆盖父 ctx 的
// graph ns（嵌套子图启动时本子图是新的 graph context）。
func WithGraphNS(ctx context.Context, ns string) context.Context {
	return context.WithValue(ctx, graphNSKey{}, ns)
}

// GetGraphNS 返回当前 graph 整体 ns。未注入时返回 ""（根 graph）。
func GetGraphNS(ctx context.Context) string {
	if v, ok := ctx.Value(graphNSKey{}).(string); ok {
		return v
	}
	return ""
}

// **历史删除**:
//   - WithCurrentNS / GetCurrentNS / EnterChildNS (audit follow-up #6): 0 production
//     caller, godoc 谎报 "业务 tool 算 interrupt_id 用 currentNS" 实际不用. 删整
//     套 currentNS 维度后业务想算 InterruptID 走 CurrentInterruptSite helper.
//   - EnterFanoutBranchNS (R32+ 视角 4): 生产 0 caller, 设计被 ParallelAgent JoinNS +
//     AddBranch panic 防重复名 + AgentAsTool/SkillTool 直传 hash(tc.ID) (ISSUE-78)
//     替代. CLAUDE.md "项目未上线不留兼容路径", 删干净比留死代码好。
