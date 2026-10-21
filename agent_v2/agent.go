package agent_v2

import (
	"context"

	"crew-go/schema"
)

// Agent 是所有 agent 类型实现的接口。
//
// 单接口对齐 ISSUE-71 决策——Run 入口判断嵌入运行 vs 独立运行（v2 通过 ctx
// `compose_v2.WithGraphNS(ctx, JoinNS(parentNS, "<childName>"))` 注入位置;
// v1 `EnclosingParent` ctx-value 协议已删跟 channel-based 模型对齐），不再分
// NestableAgent / Agent 双接口。
type Agent interface {
	// Run 执行 agent 并返回本次产生的事件增量（不是 session 全量历史）。
	// 用 FinalMessage(events) 提取最终答案消息。
	//
	// session 的 RunID 是任务的身份：在一个正在执行的任务里（节点体、钩子、嵌套调用的被调方……）
	// 它必须属于那个任务，对不上就在入口拒绝；nil 时补一个属于所在任务的。本包 9 个编排都这样做，
	// 业务自己实现 Agent 时在 Run 入口调 [EnterRun] 就是同一套判据（`a2a.RemoteAgent` 就是
	// 这么接的）。要开一个独立的任务，调用方先 [DetachTask]。
	Run(ctx context.Context, session *RunSession, input *schema.Message, opts ...RunOption) ([]*AgentEvent, error)

	// Resume 续跑被 InterruptError 中断的 Run。从 ctx 取 Checkpointer 加载 cp。
	// Action 决定 Continue / Replan / Abort 三态。只在顶层调：在一个正在执行的任务里续跑别的任务
	// 先 [DetachTask]。
	Resume(ctx context.Context, sessionID string, opts ...ResumeOption) ([]*AgentEvent, error)

	// Name 返回 agent 名（用于 hook AgentInfo + 日志 + cp）。
	Name() string

	// Description 返回 agent 职责的一句话描述。
	//
	// 用途：
	//   - LLMPlanner / SupervisorAgent 等编排自动从 sub-agent 池子抽 description
	//     拼进 planner system prompt，让 LLM 按能力选择 sub-agent
	//   - AgentAsTool 包装时作为 tool description，父 LLM 通过 tool schema
	//     看到能力清单不必在父 prompt 手抄
	//
	// 空字符串 = "未提供描述"——LLMPlanner 等消费者跳过此 agent 描述行
	// （不强制 panic 给 caller 渐进迁移空间）。
	Description() string
}

// ResumeAction 是 Resume 操作的三态枚举（对照 compose/agent.ResumeAction）。
type ResumeAction int

const (
	// ResumeActionContinue 续跑（默认）：用 UserInputs 答 ask_user 后继续从中断点跑
	ResumeActionContinue ResumeAction = iota

	// ResumeActionReplan 重规划：把 user 答案喂回 Planner 让它重新规划剩余步骤
	// （典型场景：用户中途说"换个方向"）。
	//
	// **实际支持矩阵**（audit Phase 5.2 Bug #3 收紧后 commit c213263 + R21 audit
	// MED-1 fix commit 31660b1 GraphAgent 透传）：
	//   - PlanAgent       ✅ 主流场景——LLMPlanner 通过 GetResumeUserText
	//                        消费 user 文本主动重规划
	//   - BasicAgent      ✅（透传）——内嵌 sub-agent（如 PlanAgent 包成 tool）
	//                        会自然消费 ResumeContext.UserInputs
	//   - GraphAgent      ✅（透传）——R21 audit MED-1 fix 后业务 NodeFunc 内若
	//                        嵌 BasicAgent + skill，Replan 透传给底层（不再 fail-fast）
	//   - 其余 6 编排     ❌ fail-fast `return error "<type> agent %q:
	//     (Sequential / Parallel /    ResumeActionReplan not supported"`
	//      Loop / Routing / Reflect /
	//      Handoff)
	//
	// caller 区分：Continue 把 text 配 lastInterruptID 走 `WithResumeUserInputs`（寻址型，
	// 答某个具体提问）；Replan 把 text 走 `WithResumeUserText`（非寻址型，planner 读它
	// 重新规划）。**两条独立通道，别再用假 key 把后者塞进前者。**
	//
	// ⚠️ **原先有个「Replan 不能与 ask_user pending 并存」的坑，已随通道拆分消失**：
	// 那时 Replan 的文本只能拿假 key（`"_replan_"`）挤进按 ID 寻址的答案 map，而它
	// 不匹配 ask_user 自动算的 stableID ⟹ ask_user 取不到自己的答案、反复重抛中断
	// **死循环**。当时记的长期修法是「Replan 路径主动 clearInterrupts」——没走那条，
	// 走的是把非寻址型负载拆去独立通道（`WithResumeUserText`），假 key 整个不再需要。
	ResumeActionReplan

	// ResumeActionAbort 终止：清掉 cp 退出（用户主动取消任务）。所有 agent 支持。
	// Resume 返回 (nil, nil) 让 caller 区分于"自然跑完"（caller 显式发 aborted 事件）。
	ResumeActionAbort
)

// String 返人类可读名。
func (a ResumeAction) String() string {
	switch a {
	case ResumeActionContinue:
		return "continue"
	case ResumeActionReplan:
		return "replan"
	case ResumeActionAbort:
		return "abort"
	default:
		return "unknown"
	}
}
