// Package agent_v2 是基于 compose_v2 channel-based graph 引擎的高层 agent
// 编排包，提供 Agent 接口 + BasicAgent / PlanAgent / AgentAsTool + 4 层 context
// compaction + RunSession/AgentEvent 审计流。对标现有 agent 包，但底层 graph
// 引擎切到 compose_v2（channels + ns 物理分离 cp + GraphID 防串台）。
//
// 包依赖：
//
//	schema ← model/tool/hook ← compose_v2 ← agent_v2
//
// 不依赖 compose 包——agent_v2 是 compose_v2 上的对等替代。
package agent_v2

import (
	"crew-go/schema"
)

// AgentEvent 是 agent 执行历史的单位——LLM 的最终产出 / 中断 等关键节点
// 被包装成 AgentEvent 存到 RunSession 提供多轮记忆 + 跨嵌套审计追踪。
//
// ⚠️ **tool 结果不进 events**：生产路径上所有 AddEvent 写的都是 Output / Exit 的 assistant
// 消息（BasicAgent 的 input 与 commitFinal、各编排的 input 与 final），零处写 role=tool。
// 所以从 events 重建 prior 时，带 tool_call 的 Exit（return-direct 收尾）必然配不上结果，
// 由 buildInitialMessages 出口的 dropDanglingToolCalls 兜底；逐字完整流（含工具往返）只在
// 会话历史层（WithHistory）。events 该不该继续当 prior 源待拍板，见 doc/code/todo.md 对应条目。
//
// 设计对照 compose.AgentEvent，简化版（去掉 fork lane / NS / agentActionWire 等
// PoC 暂不需要的字段）。Phase 5.2 后续按需补齐。
type AgentEvent struct {
	// AgentName 产生此事件的 agent 名字
	AgentName string `json:"agent_name"`

	// Output 携带步骤产出消息
	Output *AgentOutput `json:"output,omitempty"`

	// Action 携带控制流信号——仅最终答案 / 中断 / chunk 时非 nil
	Action *AgentAction `json:"action,omitempty"`

	// Err 步骤失败时非 nil
	Err *schema.CommError `json:"err,omitempty"`

	// CreatedAt 事件产生时刻（UnixNano），由 RunSession.AddEvent 自动填
	CreatedAt int64 `json:"created_at"`

	// RunID 标识本事件所属的 LLM 调用/工具批次（前端按 run_id + chunk_id
	// 幂等去重）。BasicAgent 每轮 LLM 调用前生成 UUID 透传到本轮所有事件。
	RunID string `json:"run_id,omitempty"`

	// **R32+ 视角 4 audit 删**: NS 字段生产 0 reader (仅 AddEventInGraph 写但没人读),
	// SelfHistoryByNS 也不存在。子图嵌套场景已被 ParallelAgent ForkLane +
	// CommitLaneEvents (ISSUE-80) lane 物理隔离替代。
}

// AgentOutput 包装 agent 在一个执行步骤中生成的消息。
type AgentOutput struct {
	Message *schema.Message `json:"message,omitempty"`
}

// AgentAction 携带控制流信号。
type AgentAction struct {
	// Exit true 表示 agent 给出最终答案应停止
	Exit bool `json:"exit,omitempty"`

	// IsChunk true 表示 LLM 流式 chunk——不写入 sess.Events（避免长流刷爆历史），
	// 仅供前端 SSE 实时渲染
	IsChunk bool `json:"is_chunk,omitempty"`

	// ResetStream true 表示前端应丢弃此前累积的 chunk 缓冲（典型场景：
	// ReactiveCompact 触发 retry，第二次 LLM 调用产生全新流，第一次 attempt
	// 已 emit 的 chunks 应丢弃）
	ResetStream bool `json:"reset_stream,omitempty"`
}

// NewOutputEvent 创建包装中间输出（如工具结果）的事件。
func NewOutputEvent(agentName string, msg *schema.Message) *AgentEvent {
	return &AgentEvent{
		AgentName: agentName,
		Output:    &AgentOutput{Message: msg},
	}
}

// NewExitEvent 创建包装最终答案的事件，Action.Exit = true。
func NewExitEvent(agentName string, msg *schema.Message) *AgentEvent {
	return &AgentEvent{
		AgentName: agentName,
		Output:    &AgentOutput{Message: msg},
		Action:    &AgentAction{Exit: true},
	}
}

// NewChunkEvent 创建一个 LLM 流式 token chunk 事件。
//
// 通过 OrchestrationHooks.OnNodeEvent 实时投递给前端 SSE。chunk 不写入
// sess.Events——最终聚合后的完整 assistant 消息走 NewOutputEvent / NewExitEvent
// 写入 session 作审计。
func NewChunkEvent(agentName string, msg *schema.Message) *AgentEvent {
	return &AgentEvent{
		AgentName: agentName,
		Output:    &AgentOutput{Message: msg},
		Action:    &AgentAction{IsChunk: true},
	}
}

// FinalMessage 从 events 中找最终答案（Action.Exit=true）的消息。
// 找不到返 nil（agent 还在跑或未给最终答案）。
func FinalMessage(events []*AgentEvent) *schema.Message {
	for i := len(events) - 1; i >= 0; i-- {
		e := events[i]
		if e.Action != nil && e.Action.Exit && e.Output != nil {
			return e.Output.Message
		}
	}
	return nil
}

// FinalMessageByAgent 反向扫 events 找匹配 agentName 的最后一个 Exit 消息。
//
// 用途：ParallelAgent 等共享 parentSession 的 fan-out 场景——sub-agent.Run() 返回的
// events 是 session.Events() 切片快照，可能含 sibling branch 同时写入的 Exit 事件。
// FinalMessage 直接反向扫会拿到 sibling 的 Exit 串台。改按 sub-agent.Name() 过滤。
//
// **限制**：业务把同 sub-agent 实例（同 Name）作多 branches 时本函数无法区分——
// 此为合理限制，业务应用 distinct sub-agent 名作 branches。
func FinalMessageByAgent(events []*AgentEvent, agentName string) *schema.Message {
	for i := len(events) - 1; i >= 0; i-- {
		e := events[i]
		if e == nil || e.AgentName != agentName {
			continue
		}
		if e.Action != nil && e.Action.Exit && e.Output != nil {
			return e.Output.Message
		}
	}
	return nil
}
