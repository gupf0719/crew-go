// Package a2a 把远程 A2A（Agent2Agent）协议 agent 接入 crew-go。
//
// **两种形态，共享同一 RemoteAgent 核心**：
//   - NewRemoteAgent → agent_v2.Agent：直接嵌入编排（Sequential/Parallel/Loop 等收 Agent）
//     当确定性 step；也能经 agent_v2.AgentAsTool 转 tool。
//   - NewRemoteAgentTool → tool.Tool（= AgentAsTool(NewRemoteAgent(...))）：注册进 host
//     BasicAgent 的 toolkit，host LLM 通过 tool-calling 自主调度远程 agent。
//
// agent 形态是更根本的原语（crew-go 能 agent→tool 经 AgentAsTool、但不能 tool→agent），
// 覆盖"进编排"+"当 tool"两种用法。
//
// 消费远程 A2A 的形态按编排模型分派：crew-go 同时给 tool + agent 两形态（tool 进
// BasicAgent toolkit、agent 进编排），且骑 host cp 实现远程 HITL（见 RemoteAgent / WithHumanGate）。
//
// **为何不把远程当 sub-agent 显式编排**：A2A 远程是黑盒（host 只知 AgentCard 的
// name/description，看不到远程工具/内部状态），适合 host LLM 经 tool-calling 自主调度
// 或当确定性 step 委派，不适合像本地 agent 那样细粒度编排。
package a2a

import (
	"context"

	"crew-go/agent_v2"
	"crew-go/tool"
)

// NewRemoteAgentTool 解析远程 A2A AgentCard + 建 client，返回可注册进 BasicAgent
// toolkit 的 tool.Tool——host LLM 通过 tool-calling 委派任务给远程 agent。
//
// 便利包装，等价 agent_v2.AgentAsTool(NewRemoteAgent(ctx, cardURL, opts...))。tool 名 /
// 描述取远程 AgentCard（或 WithName / WithDescription override）；HITL（WithHumanGate）时
// RemoteAgent 抛的 InterruptError 经 AgentAsTool 透传 → 骑 host BasicAgent toolsNode cp。
func NewRemoteAgentTool(ctx context.Context, cardURL string, opts ...RemoteOption) (tool.Tool, error) {
	ra, err := NewRemoteAgent(ctx, cardURL, opts...)
	if err != nil {
		return nil, err
	}
	return agent_v2.AgentAsTool(ra), nil
}
