package observability

import "go.opentelemetry.io/otel/attribute"

// 属性键。gen_ai.* 取自 OpenTelemetry GenAI 语义约定（open-telemetry/semantic-conventions-genai，
// Development 状态）；crew.* 是本框架自己的维度，GenAI 约定里没有对应。
//
// 不引 go.opentelemetry.io/otel/semconv：GenAI 约定已从主 semconv 仓库迁出，Go 的 semconv 包从
// v1.40 起不再生成 gen_ai.*，而 v1.39 里的那份又缺后来加的键（cache_read / reasoning token 等）。
// 键名集中在本文件，约定改名时只改这里。
const (
	AttrOperationName         = attribute.Key("gen_ai.operation.name")
	AttrProviderName          = attribute.Key("gen_ai.provider.name")
	AttrRequestModel          = attribute.Key("gen_ai.request.model")
	AttrRequestTemperature    = attribute.Key("gen_ai.request.temperature")
	AttrRequestMaxTokens      = attribute.Key("gen_ai.request.max_tokens")
	AttrRequestTopP           = attribute.Key("gen_ai.request.top_p")
	AttrRequestStream         = attribute.Key("gen_ai.request.stream")
	AttrResponseModel         = attribute.Key("gen_ai.response.model")
	AttrResponseFinishReasons = attribute.Key("gen_ai.response.finish_reasons")
	AttrResponseTTFC          = attribute.Key("gen_ai.response.time_to_first_chunk")
	AttrUsageInputTokens      = attribute.Key("gen_ai.usage.input_tokens")
	AttrUsageOutputTokens     = attribute.Key("gen_ai.usage.output_tokens")
	AttrUsageCacheReadTokens  = attribute.Key("gen_ai.usage.cache_read.input_tokens")
	AttrUsageCacheWriteTokens = attribute.Key("gen_ai.usage.cache_write.input_tokens")
	AttrUsageReasoningTokens  = attribute.Key("gen_ai.usage.reasoning.output_tokens")
	AttrTokenModality         = attribute.Key("gen_ai.token.modality")
	AttrConversationID        = attribute.Key("gen_ai.conversation.id")
	AttrAgentName             = attribute.Key("gen_ai.agent.name")
	AttrWorkflowName          = attribute.Key("gen_ai.workflow.name")
	AttrToolName              = attribute.Key("gen_ai.tool.name")
	AttrToolType              = attribute.Key("gen_ai.tool.type")
	AttrToolCallID            = attribute.Key("gen_ai.tool.call.id")
	AttrToolCallArguments     = attribute.Key("gen_ai.tool.call.arguments")
	AttrToolCallResult        = attribute.Key("gen_ai.tool.call.result")
	AttrSkillName             = attribute.Key("gen_ai.skill.name")
	AttrSystemInstructions    = attribute.Key("gen_ai.system_instructions")
	AttrInputMessages         = attribute.Key("gen_ai.input.messages")
	AttrOutputMessages        = attribute.Key("gen_ai.output.messages")
	AttrErrorType             = attribute.Key("error.type")

	// AttrOutcome 是一次执行的结局：success / error / timeout / cancelled / blocked（等人答复的挂起）。
	// span status 只有 Unset / Error 两档有用，「用户点了停止」「等人审批」都不该算错误，靠它区分。
	AttrOutcome        = attribute.Key("crew.outcome")
	AttrAgentKind      = attribute.Key("crew.agent.kind") // AgentInfo.Type：base / sequential / parallel / loop / routing / handoff / plan / reflect / graph / a2a_remote，业务自研 agent 自己定
	AttrAgentRound     = attribute.Key("crew.agent.round")
	AttrRunID          = attribute.Key("crew.run.id") // RunSession.RunID：一个任务的身份
	AttrGraphNS        = attribute.Key("crew.graph.ns")
	AttrNodeID         = attribute.Key("crew.node.id")
	AttrNodeInstance   = attribute.Key("crew.node.instance")
	AttrNodeComponent  = attribute.Key("crew.node.component")
	AttrToolCategory   = attribute.Key("crew.tool.category")
	AttrResumeAction   = attribute.Key("crew.resume.action")
	AttrInterruptID    = attribute.Key("crew.interrupt.id")
	AttrInterruptCount = attribute.Key("crew.interrupt.queries")
	AttrStreamChunks   = attribute.Key("crew.stream.chunks")
	AttrCompactLayer   = attribute.Key("crew.compact.layer")
	AttrCheckpointOp   = attribute.Key("crew.checkpoint.op")
	AttrStaleOp        = attribute.Key("crew.checkpoint.stale_op")
	AttrFanoutKind     = attribute.Key("crew.fanout.kind")
	AttrFailoverReason = attribute.Key("crew.failover.reason")
	AttrFailoverFrom   = attribute.Key("crew.failover.from")
	AttrTrippedMember  = attribute.Key("crew.fallback.member")
	AttrTrippedReason  = attribute.Key("crew.fallback.reason")
	AttrPlanPhase      = attribute.Key("crew.plan.phase")
	AttrRouteTarget    = attribute.Key("crew.route.target")
	AttrA2AOperation   = attribute.Key("crew.a2a.operation")
	AttrA2AMethod      = attribute.Key("crew.a2a.method")
	AttrA2ATransport   = attribute.Key("crew.a2a.transport")
	AttrA2ATaskID      = attribute.Key("crew.a2a.task_id")
)

// gen_ai.operation.name 的取值。
const (
	OpChat           = "chat"
	OpExecuteTool    = "execute_tool"
	OpInvokeAgent    = "invoke_agent"
	OpInvokeWorkflow = "invoke_workflow"
	OpRetrieval      = "retrieval"
	OpEmbeddings     = "embeddings"
)

// crew.outcome 的取值。
const (
	OutcomeSuccess   = "success"
	OutcomeError     = "error"
	OutcomeTimeout   = "timeout"
	OutcomeCancelled = "cancelled"
	OutcomeBlocked   = "blocked"
)

// 指标名。gen_ai.* 按 GenAI 语义约定（名字、单位、属性集）；crew.* 是框架自己的。
const (
	MetricInferenceDuration      = "gen_ai.client.inference.duration"
	MetricInferenceTTFC          = "gen_ai.client.inference.time_to_first_chunk"
	MetricUsageInputTokens       = "gen_ai.client.inference.usage.input_tokens"
	MetricUsageOutputTokens      = "gen_ai.client.inference.usage.output_tokens"
	MetricUsageCacheReadTokens   = "gen_ai.client.inference.usage.cache_read.input_tokens"
	MetricUsageCacheWriteTokens  = "gen_ai.client.inference.usage.cache_write.input_tokens"
	MetricUsageReasoningTokens   = "gen_ai.client.inference.usage.reasoning.output_tokens"
	MetricInvokeAgentDuration    = "gen_ai.invoke_agent.duration"
	MetricInvokeAgentInferences  = "gen_ai.invoke_agent.inference_calls"
	MetricInvokeAgentToolCalls   = "gen_ai.invoke_agent.tool_calls"
	MetricInvokeWorkflowDuration = "gen_ai.invoke_workflow.duration"
	MetricExecuteToolDuration    = "gen_ai.execute_tool.duration"

	MetricAgentInvocations     = "crew.agent.invocations"
	MetricAgentActive          = "crew.agent.active"
	MetricInterrupts           = "crew.interrupts"
	MetricInterruptWait        = "crew.interrupt.wait.duration"
	MetricResumes              = "crew.resumes"
	MetricSkillActivations     = "crew.skill.activations"
	MetricCompactions          = "crew.compactions"
	MetricCompactionDuration   = "crew.compaction.duration"
	MetricCompactionRatio      = "crew.compaction.ratio"
	MetricCheckpointOps        = "crew.checkpoint.operations"
	MetricCheckpointOpDuration = "crew.checkpoint.operation.duration"
	MetricStaleWrites          = "crew.checkpoint.stale_writes"
	MetricFanoutBranches       = "crew.fanout.branches"
	MetricFailovers            = "crew.fallback.failovers"
	MetricMemberTrips          = "crew.fallback.member_trips"
	MetricNodeExecutions       = "crew.node.executions"
	MetricPlanReplans          = "crew.plan.replans"
	MetricRouteDecisions       = "crew.route.decisions"
)
