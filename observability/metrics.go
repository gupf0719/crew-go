package observability

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// 直方图桶。时长桶取 GenAI 语义约定对 operation duration 的建议值（秒），计数桶给「一次调用里调了几次」用。
var (
	durationBuckets = []float64{0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.64, 1.28, 2.56, 5.12, 10.24, 20.48, 40.96, 81.92}
	countBuckets    = []float64{0, 1, 2, 3, 5, 8, 13, 21, 34, 55}
	ratioBuckets    = []float64{0.1, 0.2, 0.3, 0.5, 0.7, 0.9, 1}
	// waitBuckets 给「挂起到续跑」用：等的是人，从几秒到一天，时长桶最大 81.92s 会让样本几乎全落进 +Inf。
	waitBuckets = []float64{1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600, 7200, 14400, 43200, 86400}
)

// instruments 是框架自动记录的全部指标。都在 New 时建好，热路径上只做 Record / Add。
type instruments struct {
	inferenceDuration  metric.Float64Histogram
	inferenceTTFC      metric.Float64Histogram
	usageInput         metric.Int64Counter
	usageOutput        metric.Int64Counter
	usageCacheRead     metric.Int64Counter
	usageCacheWrite    metric.Int64Counter
	usageReasoning     metric.Int64Counter
	agentDuration      metric.Float64Histogram
	agentInferences    metric.Int64Histogram
	agentToolCalls     metric.Int64Histogram
	workflowDuration   metric.Float64Histogram
	toolDuration       metric.Float64Histogram
	agentInvocations   metric.Int64Counter
	agentActive        metric.Int64UpDownCounter
	interrupts         metric.Int64Counter
	interruptWait      metric.Float64Histogram
	resumes            metric.Int64Counter
	skillActivations   metric.Int64Counter
	compactions        metric.Int64Counter
	compactionDuration metric.Float64Histogram
	compactionRatio    metric.Float64Histogram
	checkpointOps      metric.Int64Counter
	checkpointDuration metric.Float64Histogram
	staleWrites        metric.Int64Counter
	fanoutBranches     metric.Int64Histogram
	failovers          metric.Int64Counter
	memberTrips        metric.Int64Counter
	nodeExecutions     metric.Int64Counter
	planReplans        metric.Int64Counter
	routeDecisions     metric.Int64Counter
}

// newInstruments 建全部指标。建失败（名字非法这类，正常不会发生）交给 otel.Handle，那一个指标退成
// no-op，不影响其余指标和 trace。
func newInstruments(m metric.Meter) *instruments {
	h := func(err error) {
		if err != nil {
			otel.Handle(err)
		}
	}
	f64h := func(name, unit, desc string, buckets []float64) metric.Float64Histogram {
		x, err := m.Float64Histogram(name, metric.WithUnit(unit), metric.WithDescription(desc),
			metric.WithExplicitBucketBoundaries(buckets...))
		h(err)
		return x
	}
	i64h := func(name, unit, desc string) metric.Int64Histogram {
		x, err := m.Int64Histogram(name, metric.WithUnit(unit), metric.WithDescription(desc),
			metric.WithExplicitBucketBoundaries(countBuckets...))
		h(err)
		return x
	}
	ctr := func(name, unit, desc string) metric.Int64Counter {
		x, err := m.Int64Counter(name, metric.WithUnit(unit), metric.WithDescription(desc))
		h(err)
		return x
	}
	updown, err := m.Int64UpDownCounter(MetricAgentActive, metric.WithUnit("{invocation}"),
		metric.WithDescription("正在执行的 agent 调用数"))
	h(err)
	return &instruments{
		inferenceDuration: f64h(MetricInferenceDuration, "s", "GenAI client inference operation duration.", durationBuckets),
		inferenceTTFC:     f64h(MetricInferenceTTFC, "s", "Time to receive the first chunk of a streaming inference response.", durationBuckets),
		usageInput:        ctr(MetricUsageInputTokens, "{token}", "The number of input (prompt) tokens used, including cached tokens."),
		usageOutput:       ctr(MetricUsageOutputTokens, "{token}", "The number of output (completion) tokens used, including reasoning tokens."),
		usageCacheRead:    ctr(MetricUsageCacheReadTokens, "{token}", "The number of input tokens served from a provider-managed cache."),
		usageCacheWrite:   ctr(MetricUsageCacheWriteTokens, "{token}", "The number of input tokens written to a provider-managed cache."),
		usageReasoning:    ctr(MetricUsageReasoningTokens, "{token}", "The number of output tokens used for reasoning."),
		agentDuration:     f64h(MetricInvokeAgentDuration, "s", "The end-to-end duration of a single in-process agent invocation.", durationBuckets),
		agentInferences:   i64h(MetricInvokeAgentInferences, "{inference_call}", "The number of inference calls an agent makes during a single invocation."),
		agentToolCalls:    i64h(MetricInvokeAgentToolCalls, "{tool_call}", "The number of tool calls an agent makes during a single invocation."),
		workflowDuration:  f64h(MetricInvokeWorkflowDuration, "s", "Records duration of GenAI workflow.", durationBuckets),
		toolDuration:      f64h(MetricExecuteToolDuration, "s", "The duration of a single tool execution.", durationBuckets),

		agentInvocations:   ctr(MetricAgentInvocations, "{invocation}", "agent 调用次数，按结局（crew.outcome）分"),
		agentActive:        updown,
		interrupts:         ctr(MetricInterrupts, "{interrupt}", "等人答复的挂起次数（HITL 中断）"),
		interruptWait:      f64h(MetricInterruptWait, "s", "从挂起到续跑的等待时长", waitBuckets),
		resumes:            ctr(MetricResumes, "{resume}", "续跑次数，按动作（crew.resume.action）分"),
		skillActivations:   ctr(MetricSkillActivations, "{activation}", "skill 激活次数"),
		compactions:        ctr(MetricCompactions, "{compaction}", "上下文压缩次数，按层与结局分"),
		compactionDuration: f64h(MetricCompactionDuration, "s", "单次上下文压缩耗时", durationBuckets),
		compactionRatio:    f64h(MetricCompactionRatio, "1", "压缩后 token 数与压缩前之比", ratioBuckets),
		checkpointOps:      ctr(MetricCheckpointOps, "{operation}", "存档读写次数，按操作与结局分"),
		checkpointDuration: f64h(MetricCheckpointOpDuration, "s", "存档读写耗时", durationBuckets),
		staleWrites:        ctr(MetricStaleWrites, "{write}", "陈旧写回被拒次数（同一会话被并发执行）"),
		fanoutBranches:     i64h(MetricFanoutBranches, "{branch}", "一次扇出的分支数"),
		failovers:          ctr(MetricFailovers, "{failover}", "fallback 候选切换次数"),
		memberTrips:        ctr(MetricMemberTrips, "{trip}", "fallback 候选断路器熔断次数"),
		nodeExecutions:     ctr(MetricNodeExecutions, "{execution}", "图节点执行次数"),
		planReplans:        ctr(MetricPlanReplans, "{plan}", "PlanAgent 规划次数，按阶段（initial / replan）分"),
		routeDecisions:     ctr(MetricRouteDecisions, "{decision}", "RoutingAgent 路由决策次数，按目标分"),
	}
}
