package observability

import (
	"context"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/fallback"
)

// eventSeenKey 标记「这次事件投递本实例已经处理过」（见 OnNodeEvent）。
type eventSeenKey struct{ o *Observability }

// orchHooks 实现 agent_v2.OrchestrationHooks：图与节点各一个 span，框架事件记成节点 span 上的事件并转成指标。
type orchHooks struct{ o *Observability }

var _ agent_v2.OrchestrationHooks = (*orchHooks)(nil)

// OnGraphStart 只在图外面没有包着它的 agent 时建工作流 span：agent_v2 的编排在跑图之前已经建了
// invoke_agent / invoke_workflow span，再建一层是同一段时间的重复。直接用 compose_v2.Run 跑图时
// 图就是唯一的表达。
func (h *orchHooks) OnGraphStart(ctx context.Context) (context.Context, error) {
	ctx, _ = h.o.startGraph(ctx)
	return ctx, nil
}

// startGraph 建工作流 span；没建（外面是同一 ns 的 agent，或同一个 Observability 装了两层、外层已为这张图建过）
// 时返回 nil 句柄。
func (o *Observability) startGraph(ctx context.Context) (context.Context, *handle) {
	o.orchFired.Store(true)
	graphNS := compose_v2.GetGraphNS(ctx)
	if cur := o.handleFrom(ctx); cur != nil && cur.graphNS == graphNS && !cur.ended.Load() &&
		(cur.kind == kindAgent || cur.kind == kindGraph) {
		return ctx, nil
	}
	name := workflowName(graphNS)
	attrs := append([]attribute.KeyValue{AttrOperationName.String(OpInvokeWorkflow), AttrWorkflowName.String(name)}, runAttrs(ctx)...)
	if graphNS != "" {
		attrs = append(attrs, AttrGraphNS.String(graphNS))
	}
	ctx, span := o.tracer.Start(ctx, OpInvokeWorkflow+" "+name, trace.WithSpanKind(trace.SpanKindInternal), trace.WithAttributes(attrs...))
	gh := &handle{kind: kindGraph, span: span, start: time.Now(), name: name, graphNS: graphNS, scope: o.scopeFrom(ctx)}
	return o.withHandle(ctx, gh), gh
}

// workflowName 是嵌套图的工作流名：ns 最后一段的节点 ID，不带扇出实例号（#N）与重访序号（@N）——实例号
// 每次投递都新铸，带上的话 span 名与 gen_ai.workflow.name 指标维度的取值没有上限。完整 ns 另记在 crew.graph.ns。
// 顶层图（ns 为空）叫 root；ns 解析不了时叫 unknown。
func workflowName(graphNS string) string {
	if graphNS == "" {
		return "root"
	}
	segs, err := compose_v2.SplitNS(graphNS)
	if err != nil || len(segs) == 0 {
		return "unknown"
	}
	return segs[len(segs)-1].NodeID
}

func (h *orchHooks) OnGraphEnd(ctx context.Context, err error) context.Context {
	o := h.o
	gh := o.handleFrom(ctx)
	if gh == nil || gh.kind != kindGraph || gh.graphNS != compose_v2.GetGraphNS(ctx) {
		return ctx
	}
	r := classify(ctx, err)
	if o.finish(gh, r) {
		a := []attribute.KeyValue{AttrWorkflowName.String(gh.name)}
		if r.failed() {
			a = append(a, AttrErrorType.String(r.errType))
		}
		o.inst.workflowDuration.Record(ctx, seconds(gh.start), metric.WithAttributes(a...))
	}
	return ctx
}

// OnNodeStart 建节点 span。节点声明了组件类型（compose_v2.WithNodeComponent）时按 GenAI 约定的操作名标：
// retrieval / embedding 分别是 retrieval / embeddings 操作，guardrail 只标 crew.node.component。
func (h *orchHooks) OnNodeStart(ctx context.Context, nodeID string) (context.Context, error) {
	ctx, _ = h.o.startNode(ctx, nodeID)
	return ctx, nil
}

// startNode 建节点 span；同一个 Observability 装了两层、外层已为这次节点执行建过时返回 nil 句柄。
func (o *Observability) startNode(ctx context.Context, nodeID string) (context.Context, *handle) {
	graphNS := compose_v2.GetGraphNS(ctx)
	inst := compose_v2.GetCurrentInstanceID(ctx)
	if cur := o.handleFrom(ctx); cur != nil && cur.kind == kindNode && !cur.ended.Load() &&
		cur.name == nodeID && cur.graphNS == graphNS && cur.instance == inst {
		return ctx, nil
	}
	attrs := []attribute.KeyValue{AttrNodeID.String(nodeID)}
	if graphNS != "" {
		attrs = append(attrs, AttrGraphNS.String(graphNS))
	}
	if inst != "" {
		attrs = append(attrs, AttrNodeInstance.String(inst))
	}
	spanName := "node " + nodeID
	switch comp := compose_v2.GetComponentKind(ctx); comp {
	case "retrieval":
		attrs = append(attrs, AttrOperationName.String(OpRetrieval), AttrNodeComponent.String(comp))
		spanName = OpRetrieval + " " + nodeID
	case "embedding":
		attrs = append(attrs, AttrOperationName.String(OpEmbeddings), AttrNodeComponent.String(comp))
		spanName = OpEmbeddings + " " + nodeID
	case "guardrail":
		attrs = append(attrs, AttrNodeComponent.String(comp))
		spanName = "guardrail " + nodeID
	}
	ctx, span := o.tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindInternal), trace.WithAttributes(attrs...))
	nh := &handle{kind: kindNode, span: span, start: time.Now(), name: nodeID, graphNS: graphNS, instance: inst, scope: o.scopeFrom(ctx)}
	if o.recordContent {
		nh.partialBuf = &boundedBuffer{max: o.maxContent}
	}
	o.inst.nodeExecutions.Add(ctx, 1, metric.WithAttributes(AttrNodeID.String(nodeID)))
	return o.withHandle(ctx, nh), nh
}

func (h *orchHooks) OnNodeEnd(ctx context.Context, nodeID string, err error) context.Context {
	o := h.o
	nh := o.handleFrom(ctx)
	// 只收本次执行自己的句柄：开始钩子失败时框架拿开始之前的 ctx 调结束钩子，那里最内层的句柄可能是外层的
	// 同名节点，靠 ns 区分。
	if nh == nil || nh.kind != kindNode || nh.name != nodeID || nh.graphNS != compose_v2.GetGraphNS(ctx) {
		return ctx
	}
	r := classify(ctx, err)
	var extra []attribute.KeyValue
	if n := nh.chunks.Load(); n > 0 {
		extra = append(extra, AttrStreamChunks.Int64(n))
		if first := nh.firstNano.Load(); first > 0 {
			extra = append(extra, attribute.Float64("crew.stream.first_chunk_seconds", time.Unix(0, first).Sub(nh.start).Seconds()))
		}
		// 中断 / 出错时模型可能已经吐了一半：把攒下的片段留在 span 上（只在打开内容采集时）。
		if nh.partialBuf != nil && r.outcome != OutcomeSuccess {
			if s := nh.partialBuf.String(); s != "" {
				nh.span.AddEvent("crew.stream.partial_output", trace.WithAttributes(attribute.String("content", o.text(s))))
			}
		}
	}
	o.finish(nh, r, extra...)
	return ctx
}

// OnNodeEvent 把框架事件记成节点 span 上的事件（名字加 crew. 前缀），并按事件类型记指标。流式片段不逐条记，
// 只计数、记首片时间。
func (h *orchHooks) OnNodeEvent(ctx context.Context, nodeID string, ev agent_v2.Event) context.Context {
	o := h.o
	// 同一个 Observability 装了两层时，同一次投递会被两层各交一次：第一层处理完在返回的 ctx 上留个记号，
	// 链里往下传到第二层时认出来跳过——不然片段计数、事件、框架指标都翻倍。框架不采纳 OnNodeEvent 的返回值，
	// 记号留不到下一次投递。
	if ctx.Value(eventSeenKey{o}) != nil {
		return ctx
	}
	ctx = context.WithValue(ctx, eventSeenKey{o}, true)
	cur := o.handleFrom(ctx)
	if ev.Type == "chunk" {
		if cur != nil && cur.kind == kindNode {
			cur.chunks.Add(1)
			cur.firstNano.CompareAndSwap(0, time.Now().UnixNano())
			if cur.partialBuf != nil {
				cur.partialBuf.append(ev.Content)
			}
		}
		return ctx
	}
	if cur != nil {
		attrs := o.eventAttrs(ev)
		if ev.Content != "" && o.recordContent {
			attrs = append(attrs, attribute.String("content", o.text(ev.Content)))
		}
		cur.span.AddEvent("crew."+ev.Type, trace.WithAttributes(attrs...))
	}
	o.recordEventMetric(ctx, ev)
	return ctx
}

// eventAttrs 把事件元数据里的标量（字符串 / 数字 / 布尔）记成事件属性；结构化的 action 按类型展开。
// 字符串经脱敏、截断（跟错误信息同一套处理）。
func (o *Observability) eventAttrs(ev agent_v2.Event) []attribute.KeyValue {
	var attrs []attribute.KeyValue
	for k, v := range ev.Metadata {
		switch x := v.(type) {
		case string:
			attrs = append(attrs, attribute.String(k, o.errText(x)))
		case bool:
			attrs = append(attrs, attribute.Bool(k, x))
		case int:
			attrs = append(attrs, attribute.Int(k, x))
		case int32:
			attrs = append(attrs, attribute.Int(k, int(x)))
		case int64:
			attrs = append(attrs, attribute.Int64(k, x))
		case float64:
			attrs = append(attrs, attribute.Float64(k, x))
		}
	}
	if a, ok := ev.Metadata["action"].(*agent_v2.CompactionAction); ok && a != nil {
		attrs = append(attrs, o.compactionAttrs(a)...)
	}
	return attrs
}

// compactionAttrs 展开压缩事件：哪一层、为什么压、压掉多少、前后 token、耗时。
func (o *Observability) compactionAttrs(a *agent_v2.CompactionAction) []attribute.KeyValue {
	attrs := []attribute.KeyValue{AttrCompactLayer.String(string(a.Layer)), attribute.String("crew.compact.phase", string(a.Type))}
	switch a.Type {
	case agent_v2.CompactionActionBefore:
		if b := a.Before; b != nil {
			if b.Trigger != "" {
				attrs = append(attrs, attribute.String("crew.compact.trigger", b.Trigger))
			}
			if b.InputTokens > 0 {
				attrs = append(attrs, attribute.Int("crew.compact.input_tokens", int(b.InputTokens)))
			}
		}
	case agent_v2.CompactionActionAfter:
		if af := a.After; af != nil {
			attrs = append(attrs, attribute.Int("crew.compact.dropped", int(af.Dropped)), attribute.Int("crew.compact.kept", int(af.Kept)))
			if af.InputTokens > 0 {
				attrs = append(attrs, attribute.Int("crew.compact.input_tokens", int(af.InputTokens)))
			}
			if af.OutputTokens > 0 {
				attrs = append(attrs, attribute.Int("crew.compact.output_tokens", int(af.OutputTokens)))
			}
			if af.DurationMs > 0 {
				attrs = append(attrs, attribute.Int64("crew.compact.duration_ms", af.DurationMs))
			}
			if af.Err != "" {
				attrs = append(attrs, attribute.String("crew.compact.error", o.errText(af.Err)))
			}
		}
	case agent_v2.CompactionActionGenerate:
		if g := a.Generate; g != nil {
			attrs = append(attrs, attribute.Int("crew.compact.attempt", int(g.Attempt)))
			if g.Err != "" {
				attrs = append(attrs, attribute.String("crew.compact.error", o.errText(g.Err)))
			}
		}
	case agent_v2.CompactionActionRetry:
		if rt := a.Retry; rt != nil {
			attrs = append(attrs, attribute.Int("crew.compact.attempt", int(rt.Attempt)),
				attribute.Int("crew.compact.max_attempts", int(rt.MaxAttempts)))
			if rt.Reason != "" {
				attrs = append(attrs, attribute.String("crew.compact.reason", rt.Reason))
			}
		}
	}
	return attrs
}

// recordEventMetric 按事件类型记框架指标。这些事件只在接了编排钩子时才会投递到这里。
func (o *Observability) recordEventMetric(ctx context.Context, ev agent_v2.Event) {
	switch ev.Type {
	case agent_v2.CompactionEventType:
		a, ok := ev.Metadata["action"].(*agent_v2.CompactionAction)
		if !ok || a == nil || a.Type != agent_v2.CompactionActionAfter || a.After == nil {
			return
		}
		layer := AttrCompactLayer.String(string(a.Layer))
		outcome := OutcomeSuccess
		if a.After.Err != "" {
			outcome = OutcomeError
		}
		o.inst.compactions.Add(ctx, 1, metric.WithAttributes(layer, AttrOutcome.String(outcome)))
		if a.After.DurationMs > 0 {
			o.inst.compactionDuration.Record(ctx, float64(a.After.DurationMs)/1000, metric.WithAttributes(layer))
		}
		if a.After.InputTokens > 0 {
			o.inst.compactionRatio.Record(ctx, float64(a.After.OutputTokens)/float64(a.After.InputTokens), metric.WithAttributes(layer))
		}
	case compose_v2.CpEventType:
		a, ok := ev.Metadata["action"].(*compose_v2.CpAction)
		if !ok || a == nil {
			return
		}
		var okFlag bool
		var durMs int64
		switch {
		case a.Type == compose_v2.CpOperationSave && a.Save != nil:
			okFlag, durMs = a.Save.Ok, a.Save.DurationMs
		case a.Type == compose_v2.CpOperationLoad && a.Load != nil:
			okFlag, durMs = a.Load.Ok, a.Load.DurationMs
		case a.Type == compose_v2.CpOperationCleanup && a.Cleanup != nil:
			okFlag, durMs = a.Cleanup.Ok, a.Cleanup.DurationMs
		default:
			return
		}
		outcome := OutcomeSuccess
		if !okFlag {
			outcome = OutcomeError
		}
		op := AttrCheckpointOp.String(string(a.Type))
		o.inst.checkpointOps.Add(ctx, 1, metric.WithAttributes(op, AttrOutcome.String(outcome)))
		if durMs > 0 {
			o.inst.checkpointDuration.Record(ctx, float64(durMs)/1000, metric.WithAttributes(op))
		}
	case agent_v2.FanoutEventType:
		if a, ok := ev.Metadata["action"].(*agent_v2.FanoutAction); ok && a != nil {
			o.inst.fanoutBranches.Record(ctx, int64(a.Branches), metric.WithAttributes(AttrFanoutKind.String(string(a.Kind))))
		}
	case agent_v2.PlanReplanEventType:
		if a, ok := ev.Metadata["action"].(*agent_v2.PlanReplanAction); ok && a != nil {
			o.inst.planReplans.Add(ctx, 1, metric.WithAttributes(AttrPlanPhase.String(string(a.Phase))))
		}
	case agent_v2.RouteDecisionEventType:
		if a, ok := agent_v2.RouteDecisionOf(ev); ok {
			target := a.Target
			if target == "" {
				target = "__none__"
			}
			o.inst.routeDecisions.Add(ctx, 1, metric.WithAttributes(AttrAgentName.String(a.AgentName), AttrRouteTarget.String(target)))
		}
	case fallback.FailoverEventType:
		if a, ok := ev.Metadata["action"].(*fallback.FailoverAction); ok && a != nil {
			o.inst.failovers.Add(ctx, 1, metric.WithAttributes(AttrFailoverReason.String(a.Reason),
				AttrFailoverFrom.String(strconv.Itoa(a.FromIdx))))
		}
	case fallback.MemberTrippedEventType:
		if a, ok := ev.Metadata["action"].(*fallback.MemberTrippedAction); ok && a != nil {
			o.inst.memberTrips.Add(ctx, 1, metric.WithAttributes(AttrTrippedMember.String(strconv.Itoa(a.MemberIdx)),
				AttrTrippedReason.String(a.Reason)))
		}
	}
}
