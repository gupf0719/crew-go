package observability

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	specModel "crew-go/modules/model"
	"crew-go/schema"
)

var isUnresolvedCallsRecord = agent_v2.IsUnresolvedCallsRecord

// workflowKinds 是记成 invoke_workflow 的 AgentInfo.Type：协调多个 agent 的编排。BasicAgent（base）、远端
// agent（a2a_remote）以及业务自研、类型不在表里的 agent 都按单个 agent 记 invoke_agent——GenAI 约定里单独的
// agent 调用不报 workflow。
var workflowKinds = map[string]bool{
	"sequential": true, "parallel": true, "loop": true, "routing": true,
	"handoff": true, "plan": true, "reflect": true, "graph": true,
}

// agentHooks 实现 hook.Hooks：agent 调用、模型调用、工具调用各一个 span。
type agentHooks struct{ o *Observability }

var _ hook.Hooks = (*agentHooks)(nil)

func (h *agentHooks) OnAgentStart(ctx context.Context, info *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	o := h.o
	workflow := workflowKinds[info.Type]
	op, nameKey := OpInvokeAgent, AttrAgentName
	if workflow {
		op, nameKey = OpInvokeWorkflow, AttrWorkflowName
	}
	spanName := op
	if info.Name != "" {
		spanName += " " + info.Name
	}
	graphNS := compose_v2.GetGraphNS(ctx)
	attrs := []attribute.KeyValue{AttrOperationName.String(op), nameKey.String(info.Name)}
	if info.Type != "" {
		attrs = append(attrs, AttrAgentKind.String(info.Type))
	}
	attrs = append(attrs, runAttrs(ctx)...)
	if graphNS != "" {
		attrs = append(attrs, AttrGraphNS.String(graphNS))
	}
	ctx, span := o.tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindInternal), trace.WithAttributes(attrs...))
	ah := &handle{kind: kindAgent, span: span, start: time.Now(), name: info.Name, graphNS: graphNS,
		agentKind: info.Type, workflow: workflow}
	ah.scope = &agentScope{}
	o.inst.agentActive.Add(ctx, 1, metric.WithAttributes(AttrAgentName.String(info.Name)))

	var inputMsgs []*schema.Message
	if action, isResume := agent_v2.GetResumePhase(ctx); isResume {
		labels := metric.WithAttributes(AttrAgentName.String(info.Name), AttrAgentKind.String(info.Type),
			AttrResumeAction.String(action.String()))
		o.inst.resumes.Add(ctx, 1, labels)
		evAttrs := []attribute.KeyValue{AttrResumeAction.String(action.String())}
		sid := compose_v2.GetSessionID(ctx)
		if action == agent_v2.ResumeActionAbort {
			// 放弃只在最外层触发开始 / 结束钩子，里面各层挂着的记录没人来取：连这棵子树一起丢掉。
			o.dropInterruptsUnder(sid, graphNS, info.Name)
		}
		key := interruptKey(sid, graphNS, info.Name)
		if v, ok := o.interruptedAt.LoadAndDelete(key); ok {
			wait := time.Since(v.(time.Time)).Seconds()
			o.inst.interruptWait.Record(ctx, wait, metric.WithAttributes(AttrAgentName.String(info.Name), AttrAgentKind.String(info.Type)))
			evAttrs = append(evAttrs, attribute.Float64("crew.interrupt.wait_seconds", wait))
		}
		if rc, ok := compose_v2.GetResumeContext(ctx); ok && rc != nil {
			evAttrs = append(evAttrs, attribute.Int("crew.resume.inputs", resumeInputCount(rc)))
			inputMsgs = resumeInputMessages(rc)
		}
		span.AddEvent("crew.resume", trace.WithAttributes(evAttrs...))
	} else if in != nil {
		if m := lastUserMessage(in.Messages); m != nil {
			inputMsgs = []*schema.Message{m}
		}
	}
	if o.recordContent && len(inputMsgs) > 0 {
		span.SetAttributes(AttrInputMessages.String(o.messagesJSON(inputMsgs, "")))
	}
	return withHandle(ctx, ah), nil
}

func (h *agentHooks) OnAgentEnd(ctx context.Context, info *hook.AgentInfo, out *hook.AgentOutput) context.Context {
	o := h.o
	ah := agentHandleFrom(ctx)
	// 只收本次调用自己的句柄：框架在某些路径上拿开始之前的 ctx 调结束钩子（如放弃续跑时开始钩子报错），那个
	// ctx 里最内层的 agent 句柄可能是外层 agent 的，收了就把外层的 span 提前结束了。
	if ah == nil || ah.ended.Load() || ah.name != info.Name || ah.graphNS != compose_v2.GetGraphNS(ctx) {
		return ctx
	}
	r := classify(ctx, agent_v2.GetEndError(ctx))
	// 只装了一半的告警只在「这次调用一定跑过图」时判：跑图的 agent（BasicAgent 与各编排）正常结束（成功或挂起）。
	// 开始一侧就被拒绝、图没建起来就失败、远端 agent 这类根本没有图的，编排钩子本来就不会跑，判了就是误报。
	if (info.Type == "base" || workflowKinds[info.Type]) && (r.outcome == OutcomeSuccess || r.outcome == OutcomeBlocked) {
		o.warnIfHalfInstalled(ctx)
	}
	// 结局只按框架交来的错误判。陈旧写回先于中断判：它可能跟中断绑在一条链上，按中断记会导出一个
	// 跟正常挂起一模一样的 span，运维看不出同一会话被两个副本处理了。
	if retErr := agent_v2.GetEndError(ctx); retErr != nil && ctx.Err() == nil {
		if errors.Is(retErr, compose_v2.ErrCheckpointStale) {
			op := "unknown"
			var sw *compose_v2.StaleWriteError
			if errors.As(retErr, &sw) && sw.Op != "" {
				op = sw.Op
			}
			o.inst.staleWrites.Add(ctx, 1, metric.WithAttributes(AttrStaleOp.String(op)))
		} else if ie, ok := compose_v2.IsInterrupt(retErr); ok && r.outcome == OutcomeBlocked {
			o.inst.interrupts.Add(ctx, 1, metric.WithAttributes(AttrAgentName.String(info.Name), AttrAgentKind.String(info.Type)))
			o.markInterrupted(interruptKey(compose_v2.GetSessionID(ctx), ah.graphNS, info.Name), time.Now())
			qs := ie.AllQueries()
			evAttrs := []attribute.KeyValue{AttrInterruptCount.Int(len(qs)), attribute.String("crew.interrupt.reason", ie.Reason)}
			if len(qs) > 0 && qs[0] != nil {
				evAttrs = append(evAttrs, AttrInterruptID.String(qs[0].InterruptID))
				if o.recordContent {
					evAttrs = append(evAttrs, attribute.String("crew.interrupt.question", o.text(qs[0].Question)))
				}
			}
			ah.span.AddEvent("crew.interrupt", trace.WithAttributes(evAttrs...))
		}
	}

	// 错误路径上框架不调 OnModelEnd / OnToolEnd，这些 span 在这里收尾：挂起时它们是被中断的，不算失败。
	for _, leaf := range ah.scope.drain(0) {
		lr := r
		if lr.outcome == OutcomeSuccess {
			lr = result{outcome: OutcomeError, errType: "_OTHER", errMsg: "结束钩子没有触发，由 agent 收尾"}
		}
		o.finish(leaf, lr)
	}

	var extra []attribute.KeyValue
	if o.recordContent && out != nil && r.outcome != OutcomeBlocked {
		if m := lastAssistantMessage(out.Messages); m != nil {
			extra = append(extra, AttrOutputMessages.String(o.messagesJSON([]*schema.Message{m}, "")))
		}
	}
	extra = append(extra, attribute.Int64("crew.agent.inference_calls", ah.scope.inference.Load()),
		attribute.Int64("crew.agent.tool_calls", ah.scope.toolCalls.Load()))
	if !o.finish(ah, r, extra...) {
		return ctx
	}

	errAttr := []attribute.KeyValue{}
	if r.failed() {
		errAttr = append(errAttr, AttrErrorType.String(r.errType))
	}
	dur := seconds(ah.start)
	if ah.workflow {
		o.inst.workflowDuration.Record(ctx, dur, metric.WithAttributes(append(errAttr, AttrWorkflowName.String(info.Name))...))
	} else {
		a := append(errAttr, AttrAgentName.String(info.Name))
		if m, _ := ah.scope.lastModel.Load().(string); m != "" {
			a = append(a, AttrRequestModel.String(m))
		}
		o.inst.agentDuration.Record(ctx, dur, metric.WithAttributes(a...))
		o.inst.agentInferences.Record(ctx, ah.scope.inference.Load(), metric.WithAttributes(AttrAgentName.String(info.Name)))
		o.inst.agentToolCalls.Record(ctx, ah.scope.toolCalls.Load(), metric.WithAttributes(AttrAgentName.String(info.Name)))
	}
	o.inst.agentInvocations.Add(ctx, 1, metric.WithAttributes(AttrAgentName.String(info.Name),
		AttrAgentKind.String(info.Type), AttrOutcome.String(r.outcome)))
	o.inst.agentActive.Add(ctx, -1, metric.WithAttributes(AttrAgentName.String(info.Name)))
	return ctx
}

func (h *agentHooks) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	o := h.o
	attrs := []attribute.KeyValue{AttrOperationName.String(OpChat), AttrAgentName.String(info.Name),
		AttrAgentRound.Int(int(info.Round))}
	var reqModel string
	if in != nil && in.Config != nil {
		reqModel = in.Config.ModelName
		if reqModel != "" {
			attrs = append(attrs, AttrRequestModel.String(reqModel))
		}
		if in.Config.Temperature > 0 {
			attrs = append(attrs, AttrRequestTemperature.Float64(float64(in.Config.Temperature)))
		}
		if in.Config.MaxTokens > 0 {
			attrs = append(attrs, AttrRequestMaxTokens.Int(int(in.Config.MaxTokens)))
		}
		if in.Config.TopP > 0 {
			attrs = append(attrs, AttrRequestTopP.Float64(float64(in.Config.TopP)))
		}
	}
	if conv := compose_v2.GetThreadID(ctx); conv != "" {
		attrs = append(attrs, AttrConversationID.String(conv))
	}
	if o.recordContent && in != nil {
		if sys := o.systemJSON(in.Preamble); sys != "" {
			attrs = append(attrs, AttrSystemInstructions.String(sys))
		}
		attrs = append(attrs, AttrInputMessages.String(o.messagesJSON(in.Messages, "")))
	}
	spanName := OpChat
	if reqModel != "" {
		spanName += " " + reqModel
	}
	ctx, span := o.tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
	mh := &handle{kind: kindModel, span: span, start: time.Now(), name: info.Name,
		graphNS: compose_v2.GetGraphNS(ctx), requestModel: reqModel, scope: scopeFrom(ctx), inputModality: "text"}
	if in != nil {
		mh.inputModality = tokenModality(in.Preamble, in.Messages)
	}
	mh.scope.add(mh)
	if mh.scope != nil {
		mh.scope.inference.Add(1)
	}
	return withHandle(ctx, mh), nil
}

// OnModelEndStream 给流包一层，记首片到达的时间（gen_ai.response.time_to_first_chunk）。
func (h *agentHooks) OnModelEndStream(ctx context.Context, info *hook.AgentInfo, sr specModel.MessageStreamReader) (context.Context, specModel.MessageStreamReader, error) {
	mh := handleFrom(ctx)
	if mh == nil || mh.kind != kindModel || sr == nil {
		return ctx, sr, nil
	}
	mh.span.SetAttributes(AttrRequestStream.Bool(true))
	return ctx, &firstChunkReader{inner: sr, h: mh}, nil
}

type firstChunkReader struct {
	inner specModel.MessageStreamReader
	h     *handle
}

func (r *firstChunkReader) Recv(ctx context.Context) (*schema.Message, error) {
	m, err := r.inner.Recv(ctx)
	if err == nil && m != nil {
		r.h.firstChunk.CompareAndSwap(0, time.Now().UnixNano())
	}
	return m, err
}

func (r *firstChunkReader) Close() error { return r.inner.Close() }

func (h *agentHooks) OnModelEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	o := h.o
	mh := handleFrom(ctx)
	if mh == nil || mh.kind != kindModel {
		return ctx, nil
	}
	var provider, respModel, finish string
	if out != nil && out.Message != nil && out.Message.Metadata != nil {
		md := out.Message.Metadata
		provider, respModel, finish = md.System, md.Model, md.FinishReason
	}
	var extra []attribute.KeyValue
	if provider != "" {
		extra = append(extra, AttrProviderName.String(provider))
	}
	if respModel != "" {
		extra = append(extra, AttrResponseModel.String(respModel))
		if mh.requestModel == "" {
			mh.span.SetName(OpChat + " " + respModel)
		}
	}
	if finish != "" {
		extra = append(extra, AttrResponseFinishReasons.StringSlice([]string{finish}))
	}
	var usage *schema.MessageUsage
	if out != nil {
		usage = out.Usage
	}
	if usage != nil {
		extra = append(extra, AttrUsageInputTokens.Int(int(usage.InputTokens)),
			AttrUsageOutputTokens.Int(int(usage.OutputTokens)))
		if usage.CachedInputTokens > 0 {
			extra = append(extra, AttrUsageCacheReadTokens.Int(int(usage.CachedInputTokens)))
		}
		if usage.ThinkTokens > 0 {
			extra = append(extra, AttrUsageReasoningTokens.Int(int(usage.ThinkTokens)))
		}
	}
	var ttfc float64
	if first := mh.firstChunk.Load(); first > 0 {
		ttfc = time.Unix(0, first).Sub(mh.start).Seconds()
		extra = append(extra, AttrResponseTTFC.Float64(ttfc))
	}
	if o.recordContent && out != nil && out.Message != nil {
		extra = append(extra, AttrOutputMessages.String(o.messagesJSON([]*schema.Message{out.Message}, finish)))
	}
	mh.scope.remove(mh)
	if !o.finish(mh, result{outcome: OutcomeSuccess}, extra...) {
		return ctx, nil
	}
	// gen_ai.request.model 只填调用方请求的模型；不知道时不拿响应里的模型名去顶（两者可以不同，比如别名解析到具体版本）。
	if mh.scope != nil && mh.requestModel != "" {
		mh.scope.lastModel.Store(mh.requestModel)
	}
	labels := inferenceLabels(provider, mh.requestModel, respModel)
	o.inst.inferenceDuration.Record(ctx, seconds(mh.start), metric.WithAttributes(labels...))
	if ttfc > 0 {
		o.inst.inferenceTTFC.Record(ctx, ttfc, metric.WithAttributes(labels...))
	}
	if usage != nil {
		// cache_read 是输入的子集、reasoning 是输出的子集，各跟所属那一侧的模态。
		in := metric.WithAttributes(append(labels, AttrTokenModality.String(mh.inputModality))...)
		var outMsg *schema.Message
		if out != nil {
			outMsg = out.Message
		}
		outTok := metric.WithAttributes(append(labels, AttrTokenModality.String(tokenModality([]*schema.Message{outMsg})))...)
		addTokens(ctx, o.inst.usageInput, usage.InputTokens, in)
		addTokens(ctx, o.inst.usageCacheRead, usage.CachedInputTokens, in)
		addTokens(ctx, o.inst.usageOutput, usage.OutputTokens, outTok)
		addTokens(ctx, o.inst.usageReasoning, usage.ThinkTokens, outTok)
	}
	return ctx, nil
}

func inferenceLabels(provider, requestModel, responseModel string) []attribute.KeyValue {
	a := []attribute.KeyValue{AttrOperationName.String(OpChat)}
	if provider != "" {
		a = append(a, AttrProviderName.String(provider))
	}
	if requestModel != "" {
		a = append(a, AttrRequestModel.String(requestModel))
	}
	if responseModel != "" {
		a = append(a, AttrResponseModel.String(responseModel))
	}
	return a
}

func addTokens(ctx context.Context, c metric.Int64Counter, n int32, opt metric.AddOption) {
	if n > 0 {
		c.Add(ctx, int64(n), opt)
	}
}

func (h *agentHooks) OnToolStart(ctx context.Context, info *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	o := h.o
	if in == nil {
		return ctx, nil
	}
	category, skill := toolCategory(in)
	attrs := []attribute.KeyValue{AttrOperationName.String(OpExecuteTool), AttrToolName.String(in.Name),
		AttrToolType.String("function"), AttrAgentName.String(info.Name)}
	if in.CallId != "" {
		attrs = append(attrs, AttrToolCallID.String(in.CallId))
	}
	if category != "" {
		attrs = append(attrs, AttrToolCategory.String(category))
	}
	if skill != "" {
		attrs = append(attrs, AttrSkillName.String(skill))
	}
	if conv := compose_v2.GetThreadID(ctx); conv != "" {
		attrs = append(attrs, AttrConversationID.String(conv))
	}
	if o.recordContent {
		attrs = append(attrs, AttrToolCallArguments.String(o.jsonText(in.ArgumentsJSON)))
	}
	// 加载 skill 的工具调用按 GenAI 约定把 skill 名也放进 span 名：execute_tool {工具名} {skill 名}。
	spanName := OpExecuteTool + " " + in.Name
	if skill != "" {
		spanName += " " + skill
	}
	ctx, span := o.tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindInternal), trace.WithAttributes(attrs...))
	th := &handle{kind: kindTool, span: span, start: time.Now(), name: in.Name, graphNS: compose_v2.GetGraphNS(ctx),
		callID: in.CallId, category: category, skill: skill, scope: scopeFrom(ctx)}
	th.scope.add(th)
	if th.scope != nil {
		th.scope.toolCalls.Add(1)
	}
	return withHandle(ctx, th), nil
}

// toolCategory 给工具调用分类：activate_skill 是 skill 激活（带上 skill 名）；业务工具实现了
// tool.Categorizer 的按它报的类别（retrieval / embedding / guardrail）。
func toolCategory(in *schema.ToolInput) (category, skill string) {
	if in.Name == agent_v2.ActivateSkillToolName {
		var a struct {
			Skill string `json:"skill"`
		}
		if json.Unmarshal([]byte(in.ArgumentsJSON), &a) == nil && a.Skill != "" {
			return "skill", a.Skill
		}
		return "", ""
	}
	if c, ok := in.Extra["category"].(string); ok {
		switch c {
		case "retrieval", "embedding", "guardrail":
			return c, ""
		}
	}
	return "", ""
}

func (h *agentHooks) OnToolEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	o := h.o
	if out == nil {
		return ctx, nil
	}
	th := handleFrom(ctx)
	if th == nil || th.kind != kindTool || (out.CallId != "" && th.callID != "" && th.callID != out.CallId) {
		th = scopeFrom(ctx).findTool(out.CallId)
	}
	if th == nil {
		return ctx, nil
	}
	r := result{outcome: OutcomeSuccess}
	var extra []attribute.KeyValue
	if res := out.Result; res != nil {
		if !res.Success {
			errType := res.ErrorCode
			if errType == "" {
				errType = "tool_error"
			}
			msg := res.Error
			if msg == "" && o.recordContent {
				// 工具没给错误说明时拿输出顶——输出是内容，只在打开内容采集时才写进 span。
				msg = res.Output
			}
			r = result{outcome: OutcomeError, errType: errType, errMsg: msg}
		}
		if o.recordContent {
			extra = append(extra, AttrToolCallResult.String(o.jsonText(res.Output)))
		}
	}
	th.scope.remove(th)
	if !o.finish(th, r, extra...) {
		return ctx, nil
	}
	o.recordToolDuration(ctx, th, info.Name, r)
	if th.skill != "" && r.outcome == OutcomeSuccess {
		o.inst.skillActivations.Add(ctx, 1, metric.WithAttributes(AttrAgentName.String(info.Name), AttrSkillName.String(th.skill)))
	}
	return ctx, nil
}

func (o *Observability) recordToolDuration(ctx context.Context, th *handle, agentName string, r result) {
	a := []attribute.KeyValue{AttrToolName.String(th.name), AttrToolType.String("function"), AttrAgentName.String(agentName)}
	if r.failed() {
		a = append(a, AttrErrorType.String(r.errType))
	}
	o.inst.toolDuration.Record(ctx, seconds(th.start), metric.WithAttributes(a...))
}

// OnError 在框架报告错误时收尾：模型 / 工具 span 在错误路径上等不到结束钩子，在这里以失败结束。一批工具
// 整体失败时框架交来的是节点的 ctx，分不出是哪一个，这批还开着的工具 span 一起以失败结束。agent / 节点 /
// 工作流 span 这里不碰：错误要是被处理掉了（重试成功、分支失败被兜住），按 OTel 的 recording-errors 指引
// 不该记；要是没被处理，它们结束时自己会以失败收尾。
func (h *agentHooks) OnError(ctx context.Context, info *hook.AgentInfo, err *schema.CommError) context.Context {
	o := h.o
	if err == nil {
		return ctx
	}
	r := result{outcome: OutcomeError, errType: err.Code, errMsg: err.Message}
	if r.errType == "" {
		r.errType = "_OTHER"
	}
	if ctx.Err() != nil {
		r = classify(ctx, errors.New(err.Message))
	}
	cur := handleFrom(ctx)
	switch {
	case cur != nil && cur.kind == kindModel:
		cur.scope.remove(cur)
		if o.finish(cur, r) {
			o.inst.inferenceDuration.Record(ctx, seconds(cur.start), metric.WithAttributes(
				append(inferenceLabels("", cur.requestModel, ""), AttrErrorType.String(r.errType))...))
		}
	case cur != nil && cur.kind == kindTool:
		cur.scope.remove(cur)
		if o.finish(cur, r) {
			o.recordToolDuration(ctx, cur, info.Name, r)
		}
	default:
		// 只有一批工具整体失败（tool_call_error，交来的是节点 ctx）才收尾这批还开着的工具 span。别的 tool_* 错误
		// 只关乎单个调用：比如第 i 个调用的开始钩子失败（tool_start_error）时，排在它前面的调用已经开了 span、
		// 照常执行，不能被一起记成失败。
		if err.Code == "tool_call_error" {
			for _, th := range scopeFrom(ctx).drain(kindTool) {
				if o.finish(th, r) {
					o.recordToolDuration(ctx, th, info.Name, r)
				}
			}
		}
	}
	return ctx
}

// runAttrs 是任务与会话的身份：crew.run.id（RunID）与 gen_ai.conversation.id（ThreadID）。
func runAttrs(ctx context.Context) []attribute.KeyValue {
	var a []attribute.KeyValue
	if sid := compose_v2.GetSessionID(ctx); sid != "" {
		a = append(a, AttrRunID.String(sid))
	}
	if conv := compose_v2.GetThreadID(ctx); conv != "" {
		a = append(a, AttrConversationID.String(conv))
	}
	return a
}

// resumeInputCount 数这次续跑调用方给了几条输入——按提问配对的答案与「用户这轮说了什么」两条通道都算。
// 只数前者的话，走后一条通道的续跑（Replan 的推荐形态）在 trace 上显示「0 输入」。
func resumeInputCount(rc *compose_v2.ResumeContext) int {
	n := len(rc.UserInputs)
	if rc.UserText != "" {
		n++
	}
	return n
}

func resumeInputMessages(rc *compose_v2.ResumeContext) []*schema.Message {
	msgs := make([]*schema.Message, 0, len(rc.UserInputs)+1)
	for _, m := range rc.UserInputs {
		if m != nil {
			msgs = append(msgs, m)
		}
	}
	if rc.UserText != "" {
		msgs = append(msgs, schema.UserMessage(rc.UserText))
	}
	return msgs
}
