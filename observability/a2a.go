package observability

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// A2A 跨进程 trace：client 建一个 invoke_agent CLIENT span，把 trace context（W3C traceparent /
// tracestate / baggage）写进出站 Message.metadata；server 从入站 metadata 取出来作父，建 SERVER span，
// 远端 agent 的整棵 span 树挂在它下面。走 metadata 而不是 HTTP 头，是因为 A2A 的传输可以不是 HTTP，
// 而且 a2asrv 的执行 ctx 跟 HTTP 请求是断开的。

// InjectToMetadata 把 ctx 里的 trace context 写进 md（A2A Message.metadata）。md 为 nil 时什么也不做。
func (o *Observability) InjectToMetadata(ctx context.Context, md map[string]any) {
	if o == nil || md == nil {
		return
	}
	carrier := propagation.MapCarrier{}
	o.propagator.Inject(ctx, carrier)
	for k, v := range carrier {
		md[k] = v
	}
}

// ExtractFromMetadata 从 md（A2A Message.metadata）里取出 trace context，返回带远端父 span 的 ctx。
// md 里没有或格式不对时原样返回 ctx。
func (o *Observability) ExtractFromMetadata(ctx context.Context, md map[string]any) context.Context {
	if o == nil || len(md) == 0 {
		return ctx
	}
	carrier := propagation.MapCarrier{}
	for _, k := range o.propagator.Fields() {
		if s, ok := md[k].(string); ok && s != "" {
			carrier[k] = s
		}
	}
	if len(carrier) == 0 {
		return ctx
	}
	return o.propagator.Extract(ctx, carrier)
}

// A2AClientSpan 是调用远端 agent 的那个 span。
type A2AClientSpan struct {
	o    *Observability
	span trace.Span
	h    *handle
}

// StartA2AClientSpan 在发 A2A 请求前建 invoke_agent CLIENT span，返回带它的 ctx——拿这个 ctx 调
// InjectToMetadata，远端才会挂在它下面。o 为 nil 时返回 ctx 与一个什么都不做的句柄。
func (o *Observability) StartA2AClientSpan(ctx context.Context, remoteName, method, transport, contextID, taskID string) (context.Context, *A2AClientSpan) {
	if o == nil {
		return ctx, &A2AClientSpan{}
	}
	attrs := []attribute.KeyValue{AttrOperationName.String(OpInvokeAgent), AttrProviderName.String("a2a"),
		AttrAgentName.String(remoteName), AttrA2AMethod.String(method)}
	if transport != "" { // 不知道用的是哪种传输时不记，记错比不记更糟
		attrs = append(attrs, AttrA2ATransport.String(transport))
	}
	if contextID != "" {
		attrs = append(attrs, AttrConversationID.String(contextID))
	}
	if taskID != "" {
		attrs = append(attrs, AttrA2ATaskID.String(taskID))
	}
	ctx, span := o.tracer.Start(ctx, OpInvokeAgent+" "+remoteName, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
	return ctx, &A2AClientSpan{o: o, span: span, h: &handle{span: span, start: time.Now(), name: remoteName}}
}

// End 结束 client span，ctx 是发请求用的那个：nil 是成功；远端要更多输入（中断）是挂起；ctx 被取消 / 到期
// 是 cancelled / timeout；其余是失败（error.type=a2a_remote_error）。远端的错误详情不写进 span，它可能含对端的
// 敏感信息。
// SetMethod 改记这次调用最终用的 A2A 方法：span 要在调用方决定怎么跟远端交互之前开（决定的过程本身也要发请求，
// 那些请求要落在 span 里），定下之后再补上。
func (s *A2AClientSpan) SetMethod(method string) {
	if s == nil || s.o == nil || s.span == nil {
		return
	}
	s.span.SetAttributes(AttrA2AMethod.String(method))
}

func (s *A2AClientSpan) End(ctx context.Context, err error) {
	if s == nil || s.o == nil {
		return
	}
	r := classify(ctx, err)
	if r.outcome == OutcomeError && r.errType == "_OTHER" {
		r.errType = "a2a_remote_error"
	}
	r.errMsg = ""
	s.o.finish(s.h, r)
}

// A2AServerSpan 是 server 处理一次 A2A 请求的 span，覆盖 agent 执行到终态。
type A2AServerSpan struct {
	o *Observability
	h *handle
}

// StartA2AServerSpan 建 SERVER span，返回带它的 ctx——agent 要拿这个 ctx 跑，它的 span 才挂在下面。
// 调用前先用 ExtractFromMetadata 把远端父放进 ctx。o 为 nil 时返回 ctx 与一个什么都不做的句柄。
func (o *Observability) StartA2AServerSpan(ctx context.Context, operation, contextID, taskID string) (context.Context, *A2AServerSpan) {
	if o == nil {
		return ctx, &A2AServerSpan{}
	}
	attrs := []attribute.KeyValue{AttrA2AOperation.String(operation)}
	if contextID != "" {
		attrs = append(attrs, AttrConversationID.String(contextID))
	}
	if taskID != "" {
		attrs = append(attrs, AttrA2ATaskID.String(taskID))
	}
	ctx, span := o.tracer.Start(ctx, "a2a.server "+operation, trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(attrs...))
	return ctx, &A2AServerSpan{o: o, h: &handle{span: span, start: time.Now()}}
}

// End 结束 server span。outcome 取 crew.outcome 的值（success / error / cancelled / blocked …），
// errType 只在失败时有意义；错误详情不写进 span。
func (s *A2AServerSpan) End(outcome, errType string) {
	if s == nil || s.o == nil {
		return
	}
	r := result{outcome: outcome, errType: errType}
	if r.failed() && r.errType == "" {
		r.errType = "_OTHER"
	}
	s.o.finish(s.h, r)
}
