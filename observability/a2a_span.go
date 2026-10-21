package observability

import (
	"context"

	"crew-go/compose_v2"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// a2a 跨语言 trace span（跨语言 a2a trace 串联协议）：
//
//	本地 agent span → a2a_client span → (wire) → a2a_server span → 远端 agent/model/tool span
//
// client span 命名 gen_ai.a2a_client.<remoteName>，server span 命名
// gen_ai.a2a_server.message_send / message_stream，内部 span 挂 server span 下。
//
// 这俩公开 API 是 a2a 包接入 observability 的唯一 span 入口（依赖方向 a2a →
// observability 无环——observability 永不 import a2a）。trace context 的注入/提取
// 走纯函数 InjectToMetadata/ExtractFromMetadata（propagation.go），无需 *Observability；
// 建 span 走这俩（需 *Observability）。

// a2a span trace attr key（crew.a2a.* prefix）——context_id/task_id 是高基数，
// 只进 span attr 不进 metric series label。
const (
	attrA2AOperation  = "crew.a2a.operation"
	attrA2ARemoteName = "crew.a2a.remote_name"
	attrA2AMethod     = "crew.a2a.method"
	attrA2ATransport  = "crew.a2a.transport"
	attrA2AContextID  = "crew.a2a.context_id"
	attrA2ATaskID     = "crew.a2a.task_id"
)

// ── server span ──

// A2AServerSpan 是 server 侧 a2a_server span 句柄（a2a/server 持有，End 收尾）。
type A2AServerSpan struct {
	o   *Observability
	ctx context.Context
}

// StartA2AServerSpan 在 server 入口建 gen_ai.a2a_server.{operation} span。
//
// 前置：ctx 须已 WithTraceContext（a2a server 从入站 Message.metadata 提取的 remote
// TraceContext）+ WithSessionIDCtx —— sessionByIDSeeded 据此用 remote traceID seed
// 本 session（顶层挂 remote parent）。本 span 即 trace 内顶层；建完把
// sess.rootParentSpanID 更新成本 span 的 spanID，让后续内部 agent/model/tool span
// 自动挂到 a2a_server span 下（协议拓扑：client → a2a_server → 内部 span）。
//
// operation: "message_send"（同步）/ "message_stream"（流式）。未接 observability /
// 未采样时返 no-op 句柄（End 安全）。
//
// **调用约束**：per-sid 单次调用。本函数写 sess.rootParentSpanID（普通 string 无锁），
// 与 startSpan 读它在同 sid 并发时是 data race。a2a server executor 满足约束（每 task
// 独立 sid + 写先于 spawn agent goroutine 读 + busy 串行 Run/Resume），故生产安全；自定义
// caller 不得在共享 sid 上并发调本函数。
func (o *Observability) StartA2AServerSpan(ctx context.Context, operation, contextID, taskID string) *A2AServerSpan {
	if o == nil || o.exporter == nil {
		return &A2AServerSpan{}
	}
	attrs := []*commonpb.KeyValue{
		strKV(attrA2AOperation, operation),
		strKV(attrA2AContextID, contextID),
		strKV(attrA2ATaskID, taskID),
	}
	// kind="agent" 让本 span 走 startSpan 顶层分支（parentSpanID="" → 挂 remote
	// parent）+ 与内部 agent span 同 kind 但 name="a2a_server" 不同（registryKey 不撞）。
	entry := o.startSpan(ctx, "agent", "a2a_server", "gen_ai.a2a_server."+operation,
		"a2a_server", 0, "", "", "", attrs)
	if entry == nil {
		return &A2AServerSpan{} // 未采样
	}
	// 后续顶层内部 agent span 挂到本 a2a_server span 下（而非直接挂 remote parent）。
	// 建时（上面 startSpan）本 span 已挂 remote；此处把"后续顶层 parent"指向本 span。
	o.session(ctx).rootParentSpanID = string(entry.span.SpanId)
	return &A2AServerSpan{o: o, ctx: ctx}
}

// End 结束 a2a_server span。status: "success" / "error" / "cancelled" / "timeout"。
func (s *A2AServerSpan) End(status, errType, errMsg string) {
	if s.o == nil {
		return
	}
	s.o.endSpan(s.ctx, "agent", "a2a_server", 0, "", status, errType, errMsg, "", "", nil)
}

// ── client span ──

// A2AClientSpan 是 client 侧 a2a_client span 句柄。
type A2AClientSpan struct {
	o          *Observability
	ctx        context.Context
	remoteName string
	tc         TraceContext // 本 span 身份，注入 outbound Message.metadata
}

// StartA2AClientSpan 在 client 发请求前建 gen_ai.a2a_client.{remoteName} span。
//
// 协议：仅当有有效父 span（当前 session 有活跃 agent span）时才建——无父返 no-op
// 句柄（TraceContext().IsValid()=false，caller 据此不注入 metadata，避免孤立 client span）。
//
// method: A2A 方法（"message_send"/"message_stream"）；transport: "jsonrpc" 等。
func (o *Observability) StartA2AClientSpan(ctx context.Context, remoteName, method, transport, contextID, taskID string) *A2AClientSpan {
	if o == nil || o.exporter == nil {
		return &A2AClientSpan{}
	}
	sess := o.session(ctx)
	// 沿 ParentNS 上溯找父 agent span：RemoteAgent 经 AgentAsTool / RunSubAgent（主用法）
	// 在 child ns，但 host agent span 在 parent ns —— 精确匹配 child ns 找不到会断链
	//（client span 不建 → metadata 不注入 → 远端自起 trace）。上溯到祖先层命中 host span。
	parentSpanID, parentNodeID := sess.findActiveAgentSpanFrom(compose_v2.GetGraphNS(ctx))
	if parentSpanID == "" {
		return &A2AClientSpan{} // 无有效父 → 不建孤立 span（协议）
	}
	attrs := []*commonpb.KeyValue{
		strKV(attrA2ARemoteName, remoteName),
		strKV(attrA2AMethod, method),
		strKV(attrA2ATransport, transport),
		strKV(attrA2AContextID, contextID),
		strKV(attrA2ATaskID, taskID),
	}
	entry := o.startSpan(ctx, "tool", "a2a_client", "gen_ai.a2a_client."+remoteName,
		"a2a_client:"+remoteName, 0, "", parentSpanID, parentNodeID, attrs)
	if entry == nil {
		return &A2AClientSpan{}
	}
	return &A2AClientSpan{
		o: o, ctx: ctx, remoteName: remoteName,
		tc: TraceContext{
			TraceID: sess.traceID, SpanID: string(entry.span.SpanId),
			Sampled: sess.sampled, AppName: o.cfg.Resource.AppName, NextIdx: 1,
		},
	}
}

// TraceContext 返本 client span 身份——caller 注入 outbound Message.metadata。
// no-op 句柄（无父/未采样）返 IsValid()=false，caller 不应注入。
func (s *A2AClientSpan) TraceContext() TraceContext { return s.tc }

// End 结束 a2a_client span。status: "success" / "error" / "timeout" / "cancelled"。
func (s *A2AClientSpan) End(status, errType, errMsg string) {
	if s.o == nil {
		return
	}
	s.o.endSpan(s.ctx, "tool", "a2a_client:"+s.remoteName, 0, "", status, errType, errMsg, "", "", nil)
}
