// Package observability 用 OpenTelemetry 记录 crew-go 的执行：agent / 工作流 / 节点 / 模型调用 /
// 工具调用各一个 span，配套 GenAI 语义约定的指标与框架自己的 crew.* 指标。
//
// 本包只依赖 OTel API。TracerProvider / MeterProvider / 导出器 / Resource / 采样都归应用配置：
// 不传就用全局的（otel.SetTracerProvider 等），应用没配时 API 全是 no-op、零开销。
//
//	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter))   // 应用自己配 SDK
//	obs := observability.New(observability.WithTracerProvider(tp))
//	ctx, opts := obs.Install(ctx, agent_v2.WithName("assistant"), agent_v2.WithModel(m))
//	ag := agent_v2.NewBasicAgent(opts...)
//	events, err := ag.Run(ctx, sess, input)   // 必须用 Install 返回的 ctx
//
// span 的父子关系完全走 ctx：钩子在开始一侧把 span 放进返回的 ctx，框架把它交给这次调用里的一切
// （节点、模型调用、工具、嵌套 agent），结束一侧从 ctx 取回。应用在外面开的 span（HTTP 请求等）
// 自然成为 agent span 的父。
//
// 属性名与指标名见 semconv.go；设计说明见 doc/v2/observability.md。
package observability

import (
	"context"
	"log/slog"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
)

// ScopeName 是本包的 instrumentation scope 名（tracer / meter 的名字）。
const ScopeName = "crew-go/observability"

// Observability 是钩子与指标的持有者。并发安全，一个进程建一个即可。
type Observability struct {
	tracer        trace.Tracer
	meter         metric.Meter
	propagator    propagation.TextMapPropagator
	inst          *instruments
	recordContent bool
	redact        []*regexp.Regexp
	maxContent    int

	// interruptedAt 记每个挂起的 agent 是什么时候停下的，续跑时算等待时长（crew.interrupt.wait.duration）。
	// 键是 interruptKey(sid, ns, agent)；续跑取走即删，放弃的任务由 CleanupSession 或过期清掉。
	interruptedAt sync.Map
	interruptN    atomic.Int64

	// orchFired / halfInstallWarned：只接了业务级钩子、没接编排级钩子时喊一次（见 warnIfHalfInstalled）。
	orchFired         atomic.Bool
	halfInstallWarned atomic.Bool
}

// Option 配置 Observability。
type Option func(*Observability)

// WithTracerProvider 指定 TracerProvider；不传用 otel.GetTracerProvider()。
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *Observability) {
		if tp != nil {
			o.tracer = tp.Tracer(ScopeName)
		}
	}
}

// WithMeterProvider 指定 MeterProvider；不传用 otel.GetMeterProvider()。
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(o *Observability) {
		if mp != nil {
			o.meter = mp.Meter(ScopeName)
		}
	}
}

// WithPropagator 指定跨进程传 trace context 用的 propagator（A2A 消息元数据的注入 / 提取）。
// 不传用 W3C Trace Context + Baggage——不跟全局的走，全局默认是 no-op，那样 A2A 两端串不起来。
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(o *Observability) {
		if p != nil {
			o.propagator = p
		}
	}
}

// WithContentRecording 打开内容采集：模型的输入输出（gen_ai.input.messages / gen_ai.output.messages /
// gen_ai.system_instructions）、工具的参数与结果（gen_ai.tool.call.arguments / gen_ai.tool.call.result）、
// agent 的输入与最终答复。默认关——内容可能含个人信息，按 GenAI 约定是 opt-in。打开时建议配 WithRedactPatterns。
func WithContentRecording(enabled bool) Option {
	return func(o *Observability) { o.recordContent = enabled }
}

// WithRedactPatterns 在内容与错误信息写进 span 之前，把命中的片段换成 [REDACTED]。
func WithRedactPatterns(patterns ...*regexp.Regexp) Option {
	return func(o *Observability) {
		for _, p := range patterns {
			if p != nil {
				o.redact = append(o.redact, p)
			}
		}
	}
}

// WithMaxContentChars 限制采集内容里每一段文本的长度（按字符），超出部分截掉并标 …[truncated]。
// 默认 16384；<= 0 不限。
func WithMaxContentChars(n int) Option {
	return func(o *Observability) { o.maxContent = n }
}

// New 建一个 Observability。
func New(opts ...Option) *Observability {
	o := &Observability{maxContent: 16384}
	for _, opt := range opts {
		opt(o)
	}
	if o.tracer == nil {
		o.tracer = otel.GetTracerProvider().Tracer(ScopeName)
	}
	if o.meter == nil {
		o.meter = otel.GetMeterProvider().Meter(ScopeName)
	}
	if o.propagator == nil {
		o.propagator = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	}
	o.inst = newInstruments(o.meter)
	return o
}

// Tracer 返回本包用的 tracer，业务想在同一个 scope 下补自己的 span 时用。
func (o *Observability) Tracer() trace.Tracer {
	if o == nil {
		return otel.GetTracerProvider().Tracer(ScopeName)
	}
	return o.tracer
}

// Meter 返回本包用的 meter，业务自己的指标（成本、业务成功率等）从它建。
func (o *Observability) Meter() metric.Meter {
	if o == nil {
		return otel.GetMeterProvider().Meter(ScopeName)
	}
	return o.meter
}

// AgentHooks 返回业务级钩子（hook.Hooks），用 agent_v2.WithHooks 装上。
//
// 业务自己也有钩子时交给 inner：observability 在最外层、inner 按顺序串在里面，里面的钩子开始一侧失败（报错或
// 抛中断）时 observability 当场收掉刚开的 span（见 wrap.go）。别自己用 hook.NewChainHooks 把它跟别的钩子串：
// 那样后面的钩子开始失败时它的 span 收不了尾。nil 的 inner 被忽略。
func (o *Observability) AgentHooks(inner ...hook.Hooks) hook.Hooks {
	if o == nil {
		return hook.NoOpHooks{}
	}
	obs := &agentHooks{o: o}
	hs := make([]hook.Hooks, 0, len(inner))
	for _, h := range inner {
		if isNilIface(h) || isNoOpHooks(h) {
			continue
		}
		hs = append(hs, h)
	}
	if len(hs) == 0 {
		return obs
	}
	return &wrappedHooks{obs: obs, inner: hook.NewChainHooks(hs...)}
}

// OrchHooks 返回编排级钩子（agent_v2.OrchestrationHooks），用 agent_v2.WithCtxOrchHooks(ctx, …) 或
// RunOption agent_v2.WithOrchHooks 装上。不装就没有节点 / 工作流 span，压缩、存档、扇出、路由、
// 规划、fallback 这些框架事件的指标也不记。inner 的含义同 AgentHooks：业务的编排钩子交给它，别自己串。
func (o *Observability) OrchHooks(inner ...agent_v2.OrchestrationHooks) agent_v2.OrchestrationHooks {
	if o == nil {
		return agent_v2.NoOpOrchestrationHooks{}
	}
	obs := &orchHooks{o: o}
	hs := make([]agent_v2.OrchestrationHooks, 0, len(inner))
	for _, h := range inner {
		if isNilIface(h) || isNoOpOrchHooks(h) {
			continue
		}
		hs = append(hs, h)
	}
	if len(hs) == 0 {
		return obs
	}
	return &wrappedOrchHooks{obs: obs, inner: agent_v2.NewChainOrchHooks(hs...)}
}

// Install 一次接上两条钩子链：返回已装编排钩子的 ctx，和追加了业务钩子的 agent 构造选项。
// 拿不到 opts 就拿不到 ctx，没有只装一半的用法。ctx 上已有的编排钩子、opts 里已配的 WithHooks 都被包进
// observability 里面（同 AgentHooks / OrchHooks 的 inner），不会被顶掉。
//
// agent 在一处构造、ctx 在每个请求里另外准备的拓扑（如 a2a/server）收不进一次调用，用 AgentHooks +
// OrchHooks 两半各装各的。
//
// 顶不掉的范围到这两处为止：返回的 opts 之后再接 agent_v2.WithHooks(x) 会整个覆盖业务级钩子（WithHooks 是覆盖写），
// Run 时传 RunOption agent_v2.WithOrchHooks 会盖过 ctx 上的编排钩子——要加钩子就加在交给 Install 的 opts / ctx 上。
// 同一个 Observability 装了两层（Install 两次、Install 之外又 WithHooks(obs.AgentHooks())）时只记一份。
func (o *Observability) Install(ctx context.Context, opts ...agent_v2.Option) (context.Context, []agent_v2.Option) {
	if o == nil {
		return ctx, opts
	}
	ctx = agent_v2.WithCtxOrchHooks(ctx, o.OrchHooks(agent_v2.GetCtxOrchHooks(ctx)))
	merged := make([]agent_v2.Option, 0, len(opts)+1)
	merged = append(merged, opts...)
	// 排在最后：apply 到这里时 c.Hooks 已是业务配的那一份，把它包进来。
	merged = append(merged, func(c *agent_v2.Config) { c.Hooks = o.AgentHooks(c.Hooks) })
	return ctx, merged
}

// isNoOpHooks / isNoOpOrchHooks 认出什么都不做的钩子（agent 默认的 Config.Hooks 就是 NoOpHooks），包进来只多一层调用。
func isNoOpHooks(h hook.Hooks) bool {
	switch h.(type) {
	case hook.NoOpHooks, *hook.NoOpHooks:
		return true
	}
	return false
}

func isNoOpOrchHooks(h agent_v2.OrchestrationHooks) bool {
	switch h.(type) {
	case agent_v2.NoOpOrchestrationHooks, *agent_v2.NoOpOrchestrationHooks:
		return true
	}
	return false
}

// isNilIface 判 nil 与 typed nil（业务 `var h *MyHooks; obs.AgentHooks(h)`）。
func isNilIface(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface:
		return rv.IsNil()
	}
	return false
}

// CleanupSession 丢掉某个会话还没续跑的挂起记录。放弃一个挂起的任务时调；不调也不会无界增长
// （超过一天的记录在后续挂起时被顺手清掉），只是那次等待时长不再记。
func (o *Observability) CleanupSession(sessionID string) {
	if o == nil || sessionID == "" {
		return
	}
	prefix := sessionID + "\x00"
	o.interruptedAt.Range(func(k, _ any) bool {
		if s, _ := k.(string); len(s) >= len(prefix) && s[:len(prefix)] == prefix {
			o.interruptedAt.Delete(k)
		}
		return true
	})
}

// PendingInterrupts 返回还在等续跑的挂起记录数。挂起的任务被放弃又没人调 CleanupSession 时它会涨，
// 可以拿来监控泄漏。
func (o *Observability) PendingInterrupts() int {
	if o == nil {
		return 0
	}
	n := 0
	o.interruptedAt.Range(func(_, _ any) bool { n++; return true })
	return n
}

// TraceIDFromContext 返回 ctx 里当前 span 的 trace ID（32 位十六进制），没有时返回空串。
func TraceIDFromContext(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.HasTraceID() {
		return ""
	}
	return sc.TraceID().String()
}

func interruptKey(sid, ns, agent string) string { return sid + "\x00" + ns + "\x00" + agent }

// dropInterruptsUnder 丢掉 (sid, ns) 这一层自己（agent 名为 agent 的那条）以外、ns 子树里的挂起记录。
// 本层那条留给调用方 LoadAndDelete（要用它算等待时长）。
func (o *Observability) dropInterruptsUnder(sid, ns, agent string) {
	prefix := sid + "\x00"
	self := interruptKey(sid, ns, agent)
	o.interruptedAt.Range(func(k, _ any) bool {
		key, _ := k.(string)
		rest, ok := strings.CutPrefix(key, prefix)
		if !ok || key == self {
			return true
		}
		if kns, _, found := strings.Cut(rest, "\x00"); found && compose_v2.IsNSInSubtree(ns, kns) {
			o.interruptedAt.Delete(k)
		}
		return true
	})
}

// markInterrupted 记一次挂起。每 256 次顺手清一遍超过一天的旧记录（放弃了没人续跑的任务）。
func (o *Observability) markInterrupted(key string, at time.Time) {
	o.interruptedAt.Store(key, at)
	if o.interruptN.Add(1)%256 != 0 {
		return
	}
	cutoff := at.Add(-24 * time.Hour)
	o.interruptedAt.Range(func(k, v any) bool {
		if t, ok := v.(time.Time); ok && t.Before(cutoff) {
			o.interruptedAt.Delete(k)
		}
		return true
	})
}

// warnIfHalfInstalled 在 OnAgentEnd 时检查编排钩子跑过没有：两条链装在不同地方，漏装编排那条不报错，
// 只是节点 span 与框架事件指标静默缺失。每个实例只喊一次。判据是「编排钩子自己跑过没有」，不是
// 「ctx 里有没有」——编排钩子也可以走 RunOption。
func (o *Observability) warnIfHalfInstalled(ctx context.Context) {
	if o.orchFired.Load() || o.halfInstallWarned.Swap(true) {
		return
	}
	slog.WarnContext(ctx, "observability: 只接了业务级钩子，编排级钩子没接上——节点 / 工作流 span 与压缩、存档、"+
		"扇出、路由等框架指标不会记录。用 obs.Install(ctx, opts...) 一次接上两条；agent 与 ctx 分开装配时"+
		"（如 a2a/server）记得 agent_v2.WithCtxOrchHooks(ctx, obs.OrchHooks()) 那一半")
}
