package compose_v2

import (
	"context"
	"log/slog"

	"crew-go/internal/panicerr"
)

// OrchestrationHooks 是 graph 执行生命周期的 5 个观测点回调。
//
// 设计对齐现有 compose.OrchestrationHooks（撞 audit F8 [MED]）：保留 5 个核心
// 回调时机；channel-based 模型下回调入参从 *GraphState 改为 *ChannelManager
// 的只读视图（Channels）。
//
// **线程安全**：fan-out 多 goroutine 并发执行时，OnNodeStart / OnNodeEnd /
// OnNodeEvent 会从多个 goroutine 并发调用——hook 实现须保证线程安全（典型：
// 内部 mutex 保护写入）。
//
// **panic 语义**：业务钩子 panic 由框架兜住（分工见 `panic_recovery.go`），不让 Run goroutine 崩。
// 兜住之后怎么处置，看钩子跑在它报告的那段工作之前还是之后：
//   - OnGraphStart / OnNodeStart 在工作之前：panic 转成这次 Run / 这个节点的错误（同返回 error）。
//   - OnNodeEnd / OnGraphEnd 在工作之后：节点体（图）失败时 panic 转成那个节点（这次 Run）的错误；
//     成功时就地兜住、带栈记一条 ERROR，结果与写入照常提交。
//   - OnNodeEvent 只作观测：任何时候 panic 都就地兜住、记 ERROR（节点里发的事件、Run 级的存档事件都是）。
//
// 理由是钩子出错不得让已经做完的工作再做一次：成功之后的钩子 panic 曾经一律转成错误，失败档被续跑或重试
// 捡起，已经做完的节点体重跑、副作用重放（实测同一个节点体跑 3～4 次）。一个钩子 panic 时，链里排在它
// 后面的钩子不再调（[ChainOrchHooks] 不逐个隔离）。
//
// **ctx-based 协议** (audit² 改): 每个 hook 方法返 context.Context, 入参 ctx 也
// 由 hook 方法接收. framework 内自动 propagate 给下游 (下一个
// hook / 节点 / sub-call). 业务 hook 内可 `ctx = context.WithValue(ctx, spanKey{}, span)`
// 让后续 hook 拿到 (典型 OTel trace 模式).
//
// **错误传播**：
//   - OnGraphStart/OnNodeStart 返 (ctx, error) → error 时 graph 终止 (基础设施错)
//   - OnGraphEnd/OnNodeEnd 返 ctx (监听语义, 不拦截执行)
//   - OnNodeEvent 返 ctx (单事件实时投递, 不拦截)
//
// **配对承诺** (修 Bug #4 对齐设计):
//   - OnNodeStart 返 error 时仍会调 OnNodeEnd 配对——hook 实现者用 start/end
//     管理 tracing span / metric inc-dec / 资源 acquire-release, 缺 end 会泄漏
type OrchestrationHooks interface {
	// OnGraphStart 在 graph 开始执行前调用一次.
	// 返 error 终止 graph 执行 (OnGraphEnd 仍会调).
	OnGraphStart(ctx context.Context, chs Channels) (context.Context, error)

	// OnGraphEnd 在 graph 完成后调用一次 (成功或失败). err 是 Run 返回值.
	//
	// panic：图成功跑完时就地兜住、带栈记一条 ERROR，Run 照常返回 nil——这一刻图体已经成功收尾、成功路径的
	// cp 清理随后照跑，转成错误的话调用方拿到失败而存档里没有可续的进度，重试整图从头重跑，嵌套时父图把整个
	// 子图重跑一遍（曾经如此，实测）。图失败时 panic 并到 Run 的错误上。
	OnGraphEnd(ctx context.Context, chs Channels, err error) context.Context

	// OnNodeStart 在节点 NodeFunc 调用前立即调用.
	// 返 error 视为基础设施错——节点跳过执行, OnNodeEnd 仍配对调用.
	OnNodeStart(ctx context.Context, nodeID NodeID, chs Channels) (context.Context, error)

	// OnNodeEnd 在节点 NodeFunc 返回后立即调用. nodeErr = NodeFunc 的返回值
	// (含业务错 / InterruptError / nil).
	//
	// panic：节点体成功时就地兜住、带栈记一条 ERROR，节点结果与写入照常提交；节点体失败时转成这个节点的错误.
	OnNodeEnd(ctx context.Context, nodeID NodeID, chs Channels, nodeErr error) context.Context

	// OnNodeEvent 在节点内主动发出 event 时实时触发（节点内流式事件投递）.
	//
	// **只作观测**：在这里做出的上行信号不生效——ctx 上没有收信号处，[EmitSignal] 返回 false，
	// 转交（agent_v2.HandoffTo）报错。要做决定就在发事件的那段代码里直接做：事件不带它是从哪一段执行
	// 发出来的，在这里做出的决定归不到它该归的那一段。同理别在这里调起嵌套调用、提问或记节点私有态——
	// 续跑时被跳过的那些段不会再发事件，这里的次序跨续跑不稳定。返回的 ctx 不被使用。它 panic 时就地兜住、
	// 带栈记一条 ERROR，执行照常往下走。
	//
	// nodeID 与 ctx：节点体里发的事件，nodeID 是那个节点、ctx 是节点的（带着 OnNodeStart 推的值）；条件边、
	// 节点级钩子、图级钩子里发的是图级事件，nodeID 为空、ctx 是图的（带着 OnGraphStart 推的值——OnGraphStart
	// 钩子自己发的那一刻还没有，拿到的是调它之前的）。
	OnNodeEvent(ctx context.Context, nodeID NodeID, event Event) context.Context
}

// Event 是节点内主动发出的观测事件——典型场景 streaming chunk / 进度报告。
//
// 简化版（Phase 1.1）：仅 Type + Content + Metadata 三字段。Phase 1.2 扩展
// typed event 支持。
type Event struct {
	Type     string
	Content  string
	Metadata map[string]any
}

// NoOpOrchestrationHooks 是空实现——业务嵌入此 struct 只覆盖关心的回调。
//
//	type myHooks struct {
//	    compose_v2.NoOpOrchestrationHooks
//	}
//	func (h *myHooks) OnNodeEnd(ctx, id, chs, err) { /* my logic */ }
type NoOpOrchestrationHooks struct{}

func (NoOpOrchestrationHooks) OnGraphStart(ctx context.Context, _ Channels) (context.Context, error) {
	return ctx, nil
}
func (NoOpOrchestrationHooks) OnGraphEnd(ctx context.Context, _ Channels, _ error) context.Context {
	return ctx
}
func (NoOpOrchestrationHooks) OnNodeStart(ctx context.Context, _ NodeID, _ Channels) (context.Context, error) {
	return ctx, nil
}
func (NoOpOrchestrationHooks) OnNodeEnd(ctx context.Context, _ NodeID, _ Channels, _ error) context.Context {
	return ctx
}
func (NoOpOrchestrationHooks) OnNodeEvent(ctx context.Context, _ NodeID, _ Event) context.Context {
	return ctx
}

// ChainOrchHooks 把多个 OrchestrationHooks 串成一个——业务想同时挂 SSE 适配 +
// audit 日志 + metrics 等多 hook 时用。对齐 hook.ChainHooks 设计.
//
// **panic 语义**：链里每个钩子直接调、不逐个隔离——一个钩子 panic 时排在它后面的不再调，panic 交给
// 调用这条链的地方处置（转成错误还是就地兜住记日志，见 [OrchestrationHooks]「panic 语义」）。
//
// 用法:
//
//	chain := compose_v2.NewChainOrchHooks(sseAdapter, auditLogger, metricCollector)
//	compose_v2.Run(ctx, g, cm, compose_v2.WithHooks(chain))
type ChainOrchHooks struct {
	hooks []OrchestrationHooks
}

// NewChainOrchHooks 创建多 OrchHooks 链。nil / typed nil hook 自动跳过。
//
// **audit²⁸ HIGH-1 fix**: 原 `if h != nil` 漏 Go interface "typed nil" — 业务
// 传 `var h *MyOrchHooks` typed nil 时 interface header 持 type tag → `h != nil`
// 为 true → safeOn*(h) 内 `h == nil` 也为 true 不预拦 → 调 h.OnXxx 远 stack nil
// deref panic. 改用 `isFrameworkNil` 配 reflect.IsNil 跨 5 包协议一致.
func NewChainOrchHooks(hooks ...OrchestrationHooks) *ChainOrchHooks {
	filtered := make([]OrchestrationHooks, 0, len(hooks))
	for _, h := range hooks {
		if isFrameworkNil(h) {
			continue // 静默过滤 nil / typed nil — 与 hook.NewChainHooks 协议对称
		}
		filtered = append(filtered, h)
	}
	return &ChainOrchHooks{hooks: filtered}
}

// **ctx propagation 协议**: ChainOrchHooks 内 ctx 链式串 — 每个 hook 返的 ctx
// 给下一个 hook 用. 业务在 hook A 内 push 的 span 让 hook B 拿到.
//
// **panic 语义**: 一个 hook panic 中断剩余链，处置见 [OrchestrationHooks]「panic 语义」.
func (c *ChainOrchHooks) OnGraphStart(ctx context.Context, chs Channels) (context.Context, error) {
	// **audit⁴ B MED-1 fix**: 早 return 用入参 ctx (跟 OnNodeStart + ChainHooks B2 同款).
	// 之前 `return ctx, err` 看似回入参实际回 `ctx = newCtx` 后的 hook A 已 mutate 值
	// — hook B fail 时 caller 拿到含 hook A push span 的 ctx, 跨 ChainHooks 行为不一致.
	inCtx := ctx
	for _, h := range c.hooks {
		newCtx, err := h.OnGraphStart(ctx, chs)
		if err != nil {
			return inCtx, err // fail-fast: start 错中断剩余 handlers + 回入参 ctx
		}
		ctx = newCtx
	}
	return ctx, nil
}

func (c *ChainOrchHooks) OnGraphEnd(ctx context.Context, chs Channels, runErr error) context.Context {
	for _, h := range c.hooks {
		ctx = h.OnGraphEnd(ctx, chs, runErr)
	}
	return ctx
}

func (c *ChainOrchHooks) OnNodeStart(ctx context.Context, id NodeID, chs Channels) (context.Context, error) {
	// **audit³ B2 fix**: 错误路径回入参 ctx (跟 OnGraphStart + ChainHooks 6 method 同款)
	inCtx := ctx
	for _, h := range c.hooks {
		newCtx, err := h.OnNodeStart(ctx, id, chs)
		if err != nil {
			return inCtx, err
		}
		ctx = newCtx
	}
	return ctx, nil
}

func (c *ChainOrchHooks) OnNodeEnd(ctx context.Context, id NodeID, chs Channels, nodeErr error) context.Context {
	for _, h := range c.hooks {
		ctx = h.OnNodeEnd(ctx, id, chs, nodeErr)
	}
	return ctx
}

// OnNodeEvent 依次交给链里每个钩子，每个拿到的都摘掉了收信号处（OnNodeEvent 只作观测，见接口）。前一个钩子
// 返回的 ctx 照常交给后一个，但前一个返回的可能是别处存下的 ctx（比如它在 OnNodeStart 存的、带着节点的收信号
// 处）——曾经原样交下去，后一个钩子在里面做的决定照样交到了调用方（审计实测）。
func (c *ChainOrchHooks) OnNodeEvent(ctx context.Context, id NodeID, ev Event) context.Context {
	for _, h := range c.hooks {
		ctx = h.OnNodeEvent(eventHookCtx(ctx), id, ev)
	}
	return ctx
}

// audit² 改: 删 safeOrchHooks wrapper + safeOnXxx helper —
// per-hook panic isolation 是过度设计. 框架在调用这条链的地方兜住业务 hook panic（分工见
// panic_recovery.go）. ChainOrchHooks 内 for 循环直接调 raw.

// ── 节点内事件发送 helper ──

// EventSink 是 NodeFunc 内主动发事件的接口——通过 ctx 注入。
//
// 用法：
//
//	if sink := compose_v2.GetEventSink(ctx); sink != nil {
//	    sink.Send(compose_v2.Event{Type: "chunk", Content: "hello"})
//	}
type EventSink interface {
	Send(ev Event)
}

type eventSinkKey struct{}

// withEventSink 是 framework 内部 ctx 注入 helper, runNodeWithHooks 在节点入口
// 调本函数挂 nodeEventSink. 业务侧用 WithEventSink (公开版本).
func withEventSink(ctx context.Context, sink EventSink) context.Context {
	if sink == nil {
		return ctx
	}
	return context.WithValue(ctx, eventSinkKey{}, sink)
}

// WithEventSink 把 sink 挂到 ctx 上，跟 GetEventSink 对称。
//
// 框架在 [Run] 入口与每个节点入口都挂上自己的 sink（转发给 OrchestrationHooks.OnNodeEvent），在那之前
// 挂的被遮住；采纳钩子返回的 ctx 时 sink 也取框架自己的（它是执行状态，见 [CallHook]）。所以：
//
//   - 在 Run 之外直接调框架组件（fallback 模型、压缩函数、EmitCompactionEvent……）时，用它收这些组件
//     发的事件，测试同理；
//   - 节点体里自己再派生一层，对之后直接调用的组件照样生效（只截获那一段）；
//   - 接管不了整张图的事件流——要接管就实现 OnNodeEvent。
func WithEventSink(ctx context.Context, sink EventSink) context.Context {
	return withEventSink(ctx, sink)
}

// GetEventSink 从 ctx 取 EventSink——nil 安全（hook 未设时返 nil sink）。
func GetEventSink(ctx context.Context) EventSink {
	if sink, ok := ctx.Value(eventSinkKey{}).(EventSink); ok {
		return sink
	}
	return nil
}

// eventHookCtx 是 OnNodeEvent 拿到的 ctx：给定的 ctx 摘掉收信号处——OnNodeEvent 只作观测，在里面做出的
// 上行信号不生效（[EmitSignal] 返回 false，转交报错）。
//
// 决定在发事件的那段代码里直接做。事件经 sink 转发、`Send` 不带 ctx，OnNodeEvent 分不出事件是从哪一段
// 执行发出来的（一个 tool_call、一次嵌套调用、一次钩子调用……），在那里做出的决定归不到它该归的那一段：
// 失败的调用里做出的照样交出、暂停续跑之后丢失、同一批里谁赢随调度（均实测）。曾经在各段的入口按具体
// 类型重绑 sink 去补，业务自己包一层转发的 sink、钩子里再发一次事件就又漏了（同样实测）。
func eventHookCtx(ctx context.Context) context.Context { return withSignalScope(ctx, nil) }

// graphEventSink 建图级的 EventSink（nodeID 为空）：条件边、节点级钩子、图级钩子里发的事件经它送达
// OnNodeEvent，带着 ctx（摘掉收信号处）。
func graphEventSink(ctx context.Context, hooks OrchestrationHooks, graphNS string) *nodeEventSink {
	return &nodeEventSink{ctx: eventHookCtx(ctx), hooks: hooks, nodeID: "", graphNS: graphNS}
}

// nodeEventSink 是 framework 层 EventSink 实现——把节点 send 的 event 转发到
// OrchestrationHooks.OnNodeEvent。每节点入口创建（含 nodeID 闭包 + 缓存 graphNS）。ctx 是交给
// OnNodeEvent 的那个，建的时候已经经 eventHookCtx 摘掉了收信号处。
type nodeEventSink struct {
	ctx     context.Context
	hooks   OrchestrationHooks
	nodeID  NodeID
	graphNS string // 节点入口时 ctx 内 graphNS 快照——避免每次 Send 走 ctx.Value lookup
}

func (s *nodeEventSink) Send(ev Event) {
	if s == nil || s.hooks == nil {
		return
	}
	// 自动注入 graph_ns 到 ev.Metadata——让前端 SSE 流可按 ns 分通道展示父子嵌套.
	// 业务自填 graph_ns 时不覆盖 (业务优先, 让自定义 ns 维度生效).
	//
	// **浅拷贝业务 map 避 mutate**：业务可能复用同 map 实例多次 Send (反模式但合法)——
	// 直接 `ev.Metadata["graph_ns"] = ...` 会污染业务持有的 map, 第 N 次 Send 时
	// 上次注入残留让"业务自填取胜"判断错误. 浅拷贝代价小但语义干净.
	if s.graphNS != "" {
		m := make(map[string]any, len(ev.Metadata)+1)
		for k, v := range ev.Metadata {
			m[k] = v
		}
		if _, exists := m["graph_ns"]; !exists {
			m["graph_ns"] = s.graphNS
		}
		ev.Metadata = m
	}
	// OnNodeEvent 只作观测：它 panic 时就地兜住、带栈记一条 ERROR，Send 照常返回、节点体接着跑。转成节点的错误的话
	// 节点体在这里中途失败，失败档被续跑或重试捡起，Send 之前已经做过的副作用重放（实测）。
	// EventSink.Send 是 fire-and-forget 不收 ctx (业务 NodeFunc 已持 ctx).
	defer func() {
		if r := recover(); r != nil {
			panicerr.Log(s.ctx, slog.LevelError, "compose_v2: OnNodeEvent 钩子 panic（这个事件没投递完，节点照常往下跑）", panicerr.New(r),
				"node", s.nodeID, "event_type", ev.Type)
		}
	}()
	_ = s.hooks.OnNodeEvent(s.ctx, s.nodeID, ev)
}

// ── RunOption 集成 ──

// WithHooks 配置 graph 执行的 OrchestrationHooks. 多次调用后者覆盖前者.
//
// 单 hooks instance 适用 90% 场景. 多 hooks 链用 NewChainOrchHooks.
func WithHooks(hooks OrchestrationHooks) RunOption {
	return func(o *runOptions) {
		o.hooks = hooks
	}
}

// ── ctx-aware OrchHooks 协议 ──
//
// 嵌套场景（PlanAgent 调子 BasicAgent / SequentialAgent 嵌入子 graph）下，父
// graph 的 OrchHooks 应自动透传给子 graph——否则 streaming 跨子图边界丢失。
//
// 设计：
//   - Run 入口若 RunOption 显式传 OrchHooks → 注入 ctx 让下游可见
//   - 业务 NodeFunc 调子 agent 时透传 ctx 即可，无需手动 push hooks
//   - GetOrchHooks 让下游编排 wrapper 决定 hook fallback：RunOption > ctx
//
// **vs 现有 RunOption**：RunOption 是显式给某次 Run 的入参；ctx-aware helper
// 是把 hooks 沿 ctx 链透传给所有下游 sub-agent.Run 调用，让 streaming 跨子图
// 边界不丢。

type orchHooksKey struct{}

// WithOrchHooks 把 OrchestrationHooks 注入 ctx——业务（agent_v2 高层 wrapper /
// 自定义中间层）想让下游 Run 调用从 ctx 取 hook 而不是显式传 RunOption 时用。
//
// 与 RunOption WithHooks 共存：RunOption 是某次 Run 显式注入；本 helper 沿 ctx
// 链透传给所有下游调用。
//
// 典型用法（嵌套编排 sub-agent.Run 入口）：
//
//	ctx = compose_v2.WithOrchHooks(ctx, hooks)
//	events, err := subAgent.Run(ctx, sess, input)  // sub-agent 内部 GetOrchHooks 拿到 hooks
func WithOrchHooks(ctx context.Context, hooks OrchestrationHooks) context.Context {
	// audit²¹ HIGH-4 fix: isFrameworkNil 检 typed nil (协议二十一复证, 与 agent_v2 WithOrchHooks 协议对称).
	if isFrameworkNil(hooks) {
		return ctx
	}
	return context.WithValue(ctx, orchHooksKey{}, hooks)
}

// GetOrchHooks 从 ctx 取 OrchestrationHooks——nil 表示业务未注入。
//
// 编排 wrapper（agent_v2 各编排 Run 入口）按"RunOption 优先 → ctx fallback"取值。
func GetOrchHooks(ctx context.Context) OrchestrationHooks {
	if h, ok := ctx.Value(orchHooksKey{}).(OrchestrationHooks); ok {
		return h
	}
	return nil
}
