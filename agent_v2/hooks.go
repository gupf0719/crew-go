package agent_v2

import (
	"context"

	"crew-go/compose_v2"
)

// OrchestrationHooks 是图级观测/streaming hook (高层版本, ctx-based).
//
// 与 compose_v2.OrchestrationHooks 形态一致——但 agent_v2 提供 caller-friendly
// adapter (composeHooksAdapter) 让业务用 OrchestrationHooks (agent 视角) 而无需
// 直接对接底层 channel-based 视角.
//
// ─── ctx-based 协议 (audit² 改: 每个 hook 方法 OnStart(ctx)→ctx 链式透传) ───
// 每个 hook 方法返 context.Context, framework 内自动 propagate 给下游 (下一个
// hook / sub-call). 业务 hook 内可 `ctx = context.WithValue(ctx, spanKey{}, span)`
// 让后续 hook 拿到. 业务无 ctx mutation 需求时直接 `return ctx, nil` 一行即可.
//
// 5 个回调时机：
//   - OnGraphStart：Run 入口 (首次进入根 graph)
//   - OnGraphEnd：Run 出口 (含 InterruptError 路径)
//   - OnNodeStart：每个节点 NodeFunc 执行前
//   - OnNodeEnd：每个节点 NodeFunc 执行后
//   - OnNodeEvent：节点内通过 EventSink 实时投递的事件 (chunk / step_dispatch / 业务自定义)。**只作观测**：
//     在这里做出的转交 / 路由决定不生效（HandoffTo 报错），要做决定就在发事件的那段代码里直接做——理由见
//     compose_v2.OrchestrationHooks 的 OnNodeEvent（那里也写了 nodeID 与 ctx 各是什么）
type OrchestrationHooks interface {
	OnGraphStart(ctx context.Context) (context.Context, error)
	OnGraphEnd(ctx context.Context, err error) context.Context
	OnNodeStart(ctx context.Context, nodeID string) (context.Context, error)
	OnNodeEnd(ctx context.Context, nodeID string, err error) context.Context
	OnNodeEvent(ctx context.Context, nodeID string, event Event) context.Context
}

// Event 是节点内实时投递的事件。Type 由业务约定，常见有：
//   - "chunk": LLM 流式 token chunk (Content 是 token 文本)
//   - "step_dispatch": Plan-Execute 派发新 step 时投递 (Metadata 含 step idx / agent / refs)
//   - "plan": Planner 输出新计划 (Metadata 含 plan steps)
//
// 字段对齐 compose_v2.Event (Type/Content/Metadata), 让 adapter 直传.
type Event struct {
	Type     string
	Content  string         // 文本内容 (chunk 类型常用)
	Metadata map[string]any // 任意 KV
}

// NoOpOrchestrationHooks 提供所有回调的空实现, 业务实现自己的 hook 时可嵌入
// 此结构只覆写关心的方法, 其余走默认空实现.
type NoOpOrchestrationHooks struct{}

func (NoOpOrchestrationHooks) OnGraphStart(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (NoOpOrchestrationHooks) OnGraphEnd(ctx context.Context, _ error) context.Context {
	return ctx
}
func (NoOpOrchestrationHooks) OnNodeStart(ctx context.Context, _ string) (context.Context, error) {
	return ctx, nil
}
func (NoOpOrchestrationHooks) OnNodeEnd(ctx context.Context, _ string, _ error) context.Context {
	return ctx
}
func (NoOpOrchestrationHooks) OnNodeEvent(ctx context.Context, _ string, _ Event) context.Context {
	return ctx
}

var _ OrchestrationHooks = NoOpOrchestrationHooks{}

// composeHooksAdapter 把 agent_v2.OrchestrationHooks 适配到 compose_v2.OrchestrationHooks
// 接口——让 BasicAgent / PlanAgent 内部 Run compose_v2.Graph 时把 caller 提供的
// agent 视角 hook 自动桥接到底层 channel-based 视角.
type composeHooksAdapter struct {
	inner OrchestrationHooks
}

func (a *composeHooksAdapter) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	return a.inner.OnGraphStart(ctx)
}

func (a *composeHooksAdapter) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, err error) context.Context {
	return a.inner.OnGraphEnd(ctx, err)
}

func (a *composeHooksAdapter) OnNodeStart(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	return a.inner.OnNodeStart(ctx, string(id))
}

func (a *composeHooksAdapter) OnNodeEnd(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels, err error) context.Context {
	return a.inner.OnNodeEnd(ctx, string(id), err)
}

func (a *composeHooksAdapter) OnNodeEvent(ctx context.Context, id compose_v2.NodeID, ev compose_v2.Event) context.Context {
	return a.inner.OnNodeEvent(ctx, string(id), Event{
		Type:     ev.Type,
		Content:  ev.Content,
		Metadata: ev.Metadata,
	})
}

// adaptHooks 把 agent_v2 hook 包成 compose_v2 hook (nil 直接返 nil).
func adaptHooks(h OrchestrationHooks) compose_v2.OrchestrationHooks {
	if h == nil {
		return nil
	}
	return &composeHooksAdapter{inner: h}
}

// ChainOrchHooks 串联 N 个 OrchestrationHooks (agent_v2 视角) — review-v6 M-15 fix.
//
// 之前业务想挂 N 个 hook (SSE adapter + audit logger + metric collector) 时 agent_v2
// 缺 helper 业务自管 chain 易漏 nil 过滤. 本 helper 过滤 nil/typed-nil 后复用底层
// compose_v2.ChainOrchHooks.
//
// **panic 语义**：chain 内 hook 直接调、不逐个隔离——单 hook panic 中断剩余链，由调用这条链的地方处置
// （转成错误还是就地兜住记日志，见 compose_v2.OrchestrationHooks「panic 语义」）。
//
// 用法:
//
//	hooks := agent_v2.NewChainOrchHooks(sseHook, auditHook, metricHook)
//	ag.Run(ctx, sess, input, agent_v2.WithOrchHooks(hooks))
func NewChainOrchHooks(hooks ...OrchestrationHooks) OrchestrationHooks {
	composeHooks := make([]compose_v2.OrchestrationHooks, 0, len(hooks))
	for _, h := range hooks {
		if isFrameworkNil(h) { // typed-nil 也过滤——否则包进 adapter 后绕过 compose 侧 isFrameworkNil（它只检 adapter 本身）
			continue
		}
		composeHooks = append(composeHooks, &composeHooksAdapter{inner: h})
	}
	if len(composeHooks) == 0 {
		return NoOpOrchestrationHooks{}
	}
	chained := compose_v2.NewChainOrchHooks(composeHooks...)
	return &chainOrchAdapter{inner: chained}
}

// chainOrchAdapter 反向适配 — 把 compose_v2.ChainOrchHooks 包回 agent_v2.OrchestrationHooks.
type chainOrchAdapter struct {
	inner compose_v2.OrchestrationHooks
}

func (a *chainOrchAdapter) OnGraphStart(ctx context.Context) (context.Context, error) {
	return a.inner.OnGraphStart(ctx, compose_v2.Channels{})
}

func (a *chainOrchAdapter) OnGraphEnd(ctx context.Context, err error) context.Context {
	return a.inner.OnGraphEnd(ctx, compose_v2.Channels{}, err)
}

func (a *chainOrchAdapter) OnNodeStart(ctx context.Context, nodeID string) (context.Context, error) {
	return a.inner.OnNodeStart(ctx, compose_v2.NodeID(nodeID), compose_v2.Channels{})
}

func (a *chainOrchAdapter) OnNodeEnd(ctx context.Context, nodeID string, err error) context.Context {
	return a.inner.OnNodeEnd(ctx, compose_v2.NodeID(nodeID), compose_v2.Channels{}, err)
}

func (a *chainOrchAdapter) OnNodeEvent(ctx context.Context, nodeID string, event Event) context.Context {
	return a.inner.OnNodeEvent(ctx, compose_v2.NodeID(nodeID), compose_v2.Event{
		Type:     event.Type,
		Content:  event.Content,
		Metadata: event.Metadata,
	})
}
