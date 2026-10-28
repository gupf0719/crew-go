package observability

import (
	"context"
	"fmt"

	"crew-go/agent_v2"
	"crew-go/hook"
	specModel "crew-go/modules/model"
	"crew-go/schema"
)

// 包装层：AgentHooks(inner...) / OrchHooks(inner...) 把业务钩子包在 observability 里面。跟自己用
// hook.NewChainHooks / agent_v2.NewChainOrchHooks 把 observability 跟别的钩子串起来相比，差在三处：
//
//   - 开始一侧失败：里面的钩子开始失败（报错或抛中断）时，observability 当场收掉它刚开的 span。链做不到——链在
//     后面的钩子失败时交回开始之前的 ctx，框架随后要么不再调结束钩子（agent 级），要么拿开始之前的 ctx 调
//     （节点 / 图），observability 都取不到自己的句柄：span 永远不结束（不会被导出），crew.agent.active 永久多 1。
//     鉴权 / 限额钩子拒绝一次调用、HITL 闸门在节点前抛中断，走的都是这条路。
//   - 外来 ctx：里面的钩子返回外来的 ctx（缓存的、Background）时，把本层的句柄与 span 挂回去（keepHandles）。
//   - 改写之后的值：开始一侧先开 span（父子关系要它），依赖入参内容的属性等里面的钩子跑完再记；结束一侧里面的
//     钩子先跑、observability 后记。业务钩子可以改写 [消费] 字段（输入消息、工具名与参数、模型输出、工具结果），
//     span 记的是改写之后、真正交下去 / 被框架用上的那份——业务在钩子里做的 PII 过滤，span 里也是过滤后的。
//   - 里面的钩子 panic：结束一侧的收尾放在 defer 里，里面的结束钩子报错、停下或 panic 时照样按这次调用自己的结局
//     收尾（框架兜住结束钩子的 panic、不改调用的结局，不收的话 span 永远不结束）；开始一侧里面的钩子 panic 时
//     当场收掉刚开的 span 再原样抛出（框架把它转成这次调用的错误，不会再调结束钩子）。

// wrappedHooks 是 AgentHooks(inner...) 的返回值。
type wrappedHooks struct {
	obs   *agentHooks
	inner hook.Hooks
}

var _ hook.Hooks = (*wrappedHooks)(nil)

// abortOnPanic 用在开始一侧的 defer 里：里面的开始钩子 panic 时，按这个 panic 收掉本层刚开的 span，再原样抛出。
// 框架把开始钩子的 panic 转成这次调用的错误、之后不再调结束钩子，这里不收的话 span 永远不结束。没有 panic 时什么也不做。
func abortOnPanic(abort func(err error)) {
	if r := recover(); r != nil {
		abort(fmt.Errorf("hook panic: %v", r))
		panic(r)
	}
}

func (w *wrappedHooks) OnAgentStart(ctx context.Context, info *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	o := w.obs.o
	ctx1, ah := o.startAgent(ctx, info)
	defer abortOnPanic(func(err error) {
		if ah != nil {
			o.endAgent(agent_v2.WithEndError(ctx1, err), info, &hook.AgentOutput{})
		}
	})
	ctx2, err := w.inner.OnAgentStart(ctx1, info, in)
	if err != nil {
		// 框架对开始失败的 agent 不再调 OnAgentEnd：按这个错误当场收尾（中断记 blocked 并留挂起记录）。
		if ah != nil {
			o.endAgent(agent_v2.WithEndError(ctx1, err), info, &hook.AgentOutput{})
		}
		return ctx, err
	}
	o.agentStartContent(ctx1, ah, in)
	return o.keepHandles(ctx1, ctx2), nil
}

// OnAgentEnd 先让里面的钩子跑，再收尾 agent span。收尾放在 defer 里：里面的钩子 panic 时框架兜住、不改变调用的结局，
// 这里不收的话 span 永远不结束（不会被导出），crew.agent.active 也永久多 1。panic 照样往上抛，交给框架处置。
func (w *wrappedHooks) OnAgentEnd(ctx context.Context, info *hook.AgentInfo, out *hook.AgentOutput) context.Context {
	defer w.obs.o.endAgent(ctx, info, out)
	return w.inner.OnAgentEnd(ctx, info, out)
}

func (w *wrappedHooks) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	o := w.obs.o
	ctx1, mh := o.startModel(ctx, info, in)
	defer abortOnPanic(func(err error) { o.abortLeaf(ctx1, mh, err, "model_start_error") })
	ctx2, err := w.inner.OnModelStart(ctx1, info, in)
	if err != nil {
		o.abortLeaf(ctx1, mh, err, "model_start_error")
		return ctx, err
	}
	o.modelStartContent(mh, in)
	return o.keepHandles(ctx1, ctx2), nil
}

func (w *wrappedHooks) OnModelEndStream(ctx context.Context, info *hook.AgentInfo, sr specModel.MessageStreamReader) (context.Context, specModel.MessageStreamReader, error) {
	ctx1, sr1, _ := w.obs.OnModelEndStream(ctx, info, sr)
	ctx2, sr2, err := w.inner.OnModelEndStream(ctx1, info, sr1)
	if err != nil {
		// 跟链一样：出错时关掉已经包过的流（Close 幂等，inner 自己关过也无妨）。chat span 随后由 OnError 收尾。
		if sr1 != nil {
			_ = sr1.Close()
		}
		return ctx, nil, err
	}
	return w.obs.o.keepHandles(ctx1, ctx2), sr2, nil
}

// OnModelEnd 先让里面的钩子改写，再按改写之后的值收尾（defer）。里面的钩子报错（拒掉这份答复、等人、被叫停）或 panic
// 时也收尾：模型调用本身已经完成，按它自己的结局记——用量只在这一遍交来，续跑沿用这份答复的那一遍不带用量、也不再记
// （见 hook.ModelResponseReusedKey），这里不收的话这次调用一次也记不上。
func (w *wrappedHooks) OnModelEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	defer w.obs.OnModelEnd(ctx, info, out)
	ret, err := w.inner.OnModelEnd(ctx, info, out)
	if err != nil {
		return ctx, err
	}
	return ret, nil
}

func (w *wrappedHooks) OnToolStart(ctx context.Context, info *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	o := w.obs.o
	ctx1, th := o.startTool(ctx, info, in)
	defer abortOnPanic(func(err error) { o.abortLeaf(ctx1, th, err, "tool_start_error") })
	ctx2, err := w.inner.OnToolStart(ctx1, info, in)
	if err != nil {
		o.abortLeaf(ctx1, th, err, "tool_start_error")
		return ctx, err
	}
	o.toolStartContent(th, in)
	return o.keepHandles(ctx1, ctx2), nil
}

// OnToolEnd 同 OnModelEnd：里面的钩子报错或 panic 时工具已经执行过，照样按这次调用自己的结局收尾（续跑时它从工具账
// 重放，带 agent_v2.ToolReplayedExtraKey，那一次不再记）。
func (w *wrappedHooks) OnToolEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	defer w.obs.OnToolEnd(ctx, info, out)
	ret, err := w.inner.OnToolEnd(ctx, info, out)
	if err != nil {
		return ctx, err
	}
	return ret, nil
}

func (w *wrappedHooks) OnError(ctx context.Context, info *hook.AgentInfo, err *schema.CommError) context.Context {
	ctx = w.obs.OnError(ctx, info, err)
	return w.inner.OnError(ctx, info, err)
}

// wrappedOrchHooks 是 OrchHooks(inner...) 的返回值。
type wrappedOrchHooks struct {
	obs   *orchHooks
	inner agent_v2.OrchestrationHooks
}

var _ agent_v2.OrchestrationHooks = (*wrappedOrchHooks)(nil)

func (w *wrappedOrchHooks) OnGraphStart(ctx context.Context) (context.Context, error) {
	o := w.obs.o
	ctx1, gh := o.startGraph(ctx)
	defer abortOnPanic(func(err error) {
		if gh != nil {
			w.obs.OnGraphEnd(ctx1, err)
		}
	})
	ctx2, err := w.inner.OnGraphStart(ctx1)
	if err != nil {
		// 框架随后用开始之前的 ctx 调 OnGraphEnd，那里取不到这个 span：当场收尾。
		if gh != nil {
			w.obs.OnGraphEnd(ctx1, err)
		}
		return ctx, err
	}
	return o.keepHandles(ctx1, ctx2), nil
}

// OnGraphEnd 先让里面的钩子跑，再收尾（defer）：里面的钩子 panic 时框架兜住、不改结局，span 照样收尾。
func (w *wrappedOrchHooks) OnGraphEnd(ctx context.Context, err error) context.Context {
	defer w.obs.OnGraphEnd(ctx, err)
	return w.inner.OnGraphEnd(ctx, err)
}

func (w *wrappedOrchHooks) OnNodeStart(ctx context.Context, nodeID string) (context.Context, error) {
	o := w.obs.o
	ctx1, nh := o.startNode(ctx, nodeID)
	defer abortOnPanic(func(err error) {
		if nh != nil {
			w.obs.OnNodeEnd(ctx1, nodeID, err)
		}
	})
	ctx2, err := w.inner.OnNodeStart(ctx1, nodeID)
	if err != nil {
		if nh != nil {
			w.obs.OnNodeEnd(ctx1, nodeID, err)
		}
		return ctx, err
	}
	return o.keepHandles(ctx1, ctx2), nil
}

// OnNodeEnd 同 OnGraphEnd：里面的钩子 panic 时节点 span 照样收尾。
func (w *wrappedOrchHooks) OnNodeEnd(ctx context.Context, nodeID string, err error) context.Context {
	defer w.obs.OnNodeEnd(ctx, nodeID, err)
	return w.inner.OnNodeEnd(ctx, nodeID, err)
}

func (w *wrappedOrchHooks) OnNodeEvent(ctx context.Context, nodeID string, ev agent_v2.Event) context.Context {
	ctx = w.obs.OnNodeEvent(ctx, nodeID, ev)
	return w.inner.OnNodeEvent(ctx, nodeID, ev)
}

// abortLeaf 收尾一个没开始成的模型 / 工具调用（里面的钩子在开始一侧拒绝了它）：span 按这个错误结束（中断记
// blocked），不记时长样本——调用没有发生——并把 agent 下的调用计数退回去。h 为 nil（本层没开 span）时什么也不做。
func (o *Observability) abortLeaf(ctx context.Context, h *handle, err error, code string) {
	if h == nil {
		return
	}
	h.scope.remove(h)
	if h.scope != nil {
		if h.kind == kindModel {
			h.scope.inference.Add(-1)
		} else {
			h.scope.toolCalls.Add(-1)
		}
	}
	r := classify(ctx, err)
	if r.outcome == OutcomeError && r.errType == "_OTHER" {
		r.errType = code
	}
	o.finish(h, r)
}
