package observability

import (
	"context"

	"crew-go/agent_v2"
	"crew-go/hook"
	specModel "crew-go/modules/model"
	"crew-go/schema"
)

// 包装层：AgentHooks(inner...) / OrchHooks(inner...) 把业务钩子包在 observability 里面。跟自己用
// hook.NewChainHooks / agent_v2.NewChainOrchHooks 把 observability 排在第一位串起来相比，只差在开始一侧失败时：
// 里面的钩子开始失败（报错或抛中断）时，observability 当场收掉它刚开的 span。
//
// 链本身做不到这一点：链在后面的钩子失败时交回开始之前的 ctx，框架随后要么不再调结束钩子（agent 级），要么
// 拿开始之前的 ctx 调（节点 / 图）——两种情况下 observability 都取不到自己的句柄，span 永远不结束（不会被导出），
// crew.agent.active 也永久多 1。这正是鉴权 / 限额钩子拒绝一次调用、HITL 闸门在节点前抛中断时会走的路。

// wrappedHooks 是 AgentHooks(inner...) 的返回值。结束一侧与 OnError 跟链一样按顺序调：observability 先、inner 后。
type wrappedHooks struct {
	obs   *agentHooks
	inner hook.Hooks
}

var _ hook.Hooks = (*wrappedHooks)(nil)

func (w *wrappedHooks) OnAgentStart(ctx context.Context, info *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	ctx1, _ := w.obs.OnAgentStart(ctx, info, in)
	ctx2, err := w.inner.OnAgentStart(ctx1, info, in)
	if err != nil {
		// 框架对开始失败的 agent 不再调 OnAgentEnd：按这个错误当场收尾（中断记 blocked 并留挂起记录）。
		w.obs.OnAgentEnd(agent_v2.WithEndError(ctx1, err), info, &hook.AgentOutput{})
		return ctx, err
	}
	return ctx2, nil
}

func (w *wrappedHooks) OnAgentEnd(ctx context.Context, info *hook.AgentInfo, out *hook.AgentOutput) context.Context {
	ctx = w.obs.OnAgentEnd(ctx, info, out)
	return w.inner.OnAgentEnd(ctx, info, out)
}

func (w *wrappedHooks) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	ctx1, _ := w.obs.OnModelStart(ctx, info, in)
	ctx2, err := w.inner.OnModelStart(ctx1, info, in)
	if err != nil {
		w.obs.o.abortLeaf(ctx1, err, "model_start_error")
		return ctx, err
	}
	return ctx2, nil
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
	return ctx2, sr2, nil
}

func (w *wrappedHooks) OnModelEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	ctx1, _ := w.obs.OnModelEnd(ctx, info, out)
	ctx2, err := w.inner.OnModelEnd(ctx1, info, out)
	if err != nil {
		return ctx, err
	}
	return ctx2, nil
}

func (w *wrappedHooks) OnToolStart(ctx context.Context, info *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	ctx1, _ := w.obs.OnToolStart(ctx, info, in)
	ctx2, err := w.inner.OnToolStart(ctx1, info, in)
	if err != nil {
		w.obs.o.abortLeaf(ctx1, err, "tool_start_error")
		return ctx, err
	}
	return ctx2, nil
}

func (w *wrappedHooks) OnToolEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	ctx1, _ := w.obs.OnToolEnd(ctx, info, out)
	ctx2, err := w.inner.OnToolEnd(ctx1, info, out)
	if err != nil {
		return ctx, err
	}
	return ctx2, nil
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
	ctx1, _ := w.obs.OnGraphStart(ctx)
	ctx2, err := w.inner.OnGraphStart(ctx1)
	if err != nil {
		// 框架随后用开始之前的 ctx 调 OnGraphEnd，那里取不到这个 span：当场收尾。
		w.obs.OnGraphEnd(ctx1, err)
		return ctx, err
	}
	return ctx2, nil
}

func (w *wrappedOrchHooks) OnGraphEnd(ctx context.Context, err error) context.Context {
	ctx = w.obs.OnGraphEnd(ctx, err)
	return w.inner.OnGraphEnd(ctx, err)
}

func (w *wrappedOrchHooks) OnNodeStart(ctx context.Context, nodeID string) (context.Context, error) {
	ctx1, _ := w.obs.OnNodeStart(ctx, nodeID)
	ctx2, err := w.inner.OnNodeStart(ctx1, nodeID)
	if err != nil {
		w.obs.OnNodeEnd(ctx1, nodeID, err)
		return ctx, err
	}
	return ctx2, nil
}

func (w *wrappedOrchHooks) OnNodeEnd(ctx context.Context, nodeID string, err error) context.Context {
	ctx = w.obs.OnNodeEnd(ctx, nodeID, err)
	return w.inner.OnNodeEnd(ctx, nodeID, err)
}

func (w *wrappedOrchHooks) OnNodeEvent(ctx context.Context, nodeID string, ev agent_v2.Event) context.Context {
	ctx = w.obs.OnNodeEvent(ctx, nodeID, ev)
	return w.inner.OnNodeEvent(ctx, nodeID, ev)
}

// abortLeaf 收尾一个没开始成的模型 / 工具调用（里面的钩子在开始一侧拒绝了它）：span 按这个错误结束（中断记
// blocked），不记时长样本——调用没有发生——并把 agent 下的调用计数退回去。
func (o *Observability) abortLeaf(ctx context.Context, err error, code string) {
	h := handleFrom(ctx)
	if h == nil || (h.kind != kindModel && h.kind != kindTool) {
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
