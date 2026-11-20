package observability

import (
	"context"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/internal/inference"
	"crew-go/schema"
)

// GenerateObserver 由 RoutingAgent 选中的编排钩子提供采样路由的推理观测。
func (h *orchHooks) GenerateObserver() inference.GenerateObserver {
	return h.o.observeGenerate
}

func (w *wrappedOrchHooks) GenerateObserver() inference.GenerateObserver {
	outer := w.obs.GenerateObserver()
	if inner := inference.GenerateObserverFrom(w.inner); inner != nil {
		return inference.ChainGenerateObservers([]inference.GenerateObserver{outer, inner})
	}
	return outer
}

// observeGenerate 观测编排内直接发出的模型请求，沿用 chat span 和推理统计的口径。
func (o *Observability) observeGenerate(ctx context.Context, agentName string, in *schema.ModelInput, next inference.GenerateFunc) (*schema.Message, error) {
	var scope *agentScope
	if ah := o.agentHandleFrom(ctx); ah != nil && ah.name == agentName && ah.graphNS == compose_v2.GetGraphNS(ctx) &&
		ah.sessionID == compose_v2.GetSessionID(ctx) && !ah.ended.Load() {
		scope = ah.scope
	}
	callCtx, mh := o.startModel(ctx, &hook.AgentInfo{Name: agentName}, in, scope)
	if mh == nil {
		return next(ctx)
	}
	// 模型 panic 继续交给引擎处理，观测层先结束这次调用。
	defer func() {
		if !mh.ended.Load() {
			o.finishModelError(callCtx, mh, result{outcome: OutcomeError, errType: "model_panic"})
		}
	}()

	reply, err := next(callCtx)
	modelErr := err
	if modelErr == nil {
		modelErr = callCtx.Err()
	}
	switch {
	case modelErr != nil:
		r := classify(callCtx, modelErr)
		if r.errType == "_OTHER" {
			r.errType = "model_call_error"
		}
		o.finishModelError(callCtx, mh, r)
	case reply == nil:
		o.finishModelError(callCtx, mh, result{outcome: OutcomeError, errType: "model_nil_output"})
	default:
		out := &schema.ModelOutput{Message: reply}
		if reply.Metadata != nil {
			out.Usage = reply.Metadata.Usage
		}
		o.finishModel(callCtx, mh, out, false)
	}
	return reply, err
}
