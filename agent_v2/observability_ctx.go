package agent_v2

import "context"

// observability_ctx 集中放 agent_v2 → observability 的 ctx 桥接协议——
// 业务**不**直接调本文件 helper, 它们是 framework 内部协议, 让 observability
// hook 在 OnAgentStart / OnAgentEnd 拿到 agent 层的 retErr / Resume action.
//
// 为什么不放 options.go: options.go 是业务 API (With* / Resume*); 本文件是
// framework 内部桥接, 业务不应调.
//
// 设计源头: hook.Hooks 接口的 OnAgentEnd 签名是
// `OnAgentEnd(ctx, info, output) ctx` —— 没有 err 入参. 但 observability
// hook 想识别 Run/Resume 是否走 InterruptError 路径以 fire
// crew.interrupts 计数 / 记录 interrupt→resume duration 时, 需要拿到
// retErr. 加新 hook method 会破坏所有业务 Hooks 实现 — 改 ctx push 协议是
// 最小侵入的 backward-compatible 方案.

// endErrCtxKey ctx 键 — Run / Resume defer 前 push retErr.
type endErrCtxKey struct{}

// WithEndError 把 Run / Resume 的最终 retErr 注入 ctx, 让 observability hook
// 的 OnAgentEnd 通过 GetEndError 识别 InterruptError 等错误路径.
//
// 9 编排在 defer OnAgentEnd 之前调本 helper:
//
//	defer func() {
//	    ctxOut := agent_v2.WithEndError(ctx, retErr) // retErr 是 named return
//	    _ = hooks.OnAgentEnd(ctxOut, info, &hook.AgentOutput{Messages: msgs})
//	}()
//
// err == nil 时 no-op 返原 ctx — 让 observability 在成功路径 GetEndError 返 nil
// 不误 fire interrupt metric.
func WithEndError(ctx context.Context, err error) context.Context {
	if err == nil {
		return ctx
	}
	return context.WithValue(ctx, endErrCtxKey{}, err)
}

// GetEndError 从 ctx 读 Run / Resume 的 retErr.
// 返 nil 表示当前调用走成功路径 (或 framework 未 push, 业务 hook 链路被截断).
func GetEndError(ctx context.Context) error {
	if err, ok := ctx.Value(endErrCtxKey{}).(error); ok {
		return err
	}
	return nil
}

// resumePhaseCtxKey ctx 键 — Resume 入口 push ResumeAction.
type resumePhaseCtxKey struct{}

// WithResumePhase 把当前 Resume 的 action (Continue / Replan / Abort) 注入 ctx.
// commonResumePrep 内部调用; observability 的 OnAgentStart 通过 GetResumePhase
// 区分 Run vs Resume 并按 action label fire counter.
//
// Run 路径不调本 helper — 让 GetResumePhase 返 (Continue, false) 让 observability
// 跳过 resume 计数; 仅 Resume 入口才 push.
func WithResumePhase(ctx context.Context, action ResumeAction) context.Context {
	return context.WithValue(ctx, resumePhaseCtxKey{}, action)
}

// GetResumePhase 从 ctx 读 Resume action.
//
//	返 (action, true): 当前是 Resume 路径 (observability 据此 记 crew.resumes{crew.resume.action})
//	返 (Continue, false): Run 路径 (默认值, observability 跳过 resume 计数)
func GetResumePhase(ctx context.Context) (ResumeAction, bool) {
	if a, ok := ctx.Value(resumePhaseCtxKey{}).(ResumeAction); ok {
		return a, true
	}
	return ResumeActionContinue, false
}
