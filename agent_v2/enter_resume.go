package agent_v2

import (
	"context"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// ResumeRequest 是自研 [Agent] 经 [EnterResume] 得到的续跑请求。
type ResumeRequest struct {
	// Session 是原任务的会话。未传 WithResumeSession 时只恢复 RunID。
	Session *RunSession
	// Action 是调用方请求的继续、重规划或放弃操作。
	Action ResumeAction
	// UserInputs 是按 InterruptID 索引的答案快照，须与原任务的挂起项配对。
	UserInputs map[string]*schema.Message
	// UserText 是本轮未指向具体挂起项的指示。
	UserText string
	// OrchHooks 是本次显式传入的编排钩子，缺省时从请求上下文取。
	OrchHooks OrchestrationHooks
}

// EnterResume 校验自研 [Agent] 的独立续跑入口。它用原 RunID 进入任务，
// 保留会话内的审批规则，隔离外层节点的授予。返回的 ctx 用于本次调用，
// 钩子可通过 [GetResumePhase] 查询操作。
//
// 实现负责按调用主体找到原任务、核验答案、控制并发，并完成自己的存档和历史收尾。
// 使用框架图存档和历史时，把原 ctx 和选项交给 [GraphAgent.Resume]。
// 宿主管理的 Abort 收尾和图专用选项需要对应的内置执行入口。
func EnterResume(ctx context.Context, sessionID, agentName string, opts ...ResumeOption) (context.Context, ResumeRequest, error) {
	const agentType = "custom agent"
	if err := validateResumeScope(ctx, agentType, agentName, sessionID); err != nil {
		return ctx, ResumeRequest{}, err
	}
	if sessionID == "" {
		return ctx, ResumeRequest{}, rejectCall(ctx, "%s %q: resume requires a non-empty sessionID", agentType, agentName)
	}
	cfg, err := applyResumeOptions(opts)
	if err != nil {
		return ctx, ResumeRequest{}, rejectCall(ctx, "%s %q: %s", agentType, agentName, err)
	}
	if err := validateResumeSession(ctx, agentType, agentName, sessionID, cfg.session); err != nil {
		return ctx, ResumeRequest{}, err
	}
	switch cfg.action {
	case ResumeActionContinue, ResumeActionReplan:
	case ResumeActionAbort:
		if cfg.abortContext != nil || cfg.abortCleanup != nil || cfg.abortFinalize != nil ||
			cfg.useAbortHistoryKey || cfg.abortAgentHistory || cfg.abortHistoryStore != nil {
			return ctx, ResumeRequest{}, rejectCall(ctx, "%s %q: hosted abort options require a framework-managed agent", agentType, agentName)
		}
	default:
		return ctx, ResumeRequest{}, rejectCall(ctx, "%s %q: invalid resume action %d", agentType, agentName, cfg.action)
	}
	if !cfg.breakpoints.empty() {
		return ctx, ResumeRequest{}, rejectCall(ctx, "%s %q: per-call breakpoints require GraphAgent", agentType, agentName)
	}
	if !cfg.overrides().isZero() {
		return ctx, ResumeRequest{}, rejectCall(ctx, "%s %q: per-call model overrides require BasicAgent", agentType, agentName)
	}

	session := cfg.session
	if session == nil {
		session = &RunSession{RunID: sessionID}
	}
	var answers map[string]*schema.Message
	if cfg.userInputs != nil {
		answers = make(map[string]*schema.Message, len(cfg.userInputs))
		for id, answer := range cfg.userInputs {
			answers[id] = cloneCalleeInput(answer)
		}
	}
	hooks := cfg.orchHooks
	if hooks == nil {
		hooks = GetCtxOrchHooks(ctx)
	}
	ctx = compose_v2.StripResumeContext(ctx)
	ctx = enterResumeTask(WithSession(WithResumePhase(ctx, cfg.action), session), session)
	return ctx, ResumeRequest{
		Session: session, Action: cfg.action, UserInputs: answers, UserText: cfg.userText, OrchHooks: hooks,
	}, nil
}
