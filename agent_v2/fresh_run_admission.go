package agent_v2

import (
	"context"
	"reflect"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// ErrRunIDHasResumableCheckpoint 表示新任务引用了仍有可续存档的 RunID。
// 可以 Resume 原任务，或用 RunSession.NextTask 分配新 RunID。
var ErrRunIDHasResumableCheckpoint = compose_v2.ErrRunIDHasResumableCheckpoint

type rootRunAdmissionKey struct{}

func (rootRunAdmissionKey) ExecutionState() {}

type runActivity struct {
	*compose_v2.TaskActivity
	runID       string
	initial     compose_v2.Checkpointer
	hookBackend *compose_v2.TaskActivity
}

func (a *runActivity) Release() {
	if a.hookBackend != nil {
		a.hookBackend.Release()
	}
	a.TaskActivity.Release()
}

func (a *runActivity) callAgentStart(ctx context.Context, hooks hook.Hooks, info *hook.AgentInfo, input *hook.AgentInput) (context.Context, error) {
	ctx, err := CallAgentStart(ctx, hooks, info, input)
	if err != nil {
		if _, interrupted := compose_v2.IsInterrupt(err); !interrupted && ctx.Err() == nil {
			_ = hooks.OnError(ctx, info, &schema.CommError{Code: "agent_start_error", Message: err.Error()})
		}
		return ctx, WrapAgentStartErr(ctx, err)
	}
	ctx, err = a.checkHookBackend(ctx)
	if err != nil {
		// 钩子已成功开始，调用方尚未登记平时的结束钩子。
		fireAgentEnd(WithEndError(ctx, err), hooks, info, &hook.AgentOutput{}, err)
	}
	return ctx, err
}

func (a *runActivity) checkHookBackend(ctx context.Context) (next context.Context, retErr error) {
	next = ctx
	defer recoverToErr(ctx, "check fresh run checkpoint after agent start", &retErr)
	cp := compose_v2.GetCheckpointer(ctx)
	if a.runID == "" || cp == nil || (reflect.ValueOf(cp).Comparable() && cp == a.initial) {
		return ctx, nil
	}
	next, activity, err := compose_v2.BeginTaskActivity(ctx, a.runID, nil)
	if err != nil {
		return next, err
	}
	a.hookBackend = activity
	return next, nil
}
