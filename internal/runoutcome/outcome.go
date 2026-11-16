// Package runoutcome 将引擎的运行结果归类为协议可以公开的状态。
package runoutcome

import (
	"context"
	"errors"

	"crew-go/compose_v2"
)

// Kind 是一次执行的结果类别。失败类别的值用作机器可读错误码。
type Kind string

const (
	Success                Kind = "success"
	Interrupted            Kind = "interrupt"
	Canceled               Kind = "cancelled"
	InvalidInterrupt       Kind = "invalid_interrupt"
	CheckpointNotResumable Kind = "checkpoint_not_resumable"
	CheckpointStale        Kind = "checkpoint_stale"
	GraphIDConflict        Kind = "graph_id_conflict"
	CheckpointSaveFailed   Kind = "checkpoint_save_failed"
	NoPendingCheckpoint    Kind = "no_pending_checkpoint"
	RunAborted             Kind = "run_aborted"
	Busy                   Kind = "busy"
	Timeout                Kind = "timeout"
	NonRetryable           Kind = "non_retryable"
	AgentError             Kind = "agent_error"
)

// Classify 保留存档和终局失败的具体语义，并返回可投影的挂起问题。
// 已生成的合法中断仍可续跑。普通失败与调用方取消并存时，以执行 ctx 为准，
// 活跃 ctx 上的内层 context.Canceled 则仍是失败。
func Classify(ctx context.Context, err error) (Kind, *compose_v2.InterruptError) {
	switch {
	case err == nil:
		return Success, nil
	case errors.Is(err, compose_v2.ErrCheckpointNotResumable):
		return CheckpointNotResumable, nil
	case errors.Is(err, compose_v2.ErrCheckpointStale) || errors.Is(err, compose_v2.ErrTaskLeaseLost):
		return CheckpointStale, nil
	case errors.Is(err, compose_v2.ErrGraphIDConflict):
		return GraphIDConflict, nil
	case errors.Is(err, compose_v2.ErrCheckpointNotPersisted):
		return CheckpointSaveFailed, nil
	case errors.Is(err, compose_v2.ErrRunAborted):
		return RunAborted, nil
	case errors.Is(err, compose_v2.ErrNoPendingCheckpoint):
		return NoPendingCheckpoint, nil
	case errors.Is(err, compose_v2.ErrTaskBusy):
		return Busy, nil
	}

	ctxErr := ctx.Err()
	// 租约续期失败可能只取消执行 ctx，返回的业务错误未必带丢权哨兵。
	if ctxErr != nil && errors.Is(context.Cause(ctx), compose_v2.ErrTaskLeaseLost) {
		return CheckpointStale, nil
	}
	if errors.Is(err, compose_v2.ErrSoftCanceled) {
		return Canceled, nil
	}

	if ie, ok := compose_v2.IsInterrupt(err); ok {
		if len(ie.AllQueries()) == 0 || ie.Validate() != nil {
			return InvalidInterrupt, nil
		}
		return Interrupted, ie
	}

	var nonRetryable compose_v2.NonRetryable
	switch {
	case errors.Is(ctxErr, context.DeadlineExceeded):
		return Timeout, nil
	case ctxErr != nil:
		return Canceled, nil
	case errors.Is(err, context.DeadlineExceeded):
		return Timeout, nil
	case errors.As(err, &nonRetryable):
		return NonRetryable, nil
	default:
		return AgentError, nil
	}
}

// Message 返回可以发送给客户端的固定错误正文。
func (k Kind) Message() string {
	switch k {
	case Canceled:
		return "the task was canceled"
	case InvalidInterrupt:
		return "the run has an invalid pending interrupt"
	case CheckpointNotResumable:
		return "the saved state for this session is inconsistent and cannot be resumed"
	case CheckpointStale:
		return "this session was concurrently advanced by another run; result was not persisted"
	case GraphIDConflict:
		return "this session's saved state belongs to a different agent; start a new session"
	case CheckpointSaveFailed:
		return "this run's state could not be saved, so it cannot be resumed"
	case NoPendingCheckpoint:
		return "there is no resumable state"
	case RunAborted:
		return "the run ended with a terminal failure and cannot be retried or resumed"
	case Busy:
		return "the task is busy; retry the operation after it finishes"
	case Timeout:
		return "the run timed out"
	case NonRetryable:
		return "the run failed and the same request cannot be retried"
	default:
		return "the agent run failed"
	}
}
