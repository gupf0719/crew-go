package background

import (
	"context"
	"errors"

	"crew-go/compose_v2"
)

// CompletionReason 标识父任务中由 [Manager.Await] 发出的提问。通知方按它匹配待答点。
func CompletionReason(taskID string) string { return "crew:background-completed:" + taskID }

// Await 挂起当前图节点，等待后台子任务的终态通知。通知方用子任务 ID 作为该问题的答案。
func (m *Manager) Await(ctx context.Context, taskID string) (*Task, error) {
	parent := ParentFromContext(ctx)
	if _, err := m.Get(ctx, parent, taskID); err != nil {
		return nil, err
	}
	answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{
		Question: "等待后台子任务完成",
		Reason:   CompletionReason(taskID),
	})
	if err != nil {
		return nil, err
	}
	if answer == nil || answer.Content != taskID {
		return nil, errors.New("background: completion answer does not match the child task")
	}
	task, err := m.Get(ctx, parent, taskID)
	if err != nil {
		return nil, err
	}
	if !task.Terminal() {
		return nil, errors.New("background: child task has no committed result")
	}
	return task, nil
}
