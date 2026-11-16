// Package background 管理独立存档、可跨进程接手的后台子 Agent。
package background

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// Parent 标识派单方。Owner 由应用从已认证的身份取得，TaskID 是父任务的执行身份。
type Parent struct {
	Owner    string `json:"owner"`
	TaskID   string `json:"task_id"`
	ThreadID string `json:"thread_id,omitempty"`
}

// ParentFromContext 提取图节点当前的派单身份。
func ParentFromContext(ctx context.Context) Parent {
	return Parent{Owner: compose_v2.GetOwner(ctx), TaskID: compose_v2.GetSessionID(ctx), ThreadID: compose_v2.GetThreadID(ctx)}
}

// Status 是后台子任务的持久状态。
type Status string

const (
	Queued    Status = "queued"
	Running   Status = "running"
	Waiting   Status = "waiting"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
	Cancelled Status = "cancelled"
)

// Task 是后台子任务的一份存储快照。调用方可以修改快照。
type Task struct {
	ID              string                     `json:"id"`
	Parent          Parent                     `json:"parent"`
	Worker          string                     `json:"worker"`
	Input           *schema.Message            `json:"input"`
	Status          Status                     `json:"status"`
	Pending         []*compose_v2.AskQuery     `json:"pending,omitempty"`
	Answers         map[string]*schema.Message `json:"answers,omitempty"`
	Result          *schema.Message            `json:"result,omitempty"`
	Failure         string                     `json:"failure,omitempty"`
	CancelRequested bool                       `json:"cancel_requested,omitempty"`
	Notified        bool                       `json:"notified,omitempty"`
	Digest          string                     `json:"digest"`
	Revision        int64                      `json:"revision"`
}

// Terminal 报任务是否有持久终态。
func (t Task) Terminal() bool {
	return t.Status == Succeeded || t.Status == Failed || t.Status == Cancelled
}

// Store 把子任务状态和 Checkpointer 放在同一个存储域。读方法交出独立快照。
type Store interface {
	compose_v2.Checkpointer
	compose_v2.TaskLeaseProvider

	// CreateBackgroundTask 原子插入或返回同 ID 的存量记录。新记录的 Revision 是 1。
	CreateBackgroundTask(ctx context.Context, task *Task) (*Task, error)
	// LoadBackgroundTask 在无记录时返回 (nil, nil)。
	LoadBackgroundTask(ctx context.Context, id string) (*Task, error)
	// ListReadyBackgroundTasks 按 ID 升序分页返回可运行、待取消或需通知父任务的 ID。
	ListReadyBackgroundTasks(ctx context.Context, after string, limit int, notify bool) ([]string, error)
	// UpdateBackgroundTask 以 task.Revision 为读版本。成功时原子替换记录并加一，
	// 版本冲突返回 (false, nil)。ctx 带该任务租约时必须原子校验。清档时必须持有效租约，
	// 并与终态写入同一事务。调用方持有的 task 保持原样。
	UpdateBackgroundTask(ctx context.Context, task *Task, clearCheckpoint bool) (bool, error)
}

// Factory 用已保存的任务信息重建一个可执行的 Agent。同一个 worker 名应维持兼容定义。
type Factory func(ctx context.Context, taskID string) (agent_v2.Agent, error)

// Notify 在终态落盘后唤醒父任务。失败后会重试，用 Task.ID 对通知去重。
type Notify func(ctx context.Context, task Task) error

// Config 配置工作进程。每次 Serve 串行驱动任务，可启动多个工作进程共享 Store。
type Config struct {
	Workers        map[string]Factory
	Notify         Notify
	PollInterval   time.Duration
	AttemptTimeout time.Duration
}

// Manager 提交和驱动持久后台子任务。
type Manager struct {
	store Store
	cfg   Config
}

var (
	ErrNotFound        = errors.New("background: task not found")
	ErrRequestConflict = errors.New("background: request key belongs to a different submission")
	ErrNotWaiting      = errors.New("background: task is not waiting for those answers")
)

// New 创建管理器。零轮询间隔取一秒，零执行期限取两分钟。
func New(store Store, cfg Config) (*Manager, error) {
	if isNil(store) || isNil(store.CheckpointTaskLeases()) {
		return nil, errors.New("background: store must support shared task leases")
	}
	if cfg.PollInterval < 0 || cfg.AttemptTimeout < 0 {
		return nil, errors.New("background: durations must be non-negative")
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.AttemptTimeout == 0 {
		cfg.AttemptTimeout = 2 * time.Minute
	}
	workers := make(map[string]Factory, len(cfg.Workers))
	for name, factory := range cfg.Workers {
		if strings.TrimSpace(name) == "" || factory == nil {
			return nil, errors.New("background: each worker requires a name and factory")
		}
		workers[name] = factory
	}
	cfg.Workers = workers
	return &Manager{store: store, cfg: cfg}, nil
}

func isNil(value any) bool {
	switch v := reflect.ValueOf(value); v.Kind() {
	case reflect.Invalid:
		return true
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// Submit 用父任务内稳定的 requestKey 幂等派单。输入消息的构造期 ID 可以变化。
func (m *Manager) Submit(ctx context.Context, parent Parent, requestKey, worker string, input *schema.Message) (*Task, error) {
	if parent.Owner == "" || parent.TaskID == "" || strings.TrimSpace(requestKey) == "" ||
		strings.TrimSpace(worker) == "" || input == nil {
		return nil, errors.New("background: submission requires owner, parent task, request key, worker and input")
	}
	view := *input
	view.ID = ""
	data, err := json.Marshal(view)
	if err != nil {
		return nil, fmt.Errorf("background: encode input: %w", err)
	}
	key := sha256.Sum256(fmt.Appendf(nil, "%q %q %q", parent.Owner, parent.TaskID, requestKey))
	digest := sha256.Sum256(fmt.Appendf(nil, "%q %q %s", parent.ThreadID, worker, data))
	task := &Task{ID: "bg-" + hex.EncodeToString(key[:]), Parent: parent, Worker: worker, Input: input,
		Status: Queued, Digest: hex.EncodeToString(digest[:]), Revision: 1}
	stored, err := m.store.CreateBackgroundTask(ctx, task)
	if err != nil {
		return nil, err
	}
	if stored == nil {
		return nil, errors.New("background: store returned no task after submission")
	}
	if stored.Parent != parent || stored.Digest != task.Digest {
		return nil, ErrRequestConflict
	}
	return stored, nil
}

// Get 查询指定父任务的子任务。其他归属与不存在都返回 [ErrNotFound]。
func (m *Manager) Get(ctx context.Context, parent Parent, id string) (*Task, error) {
	task, err := m.store.LoadBackgroundTask(ctx, id)
	if err != nil {
		return nil, err
	}
	if task == nil || task.Parent != parent {
		return nil, ErrNotFound
	}
	return task, nil
}

// Cancel 记录取消请求。工作进程看到后停止执行，持久化取消终态，并清理子任务的存档。
func (m *Manager) Cancel(ctx context.Context, parent Parent, id string) (*Task, error) {
	for ctx.Err() == nil {
		task, err := m.Get(ctx, parent, id)
		if err != nil {
			return nil, err
		}
		if task.Terminal() || task.CancelRequested {
			return task, nil
		}
		task.CancelRequested = true
		if saved, err := m.save(ctx, task, false); err != nil || saved {
			return task, err
		}
	}
	return nil, ctx.Err()
}

// Answer 按 InterruptID 提交当前批次的全部文本答复。调用方校验业务内容和 ResponseSchema。
//
// 在该批次仍为当前批次时，重复提交相同的 ID 和文本会返回已接纳的状态。
func (m *Manager) Answer(ctx context.Context, parent Parent, id string, answers map[string]string) (*Task, error) {
	for ctx.Err() == nil {
		task, err := m.Get(ctx, parent, id)
		if err != nil {
			return nil, err
		}
		if task.CancelRequested || len(answers) == 0 || len(answers) != len(task.Pending) {
			return nil, ErrNotWaiting
		}
		if task.Status == Running && alreadyAnswered(task, answers) {
			return task, nil
		}
		if task.Status != Waiting {
			return nil, ErrNotWaiting
		}
		if task.Answers == nil {
			task.Answers = make(map[string]*schema.Message, len(answers))
		}
		for _, query := range task.Pending {
			answer, ok := answers[query.InterruptID]
			if !ok {
				return nil, ErrNotWaiting
			}
			task.Answers[query.InterruptID] = schema.UserMessage(answer)
		}
		task.Status = Running
		if saved, err := m.save(ctx, task, false); err != nil || saved {
			return task, err
		}
	}
	return nil, ctx.Err()
}

func alreadyAnswered(task *Task, answers map[string]string) bool {
	for _, query := range task.Pending {
		id := query.InterruptID
		text, ok := answers[id]
		if msg := task.Answers[id]; !ok || msg == nil || msg.Content != text {
			return false
		}
	}
	return true
}

func (m *Manager) save(ctx context.Context, task *Task, clearCheckpoint bool) (bool, error) {
	saved, err := m.store.UpdateBackgroundTask(ctx, task, clearCheckpoint)
	if saved && err == nil {
		task.Revision++
	}
	return saved, err
}
