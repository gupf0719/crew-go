package background

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/internal/panicerr"
	"crew-go/schema"
)

const storageGrace = 10 * time.Second

// Serve 周期驱动可运行任务和待交付通知。宿主取消 ctx 后等待这次执行收尾。
func (m *Manager) Serve(ctx context.Context) error {
	for ctx.Err() == nil {
		if err := m.RunOnce(ctx); err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "后台子 Agent 扫描未完成", "err", err)
		}
		timer := time.NewTimer(m.cfg.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	return ctx.Err()
}

// RunOnce 扫描一轮可运行任务。可由宿主的调度器调用。
func (m *Manager) RunOnce(ctx context.Context) error {
	var failures []error
	for after := ""; ctx.Err() == nil; {
		pollCtx, stop := context.WithTimeout(ctx, storageGrace)
		ids, err := m.store.ListReadyBackgroundTasks(pollCtx, after, 64, m.cfg.Notify != nil)
		stop()
		if err != nil {
			return errors.Join(append(failures, err)...)
		}
		if len(ids) == 0 {
			break
		}
		for _, id := range ids {
			if ctx.Err() != nil {
				return errors.Join(append(failures, ctx.Err())...)
			}
			if id <= after {
				return errors.Join(append(failures, errors.New("background: task listing did not advance"))...)
			}
			after = id
			if err := m.runTask(ctx, id); err != nil && !errors.Is(err, compose_v2.ErrTaskBusy) {
				failures = append(failures, fmt.Errorf("background: drive task %q: %w", id, err))
			}
		}
	}
	return errors.Join(append(failures, ctx.Err())...)
}

// runTask 隔离业务回调退出 goroutine，扫描器仍可接着处理下一项。
func (m *Manager) runTask(ctx context.Context, id string) error {
	done := make(chan error, 1)
	go func() {
		err := errors.New("background: task driver exited without returning")
		defer func() { done <- err }()
		err = m.advance(ctx, id)
	}()
	return <-done
}

// taskCheckpoint 将这次任务根档的清理延迟到持久结果提交。子图照常清理自己的存档。
type taskCheckpoint struct {
	Store
	id string
}

func (cp taskCheckpoint) DeleteSubtree(ctx context.Context, sid, ns string, condition ...compose_v2.CheckpointDeleteCondition) (int32, error) {
	if sid == cp.id && ns == "" {
		return 0, nil
	}
	return cp.Store.DeleteSubtree(ctx, sid, ns, condition...)
}

func (m *Manager) advance(workerCtx context.Context, id string) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			p := panicerr.New(rec)
			panicerr.Log(workerCtx, slog.LevelError, "后台子任务驱动捕获 panic", p)
			err = panicerr.WrapOpaque("background drive", p)
		}
	}()
	// 派单进程的运行态和会话键不会进入子任务。
	ctx := compose_v2.WithBoundCheckpointerCtx(agent_v2.DetachTask(workerCtx), taskCheckpoint{Store: m.store, id: id})
	ctx, lease, err := compose_v2.BeginTaskActivity(ctx, id, nil)
	if err != nil {
		return err
	}
	defer lease.Release()

	readCtx, stop := context.WithTimeout(ctx, storageGrace)
	task, err := m.store.LoadBackgroundTask(readCtx, id)
	stop()
	if err != nil || task == nil {
		return err
	}
	ctx = compose_v2.WithThreadIDCtx(compose_v2.WithOwner(ctx, task.Parent.Owner), task.ID)
	if task.Terminal() {
		return m.notify(ctx, task)
	}
	if task.CancelRequested {
		return m.finish(ctx, id, Cancelled, nil, "")
	}
	factory := m.cfg.Workers[task.Worker]
	if factory == nil || task.Status == Waiting {
		return nil
	}
	if task.Status != Queued && task.Status != Running {
		return fmt.Errorf("unknown task status %q", task.Status)
	}

	loadCtx, stop := context.WithTimeout(ctx, storageGrace)
	cp, err := m.store.LoadNS(loadCtx, id, "")
	stop()
	if err != nil {
		return err
	}
	first := task.Status == Queued
	if cp != nil && (first || cp.ID != id || cp.NS != "") {
		return errors.New("background: checkpoint does not belong to the current task")
	}
	if !first && !cp.HasResumableWork() {
		// 首次派发和最终结算之间失联且无档时，无法确认副作用，保留可查终态。
		return m.finish(ctx, id, Failed, nil, "execution ended before a resumable checkpoint or result was recorded")
	}
	if cp != nil && len(cp.PendingInterrupts) > 0 && !allAnswered(cp.PendingInterrupts, task.Answers) {
		return m.suspend(ctx, id, cp.PendingInterrupts)
	}

	runCtx, cancel := context.WithTimeout(ctx, m.cfg.AttemptTimeout)
	defer cancel()
	agent, err := factory(runCtx, task.ID)
	if err != nil {
		// 工厂的依赖可能暂时不可用，保留任务供后续接手。
		return fmt.Errorf("construct worker %q: %w", task.Worker, err)
	}
	if agent_v2.IsNilAgent(agent) {
		return fmt.Errorf("worker %q returned no agent", task.Worker)
	}
	task, err = retryWithLease(runCtx, func(ctx context.Context) (*Task, error) {
		return m.claim(ctx, id)
	})
	if err != nil || task == nil {
		return err
	}
	if task.CancelRequested {
		return m.finish(ctx, id, Cancelled, nil, "")
	}

	runCtx = compose_v2.WithDurabilityCtx(runCtx, compose_v2.DurabilitySync)
	events, runErr := m.call(runCtx, cancel, task, agent, !first)
	if err := compose_v2.CheckCheckpointTaskLease(ctx); err != nil {
		return err
	}
	aborted := errors.Is(runErr, compose_v2.ErrRunAborted)
	if errors.Is(runErr, compose_v2.ErrTaskLeaseLost) || errors.Is(runErr, compose_v2.ErrCheckpointStale) ||
		errors.Is(runErr, compose_v2.ErrGraphIDConflict) || errors.Is(runErr, compose_v2.ErrCheckpointNotResumable) ||
		(!aborted && errors.Is(runErr, compose_v2.ErrCheckpointNotPersisted)) {
		return runErr
	}
	if stop := workerCtx.Err(); stop != nil && errors.Is(runErr, stop) && !aborted {
		// 因宿主停止而退出时交出执行权，已知的结果仍正常提交。
		return stop
	}
	if _, interrupted := compose_v2.IsInterrupt(runErr); interrupted && !aborted {
		loadCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), storageGrace)
		latest, err := m.store.LoadNS(loadCtx, id, "")
		stop()
		if err != nil {
			return err
		}
		if latest == nil || len(latest.PendingInterrupts) == 0 {
			return m.finish(ctx, id, Failed, nil, "agent interrupted without a persisted checkpoint")
		}
		if latest.ID != id || latest.NS != "" {
			return errors.New("background: interrupt checkpoint does not belong to the current task")
		}
		return m.suspend(ctx, id, latest.PendingInterrupts)
	}
	if runErr != nil {
		return m.finish(ctx, id, Failed, nil, runErr.Error())
	}
	result := agent_v2.FinalMessage(events)
	if result == nil {
		return m.finish(ctx, id, Failed, nil, "agent completed without a final message")
	}
	return m.finish(ctx, id, Succeeded, result, "")
}

// claim 在当前租约内读回上一次提交。已写 Running 但确认丢失时，首次派发仍由同一工作进程完成。
func (m *Manager) claim(ctx context.Context, id string) (*Task, error) {
	for ctx.Err() == nil {
		task, err := m.store.LoadBackgroundTask(ctx, id)
		if err != nil || task == nil || task.CancelRequested {
			return task, err
		}
		if task.Status == Running {
			return task, nil
		}
		if task.Status != Queued {
			return nil, fmt.Errorf("background: cannot start a task in status %q", task.Status)
		}
		task.Status = Running
		saved, err := m.save(ctx, task, false)
		if saved || err != nil {
			return task, err
		}
	}
	return nil, ctx.Err()
}

func (m *Manager) call(ctx context.Context, cancel context.CancelFunc, task *Task, agent agent_v2.Agent, resume bool) ([]*agent_v2.AgentEvent, error) {
	// 轮询取消只控制本次子任务的 ctx，执行结束后等待观察 goroutine 退出。
	watchCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if rec := recover(); rec != nil {
				panicerr.Log(ctx, slog.LevelError, "后台子任务的取消检查捕获 panic", panicerr.New(rec))
				cancel()
			}
		}()
		ticker := time.NewTicker(m.cfg.PollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-ticker.C:
				readCtx, finish := context.WithTimeout(watchCtx, storageGrace)
				latest, err := m.store.LoadBackgroundTask(readCtx, task.ID)
				finish()
				if err == nil && latest != nil && latest.CancelRequested {
					cancel()
					return
				}
			}
		}
	}()
	defer func() {
		stop()
		<-done
	}()
	session := agent_v2.NewRunSession()
	session.RunID = task.ID
	if resume {
		return agent.Resume(ctx, task.ID, agent_v2.WithResumeSession(session), agent_v2.WithResumeUserInputs(task.Answers))
	}
	return agent.Run(ctx, session, task.Input)
}

func allAnswered(pending []*compose_v2.AskQuery, answers map[string]*schema.Message) bool {
	for _, query := range pending {
		if query == nil || answers[query.InterruptID] == nil {
			return false
		}
	}
	return true
}

func (m *Manager) suspend(ctx context.Context, id string, pending []*compose_v2.AskQuery) error {
	seen := make(map[string]bool, len(pending))
	for _, query := range pending {
		if query == nil || query.InterruptID == "" || seen[query.InterruptID] {
			return errors.New("background: checkpoint contains an invalid pending interrupt")
		}
		seen[query.InterruptID] = true
	}
	return m.complete(ctx, id, Waiting, nil, "", pending)
}

func (m *Manager) finish(ctx context.Context, id string, status Status, result *schema.Message, failure string) error {
	return m.complete(ctx, id, status, result, failure, nil)
}

func (m *Manager) complete(ctx context.Context, id string, status Status, result *schema.Message, failure string, pending []*compose_v2.AskQuery) error {
	// 进度可能停在最后超步前。已知结果交给当前租约重试落盘，避免换进程后重做最后一步。
	task, err := retryWithLease(context.WithoutCancel(ctx), func(ctx context.Context) (*Task, error) {
		return m.commitOutcome(ctx, id, status, result, failure, pending)
	})
	if err != nil || task == nil || !task.Terminal() {
		return err
	}
	return m.notify(ctx, task)
}

func retryWithLease(ctx context.Context, attempt func(context.Context) (*Task, error)) (*Task, error) {
	writeCtx, stop := context.WithTimeout(ctx, storageGrace)
	defer stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for writeCtx.Err() == nil {
		if err := compose_v2.CheckCheckpointTaskLease(writeCtx); err != nil {
			return nil, err
		}
		task, err := attempt(writeCtx)
		if err == nil || errors.Is(err, compose_v2.ErrTaskLeaseLost) || writeCtx.Err() != nil {
			return task, err
		}
		select {
		case <-writeCtx.Done():
			return nil, errors.Join(err, writeCtx.Err())
		case <-ticker.C:
		}
	}
	return nil, writeCtx.Err()
}

func (m *Manager) commitOutcome(ctx context.Context, id string, status Status, result *schema.Message, failure string, pending []*compose_v2.AskQuery) (*Task, error) {
	for ctx.Err() == nil {
		task, err := m.store.LoadBackgroundTask(ctx, id)
		if err != nil || task == nil {
			return nil, err
		}
		if task.Terminal() {
			return task, nil
		}
		task.Status, task.Result, task.Failure, task.Pending = status, result, failure, pending
		if task.CancelRequested {
			task.Status, task.Result, task.Failure, task.Pending = Cancelled, nil, "", nil
		}
		terminal := task.Terminal()
		if terminal {
			task.Answers = nil
		} else {
			for _, query := range pending {
				delete(task.Answers, query.InterruptID)
			}
		}
		saved, err := m.save(ctx, task, terminal)
		if err != nil {
			return nil, err
		}
		if saved {
			return task, nil
		}
	}
	return nil, ctx.Err()
}
func (m *Manager) notify(ctx context.Context, task *Task) error {
	if m.cfg.Notify == nil || task.Notified || ctx.Err() != nil {
		return ctx.Err()
	}
	// 通知可以驱动正挂起的父任务，重建它的会话和存档上下文。
	parentCtx := compose_v2.WithCheckpointerCtx(agent_v2.DetachTask(ctx), m.store)
	parentCtx = compose_v2.WithThreadIDCtx(parentCtx, task.Parent.ThreadID)
	notifyCtx, stop := context.WithTimeout(parentCtx, m.cfg.AttemptTimeout)
	defer stop()
	if err := m.cfg.Notify(notifyCtx, *task); err != nil {
		return fmt.Errorf("notify parent: %w", err)
	}
	writeCtx, stopWrite := context.WithTimeout(context.WithoutCancel(ctx), storageGrace)
	defer stopWrite()
	// 单独读取用于确认的快照，回调修改结果或输入时不会改写已提交的内容。
	stored, err := m.store.LoadBackgroundTask(writeCtx, task.ID)
	if err != nil {
		return err
	}
	if stored == nil || !stored.Terminal() {
		return errors.New("background: terminal notification lost its task record")
	}
	stored.Notified = true
	saved, err := m.save(writeCtx, stored, false)
	if err == nil && !saved {
		return errors.New("background: task changed while acknowledging notification")
	}
	return err
}
