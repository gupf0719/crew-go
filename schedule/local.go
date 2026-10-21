package schedule

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"crew-go/internal/panicerr"
)

// Local 是进程内定时任务调度器，对标 APScheduler 的 BackgroundScheduler。
//
// 每个任务一个 goroutine，串行地 wait→run→wait，因此同一任务天然不会重叠执行：
// 上一次没跑完就跳过本次触发窗口（等价 APScheduler max_instances=1）。
//
// 零值不可用，请用 NewLocal 构造。所有 Cron/Every/Once 注册必须在 Start 之前完成；
// Start 后再注册返回 error。调度器是一次性的：Stop 之后不能再 Start（返回 error），
// 需要重启请新建一个 Local。
type Local struct {
	mu      sync.Mutex
	entries []*localEntry
	started bool
	stopped bool // Stop 后置 true；一次性语义，禁止 restart（防跨代复用 wg / trigger 闭包）
	cancel  context.CancelFunc
	onError func(name string, err error)
	loc     *time.Location
	wg      sync.WaitGroup
}

type triggerFunc func(after time.Time) (time.Time, bool)

type localEntry struct {
	name    string
	trigger triggerFunc
	job     JobFunc
}

// LocalOption 配置 Local。
type LocalOption func(*Local)

// WithErrorHandler 设置任务返回 error 或 panic 时的回调（默认用 slog.Error 记录）。
// 多个任务并发失败时该回调可能被并发调用，实现需自行保证线程安全。
func WithErrorHandler(fn func(name string, err error)) LocalOption {
	return func(l *Local) {
		if fn != nil {
			l.onError = fn
		}
	}
}

// WithLocation 设置 Every/Once 的默认时区参照（Cron 的时区在其表达式注册时单独给）。
// 默认 time.Local。
func WithLocation(loc *time.Location) LocalOption {
	return func(l *Local) {
		if loc != nil {
			l.loc = loc
		}
	}
}

// NewLocal 构造进程内调度器。
func NewLocal(opts ...LocalOption) *Local {
	l := &Local{
		loc: time.Local,
		onError: func(name string, err error) {
			slog.Error("scheduled job failed", "job", name, "err", err)
		},
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Cron 按标准 crontab 表达式（5 字段：分 时 日 月 周）注册任务。tz 为 IANA 时区名
// （如 "Asia/Shanghai"，空串用调度器默认时区）。表达式非法或时区未知返回 error。
func (l *Local) Cron(name, expr, tz string, job JobFunc) error {
	if err := validate(name, job); err != nil {
		return err
	}
	loc := l.loc
	if tz != "" {
		parsed, err := time.LoadLocation(tz)
		if err != nil {
			return fmt.Errorf("load timezone %q: %w", tz, err)
		}
		loc = parsed
	}
	sched, err := parseCron(expr, loc)
	if err != nil {
		return err
	}
	return l.add(name, sched.next, job)
}

// Every 按固定间隔注册任务，对齐固定节拍（不因 handler 耗时逐次漂移）。d 必须为正。
func (l *Local) Every(name string, d time.Duration, job JobFunc) error {
	if err := validate(name, job); err != nil {
		return err
	}
	if d <= 0 {
		return fmt.Errorf("interval must be positive, got %v (job %q)", d, name)
	}
	return l.add(name, intervalTrigger(d), job)
}

// Once 在指定时刻触发一次；若该时刻在 Start 时已过，则启动后立即触发一次。
func (l *Local) Once(name string, at time.Time, job JobFunc) error {
	if err := validate(name, job); err != nil {
		return err
	}
	return l.add(name, onceTrigger(at), job)
}

func (l *Local) add(name string, trigger triggerFunc, job JobFunc) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.started || l.stopped {
		return fmt.Errorf("cannot add job %q after Start/Stop", name)
	}
	for _, e := range l.entries {
		if e.name == name {
			return fmt.Errorf("duplicate job name %q", name)
		}
	}
	l.entries = append(l.entries, &localEntry{name: name, trigger: trigger, job: job})
	return nil
}

// Start 启动调度：为每个已注册任务开一个 goroutine 循环触发。重复 Start 返回 error。
// 传入的 ctx 取消等价于调用 Stop（但不等待在跑的 handler 收尾，需要优雅停请用 Stop）。
func (l *Local) Start(ctx context.Context) error {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return fmt.Errorf("scheduler has been stopped and cannot be restarted")
	}
	if l.started {
		l.mu.Unlock()
		return fmt.Errorf("scheduler already started")
	}
	l.started = true
	runCtx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	entries := l.entries // Start 后 add 被拒，切片不再变更，可安全引用
	// wg.Add 必须在释放 mu 前完成：Stop 在 mu 保护下判定 started，任何看到
	// started=true 的并发 Stop，其随后（释放 mu 后）的 wg.Wait 一定晚于这里的 Add，
	// 从而杜绝 "Add 与 Wait 并发" 的 WaitGroup 误用（Start/Stop 并发调用时）。
	l.wg.Add(len(entries))
	l.mu.Unlock()

	for _, e := range entries {
		go l.runEntry(runCtx, e)
	}
	return nil
}

// Stop 优雅停止：取消所有任务的 ctx（不再触发新一轮），并等待在跑的 handler 收尾。
// 若 ctx 先到期（handler 未在期限内退出）返回 ctx.Err()。重复调用幂等（返回 nil）；
// 未 Start 即 Stop 也是 no-op，但同样标记为已停——之后不能再 Start。
func (l *Local) Stop(ctx context.Context) error {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return nil // 幂等：重复 Stop、或未 Start 即 Stop
	}
	l.stopped = true
	wasStarted := l.started
	l.started = false
	cancel := l.cancel
	l.mu.Unlock()

	if !wasStarted {
		return nil // 从未 Start，无 goroutine 需等待
	}
	cancel()
	done := make(chan struct{})
	go func() {
		l.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *Local) runEntry(ctx context.Context, e *localEntry) {
	defer l.wg.Done()
	for {
		next, ok := e.trigger(time.Now())
		if !ok {
			return // 一次性已触发 / cron 未来无匹配
		}
		d := time.Until(next)
		if d < 0 {
			d = 0
		}
		timer := time.NewTimer(d)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if err := runSafely(ctx, e.job); err != nil {
			l.onError(e.name, err)
		}
	}
}

// runSafely 调用 handler 并把 panic 转成 error，避免一个任务 panic 掀翻它的调度
// goroutine（进而让该任务永久停摆 / 拖垮别的任务）。
func runSafely(ctx context.Context, job JobFunc) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pe := panicerr.New(r)
			err = panicerr.WrapOpaque("job panicked", pe)
			panicerr.Log(ctx, slog.LevelError, "schedule: 任务 panic，已转成错误交给 onError", pe)
		}
	}()
	return job(ctx)
}

func validate(name string, job JobFunc) error {
	if name == "" {
		return fmt.Errorf("job name must not be empty")
	}
	if job == nil {
		return fmt.Errorf("job func must not be nil (job %q)", name)
	}
	return nil
}

// intervalTrigger 生成固定间隔触发器：首次在 start+d 触发，之后对齐 start+n*d 的节拍，
// 因此 handler 耗时不会逐次累积漂移。触发器仅被单个调度 goroutine 串行调用，闭包内
// anchor 无需加锁。
func intervalTrigger(d time.Duration) triggerFunc {
	var anchor time.Time
	return func(after time.Time) (time.Time, bool) {
		if anchor.IsZero() {
			anchor = after
		}
		n := int64(after.Sub(anchor) / d)
		return anchor.Add(time.Duration(n+1) * d), true
	}
}

// onceTrigger 生成一次性触发器：过期则返回 after（启动后立即触发），否则返回目标时刻；
// 触发一次后再调返回 false 让调度 goroutine 退出。同样仅被单 goroutine 串行调用。
func onceTrigger(at time.Time) triggerFunc {
	var fired bool
	return func(after time.Time) (time.Time, bool) {
		if fired {
			return time.Time{}, false
		}
		fired = true
		if at.Before(after) {
			return after, true
		}
		return at, true
	}
}
