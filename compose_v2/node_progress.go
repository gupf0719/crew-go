package compose_v2

import (
	"context"
	"sync"
	"time"
)

type nodeProgressKey struct{}

func nodeProgressFrom(ctx context.Context) *nodeProgress {
	p, _ := ctx.Value(nodeProgressKey{}).(*nodeProgress)
	return p
}

func withNodeProgress(ctx context.Context, p *nodeProgress) context.Context {
	return context.WithValue(ctx, nodeProgressKey{}, p)
}

// Heartbeat 为当前节点尝试及其外层尝试报告进度。可以并发调用。
// 图外、已取消或已结束的尝试调用它时无效果。
func Heartbeat(ctx context.Context) {
	if ctx == nil {
		return
	}
	if p := nodeProgressFrom(ctx); p != nil && p.tracksIdle && ctx.Err() == nil {
		p.touch()
	}
}

// nodeProgress 记录尝试的寿命。继承外层预算的子尝试也有独立的寿命，
// 因此保存的旧 ctx 或 EventSink 无法替后续尝试续期。
type nodeProgress struct {
	parent   *nodeProgress
	lifetime context.Context // 尝试自身的取消归属，派生钩子可以先取消自己的 ctx。

	limit      time.Time // ctx 对下游公布的不变 deadline。
	wall       time.Time // 本次尝试自身的固定时限。
	idle       time.Duration
	tracksIdle bool
	expire     func()

	mu       sync.Mutex
	stopped  bool
	deadline time.Time
	timer    *time.Timer
}

func newNodeProgress(ctx context.Context, parent *nodeProgress, wall time.Time, idle time.Duration, expire func()) *nodeProgress {
	limit, _ := ctx.Deadline()
	p := &nodeProgress{
		parent: parent, lifetime: ctx, limit: limit, wall: wall, idle: idle, expire: expire,
		tracksIdle: idle > 0 || (parent != nil && parent.tracksIdle),
	}
	if idle > 0 {
		// 极短预算也要等 timer 已登记，再让回调拿锁。
		p.mu.Lock()
		p.deadline = time.Now().Add(idle)
		p.timer = time.AfterFunc(idle, p.tick)
		p.mu.Unlock()
	}
	return p
}

func (p *nodeProgress) touch() {
	now := time.Now()
	for ; p != nil && p.renew(now); p = p.parent {
	}
}

func (p *nodeProgress) renew(now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped || p.lifetime.Err() != nil {
		return false
	}
	if p.idleDue(now) {
		p.expire()
		return false
	}
	if !p.limit.IsZero() && !now.Before(p.limit) {
		return false
	}
	if p.idle > 0 {
		// 并发事件可能乱序取锁。仅推进时限，不为每个流式片段重置 timer。
		if next := now.Add(p.idle); next.After(p.deadline) {
			p.deadline = next
		}
	}
	return true
}

func (p *nodeProgress) tick() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped || p.lifetime.Err() != nil {
		return
	}
	now := time.Now()
	if p.idleDue(now) {
		p.expire()
		return
	}
	if remaining := p.deadline.Sub(now); remaining > 0 {
		p.timer.Reset(remaining)
	}
}

// idleDue 在 mu 内裁决。固定或继承的 deadline 更早或相同时，由其原有 ctx 负责取消。
func (p *nodeProgress) idleDue(now time.Time) bool {
	return p.idle > 0 && !now.Before(p.deadline) && (p.limit.IsZero() || p.deadline.Before(p.limit))
}

func (p *nodeProgress) stop(at time.Time) bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return false
	}
	// 返回可能先于已到期的 timer 回调拿到锁。用绝对时间核对迟到的结果。
	due := !at.IsZero() && p.idleDue(at)
	if due && p.lifetime.Err() == nil {
		p.expire()
	}
	p.stopped = true
	if p.timer != nil {
		p.timer.Stop()
	}
	return due
}

// inheritedNodeTimeout 检查从外层节点继承的预算，供子图执行和结算使用。
// ctx 尚未收到定时器通知时也检查上层固定期限和当前空闲时限。
func inheritedNodeTimeout(ctx context.Context) error {
	p := nodeProgressFrom(ctx)
	if p == nil {
		return nil
	}
	now := time.Now()
	for ; p != nil; p = p.parent {
		if p.expireIfDue(now) {
			// 固定时限已到时，让原生 timer 送达取消再核对归属。
			<-p.lifetime.Done()
		}
		if _, timedOut := context.Cause(p.lifetime).(*NodeTimeoutError); timedOut {
			if err := ctx.Err(); err != nil {
				return err
			}
			return p.lifetime.Err()
		}
	}
	return nil
}

func (p *nodeProgress) expireIfDue(at time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped || p.lifetime.Err() != nil {
		return false
	}
	if p.idleDue(at) {
		p.expire()
		return true
	}
	return !p.wall.IsZero() && !at.Before(p.wall)
}

// progressEventSink 在进入同步观测钩子前登记节点事件的进度。流撤回属于收尾。
type progressEventSink struct {
	inner    EventSink
	progress *nodeProgress
}

func (s progressEventSink) Send(ev Event) {
	if ev.Type != "reset_stream" {
		s.progress.touch()
	}
	s.inner.Send(ev)
}
