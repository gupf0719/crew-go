package compose_v2

import (
	"context"
	"errors"
	"sync/atomic"
)

// ErrSoftCanceled 是软取消触发后 Run 返回的哨兵——业务 errors.Is(err, ErrSoftCanceled) 判定，
// 区别于 InterruptError（HITL 等用户答，cp.PendingInterrupts 非空）和 context.Canceled（硬停）。
var ErrSoftCanceled = errors.New("run soft-canceled at superstep boundary")

// SoftCancelSignal 是外部异步「优雅取消」句柄。业务持有它、在任意 goroutine 调 Trigger() 请求
// Run「在下个超步边界优雅停下并存档」。
//
// 与 ctx.Done()（硬停）的区别：
//   - **不掐正在跑的超步**——当前超步所有分支的 NodeFunc / LLM / tool 都跑完才停（current
//     superstep 善终），停在干净的超步边界。
//   - **存档可 Resume**——复用 frontierAtExit + Run 出口 SaveNS（需配 Checkpointer），Run 返
//     ErrSoftCanceled，cp.CurrentFrontier 指向下一超步；业务可 Resume 续跑或 ResumeActionAbort 弃用。
//
// **协作式 drain**（在超步边界优雅停，非抢占式掐断）：超步边界天然是安全点，所以只需这一个
// 外部信号就能优雅停整个 Run，不必引入「turn 三态」之类的额外状态机。
//
// **一次性语义**：Trigger 后 Triggered() 恒返 true——每次 Run / Resume 都建**新**信号，不跨
// Run 复用已触发的。复用已触发信号去 Resume 会在节点重跑**之前**于首个超步边界停下、把
// cp.PendingInterrupts 视图清空（状态可恢复，但 HITL UI 读 pending 会反直觉）。
//
// **边界丢弃语义**：Trigger 到达**最后一个超步**（isEnd 那步）是 no-op——Run 正常成功完成
// （err==nil）、软取消被丢弃（任务本就要结束了）。所以**别依赖「Trigger 后 err 必是
// ErrSoftCanceled」**；软取消只在「还有下一超步要跑」时生效。
//
// **嵌套子图**：经 ctx 透传自动继承父信号（见 WithSoftCancelCtx），一个顶层 Trigger 优雅停
// 整棵树（业务用全新 ctx 调 sub.Run 则子图不被父软取消影响，边界正确）。**并发安全**：
// Trigger（业务 goroutine）与 Triggered（Run goroutine 在超步边界读）经 atomic，-race clean。
type SoftCancelSignal struct {
	triggered atomic.Bool
	reason    atomic.Pointer[string]
}

// NewSoftCancelSignal 建一个未触发的软取消信号。
func NewSoftCancelSignal() *SoftCancelSignal { return &SoftCancelSignal{} }

// Trigger 请求优雅停止（可在任意 goroutine 调）。reason 写进返回的 ErrSoftCanceled 供诊断/UI。
func (s *SoftCancelSignal) Trigger(reason string) {
	s.reason.Store(&reason) // 先存 reason 再置 triggered——保证读到 triggered=true 时 reason 已就绪
	s.triggered.Store(true)
}

// Triggered 返回是否已请求取消 + reason（Run 在超步边界调）。
func (s *SoftCancelSignal) Triggered() (bool, string) {
	if !s.triggered.Load() {
		return false, ""
	}
	if r := s.reason.Load(); r != nil {
		return true, *r
	}
	return true, ""
}

// softCancelCtxKey 是软取消信号跨 Run / 子图透传的私有 ctx key（私有类型防碰撞）。
type softCancelCtxKey struct{}

// WithSoftCancelCtx 把软取消信号塞进 ctx——嵌套子图 / sub-agent 沿用父 ctx 自动继承同一信号
// （指针共享，顶层 Trigger 停整棵树）。业务用 agent_v2 编排时经此透传（同 WithCheckpointerCtx
// 一致模式）；compose_v2.Run 直接调时用 WithSoftCancel RunOption 即可。
func WithSoftCancelCtx(ctx context.Context, s *SoftCancelSignal) context.Context {
	return context.WithValue(ctx, softCancelCtxKey{}, s)
}

// GetSoftCancel 从 ctx 读软取消信号，未设返 nil。
func GetSoftCancel(ctx context.Context) *SoftCancelSignal {
	if s, ok := ctx.Value(softCancelCtxKey{}).(*SoftCancelSignal); ok {
		return s
	}
	return nil
}

// WithSoftCancel 给本次 Run 配软取消信号（RunOption）。优先级高于 ctx 继承；都没设则无软取消。
func WithSoftCancel(s *SoftCancelSignal) RunOption {
	return func(o *runOptions) { o.softCancel = s }
}

// resolveSoftCancel 定 Run 实际用哪个信号：RunOption 显式 > 父 ctx 继承 > nil（无软取消）。
func resolveSoftCancel(ctx context.Context, optVal *SoftCancelSignal) *SoftCancelSignal {
	if optVal != nil {
		return optVal
	}
	return GetSoftCancel(ctx)
}
