package compose_v2

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"
)

// nodePolicy 是一个节点声明的执行策略：每次尝试的超时（[WithNodeTimeout]）与失败后的重试
// （[WithNodeRetry]）。零值 = 都不设，节点函数只跑一次、不限时。
type nodePolicy struct {
	timeout time.Duration
	retry   RetryConfig
}

func (p nodePolicy) active() bool { return p.timeout > 0 || p.retry.MaxRetries > 0 }

// nodeTimeoutStragglerGrace 是节点超时之后、还没返回时再等多久出声告警。超时取消 ctx 之后，
// 响应 ctx 的节点通常在毫秒级返回（收尾的 IO 也就一两秒）；过了这么久还没回来，基本可以断定
// 它没在看 ctx。
const nodeTimeoutStragglerGrace = 5 * time.Second

// WithNodeTimeout 给节点的**每一次执行尝试**一个时限（d <= 0 = 不设，默认）。配了
// [WithNodeRetry] 时每次重试重新计时；没有「连同重试的总时限」——那个交给调用方 ctx 的 deadline。
//
//	bld.AddNode("fetch", fetchFn,
//	    compose_v2.WithNodeTimeout(30*time.Second),
//	    compose_v2.WithNodeRetry(compose_v2.ExponentialBackoff(2, time.Second, 10*time.Second)))
//
// # 它给的是取消信号，不是「掐断」
//
// 到点时框架取消交给节点的 ctx（`context.Cause(ctx)` 是一个 [*NodeTimeoutError]，节点据此能分出
// 「我超时了」和「调用方喊停」），然后**照样等节点函数返回**。只有 select 了 `ctx.Done()` 的节点会
// 被解开——HTTP、数据库、模型调用这类走 ctx 的天然响应；纯 CPU 循环、不收 ctx 的第三方调用不会。
// Go 没有安全中止一个 goroutine 的办法，框架**不会**丢下它往前走：被丢下的执行还在跑，它的副作用
// 与对存档的写会发生在无人观测、也无人能拦的时刻，比等它更糟（工具级的 `agent_v2.WithToolTimeout`
// 同一取舍）。
//
// 取而代之的是**让失效可见**：超时之后过了几秒还没返回，`slog.Warn` 点名这个节点「它没有响应
// ctx，图在等它」；超时之后才返回成功，同样告警，并且这次结果**按超时丢弃**——超过时限的尝试就是
// 失败的尝试，不看它最后交回了什么（不然超时的效果取决于节点碰巧在什么时候返回）。
//
// # 超时之后是什么错
//
// 这次尝试以 [*NodeTimeoutError] 失败（外面包着带节点位置的 [*GraphNodeError]）。它**不在错误链上
// 露出 `context.DeadlineExceeded`**：那个错在本包的语义里是「调用方的 ctx 到期了」——默认重试谓词
// [IsRetryable] 不重试它、出口把存档标成「等续跑」的挂起。而节点自己超时是**这一次执行失败了**，
// 调用方还活着：配了 [WithNodeRetry] 就按谓词重试，重试用完就是一个普通的节点失败（同超步的兄弟
// 照常跑完、成功的照常提交，续跑只重跑这个节点）。
//
// 下面几种不改写成超时，原样往上传：调用方的 ctx 先死了、节点抛了中断（[Ask] 之类）或别的必须上传的
// 错（见 [MustPropagate]）、节点 panic 了（panic 是代码问题，比超时更该被看见）。
func WithNodeTimeout(d time.Duration) NodeOption {
	return func(c *nodeConfig) {
		if d > 0 {
			c.policy.timeout = d
		}
	}
}

// WithNodeRetry 让节点执行失败后按 cfg 重试（cfg.MaxRetries 次，0 = 不重试）。**MaxRetries > 0 时
// cfg.ShouldRetry 必填**，不给在这里当场 panic（理由见 [RetryConfig.ShouldRetry]：重试会把节点里的
// 副作用再做一遍，只有配重试的人答得上「这个错重跑有没有救」）；想用框架的保守谓词就传
// [SimpleRetry] / [ExponentialBackoff]。
//
// # 重试的是这一次节点执行
//
// 重试在同一个超步里原地进行，是**同一次**节点执行再做一遍（不推进访问计数）。每次尝试：
//
//   - 失败那次尝试的写入、[SendFrom] 投递、上行信号一律作废，只有成功那次的算数；
//   - 节点私有态（[SaveNodeState] / [SaveStepResult]）跨尝试保留——第一次尝试里已经做成、记下的
//     步骤，第二次读回来跳过，副作用不重做；
//   - 节点里的提问从同一个序号数起（[Ask] 算出同一个 ID，第一次尝试里答过的不重问），嵌套调用从
//     同一个编号数起（`agent_v2.RunSubAgent` 已完成的拿回记录、失败的续上它的存档）；节点里直接跑的
//     嵌套图（[Run]）失败或中断过的、配了 checkpointer 时从自己的存档续，已经跑完的存档随成功清掉、
//     下一次尝试从头再跑它——这些都是 [RunWithRetry] 的语义，节点重试就是在节点体外面套了一层它。
//
// 不重试的（[RunWithRetry] 的框架闸，在谓词之前）：中断、调用方的 ctx 已经取消或到期、软取消、陈旧写回、
// 终局失败、坏存档（[ErrCheckpointNotResumable]）、档没写成（[ErrCheckpointNotPersisted]）；谓词否决的也不重试。
// 节点的钩子（OnNodeStart / OnNodeEnd）在所有尝试外面各触发一次。
//
// 配了 [WithNodeTimeout] 时，超时的尝试以 [*NodeTimeoutError] 失败，默认谓词会重试它。
//
// ⚠️ 只重试节点函数本身。出边的条件函数报错不在此列——那由 [WithCondFailPolicy] 决定。
func WithNodeRetry(cfg RetryConfig) NodeOption {
	ValidateRetryConfig(cfg, "WithNodeRetry")
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	return func(c *nodeConfig) { c.policy.retry = cfg }
}

// NodeTimeoutError 是节点的一次执行尝试超过 [WithNodeTimeout] 的时限而失败。用 `errors.As` 取；
// 节点里也能从 `context.Cause(ctx)` 拿到同类型的值，分清是自己超时还是调用方喊停。
//
// **故意不实现 Unwrap**：节点超时时自己返回的错多半包着 `context.DeadlineExceeded`，露在链上的话
// 「错误链上有 ctx 错」的判据（默认重试谓词、出口的挂起判定）会把它当成调用方的 ctx 到期，配了重试
// 也一次都不重试。节点自己返回的错留在 Err 里供排查。
type NodeTimeoutError struct {
	// Timeout 是这次尝试的时限。
	Timeout time.Duration
	// Err 是节点函数自己返回的错；超时之后才返回成功时为 nil（那次结果按超时丢弃）。从
	// `context.Cause` 取到的那个值上也是 nil。
	Err error
}

// Error 返人类可读的错误信息。
func (e *NodeTimeoutError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("节点执行超时（超过 %s）", e.Timeout)
	}
	return fmt.Sprintf("节点执行超时（超过 %s）: %v", e.Timeout, e.Err)
}

// runNodeBody 跑节点函数：按节点声明的策略重试、每次尝试限时。没声明策略时就是一次 safeNodeRun，
// 跟没有这一层时逐字节相同。返回成功那次尝试的写入与它的 SendFrom 收集器（失败时收集器为 nil）。
//
// 每次尝试各配一个 SendFrom 收集器：失败那次尝试里投递的不能流进成功那次的结果——共用一个的话，
// 第一次尝试投递了之后失败、第二次又投递一遍，下一超步就跑两份。
func runNodeBody(ctx context.Context, id NodeID, fn NodeFunc, chs Channels, p nodePolicy) ([]ChannelWrite, *sendSink, error) {
	if !p.active() {
		sink := &sendSink{}
		ws, err := safeNodeRun(withSendSink(ctx, sink), id, fn, chs)
		return ws, sink, err
	}
	type attemptResult struct {
		ws   []ChannelWrite
		sink *sendSink
	}
	attempt := 0
	var lastErr error
	out, err := RunWithRetry(ctx, p.retry, func(c context.Context) (attemptResult, error) {
		attempt++
		if attempt > 1 {
			slog.WarnContext(ctx, "compose_v2: 节点执行失败，按 WithNodeRetry 重试",
				"node", id, "ns", GetGraphNS(ctx), "attempt", attempt, "err", lastErr)
		}
		sink := &sendSink{}
		ws, err := runNodeAttempt(withSendSink(c, sink), id, fn, chs, p.timeout)
		lastErr = err
		return attemptResult{ws: ws, sink: sink}, err
	})
	if err != nil {
		return nil, nil, err
	}
	return out.ws, out.sink, nil
}

// runNodeAttempt 跑节点函数的一次尝试，timeout > 0 时限时（语义见 [WithNodeTimeout]）。
func runNodeAttempt(ctx context.Context, id NodeID, fn NodeFunc, chs Channels, timeout time.Duration) ([]ChannelWrite, error) {
	if timeout <= 0 {
		return safeNodeRun(ctx, id, fn, chs)
	}
	// 每次尝试一个新的 cause 值：只认这一次尝试自己的时限（按指针比）
	cause := &NodeTimeoutError{Timeout: timeout}
	attemptCtx, cancel := context.WithTimeoutCause(ctx, timeout, cause)
	defer cancel()
	var returned atomic.Bool
	straggler := time.AfterFunc(timeout+nodeTimeoutStragglerGrace, func() {
		if returned.Load() {
			return
		}
		slog.WarnContext(ctx, "compose_v2: 节点超过 WithNodeTimeout 之后仍未返回——它没有响应 ctx 取消，图在等它",
			"node", id, "ns", GetGraphNS(ctx), "timeout", timeout)
	})
	// 节点函数 runtime.Goexit 时下面那两行跑不到，defer 补上（不然几秒后报一条「仍未返回」的假告警）
	defer func() {
		returned.Store(true)
		straggler.Stop()
	}()
	ws, err := safeNodeRun(attemptCtx, id, fn, chs)
	// 返回之后立刻置位，不等 defer：下面归因的这段时间里定时器到点的话不该再报「仍未返回」
	returned.Store(true)
	straggler.Stop()
	// 这次尝试自己的时限没到（节点在时限内返回，或者时限到之前调用方的 ctx 先死了）：结果原样交回
	if context.Cause(attemptCtx) != error(cause) {
		return ws, err
	}
	// 时限到了之后才返回成功：按超时丢弃。不看调用方此刻死没死——时限是先到的，超过时限的尝试就是
	// 失败的尝试；看的话结果取决于调用方恰好在节点返回之前还是之后取消（审计实测：时限到了、调用方随后
	// 取消、节点再返回成功，写入照样提交了）。
	if err == nil {
		slog.WarnContext(ctx, "compose_v2: 节点超过 WithNodeTimeout 之后才返回成功——它没有响应 ctx 取消，"+
			"这次执行实际没有被超时打断（结果已按超时丢弃）",
			"node", id, "ns", GetGraphNS(ctx), "timeout", timeout)
		return nil, &NodeTimeoutError{Timeout: timeout}
	}
	// 时限到了、节点带着错返回：调用方随后也死了、中断这类必须上传的错（MustPropagate 两样都认）、panic，
	// 原样交出；其余是这次尝试超时
	if MustPropagate(ctx, err) {
		return ws, err
	}
	var pe *PanicError
	if errors.As(err, &pe) {
		return ws, err
	}
	return nil, &NodeTimeoutError{Timeout: timeout, Err: err}
}
