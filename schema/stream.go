package schema

import (
	"context"
	"errors"
	"io"
	"sync"
)

// StreamReader 为消耗流项提供基于拉取的接口。
//
// 生命周期：
//   - 生产者：调用 send 函数推送项；出错时调用 SendError 标记错误；
//     完成时调用 CloseSend 信号流正常结束
//   - 消费者：调用 Recv 直到返 io.EOF（正常结束）或具体业务错；
//     ErrStreamCanceled 表示消费者主动 Close
//
// **API 演进**（Phase 5.6 audit）：
//   - 旧 Recv 返 (T, bool) — ok=false 不区分 EOF / error / cancel
//   - 新 Recv 返 (T, error) — 业务错通过 SendError 推到消费端，区分 io.EOF 与
//     具体错。compose_v2 chunk streaming 等业务能正确传播 LLM 流式中途出错。
type StreamReader[T any] struct {
	ch       chan T
	errCh    chan error // 业务错通道——SendError 写入；Recv 优先排空 ch 后从此读
	done     chan struct{}
	recvOnce sync.Once // 保护 close(done)   — 消费者端
	sendOnce sync.Once // 保护 close(ch)      — 生产者端

	// heldErr 是 Recv 已经从 errCh 取出、但 ch 里还有错误之前发出的项而暂缓交出的错误：
	// 错误必须排在它之前发出的项后面。只有 Recv 读写它。
	mu      sync.Mutex
	heldErr error
}

// ErrStreamCanceled 表示消费者主动调 Close 中止了流（Recv 应返此错给生产者）。
var ErrStreamCanceled = errors.New("stream canceled by consumer")

// NewStreamReader 创建 StreamReader 和发送函数。
// 生产者调用 send(item) 推送项，然后在完成时调用 CloseSend(sr)。
// 出错时调用 SendError(sr, err)——下次 Recv 返 err。
//
// **review-v6 M-24 文档化协议** (consumer 必读):
//
// caller 必须 defer reader.Close() — 否则消费者 abandon 时 producer 在
// `sr.ch <- item` (line 44) 阻塞 (buffer 满 + 无人读 + done 未 fire) 永久泄漏
// goroutine + 持有的 HTTP body / connection 不释放. send 闭包不监听 ctx 是
// 设计权衡 (改接 ctx 需 API 改动), caller 侧 defer Close 是必修规则.
//
// 错误 pattern (会泄漏):
//
//	reader, _ := llm.Stream(ctx, msgs)
//	if panic { return } // ★ reader 没关 → producer 永久阻塞
//
// 正确 pattern:
//
//	reader, _ := llm.Stream(ctx, msgs)
//	defer reader.Close() // ★ 必须 defer
func NewStreamReader[T any](bufSize int) (*StreamReader[T], func(T)) {
	sr := &StreamReader[T]{
		ch:    make(chan T, bufSize),
		errCh: make(chan error, 1), // 容量 1：业务错只投递一次
		done:  make(chan struct{}),
	}
	send := func(item T) {
		select {
		case sr.ch <- item:
		case <-sr.done:
		}
	}
	return sr, send
}

// Recv 返回下一个项。
//
// 返回 error 四态：
//   - nil：成功取到项，T 是有效值
//   - io.EOF：流正常结束（生产者调 CloseSend 且无 pending err）
//   - ErrStreamCanceled：消费者主动 Close 中止
//   - ctx.Err()：caller ctx cancel / deadline 到（典型 context.Canceled 或
//     context.DeadlineExceeded）——audit Round 7 Bug 4 修复后新增四态
//   - 其他业务错：生产者通过 SendError 推送（如 LLM 流式中途出错）
//
// **audit Round 7 Bug 4 修复**：之前 Recv 不接 ctx，第三方 model SDK 实现的
// producer goroutine 不感知 ctx 时（典型场景：批量 proxy / 异步队列）caller
// ctx cancel 完全无效，Recv 永久阻塞。改为强制 ctx 参数让 caller 必显式传 ctx，
// select 加 ctx.Done 分支保证 cancel 即时生效。
//
// 实现优先排空 ch（避免 ch 和 done/errCh 同时就绪时 select 随机选中导致消息
// 丢失）。ch 关闭后再检查 errCh 让"buffered chunks + 末尾 error"先消费 chunks。
//
// **错误排在它之前发出的项后面**：生产方「send → SendError」时，项一定先进了 ch。可消费方
// 若恰在两次 select 之间被抢占，醒来时 ch 与 errCh 同时就绪、select 随机选，先拿到错误就
// 丢了那一项（实测 30 万次 1 次）。所以取到错误时再看一眼 ch：还有项就先交项、错误暂缓，
// 等 ch 里错误之前的项交完再交错误。
func (sr *StreamReader[T]) Recv(ctx context.Context) (T, error) {
	var zero T
	// 优先非阻塞地排空 ch 中已有的消息（不被 ctx cancel 影响——已收到的 chunk
	// 应让 caller 看到，避免末尾几条丢失）
	select {
	case item, ok := <-sr.ch:
		if ok {
			return item, nil
		}
		return zero, sr.endErr()
	default:
	}
	if err := sr.takeHeldErr(); err != nil {
		return zero, err
	}
	// ch 为空时同时监听 done / errCh / ctx
	select {
	case item, ok := <-sr.ch:
		if ok {
			return item, nil
		}
		return zero, sr.endErr()
	case err := <-sr.errCh:
		return sr.deliverErr(err)
	case <-sr.done:
		return zero, ErrStreamCanceled
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

// deliverErr 交出刚从 errCh 取到的错误；ch 里还有项（错误之前发出的）时先交项、错误暂缓。
func (sr *StreamReader[T]) deliverErr(err error) (T, error) {
	if err == nil {
		err = io.EOF
	}
	select {
	case item, ok := <-sr.ch:
		if ok {
			sr.holdErr(err)
			return item, nil
		}
	default:
	}
	var zero T
	return zero, err
}

// endErr 是 ch 已关闭时的结论：暂缓的错误 > errCh 里的错误 > io.EOF。
func (sr *StreamReader[T]) endErr() error {
	if err := sr.takeHeldErr(); err != nil {
		return err
	}
	select {
	case err := <-sr.errCh:
		if err == nil {
			return io.EOF
		}
		return err
	default:
		return io.EOF
	}
}

func (sr *StreamReader[T]) holdErr(err error) {
	sr.mu.Lock()
	sr.heldErr = err
	sr.mu.Unlock()
}

func (sr *StreamReader[T]) takeHeldErr() error {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	err := sr.heldErr
	sr.heldErr = nil
	return err
}

// Close 表示消费者不再需要项。
// 可以安全地并发或多次调用。
//
// 生产者结束流用 [CloseSend]，不用 Close：消费者读完已发出的项之后拿到的是 ErrStreamCanceled
// （流被中止），而不是 io.EOF（流正常结束）。BasicAgent 把前者当成这次模型调用失败。
func (sr *StreamReader[T]) Close() {
	sr.recvOnce.Do(func() {
		close(sr.done)
	})
}

// Done 返回一个 channel，消费者调用 Close() 后该 channel 被关闭。
// 生产者可以监听此 channel 以感知消费者提前退出，避免 goroutine 泄漏。
func (sr *StreamReader[T]) Done() <-chan struct{} {
	return sr.done
}

// CloseSend 在生产者发送所有项时调用。
// 可以安全地并发或多次调用。
func CloseSend[T any](sr *StreamReader[T]) {
	sr.sendOnce.Do(func() {
		close(sr.ch)
	})
}

// SendError 投递业务错给消费者——下次 Recv 返该 err。仅投递一次（多次调用后续
// 静默丢弃，避免 buffer 阻塞）。
//
// 典型用法：LLM 流式生成中途出错（rate limit / network drop）时生产者投递错给
// 消费者；消费者拿到具体 err 而非"似乎正常结束"。
func SendError[T any](sr *StreamReader[T], err error) {
	if err == nil {
		return
	}
	select {
	case sr.errCh <- err:
	default:
		// errCh 已满（已投递过）——静默丢弃
	}
}

// NewSingleChunkReader 将单个值包装为已关闭的 StreamReader。
// 常用于将同步调用结果适配为流式接口（降级包装）。
func NewSingleChunkReader[T any](item T) *StreamReader[T] {
	sr, send := NewStreamReader[T](1)
	send(item)
	CloseSend(sr)
	return sr
}
