package openai

import (
	"errors"
	"fmt"
	"io"
	"time"
)

// DefaultStreamIdleTimeout 是流式响应默认的空闲超时：这么久一个字节都没收到，就当连接已经断了。
//
// 只算在等数据的时间：消费方读得慢、流在等消费方取走时不计。keep-alive 注释行（`: keep-alive`）也是
// 收到的数据——provider 排队时用它保活（DeepSeek 排队超过 10 分钟会自己关连接），所以这里不另设
// 「只收到心跳、没有内容」的计时器。
const DefaultStreamIdleTimeout = 5 * time.Minute

// ErrStreamIdleTimeout 表示流式响应超过空闲超时没收到任何数据（见 [WithStreamIdleTimeout]）。
//
// 它包在 StatusCode 为 0 的 model.APIError 里、作为流的错误交出（发生在建流之后），可重试：编排成员重试、
// 节点重试（RunWithRetry）会接住，重做整个成员 / 节点；fallback 只在建流阶段切换候选，不因它换下一个。错误链上
// 不带 context.DeadlineExceeded——这不是调用方的期限到了。
var ErrStreamIdleTimeout = errors.New("stream idle timeout")

// WithStreamIdleTimeout 设流式响应的空闲超时（默认 [DefaultStreamIdleTimeout]）：provider 发了一部分
// 之后停住、既不再发也不关连接时，超过 d 没收到任何数据就关掉连接，流以 [ErrStreamIdleTimeout] 结束。
// 没有它时只有调用方的 ctx 兜底，没设期限的调用会一直挂着。
//
// 计时只覆盖读响应体的等待，从响应头到达之后开始；连接与等响应头的时间由 http.Client 的超时与 ctx
// 管。d <= 0 关闭。已经收到结束原因之后才超时的（在等末尾的 usage 帧），输出仍算完整。
func WithStreamIdleTimeout(d time.Duration) ClientOption {
	return func(c *Client) { c.streamIdleTimeout = d }
}

// idleReader 包住响应体：每次 Read 等数据的时间超过 timeout 就关掉响应体，阻塞着的 Read 随之返回，
// 这次 Read 报 [ErrStreamIdleTimeout]。计时器只在 Read 里走：两次 Read 之间（流在等消费方）不计。
type idleReader struct {
	body    io.ReadCloser
	timeout time.Duration
	timer   *time.Timer
}

func newIdleReader(body io.ReadCloser, timeout time.Duration) *idleReader {
	r := &idleReader{body: body, timeout: timeout}
	r.timer = time.AfterFunc(timeout, func() { _ = body.Close() }) // 关闭失败也没有别的办法让 Read 返回，错误由 Read 交出
	r.timer.Stop()
	return r
}

func (r *idleReader) Read(p []byte) (int, error) {
	r.timer.Reset(r.timeout)
	n, err := r.body.Read(p)
	// Stop 返回 false：计时器已经触发（Reset 之后它一定在走），响应体是被它关掉的
	if !r.timer.Stop() {
		return n, fmt.Errorf("%w: no data for %s", ErrStreamIdleTimeout, r.timeout)
	}
	return n, err
}

func (r *idleReader) Close() error {
	r.timer.Stop()
	return r.body.Close()
}
