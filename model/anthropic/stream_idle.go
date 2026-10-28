package anthropic

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/anthropics/anthropic-sdk-go/option"

	"crew-go/model"
)

// DefaultStreamIdleTimeout 是流式响应默认的空闲超时：这么久一个字节都没收到，就当连接已经断了。
//
// 只算在等数据的时间：消费方读得慢、流在等消费方取走时不计。收到 ping 事件也算收到数据，所以不另设「只收到心跳、
// 没有内容」的计时器。
//
// 5 分钟够不够长：Anthropic 的 API 文档只说流里可能有 ping、没写间隔；Anthropic 自家的 Claude Code 在直连 API 时，对
// 「连一个字节都没收到（含 SSE 心跳）」默认 180 秒就判连接已断（code.claude.com 的 Enterprise network configuration
// 文档，Streaming idle watchdogs 一节），说明直连 API 在长时间思考（thinking 不显示时整段思考没有 thinking_delta）等
// 静默期会发 ping 保活。这里比它宽。经网关 / 代理时 ping 可能不被转发，那时按需调大或关闭。
const DefaultStreamIdleTimeout = 5 * time.Minute

// ErrStreamIdleTimeout 表示流式响应超过空闲超时没收到任何数据（见 [WithStreamIdleTimeout]）。它就是
// [model.ErrStreamIdleTimeout]（别名）：一个判据认得所有客户端的空闲超时。
//
// 它包在 StatusCode 为 0 的 model.APIError 里、作为流的错误交出（发生在建流之后），可重试：模型调用重试
// （agent_v2.WithModelRetry）、编排成员重试、节点重试会接住；fallback 只在建流阶段切换候选，不因它换下一个。错误链上
// 不带 context.DeadlineExceeded——这不是调用方的期限到了。
var ErrStreamIdleTimeout = model.ErrStreamIdleTimeout

// WithStreamIdleTimeout 设流式响应的空闲超时（默认 [DefaultStreamIdleTimeout]）：provider 发了一部分之后停住、既不再发
// 也不关连接时，超过 d 没收到任何数据就关掉连接，流以 [ErrStreamIdleTimeout] 结束。没有它时只有调用方的 ctx 兜底，
// 没设期限的调用会一直挂着。
//
// 计时只覆盖读响应体的等待，从响应头到达之后开始；连接与等响应头的时间由 http.Client 的超时与 ctx 管。d <= 0 关闭。
// 只作用于 Stream；Generate 另有单次请求超时（[WithRequestTimeout]）。
func WithStreamIdleTimeout(d time.Duration) ClientOption {
	return func(c *Client) { c.streamIdleTimeout = d }
}

// idleMiddleware 是建流那次请求上的 SDK middleware：响应成功时把响应体包进 idleReader。
func idleMiddleware(timeout time.Duration) option.RequestOption {
	return option.WithMiddleware(func(req *http.Request, next option.MiddlewareNext) (*http.Response, error) {
		resp, err := next(req)
		if err == nil && resp != nil && resp.Body != nil && resp.StatusCode/100 == 2 {
			resp.Body = newIdleReader(resp.Body, timeout)
		}
		return resp, err
	})
}

// idleReader 包住响应体：每次 Read 等数据的时间超过 timeout 就关掉响应体，阻塞着的 Read 随之返回，这次 Read 报
// [ErrStreamIdleTimeout]。计时器只在 Read 里走：两次 Read 之间（流在等消费方）不计。（model/openai 有同样的一份，
// 两个包各自的错误哨兵不同，没有抽成共用的。）
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
