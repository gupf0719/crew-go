package anthropic

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/packages/ssestream"

	"crew-go/internal/panicerr"
	"crew-go/schema"
)

// consumeStream 读 SSE 事件：文本与 thinking 文本逐片交给 send，事件同时累积成完整响应；流正常结束
// 后按完整响应交出最后一片（工具调用、标注、元数据），否则 SendError。最后总会 CloseSend。
//
// 下面这些都报错，不当正常结束：读响应体出错、调用方 ctx 取消、错误帧、事件累积失败、没收到
// message_stop 就断开、结束原因是拒答或超窗口（见 checkStopReason）。只闭流不报错的话，消费方读到
// io.EOF，会把半截内容当成完整答复。消费方自己 Close 的不给结论。
//
// cancel 取消的是读响应体用的 ctx：消费方 Close 时调它，阻塞在读响应体上的 Next 立刻返回。
func (c *Client) consumeStream(ctx context.Context, cancel context.CancelFunc, stream *ssestream.Stream[anthropic.BetaRawMessageStreamEventUnion], sr *schema.StreamReader[*schema.Message], send func(*schema.Message)) {
	defer schema.CloseSend(sr)
	defer cancel()
	defer func() { _ = stream.Close() }() // 关响应体的错误没有下文
	// 解析 / send 路径上的 panic 兜住，作为流的错误交给消费方，不让整个进程退出。
	defer func() {
		if r := recover(); r != nil {
			pe := panicerr.New(r)
			schema.SendError(sr, panicerr.WrapOpaque("anthropic consumeStream panic", pe))
			panicerr.Log(ctx, slog.LevelError, "anthropic: 流解析 goroutine 兜住 panic，已作为流错误交给消费方", pe)
		}
	}()
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		select {
		case <-sr.Done():
			cancel()
		case <-finished:
		}
	}()

	var (
		acc     anthropic.BetaMessage
		joiner  textJoiner
		stopped bool
		// rawInputs 收下每个 tool_use 块参数的原文：SDK 累积时会把截断的参数换成 {}，判断与上报截断要看
		// 原文（见 interpret）。
		rawInputs = map[int]*strings.Builder{}
		// reasoningStarted：已经交出过非空 thinking 文本；reasoningSep：下一片非空 thinking 文本前
		// 要先补分隔（新的 thinking 块开始了）。跟 joinContent 的拼法一致。
		reasoningStarted, reasoningSep bool
	)
	for stream.Next() {
		ev := stream.Current()
		if err := acc.Accumulate(ev); err != nil {
			schema.SendError(sr, c.streamError(fmt.Errorf("accumulate %s event: %w", ev.Type, err)))
			return
		}
		switch ev.Type {
		case "content_block_start":
			switch ev.ContentBlock.Type {
			case "text":
			case "thinking":
				joiner.nonText()
				reasoningSep = reasoningStarted
			default:
				joiner.nonText()
			}
		case "content_block_delta":
			switch ev.Delta.Type {
			case "text_delta":
				if s := joiner.piece(ev.Delta.Text); s != "" {
					send(&schema.Message{Role: schema.RoleAssistant, Content: s})
				}
			case "input_json_delta":
				b := rawInputs[int(ev.Index)]
				if b == nil {
					b = &strings.Builder{}
					rawInputs[int(ev.Index)] = b
				}
				b.WriteString(ev.Delta.PartialJSON)
			case "thinking_delta":
				if s := ev.Delta.Thinking; s != "" {
					if reasoningSep {
						s = reasoningSeparator + s
						reasoningSep = false
					}
					reasoningStarted = true
					send(&schema.Message{Role: schema.RoleAssistant, ReasoningContent: s})
				}
			}
		case "message_stop":
			stopped = true
		}
	}

	select {
	case <-sr.Done():
		return // 消费方已经不要了，不用再给结论
	default:
	}
	// ctx 取消要作为错误交出去：只闭流的话，消费方的 Recv 可能先读到 io.EOF。
	if err := ctx.Err(); err != nil {
		schema.SendError(sr, err)
		return
	}
	// 收到 message_stop 之后才空闲超时的（经代理 / 网关时发完了却不关连接）：答复已经完整，照常收尾、不当失败——当失败的话
	// 是可重试的错，配了重试会把一次完整的调用整个重来。别的读错照旧报错。
	if err := stream.Err(); err != nil && !(stopped && errors.Is(err, ErrStreamIdleTimeout)) {
		schema.SendError(sr, c.streamFailure(err))
		return
	}
	if !stopped {
		schema.SendError(sr, c.streamError(errors.New("stream ended without message_stop")))
		return
	}
	raw := make(map[int]string, len(rawInputs))
	for i, b := range rawInputs {
		raw[i] = b.String()
	}
	turn, err := c.interpret(ctx, &acc, raw)
	if err != nil {
		schema.SendError(sr, err)
		return
	}
	send(&schema.Message{
		Role:          schema.RoleAssistant,
		ContentBlocks: turn.calls,
		Annotations:   turn.annotations,
		Metadata:      turn.metadata,
	})
}

// streamFailure 把流中途的错误包成 *model.APIError（StatusCode 为 0）。错误帧（overloaded_error
// 等）带上它的原文，截短后放进错误文本。
func (c *Client) streamFailure(err error) error {
	var apiErr *anthropic.Error
	if errors.As(err, &apiErr) {
		return c.streamError(fmt.Errorf("stream error event: %s", clip(apiErr.RawJSON())))
	}
	return c.streamError(fmt.Errorf("read stream: %w", err))
}
