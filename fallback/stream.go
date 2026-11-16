package fallback

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"crew-go/schema"
)

const streamPeekLimit = 64

// 每次候选尝试单独传递回执。先看到同一流终态的内层可标记已经为实际候选设置等待期。
type streamGateReceiptKey struct{}

type streamGateReceipt struct {
	applied atomic.Bool
}

// peekStream 查找有效片段，同时限制暂存帧数。候选选定后由原 reader 交付前置帧。
func peekStream(ctx context.Context, raw *schema.StreamReader[*schema.Message]) (*schema.StreamReader[*schema.Message], error) {
	sawMessage := false
	frames := 0
	_, err := raw.ReadAhead(ctx, func(msg *schema.Message) bool {
		sawMessage = sawMessage || msg != nil
		frames++
		return visibleStreamChunk(msg) || frames >= streamPeekLimit
	})
	if sawMessage && (err == nil || errors.Is(err, io.EOF)) {
		return raw, nil
	}
	raw.Close()
	if err == nil || errors.Is(err, io.EOF) {
		err = fmt.Errorf("fallback: model stream returned no message during lookahead: %w", io.ErrUnexpectedEOF)
	}
	return nil, err
}

func visibleStreamChunk(msg *schema.Message) bool {
	if msg == nil {
		return false
	}
	if msg.Content != "" || msg.ReasoningContent != "" || msg.Refusal != "" {
		return true
	}
	for _, block := range msg.ContentBlocks {
		if block != nil {
			return true
		}
	}
	return false
}
