package fallback

import (
	"context"
	"errors"
	"fmt"
	"io"

	"crew-go/schema"
)

// peekStream 在交给调用方前检查首个有效片段。候选选定后由原 reader 交付前置帧。
func peekStream(ctx context.Context, raw *schema.StreamReader[*schema.Message]) (*schema.StreamReader[*schema.Message], error) {
	sawMessage := false
	_, err := raw.ReadAhead(ctx, func(msg *schema.Message) bool {
		sawMessage = sawMessage || msg != nil
		return visibleStreamChunk(msg)
	})
	if err == nil || (errors.Is(err, io.EOF) && sawMessage) {
		return raw, nil
	}
	raw.Close()
	if errors.Is(err, io.EOF) {
		err = fmt.Errorf("fallback: model stream ended before its first chunk: %w", io.ErrUnexpectedEOF)
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
