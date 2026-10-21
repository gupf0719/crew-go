package schema_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"crew-go/schema"
)

// TestStreamReader_RecvErrorVsEOF 验证 audit Bug D 修复：Recv 返 (T, error) 区分
// 三态——nil（正常）/ io.EOF（流正常结束）/ 业务错（生产者 SendError 推送）/
// ErrStreamCanceled（消费者主动 Close）。
func TestStreamReader_RecvErrorVsEOF(t *testing.T) {
	t.Run("normal_eof", func(t *testing.T) {
		sr, send := schema.NewStreamReader[string](2)
		send("a")
		send("b")
		schema.CloseSend(sr)

		if v, err := sr.Recv(context.Background()); err != nil || v != "a" {
			t.Fatalf("first recv: %v %q", err, v)
		}
		if v, err := sr.Recv(context.Background()); err != nil || v != "b" {
			t.Fatalf("second recv: %v %q", err, v)
		}
		// 第三次：EOF
		if _, err := sr.Recv(context.Background()); !errors.Is(err, io.EOF) {
			t.Fatalf("expected io.EOF after CloseSend, got %v", err)
		}
	})

	t.Run("business_error_after_chunks", func(t *testing.T) {
		sr, send := schema.NewStreamReader[string](2)
		send("first")
		bizErr := errors.New("rate limit exceeded")
		schema.SendError(sr, bizErr)
		schema.CloseSend(sr)

		// 先消费 chunks
		if v, err := sr.Recv(context.Background()); err != nil || v != "first" {
			t.Fatalf("first recv: %v %q", err, v)
		}
		// 然后拿到业务错（不是 EOF）
		_, err := sr.Recv(context.Background())
		if !errors.Is(err, bizErr) {
			t.Fatalf("expected bizErr after chunks consumed, got %v", err)
		}
	})

	t.Run("consumer_canceled", func(t *testing.T) {
		sr, _ := schema.NewStreamReader[string](2)
		sr.Close()
		_, err := sr.Recv(context.Background())
		if !errors.Is(err, schema.ErrStreamCanceled) {
			t.Fatalf("expected ErrStreamCanceled after consumer Close, got %v", err)
		}
	})

	// audit Bug #4 验证：多 chunks + SendError（无 CloseSend）时 consumer 应先
	// 消费完所有 chunks 再拿到 error——不能因为 errCh 就绪就跳过 ch 中 pending 项
	t.Run("multi_chunks_then_error_no_close_send", func(t *testing.T) {
		sr, send := schema.NewStreamReader[string](5)
		send("a")
		send("b")
		send("c")
		bizErr := errors.New("mid-stream rate limit")
		schema.SendError(sr, bizErr)
		// 注意：故意不调 CloseSend——验证 consumer 仍能拿到所有 chunks 再拿 err

		// 先 3 次 Recv 拿 chunks
		gotChunks := []string{}
		for i := 0; i < 3; i++ {
			v, err := sr.Recv(context.Background())
			if err != nil {
				t.Fatalf("recv #%d: expected chunk, got err=%v", i+1, err)
			}
			gotChunks = append(gotChunks, v)
		}
		if gotChunks[0] != "a" || gotChunks[1] != "b" || gotChunks[2] != "c" {
			t.Fatalf("chunks order wrong: %v", gotChunks)
		}
		// 第 4 次 Recv 应拿到业务错（chunks 已全消费）
		_, err := sr.Recv(context.Background())
		if !errors.Is(err, bizErr) {
			t.Fatalf("expected bizErr after consuming all chunks, got %v", err)
		}
	})
}
