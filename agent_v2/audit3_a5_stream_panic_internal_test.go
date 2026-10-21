package agent_v2

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"crew-go/modules/common"
)

// audit³ A5 fix 守护测试.
//
// A5: streamReaderToSchema goroutine 内 panic (hookReader.Recv / send / Close
// 任一) Go runtime 直接 fatal 全 process 崩——framework outer recover 不 cover
// 跨 goroutine. 修法: goroutine 顶部 defer recover 转 schema.SendError 推给
// 消费方让 modelFn 走错误路径.
//
// sanity-revert: 删 streamReaderToSchema goroutine 顶部 defer recover →
// hookReader.Recv panic 直接 fatal 进程 → test process crash.

// panicReader 模拟 hook 返的 stream reader, 首次 Recv 即 panic.
type panicReader struct {
	closed bool
}

func (r *panicReader) Recv(_ context.Context) (*common.Message, error) {
	panic("audit3 A5: hookReader.Recv panic")
}

func (r *panicReader) Close() error {
	r.closed = true
	return nil
}

// eofReader 用作 baseline (verify normal flow works).
type eofReader struct {
	closed bool
}

func (r *eofReader) Recv(_ context.Context) (*common.Message, error) {
	return nil, io.EOF
}
func (r *eofReader) Close() error {
	r.closed = true
	return nil
}

func TestAudit3_A5_StreamReaderGoroutine_PanicRecovers(t *testing.T) {
	// baseline: eofReader normal close → 无 panic + Recv 收 EOF
	{
		reader := streamReaderToSchema(context.Background(), &eofReader{})
		msg, err := reader.Recv(context.Background())
		if !errors.Is(err, io.EOF) {
			t.Fatalf("baseline: expect EOF, got msg=%v err=%v", msg, err)
		}
	}

	// panic 路径: hookReader.Recv panic — goroutine defer recover 转 schema.SendError
	pr := &panicReader{}
	reader := streamReaderToSchema(context.Background(), pr)

	// 消费方 Recv 应收到 wrapped panic err (不是进程 crash)
	_, err := reader.Recv(context.Background())
	if err == nil {
		t.Fatal("audit³ A5 fix regression: panic 后消费方应收 err, got nil\n" +
			"sanity-revert: 删 streamReaderToSchema goroutine 顶部 defer recover → 进程 fatal crash")
	}
	if !strings.Contains(err.Error(), "panic") {
		t.Errorf("err 应含 panic 字样 (说明是 panic-wrapped), got %v", err)
	}
}
