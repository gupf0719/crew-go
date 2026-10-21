package schema

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// 生产方「send → SendError」时项一定先进 ch；消费方在两次 select 之间被抢占的话，醒来时 ch 与
// errCh 同时就绪。这里直接造出那个状态：取到错误时 ch 里还有项，必须先交项、错误排在后面。
func TestStreamReader_ErrorAfterPendingItems(t *testing.T) {
	boom := errors.New("boom")
	sr, send := NewStreamReader[int](4)
	send(1)
	send(2)
	CloseSend(sr)

	v, err := sr.deliverErr(boom)
	if err != nil || v != 1 {
		t.Fatalf("ch 里还有错误之前发出的项，应先交出 1，实际 v=%d err=%v", v, err)
	}
	v, err = sr.Recv(context.Background())
	if err != nil || v != 2 {
		t.Fatalf("第二项应接着交出，实际 v=%d err=%v", v, err)
	}
	if _, err = sr.Recv(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("项交完之后应交出暂缓的错误，实际 %v", err)
	}
	if _, err = sr.Recv(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("错误交出之后应是 io.EOF，实际 %v", err)
	}
}

// 错误已暂缓、ch 空了但生产方还没 CloseSend：照样交出错误，不等关流。
func TestStreamReader_HeldErrorWithoutClose(t *testing.T) {
	boom := errors.New("boom")
	sr, send := NewStreamReader[int](4)
	send(1)
	if v, err := sr.deliverErr(boom); err != nil || v != 1 {
		t.Fatalf("应先交出 1，实际 v=%d err=%v", v, err)
	}
	// 带超时：暂缓的错误没被交出时 Recv 会一直等下去
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := sr.Recv(ctx); !errors.Is(err, boom) {
		t.Fatalf("应交出暂缓的错误，实际 %v", err)
	}
}
