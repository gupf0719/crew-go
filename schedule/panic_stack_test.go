package schedule_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schedule"
)

//go:noinline
func scheduleJobExplodes(context.Context) error { panic("job boom") }

// TestLocal_PanicErrorCarriesStack：任务 panic 时交给 onError 的错误上挂着带栈的 compose_v2.PanicError，错误文本不带栈。
func TestLocal_PanicErrorCarriesStack(t *testing.T) {
	var (
		mu  sync.Mutex
		got error
	)
	l := schedule.NewLocal(schedule.WithErrorHandler(func(_ string, err error) {
		mu.Lock()
		if got == nil {
			got = err
		}
		mu.Unlock()
	}))
	if err := l.Every("boom", 10*time.Millisecond, scheduleJobExplodes); err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		done := got != nil
		mu.Unlock()
		if done || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := l.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	err := got
	mu.Unlock()
	if err == nil {
		t.Fatal("onError 没收到错误")
	}
	var pe *compose_v2.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("errors.As 取不到 PanicError：%v", err)
	}
	if !strings.Contains(pe.Stack, "schedule_test.scheduleJobExplodes") {
		t.Fatalf("栈里没有 panic 发生处：\n%s", pe.Stack)
	}
	if strings.Contains(err.Error(), "goroutine ") {
		t.Errorf("错误文本不该带栈：%q", err.Error())
	}
}
