package schedule_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/schedule"
)

func TestLocal_Every_FiresRepeatedly(t *testing.T) {
	var n atomic.Int64
	l := schedule.NewLocal()
	if err := l.Every("tick", 20*time.Millisecond, func(context.Context) error {
		n.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)
	if err := l.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 120ms / 20ms ≈ 6 次，放宽到 ≥3 防调度抖动 flaky。
	if got := n.Load(); got < 3 {
		t.Fatalf("expected at least 3 fires in 120ms at 20ms interval, got %d", got)
	}
}

// TestLocal_Stop_WaitsForInflightJob 锁住优雅停语义：Stop 必须等在跑的 handler 收尾。
// sanity：若 Stop 只 cancel 不 wait，done 会是 false → fail 指向 bug。
func TestLocal_Stop_WaitsForInflightJob(t *testing.T) {
	var done atomic.Bool
	l := schedule.NewLocal()
	if err := l.Every("slow", 10*time.Millisecond, func(context.Context) error {
		time.Sleep(60 * time.Millisecond)
		done.Store(true)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond) // 让 slow job 进入执行中
	if err := l.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !done.Load() {
		t.Fatal("Stop returned before in-flight job finished")
	}
}

// TestLocal_Stop_TimesOutOnStuckJob 锁住 Stop 的 ctx 兜底：handler 不理会取消时，
// Stop 在自己的 deadline 到期后返回，不无限等待。
func TestLocal_Stop_TimesOutOnStuckJob(t *testing.T) {
	l := schedule.NewLocal()
	if err := l.Every("stuck", 10*time.Millisecond, func(context.Context) error {
		time.Sleep(500 * time.Millisecond) // 无视 ctx
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := l.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

// TestLocal_Panic_DoesNotKillScheduler 锁住 panic 隔离：一个任务 panic 不应拖垮别的
// 任务，且 panic 被转成 error 上报。
func TestLocal_Panic_DoesNotKillScheduler(t *testing.T) {
	var errCount, goodCount atomic.Int64
	l := schedule.NewLocal(schedule.WithErrorHandler(func(string, error) {
		errCount.Add(1)
	}))
	if err := l.Every("boom", 20*time.Millisecond, func(context.Context) error {
		panic("boom")
	}); err != nil {
		t.Fatal(err)
	}
	if err := l.Every("ok", 20*time.Millisecond, func(context.Context) error {
		goodCount.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := l.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if errCount.Load() < 2 {
		t.Fatalf("expected panics recovered and reported, got %d", errCount.Load())
	}
	if goodCount.Load() < 2 {
		t.Fatalf("expected healthy job to keep firing after sibling panics, got %d", goodCount.Load())
	}
}

func TestLocal_Once_FiresExactlyOnce(t *testing.T) {
	var n atomic.Int64
	l := schedule.NewLocal()
	if err := l.Once("soon", time.Now().Add(20*time.Millisecond), func(context.Context) error {
		n.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := l.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := n.Load(); got != 1 {
		t.Fatalf("Once should fire exactly once, fired %d times", got)
	}
}

func TestLocal_Once_PastFiresImmediately(t *testing.T) {
	var n atomic.Int64
	l := schedule.NewLocal()
	if err := l.Once("past", time.Now().Add(-time.Hour), schedule.Simple(func() {
		n.Add(1)
	})); err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := l.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := n.Load(); got != 1 {
		t.Fatalf("past Once should fire once immediately, fired %d times", got)
	}
}

func TestLocal_Validation(t *testing.T) {
	l := schedule.NewLocal()
	noop := schedule.Simple(func() {})
	if err := l.Every("", time.Second, noop); err == nil {
		t.Error("expected error for empty name")
	}
	if err := l.Every("niljob", time.Second, nil); err == nil {
		t.Error("expected error for nil job")
	}
	if err := l.Every("badinterval", 0, noop); err == nil {
		t.Error("expected error for non-positive interval")
	}
	if err := l.Every("dup", time.Second, noop); err != nil {
		t.Fatal(err)
	}
	if err := l.Every("dup", time.Second, noop); err == nil {
		t.Error("expected error for duplicate name")
	}
	if err := l.Cron("badexpr", "not a cron", "", noop); err == nil {
		t.Error("expected error for bad cron expr")
	}
	if err := l.Cron("badtz", "0 9 * * *", "Nowhere/Nowhere", noop); err == nil {
		t.Error("expected error for bad timezone")
	}
}

func TestLocal_AddAfterStartRejected(t *testing.T) {
	l := schedule.NewLocal()
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer l.Stop(context.Background())
	if err := l.Every("late", time.Second, schedule.Simple(func() {})); err == nil {
		t.Error("expected error adding job after Start")
	}
}

func TestLocal_DoubleStartRejected(t *testing.T) {
	l := schedule.NewLocal()
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer l.Stop(context.Background())
	if err := l.Start(context.Background()); err == nil {
		t.Error("expected error on double Start")
	}
}

// TestLocal_ConcurrentStartStop 用 channel 屏障强制 Start 与 Stop 同时冲，验证并发
// 调用下无 WaitGroup 误用（-race 下才有意义）。sanity：若把 wg.Add 挪回 mu 释放之后，
// -race 会在这里报 Add/Wait 竞争。
func TestLocal_ConcurrentStartStop(t *testing.T) {
	for i := 0; i < 50; i++ {
		l := schedule.NewLocal()
		if err := l.Every("j", 5*time.Millisecond, schedule.Simple(func() {})); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; _ = l.Start(context.Background()) }()
		go func() { defer wg.Done(); <-start; _ = l.Stop(context.Background()) }()
		close(start) // 屏障：两个 goroutine 同时释放
		wg.Wait()
		_ = l.Stop(context.Background()) // 兜底停掉（Stop 先于 Start 命中时 above 是 no-op）
	}
}

// TestLocal_NoRestartAfterStop 锁住一次性语义：Stop 后不能再 Start / Add。
// sanity：撤掉 Start 的 stopped 守卫，这里 Start 会成功 → 断言 fail。
func TestLocal_NoRestartAfterStop(t *testing.T) {
	l := schedule.NewLocal()
	if err := l.Every("j", time.Second, schedule.Simple(func() {})); err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := l.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err == nil {
		t.Error("Stop 后再 Start 应返回 error（一次性语义）")
	}
	if err := l.Every("late", time.Second, schedule.Simple(func() {})); err == nil {
		t.Error("Stop 后再 Add 应返回 error")
	}
}

// TestLocal_DoubleStopIdempotent 锁住重复 Stop 幂等返回 nil。
func TestLocal_DoubleStopIdempotent(t *testing.T) {
	l := schedule.NewLocal()
	if err := l.Every("j", 10*time.Millisecond, schedule.Simple(func() {})); err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := l.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := l.Stop(context.Background()); err != nil {
		t.Errorf("重复 Stop 应幂等返回 nil，得到 %v", err)
	}
}

// TestLocal_NoRestartRace 在 -race 下验证不存在 "restart 引发两代 runEntry 共享同一
// trigger 闭包" 的竞争。一次性语义禁 restart 后该路径不可达。
// sanity：撤掉 Start 的 stopped 守卫，本用例在 -race 下会因两代 runEntry 竞争 anchor 报 race。
func TestLocal_NoRestartRace(t *testing.T) {
	for i := 0; i < 100; i++ {
		release := make(chan struct{})
		l := schedule.NewLocal()
		_ = l.Every("j", time.Microsecond, func(ctx context.Context) error {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil
		})
		_ = l.Start(context.Background())
		// 第 1 代 Stop 用极短超时提前返回，handler 卡在 release，留活着的 gen-1 goroutine。
		ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
		_ = l.Stop(ctx)
		cancel()
		_ = l.Start(context.Background()) // 禁 restart 后返 error；撤守卫则起第 2 代 → race
		close(release)
		_ = l.Stop(context.Background())
	}
}
