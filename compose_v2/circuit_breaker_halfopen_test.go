package compose_v2_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
)

// TestCircuitBreaker_HalfOpen_SingleProbe 验证 half-open thundering herd 修复：cooldown
// 过后 N 个**并发** Allow 只放行 1 个探测（其他返 false），防 fan-out 多 branch 共享
// per-session breaker 时 cooldown 过后同时探测 compact LLM（N 次浪费）。
//
// sanity：revert 修复（Allow cooldown 过后无 probing 全放行）后，N 个并发 Allow 都返 true
// → allowed==N != 1 → FAIL 精确指向 thundering herd。
func TestCircuitBreaker_HalfOpen_SingleProbe(t *testing.T) {
	cb := compose_v2.NewCircuitBreakerWithCooldown(1, 10*time.Millisecond)
	cb.RecordFailure() // maxFailures=1 → trip
	if cb.Allow() {
		t.Fatal("刚 trip cooldown 未过应 false")
	}
	time.Sleep(25 * time.Millisecond) // cooldown 过

	const N = 32
	var wg sync.WaitGroup
	start := make(chan struct{})
	var allowed atomic.Int64
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // 屏障：强制真正并发（防 Go scheduler 串行化 thundering herd）
			if cb.Allow() {
				allowed.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := allowed.Load(); got != 1 {
		t.Fatalf("half-open 应只放行单次探测，got %d 个 Allow=true（thundering herd）", got)
	}
}

// TestCircuitBreaker_HalfOpen_ProbeStuckRecovers 验证 probing 卡死兜底：探测方
// panic/忘记回报（不调 RecordSuccess/Failure）时 probing 不永久卡死——超过一个
// cooldown 后允许新探测，防永久禁用（这正是引入 probing 状态机时最需防的新 bug）。
func TestCircuitBreaker_HalfOpen_ProbeStuckRecovers(t *testing.T) {
	cb := compose_v2.NewCircuitBreakerWithCooldown(1, 10*time.Millisecond)
	cb.RecordFailure() // trip
	time.Sleep(25 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("cooldown 过应放行首次探测")
	}
	if cb.Allow() {
		t.Fatal("probing 在飞期间应拒绝并发探测（单次探测语义）")
	}
	// 探测方不回报（模拟 panic/忘记 Record*）——超过一个 cooldown 后应恢复放行
	time.Sleep(25 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("探测卡死超一个 cooldown 应恢复放行（防 probing 永久卡死永久禁用）")
	}
}

// TestCircuitBreaker_HalfOpen_ProbeSuccessCloses 验证探测成功正常闭合 + 探测失败重开。
func TestCircuitBreaker_HalfOpen_ProbeSuccessCloses(t *testing.T) {
	cb := compose_v2.NewCircuitBreakerWithCooldown(1, 10*time.Millisecond)
	cb.RecordFailure()
	time.Sleep(25 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("cooldown 过应放行探测")
	}
	cb.RecordSuccess() // 探测成功 → closed
	if !cb.Allow() {
		t.Fatal("探测成功后应 closed 恒放行")
	}
	if cb.Tripped() {
		t.Fatal("探测成功后 tripped 应 false")
	}

	// 探测失败 → 重新 open，下个 cooldown 后再放行单次探测
	cb2 := compose_v2.NewCircuitBreakerWithCooldown(1, 10*time.Millisecond)
	cb2.RecordFailure()
	time.Sleep(25 * time.Millisecond)
	if !cb2.Allow() {
		t.Fatal("cooldown 过应放行探测")
	}
	cb2.RecordFailure() // 探测失败 → 重置 cooldown + 清 probing
	if cb2.Allow() {
		t.Fatal("探测失败后重新 open + cooldown 重置，应 false")
	}
	time.Sleep(25 * time.Millisecond)
	if !cb2.Allow() {
		t.Fatal("再过 cooldown 应再放行单次探测")
	}
}
