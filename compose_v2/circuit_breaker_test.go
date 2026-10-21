package compose_v2_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
)

// TestCircuitBreaker_RecordFailureEdge 验 RecordFailure 只在 closed→open 首次熔断边缘返
// true，已 tripped 时探测失败返 false——供 fallback member.tripped 边缘检测不重复计（一次
// 熔断只 emit 一次 tripped event，探测失败刷新 cooldown 但不重复观测）。
func TestCircuitBreaker_RecordFailureEdge(t *testing.T) {
	cb := compose_v2.NewCircuitBreakerWithCooldown(2, time.Minute) // maxFailures=2

	if cb.RecordFailure() {
		t.Error("第 1 次失败（<maxFailures）不应 trip，应返 false")
	}
	if !cb.RecordFailure() {
		t.Error("第 2 次失败（达 maxFailures）应返 true（closed→open 首次熔断边缘）")
	}
	if cb.RecordFailure() {
		t.Error("已 tripped 再失败（探测失败）应返 false（不重复计边缘）")
	}

	// RecordSuccess 解除 trip 后，再次连续失败到阈值应重新返回边缘 true。
	cb.RecordSuccess()
	if cb.RecordFailure() {
		t.Error("解除 trip 后第 1 次失败不应 trip")
	}
	if !cb.RecordFailure() {
		t.Error("解除 trip 后再达阈值应重新返 true（新一轮 closed→open 边缘）")
	}
}

// TestCircuitBreaker_RecordFailureEdge_Concurrent 验并发 RecordFailure 下"首次熔断边缘"
// 只被恰好一个 goroutine 观测到（justTripped 在锁内基于 !tripped 判断）——这是 fallback
// member.tripped **不重复 emit** 的并发保证；未来若破坏锁会让多个 goroutine 同时认为自己
// 触发首次熔断，本测试 -race + 计数双重兜底。
func TestCircuitBreaker_RecordFailureEdge_Concurrent(t *testing.T) {
	cb := compose_v2.NewCircuitBreakerWithCooldown(1, time.Minute) // maxFailures=1：首次失败即熔断
	const n = 50
	var trippedCount int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // 屏障：尽量同时触发 thundering herd，逼出潜在 race
			if cb.RecordFailure() {
				atomic.AddInt64(&trippedCount, 1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if trippedCount != 1 {
		t.Errorf("并发 RecordFailure 应恰好 1 个观测到首次熔断边缘, got %d", trippedCount)
	}
}
