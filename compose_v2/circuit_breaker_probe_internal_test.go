package compose_v2

import (
	"testing"
	"time"
)

func TestCircuitBreaker_ExpiredProbeCannotSettleReplacement(t *testing.T) {
	cb := NewCircuitBreakerWithCooldown(1, time.Hour)
	cb.RecordFailure()
	cb.trippedAt = time.Now().Add(-2 * time.Hour)
	old, ok := cb.AllowWithProbe()
	if !ok || old == 0 {
		t.Fatal("冷却结束后应放行半开探测")
	}
	cb.probeAt = time.Now().Add(-2 * time.Hour)
	current, ok := cb.AllowWithProbe()
	if !ok || current == 0 || current == old {
		t.Fatal("探测过期后应放行另一个探测")
	}
	for _, result := range []CircuitBreakerResult{CircuitBreakerIgnored, CircuitBreakerSucceeded, CircuitBreakerFailed} {
		cb.RecordAttempt(old, result)
		cb.RecordAttempt(0, result)
		if cb.WouldAllow() || !cb.Tripped() || cb.failures != 1 {
			t.Fatalf("过期探测或普通请求的结局 %v 影响了当前探测", result)
		}
	}
	cb.RecordAttempt(current, CircuitBreakerIgnored)
	retry, ok := cb.AllowWithProbe()
	if !ok || retry == 0 {
		t.Fatal("放弃探测后应允许再次探测")
	}
	cb.RecordAttempt(retry, CircuitBreakerSucceeded)
	if cb.Tripped() || !cb.WouldAllow() || cb.failures != 0 {
		t.Fatal("当前探测成功后应解除熔断并清空失败计数")
	}
}
