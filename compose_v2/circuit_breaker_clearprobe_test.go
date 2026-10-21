package compose_v2_test

import (
	"testing"
	"time"

	"crew-go/compose_v2"
)

// TestCircuitBreaker_ClearProbe 验证 ClearProbe 清 half-open 探测在飞标志但**不改熔断状态**——
// 用于探测既不算成功也不算失败（fallback 非切换错误：4xx / ctx 取消）时释放 probing，防
// probing 泄漏 true 致候选被 Allow 额外排除到 probeAt+cooldown 才自愈。
//
// sanity：把 ClearProbe 体改成 no-op（不清 probing）后，末尾 Allow 因 probing 仍在飞返 false → FAIL。
func TestCircuitBreaker_ClearProbe(t *testing.T) {
	cb := compose_v2.NewCircuitBreakerWithCooldown(1, 10*time.Millisecond)
	cb.RecordFailure() // maxFailures=1 → trip
	time.Sleep(20 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("cooldown 过应放行首次探测")
	}
	if cb.Allow() {
		t.Fatal("probing 在飞应拒绝并发探测（单次探测语义）")
	}
	// 探测非切换返回（既不 RecordSuccess 也不 RecordFailure）→ ClearProbe
	cb.ClearProbe()
	if !cb.Tripped() {
		t.Fatal("ClearProbe 不应改 tripped——候选仍熔断（只释放探测标志）")
	}
	if !cb.Allow() {
		t.Fatal("ClearProbe 后 probing 释放 + cooldown 仍过 → 应可立即重探（不必等 probeAt 兜底）")
	}

	// 对照：ClearProbe 不改计数——RecordSuccess 才真正解熔断
	cb.ClearProbe() // 清刚才 Allow 置的 probing
	if !cb.Tripped() {
		t.Fatal("多次 ClearProbe 仍不解熔断")
	}
	cb.RecordSuccess()
	if cb.Tripped() {
		t.Fatal("RecordSuccess 后应解熔断")
	}
}
