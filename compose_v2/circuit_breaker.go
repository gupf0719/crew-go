package compose_v2

import (
	"sync"
	"time"
)

// 通用断路器（ISSUE-65）
//
// 上下文压缩 4 层中的 AutoCompact / ReactiveCompact 共用。压缩调用 LLM 失败
// 是常见情况（quota / 网络 / provider 抖动），连续失败说明短期内大概率继续
// 失败——直接跳过节省 latency 与 cost。
//
// 用法：
//
//	if !cb.Allow() {
//	    return /* 跳过 */
//	}
//	err := doExpensive()
//	if err != nil {
//	    cb.RecordFailure()
//	    return err
//	}
//	cb.RecordSuccess()

// CircuitBreaker 计数型断路器，带半开时间窗（自愈机制）。
//
// 状态机：
//   - closed: 正常允许，连续失败达 maxFailures → 进入 open
//   - open: tripped=true，Allow 返回 false，但 cooldownDuration 后允许探测
//   - half-open: cooldown 过后**只放行单次探测**（probing 标志，其他并发 Allow 返 false
//     防 thundering herd）；探测 success → closed，探测 fail → open 重置 cooldown。探测方
//     未回报（panic/忘记 Record*）超一个 cooldown 时 probing 过期允许新探测（防永久卡死）。
//
// 设计参考标准断路器模式。修复 ISSUE-65~68 P0 bug：旧实现 trip 后永远不能
// 再调 RecordSuccess（因为 Allow 永远 false 阻断到 LLM 调用），导致 AutoCompact
// 一次 quota burst 后永久禁用——长 session 不可接受。
//
// 线程安全：内部 mutex 保护——AutoCompact / ReactiveCompact 在 fan-out
// 分支内可能并发触发。
type CircuitBreaker struct {
	maxFailures      int
	cooldownDuration time.Duration
	mu               sync.Mutex
	failures         int
	tripped          bool
	trippedAt        time.Time
	// probing 是 half-open **单次探测在飞**标志——cooldown 过后只放行一个探测，其他
	// 并发 Allow 返 false 防 thundering herd（fan-out 多 branch 共享 per-session breaker
	// 时 cooldown 过后同时探测 compact LLM，N 次浪费）。探测回报（RecordSuccess/Failure）
	// 清此标志。
	probing bool
	// probeAt 是探测放行时刻——**防探测方 panic/忘记回报致 probing 永久卡死永久禁用**：
	// 探测超过一个 cooldownDuration 未回报则视为过期，允许新探测（自愈兜底）。
	probeAt time.Time
}

// 默认 cooldown：trip 后 5 分钟允许第一次探测。覆盖大部分 quota 反弹 / API
// 临时抖动场景；过短易反复探测浪费 quota，过长长时间不可用。
const defaultCircuitBreakerCooldown = 5 * time.Minute

// NewCircuitBreaker 构造断路器，maxFailures 必须 > 0；<= 0 使用默认值 3。
// cooldown = defaultCircuitBreakerCooldown。
func NewCircuitBreaker(maxFailures int) *CircuitBreaker {
	return NewCircuitBreakerWithCooldown(maxFailures, defaultCircuitBreakerCooldown)
}

// NewCircuitBreakerWithCooldown 显式指定 cooldown 时长——0 或负值表示禁用
// 自愈（永久 trip 直到 RecordSuccess 显式调用，等同旧行为）。
func NewCircuitBreakerWithCooldown(maxFailures int, cooldown time.Duration) *CircuitBreaker {
	if maxFailures <= 0 {
		maxFailures = 3
	}
	return &CircuitBreaker{maxFailures: maxFailures, cooldownDuration: cooldown}
}

// Allow 返回当前是否允许执行。
//
//   - 未 tripped：直接 true
//   - tripped 但 cooldown 已过（cooldownDuration > 0）：true（让调用方探测）
//   - tripped 且 cooldown 未过：false
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if !cb.wouldAllowLocked() {
		return false
	}
	if cb.tripped {
		// cooldown 过——**half-open 单次探测**：占下探测名额，其他并发 Allow 返 false 防 thundering herd。
		cb.probing = true
		cb.probeAt = time.Now()
	}
	return true
}

// WouldAllow 是 Allow 的只读版本：此刻调 Allow 会不会放行。不占半开的探测名额、不改任何状态，给只想预估
// 「下一次会不会请求它」的观测用（如 fallback 报起手候选的模型名）。判定跟 Allow 是同一个函数；结果到真正调用
// Allow 时可能已经变了（别的调用先占了探测名额、cooldown 刚好过了）。
func (cb *CircuitBreaker) WouldAllow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.wouldAllowLocked()
}

// wouldAllowLocked 是放行判定本身（调用方持锁）：Allow 与 WouldAllow 共用，免得两份条件漂开。
func (cb *CircuitBreaker) wouldAllowLocked() bool {
	if !cb.tripped {
		return true
	}
	if cb.cooldownDuration <= 0 {
		return false // 禁用自愈：永久 trip 直到显式 RecordSuccess
	}
	if time.Since(cb.trippedAt) < cb.cooldownDuration {
		return false // cooldown 未过
	}
	// cooldown 过：只放行一个探测。probing 卡死兜底：探测方未回报（panic/忘记 Record*）超过一个 cooldown 时
	// probing 视为过期，允许新探测（防永久禁用）。
	return !(cb.probing && time.Since(cb.probeAt) < cb.cooldownDuration)
}

// RecordFailure 记录一次失败；连续达到 maxFailures 触发 trip。已 tripped 时
// 再失败（探测失败路径）刷新 trippedAt，重置 cooldown 倒计时。返回 true 当且仅当
// 本次调用触发**首次熔断**（closed→open 边缘）——供调用方观测熔断事件（如 fallback
// emit member.tripped metric）；已 tripped 时探测失败返 false，不重复计边缘。
func (cb *CircuitBreaker) RecordFailure() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	cb.probing = false // 探测结束（失败）——下个 cooldown 后可再放行单次探测
	justTripped := false
	if cb.failures >= cb.maxFailures {
		if !cb.tripped {
			justTripped = true // closed→open 首次熔断边缘
		}
		cb.tripped = true
		cb.trippedAt = time.Now() // 探测失败也重置 cooldown 起点
	}
	return justTripped
}

// RecordSuccess 记录一次成功——重置失败计数 + 解除 trip。
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = 0
	cb.tripped = false
	cb.probing = false // 探测成功——解除熔断 + 清探测标志
}

// ClearProbe 清 half-open 探测在飞标志——**不改熔断状态/计数**。用于 half-open 探测的
// 调用方**既不算成功也不算失败**的场景：如 fallback 探测遇非切换错误（4xx 请求本身有
// 问题 / ctx 取消客户端断连）立即返回，此时 RecordSuccess（候选可能没响应）与 RecordFailure
// （非候选故障、误计入 trip）语义都不对。不清则 probing 泄漏 true，候选被 Allow 额外排除
// 到 probeAt+cooldown 才自愈。清后候选保持 tripped 但 probing 释放，下个请求 cooldown 过
// 即可重探（fallback probing 泄漏回归修复）。
func (cb *CircuitBreaker) ClearProbe() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.probing = false
}

// Tripped 返回断路器当前状态——仅观测用，不影响 Allow 决策。
func (cb *CircuitBreaker) Tripped() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.tripped
}
