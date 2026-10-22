package fallback

import (
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/model"
)

// 白盒：startIndex 的**筛选分档**与 tryMembers 的那份是两处拷贝（门关着时按 Tripped() 二分档、门开着时
// WouldAllow()/Allow()）。这次重构只消灭了**排序**那份（挑哪个 → 收进 nextMember）。CLAUDE.md 说
// 「必须与 X 保持一致」是设计债的自白，所以这里用一条机械对账把两份绑住。
//
// 用内部测试是因为「门关着 **且** 断路器熔断中」这个状态**黑盒搭不出来**：设门要 Retry-After 超过
// memberRetryAfterLimit（60 秒），而设门的 429 按设计**不记断路器失败**（门不是失败），于是没有哪条真实
// 调用序列能同时把两者推到那个组合上。第一版写成黑盒用例、拿 40ms/80ms 的 Retry-After 搭，两个条件一个
// 都没成立（门没关、也没熔断），测出来的东西与想验的无关——先怀疑 fixture 那条规约的实例。

func mkMember(name string) *fallbackMember {
	return &fallbackMember{
		model:   &namedOnly{name: name},
		breaker: compose_v2.NewCircuitBreakerWithCooldown(1, time.Hour),
		gate:    &retryGate{},
	}
}

// namedOnly 只报模型名——startIndex 不调模型，所以不需要能 Generate。
type namedOnly struct {
	model.ChatModel
	name string
}

func (n *namedOnly) ModelName() string { return n.name }

// TestStartIndex_SkipsGatedAndTripped：门关着的候选按 Tripped() 二分档——熔断中的**跳过**（探测名额在设门
// 那个调用手里），没熔断的**推迟**。
//
// ⚠️ 删掉那个二分档（门关着一律推迟）后，既有全部 fallback 用例照样全绿，只有这条红（审计实测）：
// 候选0 会被报成起手的，而 tryMembers 按熔断跳过它、真正请求的是候选1。
func TestStartIndex_SkipsGatedAndTripped(t *testing.T) {
	now := time.Now()
	m0, m1 := mkMember("gated-tripped"), mkMember("gated-open")
	// 候选0：门关到 now+40ms **且**断路器熔断中
	m0.gate.set(now.Add(40*time.Millisecond), nil)
	m0.breaker.RecordFailure() // maxFailures=1 ⟹ 立刻跳开
	if !m0.breaker.Tripped() {
		t.Fatal("fixture 没搭对：候选0 应处于熔断中")
	}
	// 候选1：门关到 now+80ms，断路器正常
	m1.gate.set(now.Add(80*time.Millisecond), nil)
	if m1.breaker.Tripped() {
		t.Fatal("fixture 没搭对：候选1 不该熔断")
	}

	f := &FallbackChatModel{members: []*fallbackMember{m0, m1}}
	if got := f.startIndex(now); got != 1 {
		t.Errorf("候选0 门关且熔断中应被跳过（不是推迟），起手请求的是候选1，startIndex=%d", got)
	}
	if got := f.ModelName(); got != "gated-open" {
		t.Errorf("ModelName 应报 gated-open，got %q", got)
	}
}

// TestStartIndex_TerminatesOnDegenerateClocks：终止性不依赖时钟语义——两次比较（closedUntil 与 nextMember）
// 的操作数完全相同，所以 until 落在 now 之前 / 恰好相等 / 之后，以及墙钟读数，都必须终止。
//
// ⚠️ 终止性的前提是**两处同钟**（见 startIndex godoc）。把 nextMember(states, now) 换成 time.Now() 会死循环，
// 而既有用例全绿——那个变异下本用例会挂住并被 t.Fatal 的看门狗报出来。
func TestStartIndex_TerminatesOnDegenerateClocks(t *testing.T) {
	base := time.Now()
	for _, c := range []struct {
		name  string
		until time.Time
	}{
		{"门早已过期", base.Add(-time.Hour)},
		{"门恰好等于 now", base},
		{"门差 1ns 到期", base.Add(time.Nanosecond)},
		{"门在远future", base.Add(time.Hour)},
		{"墙钟读数（去掉单调钟）", base.Round(0).Add(time.Hour)},
	} {
		t.Run(c.name, func(t *testing.T) {
			m0, m1 := mkMember("a"), mkMember("b")
			m0.gate.set(c.until, nil)
			m1.gate.set(c.until, nil)
			m1.breaker.RecordFailure() // 让两个候选分别落在二分档的两侧
			done := make(chan int, 1)
			go func() { done <- (&FallbackChatModel{members: []*fallbackMember{m0, m1}}).startIndex(base) }()
			select {
			case got := <-done:
				if got < 0 || got > 1 {
					t.Errorf("下标越界：%d", got)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("startIndex 没有终止（死循环）——检查 closedUntil 与 nextMember 是不是拿了同一个 now")
			}
		})
	}
}
