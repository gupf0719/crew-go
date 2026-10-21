package observability_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/fallback"
	"crew-go/hook"
	"crew-go/observability"
	"crew-go/schema"
)

// Phase 2 Sub-5a 守护测试 (含 audit follow-up 加强).
//
// 测覆盖:
//  1. Counter Inc / Add — atomic 累加 (验 final value, audit MED-6)
//  2. metricRegistry counter lazy create + 复用同 series (并发 sync barrier, audit HIGH-3)
//  3. nil Observability / Counter API nil-safe
//  4. Counter labels 字母序保 series key 稳定
//  5. Close 后 Counter() 返 nil (audit MED-5)

// TestPhase2Sub5a_NilSafe 验 nil Observability / Counter API nil-safe.
func TestPhase2Sub5a_NilSafe(t *testing.T) {
	var o *observability.Observability
	c := o.Counter("test", nil)
	if c != nil {
		t.Errorf("nil Observability.Counter() 应返 nil, got %v", c)
	}
	c.Inc()
	c.Add(10)
	c.Add(-5)
	if c.Value() != 0 {
		t.Errorf("nil Counter.Value() 应 0, got %d", c.Value())
	}
}

// TestPhase2Sub5a_CounterConcurrentInc 验 Counter 原子累加 + final value 准确
// (audit MED-6: race detector 抓不到 logic bug, 必须 assert final value).
func TestPhase2Sub5a_CounterConcurrentInc(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	c := obs.Counter("test.counter", map[string]string{"a": "1"})
	if c == nil {
		t.Fatal("Counter() 应返非 nil 实例")
	}

	const N = 100
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Inc()
		}()
	}
	wg.Wait()
	// audit MED-6 fix: 验 final value == 100 (sanity-revert: 把 Inc 改成
	// store(load()+1) 非原子操作 → race detector 抓不到但 value < 100)
	if got := c.Value(); got != N {
		t.Errorf("audit MED-6 regression: 100 并发 Inc 后 Value 应 = %d, got %d\n"+
			"sanity-revert: 把 c.value.Add(1) 改 c.value.Store(c.value.Load()+1) → 此测可能 fail", N, got)
	}
}

// TestPhase2Sub5a_CounterReuseInstance_Concurrent 验 metricRegistry double-check
// race 修补真实工作 (audit HIGH-3 fix: 之前单线程测试假绿, 删 double-check 仍 pass).
//
// sanity-revert: 删 metricRegistry.counter() 内 r.mu.Lock 后的 double-check
// (`if c, ok := r.counters[key]; ok { return c }`) → 并发 fast path miss + slow path
// 重创建实例 → 业务多处拿到不同 *Counter 计数分裂.
func TestPhase2Sub5a_CounterReuseInstance_Concurrent(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	// barrier — 让所有 goroutine 同时调 counter() 触发 RLock→Lock 转换 race window
	const N = 50
	var ready, start, done sync.WaitGroup
	ready.Add(N)
	start.Add(1)
	done.Add(N)
	results := make([]*observability.Counter, N)
	for i := 0; i < N; i++ {
		idx := i
		go func() {
			ready.Done()
			start.Wait() // 等 barrier
			results[idx] = obs.Counter("test.reuse_concurrent", map[string]string{"k": "v"})
			done.Done()
		}()
	}
	ready.Wait()
	start.Done() // 所有 goroutine 同时跑 counter()
	done.Wait()

	// 验全部返同一 *Counter (复用同实例)
	first := results[0]
	if first == nil {
		t.Fatal("Counter() 返 nil")
	}
	for i, c := range results {
		if c != first {
			t.Errorf("audit HIGH-3 regression: 并发 counter() 同 name+labels 应全返同实例, idx=%d 返不同实例\n"+
				"sanity-revert: 删 metricRegistry.counter() 内 r.mu.Lock 后 double-check map[key] → 此测 fail",
				i)
		}
	}
}

// TestPhase2Sub5a_CounterDifferentSeries 验不同 series 不同实例.
func TestPhase2Sub5a_CounterDifferentSeries(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	c1 := obs.Counter("test.diff", map[string]string{"k": "v"})
	c2 := obs.Counter("test.diff", map[string]string{"k": "different"})
	if c1 == c2 {
		t.Errorf("不同 labels 应不同 series 不同 Counter 实例")
	}
	c3 := obs.Counter("test.other_name", map[string]string{"k": "v"})
	if c3 == c1 {
		t.Errorf("不同 name 应不同 Counter 实例")
	}
}

// TestPhase2Sub5a_CounterLabelKeyStable 验 labels 不同遍历顺序产生同 series key.
func TestPhase2Sub5a_CounterLabelKeyStable(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	c1 := obs.Counter("test.label_stable", map[string]string{
		"a": "1", "b": "2", "c": "3",
	})
	c2 := obs.Counter("test.label_stable", map[string]string{
		"c": "3", "b": "2", "a": "1",
	})
	if c1 != c2 {
		t.Errorf("Phase 2 Sub-5a regression: 同 labels 不同字面顺序应复用 Counter (按 key 排序)\n" +
			"sanity-revert: metricKey 不 sort labels → map 遍历顺序不定 → 此测可能 fail")
	}
}

// TestPhase2Sub5a_NoMetricsEndpoint 验业务不配 MetricsEndpoint+TraceEndpoint 全空时
// Provider.Counter() 返 nil + 业务 nil-safe.
func TestPhase2Sub5a_NoMetricsEndpoint(t *testing.T) {
	obs, err := observability.New(observability.Config{
		Resource: observability.Resource{
			AppName: "test", EnvName: "test", IP: "1.2.3.4", SDKVersion: "test",
		},
	})
	if err != nil {
		t.Fatalf("New err: %v", err)
	}
	defer obs.Close()

	c := obs.Counter("test.no_endpoint", nil)
	if c != nil {
		t.Errorf("MetricsEndpoint+TraceEndpoint 都空时 Counter() 应返 nil, got %v", c)
	}
	c.Inc()
}

// TestPhase2Sub5a_AuditMED5_CounterAfterClose_ReturnsNil 验 audit MED-5 fix:
// Observability.Close() 后 Counter() 返 nil 跟 trace Enqueue 对称 — 避免业务
// 在 registry 内继续创建 Counter 永不 export 内存泄漏.
//
// sanity-revert: 删 observability.go::Counter() 内 closed.Load() 守 →
// Close 后仍创建 Counter 实例 → 此测 fail.
func TestPhase2Sub5a_AuditMED5_CounterAfterClose_ReturnsNil(t *testing.T) {
	obs := mustNewObs(t)
	// 先正常用一次
	c1 := obs.Counter("test.close", nil)
	if c1 == nil {
		t.Fatal("Close 前 Counter() 应返非 nil")
	}
	// Close
	_ = obs.Close()
	// Close 后 Counter() 应返 nil
	c2 := obs.Counter("test.close", nil)
	if c2 != nil {
		t.Errorf("audit MED-5 regression: Close 后 Counter() 应返 nil 防 registry 内存泄漏, got %v\n"+
			"sanity-revert: 删 observability.go::Counter() 内 closed.Load() 守 → 此测 fail", c2)
	}
}

// ─── Sub-5b: Histogram + Gauge tests ───

// TestPhase2Sub5b_HistogramBasic 验 Histogram Observe 落入正确 bucket + Count + Sum + 各 bucket counts.
// audit MED-1 fix: 之前只验 Count, 删 counts[idx]++ 测试仍 PASS (Count 独立维护) 假绿.
// 加 BucketCounts + Sum 真值断言守护 (sanity-revert 删 counts[idx]++ 此测 fail).
func TestPhase2Sub5b_HistogramBasic(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	h := obs.Histogram("test.hist",
		[]float64{0.1, 0.5, 1.0, 5.0},
		map[string]string{"x": "1"})
	if h == nil {
		t.Fatal("Histogram() 应返非 nil")
	}
	// 5 个 Observe 分布到不同 bucket
	h.Observe(0.05) // bucket 0 (<=0.1)
	h.Observe(0.3)  // bucket 1 (<=0.5)
	h.Observe(0.8)  // bucket 2 (<=1.0)
	h.Observe(3.0)  // bucket 3 (<=5.0)
	h.Observe(100)  // bucket 4 (+Inf)
	if got := h.Count(); got != 5 {
		t.Errorf("Count 应 = 5, got %d", got)
	}
	// audit MED-1 fix: 验各 bucket counts 真值
	wantCounts := []uint64{1, 1, 1, 1, 1}
	gotCounts := h.BucketCounts()
	for i, want := range wantCounts {
		if gotCounts[i] != want {
			t.Errorf("audit MED-1 regression: BucketCounts[%d] 应 = %d, got %d\n"+
				"sanity-revert: 删 Histogram.Observe 内 h.counts[idx]++ → 此测 fail (gotCounts 全 0)",
				i, want, gotCounts[i])
		}
	}
	// 验 sum 真累计 (audit HIGH-1: 之前 int64(sum) 截断, 修后 float64 保精度)
	wantSum := 0.05 + 0.3 + 0.8 + 3.0 + 100.0 // = 104.15
	if got := h.Sum(); got != wantSum {
		t.Errorf("Sum 应 = %.2f, got %.2f", wantSum, got)
	}
}

// TestPhase2Sub5b_AuditHIGH1_HistogramSumPrecision 验 audit HIGH-1 fix: 小耗时
// 场景 Sum 不被 int64 截断为 0.
//
// sanity-revert: Histogram.toOTLP 内 Sum: float64(sum) 改 int64(sum) → 业务 100 次
// 1ms call sum=0.1 截成 0 → avg = sum/count = 0 完全失真.
func TestPhase2Sub5b_AuditHIGH1_HistogramSumPrecision(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	h := obs.Histogram("test.small_dur",
		observability.BucketsSmallDuration,
		nil)
	// 100 次 1ms Observe — sum = 0.1 (秒)
	for i := 0; i < 100; i++ {
		h.Observe(0.001)
	}
	got := h.Sum()
	if got < 0.099 || got > 0.101 { // 允许 float 精度误差
		t.Errorf("audit HIGH-1 regression: 100 次 0.001 Observe sum 应 ≈ 0.1, got %.4f\n"+
			"sanity-revert: Histogram.toOTLP 用 int64(sum) → 此测 sum 截成 0 → 业务 avg=0 失真",
			got)
	}
}

// TestPhase2Sub5b_HistogramConcurrentObserve 验并发 Observe 安全 + BucketCounts
// 真值 (audit Sub-5 Round 2 视角 J MED fix: 之前只验 Count 总数, BucketCounts
// 分布 race 漏掉 — 如果 mu 错位某 bucket 漏 ++, Count 仍累加但 BucketCounts[i] < N).
func TestPhase2Sub5b_HistogramConcurrentObserve(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	h := obs.Histogram("test.hist_concurrent",
		[]float64{1, 10, 100},
		nil)
	const N = 100
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.Observe(5.0) // 全落 bucket 1 (5 <= 10)
		}()
	}
	wg.Wait()
	if got := h.Count(); got != N {
		t.Errorf("100 并发 Observe 后 Count 应 = %d, got %d", N, got)
	}
	// audit Sub-5 Round 2 视角 J MED fix: 验 BucketCounts[1]==N (5 全落 bucket 1).
	// 防 mu 错位 bucket 漏 ++ → Count 总数对但 BucketCounts 分布错.
	gotCounts := h.BucketCounts()
	if len(gotCounts) < 2 || gotCounts[1] != N {
		t.Errorf("audit Sub-5 视角 J MED regression: 100 Observe(5.0) BucketCounts[1] 应 = %d, got %v\n"+
			"sanity-revert: 删 Histogram.Observe 内 h.counts[idx]++ 改单线程 store/load → race 让某些 bucket 漏 ++ 此测 fail",
			N, gotCounts)
	}
	// 验 Sum 真累加 (100 * 5.0 = 500)
	if got := h.Sum(); got != 500 {
		t.Errorf("Sum 应 = 500 (100 × 5.0), got %.2f", got)
	}
}

// TestPhase2Sub5b_HistogramReuseInstance 同 series 复用.
func TestPhase2Sub5b_HistogramReuseInstance(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()
	h1 := obs.Histogram("test.reuse", []float64{1, 10}, map[string]string{"k": "v"})
	h2 := obs.Histogram("test.reuse", []float64{1, 10}, map[string]string{"k": "v"})
	if h1 != h2 {
		t.Errorf("同 name+labels Histogram 应复用实例")
	}
}

// TestPhase2Sub5b_GaugeBasic 验 Gauge Set/Inc/Dec.
func TestPhase2Sub5b_GaugeBasic(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	g := obs.Gauge("test.gauge", nil)
	if g == nil {
		t.Fatal("Gauge() 应返非 nil")
	}
	g.Set(10)
	if v := g.Value(); v != 10 {
		t.Errorf("Set(10) 后 Value 应 = 10, got %d", v)
	}
	g.Inc()
	g.Inc()
	g.Dec()
	if v := g.Value(); v != 11 {
		t.Errorf("Inc+Inc+Dec 后 Value 应 = 11, got %d", v)
	}
}

// TestPhase2Sub5b_GaugeNilSafe 验 nil Gauge nil-safe.
func TestPhase2Sub5b_GaugeNilSafe(t *testing.T) {
	var g *observability.Gauge
	g.Set(10) // nil-safe
	g.Inc()
	g.Dec()
	if v := g.Value(); v != 0 {
		t.Errorf("nil Gauge.Value() 应 0, got %d", v)
	}
}

// ─── Sub-5b audit MED-2: hooks.go 接入集成测 ───

// TestPhase2Sub5b_AuditMED2_HooksGaugePairing 验 hooks.go Sub-5b 接入:
// OnAgentStart → runs.inprogress Gauge.Inc + agent.run.duration startNanos 写入
// OnAgentEnd → Gauge.Dec + Histogram.Observe + Counter.Inc(status=success)
//
// audit MED-2: 之前 hooks.go::OnAgentStart/OnAgentEnd 内 Sub-5b 接入完全无测试覆盖,
// 加这测验整个接入路径真生效 (不只是 metric 原语层 PASS).
//
// sanity-revert: 删 hooks.go::OnAgentStart 内 `g.Inc()` → Gauge.Value 永远 0 → 此测 fail.
// sanity-revert: 删 hooks.go::OnAgentEnd 内 `hg.Observe(durationSec)` → Histogram.Count 0 → 此测 fail.
// sanity-revert: 删 hooks.go::OnAgentEnd 内 `c.Inc()` → Counter.Value 0 → 此测 fail.
func TestPhase2Sub5b_AuditMED2_HooksGaugePairing(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	hooks := obs.AgentHooks()
	ctx := context.Background()
	info := &hook.AgentInfo{Name: "test_agent", Type: "base", Round: 0}

	// 调 OnAgentStart — gauge 应 ++ 到 1
	ctx, err := hooks.OnAgentStart(ctx, info, &hook.AgentInput{})
	if err != nil {
		t.Fatalf("OnAgentStart err: %v", err)
	}
	g := obs.Gauge(observability.MetricRunsInProgress, nil)
	if g == nil {
		t.Fatal("MetricRunsInProgress Gauge 应已创建")
	}
	if v := g.Value(); v != 1 {
		t.Errorf("OnAgentStart 后 runs.inprogress 应 = 1, got %d\n"+
			"sanity-revert: 删 hooks.go::OnAgentStart 内 g.Inc() → 此测 fail", v)
	}

	// 调 OnAgentEnd — gauge -- 到 0 + Histogram.Observe + Counter.Inc
	_ = hooks.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	if v := g.Value(); v != 0 {
		t.Errorf("OnAgentEnd 后 runs.inprogress 应 = 0 (跟 Init 配对), got %d\n"+
			"sanity-revert: 删 hooks.go::OnAgentEnd 内 g.Dec() → 此测 fail", v)
	}

	// 验 Histogram.Count == 1 (单次 Run duration 真观测)
	hg := obs.Histogram(observability.MetricAgentRunDuration, observability.BucketsLLMDuration,
		map[string]string{"gen_ai_agent_name": "test_agent", "gen_ai_agent_kind": "base"})
	if hg == nil {
		t.Fatal("MetricAgentRunDuration Histogram 应已创建")
	}
	if got := hg.Count(); got != 1 {
		t.Errorf("OnAgentEnd 后 agent.run.duration Count 应 = 1, got %d\n"+
			"sanity-revert: 删 hooks.go::OnAgentEnd 内 hg.Observe(durationSec) → 此测 fail (Count 0)", got)
	}
	// 验 Sum > 0 (duration 真值, 不被 int64 截断成 0)
	if got := hg.Sum(); got <= 0 {
		t.Errorf("agent.run.duration Sum 应 > 0, got %.6f\n"+
			"audit HIGH-1 regression: int64(sum) 截断让 sub-ms duration 全成 0", got)
	}

	// 验 Counter status=success
	c := obs.Counter(observability.MetricAgentRunTotal, map[string]string{"gen_ai_agent_name": "test_agent", "gen_ai_agent_kind": "base", "gen_ai_status": "success"})
	if c == nil {
		t.Fatal("MetricAgentRunTotal Counter 应已创建")
	}
	if got := c.Value(); got != 1 {
		t.Errorf("OnAgentEnd 后 agent.run.total{status=success} Counter 应 = 1, got %d\n"+
			"sanity-revert: 删 hooks.go::OnAgentEnd 内 c.Inc() → 此测 fail", got)
	}
}

// TestPhase2Sub5b_AuditMED2_HooksStatusLabel 验 OnAgentEnd status 真按 ctx.Err 区分.
// audit Sub-5a HIGH-1 fix 后 status 不再 hardcoded "success" — DeadlineExceeded→"timeout",
// Canceled→"cancelled". 验各 status label series 独立计数.
func TestPhase2Sub5b_AuditMED2_HooksStatusLabel(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	hooks := obs.AgentHooks()
	info := &hook.AgentInfo{Name: "test_status", Type: "base", Round: 0}

	// success 路径
	ctx := context.Background()
	ctx, _ = hooks.OnAgentStart(ctx, info, &hook.AgentInput{})
	_ = hooks.OnAgentEnd(ctx, info, &hook.AgentOutput{})

	// cancelled 路径
	ctxC, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消
	ctxC, _ = hooks.OnAgentStart(ctxC, info, &hook.AgentInput{})
	_ = hooks.OnAgentEnd(ctxC, info, &hook.AgentOutput{})

	cs := obs.Counter(observability.MetricAgentRunTotal, map[string]string{"gen_ai_agent_name": "test_status", "gen_ai_agent_kind": "base", "gen_ai_status": "success"})
	cc := obs.Counter(observability.MetricAgentRunTotal, map[string]string{"gen_ai_agent_name": "test_status", "gen_ai_agent_kind": "base", "gen_ai_status": "cancelled"})
	if cs == nil || cc == nil {
		t.Fatal("Counter 应已创建")
	}
	if cs.Value() != 1 {
		t.Errorf("status=success counter 应 = 1, got %d", cs.Value())
	}
	if cc.Value() != 1 {
		t.Errorf("audit Sub-5a HIGH-1 regression: status=cancelled counter 应 = 1 (ctx.Err()=Canceled), got %d\n"+
			"sanity-revert: 把 hooks.go::OnAgentEnd 内 status 改回 hardcoded \"success\" → 此测 fail", cc.Value())
	}
	// Gauge 应平 (2 ++ + 2 --)
	g := obs.Gauge(observability.MetricRunsInProgress, nil)
	if g.Value() != 0 {
		t.Errorf("2 Init + 2 End 后 Gauge 应平 = 0, got %d (init/end Inc/Dec 不配对)", g.Value())
	}
}

// ─── Sub-5c: hooks.go LLM/Tool/Agent/Node metric 接入集成测 ───

// TestPhase2Sub5c_LLMCallDurationAndTokens 验 OnModelStart→OnModelEnd 路径:
//   - gen_ai.llm.call.duration Histogram.Count == 1
//   - gen_ai.llm.tokens.total{kind=input/output/think/total} 各 series 真累加
//
// sanity-revert: 删 OnModelStart 内 entry.runStartNanos 设值 → duration 算 0 跳过.
// sanity-revert: 删 OnModelEnd 内 emitToken("input", ...) → input series Counter=0.
func TestPhase2Sub5c_LLMCallDurationAndTokens(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	hooks := obs.AgentHooks()
	ctx := context.Background()
	info := &hook.AgentInfo{Name: "test_llm", Type: "base", Round: 0}

	// 先 fire OnAgentStart 建 agent span (model 节点是 agent 子节点, findParentSpanID 需要)
	ctx, _ = hooks.OnAgentStart(ctx, info, &hook.AgentInput{})
	defer func() { _ = hooks.OnAgentEnd(ctx, info, &hook.AgentOutput{}) }()

	// OnModelStart 设 startNanos
	ctx, _ = hooks.OnModelStart(ctx, info, &schema.ModelInput{Messages: []*schema.Message{{Role: schema.RoleUser, Content: "hi"}}})
	// 模拟 LLM 耗时
	time.Sleep(2 * time.Millisecond)
	// OnModelEnd 算 duration + fire tokens
	_, _ = hooks.OnModelEnd(ctx, info, &schema.ModelOutput{
		Message: &schema.Message{Role: schema.RoleAssistant, Content: "world"},
		Usage:   &schema.MessageUsage{InputTokens: 10, OutputTokens: 20, ThinkTokens: 5, TotalTokens: 30},
	})

	hg := obs.Histogram(observability.MetricLLMCallDuration, observability.BucketsLLMDuration,
		map[string]string{
			"gen_ai_agent_name": "test_llm",
			"gen_ai_model":      "",
		})
	if hg == nil {
		t.Fatal("MetricLLMCallDuration Histogram 应已创建")
	}
	if got := hg.Count(); got != 1 {
		t.Errorf("OnModelEnd 后 llm.call.duration Count 应 = 1, got %d\n"+
			"sanity-revert: 删 hooks.go::OnModelStart 内 entry.runStartNanos 设值 → 此测 fail", got)
	}
	if got := hg.Sum(); got <= 0 {
		t.Errorf("llm.call.duration Sum 应 > 0 (真观测到 LLM 耗时), got %.6f", got)
	}

	// 验 token Counter 4 series (Round 5 Q2 fix: gen_ai.usage.type label OTel 对齐)
	cases := map[string]int64{"input": 10, "output": 20, "think": 5, "total": 30}
	for tokenType, want := range cases {
		c := obs.Counter(observability.MetricLLMTokensTotal, map[string]string{
			"gen_ai_agent_name": "test_llm",
			"gen_ai_model":      "",
			"gen_ai_token_type": tokenType,
		})
		if c == nil {
			t.Errorf("MetricLLMTokensTotal{type=%s} Counter 应已创建", tokenType)
			continue
		}
		if got := c.Value(); got != want {
			t.Errorf("llm.tokens.total{type=%s} 应 = %d, got %d\n"+
				"sanity-revert: 删 hooks.go::OnModelEnd 内 emitToken(%q, ...) → 此测 fail",
				tokenType, want, got, tokenType)
		}
	}
}

// TestPhase2Sub5c_ToolCallDurationAndError 验 OnToolStart→OnToolEnd:
//   - 成功路径: gen_ai.tool.call.duration Count=1, gen_ai.tool.error.total{tool} Counter=0
//   - 失败路径: error Counter 真+1
func TestPhase2Sub5c_ToolCallDurationAndError(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	hooks := obs.AgentHooks()
	ctx := context.Background()
	info := &hook.AgentInfo{Name: "test_tool_agent", Type: "base", Round: 0}

	ctx, _ = hooks.OnAgentStart(ctx, info, &hook.AgentInput{})
	defer func() { _ = hooks.OnAgentEnd(ctx, info, &hook.AgentOutput{}) }()

	// 成功 tool
	ctx, _ = hooks.OnToolStart(ctx, info, &schema.ToolInput{
		Name: "echo_tool", CallId: "call_ok",
		ArgumentsJSON: `{"q":"hi"}`,
	})
	time.Sleep(1 * time.Millisecond)
	_, _ = hooks.OnToolEnd(ctx, info, &schema.ToolOutput{
		CallId: "call_ok",
		Result: &schema.ToolCallResult{Success: true, Output: "ok"},
	})

	// 失败 tool
	ctx, _ = hooks.OnToolStart(ctx, info, &schema.ToolInput{
		Name: "fail_tool", CallId: "call_err",
	})
	_, _ = hooks.OnToolEnd(ctx, info, &schema.ToolOutput{
		CallId: "call_err",
		Result: &schema.ToolCallResult{Success: false, Error: "boom"},
	})

	hg := obs.Histogram(observability.MetricToolCallDuration, observability.BucketsSmallDuration,
		map[string]string{"gen_ai_agent_name": "test_tool_agent", "gen_ai_tool_name": "echo_tool"})
	if hg == nil || hg.Count() != 1 {
		t.Errorf("MetricToolCallDuration{tool=echo_tool} Count 应 = 1, got %v", hg)
	}
	// 验失败 tool 的 error counter
	cErr := obs.Counter(observability.MetricToolErrorTotal, map[string]string{"gen_ai_agent_name": "test_tool_agent", "gen_ai_tool_name": "fail_tool"})
	if cErr == nil || cErr.Value() != 1 {
		t.Errorf("MetricToolErrorTotal{tool=fail_tool} Counter 应 = 1, got %v\n"+
			"sanity-revert: 删 hooks.go::OnToolEnd 内 status==error 时 c.Inc() → 此测 fail", cErr)
	}
	// 验成功 tool 的 error counter 不存在 / 为 0
	cOK := obs.Counter(observability.MetricToolErrorTotal, map[string]string{"gen_ai_agent_name": "test_tool_agent", "gen_ai_tool_name": "echo_tool"})
	if cOK != nil && cOK.Value() != 0 {
		t.Errorf("成功 tool 不应触发 error counter, got %d", cOK.Value())
	}
}

// TestPhase2Sub5c_AgentRounds 验 OnAgentEnd 内 info.Round 真 Observe 到 Histogram.
func TestPhase2Sub5c_AgentRounds(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	hooks := obs.AgentHooks()
	ctx := context.Background()
	info := &hook.AgentInfo{Name: "test_rounds", Type: "base", Round: 5}

	ctx, _ = hooks.OnAgentStart(ctx, info, &hook.AgentInput{})
	_ = hooks.OnAgentEnd(ctx, info, &hook.AgentOutput{})

	hg := obs.Histogram(observability.MetricAgentRounds, observability.BucketsCount,
		map[string]string{"gen_ai_agent_name": "test_rounds", "gen_ai_agent_kind": "base"})
	if hg == nil {
		t.Fatal("MetricAgentRounds Histogram 应已创建")
	}
	if got := hg.Count(); got != 1 {
		t.Errorf("Count 应 = 1, got %d", got)
	}
	if got := hg.Sum(); got != 5 {
		t.Errorf("Sum 应 = 5 (info.Round), got %.2f\n"+
			"sanity-revert: 删 hooks.go::OnAgentEnd 内 hg.Observe(float64(info.Round)) → 此测 fail", got)
	}
}

// TestPhase2Sub5c_NodeExecutionTotal 验 OnNodeStart 内 NodeExecutionTotal Counter.Inc.
func TestPhase2Sub5c_NodeExecutionTotal(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	orch := obs.OrchHooks()
	ctx := context.Background()
	// 先开 graph (OnNodeStart 内 findParentSpanID 需要)
	ctx, _ = orch.OnGraphStart(ctx)
	defer func() { _ = orch.OnGraphEnd(ctx, nil) }()

	// 3 次 OnNodeStart (不同 nodeID)
	for i := 0; i < 3; i++ {
		_, _ = orch.OnNodeStart(ctx, "test_node_a")
		_ = orch.OnNodeEnd(ctx, "test_node_a", nil)
	}
	_, _ = orch.OnNodeStart(ctx, "test_node_b")
	_ = orch.OnNodeEnd(ctx, "test_node_b", nil)

	cA := obs.Counter(observability.MetricNodeExecutionTotal, map[string]string{"crew_node_id": "test_node_a"})
	cB := obs.Counter(observability.MetricNodeExecutionTotal, map[string]string{"crew_node_id": "test_node_b"})
	if cA == nil || cA.Value() != 3 {
		t.Errorf("MetricNodeExecutionTotal{node=test_node_a} 应 = 3, got %v\n"+
			"sanity-revert: 删 hooks.go::OnNodeStart 内 c.Inc() → 此测 fail", cA)
	}
	if cB == nil || cB.Value() != 1 {
		t.Errorf("MetricNodeExecutionTotal{node=test_node_b} 应 = 1, got %v", cB)
	}
}

// ─── Sub-5d: Compact metric 接入测 (OrchHooks.OnNodeEvent 路径) ───

// TestPhase2Sub5d_CompactMetric 验 OnNodeEvent 内识别 CompactionEventType (After) →
// fire CompactTotal + CompactDuration + CompactRatio metric (OTel self-contained 设计:
// After event 自带 InputTokens / OutputTokens / DurationMs, observability 端单 event 算完).
//
// 业务接入路径: agent_v2.With{Micro,Auto,Reactive}Compact 开启 + 接 observability →
// EmitCompactionEvent 走 EventSink → OrchHooks.OnNodeEvent → fireCompactionMetric.
func TestPhase2Sub5d_CompactMetric(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	orch := obs.OrchHooks()
	ctx := context.Background()
	ctx, _ = orch.OnGraphStart(ctx)
	defer func() { _ = orch.OnGraphEnd(ctx, nil) }()

	// After event 自带完整数据 (caller scope 测好 DurationMs/InputTokens/OutputTokens 后 emit)
	afterAction := &agent_v2.CompactionAction{
		Layer: agent_v2.CompactionLayerAuto,
		Type:  agent_v2.CompactionActionAfter,
		After: &agent_v2.CompactionAfterAction{
			InputTokens:  1000,
			OutputTokens: 300,
			DurationMs:   25, // 25ms
			Dropped:      5,
			Kept:         3,
		},
	}
	_ = orch.OnNodeEvent(ctx, "model", agent_v2.Event{
		Type:     agent_v2.CompactionEventType,
		Content:  "auto:after",
		Metadata: map[string]any{"action": afterAction},
	})

	// 验 CompactTotal Counter +1
	c := obs.Counter(observability.MetricCompactTotal, map[string]string{
		"crew_compact_layer": "auto", "crew_outcome": "true",
	})
	if c == nil || c.Value() != 1 {
		t.Errorf("MetricCompactTotal{layer=auto, ok=true} 应 = 1, got %v\n"+
			"sanity-revert: 删 hooks.go::fireCompactionMetric After 路径 c.Inc() → 此测 fail", c)
	}

	// 验 CompactDuration Histogram Count=1 + Sum=0.025s (25ms)
	hg := obs.Histogram(observability.MetricCompactDuration, observability.BucketsSmallDuration,
		map[string]string{"crew_compact_layer": "auto"})
	if hg == nil || hg.Count() != 1 {
		t.Errorf("MetricCompactDuration{layer=auto} Count 应 = 1, got %v", hg)
	}
	if hg != nil && (hg.Sum() < 0.024 || hg.Sum() > 0.026) {
		t.Errorf("MetricCompactDuration Sum 应 ≈ 0.025s (25ms), got %.6f\n"+
			"sanity-revert: hooks.go::fireCompactionMetric 删 hg.Observe(after.DurationMs/1000) → 此测 fail",
			hg.Sum())
	}

	// 验 CompactRatio Histogram (OutputTokens/InputTokens = 300/1000 = 0.3)
	hgRatio := obs.Histogram(observability.MetricCompactRatio,
		[]float64{0.1, 0.3, 0.5, 0.7, 0.9, 0.95, 1.0},
		map[string]string{"crew_compact_layer": "auto"})
	if hgRatio == nil || hgRatio.Count() != 1 {
		t.Errorf("MetricCompactRatio Count 应 = 1, got %v", hgRatio)
	}
	if hgRatio != nil && (hgRatio.Sum() < 0.29 || hgRatio.Sum() > 0.31) {
		t.Errorf("MetricCompactRatio Sum 应 ≈ 0.3 (300/1000), got %.4f\n"+
			"sanity-revert: 删 fireCompactionMetric ratio 路径 → 此测 fail", hgRatio.Sum())
	}
}

// TestFailoverMetric 验 fallback failover event 经 OnNodeEvent → fireFailoverMetric →
// Counter +1（业务接 observability 后 fallback 切换自动采集，跟压缩同款 sink-gated 消费路径）。
func TestFailoverMetric(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	orch := obs.OrchHooks()
	ctx := context.Background()
	ctx, _ = orch.OnGraphStart(ctx)
	defer func() { _ = orch.OnGraphEnd(ctx, nil) }()

	// fallback 切换时 emit 的 failover event（候选 0 → 1，429 分类成 rate_limit）
	_ = orch.OnNodeEvent(ctx, "model", agent_v2.Event{
		Type:    fallback.FailoverEventType,
		Content: "0->1",
		Metadata: map[string]any{"action": &fallback.FailoverAction{
			FromIdx: 0, ToIdx: 1, Reason: "rate_limit",
		}},
	})

	c := obs.Counter(observability.MetricFailoverTotal, map[string]string{
		"crew_failover_reason": "rate_limit",
		"crew_failover_from":   "0",
	})
	if c == nil || c.Value() != 1 {
		t.Errorf("MetricFailoverTotal{reason=rate_limit, from=0} 应 = 1, got %v\n"+
			"sanity-revert: 删 hooks.go::fireFailoverMetric c.Inc() 或 routeEventToMetric 的 case → 此测 fail", c)
	}
}

// TestDisabledMetrics 验 Config.DisabledMetrics 黑名单（per-category 观测关闭降本）——命中的
// metric 在 Counter/Histogram/Gauge 入口返 nil（业务 Inc/Observe/Set 自动 silent skip），
// 非黑名单不受影响；黑名单精确到 name（禁 compact_total 不影响 compact_duration）。
func TestDisabledMetrics(t *testing.T) {
	obs, err := observability.New(observability.Config{
		TraceEndpoint:   "127.0.0.1:0",
		MetricsEndpoint: "127.0.0.1:0",
		Resource: observability.Resource{
			AppName: "test", EnvName: "test", IP: "1.2.3.4", SDKVersion: "test",
		},
		// 黑名单含 Counter / Histogram / Gauge 三型，覆盖三入口 disabled 路径（sanity-revert
		// 删任一入口的 check 都有对应断言 fail，不只 counter）
		DisabledMetrics: []string{
			observability.MetricFailoverTotal,   // Counter 型
			observability.MetricCompactDuration, // Histogram 型
			observability.MetricActiveSessions,  // Gauge 型
		},
	})
	if err != nil {
		t.Fatalf("New err: %v", err)
	}
	defer obs.Close()

	// 黑名单命中 → 三入口都返 nil（业务 Inc/Observe/Set silent skip）
	if c := obs.Counter(observability.MetricFailoverTotal, map[string]string{"x": "1"}); c != nil {
		t.Errorf("黑名单 Counter %s 应返 nil，got %v\n"+
			"sanity-revert: 删 metric.go::counter 入口 `if r.disabled[name]` → 此断言 fail", observability.MetricFailoverTotal, c)
	}
	if hg := obs.Histogram(observability.MetricCompactDuration, []float64{1}, map[string]string{"x": "1"}); hg != nil {
		t.Errorf("黑名单 Histogram %s 应返 nil，got %v\n"+
			"sanity-revert: 删 metric.go::histogram 入口 check → 此断言 fail", observability.MetricCompactDuration, hg)
	}
	if g := obs.Gauge(observability.MetricActiveSessions, map[string]string{"x": "1"}); g != nil {
		t.Errorf("黑名单 Gauge %s 应返 nil，got %v\n"+
			"sanity-revert: 删 metric.go::gauge 入口 check → 此断言 fail", observability.MetricActiveSessions, g)
	}
	// 非黑名单 → 三入口都正常返非 nil（黑名单精确到 name，不是整类：禁了 compact_duration
	// 不碰 compact_total / agent_run_duration；禁了 active_sessions 不碰 runs_inprogress）
	if c := obs.Counter(observability.MetricCompactTotal, map[string]string{"x": "1"}); c == nil {
		t.Errorf("非黑名单 Counter %s 应正常返非 nil", observability.MetricCompactTotal)
	}
	if hg := obs.Histogram(observability.MetricAgentRunDuration, []float64{1}, map[string]string{"x": "1"}); hg == nil {
		t.Errorf("非黑名单 Histogram %s 应正常返非 nil", observability.MetricAgentRunDuration)
	}
	if g := obs.Gauge(observability.MetricRunsInProgress, map[string]string{"x": "1"}); g == nil {
		t.Errorf("非黑名单 Gauge %s 应正常返非 nil", observability.MetricRunsInProgress)
	}
}

// TestPhase2Sub5d_BeforeEventNoMetric 验 Before event 不 fire metric (只 trace).
// audit fix 后协议: After event 自带完整数据, Before 仅做 trace event 用 (业务订阅
// 看进度, 不算 metric).
func TestPhase2Sub5d_BeforeEventNoMetric(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	orch := obs.OrchHooks()
	ctx := context.Background()
	ctx, _ = orch.OnGraphStart(ctx)
	defer func() { _ = orch.OnGraphEnd(ctx, nil) }()

	_ = orch.OnNodeEvent(ctx, "model", agent_v2.Event{
		Type: agent_v2.CompactionEventType,
		Metadata: map[string]any{
			"action": &agent_v2.CompactionAction{
				Layer: agent_v2.CompactionLayerAuto,
				Type:  agent_v2.CompactionActionBefore,
				Before: &agent_v2.CompactionBeforeAction{
					InputTokens: 1000,
				},
			},
		},
	})

	c := obs.Counter(observability.MetricCompactTotal, map[string]string{
		"crew_compact_layer": "auto", "crew_outcome": "true",
	})
	if c != nil && c.Value() != 0 {
		t.Errorf("Before event 不应 fire CompactTotal Counter, got %d\n"+
			"audit fix 协议: fire 仅在 After event (self-contained), Before 只 trace", c.Value())
	}
}

// TestPhase2Sub5d_CompactRatio_WithInputTokens 验 InputTokens>0 路径 ratio 真 Observe
// (配对 MicroLayerNoRatio 验过滤路径 — audit Sub-5 MED-1 fix).
//
// sanity-revert: 删 hooks.go::fireCompactionMetric 内 `if after.InputTokens > 0` 后
// ratio Observe 块 → 此测 Count=0 fail.
func TestPhase2Sub5d_CompactRatio_WithInputTokens(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	orch := obs.OrchHooks()
	ctx := context.Background()
	ctx, _ = orch.OnGraphStart(ctx)
	defer func() { _ = orch.OnGraphEnd(ctx, nil) }()

	_ = orch.OnNodeEvent(ctx, "model", agent_v2.Event{
		Type: agent_v2.CompactionEventType,
		Metadata: map[string]any{
			"action": &agent_v2.CompactionAction{
				Layer: agent_v2.CompactionLayerAuto,
				Type:  agent_v2.CompactionActionAfter,
				After: &agent_v2.CompactionAfterAction{
					InputTokens:  500,
					OutputTokens: 100, // ratio = 100/500 = 0.2
					DurationMs:   10,
				},
			},
		},
	})

	hgRatio := obs.Histogram(observability.MetricCompactRatio,
		[]float64{0.1, 0.3, 0.5, 0.7, 0.9, 0.95, 1.0},
		map[string]string{"crew_compact_layer": "auto"})
	if hgRatio == nil || hgRatio.Count() != 1 {
		t.Errorf("audit Sub-5 MED-1 regression: InputTokens>0 时 CompactRatio Histogram 应 Count=1, got %v\n"+
			"sanity-revert: 删 fireCompactionMetric `if after.InputTokens > 0` 后 ratio 块 → 此测 fail",
			hgRatio)
	}
	if hgRatio != nil && (hgRatio.Sum() < 0.19 || hgRatio.Sum() > 0.21) {
		t.Errorf("Sum 应 ≈ 0.2 (100/500), got %.4f", hgRatio.Sum())
	}
}

// TestPhase2Sub5d_MicroLayerNoRatio 验 InputTokens=0 时 (MicroCompact 不计 token)
// CompactRatio Histogram 不 fire (避免除零). 配对 CompactRatio_WithInputTokens 验
// fire 路径.
func TestPhase2Sub5d_MicroLayerNoRatio(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	orch := obs.OrchHooks()
	ctx := context.Background()
	ctx, _ = orch.OnGraphStart(ctx)
	defer func() { _ = orch.OnGraphEnd(ctx, nil) }()

	_ = orch.OnNodeEvent(ctx, "model", agent_v2.Event{
		Type: agent_v2.CompactionEventType,
		Metadata: map[string]any{
			"action": &agent_v2.CompactionAction{
				Layer: agent_v2.CompactionLayerMicro,
				Type:  agent_v2.CompactionActionAfter,
				After: &agent_v2.CompactionAfterAction{
					InputTokens: 0, // Micro 不计 token
					DurationMs:  5,
					Dropped:     2,
					Kept:        10,
				},
			},
		},
	})

	// CompactRatio Histogram 不应 fire (InputTokens=0 跳过除零)
	hgRatio := obs.Histogram(observability.MetricCompactRatio,
		[]float64{0.1, 0.3, 0.5, 0.7, 0.9, 0.95, 1.0},
		map[string]string{"crew_compact_layer": "micro"})
	if hgRatio != nil && hgRatio.Count() != 0 {
		t.Errorf("MicroCompact InputTokens=0 应跳过 CompactRatio fire, got Count=%d", hgRatio.Count())
	}
	// 但 CompactTotal + CompactDuration 应正常 fire
	c := obs.Counter(observability.MetricCompactTotal, map[string]string{
		"crew_compact_layer": "micro", "crew_outcome": "true",
	})
	if c == nil || c.Value() != 1 {
		t.Errorf("MicroLayer CompactTotal 应 = 1, got %v", c)
	}
}

// TestPhase2Sub5d_CompactMetric_Failed 验 After.Err 非空时 ok=false counter +1,
// 同时验 ok=true counter 不增 (audit Sub-5 Round 2 视角 J MED fix: 加 ok=true 对照
// 防"删 ok=false 路径仍 PASS"假绿).
func TestPhase2Sub5d_CompactMetric_Failed(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	orch := obs.OrchHooks()
	ctx := context.Background()
	ctx, _ = orch.OnGraphStart(ctx)
	defer func() { _ = orch.OnGraphEnd(ctx, nil) }()

	// 用业务真实 error string (跟 autocompact.go::errBreakerTripped 一致), 防 "测试
	// fixture 用简化 string 不匹配真业务路径" 类问题 — fireCompactionMetric 只判 Err!=""
	// 非 regex, 但用真 string 更严谨.
	_ = orch.OnNodeEvent(ctx, "model", agent_v2.Event{
		Type: agent_v2.CompactionEventType,
		Metadata: map[string]any{
			"action": &agent_v2.CompactionAction{
				Layer: agent_v2.CompactionLayerReactive,
				Type:  agent_v2.CompactionActionAfter,
				After: &agent_v2.CompactionAfterAction{
					DurationMs: 5,
					Err:        "auto compact: breaker tripped, skipping",
				},
			},
		},
	})

	cFalse := obs.Counter(observability.MetricCompactTotal, map[string]string{
		"crew_compact_layer": "reactive", "crew_outcome": "false",
	})
	if cFalse == nil || cFalse.Value() != 1 {
		t.Errorf("After.Err 非空时 ok=false Counter 应 = 1, got %v", cFalse)
	}
	// audit Sub-5 Round 2 视角 J MED fix: 验 ok=true 不增 (防 fire 路径错走 ok=true 假绿)
	cTrue := obs.Counter(observability.MetricCompactTotal, map[string]string{
		"crew_compact_layer": "reactive", "crew_outcome": "true",
	})
	if cTrue != nil && cTrue.Value() != 0 {
		t.Errorf("After.Err 非空时 ok=true Counter 不应增, got %d\n"+
			"sanity-revert: 改 fireCompactionMetric 内 okStr=\"true\" 永远不走 false 分支 → 此测 fail",
			cTrue.Value())
	}
}

// ─── Sub-5e: Cp metric 接入测 (OrchHooks.OnNodeEvent 路径) ───

// TestPhase2Sub5e_CpSaveMetric 验 fire CpAction(Save) → CpSaveTotal{op=save} + CpSizeBytes Histogram.
func TestPhase2Sub5e_CpSaveMetric(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	orch := obs.OrchHooks()
	ctx := context.Background()
	ctx, _ = orch.OnGraphStart(ctx)
	defer func() { _ = orch.OnGraphEnd(ctx, nil) }()

	_ = orch.OnNodeEvent(ctx, "", agent_v2.Event{
		Type: compose_v2.CpEventType,
		Metadata: map[string]any{
			"action": &compose_v2.CpAction{
				Type: compose_v2.CpOperationSave,
				Save: &compose_v2.CpSaveAction{
					SessionID:     "sid1",
					NS:            "",
					ChannelsCount: 10, // 估 10 * 1KB = 10KB
					DurationMs:    5,
					Ok:            true,
				},
			},
		},
	})

	c := obs.Counter(observability.MetricCpSaveTotal, map[string]string{
		"crew_cp_op": "save", "crew_outcome": "true",
	})
	if c == nil || c.Value() != 1 {
		t.Errorf("MetricCpSaveTotal{op=save, ok=true} 应 = 1, got %v\n"+
			"sanity-revert: 删 hooks.go::fireCpMetric c.Inc() → 此测 fail", c)
	}

	// CpSizeBytes Histogram (估 10240 bytes = 10KB, 落 BucketsSize bucket 1024..10240)
	hg := obs.Histogram(observability.MetricCpSizeBytes, observability.BucketsSize, nil)
	if hg == nil || hg.Count() != 1 {
		t.Errorf("MetricCpSizeBytes Count 应 = 1, got %v", hg)
	}
	if hg != nil && hg.Sum() != 10240 {
		t.Errorf("CpSizeBytes Sum 应 = 10240 (10 channels * 1KB), got %.2f", hg.Sum())
	}
}

// TestPhase2Sub5e_CpLoadCleanupMetric 验 Load/Cleanup 路径只 fire SaveTotal counter (不 fire size).
func TestPhase2Sub5e_CpLoadCleanupMetric(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	orch := obs.OrchHooks()
	ctx := context.Background()
	ctx, _ = orch.OnGraphStart(ctx)
	defer func() { _ = orch.OnGraphEnd(ctx, nil) }()

	// load 成功
	_ = orch.OnNodeEvent(ctx, "", agent_v2.Event{
		Type: compose_v2.CpEventType,
		Metadata: map[string]any{
			"action": &compose_v2.CpAction{
				Type: compose_v2.CpOperationLoad,
				Load: &compose_v2.CpLoadAction{Found: true, Ok: true, Source: "auto_load"},
			},
		},
	})
	// cleanup 成功
	_ = orch.OnNodeEvent(ctx, "", agent_v2.Event{
		Type: compose_v2.CpEventType,
		Metadata: map[string]any{
			"action": &compose_v2.CpAction{
				Type:    compose_v2.CpOperationCleanup,
				Cleanup: &compose_v2.CpCleanupAction{DeletedCount: 3, Ok: true},
			},
		},
	})

	cLoad := obs.Counter(observability.MetricCpSaveTotal, map[string]string{
		"crew_cp_op": "load", "crew_outcome": "true",
	})
	cClean := obs.Counter(observability.MetricCpSaveTotal, map[string]string{
		"crew_cp_op": "cleanup", "crew_outcome": "true",
	})
	if cLoad == nil || cLoad.Value() != 1 {
		t.Errorf("op=load Counter 应 = 1, got %v", cLoad)
	}
	if cClean == nil || cClean.Value() != 1 {
		t.Errorf("op=cleanup Counter 应 = 1, got %v", cClean)
	}
}

// ─── Sub-5f: Fanout/Skill/ActiveSessions metric 接入测 ───

// TestPhase2Sub5f_FanoutBranchesMetric 验 OnNodeEvent 内识别 FanoutEventType →
// fire MetricFanoutBranches Histogram.
func TestPhase2Sub5f_FanoutBranchesMetric(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	orch := obs.OrchHooks()
	ctx := context.Background()
	ctx, _ = orch.OnGraphStart(ctx)
	defer func() { _ = orch.OnGraphEnd(ctx, nil) }()

	// 模拟 3 次 fanout: 5 / 3 / 7 分支
	for _, n := range []int{5, 3, 7} {
		_ = orch.OnNodeEvent(ctx, "tools", agent_v2.Event{
			Type: agent_v2.FanoutEventType,
			Metadata: map[string]any{
				"action": &agent_v2.FanoutAction{
					Kind:     agent_v2.FanoutKindToolCalls,
					Branches: n,
				},
			},
		})
	}

	hg := obs.Histogram(observability.MetricFanoutBranches, observability.BucketsCount,
		map[string]string{"crew_fanout_kind": "tool_calls"})
	if hg == nil || hg.Count() != 3 {
		t.Errorf("MetricFanoutBranches Count 应 = 3, got %v", hg)
	}
	wantSum := float64(5 + 3 + 7) // 15
	if hg != nil && hg.Sum() != wantSum {
		t.Errorf("MetricFanoutBranches Sum 应 = %.0f, got %.2f\n"+
			"sanity-revert: 删 hooks.go::fireFanoutMetric hg.Observe → 此测 fail", wantSum, hg.Sum())
	}
}

// TestPhase2Sub5f_FanoutN1NotEmit 验 EmitFanoutEvent caller 端 Branches=1 时 short-circuit
// 不 fire event → 通过捕获 sink 验事件**没真发**, 不是绕开 caller 直调 hook 端
// (audit Sub-5 Round 2 视角 J HIGH fix: 之前测试绕开 caller 验 hook 行为, 真业务
// caller short-circuit 路径未 cover).
func TestPhase2Sub5f_FanoutN1NotEmit(t *testing.T) {
	// 用 compose_v2 内置 capturing sink (RecordingHooks) 验 EmitFanoutEvent 走的 sink
	// 路径. N=1 时 EmitFanoutEvent 应该 short-circuit 不调 sink.Send → 捕获 events=0.
	captured := []compose_v2.Event{}
	sink := &capturingSink{onSend: func(ev compose_v2.Event) { captured = append(captured, ev) }}
	ctx := compose_v2.WithEventSink(context.Background(), sink)

	// 真业务路径 caller (basic_agent.go::toolsFn) 调 EmitFanoutEvent — N=1 时 caller
	// 自身 short-circuit (fanout_event.go:49 `if branches < 2 { return }`).
	agent_v2.EmitFanoutEvent(ctx, agent_v2.FanoutKindToolCalls, 1)
	// 验 sink 没收到 (caller short-circuit).
	if len(captured) != 0 {
		t.Errorf("audit Sub-5 视角 J HIGH regression: EmitFanoutEvent N=1 应 short-circuit\n"+
			"sanity-revert: 删 fanout_event.go::EmitFanoutEvent 内 `if branches < 2 { return }` → "+
			"sink 收到 event → 此测 fail. captured=%v", captured)
	}

	// 对照: N>=2 真 fire
	agent_v2.EmitFanoutEvent(ctx, agent_v2.FanoutKindToolCalls, 3)
	if len(captured) != 1 {
		t.Errorf("N=3 应真 fire event, captured=%d", len(captured))
	}
}

// capturingSink 测试 helper — 接 compose_v2.EventSink 接口捕获所有 Send.
type capturingSink struct {
	onSend func(compose_v2.Event)
}

func (s *capturingSink) Send(ev compose_v2.Event) {
	if s.onSend != nil {
		s.onSend(ev)
	}
}

// TestPhase2Sub5f_SkillActivateMetric 验 OnToolStart 内识别 activate_skill 工具
// fire MetricSkillActivateTotal Counter.
func TestPhase2Sub5f_SkillActivateMetric(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	hooks := obs.AgentHooks()
	ctx := context.Background()
	info := &hook.AgentInfo{Name: "test_skill_agent", Type: "base", Round: 0}

	ctx, _ = hooks.OnAgentStart(ctx, info, &hook.AgentInput{})
	defer func() { _ = hooks.OnAgentEnd(ctx, info, &hook.AgentOutput{}) }()

	// 调 activate_skill 工具 2 次. 必传 ArgumentsJSON 含 skill 字段 — eaa3fdf HIGH-2 fix
	// 后 skill counter 仅 spanKind=="skill" 时 fire, 而 routeToolSpanKind 对空/无 skill
	// 字段的 args fallback "tool" kind (无效 activate_skill 调用不算 skill activation).
	// LLM 真实调用必传 {"skill":...,"query":...}, fixture 对齐.
	for i := 0; i < 2; i++ {
		_, _ = hooks.OnToolStart(ctx, info, &schema.ToolInput{
			Name:          agent_v2.ActivateSkillToolName,
			CallId:        "skill_call_" + string(rune('0'+i)),
			ArgumentsJSON: `{"skill":"code-review","query":"x"}`,
		})
		_, _ = hooks.OnToolEnd(ctx, info, &schema.ToolOutput{
			CallId: "skill_call_" + string(rune('0'+i)),
			Result: &schema.ToolCallResult{Success: true, Output: "loaded"},
		})
	}
	// 调其他 tool 1 次 — 不应触发 skill counter
	_, _ = hooks.OnToolStart(ctx, info, &schema.ToolInput{
		Name: "other_tool", CallId: "other_call",
	})
	_, _ = hooks.OnToolEnd(ctx, info, &schema.ToolOutput{
		CallId: "other_call",
		Result: &schema.ToolCallResult{Success: true},
	})

	c := obs.Counter(observability.MetricSkillActivateTotal, map[string]string{
		"gen_ai_agent_name": "test_skill_agent",
	})
	if c == nil || c.Value() != 2 {
		t.Errorf("MetricSkillActivateTotal 应 = 2 (activate_skill 调 2 次), got %v\n"+
			"sanity-revert: 删 hooks.go::OnToolStart 内 name=ActivateSkillToolName 时 c.Inc() → 此测 fail",
			c)
	}
}

// TestPhase2Sub5f_ActiveSessionsGauge 验 sessionByID 新建路径 ++, CleanupSession --.
func TestPhase2Sub5f_ActiveSessionsGauge(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	// 触发 3 个 session (走 OnAgentStart 路径会创 sessionState)
	hooks := obs.AgentHooks()
	ctxs := make([]context.Context, 3)
	for i := 0; i < 3; i++ {
		sid := "sid_" + string(rune('a'+i))
		ctx := compose_v2.WithSessionIDCtx(context.Background(), sid)
		ctx, _ = hooks.OnAgentStart(ctx, &hook.AgentInfo{Name: "x", Type: "base"}, &hook.AgentInput{})
		ctxs[i] = ctx
	}

	g := obs.Gauge(observability.MetricActiveSessions, nil)
	if g == nil {
		t.Fatal("MetricActiveSessions Gauge 应已创建")
	}
	if v := g.Value(); v != 3 {
		t.Errorf("3 session 创建后 active.sessions 应 = 3, got %d\n"+
			"sanity-revert: 删 observability.go::sessionByID 内 if !loaded { g.Inc() } → 此测 fail", v)
	}

	// Cleanup 1 个
	obs.CleanupSession("sid_a")
	if v := g.Value(); v != 2 {
		t.Errorf("Cleanup 1 后 active.sessions 应 = 2, got %d\n"+
			"sanity-revert: 删 CleanupSession 内 if loaded { g.Dec() } → 此测 fail", v)
	}

	// 重复 Cleanup 不再 Dec (LoadAndDelete miss)
	obs.CleanupSession("sid_a")
	if v := g.Value(); v != 2 {
		t.Errorf("重复 Cleanup 不应再 Dec (LoadAndDelete miss), got %d", v)
	}

	// 同 sid 重新 OnAgentStart 视为新 session ++ 回到 3
	ctx := compose_v2.WithSessionIDCtx(context.Background(), "sid_a")
	_, _ = hooks.OnAgentStart(ctx, &hook.AgentInfo{Name: "x", Type: "base"}, &hook.AgentInput{})
	if v := g.Value(); v != 3 {
		t.Errorf("Cleanup 后再 init 同 sid 应 ++ 回 3, got %d", v)
	}
}

// ─── Sub-5 Round 4 audit K2 fix: cardinality cap 测试 ───

// TestPhase2Sub5_Round4_AuditK2_CardinalityCap 验业务高基数 label 触发 cap 后:
//   - registry 不再 lazy create 新 series (Counter 独立 cap)
//   - Counter() 返 nil (业务 Inc nil-safe)
//   - MetricCardinalityDropped() **去重** 累加被 drop 的 series 数 (Round 5 P2 fix)
//   - **per-type cap** (Round 5 P1 fix): Counter cap 满后 Histogram/Gauge 仍可创建
//     防"一个违规波及全局正常 metric"
//
// sanity-revert: 删 reachedCapForType 检查 → 业务无限增 series → 此测 fail.
func TestPhase2Sub5_Round4_AuditK2_CardinalityCap(t *testing.T) {
	obs, err := observability.New(observability.Config{
		TraceEndpoint:   "127.0.0.1:0",
		MetricsEndpoint: "127.0.0.1:0",
		MaxMetricSeries: 5, // 测试用极小 cap (per-type)
		Resource:        observability.Resource{AppName: "t", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer obs.Close()

	// 创 5 个 Counter series — 应全成功 (达 Counter cap)
	for i := 0; i < 5; i++ {
		c := obs.Counter("test.cap", map[string]string{"k": string(rune('a' + i))})
		if c == nil {
			t.Fatalf("第 %d Counter series 应能创建 (cap=5), got nil", i+1)
		}
	}

	// 再创第 6 个 Counter — 应被 cap drop 返 nil
	overflow := obs.Counter("test.cap", map[string]string{"k": "f"})
	if overflow != nil {
		t.Errorf("第 6 Counter series 超 cap=5 应返 nil, got %v\n"+
			"sanity-revert: 删 reachedCapForType(len(r.counters)) 检查 → 此测 fail", overflow)
	}
	// 验 nil-safe: .Inc() 不 panic
	overflow.Inc()

	// Round 5 P2 fix: 同 key 多次调用失败 droppedSeries 仅算 1 次 (去重)
	for i := 0; i < 10; i++ {
		_ = obs.Counter("test.cap", map[string]string{"k": "f"})
	}
	if got := obs.MetricCardinalityDropped(); got != 1 {
		t.Errorf("Round 5 P2 regression: 同 key 失败 11 次 droppedSeries 应 = 1 (去重), got %d\n"+
			"sanity-revert: 删 recordDrop 内 LoadOrStore 去重 → 此测 fail", got)
	}
	// 不同 key 失败累加
	_ = obs.Counter("test.cap", map[string]string{"k": "g"})
	if got := obs.MetricCardinalityDropped(); got != 2 {
		t.Errorf("不同 key 失败应累加: 应 = 2, got %d", got)
	}

	// Round 5 P1 fix: per-type cap — Counter 满后 Histogram/Gauge 仍可创建
	h := obs.Histogram("test.cap.h", []float64{1, 10}, nil)
	if h == nil {
		t.Errorf("Round 5 P1 regression: per-type cap 让 Histogram 不受 Counter 影响, 应能创建\n" +
			"sanity-revert: 改回 cross-type cap (sum 3 type) → 此测 fail")
	}
	g := obs.Gauge("test.cap.g", nil)
	if g == nil {
		t.Errorf("Round 5 P1 regression: per-type cap 让 Gauge 不受影响, 应能创建")
	}
}

// TestPhase2Sub5_Round6_AuditU1_DroppedKeysBounded 验 Round 6 U1 fix: droppedKeys
// 达上限后停止跟踪新 key 防无界增长 (Round 5 P2 去重引入的 OOM 修复).
//
// bounded 语义: droppedKeysCount 达 maxSeries 上限后, recordDrop 不再 LoadOrStore
// (即使同 key 也直接 ++droppedSeries) — 内存有界优先, 计数退化为近似.
//
// sanity-revert: 删 recordDrop 内 droppedKeysCount >= maxKeys 的 bounded 检查 →
// droppedKeys 无界增长 (业务高基数 label 100 万 unique key → 100 万 entry OOM).
func TestPhase2Sub5_Round6_AuditU1_DroppedKeysBounded(t *testing.T) {
	obs, err := observability.New(observability.Config{
		TraceEndpoint:   "127.0.0.1:0",
		MetricsEndpoint: "127.0.0.1:0",
		MaxMetricSeries: 2, // 极小 cap: counter cap=2 + droppedKeys 上限=2
		Resource:        observability.Resource{AppName: "t", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer obs.Close()

	// 占满 counter cap (2 series)
	_ = obs.Counter("test.bound", map[string]string{"k": "a"})
	_ = obs.Counter("test.bound", map[string]string{"k": "b"})

	// drop key X (count 0<2 → 去重) → dropped=1
	_ = obs.Counter("test.bound", map[string]string{"k": "x"})
	// drop key Y (count 1<2 → 去重) → dropped=2, droppedKeys 达上限 2
	_ = obs.Counter("test.bound", map[string]string{"k": "y"})
	if got := obs.MetricCardinalityDropped(); got != 2 {
		t.Fatalf("前 2 unique drop key 去重应 dropped=2, got %d", got)
	}

	// drop key X 再次 (count 2>=2 → bounded 不查 droppedKeys 直接 ++) → dropped=3
	// 证明 bounded 生效: 若仍去重 X 已在 droppedKeys → dropped 仍=2
	_ = obs.Counter("test.bound", map[string]string{"k": "x"})
	if got := obs.MetricCardinalityDropped(); got != 3 {
		t.Errorf("Round 6 U1 regression: droppedKeys 达上限后同 key X 应直接 ++ (bounded), got %d\n"+
			"sanity-revert: 删 recordDrop 内 droppedKeysCount >= maxKeys 检查 → X 去重 dropped 仍=2 → 此测 fail (但 droppedKeys 无界)",
			got)
	}

	// 大量不同 key drop — 验不 panic + 持续计数 (内存有界因 droppedKeys 停在上限 2)
	for i := 0; i < 1000; i++ {
		_ = obs.Counter("test.bound", map[string]string{"k": "bulk" + string(rune(i))})
	}
	if got := obs.MetricCardinalityDropped(); got < 1000 {
		t.Errorf("1000 不同 key drop 后 dropped 应 >= 1000 (近似计数仍 work), got %d", got)
	}
}

// TestPhase2Sub5_Round8_AuditCC_HistogramBoundary 验 Round 8 audit CC MED: Histogram
// Observe 数值边界 — 负值守护 + Observe(0) 落 bucket 0.
func TestPhase2Sub5_Round8_AuditCC_HistogramBoundary(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	h := obs.Histogram("test.boundary", []float64{0.1, 0.5, 1.0}, nil)
	if h == nil {
		t.Fatal("Histogram nil")
	}

	// Observe(负值) — 应被 v<0 守护忽略 (count 不变)
	h.Observe(-0.1)
	h.Observe(-100)
	if got := h.Count(); got != 0 {
		t.Errorf("audit CC MED regression: Observe(负值) 应被守护忽略 count 不变, got Count=%d\n"+
			"sanity-revert: 删 Histogram.Observe 内 `v < 0 { return }` 守护 → 此测 fail", got)
	}

	// Observe(0) — 0 <= 0.1 落 bucket 0 (右闭)
	h.Observe(0)
	if got := h.Count(); got != 1 {
		t.Errorf("Observe(0) 应正常计入 count=1, got %d", got)
	}
	gotCounts := h.BucketCounts()
	if gotCounts[0] != 1 {
		t.Errorf("Observe(0) 应落 bucket 0 (0<=0.1 右闭), got BucketCounts=%v", gotCounts)
	}
	if got := h.Sum(); got != 0 {
		t.Errorf("Observe(0) Sum 应 = 0, got %.4f", got)
	}
}

// TestPhase2Sub5_Round8_AuditCC_EmptyBuckets 验空 buckets Histogram (只 +Inf 桶) 可工作.
func TestPhase2Sub5_Round8_AuditCC_EmptyBuckets(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	// 空 buckets — counts 长度 = 0+1 = 1 (只 +Inf 桶), Observe 任何值落 idx 0
	h := obs.Histogram("test.empty_buckets", []float64{}, nil)
	if h == nil {
		t.Fatal("Histogram nil")
	}
	h.Observe(0.5)
	h.Observe(100)
	if got := h.Count(); got != 2 {
		t.Errorf("空 buckets 仍能 Observe, Count 应 = 2, got %d", got)
	}
	gotCounts := h.BucketCounts()
	if len(gotCounts) != 1 || gotCounts[0] != 2 {
		t.Errorf("空 buckets counts 应 [2] (全落 +Inf 桶), got %v", gotCounts)
	}
}

// TestPhase2Sub5_Round8_ConcurrentStress 真 -race 高并发压测 (CLAUDE.md "race 必须
// 真跑不逻辑推理"): N goroutine 同时跑完整 hook 生命周期 + 直接 fire metric (混合
// 同/不同 series) + session create/cleanup, -race flag 验证全链路无 data race.
//
// 覆盖之前 7 轮只逻辑推理没真跑的并发路径:
//   - metricRegistry counter/histogram/gauge lazy create 并发 (RLock→Lock 转换)
//   - sessions map sessionByID create + CleanupSession 并发 (ActiveSessions Gauge ++/-- race)
//   - spanEntry runStartNanos/modelName 跨 OnModelStart/End 读写
//   - droppedKeys bounded 并发 (recordDrop 在 r.mu 锁内但多 goroutine 争锁)
func TestPhase2Sub5_Round8_ConcurrentStress(t *testing.T) {
	obs := mustNewObs(t)
	defer obs.Close()

	const N = 50
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sid := fmt.Sprintf("stress_sid_%d", id)
			ctx := compose_v2.WithSessionIDCtx(context.Background(), sid)
			hooks := obs.AgentHooks()
			orch := obs.OrchHooks()
			info := &hook.AgentInfo{Name: "stress_agent", Type: "base", Round: 0}

			// 完整 hook 生命周期 (模拟真 BasicAgent.Run)
			ctx, _ = orch.OnGraphStart(ctx)
			ctx, _ = hooks.OnAgentStart(ctx, info, &hook.AgentInput{})
			_, _ = orch.OnNodeStart(ctx, "model")
			ctx, _ = hooks.OnModelStart(ctx, info, &schema.ModelInput{
				Messages: []*schema.Message{{Role: schema.RoleUser, Content: "hi"}},
			})
			_, _ = hooks.OnModelEnd(ctx, info, &schema.ModelOutput{
				Message: &schema.Message{Role: schema.RoleAssistant, Content: "ok"},
				Usage:   &schema.MessageUsage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15},
			})
			_ = orch.OnNodeEnd(ctx, "model", nil)
			// compact / cp / fanout event (走 OnNodeEvent 路由)
			_ = orch.OnNodeEvent(ctx, "model", agent_v2.Event{
				Type: agent_v2.CompactionEventType,
				Metadata: map[string]any{"action": &agent_v2.CompactionAction{
					Layer: agent_v2.CompactionLayerAuto, Type: agent_v2.CompactionActionAfter,
					After: &agent_v2.CompactionAfterAction{InputTokens: 100, OutputTokens: 30, DurationMs: 5},
				}},
			})
			_ = orch.OnNodeEvent(ctx, "tools", agent_v2.Event{
				Type: agent_v2.FanoutEventType,
				Metadata: map[string]any{"action": &agent_v2.FanoutAction{
					Kind: agent_v2.FanoutKindToolCalls, Branches: 3,
				}},
			})
			_ = hooks.OnAgentEnd(ctx, info, &hook.AgentOutput{})
			_ = orch.OnGraphEnd(ctx, nil)

			// 直接 fire metric — 混合 shared series (争同实例) + per-id series (lazy create)
			obs.Counter("stress.shared", map[string]string{"k": "v"}).Inc()
			obs.Counter("stress.perid", map[string]string{"id": sid}).Inc()
			obs.Histogram("stress.hist", observability.BucketsCount, nil).Observe(float64(id))
			obs.Gauge("stress.gauge", nil).Inc()

			obs.CleanupSession(sid)
		}(i)
	}
	wg.Wait()

	// 验 shared counter 精确 = N (并发 Inc 无丢失)
	if c := obs.Counter("stress.shared", map[string]string{"k": "v"}); c == nil || c.Value() != N {
		t.Errorf("并发 %d Inc shared counter 应 = %d, got %v", N, N, c)
	}
	// 验 ActiveSessions 平 (N create + N cleanup = 0)
	if g := obs.Gauge(observability.MetricActiveSessions, nil); g != nil && g.Value() != 0 {
		t.Errorf("N session create + N cleanup 后 ActiveSessions 应 = 0, got %d", g.Value())
	}
}

// mustNewObs 测试 helper.
func mustNewObs(t *testing.T) *observability.Observability {
	t.Helper()
	obs, err := observability.New(observability.Config{
		TraceEndpoint:   "127.0.0.1:0",
		MetricsEndpoint: "127.0.0.1:0",
		Resource: observability.Resource{
			AppName: "test", EnvName: "test", IP: "1.2.3.4", SDKVersion: "test",
		},
	})
	if err != nil {
		t.Fatalf("New err: %v", err)
	}
	return obs
}
