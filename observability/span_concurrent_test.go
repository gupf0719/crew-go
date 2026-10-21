// span 并发安全测试 — 验 N>=2 fanout 并发 OnToolEnd + 同时 OnGraphEnd sweep 无 data race.
//
// ────────────────────────────────────────────────────────────────────────
// **方案 C 并发加固 (commit f823ff9 audit 视角 C 盲区补测)**
//
// 方案 C 给 spanEntry 加了 kind/name 字段 (registry key 用 callID 唯一锚点后, OnToolEnd
// 从 entry 读展示用 kind/name 而非遍历反查). kind/name 在 startSpan 构造时设, 之后只读 —
// 设计上是"构造后不可变 + 无锁读". 但现有 -race 测试 (hooks_test.go 的 50 goroutine
// OnNodeEvent 全走 chunkBuffers 不触 spanEntry; 10 goroutine 是独立 session) 从未让
// **同 session 的 N 个 tool entry 在并发下被访问**.
//
// 此测在比真实模型 (compose_v2 串行 fire OnTool*, 仅业务 tool 函数 goroutine 并发) 更
// 激进的并发下验证 3 层安全:
//  1. entry.kind/name 无锁读 (构造后不可变 + sync.Map Store→Load happens-before)
//  2. entry.runStartNanos + entry.span 持 entry.mu (OnToolEnd endSpan vs sweep 抢锁)
//  3. sess.spans 的 LoadAndDelete (OnToolEnd) vs Range+Delete (sweep) — sync.Map 原生安全
//
// **必须 -race 跑**: go test ./observability/ -race -run TestSpanConcurrent
// 纯 race 验证 — 无功能断言, 唯一失败信号是 -race detector 报 data race.
// ────────────────────────────────────────────────────────────────────────
package observability

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/modules/common"
	"crew-go/schema"
)

func TestSpanConcurrent_FanoutToolEndVsSweep(t *testing.T) {
	_, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ah := obs.AgentHooks()
	orch := obs.OrchHooks()
	ctx := compose_v2.WithGraphNS(context.Background(), "fanout_graph")
	info := &hook.AgentInfo{Name: "fanout_agent", Type: "base", Round: 0}

	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{Messages: []*schema.Message{schema.UserMessage("go")}})
	_, _ = orch.OnGraphStart(ctx)

	const N = 16
	// 先串行创 N 个 retrieval tool span (in-flight) — 模拟 fanout 多 tool_call.
	// 用 retrieval kind (非 "tool") 走方案 C 的 callID-key + entry.kind="retrieval" 路径.
	for i := 0; i < N; i++ {
		_, _ = ah.OnToolStart(ctx, info, &schema.ToolInput{
			Name:          "vector_search",
			CallId:        fmt.Sprintf("tc_%d", i),
			ArgumentsJSON: `{"q":"x"}`,
			Extra:         map[string]any{"category": "retrieval"},
		})
	}

	// 并发: N goroutine 各 OnToolEnd 自己的 span + 1 goroutine OnGraphEnd sweep,
	// barrier 屏障让它们真正同时跑 (抢同一批 in-flight entry).
	var wg sync.WaitGroup
	barrier := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-barrier
			_, _ = ah.OnToolEnd(ctx, info, &schema.ToolOutput{
				CallId: fmt.Sprintf("tc_%d", i),
				Result: &common.ToolCallResult{Success: true, Output: "ok"},
			})
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-barrier
		orch.OnGraphEnd(ctx, nil) // 内部 sweepInFlight "tool|fanout_graph|" Range + Delete
	}()
	close(barrier) // 放闸 — N 个 OnToolEnd + 1 个 sweep 真并发抢同批 entry
	wg.Wait()

	_ = ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
}
