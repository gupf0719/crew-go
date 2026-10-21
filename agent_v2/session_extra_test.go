package agent_v2_test

import (
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// TestRunSession_KV 验证 GetValue / SetValue 基本读写。
func TestRunSession_KV(t *testing.T) {
	s := agent_v2.NewRunSession()
	s.SetValue("k1", "v1")
	s.SetValue("k2", 42)
	if v, ok := s.GetValue("k1"); !ok || v != "v1" {
		t.Errorf("k1: got %v %v", v, ok)
	}
	if v, ok := s.GetValue("k2"); !ok || v != 42 {
		t.Errorf("k2: got %v %v", v, ok)
	}
	if _, ok := s.GetValue("missing"); ok {
		t.Errorf("missing: should not exist")
	}
}

// TestRunSession_KV_LazyInitOnLiteral 验证字面量构造的 RunSession KV 不 panic。
func TestRunSession_KV_LazyInitOnLiteral(t *testing.T) {
	s := &agent_v2.RunSession{RunID: "lit"} // 不走 NewRunSession → values nil
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("SetValue panicked: %v", r)
		}
	}()
	s.SetValue("k", "v")
	if v, ok := s.GetValue("k"); !ok || v != "v" {
		t.Errorf("expect lazy-init kv, got %v %v", v, ok)
	}
}

// TestRunSession_RestoreEvents 验证 RestoreEvents 替换 events。
func TestRunSession_RestoreEvents(t *testing.T) {
	s := agent_v2.NewRunSession()
	old := []*agent_v2.AgentEvent{
		agent_v2.NewExitEvent("a", schema.AssistantMessage("old")),
	}
	s.RestoreEvents(old)
	got := s.Events()
	if len(got) != 1 || got[0].Output == nil || got[0].Output.Message.Content != "old" {
		t.Errorf("RestoreEvents failed: %v", got)
	}
}

// TestRunSession_HistoryFor_CrossAgentRewrite 验证 HistoryFor 跨 agent 改写。
func TestRunSession_HistoryFor_CrossAgentRewrite(t *testing.T) {
	s := agent_v2.NewRunSession()
	s.AddEvent(agent_v2.NewOutputEvent("self", schema.UserMessage("hello")))
	s.AddEvent(agent_v2.NewExitEvent("other", schema.AssistantMessage("from other")))

	hist := s.HistoryFor("self")
	if len(hist) != 2 {
		t.Fatalf("expect 2 messages, got %d", len(hist))
	}
	// other 的 assistant 消息应被改写成 user("For context: [other] said: from other.")
	if hist[1].Role != schema.RoleUser {
		t.Errorf("expect cross-agent message rewritten as user, got role=%s", hist[1].Role)
	}
}

// **R32+ 视角 4 audit 删**: TestRunSession_SupersedeEventsByMarker 测的 API 已删
// (生产 0 caller, godoc 自称 AutoCompact 调实际不调, 已被 SelfHistoryCompacted
// 倒扫 + CompactionSummaryAnnotationKey marker 替代)。

// TestRunSession_AddEventConcurrent 验证 AddEvent 并发安全。
func TestRunSession_AddEventConcurrent(t *testing.T) {
	s := agent_v2.NewRunSession()
	var wg sync.WaitGroup
	const N = 100
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			s.AddEvent(agent_v2.NewExitEvent("a", schema.AssistantMessage("x")))
		}()
	}
	wg.Wait()
	if len(s.Events()) != N {
		t.Errorf("expect %d events, got %d", N, len(s.Events()))
	}
}

// **R32+ 视角 4 audit 删**: TestRunSession_AddEventInGraph 测的 API 已删
// (生产 0 caller, ev.NS 字段无人读, 设计被 ParallelAgent ForkLane 替代)。

// TestRunSession_ForkLaneVsAddEventRace 验证 ForkLane → shareEventsFrom 读
// parent.events slice header 期间 parent.AddEvent 并发写不触发 race。
//
// **背景**：session.go:362 `shareEventsFrom` 读 parent.events / parent.eventsMuRef
// 漏锁——parent 主线 goroutine 并发 AddEvent 写 events 时 race detector 触发
// (R34 ISSUE-87 audit 发现; PR 2 commit 990bdad 加 store ctx push 后 scheduling
// 时序变化让原本 ~40% 命中率升到 ~66%, 根因独立于 PR 2).
//
// 用 WaitGroup 屏障让 writer / reader 真正并发 (CLAUDE.md 测试纪律: thunder-herd
// 测试需强制并发否则 Go scheduler 自然串行化掉).
//
// **sanity-revert 标准**: revert shareEventsFrom 内 RLock → 跑 `go test -race` 必 fail
// 且 fail 信息精确指向 session.go:362 + ForkLane stack。
func TestRunSession_ForkLaneVsAddEventRace(t *testing.T) {
	parent := agent_v2.NewRunSession()
	// 预填一些事件让 parent.events slice 不空 — slice header 读时 ptr/len/cap 都非零
	for i := 0; i < 4; i++ {
		parent.AddEvent(agent_v2.NewExitEvent("seed", schema.AssistantMessage("seed")))
	}

	const N = 200
	var start sync.WaitGroup
	start.Add(1) // 屏障: 所有 worker 等 start.Done() 同时起跑
	var done sync.WaitGroup

	// Writer: 持续 AddEvent 写 parent.events (走 eventsMu Lock)
	done.Add(1)
	go func() {
		defer done.Done()
		start.Wait()
		for i := 0; i < N; i++ {
			parent.AddEvent(agent_v2.NewExitEvent("w", schema.AssistantMessage("w")))
		}
	}()

	// Reader: 持续 ForkLane → shareEventsFrom 读 parent.events (无锁路径 = race 触发点)
	done.Add(1)
	go func() {
		defer done.Done()
		start.Wait()
		for i := 0; i < N; i++ {
			_ = parent.ForkLane()
		}
	}()

	start.Done() // 同时放行
	done.Wait()
}
