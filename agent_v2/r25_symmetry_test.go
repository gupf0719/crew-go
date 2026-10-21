package agent_v2_test

import (
	"context"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// R25 audit 历史回填测试集——补 R25 HIGH-1 + MED-4 5 编排对称漏修。
//
// **背景**：R25 commit f347769 改了 7 编排 18 处 hook err 路径，但只在
// `basic_agent_test.go::TestBasicAgent_R25_HIGH1` 测了 BasicAgent；同样
// MED-4 改了 6 编排 worker err 路径，但只在 `parallel_agent_test.go::TestParallelAgent_R25_MED4`
// 测了 ParallelAgent。其余 5 编排（sequential / loop / reflect / handoff / routing）
// 各 2 处对称 fix 共 10 处**裸跑无回归测试**——属 CLAUDE.md "假绿炸弹"标准。
//
// 本文件补齐 5 编排 × 2 = **10 个对称回归测试**。
//
// ---
//
// **测试设计**（套用现有 BasicAgent / ParallelAgent 模板）：
//   - HIGH-1 hook err: 注入 OnAgentStart 抛 InterruptError 的 hook，验 onErrorCalls == 0
//   - MED-4 worker err: 用 slowCanceledAgent 当 worker，ctx 50ms timeout 让 worker 收 ctx.Err，验 onErrorCalls == 0
//
// 复用：hookInterruptStartHook（basic_agent_test.go）+ onErrorCounterHook + slowCanceledAgent（parallel_agent_test.go）。

// ── R25 HIGH-1 hook err 5 编排对称回归 ──

// TestSequentialAgent_R25_HIGH1_HookInterruptNotReportedAsError 补 SequentialAgent
// 对称漏修——OnAgentStart 抛 InterruptError 时 OnError 不应被调。
func TestSequentialAgent_R25_HIGH1_HookInterruptNotReportedAsError(t *testing.T) {
	hk := &hookInterruptStartHook{}
	dummy := &slowCanceledAgent{} // 不会被调到（hook 在 sub-agent 跑前抛）
	seq := agent_v2.NewSequentialAgent(
		agent_v2.WithSequentialName("seq"),
		agent_v2.WithSequentialHooks(hk),
	).AddStep("a", dummy)

	_, err := seq.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("R25 HIGH-1 (Sequential)：hook 抛 InterruptError 应 propagate")
	}
	// 契约更新（2026-08-29）：**顶层** hook 抛中断被拒——图之前无档可落，旧行为是
	// IsInterrupt=true 而挂起档 0、用户答完 Resume 撞 ErrNoPendingCheckpoint 的**假暂停**。
	// 本用例守的真实伤害是下面那半（OnError 不误报，判据看原始 err、仍是中断故不上报）；
	// 中断类型断言随契约反转。嵌套（interruptHook 范式）的 IsInterrupt=true 由
	// TestHookInterrupt_NestedStillLandsCheckpoint 钉。
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Errorf("顶层 hook 中断该被拒成非中断错误（假暂停→响亮错误），实际仍 IsInterrupt: %v", err)
	}
	if got := hk.onErrorCalls.Load(); got != 0 {
		t.Errorf("R25 HIGH-1 (Sequential)：hook 抛 InterruptError 时 OnError 不应被调；实际调 %d 次\n"+
			"sanity-revert 删 agent_v2/sequential_builder.go OnAgentStart err 路径 IsInterrupt 守卫 → OnError 误调", got)
	}
}

// TestLoopAgent_R25_HIGH1_HookInterruptNotReportedAsError 补 LoopAgent 对称漏修。
func TestLoopAgent_R25_HIGH1_HookInterruptNotReportedAsError(t *testing.T) {
	hk := &hookInterruptStartHook{}
	dummy := &slowCanceledAgent{}
	loopAg := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("loop"),
		agent_v2.WithLoopHooks(hk),
		agent_v2.WithLoopMaxIterations(1),
	).WithBody(dummy)

	_, err := loopAg.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("R25 HIGH-1 (Loop)：hook 抛 InterruptError 应 propagate")
	}
	// 契约更新（2026-08-29）：**顶层** hook 抛中断被拒——图之前无档可落，旧行为是
	// IsInterrupt=true 而挂起档 0、用户答完 Resume 撞 ErrNoPendingCheckpoint 的**假暂停**。
	// 本用例守的真实伤害是下面那半（OnError 不误报，判据看原始 err、仍是中断故不上报）；
	// 中断类型断言随契约反转。嵌套（interruptHook 范式）的 IsInterrupt=true 由
	// TestHookInterrupt_NestedStillLandsCheckpoint 钉。
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Errorf("顶层 hook 中断该被拒成非中断错误（假暂停→响亮错误），实际仍 IsInterrupt: %v", err)
	}
	if got := hk.onErrorCalls.Load(); got != 0 {
		t.Errorf("R25 HIGH-1 (Loop)：hook 抛 InterruptError 时 OnError 不应被调；实际调 %d 次\n"+
			"sanity-revert 删 agent_v2/loop_builder.go OnAgentStart err 路径 IsInterrupt 守卫 → OnError 误调", got)
	}
}

// TestReflectAgent_R25_HIGH1_HookInterruptNotReportedAsError 补 ReflectAgent 对称漏修。
func TestReflectAgent_R25_HIGH1_HookInterruptNotReportedAsError(t *testing.T) {
	hk := &hookInterruptStartHook{}
	dummy := &slowCanceledAgent{}
	rf := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("rf"),
		agent_v2.WithReflectHooks(hk),
		agent_v2.WithEvalFunc(func(_ context.Context, _ *schema.Message) (bool, error) {
			return true, nil
		}),
	).WithActor(dummy)

	_, err := rf.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("R25 HIGH-1 (Reflect)：hook 抛 InterruptError 应 propagate")
	}
	// 契约更新（2026-08-29）：**顶层** hook 抛中断被拒——图之前无档可落，旧行为是
	// IsInterrupt=true 而挂起档 0、用户答完 Resume 撞 ErrNoPendingCheckpoint 的**假暂停**。
	// 本用例守的真实伤害是下面那半（OnError 不误报，判据看原始 err、仍是中断故不上报）；
	// 中断类型断言随契约反转。嵌套（interruptHook 范式）的 IsInterrupt=true 由
	// TestHookInterrupt_NestedStillLandsCheckpoint 钉。
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Errorf("顶层 hook 中断该被拒成非中断错误（假暂停→响亮错误），实际仍 IsInterrupt: %v", err)
	}
	if got := hk.onErrorCalls.Load(); got != 0 {
		t.Errorf("R25 HIGH-1 (Reflect)：hook 抛 InterruptError 时 OnError 不应被调；实际调 %d 次\n"+
			"sanity-revert 删 agent_v2/reflect_builder.go OnAgentStart err 路径 IsInterrupt 守卫 → OnError 误调", got)
	}
}

// TestHandoffAgent_R25_HIGH1_HookInterruptNotReportedAsError 补 HandoffAgent 对称漏修。
func TestHandoffAgent_R25_HIGH1_HookInterruptNotReportedAsError(t *testing.T) {
	hk := &hookInterruptStartHook{}
	dummy := &slowCanceledAgent{}
	ha := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("ha"),
		agent_v2.WithHandoffHooks(hk),
		agent_v2.WithHandoffInitial("a"),
	).AddAgent("a", dummy)

	_, err := ha.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("R25 HIGH-1 (Handoff)：hook 抛 InterruptError 应 propagate")
	}
	// 契约更新（2026-08-29）：**顶层** hook 抛中断被拒——图之前无档可落，旧行为是
	// IsInterrupt=true 而挂起档 0、用户答完 Resume 撞 ErrNoPendingCheckpoint 的**假暂停**。
	// 本用例守的真实伤害是下面那半（OnError 不误报，判据看原始 err、仍是中断故不上报）；
	// 中断类型断言随契约反转。嵌套（interruptHook 范式）的 IsInterrupt=true 由
	// TestHookInterrupt_NestedStillLandsCheckpoint 钉。
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Errorf("顶层 hook 中断该被拒成非中断错误（假暂停→响亮错误），实际仍 IsInterrupt: %v", err)
	}
	if got := hk.onErrorCalls.Load(); got != 0 {
		t.Errorf("R25 HIGH-1 (Handoff)：hook 抛 InterruptError 时 OnError 不应被调；实际调 %d 次\n"+
			"sanity-revert 删 agent_v2/handoff_builder.go OnAgentStart err 路径 IsInterrupt 守卫 → OnError 误调", got)
	}
}

// TestRoutingAgent_R25_HIGH1_HookInterruptNotReportedAsError 补 RoutingAgent 对称漏修。
func TestRoutingAgent_R25_HIGH1_HookInterruptNotReportedAsError(t *testing.T) {
	hk := &hookInterruptStartHook{}
	dummy := &slowCanceledAgent{}
	rt := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("rt"),
		agent_v2.WithRoutingHooks(hk),
		agent_v2.WithRouter(func(_ context.Context, _ *schema.Message) (compose_v2.NodeID, error) {
			return "a", nil
		}),
	).AddBranch("a", dummy)

	_, err := rt.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("R25 HIGH-1 (Routing)：hook 抛 InterruptError 应 propagate")
	}
	// 契约更新（2026-08-29）：**顶层** hook 抛中断被拒——图之前无档可落，旧行为是
	// IsInterrupt=true 而挂起档 0、用户答完 Resume 撞 ErrNoPendingCheckpoint 的**假暂停**。
	// 本用例守的真实伤害是下面那半（OnError 不误报，判据看原始 err、仍是中断故不上报）；
	// 中断类型断言随契约反转。嵌套（interruptHook 范式）的 IsInterrupt=true 由
	// TestHookInterrupt_NestedStillLandsCheckpoint 钉。
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Errorf("顶层 hook 中断该被拒成非中断错误（假暂停→响亮错误），实际仍 IsInterrupt: %v", err)
	}
	if got := hk.onErrorCalls.Load(); got != 0 {
		t.Errorf("R25 HIGH-1 (Routing)：hook 抛 InterruptError 时 OnError 不应被调；实际调 %d 次\n"+
			"sanity-revert 删 agent_v2/routing_builder.go OnAgentStart err 路径 IsInterrupt 守卫 → OnError 误调", got)
	}
}

// ── R25 MED-4 worker ctx.Err 5 编排对称回归 ──

// TestSequentialAgent_R25_MED4_WorkerCtxErrNotReportedAsError 补 SequentialAgent
// 对称漏修——step 返 ctx.Err() 时 OnError 不应被调。
func TestSequentialAgent_R25_MED4_WorkerCtxErrNotReportedAsError(t *testing.T) {
	counter := &onErrorCounterHook{}
	seq := agent_v2.NewSequentialAgent(
		agent_v2.WithSequentialName("seq"),
		agent_v2.WithSequentialHooks(counter),
	).AddStep("a", &slowCanceledAgent{})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _ = seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))

	if got := counter.count.Load(); got != 0 {
		t.Errorf("R25 MED-4 (Sequential)：worker err == ctx.Err() 时 OnError 不应被调；实际调 %d 次\n"+
			"sanity-revert 删 agent_v2/sequential_builder.go step err 路径 ctx.Err()==nil 守卫 → OnError 误调", got)
	}
}

// TestLoopAgent_R25_MED4_BodyCtxErrNotReportedAsError 补 LoopAgent 对称漏修。
func TestLoopAgent_R25_MED4_BodyCtxErrNotReportedAsError(t *testing.T) {
	counter := &onErrorCounterHook{}
	loopAg := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("loop"),
		agent_v2.WithLoopHooks(counter),
		agent_v2.WithLoopMaxIterations(1),
	).WithBody(&slowCanceledAgent{})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _ = loopAg.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))

	if got := counter.count.Load(); got != 0 {
		t.Errorf("R25 MED-4 (Loop)：body err == ctx.Err() 时 OnError 不应被调；实际调 %d 次\n"+
			"sanity-revert 删 agent_v2/loop_builder.go body err 路径 ctx.Err()==nil 守卫 → OnError 误调", got)
	}
}

// TestReflectAgent_R25_MED4_ActorCtxErrNotReportedAsError 补 ReflectAgent 对称漏修。
func TestReflectAgent_R25_MED4_ActorCtxErrNotReportedAsError(t *testing.T) {
	counter := &onErrorCounterHook{}
	rf := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("rf"),
		agent_v2.WithReflectHooks(counter),
		agent_v2.WithEvalFunc(func(_ context.Context, _ *schema.Message) (bool, error) {
			return true, nil
		}),
	).WithActor(&slowCanceledAgent{})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _ = rf.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))

	if got := counter.count.Load(); got != 0 {
		t.Errorf("R25 MED-4 (Reflect)：actor err == ctx.Err() 时 OnError 不应被调；实际调 %d 次\n"+
			"sanity-revert 删 agent_v2/reflect_builder.go actor err 路径 ctx.Err()==nil 守卫 → OnError 误调", got)
	}
}

// TestHandoffAgent_R25_MED4_StepCtxErrNotReportedAsError 补 HandoffAgent 对称漏修。
func TestHandoffAgent_R25_MED4_StepCtxErrNotReportedAsError(t *testing.T) {
	counter := &onErrorCounterHook{}
	ha := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("ha"),
		agent_v2.WithHandoffHooks(counter),
		agent_v2.WithHandoffInitial("a"),
	).AddAgent("a", &slowCanceledAgent{})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _ = ha.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))

	if got := counter.count.Load(); got != 0 {
		t.Errorf("R25 MED-4 (Handoff)：step err == ctx.Err() 时 OnError 不应被调；实际调 %d 次\n"+
			"sanity-revert 删 agent_v2/handoff_builder.go step err 路径 ctx.Err()==nil 守卫 → OnError 误调", got)
	}
}

// TestRoutingAgent_R25_MED4_WorkerCtxErrNotReportedAsError 补 RoutingAgent 对称漏修。
func TestRoutingAgent_R25_MED4_WorkerCtxErrNotReportedAsError(t *testing.T) {
	counter := &onErrorCounterHook{}
	rt := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("rt"),
		agent_v2.WithRoutingHooks(counter),
		agent_v2.WithRouter(func(_ context.Context, _ *schema.Message) (compose_v2.NodeID, error) {
			return "a", nil
		}),
	).AddBranch("a", &slowCanceledAgent{})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _ = rt.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))

	if got := counter.count.Load(); got != 0 {
		t.Errorf("R25 MED-4 (Routing)：worker err == ctx.Err() 时 OnError 不应被调；实际调 %d 次\n"+
			"sanity-revert 删 agent_v2/routing_builder.go worker err 路径 ctx.Err()==nil 守卫 → OnError 误调", got)
	}
}
