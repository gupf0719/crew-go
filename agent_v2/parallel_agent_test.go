package agent_v2_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// TestParallelAgent_BasicFanout 验证 ParallelAgent 端到端：3 branch 并发跑同 input，
// 默认 Merger 拼接 "[name] content" 文本。
func TestParallelAgent_BasicFanout(t *testing.T) {
	stub1 := newStubModel([]*schema.Message{schema.AssistantMessage("res1")})
	stub2 := newStubModel([]*schema.Message{schema.AssistantMessage("res2")})
	stub3 := newStubModel([]*schema.Message{schema.AssistantMessage("res3")})

	a1 := agent_v2.NewBasicAgent(agent_v2.WithName("a1"), agent_v2.WithModel(stub1))
	a2 := agent_v2.NewBasicAgent(agent_v2.WithName("a2"), agent_v2.WithModel(stub2))
	a3 := agent_v2.NewBasicAgent(agent_v2.WithName("a3"), agent_v2.WithModel(stub3))

	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("b1", a1).
		AddBranch("b2", a2).
		AddBranch("b3", a3)

	events, err := par.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatal("expected final")
	}
	// 默认 Merger 拼接含 [b1] [b2] [b3]
	for _, want := range []string{"res1", "res2", "res3", "[b1]", "[b2]", "[b3]"} {
		if !strings.Contains(final.Content, want) {
			t.Errorf("final 缺 %q: %q", want, final.Content)
		}
	}
}

// TestParallelAgent_CustomMerger 验证自定义 Merger。
func TestParallelAgent_CustomMerger(t *testing.T) {
	stub1 := newStubModel([]*schema.Message{schema.AssistantMessage("X")})
	stub2 := newStubModel([]*schema.Message{schema.AssistantMessage("Y")})

	a1 := agent_v2.NewBasicAgent(agent_v2.WithName("a1"), agent_v2.WithModel(stub1))
	a2 := agent_v2.NewBasicAgent(agent_v2.WithName("a2"), agent_v2.WithModel(stub2))

	merger := func(_ context.Context, results []agent_v2.ParallelBranchResult) (*schema.Message, error) {
		out := "MERGED:"
		for _, r := range results {
			if r.Output != nil {
				out += r.Output.Content
			}
		}
		return schema.AssistantMessage(out), nil
	}

	par := agent_v2.NewParallelAgent(
		agent_v2.WithParallelName("par"),
		agent_v2.WithParallelMerger(merger),
	).AddBranch("b1", a1).AddBranch("b2", a2)

	events, err := par.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.HasPrefix(final.Content, "MERGED:") {
		t.Fatalf("custom merger not applied: %v", final)
	}
}

// TestParallelAgent_EmptyBranchesFails 验证 fail-fast。
func TestParallelAgent_EmptyBranchesFails(t *testing.T) {
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("empty_par"))
	_, err := par.Run(context.Background(), nil, schema.UserMessage("hi"))
	if err == nil || !strings.Contains(err.Error(), "no branches") {
		t.Fatalf("expected 'no branches' err, got %v", err)
	}
}

// onErrorCounterHook 捕获 OnError 调用次数——验证 LOW-2 / R25 MED-4 fix 不上报控制流错。
type onErrorCounterHook struct {
	hook.NoOpHooks
	count atomic.Int32
}

func (h *onErrorCounterHook) OnError(ctx context.Context, _ *hook.AgentInfo, _ *schema.CommError) context.Context {
	h.count.Add(1)
	return ctx
}

// slowCanceledAgent.Run 等到 ctx canceled 后返 ctx.Err()——mock 业务"长任务被外部取消"。
//
// 相比 immediate-cancel 模式（ctx.Run 前 cancel），slowCanceledAgent 让 ctx 在
// executeFrontier **执行中**才超时——workerFn 真的运行到 err handling 路径，而非
// 被 main loop 的 ctx.Err 早退兜住（5 类炸弹 #2：早退条件让 fix 路径不执行）。
type slowCanceledAgent struct{}

func (a *slowCanceledAgent) Run(ctx context.Context, _ *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(2 * time.Second):
		return nil, nil
	}
}
func (a *slowCanceledAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *slowCanceledAgent) Name() string        { return "slow_canceled" }
func (a *slowCanceledAgent) Description() string { return "" }

// TestParallelAgent_R25_MED4_WorkerCtxErrNotReportedAsError 是 R25 audit MED-4 回归——
// 验证 worker err == ctx.Err() 时 worker 节点**不**调 OnError（对称 R24 LOW-2 finalize
// 的双守卫 IsInterrupt + ctx.Err == nil）。
//
// **Bug 历史**：R24 LOW-2 修 finalize 路径加 ctx.Err 守卫但 worker 路径只单守卫
// IsInterrupt——ctx cancel/deadline 时 worker err 是 ctx.Err()，被误判 "branch_error"
// 上报让监控误判 ctx cancel = 业务错增计数。R25 MED-4 补齐对称。
//
// 同样不对称漏修在 sequential / loop / reflect / handoff / routing 5 编排——R25 一并修。
//
// **测试设计避坑**（5 类炸弹 #2：早退兜底盖过 fix 路径）：
//   - ctx 不能在 par.Run 前 cancel——main loop ctx.Err 检查会早退在 workerFn 之前
//   - 必须让 ctx 在 executeFrontier **执行中**才 timeout，让 workerFn 真的跑到 err 处理
//   - 用 50ms timeout + workers 等 2s——ctx 在 worker select 内 done 触发 worker 返 ctx.Err()
func TestParallelAgent_R25_MED4_WorkerCtxErrNotReportedAsError(t *testing.T) {
	counter := &onErrorCounterHook{}
	par := agent_v2.NewParallelAgent(
		agent_v2.WithParallelName("par"),
		agent_v2.WithParallelHooks(counter),
	).
		AddBranch("c1", &slowCanceledAgent{}).
		AddBranch("c2", &slowCanceledAgent{})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _ = par.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))

	if got := counter.count.Load(); got != 0 {
		t.Errorf("R25 MED-4 回归：worker err == ctx.Err() 时 OnError **不**应被调（控制流非业务错）；实际调 %d 次\n"+
			"sanity-revert 删 worker 路径 ctx.Err()==nil 守卫 → OnError 误调", got)
	}
}

// TestParallelAgent_R24_LOW2_MergerInterruptNotReportedAsError 是 R24 audit LOW-2 回归——
// 验证 merger 抛 InterruptError 时 finalize 节点**不**调 OnError（控制流非业务错），
// 同时 err 仍 propagate 让框架走 cp 挂起 + Resume。
//
// **Bug 历史**：原 finalize 无差别 safeOnError 上报 "merger_error"——业务装备
// HITL gate（"merge 前用户审批最终聚合"）抛 InterruptError 时被误判为业务错污染监控。
// 修法：finalize 对照 worker 路径加 IsInterrupt + ctx.Err 检查（agent_v2/parallel_builder.go::finalizeFn）。
func TestParallelAgent_R24_LOW2_MergerInterruptNotReportedAsError(t *testing.T) {
	stub1 := newStubModel([]*schema.Message{schema.AssistantMessage("X")})
	stub2 := newStubModel([]*schema.Message{schema.AssistantMessage("Y")})

	a1 := agent_v2.NewBasicAgent(agent_v2.WithName("a1"), agent_v2.WithModel(stub1))
	a2 := agent_v2.NewBasicAgent(agent_v2.WithName("a2"), agent_v2.WithModel(stub2))

	// merger 抛 InterruptError mock"merge 前用户审批"场景
	merger := func(_ context.Context, _ []agent_v2.ParallelBranchResult) (*schema.Message, error) {
		return nil, &compose_v2.InterruptError{
			Reason: "approve_merge",
			Query:  &compose_v2.AskQuery{InterruptID: "merge_gate", Question: "approve merge?"},
		}
	}

	counter := &onErrorCounterHook{}
	par := agent_v2.NewParallelAgent(
		agent_v2.WithParallelName("par"),
		agent_v2.WithParallelMerger(merger),
		agent_v2.WithParallelHooks(counter),
	).AddBranch("b1", a1).AddBranch("b2", a2)

	_, err := par.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("R24 LOW-2 回归：merger 抛 InterruptError 时 ParallelAgent.Run 应 propagate；实际 nil")
	}
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Errorf("R24 LOW-2 回归：err 应为 InterruptError 类型——实际 %v (%T)", err, err)
	}
	if got := counter.count.Load(); got != 0 {
		t.Errorf("R24 LOW-2 回归：merger 抛 InterruptError 时 OnError **不**应被调（控制流非业务错）；实际调 %d 次\n"+
			"sanity-revert 删 IsInterrupt 检查 → OnError 误调 1 次", got)
	}
}
