package agent_v2_test

import (
	"context"
	"fmt"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/store"
)

// TestMicroCompact_OffloadNamespace 验证 audit MED fix：offload 写入与业务恢复共用
// OffloadNamespace 单点。默认档（OffloadNS 空）按 ctx sid 派生 store.NS("_micro_offload", sid)——
// 业务恢复 offloaded 全文必须 store.Get(ctx, cfg.OffloadNamespace(ctx), key)，裸用 cfg.OffloadNS
// （默认空）会查错 NS → miss。
func TestMicroCompact_OffloadNamespace(t *testing.T) {
	cfg := &agent_v2.MicroCompactConfig{}
	ctx := compose_v2.WithSessionIDCtx(context.Background(), "sess-1")

	// 默认档 + ctx 有 sid → per-sid 隔离 NS（≠ 裸 cfg.OffloadNS 的空 NS）
	ns := cfg.OffloadNamespace(ctx)
	if len(ns) != 2 || ns[0] != "_micro_offload" || ns[1] != "sess-1" {
		t.Errorf("默认档 OffloadNamespace = %v; want [_micro_offload sess-1]（裸 cfg.OffloadNS 会得空 NS → 恢复 miss）", ns)
	}
	// 显式 OffloadNS → 直接用（不派生）
	cfg2 := &agent_v2.MicroCompactConfig{OffloadNS: store.NS("custom", "x")}
	if got := cfg2.OffloadNamespace(ctx); len(got) != 2 || got[0] != "custom" || got[1] != "x" {
		t.Errorf("显式 OffloadNS 应直接用, got %v", got)
	}
	// 无 sid → 全局 fallback
	if got := cfg.OffloadNamespace(context.Background()); len(got) != 1 || got[0] != "_micro_offload" {
		t.Errorf("无 sid 应 fallback 全局 [_micro_offload], got %v", got)
	}
}

// TestHandoffState_DefaultMaxHops_ExhaustedSet 验证 audit LOW fix：默认 maxHops（不设
// WithHandoffMaxHops → h.maxHops=0，builder 用 defaultHandoffMaxHops=10 终止）撞 10 跳耗尽时
// State().Exhausted 置位。fix 前 wrapper 的 `h.maxHops>0` 门槛让默认档永不置位——业务靠
// State().Exhausted 检测"撞上限"在默认配置下收不到信号。
func TestHandoffState_DefaultMaxHops_ExhaustedSet(t *testing.T) {
	mkTransfers := func(target string) []*schema.Message {
		out := make([]*schema.Message, 12) // >10 跳，确保撞默认上限而非提前 done
		for i := range out {
			out[i] = transferToolCall(fmt.Sprintf("tc_%s_%d", target, i), target)
		}
		return out
	}
	a := agent_v2.NewBasicAgent(agent_v2.WithName("A"),
		agent_v2.WithModel(newStubModel(mkTransfers("B"))),
		agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3))
	b := agent_v2.NewBasicAgent(agent_v2.WithName("B"),
		agent_v2.WithModel(newStubModel(mkTransfers("A"))),
		agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3))
	// 不设 WithHandoffMaxHops——用默认（h.maxHops=0，builder 默认 10）
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("pingpong"),
		agent_v2.WithHandoffInitial("A"),
	).AddAgent("A", a).AddAgent("B", b)

	sess := agent_v2.NewRunSession()
	_, _ = h.Run(context.Background(), sess, schema.UserMessage("start"))
	state := h.State(sess)
	if !state.Exhausted {
		t.Errorf("默认 maxHops 撞 10 跳耗尽应 Exhausted=true（fix 前 wrapper h.maxHops>0 门槛让默认档永不置位），got %+v", state)
	}
}

// sidCaptureHook 捕获 OnAgentStart 时 ctx 里的 sid（验证 abort 路径 push sid）。
type sidCaptureHook struct {
	hook.NoOpHooks
	capturedSid string
	fired       bool
}

func (h *sidCaptureHook) OnAgentStart(ctx context.Context, info *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	h.capturedSid = compose_v2.GetSessionID(ctx)
	h.fired = true
	return ctx, nil
}

// TestResume_Abort_PushesSid 验证 audit MED fix：abort 路径 fire OnAgentStart 时 ctx 带 sid。
// fix 前 abort 分支在 WithSession 之前 return，OnAgentStart 收到空 sid → observability 用
// GetSessionID(ctx)="" 建孤儿 session + 缺 gen_ai.request.id + resume duration 漏采。
// abort 幂等（不需 pending cp——commonResumePrep abort 分支 DeleteSubtree 对不存在 ns 也合法）。
func TestResume_Abort_PushesSid(t *testing.T) {
	capHook := &sidCaptureHook{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("bot"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("ok")})),
		agent_v2.WithHooks(capHook),
	)
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)

	_, err := ag.Resume(ctx, "sess-abort-1", agent_v2.WithResumeAction(agent_v2.ResumeActionAbort))
	if err != nil {
		t.Fatalf("abort resume 应幂等返 nil: %v", err)
	}
	if !capHook.fired {
		t.Fatal("abort 路径应 fire OnAgentStart（observability 完整记录 abort）")
	}
	if capHook.capturedSid != "sess-abort-1" {
		t.Errorf("abort 路径 OnAgentStart 的 ctx 应带 sid=sess-abort-1"+
			"（fix 前为空 → observability 孤儿 session + resume duration 漏采），got %q", capHook.capturedSid)
	}
}
