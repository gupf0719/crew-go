package agent_v2_test

// 「同 sid 的下一个任务不得捡起上一个的挂起存档」的回归。
//
// # 修的是什么
//
// `Run` 是新任务、`Resume` 才是续跑——这个区分本来就在方法名上，但以前没透到引擎：
// 引擎的 auto-load 只看「有没有 Pending cp」，于是同 sid 的任务 B 会把任务 A 的存档
// 捡起来当续跑 ⟹ **B 的输入压根不进图、B 从 A 中断的位置继续跑、还读到 A 的工具产出**，
// 全程 err=nil 无告警。业务把 sid 设成会话级稳定值时就会踩（而 `NewRunSession` 的
// godoc 恰恰邀请「与业务持久化主键对齐」）。
//
// # 判据：顶层 Run 恒不当续跑
//
// 「顶层」= `compose_v2.GetGraphNS(ctx) == ""`。嵌套（ns 非空）必须保持 auto-load——
// 父 Resume 时子 agent 正是靠「父重跑 → 子的 Run 被调 → 子 auto-load 自己的 cp」恢复的，
// 去掉它嵌套 HITL 全断（实测 nested_smoke 12/12 → 3/12）。
//
// ⚠️ **这个判据以前在两处出现、必须手工同步**：一处是当年 agent 侧那个自查（叫
// skipInitialWrites，已删）决定注不注入初始输入，另一处是 `WithFreshRun` 决定捡不捡
// 存档。只改一处会跑出「两头不着的 fresh run」——那边从 StartNode 起跑、这边跳过了
// 初始写入，preamble 拿不到输入直接失败。**那对判据漂移过四次**。
//
// 现在只剩一处：播种改走 `compose_v2.WithInitialWrites`（引擎只在新开路径 apply），
// 调用方不必再自己判断这次算不算续跑，那个自查连同它那次 `LoadNS` 一起删掉了。
// **别再往回加第二份拷贝。**

import (
	"context"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

func TestCrossTaskIsolation_NewRunDoesNotInheritPendingCheckpoint(t *testing.T) {
	var pays atomic.Int32
	var lastAmt atomic.Value
	payTool := tool.NewFuncTool("pay", "扣款", map[string]any{"type": "object",
		"properties": map[string]any{"amt": map[string]any{"type": "string"}}},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			amt, _ := args["amt"].(string)
			pays.Add(1)
			lastAmt.Store(amt)
			return tool.Success("已扣款 " + amt), nil
		})
	holdTool := tool.NewFuncTool("hold", "要人工确认", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if _, ok := compose_v2.GetResumeAnswerText(ctx, "iso_q"); ok {
				return tool.Success("ok"), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "iso_q", Question: "确认?"}}
		})
	tk := tool.NewToolkit()
	tk.Register(payTool)
	tk.Register(holdTool)

	mk := func(amt string) *agent_v2.BasicAgent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("isoAg"),
			agent_v2.WithModel(newStubModel([]*schema.Message{
				schema.AssistantToolCallMessage([]schema.ToolCall{
					{ID: "tc_pay", Function: schema.FunctionCall{Name: "pay", Arguments: `{"amt":"` + amt + `"}`}},
					{ID: "tc_hold", Function: schema.FunctionCall{Name: "hold", Arguments: `{}`}},
				}),
				schema.AssistantMessage("done"),
			})),
			agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(5))
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	// **业务把 sid 设成会话级稳定值**——godoc 邀请的用法
	const stableSid = "conv_alice"

	// 任务 A：扣 100，卡在人工确认
	_, errA := mk("100").Run(ctx, &agent_v2.RunSession{RunID: stableSid}, schema.UserMessage("扣 100"))
	if _, ok := compose_v2.IsInterrupt(errA); !ok {
		t.Fatalf("任务 A 应中断, got %v", errA)
	}
	if pays.Load() != 1 || lastAmt.Load() != "100" {
		t.Fatalf("前提不成立：A 应扣款 1 次 100，实际 %d 次 %v", pays.Load(), lastAmt.Load())
	}
	if cp, _ := cper.LoadNS(ctx, stableSid, ""); cp == nil || cp.Status != compose_v2.CPStatusPending {
		t.Fatal("前提不成立：A 应留下 Pending cp")
	}

	// 任务 B：**全新任务**（Run 不是 Resume），同 sid
	before := pays.Load()
	_, _ = mk("999").Run(ctx, &agent_v2.RunSession{RunID: stableSid}, schema.UserMessage("扣 999"))

	if got := pays.Load() - before; got != 1 {
		t.Errorf("❌ 新任务期间扣款发生 %d 次，应恰 1 次\n"+
			"  0 次 = B 捡起了 A 的挂起存档：B 的输入没进图、B 从 A 中断的位置续跑、\n"+
			"  还读到了 A 已完成的工具产出。业务以为扣了 999，实际一分没扣", got)
	}
	if lastAmt.Load() != "999" {
		t.Errorf("❌ 最后一次扣款金额 %v，应是 999——B 拿到的是 A 的产出", lastAmt.Load())
	}
}

// TestCrossTaskIsolation_NestedResumeStillWorks 反面：嵌套必须仍能 auto-load。
//
// 这条钉住判据的另一半。顶层无条件 fresh 时嵌套 HITL 会全断——父 Resume 时子 agent
// 靠「父重跑 → 子的 Run 被调 → 子 auto-load 自己的 cp」恢复，实测那样改
// nested_smoke 从 12/12 掉到 3/12。
func TestCrossTaskIsolation_NestedResumeStillWorks(t *testing.T) {
	var inner atomic.Int32
	askTool := tool.NewFuncTool("ask", "问", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "nested_q"); ok {
				inner.Add(1)
				return tool.Success("答:" + ans), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "nested_q", Question: "?"}}
		})
	tk := tool.NewToolkit()
	tk.Register(askTool)

	child := agent_v2.NewBasicAgent(
		agent_v2.WithName("child"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "tc_ask", Function: schema.FunctionCall{Name: "ask", Arguments: `{}`}}}),
			schema.AssistantMessage("子完成"),
		})),
		agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(5))

	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("outer")).
		AddStep("only", child)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := seq.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应中断, got %v", err)
	}
	if _, rerr := seq.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("yes")})); rerr != nil {
		t.Fatalf("嵌套 Resume 失败: %v", rerr)
	}
	if inner.Load() != 1 {
		t.Errorf("❌ 嵌套子 agent 的工具在 Resume 后没跑（%d 次）——顶层 fresh 判据误伤了嵌套。\n"+
			"  父 Resume 时子 agent 靠「父重跑 → 子的 Run 被调 → 子 auto-load 自己的 cp」恢复", inner.Load())
	}
}
