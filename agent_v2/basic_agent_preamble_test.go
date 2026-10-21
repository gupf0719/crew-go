package agent_v2_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// **ISSUE-74 修**：BasicAgentBuilder.WithPreamble 引入 graph 首段 preamble 节点，
// 业务在节点内调 store.Search / HistoryCompactor / Skill 预激活——抛 InterruptError
// 走 graph 内 SaveNS 协议自动存 cp，Resume 自然支持（修复 Run 入口同步调用抛
// Interrupt 时 cp 不存的 HITL 链断 bug）。
//
// 本文件 3 个测试覆盖 compose_v2 引擎层 preamble 协议：
//   - Success：preamble 跑成功 → messages 写 channel → model 节点看完整视图
//   - InterruptResume：preamble 抛 Interrupt → cp 存 → Resume 重跑 preamble 拿答案续跑
//   - NoFn_BackwardCompat：不传 preamble 时拓扑保持现状不破

func init() {
	compose_v2.RegisterValueType[*agent_v2.BasicAgentPreambleInput]("*agent_v2.BasicAgentPreambleInput")
}

// TestBasicAgentBuilder_Preamble_Success 验证 preamble 节点成功路径——业务回调
// 拼装完整初始 messages（system + memory + user input）写入 channel，model 节点
// 入口 Get 看到完整视图。
func TestBasicAgentBuilder_Preamble_Success(t *testing.T) {
	preambleCalls := atomic.Int32{}
	preambleFn := func(_ context.Context, in *agent_v2.BasicAgentPreambleInput) (*agent_v2.BasicAgentPreambleOutput, error) {
		preambleCalls.Add(1)
		// mock 业务拼装：sysPrompt + memory + user input
		//
		// 全部走 Messages（= 进被压缩的消息流）是**故意**的：本用例钉的是「model 节点
		// Get 看到 preamble 写进 channel 的完整视图」，走 System 那半就绕开了 channel、
		// 测的就不是这件事了。
		return &agent_v2.BasicAgentPreambleOutput{Messages: []*schema.Message{
			schema.SystemMessage("you are helpful"),
			schema.AssistantMessage("[memory] previous context"),
			in.UserInput,
		}}, nil
	}

	modelCalledWith := atomic.Pointer[[]*schema.Message]{}
	modelFn := func(_ context.Context, _, msgs []*schema.Message) (*schema.Message, error) {
		clone := make([]*schema.Message, len(msgs))
		copy(clone, msgs)
		modelCalledWith.Store(&clone)
		return schema.AssistantMessage("ok"), nil
	}

	g, err := agent_v2.NewBasicAgentBuilder(modelFn, nil).WithPreamble(preambleFn).Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentPreambleInputWrites(
			&agent_v2.BasicAgentPreambleInput{UserInput: schema.UserMessage("hi")},
		)...),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := preambleCalls.Load(); got != 1 {
		t.Errorf("preamble 应跑 1 次，实际 %d", got)
	}
	got := modelCalledWith.Load()
	if got == nil {
		t.Fatal("model 节点应被调")
	}
	msgs := *got
	if len(msgs) != 3 {
		t.Fatalf("model 应看到 3 条 enriched messages（sys + memory + user），实际 %d: %+v", len(msgs), msgs)
	}
	if msgs[0].Role != schema.RoleSystem || !strings.Contains(msgs[0].Content, "helpful") {
		t.Errorf("第 1 条应是 SystemMessage，实际 role=%v content=%q", msgs[0].Role, msgs[0].Content)
	}
	if msgs[2].Role != schema.RoleUser || msgs[2].Content != "hi" {
		t.Errorf("第 3 条应是 UserMessage hi，实际 role=%v content=%q", msgs[2].Role, msgs[2].Content)
	}
}

// TestBasicAgentBuilder_Preamble_InterruptResume 是 ISSUE-74 修复核心回归——
// 验证 preamble 节点抛 InterruptError 时框架自动 SaveNS cp，Resume 重跑 preamble
// 时业务回调通过 ResumeContext 拿到答案续跑成功。
//
// **bug 历史**：Run 入口同步调 buildInitialMessages 抛 Interrupt → cp 不存 → caller
// 调 Resume 拿不到 cp → HITL 链断。方案 A 把业务回调放进 graph 首段 preamble 节点，
// 走 graph 内 SaveNS 协议自然支持 Resume。
func TestBasicAgentBuilder_Preamble_InterruptResume(t *testing.T) {
	preambleCalls := atomic.Int32{}
	const askID = "preamble_approval"

	preambleFn := func(ctx context.Context, in *agent_v2.BasicAgentPreambleInput) (*agent_v2.BasicAgentPreambleOutput, error) {
		preambleCalls.Add(1)
		// 检查是否已有 Resume 答案
		if ans, ok := compose_v2.GetResumeAnswerText(ctx, askID); ok {
			// 同上一条用例：全部走 Messages，让下面 modelFn 的 msgs[0] 断言仍钉在
			// 「preamble 写进 channel 的东西」上。
			return &agent_v2.BasicAgentPreambleOutput{Messages: []*schema.Message{
				schema.SystemMessage("approved: " + ans),
				in.UserInput,
			}}, nil
		}
		// 首次：抛 InterruptError mock"读 memory 前用户授权"业务场景
		return nil, &compose_v2.InterruptError{
			Reason: "approve_memory_read",
			Query: &compose_v2.AskQuery{
				InterruptID: askID,
				Question:    "approve memory read?",
			},
		}
	}

	modelFn := func(_ context.Context, _, msgs []*schema.Message) (*schema.Message, error) {
		// 验证 model 看到 enriched messages
		if len(msgs) > 0 && msgs[0].Role == schema.RoleSystem && strings.Contains(msgs[0].Content, "approved: yes") {
			return schema.AssistantMessage("done"), nil
		}
		return schema.AssistantMessage("no_preamble"), nil
	}

	makeGraph := func() *compose_v2.Graph {
		g, err := agent_v2.NewBasicAgentBuilder(modelFn, nil).WithPreamble(preambleFn).Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		return g
	}

	// === Run 1：preamble 抛 InterruptError ===
	g1 := makeGraph()
	cm1 := compose_v2.NewChannelManager()
	runErr := compose_v2.Run(context.Background(), g1, cm1,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentPreambleInputWrites(
			&agent_v2.BasicAgentPreambleInput{UserInput: schema.UserMessage("hi")},
		)...),
	)
	if runErr == nil {
		t.Fatal("Run 1 应被 InterruptError 中断")
	}
	ie, isInt := compose_v2.IsInterrupt(runErr)
	if !isInt {
		t.Fatalf("应是 InterruptError，实际 %v", runErr)
	}
	if ie.Query == nil || ie.Query.InterruptID != askID {
		t.Fatalf("Query 应是 %q，实际 %+v", askID, ie.Query)
	}

	// 验证 cm 自动记录 pending interrupt（preamble 节点抛 → graph_loop 自动 record）
	pending := cm1.PendingInterrupts()
	if len(pending) != 1 || pending[0].InterruptID != askID {
		t.Fatalf("cm 应记录 1 个 %s pending，实际 %+v", askID, pending)
	}

	// 验证 frontier 是 [preamble]——Resume 重跑 preamble
	snap, err := cm1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap.CurrentFrontier) != 1 || snap.CurrentFrontier[0].NodeID != "preamble" {
		t.Fatalf("frontier 应是 [preamble]（重跑），实际 %v", snap.CurrentFrontier)
	}

	if got := preambleCalls.Load(); got != 1 {
		t.Errorf("Run 1 后 preamble 应跑 1 次，实际 %d", got)
	}

	// === Run 2：注入答案 Resume ===
	// 不需手动 cm.Restore——Run 内部 register 完所有 channel 后自动 Restore（graph_loop.go:342-346）
	g2 := makeGraph()
	cm2 := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g2, cm2,
		compose_v2.WithResumeFrom(snap),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			askID: schema.UserMessage("yes"),
		}),
	); err != nil {
		t.Fatalf("Run 2 应成功，实际 %v", err)
	}

	if got := preambleCalls.Load(); got != 2 {
		t.Errorf("Resume 后 preamble 应再跑 1 次（共 2 次：Run 1 失败 + Resume 重跑成功），实际 %d", got)
	}

	final := agent_v2.BasicAgentFinalMessage(cm2.Channels())
	if final == nil || final.Content != "done" {
		t.Fatalf("final 应是 'done'（model 看到 enriched messages 含 approved），实际 %v", final)
	}

	// pending 应清空
	if len(cm2.PendingInterrupts()) != 0 {
		t.Errorf("Resume 后 pending interrupts 应清空，实际 %+v", cm2.PendingInterrupts())
	}
}

// TestBasicAgentBuilder_Preamble_NoFn_BackwardCompat 验证不传 preamble 时拓扑
// 保持现状 `StartNode → model → ...`——已有 caller（agent_v2/basic_agent.go 改造前）
// 不破。
func TestBasicAgentBuilder_Preamble_NoFn_BackwardCompat(t *testing.T) {
	called := atomic.Int32{}
	modelFn := func(_ context.Context, _, _ []*schema.Message) (*schema.Message, error) {
		called.Add(1)
		return schema.AssistantMessage("ok"), nil
	}

	// 不调 WithPreamble——验证拓扑保持 StartNode → model
	g, err := agent_v2.NewBasicAgentBuilder(modelFn, nil).Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentInputWrites(schema.UserMessage("hi"))...),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := called.Load(); got != 1 {
		t.Errorf("model 应跑 1 次（无 preamble），实际 %d", got)
	}
}
