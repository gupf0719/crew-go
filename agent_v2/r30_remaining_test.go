package agent_v2_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// 本文件汇总 R30 audit 剩余 4 个 fix 的回归测试 + sanity-revert 验证：
//
//   - 视角 A HIGH-1: ReflectAgent eval 跨轮 NS round_<N>（evaluator/reflector 嵌套）
//   - 视角 A LOW-1: LoopAgent ExitCondition NS round_<N>
//   - 视角 C HIGH-1: commitFinal Memory.Write 拿 ResumeContext
//   - 视角 C MED-1: AutoCompact summarize strip ResumeContext

// TestReflectAgent_R30A_HIGH1_EvalCrossRoundNSDistinct 是 R30A HIGH-1 回归——
// 验证 ReflectAgent 跨多轮 evaluator/reflector ctx graphNS 含 round_<N>。
//
// **Bug 历史**：原 evalNodeFn 调 reflector(ctx, out) / evalFn(ctx, out) 不 push
// round NS——跨多轮 eval ctx graphNS 全相同 → evaluator/reflectorAgent 嵌套
// sub-agent + WithAskUser 跨轮 InterruptID 串台 → ResumeContext 误读上轮答案。
//
// **修法**：agent_v2/reflect_builder.go::evalNodeFn 在 reflector/evalFn 调用前
// push evalCtx = WithGraphNS(ctx, JoinNS(GetGraphNS(ctx), fmt.Sprintf("round_%d", round)))。
//
// **测试设计**：业务 evalFn 用 GetGraphNS 捕获真 NS——多轮调用应 NS 不同。
func TestReflectAgent_R30A_HIGH1_EvalCrossRoundNSDistinct(t *testing.T) {
	capturedNS := []string{}
	mu := sync.Mutex{}

	// actor: 多轮各返不同 content
	// (audit ISSUE-81 视角 A LOW-1 fix: 删除 dead actorCount — atomic.Int32 含 noCopy
	// 警示, `_ = actorCount` value copy 触发 vet warning)
	actorStub := newStubModel([]*schema.Message{
		schema.AssistantMessage("draft 1"),
		schema.AssistantMessage("draft 2"),
		schema.AssistantMessage("draft 3"),
	})
	actor := agent_v2.NewBasicAgent(
		agent_v2.WithName("actor"),
		agent_v2.WithModel(actorStub),
	)

	// EvalFunc: 捕获 ctx graphNS, 强制不通过让 N 轮跑
	evalFn := func(ctx context.Context, _ *schema.Message) (bool, error) {
		mu.Lock()
		ns := compose_v2.GetGraphNS(ctx)
		capturedNS = append(capturedNS, ns)
		mu.Unlock()
		// 第 3 次评估通过（避免无限循环）
		if len(capturedNS) >= 3 {
			return true, nil
		}
		return false, nil
	}

	r := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("review"),
		agent_v2.WithReflectMaxRounds(5),
		agent_v2.WithEvalFunc(evalFn),
	).WithActor(actor)

	_, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("draft a doc"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	mu.Lock()
	ns := append([]string(nil), capturedNS...)
	mu.Unlock()
	if len(ns) < 2 {
		t.Fatalf("evalFn 应被调 >= 2 次（多轮）；实际 %d 次 NS=%v", len(ns), ns)
	}

	// **核心验证**：跨轮 NS 应不同（含 round_<N>）
	// 修后：NS = "round_1" / "round_2" / "round_3"（每轮 round 不同）
	// 修前：NS = "" 跨多轮全相同
	for i := 0; i < len(ns)-1; i++ {
		if ns[i] == ns[i+1] {
			t.Errorf("R30A HIGH-1 回归：跨轮 evalFn ctx graphNS 应不同（含 round 序号）；"+
				"实际 round %d / round %d 同 NS=%q\n"+
				"sanity-revert 删 evalCtx round NS push → 跨轮 NS 复用 → 测试 fail",
				i, i+1, ns[i])
		}
	}
	// 验证 NS 含 round 字符串
	if !strings.Contains(ns[0], "round_") {
		t.Errorf("R30A HIGH-1 回归：NS 应含 'round_' 序号；实际 %q", ns[0])
	}
}

// TestLoopAgent_R30A_LOW1_ExitCondNSDistinct 是 R30A LOW-1 回归——
// 验证 LoopAgent ExitCondition ctx graphNS 跨轮含 round_<N>。
//
// **Bug 历史**：agent_v2/loop_builder.go::bodyFn 调 exitCond(ctx, out, nextRound)
// 不 push round NS——业务在 exitCondition 内 spawn sub-agent + ask_user 跨轮串台。
//
// **修法**：exitCtx = WithGraphNS(ctx, JoinNS(GetGraphNS(ctx), fmt.Sprintf("round_%d", round)))。
func TestLoopAgent_R30A_LOW1_ExitCondNSDistinct(t *testing.T) {
	capturedNS := []string{}
	mu := sync.Mutex{}

	body := agent_v2.NewBasicAgent(
		agent_v2.WithName("body"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("body 1"),
			schema.AssistantMessage("body 2"),
			schema.AssistantMessage("body 3"),
		})),
	)
	exitCond := func(ctx context.Context, _ *schema.Message, round int) (bool, error) {
		mu.Lock()
		ns := compose_v2.GetGraphNS(ctx)
		capturedNS = append(capturedNS, ns)
		mu.Unlock()
		// 第 3 轮退出
		return round >= 3, nil
	}

	loop := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("loop"),
		agent_v2.WithLoopMaxIterations(5),
		agent_v2.WithLoopExitCondition(exitCond),
	).WithBody(body)

	_, err := loop.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	mu.Lock()
	ns := append([]string(nil), capturedNS...)
	mu.Unlock()
	if len(ns) < 2 {
		t.Fatalf("exitCond 应被调 >= 2 次；实际 %d 次", len(ns))
	}

	for i := 0; i < len(ns)-1; i++ {
		if ns[i] == ns[i+1] {
			t.Errorf("R30A LOW-1 回归：跨轮 exitCond ctx graphNS 应不同（含 round 序号）；"+
				"实际 round %d / round %d 同 NS=%q\n"+
				"sanity-revert 删 exitCtx round NS push → 跨轮 NS 复用 → 测试 fail",
				i, i+1, ns[i])
		}
	}
}

// strippableModelStub 业务自定义 ChatModel——Generate 内调 GetResumeContext
// 检查 ResumeContext 是否被 strip。
type strippableModelStub struct {
	sawResumeContext atomic.Bool
	// calls 是**前置假设的载体**：本 stub 服务的那条用例只有否定断言（「没看到
	// ResumeContext」），而那句话在「摘要模型压根没跑」时恒真。这条用例已经被两种
	// 不同的机制各打死过一次（阈值成负数 / 成本守卫早退），两次都是「没跑」。
	calls atomic.Int64
}

func (m *strippableModelStub) Generate(ctx context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.calls.Add(1)
	if _, ok := compose_v2.GetResumeContext(ctx); ok {
		m.sawResumeContext.Store(true)
	}
	// 返一个长 message 让下次触发 AutoCompact summarize（足够长超过阈值）
	return schema.AssistantMessage(strings.Repeat("summary content ", 100)), nil
}
func (m *strippableModelStub) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *strippableModelStub) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

// TestAutoCompact_R30C_MED1_SummarizeStripResumeContext 是 R30C MED-1 回归——
// 验证 AutoCompact summarize Model 不可见 ResumeContext（避免业务自定义 Model
// 误读 sub-agent ask_user 答案）。
//
// **Bug 历史**：AutoCompact cfg.Model.Generate 用 ctx 含 ResumeContext → 业务自定义
// Model 内调 GetResumeAnswerSingle 误命中 sub-agent 准备的答案当摘要指令。
//
// **修法**：summarizeCtx := compose_v2.StripResumeContext(ctx) 隔离。
//
// **测试设计**：直接调 AutoCompactRun，构造业务 model 在 Generate 内 spy
// ResumeContext 是否可见。修前可见，修后不可见。
func TestAutoCompact_R30C_MED1_SummarizeStripResumeContext(t *testing.T) {
	spyModel := &strippableModelStub{}
	cfg := &agent_v2.AutoCompactConfig{
		Enabled: true,
		Model:   spyModel,
		// ⚠️ **这个数字是承重的**：阈值 = 窗口 - SummaryMaxTokens(20000) -
		// SummaryBufferTokens(13000)，**不是按比例算的**。原先写 1000 让阈值成了
		// **负数** ⟹ `shouldTrigger` 在 `threshold <= 0` 处直接返 false ⟹ 摘要模型
		// **一次都没跑**、spy 从不运行 ⟹ 这条用例恒真（实测：删掉被测的 Strip 照样 PASS）。
		ModelContextWindow: 33_100, // 阈值 = 33100 - 33000 = 100 token
		// ⚠️ **同一条用例被同一个形态打死了第二次**（2026-09-21）：加 MinCompactableTokens
		// 那道成本守卫之后，本用例的可压缩区只有 ~800 token < 默认 5000 ⟹ 守卫在调摘要
		// 之前就早退 ⟹ spy 又是一次都没跑 ⟹ 又恒真（实测：破坏 Strip 照样 PASS）。
		// 微缩窗口的用例必须显式调小它。
		MinCompactableTokens: 1,
	}

	// 构造 messages 超阈值（threshold = 100 token）
	msgs := []*schema.Message{
		schema.SystemMessage("system prompt"),
		schema.UserMessage(strings.Repeat("user content ", 100)),
		schema.AssistantMessage(strings.Repeat("asst reply ", 100)),
		schema.UserMessage("trigger compact"),
	}

	// 注入 ResumeContext 到 ctx 模拟 Resume 路径
	ctx := compose_v2.WithResumeContextValue(context.Background(),
		&compose_v2.ResumeContext{UserInputs: map[string]*schema.Message{
			"some_id": schema.UserMessage("user answer for sub-agent ask_user"),
		}})

	// 直接调 AutoCompactRun
	_, _ = agent_v2.AutoCompactRun(ctx, cfg, msgs)

	// **先查前置假设：摘要模型到底跑了没有**。不查的话下面那条否定断言在「压缩根本
	// 没发生」时恒真——这条用例正是这么死过两次的。
	if spyModel.calls.Load() == 0 {
		t.Fatal("前置假设不成立：摘要 Model 一次都没被调用 ⟹ 下面那条断言恒真、什么都没测。" +
			"多半是压缩没触发（阈值算错 / MinCompactableTokens 拦了 / early 区为空）")
	}

	if spyModel.sawResumeContext.Load() {
		t.Errorf("R30C MED-1 回归：AutoCompact summarize Model 不应看到 ResumeContext " +
			"（应被 StripResumeContext 隔离）；实际看到 → 业务自定义 Model 内 GetResumeAnswerSingle " +
			"会误命中 sub-agent ask_user 答案当摘要指令\n" +
			"sanity-revert 删 summarizeCtx StripResumeContext → 测试 fail")
	}
}
