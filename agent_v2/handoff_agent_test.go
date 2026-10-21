package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// makeAnnotatedMsg 给 stub 用——assistant message 带 handoff_target 注解。
func makeAnnotatedMsg(content, target string) *schema.Message {
	m := schema.AssistantMessage(content)
	if m.Annotations == nil {
		m.Annotations = map[string]any{}
	}
	if target != "" {
		m.Annotations[agent_v2.HandoffSignalKey] = target
	}
	return m
}

// TestHandoffAgent_BasicHandoff 验证 frontdesk 转交 billing 后 done。
func TestHandoffAgent_BasicHandoff(t *testing.T) {
	// 程序化成员经 HandoffTo 转交（annotation 通道已删，见 HandoffTo godoc）。
	frontdesk := newProgAgent("frontdesk", "forwarded to billing", "billing")
	billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			makeAnnotatedMsg("invoice generated", ""), // 不再 handoff = done
		})),
	)

	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("frontdesk"),
		agent_v2.WithHandoffMaxHops(5),
	).
		AddAgent("frontdesk", frontdesk).
		AddAgent("billing", billing)

	events, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("I want a refund"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "invoice") {
		t.Errorf("expect billing's 'invoice' (handoff done), got %v", final)
	}
}

// TestHandoffAgent_NoHandoffOnFirstStep 验证 frontdesk 不转交时立刻 done。
func TestHandoffAgent_NoHandoffOnFirstStep(t *testing.T) {
	frontdesk := agent_v2.NewBasicAgent(
		agent_v2.WithName("frontdesk"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("answered directly"), // 无 handoff annotation
		})),
	)
	billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("never")})),
	)

	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("frontdesk"),
	).
		AddAgent("frontdesk", frontdesk).
		AddAgent("billing", billing)

	events, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "answered directly") {
		t.Errorf("expect first-step done with 'answered directly', got %v", final)
	}
}

// TestHandoffAgent_MaxHopsExhausted 验证无限 ping-pong 时按 maxHops 退出。
func TestHandoffAgent_MaxHopsExhausted(t *testing.T) {
	// a 永远转 b；b 永远转 a → 死循环 except maxHops。
	// （原先靠产出消息的 annotation ping-pong，那条通道已删——转交一次都不会发生，
	//  maxHops 根本到不了，而唯一的断言 `final != nil` 恒真。）
	a := newProgAgent("a", "ar", "b")
	b := newProgAgent("b", "br", "a")

	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("a"),
		agent_v2.WithHandoffMaxHops(3),
	).AddAgent("a", a).AddAgent("b", b)

	sess := agent_v2.NewRunSession()
	events, err := h.Run(context.Background(), sess, schema.UserMessage("start"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// **承重的量是「真的按 maxHops 退出了」**，不是「final 非空」——后者在
	// 「转交一次没发生、第一跳就收尾」时同样为真。
	st := h.State(sess)
	if !st.Exhausted {
		t.Fatalf("应按 maxHops 退出，实际 state=%+v（转交没发生的话这个用例什么也没测）", st)
	}
	if st.Hops != 3 {
		t.Errorf("应恰好跑满 maxHops=3 跳，实际 %d", st.Hops)
	}
	if agent_v2.FinalMessage(events) == nil {
		t.Error("耗尽后仍应有产出")
	}
}

// TestHandoffAgent_NoInitialFails 验证 initial 必填。
func TestHandoffAgent_NoInitialFails(t *testing.T) {
	h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("router"))
	_, err := h.Run(context.Background(), nil, schema.UserMessage("hi"))
	if err == nil || !strings.Contains(err.Error(), "initial required") {
		t.Fatalf("expect 'initial required' err, got %v", err)
	}
}

// TestHandoffAgent_InvalidInitialFails 验证 initial 必须是已注册的 agent。
func TestHandoffAgent_InvalidInitialFails(t *testing.T) {
	a := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("ok")})),
	)
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("ghost"), // 未注册
	).AddAgent("a", a)
	_, err := h.Run(context.Background(), nil, schema.UserMessage("hi"))
	if err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("expect 'not registered' err, got %v", err)
	}
}

// TestHandoff_R30D_MED1_AddAgentValidatesNS 是 R30D MED-1 回归——
// 验证 AddAgent 的 name 含 NS 非法字符（"/" 等）时 panic-fast，避免 stepFn
// JoinNS 拼出错位 NS 污染 cp 寻址。
//
// **Bug 历史**：原 AddAgent 不校验 name → handoff.AddAgent("front/desk", ag)
// 注册成功 → stepFn line 357 JoinNS(parentNS, "agent_front/desk") 错位多出
// 一层段 → 下游 SplitNS / IsNSInSubtree 错乱 + skill fork 递归保护降级。
//
// **修法**：AddAgent 入口 ValidateNodeIDForNS panic-fast，对照 RoutingAgentBuilder.AddBranch。
func TestHandoff_R30D_MED1_AddAgentValidatesNS(t *testing.T) {
	a := agent_v2.NewBasicAgent(
		agent_v2.WithName("dummy"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("ok")})),
	)
	h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("router"))

	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("R30D MED-1 回归：AddAgent name 含 '/' 应 panic-fast；实际未 panic\n" +
				"sanity-revert 删 ValidateNodeIDForNS 校验 → 测试 fail（注册成功不 panic）")
		}
		msg, ok := r.(string)
		if !ok {
			t.Errorf("panic value 应为 string，实际 %T", r)
		}
		if !strings.Contains(msg, "invalid for ns") {
			t.Errorf("panic 信息应含 'invalid for ns'；实际 %q", msg)
		}
	}()

	h.AddAgent("front/desk", a) // 含 '/' 非法
}

// TestHandoff_R30B_HIGH1_CrossHopResumeContextNoLeak 是 R30B HIGH-1 回归——
// 验证多 hop 同 active agent (frontdesk → frontdesk 自跳) 时 framework 自动算
// InterruptID 跨 hop 不串台（childNS 加 hop 序号让 5 维 graphNS 不同）。
//
// **Bug 历史**：原 childNS = "agent_<name>" 跨 hop 复用 → ask_user_tool 自动算
// stableID = MakeInterruptID(graphNS, ...) 全相同 → hop 2 ask_user 沉默命中
// hop 1 答案。
//
// **修法**：childNS 加 hop 序号 fmt.Sprintf("agent_%s_hop%d", name, currentHops)。
//
// **测试设计避坑**（5 类炸弹 #1 过滤后元素恰好原位）：
//   - **不能业务自填 InterruptID**——业务自填的话 stub 自己用 closure counter 区分
//     hop 自然产生不同 ID，framework NS 隔离与否对 ID 无影响 → 测试假绿
//   - **必须用 framework 自动算 ID 协议**：业务 stub 不填 InterruptID（让 framework
//     按 5 维 graphNS+nodeID+visit+branchIdx+idx 自动算），让 framework NS 决定 ID
//     是否冲突——这样 sanity-revert 删 hop 序号后 hop 2 ID 真等于 hop 1 ID
func TestHandoff_R30B_HIGH1_CrossHopResumeContextNoLeak(t *testing.T) {
	// stub: 用 GetGraphNS(ctx) 捕获真 framework push 的 NS（让 sanity-revert 真 catch bug）
	// hop 0+1 自跳成功，hop 2 done，stub 记录每次进入时的 graphNS
	capturedNS := []string{}
	mu := sync.Mutex{}
	// 程序化成员：每跳捕获框架 push 的 graphNS，前两跳经 HandoffTo 自跳，第三跳收尾。
	// （原先靠产出消息的 annotation 自跳，那条通道已删——见 agent_v2.HandoffTo）
	ag := newProgAgent("frontdesk", "done", "")
	ag.onRun = func(ctx context.Context, _ int) string {
		mu.Lock()
		capturedNS = append(capturedNS, compose_v2.GetGraphNS(ctx))
		mu.Unlock()
		if agent_v2.GetHandoffHops(ctx) < 2 {
			return "frontdesk" // 自跳
		}
		return ""
	}
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("frontdesk"),
		agent_v2.WithHandoffMaxHops(5),
	).AddAgent("frontdesk", ag)

	_, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	mu.Lock()
	ns := append([]string(nil), capturedNS...)
	mu.Unlock()
	if len(ns) < 3 {
		t.Fatalf("stub 应被调 3 次（hop 0+1 自跳 + hop 2 done）；实际 %d 次 NS=%v", len(ns), ns)
	}

	// **核心验证**：hop 0/1/2 的 NS 应**全不同**（含 hop 序号）
	// 修后：NS = "agent_frontdesk_hop0" / "agent_frontdesk_hop1" / "agent_frontdesk_hop2"
	// 修前：NS = "agent_frontdesk" 跨多 hop 全相同 → ask_user_tool 自动算 ID 全同 → 串台
	if ns[0] == ns[1] || ns[1] == ns[2] {
		t.Errorf("R30B HIGH-1 回归：3 hop 的 graphNS 应全不同（含 hop 序号）；"+
			"实际 hop0=%q hop1=%q hop2=%q\n"+
			"sanity-revert 删 childNS hop 序号 → NS 跨 hop 复用 → 测试 fail",
			ns[0], ns[1], ns[2])
	}
	// 验证 NS 含 hop 序号字符串
	if !strings.Contains(ns[0], "hop0") || !strings.Contains(ns[1], "hop1") {
		t.Errorf("R30B HIGH-1 回归：NS 应含 'hop<N>' 序号；实际 hop0=%q hop1=%q", ns[0], ns[1])
	}
}

// TestHandoff_R30B_HIGH2_StateHopsResumeCorrect 是 R30B HIGH-2 + MED-1 回归——
// 验证 Resume 后 HandoffAgent.State().Hops 反映真实总 hop 数（不被闭包 counter 重置）。
//
// **Bug 历史**：原 hopsCounter 是 buildComposeGraph 内闭包局部变量，Resume 时
// 新 buildComposeGraph 让 counter=0 → State().Hops 计数错。
//
// **修法**：删除 hopsCounter，stepFn 通过 agent_v2.GetHandoffHops(ctx) 读真值。
func TestHandoff_R30B_HIGH2_StateHopsResumeCorrect(t *testing.T) {
	// stub: 用 ctx GetHandoffHops 区分 hop（跨 Resume 真值持久化），不依赖 closure counter
	// hop 0+1 都自跳 frontdesk，hop 2 抛 ask_user → Resume 后 hop 2 完成 done
	stub := func(ctx context.Context, _ []*schema.Message) (*schema.Message, error) {
		hops := agent_v2.GetHandoffHops(ctx)
		if hops == 2 {
			// hop 2 抛 ask_user
			if _, ok := compose_v2.GetResumeAnswerText(ctx, "hop2_ask"); ok {
				return schema.AssistantMessage("hop 2 done"), nil
			}
			return nil, &compose_v2.InterruptError{
				Reason: "ask hop 2",
				Query:  &compose_v2.AskQuery{InterruptID: "hop2_ask", Question: "Q2"},
			}
		}
		// hop 0+1 自跳——走内置 transfer 工具（annotation 通道已删）。
		// tool_call ID 按 hop 区分：同 ID 会撞上重放记账的键。
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: fmt.Sprintf("tc_hop%d", hops), Type: "function",
			Function: schema.FunctionCall{
				Name:      agent_v2.HandoffTransferToolName,
				Arguments: `{"agent_name":"frontdesk"}`,
			},
		}}), nil
	}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("frontdesk"),
		agent_v2.WithModel(&fnModel{fn: stub}),
		agent_v2.WithHandoffTransfer(),
	)
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("frontdesk"),
		agent_v2.WithHandoffMaxHops(10),
	).AddAgent("frontdesk", ag)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// Run 1: hop 0+1 自跳成功，hop 2 中断
	_, err := h.Run(ctx, sess, schema.UserMessage("hi"))
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("Run 1 应抛 InterruptError；实际 %v", err)
	}

	// Resume: 答 hop 2 → hop 2 完成 done
	_, err = h.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			"hop2_ask": schema.UserMessage("done"),
		}),
		agent_v2.WithResumeSession(sess),
	)
	if err != nil {
		t.Fatalf("Resume err: %v", err)
	}

	// **核心验证**：Resume 后 State().Hops 应是 3（hop 0+1+2 全成功，每个 hop 完成后 +1）
	// 修前：Resume 走新 buildComposeGraph → 新 closure hopsCounter=0 → hop 2 完成
	//       后 counter=1 → State().Hops=1（错误！应是 3）
	// 修后：从 ctx 读真值 (compose_v2 stepNode push 真 hopsCh) → State().Hops=3
	state := h.State(sess)
	if state.Hops != 3 {
		t.Errorf("R30B HIGH-2 回归：Resume 后 State().Hops 应是 3（hop 0+1+2 全成功）；"+
			"实际 %d\n"+
			"sanity-revert 还原 hopsCounter 闭包局部变量 → Resume 时重置为 0 → "+
			"hop 2 完成后 Hops=1 → 测试 fail",
			state.Hops)
	}
	if !state.Done {
		t.Errorf("R30B HIGH-2 回归：State().Done 应 true（hop 2 没 handoff = done）；实际 %+v", state)
	}
}
