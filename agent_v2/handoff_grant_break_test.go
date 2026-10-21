package agent_v2_test

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"unsafe"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// ═══════════════════════════════════════════════════════════════════════════
// 打破「带受让方身份的授予」（grant）这个方案本身。
//
// 断言仍然一律是 **billing 的模型被真调了几次**——跟 zz_break_test.go 同一把尺子。
// ═══════════════════════════════════════════════════════════════════════════

// grantInnerTransfer 造一个「第一轮发 transfer、第二轮自答」的 BasicAgent。
//
// **两把闸都得开**：上层授予（grant）+ 本 agent 自己 `WithHandoffTransfer()` opt-in，
// 缺任一把都不注册 transfer 工具（`basic_agent.go` 的 buildComposeGraph）。所以这里
// 两样都给——本文件测的是**授予**那一把，opt-in 必须开着。
//
// **摘掉 opt-in 的实际后果**（独立审计逐条数过，别再凭感觉说「一整批假绿」）：
// 24 条用例仍然 24/24 通过，但其中——
//   - **2 条的硬断言变成恒真**（B2 / B7b，两条都是「billing 必须为 0」的否定式断言，
//     另一把闸关着时它们自动成立）⟹ 这两条是字面意义上的假绿
//   - **4 条只有 t.Logf、结论静默翻面**（B3b / B6 / B7 / B8h，计数 1→0）。B3b 尤其
//     刺眼：基线打印「**穿了**」（那个洞是真的），摘掉 opt-in 后打印「没穿」——
//     一条用例的结论反了而没有任何东西变红
//   - 其余 18 条本来就不依赖这个 helper，不受影响
//
// 所以准确的说法是「**2 条恒真 + 4 条结论翻面**」。那 4 条是**有意的观察式探针**
// （B3b 记录的是一个已知开着的洞，给它加断言等于把「洞存在」钉成保证），不是漏写断言
// ——区别只在于：观察式探针的结论翻面时没人会知道。
//
// ⚠️ 这里曾写着「不再配 WithHandoffTransfer（它已退役成 no-op）」，而同一个函数体第 10 行
// 就在调它——那句话是实验期草稿的残留，错的方向还特别糟（会让人以为可以删掉一把真闸）。
// 独立调研实测撞出来的：不配 opt-in 时 billing 模型被调 0 次。
func grantInnerTransfer(name, target string) (agent_v2.Agent, *brkStub) {
	m := newBrkStub(
		brkTransferCall("t1", target),
		schema.AssistantMessage(name+" 自己答了"),
	)
	return agent_v2.NewBasicAgent(
		agent_v2.WithName(name),
		agent_v2.WithModel(m),
		agent_v2.WithHandoffTransfer(), // base 对照列：base 里这才是开关
		agent_v2.WithMaxRounds(4),
	), m
}

// ── B1/B2：业务壳把 opts 原样往下转发（「礼貌透传」）──

type grantForwardShell struct {
	name  string
	inner agent_v2.Agent
	runs  int
}

func (w *grantForwardShell) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	w.runs++
	// **礼貌透传**：业务写壳时最自然的一行。
	evs, err := w.inner.Run(ctx, sess, in, opts...)
	if err != nil {
		return nil, err
	}
	f := agent_v2.FinalMessage(evs)
	if f == nil {
		f = schema.AssistantMessage("")
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, f)}, nil
}
func (w *grantForwardShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w *grantForwardShell) Name() string        { return w.name }
func (w *grantForwardShell) Description() string { return "原样转发 opts 的业务壳" }

func runForwardShellCase(t *testing.T, shellName, innerName string) (billing, inner int) {
	t.Helper()
	in, innerModel := grantInnerTransfer(innerName, "billing")
	front := &grantForwardShell{name: shellName, inner: in}
	bill, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", bill)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if front.runs == 0 {
		t.Fatalf("fixture 没搭对：壳压根没被调")
	}
	return billingModel.count(), innerModel.count()
}

// B1：壳与内层**不同名**，opts 原样转发。
func TestGrantBreak_B1_ShellForwardsOpts(t *testing.T) {
	b, i := runForwardShellCase(t, "front", "inner")
	t.Logf("B1 壳转发 opts（壳=front / 内层=inner）：billing=%d 内层模型=%d", b, i)
	if b > 0 {
		t.Logf("B1 结论：**穿了**")
	} else {
		t.Logf("B1 结论：没穿——受让方是壳，收到的是内层，对象不等")
	}
}

// B2：壳与内层**同名**（都叫 front）——名字方案在这条上必穿，指针方案呢？
func TestGrantBreak_B2_ShellForwardsOptsSameName(t *testing.T) {
	b, i := runForwardShellCase(t, "front", "front")
	t.Logf("B2 壳转发 opts 且**同名**（壳=front / 内层也叫 front）：billing=%d 内层模型=%d", b, i)
	// **承重断言**：身份是对象不是名字。红了说明有人把身份维换回了名字——
	// 那样壳跟内层取同名就能穿（名字方案正是在这条上必失守）。
	if b != 0 {
		t.Errorf("B2 同名下被动转发穿透：billing 被真调 %d 次，期望 0", b)
	}
}

// ── B3：业务能不能自己造一个 grant ──

// B3a：**编译期**。RunOption 是 `func(*runConfig)`，runConfig 不导出——包外写不出
// 这个函数字面量。本用例只能记录「写不出来」这件事本身（写得出来就编译不过了）。
// 真正可执行的绕法在 B3b。
func TestGrantBreak_B3a_CannotWriteRunOptionLiteral(t *testing.T) {
	// 下面这行若解注释会编译失败：`undefined: agent_v2.runConfig`
	//   var _ agent_v2.RunOption = func(c *agent_v2.runConfig) { c.handoffGrant = ... }
	// 用 reflect 把 RunOption 的入参类型打出来当证据。
	rt := reflect.TypeOf(agent_v2.WithRunMaxRounds(1))
	t.Logf("B3a RunOption 签名 = %v，入参 = %v（包外不可名 ⟹ 写不出字面量）", rt, rt.In(0))
	if rt.In(0).Elem().PkgPath() == "" {
		t.Fatalf("fixture 没搭对：入参不是包内未导出类型")
	}
}

// B3b：**reflect + unsafe**。绕过「不导出」的标准手法：MakeFunc 造出 RunOption，
// 在里面用 unsafe 写 runConfig 的未导出字段。受让方指向内层。
func TestGrantBreak_B3b_ForgeGrantViaUnsafe(t *testing.T) {
	inner, innerModel := grantInnerTransfer("inner", "billing")
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")

	forged := forgeGrantOption(t, inner, map[string]bool{"front": true, "billing": true})
	if forged == nil {
		t.Logf("B3b 结论：伪造失败——业务造不出 grant")
		return
	}
	front := &grantOptInjectShell{name: "front", inner: inner, extra: forged}
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("B3b unsafe 伪造 grant：billing=%d 内层模型=%d", billingModel.count(), innerModel.count())
	if billingModel.count() > 0 {
		t.Logf("B3b 结论：**穿了**——reflect+unsafe 能伪造出受让方指向任意对象的 grant")
	} else {
		t.Logf("B3b 结论：没穿")
	}
}

// grantOptInjectShell 是业务壳：调内层时**额外塞一个自己造的 RunOption**。
type grantOptInjectShell struct {
	name  string
	inner agent_v2.Agent
	extra agent_v2.RunOption
}

func (w *grantOptInjectShell) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	evs, err := w.inner.Run(ctx, sess, in, w.extra)
	if err != nil {
		return nil, err
	}
	f := agent_v2.FinalMessage(evs)
	if f == nil {
		f = schema.AssistantMessage("")
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, f)}, nil
}
func (w *grantOptInjectShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w *grantOptInjectShell) Name() string        { return w.name }
func (w *grantOptInjectShell) Description() string { return "注入伪造 option 的业务壳" }

// forgeGrantOption 用 reflect+unsafe 造一个「把 handoffGrant 字段写成 {grantee, candidates}」
// 的 RunOption。造不出来返 nil（并把卡在哪儿 log 出来）。
func forgeGrantOption(t *testing.T, grantee agent_v2.Agent, cands map[string]bool) (out agent_v2.RunOption) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Logf("B3b 伪造中途 panic：%v", r)
			out = nil
		}
	}()
	optType := reflect.TypeOf(agent_v2.WithRunMaxRounds(1)) // func(*runConfig)
	cfgType := optType.In(0).Elem()                         // runConfig
	gf, ok := cfgType.FieldByName("handoffGrant")
	if !ok {
		t.Logf("B3b 卡在：runConfig 没有 handoffGrant 字段")
		return nil
	}
	grantType := gf.Type.Elem() // handoffGrant
	// 造一个 *handoffGrant 并写它的两个未导出字段
	gv := reflect.New(grantType) // *handoffGrant，可寻址
	setUnexported := func(v reflect.Value, name string, val any) bool {
		f := v.Elem().FieldByName(name)
		if !f.IsValid() {
			t.Logf("B3b 卡在：handoffGrant 没有 %s 字段", name)
			return false
		}
		w := reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem()
		w.Set(reflect.ValueOf(val))
		return true
	}
	if !setUnexported(gv, "grantee", any(grantee)) {
		return nil
	}
	if !setUnexported(gv, "candidates", cands) {
		return nil
	}
	fn := reflect.MakeFunc(optType, func(args []reflect.Value) []reflect.Value {
		cf := args[0].Elem().FieldByName("handoffGrant")
		reflect.NewAt(cf.Type(), unsafe.Pointer(cf.UnsafeAddr())).Elem().Set(gv)
		return nil
	})
	o, ok := fn.Interface().(agent_v2.RunOption)
	if !ok {
		t.Logf("B3b 卡在：MakeFunc 的结果断言不成 RunOption")
		return nil
	}
	return o
}

// ── B4：业务自写编排的成员能不能转交（「重新授予」这个动作业务拿不拿得到）──

// grantBizOrchestration 是业务自己写的两步编排——它想让 step2 有转交能力。
// 它手上只有 RunSubAgent（收 RunOption）和 sub.Run，**造不出 grant**。
type grantBizOrchestration struct {
	name  string
	step  agent_v2.Agent
	viaHi bool // true = 走 RunSubAgent helper，false = 直接 sub.Run(ctx,...)
}

func (o *grantBizOrchestration) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	var evs []*agent_v2.AgentEvent
	var err error
	if o.viaHi {
		evs, err = agent_v2.RunSubAgent(ctx, "step", o.step, in)
	} else {
		evs, err = o.step.Run(ctx, sess, in)
	}
	if err != nil {
		return nil, err
	}
	f := agent_v2.FinalMessage(evs)
	if f == nil {
		f = schema.AssistantMessage("")
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(o.name, f)}, nil
}
func (o *grantBizOrchestration) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (o *grantBizOrchestration) Name() string        { return o.name }
func (o *grantBizOrchestration) Description() string { return "业务自写编排" }

func TestGrantBreak_B4_BizOrchestrationMemberCannotTransfer(t *testing.T) {
	for _, viaHelper := range []bool{false, true} {
		inner, innerModel := grantInnerTransfer("step2", "billing")
		front := &grantBizOrchestration{name: "front", step: inner, viaHi: viaHelper}
		billing, billingModel := brkLeaf("billing", "BILLING 接手了")
		h := agent_v2.NewHandoffAgent(
			agent_v2.WithHandoffName("router"),
			agent_v2.WithHandoffInitial("front"),
		).AddAgent("front", front).AddAgent("billing", billing)
		if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
			t.Fatalf("viaHelper=%v Run: %v", viaHelper, err)
		}
		t.Logf("B4 业务自写编排（viaRunSubAgent=%v）：billing=%d 成员模型=%d",
			viaHelper, billingModel.count(), innerModel.count())
	}
	t.Logf("B4 说明：billing=0 = 业务自写编排的成员**永远转不了**（能力回退）；billing>0 = 业务能模仿重新授予（穿透面）")
}

// ── B5：Resume 那条路上 grant 还在吗 ──

// B5a：候选中断 → HandoffAgent.Resume → 转交还在不在（E14 的兄弟，但这里候选是
// **多层**：Handoff → Sequential → BasicAgent(中断后转交)）。
func TestGrantBreak_B5a_GrantSurvivesResumeThroughComposite(t *testing.T) {
	ask := &brkAskOnce{}
	tk := tool.NewToolkit()
	tk.Register(brkAskTool(ask))
	frontModel := newBrkStub(
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "a1", Type: "function",
			Function: schema.FunctionCall{Name: "need_confirm", Arguments: `{}`},
		}}),
		brkTransferCall("t1", "billing"),
	)
	leaf := agent_v2.NewBasicAgent(
		agent_v2.WithName("leaf"),
		agent_v2.WithModel(frontModel),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(5),
	)
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("front")).AddStep("s1", leaf)
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", seq).AddAgent("billing", billing)

	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()
	_, err := h.Run(ctx, sess, schema.UserMessage("hi"))
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("fixture 没搭对：第一次 Run 应中断，实际 err=%v", err)
	}
	if _, err := h.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("确认"),
		})); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	t.Logf("B5a Handoff→Sequential→BasicAgent 跨 HITL Resume：leaf 模型=%d billing=%d",
		frontModel.count(), billingModel.count())
}

// B5b：**直接 Resume 一个 BasicAgent**（不经 HandoffAgent）——它中断前是被授予的。
// 漏授予的话是静默不转交还是有信号？
func TestGrantBreak_B5b_DirectResumeOnCandidate(t *testing.T) {
	ask := &brkAskOnce{}
	tk := tool.NewToolkit()
	tk.Register(brkAskTool(ask))
	frontModel := newBrkStub(
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "a1", Type: "function",
			Function: schema.FunctionCall{Name: "need_confirm", Arguments: `{}`},
		}}),
		brkTransferCall("t1", "billing"),
		schema.AssistantMessage("FRONT 只好自己答"),
	)
	front := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithModel(frontModel),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5),
	)
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)

	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()
	_, err := h.Run(ctx, sess, schema.UserMessage("hi"))
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("fixture 没搭对：第一次 Run 应中断，实际 err=%v", err)
	}
	// **业务在这里搞错了对象**：拿着 sid 去 Resume 那个候选本身，而不是 HandoffAgent。
	evs, rerr := front.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("确认"),
		}))
	var final string
	if f := agent_v2.FinalMessage(evs); f != nil {
		final = f.Content
	}
	t.Logf("B5b 直接 Resume 候选本身：err=%v final=%q front 模型=%d billing=%d",
		rerr, final, frontModel.count(), billingModel.count())
	t.Logf("B5b 说明：这条路上没有 grant——看它是报错（有信号）还是静默把转交吞了")
}

// ── B6：嵌套 HandoffAgent ──

func TestGrantBreak_B6_NestedHandoff(t *testing.T) {
	// 内层 Handoff：innerFront 转交给 innerBilling
	innerFront, innerFrontModel := grantInnerTransfer("inner_front", "inner_billing")
	innerBilling, innerBillingModel := brkLeaf("inner_billing", "INNER-BILLING 接手了")
	innerH := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("front"), // 它是外层的候选 front
		agent_v2.WithHandoffInitial("inner_front"),
	).AddAgent("inner_front", innerFront).AddAgent("inner_billing", innerBilling)

	outerBilling, outerBillingModel := brkLeaf("billing", "OUTER-BILLING 接手了")
	outerH := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", innerH).AddAgent("billing", outerBilling)

	evs, err := outerH.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	var final string
	if f := agent_v2.FinalMessage(evs); f != nil {
		final = f.Content
	}
	t.Logf("B6 嵌套 Handoff：err=%v final=%q inner_front模型=%d inner_billing模型=%d 外层billing模型=%d",
		err, final, innerFrontModel.count(), innerBillingModel.count(), outerBillingModel.count())
	t.Logf("B6 说明：内层转交应落在**内层**（inner_billing>0 且外层 billing=0）；外层被内层信号带走 = 干扰")
}

// B6b：内层 Handoff 的候选转交到一个**只有外层才有**的目标——内层候选集不含它，
// 应软失败回灌，不该冒到外层。
func TestGrantBreak_B6b_NestedHandoffTargetsOuterCandidate(t *testing.T) {
	innerFront, innerFrontModel := grantInnerTransfer("inner_front", "billing") // 指向外层的 billing
	innerH := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("front"),
		agent_v2.WithHandoffInitial("inner_front"),
	).AddAgent("inner_front", innerFront)

	outerBilling, outerBillingModel := brkLeaf("billing", "OUTER-BILLING 接手了")
	outerH := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", innerH).AddAgent("billing", outerBilling)

	_, err := outerH.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	t.Logf("B6b 内层转交指向外层候选：err=%v inner_front模型=%d 外层billing模型=%d",
		err, innerFrontModel.count(), outerBillingModel.count())
	t.Logf("B6b 说明：外层 billing>0 = 内层的信号越权穿到了外层")
}

// ── B7：多层编排 Handoff → Sequential → Loop → BasicAgent(转交) ──

func TestGrantBreak_B7_DeepCompositeChain(t *testing.T) {
	leaf, leafModel := grantInnerTransfer("leaf", "billing")
	lp := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("lp"),
		agent_v2.WithLoopMaxIterations(1),
	).WithBody(leaf)
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("front")).AddStep("s1", lp)
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", seq).AddAgent("billing", billing)

	evs, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	var final string
	if f := agent_v2.FinalMessage(evs); f != nil {
		final = f.Content
	}
	t.Logf("B7 Handoff→Sequential→Loop→BasicAgent：leaf 模型=%d billing=%d final=%q",
		leafModel.count(), billingModel.count(), final)
}

// B7b：**中间夹一个业务壳**——Handoff → 业务壳(转发 opts) → Sequential → BasicAgent。
// 名字校验会被「一层框架编排」绕过吗？
func TestGrantBreak_B7b_ShellForwardsToComposite(t *testing.T) {
	leaf, leafModel := grantInnerTransfer("leaf", "billing")
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("s1", leaf)
	front := &grantForwardShell{name: "front", inner: seq}
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("B7b 壳转发给 Sequential：leaf 模型=%d billing=%d", leafModel.count(), billingModel.count())
	// **这一行是这条用例唯一承重的地方**。原先只有上面两句 t.Logf——独立审计把
	// 「未自查地读 grant」洗进编排之后，billing 从 0 变成 1（转交真的穿了），而这条
	// 用例照样绿。断言选在「有没有中断 / 有没有报错」上都不承重：泄漏的实质是
	// **被转交方的模型有没有真被调用**。
	if billingModel.count() > 0 {
		t.Errorf("B7b 泄漏：billing 模型被调了 %d 次——编排替业务壳把转交能力重新发下去了，"+
			"身份校验被一层间接绕开（壳自己从没被授予过）", billingModel.count())
	}
}

// ── B8：我自己想到的 ──

// B8a：**不可比较的动态类型做受让方**——interface == 在同型不可比较时 panic。
// 候选是一个带 map 字段的 **struct 值**（不是指针）。
type grantValueShell struct {
	tags  map[string]string // 让这个类型不可比较
	inner agent_v2.Agent
	name  string
}

func (w grantValueShell) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	evs, err := w.inner.Run(ctx, sess, in, opts...) // 礼貌透传
	if err != nil {
		return nil, err
	}
	f := agent_v2.FinalMessage(evs)
	if f == nil {
		f = schema.AssistantMessage("")
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, f)}, nil
}
func (w grantValueShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w grantValueShell) Name() string        { return w.name }
func (w grantValueShell) Description() string { return "不可比较的值类型业务壳" }

func TestGrantBreak_B8a_NonComparableGrantee(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("B8a **炸了**：不可比较类型做受让方触发 panic：%v", r)
		}
	}()
	inner, innerModel := grantInnerTransfer("inner", "billing")
	front := grantValueShell{tags: map[string]string{"k": "v"}, inner: inner, name: "front"}
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("B8a 不可比较受让方：没 panic；billing=%d 内层模型=%d", billingModel.count(), innerModel.count())
}

// B8a2：受让方与自查方**同型且不可比较**——直接对 handoffGrant 的比较语义做最坏假设。
// 候选是值类型壳 A，它把 opts 转发给**同类型**的另一个值 B。两边动态类型相同。
func TestGrantBreak_B8a2_SameNonComparableTypeOnBothSides(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("B8a2 **炸了**：同型不可比较受让方触发 panic：%v", r)
		}
	}()
	leaf, leafModel := grantInnerTransfer("leaf", "billing")
	b := grantValueShell{tags: map[string]string{"b": "2"}, inner: leaf, name: "b"}
	a := grantValueShell{tags: map[string]string{"a": "1"}, inner: b, name: "front"}
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", a).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("B8a2 同型不可比较：没 panic；billing=%d leaf 模型=%d", billingModel.count(), leafModel.count())
}

// B8b：**候选对象在两次 Run 之间被重建**（web 服务每请求重建 agent 树是常态）。
// 受让方是对象 ⟹ 重建后是新对象 ⟹ 还认得出来吗？
func TestGrantBreak_B8b_RebuiltAgentTreeAcrossResume(t *testing.T) {
	ask := &brkAskOnce{} // 中断只发生一次——重建的那棵树共用同一个（模拟同一份业务代码）
	build := func(m *brkStub, bm *brkStub) *agent_v2.HandoffAgent {
		tk := tool.NewToolkit()
		tk.Register(brkAskTool(ask))
		front := agent_v2.NewBasicAgent(
			agent_v2.WithName("front"),
			agent_v2.WithModel(m),
			agent_v2.WithToolkit(tk),
			agent_v2.WithHandoffTransfer(),
			agent_v2.WithMaxRounds(5),
		)
		billing := agent_v2.NewBasicAgent(
			agent_v2.WithName("billing"),
			agent_v2.WithModel(bm),
			agent_v2.WithMaxRounds(3),
		)
		return agent_v2.NewHandoffAgent(
			agent_v2.WithHandoffName("router"),
			agent_v2.WithHandoffInitial("front"),
		).AddAgent("front", front).AddAgent("billing", billing)
	}
	frontModel := newBrkStub(
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "a1", Type: "function",
			Function: schema.FunctionCall{Name: "need_confirm", Arguments: `{}`},
		}}),
		brkTransferCall("t1", "billing"),
	)
	billingModel := newBrkStub(schema.AssistantMessage("BILLING 接手了"))

	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	h1 := build(frontModel, billingModel)
	_, err := h1.Run(ctx, sess, schema.UserMessage("hi"))
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("fixture 没搭对：第一次 Run 应中断，实际 err=%v", err)
	}
	// **整棵树重建**（新进程 / 新请求），只有 sid 和 checkpointer 是跨过来的。
	h2 := build(frontModel, billingModel)
	if _, err := h2.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("确认"),
		})); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	t.Logf("B8b 树被重建后 Resume：front 模型=%d billing=%d", frontModel.count(), billingModel.count())
}

// B8c：**业务自己注册一个叫 transfer_to_agent 的工具**——框架现在**恒**往 dispatch
// 侧塞一件同名的（授予时是真工具、没授予时是软失败桩），业务那件会怎样？
func TestGrantBreak_B8c_BusinessToolNameCollision(t *testing.T) {
	var bizCalled int
	var mu sync.Mutex
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool(agent_v2.HandoffTransferToolName, "业务自己的同名工具",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(context.Context, map[string]any) (*tool.Result, error) {
			mu.Lock()
			bizCalled++
			mu.Unlock()
			return &tool.Result{Success: true, Output: "业务工具跑了"}, nil
		}))
	m := newBrkStub(
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "x1", Type: "function",
			Function: schema.FunctionCall{Name: agent_v2.HandoffTransferToolName, Arguments: `{}`},
		}}),
		schema.AssistantMessage("跑完了"),
	)
	solo := agent_v2.NewBasicAgent(
		agent_v2.WithName("solo"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(4),
	)
	// **独立跑，头顶没有任何 HandoffAgent**——业务只是恰好取了这个名字。
	evs, err := solo.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	var final string
	if f := agent_v2.FinalMessage(evs); f != nil {
		final = f.Content
	}
	mu.Lock()
	n := bizCalled
	mu.Unlock()
	t.Logf("B8c 业务同名工具：业务工具被调 %d 次，模型被调 %d 次，err=%v final=%q", n, m.count(), err, final)
	t.Logf("B8c 说明：业务工具被调 0 次 = 框架桩**静默顶掉**了业务自己的工具")
}

// B8d：**能不能禁止某个直接候选转交**（授予之后 opt-in 没了）。
// 业务唯一的口子是 WithToolFilter——试试拿它把 transfer 裁掉。
func TestGrantBreak_B8d_CannotRevokeViaToolFilter(t *testing.T) {
	front, frontModel := grantInnerTransfer("front", "billing")
	_ = frontModel
	// 重新造一个带 ToolFilter 的（grantInnerTransfer 没这个 option）
	m := newBrkStub(
		brkTransferCall("t1", "billing"),
		schema.AssistantMessage("FRONT 只好自己答"),
	)
	front = agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithModel(m),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(4),
		// 业务意图：这个候选是终点，**不许转交出去**。
		agent_v2.WithToolFilter(func(context.Context, *schema.Message) []string {
			return []string{} // 一件工具都不给
		}),
	)
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("B8d 想用 ToolFilter 吊销转交：front 模型=%d billing=%d", m.count(), billingModel.count())
	t.Logf("B8d 说明：billing>0 = 业务**无法**禁止一个直接候选转交（框架工具恒豁免裁剪）")
}

// B8e：**AgentAsTool 的内层拿不拿得到 grant**——祖父被授予了，孙辈是它的一件工具。
// （E7 已量过 billing=0；这一条改成孙辈**与祖父同一个对象**的极端形态。）
func TestGrantBreak_B8e_SelfAsToolReentry(t *testing.T) {
	m := newBrkStub(
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "c1", Type: "function",
			Function: schema.FunctionCall{Name: "helper", Arguments: `{"input":"x"}`},
		}}),
		brkTransferCall("t1", "billing"),
	)
	helperModel := newBrkStub(brkTransferCall("h1", "billing"), schema.AssistantMessage("helper 自己答"))
	helper := agent_v2.NewBasicAgent(
		agent_v2.WithName("helper"),
		agent_v2.WithModel(helperModel),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(3),
	)
	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(helper, agent_v2.WithAgentToolDescription("帮手")))
	front := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(5),
	)
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("B8e 祖父被授予 + 孙辈(AgentAsTool)也想转交：front模型=%d helper模型=%d billing=%d",
		m.count(), helperModel.count(), billingModel.count())
	t.Logf("B8e 说明：billing 应 =1 且只能来自 front 那一次（helper 的 transfer 必须软失败）")
}

// B8f：**annotation 路径完全没被授予管住**——任何候选（含业务壳、含没被授予的编排）
// 都能靠一个 annotation 指定接管者。这是 grant 之后剩下的最大穿透面。
func TestGrantBreak_B8f_AnnotationBypassesGrant(t *testing.T) {
	// 业务壳：内层是个**普通**没被授予的 BasicAgent，壳把内层 final 打上 annotation。
	inner, innerModel := brkLeaf("inner", "我处理不了")
	front := &grantAnnotationShell{name: "front", inner: inner, target: "billing"}
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 这条曾是 grant 之后剩下的最大穿透面（"任何候选都能靠一个 annotation 指定
	// 接管者"）。通道已删——转成回归哨兵。
	if n := billingModel.count(); n != 0 {
		t.Errorf("业务壳靠 annotation 绕过授予转走了对话：billing 被真调 %d 次（应为 0）", n)
	}
	if innerModel.count() == 0 {
		t.Fatal("fixture 没搭对：内层压根没跑，上面那句断言恒真")
	}

}

type grantAnnotationShell struct {
	name   string
	inner  agent_v2.Agent
	target string
}

func (w *grantAnnotationShell) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	evs, err := w.inner.Run(ctx, sess, in)
	if err != nil {
		return nil, err
	}
	f := agent_v2.FinalMessage(evs)
	if f == nil {
		f = schema.AssistantMessage("")
	}
	cp := *f
	cp.Annotations = map[string]any{agent_v2.HandoffSignalKey: w.target}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, &cp)}, nil
}
func (w *grantAnnotationShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w *grantAnnotationShell) Name() string        { return w.name }
func (w *grantAnnotationShell) Description() string { return "打 annotation 的业务壳" }

// B8g：**opts 被业务壳存下来、跨调用重放**给同一个受让方对象。
// 受让方匹配（就是同一个对象），槽却来自**当次** ctx——会不会在别的位置生效？
func TestGrantBreak_B8g_StashedOptsReplay(t *testing.T) {
	stash := &grantOptStash{}
	inner, innerModel := grantInnerTransfer("shared", "billing")
	// 第一跳的候选：壳 A，它把收到的 opts 存下来（自己不用）。
	shellA := &grantStashShell{name: "front", stash: stash, reply: "FRONT 处理不了，转给 helper", target: "helper"}
	// 第二跳的候选：壳 B，它拿**存下来的** opts 去调 inner。
	shellB := &grantReplayShell{name: "helper", stash: stash, inner: inner}
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
		agent_v2.WithHandoffMaxHops(5),
	).AddAgent("front", shellA).AddAgent("helper", shellB).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("B8g 存下 opts 跨跳重放：stash 存了 %d 份，inner 模型=%d billing=%d",
		stash.n(), innerModel.count(), billingModel.count())
	t.Logf("B8g 说明：billing>0 = 一份在第 0 跳发的授予被拿到第 1 跳用掉了")
}

type grantOptStash struct {
	mu   sync.Mutex
	opts [][]agent_v2.RunOption
}

func (s *grantOptStash) put(o []agent_v2.RunOption) {
	s.mu.Lock()
	s.opts = append(s.opts, o)
	s.mu.Unlock()
}
func (s *grantOptStash) first() []agent_v2.RunOption {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.opts) == 0 {
		return nil
	}
	return s.opts[0]
}
func (s *grantOptStash) n() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.opts) }

type grantStashShell struct {
	name   string
	stash  *grantOptStash
	reply  string
	target string
}

func (w *grantStashShell) Run(ctx context.Context, _ *agent_v2.RunSession, _ *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	w.stash.put(opts)
	m := schema.AssistantMessage(w.reply)
	// 经 HandoffTo 跳到第二跳（annotation 通道已删）。本壳**是**直接候选，手里那份
	// 授予的受让方就是它自己，所以这一步合法——本测试要验的是它能不能把授予**改寄**
	// 给别人，那是下一跳的事。
	if err := agent_v2.HandoffTo(ctx, opts, w, w.target); err != nil {
		return nil, err
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, m)}, nil
}
func (w *grantStashShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w *grantStashShell) Name() string        { return w.name }
func (w *grantStashShell) Description() string { return "把 opts 存下来的壳" }

type grantReplayShell struct {
	name  string
	stash *grantOptStash
	inner agent_v2.Agent
}

func (w *grantReplayShell) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	evs, err := w.inner.Run(ctx, sess, in, w.stash.first()...) // 重放上一跳收到的 opts
	if err != nil {
		return nil, err
	}
	f := agent_v2.FinalMessage(evs)
	if f == nil {
		f = schema.AssistantMessage("")
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, f)}, nil
}
func (w *grantReplayShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w *grantReplayShell) Name() string        { return w.name }
func (w *grantReplayShell) Description() string { return "重放 opts 的壳" }

// B8h：**同一个 BasicAgent 对象既是直接候选、又被业务壳内层引用**。
// 壳把 opts 转发给它 ⟹ 受让方是壳、收到的是那个对象 ⟹ 不匹配；
// 但如果壳**自己就是那个候选**呢？（自引用不成立，改测「两个候选共用同一个对象」。）
func TestGrantBreak_B8h_SameObjectTwoSlots(t *testing.T) {
	shared, sharedModel := grantInnerTransfer("shared", "billing")
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("a"),
	).AddAgent("a", shared).AddAgent("b", shared).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("B8h 同一个对象注册两个候选位：shared 模型=%d billing=%d", sharedModel.count(), billingModel.count())
}

// B8i：**工具可见性探针**——直接候选的 LLM 到底看不看得到 transfer_to_agent，
// 深层的看不看得到。(bind list 是 WithTools 的入参，不在 messages 里。)
type grantPeekModel struct {
	mu    sync.Mutex
	reply *schema.Message
	tools []string
	calls int
}

func (m *grantPeekModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	c := *m.reply
	return &c, nil
}

func (m *grantPeekModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	out, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(out)
	schema.CloseSend(sr)
	return sr, nil
}

func (m *grantPeekModel) WithTools(ts []*schema.ToolInfo) (model.ChatModel, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ti := range ts {
		m.tools = append(m.tools, ti.Name)
	}
	return m, nil
}

func (m *grantPeekModel) seenTools() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.tools))
	copy(out, m.tools)
	return out
}

var _ model.ChatModel = (*grantPeekModel)(nil)

func TestGrantBreak_B8i_ToolVisibility(t *testing.T) {
	peek := &grantPeekModel{reply: schema.AssistantMessage("答完了")}
	front := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithModel(peek),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(3),
	)
	deep := &grantPeekModel{reply: schema.AssistantMessage("深层答完了")}
	deepAg := agent_v2.NewBasicAgent(
		agent_v2.WithName("deep"),
		agent_v2.WithModel(deep),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(3),
	)
	shell := &grantForwardShell{name: "shell", inner: deepAg}
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("shell", shell)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	h2 := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router2"),
		agent_v2.WithHandoffInitial("shell"),
	).AddAgent("shell", shell).AddAgent("front", front)
	if _, err := h2.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run2: %v", err)
	}
	t.Logf("B8i 直接候选（front）bind 到的工具：%v", peek.seenTools())
	t.Logf("B8i 壳内层（deep）bind 到的工具：%v", deep.seenTools())
	t.Logf("B8i 说明：直接候选应含 transfer_to_agent，深层应不含（LLM 压根看不到）")
}

// B8j：**候选是 GraphAgent**，图里的节点用 RunSubAgent 调一个想转交的 BasicAgent。
// GraphAgent 的成员在**业务写的 NodeFunc** 里被调起，框架没有地方替它重新授予。
func TestGrantBreak_B8j_GraphAgentMember(t *testing.T) {
	leaf, leafModel := grantInnerTransfer("leaf", "billing")
	inCh := compose_v2.LastValue[*schema.Message]("gin")
	outCh := compose_v2.LastValue[*schema.Message]("gout")
	b := compose_v2.NewBuilder()
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("n1", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		evs, err := agent_v2.RunSubAgent(ctx, "leaf", leaf, in)
		if err != nil {
			return nil, err
		}
		f := agent_v2.FinalMessage(evs)
		if f == nil {
			f = schema.AssistantMessage("")
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, f)}, nil
	})
	b.AddEdge(compose_v2.StartNode, "n1")
	b.AddEdge("n1", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ga := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("front"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}),
	)
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", ga).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("B8j 候选是 GraphAgent：leaf 模型=%d billing=%d", leafModel.count(), billingModel.count())
	t.Logf("B8j 说明：billing=0 = GraphAgent 的成员**转不了**（业务 NodeFunc 里没有重新授予的口子）")
}

// B8k：**PlanAgent 的 TmpSubAgent**——planner（一个 LLM）临时造出来的执行体。
// 它是被 regrant 的对象之一 ⟹ 它自动获得转交能力。base 里它没有（makeTmpSubAgent
// 从不注册 transfer 工具）。这是**授予方案新开的一个面**。
func TestGrantBreak_B8k_TmpSubAgentGainsTransfer(t *testing.T) {
	tmpModel := newBrkStub(
		brkTransferCall("t1", "billing"),
		schema.AssistantMessage("TMP 自己答了"),
	)
	planner := agent_v2.Planner(func(_ context.Context, _ *agent_v2.RunSession, _ string, done []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(done) > 0 {
			return nil, "PLAN 汇总完毕", nil
		}
		return []agent_v2.PlanStep{{
			Description: "临时干一票",
			TmpSubAgent: &agent_v2.TmpSubAgentConfig{Name: "tmp_worker", System: "你是临时工"},
		}}, "", nil
	})
	pl := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("front"),
		agent_v2.WithPlanModel(tmpModel), // TmpSubAgent 用的就是 planModel
		agent_v2.WithPlanMaxSteps(3),
	)
	// PlanAgent 要求至少一个显式 sub-agent（本例用不到它）
	idle, _ := brkLeaf("idle", "idle")
	pl.AddSubAgent("idle", idle)
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", pl).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("B8k planner 临时造的 TmpSubAgent：tmp 模型=%d billing=%d", tmpModel.count(), billingModel.count())
	t.Logf("B8k 说明：billing>0 = 一个由 LLM planner 当场决定造出来的执行体，自动拿到了转交权")
}

// B8l：**RoutingAgent 的分类器**——它是框架内部的决策 LLM，不是成员。
// 它能不能转交？（base 里它继承了 ctx 槽 ⟹ 能；grant 里它不在 regrant 名单上。）
func TestGrantBreak_B8l_RoutingClassifierCannotTransfer(t *testing.T) {
	// 分类器：不选路由，而是直接发一条 transfer_to_agent
	clsModel := newBrkStub(
		brkTransferCall("c1", "billing"),
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "r1", Type: "function",
			Function: schema.FunctionCall{Name: agent_v2.RouteTool, Arguments: `{"target":"only"}`},
		}}),
	)
	cls := agent_v2.NewBasicAgent(
		agent_v2.WithName("classifier"),
		agent_v2.WithModel(clsModel),
		agent_v2.WithRouteTool(),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(4),
	)
	only, onlyModel := brkLeaf("only", "ONLY 答了")
	rt := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("front"),
		agent_v2.WithRoutingClassifier(cls),
	).AddBranch("only", only)
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", rt).AddAgent("billing", billing)
	_, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	t.Logf("B8l 分类器想转交：err=%v 分类器模型=%d only模型=%d billing=%d",
		err, clsModel.count(), onlyModel.count(), billingModel.count())
	t.Logf("B8l 说明：billing>0 = 框架内部的决策 LLM 能夺走整场对话的归属")
}
