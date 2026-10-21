package agent_v2_test

import (
	"context"
	"runtime"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// ═══════════════════════════════════════════════════════════════════════════
// R 系列：**导出的重新授予原语** RegrantHandoffTo 之后，
//   (a) 被动转发（B1/B2/B7b/B8g）有没有跟着打开？
//   (b) 三处能力回退（业务壳 / 业务自写编排 / GraphAgent 成员）补回来几处？
//   (c) 新开了什么面？
//
// 尺子不变：**billing 的模型被真调了几次**。
// ═══════════════════════════════════════════════════════════════════════════

// ── 显式授予的壳（与 grantForwardShell「礼貌透传」逐字对照，只差那一行）──

type regrantShell struct {
	name  string
	inner agent_v2.Agent
	runs  int
	// misuse 选择误用形态：0=正确 1=self 写成内层 2=self/to 写反 3=只转发不授予
	misuse int
}

func (w *regrantShell) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	w.runs++
	var sub []agent_v2.RunOption
	switch w.misuse {
	case 1: // self 写成内层（把「我是受让方」这句话说错了）
		sub = agent_v2.RegrantHandoffTo(opts, w.inner, w.inner)
	case 2: // self / to 写反
		sub = agent_v2.RegrantHandoffTo(opts, w.inner, w)
	case 3: // 只转发（忘了调原语）
		sub = opts
	default:
		sub = agent_v2.RegrantHandoffTo(opts, w, w.inner)
	}
	evs, err := w.inner.Run(ctx, sess, in, sub...)
	if err != nil {
		return nil, err
	}
	f := agent_v2.FinalMessage(evs)
	if f == nil {
		f = schema.AssistantMessage("")
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, f)}, nil
}
func (w *regrantShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w *regrantShell) Name() string        { return w.name }
func (w *regrantShell) Description() string { return "显式重新授予的业务壳" }

func runRegrantShellCase(t *testing.T, shellName, innerName string, misuse int) (billing, inner int, final string) {
	t.Helper()
	in, innerModel := grantInnerTransfer(innerName, "billing")
	front := &regrantShell{name: shellName, inner: in, misuse: misuse}
	bill, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", bill)
	evs, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if front.runs == 0 {
		t.Fatalf("fixture 没搭对：壳压根没被调")
	}
	if f := agent_v2.FinalMessage(evs); f != nil {
		final = f.Content
	}
	return billingModel.count(), innerModel.count(), final
}

// R1：**显式授予**（对照 B1 的礼貌透传）。
func TestRegrant_R1_ExplicitRegrantWorks(t *testing.T) {
	b, i, f := runRegrantShellCase(t, "front", "inner", 0)
	t.Logf("R1 壳显式授予（壳=front / 内层=inner）：billing=%d 内层模型=%d final=%q", b, i, f)
	// **承重断言**：显式授予必须有效。红了说明业务壳没有正当出路——那样「业务写的
	// 执行体一律没资格」就成了硬回退，而这正是上一版方案被否掉的原因。
	// 跟 R1d（忘了调原语）的唯一差别就是 Run 里那一行。
	if b != 1 {
		t.Errorf("R1 显式授予失效：billing 被真调 %d 次，期望 1——业务壳没有把资格转授给内层的正当出路", b)
	}
}

// R1b：**self 写成内层**——最容易犯的那个错。
func TestRegrant_R1b_MisuseSelfIsInner(t *testing.T) {
	b, i, _ := runRegrantShellCase(t, "front", "inner", 1)
	t.Logf("R1b self 误写成内层：billing=%d 内层模型=%d", b, i)
	t.Logf("R1b 说明：billing=0 = 说错话就没效果（静默 no-op）；billing>0 = 签名把「转发」变成了「授予」")
}

// R1c：**self / to 写反**。
func TestRegrant_R1c_MisuseSwapped(t *testing.T) {
	b, i, _ := runRegrantShellCase(t, "front", "inner", 2)
	t.Logf("R1c self/to 写反：billing=%d 内层模型=%d", b, i)
}

// R1d：**忘了调原语**（= B1，但壳类型换成 regrantShell 保证唯一变量是那一行）。
func TestRegrant_R1d_ForgotToCall(t *testing.T) {
	b, i, f := runRegrantShellCase(t, "front", "inner", 3)
	t.Logf("R1d 忘了调原语（只转发）：billing=%d 内层模型=%d final=%q err=<nil>", b, i, f)
	t.Logf("R1d 说明：失败模式 = 静默不转交（无 error、无 warn），内层拿软失败回灌改成自答")
}

// R1e：壳与内层**同名**（对照 B2）。
func TestRegrant_R1e_SameName(t *testing.T) {
	b, i, _ := runRegrantShellCase(t, "front", "front", 0)
	t.Logf("R1e 显式授予且同名：billing=%d 内层模型=%d", b, i)
}

// ── R2：业务自写编排（对照 B4）──

type regrantBizOrchestration struct {
	name  string
	step  agent_v2.Agent
	viaHi bool
}

func (o *regrantBizOrchestration) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	sub := agent_v2.RegrantHandoffTo(opts, o, o.step) // 显式：这一步是我的组成部分
	var evs []*agent_v2.AgentEvent
	var err error
	if o.viaHi {
		evs, err = agent_v2.RunSubAgent(ctx, "step", o.step, in, sub...)
	} else {
		evs, err = o.step.Run(ctx, sess, in, sub...)
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
func (o *regrantBizOrchestration) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (o *regrantBizOrchestration) Name() string        { return o.name }
func (o *regrantBizOrchestration) Description() string { return "显式授予的业务自写编排" }

func TestRegrant_R2_BizOrchestrationMember(t *testing.T) {
	for _, viaHelper := range []bool{false, true} {
		inner, innerModel := grantInnerTransfer("step2", "billing")
		front := &regrantBizOrchestration{name: "front", step: inner, viaHi: viaHelper}
		billing, billingModel := brkLeaf("billing", "BILLING 接手了")
		h := agent_v2.NewHandoffAgent(
			agent_v2.WithHandoffName("router"),
			agent_v2.WithHandoffInitial("front"),
		).AddAgent("front", front).AddAgent("billing", billing)
		if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
			t.Fatalf("viaHelper=%v Run: %v", viaHelper, err)
		}
		t.Logf("R2 业务自写编排显式授予（viaRunSubAgent=%v）：billing=%d 成员模型=%d",
			viaHelper, billingModel.count(), innerModel.count())
	}
}

// ── R3：壳显式授予给一个**框架编排**（对照 B7b）──
func TestRegrant_R3_ShellRegrantsToComposite(t *testing.T) {
	leaf, leafModel := grantInnerTransfer("leaf", "billing")
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("s1", leaf)
	front := &regrantShell{name: "front", inner: seq}
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("R3 壳显式授予给 Sequential：leaf 模型=%d billing=%d", leafModel.count(), billingModel.count())
	t.Logf("R3 说明：编排收到后会继续 regrant 给自己的成员——一次显式授予穿透整棵框架子树")
}

// ── R4：GraphAgent 成员（对照 B8j）──

// R4a：GraphAgent **直接**作候选。NodeFunc 拿不到 opts——框架没有把 grant 送进去的路。
func TestRegrant_R4a_GraphAgentDirectCandidate(t *testing.T) {
	leaf, leafModel := grantInnerTransfer("leaf", "billing")
	billingModel := runGraphAgentCase(t, leaf, nil)
	t.Logf("R4a GraphAgent 直接作候选（NodeFunc 无 opts 可用）：leaf 模型=%d billing=%d",
		leafModel.count(), billingModel)
	t.Logf("R4a 说明：billing=0 = 这处回退**没补回来**——NodeFunc 的入参只有 ctx + Channels")
}

// R4b：业务在 GraphAgent **外面包一层壳**——壳是候选、拿得到 opts，regrant 后经
// 自己的字段（业务侧信道）递给闭包。补得回来吗？
func TestRegrant_R4b_GraphAgentViaOuterShell(t *testing.T) {
	leaf, leafModel := grantInnerTransfer("leaf", "billing")
	side := &optSideChannel{}
	billingModel := runGraphAgentCase(t, leaf, side)
	t.Logf("R4b GraphAgent 外面包壳 + 业务侧信道：leaf 模型=%d billing=%d 侧信道写入=%d",
		leafModel.count(), billingModel, side.n())
	t.Logf("R4b 说明：billing>0 = 业务能自己接回来（代价：侧信道是业务持有的可变状态，并发多实例要自己管）")
}

type optSideChannel struct {
	mu    sync.Mutex
	opts  []agent_v2.RunOption
	wrote int
}

func (s *optSideChannel) set(o []agent_v2.RunOption) {
	s.mu.Lock()
	s.opts, s.wrote = o, s.wrote+1
	s.mu.Unlock()
}
func (s *optSideChannel) get() []agent_v2.RunOption {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opts
}
func (s *optSideChannel) n() int { s.mu.Lock(); defer s.mu.Unlock(); return s.wrote }

// graphOuterShell 把 GraphAgent 包起来：自己是候选，收到 grant 后 regrant 给 leaf，
// 把结果放进业务自己的侧信道供 NodeFunc 闭包取用。
type graphOuterShell struct {
	name  string
	inner agent_v2.Agent
	leaf  agent_v2.Agent
	side  *optSideChannel
}

func (w *graphOuterShell) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	w.side.set(agent_v2.RegrantHandoffTo(opts, w, w.leaf))
	evs, err := w.inner.Run(ctx, sess, in)
	if err != nil {
		return nil, err
	}
	f := agent_v2.FinalMessage(evs)
	if f == nil {
		f = schema.AssistantMessage("")
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, f)}, nil
}
func (w *graphOuterShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w *graphOuterShell) Name() string        { return w.name }
func (w *graphOuterShell) Description() string { return "包住 GraphAgent 的壳" }

// runGraphAgentCase 建一个「节点里调 leaf」的 GraphAgent。side==nil 时 GraphAgent
// 直接作候选；否则外面包一层 graphOuterShell 并让节点从侧信道取 opts。
func runGraphAgentCase(t *testing.T, leaf agent_v2.Agent, side *optSideChannel) int {
	t.Helper()
	inCh := compose_v2.LastValue[*schema.Message]("gin")
	outCh := compose_v2.LastValue[*schema.Message]("gout")
	b := compose_v2.NewBuilder()
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("n1", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		var sub []agent_v2.RunOption
		if side != nil {
			sub = side.get()
		}
		evs, err := agent_v2.RunSubAgent(ctx, "leaf", leaf, in, sub...)
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
		agent_v2.WithGraphAgentName("front_graph"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}),
	)
	var cand agent_v2.Agent = ga
	if side != nil {
		cand = &graphOuterShell{name: "front", inner: ga, leaf: leaf, side: side}
	}
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", cand).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return billingModel.count()
}

// ── R5：显式授予能发给谁（穿透面的实际大小）──

// R5a：发给一个**跟本次编排毫无关系**的对象（既不是候选、也不在树里）。
func TestRegrant_R5a_GrantToUnrelatedObject(t *testing.T) {
	stranger, strangerModel := grantInnerTransfer("stranger", "billing")
	// front 是候选；它把资格发给一个跟这棵树没关系的 stranger，然后调 stranger。
	front := &regrantShell{name: "front", inner: stranger}
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("R5a 发给树外对象：stranger 模型=%d billing=%d", strangerModel.count(), billingModel.count())
	t.Logf("R5a 说明：billing>0 = 受让方不必在编排里——「我的组成部分」由业务单方面定义")
}

// R5b：发给**自己的祖先**（把资格发回给 HandoffAgent 本人，再调它）。
func TestRegrant_R5b_GrantToAncestor(t *testing.T) {
	var h *agent_v2.HandoffAgent
	back := &regrantToAncestorShell{name: "front"}
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h = agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
		agent_v2.WithHandoffMaxHops(3),
	).AddAgent("front", back).AddAgent("billing", billing)
	back.ancestor = h
	_, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	t.Logf("R5b 把资格发回给祖先并递归调它：err=%v billing=%d 递归深度=%d",
		errShort(err), billingModel.count(), back.depth)
	t.Logf("R5b 说明：看它是有界（maxHops / 报错）还是无界递归")
}

type regrantToAncestorShell struct {
	name     string
	ancestor agent_v2.Agent
	depth    int
}

func (w *regrantToAncestorShell) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	w.depth++
	if w.depth > 4 { // 自己设一个界，免得测试挂死
		return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, schema.AssistantMessage("到底了"))}, nil
	}
	sub := agent_v2.RegrantHandoffTo(opts, w, w.ancestor)
	evs, err := w.ancestor.Run(ctx, agent_v2.NewRunSession(), in, sub...)
	if err != nil {
		return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, schema.AssistantMessage("祖先报错: "+err.Error()))}, nil
	}
	f := agent_v2.FinalMessage(evs)
	if f == nil {
		f = schema.AssistantMessage("")
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, f)}, nil
}
func (w *regrantToAncestorShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w *regrantToAncestorShell) Name() string        { return w.name }
func (w *regrantToAncestorShell) Description() string { return "把资格发回祖先的壳" }

func errShort(err error) string {
	if err == nil {
		return "<nil>"
	}
	s := err.Error()
	if len(s) > 90 {
		s = s[:90] + "…"
	}
	return s
}

// ── R6：把授予当**不记名票据**用（原语新开的那一面）──

// R6a：第 0 跳的壳 regrant 给 X 并把结果**存下来**；第 1 跳的另一个壳拿它去调 X。
// X 是同一个对象 ⟹ 自查通过 ⟹ 转交资格被搬到了别的位置。
func TestRegrant_R6a_BearerTokenReplayAcrossHops(t *testing.T) {
	stash := &grantOptStash{}
	x, xModel := grantInnerTransfer("x", "billing")
	shellA := &regrantStashShell{name: "front", stash: stash, target: "helper", to: x}
	shellB := &grantReplayShell{name: "helper", stash: stash, inner: x}
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
		agent_v2.WithHandoffMaxHops(5),
	).AddAgent("front", shellA).AddAgent("helper", shellB).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 前提自检：第 1 跳必须真的跑到，否则「跨跳重放」这个场景根本没发生、下面的
	// 观察恒真。（这条栽过：姊妹壳改了转交手段而它没改，第 1 跳静默不发生。）
	if stash.n() == 0 {
		t.Fatal("fixture 没搭对：票据没被存下来")
	}
	if xModel.count() == 0 && billingModel.count() == 0 {
		t.Fatalf("fixture 没搭对：第 1 跳没跑到（X 模型=0 billing=0），" +
			"「跨跳重放」这个场景不存在，下面的说明与断言都恒真")
	}
	t.Logf("R6a 授予当票据跨跳重放：stash=%d X 模型=%d billing=%d", stash.n(), xModel.count(), billingModel.count())
	t.Logf("R6a 说明：billing>0 = **已正确寻址**的授予是不记名的——谁拿到 opts 谁都能把它交给 X")
}

// regrantStashShell：候选，收到 grant 后 regrant 给 to 并存下来，自己不用；用
// annotation 跳到下一跳。
type regrantStashShell struct {
	name   string
	stash  *grantOptStash
	target string
	to     agent_v2.Agent
}

func (w *regrantStashShell) Run(ctx context.Context, _ *agent_v2.RunSession, _ *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	w.stash.put(agent_v2.RegrantHandoffTo(opts, w, w.to))
	m := schema.AssistantMessage("FRONT 处理不了")
	// 跳到下一跳走 HandoffTo（annotation 通道已删）。本壳是直接候选、手里那份授予的
	// 受让方就是它自己，这一步合法——本用例要验的是「存下来的票据能不能跨跳重放」。
	//
	// **target 为空 = 本跳不转交**（R6b 那条就这么用），不能把空目标送进 HandoffTo
	// ——那是协议违规，会终结整个 Run。
	if w.target != "" {
		if err := agent_v2.HandoffTo(ctx, opts, w, w.target); err != nil {
			return nil, err
		}
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, m)}, nil
}
func (w *regrantStashShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w *regrantStashShell) Name() string        { return w.name }
func (w *regrantStashShell) Description() string { return "regrant 后存票据的壳" }

// R6b：**跨 Run** 重放同一张票据。
func TestRegrant_R6b_BearerTokenReplayAcrossRuns(t *testing.T) {
	stash := &grantOptStash{}
	x, xModel := grantInnerTransfer("x", "billing")
	shellA := &regrantStashShell{name: "front", stash: stash, target: "", to: x}
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", shellA).AddAgent("billing", billing)
	sess := agent_v2.NewRunSession()
	ctx := context.Background()
	if _, err := h.Run(ctx, sess, schema.UserMessage("第一次")); err != nil {
		t.Fatalf("Run1: %v", err)
	}
	before := billingModel.count()
	// 第二次：**脱离编排**，业务自己拿上一次的票据直接调 X。
	sess2 := agent_v2.NewRunSession()
	if _, err := x.Run(ctx, sess2, schema.UserMessage("第二次"), stash.first()...); err != nil {
		t.Fatalf("Run2: %v", err)
	}
	t.Logf("R6b 跨 Run 重放票据：stash=%d X 模型=%d billing(Run1 后)=%d billing(总)=%d",
		stash.n(), xModel.count(), before, billingModel.count())
	t.Logf("R6b 说明：billing 总数 > Run1 后 = 票据跨 Run 仍能兑现；相等 = 槽是 per-call 的，兑不了")
}

// R6c：**没有 self 参数**的那个形状（用户最初提的签名）——同一个 B8g fixture。
func TestRegrant_R6c_NoSelfCheckReopensB8g(t *testing.T) {
	stash := &grantOptStash{}
	inner, innerModel := grantInnerTransfer("shared", "billing")
	shellA := &grantStashShell{name: "front", stash: stash, reply: "FRONT 处理不了", target: "helper"}
	shellB := &noSelfCheckReplayShell{name: "helper", stash: stash, inner: inner}
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
		agent_v2.WithHandoffMaxHops(5),
	).AddAgent("front", shellA).AddAgent("helper", shellB).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("R6c 去掉 self 参数后重放 B8g：stash=%d inner 模型=%d billing=%d",
		stash.n(), innerModel.count(), billingModel.count())
	t.Logf("R6c 说明：billing>0 = 少了 self 这个证据参数，B8g（改寄别人的授予）当场重开")
}

type noSelfCheckReplayShell struct {
	name  string
	stash *grantOptStash
	inner agent_v2.Agent
}

func (w *noSelfCheckReplayShell) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	sub := agent_v2.RegrantHandoffToNoSelfCheck(w.stash.first(), w.inner) // 改寄
	evs, err := w.inner.Run(ctx, sess, in, sub...)
	if err != nil {
		return nil, err
	}
	f := agent_v2.FinalMessage(evs)
	if f == nil {
		f = schema.AssistantMessage("")
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, f)}, nil
}
func (w *noSelfCheckReplayShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w *noSelfCheckReplayShell) Name() string        { return w.name }
func (w *noSelfCheckReplayShell) Description() string { return "去掉 self 校验的重放壳" }

// R6d：**借用原受让方当 self**——checked 版能不能被兄弟候选绕过。
func TestRegrant_R6d_BorrowedSelf(t *testing.T) {
	stash := &grantOptStash{}
	inner, innerModel := grantInnerTransfer("shared", "billing")
	shellA := &grantStashShell{name: "front", stash: stash, reply: "FRONT 处理不了", target: "helper"}
	shellB := &borrowedSelfReplayShell{name: "helper", stash: stash, inner: inner, borrowed: shellA}
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
		agent_v2.WithHandoffMaxHops(5),
	).AddAgent("front", shellA).AddAgent("helper", shellB).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("R6d 借用原受让方当 self：stash=%d inner 模型=%d billing=%d",
		stash.n(), innerModel.count(), billingModel.count())
	t.Logf("R6d 说明：billing>0 = self 只是「出示那个对象」，兄弟候选互相持有引用即可绕过")
}

type borrowedSelfReplayShell struct {
	name     string
	stash    *grantOptStash
	inner    agent_v2.Agent
	borrowed agent_v2.Agent // 原受让方（业务自己就持有这个引用——它建的树）
}

func (w *borrowedSelfReplayShell) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	sub := agent_v2.RegrantHandoffTo(w.stash.first(), w.borrowed, w.inner)
	evs, err := w.inner.Run(ctx, sess, in, sub...)
	if err != nil {
		return nil, err
	}
	f := agent_v2.FinalMessage(evs)
	if f == nil {
		f = schema.AssistantMessage("")
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, f)}, nil
}
func (w *borrowedSelfReplayShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w *borrowedSelfReplayShell) Name() string        { return w.name }
func (w *borrowedSelfReplayShell) Description() string { return "借 self 的重放壳" }

// ── R7：仓库自带的两条（NoResidueAcrossRuns / AcrossTasks）改成显式授予 ──

// regrantGoexitOnce = scopeGoexitOnce，只把「礼貌透传」换成「显式授予」。
type regrantGoexitOnce struct {
	inner agent_v2.Agent
	mu    sync.Mutex
	n     int
}

func (g *regrantGoexitOnce) Run(ctx context.Context, s *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	g.mu.Lock()
	g.n++
	first := g.n == 1
	g.mu.Unlock()
	evs, err := g.inner.Run(ctx, s, in, agent_v2.RegrantHandoffTo(opts, g, g.inner)...)
	if first {
		runtime.Goexit()
	}
	return evs, err
}

func (g *regrantGoexitOnce) Resume(ctx context.Context, sid string, opts ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return g.inner.Resume(ctx, sid, opts...)
}
func (g *regrantGoexitOnce) Name() string        { return "front" }
func (g *regrantGoexitOnce) Description() string { return "前台" }

func regrantResidueFixture() (*agent_v2.HandoffAgent, *scopeStub, *scopeStub) {
	frontModel := newScopeStub(
		scopeTransferCall("t1", "billing"),
		schema.AssistantMessage("FRONT 自己答了，没有转交"),
	)
	frontInner := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithModel(frontModel),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(3),
	)
	billing, billingModel := scopeLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", &regrantGoexitOnce{inner: frontInner}).AddAgent("billing", billing)
	return h, billingModel, frontModel
}

func TestRegrant_R7a_NoResidueAcrossRuns(t *testing.T) {
	h, billingModel, frontModel := regrantResidueFixture()
	sess := agent_v2.NewRunSession()
	ctx := context.Background()
	runDetached(t, func() { _, _ = h.Run(ctx, sess, schema.UserMessage("第一个问题")) })
	if _, err := h.Run(ctx, sess, schema.UserMessage("完全不相干的第二个问题")); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	t.Logf("R7a 显式授予版 NoResidueAcrossRuns：front 模型=%d（前提要求 2） billing=%d（要求 0）",
		frontModel.count(), billingModel.count())
	assertResiduePremise(t, frontModel)
	if n := billingModel.count(); n != 0 {
		t.Errorf("跨 Run 幽灵转交：billing 被真调 %d 次", n)
	}
}

func TestRegrant_R7b_NoResidueAcrossTasks(t *testing.T) {
	h, billingModel, frontModel := regrantResidueFixture()
	sess := agent_v2.NewRunSession()
	ctx := context.Background()
	runDetached(t, func() { _, _ = h.Run(ctx, sess, schema.UserMessage("第一个问题")) })
	sess.NextTask()
	if _, err := h.Run(ctx, sess, schema.UserMessage("完全不相干的第二个问题")); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	t.Logf("R7b 显式授予版 NoResidueAcrossTasks：front 模型=%d（前提要求 2） billing=%d（要求 0）",
		frontModel.count(), billingModel.count())
	assertResiduePremise(t, frontModel)
	if n := billingModel.count(); n != 0 {
		t.Errorf("跨任务幽灵转交：billing 被真调 %d 次", n)
	}
}

// ── R8：原语在**没被授予**时的行为（顶层直接跑 / 跑在 HandoffAgent 之外）──
func TestRegrant_R8_NoGrantIsNoop(t *testing.T) {
	inner, innerModel := grantInnerTransfer("inner", "billing")
	front := &regrantShell{name: "front", inner: inner}
	// 不经 HandoffAgent，直接顶层跑这个壳。
	evs, err := front.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	var final string
	if f := agent_v2.FinalMessage(evs); f != nil {
		final = f.Content
	}
	t.Logf("R8 顶层直接跑（没人授予）：err=%v 内层模型=%d final=%q", errShort(err), innerModel.count(), final)
	t.Logf("R8 说明：原语必须静默 no-op——「上层没授予我」是常态，报错会把所有非 handoff 调用打断")
}

// ── R9：同一份 opts 连着 regrant 给**两个**孩子（并发），会不会串台 / 覆盖 ──

type regrantTwoChildren struct {
	name       string
	a, b       agent_v2.Agent
	concurrent bool
}

func (o *regrantTwoChildren) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	optA := agent_v2.RegrantHandoffTo(opts, o, o.a)
	optB := agent_v2.RegrantHandoffTo(opts, o, o.b)
	run := func(ag agent_v2.Agent, oo []agent_v2.RunOption) { _, _ = ag.Run(ctx, sess, in, oo...) }
	if o.concurrent {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); run(o.a, optA) }()
		go func() { defer wg.Done(); run(o.b, optB) }()
		wg.Wait()
	} else {
		run(o.a, optA)
		run(o.b, optB)
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(o.name, schema.AssistantMessage("两个都跑完了"))}, nil
}
func (o *regrantTwoChildren) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (o *regrantTwoChildren) Name() string        { return o.name }
func (o *regrantTwoChildren) Description() string { return "给两个孩子各发一份的编排" }

func TestRegrant_R9_TwoChildrenNoCrosstalk(t *testing.T) {
	for _, conc := range []bool{false, true} {
		ca, caModel := grantInnerTransfer("child_a", "billing")
		cb, cbModel := grantInnerTransfer("child_b", "billing")
		front := &regrantTwoChildren{name: "front", a: ca, b: cb, concurrent: conc}
		billing, billingModel := brkLeaf("billing", "BILLING 接手了")
		h := agent_v2.NewHandoffAgent(
			agent_v2.WithHandoffName("router"),
			agent_v2.WithHandoffInitial("front"),
		).AddAgent("front", front).AddAgent("billing", billing)
		if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
			t.Fatalf("concurrent=%v Run: %v", conc, err)
		}
		t.Logf("R9 两个孩子各发一份（concurrent=%v）：a 模型=%d b 模型=%d billing=%d",
			conc, caModel.count(), cbModel.count(), billingModel.count())
	}
	t.Logf("R9 说明：两个孩子模型都=1（各自一调即出）= 两份授予都生效且互不覆盖；某个=2 = 它那份被另一份盖掉了")
}

// R10：**授给 A、却把 opts 喂给 B**（签名最容易的错位用法）。
func TestRegrant_R10_GrantToOneFeedAnother(t *testing.T) {
	a, aModel := grantInnerTransfer("child_a", "billing")
	b, bModel := grantInnerTransfer("child_b", "billing")
	front := &regrantMisdirectShell{name: "front", grantTo: a, callee: b}
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	_ = aModel
	t.Logf("R10 授给 A 却喂给 B：b 模型=%d billing=%d", bModel.count(), billingModel.count())
	t.Logf("R10 说明：billing=0 = 错位用法静默失效（受让方对不上）；billing>0 = 授予是不记名的")
}

type regrantMisdirectShell struct {
	name    string
	grantTo agent_v2.Agent
	callee  agent_v2.Agent
}

func (w *regrantMisdirectShell) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	sub := agent_v2.RegrantHandoffTo(opts, w, w.grantTo)
	evs, err := w.callee.Run(ctx, sess, in, sub...)
	if err != nil {
		return nil, err
	}
	f := agent_v2.FinalMessage(evs)
	if f == nil {
		f = schema.AssistantMessage("")
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, f)}, nil
}
func (w *regrantMisdirectShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w *regrantMisdirectShell) Name() string        { return w.name }
func (w *regrantMisdirectShell) Description() string { return "授给 A 却喂给 B 的壳" }

// R11：原语**不得原地改**调用方的 opts（调用方通常要拿同一份给第二个孩子用）。
func TestRegrant_R11_DoesNotMutateCallerOpts(t *testing.T) {
	a, _ := grantInnerTransfer("a", "billing")
	b, _ := grantInnerTransfer("b", "billing")
	probe := &regrantAliasProbe{a: a, b: b}
	billing, _ := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", probe).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("R11 调用方 opts 长度：调用前=%d 两次 regrant 之后=%d（相等 = 没被原地改）",
		probe.lenBefore, probe.lenAfter)
	if probe.lenBefore != probe.lenAfter {
		t.Errorf("原语原地改了调用方的 opts：%d → %d", probe.lenBefore, probe.lenAfter)
	}
}

type regrantAliasProbe struct {
	a, b                agent_v2.Agent
	lenBefore, lenAfter int
}

func (w *regrantAliasProbe) Run(_ context.Context, _ *agent_v2.RunSession, _ *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	w.lenBefore = len(opts)
	_ = agent_v2.RegrantHandoffTo(opts, w, w.a)
	_ = agent_v2.RegrantHandoffTo(opts, w, w.b)
	w.lenAfter = len(opts)
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent("front", schema.AssistantMessage("ok"))}, nil
}
func (w *regrantAliasProbe) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w *regrantAliasProbe) Name() string        { return "front" }
func (w *regrantAliasProbe) Description() string { return "查 opts 有没有被原地改" }

// R12：**跨 Run 兑现票据**——Run 1 里 regrant 出来的那份 opts，在 Run 2 的一个
// **活着的** handoff 步骤里拿去调同一个 X。（R6b 只证明「没有槽就兑不了」，那与
// 「工具没注册」不可区分；本例给足槽。）
func TestRegrant_R12_BearerTokenCashedInLaterRun(t *testing.T) {
	stash := &grantOptStash{}
	x, xModel := grantInnerTransfer("x", "billing")
	front := &regrantTwoPhaseShell{name: "front", stash: stash, to: x}
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)
	ctx := context.Background()
	if _, err := h.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("第一次")); err != nil {
		t.Fatalf("Run1: %v", err)
	}
	after1 := billingModel.count()
	// Run 2：**全新 session**，front 不用这次收到的 opts，改用 Run 1 存的那份。
	if _, err := h.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("第二次")); err != nil {
		t.Fatalf("Run2: %v", err)
	}
	t.Logf("R12 跨 Run 兑现票据：stash=%d X 模型=%d billing(Run1 后)=%d billing(总)=%d",
		stash.n(), xModel.count(), after1, billingModel.count())
	t.Logf("R12 说明：总数 > Run1 后 = 一张在上一次 Run 铸出来的票据，在这一次 Run 里照样兑现")
}

// regrantTwoPhaseShell：第一次 Run 只 regrant + 存票据、自己作答；第二次 Run 拿
// **上一次**的票据去调 to。
type regrantTwoPhaseShell struct {
	name  string
	stash *grantOptStash
	to    agent_v2.Agent
	runs  int
}

func (w *regrantTwoPhaseShell) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	w.runs++
	if w.runs == 1 {
		w.stash.put(agent_v2.RegrantHandoffTo(opts, w, w.to))
		return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, schema.AssistantMessage("第一次我自己答"))}, nil
	}
	evs, err := w.to.Run(ctx, sess, in, w.stash.first()...) // 用上一次 Run 的票据
	if err != nil {
		return nil, err
	}
	f := agent_v2.FinalMessage(evs)
	if f == nil {
		f = schema.AssistantMessage("")
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, f)}, nil
}
func (w *regrantTwoPhaseShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w *regrantTwoPhaseShell) Name() string        { return w.name }
func (w *regrantTwoPhaseShell) Description() string { return "两阶段票据壳" }

// R13：**工具可见性**——显式授予之后，内层的 LLM 的 bind list 里到底有没有
// transfer_to_agent（对照 B8i：礼貌透传时深层是空的）。
func TestRegrant_R13_ToolVisibility(t *testing.T) {
	deep := &grantPeekModel{reply: schema.AssistantMessage("深层答完了")}
	deepAg := agent_v2.NewBasicAgent(
		agent_v2.WithName("deep"),
		agent_v2.WithModel(deep),
		agent_v2.WithMaxRounds(3),
	)
	fwd := &grantPeekModel{reply: schema.AssistantMessage("转发壳内层答完了")}
	fwdAg := agent_v2.NewBasicAgent(
		agent_v2.WithName("fwd_inner"),
		agent_v2.WithModel(fwd),
		agent_v2.WithMaxRounds(3),
	)
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("regranted"),
	).AddAgent("regranted", &regrantShell{name: "regranted", inner: deepAg}).
		AddAgent("forwarded", &grantForwardShell{name: "forwarded", inner: fwdAg})
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	h2 := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router2"),
		agent_v2.WithHandoffInitial("forwarded"),
	).AddAgent("forwarded", &grantForwardShell{name: "forwarded", inner: fwdAg}).
		AddAgent("regranted", &regrantShell{name: "regranted", inner: deepAg})
	if _, err := h2.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run2: %v", err)
	}
	t.Logf("R13 显式授予的内层 bind 到的工具：%v", deep.seenTools())
	t.Logf("R13 礼貌透传的内层 bind 到的工具：%v", fwd.seenTools())
	t.Logf("R13 说明：显式那侧应含 transfer_to_agent（LLM 看得见），透传那侧应为空")
}

// R14 是 self 自查的**哨兵**（R6c/R6d 只做对照，不设防）：拿着**别人的**授予调原语，
// 必须什么也不发生。摘掉 RegrantHandoffTo 里那句 grantFor(self) 时本例必红。
func TestRegrant_R14_CannotRegrantSomeoneElsesGrant(t *testing.T) {
	stash := &grantOptStash{}
	inner, innerModel := grantInnerTransfer("shared", "billing")
	shellA := &grantStashShell{name: "front", stash: stash, reply: "FRONT 处理不了", target: "helper"}
	shellB := &selfHonestReplayShell{name: "helper", stash: stash, inner: inner}
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
		agent_v2.WithHandoffMaxHops(5),
	).AddAgent("front", shellA).AddAgent("helper", shellB).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stash.n() == 0 {
		t.Fatalf("fixture 没搭对：第 0 跳的 opts 没被存下来")
	}
	t.Logf("R14 拿别人的授予去 regrant：stash=%d inner 模型=%d billing=%d",
		stash.n(), innerModel.count(), billingModel.count())
	if n := billingModel.count(); n != 0 {
		t.Errorf("授予被改寄了：helper 手上是 front 的那份授予，却把 inner 变成了受让方（billing 被真调 %d 次）", n)
	}
	// 前提：inner 真的**试过**转交。没试过的话上面那句恒真。
	// 「试过」在两种世界里长相不同：挡住了 = 软失败回灌后再答一次（模型 2 次）；
	// 没挡住 = 一调即出（模型 1 次 + billing>0）。两者都不是 ⟹ fixture 没搭对。
	if innerModel.count() < 2 && billingModel.count() == 0 {
		t.Fatalf("fixture 没搭对：inner 模型只被调 %d 次且 billing=0——它压根没发出那条 transfer，"+
			"上面那句断言在这种状态下恒真", innerModel.count())
	}
}

// selfHonestReplayShell 老老实实把**自己**当 self（它确实是本跳的候选），但手上那份
// opts 是**上一跳**的——授予不是发给它的。
type selfHonestReplayShell struct {
	name  string
	stash *grantOptStash
	inner agent_v2.Agent
}

func (w *selfHonestReplayShell) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	sub := agent_v2.RegrantHandoffTo(w.stash.first(), w, w.inner)
	evs, err := w.inner.Run(ctx, sess, in, sub...)
	if err != nil {
		return nil, err
	}
	f := agent_v2.FinalMessage(evs)
	if f == nil {
		f = schema.AssistantMessage("")
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, f)}, nil
}
func (w *selfHonestReplayShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w *selfHonestReplayShell) Name() string { return w.name }
func (w *selfHonestReplayShell) Description() string {
	return "诚实报 self 但拿的是别人授予的壳"
}
