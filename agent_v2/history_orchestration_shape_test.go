package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
)

// 编排形状矩阵——历史层的**跨形状出口契约**。
//
// # 为什么要这张表
//
// 会话历史的写入协调被改了五轮，每一轮都是「上一轮的修法在另一个形状上不成立」。第五轮
// 撤回的那次，症状是 `Handoff(Par(Seq(Basic)×3))` 从 5 条涨到 8 条、`Par(Handoff×3)` 从
// 6 条涨到 12 条——而当轮的测试全绿。原因是：**当时有测试的形状（`Seq(外部, Basic)` 和
// 「直接子里就有 BasicAgent」）在任何改法下都不变**，它们对写入协调根本不敏感。
//
// 这张表的价值不在于多测几个 case，而在于**把契约写死成一条对所有形状都成立的性质**，
// 下次再改写入协调时不需要预判「这次会漏哪个形状」——表里任何一行挂了都是契约破了。
//
// # 三条契约
//
//  1. **不重复**：每个执行体的产出在历史里恰好一条。重复不是「多几条」——同一条产出带两个
//     归属时，第二份会被判成「别人说的」走 Rewrite 变成 `For context: [X] said:`，模型会
//     以为另一个 agent 独立得出了相同结论。**污染比丢失重**。
//  2. **不丢失**：每个执行体的产出都在。多轮/多跳编排最容易在第 2 轮起静默丢。
//  3. **不泄漏**：`OnAgentStart` 改写过的输入，改写**之前**那条不得进历史。这是 [消费] 档
//     hook 的既有语义（脱敏是 CLAUDE.md 自己举的例子），边界补记握着的是闭包里改写前那条。
//
// # 输入份数是另一条轴，本表只刻画不断言
//
// 「同一条原始输入记几份」取决于**读回时有几条独立的流**：共享视图（转交）下所有执行体读
// 同一条流 ⟹ 一份；SelfOnly 扇出下每条分支流各自要完整（有问有答）⟹ 每分支一份。它跟
// 上面三条契约是正交的两回事，混在一起改就是「修 a 出 b」。本表用 t.Logf 记下实际份数，
// 已知不一致的形状在各自用例里点名，**不在这里顺手改**。
type shapeProbe struct {
	hs      agent_v2.HistoryStore
	convID  string
	ctx     context.Context
	tenant  string
	entries []*schema.Message
}

func newShapeProbe(t *testing.T, convID string) *shapeProbe {
	t.Helper()
	return &shapeProbe{
		hs:     agent_v2.NewInMemoryHistoryStore(),
		convID: convID,
		tenant: "t",
		ctx:    compose_v2.WithThreadIDCtx(context.Background(), convID),
	}
}

// leaf 造一个记历史的 BasicAgent；reply 是它的唯一产出。
func (p *shapeProbe) leaf(name, reply string) *agent_v2.BasicAgent {
	return agent_v2.NewBasicAgent(
		agent_v2.WithName(name),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage(reply)})),
		agent_v2.WithHistory(p.hs, p.tenant),
		agent_v2.WithMaxRounds(2))
}

func (p *shapeProbe) load(t *testing.T) {
	t.Helper()
	msgs, err := p.hs.Load(p.ctx, agent_v2.HistoryKey{Owner: p.tenant, ConvID: p.convID})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p.entries = msgs
}

// countOf 数**内容包含 sub** 的条数（产出物可能被编排包过一层前缀）。
func (p *shapeProbe) countOf(sub string) int {
	n := 0
	for _, m := range p.entries {
		if strings.Contains(m.Content, sub) {
			n++
		}
	}
	return n
}

func (p *shapeProbe) dump() string {
	var b strings.Builder
	fmt.Fprintf(&b, "历史共 %d 条：\n", len(p.entries))
	for i, m := range p.entries {
		owner, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		fmt.Fprintf(&b, "  [%d] %-9s owner=%-16s %s\n", i, m.Role, owner, strings.TrimSpace(m.Content))
	}
	return b.String()
}

// assertExactlyOnce 是契约 1+2 合起来：既不重复也不丢。
func (p *shapeProbe) assertExactlyOnce(t *testing.T, subs ...string) {
	t.Helper()
	for _, s := range subs {
		switch n := p.countOf(s); {
		case n == 0:
			t.Errorf("产出 %q 不在历史里——丢了\n%s", s, p.dump())
		case n > 1:
			t.Errorf("产出 %q 记了 %d 次——同一条产出多个归属，第二份会被当成别人说的走 Rewrite\n%s",
				s, n, p.dump())
		}
	}
}

// ── 形状 A：Handoff(Par(Basic×3)) ──
//
// 唯一一个「直接子里就有 BasicAgent」的形状——编排层能借到历史配置、写入器本来就是单个。
// 正因如此它对写入协调**不敏感**，历轮改动下都不变。**不能拿它当验收样本**，放进表里是
// 为了标记这条：它绿不代表任何事。
func TestHistShape_A_HandoffOverParallel(t *testing.T) {
	p := newShapeProbe(t, "shape-a")
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("br1", p.leaf("b1", "产出A1")).
		AddBranch("br2", p.leaf("b2", "产出A2")).
		AddBranch("br3", p.leaf("b3", "产出A3"))
	ho := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("front"), agent_v2.WithHandoffInitial("par"),
	).AddAgent("par", par)

	if _, err := ho.Run(p.ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	p.load(t)
	p.assertExactlyOnce(t, "产出A1", "产出A2", "产出A3")
	t.Logf("输入份数=%d（刻画，不断言）\n%s", p.countOf("原话"), p.dump())
}

// ── 形状 B：Handoff(Par(Seq(Basic)×3)) ──
//
// 中间夹了一层 Sequential ⟹ Parallel 的 subs 是 SequentialAgent，借不到历史配置。
// 实测重复来自这里：叶子记在 `front/br1/s1`，Sequential 的步骤边界查 `front/br1`，
// **精确匹配**查不到 → 再记一份 → 同一条产出两个归属 → 第二份走 Rewrite。
func TestHistShape_B_HandoffOverParallelOverSeq(t *testing.T) {
	p := newShapeProbe(t, "shape-b")
	mkSeq := func(n int) agent_v2.Agent {
		return agent_v2.NewSequentialAgent(
			agent_v2.WithSequentialName(fmt.Sprintf("seq%d", n))).
			AddStep("s1", p.leaf(fmt.Sprintf("b%d", n), fmt.Sprintf("产出B%d", n)))
	}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("br1", mkSeq(1)).AddBranch("br2", mkSeq(2)).AddBranch("br3", mkSeq(3))
	ho := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("front"), agent_v2.WithHandoffInitial("par"),
	).AddAgent("par", par)

	if _, err := ho.Run(p.ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	p.load(t)
	p.assertExactlyOnce(t, "产出B1", "产出B2", "产出B3")
	t.Logf("输入份数=%d（刻画，不断言）\n%s", p.countOf("原话"), p.dump())
}

// ── 形状 C：Par(Handoff×3) ──
//
// Parallel 的 subs 是三个 HandoffAgent ⟹ 借不到配置 ⟹ 现状下 Parallel 层没有写入器、
// 边界补记整个是 no-op。这是「重复」和「PII 泄漏」两条同时潜伏的地方：一旦编排层有了
// 写入器，Handoff 的步骤边界就会把叶子已经记过的产出再记一遍。
func TestHistShape_C_ParallelOverHandoff(t *testing.T) {
	p := newShapeProbe(t, "shape-c")
	mkHo := func(n int) agent_v2.Agent {
		return agent_v2.NewHandoffAgent(
			agent_v2.WithHandoffName(fmt.Sprintf("ho%d", n)),
			agent_v2.WithHandoffInitial("w"),
		).AddAgent("w", p.leaf(fmt.Sprintf("b%d", n), fmt.Sprintf("产出C%d", n)))
	}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("br1", mkHo(1)).AddBranch("br2", mkHo(2)).AddBranch("br3", mkHo(3))

	if _, err := par.Run(p.ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	p.load(t)
	p.assertExactlyOnce(t, "产出C1", "产出C2", "产出C3")
	// ⚠️ 这里的 3 **不是**「三条分支流各自完整」那种正确的 3。每个 Handoff 都是 Rewrite
	// 模式，每条分支读得到**所有**分支的消息，所以自己的 prompt 里原始输入就是 3 份——
	// 实测第二轮各分支 prompt 里「原始问题」×3、「第二问」×2~3。
	//
	// 根因是 nonce 的**铸造点**：它在 `HandoffAgent.Run` 里铸，而 Parallel 分发给三个
	// 分支时那条 input 还没有 nonce ⟹ 三个分支各铸一个、键都不同 ⟹ 任何基于 nonce 的
	// 去重都失效。写入器合成一个也解决不了，得把铸造点前移到扇出之前。
	// 见 issue-98 §9 #7（本用例只刻画不断言，等那条修了再收紧）。
	t.Logf("输入份数=%d（刻画，不断言；⚠️ 这个 3 是 §9 #7 的症状不是正确值）\n%s",
		p.countOf("原话"), p.dump())
}

// ── 形状 C′：Par(Handoff×3) + 叶子挂改写 hook ──
//
// 契约 3。`OnAgentStart` 是 [消费] 档，hook 把消息整条换掉（脱敏），叶子记的是脱敏版；
// 而边界补记握着的是**闭包里 hook 之前**那条、带原文。它按 nonce 扫历史判「记过没」——
// 叶子那份的 nonce 已经随着换消息丢了 ⟹ 判「没记过」⟹ **把原文写进去**。
func TestHistShape_C_RewriteHook_NoRawLeak(t *testing.T) {
	const raw, masked = "13800138000", "1***"
	p := newShapeProbe(t, "shape-c-pii")
	mkHo := func(n int) agent_v2.Agent {
		leaf := agent_v2.NewBasicAgent(
			agent_v2.WithName(fmt.Sprintf("b%d", n)),
			agent_v2.WithModel(newStubModel([]*schema.Message{
				schema.AssistantMessage(fmt.Sprintf("产出P%d", n))})),
			agent_v2.WithHistory(p.hs, p.tenant),
			agent_v2.WithHooks(&maskingHooks{from: raw, to: masked}),
			agent_v2.WithMaxRounds(2))
		return agent_v2.NewHandoffAgent(
			agent_v2.WithHandoffName(fmt.Sprintf("ho%d", n)),
			agent_v2.WithHandoffInitial("w"),
		).AddAgent("w", leaf)
	}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("br1", mkHo(1)).AddBranch("br2", mkHo(2)).AddBranch("br3", mkHo(3))

	if _, err := par.Run(p.ctx, agent_v2.NewRunSession(), schema.UserMessage("我的手机号是 "+raw)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	p.load(t)
	if n := p.countOf(raw); n > 0 {
		t.Errorf("未脱敏原文进了历史 %d 次——边界补记握的是 hook 之前那条\n%s", n, p.dump())
	}
	if p.countOf(masked) == 0 {
		t.Errorf("脱敏版也不在历史里——测试无效（hook 没生效或输入根本没记）\n%s", p.dump())
	}
}

// ── 形状 G：Seq(Basic + 脱敏 hook) ──
//
// C′ 那个形状今天恰好安全（Parallel 借不到配置 ⟹ 那层没有写入器 ⟹ 边界补记整个是 no-op），
// 所以它压不住判据本身。G 把编排层的写入器**确实建起来**（Sequential 的 subs 直接就是
// BasicAgent，借得到配置），边界真的会跑——这才是「输入按范围判」那一片的哨兵。
//
// 不用范围判会怎样：叶子记的是脱敏版（hook 换了消息，连 nonce 一起换掉了），边界握着的是
// 闭包里改写**之前**那条；按指针查——不是同一条；按 nonce 查历史——nonce 已经没了。
// 三个判据里只有「这一步的执行体已经记过东西了」拦得住。
func TestHistShape_G_SeqLeafRewriteHook_NoRawLeak(t *testing.T) {
	const raw, masked = "13800138000", "1***"
	p := newShapeProbe(t, "shape-g")
	leaf := agent_v2.NewBasicAgent(
		agent_v2.WithName("worker"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("产出G")})),
		agent_v2.WithHistory(p.hs, p.tenant),
		agent_v2.WithHooks(&maskingHooks{from: raw, to: masked}),
		agent_v2.WithMaxRounds(2))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("flow")).AddStep("s1", leaf)

	if _, err := seq.Run(p.ctx, agent_v2.NewRunSession(), schema.UserMessage("我的手机号是 "+raw)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	p.load(t)
	if n := p.countOf(raw); n > 0 {
		t.Errorf("未脱敏原文进了历史 %d 次——边界补记握的是 hook 改写之前那条\n%s", n, p.dump())
	}
	if p.countOf(masked) == 0 {
		t.Errorf("脱敏版不在历史里——测试无效（hook 没生效或输入根本没记）\n%s", p.dump())
	}
	p.assertExactlyOnce(t, "产出G")
}

// maskingHooks 模拟业务的脱敏 hook：OnAgentStart 是 [消费] 档，改写 input.Messages 生效。
type maskingHooks struct {
	hook.NoOpHooks
	from, to string
}

func (h *maskingHooks) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	for i, m := range in.Messages {
		if strings.Contains(m.Content, h.from) {
			in.Messages[i] = schema.UserMessage(strings.ReplaceAll(m.Content, h.from, h.to))
		}
	}
	return ctx, nil
}

// ── 形状 D：Par(GraphAgent(Basic)×3) ──
//
// GraphAgent 的子 agent 由业务 NodeFunc 经 RunSubAgent 调起，而 graph_agent.go 里没有
// 任何历史钩子 ⟹ 框架在那一层没有落点。Parallel 的分支边界查 `br1`，叶子记在 `br1/gsub`。
func TestHistShape_D_ParallelOverGraphAgent(t *testing.T) {
	p := newShapeProbe(t, "shape-d")
	inCh := compose_v2.LastValue[*schema.Message]("__d_in__")
	outCh := compose_v2.LastValue[*schema.Message]("__d_out__")

	mkGA := func(n int) agent_v2.Agent {
		leaf := p.leaf(fmt.Sprintf("b%d", n), fmt.Sprintf("产出D%d", n))
		bld := compose_v2.NewBuilder().WithName(fmt.Sprintf("gd%d", n))
		compose_v2.WithChannel(bld, inCh)
		compose_v2.WithChannel(bld, outCh)
		bld.AddNode("call", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			in, _ := compose_v2.Get(chs, inCh)
			evs, err := agent_v2.RunSubAgent(ctx, "gsub", leaf, in)
			if err != nil {
				return nil, err
			}
			return []compose_v2.ChannelWrite{compose_v2.Write(outCh, agent_v2.FinalMessage(evs))}, nil
		})
		bld.AddEdge(compose_v2.StartNode, "call")
		bld.AddEdge("call", compose_v2.EndNode)
		g, err := bld.Compile()
		if err != nil {
			t.Fatalf("Compile: %v", err)
		}
		return agent_v2.NewGraphAgent(g,
			agent_v2.WithGraphAgentName(fmt.Sprintf("ga%d", n)),
			agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
				return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
			}),
			agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
				out, _ := compose_v2.Get(chs, outCh)
				return out
			}))
	}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("br1", mkGA(1)).AddBranch("br2", mkGA(2)).AddBranch("br3", mkGA(3))

	if _, err := par.Run(p.ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	p.load(t)
	p.assertExactlyOnce(t, "产出D1", "产出D2", "产出D3")
	t.Logf("输入份数=%d（刻画，不断言）\n%s", p.countOf("原话"), p.dump())
}

// ── 形状 E：Seq(外部执行体, Basic) ──
//
// 边界补记存在的**唯一**理由。外部执行体（远程 / 自研 / 别的框架的壳）什么也不留，这一步
// 的输入与产出必须由边界补上。E 和 A 一样属于「历轮都不变」的形状——它是既有回归覆盖到的
// 那个，放进表里是为了保证换判据时不把它弄丢。
func TestHistShape_E_SeqWithExternal(t *testing.T) {
	p := newShapeProbe(t, "shape-e")
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("flow")).
		AddStep("remote", &externalAgent{name: "remoteA"}).
		AddStep("local", p.leaf("localB", "本地结论"))

	if _, err := seq.Run(p.ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	p.load(t)
	p.assertExactlyOnce(t, "外部产出", "本地结论")
	if p.countOf("原话") == 0 {
		t.Errorf("用户原话没进历史——第一步交给了外部执行体，没人记它\n%s", p.dump())
	}
	t.Logf("输入份数=%d（刻画）\n%s", p.countOf("原话"), p.dump())
}

// ── 形状 F：Par(同一个 *BasicAgent 实例 ×3) ──
//
// best-of-N（ISSUE-80 特意恢复的能力）。三条分支流靠**位置**区分，各自完整、互不可见。
// 归属键一旦换成挂在实例上的东西（scope），三条流就会合并——而且实测是不确定的合并。
// 本用例守的是「位置作归属」这条不能被换掉。
func TestHistShape_F_ParallelSharedInstance(t *testing.T) {
	p := newShapeProbe(t, "shape-f")
	var n atomic.Int64
	shared := agent_v2.NewBasicAgent(
		agent_v2.WithName("shared"),
		agent_v2.WithModel(&counterModel{n: &n}),
		agent_v2.WithHistory(p.hs, p.tenant),
		agent_v2.WithMaxRounds(2))
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("br1", shared).AddBranch("br2", shared).AddBranch("br3", shared)

	if _, err := par.Run(p.ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	p.load(t)

	// 三条分支流各自要有产出，且归属互不相同——合并了就是 best-of-N 的独立性没了。
	owners := map[string]int{}
	for _, m := range p.entries {
		if m.Role != schema.RoleAssistant {
			continue
		}
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		owners[o]++
	}
	if len(owners) != 3 {
		t.Errorf("三条分支流的归属应互不相同（got %v）——合并后 best-of-N 的采样不再独立\n%s",
			owners, p.dump())
	}
	for o, c := range owners {
		if c != 1 {
			t.Errorf("归属 %q 下有 %d 条产出，应恰好 1 条\n%s", o, c, p.dump())
		}
	}
}

// counterModel 是并发安全的产出源——形状 F 三条分支共享同一个 agent 实例，也就共享 model。
type counterModel struct{ n *atomic.Int64 }

func (m *counterModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(fmt.Sprintf("产出F%d", m.n.Add(1))), nil
}
func (m *counterModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *counterModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// ── 形状 Loop：Seq(Loop(外部执行体, 3 轮), Basic) ──
//
// 位置判据「太粗」的那一面：Loop 三轮共用同一个位置 `body`，第 1 轮记过之后第 2/3 轮被
// 挡掉 ⟹ 中间轮次的产出静默丢。多跳 Handoff 是同一个形状。
func TestHistShape_Loop_MultiRoundExternal(t *testing.T) {
	p := newShapeProbe(t, "shape-loop")
	body := &roundExternalAgent{name: "extBody"}
	lp := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("lp"),
		agent_v2.WithLoopMaxIterations(3)).WithBody(body)
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("flow")).
		AddStep("s0", lp).
		AddStep("s1", p.leaf("localB", "本地结论"))

	if _, err := seq.Run(p.ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	p.load(t)
	if got := body.rounds.Load(); got != 3 {
		t.Fatalf("body 只跑了 %d 轮，测试无效（要 3 轮才能验中间轮次丢没丢）", got)
	}
	p.assertExactlyOnce(t, "轮次产出1", "轮次产出2", "轮次产出3", "本地结论")
	t.Logf("输入份数=%d（刻画）\n%s", p.countOf("原话"), p.dump())
}

// roundExternalAgent 每轮产出可区分的内容——不可区分就看不出丢的是哪一轮。
type roundExternalAgent struct {
	name   string
	rounds atomic.Int64
	mu     sync.Mutex
}

func (a *roundExternalAgent) Run(_ context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := schema.AssistantMessage(fmt.Sprintf("轮次产出%d", a.rounds.Add(1)))
	sess.AddEvent(agent_v2.NewExitEvent(a.name, out))
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, out)}, nil
}

func (a *roundExternalAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *roundExternalAgent) Name() string        { return a.name }
func (a *roundExternalAgent) Description() string { return "每轮产出可区分的外部执行体" }
