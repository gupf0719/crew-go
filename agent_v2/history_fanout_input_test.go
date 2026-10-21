package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// SelfOnly 扇出下每条分支流各自要完整（有问有答），**且份数跨次稳定**。
//
// # 为什么不能用「这条消息全局记过没有」判输入
//
// 并发的分支共享同一条输入指针。用全局判就是「谁先谁赢」——先跑到的分支记下输入，其余
// 分支看到「记过了」跳过，于是它们的流里只有答、没有问。而谁先跑到取决于调度：实测
// `Par(外部×3 + 本地)` 连跑 40 轮，份数在 1/2/3/4 之间乱跳（`-race` 下更散）。
//
// 按**流**（位置）判之后，兄弟分支是不同的流，各记各的；Loop 多轮是同一条流，第 N 轮的
// 输入就是第 N-1 轮的产出、命中跳过。两个诉求各由各的键管。
//
// # 为什么查和登记必须一次做完
//
// 「先查后写」中间有窗口：同一条流上两个并发的边界（同 nodeID 的多实例扇出）会双双查到
// 「没记过」然后都记。认领在写入器的锁里做，那是这条流上唯一天然串行的点——跟
// `claimRootInput` 同一个形状。
//
// **sanity-revert**：把 `claimRecordHere(slot, input)` 换回全局的 `claimRecord(input)`，
// 本测试会 FAIL 并报出份数分布。
func TestHistory_FanoutSelfOnly_InputPerBranchStable(t *testing.T) {
	const rounds = 30
	dist := map[int]int{}
	for i := 0; i < rounds; i++ {
		dist[runFanoutInputProbe(t, i)]++
	}
	if len(dist) != 1 || dist[4] != rounds {
		t.Errorf("SelfOnly 扇出下 4 条分支流各该有一份用户原话，且跨次稳定。\n"+
			"实际份数分布 %v（跑了 %d 轮）——按全局判就会变成谁先谁赢，随调度翻", dist, rounds)
	}
}

// runFanoutInputProbe 跑一次 Par(外部×3 + 本地)，返回历史里用户原话的份数。
func runFanoutInputProbe(t *testing.T, seq int) int {
	t.Helper()
	hs := agent_v2.NewInMemoryHistoryStore()
	conv := fmt.Sprintf("conv-fanout-%d", seq)
	local := agent_v2.NewBasicAgent(
		agent_v2.WithName("localB"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("本地结论")})),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("br1", &externalAgent{name: "e1"}).
		AddBranch("br2", &externalAgent{name: "e2"}).
		AddBranch("br3", &externalAgent{name: "e3"}).
		AddBranch("br4", local)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), conv)
	if _, err := par.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("用户原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs, err := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: conv})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	n := 0
	for _, m := range msgs {
		if m.Role == schema.RoleUser && strings.Contains(m.Content, "用户原话") {
			n++
		}
	}
	return n
}

// 对照组：同一条流上（Loop 多轮共用位置 `body`）**不能**每轮再记一份输入。
//
// 没有这一条，上面那个用例可以靠「输入判据整个删掉」通过——那样 Loop 第 N 轮会把第 N-1
// 轮的产出当新输入再记一遍。两条一起才把「按流分」这个判据钉死。
func TestHistory_LoopSameSlot_InputNotReRecorded(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	body := &roundExternalAgent{name: "extBody"}
	lp := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("lp"),
		agent_v2.WithLoopMaxIterations(3)).WithBody(body)
	// 外部 body 交不出历史配置，套一层拿得到配置的编排把写入器建起来。
	local := agent_v2.NewBasicAgent(
		agent_v2.WithName("localB"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("本地结论")})),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("flow")).
		AddStep("s0", lp).AddStep("s1", local)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-loop-slot")
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("用户原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs, err := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-loop-slot"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if body.rounds.Load() != 3 {
		t.Fatalf("body 只跑了 %d 轮，测试无效", body.rounds.Load())
	}
	counts := map[string]int{}
	var dump strings.Builder
	for i, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		fmt.Fprintf(&dump, "\n  [%d] %-9s owner=%-10s %s", i, m.Role, o, strings.TrimSpace(m.Content))
		counts[strings.TrimSpace(m.Content)]++
	}
	for c, n := range counts {
		if n > 1 {
			t.Errorf("%q 在同一条流里记了 %d 次——Loop 第 N 轮的输入就是第 N-1 轮的产出，"+
				"同一条流上不该再记一份。历史：%s", c, n, dump.String())
		}
	}
}

// 编排把上一轮的 assistant 产出转手当下一轮的输入，**不算「用户说了句新的话」**。
//
// 这条用例的立论**变过一次，如实记下**：原来 `HandoffAgent` 入口会浅拷贝消息并重打
// nonce，于是「新指针 + 新 nonce」让指针判据和 nonce 判据双双落空，只有 `Role == user`
// 挡得住。消息身份改成构造期铸造之后（见 `rootInputID`），那次拷贝和重打都删了，身份
// 跨层存活 ⟹ **按身份的认领已经接管了这个形状**。
//
// ⚠️ 所以本用例现在**不是** `Role == user` 那行的哨兵——实测撤掉那行本用例照过。它现在
// 守的是「上一轮产出转手当下一轮输入时不重记」这条性质本身，由哪道判据兜住是实现细节。
func TestHistory_PrevOutputAsInput_NotRecordedAcrossCopy(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	body := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("ho"), agent_v2.WithHandoffInitial("w"),
	).AddAgent("w", &roundExternalAgent{name: "extW"})
	lp := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("lp"),
		agent_v2.WithLoopMaxIterations(2)).WithBody(body)
	local := agent_v2.NewBasicAgent(
		agent_v2.WithName("localB"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("本地结论")})),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("flow")).
		AddStep("s0", lp).AddStep("s1", local)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-prevout")
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("用户原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs, err := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-prevout"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	counts := map[string]int{}
	var dump strings.Builder
	for i, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		fmt.Fprintf(&dump, "\n  [%d] %-9s owner=%-16s %s", i, m.Role, o, strings.TrimSpace(m.Content))
		counts[strings.TrimSpace(m.Content)]++
	}
	for c, n := range counts {
		if n > 1 {
			t.Errorf("%q 记了 %d 次——上一轮的产出被当成这一轮的新输入又记一遍。历史：%s",
				c, n, dump.String())
		}
	}
	if counts["轮次产出2"] == 0 {
		t.Errorf("第 2 轮的产出不在历史里——测试无效。历史：%s", dump.String())
	}
}

// 执行体每轮返回**同一个消息对象**时，各轮的产出都要记下来。
//
// `var okMsg = schema.AssistantMessage(...)` 然后每次返回它，在 Go 里是很自然的写法。
// 产出判据若只问「这条消息全局记过没有」，就把「同一个对象」当成「同一条产出」，
// 第 2 轮起静默丢——正是这套改动要消灭的形状换了个触发条件。
//
// 加了「这次调用范围内确实记过东西」这道前提之后：外部执行体什么都没记 ⟹ 范围为空
// ⟹ 不查全局表 ⟹ 每轮照记；而框架自己的 BasicAgent 记过 ⟹ 范围非空 ⟹ 查表跳过，
// B/C/D 那些形状的去重不受影响。
//
// **sanity-revert**：把产出判据换回纯全局的 `!w.alreadyRecorded(final)`，本测试 FAIL。
func TestHistory_ExecutorReusesMessageObject_AllRoundsRecorded(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	body := &constMsgAgent{name: "constBody", out: schema.AssistantMessage("恒定产出")}
	lp := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("lp"),
		agent_v2.WithLoopMaxIterations(3)).WithBody(body)
	local := agent_v2.NewBasicAgent(
		agent_v2.WithName("localB"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("本地结论")})),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("flow")).
		AddStep("s0", lp).AddStep("s1", local)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-constmsg")
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("用户原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := body.rounds.Load(); got != 3 {
		t.Fatalf("body 只跑了 %d 轮，测试无效", got)
	}
	msgs, err := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-constmsg"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	n := 0
	var dump strings.Builder
	for i, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		fmt.Fprintf(&dump, "\n  [%d] %-9s owner=%-10s %s", i, m.Role, o, strings.TrimSpace(m.Content))
		if strings.Contains(m.Content, "恒定产出") {
			n++
		}
	}
	if n != 3 {
		t.Errorf("body 跑了 3 轮，历史里却只有 %d 条产出——执行体每轮返回同一个消息对象时，"+
			"按指针判会把「同一个对象」当成「同一条产出」。历史：%s", n, dump.String())
	}
}

// constMsgAgent 每轮返回**同一个** *schema.Message 对象。
type constMsgAgent struct {
	name   string
	out    *schema.Message
	rounds atomic.Int64
}

func (a *constMsgAgent) Run(_ context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.rounds.Add(1)
	sess.AddEvent(agent_v2.NewExitEvent(a.name, a.out))
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, a.out)}, nil
}

func (a *constMsgAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *constMsgAgent) Name() string        { return a.name }
func (a *constMsgAgent) Description() string { return "每轮返回同一个消息对象的执行体" }
