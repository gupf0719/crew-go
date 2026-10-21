package agent_v2_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// Run 和 Resume 两个入口必须**对称**地建历史写入器。
//
// 只在 Run 里建时的形状：中断之后 Resume，图重跑到某一步，边界补记去 ctx 里取写入器取到
// nil，直接 return——那一步的输入和产出永久缺席。而叶子 BasicAgent 因为在自己的 Run 里
// 兜底建了一个私有写入器，它那部分还在，所以历史不是空的、只是**缺了一半**，看不出异常。
//
// 用外部执行体（不是 BasicAgent、不会自己记）才能暴露：它那一步完全依赖边界补记。
//
// **sanity-revert**：删掉任一编排 Resume 入口的 startHistoryForOrchestration，本测试
// FAIL 并报缺了哪条。
func TestHistory_RunResumeEntrySymmetry(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	cp := compose_v2.NewInMemoryCheckpointer()

	ext := &histInterruptOnceAgent{name: "extStep"}
	local := agent_v2.NewBasicAgent(
		agent_v2.WithName("localB"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("本地结论")})),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("flow")).
		AddStep("s0", ext).
		AddStep("s1", local)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-sym")
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	sess := agent_v2.NewRunSession()

	_, err := seq.Run(ctx, sess, schema.UserMessage("用户原话"))
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("第一次 Run 应中断（测试要靠中断把 s0 推到 Resume 那一段跑）；实际 err=%v", err)
	}

	if _, err := seq.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("答案"),
		})); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	msgs, err := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-sym"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var dump strings.Builder
	fmt.Fprintf(&dump, "历史共 %d 条：\n", len(msgs))
	for i, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		fmt.Fprintf(&dump, "  [%d] %-9s owner=%-10s %s\n", i, m.Role, o, strings.TrimSpace(m.Content))
	}
	has := func(sub string) bool {
		for _, m := range msgs {
			if strings.Contains(m.Content, sub) {
				return true
			}
		}
		return false
	}
	// 外部那步整个跑在 Resume 那一段——写入器只在 Run 里建的话它这两条都不会有。
	if !has("用户原话") {
		t.Errorf("用户原话缺席——Resume 入口没建写入器，边界补记取到 nil\n%s", dump.String())
	}
	if !has("外部产出") {
		t.Errorf("外部执行体的产出缺席——Resume 入口没建写入器\n%s", dump.String())
	}
	// 对照组：叶子自己兜底建过写入器，它那份在不在跟本 fix 无关——用来证明上面两条
	// 的缺席不是「历史整个没开」。
	if !has("本地结论") {
		t.Fatalf("连叶子自己记的都没有——测试无效（历史根本没开起来）\n%s", dump.String())
	}
}

// histInterruptOnceAgent 第一次 Run 抛中断，Resume 时产出——把这一步推到 Resume 那一段执行。
type histInterruptOnceAgent struct {
	name string
	done atomic.Bool
}

func (a *histInterruptOnceAgent) Run(_ context.Context, sess *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	if a.done.CompareAndSwap(false, true) {
		return nil, &compose_v2.InterruptError{
			Reason: "need input",
			Query:  &compose_v2.AskQuery{InterruptID: "ext-q1", Question: "确认？"},
		}
	}
	out := schema.AssistantMessage("【外部产出】" + strings.TrimSpace(in.Content))
	sess.AddEvent(agent_v2.NewExitEvent(a.name, out))
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, out)}, nil
}

func (a *histInterruptOnceAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *histInterruptOnceAgent) Name() string { return a.name }
func (a *histInterruptOnceAgent) Description() string {
	return "第一次中断、之后正常产出的外部执行体"
}

// Resume 建的是**新写入器**，`recorded` 集合跟着重置 ⟹ Run 1 里已经记过的东西，
// 在 Resume 那一段如果又被走到一遍，指针判据拦不住（那是同一个 Run 内的机制）。
//
// 这条验的是：前一步在 Run 1 里完成并记录，后一步中断；Resume 之后前一步的产出
// 仍然只有一份。靠的是「完成的节点不重跑」——本用例把这个前提钉死，哪天超步调度
// 改了让完成的步骤也重跑，这里会红。
func TestHistory_ResumeDoesNotReRecordCompletedStep(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	cp := compose_v2.NewInMemoryCheckpointer()

	local := agent_v2.NewBasicAgent(
		agent_v2.WithName("localB"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("本地结论")})),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("flow")).
		AddStep("s0", &externalAgent{name: "extEarly"}).         // Run 1 里完成并被边界记下
		AddStep("s1", &histInterruptOnceAgent{name: "extLate"}). // Run 1 里中断
		AddStep("s2", local)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-rerecord")
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	sess := agent_v2.NewRunSession()

	_, err := seq.Run(ctx, sess, schema.UserMessage("用户原话"))
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("第一次 Run 应在 s1 中断；实际 err=%v", err)
	}
	if _, err := seq.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("答案"),
		})); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	msgs, err := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-rerecord"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	counts := map[string]int{}
	var dump strings.Builder
	for i, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		fmt.Fprintf(&dump, "\n  [%d] %-9s owner=%-8s %s", i, m.Role, o, strings.TrimSpace(m.Content))
		counts[strings.TrimSpace(m.Content)]++
	}
	// s0 的产出在 Run 1 里记过；Resume 那一段不该再记一份。
	for c, n := range counts {
		if n > 1 {
			t.Errorf("%q 在跨 Run/Resume 之后出现 %d 次——Resume 换了新写入器，"+
				"指针判据管不到前一个 Run 记过的东西。历史共 %d 条：%s", c, n, len(msgs), dump.String())
		}
	}
	if counts["【外部产出】用户原话"] == 0 {
		t.Errorf("s0 的产出不见了——测试无效。历史共 %d 条：%s", len(msgs), dump.String())
	}
}

// 并发扇出里一支中断、Resume 之后，历史必须跟「没中断直接跑完」一模一样。
//
// 三道输入闸门跨 Run **全部**失效：`producedHistoryHere` 是 per-call 回执（Resume 新开）、
// 认领表是 per-Run 写入器、`inputAlreadyInHistorySlot` 要 nonce 而 nonce 只有 HandoffAgent 打。
// 所以只要 Run 1 里有分支「被别人挡掉所以没记」，Resume 时挡它的人不在了，它就会补记一份
// ——历史比基线多一条，而且没有任何报错。
//
// 这条同时把「每条分支流各记各的」跟「跨 Run 不多记」两个诉求钉在一起：前者要求 Run 1 里
// 四条分支都记下自己的输入（不是互相挡），后者才有可能成立。
func TestHistory_FanoutInterruptResume_SameAsBaseline(t *testing.T) {
	base := fanoutHistoryShape(t, false)
	withResume := fanoutHistoryShape(t, true)
	if base != withResume {
		t.Errorf("中断 + Resume 之后历史跟基线不一致：\n基线：\n%s\n中断+Resume：\n%s",
			base, withResume)
	}
	if !strings.Contains(base, "用户原话") {
		t.Fatalf("基线里没有用户原话——测试无效：\n%s", base)
	}
}

// fanoutHistoryShape 跑一次 Par(外部, 外部, 中断一次的, 本地)，返回归一化后的历史形状
// （按 owner+role+content 排序，避免并发顺序噪声）。
func fanoutHistoryShape(t *testing.T, interrupt bool) string {
	t.Helper()
	hs := agent_v2.NewInMemoryHistoryStore()
	cp := compose_v2.NewInMemoryCheckpointer()
	conv := "conv-fanout-resume"
	if interrupt {
		conv += "-ir"
	}
	local := agent_v2.NewBasicAgent(
		agent_v2.WithName("localB"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("本地结论")})),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	var third agent_v2.Agent = &externalAgent{name: "e3"}
	if interrupt {
		third = &histInterruptOnceAgent{name: "e3"}
	}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("br1", &externalAgent{name: "e1"}).
		AddBranch("br2", &externalAgent{name: "e2"}).
		AddBranch("br3", third).
		AddBranch("br4", local)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), conv)
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	sess := agent_v2.NewRunSession()
	_, err := par.Run(ctx, sess, schema.UserMessage("用户原话"))
	if interrupt {
		ie, isInt := compose_v2.IsInterrupt(err)
		if !isInt {
			t.Fatalf("br3 应中断；实际 err=%v", err)
		}
		if _, err := par.Resume(ctx, sess.RunID,
			agent_v2.WithResumeUserInputs(map[string]*schema.Message{
				ie.Query.InterruptID: schema.UserMessage("答案"),
			})); err != nil {
			t.Fatalf("Resume: %v", err)
		}
	} else if err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs, err := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: conv})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	lines := make([]string, 0, len(msgs))
	for _, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		lines = append(lines, fmt.Sprintf("  %-9s owner=%-8s %s", m.Role, o, strings.TrimSpace(m.Content)))
	}
	sort.Strings(lines) // 并发下顺序本来就不定，比的是多重集
	return strings.Join(lines, "\n")
}
