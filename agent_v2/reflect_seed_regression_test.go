package agent_v2_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// recordingActor 记下每次被调用时收到的输入。
type recordingActor struct {
	mu      sync.Mutex
	seen    []string
	replies []*schema.Message
	idx     int
	haltAt  int // 第几次调用抛中断（1-based，0=不抛）
}

func (a *recordingActor) Name() string        { return "actor" }
func (a *recordingActor) Description() string { return "记录输入" }

func (a *recordingActor) Run(_ context.Context, _ *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.mu.Lock()
	a.seen = append(a.seen, in.Content)
	n := len(a.seen)
	a.mu.Unlock()
	if n == a.haltAt {
		return nil, &compose_v2.InterruptError{Reason: "actor_ask",
			Query: &compose_v2.AskQuery{Question: "?", InterruptID: "actor_q"}}
	}
	r := a.replies[min(a.idx, len(a.replies)-1)]
	a.idx++
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent("actor", r)}, nil
}

func (a *recordingActor) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// # 为什么 9 个编排里只有这一格（+ GraphAgent 一格）
//
// 「续跑时不重 apply 种子」是引擎级语义，9 个编排共用。但**共用不产生断言**，所以问题
// 是：谁真的会因为它退化？判据不是「这个编排有没有用播种」，而是**「它播的那条 channel
// 图内有没有人改写」**——没人改写的话，重复 apply 只是拿同值覆盖 LastValue，no-op。
//
// 逐条数过（`compose_v2.Write(<该 channel>` 在对应 builder 里的出现次数）：
//
//	sequential / parallel / loop / routing  播的 original|input channel  改写 0 次  ⟹ 不依赖
//	reflect  reflectActorInputCh                                         改写 1 次  ⟹ **依赖**（本格）
//	handoff  handoffAgentActiveCh                                        改写 1 次  ⟹ 依赖但被遮住
//
// Handoff 那条值得单独记：它**确实**依赖，但种子值 `startActive` 每次从 `store.Store`
// 现算，而转交当场就把 active 写进了 store ⟹ 重入时种子值已经等于演化后的值，重复
// apply 仍是 no-op。**这层遮蔽来自另一个子系统**——哪天转交改成不当场落 store，这条
// 依赖就会裸出来而**没有任何哨兵接着**。
//
// ⚠️ 别照着「9 个编排都用了播种」的直觉给每个编排补一格：实测写过 Loop / Handoff 两格，
// 在「续跑也 apply 种子」的变异下**两格都不红**——恒真的假哨兵比没有更糟，它的 godoc
// 在撒谎。判据永远是「这个 bug 真发生时，我断言的这个量会变吗」。
//
// TestReflect_SeedDoesNotClobberFeedbackOnResume 钉住「续跑时不能把已被节点改写的
// 输入回退成原始输入」。
//
// # 这条守的是什么
//
// 反思编排给 actor 的输入是一条 `LastValue` channel，而 **feedback 节点会改写它**
// （写成「原目标 + 上轮回答 + **评审反馈**」）。播种若用**「两条路径都 apply」那个
// 语义**（现在是不导出的 `withImportOverlay`，子图独家），嵌套续跑时会二次 apply、
// 把它覆写回**原始 input**。
//
// 后果是**静默退化**：重跑那一轮的 actor 看不见批评意见，等于把上一轮原样再做一遍，
// 而 `err == nil`、没有任何告警。实测过：
//
//	[Run]    actor 第 2 次收到: "原始目标：…上一轮你的回答：…评审反馈：…"
//	[Resume] actor 第 3 次收到: "原始目标XYZ"        ← 反馈没了
//
// 修法是播种改走 `WithInitialWrites`（只在新开路径 apply）。
//
// ⚠️ **必须嵌套**（顶层 Sequential 包着）——顶层 Run 会 `WithFreshRun` 跳过 auto-load，
// 二次 apply 的窗口不存在，那样这条用例恒真。
func TestReflect_SeedDoesNotClobberFeedbackOnResume(t *testing.T) {
	actor := &recordingActor{
		replies: []*schema.Message{schema.AssistantMessage("答复#1"), schema.AssistantMessage("答复#2")},
		haltAt:  2, // 第 2 次（拿到反馈那次）中断，Resume 后重跑它
	}
	round := 0
	rf := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("rf"),
		agent_v2.WithReflectMaxRounds(3),
		agent_v2.WithReflector(func(_ context.Context, _, _ *schema.Message) (*agent_v2.ReflectOpinion, error) {
			round++
			if round == 1 {
				return &agent_v2.ReflectOpinion{GoalMatched: false, Critique: "太浅了，展开讲"}, nil
			}
			return &agent_v2.ReflectOpinion{GoalMatched: true}, nil
		}),
	).WithActor(actor)

	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("outer")).AddStep("r", rf)

	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	_, err := seq.Run(ctx, sess, schema.UserMessage("原始目标XYZ"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应中断在 actor 第 2 次，got %v", err)
	}
	if _, err = seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("ans"),
		})); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	actor.mu.Lock()
	seen := append([]string(nil), actor.seen...)
	actor.mu.Unlock()
	t.Logf("actor 历次收到：%q", seen)

	if len(seen) < 3 {
		t.Fatalf("Resume 应让 actor 重跑一次，实际只被调 %d 次：%q", len(seen), seen)
	}
	// 承重：重跑那次必须**仍带着评审反馈**
	last := seen[len(seen)-1]
	if !strings.Contains(last, "太浅了") {
		t.Fatalf("Resume 重跑时 actor 的输入被回退成原始输入了——评审反馈没了。\n"+
			"实际收到：%q\n（播种若走 WithInitialWrites 就会二次 apply 覆写这条 LastValue）", last)
	}
}
