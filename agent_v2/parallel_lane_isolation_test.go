package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// 并行分支之间互不可见：分支的上文是它开始执行那一刻编排会话的事件，去掉兄弟分支提交上来的——兄弟的看不到，
// 开始之前写进会话的其余事件（编排钩子写的等）照常看得到，跟 Sequential 的成员一样。不论这次并行跑在顶层会话上，
// 还是作为编排成员跑在成员的暂存 lane 上；也不论分支开始得早晚。
//
// 曾经两条路看得到兄弟：lane 读祖先 lane 是实时读（并行作为成员时，分支 lane 的祖先是成员的暂存 lane，兄弟提交
// 进去就读得到，5f5099f9 给成员加暂存 lane 之后才出现）；分支 lane 在各分支开始时才派生（开始得晚的分支派生时
// 兄弟可能已经提交了，改动前就有）。两个分支的 agent 同名时（同一个实例放进两个分支是支持的用法），兄弟的事件
// 被当成自己的上文。

const laneQ = "Q"

// laneWait 等 ch 关闭，超时报错（同步写错时让测试失败而不是挂住）。
func laneWait(ch <-chan struct{}) error {
	select {
	case <-ch:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("等兄弟分支结束超时")
	}
}

// siblingGate 是编排钩子：分支 a 的节点结束时（它的事件已经提交进编排会话）关掉 aDone；holdB 时分支 b 的节点
// 等到那一刻才开始。
type siblingGate struct {
	agent_v2.NoOpOrchestrationHooks
	aDone chan struct{}
	once  sync.Once
	holdB bool
}

func newSiblingGate(holdB bool) *siblingGate {
	return &siblingGate{aDone: make(chan struct{}), holdB: holdB}
}

func (g *siblingGate) OnNodeStart(ctx context.Context, nodeID string) (context.Context, error) {
	if g.holdB && nodeID == "worker_b" {
		return ctx, laneWait(g.aDone)
	}
	return ctx, nil
}

func (g *siblingGate) OnNodeEnd(ctx context.Context, nodeID string, _ error) context.Context {
	if nodeID == "worker_a" {
		g.once.Do(func() { close(g.aDone) })
	}
	return ctx
}

// waitStartHooks 让 agent 在 OnAgentStart 里等 ch 关闭：这时它的 lane 已经派生、上文还没读。
type waitStartHooks struct {
	hook.NoOpHooks
	ch <-chan struct{}
}

func (h waitStartHooks) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	return ctx, laneWait(h.ch)
}

// assertNoSibling 断言分支 b 的模型输入里没有兄弟 a 的问答：问题只出现一次（b 自己这一问），没有 a 的答复。
func assertNoSibling(t *testing.T, in []*schema.Message) {
	t.Helper()
	if len(in) == 0 {
		t.Fatal("分支 b 的模型没被调用")
	}
	if got := countUserText(in, laneQ); got != 1 {
		t.Errorf("分支 b 的上文里问题该只出现一次（自己那一问），实际 %d 次：%+v", got, in)
	}
	for _, m := range in {
		if m.Content == "A-ans" {
			t.Errorf("兄弟分支 a 的答复出现在分支 b 的上文里：%+v", in)
		}
	}
}

func sameNameWorkers(bHooks hook.Hooks) (a, b agent_v2.Agent, mb *scriptModel) {
	ma := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage("A-ans"))}}
	mb = &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage("B-ans"))}}
	a = agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithModel(ma))
	opts := []agent_v2.Option{agent_v2.WithName("worker"), agent_v2.WithModel(mb)}
	if bHooks != nil {
		opts = append(opts, agent_v2.WithHooks(bHooks))
	}
	b = agent_v2.NewBasicAgent(opts...)
	return a, b, mb
}

// 并行作为 Sequential 的成员：分支 b 派生 lane 之后、读上文之前，分支 a 跑完并提交。b 的上文里不该有 a 的问答。
//
// sanity-revert：lane 实时读祖先 lane ⟹ b 的模型输入里出现 A-ans，本用例红（审计实测）。
func TestParallelAsMember_BranchDoesNotSeeSiblingCommittedLater(t *testing.T) {
	gate := newSiblingGate(false)
	a, b, mb := sameNameWorkers(waitStartHooks{ch: gate.aDone})
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).AddBranch("a", a).AddBranch("b", b)
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("par", par)
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), gate)
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(laneQ)); err != nil {
		t.Fatalf("run: %v", err)
	}
	assertNoSibling(t, mb.call(0))
}

// 并行在顶层：分支 b 的节点等分支 a 跑完并提交之后才开始。b 的上文里仍没有 a 提交上来的。
//
// sanity-revert：分支开始时不去掉兄弟提交上来的事件 ⟹ b 的模型输入里出现 A-ans，本用例红。
func TestParallel_LateBranchDoesNotSeeEarlierSibling(t *testing.T) {
	gate := newSiblingGate(true)
	a, b, mb := sameNameWorkers(nil)
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).AddBranch("a", a).AddBranch("b", b)
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), gate)
	if _, err := par.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(laneQ)); err != nil {
		t.Fatalf("run: %v", err)
	}
	assertNoSibling(t, mb.call(0))
}

// eventRecorder 记下被包住的 agent 这次 Run 返回的事件。
type eventRecorder struct {
	agent_v2.Agent
	mu  sync.Mutex
	got []*agent_v2.AgentEvent
}

func (r *eventRecorder) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	evs, err := r.Agent.Run(ctx, sess, in, opts...)
	r.mu.Lock()
	r.got = evs
	r.mu.Unlock()
	return evs, err
}

// 并行作为成员时，一个分支的 agent 返回的「本次产生的事件」里不该混进兄弟分支在它运行期间提交的事件。agent 按
// 进入时会话的事件数切增量，lane 看到的上文中途变长就会把别人的事件切进来。
//
// sanity-revert：lane 实时读祖先 lane ⟹ beta 返回的事件里有 alpha 的，本用例红（审计实测）。
func TestParallelAsMember_MemberEventsOnlyItsOwn(t *testing.T) {
	gate := newSiblingGate(false)
	ma := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage("alpha-ans"))}}
	mb := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		func([]*schema.Message) (*schema.Message, error) {
			if err := laneWait(gate.aDone); err != nil {
				return nil, err
			}
			return schema.AssistantMessage("beta-ans"), nil
		},
	}}
	alpha := agent_v2.NewBasicAgent(agent_v2.WithName("alpha"), agent_v2.WithModel(ma))
	rec := &eventRecorder{Agent: agent_v2.NewBasicAgent(agent_v2.WithName("beta"), agent_v2.WithModel(mb))}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).AddBranch("a", alpha).AddBranch("b", rec)
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("par", par)
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), gate)
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(laneQ)); err != nil {
		t.Fatalf("run: %v", err)
	}
	rec.mu.Lock()
	got := rec.got
	rec.mu.Unlock()
	if len(got) == 0 {
		t.Fatal("beta 该返回它自己这次的事件")
	}
	for _, ev := range got {
		if ev.AgentName != "beta" {
			t.Errorf("beta 返回的事件里混进了 %q 的", ev.AgentName)
		}
	}
}

func eventContents(evs []*agent_v2.AgentEvent) []string {
	out := make([]string, 0, len(evs))
	for _, ev := range evs {
		if ev.Output != nil && ev.Output.Message != nil {
			out = append(out, ev.Output.Message.Content)
		}
	}
	return out
}

// ForkLane 的契约：lane 看到的上文冻结在派生那一刻。父主线后来追加的、兄弟 lane 提交进共同父 lane 的，都不出现在
// 这条 lane 里；父 lane 自己看得到提交上来的。
//
// sanity-revert：lane 实时读祖先 lane ⟹ l2 看到 l1 的提交，本用例红。
func TestForkLane_ViewFrozenAtFork(t *testing.T) {
	ev := func(text string) *agent_v2.AgentEvent {
		return agent_v2.NewOutputEvent("x", schema.AssistantMessage(text))
	}
	root := agent_v2.NewRunSession()
	root.AddEvent(ev("before"))
	outer := root.ForkLane()
	l1, l2 := outer.ForkLane(), outer.ForkLane()
	l1.AddEvent(ev("l1"))
	agent_v2.CommitLaneEvents(outer, l1)
	root.AddEvent(ev("root-after"))
	l2.AddEvent(ev("l2"))

	if got := strings.Join(eventContents(l2.Events()), ","); got != "before,l2" {
		t.Errorf("l2 该只看到派生时的上文加自己写的，实际 %s", got)
	}
	if got := strings.Join(eventContents(outer.Events()), ","); got != "before,l1" {
		t.Errorf("outer 该看到派生时的上文加 l1 提交上来的，实际 %s", got)
	}
}

// graphStartNote 是编排钩子：第一次 OnGraphStart（并行自己那张图）往编排会话写一条归属 worker 的事件。
type graphStartNote struct {
	agent_v2.NoOpOrchestrationHooks
	once sync.Once
}

func (h *graphStartNote) OnGraphStart(ctx context.Context) (context.Context, error) {
	h.once.Do(func() {
		if s := agent_v2.GetSession(ctx); s != nil {
			s.AddEvent(agent_v2.NewOutputEvent("worker", schema.UserMessage("HOOK-NOTE")))
		}
	})
	return ctx, nil
}

// 分支开始执行之前写进编排会话的事件（这里是编排钩子在 OnGraphStart 里写的），分支都看得到——跟 Sequential
// 的成员一样。
//
// sanity-revert：快照取在建图时 ⟹ 分支的模型输入里没有 HOOK-NOTE，本用例红（审计实测）。
func TestParallel_BranchesSeeEventsWrittenBeforeFanOut(t *testing.T) {
	a, b, mb := sameNameWorkers(nil)
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).AddBranch("a", a).AddBranch("b", b)
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), &graphStartNote{})
	if _, err := par.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(laneQ)); err != nil {
		t.Fatalf("run: %v", err)
	}
	in := mb.call(0)
	found := false
	for _, m := range in {
		if m.Content == "HOOK-NOTE" {
			found = true
		}
	}
	if !found {
		t.Errorf("分支开始执行之前写进编排会话的事件，分支该看得到：%+v", in)
	}
}

// nodeStartNote 是编排钩子：分支 a 的节点开始时往编排会话写一条归属 worker 的事件。
type nodeStartNote struct {
	agent_v2.NoOpOrchestrationHooks
}

func (nodeStartNote) OnNodeStart(ctx context.Context, nodeID string) (context.Context, error) {
	if nodeID == "worker_a" {
		if s := agent_v2.GetSession(ctx); s != nil {
			s.AddEvent(agent_v2.NewOutputEvent("worker", schema.UserMessage("NODE-NOTE")))
		}
	}
	return ctx, nil
}

// 分支节点自己的 OnNodeStart 钩子写进编排会话的事件，在这个分支开始执行之前，这个分支看得到（同上）。
//
// sanity-revert：快照取在建图时或分发时 ⟹ 分支 a 的模型输入里没有 NODE-NOTE，本用例红。
func TestParallel_BranchSeesEventsWrittenByItsNodeStartHook(t *testing.T) {
	ma := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage("A-ans"))}}
	mb := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage("B-ans"))}}
	a := agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithModel(ma))
	b := agent_v2.NewBasicAgent(agent_v2.WithName("other"), agent_v2.WithModel(mb))
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).AddBranch("a", a).AddBranch("b", b)
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), nodeStartNote{})
	if _, err := par.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(laneQ)); err != nil {
		t.Fatalf("run: %v", err)
	}
	in := ma.call(0)
	found := false
	for _, m := range in {
		if m.Content == "NODE-NOTE" {
			found = true
		}
	}
	if !found {
		t.Errorf("分支 a 开始之前它的节点钩子写进编排会话的事件，a 该看得到：%d 条消息", len(in))
	}
}
