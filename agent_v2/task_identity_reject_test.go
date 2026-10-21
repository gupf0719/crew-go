package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/store"
)

// 任务身份只认 session 的 RunID：在一个正在执行的任务里，session 必须属于这个任务（对不上入口就拒），
// Resume 只在顶层调，sessionID 与 WithResumeSession 的 RunID 必须一致；开独立任务是显式动作
// （DetachTask）。下面每一格都先确认「被拒的那次一步也没执行、存档原样在」，再确认正确的写法走得通。

// pendingCount 数 sid 下挂着的存档份数。
func pendingCount(t *testing.T, cper compose_v2.Checkpointer, sid string) int {
	t.Helper()
	p, err := cper.ListPending(context.Background(), sid)
	if err != nil {
		t.Fatalf("ListPending(%s): %v", sid, err)
	}
	return len(p)
}

// TestResume_SessionRunIDMismatch_RejectedCheckpointsIntact：一个 session 对象办两件事——任务 A 中断、
// NextTask、任务 B 也中断，回头带同一个 session（RunID 已是 B）续 A。sessionID 与 session.RunID 是同一个
// 任务身份的两份声明：对不上就拒（顶层，不 wrap ErrRunAborted）。放行的话 A 的嵌套层按 B 找自己的存档、
// 从头重放，跑成功后按 B 清子树把 B 挂着的存档删掉（实测 err=nil）。拒掉之后两个任务的存档都原样在，
// 换成正确的写法各自续得上、第一个节点都不重跑。
func TestResume_SessionRunIDMismatch_RejectedCheckpointsIntact(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	root := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	firsts := &countLog{}
	ag := askingTwoNodeAgent(t, "a", firsts)
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(root, sess, schema.UserMessage("任务A"))
	ieA, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("任务 A 应停在提问上，got %v", err)
	}
	runA := sess.RunID
	sess.NextTask()
	_, err = ag.Run(root, sess, schema.UserMessage("任务B"))
	ieB, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("任务 B 应停在提问上，got %v", err)
	}
	runB := sess.RunID

	_, err = ag.Resume(root, runA, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ieA.Query.InterruptID: schema.UserMessage("好")}))
	if err == nil || !strings.Contains(err.Error(), "不一致") {
		t.Fatalf("sessionID 与 session.RunID 对不上应被拒，got %v", err)
	}
	if errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("顶层拒绝不该 wrap ErrRunAborted（存档原样在，改对了续得上）：%v", err)
	}
	if a, b := pendingCount(t, cper, runA), pendingCount(t, cper, runB); a == 0 || b == 0 {
		t.Fatalf("被拒的那次不该动任何存档：A 挂着 %d 份、B 挂着 %d 份", a, b)
	}

	if _, err := ag.Resume(root, runA,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ieA.Query.InterruptID: schema.UserMessage("好")})); err != nil {
		t.Fatalf("不带 session 续 A 应续得上：%v", err)
	}
	if _, err := ag.Resume(root, runB, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ieB.Query.InterruptID: schema.UserMessage("好")})); err != nil {
		t.Fatalf("带 RunID 一致的 session 续 B 应续得上：%v", err)
	}
	if a, b := firsts.get("任务A"), firsts.get("任务B"); a != 1 || b != 1 {
		t.Errorf("两个任务都应续上存档（第一个节点各执行 1 次），实际 A=%d B=%d", a, b)
	}
}

// TestResume_InsideRunningTask_RejectedUnlessDetached：一个图的节点里续跑另一个挂着的任务。续跑只在
// 顶层调——在正在执行的任务里调，当前执行留在 ctx 上的位置、答案、新开信号全会被带进那个任务 ⟹ 拒
// （嵌套位置，wrap ErrRunAborted），那个任务的存档原样在；DetachTask 之后续得上。
func TestResume_InsideRunningTask_RejectedUnlessDetached(t *testing.T) {
	for _, detach := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "detached"}[detach], func(t *testing.T) {
			cper := compose_v2.NewInMemoryCheckpointer()
			root := compose_v2.WithCheckpointerCtx(context.Background(), cper)
			firsts := &countLog{}
			ag := askingTwoNodeAgent(t, "a", firsts)
			sessA := agent_v2.NewRunSession()
			_, err := ag.Run(root, sessA, schema.UserMessage("任务A"))
			ieA, ok := compose_v2.IsInterrupt(err)
			if !ok {
				t.Fatalf("任务 A 应停在提问上，got %v", err)
			}
			var resumeErr error
			b := compose_v2.NewBuilder()
			b.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				if detach {
					ctx = agent_v2.DetachTask(ctx)
				}
				_, resumeErr = ag.Resume(ctx, sessA.RunID,
					agent_v2.WithResumeUserInputs(map[string]*schema.Message{ieA.Query.InterruptID: schema.UserMessage("好")}))
				return nil, resumeErr
			})
			b.AddEdge(compose_v2.StartNode, "host")
			b.AddEdge("host", compose_v2.EndNode)
			g, err := b.Compile()
			if err != nil {
				t.Fatal(err)
			}
			err = compose_v2.Run(root, g, compose_v2.NewChannelManager(), compose_v2.WithSessionID("host-task"))
			if detach {
				if err != nil || firsts.get("任务A") != 1 {
					t.Errorf("DetachTask 之后应续上任务 A（第一个节点只执行 1 次）：err=%v 执行 %d 次", err, firsts.get("任务A"))
				}
				return
			}
			if !errors.Is(resumeErr, compose_v2.ErrRunAborted) || !strings.Contains(resumeErr.Error(), "DetachTask") {
				t.Errorf("在正在执行的任务里 Resume 应被拒（wrap ErrRunAborted、给出路 DetachTask），got %v", resumeErr)
			}
			if n := pendingCount(t, cper, sessA.RunID); n == 0 {
				t.Error("被拒的那次不该动任务 A 的存档")
			}
		})
	}
}

// TestRunSubAgent_BareGraphWithOtherSessionInCtx_FollowsGraphTask：业务直接跑 compose_v2 图、ctx 里另推
// 了一个 session（无状态服务每个请求——包括续跑请求——都新建一个），图的 sid 另给。任务是那张图（它的
// sid 是存档的主索引）：RunSubAgent 的被调方必须跟图走，续跑时续上自己的存档；跟 session 走的话每个
// 请求换一个 RunID，被调方从头再跑，还在上一个 session 名下留一份孤儿存档（实测）。
func TestRunSubAgent_BareGraphWithOtherSessionInCtx_FollowsGraphTask(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	firsts := &countLog{}
	sub := askingTwoNodeAgent(t, "sub", firsts)
	b := compose_v2.NewBuilder()
	b.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := agent_v2.RunSubAgent(ctx, "sub", sub, schema.UserMessage("go"))
		return nil, err
	})
	b.AddEdge(compose_v2.StartNode, "host")
	b.AddEdge("host", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("req-graph")}
	req1 := agent_v2.NewRunSession()
	err = compose_v2.Run(agent_v2.WithSession(context.Background(), req1), g, compose_v2.NewChannelManager(), opts...)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在子 agent 的提问上，got %v", err)
	}
	req2 := agent_v2.NewRunSession()
	err = compose_v2.Run(agent_v2.WithSession(context.Background(), req2), g, compose_v2.NewChannelManager(), append(opts,
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")}))...)
	if err != nil || firsts.get("go") != 1 {
		t.Errorf("续跑请求换了 session 也该续上被调方的存档（第一个节点只执行 1 次）：err=%v 执行 %d 次", err, firsts.get("go"))
	}
	if n := pendingCount(t, cper, req1.RunID); n != 0 {
		t.Errorf("被调方的存档该落在图的任务名下，上一个 session 名下还挂着 %d 份孤儿存档", n)
	}
}

// newSessionShell 是业务自写的编排成员：每次在自己的 Run 里 new 一个 session 跑内层（为了隔离事件）。
type newSessionShell struct {
	inner agent_v2.Agent
	pass  bool // true：透传收到的 session（正确写法）
}

func (w *newSessionShell) Name() string        { return "shell" }
func (w *newSessionShell) Description() string { return "shell" }
func (w *newSessionShell) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	if !w.pass {
		sess = agent_v2.NewRunSession()
	}
	return w.inner.Run(ctx, sess, in, opts...)
}
func (w *newSessionShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, errors.New("shell: 由外层编排续跑")
}

// TestMemberShell_NewSessionPerRun_RejectedNotLivelock：成员壳每次 new 一个 session 跑内层，内层先后问两个
// 问题。当成新任务读的话外层每次续跑都新开一个任务、从头问 Q1（实测 asked=[Q1 Q2 Q1 Q2 Q1 Q2] 永不收敛、
// 每轮留一份孤儿存档）⟹ 入口拒，一个问题也没问出去；透传收到的 session 时两问各答一次就收敛。
func TestMemberShell_NewSessionPerRun_RejectedNotLivelock(t *testing.T) {
	for _, pass := range []bool{false, true} {
		t.Run(map[bool]string{false: "new_session", true: "pass_session"}[pass], func(t *testing.T) {
			cper := compose_v2.NewInMemoryCheckpointer()
			root := compose_v2.WithCheckpointerCtx(context.Background(), cper)
			var asked []string
			// 两个节点各问一个：答完 Q1 那个节点就算做成了，续跑只该再问 Q2
			inner := newTwoNodeAgent(t, "inner",
				func(ctx context.Context, c string) (string, error) {
					a1, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "Q1"})
					if err != nil {
						asked = append(asked, "Q1")
						return "", err
					}
					return a1.Content, nil
				},
				func(ctx context.Context, c, a1 string) (string, error) {
					a2, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "Q2"})
					if err != nil {
						asked = append(asked, "Q2")
						return "", err
					}
					return c + ":" + a1 + ":" + a2.Content, nil
				})
			seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
				AddStep("step", &newSessionShell{inner: inner, pass: pass})
			sess := agent_v2.NewRunSession()
			_, err := seq.Run(root, sess, schema.UserMessage("job"))
			if !pass {
				requireRejectedNotInTask(t, err)
				if len(asked) != 0 {
					t.Errorf("被拒的内层一步也不该跑：asked=%v", asked)
				}
				return
			}
			var evs []*agent_v2.AgentEvent
			for i := 0; i < 4; i++ {
				ie, ok := compose_v2.IsInterrupt(err)
				if !ok {
					break
				}
				evs, err = seq.Resume(root, sess.RunID, agent_v2.WithResumeSession(sess),
					agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("ans-" + ie.Query.Question)}))
			}
			final := agent_v2.FinalMessage(evs)
			if err != nil || strings.Join(asked, ",") != "Q1,Q2" || final == nil || final.Content != "job:ans-Q1:ans-Q2" {
				t.Errorf("透传 session 应两问各答一次就收敛：err=%v asked=%v final=%v", err, asked, final)
			}
		})
	}
}

// storeProbe 只是一个能被 FromContext 认出来的 store。
type storeProbe struct{ store.Store }

// TestDetachTask_StripsExecutionKeepsEnvironment：DetachTask 剥掉当前执行留在 ctx 上的状态（session、
// ns、节点执行范围、sid、新开信号、节点位置），留下调用方给整个请求配的环境（checkpointer、编排钩子、
// 调用主体、会话 ID、store）与取消。
func TestDetachTask_StripsExecutionKeepsEnvironment(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	hooks := &compose_v2.RecordingHooks{}
	st := &storeProbe{Store: store.NewInMemoryStore()}
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	base = compose_v2.WithCheckpointerCtx(base, cper)
	base = compose_v2.WithOrchHooks(base, hooks)
	base = compose_v2.WithOwner(base, "tenant-a")
	base = compose_v2.WithThreadIDCtx(base, "conv-1")
	base = store.WithContext(base, st)

	var inNode, detached context.Context
	b := compose_v2.NewBuilder()
	b.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		inNode = ctx
		detached = agent_v2.DetachTask(ctx)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "host")
	b.AddEdge("host", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ctx := agent_v2.WithSession(base, agent_v2.NewRunSession())
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithRunNS("outer"), compose_v2.WithFreshRun()); err != nil {
		t.Fatal(err)
	}
	// 前提：节点里这些确实都在（否则下面的「剥掉了」是空转）
	if agent_v2.GetSession(inNode) == nil || compose_v2.GetGraphNS(inNode) == "" || !compose_v2.WithinNodeScope(inNode) ||
		compose_v2.GetSessionID(inNode) == "" || !compose_v2.IsFreshRunCtx(inNode) || compose_v2.GetCurrentNodeID(inNode) == "" {
		t.Fatal("fixture 没搭对：节点 ctx 上应带着 session / ns / 节点执行范围 / sid / 新开信号 / 节点 ID")
	}
	if agent_v2.GetSession(detached) != nil {
		t.Error("session 没剥掉")
	}
	if ns := compose_v2.GetGraphNS(detached); ns != "" {
		t.Errorf("ns 没剥掉：%q", ns)
	}
	if compose_v2.WithinNodeScope(detached) {
		t.Error("节点执行范围没剥掉")
	}
	if sid := compose_v2.GetSessionID(detached); sid != "" {
		t.Errorf("sid 没剥掉：%q", sid)
	}
	if compose_v2.IsFreshRunCtx(detached) {
		t.Error("新开信号没剥掉")
	}
	if id := compose_v2.GetCurrentNodeID(detached); id != "" {
		t.Errorf("节点 ID 没剥掉：%q", id)
	}
	if compose_v2.GetCheckpointer(detached) != cper || compose_v2.GetOrchHooks(detached) != hooks ||
		compose_v2.GetOwner(detached) != "tenant-a" || store.FromContext(detached) != st {
		t.Error("调用方配的环境（checkpointer / 编排钩子 / 调用主体 / store）不该剥")
	}
	// 会话 ID 标识的是当前这场对话，独立任务按定义不是它的一部分（留着的话它写进当前会话的历史、
	// 排在用户这一轮的提问之前）——要进哪个会话在返回的 ctx 上自己推
	if compose_v2.GetThreadID(inNode) != "conv-1" {
		t.Fatal("fixture 没搭对：节点 ctx 上应带着会话 ID")
	}
	if tid := compose_v2.GetThreadID(detached); tid != "" {
		t.Errorf("会话 ID 没剥掉：%q", tid)
	}
	cancel()
	select {
	case <-detached.Done():
	default:
		t.Error("取消不该剥：调用方的请求撤了，独立任务也该停")
	}
}

// TestDetachTask_IndependentTaskInNode_OwnCheckpointAndFreshStart：节点里用 DetachTask 开一个独立任务，它
// 中断时存档落在它自己的 RunID 名下（不在当前任务的 ns 里），当前任务的节点照常收尾；之后在顶层带它自己
// 的 RunID 续跑续得上。
func TestDetachTask_IndependentTaskInNode_OwnCheckpointAndFreshStart(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	root := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	firsts := &countLog{}
	side := askingTwoNodeAgent(t, "side", firsts)
	sideSess := agent_v2.NewRunSession()
	var sideErr error
	b := compose_v2.NewBuilder()
	b.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, sideErr = side.Run(agent_v2.DetachTask(ctx), sideSess, schema.UserMessage("旁路"))
		return nil, nil // 独立任务挂着不影响当前任务
	})
	b.AddEdge(compose_v2.StartNode, "host")
	b.AddEdge("host", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	if err := compose_v2.Run(root, g, compose_v2.NewChannelManager(), compose_v2.WithSessionID("main-task")); err != nil {
		t.Fatalf("当前任务应照常收尾：%v", err)
	}
	ie, ok := compose_v2.IsInterrupt(sideErr)
	if !ok {
		t.Fatalf("独立任务应停在自己的提问上，got %v", sideErr)
	}
	if n := pendingCount(t, cper, sideSess.RunID); n == 0 {
		t.Fatal("独立任务的存档应落在它自己的 RunID 名下")
	}
	if _, err := side.Resume(root, sideSess.RunID, agent_v2.WithResumeSession(sideSess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")})); err != nil {
		t.Fatalf("在顶层续独立任务应续得上：%v", err)
	}
	if n := firsts.get("旁路"); n != 1 {
		t.Errorf("续跑应续上独立任务的存档（第一个节点只执行 1 次），实际 %d 次", n)
	}
}

// TestNilSession_BareGraphWithOtherSessionInCtx_FollowsGraphTask：同上一格的形态，但节点里把请求的
// session 推进 ctx（给下游 helper 用），然后直接 `sub.Run(ctx, nil, ...)`（不传 session）。框架补的
// session 的 RunID 取所在的任务（那张图的 sid），不取 ctx 上此刻的 sid（那是每个请求新建的 session
// 的）——取后者的话每个请求换一个 RunID，被调方的存档落在图的任务之外，续跑时从头再跑。
//
// ⚠️ 请求 session 必须在**节点里**推：在图外推的话图自己的 sid 会盖掉它，ctx 上的 sid 与图的任务
// 恰好相同，两种取法分不出来（这一格曾经这么写、变异时假绿）。（子 ns 在这里手工推，只为让被调方有
// 自己的槽；实际写法走 RunSubAgent。）
func TestNilSession_BareGraphWithOtherSessionInCtx_FollowsGraphTask(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	firsts := &countLog{}
	sub := askingTwoNodeAgent(t, "sub", firsts)
	b := compose_v2.NewBuilder()
	var req *agent_v2.RunSession // 当前这个请求的 session
	b.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ctx = agent_v2.WithSession(ctx, req)
		subCtx := compose_v2.WithGraphNS(ctx, compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), "sub"))
		_, err := sub.Run(subCtx, nil, schema.UserMessage("go"))
		return nil, err
	})
	b.AddEdge(compose_v2.StartNode, "host")
	b.AddEdge("host", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("req-graph")}
	req1 := agent_v2.NewRunSession()
	req = req1
	err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在子 agent 的提问上，got %v", err)
	}
	req = agent_v2.NewRunSession()
	err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), append(opts,
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")}))...)
	if err != nil || firsts.get("go") != 1 {
		t.Errorf("续跑请求换了 session 也该续上被调方的存档（第一个节点只执行 1 次）：err=%v 执行 %d 次", err, firsts.get("go"))
	}
	if n := pendingCount(t, cper, req1.RunID); n != 0 {
		t.Errorf("被调方的存档该落在图的任务名下，上一个 session 名下还挂着 %d 份孤儿存档", n)
	}
}
