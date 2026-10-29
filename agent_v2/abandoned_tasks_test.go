package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// abandonTransferModel：用户说「转账」就发起 transfer 调用，别的一律直接答复。
type abandonTransferModel struct{}

func (abandonTransferModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if last := msgs[len(msgs)-1]; last.Role == schema.RoleUser && strings.Contains(last.Content, "转账") {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c1", Type: "function",
			Function: schema.FunctionCall{Name: "transfer", Arguments: `{"amount":5000}`}}}), nil
	}
	return schema.AssistantMessage("好的"), nil
}
func (abandonTransferModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m abandonTransferModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// abandonFixture：转账要审批、配了会话历史的 BasicAgent，跑到停在审批上。
type abandonFixture struct {
	ctx      context.Context
	cp       compose_v2.Checkpointer
	hs       agent_v2.HistoryStore
	key      agent_v2.HistoryKey
	executed *atomic.Int32
	newAgent func() agent_v2.Agent
	pending  *agent_v2.RunSession // 停在审批上的那个任务
}

func newAbandonFixture(t *testing.T, wrap func(agent_v2.Agent) agent_v2.Agent) *abandonFixture {
	t.Helper()
	f := &abandonFixture{cp: compose_v2.NewInMemoryCheckpointer(), hs: agent_v2.NewInMemoryHistoryStore(),
		key: agent_v2.HistoryKey{Owner: "tenant", ConvID: "conv-abandon"}, executed: &atomic.Int32{}}
	f.ctx = compose_v2.WithCheckpointerCtx(compose_v2.WithThreadIDCtx(context.Background(), f.key.ConvID), f.cp)
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("transfer", "转账", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			f.executed.Add(1)
			return tool.Success("已转账"), nil
		}))
	f.newAgent = func() agent_v2.Agent {
		var ag agent_v2.Agent = agent_v2.NewBasicAgent(agent_v2.WithName("teller"), agent_v2.WithModel(abandonTransferModel{}),
			agent_v2.WithToolkit(tk), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
			agent_v2.WithHistory(f.hs, "tenant"), agent_v2.WithMaxRounds(5))
		if wrap != nil {
			ag = wrap(ag)
		}
		return ag
	}
	f.pending = agent_v2.NewRunSession()
	if _, err := f.newAgent().Run(f.ctx, f.pending, schema.UserMessage("转账 5000")); err == nil {
		t.Fatal("fixture 没搭对：转账应停在审批上")
	} else if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("fixture 没搭对：应是审批中断，实际 %v", err)
	}
	return f
}

func (f *abandonFixture) history(t *testing.T) []*schema.Message {
	t.Helper()
	msgs, err := agent_v2.LoadAllHistory(f.ctx, f.hs, f.key)
	if err != nil {
		t.Fatal(err)
	}
	return msgs
}

func abandonIndexOf(msgs []*schema.Message, pred func(*schema.Message) bool) int {
	for i, m := range msgs {
		if pred(m) {
			return i
		}
	}
	return -1
}

// 新消息顶掉挂起的审批：调用方用 WithAbandonedTasks 声明放弃了旧任务，这次 Run 的入口给它悬空的调用补一条结果（排在这次的输入
// 之前）、删它的存档；转账不执行。直接调和经编排调都一样（编排从成员借历史配置）。
func TestWithAbandonedTasks_ClosesDanglingCallsAndDropsCheckpoint(t *testing.T) {
	for _, tc := range []struct {
		name string
		wrap func(agent_v2.Agent) agent_v2.Agent
	}{
		{"BasicAgent", nil},
		{"Sequential 成员", func(a agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("desk")).AddStep("teller", a)
		}},
		// 顶层的自研壳经 EnterRun 进入、把 opts 透传给框架 agent：壳盖的是这次 Run 自己的任务章，不算嵌套（审计实测：曾按
		// 「ctx 上有没有任务章」判、在这里被拒）
		{"EnterRun 自研壳", func(a agent_v2.Agent) agent_v2.Agent { return &enterRunShell{inner: a} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAbandonFixture(t, tc.wrap)
			if n, _ := f.cp.ListPending(f.ctx, f.pending.RunID); len(n) == 0 {
				t.Fatal("fixture 没搭对：挂起的任务应有存档")
			}
			if _, err := f.newAgent().Run(f.ctx, agent_v2.NewRunSession(), schema.UserMessage("先别转了，查下余额"),
				agent_v2.WithAbandonedTasks(f.pending.RunID)); err != nil {
				t.Fatal(err)
			}
			msgs := f.history(t)
			result := abandonIndexOf(msgs, func(m *schema.Message) bool { return schema.GetToolResultID(m) == "c1" })
			input := abandonIndexOf(msgs, func(m *schema.Message) bool {
				return m.Role == schema.RoleUser && strings.Contains(m.Content, "先别转了")
			})
			if result < 0 {
				t.Fatalf("放弃的任务里悬空的 transfer 调用应补上结果（历史 %d 条）", len(msgs))
			}
			if input >= 0 && result > input {
				t.Fatalf("补的结果应排在这次的输入之前：结果在第 %d 条、输入在第 %d 条", result, input)
			}
			if n, _ := f.cp.ListPending(f.ctx, f.pending.RunID); len(n) != 0 {
				t.Fatalf("放弃的任务的存档应删掉，还剩 %d 份", len(n))
			}
			if n := f.executed.Load(); n != 0 {
				t.Fatalf("被放弃的转账不该执行，实际执行 %d 次", n)
			}
		})
	}
}

// 不声明放弃时旧任务照样能续，框架不替调用方判它放没放弃：历史里不补结果、存档不动（补了的话续跑执行之后真结果排在后面，
// 按先到的配对被当成多余的）。
func TestWithAbandonedTasks_NotGivenLeavesPendingTaskResumable(t *testing.T) {
	f := newAbandonFixture(t, nil)
	if _, err := f.newAgent().Run(f.ctx, agent_v2.NewRunSession(), schema.UserMessage("顺便查下余额")); err != nil {
		t.Fatal(err)
	}
	if abandonIndexOf(f.history(t), func(m *schema.Message) bool { return schema.GetToolResultID(m) == "c1" }) >= 0 {
		t.Fatal("没声明放弃时不该替调用方补「被放弃」的结果")
	}
	if n, _ := f.cp.ListPending(f.ctx, f.pending.RunID); len(n) == 0 {
		t.Fatal("没声明放弃时旧任务的存档应还在、还能续")
	}
}

// 不能放弃这次 Run 自己的任务：拒，什么都不执行。
func TestWithAbandonedTasks_RejectsOwnTask(t *testing.T) {
	f := newAbandonFixture(t, nil)
	sess := agent_v2.NewRunSession()
	if _, err := f.newAgent().Run(f.ctx, sess, schema.UserMessage("转账 1"), agent_v2.WithAbandonedTasks(sess.RunID)); err == nil {
		t.Fatal("放弃这次 Run 自己的任务应被拒")
	}
	if n, _ := f.cp.ListPending(f.ctx, sess.RunID); len(n) != 0 {
		t.Fatalf("被拒的 Run 不该留下存档，实际 %d 份", len(n))
	}
}

// 顶层的拒绝什么都还没做：不带 ErrRunAborted（存档原样在、改对再来即可），不可重试，调用方前文也没写进历史（校验排在建写入器
// 之前）。审计实测：校验曾排在盖任务章之后，纯顶层被当成嵌套、带上 ErrRunAborted，错误文本说「存档已清」；前文已同步写进历史。
func TestWithAbandonedTasks_TopLevelRejectionDoesNothing(t *testing.T) {
	f := newAbandonFixture(t, nil)
	sess := agent_v2.NewRunSession()
	for name, ids := range map[string][]string{"empty_id": {""}, "own_task": {sess.RunID}} {
		t.Run(name, func(t *testing.T) {
			key := agent_v2.HistoryKey{Owner: "tenant", ConvID: "conv-reject-" + name}
			ctx := compose_v2.WithThreadIDCtx(f.ctx, key.ConvID)
			ctx = agent_v2.WithCallerHistoryCtx(ctx, []*schema.Message{schema.UserMessage("前文 u"), schema.AssistantMessage("前文 a")})
			_, err := f.newAgent().Run(ctx, sess, schema.UserMessage("查余额"), agent_v2.WithAbandonedTasks(ids...))
			if err == nil {
				t.Fatal("应被拒")
			}
			if errors.Is(err, compose_v2.ErrRunAborted) {
				t.Errorf("顶层拒绝不该带 ErrRunAborted（什么都还没执行、存档原样在）：%v", err)
			}
			if compose_v2.IsRetryable(err) {
				t.Errorf("拒绝应不可重试：%v", err)
			}
			if msgs, _ := agent_v2.LoadAllHistory(context.Background(), f.hs, key); len(msgs) != 0 {
				t.Errorf("被拒的 Run 不该往历史里写任何东西（含调用方前文），实际 %d 条", len(msgs))
			}
		})
	}
}

// abandonHookHelper 在宿主的 OnAgentStart 里经 RunSubAgent 调一个 agent，透传 WithAbandonedTasks。
type abandonHookHelper struct {
	hook.NoOpHooks
	host   string
	helper agent_v2.Agent
	old    string
	err    error
}

func (h *abandonHookHelper) OnAgentStart(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if info.Name == h.host {
		_, h.err = agent_v2.RunSubAgent(ctx, "helper", h.helper, schema.UserMessage("查余额"), agent_v2.WithAbandonedTasks(h.old))
	}
	return ctx, nil
}

// 嵌套位置上（节点里、或顶层 agent 级钩子里经 RunSubAgent 透传）给这个选项：删的是被放弃任务的整份存档，不是这次调用所在的
// 子 ns——旧任务续不上了，悬空的调用补上一条、只一条，转账不执行。审计实测：删档曾用这次调用所在的 ns，子 ns 下什么都没有、
// 算删成功，历史里补了「被放弃」、旧任务照样能续，续跑执行之后 c1 两条结果。
func TestWithAbandonedTasks_InNestedPositionDropsTheWholeTask(t *testing.T) {
	for _, where := range []string{"node", "top_level_hook"} {
		t.Run(where, func(t *testing.T) {
			f := newAbandonFixture(t, nil)
			old := f.pending.RunID
			var runErr error
			if where == "node" {
				ga := mkEchoGraph(t, "abandon_nested", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
					ev, err := agent_v2.RunSubAgent(ctx, "sub", f.newAgent(), in, agent_v2.WithAbandonedTasks(old))
					if err != nil {
						return nil, err
					}
					return agent_v2.FinalMessage(ev), nil
				})
				_, runErr = ga.Run(f.ctx, agent_v2.NewRunSession(), schema.UserMessage("查下余额"))
			} else {
				h := &abandonHookHelper{host: "host", helper: f.newAgent(), old: old}
				host := agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithModel(abandonTransferModel{}),
					agent_v2.WithHistory(f.hs, "tenant"), agent_v2.WithHooks(h), agent_v2.WithMaxRounds(3))
				_, runErr = host.Run(f.ctx, agent_v2.NewRunSession(), schema.UserMessage("你好"))
				if h.err != nil {
					t.Fatalf("钩子里经 RunSubAgent 透传放弃应照常跑：%v", h.err)
				}
			}
			if runErr != nil {
				t.Fatalf("Run: %v", runErr)
			}
			if n, _ := f.cp.ListPending(f.ctx, old); len(n) != 0 {
				t.Fatalf("被放弃任务的存档应整份删掉，还剩 %d 份", len(n))
			}
			results := 0
			for _, m := range f.history(t) {
				if schema.GetToolResultID(m) == "c1" {
					results++
				}
			}
			if results != 1 {
				t.Errorf("悬空的 c1 应补上恰好一条结果，实际 %d 条", results)
			}
			if _, err := f.newAgent().Resume(f.ctx, old, agent_v2.WithResumeSession(f.pending)); !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
				t.Errorf("被放弃的任务应续不上（ErrNoPendingCheckpoint），实际 %v", err)
			}
			if n := f.executed.Load(); n != 0 {
				t.Errorf("被放弃的转账不该执行，实际 %d 次", n)
			}
		})
	}
}

// 嵌套位置上放弃所在的任务（被调方的 session 属于它）：拒，所在的任务原样不动。
func TestWithAbandonedTasks_NestedRejectsEnclosingTask(t *testing.T) {
	f := newAbandonFixture(t, nil)
	var subErr error
	ga := mkEchoGraph(t, "abandon_self", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
		_, subErr = agent_v2.RunSubAgent(ctx, "sub", f.newAgent(), in, agent_v2.WithAbandonedTasks(agent_v2.GetSession(ctx).RunID))
		return schema.AssistantMessage("ok"), nil
	})
	if _, err := ga.Run(f.ctx, agent_v2.NewRunSession(), schema.UserMessage("查下余额")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if subErr == nil {
		t.Fatal("放弃所在的任务应被拒")
	}
	if !errors.Is(subErr, compose_v2.ErrRunAborted) {
		t.Errorf("嵌套位置上的拒绝应带 ErrRunAborted（确定性的调用错误，同入口的其他拒绝）：%v", subErr)
	}
}

// 任务 T 的工具里直接用另一个 sid 跑一张嵌套图，图的节点里经 RunSubAgent 透传 WithAbandonedTasks(T)：所在任务的身份被内层 sid
// 盖掉了，只比 session 与脱离链认不出它；T 此刻正在执行，拒。审计实测：曾经放行，T 正在执行的 c1 先被补了「被放弃」、真结果排在后面。
func TestWithAbandonedTasks_RejectsEnclosingTaskThroughOtherSIDGraph(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	hs := agent_v2.NewInMemoryHistoryStore()
	key := agent_v2.HistoryKey{Owner: "tenant", ConvID: "conv-other-sid"}
	ctx := compose_v2.WithCheckpointerCtx(compose_v2.WithThreadIDCtx(context.Background(), key.ConvID), cp)
	helper := agent_v2.NewBasicAgent(agent_v2.WithName("helper"), agent_v2.WithModel(abandonTransferModel{}),
		agent_v2.WithHistory(hs, "tenant"), agent_v2.WithMaxRounds(2))
	sess := agent_v2.NewRunSession()
	var helperErr error
	ran := false
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("transfer", "转账", map[string]any{"type": "object"},
		func(tctx context.Context, _ map[string]any) (*tool.Result, error) {
			b := compose_v2.NewBuilder().WithName("g_other_sid")
			b.AddNode("n", func(nctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				ran = true
				_, helperErr = agent_v2.RunSubAgent(nctx, "w", helper, schema.UserMessage("帮忙看下"), agent_v2.WithAbandonedTasks(sess.RunID))
				return nil, nil
			})
			b.AddEdge(compose_v2.StartNode, "n")
			b.AddEdge("n", compose_v2.EndNode)
			g, err := b.Compile()
			if err != nil {
				return nil, err
			}
			if err := compose_v2.Run(tctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("S2")); err != nil {
				return nil, err
			}
			return tool.Success("已转账"), nil
		}))
	host := agent_v2.NewBasicAgent(agent_v2.WithName("teller"), agent_v2.WithModel(abandonTransferModel{}),
		agent_v2.WithToolkit(tk), agent_v2.WithHistory(hs, "tenant"), agent_v2.WithMaxRounds(3))
	if _, err := host.Run(ctx, sess, schema.UserMessage("转账 5000")); err != nil {
		t.Fatalf("host: %v", err)
	}
	if !ran {
		t.Fatal("fixture 没搭对：嵌套图的节点应跑到")
	}
	if helperErr == nil {
		t.Error("放弃一个正在执行的任务应被拒")
	}
	msgs, _ := agent_v2.LoadAllHistory(context.Background(), hs, key)
	results := 0
	for _, m := range msgs {
		if schema.GetToolResultID(m) == "c1" {
			results++
			if !strings.Contains(m.Content, "已转账") {
				t.Errorf("c1 只该有真结果，实际有一条 %q", m.Content)
			}
		}
	}
	if results != 1 {
		t.Errorf("c1 应恰有 1 条结果，实际 %d 条", results)
	}
}

// 被放弃的任务此刻在另一个 goroutine 里正在跑：拒，它照常跑完。
func TestWithAbandonedTasks_RejectsTaskRunningElsewhere(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	entered, release := make(chan struct{}), make(chan struct{})
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("transfer", "转账", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			close(entered)
			<-release
			return tool.Success("已转账"), nil
		}))
	busy := agent_v2.NewRunSession()
	done := make(chan error, 1)
	go func() {
		_, err := agent_v2.NewBasicAgent(agent_v2.WithName("busy"), agent_v2.WithModel(abandonTransferModel{}),
			agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3)).Run(ctx, busy, schema.UserMessage("转账 5000"))
		done <- err
	}()
	<-entered
	_, err := agent_v2.NewBasicAgent(agent_v2.WithName("other"), agent_v2.WithModel(abandonTransferModel{}), agent_v2.WithMaxRounds(2)).
		Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("查余额"), agent_v2.WithAbandonedTasks(busy.RunID))
	close(release)
	if err == nil {
		t.Error("放弃一个正在执行的任务应被拒")
	}
	if errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("顶层的拒绝不该带 ErrRunAborted：%v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("正在跑的任务应照常跑完：%v", err)
	}
}

// 嵌套的被调方自己没配历史时，补结果写进父层正在用的那场对话（被调方的历史本来就沿用父写入器）。审计实测：曾经只按被调方自己的
// 配置找，它没配历史就一条不补。
func TestWithAbandonedTasks_NestedCalleeWithoutHistoryUsesParentWriter(t *testing.T) {
	f := newAbandonFixture(t, nil)
	old := f.pending.RunID
	h := &abandonHookHelper{host: "host", old: old,
		helper: agent_v2.NewBasicAgent(agent_v2.WithName("plain"), agent_v2.WithModel(abandonTransferModel{}), agent_v2.WithMaxRounds(2))}
	host := agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithModel(abandonTransferModel{}),
		agent_v2.WithHistory(f.hs, "tenant"), agent_v2.WithHooks(h), agent_v2.WithMaxRounds(3))
	if _, err := host.Run(f.ctx, agent_v2.NewRunSession(), schema.UserMessage("你好")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if h.err != nil {
		t.Fatalf("钩子里的放弃应照常跑：%v", h.err)
	}
	if n, _ := f.cp.ListPending(f.ctx, old); len(n) != 0 {
		t.Fatalf("被放弃任务的存档应删掉，还剩 %d 份", len(n))
	}
	results := 0
	for _, m := range f.history(t) {
		if schema.GetToolResultID(m) == "c1" {
			results++
		}
	}
	if results != 1 {
		t.Errorf("悬空的 c1 应补上恰好一条结果，实际 %d 条", results)
	}
}

// 脱离之后不能放弃脱离出来之前所在、此刻仍在执行的任务。审计实测：曾经放行，父任务正在执行的调用被补了一条「被放弃」，工具随后
// 返回的真结果排在它后面。
func TestWithAbandonedTasks_RejectsDetachedRunningParent(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	key := agent_v2.HistoryKey{Owner: "tenant", ConvID: "conv-detach-abandon"}
	ctx := compose_v2.WithCheckpointerCtx(compose_v2.WithThreadIDCtx(context.Background(), key.ConvID), compose_v2.NewInMemoryCheckpointer())
	var innerErr error
	innerRan := false
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("transfer", "转账", map[string]any{"type": "object"},
		func(c context.Context, _ map[string]any) (*tool.Result, error) {
			parent := agent_v2.GetSession(c).RunID
			d := compose_v2.WithThreadIDCtx(agent_v2.DetachTask(c), key.ConvID)
			inner := agent_v2.NewBasicAgent(agent_v2.WithName("side"), agent_v2.WithModel(abandonTransferModel{}),
				agent_v2.WithHistory(hs, "tenant"), agent_v2.WithMaxRounds(3))
			innerRan = true
			_, innerErr = inner.Run(d, agent_v2.NewRunSession(), schema.UserMessage("旁路"), agent_v2.WithAbandonedTasks(parent))
			return tool.Success("已转账"), nil
		}))
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("teller"), agent_v2.WithModel(abandonTransferModel{}),
		agent_v2.WithToolkit(tk), agent_v2.WithHistory(hs, "tenant"), agent_v2.WithMaxRounds(5))
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("转账 5000")); err != nil {
		t.Fatalf("parent: %v", err)
	}
	if !innerRan {
		t.Fatal("fixture 没搭对：工具应调起 inner")
	}
	if innerErr == nil {
		t.Error("脱离之后放弃仍在执行的父任务应被拒")
	}
	msgs, _ := agent_v2.LoadAllHistory(context.Background(), hs, key)
	for _, m := range msgs {
		if schema.GetToolResultID(m) == "c1" && strings.Contains(m.Content, "放弃") {
			t.Fatalf("仍在执行的父任务的调用不该被补「被放弃」：%q", m.Content)
		}
	}
}

// 这次 Run 声明了不进历史（WithoutHistoryCtx）时，放弃旧任务只删存档、不往历史里补（同这次 Run 自己不写历史）。
func TestWithAbandonedTasks_HistorySuppressedOnlyDropsCheckpoint(t *testing.T) {
	f := newAbandonFixture(t, nil)
	if _, err := f.newAgent().Run(agent_v2.WithoutHistoryCtx(f.ctx), agent_v2.NewRunSession(), schema.UserMessage("查余额"),
		agent_v2.WithAbandonedTasks(f.pending.RunID)); err != nil {
		t.Fatal(err)
	}
	if abandonIndexOf(f.history(t), func(m *schema.Message) bool { return schema.GetToolResultID(m) == "c1" }) >= 0 {
		t.Error("声明了不进历史时不该往历史里补结果")
	}
	if n, _ := f.cp.ListPending(f.ctx, f.pending.RunID); len(n) != 0 {
		t.Errorf("存档照样要删，还剩 %d 份", len(n))
	}
}

// failDeleteCheckpointer 删 sid 这个任务的存档时报错，别的照常。
type failDeleteCheckpointer struct {
	compose_v2.Checkpointer
	sid string
}

func (c failDeleteCheckpointer) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	if sid == c.sid {
		return 0, errors.New("backend down")
	}
	return c.Checkpointer.DeleteSubtree(ctx, sid, ns)
}

// 删档失败时不补「被放弃」：任务还能续，补了的话续跑执行之后真结果排在它后面、按先到的配对被当成多余的。入口收尾与 Resume 的
// 放弃分支同一条规矩（先删档、删成功才补）。审计实测：曾经先补后删，删失败（或共用预算在两步之间到点）时 c1 有两条结果。
func TestWithAbandonedTasks_DeleteFailureLeavesTaskResumableAndUnclosed(t *testing.T) {
	for _, viaResumeAbort := range []bool{false, true} {
		t.Run(map[bool]string{false: "WithAbandonedTasks", true: "ResumeActionAbort"}[viaResumeAbort], func(t *testing.T) {
			f := newAbandonFixture(t, nil)
			old := f.pending.RunID
			ctx := compose_v2.WithCheckpointerCtx(f.ctx, failDeleteCheckpointer{Checkpointer: f.cp, sid: old})
			var err error
			if viaResumeAbort {
				_, err = f.newAgent().Resume(ctx, old, agent_v2.WithResumeSession(f.pending),
					agent_v2.WithResumeAction(agent_v2.ResumeActionAbort))
			} else {
				_, err = f.newAgent().Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("查余额"), agent_v2.WithAbandonedTasks(old))
			}
			if err != nil {
				t.Fatalf("删档失败只记日志，调用照常：%v", err)
			}
			if abandonIndexOf(f.history(t), func(m *schema.Message) bool { return schema.GetToolResultID(m) == "c1" }) >= 0 {
				t.Fatal("档没删掉（任务还能续）时不该补「被放弃」")
			}
			if n, _ := f.cp.ListPending(f.ctx, old); len(n) == 0 {
				t.Fatal("fixture 没搭对：删档失败时存档应还在")
			}
		})
	}
}

type abandonCpEventSink struct{ cleanups []*compose_v2.CpCleanupAction }

func (s *abandonCpEventSink) Send(ev compose_v2.Event) {
	if ev.Type != compose_v2.CpEventType {
		return
	}
	if a, ok := ev.Metadata["action"].(*compose_v2.CpAction); ok && a.Cleanup != nil {
		s.cleanups = append(s.cleanups, a.Cleanup)
	}
}

// 入口删被放弃任务的存档同样发 cp 清理事件（reason "abandoned"），同 Resume 放弃分支的 "resume_abort"。
func TestWithAbandonedTasks_EmitsCleanupEvent(t *testing.T) {
	f := newAbandonFixture(t, nil)
	sink := &abandonCpEventSink{}
	ctx := compose_v2.WithEventSink(compose_v2.WithEmitCpEventCtx(f.ctx, true), sink)
	if _, err := f.newAgent().Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("查余额"), agent_v2.WithAbandonedTasks(f.pending.RunID)); err != nil {
		t.Fatal(err)
	}
	for _, c := range sink.cleanups {
		if c.Reason == "abandoned" && c.SessionID == f.pending.RunID {
			if c.DeletedCount == 0 {
				t.Errorf("清理事件应报删了几份，实际 0")
			}
			return
		}
	}
	t.Fatalf("应发一条 reason=abandoned 的清理事件，实际 %d 条：%+v", len(sink.cleanups), sink.cleanups)
}
