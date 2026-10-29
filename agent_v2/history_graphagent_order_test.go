package agent_v2_test

// 顶层 GraphAgent 的原始输入在会话历史里排在图内子 agent 的问答之前（todo「顶层 GraphAgent 的用户输入到出口才写进
// 历史」）。曾经到出口才结算输入，而图里经 RunSubAgent 调起的子 agent 在执行中就记下了自己的问答，用户那句话排在它们
// 之后。现在输入登记成待补、由写入器排在第一批记录前面（见 deferRootInput）；转发形状下子 agent 自己记这一问，不补。

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

func historyIndexOf(msgs []*schema.Message, role schema.MessageRole, content string) int {
	for i, m := range msgs {
		if m.Role == role && m.Content == content {
			return i
		}
	}
	return -1
}

func historyOwnerOf(m *schema.Message) string {
	o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
	return o
}

// 节点把一条派生任务交给子 agent（委派）：原话归图、排在最前，子 agent 的问答在它之后。
func TestHistGraphAgent_TopLevel_DelegateShape_InputComesFirst(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	leaf := agent_v2.NewBasicAgent(agent_v2.WithName("leaf"), agent_v2.WithModel(&identityProbeModel{reply: "ans"}), agent_v2.WithMaxRounds(2))
	ga := mkEchoGraph(t, "deleg", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
		ev, err := agent_v2.RunSubAgent(ctx, "worker", leaf, schema.UserMessage("派生任务:"+in.Content))
		if err != nil {
			return nil, err
		}
		return agent_v2.FinalMessage(ev), nil
	})
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_deleg")
	if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs := loadHist(t, hs, ctx, "c_deleg")
	if n := countUserWithContent(msgs, "原话"); n != 1 {
		t.Fatalf("原话应 1 条，实际 %d：%s", n, histDump(msgs))
	}
	root, derived, ans := historyIndexOf(msgs, schema.RoleUser, "原话"), historyIndexOf(msgs, schema.RoleUser, "派生任务:原话"),
		historyIndexOf(msgs, schema.RoleAssistant, "ans")
	if derived < 0 || ans < 0 {
		t.Fatalf("fixture 没搭对：子 agent 的问答应在历史里：%s", histDump(msgs))
	}
	if root != 0 || root > derived || root > ans {
		t.Errorf("原话应排在最前、在子 agent 的问答之前，实际：%s\n"+
			"  排在后面 = 顶层的输入又到出口才记", histDump(msgs))
	}
	if o := historyOwnerOf(msgs[root]); o != "deleg" {
		t.Errorf("原话归属应是图 deleg，实际 %q：%s", o, histDump(msgs))
	}
}

// 委派之后节点把收到的输入原样当产出返回（输出读取器回落返回输入是常见写法）：补记的那份原话就是这条产出，不再记一遍。
func TestHistGraphAgent_TopLevel_DelegateShape_OutputIsInput(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	leaf := agent_v2.NewBasicAgent(agent_v2.WithName("leaf"), agent_v2.WithModel(&identityProbeModel{reply: "ans"}), agent_v2.WithMaxRounds(2))
	ga := mkEchoGraph(t, "deleg_echo", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
		if _, err := agent_v2.RunSubAgent(ctx, "worker", leaf, schema.UserMessage("派生任务:"+in.Content)); err != nil {
			return nil, err
		}
		return in, nil
	})
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_deleg_echo")
	if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs := loadHist(t, hs, ctx, "c_deleg_echo")
	if n := countUserWithContent(msgs, "原话"); n != 1 {
		t.Errorf("原话应 1 条（补记的那份就是产出），实际 %d：%s", n, histDump(msgs))
	}
}

// 子 agent 记完之后节点停下问人，续跑收尾：原话同样排在最前、只一份（中断出口那次结算看到它已记过）。
func TestHistGraphAgent_TopLevel_DelegateThenInterrupt_InputComesFirst(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	leaf := agent_v2.NewBasicAgent(agent_v2.WithName("leaf"), agent_v2.WithModel(&identityProbeModel{reply: "ans"}), agent_v2.WithMaxRounds(2))
	ga := mkEchoGraph(t, "deleg_intr", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
		if _, err := agent_v2.RunSubAgent(ctx, "worker", leaf, schema.UserMessage("派生任务:"+in.Content)); err != nil {
			return nil, err
		}
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续吗"}); err != nil {
			return nil, err
		}
		return schema.AssistantMessage("【图产出】"), nil
	})
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_deleg_intr")
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ga.Run(ctx, sess, schema.UserMessage("原话"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("Run 应停在提问上，got %v", err)
	}
	if _, err := ga.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage("继续"),
	})); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	msgs := loadHist(t, hs, ctx, "c_deleg_intr")
	if n := countUserWithContent(msgs, "原话"); n != 1 {
		t.Fatalf("原话应 1 条，实际 %d：%s", n, histDump(msgs))
	}
	if root, ans := historyIndexOf(msgs, schema.RoleUser, "原话"), historyIndexOf(msgs, schema.RoleAssistant, "ans"); ans < 0 || root != 0 {
		t.Errorf("原话应排在最前、在子 agent 的答复之前，实际：%s", histDump(msgs))
	}
	assertCount(t, msgs, "【图产出】", 1)
}

// 一个节点里先后调两个子 agent：一个收派生任务、一个收原话（转发）。份数由谁先写决定、跟调度无关（这里是顺序调用）：
//   - 先转发：转发的子 agent 自己记原话，那一份就在最前，不补——1 份；
//   - 先委派：先补一份归图、排在最前，转发的那个子 agent 再在自己名下记一份（它的视图只看自己名下的记录，要自己持有
//     这一问）——2 份。
//
// 两种顺序里原话都排在最前。
func TestHistGraphAgent_TopLevel_MixedForwardAndDelegate(t *testing.T) {
	for _, forwardFirst := range []bool{true, false} {
		name := map[bool]string{true: "forward_first", false: "delegate_first"}[forwardFirst]
		t.Run(name, func(t *testing.T) {
			hs := agent_v2.NewInMemoryHistoryStore()
			fwd := agent_v2.NewBasicAgent(agent_v2.WithName("fwd"), agent_v2.WithModel(&identityProbeModel{reply: "fwd_ans"}), agent_v2.WithMaxRounds(2))
			del := agent_v2.NewBasicAgent(agent_v2.WithName("del"), agent_v2.WithModel(&identityProbeModel{reply: "del_ans"}), agent_v2.WithMaxRounds(2))
			ga := mkEchoGraph(t, "mixed_"+name, func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
				callFwd := func() error { _, err := agent_v2.RunSubAgent(ctx, "fwd", fwd, in); return err }
				callDel := func() error {
					_, err := agent_v2.RunSubAgent(ctx, "del", del, schema.UserMessage("派生任务:"+in.Content))
					return err
				}
				first, second := callDel, callFwd
				if forwardFirst {
					first, second = callFwd, callDel
				}
				if err := first(); err != nil {
					return nil, err
				}
				if err := second(); err != nil {
					return nil, err
				}
				return schema.AssistantMessage("【图产出】"), nil
			})
			ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
			ctx = compose_v2.WithThreadIDCtx(ctx, "c_mixed_"+name)
			if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
				t.Fatalf("Run: %v", err)
			}
			msgs := loadHist(t, hs, ctx, "c_mixed_"+name)
			want := 2
			if forwardFirst {
				want = 1
			}
			if n := countUserWithContent(msgs, "原话"); n != want {
				t.Errorf("原话应 %d 条，实际 %d：%s", want, n, histDump(msgs))
			}
			if len(msgs) == 0 || msgs[0].Role != schema.RoleUser || msgs[0].Content != "原话" {
				t.Errorf("原话应排在最前，实际：%s", histDump(msgs))
			}
			// 每个子 agent 自己名下的流都有问有答
			for owner, q := range map[string]string{"fwd": "原话", "del": "派生任务:原话"} {
				hasQ := false
				for _, m := range msgs {
					if historyOwnerOf(m) == owner && m.Role == schema.RoleUser && m.Content == q {
						hasQ = true
					}
				}
				if !hasQ {
					t.Errorf("%s 名下的流缺了它收到的那一问 %q：%s", owner, q, histDump(msgs))
				}
			}
		})
	}
}

// quietAgent 什么也不记（远程 / 自研壳的形状）：编排边界替它记输入与产出。
type quietAgent struct {
	name  string
	reply *schema.Message
}

func (c *quietAgent) Name() string        { return c.name }
func (c *quietAgent) Description() string { return "" }
func (c *quietAgent) Run(context.Context, *agent_v2.RunSession, *schema.Message, ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(c.name, c.reply)}, nil
}
func (c *quietAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

// idGatedHistoryStore 在第一次按 gateID 查消息 ID 时停住：通知 inWindow，等 release（最多 3 秒）。
type idGatedHistoryStore struct {
	agent_v2.HistoryStore
	gateID   string
	inWindow chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (s *idGatedHistoryStore) Load(ctx context.Context, key agent_v2.HistoryKey, q agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	if q.MessageID == s.gateID {
		s.once.Do(func() {
			close(s.inWindow)
			select {
			case <-s.release:
			case <-time.After(3 * time.Second):
			}
		})
	}
	return s.HistoryStore.Load(ctx, key, q)
}

// 并发扇出：分支 a 委派给 BasicAgent，分支 b 把原话转发给一个自己不记历史的执行体（边界补记替它记）。b 的边界补记先认领
// 原话、读一次历史判产出之后才入队；让 a 的第一批恰好落在这个窗口里——原话照样排在最前（补一份归图，b 再记自己的一份）。
// 审计实测：补不补曾看登记表，b 认领了还没入队的那份让待补被丢掉，原话排到了 a 的问答之后。
func TestHistGraphAgent_TopLevel_ConcurrentForwardAndDelegate_InputComesFirst(t *testing.T) {
	base := agent_v2.NewInMemoryHistoryStore()
	bFinal := schema.AssistantMessage("b_ans")
	hs := &idGatedHistoryStore{HistoryStore: base, gateID: bFinal.ID, inWindow: make(chan struct{}), release: make(chan struct{})}
	leaf := agent_v2.NewBasicAgent(agent_v2.WithName("leaf"), agent_v2.WithModel(&identityProbeModel{reply: "a_ans"}), agent_v2.WithMaxRounds(2))
	quiet := &quietAgent{name: "remote", reply: bFinal}

	inCh := compose_v2.LastValue[*schema.Message]("__fan_in__")
	aCh := compose_v2.LastValue[*schema.Message]("__fan_a__")
	bCh := compose_v2.LastValue[*schema.Message]("__fan_b__")
	bld := compose_v2.NewBuilder().WithName("g_fan")
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, aCh)
	compose_v2.WithChannel(bld, bCh)
	bld.AddNode("a", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		select {
		case <-hs.inWindow:
		case <-time.After(3 * time.Second):
			return nil, errors.New("fixture 没搭对：b 的边界补记没进窗口")
		}
		defer close(hs.release)
		ev, err := agent_v2.RunSubAgent(ctx, "a", leaf, schema.UserMessage("派生任务:"+in.Content))
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(aCh, agent_v2.FinalMessage(ev))}, nil
	})
	bld.AddNode("b", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		ev, err := agent_v2.RunSubAgent(ctx, "b", quiet, in)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(bCh, agent_v2.FinalMessage(ev))}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "a")
	bld.AddEdge(compose_v2.StartNode, "b")
	bld.AddEdge("a", compose_v2.EndNode)
	bld.AddEdge("b", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ga := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("fan"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, aCh)
			return out
		}))
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_fan")
	if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs := loadHist(t, base, ctx, "c_fan")
	if historyIndexOf(msgs, schema.RoleAssistant, "a_ans") < 0 || historyIndexOf(msgs, schema.RoleAssistant, "b_ans") < 0 {
		t.Fatalf("fixture 没搭对：两个分支的答复都应在历史里：%s", histDump(msgs))
	}
	if len(msgs) == 0 || msgs[0].Role != schema.RoleUser || msgs[0].Content != "原话" {
		t.Errorf("原话应排在最前，实际：%s", histDump(msgs))
	}
	if n := countUserWithContent(msgs, "原话"); n != 2 {
		t.Errorf("委派的分支先写：原话应 2 份（归图一份、转发的分支一份），实际 %d：%s", n, histDump(msgs))
	}
}

// endHookHelper 在图的 OnAgentEnd 里请教一个 helper（照常进历史）。
type endHookHelper struct {
	hook.NoOpHooks
	graph  string
	helper agent_v2.Agent
	err    error
}

func (h *endHookHelper) OnAgentEnd(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	if info.Name == h.graph {
		_, h.err = h.helper.Run(ctx, agent_v2.GetSession(ctx), schema.UserMessage("钩子通知"))
	}
	return ctx
}

// 图一条记录都没写就终局失败，之后 OnAgentEnd 钩子里请教的 helper 写了一批：原话不被它带出来（待补只覆盖图执行那一段，
// 终局失败不记）。审计实测：待补曾留到图外，原话记不记取决于结束钩子碰巧写没写。
func TestHistGraphAgent_TopLevel_AbortWithoutRecords_EndHookDoesNotFlushInput(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	hooks := &endHookHelper{graph: "abort_end",
		helper: agent_v2.NewBasicAgent(agent_v2.WithName("helper"), agent_v2.WithModel(&identityProbeModel{reply: "helper_ans"}), agent_v2.WithMaxRounds(2))}
	inCh := compose_v2.LastValue[*schema.Message]("__abort_end_in__")
	bld := compose_v2.NewBuilder().WithName("g_abort_end")
	compose_v2.WithChannel(bld, inCh)
	bld.AddNode("work", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, fmt.Errorf("业务判终局: %w", compose_v2.ErrRunAborted)
	})
	bld.AddEdge(compose_v2.StartNode, "work")
	bld.AddEdge("work", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ga := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("abort_end"), agent_v2.WithGraphAgentHooks(hooks),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return nil }))
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_abort_end")
	if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("fixture 没搭对：Run 应终局失败，got %v", err)
	}
	if hooks.err != nil {
		t.Fatalf("fixture 没搭对：钩子里的 helper 应跑成：%v", hooks.err)
	}
	msgs := loadHist(t, hs, ctx, "c_abort_end")
	if historyIndexOf(msgs, schema.RoleAssistant, "helper_ans") < 0 {
		t.Fatalf("fixture 没搭对：helper 的答复应在历史里：%s", histDump(msgs))
	}
	if n := countUserWithContent(msgs, "原话"); n != 0 {
		t.Errorf("终局失败、图里一条记录都没写：原话不该被图外的记录带出来，实际 %d 份：%s", n, histDump(msgs))
	}
}

// countingHistoryStore 数按消息 ID 查的 Load。
type countingHistoryStore struct {
	agent_v2.HistoryStore
	mu   sync.Mutex
	byID map[string]int
}

func (s *countingHistoryStore) Load(ctx context.Context, key agent_v2.HistoryKey, q agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	if q.MessageID != "" {
		s.mu.Lock()
		s.byID[q.MessageID]++
		s.mu.Unlock()
	}
	return s.HistoryStore.Load(ctx, key, q)
}

// 纯节点函数的图：原话「跨 Run 记过没有」只在入口读一次历史，出口结算认得入口判过的这条、不再读（审计实测：曾读两次）。
func TestHistGraphAgent_TopLevel_PureNodeFunc_ReadsHistoryOnceForInput(t *testing.T) {
	hs := &countingHistoryStore{HistoryStore: agent_v2.NewInMemoryHistoryStore(), byID: map[string]int{}}
	ga := mkEchoGraph(t, "pure_reads", func(context.Context, *schema.Message) (*schema.Message, error) {
		return schema.AssistantMessage("【图产出】"), nil
	})
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_pure_reads")
	in := schema.UserMessage("原话")
	if _, err := ga.Run(ctx, agent_v2.NewRunSession(), in); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if msgs := loadHist(t, hs, ctx, "c_pure_reads"); countUserWithContent(msgs, "原话") != 1 {
		t.Fatalf("原话应 1 份：%s", histDump(msgs))
	}
	if n := hs.byID[in.ID]; n != 1 {
		t.Errorf("按原话 ID 应只读 1 次历史，实际 %d 次", n)
	}
}

// startHookForwarder 在图的 OnAgentStart 里把原话原样交给一个会记历史的 helper（比如防护 agent）。
type startHookForwarder struct {
	hook.NoOpHooks
	graph  string
	helper agent_v2.Agent
	err    error
}

func (h *startHookForwarder) OnAgentStart(ctx context.Context, info *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	if info.Name == h.graph {
		_, h.err = h.helper.Run(ctx, agent_v2.GetSession(ctx), in.Messages[len(in.Messages)-1])
	}
	return ctx, nil
}

// OnAgentStart 钩子里已经把原话原样记过（交给了会记历史的 helper），图里随后委派：原话不再补一份。审计实测：补不补只看
// 入队的那一批时，钩子那一批早于登记待补、取不走它，委派那一批又不含原话，于是记了两份。
func TestHistGraphAgent_TopLevel_StartHookRecordedInput_NotDeferredAgain(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	hooks := &startHookForwarder{graph: "start_fwd",
		helper: agent_v2.NewBasicAgent(agent_v2.WithName("guard"), agent_v2.WithModel(&identityProbeModel{reply: "guard_ans"}), agent_v2.WithMaxRounds(2))}
	leaf := agent_v2.NewBasicAgent(agent_v2.WithName("leaf"), agent_v2.WithModel(&identityProbeModel{reply: "ans"}), agent_v2.WithMaxRounds(2))
	inCh := compose_v2.LastValue[*schema.Message]("__start_fwd_in__")
	outCh := compose_v2.LastValue[*schema.Message]("__start_fwd_out__")
	bld := compose_v2.NewBuilder().WithName("g_start_fwd")
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, outCh)
	bld.AddNode("work", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		ev, err := agent_v2.RunSubAgent(ctx, "worker", leaf, schema.UserMessage("派生任务:"+in.Content))
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, agent_v2.FinalMessage(ev))}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "work")
	bld.AddEdge("work", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ga := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("start_fwd"), agent_v2.WithGraphAgentHooks(hooks),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_start_fwd")
	if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if hooks.err != nil {
		t.Fatalf("fixture 没搭对：钩子里的 helper 应跑成：%v", hooks.err)
	}
	msgs := loadHist(t, hs, ctx, "c_start_fwd")
	if historyIndexOf(msgs, schema.RoleAssistant, "guard_ans") < 0 || historyIndexOf(msgs, schema.RoleAssistant, "ans") < 0 {
		t.Fatalf("fixture 没搭对：helper 与委派的子 agent 的答复都应在历史里：%s", histDump(msgs))
	}
	if n := countUserWithContent(msgs, "原话"); n != 1 {
		t.Errorf("钩子里已经原样记过原话：应只有 1 份，实际 %d：%s", n, histDump(msgs))
	}
}

// 业务的输出读取器 panic（runGraph 里没兜住它，直接走到 OnAgentEnd 那个 defer），图里一条记录都没写：OnAgentEnd 钩子里起的
// 调用不把原话带出来。审计实测：待补只在 runGraph 正常返回之后收回，panic 时留着。
func TestHistGraphAgent_TopLevel_OutputReaderPanic_EndHookDoesNotFlushInput(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	hooks := &endHookHelper{graph: "reader_panic",
		helper: agent_v2.NewBasicAgent(agent_v2.WithName("helper"), agent_v2.WithModel(&identityProbeModel{reply: "helper_ans"}), agent_v2.WithMaxRounds(2))}
	inCh := compose_v2.LastValue[*schema.Message]("__reader_panic_in__")
	bld := compose_v2.NewBuilder().WithName("g_reader_panic")
	compose_v2.WithChannel(bld, inCh)
	bld.AddNode("work", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	bld.AddEdge(compose_v2.StartNode, "work")
	bld.AddEdge("work", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ga := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("reader_panic"), agent_v2.WithGraphAgentHooks(hooks),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { panic("读取器坏了") }))
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_reader_panic")
	if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err == nil {
		t.Fatal("fixture 没搭对：读取器 panic 应转成错误")
	}
	if hooks.err != nil {
		t.Fatalf("fixture 没搭对：钩子里的 helper 应跑成：%v", hooks.err)
	}
	msgs := loadHist(t, hs, ctx, "c_reader_panic")
	if historyIndexOf(msgs, schema.RoleAssistant, "helper_ans") < 0 {
		t.Fatalf("fixture 没搭对：helper 的答复应在历史里：%s", histDump(msgs))
	}
	if n := countUserWithContent(msgs, "原话"); n != 0 {
		t.Errorf("读取器 panic、图里一条记录都没写：原话不该被图外的记录带出来，实际 %d 份：%s", n, histDump(msgs))
	}
}
