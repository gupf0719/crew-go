package agent_v2_test

// 调用方前文「一次运行只认一个来源」的不变量（来源怎么定见 caller_history_owner_test.go）：
//   - 以服务端为准：读历史的 agent 看到的早先轮次全部来自历史层的记录，客户端交来的文本一条都不出现——裁过、改过、
//     编辑重发都一样；
//   - 以客户端为准：看到的就是客户端交来的那份，历史层的记录一条都不出现——没配历史层的 agent 接的轮、编辑重发后的新
//     分支都照客户端的样子；
//   - 建写入器时读不出历史层：这次运行以客户端为准；
//   - 两种情况下每一轮都恰好一份。

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// nodeCallsAgentGraph 建一个自己不配历史层的 GraphAgent：节点里经 RunSubAgent 调 pick(input) 选出的子 agent。
func nodeCallsAgentGraph(t *testing.T, name string, pick func(in *schema.Message) (string, agent_v2.Agent)) *agent_v2.GraphAgent {
	t.Helper()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	b := compose_v2.NewBuilder().WithName(name)
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("call", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		id, sub := pick(in)
		evs, err := agent_v2.RunSubAgent(ctx, id, sub, in)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, agent_v2.FinalMessage(evs))}, nil
	})
	b.AddEdge(compose_v2.StartNode, "call")
	b.AddEdge("call", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName(name),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
}

// aguiLikeClient 模拟 AG-UI 前端：每轮把此前的全部问答作为前文交来。
type aguiLikeClient struct{ turns []*schema.Message }

func (c *aguiLikeClient) run(t *testing.T, ag agent_v2.Agent, conv, q string) {
	t.Helper()
	ctx := agent_v2.WithCallerHistoryCtx(threadCtx(conv), c.turns)
	evs, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(q))
	if err != nil {
		t.Fatal(err)
	}
	c.turns = append(c.turns, schema.UserMessage(q), schema.AssistantMessage(agent_v2.FinalMessage(evs).Content))
}

// historyLines 读出历史层里这场对话的记录，写进去的前文前面加 "~"。
func historyLines(t *testing.T, st agent_v2.HistoryStore, conv string) []string {
	t.Helper()
	recs, err := agent_v2.LoadAllHistory(context.Background(), st, agent_v2.HistoryKey{Owner: "tenant-a", ConvID: conv})
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, r := range recs {
		line := string(r.Role) + ":" + r.Content
		if v, _ := r.Annotations[agent_v2.CallerHistoryAnnotation].(bool); v {
			line = "~" + line
		}
		lines = append(lines, line)
	}
	return lines
}

// 前端「编辑第二轮的问题并重发」：截掉那条之后的消息、带改过的问题重跑。以服务端为准，被放弃的轮次照旧在模型的视图
// 里、按追加顺序（编辑不改写服务端的记录；要编辑生效就让前端拥有历史，见下面以客户端为准的那条）。
func TestCallerHistory_ServerAuthoritative_ClientEditNotReflected(t *testing.T) {
	st := agent_v2.NewInMemoryHistoryStore()
	m := &callerSeenModel{reply: "答"}
	ag := newCallerAgent("assistant", m, agent_v2.WithHistory(st, "tenant-a"))
	c := &aguiLikeClient{}
	for _, q := range []string{"Q1", "Q2", "Q3"} {
		c.run(t, ag, "conv-edit", q)
	}
	edited := &aguiLikeClient{turns: slices.Clone(c.turns[:2])}
	edited.run(t, ag, "conv-edit", "Q2改")
	edited.run(t, ag, "conv-edit", "Q4")
	want := []string{"user:Q1", "assistant:答-1", "user:Q2", "assistant:答-2", "user:Q3", "assistant:答-3",
		"user:Q2改", "assistant:答-4", "user:Q4"}
	if got := m.call(t, 4); !slices.Equal(got, want) {
		t.Errorf("编辑后下一轮模型看到 %q，want %q", got, want)
	}
}

// 客户端交来的比历史层见过的少（前端只留了最近的轮次、去掉了开场白）：以服务端为准，客户端的不用、不写。
func TestCallerHistory_ServerAuthoritative_ClientTrimmed(t *testing.T) {
	t.Run("fewer_rounds", func(t *testing.T) {
		st := agent_v2.NewInMemoryHistoryStore()
		m := &callerSeenModel{reply: "答"}
		ag := newCallerAgent("assistant", m, agent_v2.WithHistory(st, "tenant-a"))
		c := &aguiLikeClient{}
		for _, q := range []string{"一", "二", "三"} {
			c.run(t, ag, "conv-trim", q)
		}
		trimmed := &aguiLikeClient{turns: c.turns[4:]} // 只留第三轮
		trimmed.run(t, ag, "conv-trim", "四")
		want := []string{"user:一", "assistant:答-1", "user:二", "assistant:答-2", "user:三", "assistant:答-3", "user:四"}
		if got := m.call(t, 3); !slices.Equal(got, want) {
			t.Errorf("第四轮模型看到 %q，want %q", got, want)
		}
		noCallerRecords(t, st, "conv-trim")
	})
	t.Run("greeting_dropped", func(t *testing.T) {
		st := agent_v2.NewInMemoryHistoryStore()
		m := &callerSeenModel{reply: "答"}
		ag := newCallerAgent("assistant", m, agent_v2.WithHistory(st, "tenant-a"))
		c := &aguiLikeClient{turns: []*schema.Message{schema.AssistantMessage("欢迎"), schema.UserMessage("我叫小明"),
			schema.AssistantMessage("你好，小明")}}
		c.run(t, ag, "conv-shift", "第一问")
		dropped := &aguiLikeClient{turns: c.turns[1:]} // 去掉了开场白
		dropped.run(t, ag, "conv-shift", "第二问")
		want := []string{"~assistant:欢迎", "~user:我叫小明", "~assistant:你好，小明", "user:第一问", "assistant:答-1", "user:第二问"}
		if got := m.call(t, 1); !slices.Equal(got, want) {
			t.Errorf("第二轮模型看到 %q，want %q", got, want)
		}
	})
}

// rewriteInputHooks 在 OnAgentStart 把输入换成带后缀的拷贝（业务做输入包装）。
type rewriteInputHooks struct{ hook.NoOpHooks }

func (rewriteInputHooks) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	if n := len(in.Messages); n > 0 && in.Messages[n-1] != nil {
		cp := *in.Messages[n-1]
		cp.Content += "（已登录）"
		in.Messages[n-1] = &cp
	}
	return ctx, nil
}

// 钩子改写了输入：以服务端为准，模型看到的是记下的那份（改写后的），客户端的原文不再出现一遍。
func TestCallerHistory_ServerAuthoritative_InputRewrittenByHook(t *testing.T) {
	st := agent_v2.NewInMemoryHistoryStore()
	m := &callerSeenModel{reply: "答"}
	ag := newCallerAgent("assistant", m, agent_v2.WithHistory(st, "tenant-a"), agent_v2.WithHooks(rewriteInputHooks{}))
	c := &aguiLikeClient{}
	c.run(t, ag, "conv-rewrite", "订机票")
	c.run(t, ag, "conv-rewrite", "订好了吗")
	want := []string{"user:订机票（已登录）", "assistant:答-1", "user:订好了吗（已登录）"}
	if got := m.call(t, 1); !slices.Equal(got, want) {
		t.Errorf("第二轮模型看到 %q，want %q", got, want)
	}
}

// failLoadsStore：failOnce 为 true 时下一次 Load 失败。
type failLoadsStore struct {
	*agent_v2.InMemoryHistoryStore
	mu       sync.Mutex
	failOnce bool
}

func (s *failLoadsStore) Load(ctx context.Context, key agent_v2.HistoryKey, q agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	s.mu.Lock()
	fail := s.failOnce
	s.failOnce = false
	s.mu.Unlock()
	if fail {
		return agent_v2.HistoryPage{}, errors.New("transient")
	}
	return s.InMemoryHistoryStore.Load(ctx, key, q)
}

// 这场对话早有记录，第二轮建写入器时那次读失败（判不出要不要写前文），后面的读正常：这次运行以客户端为准——每一轮
// 恰好一份，不会历史层一份、客户端一份。
func TestCallerHistory_WriterLoadFails_ClientPriorOnce(t *testing.T) {
	st := &failLoadsStore{InMemoryHistoryStore: agent_v2.NewInMemoryHistoryStore()}
	m := &callerSeenModel{reply: "答"}
	ag := newCallerAgent("assistant", m, agent_v2.WithHistory(st, "tenant-a"))
	c := &aguiLikeClient{turns: earlierTurns()}
	c.run(t, ag, "conv-flake", "第一问")
	st.mu.Lock()
	st.failOnce = true // 第二轮建写入器时那次读
	st.mu.Unlock()
	c.run(t, ag, "conv-flake", "第二问")
	want := append(slices.Clone(wantEarlier), "~user:第一问", "~assistant:答-1", "user:第二问")
	if got := m.call(t, 1); !slices.Equal(got, want) {
		t.Errorf("第二轮模型看到 %q，want %q", got, want)
	}
}

// GraphAgent 只包了一个对话 agent，历史层配在它身上（嵌套写入器，每一轮都是它接）：以客户端为准，每轮都只有一份。
func TestCallerHistory_ClientAuthoritative_NestedWriterEveryRound(t *testing.T) {
	st := agent_v2.NewInMemoryHistoryStore()
	m := &callerSeenModel{reply: "答"}
	chat := newCallerAgent("chat", m, agent_v2.WithHistory(st, "tenant-a"))
	app := nodeCallsAgentGraph(t, "app", func(*schema.Message) (string, agent_v2.Agent) { return "chat", chat })
	c := &aguiLikeClient{}
	for _, q := range []string{"我叫小明", "我订了去北京的机票", "我叫什么、订了什么？"} {
		c.run(t, app, "conv-wrap", q)
	}
	want := []string{"~user:我叫小明", "~assistant:答-1", "~user:我订了去北京的机票", "~assistant:答-2", "user:我叫什么、订了什么？"}
	if got := m.call(t, 2); !slices.Equal(got, want) {
		t.Errorf("第三轮模型看到 %q，want %q", got, want)
	}
}

// 配了历史层的 A 只接第一、三轮，第二轮由没配历史层的 B 接（GraphAgent 节点里按内容选）：以客户端为准，第三轮 A 看到
// 三轮按时间顺序、各一份，B 接的那一轮没有消失。
func TestCallerHistory_ClientAuthoritative_RoundWithoutWriterKept(t *testing.T) {
	st := agent_v2.NewInMemoryHistoryStore()
	ma, mb := &callerSeenModel{reply: "已订好"}, &callerSeenModel{reply: "已取消"}
	a := newCallerAgent("booker", ma, agent_v2.WithHistory(st, "tenant-a"))
	bAg := newCallerAgent("canceller", mb)
	app := nodeCallsAgentGraph(t, "app2", func(in *schema.Message) (string, agent_v2.Agent) {
		if strings.Contains(in.Content, "取消") {
			return "b", bAg
		}
		return "a", a
	})
	c := &aguiLikeClient{}
	for _, q := range []string{"帮我订机票", "取消刚才的订单", "我现在有订单吗？"} {
		c.run(t, app, "conv-skip", q)
	}
	want := []string{"~user:帮我订机票", "~assistant:已订好-1", "~user:取消刚才的订单", "~assistant:已取消-1", "user:我现在有订单吗？"}
	if got := ma.call(t, 1); !slices.Equal(got, want) {
		t.Errorf("第三轮 A 看到 %q，want %q", got, want)
	}
	noCallerRecords(t, st, "conv-skip")
}

// 以客户端为准时编辑重发照前端的样子：被放弃的那一支不出现，之后由没配历史层的 agent 接的轮也在（曾经编辑一次之后，
// 没有写入器的轮从此在所有视图里消失）。
func TestCallerHistory_ClientAuthoritative_EditReflected(t *testing.T) {
	st := agent_v2.NewInMemoryHistoryStore()
	ma, mb := &callerSeenModel{reply: "A"}, &callerSeenModel{reply: "B"}
	main := newCallerAgent("main", ma, agent_v2.WithHistory(st, "tenant-a"))
	side := newCallerAgent("side", mb)
	app := nodeCallsAgentGraph(t, "app-edit", func(in *schema.Message) (string, agent_v2.Agent) {
		if strings.Contains(in.Content, "旁") {
			return "b", side
		}
		return "a", main
	})
	c := &aguiLikeClient{}
	for _, q := range []string{"一", "二", "三"} {
		c.run(t, app, "conv-edit-client", q)
	}
	edited := &aguiLikeClient{turns: slices.Clone(c.turns[:2])} // 编辑第二轮：截掉第二轮及之后
	for _, q := range []string{"二改", "旁问", "四"} {
		edited.run(t, app, "conv-edit-client", q)
	}
	want := []string{"~user:一", "~assistant:A-1", "~user:二改", "~assistant:A-4", "~user:旁问", "~assistant:B-1", "user:四"}
	if got := ma.call(t, 4); !slices.Equal(got, want) {
		t.Errorf("编辑后模型看到 %q，want %q", got, want)
	}
}

// RoutingAgent 第一轮路由到 A、第二轮到 B（成员 SelfOnly）：没配历史层时 B 看到客户端交来的第一轮；配了历史层时以服务端
// 为准，B 的视图里没有客户端的文本（SelfOnly 下 B 看不到 A 接的那一轮，跟没有调用方前文时一样）。
func TestCallerHistory_RoutingMember_ViewFollowsSource(t *testing.T) {
	for _, history := range []bool{false, true} {
		st := agent_v2.NewInMemoryHistoryStore()
		ma, mb := &callerSeenModel{reply: "A"}, &callerSeenModel{reply: "B"}
		var opts []agent_v2.Option
		if history {
			opts = append(opts, agent_v2.WithHistory(st, "tenant-a"))
		}
		r := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("router"),
			agent_v2.WithRouter(func(_ context.Context, in *schema.Message) (compose_v2.NodeID, error) {
				if strings.Contains(in.Content, "退") {
					return "b", nil
				}
				return "a", nil
			})).
			AddBranch("a", newCallerAgent("booker", ma, opts...)).
			AddBranch("b", newCallerAgent("refunder", mb, opts...))
		c := &aguiLikeClient{}
		c.run(t, r, "conv-route", "帮我订去北京的机票")
		c.run(t, r, "conv-route", "我要退票")
		got := mb.call(t, 0)
		if !history {
			if want := []string{"~user:帮我订去北京的机票", "~assistant:A-1", "user:我要退票"}; !slices.Equal(got, want) {
				t.Errorf("没配历史层：第二轮 B 看到 %q，want %q", got, want)
			}
			continue
		}
		for _, l := range got {
			if strings.HasPrefix(l, "~") {
				t.Errorf("配了历史层：第二轮 B 看到了客户端的文本 %q（应以服务端为准）", got)
				break
			}
		}
		if len(got) == 0 || got[len(got)-1] != "user:我要退票" {
			t.Errorf("配了历史层：第二轮 B 看到 %q，应以这一轮的输入结尾", got)
		}
	}
}
