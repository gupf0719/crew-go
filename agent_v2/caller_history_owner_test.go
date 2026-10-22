package agent_v2_test

// 谁拥有这场对话（决定一次运行以谁为准）的不变量：
//   - 拥有者 = 建写入器时还不在任何正在执行的任务里的那一个：顶层 agent，或者从整棵静态子树借到配置的最外层编排；
//   - 在钩子里、节点里、工具里（嵌套调用）调起的 agent 自己配的历史层不拥有这场对话：它看到客户端的前文（它的子对话
//     记录这次不读），也不把前文写进去——它的子对话输入跟用户的问题同文时，用户收到的答复照样在；
//   - 嵌套的编排里配的历史层由最外层的编排拥有；树里两处配得不一样直接报错。

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// helperInHookHooks 在 OnAgentStart 经 RunSubAgent 把这一轮的输入交给 helper（业务在钩子里做预查询）。
type helperInHookHooks struct {
	hook.NoOpHooks
	helper agent_v2.Agent
}

func (h helperInHookHooks) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	if n := len(in.Messages); n > 0 && in.Messages[n-1] != nil {
		if _, err := agent_v2.RunSubAgent(ctx, "lookup", h.helper, in.Messages[n-1]); err != nil {
			return ctx, err
		}
	}
	return ctx, nil
}

// noCallerRecords 断言历史层里这场对话没有写进去的前文。
func noCallerRecords(t *testing.T, st agent_v2.HistoryStore, conv string) {
	t.Helper()
	for _, l := range historyLines(t, st, conv) {
		if strings.HasPrefix(l, "~") {
			t.Errorf("不拥有这场对话的写入器把前文写进了历史层：%q", historyLines(t, st, conv))
			return
		}
	}
}

// 顶层 agent 没配历史层，它的 OnAgentStart 把输入交给配了历史层的 lookup：钩子里已经在 main 的任务里，lookup 的写入器
// 不拥有这场对话。第二轮 lookup 看到客户端的前文——里面有 main 给用户的答复（以它自己的子对话为准的话，那条答复就丢了：
// 子对话的输入跟用户的问题同文）。
func TestCallerHistory_HookHelper_NotOwner(t *testing.T) {
	st := agent_v2.NewInMemoryHistoryStore()
	ml, mm := &callerSeenModel{reply: "L"}, &callerSeenModel{reply: "M"}
	lookup := newCallerAgent("lookup", ml, agent_v2.WithHistory(st, "tenant-a"))
	main := newCallerAgent("main", mm, agent_v2.WithHooks(helperInHookHooks{helper: lookup}))
	c := &aguiLikeClient{}
	c.run(t, main, "conv-hook-helper", "我的订单到哪了")
	c.run(t, main, "conv-hook-helper", "再确认一下")
	want := []string{"~user:我的订单到哪了", "~assistant:M-1", "user:再确认一下"}
	if got := ml.call(t, 1); !slices.Equal(got, want) {
		t.Errorf("第二轮 lookup 看到 %q，want %q", got, want)
	}
	noCallerRecords(t, st, "conv-hook-helper")
}

// twoStepGraph：GraphAgent（自己不配历史层），节点里先经 RunSubAgent 调 lookup（输入是原输入），再调 composer 写给用户的
// 答复。
func twoStepGraph(t *testing.T, name string, lookup, composer agent_v2.Agent) *agent_v2.GraphAgent {
	t.Helper()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	b := compose_v2.NewBuilder().WithName(name)
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("call", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		if _, err := agent_v2.RunSubAgent(ctx, "lookup", lookup, in); err != nil {
			return nil, err
		}
		evs, err := agent_v2.RunSubAgent(ctx, "composer", composer, schema.UserMessage("据此作答"))
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

// 同一个形状换成节点里调起：GraphAgent 节点把原输入交给配了历史层的 lookup，再由没配历史层的 composer 写给用户的答复。
func TestCallerHistory_NodeHelper_NotOwner(t *testing.T) {
	st := agent_v2.NewInMemoryHistoryStore()
	ml, mc := &callerSeenModel{reply: "L"}, &callerSeenModel{reply: "C"}
	app := twoStepGraph(t, "app-helper", newCallerAgent("lookup", ml, agent_v2.WithHistory(st, "tenant-a")), newCallerAgent("composer", mc))
	c := &aguiLikeClient{}
	c.run(t, app, "conv-node-helper", "我的订单到哪了")
	c.run(t, app, "conv-node-helper", "再确认一下")
	want := []string{"~user:我的订单到哪了", "~assistant:C-1", "user:再确认一下"}
	if got := ml.call(t, 1); !slices.Equal(got, want) {
		t.Errorf("第二轮 lookup 看到 %q，want %q", got, want)
	}
	noCallerRecords(t, st, "conv-node-helper")
}

// forwardToolModel 是父 agent 的模型：每一轮先把这一轮的用户问题（加 suffix）转给子 agent 工具，看到工具结果后回答
// 「父答:<问题>」。
type forwardToolModel struct {
	tool   string
	suffix string
}

func (m forwardToolModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	last := -1
	for i, x := range msgs {
		if x != nil && x.Role == schema.RoleUser {
			last = i
		}
	}
	q := ""
	if last >= 0 {
		q = msgs[last].Content
		for _, x := range msgs[last:] {
			if x != nil && x.Role == schema.RoleTool {
				return schema.AssistantMessage("父答:" + q), nil
			}
		}
	}
	args, _ := json.Marshal(map[string]string{"input": q + m.suffix})
	return schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "c1", Type: "function", Function: schema.FunctionCall{Name: m.tool, Arguments: string(args)}},
	}), nil
}
func (m forwardToolModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m forwardToolModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// 父 agent 没配历史层、AgentAsTool 子 agent 配了：子 agent 在工具里被调起，不拥有这场对话。父把用户的问题原样（或加个
// 句号）转给它时，第二轮它都看到客户端的前文、各一份，父给用户的答复在里面。
func TestCallerHistory_AgentAsToolChild_NotOwner(t *testing.T) {
	for _, tc := range []struct{ name, suffix string }{{"same_text", ""}, {"different_text", "。"}} {
		t.Run(tc.name, func(t *testing.T) {
			st := agent_v2.NewInMemoryHistoryStore()
			child := &callerSeenModel{reply: "子"}
			tk := tool.NewToolkit()
			tk.Register(agent_v2.AgentAsTool(newCallerAgent("child", child, agent_v2.WithHistory(st, "tenant-a"))))
			parent := agent_v2.NewBasicAgent(agent_v2.WithName("parent"), agent_v2.WithDescription("p"),
				agent_v2.WithModel(forwardToolModel{tool: "child", suffix: tc.suffix}), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3))
			conv := "conv-subtool-" + tc.name
			c := &aguiLikeClient{}
			c.run(t, parent, conv, "帮我查订单A")
			c.run(t, parent, conv, "然后呢")
			want := []string{"~user:帮我查订单A", "~assistant:父答:帮我查订单A", "user:然后呢" + tc.suffix}
			if got := child.call(t, 1); !slices.Equal(got, want) {
				t.Errorf("第二轮子 agent 看到 %q，want %q", got, want)
			}
			noCallerRecords(t, st, conv)
		})
	}
}

// callTwoChildrenModel：每一轮对话先一批并发调两个子 agent 工具，看到这一轮的工具结果后收尾。
type callTwoChildrenModel struct{ a, b string }

func (m callTwoChildrenModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	last := 0
	for i, x := range msgs {
		if x != nil && x.Role == schema.RoleUser {
			last = i
		}
	}
	for _, x := range msgs[last:] {
		if x != nil && x.Role == schema.RoleTool {
			return schema.AssistantMessage("完成"), nil
		}
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "c1", Type: "function", Function: schema.FunctionCall{Name: m.a, Arguments: `{"input":"任务甲"}`}},
		{ID: "c2", Type: "function", Function: schema.FunctionCall{Name: m.b, Arguments: `{"input":"任务乙"}`}},
	}), nil
}
func (m callTwoChildrenModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m callTwoChildrenModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// 同一批两个 AgentAsTool 子 agent 各自配了历史、并发开写入器：都不拥有这场对话，各看到一份前文，前文不写进去。
func TestCallerHistory_ConcurrentNestedWriters_NoSeed(t *testing.T) {
	st := agent_v2.NewInMemoryHistoryStore()
	ma, mb := &callerSeenModel{reply: "甲"}, &callerSeenModel{reply: "乙"}
	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(newCallerAgent("child_a", ma, agent_v2.WithHistory(st, "tenant-a"))))
	tk.Register(agent_v2.AgentAsTool(newCallerAgent("child_b", mb, agent_v2.WithHistory(st, "tenant-a"))))
	parent := agent_v2.NewBasicAgent(agent_v2.WithName("parent"), agent_v2.WithDescription("p"),
		agent_v2.WithModel(callTwoChildrenModel{a: "child_a", b: "child_b"}), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3))
	ctx := agent_v2.WithCallerHistoryCtx(threadCtx("conv-nested-race"), earlierTurns())
	if _, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("帮我办两件事")); err != nil {
		t.Fatal(err)
	}
	for name, m := range map[string]*callerSeenModel{"甲": ma, "乙": mb} {
		if got := m.call(t, 0); len(got) < 2 || !slices.Equal(got[:2], wantEarlier) || countPrefixLines(got, "我叫小明") != 1 {
			t.Errorf("子 agent %s 看到 %q，应以前文开头、只有一份", name, got)
		}
	}
	noCallerRecords(t, st, "conv-nested-race")
}

// 外层编排的直接子是编排、配了历史层的 BasicAgent 在更深一层：最外层的编排拥有写入器，以服务端为准——客户端交回的那份
// 答复跟记录不一样时（前端改过，或者流式拿到的是钩子改写前的文本），模型看到的是记录，不是两份都有。
func TestCallerHistory_DoublyNestedOrchestration_OuterOwns(t *testing.T) {
	st := agent_v2.NewInMemoryHistoryStore()
	m := &callerSeenModel{reply: "答"}
	inner := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("inner")).
		AddStep("chat", newCallerAgent("chat", m, agent_v2.WithHistory(st, "tenant-a")))
	outer := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("outer")).AddStep("flow", inner)
	c := &aguiLikeClient{}
	c.run(t, outer, "conv-nested-orch", "Q1")
	c.turns[1] = schema.AssistantMessage("答-1（客户端改过）")
	c.run(t, outer, "conv-nested-orch", "Q2")
	want := []string{"user:Q1", "assistant:答-1", "user:Q2"}
	if got := m.call(t, 1); !slices.Equal(got, want) {
		t.Errorf("第二轮 chat 看到 %q，want %q（以服务端为准）", got, want)
	}
}

// renamedHandoff 是业务给编排包的一层值类型装饰（改个名字）：内嵌指针，编排的方法都提升上来。
type renamedHandoff struct{ *agent_v2.HandoffAgent }

func (r renamedHandoff) Name() string { return "renamed" }

// 值类型包装内嵌编排指针、转交候选里有它自己（组合成环）：借历史配置那一趟停得下来，配不配历史层都一样（曾经只认指针
// 去重，值类型包装每一圈都是新值，无限递归、整个进程栈溢出——每个编排的 Run 入口都要走这一趟）。
func TestCallerHistory_ValueWrapperCycle_Terminates(t *testing.T) {
	for _, withHistory := range []bool{false, true} {
		m := &callerSeenModel{reply: "答"}
		var opts []agent_v2.Option
		if withHistory {
			opts = append(opts, agent_v2.WithHistory(agent_v2.NewInMemoryHistoryStore(), "tenant-a"))
		}
		h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("h"), agent_v2.WithHandoffDescription("h"),
			agent_v2.WithHandoffInitial("chat"))
		h.AddAgent("chat", newCallerAgent("chat", m, opts...))
		h.AddAgent("again", renamedHandoff{h})
		if _, err := h.Run(threadCtx("conv-cycle"), agent_v2.NewRunSession(), schema.UserMessage("Q1")); err != nil {
			t.Fatalf("withHistory=%v：%v", withHistory, err)
		}
		if got := m.call(t, 0); len(got) == 0 || got[len(got)-1] != "user:Q1" {
			t.Errorf("withHistory=%v：chat 看到 %q，应以这一轮的输入结尾", withHistory, got)
		}
	}
}

// 冲突报错说清是哪两个 agent、差在哪：同一个 store、同一个 owner，只是两处各传了一个 WithHistoryOwnerFunc 函数字面量
// （返回值相同）——按函数值比算不同，报错要说出来，不然业务只看到 owner "t" vs "t"。
func TestCallerHistory_ConflictMessage_NamesAgentsAndDiff(t *testing.T) {
	st := agent_v2.NewInMemoryHistoryStore()
	a := newCallerAgent("member_a", &callerSeenModel{reply: "A"},
		agent_v2.WithHistory(st, "", agent_v2.WithHistoryOwnerFunc(func(context.Context) string { return "t" })))
	b := newCallerAgent("member_b", &callerSeenModel{reply: "B"},
		agent_v2.WithHistory(st, "", agent_v2.WithHistoryOwnerFunc(func(context.Context) string { return "t" })))
	outer := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("outer")).
		AddStep("sa", agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("sa")).AddStep("a", a)).
		AddStep("sb", agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("sb")).AddStep("b", b))
	_, err := outer.Run(threadCtx("conv-conflict-msg"), agent_v2.NewRunSession(), schema.UserMessage("q"))
	if err == nil {
		t.Fatal("两处 owner 函数不是同一个，应报配置冲突")
	}
	for _, want := range []string{"conflicting history config", `"member_a"`, `"member_b"`, "WithHistoryOwnerFunc"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("报错 %q 应含 %q", err.Error(), want)
		}
	}
}

// 整棵静态子树里两处配了不同的历史层（嵌套的编排里那一处也算）：直接报错，不静默挑一个。
func TestCallerHistory_NestedOrchestrationConflict_Rejected(t *testing.T) {
	a := newCallerAgent("a", &callerSeenModel{reply: "A"}, agent_v2.WithHistory(agent_v2.NewInMemoryHistoryStore(), "tenant-a"))
	b := newCallerAgent("b", &callerSeenModel{reply: "B"}, agent_v2.WithHistory(agent_v2.NewInMemoryHistoryStore(), "tenant-a"))
	inner := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("inner")).AddStep("a", a)
	outer := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("outer")).AddStep("flow", inner).AddStep("b", b)
	_, err := outer.Run(threadCtx("conv-conflict"), agent_v2.NewRunSession(), schema.UserMessage("q"))
	if err == nil || !strings.Contains(err.Error(), "conflicting history config") {
		t.Fatalf("嵌套的编排里配了另一个历史层，应报配置冲突，got %v", err)
	}
}
