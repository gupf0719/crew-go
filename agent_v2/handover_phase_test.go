package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// 把一次执行交给被调方（compose_v2.HandOver）与业务钩子（compose_v2.EnterPhase）之后的几条性质：
// 被调方主图的入口否决重做的是调用方的产出；被调方、钩子各有自己的提问与嵌套调用计数与相位名；
// 暂停过的嵌套调用在历史里只记一份输入。

// hoJudge 是一个 GraphAgent：入口条件边读输入，内容含 bad 时按 CondFailRerunFrom 否决，放行时产出
// 「accepted:<输入>」。
func hoJudge(t *testing.T, name, bad string, evals *int) agent_v2.Agent {
	t.Helper()
	inCh := compose_v2.LastValue[*schema.Message](name + "_in")
	outCh := compose_v2.LastValue[*schema.Message](name + "_out")
	b := compose_v2.NewBuilder().WithName(name)
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("x", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("accepted:"+in.Content))}, nil
	})
	b.AddConditionalEdge(compose_v2.StartNode, func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		*evals++
		if in, _ := compose_v2.Get(chs, inCh); in != nil && strings.Contains(in.Content, bad) {
			return "", errors.New(in.Content + " 不合格")
		}
		return "x", nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	b.AddEdge("x", compose_v2.EndNode)
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

// hoFuncAgent 是只实现了 Run 的 agent 桩。
type hoFuncAgent struct {
	name string
	run  func(ctx context.Context, in *schema.Message) ([]*agent_v2.AgentEvent, error)
}

func (a *hoFuncAgent) Run(ctx context.Context, _ *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	return a.run(ctx, in)
}
func (a *hoFuncAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, errors.New("hoFuncAgent: 不支持 Resume")
}
func (a *hoFuncAgent) Name() string        { return a.name }
func (a *hoFuncAgent) Description() string { return a.name }

// hoDrive 跑一张 compose_v2 图：首跑之后不管什么错都续跑（问到的都答「批准#<第几问>」、累积传），最多
// 6 次。返回续跑次数、最后的错、依次问过的问题。
func hoDrive(g *compose_v2.Graph) (resumes int, err error, asked []string) {
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()), compose_v2.WithSessionID("s")}
	answers := map[string]*schema.Message{}
	err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...)
	for ; resumes < 6 && err != nil; resumes++ {
		if ie, ok := compose_v2.IsInterrupt(err); ok {
			for _, q := range ie.AllQueries() {
				asked = append(asked, q.Question)
				answers[q.InterruptID] = schema.UserMessage(fmt.Sprintf("批准#%d", len(asked)))
			}
		}
		err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
			append(append([]compose_v2.RunOption{}, opts...), compose_v2.WithResumeAnswers(answers))...)
	}
	return resumes, err, asked
}

func hoHostGraph(t *testing.T, name string, body compose_v2.NodeFunc) *compose_v2.Graph {
	t.Helper()
	b := compose_v2.NewBuilder().WithName(name)
	b.AddNode("host", body)
	b.AddEdge(compose_v2.StartNode, "host")
	b.AddEdge("host", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// TestRunSubAgent_JudgeEntryRejection_RedoesRerunnableProducer：节点里经 RunSubAgent（WithRerunnable）
// 起草，再经 RunSubAgent 交给一个入口否决 v1 的 GraphAgent。起草那次调用是产出，要作废重做：续跑
// 一次、起草两次收敛。
//
// 曾经被调方主图的入口否决推进的是交给被调方的那一格（HandOver 之前的 WithNodeStateSealed），调用方
// 起草的那次调用原样拿回：续跑 4 次每次都是「v1 不合格」（实测，相对上一轮是回退）。
func TestRunSubAgent_JudgeEntryRejection_RedoesRerunnableProducer(t *testing.T) {
	evals, drafts := 0, 0
	judge := hoJudge(t, "hp_judge", "draft-v1", &evals)
	drafter := &hoFuncAgent{name: "drafter", run: func(context.Context, *schema.Message) ([]*agent_v2.AgentEvent, error) {
		drafts++
		return []*agent_v2.AgentEvent{agent_v2.NewExitEvent("drafter", schema.AssistantMessage(fmt.Sprintf("draft-v%d", drafts)))}, nil
	}}
	var final string
	g := hoHostGraph(t, "hp_producer", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ev, err := agent_v2.RunSubAgent(ctx, "drafter", drafter, schema.UserMessage("写草稿"), agent_v2.WithRerunnable())
		if err != nil {
			return nil, err
		}
		ev, err = agent_v2.RunSubAgent(ctx, "judge", judge, agent_v2.FinalMessage(ev))
		if err != nil {
			return nil, err
		}
		final = agent_v2.FinalMessage(ev).Content
		return nil, nil
	})
	resumes, err, _ := hoDrive(g)
	if err != nil {
		t.Fatalf("续跑 %d 次仍失败：%v", resumes, err)
	}
	if resumes != 1 || drafts != 2 || final != "accepted:draft-v2" {
		t.Errorf("应续跑 1 次、起草 2 次、最终接受 v2，实际续跑 %d 次、起草 %d 次、产出 %q", resumes, drafts, final)
	}
}

// TestRunSubAgent_JudgeRejection_NestedApprovalAsksNewContent：节点体每次现起草 plan-vN，先在节点里直接跑
// 的嵌套图里问「批准 plan-vN 吗」，再经 RunSubAgent 交给判官（入口否决 v1）。否决之后重跑出来的新方案
// 必须重新问，不能拿对 v1 的批准兑付。
//
// 曾经判官的否决推进的是它自己那一格，节点体的代际不变，嵌套图里的提问 ID 不变：只问过「批准
// plan-v1 吗」，执行的是「plan-v3 已批准(批准#1)」（实测，err=nil）。
func TestRunSubAgent_JudgeRejection_NestedApprovalAsksNewContent(t *testing.T) {
	evals := 0
	judge := hoJudge(t, "hp_judge_ap", "plan-v1", &evals)
	planCh := compose_v2.LastValue[string]("hp_ap_plan")
	outCh := compose_v2.LastValue[string]("hp_ap_out")
	ab := compose_v2.NewBuilder().WithName("hp_ap_confirm")
	compose_v2.WithChannel(ab, planCh)
	compose_v2.WithChannel(ab, outCh)
	ab.AddNode("confirm", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		p, _ := compose_v2.Get(chs, planCh)
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准 " + p + " 吗"})
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, p+" 已批准("+ans.Content+")")}, nil
	})
	ab.AddEdge(compose_v2.StartNode, "confirm")
	ab.AddEdge("confirm", compose_v2.EndNode)
	confirm, err := ab.Compile()
	if err != nil {
		t.Fatal(err)
	}
	plans := 0
	var approved []string
	g := hoHostGraph(t, "hp_ap_host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		plans++
		p := fmt.Sprintf("plan-v%d", plans)
		cm := compose_v2.NewChannelManager()
		if err := compose_v2.Run(ctx, confirm, cm, compose_v2.WithRunNS("hp_ap_confirm"),
			compose_v2.WithInitialWrites(compose_v2.Write(planCh, p))); err != nil {
			return nil, err
		}
		out, _ := compose_v2.Get(cm.Channels(), outCh)
		approved = append(approved, out)
		_, err := agent_v2.RunSubAgent(ctx, "judge", judge, schema.UserMessage(out))
		return nil, err
	})
	_, err, asked := hoDrive(g)
	if err != nil {
		t.Fatalf("没收敛：%v", err)
	}
	for _, a := range approved {
		var k, m int
		if _, serr := fmt.Sscanf(a, "plan-v%d 已批准(批准#%d)", &k, &m); serr != nil {
			t.Fatalf("产出格式不对：%q", a)
		}
		if m < 1 || m > len(asked) || asked[m-1] != fmt.Sprintf("批准 plan-v%d 吗", k) {
			t.Errorf("%q 用的答案答的不是它自己的问题（问过 %v）——新方案没被问过就被批准了", a, asked)
		}
	}
}

// TestRunSubAgent_PendingPlainCall_NotReaskedWhenSiblingRejected：节点体里一次普通嵌套调用（会问人、挂着）
// 与一张入口否决 v1 的嵌套图同批失败。否决让节点体重做，但那次普通调用的身份没变——它里面已经答过的
// 问题不该换 ID 再问。
//
// 曾经被调方主图的提问带着调用方的外层代际：节点体一换代，已答过的「批准预算吗」换了 ID 被再问一遍
// （实测）。
func TestRunSubAgent_PendingPlainCall_NotReaskedWhenSiblingRejected(t *testing.T) {
	aIn := compose_v2.LastValue[*schema.Message]("hp_pp_in")
	aOut := compose_v2.LastValue[*schema.Message]("hp_pp_out")
	ab := compose_v2.NewBuilder().WithName("hp_pp_approver")
	compose_v2.WithChannel(ab, aIn)
	compose_v2.WithChannel(ab, aOut)
	ab.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准预算吗"})
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(aOut, schema.AssistantMessage("预算:"+ans.Content))}, nil
	})
	ab.AddEdge(compose_v2.StartNode, "ask")
	ab.AddEdge("ask", compose_v2.EndNode)
	ag, err := ab.Compile()
	if err != nil {
		t.Fatal(err)
	}
	approver := agent_v2.NewGraphAgent(ag, agent_v2.WithGraphAgentName("approver"),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(aIn, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			o, _ := compose_v2.Get(chs, aOut)
			return o
		}))
	draftCh := compose_v2.LastValue[string]("hp_pp_draft")
	jb := compose_v2.NewBuilder().WithName("hp_pp_judge")
	compose_v2.WithChannel(jb, draftCh)
	jb.AddNode("x", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	jb.AddConditionalEdge(compose_v2.StartNode, func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		if d, _ := compose_v2.Get(chs, draftCh); d == "v1" {
			return "", errors.New("v1 不合格")
		}
		return "x", nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	jb.AddEdge("x", compose_v2.EndNode)
	judge, err := jb.Compile()
	if err != nil {
		t.Fatal(err)
	}
	drafts := 0
	g := hoHostGraph(t, "hp_pp_host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, errA := agent_v2.RunSubAgent(ctx, "approver", approver, schema.UserMessage("预算 100"))
		d, ok := compose_v2.GetStepResult[string](ctx, "draft")
		if !ok {
			drafts++
			d = fmt.Sprintf("v%d", drafts)
			if err := compose_v2.SaveStepResult(ctx, "draft", d, compose_v2.Rerunnable()); err != nil {
				return nil, err
			}
		}
		errG := compose_v2.Run(ctx, judge, compose_v2.NewChannelManager(), compose_v2.WithRunNS("hp_pp_judge"),
			compose_v2.WithInitialWrites(compose_v2.Write(draftCh, d)))
		if errA != nil || errG != nil {
			return nil, errors.Join(errA, errG)
		}
		return nil, nil
	})
	_, err, asked := hoDrive(g)
	if err != nil {
		t.Fatalf("没收敛：%v", err)
	}
	if drafts != 2 {
		t.Fatalf("fixture 没搭对：v1 被否决后应重起草，实际起草 %d 次", drafts)
	}
	if len(asked) != 1 {
		t.Errorf("「批准预算吗」应只问一次，实际问过 %v", asked)
	}
}

// hoAskInToolStart 在 OnToolStart 里问一次人（钩子相位的提问）。
type hoAskInToolStart struct {
	hook.NoOpHooks
	answers []string
}

func (h *hoAskInToolStart) OnToolStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ToolInput) (context.Context, error) {
	ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "钩子：允许调这个工具吗"})
	if err != nil {
		return ctx, err
	}
	h.answers = append(h.answers, ans.Content)
	return ctx, nil
}

// hoResume 带累积答案续跑一个 agent 到收敛（最多 5 轮），记下问过的问题（按问题计数）。
func hoResume(ctx context.Context, ag agent_v2.Agent, sess *agent_v2.RunSession, err error) (int, error, map[string]int) {
	answers := map[string]*schema.Message{}
	asked := map[string]int{}
	rounds := 0
	for ; rounds < 5; rounds++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			break
		}
		for _, q := range ie.AllQueries() {
			asked[q.Question]++
			answers[q.InterruptID] = schema.UserMessage("答:" + q.Question)
		}
		_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	}
	return rounds, err, asked
}

// TestToolHook_EmptyToolCallID_HookAndToolAsksBothResolve：provider 不回 tool_call ID，OnToolStart 钩子与
// 工具体各问一次人。两问是不同的问题，各答各的、一轮一问地收敛。
//
// 曾经两问算出同一个 ID：用户答了钩子那问，工具体那问被判「答案已兑付」、整个 Run 终局失败（实测）。
func TestToolHook_EmptyToolCallID_HookAndToolAsksBothResolve(t *testing.T) {
	var toolGot []string
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("pay", "pay", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "工具：确认扣款吗"})
			if err != nil {
				return nil, err
			}
			toolGot = append(toolGot, ans.Content)
			return tool.Success("ok"), nil
		}))
	h := &hoAskInToolStart{}
	tc := schema.ToolCall{ID: "", Type: "function", Function: schema.FunctionCall{Name: "pay", Arguments: `{}`}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("p"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithHooks(h),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantToolCallMessage([]schema.ToolCall{tc}), schema.AssistantMessage("done")})))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("go"))
	rounds, err, asked := hoResume(ctx, ag, sess, err)
	if err != nil {
		t.Fatalf("续跑 %d 轮仍失败（问过 %v）：%v", rounds, asked, err)
	}
	if asked["钩子：允许调这个工具吗"] != 1 || asked["工具：确认扣款吗"] != 1 {
		t.Errorf("两问应各问一次，实际 %v", asked)
	}
	if len(toolGot) != 1 || toolGot[0] != "答:工具：确认扣款吗" {
		t.Errorf("工具体应拿到自己那一问的答案，实际 %v", toolGot)
	}
}

// hoKeyedCheckInToolStart 在 OnToolStart 里带同一个 call key 调同一个子 agent（按订单号预检）。
type hoKeyedCheckInToolStart struct {
	hook.NoOpHooks
	sub agent_v2.Agent
}

func (h *hoKeyedCheckInToolStart) OnToolStart(ctx context.Context, _ *hook.AgentInfo, ti *schema.ToolInput) (context.Context, error) {
	_, err := agent_v2.RunSubAgent(ctx, "checker", h.sub, schema.UserMessage("预检:"+ti.CallId), agent_v2.WithCallKey("O-1"))
	return ctx, err
}

// TestToolHook_SameCallKeyInTwoToolCalls_NotRejected：同一批两个 tool_call 各触发一次 OnToolStart，每次
// 都带同一个 call key 调同一个子 agent。两次钩子调用是两个相位，各自的调用不撞。
//
// 曾经工具钩子只封了私有态、共用节点的嵌套调用计数：第二次钩子调用被判成「同一个位置上重复用 key」，
// 整个 Run 终局失败（实测）。
func TestToolHook_SameCallKeyInTwoToolCalls_NotRejected(t *testing.T) {
	log := &inputLog{}
	sub := newSingleNodeAgent(t, "checker", "work", log,
		func(_ context.Context, c string) (string, error) { return "ok(" + c + ")", nil })
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("noop", "noop", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("noop ok"), nil }))
	host := agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithHooks(&hoKeyedCheckInToolStart{sub: sub}),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "c1", Type: "function", Function: schema.FunctionCall{Name: "noop", Arguments: `{}`}},
				{ID: "c2", Type: "function", Function: schema.FunctionCall{Name: "noop", Arguments: `{}`}},
			}),
			schema.AssistantMessage("ok")})))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	if _, err := host.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("两个 tool_call 的钩子是两次钩子调用，同一个 key 不该撞：%v", err)
	}
	if got := log.snapshot(); len(got) != 2 {
		t.Errorf("子 agent 应被两次钩子调用各调一次，实际看到 %q", got)
	}
}

// hoGateExternal 是非图执行体：Run 里直接问一次人（a2a RemoteAgent 的 human gate 就是这个形状）。
type hoGateExternal struct {
	name string
	runs atomic.Int32
}

func (a *hoGateExternal) Run(ctx context.Context, _ *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.runs.Add(1)
	ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "外部 gate：放行 " + in.Content + "？"})
	if err != nil {
		return nil, err
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, schema.AssistantMessage("【外部产出】"+ans.Content))}, nil
}
func (a *hoGateExternal) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *hoGateExternal) Name() string        { return a.name }
func (a *hoGateExternal) Description() string { return "外部 gate" }

// hoGraphHost 是单节点 GraphAgent，节点叫 nodeID，节点体里调 call。
func hoGraphHost(t *testing.T, name, nodeID string, call func(ctx context.Context, in string) (string, error)) agent_v2.Agent {
	t.Helper()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	b := compose_v2.NewBuilder().WithName("g_" + name)
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode(compose_v2.NodeID(nodeID), func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		out, err := call(ctx, in.Content)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage(out))}, nil
	})
	b.AddEdge(compose_v2.StartNode, compose_v2.NodeID(nodeID))
	b.AddEdge(compose_v2.NodeID(nodeID), compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName(name),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
}

// TestHandOver_CalleeAsksOutsideGraph_CallerQuestionAskedOnce：节点体先经 RunSubAgent 调一个在自己图外
// 问人的执行体，再自己问一次。被调方那一问有自己的计数，节点自己那一问续跑时 ID 不变、只问一次。
//
// 曾经被调方共用调用方的提问计数：它问一次就占掉节点的一个序号，续跑时这次调用被拿回、不再执行，
// 节点自己那一问的序号往前挪、ID 变了，已经答过的「最终确认」被再问一遍（实测）。
func TestHandOver_CalleeAsksOutsideGraph_CallerQuestionAskedOnce(t *testing.T) {
	ext := &hoGateExternal{name: "gate"}
	var effects atomic.Int32
	host := hoGraphHost(t, "ga", "call", func(ctx context.Context, _ string) (string, error) {
		events, err := agent_v2.RunSubAgent(ctx, "gate", ext, schema.UserMessage("转账"))
		if err != nil {
			return "", err
		}
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "节点：最终确认转账？"}); err != nil {
			return "", err
		}
		effects.Add(1)
		return nestedFinal(events), nil
	})
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, schema.UserMessage("原话"))
	rounds, err, asked := hoResume(ctx, host, sess, err)
	if err != nil {
		t.Fatalf("续跑 %d 轮仍失败：%v", rounds, err)
	}
	if asked["节点：最终确认转账？"] != 1 || asked["外部 gate：放行 转账？"] != 1 || effects.Load() != 1 {
		t.Errorf("两问应各问一次、副作用一次，实际问过 %v、副作用 %d 次", asked, effects.Load())
	}
}

// hoWorkerTwoAsks 让 worker 在 OnAgentStart（它自己的图之外）问一次、在自己 tools 节点的 OnToolStart 再问一次。
type hoWorkerTwoAsks struct {
	hook.NoOpHooks
}

func (h *hoWorkerTwoAsks) OnAgentStart(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if info.Name != "worker" {
		return ctx, nil
	}
	_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "worker 开工前确认？"})
	return ctx, err
}

func (h *hoWorkerTwoAsks) OnToolStart(ctx context.Context, info *hook.AgentInfo, ti *schema.ToolInput) (context.Context, error) {
	if info.Name != "worker" {
		return ctx, nil
	}
	_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "worker 调工具 " + ti.Name + " 前确认？"})
	return ctx, err
}

// TestHandOver_HostNodeNamedTools_CalleeHookAsksDistinct：GraphAgent 的节点恰好叫 "tools"（跟 BasicAgent 内部
// 节点同名），节点里经 RunSubAgent 调 worker；worker 在自己图外（OnAgentStart）问一次、在自己 tools 节点的
// OnToolStart 再问一次。两问不同，各问一次、收敛。
//
// 曾经被调方图外那一段的位置是「被调方的 ns + 调用方的节点」，调用方节点叫 "tools" 时跟被调方自己
// tools 节点里的提问撞成同一个 ID，整个 Run 终局失败（实测，不需要空 ID）。
func TestHandOver_HostNodeNamedTools_CalleeHookAsksDistinct(t *testing.T) {
	wtk := tool.NewToolkit()
	wtk.Register(tool.NewFuncTool("lookup", "查", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("查到了"), nil }))
	worker := agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithDescription("w"),
		agent_v2.WithHooks(&hoWorkerTwoAsks{}), agent_v2.WithToolkit(wtk), agent_v2.WithMaxRounds(4),
		agent_v2.WithModel(&scriptedToolModel{replies: []*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "w1", Type: "function", Function: schema.FunctionCall{Name: "lookup", Arguments: `{}`}}}),
			schema.AssistantMessage("worker done"),
		}}))
	host := hoGraphHost(t, "ga", "tools", func(ctx context.Context, in string) (string, error) {
		events, err := agent_v2.RunSubAgent(ctx, "worker", worker, schema.UserMessage("做:"+in))
		if err != nil {
			return "", err
		}
		return nestedFinal(events), nil
	})
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, schema.UserMessage("go"))
	rounds, err, asked := hoResume(ctx, host, sess, err)
	if err != nil {
		t.Fatalf("续跑 %d 轮仍失败（问过 %v）：%v", rounds, asked, err)
	}
	if asked["worker 开工前确认？"] != 1 || asked["worker 调工具 lookup 前确认？"] != 1 {
		t.Errorf("两问应各问一次，实际 %v", asked)
	}
}

// hoCountOwner 数历史里归属是 owner、角色是 role、内容含 sub 的消息条数。
func hoCountOwner(msgs []*schema.Message, owner string, role schema.MessageRole, sub string) int {
	n := 0
	for _, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		if o == owner && m.Role == role && strings.Contains(m.Content, sub) {
			n++
		}
	}
	return n
}

// hoAskOnceExternal 是自己不记历史的执行体：第一次 Run 挂起，之后产出。
type hoAskOnceExternal struct {
	name  string
	calls atomic.Int32
}

func (a *hoAskOnceExternal) Run(_ context.Context, _ *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	if a.calls.Add(1) == 1 {
		return nil, &compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "ext-" + a.name, Question: "外部确认？"}}
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, schema.AssistantMessage("【外部产出】"+in.Content))}, nil
}
func (a *hoAskOnceExternal) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *hoAskOnceExternal) Name() string        { return a.name }
func (a *hoAskOnceExternal) Description() string { return "会先问一次的外部 agent" }

// TestHistory_AgentAsToolInNodeBody_PauseThenDone_InputOnce：GraphAgent 节点体里用 AgentAsTool 调一个自己不
// 记历史的执行体（它的产出不会成为 tool_result，这一层补记历史）；它先挂起、续跑后完成。输入与产出在
// 历史里各一份。
//
// 曾经 AgentAsTool 每次从参数现拼一条输入（新的消息 ID），暂停时记一份、完成时按消息 ID 认不出又记一份
// （实测每多一次中断多一份）。
func TestHistory_AgentAsToolInNodeBody_PauseThenDone_InputOnce(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ext := &hoAskOnceExternal{name: "helper"}
	at := agent_v2.AgentAsTool(ext)
	host := hoGraphHost(t, "ga", "call", func(ctx context.Context, in string) (string, error) {
		res, err := at.Call(ctx, map[string]any{"input": "子任务:" + in})
		if err != nil {
			return "", err
		}
		s, _ := res.Output.(string)
		return "节点结果(" + s + ")", nil
	})
	ctx := agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(
		compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer()), "conv-hp1"), hs, "t")
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, schema.UserMessage("原话"))
	if _, err, _ = hoResume(ctx, host, sess, err); err != nil {
		t.Fatal(err)
	}
	msgs := loadHist(t, hs, ctx, "conv-hp1")
	in := hoCountOwner(msgs, "helper", schema.RoleUser, "子任务:原话")
	out := hoCountOwner(msgs, "helper", schema.RoleAssistant, "【外部产出】子任务:原话")
	if in != 1 || out != 1 {
		t.Errorf("被调方的输入与产出应在历史里各一份：输入 %d 份、产出 %d 份", in, out)
	}
}

// TestHistory_CalleeAsksTwice_FreshInput_InputOnce：节点体每次都现拼一条输入，经 RunSubAgent 交给一个会问两次
// 人的 GraphAgent（它自己不记历史）。挂起两次、续跑两轮之后完成：输入在历史里只一份。
//
// 曾经两处按消息 ID 认「这条输入记过没有」：执行器边界的暂停结算、GraphAgent 自己作为子时在暂停出口的
// 结算。调用方每次重跑现拼的输入 ID 都不同，第二次暂停时又记一份（实测 2 份、问三次 3 份）。
func TestHistory_CalleeAsksTwice_FreshInput_InputOnce(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ib := compose_v2.NewBuilder().WithName("asker2")
	inCh := compose_v2.LastValue[*schema.Message]("a2_in")
	outCh := compose_v2.LastValue[*schema.Message]("a2_out")
	compose_v2.WithChannel(ib, inCh)
	compose_v2.WithChannel(ib, outCh)
	ib.AddNode("ask", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "第一问"}); err != nil {
			return nil, err
		}
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "第二问"}); err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("【被调方产出】"+in.Content))}, nil
	})
	ib.AddEdge(compose_v2.StartNode, "ask")
	ib.AddEdge("ask", compose_v2.EndNode)
	ig, err := ib.Compile()
	if err != nil {
		t.Fatal(err)
	}
	asker := agent_v2.NewGraphAgent(ig, agent_v2.WithGraphAgentName("asker2"),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
	host := hoGraphHost(t, "ga", "call", func(ctx context.Context, in string) (string, error) {
		events, err := agent_v2.RunSubAgent(ctx, "asker2", asker, schema.UserMessage("子任务:"+in))
		if err != nil {
			return "", err
		}
		return "节点结果(" + nestedFinal(events) + ")", nil
	})
	ctx := agent_v2.WithHistoryCtx(compose_v2.WithThreadIDCtx(
		compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer()), "conv-hp2"), hs, "t")
	sess := agent_v2.NewRunSession()
	_, err = host.Run(ctx, sess, schema.UserMessage("原话"))
	rounds, err, _ := hoResume(ctx, host, sess, err)
	if err != nil {
		t.Fatal(err)
	}
	if rounds != 2 {
		t.Fatalf("fixture 没搭对：被调方应挂起两次，实际续跑 %d 轮", rounds)
	}
	msgs := loadHist(t, hs, ctx, "conv-hp2")
	in := hoCountOwner(msgs, "asker2", schema.RoleUser, "子任务:原话")
	out := hoCountOwner(msgs, "asker2", schema.RoleAssistant, "【被调方产出】子任务:原话")
	if in != 1 || out != 1 {
		t.Errorf("被调方的输入与产出应在历史里各一份：输入 %d 份、产出 %d 份", in, out)
	}
}
