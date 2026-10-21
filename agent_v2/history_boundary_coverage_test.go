package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/skill"
	"crew-go/tool"
)

// 边界补记的**覆盖面**：每条「编排调 sub-agent」的路径都要有它。
//
// 原来只有 5 个编排接了（Sequential / Parallel / Routing / Loop / Handoff）。而
// PlanAgent 的三条 worker 路径、ReflectAgent 的 actor、`RunSubAgent`（业务在 GraphAgent
// 的 NodeFunc 里调子 agent 的推荐入口）、skill fork——这四类都 push 了位置却从不补记，
// 于是这些路径下的外部执行体**什么也不留**，正是 settleHistorySlot 自陈要防的形状。
//
// PlanAgent 的 executor 尤其不是边角：`SetExecutor` 收的就是任意 `Agent`，业务拿它接
// 自研 / 远程执行器是一等公民用法。
//
// 每个用例都必须用**外部执行体**——换成 BasicAgent 就测不出来（它自己会记，补不补记
// 结果一样）。

func loadHist(t *testing.T, hs agent_v2.HistoryStore, ctx context.Context, conv string) []*schema.Message {
	t.Helper()
	msgs, err := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: conv})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return msgs
}

func assertHas(t *testing.T, msgs []*schema.Message, subs ...string) {
	t.Helper()
	for _, want := range subs {
		found := false
		for _, m := range msgs {
			if strings.Contains(m.Content, want) {
				found = true
				break
			}
		}
		if !found {
			var b strings.Builder
			for i, m := range msgs {
				o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
				b.WriteString("\n  [" + string(rune('0'+i)) + "] owner=" + o + " " + strings.TrimSpace(m.Content))
			}
			t.Errorf("%q 不在历史里——这条路径没有边界补记，外部执行体什么也没留下。历史共 %d 条：%s",
				want, len(msgs), b.String())
		}
	}
}

// ReflectAgent 的 actor 可以是任意 Agent。
func TestHistBoundary_ReflectActor(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	// evaluator 用 BasicAgent 起写入器（编排从子那儿借配置）
	eval := agent_v2.NewBasicAgent(
		agent_v2.WithName("evaluator"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("PASS")})),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	rf := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("rf"),
		agent_v2.WithEvaluator(eval),
		// 固定 1 轮：多轮的话 actor 每轮产出一条**不同**的消息，条数变多不是重复
		// （第一版就是这么误报的，评估器 stub 用完后循环了 9 轮）。
		agent_v2.WithReflectMaxRounds(1),
	).WithActor(&externalAgent{name: "extActor"})

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-reflect")
	if _, err := rf.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs := loadHist(t, hs, ctx, "conv-reflect")
	assertHas(t, msgs, "原话")
	assertCount(t, msgs, "外部产出", 1) // 补记也不能补出第二份
}

// PlanAgent 的 executor 是「任意 Agent」的一等公民入口。
func TestHistBoundary_PlanExecutor(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	helper := agent_v2.NewBasicAgent(
		agent_v2.WithName("helper"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("辅助")})),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))

	planned := false
	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, _ []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if planned {
			return nil, "完成", nil
		}
		planned = true
		return []agent_v2.PlanStep{{Description: "干一件事"}}, "", nil
	}
	plan := agent_v2.NewPlanAgent(agent_v2.Planner(planner), agent_v2.PlanName("plan"))
	plan.AddSubAgent("helper", helper)
	plan.SetExecutor(&externalAgent{name: "extExec"})

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-plan")
	if _, err := plan.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertCount(t, loadHist(t, hs, ctx, "conv-plan"), "外部产出", 1)
}

// RunSubAgent 是业务在 GraphAgent 的 NodeFunc 里调子 agent 的推荐入口。
func TestHistBoundary_RunSubAgent(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	inCh := compose_v2.LastValue[*schema.Message]("__rsa_in__")
	outCh := compose_v2.LastValue[*schema.Message]("__rsa_out__")

	bld := compose_v2.NewBuilder().WithName("g_rsa")
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, outCh)
	bld.AddNode("call", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		evs, err := agent_v2.RunSubAgent(ctx, "worker", &externalAgent{name: "extWorker"}, in)
		if err != nil {
			return nil, err
		}
		// **NodeFunc 加工过再往下写**——这是 GraphAgent 的常规用法，也是本用例能隔离出
		// RunSubAgent 那层补记的关键：不加工的话外层 Sequential 的步骤边界会把同一条
		// 内容记进去，撤掉 RunSubAgent 的补记测试照样绿（第一版就是这么假绿的）。
		// 加工后的内容**不含**原文——含的话子串断言会被它命中，等于没隔离出来
		// （第二版就是这么假绿的：`【节点加工】【外部产出】…` 把原文包在里面了）。
		_ = agent_v2.FinalMessage(evs)
		out := schema.AssistantMessage("【节点加工结果】")
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, out)}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "call")
	bld.AddEdge("call", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ga := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("ga"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))

	// GraphAgent 自己不接历史层（子 agent 由业务 NodeFunc 调起，框架在那层没有落点），
	// 所以套一层 Sequential 把写入器建起来——这正是业务的真实形状。
	local := agent_v2.NewBasicAgent(
		agent_v2.WithName("localB"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("本地结论")})),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("flow")).
		AddStep("g", ga).AddStep("local", local)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-rsa")
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs := loadHist(t, hs, ctx, "conv-rsa")
	// 外部执行体的**原始**产出只可能来自 RunSubAgent 那层的补记——外层 Sequential
	// 的边界看到的是加工后那条。
	assertHas(t, msgs, "原话")
	assertCount(t, msgs, "外部产出", 1)
	// 对照组：加工后那条由外层边界记（证明外层边界确实在工作，上面那条的缺席不是
	// 「整条链都没记」）。
	assertHas(t, msgs, "【节点加工结果】")
}

// 作为**工具**调起的 sub-agent 不该再被边界补记一遍。
//
// `AgentAsTool` 和 skill fork 都走 `RunSubAgent`，而它们不是编排的一步、是一次 tool_call
// ——父的 tools 节点本来就把结果写成 tool_result 进历史。两边都记就是同一条内容两个归属，
// 正是这套改动要消灭的形状。这条是我自己引入又量出来的：加边界补记时没分清「编排步骤」
// 和「工具调用」，实测「外部产出」出现了 2 次。
func TestHistBoundary_AgentAsTool_NoDoubleRecord(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(&externalAgent{name: "helper"}))
	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID: "tc1", Type: "function",
				Function: schema.FunctionCall{Name: "helper", Arguments: `{"input":"子任务"}`},
			}}),
			schema.AssistantMessage("综合结论"),
		})),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(4))

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-aat")
	if _, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs := loadHist(t, hs, ctx, "conv-aat")
	assertCount(t, msgs, "外部产出", 1)
	// 对照组：内容确实进了历史（不是「一次都没有」蒙混过关）。
	assertHas(t, msgs, "综合结论")
}

// skill fork 同理——它也走 RunSubAgent，且恒在 tool_call 里。
func TestHistBoundary_SkillFork_NoDoubleRecord(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	box := &forkBox{
		skills: []skill.SkillDescriptor{
			{Name: "research", Description: "调研", Context: "fork", Agent: "research_agent"},
		},
		activations: map[string]string{"research": "你是调研专家。"},
	}
	ext := &externalAgent{name: "skill_research"}
	resolver := func(name string) (agent_v2.Agent, error) {
		if name == "research_agent" {
			return ext, nil
		}
		return nil, nil
	}
	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID: "tc1", Type: "function",
				Function: schema.FunctionCall{
					Name:      "activate_skill",
					Arguments: `{"skill":"research","query":"调研子任务"}`,
				},
			}}),
			schema.AssistantMessage("综合结论"),
		})),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithSkillSubAgentResolver(resolver),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(4))

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-fork")
	if _, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs := loadHist(t, hs, ctx, "conv-fork")
	assertCount(t, msgs, "外部产出", 1)
	assertHas(t, msgs, "综合结论")
}

// assertCount 断言某段内容在历史里恰好出现 n 次。
func assertCount(t *testing.T, msgs []*schema.Message, sub string, want int) {
	t.Helper()
	got := 0
	for _, m := range msgs {
		if strings.Contains(m.Content, sub) {
			got++
		}
	}
	if got == want {
		return
	}
	var b strings.Builder
	for i, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		fmt.Fprintf(&b, "\n  [%d] %-9s owner=%-20s %s", i, m.Role, o, strings.TrimSpace(m.Content))
	}
	t.Errorf("%q 在历史里出现 %d 次，应 %d 次。历史共 %d 条：%s", sub, got, want, len(msgs), b.String())
}

// 嵌套在 tool_call 里的 RunSubAgent **不能**跟着被豁免。
//
// `AgentAsTool` 那层不补记，是因为它的产出会由父的 tools 节点写成 tool_result。但这个
// 理由**只对它自己那一层成立**：`BasicAgent → AgentAsTool(GraphAgent) → NodeFunc →
// RunSubAgent(外部执行体)` 里，外层 tool_result 只带图的**聚合产出**，内层那个执行体的
// 输入和产出什么都不剩。
//
// 判据一度写成「ctx 里有没有 tool-call 信号」——那个信号**没有清除点**，一路继承到任意
// 深度，于是内层这次也被豁免，实测内层产出出现 0 次。「祖先里有没有 tool_call」和「我这
// 一层是不是那次 tool_call」是两个问题，只有调用点知道后者。
func TestHistBoundary_NestedInToolCall_StillRecords(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	inCh := compose_v2.LastValue[*schema.Message]("__ntc_in__")
	outCh := compose_v2.LastValue[*schema.Message]("__ntc_out__")

	bld := compose_v2.NewBuilder().WithName("g_ntc")
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, outCh)
	bld.AddNode("call", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		if _, err := agent_v2.RunSubAgent(ctx, "worker", &externalAgent{name: "extWorker"}, in); err != nil {
			return nil, err
		}
		// 聚合产出**不含**内层原文——含的话子串断言会被它命中。
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("【图聚合结果】"))}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "call")
	bld.AddEdge("call", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ga := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("ga"),
		agent_v2.WithGraphAgentDescription("图工具"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))

	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(ga))
	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID: "tc1", Type: "function",
				Function: schema.FunctionCall{Name: "ga", Arguments: `{"input":"子任务"}`},
			}}),
			schema.AssistantMessage("综合结论"),
		})),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(4))

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-nested-tc")
	if _, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs := loadHist(t, hs, ctx, "conv-nested-tc")
	assertCount(t, msgs, "外部产出", 1)  // 内层执行体的产出，只可能来自 NodeFunc 那层的补记
	assertCount(t, msgs, "图聚合结果", 1) // 外层 tool_result，正好一份
}

// TestHistBoundary_AgentAsToolBehindRenamingShell_NoDoubleRecord：AgentAsTool 的工具名恒等于 agent 名，
// 想用别的工具名暴露同一个 agent 只能包一层异名转发壳。转发出去的产出同样会成为 tool_result 进历史
// ——AgentAsTool 按「是不是 tools 节点直接调起的」决定补不补记的话，这一层被当成组合工具补记一份，
// 同一条内容两个归属（实测）。
func TestHistBoundary_AgentAsToolBehindRenamingShell_NoDoubleRecord(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	at := agent_v2.AgentAsTool(&externalAgent{name: "helper"})
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("ask_legal_team", "问法务", at.InputSchema(),
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			return at.Call(ctx, args) // 原样转发
		}))
	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID: "tc1", Type: "function",
				Function: schema.FunctionCall{Name: "ask_legal_team", Arguments: `{"input":"子任务"}`},
			}}),
			schema.AssistantMessage("综合结论"),
		})),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(4))
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-rename")
	if _, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs := loadHist(t, hs, ctx, "conv-rename")
	assertCount(t, msgs, "外部产出", 1)
	assertHas(t, msgs, "综合结论")
}
