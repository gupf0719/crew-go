package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// 嵌套调用的提问身份与「重做」的两条不变量（compose_v2.HandOver）：
//
//  1. 同一次调用（身份不变）里被调方问的问题——在它的主图里，还是在它自己的图之外——不随调用方那一层
//     （调用方节点的这次执行，连同交接之前所在的分域）的重做而换 ID；按 Rerunnable 记在交接那一格的
//     东西也不跟着作废。编排的成员与内部决策 LLM 同理。
//  2. 调用方所在那张图之外的执行被否决重做时，调用方的图从头跑、这次调用的完成记录跟着没了，被调方
//     重新产出——它的提问必须换新，不能拿对旧内容的回答兑付。

// hrDraftConfirmCallee 是一个 GraphAgent：draft 节点每次执行起草一份新方案（plan-v<N>），confirm 节点问
// 「批准 <方案> 吗」，产出「<方案> 已批准(<答案>)」。
func hrDraftConfirmCallee(t *testing.T, name string, drafts *int) agent_v2.Agent {
	t.Helper()
	inCh := compose_v2.LastValue[*schema.Message](name + "_in")
	draftCh := compose_v2.LastValue[string](name + "_draft")
	outCh := compose_v2.LastValue[*schema.Message](name + "_out")
	b := compose_v2.NewBuilder().WithName(name)
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, draftCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("draft", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		*drafts++
		return []compose_v2.ChannelWrite{compose_v2.Write(draftCh, fmt.Sprintf("plan-v%d", *drafts))}, nil
	})
	b.AddNode("confirm", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		d, _ := compose_v2.Get(chs, draftCh)
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准 " + d + " 吗"})
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage(d+" 已批准("+ans.Content+")"))}, nil
	})
	b.AddEdge(compose_v2.StartNode, "draft")
	b.AddEdge("draft", "confirm")
	b.AddEdge("confirm", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName(name),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			o, _ := compose_v2.Get(chs, outCh)
			return o
		}))
}

// hrCheckApprovals：每条「plan-vK 已批准(批准#M)」用的第 M 个答案，答的必须是「批准 plan-vK 吗」。
func hrCheckApprovals(t *testing.T, approved, asked []string) {
	t.Helper()
	if len(approved) == 0 {
		t.Fatal("fixture 没搭对：一条批准都没有产出")
	}
	for _, a := range approved {
		var k, m int
		if _, err := fmt.Sscanf(a, "plan-v%d 已批准(批准#%d)", &k, &m); err != nil {
			t.Fatalf("产出格式不对：%q", a)
		}
		if m < 1 || m > len(asked) || asked[m-1] != fmt.Sprintf("批准 plan-v%d 吗", k) {
			t.Errorf("%q 用的答案答的不是它自己的问题（问过 %v）——新方案没被问过就被批准了", a, asked)
		}
	}
}

// hrDraftCallerGraph 是一张嵌套图：n1 经 RunSubAgent 调起草 + 问批准的被调方，把它的产出写进 resCh。
func hrDraftCallerGraph(t *testing.T, name string, callee agent_v2.Agent, resCh *compose_v2.Channel[string]) *compose_v2.Graph {
	t.Helper()
	b := compose_v2.NewBuilder().WithName(name)
	compose_v2.WithChannel(b, resCh)
	b.AddNode("n1", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ev, err := agent_v2.RunSubAgent(ctx, "drafter", callee, schema.UserMessage("写方案"))
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(resCh, agent_v2.FinalMessage(ev).Content)}, nil
	})
	b.AddEdge(compose_v2.StartNode, "n1")
	b.AddEdge("n1", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// hrVetoV1 是外层节点的出边：产出是对 plan-v1 的批准时按 CondFailRerunFrom 否决，其余收尾。
func hrVetoV1(resCh *compose_v2.Channel[string], approved *[]string) compose_v2.EdgeCondition {
	return func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		r, _ := compose_v2.Get(chs, resCh)
		*approved = append(*approved, r)
		if strings.HasPrefix(r, "plan-v1 ") {
			return "", errors.New("v1 不合格")
		}
		return compose_v2.EndNode, nil
	}
}

// TestOuterRedo_NestedGraphCallee_AsksAboutRegeneratedContent（不变量 2）：外层节点 outer 在节点体里直接跑嵌套图
// G1，G1 的 n1 经 RunSubAgent 调「起草 + 问批准」的被调方。outer 的出边否决 v1 之后，outer 重做、G1 从头跑，
// 被调方重新起草 v2——v2 必须重新问。
//
// 曾经交接过来的主图把外层代际链整条清空：v2 的提问与 v1 的算出同一个 ID，调用方累积传的「批准#1」直接
// 兑付——问过 [批准 plan-v1 吗]，产出 plan-v2 已批准(批准#1)，err=nil（实测）。
func TestOuterRedo_NestedGraphCallee_AsksAboutRegeneratedContent(t *testing.T) {
	drafts := 0
	resCh := compose_v2.LastValue[string]("hr_ng_res")
	g1 := hrDraftCallerGraph(t, "hr_ng_g1", hrDraftConfirmCallee(t, "hr_ng_callee", &drafts), resCh)
	approvedCh := compose_v2.LastValue[string]("hr_ng_approved")
	var approved []string
	b0 := compose_v2.NewBuilder().WithName("hr_ng_g0")
	compose_v2.WithChannel(b0, approvedCh)
	b0.AddNode("outer", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		cm1 := compose_v2.NewChannelManager()
		if err := compose_v2.Run(ctx, g1, cm1, compose_v2.WithRunNS("hr_ng_g1")); err != nil {
			return nil, err
		}
		r, _ := compose_v2.Get(cm1.Channels(), resCh)
		return []compose_v2.ChannelWrite{compose_v2.Write(approvedCh, r)}, nil
	})
	b0.AddEdge(compose_v2.StartNode, "outer")
	b0.AddConditionalEdge("outer", hrVetoV1(approvedCh, &approved), compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	g0, err := b0.Compile()
	if err != nil {
		t.Fatal(err)
	}
	resumes, err, asked := hoDrive(g0)
	if err != nil {
		t.Fatalf("续跑 %d 次仍失败：%v", resumes, err)
	}
	if drafts != 2 {
		t.Fatalf("fixture 没搭对：v1 被否决后被调方应重新起草，实际起草 %d 次（问过 %v）", drafts, asked)
	}
	hrCheckApprovals(t, approved, asked)
}

// TestOuterRedo_SubgraphNodeCallee_AsksAboutRegeneratedContent（不变量 2）：同上，外层换成 AddSubgraphNode——
// 子图节点的出边否决 v1，子图从头跑。
func TestOuterRedo_SubgraphNodeCallee_AsksAboutRegeneratedContent(t *testing.T) {
	drafts := 0
	resCh := compose_v2.LastValue[string]("hr_sg_res")
	g1 := hrDraftCallerGraph(t, "hr_sg_g1", hrDraftConfirmCallee(t, "hr_sg_callee", &drafts), resCh)
	var approved []string
	b0 := compose_v2.NewBuilder().WithName("hr_sg_g0")
	compose_v2.WithChannel(b0, resCh)
	b0.AddSubgraphNode("sub", g1, compose_v2.WithExport(resCh))
	b0.AddEdge(compose_v2.StartNode, "sub")
	b0.AddConditionalEdge("sub", hrVetoV1(resCh, &approved), compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	g0, err := b0.Compile()
	if err != nil {
		t.Fatal(err)
	}
	resumes, err, asked := hoDrive(g0)
	if err != nil {
		t.Fatalf("续跑 %d 次仍失败：%v", resumes, err)
	}
	if drafts != 2 {
		t.Fatalf("fixture 没搭对：v1 被否决后被调方应重新起草，实际起草 %d 次（问过 %v）", drafts, asked)
	}
	hrCheckApprovals(t, approved, asked)
}

// TestCalleeCellRedo_MainGraphAsksAboutRegeneratedContent（不变量 2，交接那一格本身）：自研的被调方先跑自己的
// 主图（起草 + 问批准），再在图外按 Rerunnable 记一份摘要交给判官；判官否决 v1——推进的是交接那一格。续跑时
// 这次调用的身份没变，被调方重跑：主图上次成功时存档已清、从头起草 v2，v2 必须重新问。
//
// 主图的外层代际链里承载它的那一项是交接那一格的代际（从交接那一层数起）：只带调用方所在那张图的链、不带这一项
// 的话，v2 的提问与 v1 的同一个 ID，对 v1 的批准直接兑付。
func TestCalleeCellRedo_MainGraphAsksAboutRegeneratedContent(t *testing.T) {
	inCh := compose_v2.LastValue[*schema.Message]("hr_cell_in")
	draftCh := compose_v2.LastValue[string]("hr_cell_plan")
	outCh := compose_v2.LastValue[string]("hr_cell_out")
	drafts := 0
	mb := compose_v2.NewBuilder().WithName("hr_cell_main")
	compose_v2.WithChannel(mb, inCh)
	compose_v2.WithChannel(mb, draftCh)
	compose_v2.WithChannel(mb, outCh)
	mb.AddNode("draft", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		drafts++
		return []compose_v2.ChannelWrite{compose_v2.Write(draftCh, fmt.Sprintf("plan-v%d", drafts))}, nil
	})
	mb.AddNode("confirm", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		d, _ := compose_v2.Get(chs, draftCh)
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准 " + d + " 吗"})
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, d+" 已批准("+ans.Content+")")}, nil
	})
	mb.AddEdge(compose_v2.StartNode, "draft")
	mb.AddEdge("draft", "confirm")
	mb.AddEdge("confirm", compose_v2.EndNode)
	mainGraph, err := mb.Compile()
	if err != nil {
		t.Fatal(err)
	}
	sumCh := compose_v2.LastValue[string]("hr_cell_sum")
	judge := hrRejectV1Graph(t, "hr_cell_judge", sumCh)
	summaries := 0
	callee := &hoFuncAgent{name: "callee", run: func(ctx context.Context, in *schema.Message) ([]*agent_v2.AgentEvent, error) {
		cm := compose_v2.NewChannelManager()
		if err := compose_v2.Run(ctx, mainGraph, cm, compose_v2.WithInitialWrites(compose_v2.Write(inCh, in))); err != nil {
			return nil, err
		}
		out, _ := compose_v2.Get(cm.Channels(), outCh)
		if err := hrRedraftAndJudge(ctx, judge, sumCh, "hr_cell_judge", &summaries); err != nil {
			return nil, err
		}
		return []*agent_v2.AgentEvent{agent_v2.NewExitEvent("callee", schema.AssistantMessage(out))}, nil
	}}
	var approved []string
	g := hoHostGraph(t, "hr_cell_host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ev, err := agent_v2.RunSubAgent(ctx, "callee", callee, schema.UserMessage("写方案"))
		if err != nil {
			return nil, err
		}
		approved = append(approved, agent_v2.FinalMessage(ev).Content)
		return nil, nil
	})
	resumes, err, asked := hoDrive(g)
	if err != nil {
		t.Fatalf("续跑 %d 次仍失败：%v", resumes, err)
	}
	if drafts != 2 || summaries != 2 {
		t.Fatalf("fixture 没搭对：摘要 v1 被否决后被调方应重跑、主图重新起草，实际起草 %d 次、摘要 %d 次（问过 %v）",
			drafts, summaries, asked)
	}
	hrCheckApprovals(t, approved, asked)
}

// TestSameCall_CalleeToolCallQuestion_KeptAcrossCallerRedo（不变量 1）：被调方在自己的图之外又开了分域（自研 agent
// 自己调工具：WithCurrentToolCallContext），在里面问人；调用方节点体被判官否决重做。交接那一格下面叠的分域同样
// 从交接那一层数代际，这一问不该换 ID 再问。
func TestSameCall_CalleeToolCallQuestion_KeptAcrossCallerRedo(t *testing.T) {
	runs := 0
	gate := &hoFuncAgent{name: "gate", run: func(ctx context.Context, _ *schema.Message) ([]*agent_v2.AgentEvent, error) {
		runs++
		tc := compose_v2.WithCurrentToolCallContext(ctx, "t1", 0)
		if _, err := compose_v2.Ask(tc, &compose_v2.AskQuery{Question: "外部工具：放行吗？"}); err != nil {
			return nil, err
		}
		return []*agent_v2.AgentEvent{agent_v2.NewExitEvent("gate", schema.AssistantMessage("放行"))}, nil
	}}
	draftCh := compose_v2.LastValue[string]("hr_ctc_draft")
	judge := hrRejectV1Graph(t, "hr_ctc_judge", draftCh)
	drafts := 0
	g := hoHostGraph(t, "hr_ctc_host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, errA := agent_v2.RunSubAgent(ctx, "gate", gate, schema.UserMessage("预算 100"))
		errG := hrRedraftAndJudge(ctx, judge, draftCh, "hr_ctc_judge", &drafts)
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
	if len(asked) != 1 || asked[0] != "外部工具：放行吗？" {
		t.Errorf("被调方在图外的工具里那一问应只问一次，实际问过 %v（gate 执行 %d 次）", asked, runs)
	}
}

// hrRejectV1Graph 是入口按 CondFailRerunFrom 否决 "v1" 的嵌套图（读 draftCh）。
func hrRejectV1Graph(t *testing.T, name string, draftCh *compose_v2.Channel[string]) *compose_v2.Graph {
	t.Helper()
	b := compose_v2.NewBuilder().WithName(name)
	compose_v2.WithChannel(b, draftCh)
	b.AddNode("x", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	b.AddConditionalEdge(compose_v2.StartNode, func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		if d, _ := compose_v2.Get(chs, draftCh); d == "v1" {
			return "", errors.New("v1 不合格")
		}
		return "x", nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	b.AddEdge("x", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// hrRedraftAndJudge 在节点体里按 Rerunnable 起草（记过就拿回）、交给入口否决 v1 的判官图：第一次执行
// 让节点体的执行级代际推进一次。
func hrRedraftAndJudge(ctx context.Context, judge *compose_v2.Graph, draftCh *compose_v2.Channel[string], ns string, drafts *int) error {
	d, ok := compose_v2.GetStepResult[string](ctx, "draft")
	if !ok {
		*drafts++
		d = fmt.Sprintf("v%d", *drafts)
		if err := compose_v2.SaveStepResult(ctx, "draft", d, compose_v2.Rerunnable()); err != nil {
			return err
		}
	}
	return compose_v2.Run(ctx, judge, compose_v2.NewChannelManager(), compose_v2.WithRunNS(ns),
		compose_v2.WithInitialWrites(compose_v2.Write(draftCh, d)))
}

// TestSameCall_OutOfGraphQuestion_KeptAcrossCallerRedo（不变量 1）：节点体里一次普通嵌套调用挂着，被调方在
// 自己的图之外问人（a2a RemoteAgent 的 human gate 就是这个形状）；同一次执行里的判官否决 v1、节点体重做。
// 这次调用身份没变，已经答过的问题不该换 ID 再问。
//
// 曾经交接那一格的重做代际把调用方节点的执行级也算进来：节点体一换代，「外部 gate：放行 预算 100？」换了
// ID 被再问一遍、gate 多执行一次（实测）。主图里的提问早已不带调用方的代际
// （TestRunSubAgent_PendingPlainCall_NotReaskedWhenSiblingRejected），图外的没跟上。
func TestSameCall_OutOfGraphQuestion_KeptAcrossCallerRedo(t *testing.T) {
	gate := &hoGateExternal{name: "gate"}
	draftCh := compose_v2.LastValue[string]("hr_og_draft")
	judge := hrRejectV1Graph(t, "hr_og_judge", draftCh)
	drafts := 0
	g := hoHostGraph(t, "hr_og_host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, errA := agent_v2.RunSubAgent(ctx, "gate", gate, schema.UserMessage("预算 100"))
		errG := hrRedraftAndJudge(ctx, judge, draftCh, "hr_og_judge", &drafts)
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
	if len(asked) != 1 || asked[0] != "外部 gate：放行 预算 100？" {
		t.Errorf("被调方在图外的那一问应只问一次，实际问过 %v（gate 执行 %d 次）", asked, gate.runs.Load())
	}
}

// TestSameCall_OutOfGraphQuestion_KeptAcrossToolCallScopeRedo（不变量 1）：同上，但调用发生在一个 tool_call
// 里，被否决、重做的是这个 tool_call 的分域（交接之前所在的分域），不是节点的执行级。这一层同样属于调用方。
func TestSameCall_OutOfGraphQuestion_KeptAcrossToolCallScopeRedo(t *testing.T) {
	gate := &hoGateExternal{name: "gate"}
	draftCh := compose_v2.LastValue[string]("hr_tc_draft")
	judge := hrRejectV1Graph(t, "hr_tc_judge", draftCh)
	drafts := 0
	g := hoHostGraph(t, "hr_tc_host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		tc := compose_v2.WithCurrentToolCallContext(ctx, "call_1", 0)
		_, errA := agent_v2.RunSubAgent(tc, "gate", gate, schema.UserMessage("预算 100"))
		errG := hrRedraftAndJudge(tc, judge, draftCh, "hr_tc_judge", &drafts)
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
	if len(asked) != 1 || asked[0] != "外部 gate：放行 预算 100？" {
		t.Errorf("被调方在图外的那一问应只问一次，实际问过 %v（gate 执行 %d 次）", asked, gate.runs.Load())
	}
}

// TestSameCall_HandedOverRerunnableRecord_KeptAcrossCallerRedo（不变量 1）：被调方在自己的图之外按 Rerunnable
// 记下一份方案、再问「批准 <方案> 吗」，挂着；同一次执行里的判官否决 v1、调用方节点体重做。这次调用身份
// 没变：方案原样拿回（不重新产出），问的还是那一问，答案配给的就是它问的那份方案。
//
// 这条同时钉着存档投影：续跑从存档恢复，那一格的记录要在存档里。交接那一格的代际从交接那一层数起、
// 可以比执行级小——拿执行级代际判过期的话，方案掉出存档、续跑时重新产出一份，而提问的 ID 没变，
// 对 plan-v1 的批准兑付给了 plan-v2。
func TestSameCall_HandedOverRerunnableRecord_KeptAcrossCallerRedo(t *testing.T) {
	plans := 0
	planner := &hoFuncAgent{name: "planner", run: func(ctx context.Context, _ *schema.Message) ([]*agent_v2.AgentEvent, error) {
		p, ok := compose_v2.GetStepResult[string](ctx, "plan")
		if !ok {
			plans++
			p = fmt.Sprintf("plan-v%d", plans)
			if err := compose_v2.SaveStepResult(ctx, "plan", p, compose_v2.Rerunnable()); err != nil {
				return nil, err
			}
		}
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准 " + p + " 吗"})
		if err != nil {
			return nil, err
		}
		return []*agent_v2.AgentEvent{agent_v2.NewExitEvent("planner", schema.AssistantMessage(p+" 已批准("+ans.Content+")"))}, nil
	}}
	draftCh := compose_v2.LastValue[string]("hr_rr_draft")
	judge := hrRejectV1Graph(t, "hr_rr_judge", draftCh)
	drafts := 0
	var approved []string
	g := hoHostGraph(t, "hr_rr_host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ev, errA := agent_v2.RunSubAgent(ctx, "planner", planner, schema.UserMessage("出方案"))
		if errA == nil {
			approved = append(approved, agent_v2.FinalMessage(ev).Content)
		}
		errG := hrRedraftAndJudge(ctx, judge, draftCh, "hr_rr_judge", &drafts)
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
	if plans != 1 || len(asked) != 1 {
		t.Errorf("同一次调用里的方案应只产出一次、只问一次，实际产出 %d 次、问过 %v", plans, asked)
	}
	hrCheckApprovals(t, approved, asked)
}

// hrResumeAll 首跑之后不管什么错都续跑（问到的都答「答:<问题>」、累积传），最多 6 轮；按问题计数。
func hrResumeAll(ctx context.Context, ag agent_v2.Agent, sess *agent_v2.RunSession, err error) (error, map[string]int) {
	answers := map[string]*schema.Message{}
	asked := map[string]int{}
	for i := 0; i < 6 && err != nil; i++ {
		if ie, ok := compose_v2.IsInterrupt(err); ok {
			for _, q := range ie.AllQueries() {
				asked[q.Question]++
				answers[q.InterruptID] = schema.UserMessage("答:" + q.Question)
			}
		}
		_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	}
	return err, asked
}

// TestSameCall_MemberOutOfGraphQuestion_KeptAcrossOrchestrationRedo（不变量 1，编排成员）：SequentialAgent 的成员
// 在自己的图之外问一次人，它的主图入口第一次否决（输入不合格）、编排的那一步因此重做。成员还是同一次调用，
// 已经答过的那一问不该再问。
//
// 也是编排成员交接相位（runMember 里的 compose_v2.HandOver）的哨兵：没有它时成员在图外的提问用的是编排节点
// 的代际，那一步一重做就换 ID 再问。
func TestSameCall_MemberOutOfGraphQuestion_KeptAcrossOrchestrationRedo(t *testing.T) {
	evals := 0
	jb := compose_v2.NewBuilder().WithName("hr_mem_judge")
	jb.AddNode("x", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	jb.AddConditionalEdge(compose_v2.StartNode, func(context.Context, compose_v2.Channels) (compose_v2.NodeID, error) {
		evals++
		if evals == 1 {
			return "", errors.New("第一次不合格")
		}
		return "x", nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	jb.AddEdge("x", compose_v2.EndNode)
	judge, err := jb.Compile()
	if err != nil {
		t.Fatal(err)
	}
	member := &hoFuncAgent{name: "member", run: func(ctx context.Context, _ *schema.Message) ([]*agent_v2.AgentEvent, error) {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "成员开工前确认？"}); err != nil {
			return nil, err
		}
		if err := compose_v2.Run(ctx, judge, compose_v2.NewChannelManager()); err != nil {
			return nil, err
		}
		return []*agent_v2.AgentEvent{agent_v2.NewExitEvent("member", schema.AssistantMessage("member done"))}, nil
	}}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("hr_seq")).AddStep("s1", member)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err = seq.Run(ctx, sess, schema.UserMessage("go"))
	err, asked := hrResumeAll(ctx, seq, sess, err)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %v）", err, asked)
	}
	if evals != 2 {
		t.Fatalf("fixture 没搭对：判官应先否决一次再放行，实际判了 %d 次", evals)
	}
	if asked["成员开工前确认？"] != 1 {
		t.Errorf("成员在图外的那一问应只问一次，实际 %v", asked)
	}
}

// TestDecisionAgent_OutOfGraphNodeState_KeptApartFromClassifyProgress：RoutingAgent 的 classify 节点把路由升级链
// 已经跑完的档记在节点私有态里（续跑时重放、不再调用）。自研的分类器在自己的图之外也记了一份进度、然后问人
// 挂起——续跑时决策方不该被再调一次。
//
// 是内部决策 LLM 交接相位（runDecisionAgent 里的 compose_v2.HandOver）的哨兵：没有它时分类器的 SaveNodeState
// 写进 classify 节点体那一格、把升级链的进度顶掉，续跑时决策方被重新调用。
func TestDecisionAgent_OutOfGraphNodeState_KeptApartFromClassifyProgress(t *testing.T) {
	deciderCalls := 0
	decider := agent_v2.RouteDecider(func(context.Context, agent_v2.RouteRequest) (agent_v2.RouteDecision, error) {
		deciderCalls++
		return agent_v2.RouteDecision{Target: "a", Confidence: 0.1}, nil
	})
	classifier := &hoFuncAgent{name: "cls", run: func(ctx context.Context, _ *schema.Message) ([]*agent_v2.AgentEvent, error) {
		if err := compose_v2.SaveNodeState(ctx, "分类器自己的进度"); err != nil {
			return nil, err
		}
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "分类器：走哪个分支？"}); err != nil {
			return nil, err
		}
		return []*agent_v2.AgentEvent{agent_v2.NewExitEvent("cls", schema.AssistantMessage("a"))}, nil
	}}
	r := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("hr_route"),
		agent_v2.WithRouteDecider(decider, 0.9), agent_v2.WithRoutingClassifier(classifier)).
		AddBranch("a", dtBasic("a", "branch a done")).
		AddBranch("b", dtBasic("b", "branch b done"))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := r.Run(ctx, sess, schema.UserMessage("go"))
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("fixture 没搭对：分类器应挂起，实际 %v", err)
	}
	err, asked := hrResumeAll(ctx, r, sess, err)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %v）", err, asked)
	}
	if asked["分类器：走哪个分支？"] != 1 {
		t.Fatalf("fixture 没搭对：分类器应只问一次，实际 %v", asked)
	}
	if deciderCalls != 1 {
		t.Errorf("决策方应只调一次（续跑时按 classify 节点记下的进度重放），实际调了 %d 次", deciderCalls)
	}
}

// hrGraphStartCaller 在 ns 为 ns 的那张图的 OnGraphStart 里调 call。
type hrGraphStartCaller struct {
	compose_v2.NoOpOrchestrationHooks
	ns   string
	call func(ctx context.Context) error
}

func (h *hrGraphStartCaller) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	if compose_v2.GetGraphNS(ctx) != h.ns {
		return ctx, nil
	}
	return ctx, h.call(ctx)
}

// hrBudgetApprover 是一个 GraphAgent：唯一的节点问「批准预算吗」。
func hrBudgetApprover(t *testing.T, name string) agent_v2.Agent {
	t.Helper()
	return hoGraphHost(t, name, "ask", func(ctx context.Context, _ string) (string, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准预算吗"})
		if err != nil {
			return "", err
		}
		return "预算:" + ans.Content, nil
	})
}

// TestSameCall_GraphHookHandover_QuestionKeptAcrossHostRedo（不变量 1，调用发生在嵌套图的图级钩子里）：外层节点体里
// 直接跑一张嵌套图，它的 OnGraphStart 经 RunSubAgent 调一个会问人的被调方（挂着）；同一次外层执行里节点体按
// Rerunnable 起草的 v1 被判官否决、外层节点重做。图级钩子记在外层节点执行名下——调用方那一层就是外层节点，这次
// 调用身份没变，被调方主图里问过的问题不该再问。
//
// 曾经被调方主图的外层代际链按 ctx 取：图级钩子拿到的 ctx 上已经是嵌套图自己的链，含外层节点（调用方那一层）的
// 代际，外层节点一重做就换 ID 再问一遍（审计实测）。现在按调用方那一格所在的 cm 取（ChannelManager.outerGens）。
func TestSameCall_GraphHookHandover_QuestionKeptAcrossHostRedo(t *testing.T) {
	callee := hrBudgetApprover(t, "hr_gh_approver")
	ib := compose_v2.NewBuilder().WithName("hr_gh_inner")
	ib.AddNode("n", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	ib.AddEdge(compose_v2.StartNode, "n")
	ib.AddEdge("n", compose_v2.EndNode)
	inner, err := ib.Compile()
	if err != nil {
		t.Fatal(err)
	}
	caller := &hrGraphStartCaller{ns: "hr_gh_inner", call: func(ctx context.Context) error {
		_, err := agent_v2.RunSubAgent(ctx, "approver", callee, schema.UserMessage("预算 100"))
		return err
	}}
	draftCh := compose_v2.LastValue[string]("hr_gh_draft")
	judge := hrRejectV1Graph(t, "hr_gh_judge", draftCh)
	drafts := 0
	g := hoHostGraph(t, "hr_gh_host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		errI := compose_v2.Run(ctx, inner, compose_v2.NewChannelManager(), compose_v2.WithRunNS("hr_gh_inner"), compose_v2.WithHooks(caller))
		errG := hrRedraftAndJudge(ctx, judge, draftCh, "hr_gh_judge", &drafts)
		if errI != nil || errG != nil {
			return nil, errors.Join(errI, errG)
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
	if len(asked) != 1 || asked[0] != "批准预算吗" {
		t.Errorf("「批准预算吗」应只问一次（调用身份没变），实际问过 %v", asked)
	}
}
