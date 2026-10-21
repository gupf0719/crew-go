package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// 重做代际按分域记、只在真的重做时推进（见 compose_v2 的 rerunGeneration）：
//
//   - 嵌套图的入口条件边否决时，**不当场推进**——等承载它的成员以这个错失败时，才推进产出那份输入
//     的分域。错被业务吞掉、被重试消化、被转成工具的失败结果，就不推进；
//   - 只推进产出那份输入的分域（某个 tool_call、某个钩子相位），同一次执行里别的分域不受影响；
//   - 外层执行被否决重做时，嵌套图里的提问带上外层代际链，换新 ID。

func rsCompile(t *testing.T, b *compose_v2.GraphBuilder) *compose_v2.Graph {
	t.Helper()
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// rsRejectOnceGraph 是一张嵌套图：入口条件边第一次求值按 CondFailRerunFrom 否决，之后放行。
func rsRejectOnceGraph(t *testing.T, name string, evals *int) *compose_v2.Graph {
	t.Helper()
	ib := compose_v2.NewBuilder().WithName(name)
	ib.AddNode("x", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	ib.AddConditionalEdge(compose_v2.StartNode, func(context.Context, compose_v2.Channels) (compose_v2.NodeID, error) {
		*evals++
		if *evals == 1 {
			return "", errors.New("router 第一次判不出分支")
		}
		return "x", nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	ib.AddEdge("x", compose_v2.EndNode)
	return rsCompile(t, ib)
}

// rsLoop 跑到收敛：每轮记下抛出的问题、累积答案（文档化的写法），再续跑。
func rsLoop(t *testing.T, g *compose_v2.Graph, opts []compose_v2.RunOption, firstExtra ...compose_v2.RunOption) (asked []string, err error) {
	t.Helper()
	answers := map[string]*schema.Message{}
	err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), append(append([]compose_v2.RunOption{}, opts...), firstExtra...)...)
	for i := 0; i < 8 && err != nil; i++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			return asked, err
		}
		for _, q := range ie.AllQueries() {
			asked = append(asked, q.Question)
			answers[q.InterruptID] = schema.UserMessage(fmt.Sprintf("批准#%d", len(asked)))
		}
		err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
			append(append([]compose_v2.RunOption{}, opts...), compose_v2.WithResumeAnswers(answers))...)
	}
	return asked, err
}

func rsOpts() []compose_v2.RunOption {
	return []compose_v2.RunOption{compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()), compose_v2.WithSessionID("s")}
}

// TestRerunScope_SiblingToolCallQuestionKeepsID：同一个节点里 tool_call 0 问人、tool_call 1 跑的嵌套图
// 入口条件边否决一次（错被工具吞成失败结果）。用户答过 tool_call 0 的问题，续跑时不该换 ID 再问。
// 曾经否决当场推进整次执行的代际：兄弟 tool_call 的提问换了 ID（实测，框架工具审批同样中招）。
func TestRerunScope_SiblingToolCallQuestionKeepsID(t *testing.T) {
	evals := 0
	ig := rsRejectOnceGraph(t, "rs_inner_c2", &evals)
	aExec := 0
	ob := compose_v2.NewBuilder().WithName("rs_outer_c2")
	ob.AddNode("tools", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		var errs []error
		c0 := compose_v2.WithCurrentToolCallContext(ctx, "call_a", 0)
		if _, err := compose_v2.Ask(c0, &compose_v2.AskQuery{Question: "批准工具A吗"}); err != nil {
			errs = append(errs, err)
		} else {
			aExec++
		}
		c1 := compose_v2.WithCurrentToolCallContext(ctx, "call_b", 1)
		_ = compose_v2.Run(c1, ig, compose_v2.NewChannelManager(), compose_v2.WithRunNS("rs_inner_c2")) // 吞成工具失败结果
		return nil, compose_v2.PickFanoutError(errs)
	})
	ob.AddEdge(compose_v2.StartNode, "tools")
	ob.AddEdge("tools", compose_v2.EndNode)
	asked, err := rsLoop(t, rsCompile(t, ob), rsOpts())
	if err != nil {
		t.Fatalf("没收敛：%v", err)
	}
	if len(asked) != 1 || aExec != 1 {
		t.Errorf("工具 A 的审批应只问一次、执行一次：asked=%v aExec=%d", asked, aExec)
	}
}

// TestRerunScope_RejectDigestedByRetry_EarlierQuestionKeepsID：节点先问 q1（已答），再用 RunWithRetry 跑一张
// 入口否决一次的嵌套图（第二次尝试成功，否决被重试消化），再问 q2。节点的产出从没被否决过，续跑时 q1
// 不该被再问。
func TestRerunScope_RejectDigestedByRetry_EarlierQuestionKeepsID(t *testing.T) {
	evals := 0
	ig := rsRejectOnceGraph(t, "rs_inner_c3", &evals)
	ob := compose_v2.NewBuilder().WithName("rs_outer_c3")
	ob.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "q1"}); err != nil {
			return nil, err
		}
		if _, err := compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (struct{}, error) {
			return struct{}{}, compose_v2.Run(ctx, ig, compose_v2.NewChannelManager(), compose_v2.WithRunNS("rs_inner_c3"))
		}); err != nil {
			return nil, err
		}
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "q2"}); err != nil {
			return nil, err
		}
		return nil, nil
	})
	ob.AddEdge(compose_v2.StartNode, "host")
	ob.AddEdge("host", compose_v2.EndNode)
	asked, err := rsLoop(t, rsCompile(t, ob), rsOpts())
	if err != nil {
		t.Fatalf("没收敛：%v", err)
	}
	if strings.Join(asked, ",") != "q1,q2" {
		t.Errorf("q1、q2 应各问一次（否决被重试消化，节点没被要求重做），实际 %v", asked)
	}
}

// TestRerunScope_RejectSwallowedByBusiness_RerunnableKept：节点先按 Rerunnable 记一份草稿，再跑一张入口否决
// 一次的嵌套图（错被业务吞掉、走兜底），然后问「继续?」。节点的产出从没被否决过，续跑时草稿不该重做。
func TestRerunScope_RejectSwallowedByBusiness_RerunnableKept(t *testing.T) {
	evals := 0
	ig := rsRejectOnceGraph(t, "rs_inner_c7", &evals)
	drafts := 0
	ob := compose_v2.NewBuilder().WithName("rs_outer_c7")
	ob.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, ok := compose_v2.GetStepResult[string](ctx, "draft"); !ok {
			drafts++
			if err := compose_v2.SaveStepResult(ctx, "draft", fmt.Sprintf("draft-v%d", drafts), compose_v2.Rerunnable()); err != nil {
				return nil, err
			}
		}
		_ = compose_v2.Run(ctx, ig, compose_v2.NewChannelManager(), compose_v2.WithRunNS("rs_inner_c7")) // 失败就走兜底
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续?"}); err != nil {
			return nil, err
		}
		return nil, nil
	})
	ob.AddEdge(compose_v2.StartNode, "host")
	ob.AddEdge("host", compose_v2.EndNode)
	asked, err := rsLoop(t, rsCompile(t, ob), rsOpts())
	if err != nil {
		t.Fatalf("没收敛：%v", err)
	}
	if drafts != 1 || len(asked) != 1 {
		t.Errorf("草稿不该重做、「继续?」只问一次：drafts=%d asked=%v", drafts, asked)
	}
}

// rsHookNested 在 plan 节点的 OnNodeStart 里跑一张嵌套图（钩子自己的检查）。
type rsHookNested struct {
	compose_v2.NoOpOrchestrationHooks
	run func(ctx context.Context) error
}

func (h rsHookNested) OnNodeStart(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	if id != "plan" {
		return ctx, nil
	}
	return ctx, h.run(ctx)
}

// TestRerunScope_HookPhaseReject_BodyUntouched：节点体按 Rerunnable 记草稿后问「继续?」挂起；续跑时
// OnNodeStart 钩子里跑的嵌套图入口否决一次。被否决的是钩子那张图的输入（钩子相位产出的），节点体的
// 草稿与已答的问题不受影响：只推进钩子相位那个分域。曾经推进整次执行：草稿重做、「继续?」换 ID 再问
// （实测）。
func TestRerunScope_HookPhaseReject_BodyUntouched(t *testing.T) {
	hookEvals := 0
	ib := compose_v2.NewBuilder().WithName("rs_hook_inner")
	ib.AddNode("x", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	ib.AddConditionalEdge(compose_v2.StartNode, func(context.Context, compose_v2.Channels) (compose_v2.NodeID, error) {
		hookEvals++
		if hookEvals == 2 { // 续跑时钩子的检查器一时判不出
			return "", errors.New("钩子检查器一时判不出")
		}
		return "x", nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	ib.AddEdge("x", compose_v2.EndNode)
	ig := rsCompile(t, ib)
	hooks := rsHookNested{run: func(ctx context.Context) error {
		return compose_v2.Run(ctx, ig, compose_v2.NewChannelManager(), compose_v2.WithRunNS("rs_hook_inner"))
	}}
	drafts := 0
	b := compose_v2.NewBuilder().WithName("rs_c14")
	b.AddNode("plan", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, ok := compose_v2.GetStepResult[string](ctx, "draft"); !ok {
			drafts++
			if err := compose_v2.SaveStepResult(ctx, "draft", fmt.Sprintf("draft-v%d", drafts), compose_v2.Rerunnable()); err != nil {
				return nil, err
			}
		}
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续?"}); err != nil {
			return nil, err
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "plan")
	b.AddEdge("plan", compose_v2.EndNode)
	g := rsCompile(t, b)
	opts := append(rsOpts(), compose_v2.WithHooks(hooks))
	answers := map[string]*schema.Message{}
	var asked []string
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...)
	for i := 0; i < 8 && err != nil; i++ {
		if ie, ok := compose_v2.IsInterrupt(err); ok {
			for _, q := range ie.AllQueries() {
				asked = append(asked, q.Question)
				answers[q.InterruptID] = schema.UserMessage("好")
			}
		}
		err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), append(append([]compose_v2.RunOption{}, opts...), compose_v2.WithResumeAnswers(answers))...)
	}
	if err != nil {
		t.Fatalf("没收敛：%v", err)
	}
	if drafts != 1 || len(asked) != 1 {
		t.Errorf("钩子相位的否决不该作废节点体：drafts=%d（应 1）asked=%v（应只问一次）", drafts, asked)
	}
}

// rsDraftConfirmGraph：嵌套图 draft（产出 plan-vN）→ confirm（问「批准 plan-vN 吗」，答案拼进产出）。
func rsDraftConfirmGraph(t *testing.T, produced *int, outCh *compose_v2.Channel[string]) *compose_v2.Graph {
	t.Helper()
	ib := compose_v2.NewBuilder().WithName("rs_draft_confirm")
	draftCh := compose_v2.LastValue[string]("rs_draft")
	compose_v2.WithChannel(ib, draftCh)
	compose_v2.WithChannel(ib, outCh)
	ib.AddNode("draft", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		*produced++
		return []compose_v2.ChannelWrite{compose_v2.Write(draftCh, fmt.Sprintf("plan-v%d", *produced))}, nil
	})
	ib.AddNode("confirm", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		d, _ := compose_v2.Get(chs, draftCh)
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准 " + d + " 吗"})
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, d+" 已批准("+ans.Content+")")}, nil
	})
	ib.AddEdge(compose_v2.StartNode, "draft")
	ib.AddEdge("draft", "confirm")
	ib.AddEdge("confirm", compose_v2.EndNode)
	return rsCompile(t, ib)
}

// rsRejectV1 是外层的条件出边：产出以 plan-v1 开头就按 CondFailRerunFrom 否决。
func rsRejectV1(ch *compose_v2.Channel[string]) func(context.Context, compose_v2.Channels) (compose_v2.NodeID, error) {
	return func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		if p, _ := compose_v2.Get(chs, ch); strings.HasPrefix(p, "plan-v1") {
			return "", errors.New("plan-v1 不合格")
		}
		return compose_v2.EndNode, nil
	}
}

// TestRerunScope_OuterRejected_NestedGraphAsksAnew：外层节点里跑一张嵌套图（draft → confirm 问人），外层
// 条件边否决 v1。调用方累积传答案。重做时嵌套图产出 v2、问「批准 plan-v2 吗」——必须真的问到用户，不能
// 拿 v1 的批准兑付。嵌套图有自己的 cm，从头跑时在同一个位置问，没有外层代际链时 ID 跟 v1 那问相同
// （实测：v2 从没被问过就执行了）。两种形态：节点体里直接跑、AddSubgraphNode。
func TestRerunScope_OuterRejected_NestedGraphAsksAnew(t *testing.T) {
	t.Run("直接跑", func(t *testing.T) {
		produced := 0
		outCh := compose_v2.LastValue[string]("rs_out9")
		ig := rsDraftConfirmGraph(t, &produced, outCh)
		ob := compose_v2.NewBuilder().WithName("rs_outer9")
		planCh := compose_v2.LastValue[string]("rs_plan9")
		compose_v2.WithChannel(ob, planCh)
		ob.AddNode("plan", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			cm := compose_v2.NewChannelManager()
			if err := compose_v2.Run(ctx, ig, cm, compose_v2.WithRunNS("g9")); err != nil {
				return nil, err
			}
			out, _ := compose_v2.Get(cm.Channels(), outCh)
			return []compose_v2.ChannelWrite{compose_v2.Write(planCh, out)}, nil
		})
		ob.AddEdge(compose_v2.StartNode, "plan")
		ob.AddConditionalEdge("plan", rsRejectV1(planCh), compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
		cm := compose_v2.NewChannelManager()
		asked, err := rsLoopWithCM(t, rsCompile(t, ob), rsOpts(), &cm)
		if err != nil {
			t.Fatalf("没收敛：%v", err)
		}
		if final, _ := compose_v2.Get(cm.Channels(), planCh); final != "plan-v2 已批准(批准#2)" {
			t.Errorf("执行的 v2 应被单独问过：final=%q asked=%v", final, asked)
		}
	})
	t.Run("AddSubgraphNode", func(t *testing.T) {
		produced := 0
		outCh := compose_v2.LastValue[string]("rs_out10")
		sg := rsDraftConfirmGraph(t, &produced, outCh)
		ob := compose_v2.NewBuilder().WithName("rs_outer10")
		compose_v2.WithChannel(ob, outCh)
		ob.AddSubgraphNode("sub", sg, compose_v2.WithExport(outCh))
		ob.AddEdge(compose_v2.StartNode, "sub")
		ob.AddConditionalEdge("sub", rsRejectV1(outCh), compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
		cm := compose_v2.NewChannelManager()
		asked, err := rsLoopWithCM(t, rsCompile(t, ob), rsOpts(), &cm)
		if err != nil {
			t.Fatalf("没收敛：%v", err)
		}
		if final, _ := compose_v2.Get(cm.Channels(), outCh); final != "plan-v2 已批准(批准#2)" {
			t.Errorf("执行的 v2 应被单独问过：final=%q asked=%v", final, asked)
		}
	})
}

// rsLoopWithCM 同 rsLoop，但每次用新的 cm 并把最后一个交回（读收敛之后的 channel）。
func rsLoopWithCM(t *testing.T, g *compose_v2.Graph, opts []compose_v2.RunOption, cm **compose_v2.ChannelManager) (asked []string, err error) {
	t.Helper()
	answers := map[string]*schema.Message{}
	*cm = compose_v2.NewChannelManager()
	err = compose_v2.Run(context.Background(), g, *cm, opts...)
	for i := 0; i < 8 && err != nil; i++ {
		if ie, ok := compose_v2.IsInterrupt(err); ok {
			for _, q := range ie.AllQueries() {
				asked = append(asked, q.Question)
				answers[q.InterruptID] = schema.UserMessage(fmt.Sprintf("批准#%d", len(asked)))
			}
		}
		*cm = compose_v2.NewChannelManager()
		err = compose_v2.Run(context.Background(), g, *cm, append(append([]compose_v2.RunOption{}, opts...), compose_v2.WithResumeAnswers(answers))...)
	}
	return asked, err
}

// rsScopeHooks 记下嵌套图每次 OnGraphStart 看到的相位名。
type rsScopeHooks struct {
	compose_v2.NoOpOrchestrationHooks
	scopes *[]string
}

func (h rsScopeHooks) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	*h.scopes = append(*h.scopes, compose_v2.SubCallScope(ctx))
	return ctx, nil
}

// TestRerunScope_GraphPhaseNameStableAcrossExplicitFresh：外层任务首跑带「新开」信号（agent_v2 顶层恒如此），
// 外层节点里亲口 WithFreshRun 跑嵌套图，再问一个问题；续跑。两次 OnGraphStart 的相位名必须相同——曾经
// 按「亲口新开之后被置 nil 的外层访问器」决定带不带编号，首跑带、续跑不带，图级相位里发起的调用在
// 首跑与续跑之间换了身份（实测：同一个把关问题问了两遍）。
func TestRerunScope_GraphPhaseNameStableAcrossExplicitFresh(t *testing.T) {
	for _, topFresh := range []bool{true, false} {
		var scopes []string
		ib := compose_v2.NewBuilder().WithName("rs_inner4")
		ib.AddNode("x", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
		ib.AddEdge(compose_v2.StartNode, "x")
		ib.AddEdge("x", compose_v2.EndNode)
		ig := rsCompile(t, ib)
		ob := compose_v2.NewBuilder().WithName("rs_outer4")
		ob.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if err := compose_v2.Run(ctx, ig, compose_v2.NewChannelManager(), compose_v2.WithRunNS("rs_inner4"),
				compose_v2.WithFreshRun(), compose_v2.WithHooks(rsScopeHooks{scopes: &scopes})); err != nil {
				return nil, err
			}
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续?"}); err != nil {
				return nil, err
			}
			return nil, nil
		})
		ob.AddEdge(compose_v2.StartNode, "host")
		ob.AddEdge("host", compose_v2.EndNode)
		var first []compose_v2.RunOption
		if topFresh {
			first = append(first, compose_v2.WithFreshRun())
		}
		if _, err := rsLoop(t, rsCompile(t, ob), rsOpts(), first...); err != nil {
			t.Fatalf("没收敛：%v", err)
		}
		if len(scopes) != 2 || scopes[0] != scopes[1] {
			t.Errorf("topFresh=%v：同一次外层执行里同一张嵌套图，首跑与续跑的 OnGraphStart 相位名应相同：%q", topFresh, scopes)
		}
	}
}

// TestRerunScope_ToolCallScopeReject_RedoesThatScopeAndSurvivesCheckpoint：tool_call 里按 Rerunnable 记一份
// 草稿，再跑一张入口条件边会否决「草稿 v1」的嵌套图，错一路冒出节点（节点因此失败）。续跑时只有这个
// tool_call 分域换代：草稿重做成 v2、入口放行，然后问人挂起——新一代的草稿记录要随存档带走，答完再
// 续跑时拿回 v2、不再起草（共起草 2 次）。
//
// 两处都承重：分域的代际不进有效代际时，续跑拿回 v1、入口再否决，永远收敛不了；存档投影按「代际 ≠
// 执行级代际」丢记录时，v2 的记录没进存档，答完续跑又起草一遍（v3）。
func TestRerunScope_ToolCallScopeReject_RedoesThatScopeAndSurvivesCheckpoint(t *testing.T) {
	ib := compose_v2.NewBuilder().WithName("rs_tc_inner")
	draftCh := compose_v2.LastValue[string]("rs_tc_draft")
	compose_v2.WithChannel(ib, draftCh)
	ib.AddNode("x", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	ib.AddConditionalEdge(compose_v2.StartNode, func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		if d, _ := compose_v2.Get(chs, draftCh); d == "draft-v1" {
			return "", errors.New("draft-v1 不合格")
		}
		return "x", nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	ib.AddEdge("x", compose_v2.EndNode)
	ig := rsCompile(t, ib)
	drafts := 0
	ob := compose_v2.NewBuilder().WithName("rs_tc_outer")
	ob.AddNode("tools", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		tc := compose_v2.WithCurrentToolCallContext(ctx, "call_x", 0)
		d, ok := compose_v2.GetStepResult[string](tc, "draft")
		if !ok {
			drafts++
			d = fmt.Sprintf("draft-v%d", drafts)
			if err := compose_v2.SaveStepResult(tc, "draft", d, compose_v2.Rerunnable()); err != nil {
				return nil, err
			}
		}
		if err := compose_v2.Run(tc, ig, compose_v2.NewChannelManager(), compose_v2.WithRunNS("rs_tc_inner"),
			compose_v2.WithInitialWrites(compose_v2.Write(draftCh, d))); err != nil {
			return nil, err
		}
		if _, err := compose_v2.Ask(tc, &compose_v2.AskQuery{Question: "发布 " + d + "？"}); err != nil {
			return nil, err
		}
		return nil, nil
	})
	ob.AddEdge(compose_v2.StartNode, "tools")
	ob.AddEdge("tools", compose_v2.EndNode)
	og := rsCompile(t, ob)
	opts := rsOpts()
	answers := map[string]*schema.Message{}
	var asked []string
	err := compose_v2.Run(context.Background(), og, compose_v2.NewChannelManager(), opts...)
	for i := 0; i < 6 && err != nil; i++ {
		if ie, ok := compose_v2.IsInterrupt(err); ok {
			for _, q := range ie.AllQueries() {
				asked = append(asked, q.Question)
				answers[q.InterruptID] = schema.UserMessage("好")
			}
		}
		err = compose_v2.Run(context.Background(), og, compose_v2.NewChannelManager(),
			append(append([]compose_v2.RunOption{}, opts...), compose_v2.WithResumeAnswers(answers))...)
	}
	if err != nil {
		t.Fatalf("没收敛：%v（起草 %d 次，问过 %v）", err, drafts, asked)
	}
	if drafts != 2 || strings.Join(asked, ",") != "发布 draft-v2？" {
		t.Errorf("应起草 2 次、只就 v2 问一次：起草 %d 次，问过 %v", drafts, asked)
	}
}
