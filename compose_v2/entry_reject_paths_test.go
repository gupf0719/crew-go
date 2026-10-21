package compose_v2_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// 嵌套图入口否决推进「产出那份输入的分域」的几条路径（前提见 rerun_scope_test.go）：节点级钩子
// 相位、业务用 %v 包断了否决标记、扇出的节点只把一个错往上报、同一个分域被反复否决时存档不膨胀。

// erRejectGraph 是一张嵌套图：入口条件边读 draftCh，值等于 bad 时按 CondFailRerunFrom 否决。
func erRejectGraph(t *testing.T, name string, draftCh *compose_v2.Channel[string], bad string, evals *int) *compose_v2.Graph {
	t.Helper()
	ib := compose_v2.NewBuilder().WithName(name)
	compose_v2.WithChannel(ib, draftCh)
	ib.AddNode("x", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	ib.AddConditionalEdge(compose_v2.StartNode, func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		*evals++
		if d, _ := compose_v2.Get(chs, draftCh); d == bad {
			return "", errors.New(d + " 不合格")
		}
		return "x", nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	ib.AddEdge("x", compose_v2.EndNode)
	return rsCompile(t, ib)
}

// erLoop 首跑之后不管什么错都续跑（中断就把问到的都答上），最多 5 次；返回续跑了几次与最后的错。
func erLoop(g *compose_v2.Graph, opts []compose_v2.RunOption) (int, error) {
	answers := map[string]*schema.Message{}
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...)
	i := 0
	for ; i < 5 && err != nil; i++ {
		if ie, ok := compose_v2.IsInterrupt(err); ok {
			for _, q := range ie.AllQueries() {
				answers[q.InterruptID] = schema.UserMessage("好")
			}
		}
		err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
			append(append([]compose_v2.RunOption{}, opts...), compose_v2.WithResumeAnswers(answers))...)
	}
	return i, err
}

// erDraftOnce 在 ctx 所在分域里按 Rerunnable 记一份草稿（记过就拿回），返回草稿。
func erDraftOnce(ctx context.Context, drafts *int, prefix string) (string, error) {
	if d, ok := compose_v2.GetStepResult[string](ctx, "draft"); ok {
		return d, nil
	}
	*drafts++
	d := fmt.Sprintf("%s-v%d", prefix, *drafts)
	return d, compose_v2.SaveStepResult(ctx, "draft", d, compose_v2.Rerunnable())
}

type erNodeStartHook struct {
	compose_v2.NoOpOrchestrationHooks
	fn func(ctx context.Context) error
}

func (h erNodeStartHook) OnNodeStart(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	if id != "plan" {
		return ctx, nil
	}
	return ctx, h.fn(ctx)
}

// TestEntryReject_NodeHookPhaseProducer_IsRedone：OnNodeStart 钩子里按 Rerunnable 起草、拿草稿跑一张入口
// 否决 v1 的嵌套图，错一路冒出钩子（节点因此失败）。续跑时钩子相位要重新起草、一次收敛。
//
// 曾经节点级钩子相位的访问器是直接填 scope 的字面量，分域链是空的：否决推进了钩子这一格，钩子自己
// 读有效代际时看不见，续跑永远拿回 v1（实测续跑 5 次不收敛）。
func TestEntryReject_NodeHookPhaseProducer_IsRedone(t *testing.T) {
	draftCh := compose_v2.LastValue[string]("er_hook_draft")
	evals, drafts := 0, 0
	inner := erRejectGraph(t, "er_hook_inner", draftCh, "hd-v1", &evals)
	var gens []int
	hooks := erNodeStartHook{fn: func(ctx context.Context) error {
		gens = append(gens, compose_v2.RerunGeneration(ctx))
		d, err := erDraftOnce(ctx, &drafts, "hd")
		if err != nil {
			return err
		}
		return compose_v2.Run(ctx, inner, compose_v2.NewChannelManager(), compose_v2.WithRunNS("er_hook_inner"),
			compose_v2.WithInitialWrites(compose_v2.Write(draftCh, d)))
	}}
	b := compose_v2.NewBuilder().WithName("er_hook_outer")
	b.AddNode("plan", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	b.AddEdge(compose_v2.StartNode, "plan")
	b.AddEdge("plan", compose_v2.EndNode)
	g := rsCompile(t, b)
	resumes, err := erLoop(g, append(rsOpts(), compose_v2.WithHooks(hooks)))
	if err != nil {
		t.Fatalf("续跑 %d 次仍失败（钩子里看到的代际 %v）：%v", resumes, gens, err)
	}
	if resumes != 1 || drafts != 2 {
		t.Errorf("应续跑 1 次、起草 2 次（v1 被否决后重起草 v2），实际续跑 %d 次、起草 %d 次；代际 %v", resumes, drafts, gens)
	}
}

// erHostGraph 的 host 节点在节点体里起草、跑入口否决 v1 的嵌套图，把嵌套图的错交给 wrap 处理后返回。
func erHostGraph(t *testing.T, name string, drafts *int, wrap func(error) error) *compose_v2.Graph {
	t.Helper()
	draftCh := compose_v2.LastValue[string](name + "_draft")
	evals := 0
	inner := erRejectGraph(t, name+"_inner", draftCh, "d-v1", &evals)
	b := compose_v2.NewBuilder().WithName(name)
	b.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		d, err := erDraftOnce(ctx, drafts, "d")
		if err != nil {
			return nil, err
		}
		if err := compose_v2.Run(ctx, inner, compose_v2.NewChannelManager(), compose_v2.WithRunNS(name+"_inner"),
			compose_v2.WithInitialWrites(compose_v2.Write(draftCh, d))); err != nil {
			return nil, wrap(err)
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "host")
	b.AddEdge("host", compose_v2.EndNode)
	return rsCompile(t, b)
}

// TestEntryReject_MarkerLostToPercentV_StillRedone：业务把嵌套图的错用 %v 包一层再返回——否决标记
// 顺着错误链传不上来了，成员以一个普通业务错失败。产出那份输入的草稿照样要重做、一次收敛。
//
// 曾经只认链上的标记：%v 断链之后什么都不推进，续跑永远拿回 v1（实测 5 次不收敛）。
func TestEntryReject_MarkerLostToPercentV_StillRedone(t *testing.T) {
	drafts := 0
	g := erHostGraph(t, "er_pctv", &drafts, func(err error) error { return fmt.Errorf("业务包一层: %v", err) })
	resumes, err := erLoop(g, rsOpts())
	if err != nil {
		t.Fatalf("续跑 %d 次仍失败：%v", resumes, err)
	}
	if resumes != 1 || drafts != 2 {
		t.Errorf("应续跑 1 次、起草 2 次，实际续跑 %d 次、起草 %d 次", resumes, drafts)
	}
}

// TestEntryReject_OnlyOneErrorReported_BothScopesRedone：同一次执行里两个 tool_call 各自起草、各跑一张
// 入口否决 v1 的嵌套图，节点只把一个错往上报（compose_v2.PickFanoutError）。两个 tool_call 的草稿都要
// 重做，一次续跑收敛。
//
// 曾经只认链上的标记：落选的那个 tool_call 的否决丢了，多续跑一轮、它的嵌套图多跑一遍（实测）。
func TestEntryReject_OnlyOneErrorReported_BothScopesRedone(t *testing.T) {
	draftCh := compose_v2.LastValue[string]("er_pick_draft")
	evals := 0
	inner := erRejectGraph(t, "er_pick_inner", draftCh, "tc-v1", &evals)
	drafts := map[int]*int{0: new(int), 1: new(int)}
	innerRuns := map[int]int{}
	b := compose_v2.NewBuilder().WithName("er_pick_outer")
	b.AddNode("tools", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		var errs []error
		for i := 0; i < 2; i++ {
			tc := compose_v2.WithCurrentToolCallContext(ctx, fmt.Sprintf("call_%d", i), i)
			d, err := erDraftOnce(tc, drafts[i], "tc")
			if err != nil {
				return nil, err
			}
			innerRuns[i]++
			errs = append(errs, compose_v2.Run(tc, inner, compose_v2.NewChannelManager(),
				compose_v2.WithRunNS(fmt.Sprintf("er_pick_inner_%d", i)),
				compose_v2.WithInitialWrites(compose_v2.Write(draftCh, d))))
		}
		return nil, compose_v2.PickFanoutError(errs)
	})
	b.AddEdge(compose_v2.StartNode, "tools")
	b.AddEdge("tools", compose_v2.EndNode)
	g := rsCompile(t, b)
	resumes, err := erLoop(g, rsOpts())
	if err != nil {
		t.Fatalf("续跑 %d 次仍失败：%v", resumes, err)
	}
	if resumes != 1 || *drafts[0] != 2 || *drafts[1] != 2 || innerRuns[0] != 2 || innerRuns[1] != 2 {
		t.Errorf("应续跑 1 次、两个 tool_call 各起草 2 次、各跑 2 遍嵌套图，实际续跑 %d 次、起草 %d/%d 次、嵌套图 %v",
			resumes, *drafts[0], *drafts[1], innerRuns)
	}
}

// TestEntryReject_RepeatedScopeRejections_CheckpointDoesNotGrow：一个 tool_call 分域里按 Rerunnable 记一份
// 大草稿、跑一张入口永远否决的嵌套图，反复续跑。每一代草稿都被否决作废，存档里只该留当前那一代。
//
// 曾经存档投影只丢「比执行级代际还旧」的可重做记录，分域级否决推进的代际不看：每续跑一次存档里多
// 一份整条草稿，线性增长（实测 8 次后 9 条 / 17KB）。
func TestEntryReject_RepeatedScopeRejections_CheckpointDoesNotGrow(t *testing.T) {
	draftCh := compose_v2.LastValue[string]("er_grow_draft")
	ib := compose_v2.NewBuilder().WithName("er_grow_inner")
	compose_v2.WithChannel(ib, draftCh)
	ib.AddNode("x", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	ib.AddConditionalEdge(compose_v2.StartNode, func(context.Context, compose_v2.Channels) (compose_v2.NodeID, error) {
		return "", errors.New("永远不合格")
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	ib.AddEdge("x", compose_v2.EndNode)
	inner := rsCompile(t, ib)
	big := strings.Repeat("x", 2000)
	drafts := 0
	b := compose_v2.NewBuilder().WithName("er_grow_outer")
	b.AddNode("tools", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		tc := compose_v2.WithCurrentToolCallContext(ctx, "call_x", 0)
		d, err := erDraftOnce(tc, &drafts, big)
		if err != nil {
			return nil, err
		}
		return nil, compose_v2.Run(tc, inner, compose_v2.NewChannelManager(), compose_v2.WithRunNS("er_grow_inner"),
			compose_v2.WithInitialWrites(compose_v2.Write(draftCh, d)))
	})
	b.AddEdge(compose_v2.StartNode, "tools")
	b.AddEdge("tools", compose_v2.EndNode)
	g := rsCompile(t, b)
	cper := compose_v2.NewInMemoryCheckpointer()
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("s")}
	var sizes []int
	for i := 0; i < 6; i++ {
		if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...); err == nil {
			t.Fatal("入口永远否决，Run 不该成功")
		}
		cp, err := cper.LoadNS(context.Background(), "s", "")
		if err != nil || cp == nil {
			t.Fatalf("第 %d 次之后没有存档：%v", i+1, err)
		}
		raw, err := json.Marshal(cp)
		if err != nil {
			t.Fatal(err)
		}
		sizes = append(sizes, len(raw))
	}
	if drafts != 6 {
		t.Fatalf("fixture 没搭对：每次都该重新起草（否决推进了 tool_call 分域的代际），实际起草 %d 次", drafts)
	}
	if sizes[len(sizes)-1] > sizes[0]+len(big) {
		t.Errorf("存档随否决次数增长：%v（字节）——作废的旧代草稿不该导出", sizes)
	}
}
