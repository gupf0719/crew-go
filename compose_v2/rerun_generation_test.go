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

// 条件边按 CondFailRerunFrom 否决之后，被否决的那次节点执行「重做」：按 Rerunnable 记的产出作废，
// 重做时问的问题是新问题。嵌套图的入口条件边否决时，重做的是承载它的外层节点。

// TestNestedGraphEntryCond_RerunFromRedoesRerunnableRoute：嵌套图的入口条件边里按 Rerunnable 记下
// 路由（LLM 型 router 的产出），路由不合法时按 CondFailRerunFrom 报错。续跑时路由必须重做——入口
// 条件边记在外层节点执行名下，否决要推进的是外层节点这次执行的代际。不推进时原样拿回那份坏路由，
// 续跑多少次都一样（实测续跑 4 次仍是同一个错）。
func TestNestedGraphEntryCond_RerunFromRedoesRerunnableRoute(t *testing.T) {
	routes := 0
	ib := compose_v2.NewBuilder().WithName("inner")
	ib.AddNode("x", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	ib.AddConditionalEdge(compose_v2.StartNode, func(ctx context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		r, ok := compose_v2.GetStepResult[string](ctx, "route")
		if !ok {
			routes++
			r = "bogus"
			if routes > 1 {
				r = "x"
			}
			if err := compose_v2.SaveStepResult(ctx, "route", r, compose_v2.Rerunnable()); err != nil {
				return "", err
			}
		}
		if r != "x" {
			return "", fmt.Errorf("router 给了不存在的分支 %q", r)
		}
		return "x", nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	ib.AddEdge("x", compose_v2.EndNode)
	ig, err := ib.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ob := compose_v2.NewBuilder().WithName("outer")
	ob.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, compose_v2.Run(ctx, ig, compose_v2.NewChannelManager(), compose_v2.WithRunNS("inner"))
	})
	ob.AddEdge(compose_v2.StartNode, "host")
	ob.AddEdge("host", compose_v2.EndNode)
	og, err := ob.Compile()
	if err != nil {
		t.Fatal(err)
	}
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()), compose_v2.WithSessionID("s")}
	if err := compose_v2.Run(context.Background(), og, compose_v2.NewChannelManager(), opts...); err == nil {
		t.Fatal("前提：首跑的路由不合法，入口条件边应报错")
	}
	if err := compose_v2.Run(context.Background(), og, compose_v2.NewChannelManager(), opts...); err != nil {
		t.Fatalf("入口条件边否决之后，一次续跑就该重做路由并收敛：%v", err)
	}
	if routes != 2 {
		t.Errorf("路由应重做一次（共 2 次），实际 %d 次", routes)
	}
}

// TestCondReject_RedoneQuestionIsANewQuestion：节点先按 Rerunnable 产出方案、再问「批准 vN 吗」，
// 条件边否决 v1。调用方累积传历次答案（预期写法）。重做之后问的是 v2，必须是个新问题——跟 v1 的
// 提问同一个 ID 的话，用户对 v1 的批准直接兑付给 v2，v2 的问句从头到尾没出现，`err == nil`。
func TestCondReject_RedoneQuestionIsANewQuestion(t *testing.T) {
	produced := 0
	b := compose_v2.NewBuilder().WithName("plan_ask")
	planCh := compose_v2.LastValue[string]("plan")
	compose_v2.WithChannel(b, planCh)
	b.AddNode("plan", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		p, ok := compose_v2.GetStepResult[string](ctx, "llm")
		if !ok {
			produced++
			p = fmt.Sprintf("plan-v%d", produced)
			if err := compose_v2.SaveStepResult(ctx, "llm", p, compose_v2.Rerunnable()); err != nil {
				return nil, err
			}
		}
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准 " + p + " 吗"})
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(planCh, p+" 已批准("+ans.Content+")")}, nil
	})
	b.AddEdge(compose_v2.StartNode, "plan")
	b.AddConditionalEdge("plan", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		if p, _ := compose_v2.Get(chs, planCh); strings.HasPrefix(p, "plan-v1") {
			return "", errors.New("plan-v1 不合格")
		}
		return compose_v2.EndNode, nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()), compose_v2.WithSessionID("s")}
	cm := compose_v2.NewChannelManager()
	err = compose_v2.Run(context.Background(), g, cm, opts...)
	answers := map[string]*schema.Message{}
	var asked []string
	for i := 0; i < 6 && err != nil; i++ {
		if ie, ok := compose_v2.IsInterrupt(err); ok {
			for _, q := range ie.AllQueries() {
				asked = append(asked, q.Question)
				answers[q.InterruptID] = schema.UserMessage(fmt.Sprintf("批准#%d", len(asked)))
			}
		}
		cm = compose_v2.NewChannelManager()
		err = compose_v2.Run(context.Background(), g, cm, append(opts, compose_v2.WithResumeAnswers(answers))...)
	}
	if err != nil {
		t.Fatalf("没能收敛：%v", err)
	}
	p, _ := compose_v2.Get(cm.Channels(), planCh)
	if p != "plan-v2 已批准(批准#2)" {
		t.Errorf("最后执行的 v2 必须是被单独问过、单独批准的：final=%q，问过的=%q", p, asked)
	}
	if len(asked) != 2 || asked[1] != "批准 plan-v2 吗" {
		t.Errorf("v2 的问句应出现在第二问，问过的=%q", asked)
	}
}

// TestNestedGraph_SameNSTwiceInOneExecution_RecordsNotShared：外层节点在循环里对两份相同的工单各跑
// 一遍同一个 ns 的嵌套图。入口条件边里记下的路由（按次序铸：x、y）属于各自那一遍——只按 ns 分的话
// 第二遍拿回第一遍的记录，路由被照搬。外层节点重跑时两遍按原次序各自拿回，不重做。
func TestNestedGraph_SameNSTwiceInOneExecution_RecordsNotShared(t *testing.T) {
	routes := 0
	outCh := compose_v2.LastValue[string]("out")
	ib := compose_v2.NewBuilder().WithName("inner")
	compose_v2.WithChannel(ib, outCh)
	for _, id := range []string{"x", "y"} {
		id := id
		ib.AddNode(compose_v2.NodeID(id), func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(outCh, id)}, nil
		})
		ib.AddEdge(compose_v2.NodeID(id), compose_v2.EndNode)
	}
	ib.AddConditionalEdge(compose_v2.StartNode, func(ctx context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		r, ok := compose_v2.GetStepResult[string](ctx, "route")
		if !ok {
			routes++
			r = "x"
			if routes%2 == 0 {
				r = "y"
			}
			if err := compose_v2.SaveStepResult(ctx, "route", r); err != nil {
				return "", err
			}
		}
		return compose_v2.NodeID(r), nil
	})
	ig, err := ib.Compile()
	if err != nil {
		t.Fatal(err)
	}
	var results []string
	ob := compose_v2.NewBuilder().WithName("outer")
	ob.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		results = results[:0]
		for i := 0; i < 2; i++ {
			cm := compose_v2.NewChannelManager()
			if err := compose_v2.Run(ctx, ig, cm, compose_v2.WithRunNS("inner")); err != nil {
				return nil, err
			}
			out, _ := compose_v2.Get(cm.Channels(), outCh)
			results = append(results, out)
		}
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"}); err != nil {
			return nil, err
		}
		return nil, nil
	})
	ob.AddEdge(compose_v2.StartNode, "host")
	ob.AddEdge("host", compose_v2.EndNode)
	og, err := ob.Compile()
	if err != nil {
		t.Fatal(err)
	}
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()), compose_v2.WithSessionID("s")}
	err = compose_v2.Run(context.Background(), og, compose_v2.NewChannelManager(), opts...)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("前提：外层节点应停在提问上，got %v", err)
	}
	if strings.Join(results, ",") != "x,y" {
		t.Errorf("两遍各有自己的路由记录：want x,y，got %v（第二遍拿回了第一遍的）", results)
	}
	if err := compose_v2.Run(context.Background(), og, compose_v2.NewChannelManager(), append(opts,
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")}))...); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if routes != 2 || strings.Join(results, ",") != "x,y" {
		t.Errorf("外层节点重跑时两遍按原次序拿回：路由铸了 %d 次（want 2），结果 %v", routes, results)
	}
}

// TestNestedGraph_ExplicitFreshRunRecallsNothing：外层节点每次执行都用 WithFreshRun 新开嵌套图（业务明说
// 「别续，从头来」），第一次执行在嵌套图跑完之后自己失败，续跑时外层节点重跑。嵌套图从头跑，它的入口
// 条件边也得重新判——记在外层节点执行名下的上一次路由不能拿回（实测曾照搬）。
func TestNestedGraph_ExplicitFreshRunRecallsNothing(t *testing.T) {
	routes := 0
	outCh := compose_v2.LastValue[string]("out")
	ib := compose_v2.NewBuilder().WithName("inner")
	compose_v2.WithChannel(ib, outCh)
	for _, id := range []string{"x", "y"} {
		id := id
		ib.AddNode(compose_v2.NodeID(id), func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(outCh, id)}, nil
		})
		ib.AddEdge(compose_v2.NodeID(id), compose_v2.EndNode)
	}
	ib.AddConditionalEdge(compose_v2.StartNode, func(ctx context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		r, ok := compose_v2.GetStepResult[string](ctx, "route")
		if !ok {
			routes++
			r = "x"
			if routes > 1 {
				r = "y"
			}
			if err := compose_v2.SaveStepResult(ctx, "route", r); err != nil {
				return "", err
			}
		}
		return compose_v2.NodeID(r), nil
	})
	ig, err := ib.Compile()
	if err != nil {
		t.Fatal(err)
	}
	var results []string
	attempt := 0
	ob := compose_v2.NewBuilder().WithName("outer")
	ob.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		attempt++
		cm := compose_v2.NewChannelManager()
		if err := compose_v2.Run(ctx, ig, cm, compose_v2.WithRunNS("inner"), compose_v2.WithFreshRun()); err != nil {
			return nil, err
		}
		out, _ := compose_v2.Get(cm.Channels(), outCh)
		results = append(results, out)
		if attempt == 1 {
			return nil, errors.New("下游暂时不可用")
		}
		return nil, nil
	})
	ob.AddEdge(compose_v2.StartNode, "host")
	ob.AddEdge("host", compose_v2.EndNode)
	og, err := ob.Compile()
	if err != nil {
		t.Fatal(err)
	}
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()), compose_v2.WithSessionID("s")}
	if err := compose_v2.Run(context.Background(), og, compose_v2.NewChannelManager(), opts...); err == nil {
		t.Fatal("前提：首跑外层节点应失败")
	}
	if err := compose_v2.Run(context.Background(), og, compose_v2.NewChannelManager(), opts...); err != nil {
		t.Fatalf("续跑: %v", err)
	}
	if routes != 2 || strings.Join(results, ",") != "x,y" {
		t.Errorf("亲口声明新开的嵌套图不该拿回上一次的路由：路由判了 %d 次（want 2），结果 %v", routes, results)
	}
}
