package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// CondFailRerunFrom 只在 cond **判过**产出、判为不合格时作废按 Rerunnable 记下的产出。cond 没判完
// 就被打断（软取消、调用方 ctx 取消）时产出没被判过，续跑照常拿回——当成否决的话，用户只是按了
// 停止，续跑就把已经完成的产出重做一遍。ctx 死了就一律按没判完：判官被打断时返回的错不一定带着
// ctx 错的链，(ctx, err) 分不出「判完才到点」与「被打断」，而把后者当否决的代价（重做产出连同副作用）
// 远大于把前者当没判完的代价（多一次续跑、多判一次）。

type judgeOutcome int

const (
	judgeSoftCancel    judgeOutcome = iota // 判官跑着的时候用户按了停止
	judgeCtxCancel                         // 判官跑着的时候调用方 ctx 被取消
	judgeReject                            // 判官判了：不合格
	judgeRejectCtxDies                     // 判官返回了一个错、ctx 这时已经死了，错里没带 ctx 错的链
)

// runPlanJudge 跑一张「plan 节点按 Rerunnable 记产出 → cond 经嵌套图调判官」的图：第一次按 first
// 那种方式收场，之后续跑到收敛，返回 plan 节点实际产出了几次、续跑了几次。
func runPlanJudge(t *testing.T, first judgeOutcome) (produced, resumes int) {
	t.Helper()
	sig := compose_v2.NewSoftCancelSignal()
	ctx1, cancel := context.WithCancel(context.Background())
	defer cancel()

	jb := compose_v2.NewBuilder().WithName("judge")
	judged := 0
	jb.AddNode("j", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		judged++
		return nil, nil
	})
	jb.AddEdge(compose_v2.StartNode, "j")
	jb.AddEdge("j", compose_v2.EndNode)
	jg, err := jb.Compile()
	if err != nil {
		t.Fatal(err)
	}

	b := compose_v2.NewBuilder().WithName("plan_judge")
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
		if produced == 1 && judged == 0 {
			switch first {
			case judgeSoftCancel:
				sig.Trigger("用户按了停止")
			case judgeCtxCancel:
				cancel()
			}
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(planCh, p)}, nil
	})
	b.AddEdge(compose_v2.StartNode, "plan")
	b.AddConditionalEdge("plan", func(ctx context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		if err := compose_v2.Run(ctx, jg, compose_v2.NewChannelManager(), compose_v2.WithRunNS("judge")); err != nil {
			return "", err
		}
		if p, _ := compose_v2.Get(chs, planCh); p == "plan-v1" {
			switch first {
			case judgeReject:
				return "", errors.New("plan-v1 不合格")
			case judgeRejectCtxDies:
				if judged == 1 {
					cancel()
					return "", fmt.Errorf("判官中止：%v", context.Canceled) // %v：链断了
				}
				return "", errors.New("plan-v1 不合格")
			}
		}
		return compose_v2.EndNode, nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("s")}
	if err := compose_v2.Run(ctx1, g, compose_v2.NewChannelManager(), append(opts, compose_v2.WithSoftCancel(sig))...); err == nil {
		t.Fatalf("首跑应以 cond 失败收场")
	}
	for resumes = 1; resumes <= 3; resumes++ {
		if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...); err == nil {
			return produced, resumes
		}
	}
	t.Fatalf("续跑三次仍未收敛")
	return produced, resumes
}

func TestCondFailRerunFrom_InterruptedJudgeKeepsRerunnableOutput(t *testing.T) {
	for _, c := range []struct {
		name  string
		first judgeOutcome
	}{{"soft_cancel", judgeSoftCancel}, {"ctx_cancel", judgeCtxCancel}} {
		t.Run(c.name, func(t *testing.T) {
			if n, r := runPlanJudge(t, c.first); n != 1 || r != 1 {
				t.Errorf("判官被打断、产出没被判过：一次续跑拿回按 Rerunnable 记下的那份，实际产出 %d 次、续跑 %d 次", n, r)
			}
		})
	}
}

// TestCondFailRerunFrom_RejectingJudgeRedoesRerunnableOutput 是对照格：判官真的否决了（ctx 活着），产出
// 作废、重做，一次续跑收敛。
func TestCondFailRerunFrom_RejectingJudgeRedoesRerunnableOutput(t *testing.T) {
	if n, r := runPlanJudge(t, judgeReject); n != 2 || r != 1 {
		t.Errorf("判官否决之后应重新产出（共 2 次）、一次续跑收敛，实际产出 %d 次、续跑 %d 次", n, r)
	}
}

// TestCondFailRerunFrom_CtxDeadWithoutCtxChain_NotRejected：判官返回错的时候调用方 ctx 已经死了，错里却
// 没带 ctx 错的链（业务自写的判官、第三方 SDK 用 %v 包了一层）。(ctx, err) 分不出这是「判完才到点」
// 还是「被打断」，按没判完：产出不作废，续跑先拿回原产出再判一次（多一次续跑），真被否决了再重做。
// 曾经要求「链上有 ctx 错」才算被打断，这个形态被判成否决：用户按了停止，续跑重做了已经完成的产出
// （实测）。
func TestCondFailRerunFrom_CtxDeadWithoutCtxChain_NotRejected(t *testing.T) {
	if n, r := runPlanJudge(t, judgeRejectCtxDies); n != 2 || r != 2 {
		t.Errorf("ctx 死了按没判完：第一次续跑拿回原产出、再判时否决，第二次续跑重做（产出 2 次、续跑 2 次），"+
			"实际产出 %d 次、续跑 %d 次", n, r)
	}
}

// TestNestedGraphEntryCondEdge_RecordsUnderOuterNodeExecution：嵌套图的入口条件边（从 StartNode 出发）
// 跟图级钩子一样记在承载它的外层节点执行名下——外层节点重跑、嵌套图跟着从头跑时，入口条件边里
// 记下的从那里读回。顶层图的入口没有外层节点，那里记不下（照旧 no-op）。
func TestNestedGraphEntryCondEdge_RecordsUnderOuterNodeExecution(t *testing.T) {
	var entryWork int
	ib := compose_v2.NewBuilder().WithName("inner")
	ib.AddNode("x", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	ib.AddConditionalEdge(compose_v2.StartNode, func(ctx context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		if _, ok := compose_v2.GetStepResult[string](ctx, "route"); !ok {
			entryWork++
			if err := compose_v2.SaveStepResult(ctx, "route", "x"); err != nil {
				return "", err
			}
		}
		return "x", nil
	})
	ib.AddEdge("x", compose_v2.EndNode)
	ig, err := ib.Compile()
	if err != nil {
		t.Fatal(err)
	}
	asked := false
	ob := compose_v2.NewBuilder().WithName("outer")
	ob.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if err := compose_v2.Run(ctx, ig, compose_v2.NewChannelManager(), compose_v2.WithRunNS("inner")); err != nil {
			return nil, err
		}
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"}); err != nil {
			asked = true
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
	cper := compose_v2.NewInMemoryCheckpointer()
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("s")}
	err = compose_v2.Run(context.Background(), og, compose_v2.NewChannelManager(), opts...)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || !asked {
		t.Fatalf("外层节点应停在提问上，got %v", err)
	}
	if err := compose_v2.Run(context.Background(), og, compose_v2.NewChannelManager(), append(opts,
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")}))...); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if entryWork != 1 {
		t.Errorf("嵌套图入口条件边里记下的应在外层节点重跑时读回（只做 1 次），实际 %d 次", entryWork)
	}
}
