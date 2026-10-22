package compose_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestAdvanceRerunGeneration_VoidsOnlyThatPhase：节点在相位 p 里问一次、记一条普通记录与一条可重做记录，拿到答案之后判定这一格
// 的产出作废（AdvanceRerunGeneration）并以失败退出。续跑时：相位 p 里的提问换新 ID（累积传的旧答案对不上，停在新问题上），
// 普通记录照常拿回、可重做记录作废；另一个相位 q 里的提问与节点体的代际不受影响。
func TestAdvanceRerunGeneration_VoidsOnlyThatPhase(t *testing.T) {
	type seen struct{ normal, redo bool }
	var execs []seen
	var pGens, bodyGens []int
	advanced, afterAdvance := false, -1 // afterAdvance：作废之后紧接着的那次执行在 execs 里的下标
	b := compose_v2.NewBuilder().WithName("advance")
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		q := compose_v2.EnterPhase(ctx, "q")
		if _, err := compose_v2.Ask(q, &compose_v2.AskQuery{Question: "相位 q 的问题"}); err != nil {
			return nil, err
		}
		p := compose_v2.EnterPhase(ctx, "p")
		_, normal := compose_v2.GetStepResult[int](p, "normal")
		_, redo := compose_v2.GetStepResult[int](p, "redo")
		execs = append(execs, seen{normal, redo})
		pGens, bodyGens = append(pGens, compose_v2.RerunGeneration(p)), append(bodyGens, compose_v2.RerunGeneration(ctx))
		if err := compose_v2.SaveStepResult(p, "normal", 1); err != nil {
			return nil, err
		}
		if err := compose_v2.SaveStepResult(p, "redo", 1, compose_v2.Rerunnable()); err != nil {
			return nil, err
		}
		if _, err := compose_v2.Ask(p, &compose_v2.AskQuery{Question: "相位 p 的问题"}); err != nil {
			return nil, err
		}
		if !advanced {
			advanced, afterAdvance = true, len(execs)
			compose_v2.AdvanceRerunGeneration(p)
			return nil, errors.New("这一格的产出作废")
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g := rsCompile(t, b)
	opts := rsOpts()
	answers := map[string]*schema.Message{}
	var asked []string
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...)
	sawFailure := false
	for i := 0; i < 8 && err != nil; i++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			if sawFailure {
				t.Fatalf("第二次失败：%v", err)
			}
			sawFailure = true // 那次作废，调用方原样重试
		} else {
			for _, q := range ie.AllQueries() {
				asked = append(asked, q.Question+"/"+q.InterruptID)
				answers[q.InterruptID] = schema.UserMessage("是")
			}
		}
		err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
			append(append([]compose_v2.RunOption{}, opts...), compose_v2.WithResumeAnswers(answers))...)
	}
	if err != nil {
		t.Fatalf("没收敛：%v", err)
	}
	if !sawFailure {
		t.Fatal("fixture 没搭对：应有一次作废失败")
	}
	// 问过的：q 一次、p 两次（作废之后换了新 ID）
	var qCount, pIDs = 0, map[string]bool{}
	for _, a := range asked {
		switch {
		case len(a) > len("相位 q 的问题") && a[:len("相位 q 的问题")] == "相位 q 的问题":
			qCount++
		default:
			pIDs[a] = true
		}
	}
	if qCount != 1 {
		t.Errorf("相位 q 的问题应只问一次（没被推进），实际 %d 次：%v", qCount, asked)
	}
	if len(pIDs) != 2 {
		t.Errorf("相位 p 的问题作废之后应换新 ID 再问一次（共 2 个 ID），实际 %v", asked)
	}
	if afterAdvance < 0 || afterAdvance >= len(execs) {
		t.Fatalf("fixture 没搭对：作废之后应还有执行（执行记录 %v）", execs)
	}
	next := execs[afterAdvance]
	if !next.normal {
		t.Errorf("作废之后的那次执行应拿回相位 p 的普通记录，实际拿不到（执行记录 %v）", execs)
	}
	if next.redo {
		t.Errorf("作废之后相位 p 的可重做记录应不可达，实际还在（执行记录 %v）", execs)
	}
	if pGens[afterAdvance] != 1 || bodyGens[afterAdvance] != 0 {
		t.Errorf("作废之后相位 p 的代际应为 1、节点体为 0，实际 p=%v 节点体=%v", pGens, bodyGens)
	}
}
