package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestCompletedSibling_VisitAdvancesAtCommit 钉住「**代际推进 = 写入提交成功那一刻**」，
// 且这一刻对**部分失败超步里成功的那个成员**同样到达。
//
// # 它守的那个洞
//
// 「成功」被收敛成一个原子时刻之后，第一版把代际推进放在「本超步全员成功」那一段——
// 于是部分失败超步里成功的那个成员掉进一个新的半成功态：**写入已提交**（ApplyAll 在
// 失败 return 之前）+ **进 CompletedFrontier 后永不重跑** + **代际却没推进**。
//
// 后果不是「少加了个数」：该节点被回边重访时提的**新问题**，算出与上一轮**完全相同**的
// InterruptID ⟹ 业务累积传的历次答案（框架自己文档化的预期形态，见 warnUnmatchedAnswerIDs
// 的 hint）把它静默满足 ⟹ **用户根本没被问到，而 `err == nil`**。
//
// 实测（修复前）：visit 序列 `[0 0 0]`、`NodeVisits=map[]`、Run3 直接 `err=nil` 跑完，
// A 两轮各拿到一次「答案」而人只答过一次。
//
// sanity-revert：把 advanceVisitsLocked 里的 visit 自增循环（`cm.visitCount[m.NodeID]++`）挪到下面「全员成功」那段
// （即 `res.FirstErr != nil` 的 return 之后）→ 本用例红。
func TestCompletedSibling_VisitAdvancesAtCommit(t *testing.T) {
	var askVisits []int
	var ids []string
	bReady := false
	round := 0

	mk := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("f1")
		loopCh := compose_v2.Sum("__f1_loop__")
		compose_v2.WithChannel(b, loopCh)
		b.AddNode("A", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			s := compose_v2.CurrentInterruptSite(ctx)
			askVisits = append(askVisits, s.Visit)
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: fmt.Sprintf("第 %d 轮批准吗", round)})
			if err != nil {
				return nil, err
			}
			ids = append(ids, fmt.Sprintf("visit=%d ans=%v", s.Visit, ans != nil))
			return nil, nil
		})
		b.AddNode("B", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if !bReady {
				return nil, errors.New("B 还没准备好")
			}
			return nil, nil
		})
		b.AddNode("C", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			round++
			return []compose_v2.ChannelWrite{compose_v2.Write(loopCh, 1)}, nil
		})
		b.AddEdge(compose_v2.StartNode, "A")
		b.AddEdge(compose_v2.StartNode, "B")
		b.AddEdge("A", "C")
		b.AddEdge("B", "C")
		b.AddConditionalEdge("C", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
			n, _ := compose_v2.Get(chs, loopCh)
			if n >= 2 {
				return compose_v2.EndNode, nil
			}
			return "A", nil // 回边重访 A
		})
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		return g
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "f1_sid"

	// Run1：A 中断（拿到第 1 轮 ID）、B 失败
	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, mk(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid)))
	if !ok {
		t.Fatal("Run1 该中断")
	}
	id1 := ie.AllQueries()[0].InterruptID
	answers := map[string]*schema.Message{id1: schema.UserMessage("批准")}

	// Run2：带答案 → A 成功（进 completed）、B 仍失败
	_ = compose_v2.Run(ctx, mk(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(answers))
	cp, _ := cper.LoadNS(ctx, sid, "")
	t.Logf("Run2 后 cp: CurrentFrontier=%+v CompletedFrontier=%+v NodeVisits=%v",
		cp.CurrentFrontier, cp.CompletedFrontier, cp.NodeVisits)

	// Run3：B 好了 → 全链跑通 → C 回边重访 A，A 提**第 2 轮**的新问题
	bReady = true
	err3 := compose_v2.Run(ctx, mk(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(answers)) // 业务累积传旧答案（框架文档化的预期形态）
	t.Logf("Run3 err=%v", err3)
	t.Logf("A 每次执行看到的 visit = %v", askVisits)
	t.Logf("A 拿到答案的记录 = %v", ids)

	// **承重 1**：成功成员的代际在提交点推进了——存档里看得见
	if cp.NodeVisits["A"] != 1 {
		t.Errorf("部分失败超步里成功的成员 A，其代际该在写入提交那一刻推进，实际 NodeVisits=%v\n"+
			"  空 = 代际推进被放在「全员成功」那段 ⟹ A 写入已提交、永不重跑、却永远停在第 0 代", cp.NodeVisits)
	}
	// **承重 2**：A 被回边重访时提的是**新问题**，不该被上一轮的旧答案静默满足
	if _, isInt := compose_v2.IsInterrupt(err3); !isInt {
		t.Errorf("A 第 2 轮的新提问应重新中断问人，实际 err=%v\n"+
			"  err=nil = 新问题算出了跟上一轮相同的 InterruptID，被累积传的旧答案静默消费——"+
			"用户根本没被问到\n  visit 序列 %v（应是 [0 0 1]，全 0 即代际没推进）", err3, askVisits)
	}
	// **承重 3（正向）**：人只答过一次，就只该有一次「拿到答案」
	if len(ids) != 1 {
		t.Errorf("人只答过 1 次，A 却拿到 %d 次答案：%v", len(ids), ids)
	}
}
