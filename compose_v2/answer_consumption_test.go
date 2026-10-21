package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestAnswerConsumption_StaleIdentityFailsLoudly 钉住「**一个答案只能满足一个提问**」。
//
// # 它防的是哪一类，而不是哪一个
//
// 中断寻址的身份由执行位置派生，任何一个位置维算错都会让**两个不同的提问落在同一个
// 答案位上**；而调用方**累积传历次答案**是本框架文档化的预期形态（`warnUnmatchedAnswerIDs`
// 的 hint 明写那是合法场景、别升级成 error）。两件事一叠：后一个提问被前一个的旧答案
// **静默满足**——用户根本没被问到，`err == nil`。
//
// 这不是假想。出边归成员那次改动里，代际推进点挪错一格就造出了这个形态，而当时
// **变异验证 + 两个 binary 冒烟 + 真 LLM 冒烟全绿**，靠一个独立审计才抓到。
//
// 所以这条哨兵**不守某一个身份算法**，守的是那个与算法正交的事实：兑付过的票不能再兑
// 第二次。身份判据将来再出错时，它会**当场终局失败**而不是静默生效。
//
// 判别式：本用例故意让两个不同的提问算出同一个身份（业务自填同一个 ID——那是唯一还
// 允许自填的路径），断言第二个提问**报错**而不是拿到第一个的答案。
func TestAnswerConsumption_StaleIdentityFailsLoudly(t *testing.T) {
	const sharedID = "CONFIRM" // 业务两处都写同一个常量——身份漂移的可控复刻
	var asked []string
	var did []string

	mk := func(round *int) *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("answer_consumption")
		step := compose_v2.Sum("__ac_step__")
		compose_v2.WithChannel(b, step)
		// n 每轮问一个**不同**的问题，但用同一个自填 ID
		b.AddNode("n", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			r, _ := compose_v2.Get(chs, step)
			q := fmt.Sprintf("第 %d 件事：%s", r, []string{"确认转账 5000", "确认删除全部历史"}[min(r, 1)])
			if ans, ok := compose_v2.GetResumeAnswer(ctx, sharedID); ok && ans != nil {
				did = append(did, q)
				return []compose_v2.ChannelWrite{compose_v2.Write(step, 1)}, nil
			}
			asked = append(asked, q)
			return nil, &compose_v2.InterruptError{
				Reason: "confirm",
				Query:  &compose_v2.AskQuery{InterruptID: sharedID, Question: q},
			}
		})
		b.AddNode("tail", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		})
		b.AddEdge(compose_v2.StartNode, "n")
		b.AddEdge("tail", compose_v2.EndNode)
		b.AddConditionalEdge("n", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
			r, _ := compose_v2.Get(chs, step)
			if r >= 2 {
				return "tail", nil
			}
			return "n", nil // 回边：同一节点问第二件事
		})
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		return g
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "ac_sid"
	r := 0

	// Run1：问第一件事
	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, mk(&r), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))); !ok {
		t.Fatal("Run1 该中断在第一件事")
	}
	// 用户只答了第一件事
	answers := map[string]*schema.Message{sharedID: schema.UserMessage("批准")}

	// Run2：第一件事被批准执行 → 回边 → 节点问**第二件事**，而它算出同一个身份
	err2 := compose_v2.Run(ctx, mk(&r), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(answers))

	t.Logf("问过: %v", asked)
	t.Logf("执行过: %v", did)
	t.Logf("Run2 err = %v", err2)

	// **承重 1**：第二件事**没有**被那张旧票放行
	if len(did) > 1 {
		t.Errorf("用户只批准了第一件事，却执行了 %d 件：%v\n"+
			"  第二件事被第一件的旧答案静默满足了——这正是「一个答案只能满足一个提问」要防的", len(did), did)
	}
	// **承重 2**：而且是**响的**——终局失败，不是静默跑完、也不是反复问同一个问题
	if !errors.Is(err2, compose_v2.ErrRunAborted) {
		t.Errorf("落在已兑付答案位上的新提问应终局失败（ErrRunAborted），实际 err=%v\n"+
			"  err=nil 说明它被静默满足了；反复中断说明变成了死循环——两者都不如当场报错", err2)
	}
	// **承重 3（正向）**：第一件事确实被正常执行过——否则这条测的就不是「第二件被拦」
	if len(did) != 1 {
		t.Errorf("第一件事该被正常批准执行 1 次，实际 %v", did)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestAnswerConsumption_LegitimateRerunStillGetsAnswer 守反向：**同一处提问重跑时，
// 答案必须还能兑付**——否则这道兜底会把正常的 Resume 砸掉。
//
// 判据是「这一步成没成」：节点读了答案之后仍可能失败（业务错 / 后续中断 / 提交失败），
// 那次执行整体不算数，重跑时它必须还能拿到同一个答案。所以兑付记账在**提交点**，不在
// 读取点——读到就记的话，本用例会红。
//
// sanity-revert：把 NextAnswerSlot 里的 `record` 挪成「读到即并入 cm」（绕过提交点）
// → 第二次执行读不到答案 → 本用例红。
func TestAnswerConsumption_LegitimateRerunStillGetsAnswer(t *testing.T) {
	var reads int
	failOnce := true

	mk := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("answer_rerun")
		b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗"})
			if err != nil {
				return nil, err
			}
			if ans != nil {
				reads++
			}
			// 拿到答案之后**才**失败一次：这次执行整体不算数，重跑必须还能拿到同一个答案
			if failOnce {
				failOnce = false
				return nil, errors.New("拿到答案之后才失败的业务错")
			}
			return nil, nil
		})
		b.AddEdge(compose_v2.StartNode, "n")
		b.AddEdge("n", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		return g
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "ac_rerun_sid"

	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, mk(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid)))
	if !ok {
		t.Fatal("Run1 该中断")
	}
	answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("批准")}

	// Run2：读到答案后失败 —— 这次不算兑付
	if err := compose_v2.Run(ctx, mk(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(answers)); err == nil {
		t.Fatal("fixture 没搭对：Run2 该失败")
	}
	// Run3：重跑，**必须还能拿到同一个答案**
	err3 := compose_v2.Run(ctx, mk(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(answers))
	if err3 != nil {
		t.Fatalf("同一处提问重跑该照常拿到答案，实际 err=%v\n"+
			"  ErrRunAborted = 兑付记在了读取点而不是提交点——那会把正常的 Resume 砸掉", err3)
	}
	if reads != 2 {
		t.Errorf("答案该被读到 2 次（失败那次 + 重跑那次），实际 %d 次", reads)
	}
}
