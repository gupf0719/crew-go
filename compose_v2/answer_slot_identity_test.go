package compose_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestAsk_IdentityIsFrameworkDerived 钉住「答案的投递身份只能由执行位置派生」。
//
// # 它守的三种泄漏
//
// 曾经 `Ask` 认调用方自填的 `AskQuery.InterruptID`（空才派生）。业务能选 key ⟹ 两个
// **不同的提问**可以落在**同一个答案位**上，实测三种形态、全部 `err == nil`：
//
//  1. **跨超步**：s1 问「确认转账」、s2 问「确认删除历史」，两处都填同一个 ID ⟹ 用户只
//     **见过**转账（s2 的问题从未呈现给任何人），Resume 后 s2 照样取到答案、执行。
//  2. **跨 visit**：循环节点每轮用同一个常量 ID ⟹ 用户批一次，之后每轮静默通过。
//  3. **同节点两问**：一个节点里问两次、复用同一个 ID ⟹ 第二件事跟着第一件的授权执行。
//
// 三种的共同前提是**业务能选 key**。key 只能由站点派生之后它们结构上都不存在：
// 节点不同 ⟹ NodeID 不同；轮次不同 ⟹ Visit 不同；同节点第二问 ⟹ 计数器不同。
//
// 本用例断言的是**第 1 种和第 3 种**（第 2 种要跑循环图，成本高且同理），外加
// 「同站点重跑仍取得到」——那是幂等续跑的命脉，改坏了 HITL 直接死循环。
func TestAsk_IdentityIsFrameworkDerived(t *testing.T) {
	t.Run("业务自填身份当场被拒", func(t *testing.T) {
		// 直接断言那道门本身——把它拆掉时这一条精确红，不必绕经泄漏后果。
		b := compose_v2.NewBuilder().WithName("reject")
		var got error
		b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			_, got = compose_v2.Ask(ctx, &compose_v2.AskQuery{
				InterruptID: "CONFIRM", Question: "确认转账"})
			return nil, nil
		})
		b.AddEdge(compose_v2.StartNode, "n")
		b.AddEdge("n", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager()); err != nil {
			t.Fatalf("图本身该跑完: %v", err)
		}
		if got == nil {
			t.Fatal("Ask 接受了自填身份——两个不同的提问就能共用一个答案位了")
		}
		if _, isInt := compose_v2.IsInterrupt(got); isInt {
			t.Fatalf("该返「拒绝」而不是中断（中断意味着它认了这个身份）：%v", got)
		}
		if !strings.Contains(got.Error(), "不接受自填") {
			t.Fatalf("拒绝时要说清为什么：%v", got)
		}
	})

	t.Run("两个节点各自派生独立的答案位", func(t *testing.T) {
		var did []string
		mk := func(act string) compose_v2.NodeFunc {
			return func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: act})
				if err != nil {
					return nil, err
				}
				did = append(did, act+"="+ans.Content)
				return nil, nil
			}
		}
		b := compose_v2.NewBuilder().WithName("two_sites")
		b.AddNode("s1", mk("转账5000"))
		b.AddNode("s2", mk("删除全部历史"))
		b.AddEdge(compose_v2.StartNode, "s1")
		b.AddEdge("s1", "s2")
		b.AddEdge("s2", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		cper := compose_v2.NewInMemoryCheckpointer()
		ctx := context.Background()

		ie1, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("ts")))
		if !ok {
			t.Fatal("fixture 没搭对：s1 该中断")
		}
		id1 := ie1.AllQueries()[0].InterruptID

		// 只答 s1 那一条
		ie2, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("ts"),
			compose_v2.WithResumeAnswers(map[string]*schema.Message{id1: schema.UserMessage("同意")})))

		// **承重**：s2 必须重新问出来，而不是拿 s1 的答案把事办了
		if !ok {
			t.Fatalf("s2 没有重新提问——它拿到了 s1 的答案。已执行：%v", did)
		}
		if q := ie2.AllQueries()[0]; q.Question != "删除全部历史" {
			t.Fatalf("第二次问的该是 s2 那件事，实际 %q", q.Question)
		}
		if id2 := ie2.AllQueries()[0].InterruptID; id2 == id1 {
			t.Fatalf("两个节点派生出了同一个身份 %q——那样答案照样会串", id2)
		}
		if len(did) != 1 || did[0] != "转账5000=同意" {
			t.Fatalf("只有 s1 该被执行，实际 %v", did)
		}
	})

	t.Run("同一个节点问两次是两个独立的答案位", func(t *testing.T) {
		var did []string
		b := compose_v2.NewBuilder().WithName("two_asks")
		b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			a1, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "扣款吗"})
			if err != nil {
				return nil, err
			}
			did = append(did, "扣款="+a1.Content)
			a2, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "通知全员吗"})
			if err != nil {
				return nil, err
			}
			did = append(did, "通知="+a2.Content)
			return nil, nil
		})
		b.AddEdge(compose_v2.StartNode, "n")
		b.AddEdge("n", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		cper := compose_v2.NewInMemoryCheckpointer()
		ctx := context.Background()

		ie1, _ := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("ta")))
		idA := ie1.AllQueries()[0].InterruptID

		// 只答第一问 ⟹ 第二问必须被问出来（而不是复用第一问的授权）
		ie2, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("ta"),
			compose_v2.WithResumeAnswers(map[string]*schema.Message{idA: schema.UserMessage("好")})))
		if !ok {
			t.Fatalf("第二问没被问出来——它复用了第一问的答案。已执行：%v", did)
		}
		idB := ie2.AllQueries()[0].InterruptID
		if idB == idA {
			t.Fatalf("同一节点的两问派生出同一身份 %q", idB)
		}
		if q := ie2.AllQueries()[0].Question; q != "通知全员吗" {
			t.Fatalf("第二问该是通知，实际 %q", q)
		}

		// **同站点重跑仍取得到**：两个答案一起给，整条链跑完
		if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("ta"),
			compose_v2.WithResumeAnswers(map[string]*schema.Message{
				idA: schema.UserMessage("好"), idB: schema.UserMessage("发"),
			})); err != nil {
			t.Fatalf("两个答案给齐该收尾：%v", err)
		}
		// 正向断言：两件事都办了、各按各的答案
		if len(did) < 2 || did[len(did)-2] != "扣款=好" || did[len(did)-1] != "通知=发" {
			t.Fatalf("两问该各拿各的答案，实际 %v", did)
		}
	})
}
