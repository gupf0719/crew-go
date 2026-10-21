package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// abortNode 返一个终局失败（业务违反协议那一类：重跑必然一样）。
func abortNode(ran *int) compose_v2.NodeFunc {
	return func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if ran != nil {
			*ran++
		}
		return nil, fmt.Errorf("%w: 业务违反协议", compose_v2.ErrRunAborted)
	}
}

// TestExitErr_AbortDominatesInterrupt 钉住「终局失败 > 中断 > 第一个业务错」这个次序，
// 以及它守的两条实测伤害。
//
// # 为什么终局失败要排在中断前面
//
// **不是「换一种偏袒」，是它严格支配**：终局失败意味着这次 Run 已经定了（重跑必然
// 一样），**不存在「用户答了就有用」的未来**。报中断上去的代价是一次白问 + 一次销毁，
// 换来的信息量为零。
//
// ⚠️ 次序只管**报告**。「本超步有没有分支终局失败」这个**事实**记在
// `frontierResult.AbortErr` 上，后果（清不清 cp 子树 / 能不能重试）读那个字段——
// 后果不该取决于谁赢了仲裁。
func TestExitErr_AbortDominatesInterrupt(t *testing.T) {
	t.Run("终局失败+业务错：结局确定，不再靠 map 迭代序", func(t *testing.T) {
		// 改之前实测 40 次里 34 次清档、6 次留 failed 死档且被判可重试——
		// **同一份记录、两种后果**。这一条专抓那个抖动。
		const rounds = 40
		biz := 0
		for i := 0; i < rounds; i++ {
			b := compose_v2.NewBuilder().WithName("prio_nondet")
			b.AddNode("a", abortNode(nil))
			b.AddNode("z", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				return nil, errors.New("普通业务错")
			})
			for _, n := range []compose_v2.NodeID{"a", "z"} {
				b.AddEdge(compose_v2.StartNode, n)
				b.AddEdge(n, compose_v2.EndNode)
			}
			g, err := b.Compile()
			if err != nil {
				t.Fatal(err)
			}
			runErr := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager())
			if !errors.Is(runErr, compose_v2.ErrRunAborted) {
				biz++
			}
		}
		if biz != 0 {
			t.Fatalf("%d/%d 次返了业务错——那些次会被判**可重试**（确定性协议违规重跑 "+
				"MaxRetries+1 遍、副作用重放）且留下 failed 死档占着 (sid,ns) 单槽。"+
				"结局不该取决于 map 迭代序", biz, rounds)
		}
	})

	t.Run("终局失败+中断：这一轮就报，人不会被白问一次", func(t *testing.T) {
		var asked, aborted int
		b := compose_v2.NewBuilder().WithName("prio_interrupt")
		b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			asked++
			_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准转账吗"})
			return nil, err
		})
		b.AddNode("bad", abortNode(&aborted))
		for _, n := range []compose_v2.NodeID{"ask", "bad"} {
			b.AddEdge(compose_v2.StartNode, n)
			b.AddEdge(n, compose_v2.EndNode)
		}
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		cper := compose_v2.NewInMemoryCheckpointer()
		runErr := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("prio"))

		if aborted != 1 || asked != 1 {
			t.Fatalf("fixture 没搭对：两个分支各该跑一次，实际 ask=%d abort=%d", asked, aborted)
		}
		if !errors.Is(runErr, compose_v2.ErrRunAborted) {
			t.Fatalf("该报终局失败，实际 %v", runErr)
		}
		// **承重**：不能报成中断——报了调用方就会去问人，而那个问题注定答不成；
		// 等他答完下一轮才撞上 abort，purge 把他刚给的答案连同兄弟已完成的工作一起清掉。
		if _, isInt := compose_v2.IsInterrupt(runErr); isInt {
			t.Fatal("报成了中断——调用方会去问一个注定答不成的问题，" +
				"答完下一轮才撞终局失败，答案被 purge 清掉")
		}
		// 正向断言：终局失败该清子树，不留死档
		if pend, _ := cper.ListPending(context.Background(), "prio"); len(pend) != 0 {
			t.Errorf("终局失败该清掉子树，实际残留 %d 条挂起档——"+
				"它占着 (sid,ns) 单槽，而终局失败重跑必然一样", len(pend))
		}
	})

	t.Run("没有终局失败时中断仍优先", func(t *testing.T) {
		// 对偶：别把上面那条改成「abort 永远赢」之外还顺手动了中断的优先级。
		// 中断输给业务错的话，cp 里挂着的那个问题永远没人去答。
		b := compose_v2.NewBuilder().WithName("prio_dual")
		b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗"})
			return nil, err
		})
		b.AddNode("z", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, errors.New("普通业务错")
		})
		for _, n := range []compose_v2.NodeID{"ask", "z"} {
			b.AddEdge(compose_v2.StartNode, n)
			b.AddEdge(n, compose_v2.EndNode)
		}
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		cper := compose_v2.NewInMemoryCheckpointer()
		runErr := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("dual"))
		ie, isInt := compose_v2.IsInterrupt(runErr)
		if !isInt {
			t.Fatalf("中断必须优先于业务错，否则挂起档没人去答：%v", runErr)
		}
		// **正向断言**：问题真的能被答完，不只是「报了中断」
		if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("dual"),
			compose_v2.WithResumeAnswers(map[string]*schema.Message{
				ie.AllQueries()[0].InterruptID: schema.UserMessage("批准"),
			})); err == nil {
			t.Fatal("fixture 没搭对：兄弟分支的业务错还在，这一轮不该成功")
		} else if _, stillInt := compose_v2.IsInterrupt(err); stillInt {
			t.Error("答完之后不该还是中断——说明答案没被那条挂起消费")
		}
	})
}
