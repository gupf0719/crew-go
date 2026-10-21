package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestStayPending_IsFactNotArbitration 钉住「存档标 Pending 还是 Failed 看**事实**，
// 不看谁赢了那场仲裁」。
//
// # 它守的伤害
//
// 超步部分失败时，N 个分支的结局被仲裁成**一个** error 返给调用方，而档位曾经读那个
// 单值（`shouldStayPending(runErr)`）。于是「一分支以控制流信号退出（软取消 / ctx 超时 /
// `CondFailRerunFrom` / 陈旧写回…）、另一分支是普通业务错」时，返谁按 frontier 索引序，
// 而静态边成员的次序来自对 map 的遍历——**同一份记录、同一次超时，40 次里 34 次存档
// Pending（可续、历史留住用户原话）、6 次 Failed（永久不可续、历史里那句话彻底没有）。**
//
// 现在「有没有分支值得留 Pending」是 `frontierResult.StayPending` 这个独立事实，
// 经 Run 级局部变量直达出口，**不经过报告那条路**。
func TestStayPending_IsFactNotArbitration(t *testing.T) {
	t.Run("控制流信号+业务错：档位确定，不随 map 迭代序抖", func(t *testing.T) {
		const rounds = 40
		failed := 0
		for i := 0; i < rounds; i++ {
			b := compose_v2.NewBuilder().WithName("sp_nondet")
			b.AddNode("a", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				// 控制流信号：下游 HTTP 客户端撞 ctx deadline 是最常见的真实形态
				return nil, fmt.Errorf("下游超时: %w", context.DeadlineExceeded)
			})
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
			cper := compose_v2.NewInMemoryCheckpointer()
			sid := fmt.Sprintf("sp%d", i)
			_ = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
				compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
			cp, _ := cper.LoadNS(context.Background(), sid, "")
			if cp == nil || cp.Status != compose_v2.CPStatusPending {
				failed++
			}
		}
		if failed != 0 {
			t.Fatalf("%d/%d 次标了 Failed——那些次在 ListPending 里列不出来（谁在等答案查不到），"+
				"而兄弟分支明明是「等下次接着跑」那一类。档位是**后果**，不该取决于"+
				"仲裁恰好挑中了哪个分支的错", failed, rounds)
		}
	})

	t.Run("全是业务错时仍标 Failed", func(t *testing.T) {
		// 对偶：别把上面那条做成「永远 Pending」。没有任何分支值得留时必须标 Failed,
		// 否则这份确定性失败会混进 ListPending 的「谁在等答案」清单。
		b := compose_v2.NewBuilder().WithName("sp_dual")
		b.AddNode("a", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, errors.New("业务错 A")
		})
		b.AddNode("z", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, errors.New("业务错 Z")
		})
		for _, n := range []compose_v2.NodeID{"a", "z"} {
			b.AddEdge(compose_v2.StartNode, n)
			b.AddEdge(n, compose_v2.EndNode)
		}
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		cper := compose_v2.NewInMemoryCheckpointer()
		_ = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("spd"))
		cp, _ := cper.LoadNS(context.Background(), "spd", "")
		if cp == nil {
			t.Fatal("失败现场该留着")
		}
		if cp.Status != compose_v2.CPStatusFailed {
			t.Fatalf("没有分支值得留 Pending 时必须标 Failed，实际 %s——"+
				"标成 Pending 会让确定性失败混进 ListPending 的「谁在等答案」清单", cp.Status)
		}
	})

	t.Run("中断+业务错：挂起档照常可续", func(t *testing.T) {
		// 中断本来就在 shouldStayPending 里，这条防的是「加事实位时把中断那半改坏」。
		b := compose_v2.NewBuilder().WithName("sp_int")
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
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("spi"))
		ie, isInt := compose_v2.IsInterrupt(runErr)
		if !isInt {
			t.Fatalf("该报中断：%v", runErr)
		}
		pend, _ := cper.ListPending(context.Background(), "spi")
		if len(pend) != 1 {
			t.Fatalf("挂起档该被 auto-load 看得到，实际 %d 条", len(pend))
		}
		// 正向断言：答得完
		if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("spi"),
			compose_v2.WithResumeAnswers(map[string]*schema.Message{
				ie.AllQueries()[0].InterruptID: schema.UserMessage("批准"),
			})); err == nil {
			t.Fatal("fixture 没搭对：兄弟的业务错还在，这轮不该成功")
		} else if _, still := compose_v2.IsInterrupt(err); still {
			t.Error("答完不该还是中断——说明答案没被消费")
		}
	})
}
