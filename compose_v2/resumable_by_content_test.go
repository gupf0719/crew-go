package compose_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// dropStatusCper 模拟**业务自研 backend 漏序列化 `Status`**——读回来那个字段是零值
// （`Unset`）。这是「按档位判」和「按内容判」行为分叉最锋利的一处：写进去是完整的，
// 读回来少一个字段，而且读写两侧都不报错。
type dropStatusCper struct {
	compose_v2.Checkpointer
}

func (d *dropStatusCper) LoadNS(ctx context.Context, sid, ns string) (*compose_v2.Checkpoint, error) {
	cp, err := d.Checkpointer.LoadNS(ctx, sid, ns)
	if err != nil || cp == nil {
		return cp, err
	}
	clone := *cp
	clone.Status = compose_v2.CPStatusUnset // ← 漏掉的就是这一个字段
	return &clone, nil
}

// TestAutoLoad_ByContentNotByStatus 钉住「捡不捡这份档**按内容判**，不按档位判」。
//
// 判据是 [compose_v2.Checkpoint.HasResumableWork]（frontier 或挂起清单非空），三处闸门
// 共用：Run 入口 auto-load、子图载入、`agent_v2.Resume`。
//
// 三个子用例分别守它的三条理由，每条都是「按档位判」下实测过的伤害。
func TestAutoLoad_ByContentNotByStatus(t *testing.T) {
	// mkFlaky 造一个「上游成功一次、下游首跑失败」的两节点链。
	// upstream 计数是关键的量——它区分「从失败点续」和「fresh run 从头跑」。
	mkFlaky := func(upstream *int, downOK *bool) *compose_v2.Graph {
		t.Helper()
		b := compose_v2.NewBuilder().WithName("flaky")
		ch := compose_v2.LastValue[string]("__f__")
		compose_v2.WithChannel(b, ch)
		b.AddNode("charge", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			*upstream++
			return []compose_v2.ChannelWrite{compose_v2.Write(ch, "paid")}, nil
		})
		b.AddNode("ship", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if !*downOK {
				return nil, errors.New("发货网关 503")
			}
			return nil, nil
		})
		b.AddEdge(compose_v2.StartNode, "charge")
		b.AddEdge("charge", "ship")
		b.AddEdge("ship", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		return g
	}

	t.Run("失败档被捡起：只重跑失败节点，上游副作用不重放", func(t *testing.T) {
		// 这是撤掉「auto-load 只捡 Pending」那道 gate 的**核心收益**。
		// 按档位判时：失败档被跳过 ⟹ fresh run ⟹ 扣款重放（实测 1→2）。
		charge := 0
		shipOK := false
		cper := compose_v2.NewInMemoryCheckpointer()
		ctx := context.Background()

		if err := compose_v2.Run(ctx, mkFlaky(&charge, &shipOK), compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("bc")); err == nil {
			t.Fatal("fixture 没搭对：首跑该失败")
		}
		if charge != 1 {
			t.Fatalf("fixture 没搭对：首跑该扣款 1 次，实际 %d", charge)
		}
		stored, _ := cper.LoadNS(ctx, "bc", "")
		if stored == nil || stored.Status != compose_v2.CPStatusFailed {
			t.Fatalf("fixture 没搭对：该留一份失败档，实际 %+v", stored)
		}

		// 故障恢复，同 sid 再跑
		shipOK = true
		before := charge
		if err := compose_v2.Run(ctx, mkFlaky(&charge, &shipOK), compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("bc")); err != nil {
			t.Fatalf("故障恢复后该跑通: %v", err)
		}
		// **承重**：扣款不该再来一遍
		if replayed := charge - before; replayed != 0 {
			t.Fatalf("上游重放了 %d 次——失败档没被捡起，走成了 fresh run。"+
				"这正是撤掉的那道档位 gate 的伤害（嵌套 Resume 时子图捡不起自己的失败子档）", replayed)
		}
	})

	t.Run("backend 漏序列化 Status：照样续得上", func(t *testing.T) {
		// 按档位判时：档读回 Unset ⟹ 不捡 ⟹ 一次真 HITL 中断退化成
		// 「fresh run + 换新 InterruptID + 答案永远配不上」，全程只有一条 warn。
		asked := 0
		b := compose_v2.NewBuilder().WithName("drop_status")
		b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			asked++
			_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗"})
			return nil, err
		})
		b.AddEdge(compose_v2.StartNode, "ask")
		b.AddEdge("ask", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		inner := compose_v2.NewInMemoryCheckpointer()
		cper := &dropStatusCper{Checkpointer: inner}
		ctx := context.Background()

		ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("ds")))
		if !ok {
			t.Fatal("fixture 没搭对：首跑该中断")
		}
		id := ie.AllQueries()[0].InterruptID

		// **承重**：答案要送达（身份没漂 + 档被捡起），整条链收尾
		if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("ds"),
			compose_v2.WithResumeAnswers(map[string]*schema.Message{id: schema.UserMessage("批准")})); err != nil {
			t.Fatalf("漏了 Status 字段不该让 HITL 闭环断掉: %v", err)
		}
		if asked != 2 {
			t.Fatalf("ask 该跑 2 次（首跑抛中断 + 续跑取答案），实际 %d——"+
				"更多说明走了 fresh run，答案配不上新身份", asked)
		}
	})

	t.Run("业务存了一份跑完的快照：不当作可续的活", func(t *testing.T) {
		// 按存在判会把它捡起来 ⟹ 实测 Run 静默返 nil、一个节点都没跑、存档还被当成功清掉。
		// 按内容判它 frontier 与挂起都空，不是「可续的活」，照常 fresh run。
		ran := 0
		b := compose_v2.NewBuilder().WithName("stored_done")
		b.AddNode("n", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			ran++
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
		// 业务把一份「跑完了」的快照存进续跑的槽
		if err := cper.SaveNS(ctx, "sd", "", &compose_v2.Checkpoint{ID: "d", NS: "", GraphID: g.ID()}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("sd")); err != nil {
			t.Fatalf("该照常 fresh run: %v", err)
		}
		// **正向断言**：节点真跑了。只断言 err==nil 的话，「静默零节点」那个退化也会绿
		if ran != 1 {
			t.Fatalf("节点执行了 %d 次，期望 1 次——0 次说明那份跑完的快照被当成可续的活"+
				"捡了起来，Run 静默返 nil 且存档被当成功清掉", ran)
		}
	})
}
