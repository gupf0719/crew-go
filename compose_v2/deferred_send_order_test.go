package compose_v2_test

import (
	"context"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestDeferredSends_MergedBeforeNewSends 守 deferred sends 的**前置**合并次序。
//
// # 承诺在哪
//
// `graph_loop.go` 的合并点自己写着：
//
//	PA-H1：重放成功成员的 SendDirectives——**前置**合并（deferred 属更早成员，
//	无中断执行时其 sends 先于本超步新 sends 进流，保 Append 等顺序敏感 reducer
//	与无中断路径顺序一致）
//
//	res.SendDirectives = append(deferredSends, res.SendDirectives...)
//
// 也就是说这条承诺**只对顺序敏感的 reducer 可观测**——`LastValue` 上「先写 A 后写 B」
// 和「先写 B 后写 A」的终值不同但两种都「有值」，`Append` 才把次序留在结果里。
//
// # 为什么补这个哨兵
//
// 承诺明写，判别形态（deferred 与本超步新 sends **同时非空**）在既有用例里**真被走到过**
// ——但**没有任何用例断言过两者的相对次序**：把 `append(deferredSends, res.SendDirectives...)`
// 前后调换后跑 `./compose_v2/... ./agent_v2/` 照样全绿。
//
// 走到了、却没人断言，跟前缀消歧被抹平那次是同一形状。
//
// # 判别形态怎么搭出来
//
// split 投 Z ×2 ⟹ 两个实例。一个中断（进 CurrentFrontier）、一个成功并投 W
// （它的 send 进 cp.DeferredSends）。Resume 时中断那个重跑、也投 W——于是这一超步里
// **deferred（兄弟上一轮存下的）与 new（本轮重跑刚产的）同时非空**，合并次序才可观测。
//
// sanity-revert：把合并改成 `append(res.SendDirectives, deferredSends...)` → 本用例红，
// 且红在「顺序应为 [DEFERRED NEW]，实际 [NEW DEFERRED]」。
func TestDeferredSends_MergedBeforeNewSends(t *testing.T) {
	logCh := compose_v2.Append[string]("__order_log__")
	var interrupted atomic.Bool

	b := compose_v2.NewBuilder().WithName("deferred_order")
	b = compose_v2.WithChannel(b, logCh)
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "Z"},
			compose_v2.SendDirective{Target: "Z"},
		)
		return nil, nil
	})
	b.AddNode("Z", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, answered := compose_v2.GetResumeAnswer(ctx, "__ord_q__"); answered {
			// Resume 重跑的那个实例——它的 send 是**本超步新产的**
			compose_v2.SendFrom(ctx, compose_v2.SendDirective{
				Target:  "W",
				Payload: []compose_v2.ChannelWrite{compose_v2.Write(logCh, []string{"NEW"})},
			})
			return nil, nil
		}
		if interrupted.CompareAndSwap(false, true) {
			return nil, &compose_v2.InterruptError{
				Reason: "ord",
				Query:  &compose_v2.AskQuery{InterruptID: "__ord_q__", Question: "?"},
			}
		}
		// 首跑成功的那个实例——它的 send 会存进 cp.DeferredSends 等 Resume 重放
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{
			Target:  "W",
			Payload: []compose_v2.ChannelWrite{compose_v2.Write(logCh, []string{"DEFERRED"})},
		})
		return nil, nil
	})
	b.AddNode("W", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("W", compose_v2.EndNode)

	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "ord_sid"

	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))); !ok {
		t.Fatal("Run1 应中断（一个 Z 实例抛 InterruptError）")
	}

	// **fixture 自检**：判别形态必须真搭出来了——deferred 非空，否则下面的次序断言
	// 在只有一条 send 的情况下恒真（CLAUDE.md：fixture 没搭对场景 ⟹ 用例恒真）。
	cp1, _ := cper.LoadNS(ctx, sid, "")
	if cp1 == nil || len(cp1.DeferredSends) != 1 {
		t.Fatalf("fixture 没搭对：期望成功实例留下 1 条 DeferredSends，实际 %+v", cp1)
	}

	cm2 := compose_v2.NewChannelManager()
	if err := compose_v2.Run(ctx, g, cm2,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			"__ord_q__": schema.UserMessage("go"),
		})); err != nil {
		t.Fatalf("Resume 应成功: %v", err)
	}

	got, _ := compose_v2.Get(cm2.Channels(), logCh)
	t.Logf("Append 顺序: %v", got)

	// **承重 1**：两条都得在——少一条的话次序比较没有意义。
	if len(got) != 2 {
		t.Fatalf("期望 deferred 与 new 各一条（共 2 条），实际 %v", got)
	}

	// **承重 2**：deferred 在前。这是那条承诺唯一可观测的地方。
	if got[0] != "DEFERRED" || got[1] != "NEW" {
		t.Errorf("deferred sends 应**前置**合并，期望 [DEFERRED NEW]，实际 %v\n"+
			"  deferred 属更早成员，无中断执行时它的 send 先于本超步新 sends 进流；\n"+
			"  次序反了 ⟹ Append 等顺序敏感 reducer 的结果跟无中断路径不一致", got)
	}
}
