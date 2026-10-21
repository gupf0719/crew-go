package compose_v2_test

// 同 NodeID 多并发实例（SendFrom 同 Target 投 N 次 → frontier 里 N 份，各带
// branchIdx）+ 部分失败 的回归网。
//
// **为什么单开一个文件**：PA-H1 / issue-95 的既有回归全是"一个 NodeID 一个实例"，
// 这个维度是**覆盖盲区**——issue-95#1 的 owner purge 正是在这里翻了车（按 NodeID 清
// 把兄弟实例的记录一起清了 → 静默丢 send），而当时"5 个 PA-H1 回归 + 整包全绿"
// 照样是绿的。
//
// 承重不变量：**记录以 NodeID 为身份，执行单位却是 branch 实例**。凡是"按 NodeID
// 过滤 completedFrontier / deferredSends"的地方都要问一句：这个 NodeID 是不是还有
// 别的实例？兄弟实例已成功、永远不会重跑重产——它的记录清掉就是永久丢。

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestMultiInstance_PartialFailure_SiblingSendPreserved：split 投 Z ×2，实例 a 中断、
// 实例 b 成功并投递 W。Resume 后 a 重跑也投 W ⟹ W 应恰跑 2 次。
//
// sanity-revert：给 appendCompletedSiblingsLocked（graph_loop node 阶段失败路径）加"按
// FailedBranches 过滤" → W 掉到 1（b 的记录连同它的 send 被误删）。**现有 PA-H1
// 回归抓不到这个**，本测试是唯一哨兵。
func TestMultiInstance_PartialFailure_SiblingSendPreserved(t *testing.T) {
	var ranW atomic.Int32
	var interrupted atomic.Bool

	b := compose_v2.NewBuilder().WithName("mi_sibling")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "Z"},
			compose_v2.SendDirective{Target: "Z"},
		)
		return nil, nil
	})
	b.AddNode("Z", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// 首跑：抢到 CAS 的那个实例中断，另一个成功 + 投 W
		if _, answered := compose_v2.GetResumeAnswer(ctx, "__mi_q__"); !answered {
			if interrupted.CompareAndSwap(false, true) {
				return nil, &compose_v2.InterruptError{
					Reason: "mi",
					Query:  &compose_v2.AskQuery{InterruptID: "__mi_q__", Question: "?"},
				}
			}
		}
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{Target: "W"})
		return nil, nil
	})
	b.AddNode("W", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ranW.Add(1)
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

	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("mi_sid"))); !ok {
		t.Fatal("Run1 应中断（一个 Z 实例抛 InterruptError）")
	}
	// 同一个 NodeID 同时在两侧——这是多实例的**正常**状态，不是 bug
	cp1, _ := cper.LoadNS(ctx, "mi_sid", "")
	if len(cp1.CurrentFrontier) != 1 || len(cp1.CompletedFrontier) != 1 {
		t.Fatalf("期望 Current=[Z]（中断实例）+ Completed=[Z]（成功实例），实际 Current=%v Completed=%v",
			cp1.CurrentFrontier, cp1.CompletedFrontier)
	}
	if len(cp1.DeferredSends) != 1 || cp1.DeferredSends[0].Owner != "Z" {
		t.Fatalf("成功实例的 sendW 应存下且 owner=Z, 实际 %+v", cp1.DeferredSends)
	}

	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("mi_sid"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			"__mi_q__": schema.UserMessage("go"),
		})); err != nil {
		t.Fatalf("Resume 应成功, got %v", err)
	}
	if got := ranW.Load(); got != 2 {
		t.Errorf("W 应恰跑 2 次（2 个 Z 实例各投 1 个 W），实际 %d 次\n"+
			"  1 次 = 成功实例的记录被按 NodeID 误清（它已成功、不会重跑重产 → 永久丢）\n"+
			"  3 次 = 重跑实例的旧 send 没清（issue-95#1 双发）", got)
	}
}

// errMultiInstanceCond 是多实例 cond 用的 transient 业务错。
var errMultiInstanceCond = &miCondErr{}

type miCondErr struct{}

func (*miCondErr) Error() string { return "transient cond error on multi-instance node" }

// TestMultiInstance_CondFailedInstanceOnlyRerun 钉「**只有出边算不出来的那个实例重跑**」。
//
// # 这里的语义换过一次，换掉的那半值得写下来
//
// 出边曾经是提交之后的独立相位、按 NodeID 只算一次：cond 读的是 reducer **合并后**的
// channel，报错时无从归因是哪个实例的产出有问题，于是只能把该 NodeID 的**全部实例**一起
// 拉回来重跑，还要额外一套机件把它们已记下的 completed / deferred / memo 精确 purge 掉
// （否则旧 send 重放叠上重跑新产 = Target 双发，即 issue-95#1）。
//
// 出边归成员之后，每个实例**各算各的**、视图只含它自己的写入 ⟹ 归因是精确的 ⟹
//
//   - 只有失败的那个进 FailedBranches、只有它重跑；
//   - 成功的兄弟照常提交写入、记入 completed，**永远不会被拉回来** ⟹ 那套 purge 机件
//     连同它的前提一起删掉了（`mergeCompletedSiblings`）。
//
// # 判别式
//
// **Z 的执行次数**是三选一：2 次 = 兄弟被漏掉（它的活永远没干）；4 次 = 两个实例都重跑了
// （归因没生效，退回按 NodeID 的老语义）；**3 次 = 正确**（首跑 2 次 + 只重跑失败那 1 次）。
//
// **W 的次数**独立守 send 那一维：2 次 = 每个 Z 实例各投一个 W；3 次 = 兄弟的旧 send
// 重放又叠上了重跑新产的（双发）。
func TestMultiInstance_CondFailedInstanceOnlyRerun(t *testing.T) {
	var ranZ, ranW atomic.Int32
	var condCalls atomic.Int32

	b := compose_v2.NewBuilder().WithName("mi_cond_only")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "Z"},
			compose_v2.SendDirective{Target: "Z"},
		)
		return nil, nil
	})
	b.AddNode("Z", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ranZ.Add(1)
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{Target: "W"})
		return nil, nil
	})
	b.AddNode("W", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ranW.Add(1)
		return nil, nil
	})
	b.AddNode("X", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("W", compose_v2.EndNode)
	b.AddEdge("X", compose_v2.EndNode)
	// 两个实例并发跑 ⟹ cond 被调两次；让**第一次**报错，于是恰好一个实例失败、一个成功。
	// 用 atomic 是因为两个实例真并发（`-race` 抓得到裸 int）。
	b.AddConditionalEdge("Z", func(_ context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		if condCalls.Add(1) == 1 {
			return "", errMultiInstanceCond
		}
		return "X", nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))

	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// Run1：Z 两个实例都跑完，其中一个的 cond 报错
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("mico_sid")); err == nil {
		t.Fatal("Run1 应因其中一个实例的 cond 报错而失败")
	}
	if got := ranZ.Load(); got != 2 {
		t.Fatalf("fixture 没搭对：Run1 里 Z 该跑 2 次（两个实例），实际 %d", got)
	}
	cp, _ := cper.LoadNS(ctx, "mico_sid", "")
	if cp == nil {
		t.Fatal("CondFailRerunFrom 应留下可续的存档")
	}
	// **承重 1**：只有失败的那个实例进重跑集合，且带着实例身份
	if len(cp.CurrentFrontier) != 1 || cp.CurrentFrontier[0].InstanceID == "" {
		t.Errorf("只有 cond 算不出出边的那个实例该重跑（且带实例身份），实际 %+v\n"+
			"  2 个 = 归因没生效，退回了「按 NodeID 把全部实例一起拉回来」的老语义",
			cp.CurrentFrontier)
	}
	// **承重 2**：成功的兄弟被完整记下——身份 + 它投的 send + 它当时算好的出边目标
	if len(cp.CompletedFrontier) != 1 {
		t.Errorf("成功的兄弟实例该记入 CompletedFrontier，实际 %+v", cp.CompletedFrontier)
	}
	if len(cp.DeferredSends) != 1 || cp.DeferredSends[0].Target != "W" {
		t.Errorf("兄弟投的 W 该存进 DeferredSends，实际 %+v", cp.DeferredSends)
	}
	if len(cp.CompletedTargets) != 1 || cp.CompletedTargets[0] != "X" {
		t.Errorf("兄弟**当时算好的出边目标**该存下（不能等 Resume 重算），实际 %v\n"+
			"  空 = 兄弟的后继 X 永久丢失、graph 假成功", cp.CompletedTargets)
	}

	// Run2（Resume）：只有失败那个实例重跑
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("mico_sid")); err != nil {
		t.Fatalf("Resume 应跑通, got %v", err)
	}
	// **承重 3**：判别式——3 次才对
	if got := ranZ.Load(); got != 3 {
		t.Errorf("Z 应恰跑 3 次（首跑 2 + 只重跑失败的 1），实际 %d\n"+
			"  2 = 失败那个没重跑（它的活永远没干）；4 = 兄弟被一起拉回来重跑（副作用双跑）", got)
	}
	// **承重 4**：send 那一维不双发
	if got := ranW.Load(); got != 2 {
		t.Errorf("W 应恰跑 2 次（每个 Z 实例各投一个），实际 %d\n"+
			"  3 = 兄弟的旧 send 重放又叠上了重跑新产的（issue-95#1 双发形态）", got)
	}
}

// TestMultiInstance_CondUnregisteredTarget_Terminal 钉 cond 返**未注册 target** 的处置：
// **终局**，不是可续。
//
// 判据是 CLAUDE.md 那条：「这个错重跑一次可能就好，还是重跑必然一样？」——cond 指向一个
// 图里不存在的节点是**图定义错**，重跑必然一样，故 `%w ErrRunAborted`（不重试 + Run 出口
// 清 cp 子树）。放它留 Pending 档的话，auto-load 每次捡起来都在同一处原地再炸一次。
//
// 它曾经返普通 error（于是留下一份永远跑不动的存档），跟 CondFailRerunFrom 混在同一条
// 路径上——两者的诉求其实相反：一个是「上游重产一次可能就好」，一个是「代码写错了」。
func TestMultiInstance_CondUnregisteredTarget_Terminal(t *testing.T) {
	var ranZ atomic.Int32

	b := compose_v2.NewBuilder().WithName("mi_unreg")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "Z"},
			compose_v2.SendDirective{Target: "Z"},
		)
		return nil, nil
	})
	b.AddNode("Z", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ranZ.Add(1)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddConditionalEdge("Z", func(_ context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		return "nope_not_registered", nil
	})

	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	runErr := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("unreg_sid"))
	if runErr == nil {
		t.Fatal("cond 返未注册 target 应失败")
	}
	// **承重 1**：终局——业务据此判「已终局、别再 Resume」
	if !errors.Is(runErr, compose_v2.ErrRunAborted) {
		t.Errorf("cond 返未注册 target 是图定义错、重跑必然一样 ⟹ 应 %%w ErrRunAborted, got %v", runErr)
	}
	// **承重 2**：存档清光——否则 auto-load 捡起来在同一处原地再炸
	if left, _ := cper.LoadAllInSession(ctx, "unreg_sid"); len(left) != 0 {
		t.Errorf("终局失败应清光 cp 子树，残留 %d 份——留着 = 每次 Resume 原地再炸一次", len(left))
	}
	// **承重 3（正向）**：Z 真的跑过——只断言错误的话，「一个节点都没跑」也会绿
	if got := ranZ.Load(); got == 0 {
		t.Error("fixture 没搭对：Z 一次都没跑，那这条测的就不是 cond 的处置")
	}
}
