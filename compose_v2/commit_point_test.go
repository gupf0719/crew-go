package compose_v2_test

// 「成功是一个原子事实」的哨兵。
//
// # 根因与改法
//
// 「一个成员算不算成功」曾经分三处、分三个时刻定：代际推进在节点函数一返回、写入提交在
// 本批执行完、出边计算在提交之后。任何一处之后失败，前面已经定下的就成了**半成功**——
// 中断身份漂移（用户答过的问题被重新问）、节点私有态键位错配、写入按 Append/Sum 双计、
// 已成功成员被放回待执行集合。
//
// 改法是让这三样在**同一个时刻**一起生效或一起不生效：
//
//  1. 出边计算归成员（排在提交之前，视图 = 已提交 + 本成员自己的写入）——算不出来就是
//     这个成员失败；
//  2. 代际推进（visit）挪到提交点；
//  3. 写入提交本来就是原子的。
//
// 本文件钉的是这三条**合起来**的效果，按「哪条路径会把已成功成员放回去重跑」逐条守。

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestCommitPoint_ReducerPanicRerun_IdentityStable 守 **reducer panic 整超步重跑**这条路。
//
// 它是三条回滚路径里唯一「写入被原子回滚、但成员确实跑成功过」的一条：`ApplyAll` 中途
// panic ⟹ 全部写入回滚 ⟹ 整个 frontier（含已成功成员）重跑。
//
// 代际若在「节点函数一返回」就推进，重跑时 visit 已经变了 ⟹ 同一位置算出的中断身份跟
// 上一轮不同 ⟹ **用户答过的问题被重新问**，而调用方攥着的答案永远配不上。
//
// sanity-revert：把 advanceVisitsLocked 里的 visit 自增（channel_manager.go，`cm.visitCount[m.NodeID]++`）挪回 runNodeWithHooks 的成功
// 路径 → ask 节点重跑时 visit 从 0 变 1 → 身份变了 → 本用例的「两轮 ID 相同」FAIL。
func TestCommitPoint_ReducerPanicRerun_IdentityStable(t *testing.T) {
	var boom atomic.Bool
	var ids []string
	var asked atomic.Int32

	// 自定义 reducer：第一次 apply 时 panic 一次。
	//
	// **ask 自己写这个 channel**——它中断那轮的写入根本没到提交（失败成员），所以第一次
	// 真正的 apply 恰好落在「ask 带着答案跑成功」那一轮：节点通过 → 出边算完 → 提交时
	// 炸 → 整超步原子回滚 → 重跑。这正是要守的时序。
	//
	// 不用旁边放一个 boomer 节点：那个节点在中断超步里会成为 completedSibling，
	// Resume 时按 PA-H1 不重跑，第二次 apply 永远不会来（实测）。
	panicky := compose_v2.Custom("__cp_panicky__", func(prev, next string) string {
		if next == "trigger" && boom.CompareAndSwap(false, true) {
			panic("reducer boom")
		}
		return next
	}, "")

	b := compose_v2.NewBuilder().WithName("commit_point_panic")
	compose_v2.WithChannel(b, panicky)
	b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		asked.Add(1)
		site := compose_v2.CurrentInterruptSite(ctx)
		ids = append(ids, site.InterruptID())
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗"}); err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(panicky, "trigger")}, nil
	})
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "cp_panic_sid"

	// Run1：ask 中断，拿到身份
	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid)))
	if !ok {
		t.Fatal("Run1 应中断在 ask")
	}
	id1 := ie.AllQueries()[0].InterruptID

	// Run2：带答案续跑 → ask 成功、boomer 触发 reducer panic → 整超步回滚
	ans := map[string]*schema.Message{id1: schema.UserMessage("批准")}
	err2 := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(ans))
	if err2 == nil {
		t.Fatal("fixture 没搭对：Run2 该因 reducer panic 失败")
	}

	// Run3：panic 已经放过（一次性），整超步重跑
	err3 := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(ans))

	// **承重 1**：ask 在重跑时算出的身份必须跟第一次一样——否则用户手里的答案配不上
	for i, got := range ids {
		if got != ids[0] {
			t.Fatalf("ask 第 %d 次执行算出的中断身份变了：%s vs 首次 %s\n"+
				"  变了 = 代际在「这一步最终没成」的情况下也推进了 ⟹ 用户答过的问题被重新问，\n"+
				"  而调用方攥着的旧 ID 永远配不上。\n"+
				"  sanity-revert: 把 advanceVisitsLocked 的 visit 自增挪回 runNodeWithHooks 成功路径 → 本断言 fail",
				i+1, got, ids[0])
		}
	}
	// **承重 2（正向）**：整条链真的收敛了——只断言「ID 没变」的话，「什么都没跑」也会绿
	if err3 != nil {
		t.Fatalf("panic 放过之后应跑通, got %v", err3)
	}
	if asked.Load() < 2 {
		t.Fatalf("fixture 没搭对：ask 该被重跑（首跑 + 续跑 + 回滚重跑），实际只跑了 %d 次", asked.Load())
	}
}

// TestCommitPoint_CondFailure_IsMemberFailure 守「cond 算不出出边 = **这个成员**失败」。
//
// 三件事一起验，它们是同一个原子事实的三面：
//
//   - 它的**写入没提交**（先前 cond 排在提交之后，写入早就进 channel 了 ⟹ 重跑时
//     Append/Sum 双计，而那笔账当时是记在业务头上的「须保 writes 幂等」）；
//   - 它的**代际没推进**；
//   - 它进的是**失败集合**，于是走既有的部分失败机件（兄弟照常提交 + 记入 completed）。
//
// sanity-revert：out_edges.go 里让 evalOutEdges 吞掉 cond 的错误返 nil → 成员算成功 →
// 写入被提交 → 本用例的「count 恰为 1」FAIL（变成 2）。
func TestCommitPoint_CondFailure_IsMemberFailure(t *testing.T) {
	var condCalls atomic.Int32
	countCh := compose_v2.Sum("__cp_count__")

	b := compose_v2.NewBuilder().WithName("commit_point_cond")
	compose_v2.WithChannel(b, countCh)
	b.AddNode("w", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// 累加型 reducer：写入若被提交过，重跑就会看到 2
		return []compose_v2.ChannelWrite{compose_v2.Write(countCh, 1)}, nil
	})
	tailSeen := -1
	b.AddNode("tail", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		tailSeen, _ = compose_v2.Get(chs, countCh)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "w")
	b.AddEdge("tail", compose_v2.EndNode)
	b.AddConditionalEdge("w", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		if condCalls.Add(1) == 1 {
			return "", errors.New("上游数据还没备齐")
		}
		// **顺带守叠加视图**：cond 必须看得见本成员刚写的那 1（视图 = 已提交 + 自己的写入）
		if n, _ := compose_v2.Get(chs, countCh); n == 0 {
			return "", fmt.Errorf("cond 看不到本成员自己刚写的值（叠加视图没生效）")
		}
		return "tail", nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))

	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "cp_cond_sid"

	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid)); err == nil {
		t.Fatal("Run1 该因 cond 报错失败")
	}
	// **承重 1**：cond 失败那轮的写入**没提交**——存档是 cond 失败之后写的，
	// 若失败成员的写入被提交过，它就已经烤进 cp.Channels 了。
	cp, _ := cper.LoadNS(ctx, sid, "")
	if cp == nil {
		t.Fatal("CondFailRerunFrom 应留下可续的存档")
	}
	if v, ok := cp.Channels["__cp_count__"]; ok {
		t.Errorf("cond 失败那轮的写入不该提交，存档里却有 __cp_count__=%v\n"+
			"  有值 = cond 还排在提交之后 ⟹ 失败成员的写入已进 channel ⟹ 重跑对 Sum/Append 双计\n"+
			"  sanity-revert: 让 evalOutEdges 吞掉 cond 的错误返 nil → 成员算成功 → 写入提交 → 本断言 fail", v)
	}

	// Run2：重跑 w，cond 二次评估通过
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid)); err != nil {
		t.Fatalf("Resume 应跑通, got %v", err)
	}

	// **承重 2（正向）**：tail 真的看到了恰好 1 次累加——只断言「存档里没有」的话，
	// 「整条链什么都没提交」那个退化也会绿。
	if tailSeen != 1 {
		t.Errorf("tail 应看到累加值恰为 1（w 只有重跑那轮的写入被提交），实际 %d\n"+
			"  0 = 什么都没提交；2 = cond 失败那轮的写入也提交了（双计）", tailSeen)
	}
	// cond 恰评估两次：失败一次 + 重跑成功一次
	if got := condCalls.Load(); got != 2 {
		t.Errorf("cond 应恰评估 2 次（失败 1 + 重跑成功 1），实际 %d", got)
	}
}
