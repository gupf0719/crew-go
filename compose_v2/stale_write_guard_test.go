package compose_v2_test

// WithStaleWriteGuard：并发 Run/Resume 同一 sessionID 时，让"后写者静默覆盖先写者"
// 变成一个明确的错误。
//
// 它**不阻止副作用双跑**（检测在写回时，那时两边都执行完了），只把最坏的"两边都
// 报成功、事后无从察觉"变成"至少一方拿到 ErrCheckpointStale"。

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// buildGuardGraph 造一个会中断的图：ask 节点首跑抛 Interrupt，Resume 后放行。
func buildGuardGraph(t *testing.T, ranWork *atomic.Int32) *compose_v2.Graph {
	t.Helper()
	b := compose_v2.NewBuilder().WithName("guard")
	b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "?"}); err != nil {
			return nil, err
		}
		return nil, nil
	})
	b.AddNode("work", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ranWork.Add(1)
		return nil, nil
	})
	// work 之后再中断一次——让 Resume 也走"写回 cp"路径（否则成功即 cleanup 清 cp，
	// 没有写回就没有可比对的写点，测不到守卫）。
	b.AddNode("ask2", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "?"}); err != nil {
			return nil, err
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", "work")
	b.AddEdge("work", "ask2")
	b.AddEdge("ask2", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// TestStaleWriteGuard_SecondWriterOnSameBaselineRejected：两个执行者**基于同一代
// cp** 各自跑完，先写的成功、后写的拿到 ErrCheckpointStale——这是守卫的核心场景。
//
// 用 `WithResumeFrom` 把"两者载入同一代"这件事显式化（等价于两个进程副本几乎同时
// auto-load 到同一份 cp），再让它们**先后**写回。真实并发里两个副本执行耗时不同
// （LLM 调用快慢不一），先后写回是常态。
//
// **真正同时到达写回点的情况本守卫抓不到**——检测是 read-then-write、非原子，
// 两者可能都在对方写入前读到旧值。这是最小档的已知边界，见 WithStaleWriteGuard
// godoc；要消除需要 backend 支持条件写。
//
// sanity-revert：去掉 graph_loop 出口的 staleWriteGuard 比对块 → 后写者也成功，
// 静默覆盖先写者的成果。
func TestStaleWriteGuard_SecondWriterOnSameBaselineRejected(t *testing.T) {
	var ranWork atomic.Int32
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, buildGuardGraph(t, &ranWork),
		compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("base_sid"),
		compose_v2.WithStaleWriteGuard()))
	if !ok {
		t.Fatal("Run1 应中断在 ask")
	}
	baseline, _ := cper.LoadNS(ctx, "base_sid", "")
	if baseline == nil || baseline.Epoch == "" {
		t.Fatalf("cp 应带 Epoch, got %+v", baseline)
	}

	ans := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("go")}
	runFrom := func(cp *compose_v2.Checkpoint) error {
		// 传副本，避免两个 Run 共享同一个 *Checkpoint 对象被就地改写
		dup := *cp
		return compose_v2.Run(ctx, buildGuardGraph(t, &ranWork), compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("base_sid"),
			compose_v2.WithResumeFrom(&dup), compose_v2.WithResumeAnswers(ans),
			compose_v2.WithStaleWriteGuard())
	}

	// 执行者 A：基于 baseline 跑完并写回（槽里换新一代）
	errA := runFrom(baseline)
	if errors.Is(errA, compose_v2.ErrCheckpointStale) {
		t.Fatalf("先写者不该被拒: %v", errA)
	}
	// 执行者 B：**手里还是同一代 baseline**，跑完写回时应发现槽已被推进
	errB := runFrom(baseline)
	if !errors.Is(errB, compose_v2.ErrCheckpointStale) {
		t.Errorf("后写者应拿到 ErrCheckpointStale, got %v\n"+
			"  没有 = 它静默覆盖了先写者的成果，两边都以为自己成功了", errB)
	}
	t.Logf("work 执行了 %d 次——守卫不阻止副作用双跑，只让冲突可见", ranWork.Load())
}

// TestStaleWriteGuard_DefaultOffUnchanged：不开 option 时行为与引入前完全一致。
//
// **用与 T1 相同的确定性 fixture**（同基线两执行者先后写回），只是不开守卫——这样
// 它是个真正的 positive control：不仅断言"没报 stale"，还断言**后写者的结果真的
// 落进了槽**（读回比 Epoch）。
//
// 原版只有"不返回 stale"这个否定断言，守卫坏成什么样它都绿（变异测试实证：删掉整个
// 比对块它照样 PASS）；并发版则因 Run 体太短常被 scheduler 串行化而 flaky。
func TestStaleWriteGuard_DefaultOffUnchanged(t *testing.T) {
	var ranWork atomic.Int32
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, buildGuardGraph(t, &ranWork),
		compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("off_sid")))
	if !ok {
		t.Fatal("Run1 应中断")
	}
	baseline, _ := cper.LoadNS(ctx, "off_sid", "")
	if baseline == nil || baseline.Epoch == "" {
		t.Fatal("即使不开守卫，Epoch 也该照常写入（业务 backend 序列化它即可，不必理解语义）")
	}

	ans := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("go")}
	runFrom := func(cp *compose_v2.Checkpoint) error {
		dup := *cp
		return compose_v2.Run(ctx, buildGuardGraph(t, &ranWork), compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("off_sid"),
			compose_v2.WithResumeFrom(&dup), compose_v2.WithResumeAnswers(ans))
		// 注意：**不开** WithStaleWriteGuard
	}
	if errA := runFrom(baseline); errors.Is(errA, compose_v2.ErrCheckpointStale) {
		t.Fatalf("守卫默认关闭，不该有 stale 错误: %v", errA)
	}
	afterA, _ := cper.LoadNS(ctx, "off_sid", "")

	// 后写者：同一老基线。默认关闭时它必须**成功覆盖**——这正是引入守卫前的行为
	if errB := runFrom(baseline); errors.Is(errB, compose_v2.ErrCheckpointStale) {
		t.Fatalf("守卫默认关闭，后写者不该被拒: %v", errB)
	}
	afterB, _ := cper.LoadNS(ctx, "off_sid", "")
	if afterB == nil || afterB.Epoch == afterA.Epoch {
		t.Errorf("默认关闭时后写者应静默覆盖先写者（引入前的行为）——槽里 Epoch 该变，"+
			"A 后=%v B 后=%v", afterA.Epoch, afterB.Epoch)
	}
}

// TestStaleWriteGuard_SingleRunUnaffected：没有并发时，开着守卫也必须一路畅通
// （守卫只在"槽被别人推进过"时拦，不能把正常的多轮 Resume 拦掉）。
//
// **它守的是"比对方向不能反"**——默认 Exit 档每个 Run 只有出口一次写，基线每轮由
// auto-load 重新建立，所以"写后更新基线"那条路径在这里根本不起作用（变异实证：
// 删掉写后更新基线，本条照绿）。守那条的是 WorksUnderSyncDurability。
func TestStaleWriteGuard_SingleRunUnaffected(t *testing.T) {
	var ranWork atomic.Int32
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	opts := func(extra ...compose_v2.RunOption) []compose_v2.RunOption {
		return append([]compose_v2.RunOption{
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("solo_sid"),
			compose_v2.WithStaleWriteGuard(),
		}, extra...)
	}

	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, buildGuardGraph(t, &ranWork),
		compose_v2.NewChannelManager(), opts()...))
	if !ok {
		t.Fatal("Run1 应中断在 ask")
	}
	// 第 2 轮：答完第一个问题，跑到 ask2 再中断——这一轮自己写过 cp（换了 Epoch）
	err2 := compose_v2.Run(ctx, buildGuardGraph(t, &ranWork), compose_v2.NewChannelManager(),
		opts(compose_v2.WithResumeAnswers(map[string]*schema.Message{
			ie.AllQueries()[0].InterruptID: schema.UserMessage("go"),
		}))...)
	ie2, ok := compose_v2.IsInterrupt(err2)
	if !ok {
		t.Fatalf("Resume1 应中断在 ask2, got %v", err2)
	}
	// 第 3 轮：答完第二个问题跑完。它载入的是第 2 轮写的那一代，必须通过
	err3 := compose_v2.Run(ctx, buildGuardGraph(t, &ranWork), compose_v2.NewChannelManager(),
		opts(compose_v2.WithResumeAnswers(map[string]*schema.Message{
			ie.AllQueries()[0].InterruptID:  schema.UserMessage("go"),
			ie2.AllQueries()[0].InterruptID: schema.UserMessage("go2"),
		}))...)
	if err3 != nil {
		t.Fatalf("串行多轮 Resume 不该被守卫拦住（每轮载入的都是上一轮写的那代）, got %v", err3)
	}
	if got := ranWork.Load(); got != 1 {
		t.Errorf("串行执行 work 应恰跑 1 次, 实际 %d", got)
	}
}

// TestStaleWriteGuard_WorksUnderSyncDurability：`Durability=Sync` 每超步都写 cp，
// 守卫必须在**那些写点**也生效，且不能把本 Run 自己写的判成别人写的。
//
// 两件事一起验：
//   - 串行跑不误报（自己每超步写完更新基线，出口比对是"自己 vs 自己"必须通过）
//   - 同基线的第二个执行者仍被拒（中途写点没把守卫洗掉）
//
// sanity-revert：把 guardedSaveNS 里的比对块去掉，或让 saveStepCheckpoint 绕开它
// 直接 SaveNS → 第二个执行者不再被拒（中途写覆盖别人后，出口比对变成自己跟自己比）。
func TestStaleWriteGuard_WorksUnderSyncDurability(t *testing.T) {
	var ranWork atomic.Int32
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	base := func(extra ...compose_v2.RunOption) []compose_v2.RunOption {
		return append([]compose_v2.RunOption{
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("sync_sid"),
			compose_v2.WithStaleWriteGuard(),
			compose_v2.WithDurability(compose_v2.DurabilitySync), // 每超步写
		}, extra...)
	}

	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, buildGuardGraph(t, &ranWork),
		compose_v2.NewChannelManager(), base()...))
	if !ok {
		t.Fatal("Run1 应中断在 ask")
	}
	baseline, _ := cper.LoadNS(ctx, "sync_sid", "")
	if baseline == nil || baseline.Epoch == "" {
		t.Fatalf("Sync 档每超步写也必须带 Epoch（否则守卫被洗成空、静默失效）, got %+v", baseline)
	}

	ans := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("go")}
	runFrom := func(cp *compose_v2.Checkpoint) error {
		dup := *cp
		return compose_v2.Run(ctx, buildGuardGraph(t, &ranWork), compose_v2.NewChannelManager(),
			base(compose_v2.WithResumeFrom(&dup), compose_v2.WithResumeAnswers(ans))...)
	}

	// 执行者 A：跨多个超步、每步都写——不能自己把自己判成 stale
	if errA := runFrom(baseline); errors.Is(errA, compose_v2.ErrCheckpointStale) {
		t.Fatalf("串行执行不该被守卫拦住——本 Run 每超步写完应更新自己的基线, got %v", errA)
	}
	// 执行者 B：手里还是同一代，必须被拒
	if errB := runFrom(baseline); !errors.Is(errB, compose_v2.ErrCheckpointStale) {
		t.Errorf("Sync 档下第二个执行者也该拿到 ErrCheckpointStale, got %v\n"+
			"  没有 = 中途超步写把别人的 cp 覆盖了，出口比对变成自己跟自己比、恒通过", errB)
	}
}

// hijackSlot 在节点内模拟"另一个执行者推进了这个槽"——读出当前 cp、换个 Epoch 写回。
// 用它把并发写造成确定性事件，不依赖 goroutine 调度。
func hijackSlot(t *testing.T, cper compose_v2.Checkpointer, sid, ns string) {
	t.Helper()
	cur, err := cper.LoadNS(context.Background(), sid, ns)
	if err != nil || cur == nil {
		t.Fatalf("劫持槽失败: cp=%v err=%v", cur, err)
	}
	dup := *cur
	dup.Epoch = "epoch_from_other_writer"
	if err := cper.SaveNS(context.Background(), sid, ns, &dup); err != nil {
		t.Fatalf("劫持写入失败: %v", err)
	}
}

// TestStaleWriteGuard_AutoLoadPathDetects：**auto-load 是业务默认路径**（同 sid
// 直接再 Run 一次，不传 WithResumeFrom），守卫在这条路径上必须同样生效。
//
// 变异测试暴露的洞：原有测试全走 `WithResumeFrom`，删掉 auto-load 分支的基线登记
// 后**四条全绿**——默认路径的正向检测当时是零覆盖。
//
// sanity-revert：删 graph_loop auto-load 分支里的 `cm.setCpEpochBaseline(rootCp.Epoch)`
// → 基线恒空 → 守卫全程跳过 → 本条 FAIL。
func TestStaleWriteGuard_AutoLoadPathDetects(t *testing.T) {
	var ranWork atomic.Int32
	var hijacked atomic.Bool
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "autoload_sid"

	build := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("autoload")
		b.AddNode("ask", func(c context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if _, err := compose_v2.Ask(c, &compose_v2.AskQuery{Question: "?"}); err != nil {
				return nil, err
			}
			return nil, nil
		})
		b.AddNode("work", func(c context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			ranWork.Add(1)
			// 本 Run 执行期间，另一个执行者推进了槽
			if hijacked.CompareAndSwap(false, true) {
				hijackSlot(t, cper, sid, "")
			}
			return nil, nil
		})
		b.AddNode("ask2", func(c context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if _, err := compose_v2.Ask(c, &compose_v2.AskQuery{Question: "?"}); err != nil {
				return nil, err
			}
			return nil, nil
		})
		b.AddEdge(compose_v2.StartNode, "ask")
		b.AddEdge("ask", "work")
		b.AddEdge("work", "ask2")
		b.AddEdge("ask2", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		return g
	}

	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, build(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithStaleWriteGuard()))
	if !ok {
		t.Fatal("Run1 应中断在 ask")
	}

	// Run2 **不传 WithResumeFrom**，走 auto-load
	err := compose_v2.Run(ctx, build(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithStaleWriteGuard(),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			ie.AllQueries()[0].InterruptID: schema.UserMessage("go"),
		}))
	if !errors.Is(err, compose_v2.ErrCheckpointStale) {
		t.Errorf("auto-load 路径也该检出 stale, got %v\n"+
			"  没有 = 该路径没登记基线，守卫对业务默认调用方式静默失效", err)
	}
}

// TestStaleWriteGuard_SuccessPurgeAlsoGuarded：**删 cp 也要过守卫**。
//
// 陈旧执行者一路跑到成功会走 DeleteSubtree 清理——删掉的可能是另一个执行者的 HITL
// 存档，它的用户答完回来会发现会话凭空消失（比覆盖更不可逆）。
//
// sanity-revert：去掉成功路径 purgeExitSlot 里删除前的 checkStale → 本条 FAIL（err=nil
// 且槽被清空）。
func TestStaleWriteGuard_SuccessPurgeAlsoGuarded(t *testing.T) {
	var ranWork atomic.Int32
	var hijacked atomic.Bool
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "purge_sid"

	build := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("purge")
		b.AddNode("ask", func(c context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if _, err := compose_v2.Ask(c, &compose_v2.AskQuery{Question: "?"}); err != nil {
				return nil, err
			}
			return nil, nil
		})
		b.AddNode("work", func(c context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			ranWork.Add(1)
			if hijacked.CompareAndSwap(false, true) {
				hijackSlot(t, cper, sid, "")
			}
			return nil, nil
		})
		b.AddEdge(compose_v2.StartNode, "ask")
		b.AddEdge("ask", "work")
		b.AddEdge("work", compose_v2.EndNode) // 跑到成功 → 触发 DeleteSubtree
		g, err := b.Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		return g
	}

	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, build(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithStaleWriteGuard()))
	if !ok {
		t.Fatal("Run1 应中断")
	}

	err := compose_v2.Run(ctx, build(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithStaleWriteGuard(),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			ie.AllQueries()[0].InterruptID: schema.UserMessage("go"),
		}))
	if !errors.Is(err, compose_v2.ErrCheckpointStale) {
		t.Errorf("陈旧执行者跑到成功时，清理也该被守卫拦住, got %v\n"+
			"  没有 = 它把别人的 cp 删了还返回成功——守卫想消灭的静默的最纯形态", err)
	}
	// 别人的 cp 必须还在
	if cur, _ := cper.LoadNS(ctx, sid, ""); cur == nil {
		t.Error("被拦住时不该删——别人的 cp 已经没了")
	}
}

// TestStaleWriteGuard_InheritedBySubgraph：**嵌套子图必须继承守卫**。
//
// 子图 Run 各写自己的 `(sid, childNS)` 槽。守卫不透传的话，父层报 stale 说"本次结果
// 未落盘"，但子 ns 的 cp 早在父节点执行期间就被本次 Run 覆盖了——父槽留着别人的状态、
// 子槽是我的，**父子 cp 撕裂**。
//
// 继承走 ctx（同 durability 范式），因为 subgraph 组 subOpts 时不透传业务的 RunOption。
//
// sanity-revert：去掉 graph_loop 里的 `ctx = WithStaleWriteGuardCtx(ctx)` → 子图不
// 继承 → 子槽被静默覆盖，本条 FAIL。
func TestStaleWriteGuard_InheritedBySubgraph(t *testing.T) {
	var hijacked atomic.Bool
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "subg_sid"
	const childNS = "outer"

	// 子图：中断一次，让它有自己的 cp
	subB := compose_v2.NewBuilder().WithName("inner")
	subB.AddNode("sask", func(c context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, err := compose_v2.Ask(c, &compose_v2.AskQuery{Question: "?"}); err != nil {
			return nil, err
		}
		// Resume 到这里时，另一个执行者已推进了子槽
		if hijacked.CompareAndSwap(false, true) {
			hijackSlot(t, cper, sid, childNS)
		}
		return nil, nil
	})
	subB.AddEdge(compose_v2.StartNode, "sask")
	subB.AddEdge("sask", compose_v2.EndNode)
	subG, err := subB.Compile()
	if err != nil {
		t.Fatalf("compile sub: %v", err)
	}

	build := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("outerg")
		b.AddSubgraphNode(childNS, subG)
		b.AddEdge(compose_v2.StartNode, childNS)
		b.AddEdge(childNS, compose_v2.EndNode)
		g, cerr := b.Compile()
		if cerr != nil {
			t.Fatalf("compile outer: %v", cerr)
		}
		return g
	}

	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, build(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithStaleWriteGuard()))
	if !ok {
		t.Fatal("Run1 应因子图中断而中断")
	}
	before, _ := cper.LoadNS(ctx, sid, childNS)
	if before == nil {
		t.Fatal("子图应落自己的 cp")
	}

	err2 := compose_v2.Run(ctx, build(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithStaleWriteGuard(),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			ie.AllQueries()[0].InterruptID: schema.UserMessage("go"),
		}))
	if !errors.Is(err2, compose_v2.ErrCheckpointStale) {
		t.Errorf("子图应继承守卫并检出 stale, got %v", err2)
	}
	// 子槽必须还是劫持者写的那代——没被本次 Run 覆盖
	after, _ := cper.LoadNS(ctx, sid, childNS)
	if after == nil || after.Epoch != "epoch_from_other_writer" {
		t.Errorf("子槽被本次 Run 覆盖了（父子撕裂）——期望仍是劫持者那代, got %+v", after)
	}
}

// TestStaleWriteGuard_WorksUnderAsyncDurability：Async 档是**唯一跨 goroutine 更新
// 基线**的档位（后台 worker 写完 set 基线、主线程出口读），锁死它不出 race 也不失效。
//
// join-before-cleanup 保证出口比对时后台写已落地。
func TestStaleWriteGuard_WorksUnderAsyncDurability(t *testing.T) {
	var ranWork atomic.Int32
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	base := func(extra ...compose_v2.RunOption) []compose_v2.RunOption {
		return append([]compose_v2.RunOption{
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("async_sid"),
			compose_v2.WithStaleWriteGuard(),
			compose_v2.WithDurability(compose_v2.DurabilityAsync),
		}, extra...)
	}

	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, buildGuardGraph(t, &ranWork),
		compose_v2.NewChannelManager(), base()...))
	if !ok {
		t.Fatal("Run1 应中断在 ask")
	}
	baseline, _ := cper.LoadNS(ctx, "async_sid", "")
	if baseline == nil || baseline.Epoch == "" {
		t.Fatalf("Async 档写的 cp 也必须带 Epoch, got %+v", baseline)
	}

	ans := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("go")}
	runFrom := func(cp *compose_v2.Checkpoint) error {
		dup := *cp
		return compose_v2.Run(ctx, buildGuardGraph(t, &ranWork), compose_v2.NewChannelManager(),
			base(compose_v2.WithResumeFrom(&dup), compose_v2.WithResumeAnswers(ans))...)
	}
	if errA := runFrom(baseline); errors.Is(errA, compose_v2.ErrCheckpointStale) {
		t.Fatalf("串行执行不该被拦——后台 worker 写完应更新自己的基线, got %v", errA)
	}
	if errB := runFrom(baseline); !errors.Is(errB, compose_v2.ErrCheckpointStale) {
		t.Errorf("Async 档下第二个执行者也该被拒, got %v", errB)
	}
}

// TestStaleWriteGuard_InterruptNotReportedWhenNotPersisted：中断出口撞上陈旧写回、档没写成时，
// **不再报告成中断**，而槽里那份属于别人的档原样留着。
//
// 档没写成就不能报告成可以续：曾经把 stale 跟中断一起 `%w`，`IsInterrupt` 仍为真，上层照常请用户
// 回答，答完续进的是另一个执行者的状态——重新问一遍已经答过的问题，来回不收敛（agui / a2a 当时各写
// 了一条排除逻辑绕开它）。现在返 ErrCheckpointNotPersisted：stale 照样 errors.Is 得出来，中断不在
// 错误链上；槽属于别人，不带 ErrRunAborted、不删那份档。
func TestStaleWriteGuard_InterruptNotReportedWhenNotPersisted(t *testing.T) {
	var ranWork atomic.Int32
	var hijacked atomic.Bool
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "sem_sid"

	build := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("sem")
		b.AddNode("ask", func(c context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if _, err := compose_v2.Ask(c, &compose_v2.AskQuery{Question: "?"}); err != nil {
				return nil, err
			}
			return nil, nil
		})
		b.AddNode("work", func(c context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			ranWork.Add(1)
			if hijacked.CompareAndSwap(false, true) {
				hijackSlot(t, cper, sid, "")
			}
			return nil, nil
		})
		// 再中断一次：Run2 的出口错误是 InterruptError，且槽已被劫持 → 两个条件同时成立
		b.AddNode("ask2", func(c context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if _, err := compose_v2.Ask(c, &compose_v2.AskQuery{Question: "?"}); err != nil {
				return nil, err
			}
			return nil, nil
		})
		b.AddEdge(compose_v2.StartNode, "ask")
		b.AddEdge("ask", "work")
		b.AddEdge("work", "ask2")
		b.AddEdge("ask2", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		return g
	}

	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, build(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithStaleWriteGuard()))
	if !ok {
		t.Fatal("Run1 应中断")
	}

	err := compose_v2.Run(ctx, build(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithStaleWriteGuard(),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			ie.AllQueries()[0].InterruptID: schema.UserMessage("go"),
		}))

	if !errors.Is(err, compose_v2.ErrCheckpointStale) {
		t.Errorf("应能判出 stale（本次结果没落盘）, got %v", err)
	}
	if !errors.Is(err, compose_v2.ErrCheckpointNotPersisted) {
		t.Errorf("应能判出 ErrCheckpointNotPersisted, got %v", err)
	}
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Errorf("档没写成，不该再报告成中断（上层会请用户回答一个续不上的问题）, got %v", err)
	}
	if errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("槽属于别的执行者，不该带 ErrRunAborted（上层出口会跟着清档）, got %v", err)
	}
	cur, loadErr := cper.LoadNS(ctx, sid, "")
	if loadErr != nil || cur == nil || cur.Epoch != "epoch_from_other_writer" {
		t.Errorf("别的执行者写下的档应原样留着, got cp=%v err=%v", cur, loadErr)
	}
}

// TestStaleWriteGuard_AllPathsAreErrorsIsDetectable：**三条 stale 路径的错误都必须
// 能被 errors.Is 判出来**。
//
// 守卫有三个判定点（写回 / 成功清理前 / 终局失败清理前），三处的 wrap 方式一度不一致
// ——终局失败那条用 `%v` 包 staleErr，导致 `errors.Is(err, ErrCheckpointStale)` 恒
// false。业务照文档写 `if errors.Is(err, ErrCheckpointStale) { 补偿 }` 在那条路径直接
// 漏判，而它恰恰是"我基于过时状态跑出了终局失败"——最该告警的一种。
//
// 这里直接钉 wrap 语义（不走完整 Run，因为要覆盖的是错误构造而非执行路径）。
func TestStaleWriteGuard_AllPathsAreErrorsIsDetectable(t *testing.T) {
	stale := fmt.Errorf("%w (probe)", compose_v2.ErrCheckpointStale)

	cases := []struct {
		name string
		err  error
		// 除 stale 外还必须能判出的原始语义（nil = 只需判 stale）
		alsoIs error
	}{
		// 写回被拒那条走 ErrCheckpointNotPersisted，由 TestStaleWriteGuard_InterruptNotReportedWhenNotPersisted
		// 走完整 Run 钉住。
		{
			name: "成功清理被拒（本次成功降级为 stale）",
			err:  stale,
		},
		{
			name:   "终局失败清理被拒（ErrRunAborted 与 stale 都要判得出）",
			err:    fmt.Errorf("%w (跳过清理 cp: %w)", compose_v2.ErrRunAborted, stale),
			alsoIs: compose_v2.ErrRunAborted,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if !errors.Is(c.err, compose_v2.ErrCheckpointStale) {
				t.Errorf("errors.Is(err, ErrCheckpointStale) 必须为 true——业务靠它决定要不要补偿, got %v", c.err)
			}
			if c.alsoIs != nil && !errors.Is(c.err, c.alsoIs) {
				t.Errorf("原始语义也必须保住（%v）, got %v", c.alsoIs, c.err)
			}
		})
	}
}

// TestStaleWriteGuard_FirstRoundOfSessionProtected：**会话第一轮也要受保护**。
//
// 两个副本同时收到同一个**新** sid（消息重复消费 / 客户端重试 / 多副本同投）时，
// 双方 auto-load 都拿到 nil。若把"载入到空槽"和"无从比对"合并成一个放行，双方谁也
// 发现不了对方——而这恰是 `WithStaleWriteGuard` godoc 举的头号场景的第一轮。
//
// 关键区分：**"我载入时槽是空的"本身就是一个可比对的基线**——槽后来变非空，就说明
// 有人在我执行期间创建了它。
//
// 时序用屏障固定（B 先载入 → A 完整写入 → B 才写回）：真并发下两者可能都在对方写入
// 前完成检测，那是已知的非原子窗口、本守卫不保证；能保证的是先后到达写回点这一档，
// 而真实并发里两副本执行耗时不同、先后写回才是常态。
//
// sanity-revert：把 checkStale 的 `baseline == "" && !slotWasEmpty` 改回
// `baseline == ""` → B 不再检出，它的存档静默覆盖 A 的。
func TestStaleWriteGuard_FirstRoundOfSessionProtected(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	aDone := make(chan struct{})
	bLoaded := make(chan struct{}) // B 完成 auto-load 的真信号

	// **图名必须一致**：两个执行者是同一张图的两个副本。名字不同会先撞
	// ErrGraphIDConflict（那是另一条守卫），根本走不到世代比对。
	mk := func(name string, waitForA bool) *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("first_round")
		b.AddNode("ask", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if waitForA {
				// **进到节点 = auto-load 已完成**，这是唯一可靠的同步时点。
				// 别用"轮询槽直到非空"当屏障——此刻 A 还没跑、槽必然为空，那种循环
				// 空转到上限就落下去，根本没同步到任何东西（实测整包跑 5/5 挂）。
				close(bLoaded)
				<-aDone // 等 A 写完再往下走
			}
			return nil, &compose_v2.InterruptError{
				Reason: name, Query: &compose_v2.AskQuery{InterruptID: "__fr_q__", Question: "?"},
			}
		})
		b.AddEdge(compose_v2.StartNode, "ask")
		b.AddEdge("ask", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		return g
	}
	run := func(g *compose_v2.Graph) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("first_sid"),
			compose_v2.WithStaleWriteGuard())
	}

	errBCh := make(chan error, 1)
	go func() { errBCh <- run(mk("B", true)) }()
	<-bLoaded // B 已 auto-load 到空槽，基线 = 空槽态

	errA := run(mk("A", false)) // A 完整跑完并写入
	if errors.Is(errA, compose_v2.ErrCheckpointStale) {
		t.Fatalf("A 是这个槽的第一个写入者，不该被拒: %v", errA)
	}
	close(aDone) // 放 B 走到写回

	errB := <-errBCh
	if !errors.Is(errB, compose_v2.ErrCheckpointStale) {
		t.Errorf("B 载入时槽为空、写回时已被 A 创建 → 应检出 stale, got %v\n"+
			"  没有 = 会话第一轮完全裸奔，B 的存档静默覆盖 A 的", errB)
	}
}

// TestStaleWriteGuard_ResumeFromIntoEmptySlot：`WithResumeFrom` 把 cp 恢复到一个
// **空槽**（fork / 从归档恢复到新 sid）时，守卫**不能误判 stale**。
//
// 这条守的是 fail-closed 误报——比漏报更糟：写回被拒 ⟹ 存档根本没落盘，而 caller
// 拿到的是 InterruptError（去问用户），答完 Resume 撞 `ErrNoPendingCheckpoint`，
// **fork 出的会话永久不可 Resume**。全程只有一个执行者。
//
// 根因是「载入时槽是空的」这个状态只在 auto-load 分支被标记，`WithResumeFrom` 路径
// 绕开它 ⟹ `slotWasEmpty` 恒 false ⟹ 守卫看到「基线非空 + 槽为空」判成"有人删过"。
//
// sanity-revert：删 graph_loop resumeFrom 分支里的 `markCpSlotEmpty()` 探测 →
// 本条 FAIL（报 stale 且存档没写）。
func TestStaleWriteGuard_ResumeFromIntoEmptySlot(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	var ranWork atomic.Int32

	// 原会话跑到中断，产出可归档的 cp
	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, buildGuardGraph(t, &ranWork),
		compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("fork_orig")))
	if !ok {
		t.Fatal("原会话应中断")
	}
	archived, _ := cper.LoadNS(ctx, "fork_orig", "")
	if archived == nil {
		t.Fatal("应有归档 cp")
	}

	// fork 到全新 sid：目标槽本来就是空的
	dup := *archived
	err := compose_v2.Run(ctx, buildGuardGraph(t, &ranWork), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("fork_target"),
		compose_v2.WithResumeFrom(&dup), compose_v2.WithStaleWriteGuard(),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			ie.AllQueries()[0].InterruptID: schema.UserMessage("go"),
		}))
	if errors.Is(err, compose_v2.ErrCheckpointStale) {
		t.Fatalf("误判 stale——全程只有一个执行者、目标槽本来就空: %v", err)
	}
	// 二次中断的存档必须真的落盘，否则 caller 答完就 Resume 不了了
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		cp, _ := cper.LoadNS(ctx, "fork_target", "")
		if cp == nil {
			t.Error("拿到 InterruptError 但存档没落盘——caller 答完 Resume 会撞 " +
				"ErrNoPendingCheckpoint，会话永久不可恢复")
		}
	}
}

// flakyLoadCheckpointer 让指定序号的 LoadNS 报错，其余透传——模拟一次读抖动。
type flakyLoadCheckpointer struct {
	compose_v2.Checkpointer
	failNth int32 // 第几次 LoadNS 失败（1-based）
	calls   atomic.Int32
}

func (f *flakyLoadCheckpointer) LoadNS(ctx context.Context, sid, ns string) (*compose_v2.Checkpoint, error) {
	if f.calls.Add(1) == f.failNth {
		return nil, errors.New("transient backend read error")
	}
	return f.Checkpointer.LoadNS(ctx, sid, ns)
}

// TestStaleWriteGuard_ProbeReadFailure_FailsOpen：探测槽状态的那次 LoadNS 报错时，
// 守卫必须 **fail-open**，不能把上面那条刚修好的 bug 原样复现。
//
// `WithResumeFrom` 路径要单独探一次"槽当前空不空"。探不到时**不能倒向任何一边**：
//   - 当成"空" → 普通 Resume 在读抖动时会误判成"载入时空、现在非空"，砸掉最常见路径
//   - 当成"非空"（维持默认）→ 槽真为空时报"别人删了"，**一次读抖动就够**让 fork
//     出的会话拿到 stale + 存档不落盘 + 永久不可 Resume
//
// 所以标第三态 unknown，由 checkStale 在 `cur == nil` 分支放行——与它自己读失败时
// 同款处置，也是 checkpoint-storage.md 白纸黑字的契约（守卫是护栏不是主链路）。
//
// sanity-revert：把 graph_loop 探测里的 `markCpSlotUnknown()` 删掉（回到 err 时
// 什么都不标）→ 本条 FAIL 并报 stale。
func TestStaleWriteGuard_ProbeReadFailure_FailsOpen(t *testing.T) {
	inner := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	var ranWork atomic.Int32

	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, buildGuardGraph(t, &ranWork),
		compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(inner), compose_v2.WithSessionID("probe_orig")))
	if !ok {
		t.Fatal("原会话应中断")
	}
	archived, _ := inner.LoadNS(ctx, "probe_orig", "")
	if archived == nil {
		t.Fatal("应有归档 cp")
	}

	// 第 1 次 LoadNS 正是探测那次（fork 目标槽为空）
	cper := &flakyLoadCheckpointer{Checkpointer: inner, failNth: 1}
	dup := *archived
	err := compose_v2.Run(ctx, buildGuardGraph(t, &ranWork), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("probe_target"),
		compose_v2.WithResumeFrom(&dup), compose_v2.WithStaleWriteGuard(),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			ie.AllQueries()[0].InterruptID: schema.UserMessage("go"),
		}))
	if errors.Is(err, compose_v2.ErrCheckpointStale) {
		t.Fatalf("探测读抖动一次就被判 stale——全程只有一个执行者，守卫是护栏不是主链路: %v", err)
	}
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		cp, _ := inner.LoadNS(ctx, "probe_target", "")
		if cp == nil {
			t.Error("拿到 InterruptError 但存档没落盘——一次读抖动就让会话永久不可 Resume")
		}
	}
}

// TestStaleWriteGuard_ProbeReadFailure_StillDetectsRealConflict：探测失败不能把守卫
// 整个关掉——`cur != nil` 那一维仍要照常比 Epoch。
//
// unknown 态只在"槽为空"这一个分支放行（那时无法区分"本来就空"和"别人删了"）。
// 槽里有东西时基线是明确的（caller 给的那代），该拦还得拦。
func TestStaleWriteGuard_ProbeReadFailure_StillDetectsRealConflict(t *testing.T) {
	inner := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	var ranWork atomic.Int32

	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, buildGuardGraph(t, &ranWork),
		compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(inner), compose_v2.WithSessionID("conflict_orig")))
	if !ok {
		t.Fatal("原会话应中断")
	}
	archived, _ := inner.LoadNS(ctx, "conflict_orig", "")

	// 目标槽里已经有**别人**的 cp（epoch 与 archived 那代不同）
	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, buildGuardGraph(t, &ranWork),
		compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(inner), compose_v2.WithSessionID("conflict_target"))); !ok {
		t.Fatal("占位会话应中断")
	}

	cper := &flakyLoadCheckpointer{Checkpointer: inner, failNth: 1} // 探测那次失败
	dup := *archived
	err := compose_v2.Run(ctx, buildGuardGraph(t, &ranWork), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("conflict_target"),
		compose_v2.WithResumeFrom(&dup), compose_v2.WithStaleWriteGuard(),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			ie.AllQueries()[0].InterruptID: schema.UserMessage("go"),
		}))
	if !errors.Is(err, compose_v2.ErrCheckpointStale) {
		t.Errorf("槽里是别人的 cp、基线是 caller 给的那代 → 仍应检出 stale，got %v\n"+
			"  unknown 态只该在'槽为空'那一个分支放行，不是把守卫整个关掉", err)
	}
}

// TestStaleWriteGuard_ProbeReadFailure_NormalResumeNotMisjudged：探测读抖动时把槽
// **当成空**同样会出事——这条钉的是三态相对"二态 + 读失败当空槽"的差异点。
//
// 普通 `WithResumeFrom`（槽里就是我要恢复的那份）遇到探测读失败：
//   - 当成"空槽" → 写回时 `cur != nil` + slotWasEmpty ⟹ 报"载入时槽为空，现已被
//     写入 — 另一个执行者在本次 Run 期间创建了它" ⟹ **最常见的 Resume 路径被一次
//     读抖动砸掉**
//   - 标 unknown → `cur != nil` 分支照常比 Epoch，基线就是 caller 给的那代 ⟹ 匹配
//
// sanity-revert：把 graph_loop 探测里的 `markCpSlotUnknown()` 换成 `markCpSlotEmpty()`
// → 本条 FAIL 并报 stale。
func TestStaleWriteGuard_ProbeReadFailure_NormalResumeNotMisjudged(t *testing.T) {
	inner := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	var ranWork atomic.Int32

	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, buildGuardGraph(t, &ranWork),
		compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(inner), compose_v2.WithSessionID("normal_sid")))
	if !ok {
		t.Fatal("原会话应中断")
	}
	saved, _ := inner.LoadNS(ctx, "normal_sid", "")
	if saved == nil {
		t.Fatal("应有存档")
	}

	// 同一个 sid 上 resume 它自己的那份 cp——槽非空、基线正是槽里那代
	cper := &flakyLoadCheckpointer{Checkpointer: inner, failNth: 1} // 探测那次失败
	dup := *saved
	err := compose_v2.Run(ctx, buildGuardGraph(t, &ranWork), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("normal_sid"),
		compose_v2.WithResumeFrom(&dup), compose_v2.WithStaleWriteGuard(),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			ie.AllQueries()[0].InterruptID: schema.UserMessage("go"),
		}))
	if errors.Is(err, compose_v2.ErrCheckpointStale) {
		t.Fatalf("普通 Resume 被一次探测读抖动判成 stale: %v\n"+
			"  把读失败当'空槽'会让写回时撞上'载入时槽为空，现已被写入'——"+
			"这是最常见的 Resume 路径，不能因为护栏读了一次失败就砸掉", err)
	}
}

// buildNoInterruptGraph 是个跑完就结束、不中断的最小图（用来触发 purge-success 路径）。
func buildNoInterruptGraph(t *testing.T) *compose_v2.Graph {
	t.Helper()
	b := compose_v2.NewBuilder()
	done := compose_v2.LastValue[string]("__purge_done__")
	compose_v2.WithChannel(b, done)
	b.AddNode("work", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(done, "ok")}, nil
	})
	b.AddEdge(compose_v2.StartNode, "work")
	b.AddEdge("work", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// TestStaleWriteGuard_NoBaselineDoesNotBlockCleanup 钉住「**没有基线就不比，写和删一视同仁**」。
//
// # 这里曾经反过来
//
// 有一版实现在无基线时**单独拒绝删除**，理由是「没凭据就别动别人的东西」。那条极性
// 建立在「这个槽可能不归我」上——而 `RunSession.RunID` 的契约把这件事定死了：**一枚 sid
// 只服务一个任务，槽就是归本次 Run 的**（`agent_v2/session.go` 那段 godoc）。于是它拒掉的
// 全是合法调用。
//
// 三条独立理由，任何一条都足以撤：
//
//  1. **实测把正常路径废掉**：顶层 Run 恒 `WithFreshRun`（任务隔离要的）⟹ 恒无基线。
//     业务一开守卫，同一个 sess 上但凡有过一次被放弃的中断，此后**每一个完全跑成功的
//     任务**都返 `ErrCheckpointStale`，且不自愈（实测连续三个任务全中）。
//  2. **它在自己声称的用例上就不成立**：要防的是「别抹掉另一个执行者正等着的挂起存档」，
//     可同处境下的**写**是 fail-open 的——实测守卫开着时 A 的挂起提问照样被 B 的写覆盖。
//     拦住删、拦不住写，保不了任何东西。
//  3. **它是「你的意图 vs 我发现的状态」型守卫**：调用方要让它不报错，得先知道「这个 sid
//     底下有没有别人的挂起档」，那是框架的私有知识。
//
// 本用例用**最像那条极性要保护的形态**做 fixture（子树里真有一份别人的挂起档），断言
// 它现在照样放行——重新加回那条拒绝的话，这里会红。
//
// ⚠️ 撤掉不等于那个危害不存在：两个任务共用一枚 sid 时，后者收尾确实会清掉前者的存档。
// 但那是**违反契约**，框架分辨不出来（合法的崩溃恢复长得一模一样）。真解在 `NextTask`，
// 哨兵见 `agent_v2/session_task_contract_test.go`。
func TestStaleWriteGuard_NoBaselineDoesNotBlockCleanup(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	// 子树里放一份挂起档：这正是被撤掉那条极性最想保护的形态
	seed := &compose_v2.Checkpoint{
		ID: "v", NS: "leftover", GraphID: "g_other", Status: compose_v2.CPStatusPending,
		CurrentFrontier:   []compose_v2.FrontierMember{{NodeID: "ask"}},
		PendingInterrupts: []*compose_v2.AskQuery{{InterruptID: "q", Question: "上一个任务留下的"}},
	}
	if err := cper.SaveNS(ctx, "nb_sid", "leftover", seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	err := compose_v2.Run(ctx, buildNoInterruptGraph(t), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("nb_sid"),
		compose_v2.WithFreshRun(), compose_v2.WithStaleWriteGuard())
	if err != nil {
		t.Fatalf("一个完全跑成功的 Run 被守卫降级成失败：%v\n"+
			"  无基线是顶层 Run 的**常态**（agent_v2 的新任务判定恒 WithFreshRun），不是异常；"+
			"在这儿拒绝等于把 opt-in 的观测功能变成「开了就跑不通」", err)
	}
	// **正向断言**：不光没报错，清理真的发生了——否则「守卫放行」跟「什么都没做」分不开。
	if left, _ := cper.LoadAllInSession(ctx, "nb_sid"); len(left) != 0 {
		t.Errorf("成功 Run 的 cleanup 没真跑（子树还剩 %d 份）——只断言 err==nil 的话，"+
			"一个静默跳过清理的退化也会绿", len(left))
	}
}
