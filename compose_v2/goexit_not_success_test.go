package compose_v2_test

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"

	"crew-go/compose_v2"
)

// TestFanout_GoexitIsNotSuccess 守「goroutine 异常终止不能被当成功」。
//
// # 它防的是什么
//
// fan-out 的每个成员在自己的 goroutine 里跑，结果写进**预分配**的 results 切片。
// `runtime.Goexit` 是第三条出路——它**会**跑 defer，但 `recover()` 返 nil，所以
// panic 那支接不住；于是 `results[i]` 留在零值：`err == nil` ⟹ 算成功，`member`
// 零值 ⟹ 一个 `NodeID:""` 的幽灵成员进 `CompletedFrontier`（语义是「已成功、
// 永不重跑」）⟹ 这条分支连同下游跨 Run + Resume **永久消失**，最后存档被当成功
// 清掉，全程 `err == nil`。
//
// # 为什么这不是合成场景
//
// `zap` 的 godoc 主动推荐在 `zap.WithFatalHook` 里调 `runtime.Goexit`——「Fatal 别杀
// 进程、只结束当前 goroutine」是服务端常见诉求，配了它之后业务一句 `logger.Fatal`
// 就走这条路。`os/user` 在 Windows 上也有一条（`user.Current()` 走得到），testify
// 的 `require.*` 全系同理。
//
// # 断言选在哪
//
// **错误正文里必须有那个节点的名字**——它一条守住两件事：修复前 Run 返 nil（没有
// 正文），而只修「返错」却忘了在这一支带上 `member: m` 的话，身份仍是零值。
//
// sanity-revert：把 `normalReturn` 那一支从 graph_loop.go 的 defer 里去掉 → 本用例红，
// 且红在「Run 该返错，实际 err=<nil>」。
func TestFanout_GoexitIsNotSuccess(t *testing.T) {
	var mu sync.Mutex
	var ran []string
	rec := func(id string) { mu.Lock(); ran = append(ran, id); mu.Unlock() }

	b := compose_v2.NewBuilder().WithName("goexit_fanout")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		rec("split")
		return nil, nil
	})
	b.AddNode("A", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		rec("A")
		runtime.Goexit() // 既不返回也不 panic
		return nil, nil
	})
	b.AddNode("B", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		rec("B")
		return nil, nil
	})
	b.AddNode("afterA", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		rec("afterA")
		return nil, nil
	})
	b.AddNode("afterB", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		rec("afterB")
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("split", "A")
	b.AddEdge("split", "B")
	b.AddEdge("A", "afterA")
	b.AddEdge("B", "afterB")
	b.AddEdge("afterA", compose_v2.EndNode)
	b.AddEdge("afterB", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "goexit_sid"
	runErr := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))

	mu.Lock()
	got := strings.Join(ran, " ")
	mu.Unlock()
	t.Logf("执行过: %s / err=%v", got, runErr)

	// **承重 1**：不能报成功。修复前这里是 <nil>——一条分支连同下游静默消失而 Run 说一切正常。
	if runErr == nil {
		t.Fatalf("goroutine 异常终止（runtime.Goexit）被当成了成功：Run 返 nil\n"+
			"  执行过 = %s（afterA 没跑，说明 A 那条分支确实没走完）", got)
	}

	// **承重 2**：正文里要有那个节点的身份。
	//
	// ⚠️ 它守的是 fmt 参数那条通路（`m.NodeID` 进错误正文），**不守 `member` 字段**——
	// 两者是独立赋值，抠掉 `member: m` 错误正文照样有 "A"（独立审计变异实测过）。
	// `member` 字段由承重 5 守（丢了它 CurrentFrontier 里是幽灵 `{NodeID:""}`）。
	if !strings.Contains(runErr.Error(), `"A"`) {
		t.Errorf("错误正文没点名出事的节点，定位不了：%v", runErr)
	}

	// **承重 3**：留下能续的档——框架判不出 Goexit 的触发条件是瞬时还是确定性
	//（`logger.Fatal` 可能因为下游挂了而走到），那是业务的知识，所以按普通业务错处置：
	// 留 Failed 档让业务自己决定 Resume 重试还是放弃。**不是** `ErrRunAborted`
	//（那个哨兵的判据是「重跑必然一样」，会连档一起清掉、不给业务选）。
	saved, _ := cper.LoadNS(ctx, sid, "")
	if saved == nil {
		t.Fatalf("该留下存档让业务能 Resume 重试这一个失败分支，实际没有")
	}
	if !saved.HasResumableWork() {
		t.Errorf("留下的档该是能续的（frontier 非空），实际 Current=%v", saved.CurrentFrontier)
	}
	if errors.Is(runErr, compose_v2.ErrRunAborted) {
		t.Errorf("不该判成终局失败——框架判不出这次 Goexit 是瞬时还是确定性，清档等于替业务做了决定")
	}

	// **承重 4（最要害）**：幽灵成员不能进「已成功、永不重跑」的集合。
	//
	// 这是原始缺陷最重的后果：零值 member ⟹ `{NodeID:""}` 被持久化进
	// `CompletedFrontier` ⟹ Resume 只重跑别的成员，**这条分支跨 Run 永久消失**。
	for _, fm := range saved.CompletedFrontier {
		if fm.NodeID == "" {
			t.Errorf("`CompletedFrontier` 里出现了 NodeID 为空的幽灵成员：%+v\n"+
				"  那个集合的语义是「已成功、永不重跑」——幽灵进去 = 真实分支被顶替、跨 Run 永久消失",
				saved.CompletedFrontier)
		}
	}

	// **承重 5**：出事的那个成员要能被重跑（在 frontier 里点着名）。
	var inFrontier bool
	for _, fm := range saved.CurrentFrontier {
		if fm.NodeID == "A" {
			inFrontier = true
		}
	}
	if !inFrontier {
		t.Errorf("异常终止的 A 该留在 CurrentFrontier 里等重跑，实际 Current=%v Completed=%v",
			saved.CurrentFrontier, saved.CompletedFrontier)
	}

	// **承重 6**：兄弟在同一超步里照常跑完（best-effort 不 cancel 兄弟，语义没被改坏）。
	if !ranHas(ran, "B") {
		t.Errorf("兄弟分支 B 该照常跑完（超步内失败不 cancel 兄弟），实际执行过 = %s", got)
	}

	// **承重 7**：而兄弟的**后继**不该跑——超步失败就不推进下一超步。
	//
	// ⚠️ 这一条方向容易写反：直觉上「兄弟不受牵连」像是连后继也该跑，但那正是**修复前
	// 的症状**——A 被当成功 ⟹ 超步成功 ⟹ 推进 ⟹ afterB 跑了。所以「afterB 跑没跑」
	// 是本用例真正承重的那一维，比 err != nil 更贴近后果。
	if ranHas(ran, "afterB") {
		t.Errorf("超步里有成员异常终止，不该推进到下一超步，实际执行过 = %s\n"+
			"  afterB 跑了 = 那个成员被当成了成功", got)
	}
}

func ranHas(ran []string, id string) bool {
	for _, s := range ran {
		if s == id {
			return true
		}
	}
	return false
}

// pairCountHooks 数 OnNodeStart / OnNodeEnd 的次数并存 OnGraphEnd 收到的 err。
type pairCountHooks struct {
	compose_v2.NoOpOrchestrationHooks
	mu        sync.Mutex
	starts    int
	ends      int
	lastEnd   error
	graphErr  error
	graphDone bool
}

func (h *pairCountHooks) OnNodeStart(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	h.mu.Lock()
	h.starts++
	h.mu.Unlock()
	return ctx, nil
}

func (h *pairCountHooks) OnNodeEnd(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Channels, err error) context.Context {
	h.mu.Lock()
	h.ends++
	h.lastEnd = err
	h.mu.Unlock()
	return ctx
}

func (h *pairCountHooks) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, err error) context.Context {
	h.mu.Lock()
	h.graphErr = err
	h.graphDone = true
	h.mu.Unlock()
	return ctx
}

// TestFastPath_GoexitStateIsNotALie 守 **fast path**（单节点超步）上的 Goexit——
// 生产最常走的那条路（BasicAgent 的四个节点全是单节点超步）。
//
// # 它防的是什么
//
// fast path 同步 inline 调用 NodeFunc，不起 goroutine。`runtime.Goexit` 掐断整条栈：
// Run **永不返回**（这是 Goexit 的语义，调用方 goroutine 死亡正是 zap 那个配置的意图，
// 不修也不该修）——**但 Run 的 defer 全都会跑**，而 `recover()` 返 nil ⟹ `runErr`
// 保持 nil ⟹ 修复前 cleanup 打着 `reason=success` **删掉用户正等着答的挂起档**、
// `OnGraphEnd` 收到 nil、OnNodeEnd 不配对。修的是**状态不说谎**。
//
// # 判据（两道旗，testing.tRunner 的 finished 同款）
//
//   - Run 层 `bodyReached`：只在唯一的成功 return 前置位 ⟹ 「recover nil + runErr nil +
//     旗 false」= 主体被掐 ⟹ runErr 从记录位取带身份的错
//   - 节点层 `bodyReturned`：safeNodeRun 返回后置位 ⟹ 给 OnNodeEnd 配对 + 记身份
//
// sanity-revert：把 Run 层那支（`!bodyReached` 分支）去掉 → 承重 2/3 红；把节点层
// 那支去掉 → 承重 4/5 红（存档还在但错误没身份、配对断）。
func TestFastPath_GoexitStateIsNotALie(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "goexit_fast_sid"

	// Run1：造一条真实的 HITL 挂起档——它是修复前会被 reason=success 误删的受害者
	mkAsk := func(goexit bool) *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("goexit_fast")
		b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if goexit {
				runtime.Goexit()
			}
			if _, ok := compose_v2.GetResumeAnswer(ctx, "gfq"); !ok {
				return nil, &compose_v2.InterruptError{Reason: "gf",
					Query: &compose_v2.AskQuery{InterruptID: "gfq", Question: "批准吗"}}
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
	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, mkAsk(false), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))); !ok {
		t.Fatal("Run1 该中断")
	}

	// Run2：同 sid，节点 Goexit。跑在自己的 goroutine 里（模拟 HTTP handler /
	// agent goroutine——生产形态；Goexit 掐的就是这条）。wg.Done 在 defer 里，
	// Goexit 也会跑它，所以 Wait 能返回。
	hooks := &pairCountHooks{}
	var wg sync.WaitGroup
	var returned bool
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = compose_v2.Run(ctx, mkAsk(true), compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
			compose_v2.WithHooks(hooks))
		returned = true
	}()
	wg.Wait()

	hooks.mu.Lock()
	starts, ends, lastEnd, graphErr, graphDone := hooks.starts, hooks.ends, hooks.lastEnd, hooks.graphErr, hooks.graphDone
	hooks.mu.Unlock()
	saved, _ := cper.LoadNS(ctx, sid, "")
	t.Logf("returned=%v starts=%d ends=%d graphErr=%v 存档=%v", returned, starts, ends, graphErr, saved != nil)

	// **承重 1（fixture 自检）**：Run 确实没返回——返回了说明场景没搭出来（Goexit 没生效）
	if returned {
		t.Fatal("Goexit 下 Run 不该正常返回——返回了说明 fixture 没搭对")
	}
	if !graphDone {
		t.Fatal("OnGraphEnd 该被调到（defer 链在 Goexit 下照跑）")
	}

	// **承重 2**：OnGraphEnd 收到的不是 nil——修复前这里是 nil（「一切正常」）
	if graphErr == nil {
		t.Error("OnGraphEnd 收到 err=nil：主体被掐断被报成了成功")
	}

	// **承重 3（最要害）**：挂起档没有被 reason=success 删掉，且还能续
	if saved == nil {
		t.Fatal("用户正等着答的存档被当成功清掉了——修复前的原始伤害")
	}
	if !saved.HasResumableWork() {
		t.Errorf("留下的档该可续（frontier 非空），实际 Current=%v", saved.CurrentFrontier)
	}

	// **承重 4**：错误带节点身份（节点层旗记的）
	if graphErr != nil && !strings.Contains(graphErr.Error(), `"n"`) {
		t.Errorf("错误正文没点名出事的节点：%v", graphErr)
	}

	// **承重 5**：OnNodeStart / OnNodeEnd 配对（Goexit 那次的 end 带合成错误）
	if starts != ends {
		t.Errorf("hook 配对断了：OnNodeStart=%d OnNodeEnd=%d（被掐断的节点漏了不闭合的 span）", starts, ends)
	}
	if lastEnd == nil {
		t.Error("Goexit 节点的 OnNodeEnd 该收到合成错误，实际 nil")
	}
}

// rogueCtxHooks 的 OnNodeStart 返回一个**非派生**的 ctx（违约但真实发生：业务
// 图省事写了 context.Background()）。框架挂在 ctx 上的值会整个丢失。
type rogueCtxHooks struct {
	compose_v2.NoOpOrchestrationHooks
	mu       sync.Mutex
	graphErr error
}

func (h *rogueCtxHooks) OnNodeStart(_ context.Context, _ compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	return context.Background(), nil // ← 丢掉所有框架 ctx 值
}

func (h *rogueCtxHooks) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, err error) context.Context {
	h.mu.Lock()
	h.graphErr = err
	h.mu.Unlock()
	return ctx
}

// TestGoexit_IdentitySurvivesRogueHookCtx 守「业务 hook 换掉 ctx 也不丢节点身份」。
//
// 节点异常终止的记录槽从 Run 层经 ctx 流到节点层。业务 OnNodeStart 返非派生 ctx 时
// 槽会跟着丢 ⟹ Goexit 的 note 无处可记 ⟹ 错误退化成没有节点身份的通用文案，定位不了。
//
// 修法：槽跟 componentKind / instanceID / answerLedger 一样收进 applyFrameworkNodeCtx
// 一起重铺——那个函数的意义本就是「框架自己要往节点 ctx 上挂的东西收在一处」。
//
// sanity-revert：把 applyFrameworkNodeCtx 里的 withAbnormalExitSlot 那行去掉 → 本用例红
// （错误变成不带节点名的通用文案）。
func TestGoexit_IdentitySurvivesRogueHookCtx(t *testing.T) {
	hooks := &rogueCtxHooks{}
	b := compose_v2.NewBuilder().WithName("rogue_ctx")
	b.AddNode("victim", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		runtime.Goexit()
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "victim")
	b.AddEdge("victim", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
			compose_v2.WithHooks(hooks))
	}()
	wg.Wait()

	hooks.mu.Lock()
	gotErr := hooks.graphErr
	hooks.mu.Unlock()
	t.Logf("OnGraphEnd err=%v", gotErr)

	if gotErr == nil {
		t.Fatal("hook 换了 ctx 也不该把 Goexit 报成成功")
	}
	// **承重**：业务违约丢了 ctx，节点身份仍要在——否则线上只能看到一句通用文案
	if !strings.Contains(gotErr.Error(), `"victim"`) {
		t.Errorf("错误丢了节点身份（槽随业务 hook 的非派生 ctx 一起丢了）：%v", gotErr)
	}
}
