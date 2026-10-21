package compose_v2_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// 引擎入口的默认档是「**恢复否则新开**」：该 (sid, ns) 有挂起存档就续上、没有就拿种子从头跑。
//
// # 为什么种子和挂起存档同时出现不是矛盾
//
// `WithInitialWrites` 已经收窄成「**只在新开路径 apply**」，于是它的语义是「这是我给新开
// 路径的输入」——**它不声称「这是一个新任务」**。所以这个组合读作「有东西可续就续、否则
// 用我这份输入从头开始」。
//
// ⚠️ 「只在新开路径 apply」这条性质**不由本文件钉**——本文件两次调用播的是同一个值
// （崩溃恢复就是同一个业务函数原样再调，必然同值），续跑时重播只是把同样的值又写一遍、
// 污染不显现。钉它的是 `cp_event_test.go` 的 `TestInitialWrites_OnlyAppliedOnFreshPath`
// （用 `Append` channel 才有分辨力）。
//
// 这正是崩溃恢复的形态：开 `sync`/`async` 硬崩后同 sid 再调一次同一个业务函数，而那个
// 函数照常播种子（它并不知道自己是在恢复）。
//
// # 这里曾经有一道守卫，撤了
//
// 曾加过「顶层 + 有种子 + 存档命中 ⟹ 报错」，理由是「两种意图打架」。实测下来三条都不成立：
//
//  1. 立论在语义收窄之后就没了——种子早已不声称「这是新任务」
//  2. **零真阳性**：9 编排顶层恒声明新任务、根本进不了这条闸门，守卫只能对「直连
//     `compose_v2.Run` 且没传 `WithFreshRun`」的人开火，而那批人要的就是「恢复否则新开」
//  3. **合规方式不存在**：调用方只能自己查存档，而它能用的 `ExistsNS` 跟引擎的判据
//     （`Status == Pending`）不是同一个谓词——`Failed` 存档上分叉，实测据此摘掉种子后节点
//     拿到**空输入**且 `err == nil`
//
// 本文件的用例就是防它被写回来：加回守卫 → `TestRecoverOrStart_CrashRecovery` 立刻红。

// buildRecoverGraph 造「先做副作用、再提问」的两节点图——副作用有没有重放是承重量。
func buildRecoverGraph(t *testing.T, trace *[]string, stall *bool) (*compose_v2.Graph, *compose_v2.Channel[string]) {
	t.Helper()
	inCh := compose_v2.LastValue[string]("ros_in")
	b := compose_v2.NewBuilder()
	b = compose_v2.WithChannel(b, inCh)
	b.AddNode("side_effect", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		v, _ := compose_v2.Get(chs, inCh)
		*trace = append(*trace, "副作用@"+v)
		return nil, nil
	})
	b.AddNode("finish", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if *stall { // 模拟「跑到一半进程被掐」
			<-ctx.Done()
			return nil, ctx.Err()
		}
		v, _ := compose_v2.Get(chs, inCh)
		*trace = append(*trace, "收尾@"+v)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "side_effect")
	b.AddEdge("side_effect", "finish")
	b.AddEdge("finish", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g, inCh
}

// TestRecoverOrStart_CrashRecovery 主证：硬崩后**同 sid 再调一次同一个业务函数**能续上。
//
// 这条是文档承诺的崩溃恢复流程，也是那道被撤守卫拆掉的那条路。
func TestRecoverOrStart_CrashRecovery(t *testing.T) {
	var trace []string
	stall := true
	g, inCh := buildRecoverGraph(t, &trace, &stall)
	cper := compose_v2.NewInMemoryCheckpointer()

	// 「业务函数」：崩溃恢复时被原样再调一次，它照常播种子（它不知道自己在恢复）
	bizRun := func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("crash"),
			compose_v2.WithDurability(compose_v2.DurabilitySync),
			compose_v2.WithInitialWrites(compose_v2.Write(inCh, "订单123")))
	}

	ctx1, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	err1 := bizRun(ctx1)
	cancel()
	if err1 == nil {
		t.Fatal("第一次应被 deadline 掐死（fixture 没搭对）")
	}
	if len(trace) == 0 || trace[0] != "副作用@订单123" {
		t.Fatalf("第一次应先做完副作用，轨迹=%v", trace)
	}
	pend, _ := cper.ListPending(context.Background(), "crash")
	if len(pend) != 1 {
		t.Fatalf("前提：崩溃后应留 1 份挂起存档，实际 %d（没留存档就没有恢复可谈）", len(pend))
	}

	// 进程重启，同 sid 再调一次
	stall, trace = false, nil
	if err := bizRun(context.Background()); err != nil {
		t.Fatalf("崩溃恢复应成功，got %v\n"+
			"（若返 ambiguous/歧义类错误，说明那道「有种子 + 存档命中就报错」的守卫被写回来了——\n"+
			" 它拒掉的全是合法调用，理由见本文件头注释）", err)
	}

	// 承重①：**副作用不重放**。只断「没报错」分辨不出「从头又跑了一遍」——那种情况下
	// err 同样是 nil，而钱已经扣了两次。
	for _, s := range trace {
		if strings.HasPrefix(s, "副作用") {
			t.Fatalf("恢复时重放了副作用——这是从头新开不是续跑，轨迹=%v", trace)
		}
	}
	// 承重②：确实往下跑完了，而且**恰好一次**。
	//
	// ⚠️ 断的是**次数**不是「最后一个元素是什么」——`["收尾","收尾"]` 的最后一个元素同样
	// 是「收尾」，于是「续跑时节点被重复投递」这类退化照过。审计用计数器实测过：那种
	// 退化下这 4 条全绿，而它跟「副作用重放」是同一类问题、正在本文件的射程之内。
	if len(trace) != 1 || trace[0] != "收尾@订单123" {
		t.Fatalf("恢复后应恰好只跑一次收尾，轨迹=%v", trace)
	}
}

// TestRecoverOrStart_NoCheckpointUsesSeeds 另一半：没东西可续时，种子照常从头跑。
//
// 「恢复否则新开」两半都得成立——只有前一半的话，第一次调用会跑不起来。
func TestRecoverOrStart_NoCheckpointUsesSeeds(t *testing.T) {
	var trace []string
	stall := false
	g, inCh := buildRecoverGraph(t, &trace, &stall)
	cper := compose_v2.NewInMemoryCheckpointer()

	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("fresh"),
		compose_v2.WithInitialWrites(compose_v2.Write(inCh, "订单999"))); err != nil {
		t.Fatalf("无存档时应用种子从头跑，got %v", err)
	}
	if len(trace) != 2 || trace[0] != "副作用@订单999" || trace[1] != "收尾@订单999" {
		t.Fatalf("种子没进图或没跑完，轨迹=%v", trace)
	}
}

// TestRecoverOrStart_SeedsDoNotOverrideExplicitResume 顶层「种子 + 明示的续跑答案」同给时，
// **种子不得压过那个明示声明**。
//
// # 为什么单独一格
//
// 这条不变量是「为什么不按种子推断新任务」的**全部依据**：那一档会让种子压过
// `WithResumeAnswers` 这个明确无误的「我在续跑」，静默变成从头重跑、**副作用整轮重放**，
// 而 `err == nil`。
//
// 撤守卫时连带删掉了钉它的那条用例，于是这条不变量**只活在注释里、全仓零覆盖**——审计
// 实测：把「顶层 + 种子 + 明示答案 ⟹ 当新任务」注入引擎，`compose_v2` 和 `agent_v2` 全包
// 都绿。守卫可以撤，不变量不能跟着撤。
func TestRecoverOrStart_SeedsDoNotOverrideExplicitResume(t *testing.T) {
	var trace []string
	inCh := compose_v2.LastValue[string]("sor_in")
	b := compose_v2.NewBuilder()
	b = compose_v2.WithChannel(b, inCh)
	b.AddNode("side_effect", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		v, _ := compose_v2.Get(chs, inCh)
		trace = append(trace, "副作用@"+v)
		return nil, nil
	})
	b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if a, ok := compose_v2.GetResumeAnswer(ctx, "sor_q"); ok && a != nil {
			trace = append(trace, "答:"+a.Content)
			return nil, nil
		}
		return nil, &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{InterruptID: "sor_q", Question: "?"}}
	})
	b.AddEdge(compose_v2.StartNode, "side_effect")
	b.AddEdge("side_effect", "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	// ⚠️ 顶层（ns 为空）——这正是漏掉的那半；嵌套那半由 NestedResumes 覆盖
	if _, isInt := compose_v2.IsInterrupt(compose_v2.Run(ctx, g,
		compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("sor"),
		compose_v2.WithInitialWrites(compose_v2.Write(inCh, "订单123")))); !isInt {
		t.Fatal("首跑应中断")
	}

	trace = nil
	// **种子和明示的续跑答案同给**——调用方照常播种子（它不知道自己在恢复），
	// 同时给了答案。答案是明示声明，种子不是。
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("sor"),
		compose_v2.WithInitialWrites(compose_v2.Write(inCh, "订单123")),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			"sor_q": schema.UserMessage("批准")})); err != nil {
		t.Fatalf("应照常续跑，got %v", err)
	}
	// 承重：副作用**不重放** + 答案恰好消费一次。
	// 只断 err==nil 分辨不出——按种子推断那一档同样 err==nil，只是钱扣了两次。
	for _, s := range trace {
		if strings.HasPrefix(s, "副作用") {
			t.Fatalf("种子压过了明示的续跑声明——从头重跑、副作用重放了，轨迹=%v", trace)
		}
	}
	if len(trace) != 1 || trace[0] != "答:批准" {
		t.Fatalf("应恰好消费一次答案，轨迹=%v", trace)
	}
}

// TestRecoverOrStart_FreshRunOverrides 显式出口：`WithFreshRun()` 忽略存档、拿种子从头跑。
//
// 这是「复用 sid 开新任务」的正确写法——默认档会续上旧的，而那在开新任务时是错的。
func TestRecoverOrStart_FreshRunOverrides(t *testing.T) {
	var trace []string
	stall := true
	g, inCh := buildRecoverGraph(t, &trace, &stall)
	cper := compose_v2.NewInMemoryCheckpointer()

	ctx1, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	_ = compose_v2.Run(ctx1, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("ovr"),
		compose_v2.WithDurability(compose_v2.DurabilitySync),
		compose_v2.WithInitialWrites(compose_v2.Write(inCh, "任务A")))
	cancel()
	if pend, _ := cper.ListPending(context.Background(), "ovr"); len(pend) != 1 {
		t.Fatalf("前提：应留 1 份挂起存档，实际 %d（fixture 没搭对，这条测不到覆盖）", len(pend))
	}

	stall, trace = false, nil
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("ovr"),
		compose_v2.WithFreshRun(),
		compose_v2.WithInitialWrites(compose_v2.Write(inCh, "任务B"))); err != nil {
		t.Fatalf("显式新任务应成功，got %v", err)
	}
	// 承重：新任务的种子必须真的进图（从头跑 ⟹ 副作用也会重做，那是新任务应有的样子）
	if len(trace) != 2 || trace[0] != "副作用@任务B" {
		t.Fatalf("WithFreshRun 下新任务的输入没进图，轨迹=%v", trace)
	}
}

// TestRecoverOrStart_NestedResumes 钉的是「**ns 非空时 auto-load 照常发生**」。
//
// ⚠️ 名字里的「嵌套」要按字面读窄一点：这里只有一张平图 + 一个非空 ns 字符串，**没有父图**。
// 所以它不钉「父 Resume 时子层靠它恢复」那句更强的话——那半由 `subgraph_*_test.go` 里的
// 真嵌套用例（`TestSubgraph_NestedInterrupt_ResumeFromSubFrontier` 等）和
// `fresh_run_option_test.go` 接住。审计实测：把 `WithRunNS` 整个变 no-op，本条照过，
// 而那 11 条真嵌套用例会红。
func TestRecoverOrStart_NestedResumes(t *testing.T) {
	var trace []string
	inCh := compose_v2.LastValue[string]("nst_in")
	b := compose_v2.NewBuilder()
	b = compose_v2.WithChannel(b, inCh)
	b.AddNode("side_effect", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		v, _ := compose_v2.Get(chs, inCh)
		trace = append(trace, "副作用@"+v)
		return nil, nil
	})
	b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if a, ok := compose_v2.GetResumeAnswer(ctx, "nst_q"); ok && a != nil {
			trace = append(trace, "答:"+a.Content)
			return nil, nil
		}
		return nil, &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{InterruptID: "nst_q", Question: "?"}}
	})
	b.AddEdge(compose_v2.StartNode, "side_effect")
	b.AddEdge("side_effect", "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	// 父编排每次调子层都照常带种子——父 Resume 时也一样
	nested := func(extra ...compose_v2.RunOption) error {
		opts := append([]compose_v2.RunOption{
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("nst"),
			compose_v2.WithRunNS("parent/child"),
			compose_v2.WithInitialWrites(compose_v2.Write(inCh, "子输入")),
		}, extra...)
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...)
	}
	if _, isInt := compose_v2.IsInterrupt(nested()); !isInt {
		t.Fatal("子层首跑应中断")
	}
	trace = nil
	if err := nested(compose_v2.WithResumeAnswers(map[string]*schema.Message{
		"nst_q": schema.UserMessage("go")})); err != nil {
		t.Fatalf("子层续跑应成功，got %v", err)
	}
	// 承重：真续跑（副作用不重放）+ 答案被消费。少了后一条的话，「静默什么都没跑」
	// 也满足前一条——否定式断言的集合不构成「它真干活了」。
	for _, s := range trace {
		if strings.HasPrefix(s, "副作用") {
			t.Fatalf("子层续跑重放了副作用，轨迹=%v", trace)
		}
	}
	// 同上：断次数不断「最后一个元素」——重复投递时 `["答:go","答:go"]` 照过。
	if len(trace) != 1 || trace[0] != "答:go" {
		t.Fatalf("子层续跑应恰好消费一次答案，轨迹=%v", trace)
	}
}
