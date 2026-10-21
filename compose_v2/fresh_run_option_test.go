package compose_v2_test

import (
	"context"
	"testing"

	"crew-go/compose_v2"
)

// `WithFreshRun()` 必须**穿到嵌套子图**。
//
// # 修的是什么
//
// 「这次是新任务」有两个入口说同一句话——RunOption `WithFreshRun()` 和 ctx 信号
// `WithFreshRunCtx`——而原先**只接了一个**：option 只在本层的 auto-load 闸门被消费，
// 子图读的却是 ctx 信号。
//
// 于是调用方照着 `WithFreshRun` 的说明复用 sid 开新任务时，**子层照旧把上个任务的挂起
// 存档捡起来接着跑**，前置节点整个被跳过，而 `err == nil` 全程无告警。
//
// 而 `IsFreshRunCtx` 的 godoc 明写「框架内部协议，业务不要 push」——**业务唯一能用的出口
// 就是那个 option**，它不穿透就等于这条路根本不通。
//
// 形状是本仓反复栽的那个：**同一个判断在两处各判各的**。修法是让 option 立刻落进 ctx，
// 两个入口汇成同一个信号。
func TestFreshRunOption_ReachesNestedSubgraph(t *testing.T) {
	var trace []string
	halt := true
	inCh := compose_v2.LastValue[string]("fro_in")

	subB := compose_v2.NewBuilder()
	subB = compose_v2.WithChannel(subB, inCh)
	subB.AddNode("pre", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		v, _ := compose_v2.Get(chs, inCh)
		trace = append(trace, "前置@"+v)
		return nil, nil
	})
	subB.AddNode("ask", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		v, _ := compose_v2.Get(chs, inCh)
		if halt {
			trace = append(trace, "问@"+v)
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "fro_q", Question: "?"}}
		}
		trace = append(trace, "收尾@"+v)
		return nil, nil
	})
	subB.AddEdge(compose_v2.StartNode, "pre")
	subB.AddEdge("pre", "ask")
	subB.AddEdge("ask", compose_v2.EndNode)
	sub, err := subB.WithName("fro_sub").Compile()
	if err != nil {
		t.Fatalf("compile sub: %v", err)
	}

	parentB := compose_v2.NewBuilder()
	parentB = compose_v2.WithChannel(parentB, inCh)
	parentB.AddSubgraphNode("child", sub, compose_v2.WithImport(inCh)).
		AddEdge(compose_v2.StartNode, "child").
		AddEdge("child", compose_v2.EndNode)
	parent, err := parentB.WithName("fro_parent").Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// 任务 A：跑到子图的提问处中断，**用户就此放弃**（不 Resume）。
	// ⚠️ 必须停在中断——跑完的话存档被清，没有残留可污染，这条用例恒真。
	if _, isInt := compose_v2.IsInterrupt(compose_v2.Run(ctx, parent,
		compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("fro"),
		compose_v2.WithInitialWrites(compose_v2.Write(inCh, "任务A")))); !isInt {
		t.Fatal("任务 A 应中断在子图里")
	}
	pend, _ := cper.ListPending(ctx, "fro")
	if len(pend) == 0 {
		t.Fatal("前提：任务 A 应留下挂起存档（fixture 没搭对，这条测不到任何东西）")
	}

	// 任务 B：照 `WithFreshRun` 的说明，复用 sid 开新任务
	halt, trace = false, nil
	if err := compose_v2.Run(ctx, parent, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("fro"),
		compose_v2.WithFreshRun(),
		compose_v2.WithInitialWrites(compose_v2.Write(inCh, "任务B"))); err != nil {
		t.Fatalf("新任务应成功，got %v", err)
	}

	// 承重①：**子图的前置节点必须真的跑**——它被跳过就说明子层把 A 的存档捡起来续了。
	// 断的是「跑没跑」不是「最终结果对不对」：续跑时最终结果照样看着正常。
	if len(trace) == 0 || trace[0] != "前置@任务B" {
		t.Fatalf("`WithFreshRun()` 没穿到子图——子层从任务 A 的中断点续跑了。\n"+
			"实际轨迹=%v（应以「前置@任务B」开头）", trace)
	}
	// 承重②：子图看到的是新任务的输入，不是 A 的
	for _, s := range trace {
		if s == "前置@任务A" || s == "收尾@任务A" {
			t.Fatalf("子图拿到的是任务 A 的输入，轨迹=%v", trace)
		}
	}
}
