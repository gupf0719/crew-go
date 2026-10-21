package compose_v2_test

// 子图 import / export 的 baseline 语义。
//
// # 钉的是什么
//
// 「把 X 从 A 复制到 B」曾经实现成「把 X 经 B 的 reducer 写进 B」。对 LastValue 那是复制；
// 对 Sum / Append 那是**相加**，而 B 里本来就有 X：
//   - export 侧：子图没碰过的 import channel 被原样回送 ⟹ 父图 Sum 5→10、Append [a]→[a a]
//   - import 侧：续跑时子 cp 里已有上次 import 的那份，再灌一次 ⟹ 子图读到 13、父最终 18
// 两侧 err 全程 nil。现在 import 摆成不经 reducer 的 baseline，export 只送子图自己写的。
//
// 每条用例都带**正向断言**（子图真读到了 import 值 / 父图真收到了增量）——只断言
// 「没翻倍」在「import 整个失效」的状态上恒真。

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// twoLayer 造「父 seed → 子图(import+export 同一批 channel) → End」的父图。
// subBody 是子图唯一节点的函数；返回父图 + 供断言用的 channel。
func twoLayer(t *testing.T, name string, sumCh *compose_v2.Channel[int], appCh *compose_v2.Channel[[]string],
	subBody compose_v2.NodeFunc) *compose_v2.Graph {
	t.Helper()
	sub := compose_v2.NewBuilder().WithName(name + "_sub")
	sub.AddNode("body", subBody)
	sub.AddEdge(compose_v2.StartNode, "body")
	sub.AddEdge("body", compose_v2.EndNode)
	subG, err := sub.Compile()
	if err != nil {
		t.Fatalf("sub compile: %v", err)
	}
	b := compose_v2.NewBuilder().WithName(name)
	b.AddNode("seed", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{
			compose_v2.Write(sumCh, 5),
			compose_v2.Write(appCh, []string{"a"}),
		}, nil
	})
	b.AddSubgraphNode("sg", subG,
		compose_v2.WithImport(sumCh), compose_v2.WithExport(sumCh),
		compose_v2.WithImport(appCh), compose_v2.WithExport(appCh))
	b.AddEdge(compose_v2.StartNode, "seed")
	b.AddEdge("seed", "sg")
	b.AddEdge("sg", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

func TestSubgraph_ImportExportSame_UntouchedChannelNotEchoed(t *testing.T) {
	sumCh := compose_v2.Sum("bl_sum_1")
	appCh := compose_v2.Append[string]("bl_app_1")
	var sawSum atomic.Int32
	var sawApp atomic.Value
	g := twoLayer(t, "bl_untouched", sumCh, appCh,
		func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			// 子图**读**但不写——正向断言：import 真的把值送进来了
			sawSum.Store(int32(compose_v2.MustGet(chs, sumCh)))
			sawApp.Store(compose_v2.MustGet(chs, appCh))
			return nil, nil
		})
	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, sumCh)
	compose_v2.RegisterChannel(cm, appCh)
	if err := compose_v2.Run(context.Background(), g, cm); err != nil {
		t.Fatalf("run: %v", err)
	}
	if sawSum.Load() != 5 {
		t.Fatalf("fixture 没搭对：子图应读到 import 的 5，实际 %d——import 没生效则下面的断言恒真", sawSum.Load())
	}
	if got := sawApp.Load().([]string); len(got) != 1 || got[0] != "a" {
		t.Fatalf("fixture 没搭对：子图应读到 import 的 [a]，实际 %v", got)
	}
	if got := compose_v2.MustGet(cm.Channels(), sumCh); got != 5 {
		t.Errorf("子图没碰 Sum，父图应仍是 5，实际 %d（10 = import 值被原样回送再相加）", got)
	}
	if got := compose_v2.MustGet(cm.Channels(), appCh); len(got) != 1 {
		t.Errorf("子图没碰 Append，父图应仍是 [a]，实际 %v（[a a] = import 值被原样回送再追加）", got)
	}
}

func TestSubgraph_ImportExportSame_OnlyDeltaExported(t *testing.T) {
	sumCh := compose_v2.Sum("bl_sum_2")
	appCh := compose_v2.Append[string]("bl_app_2")
	g := twoLayer(t, "bl_delta", sumCh, appCh,
		func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{
				compose_v2.Write(sumCh, 3),
				compose_v2.Write(appCh, []string{"b"}),
			}, nil
		})
	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, sumCh)
	compose_v2.RegisterChannel(cm, appCh)
	if err := compose_v2.Run(context.Background(), g, cm); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := compose_v2.MustGet(cm.Channels(), sumCh); got != 8 {
		t.Errorf("父 5 + 子图增量 3 应是 8，实际 %d（13 = 子图把 import 的 5 连同自己的 3 一起送回来）", got)
	}
	if got := compose_v2.MustGet(cm.Channels(), appCh); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("父 [a] ++ 子图增量 [b] 应是 [a b]，实际 %v", got)
	}
}

func TestSubgraph_ImportedValueVisibleAsBasePlusOwn(t *testing.T) {
	// 子图内先写再读：读到的应是 父 ⊕ 自己（不是只有自己的、也不是只有父的）
	sumCh := compose_v2.Sum("bl_sum_3")
	appCh := compose_v2.Append[string]("bl_app_3")
	var sawSum atomic.Int32
	var sawApp atomic.Value

	sub := compose_v2.NewBuilder().WithName("bl_rw_sub")
	sub.AddNode("w", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(sumCh, 3), compose_v2.Write(appCh, []string{"b"})}, nil
	})
	sub.AddNode("r", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sawSum.Store(int32(compose_v2.MustGet(chs, sumCh)))
		sawApp.Store(compose_v2.MustGet(chs, appCh))
		return nil, nil
	})
	sub.AddEdge(compose_v2.StartNode, "w")
	sub.AddEdge("w", "r")
	sub.AddEdge("r", compose_v2.EndNode)
	subG, err := sub.Compile()
	if err != nil {
		t.Fatalf("sub compile: %v", err)
	}
	b := compose_v2.NewBuilder().WithName("bl_rw")
	b.AddNode("seed", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(sumCh, 5), compose_v2.Write(appCh, []string{"a"})}, nil
	})
	b.AddSubgraphNode("sg", subG, compose_v2.WithImport(sumCh), compose_v2.WithImport(appCh))
	b.AddEdge(compose_v2.StartNode, "seed")
	b.AddEdge("seed", "sg")
	b.AddEdge("sg", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, sumCh)
	compose_v2.RegisterChannel(cm, appCh)
	if err := compose_v2.Run(context.Background(), g, cm); err != nil {
		t.Fatalf("run: %v", err)
	}
	if sawSum.Load() != 8 {
		t.Errorf("子图写完再读应看到 父 5 + 自己 3 = 8，实际 %d", sawSum.Load())
	}
	if got := sawApp.Load().([]string); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("子图写完再读应看到 [a b]，实际 %v", got)
	}
}

func TestSubgraph_ImportResume_NotReappliedThroughReducer(t *testing.T) {
	// 子图写 +3 后中断；续跑时 import 再摆一次——不该再累加。
	acc := compose_v2.Sum("bl_resume_acc")
	var sawAfterResume atomic.Int32

	sub := compose_v2.NewBuilder().WithName("bl_resume_sub")
	sub.AddNode("add3", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(acc, 3)}, nil
	})
	sub.AddNode("ask", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "go?"}); err != nil {
			return nil, err
		}
		sawAfterResume.Store(int32(compose_v2.MustGet(chs, acc)))
		return nil, nil
	})
	sub.AddEdge(compose_v2.StartNode, "add3")
	sub.AddEdge("add3", "ask")
	sub.AddEdge("ask", compose_v2.EndNode)
	subG, err := sub.Compile()
	if err != nil {
		t.Fatalf("sub compile: %v", err)
	}
	b := compose_v2.NewBuilder().WithName("bl_resume")
	b.AddNode("seed", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(acc, 5)}, nil
	})
	b.AddSubgraphNode("sg", subG, compose_v2.WithImport(acc), compose_v2.WithExport(acc))
	b.AddEdge(compose_v2.StartNode, "seed")
	b.AddEdge("seed", "sg")
	b.AddEdge("sg", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, acc)
	err = compose_v2.Run(ctx, g, cm, compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("bl_sid"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应中断, got %v", err)
	}
	// 子 cp 里存的是子图**自己的**进度（3），不是 import 进来的 5 + 3
	subCP, _ := cper.LoadNS(ctx, "bl_sid", "sg")
	if subCP == nil {
		t.Fatal("fixture 没搭对：中断后应有子 cp")
	}
	if tv, has := subCP.Channels["bl_resume_acc"]; !has || string(tv.Data) != "3" {
		t.Errorf("子 cp 应只存子图自己写的 3，实际 %v（8 = import 值被写进了 values 并随 cp 落盘）", subCP.Channels)
	}

	cm2 := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm2, acc)
	if err := compose_v2.Run(ctx, g, cm2, compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("bl_sid"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("go")})); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if sawAfterResume.Load() != 8 {
		t.Errorf("续跑后子图应读到 父 5 + 自己 3 = 8，实际 %d（13 = import 经 reducer 又加了一次）", sawAfterResume.Load())
	}
	if got := compose_v2.MustGet(cm2.Channels(), acc); got != 8 {
		t.Errorf("父最终应是 8，实际 %d（18 = 子图把翻倍后的 13 整个送回来）", got)
	}
}

func TestSubgraph_ImportLastValue_OwnWriteSurvivesResume(t *testing.T) {
	// 子图改写了 import 进来的 LastValue 后中断；续跑时父图的值再摆一次，
	// 不该把子图自己的写盖回去（旧实现经 reducer 覆盖写会盖掉）。
	goal := compose_v2.LastValue[string]("bl_goal")
	var sawAfterResume atomic.Value

	sub := compose_v2.NewBuilder().WithName("bl_lv_sub")
	sub.AddNode("rewrite", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(goal, compose_v2.MustGet(chs, goal)+" + feedback")}, nil
	})
	sub.AddNode("ask", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "go?"}); err != nil {
			return nil, err
		}
		sawAfterResume.Store(compose_v2.MustGet(chs, goal))
		return nil, nil
	})
	sub.AddEdge(compose_v2.StartNode, "rewrite")
	sub.AddEdge("rewrite", "ask")
	sub.AddEdge("ask", compose_v2.EndNode)
	subG, err := sub.Compile()
	if err != nil {
		t.Fatalf("sub compile: %v", err)
	}
	b := compose_v2.NewBuilder().WithName("bl_lv")
	b.AddNode("seed", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(goal, "G")}, nil
	})
	b.AddSubgraphNode("sg", subG, compose_v2.WithImport(goal))
	b.AddEdge(compose_v2.StartNode, "seed")
	b.AddEdge("seed", "sg")
	b.AddEdge("sg", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, goal)
	err = compose_v2.Run(ctx, g, cm, compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("bl_lv_sid"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应中断, got %v", err)
	}
	cm2 := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm2, goal)
	if err := compose_v2.Run(ctx, g, cm2, compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("bl_lv_sid"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("go")})); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if got, _ := sawAfterResume.Load().(string); got != "G + feedback" {
		t.Errorf("续跑后子图应仍看到自己改写的 %q，实际 %q（%q = 父图的值经 reducer 覆盖写把进度盖回去了）",
			"G + feedback", got, "G")
	}
}

func TestSubgraph_ReducerPanicInRead_NamesChannel(t *testing.T) {
	// reducer 在子图读路径的合成里 panic：报文必须带 "reducer" 与 channel 名。
	// 不带的话它长成「node "r": node panic recovered: boom」——落在第一个读者头上，
	// 而那个节点只做了一次 Get，排查的人会去翻一个无辜节点的代码。
	boom := compose_v2.Custom("bl_boom", func(prev, next int) int {
		if prev == 5 && next == 3 {
			panic("boom")
		}
		return prev + next
	}, 0)

	sub := compose_v2.NewBuilder().WithName("bl_boom_sub")
	sub.AddNode("w", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(boom, 3)}, nil // own = reduce(0,3) = 3，不炸
	})
	sub.AddNode("r", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_ = compose_v2.MustGet(chs, boom) // 合成 reduce(baseline=5, own=3) → 炸
		return nil, nil
	})
	sub.AddEdge(compose_v2.StartNode, "w")
	sub.AddEdge("w", "r")
	sub.AddEdge("r", compose_v2.EndNode)
	subG, err := sub.Compile()
	if err != nil {
		t.Fatalf("sub compile: %v", err)
	}
	b := compose_v2.NewBuilder().WithName("bl_boom")
	b.AddNode("seed", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(boom, 5)}, nil
	})
	b.AddSubgraphNode("sg", subG, compose_v2.WithImport(boom))
	b.AddEdge(compose_v2.StartNode, "seed")
	b.AddEdge("seed", "sg")
	b.AddEdge("sg", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, boom)
	err = compose_v2.Run(context.Background(), g, cm)
	if err == nil {
		t.Fatal("fixture 没搭对：reducer 应在合成读时 panic 并被兜成错，实际 err=nil")
	}
	msg := err.Error()
	// **正向断言**：归因信息只有 composeRead 补了才产得出
	for _, want := range []string{"reducer panic during read", `"bl_boom"`, "boom"} {
		if !strings.Contains(msg, want) {
			t.Errorf("报文缺 %q（没有它排查的人会去翻读者节点的代码）：%s", want, msg)
		}
	}
	// 落点不变：仍是那个读者节点失败，不是 Run 级 panic
	if !strings.Contains(msg, `node "r"`) {
		t.Errorf("落点应仍是读者节点 r（safeNodeRun 兜），实际：%s", msg)
	}
}

// startProbeHooks 记下每次 OnGraphStart 时某个 Sum channel 读到什么。
type startProbeHooks struct {
	compose_v2.NoOpOrchestrationHooks
	ch   *compose_v2.Channel[int]
	mu   sync.Mutex
	seen []string // "ns=<graphNS> v=<val> ok=<bool>"
}

func (h *startProbeHooks) OnGraphStart(ctx context.Context, chs compose_v2.Channels) (context.Context, error) {
	v, ok := compose_v2.Get(chs, h.ch)
	h.mu.Lock()
	h.seen = append(h.seen, fmt.Sprintf("ns=%q v=%d ok=%v", compose_v2.GetGraphNS(ctx), v, ok))
	h.mu.Unlock()
	return ctx, nil
}

func TestSubgraph_OnGraphStartSeesImportBaseline(t *testing.T) {
	// 子图的 OnGraphStart hook 里必须能读到 import 进来的值——baseline 在 hook 之前摆好。
	// 曾经摆在 hook 之后，hook 里 Get(importCh) 拿到 ok=false（独立审计实测）。
	acc := compose_v2.Sum("bl_start_acc")
	sub := compose_v2.NewBuilder().WithName("bl_start_sub")
	sub.AddNode("noop", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	sub.AddEdge(compose_v2.StartNode, "noop")
	sub.AddEdge("noop", compose_v2.EndNode)
	subG, err := sub.Compile()
	if err != nil {
		t.Fatalf("sub compile: %v", err)
	}
	b := compose_v2.NewBuilder().WithName("bl_start")
	b.AddNode("seed", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(acc, 5)}, nil
	})
	b.AddSubgraphNode("sg", subG, compose_v2.WithImport(acc))
	b.AddEdge(compose_v2.StartNode, "seed")
	b.AddEdge("seed", "sg")
	b.AddEdge("sg", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	hooks := &startProbeHooks{ch: acc}
	ctx := compose_v2.WithOrchHooks(context.Background(), hooks)
	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, acc)
	if err := compose_v2.Run(ctx, g, cm); err != nil {
		t.Fatalf("run: %v", err)
	}
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	// 正向：子图那层的 OnGraphStart 真跑了、且读到 5
	var subSaw bool
	for _, s := range hooks.seen {
		if strings.Contains(s, `ns="sg"`) {
			if s != `ns="sg" v=5 ok=true` {
				t.Errorf("子图 OnGraphStart 应读到 import 的 5（ok=true），实际 %s", s)
			}
			subSaw = true
		}
	}
	if !subSaw {
		t.Fatalf("fixture 没搭对：没有观察到子图那层的 OnGraphStart，全部记录 %v", hooks.seen)
	}
}
