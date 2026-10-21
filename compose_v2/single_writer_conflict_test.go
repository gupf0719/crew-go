package compose_v2_test

// 单写入者冲突：两个成员在同一超步写同一个 `LastValue` channel。
//
// # 钉的是什么
//
// `LastValue` 的 reducer 是 `return next`。两个并发成员同时写它时**静默丢一个**，
// 赢家由 frontier 索引序决定——「N=1 正确、N≥2 静默错、err == nil」的标准签名。
// 引擎在超步归并前拒掉这种图。
//
// # 断言里必须有正向的那条
//
// 「报错了」「没提交」「没重放」三条都在「什么都没发生」上恒真。所以这里除了
// 「拒了」，还断言**错误正文点得出是哪个 channel、哪两个成员**——那是只有真的
// 检测到冲突才产得出的东西。

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"crew-go/schema"

	"crew-go/compose_v2"
)

// buildTwoWriterGraph 造一张「split 扇出两个 W 实例、两个都写 out」的图。
// out 由 caller 注册成 LastValue 或 Append，决定这张图合不合法。
func buildTwoWriterGraph(t *testing.T, out *compose_v2.Channel[string], name string) *compose_v2.Graph {
	t.Helper()
	b := compose_v2.NewBuilder().WithName(name)
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "W"},
			compose_v2.SendDirective{Target: "W"},
		)
		return nil, nil
	})
	b.AddNode("W", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(out, "x")}, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("W", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

func TestSingleWriter_TwoMembersWritingLastValue_Rejected(t *testing.T) {
	out := compose_v2.LastValue[string]("sw_out")
	g := buildTwoWriterGraph(t, out, "sw_conflict")

	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, out)
	err := compose_v2.Run(context.Background(), g, cm)

	if err == nil {
		t.Fatal("两个成员并发写同一个 LastValue，应当被拒——静默丢一个才是要防的那个行为")
	}
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("应 wrap ErrRunAborted（图接线矛盾、重跑必然一样：不重试 + 清 cp 子树），实际 %v", err)
	}
	// **正向断言**：错误正文必须点得出是哪个 channel、哪两个成员。
	// 这是只有真检测到冲突才产得出的信息——「什么都没发生」时产不出。
	msg := err.Error()
	for _, want := range []string{"sw_out", "W#"} {
		if !strings.Contains(msg, want) {
			t.Errorf("错误正文缺定位信息 %q（没有它排查时不知道该看哪儿）：%s", want, msg)
		}
	}
	// 两个成员应各自出现一次（W#<inst1> 与 W#<inst2> 是不同的 token）
	if n := strings.Count(msg, "W#"); n != 2 {
		t.Errorf("应点出**两个**冲突成员，实际出现 %d 次：%s", n, msg)
	}
}

func TestSingleWriter_AppendChannel_NotRejected(t *testing.T) {
	// 反例组：同一张图、同样两个成员并发写，只把 channel 换成能合并的 Append。
	// 没有这一组就证明不了守卫拦的是「冲突」而不是「凡扇出就拒」。
	acc := compose_v2.Append[string]("sw_acc")
	b := compose_v2.NewBuilder().WithName("sw_append")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "W"},
			compose_v2.SendDirective{Target: "W"},
		)
		return nil, nil
	})
	b.AddNode("W", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(acc, []string{"x"})}, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("W", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, acc)
	if err := compose_v2.Run(context.Background(), g, cm); err != nil {
		t.Fatalf("Append 能合并，两个成员并发写是合法的，不该拒：%v", err)
	}
	// 正向断言：两条都落地了（守卫没顺手把合法的写入吞掉）
	if got := compose_v2.MustGet(cm.Channels(), acc); len(got) != 2 {
		t.Errorf("两个成员各写一条，应累积 2 条，实际 %v", got)
	}
}

func TestSingleWriter_SameMemberWritesTwice_Allowed(t *testing.T) {
	// 同一个成员在自己的 []ChannelWrite 里连写两次：有序、确定，是它自己的显式意图，
	// 不算冲突。守卫只看**跨成员**。
	//
	// ⚠️ fixture 必须是**宽度 ≥2** 的超步。第一版写成 Start→N→End 单节点，实测
	// **不承重**：`executeFrontier` 对宽度 1 的 frontier 有 fast path，整个检测循环
	// 都没跑到——摘掉「只看跨成员」那条限制，用例照样绿（早退兜底盖过 fix 路径）。
	// 所以这里扇出两个成员，各自连写两次**自己的** channel：没有跨成员冲突，
	// 但检测循环真的跑了。
	chA := compose_v2.LastValue[string]("sw_self_a")
	chB := compose_v2.LastValue[string]("sw_self_b")
	b := compose_v2.NewBuilder().WithName("sw_self_twice")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "A"},
			compose_v2.SendDirective{Target: "B"},
		)
		return nil, nil
	})
	b.AddNode("A", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{
			compose_v2.Write(chA, "first"),
			compose_v2.Write(chA, "second"),
		}, nil
	})
	b.AddNode("B", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{
			compose_v2.Write(chB, "first"),
			compose_v2.Write(chB, "second"),
		}, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("A", compose_v2.EndNode)
	b.AddEdge("B", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, chA)
	compose_v2.RegisterChannel(cm, chB)
	if err := compose_v2.Run(context.Background(), g, cm); err != nil {
		t.Fatalf("同成员连写两次不该被拒：%v", err)
	}
	// 正向断言：后写的赢，LastValue 语义没被守卫改掉
	if got := compose_v2.MustGet(cm.Channels(), chA); got != "second" {
		t.Errorf("chA 应是后写的赢，实际 %q", got)
	}
	if got := compose_v2.MustGet(cm.Channels(), chB); got != "second" {
		t.Errorf("chB 应是后写的赢，实际 %q", got)
	}
}

func TestSingleWriter_SendPayloadsCollapse_Rejected(t *testing.T) {
	// N 条 SendFrom 指令的 payload 写同一个 LastValue —— 这是**本包 godoc 一度教的**
	// map-reduce 写法。payload 灌的是共享 channel，N 条归并进同一个格子后只剩最后一条，
	// N 个 worker 全看到同一个值而 err == nil。
	//
	// ⚠️ 这条**抓不到**成员间那道检测：三条指令全出自同一个 split 节点，按「谁写的」
	// 分组是一组。所以它需要自己那道按 directive 分组的检测。
	in := compose_v2.LastValue[string]("swp_in")
	b := compose_v2.NewBuilder().WithName("swp")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		var dirs []compose_v2.SendDirective
		for _, task := range []string{"task-1", "task-2", "task-3"} {
			dirs = append(dirs, compose_v2.SendDirective{
				Target:  "worker",
				Payload: []compose_v2.ChannelWrite{compose_v2.Write(in, task)},
			})
		}
		compose_v2.SendFrom(ctx, dirs...)
		return nil, nil
	})
	var seen []string
	var mu sync.Mutex
	b.AddNode("worker", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		mu.Lock()
		seen = append(seen, compose_v2.MustGet(chs, in))
		mu.Unlock()
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("worker", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, in)
	runErr := compose_v2.Run(context.Background(), g, cm)

	if runErr == nil {
		mu.Lock()
		got := append([]string(nil), seen...)
		mu.Unlock()
		t.Fatalf("三条 payload 塌成一条应被拒，实际 err=nil，三个 worker 看到 %v", got)
	}
	if !errors.Is(runErr, compose_v2.ErrRunAborted) {
		t.Errorf("应 wrap ErrRunAborted，实际 %v", runErr)
	}
	// **正向断言**：正文要点出 channel + 是哪两条指令。只有真检测到才产得出。
	msg := runErr.Error()
	for _, want := range []string{"swp_in", "第 0 条", "第 1 条"} {
		if !strings.Contains(msg, want) {
			t.Errorf("错误正文缺定位信息 %q：%s", want, msg)
		}
	}
	// **正向断言**：拒在提交之前 —— 一个 worker 都不该跑起来。
	mu.Lock()
	n := len(seen)
	mu.Unlock()
	if n != 0 {
		t.Errorf("应在提交前拒掉，worker 一个都不该跑，实际跑了 %d 个", n)
	}
}

func TestSingleWriter_MemberWriteVsPayload_Rejected(t *testing.T) {
	// 同一超步：成员 A 写 LastValue X；成员 B 发一条 SendFrom 指令，payload 也写 X。
	// payload 走第二次 apply，会静默盖掉 A 的写——也是「两个写入者、赢家由 apply 顺序决定」。
	x := compose_v2.LastValue[string]("swx")
	b := compose_v2.NewBuilder().WithName("sw_member_vs_payload")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "A"},
			compose_v2.SendDirective{Target: "B"},
		)
		return nil, nil
	})
	b.AddNode("A", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(x, "from-A")}, nil
	})
	b.AddNode("B", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{
			Target:  "C",
			Payload: []compose_v2.ChannelWrite{compose_v2.Write(x, "from-B-payload")},
		})
		return nil, nil
	})
	var cSaw string
	b.AddNode("C", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		cSaw = compose_v2.MustGet(chs, x)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("A", compose_v2.EndNode)
	b.AddEdge("C", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, x)
	err = compose_v2.Run(context.Background(), g, cm)
	if err == nil {
		t.Fatalf("成员写与 payload 写同一个 LastValue 应被拒，实际 err=nil，C 读到 %q（A 的写被静默盖掉）", cSaw)
	}
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("应 wrap ErrRunAborted，实际 %v", err)
	}
	// **正向断言**：正文点出 channel、写它的成员、那条指令的来源
	msg := err.Error()
	// 成员带实例身份渲染（A#i1 / B#i2），按前缀匹配，不钉死 token
	for _, want := range []string{`"swx"`, "成员 A#", "B#", "→ C", "payload"} {
		if !strings.Contains(msg, want) {
			t.Errorf("错误正文缺定位信息 %q：%s", want, msg)
		}
	}
}

func TestSingleWriter_MemberWriteVsPayload_AppendAllowed(t *testing.T) {
	// 反例组：同结构换成 Append——能合并，不拒，且两份都在。
	x := compose_v2.Append[string]("swx_app")
	b := compose_v2.NewBuilder().WithName("sw_member_vs_payload_app")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{Target: "A"}, compose_v2.SendDirective{Target: "B"})
		return nil, nil
	})
	b.AddNode("A", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(x, []string{"from-A"})}, nil
	})
	b.AddNode("B", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{
			Target:  "C",
			Payload: []compose_v2.ChannelWrite{compose_v2.Write(x, []string{"from-B-payload"})},
		})
		return nil, nil
	})
	b.AddNode("C", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("A", compose_v2.EndNode)
	b.AddEdge("C", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, x)
	if err := compose_v2.Run(context.Background(), g, cm); err != nil {
		t.Fatalf("Append 能合并，不该拒：%v", err)
	}
	if got := compose_v2.MustGet(cm.Channels(), x); len(got) != 2 {
		t.Errorf("成员写 + payload 写应都在，实际 %v", got)
	}
}

func TestSingleWriter_MemberWriteVsDeferredPayload_AfterSiblingFailure(t *testing.T) {
	// 兄弟恰好失败过一次 ⟹ 续跑时 frontier 缩成 1 ⟹ 走 executeFrontier 的 fast path。
	// 那条路曾经不填 WriteOwners，于是「成员写 vs payload 写」的检测整个失效——
	// **守卫的结论取决于兄弟是不是恰好失败过**，正是它要消灭的那种侥幸（实测 err=nil、
	// 成员的写被上个超步存档重放的 payload 静默盖掉）。
	x := compose_v2.LastValue[string]("swd_x")
	bFail := true
	b := compose_v2.NewBuilder().WithName("sw_deferred")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "A"},
			compose_v2.SendDirective{Target: "B"},
		)
		return nil, nil
	})
	b.AddNode("A", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{
			Target:  "C",
			Payload: []compose_v2.ChannelWrite{compose_v2.Write(x, "from-A-payload")},
		})
		return nil, nil
	})
	b.AddNode("B", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if bFail {
			return nil, &compose_v2.InterruptError{
				Reason: "swd", Query: &compose_v2.AskQuery{InterruptID: "__swd_q__", Question: "?"},
			}
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(x, "from-B")}, nil
	})
	var cSaw string
	b.AddNode("C", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		cSaw = compose_v2.MustGet(chs, x)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("C", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, x)
	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, cm,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("swd_sid"))); !ok {
		t.Fatal("fixture 没搭对：Run1 应中断，好让 A 的 payload 指令进 DeferredSends、续跑时 frontier 缩成 1")
	}

	bFail = false
	cm2 := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm2, x)
	err2 := compose_v2.Run(ctx, g, cm2, compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("swd_sid"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{"__swd_q__": schema.UserMessage("go")}))
	if err2 == nil {
		t.Fatalf("续跑时 B 的写与重放的 payload 写同一个 LastValue，应被拒；实际 err=nil，C 读到 %q", cSaw)
	}
	if !errors.Is(err2, compose_v2.ErrRunAborted) {
		t.Errorf("应 wrap ErrRunAborted，实际 %v", err2)
	}
	// **正向断言**：正文点出写它的成员与那条指令——只有 fast path 真填了 WriteOwners 才产得出
	msg := err2.Error()
	for _, want := range []string{`"swd_x"`, "成员 B#", "A#", "payload"} {
		if !strings.Contains(msg, want) {
			t.Errorf("错误正文缺定位信息 %q：%s", want, msg)
		}
	}
}
