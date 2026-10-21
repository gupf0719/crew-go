package agent_v2_test

// 顶层 GraphAgent 结算判据的四条回归——两轮独立审计各自实测出来的。
//
// 共同的根因值得点明：**边界补记那套判据的前提是「执行体收到的就是这条输入」**，对 7 编排
// 成立（它们把输入原样传给子），对 `settleHistoryTopLevel` 不成立——那里的「执行体」是整张
// 图，而图底下记的是后代**自己的派生输入**。「有人记过东西」≠「有人记过这条输入」。

import (
	"context"
	"errors"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// gateGraph 造图：body 拿到入口消息，返 (产出, error)。
func gateGraph(t *testing.T, name string, body func(ctx context.Context, in *schema.Message) (*schema.Message, error)) *agent_v2.GraphAgent {
	t.Helper()
	inCh := compose_v2.LastValue[*schema.Message]("__gg_in_" + name + "__")
	outCh := compose_v2.LastValue[*schema.Message]("__gg_out_" + name + "__")
	bld := compose_v2.NewBuilder().WithName("gg_" + name)
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, outCh)
	bld.AddNode("work", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		out, err := body(ctx, in)
		if err != nil {
			return nil, err
		}
		if out == nil {
			return nil, nil
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, out)}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "work")
	bld.AddEdge("work", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName(name),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
}

func gateHist(t *testing.T, hs agent_v2.HistoryStore, ctx context.Context, conv string) []*schema.Message {
	t.Helper()
	msgs, err := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: conv})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return msgs
}

func countRole(msgs []*schema.Message, role schema.MessageRole, sub string) int {
	n := 0
	for _, m := range msgs {
		if m.Role == role && (sub == "" || m.Content == sub) {
			n++
		}
	}
	return n
}

// TestHistGAGate_DerivedDelegation_KeepsOriginalInput 图把请求**拆解**后委派，而不是原样
// 转发——用户原话仍须留下。
//
// 判据若问「后代记过东西吗」（`producedHistoryHere`），子 agent 记的是它自己的派生输入
// （"子任务:查库存"），闸门就以为「输入有人管了」⟹ 用户原话 0 条。而委派是 GraphAgent
// 的主要用法之一，不是边角。
func TestHistGAGate_DerivedDelegation_KeepsOriginalInput(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ga := gateGraph(t, "derive", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
		// 注意：传给子的是**新构造的派生输入**，不是 in
		if _, err := agent_v2.RunSubAgent(ctx, "worker", &externalAgent{name: "ext"},
			schema.UserMessage("子任务:查库存")); err != nil {
			return nil, err
		}
		return schema.AssistantMessage("【图结论】"), nil
	})

	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_derive")
	if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("我的原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs := gateHist(t, hs, ctx, "c_derive")
	if got := countRole(msgs, schema.RoleUser, "我的原话"); got != 1 {
		t.Errorf("用户原话应恰 1 条，实际 %d 条（历史 %d 条）\n"+
			"  0 条 = 顶层的输入闸门问的是「后代记过东西吗」，而后代记的是它自己的派生输入\n"+
			"  ——「有人记过东西」≠「有人记过这条输入」。下一轮模型看到的会话里用户从没说过话",
			got, len(msgs))
		for i, m := range msgs {
			o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
			t.Logf("  [%d] %s owner=%s %s", i, m.Role, o, m.Content)
		}
	}
}

// TestHistGAGate_DerivedDelegation_InterruptResume 同上但走中断→Resume。
//
// 这条更要紧：中断出口那次结算本来就是**专门**为「Resume 拿不到原始 input」加的，而它走
// 同一道闸 ⟹ 图只要委派了任何东西，那个 fix 就被绕过。
func TestHistGAGate_DerivedDelegation_InterruptResume(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	var asked bool
	ga := gateGraph(t, "derive_intr", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
		if _, err := agent_v2.RunSubAgent(ctx, "worker", &externalAgent{name: "ext"},
			schema.UserMessage("子任务:查库存")); err != nil {
			return nil, err
		}
		if !asked {
			asked = true
			return nil, &compose_v2.InterruptError{
				Reason: "approve",
				Query:  &compose_v2.AskQuery{InterruptID: "gate_q", Question: "ok?"},
			}
		}
		return schema.AssistantMessage("【图结论】"), nil
	})

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_derive_intr")
	ctx = compose_v2.WithCheckpointerCtx(ctx, cper)

	sess := agent_v2.NewRunSession()
	if _, err := ga.Run(ctx, sess, schema.UserMessage("我的原话")); err == nil {
		t.Fatal("Run 应中断")
	}
	if _, err := ga.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{"gate_q": schema.UserMessage("ok")})); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	msgs := gateHist(t, hs, ctx, "c_derive_intr")
	if got := countRole(msgs, schema.RoleUser, "我的原话"); got != 1 {
		t.Errorf("中断→Resume 后用户原话应恰 1 条，实际 %d 条（历史 %d 条）\n"+
			"  中断出口那次结算是专门为「Resume 拿不到原始 input」加的，被同一道闸绕过了",
			got, len(msgs))
	}
}

// TestHistGAGate_PassthroughOutput_NoDoubleInput outputReader 在出口 channel 空时回落返
// input（`if out == nil { return in }`）——`GraphOutputReader` 自己的 godoc 就建议业务别返
// nil，这是相当自然的写法。此时输入不该被记两遍。
//
// 机制：同一次结算里 `msgs` 变成 `[input, input]`。产出那道守卫是
// `!(producedHistoryHere && alreadyRecorded(final))`，纯 NodeFunc 图里 `producedHistoryHere`
// 为 false，`&&` 把整道守卫短路掉了。
func TestHistGAGate_PassthroughOutput_NoDoubleInput(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	inCh := compose_v2.LastValue[*schema.Message]("__gg_pt_in__")
	outCh := compose_v2.LastValue[*schema.Message]("__gg_pt_out__")
	bld := compose_v2.NewBuilder().WithName("gg_pt")
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, outCh)
	bld.AddNode("work", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil // 不写出口
	})
	bld.AddEdge(compose_v2.StartNode, "work")
	bld.AddEdge("work", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ga := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("ptga"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			if out, _ := compose_v2.Get(chs, outCh); out != nil {
				return out
			}
			in, _ := compose_v2.Get(chs, inCh) // 回落：把输入当产出返
			return in
		}))

	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_pt")
	if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs := gateHist(t, hs, ctx, "c_pt")
	if len(msgs) != 1 {
		t.Errorf("产出回落成 input 时该只留 1 条，实际 %d 条\n"+
			"  2 条 = 同一次结算里 msgs 成了 [input, input]；下一轮模型读到问题两遍、中间没有答案",
			len(msgs))
		for i, m := range msgs {
			t.Logf("  [%d] %s %s", i, m.Role, m.Content)
		}
	}
}

// TestHistGAGate_SoftCancel_KeepsInput 软取消是**文档承诺可 Resume**的退出档（cp 留
// Pending），但它返的不是 InterruptError。只认中断的闸门会漏掉它 ⟹ Resume 成功后历史落成
// 「有答无问」。
//
// 同类还有 `CondFailRerunFrom`（返业务错但 cp 留 Pending）——判据该是「框架对这个错的处置
// 意图是不是留 Pending」，不是枚举错类型。
func TestHistGAGate_SoftCancel_KeepsInput(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	sig := compose_v2.NewSoftCancelSignal()

	// **必须是多超步图**：软取消在**超步边界**才生效，末超步 Trigger 是 no-op。
	// 两个节点串成两个超步，第一个节点里 Trigger ⟹ 第二个超步之前停下。
	inCh := compose_v2.LastValue[*schema.Message]("__gg_sc_in__")
	midCh := compose_v2.LastValue[*schema.Message]("__gg_sc_mid__")
	outCh := compose_v2.LastValue[*schema.Message]("__gg_sc_out__")
	bld := compose_v2.NewBuilder().WithName("gg_softc")
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, midCh)
	compose_v2.WithChannel(bld, outCh)
	var triggered bool
	bld.AddNode("first", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if !triggered {
			triggered = true
			sig.Trigger("用户点了停止")
		}
		in, _ := compose_v2.Get(chs, inCh)
		return []compose_v2.ChannelWrite{compose_v2.Write(midCh, in)}, nil
	})
	bld.AddNode("second", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("【图结论】"))}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "first")
	bld.AddEdge("first", "second")
	bld.AddEdge("second", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ga := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("scga"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_softc")
	ctx = compose_v2.WithCheckpointerCtx(ctx, cper)

	sess := agent_v2.NewRunSession()
	_, err = ga.Run(compose_v2.WithSoftCancelCtx(ctx, sig), sess, schema.UserMessage("我的原话"))
	if err == nil || !errors.Is(err, compose_v2.ErrSoftCanceled) {
		t.Fatalf("Run 应返 ErrSoftCanceled，实际 %v", err)
	}
	if got := countRole(gateHist(t, hs, ctx, "c_softc"), schema.RoleUser, "我的原话"); got != 1 {
		t.Fatalf("软取消退出后用户原话应恰 1 条，实际 %d 条\n"+
			"  0 条 = 闸门只认 InterruptError，漏掉了其它「cp 留 Pending、之后能 Resume」的档位\n"+
			"  （软取消是文档承诺可 Resume 的档，CondFailRerunFrom 同理）", got)
	}

	// 续跑：不带软取消信号，应跑完
	if _, err := ga.Resume(ctx, sess.RunID); err != nil {
		t.Fatalf("Resume 应成功, got %v", err)
	}
	msgs := gateHist(t, hs, ctx, "c_softc")
	if countRole(msgs, schema.RoleUser, "我的原话") != 1 || countRole(msgs, schema.RoleAssistant, "【图结论】") != 1 {
		t.Errorf("续跑后应是「问 1 答 1」，实际 %d 条：", len(msgs))
		for i, m := range msgs {
			t.Logf("  [%d] %s %s", i, m.Role, m.Content)
		}
	}
}

// TestHistGAGate_ReRunSameSid_SamePointer_NoDoubleInput 钉住「跨 Run 输入去重那道守卫在
// 顶层路径上真的生效」。
//
// 它此前是**死代码**：`settleHistoryAt` 的 slot 是显式参数，顶层那条路上等于 agentName，
// 而守卫内部自己去 `HistorySlot(ctx)` 取（顶层恒空串）⟹ 拿空串比对 owner=agentName 的
// 记录，恒不命中。
//
// 可观测后果就是本用例：同一个 sid 上 Run → 中断 → 再 Run，**即便调用方传的是同一个
// message 指针**，用户那句话也被记两遍。（换成新构造的消息仍会记两遍——那是身份不同，
// 框架确实分不出来，不在本用例范围。）
func TestHistGAGate_ReRunSameSid_SamePointer_NoDoubleInput(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	var asked bool
	ga := gateGraph(t, "rerun", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
		if !asked {
			asked = true
			return nil, &compose_v2.InterruptError{
				Reason: "approve",
				Query:  &compose_v2.AskQuery{InterruptID: "rerun_q", Question: "ok?"},
			}
		}
		return schema.AssistantMessage("【图结论】"), nil
	})

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_rerun")
	ctx = compose_v2.WithCheckpointerCtx(ctx, cper)

	// **同一个指针**跑两次 Run（业务复用 session 对象是有的写法）
	theInput := schema.UserMessage("我的原话")
	sess := agent_v2.NewRunSession()
	if _, err := ga.Run(ctx, sess, theInput); err == nil {
		t.Fatal("Run 1 应中断")
	}
	if _, err := ga.Run(ctx, sess, theInput); err != nil {
		t.Fatalf("Run 2: %v", err)
	}

	msgs := gateHist(t, hs, ctx, "c_rerun")
	if got := countRole(msgs, schema.RoleUser, "我的原话"); got != 1 {
		t.Errorf("同指针重跑时用户原话应恰 1 条，实际 %d 条（历史 %d 条）\n"+
			"  2 条 = 跨 Run 去重那道守卫自己从 ctx 取 slot（顶层恒空串），比对不上\n"+
			"  owner=agentName 的记录 ⟹ 在顶层路径上是死代码", got, len(msgs))
		for i, m := range msgs {
			o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
			t.Logf("  [%d] %s owner=%s %s", i, m.Role, o, m.Content)
		}
	}
}
