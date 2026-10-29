package agent_v2_test

// GraphAgent 的会话历史落点（backlog「GraphAgent 没接历史层任何钩子」）。
//
// # 它跟其余 7 编排不是同一个形状
//
// 那 7 个都有静态子列表，配置从子那儿借、补记写在**编译出来的图体内**（Resume 会连着
// 重跑一遍）。GraphAgent 两样都没有：子 agent 由业务 NodeFunc 经 `RunSubAgent` 调起、
// 框架编译期看不见；它的"图体"是业务自己的图，wrapper 这层的补记 Resume 时不重来。
//
// 所以两件事各自解决：
//   - **配置**走运行入口声明 `WithHistoryCtx`（那个出口本来就是为「一个 BasicAgent 都
//     没有」这种形状留的），不另开专属 option
//   - **输入不等成功**：入口登记成待补，排在这次 Run 写进历史的第一批记录前面（图里的子
//     agent 执行中就记自己的问答，到出口才记的话用户那句话排在它们之后）；一批都没有时在
//     两个出口结算（成功 + 可 Resume 的退出）——Resume 入口拿不到原始 input（它被 inputWriter
//     交给业务 channel 了，框架不知道是哪个），只在成功时记 = 中断过的会话永远没有用户那句话。
//     排序的哨兵在 history_graphagent_order_test.go
//
// # 为什么要 ownsHistory 闸门
//
// `settle` 在这套设计里是**调用点参数**不是被调用方的自觉（`AgentAsTool` 就是靠调用点
// 传 false 来避免跟 tool_result 重复的）。GraphAgent 在自己 Run 里无条件补记 = 把那个
// 决定塞回被调用方，实测立刻在「GraphAgent 被当 tool 调」时把产出记两遍。
// 判据是「写入器是不是我建的」——它有天然的生命周期，不像 ctx 信号那样没有清除点。

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// mkEchoGraph 造一个纯 NodeFunc 的图：读入口消息 → 写一条加工过的产出。
// transform 让调用方决定产出内容（要构造「子 agent 产出被透传」时用）。
func mkEchoGraph(t *testing.T, name string, body func(ctx context.Context, in *schema.Message) (*schema.Message, error)) *agent_v2.GraphAgent {
	t.Helper()
	inCh := compose_v2.LastValue[*schema.Message]("__ga_in_" + name + "__")
	outCh := compose_v2.LastValue[*schema.Message]("__ga_out_" + name + "__")

	bld := compose_v2.NewBuilder().WithName("g_" + name)
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, outCh)
	bld.AddNode("work", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		out, err := body(ctx, in)
		if err != nil {
			return nil, err
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
		}),
	)
}

func histDump(msgs []*schema.Message) string {
	var b strings.Builder
	for i, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		b.WriteString("\n  [" + string(rune('0'+i)) + "] " + string(m.Role) + " owner=" + o + " " + strings.TrimSpace(m.Content))
	}
	return b.String()
}

// TestHistGraphAgent_TopLevel_PureNodeFunc 主症状：顶层直接跑一个纯 NodeFunc 的
// GraphAgent，历史此前**全程为空**（图里没有任何 BasicAgent，没人写）。
func TestHistGraphAgent_TopLevel_PureNodeFunc(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ga := mkEchoGraph(t, "pure", func(_ context.Context, in *schema.Message) (*schema.Message, error) {
		return schema.AssistantMessage("【图产出】"), nil
	})

	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_pure")
	if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs := loadHist(t, hs, ctx, "c_pure")
	if len(msgs) != 2 {
		t.Fatalf("顶层纯 NodeFunc 图应留下「输入 + 产出」2 条，实际 %d 条：%s\n"+
			"  0 条 = GraphAgent 没接历史层（它没有静态子列表，那套借配置的机制对它整体没接）",
			len(msgs), histDump(msgs))
	}
	if msgs[0].Role != schema.RoleUser || !strings.Contains(msgs[0].Content, "原话") {
		t.Errorf("第 1 条应是用户输入，实际：%s", histDump(msgs))
	}
	if msgs[1].Role != schema.RoleAssistant || !strings.Contains(msgs[1].Content, "【图产出】") {
		t.Errorf("第 2 条应是图产出，实际：%s", histDump(msgs))
	}
}

// TestHistGraphAgent_TopLevel_WithSubAgent 图里的节点经 RunSubAgent 调外部执行体时，
// 两层各记各的、不互相重复。
func TestHistGraphAgent_TopLevel_WithSubAgent(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ga := mkEchoGraph(t, "withsub", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
		if _, err := agent_v2.RunSubAgent(ctx, "worker", &externalAgent{name: "extW"}, in); err != nil {
			return nil, err
		}
		return schema.AssistantMessage("【图聚合】"), nil
	})

	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_sub")
	if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs := loadHist(t, hs, ctx, "c_sub")
	// 数 user 角色的条数，不按子串——外部执行体的产出回显了输入，子串断言会被它命中。
	users := 0
	for _, m := range msgs {
		if m.Role == schema.RoleUser {
			users++
		}
	}
	if users != 1 {
		t.Errorf("原始输入应恰 1 条 user 记录，实际 %d 条：%s\n"+
			"  2 条 = GraphAgent 在内层已经记过之后又补了一遍（范围回执没挡住）", users, histDump(msgs))
	}
	assertCount(t, msgs, "【图聚合】", 1)
	assertHas(t, msgs, "【外部产出】")
	// **内层归属不带图名前缀**——顶层 GraphAgent 只开范围回执、不往 ctx 推 slot。
	// 推了会把图名变成所有后代归属的根段：实测代价是「顶层图改个名，它下面所有子的历史
	// 全部变孤儿」+「同一张图两种配法（WithHistoryCtx / WithHistory 挂叶子）归属互不相认」。
	// 详见 settleHistoryTopLevel 的说明。
	if o, _ := msgs[0].Annotations[agent_v2.HistoryAgentAnnotation].(string); o != "worker" {
		t.Errorf("内层归属应是裸的 worker（顶层图名不进后代归属），实际 %q：%s\n"+
			"  带上前缀 = 顶层往 ctx 推了 slot，图名成了会话身份的一部分、改名即失忆", o, histDump(msgs))
	}
	// 图自己那层的归属回落到 agent 名
	if o, _ := msgs[len(msgs)-1].Annotations[agent_v2.HistoryAgentAnnotation].(string); o != "withsub" {
		t.Errorf("图自己那层归属应回落 agent 名 withsub，实际 %q：%s", o, histDump(msgs))
	}
}

// TestHistGraphAgent_TopLevel_InterruptResume_KeepsInput 钉住「输入在入口就记」。
//
// sanity-revert：把 Run 里那句 settleHistorySlot(ctx, name, input, nil) 挪到成功之后
// → 中断的那次不记，Resume 又拿不到原始 input → 用户那句话永久缺席。
func TestHistGraphAgent_TopLevel_InterruptResume_KeepsInput(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	var asked bool
	ga := mkEchoGraph(t, "intr", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
		if !asked {
			asked = true
			return nil, &compose_v2.InterruptError{
				Reason: "need_approval",
				Query:  &compose_v2.AskQuery{InterruptID: "ga_q", Question: "approve?"},
			}
		}
		return schema.AssistantMessage("【图产出】"), nil
	})

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_intr")
	ctx = compose_v2.WithCheckpointerCtx(ctx, cper)

	sess := agent_v2.NewRunSession()
	_, err := ga.Run(ctx, sess, schema.UserMessage("原话"))
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("Run 应中断, got %v", err)
	}

	if _, err := ga.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{"ga_q": schema.UserMessage("ok")})); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	msgs := loadHist(t, hs, ctx, "c_intr")
	assertCount(t, msgs, "原话", 1)
	assertCount(t, msgs, "【图产出】", 1)
	if len(msgs) < 2 || msgs[0].Role != schema.RoleUser {
		t.Errorf("中断过的会话里用户那句话必须在、且在最前，实际：%s\n"+
			"  缺席 = 输入被挪到「成功之后」才记，而 Resume 入口拿不到原始 input\n"+
			"  （它被 inputWriter 交给业务 channel 了，框架不知道是哪个）", histDump(msgs))
	}
}

// TestHistGraphAgent_NestedUnderSequential_DoesNotDoubleRecord 嵌套时 GraphAgent
// **不**自己补记——写入器是父建的，父的调用点负责补记。
func TestHistGraphAgent_NestedUnderSequential_DoesNotDoubleRecord(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ga := mkEchoGraph(t, "nested", func(_ context.Context, in *schema.Message) (*schema.Message, error) {
		return schema.AssistantMessage("【图产出】"), nil
	})

	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"))
	seq.AddStep("g", ga)
	seq.AddStep("tail", &externalAgent{name: "extTail"})

	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_nest")
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs := loadHist(t, hs, ctx, "c_nest")
	// 数「归属是 g 的 assistant 产出」条数——不能按子串数，Sequential 给下一步的派生
	// 输入里带着上一步的产出原文，子串断言会被它命中（这个坑值得留个记号）。
	n := 0
	for _, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		if o == "g" && m.Role == schema.RoleAssistant {
			n++
		}
		if o == "nested" {
			t.Errorf("嵌套时 GraphAgent 不该用自己的 agent 名当归属（写入器是父建的，"+
				"父的调用点负责结算）——归属应是父给的注册位置 g。实际：%s", histDump(msgs))
		}
	}
	if n != 1 {
		t.Errorf("g 这一步的产出应恰 1 条，实际 %d 条：%s\n"+
			"  2 条 = GraphAgent 自己补记了一遍、父的调用点又补了一遍", n, histDump(msgs))
	}
}
