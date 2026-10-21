package agent_v2_test

// 两条「作用域」问题——审计定位、我一开始判为「要动更宽的设计」而没动手，实际范围比我
// 断言的小。共同根因：**HITL 会话跨多次调用，而 ownsHistory / 范围回执 / 全局登记表都只
// 作用于一次调用**。

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

func scopeHist(t *testing.T, hs agent_v2.HistoryStore, ctx context.Context, conv string) []*schema.Message {
	t.Helper()
	msgs, err := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: conv})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return msgs
}

func scopeDump(msgs []*schema.Message) string {
	var b strings.Builder
	for i, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		b.WriteString("\n  [" + string(rune('0'+i)) + "] " + string(m.Role) + " owner=" + o + " " + strings.TrimSpace(m.Content))
	}
	return b.String()
}

// TestHistGAScope_PassthroughAcrossResume 顶层图透传子 agent 产出**指针**，且中断跨 Resume。
//
// 产出侧两道守卫都是 per-call：范围回执每次 Run/Resume 新建、全局登记表挂在写入器上同样
// 每次新建。而 Resume **不重跑已完成节点** ⟹ 回执不会再被 bump ⟹
// `!(producedHistoryHere && alreadyRecorded)` 的前半永远是 false，整道守卫短路 ⟹ Run 里
// 子记过的那条产出，Resume 出口又记一次，**同一指针两个归属**。
//
// 单 Run 同形状对照只记 1 条 —— 所以这是违反既定意图（「透传的跳过」）不是取舍。
func TestHistGAScope_PassthroughAcrossResume(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	inCh := compose_v2.LastValue[*schema.Message]("__sc_pt_in__")
	outCh := compose_v2.LastValue[*schema.Message]("__sc_pt_out__")
	gateCh := compose_v2.LastValue[*schema.Message]("__sc_pt_gate__")

	leaf := agent_v2.NewBasicAgent(
		agent_v2.WithName("leaf"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("【子产出】")})),
		agent_v2.WithMaxRounds(2))

	var asked bool
	bld := compose_v2.NewBuilder().WithName("sc_pt")
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, outCh)
	compose_v2.WithChannel(bld, gateCh)
	bld.AddNode("n1", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		ev, err := agent_v2.RunSubAgent(ctx, "n1", leaf, in)
		if err != nil {
			return nil, err
		}
		// **把子的 final 指针原样写进出口**——透传，不加工
		return []compose_v2.ChannelWrite{
			compose_v2.Write(outCh, agent_v2.FinalMessage(ev)),
			compose_v2.Write(gateCh, schema.AssistantMessage("go")),
		}, nil
	})
	bld.AddNode("n2", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if !asked {
			asked = true
			return nil, &compose_v2.InterruptError{
				Reason: "approve",
				Query:  &compose_v2.AskQuery{InterruptID: "sc_q", Question: "ok?"},
			}
		}
		return nil, nil // 不碰出口 ⟹ final 仍是 n1 写进去的那个指针
	})
	bld.AddEdge(compose_v2.StartNode, "n1")
	bld.AddEdge("n1", "n2")
	bld.AddEdge("n2", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ga := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("ptGraph"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_sc_pt")
	ctx = compose_v2.WithCheckpointerCtx(ctx, cper)

	sess := agent_v2.NewRunSession()
	if _, err := ga.Run(ctx, sess, schema.UserMessage("我的原话")); err == nil {
		t.Fatal("Run 应中断")
	}
	if _, err := ga.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{"sc_q": schema.UserMessage("ok")})); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	msgs := scopeHist(t, hs, ctx, "c_sc_pt")
	n := 0
	for _, m := range msgs {
		if strings.Contains(m.Content, "【子产出】") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("透传的子产出应恰 1 条，实际 %d 条：%s\n"+
			"  2 条 = 产出侧两道守卫都是 per-call，Resume 不重跑已完成节点 ⟹ 回执不再 bump\n"+
			"  ⟹ 整道守卫短路。同一份内容两个归属，下一轮模型读到两遍", n, scopeDump(msgs))
	}
}

// TestHistGAScope_NestedInterrupt_DirectChildResume 嵌套图中断后，调用方**直接** Resume 子
// agent（R31 特意支持的路径，不经父编排）。
//
// 链条：嵌套 ⟹ GraphAgent `ownsHistory=false` ⟹ 跳过自己的中断出口结算；而父的边界补记是
// **成功后**才做的、这次中断了也没触发 ⟹ 两边都没记。之后直接 `.Resume` 时
// `ownsHistory=true` 但 Resume 的 input 恒 nil ⟹ 用户原话永久缺席。
//
// ⚠️ **经父 Resume 那条路是好的**（父的 step 重跑成功、父补记，输入随之落下）——所以问题
// 只出在「直接 Resume 子」这一条路上，范围比「7 编排的中断路径都不结算输入」窄得多。
// 我一开始把它说成后者，是没实测就下的断言。
func TestHistGAScope_NestedInterrupt_DirectChildResume(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	inCh := compose_v2.LastValue[*schema.Message]("__sc_ni_in__")
	outCh := compose_v2.LastValue[*schema.Message]("__sc_ni_out__")
	var asked bool
	bld := compose_v2.NewBuilder().WithName("sc_ni")
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, outCh)
	bld.AddNode("work", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if !asked {
			asked = true
			return nil, &compose_v2.InterruptError{
				Reason: "approve",
				Query:  &compose_v2.AskQuery{InterruptID: "ni_q", Question: "ok?"},
			}
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("【图产出】"))}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "work")
	bld.AddEdge("work", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ga := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("gaLeaf"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))

	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seqOuter"))
	seq.AddStep("theStep", ga)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_sc_ni")
	ctx = compose_v2.WithCheckpointerCtx(ctx, cper)

	sess := agent_v2.NewRunSession()
	if _, err := seq.Run(ctx, sess, schema.UserMessage("我的原话")); err == nil {
		t.Fatal("父 Run 应中断")
	}

	// 直接 Resume 子（带父推的 ns —— R31 硬化过这条路径）
	childCtx := compose_v2.WithGraphNS(ctx, "theStep")
	if _, err := ga.Resume(childCtx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{"ni_q": schema.UserMessage("ok")})); err != nil {
		t.Fatalf("直接 Resume 子: %v", err)
	}

	msgs := scopeHist(t, hs, ctx, "c_sc_ni")
	users := 0
	for _, m := range msgs {
		if m.Role == schema.RoleUser && m.Content == "我的原话" {
			users++
		}
	}
	if users != 1 {
		t.Errorf("直接 Resume 子之后用户原话应恰 1 条，实际 %d 条：%s\n"+
			"  0 条 = 中断那次两边都没结算（子 ownsHistory=false 跳过、父的补记是成功后才做），\n"+
			"  而直接 Resume 的 input 恒 nil ⟹ 永久缺席", users, scopeDump(msgs))
	}
}

// TestHistGAScope_NestedInterrupt_ParentResume 上一条的**对照面**：经父 Resume 时不能因为
// 子在中断出口记过一次就变成两条。
//
// 这条是上一个修复的承重哨兵——子在中断出口记了输入，父后续补记时必须靠跨 Run 那道守卫
// （`inputAlreadyInHistorySlot`，同一条流上查落盘记录）跳过，否则修一个引入一个。
func TestHistGAScope_NestedInterrupt_ParentResume(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	inCh := compose_v2.LastValue[*schema.Message]("__sc_pr_in__")
	outCh := compose_v2.LastValue[*schema.Message]("__sc_pr_out__")
	var asked bool
	bld := compose_v2.NewBuilder().WithName("sc_pr")
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, outCh)
	bld.AddNode("work", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if !asked {
			asked = true
			return nil, &compose_v2.InterruptError{
				Reason: "approve",
				Query:  &compose_v2.AskQuery{InterruptID: "pr_q", Question: "ok?"},
			}
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("【图产出】"))}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "work")
	bld.AddEdge("work", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ga := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("gaLeafPR"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))

	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seqPR"))
	seq.AddStep("theStep", ga)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_sc_pr")
	ctx = compose_v2.WithCheckpointerCtx(ctx, cper)

	sess := agent_v2.NewRunSession()
	if _, err := seq.Run(ctx, sess, schema.UserMessage("我的原话")); err == nil {
		t.Fatal("父 Run 应中断")
	}
	if _, err := seq.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{"pr_q": schema.UserMessage("ok")})); err != nil {
		t.Fatalf("父 Resume: %v", err)
	}

	msgs := scopeHist(t, hs, ctx, "c_sc_pr")
	users, outs := 0, 0
	for _, m := range msgs {
		if m.Role == schema.RoleUser && m.Content == "我的原话" {
			users++
		}
		if strings.Contains(m.Content, "【图产出】") {
			outs++
		}
	}
	if users != 1 || outs != 1 {
		t.Errorf("经父 Resume 应是「问 1 答 1」，实际 问 %d / 答 %d：%s\n"+
			"  问 2 条 = 子在中断出口记了一次、父补记又记一次（跨 Run 那道守卫没挡住）",
			users, outs, scopeDump(msgs))
	}
	// 归属必须是父给的位置，不是裸 agent 名
	for _, m := range msgs {
		if o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string); o != "theStep" {
			t.Errorf("归属应恒是父给的位置 theStep，实际 %q：%s\n"+
				"  裸 agent 名 = 中断出口用了 TopLevel 那条；同一位置两种归属比丢失更难查",
				o, scopeDump(msgs))
			break
		}
	}
}
