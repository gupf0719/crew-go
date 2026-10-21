package agent_v2_test

// GraphAgent 历史落点的补充形状——前一批 4 条覆盖主路径（纯 NodeFunc / 有子 agent /
// 中断→Resume / 嵌套在 Sequential 下），这批补 4 个边角：
//
//   - 两层 GraphAgent 嵌套（透传的产出不重记；⚠️ 它 pin 的不是 ownsHistory 闸门，见该用例注释）
//   - 同 ConvID 连跑两个 Run（第二轮的输入记不记得上、会不会被"已在历史里"误判吞掉）
//   - outputReader 返 nil（别记一条空的 assistant）
//   - WithoutHistoryCtx 摘除（顶层结算也得认这个开关）
//
// 这几条本来派给审计 agent 跑，两个 agent 撞上配额挂了，改自己实测补上。

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// gaOf 造一个单节点图包成 GraphAgent，body 决定产出（返 nil 表示不产出）。
func gaOf(t *testing.T, name string, body func(ctx context.Context, in *schema.Message) (*schema.Message, error)) *agent_v2.GraphAgent {
	t.Helper()
	inCh := compose_v2.LastValue[*schema.Message]("__gs_in_" + name + "__")
	outCh := compose_v2.LastValue[*schema.Message]("__gs_out_" + name + "__")
	bld := compose_v2.NewBuilder().WithName("gs_" + name)
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

func ownersOf(msgs []*schema.Message) string {
	var b strings.Builder
	for i, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		b.WriteString("\n  [" + string(rune('0'+i)) + "] " + string(m.Role) + " owner=" + o + " " + strings.TrimSpace(m.Content))
	}
	return b.String()
}

// TestHistGAShape_NestedGraphInGraph 两层 GraphAgent：内层产出只记一份。
//
// ⚠️ **它 pin 的不是 ownsHistory 闸门**——我最初以为是，实测撤掉闸门（改成
// `ownsHistory = historyWriterFrom(ctx) != nil`，让内层也自认顶层）本用例**照过**。
// 真正挡住这里的是全局登记表：内层结算记了那条产出，外层出口透传的是**同一个指针**，
// `alreadyRecorded(final)` 命中就跳过了。
//
// 闸门实际只有一个哨兵守着——`TestHistBoundary_NestedInToolCall_StillRecords`
// （GraphAgent 被当 tool 调时它的产出会由父的 tools 节点写成 tool_result，自己再记
// 一遍就是重复）。撤掉闸门时全 `agent_v2` 只有那一条红。哨兵薄，改闸门时别只看本文件。
//
// 本用例仍然值得留：「透传产出不重记」是独立的不变量，且它覆盖的是两层图这个
// 前一批 4 条没碰的形状。
func TestHistGAShape_NestedGraphInGraph(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	inner := gaOf(t, "inner", func(_ context.Context, in *schema.Message) (*schema.Message, error) {
		return schema.AssistantMessage("【内层产出】"), nil
	})
	outer := gaOf(t, "outer", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
		ev, err := agent_v2.RunSubAgent(ctx, "innerNode", inner, in)
		if err != nil {
			return nil, err
		}
		return agent_v2.FinalMessage(ev), nil // 透传内层产出
	})

	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_nn")
	if _, err := outer.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "c_nn"})
	n := 0
	for _, m := range msgs {
		if strings.Contains(m.Content, "【内层产出】") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("内层产出应恰 1 条，实际 %d 条：%s\n"+
			"  2 条 = 透传的产出被记了两遍（外层出口该靠全局登记表认出「这条已经记过」）", n, ownersOf(msgs))
	}
}

// TestHistGAShape_TwoRunsSameConv 同 ConvID 连跑两个 Run——第二轮的输入必须记得上。
//
// 判据里有一道 inputAlreadyInHistorySlot：若它问的是「这条身份**曾经**在历史里吗」而不是
// 「**这个 Run** 记过吗」，第二轮复用同一个 input 对象时就会被吞掉（历史层踩过这个坑）。
func TestHistGAShape_TwoRunsSameConv(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ga := gaOf(t, "tworuns", func(_ context.Context, in *schema.Message) (*schema.Message, error) {
		return schema.AssistantMessage("答:" + in.Content), nil
	})
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_2r")

	for _, q := range []string{"第一问", "第二问"} {
		if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(q)); err != nil {
			t.Fatalf("Run(%s): %v", q, err)
		}
	}
	msgs, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "c_2r"})
	if len(msgs) != 4 {
		t.Fatalf("两轮各「输入+产出」应共 4 条，实际 %d 条：%s", len(msgs), ownersOf(msgs))
	}
	for i, want := range []string{"第一问", "答:第一问", "第二问", "答:第二问"} {
		if !strings.Contains(msgs[i].Content, want) {
			t.Errorf("第 %d 条应含 %q：%s", i, want, ownersOf(msgs))
		}
	}
}

// TestHistGAShape_NilOutput outputReader 返 nil 时不该记一条空 assistant。
func TestHistGAShape_NilOutput(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ga := gaOf(t, "nilout", func(_ context.Context, _ *schema.Message) (*schema.Message, error) {
		return nil, nil // 不写出口 channel ⟹ outputReader 拿到 nil
	})
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_nil")
	if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "c_nil"})
	if len(msgs) != 1 || msgs[0].Role != schema.RoleUser {
		t.Errorf("产出为 nil 时只该留输入 1 条，实际 %d 条：%s\n"+
			"  多出的空 assistant 会让下一轮模型读到一句空话", len(msgs), ownersOf(msgs))
	}
}

// TestHistGAShape_Suppressed WithoutHistoryCtx 摘除时顶层结算也得认这个开关。
func TestHistGAShape_Suppressed(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ga := gaOf(t, "supp", func(_ context.Context, _ *schema.Message) (*schema.Message, error) {
		return schema.AssistantMessage("【产出】"), nil
	})
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_supp")
	ctx = agent_v2.WithoutHistoryCtx(ctx)
	if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "c_supp"})
	if len(msgs) != 0 {
		t.Errorf("被 WithoutHistoryCtx 摘除时一条都不该记，实际 %d 条：%s\n"+
			"  记了 = 顶层结算这条路绕过了摘除开关（内部决策 LLM 靠它挡在历史外）",
			len(msgs), ownersOf(msgs))
	}
}
