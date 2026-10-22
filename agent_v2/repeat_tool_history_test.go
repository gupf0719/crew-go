package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestRepeatTool_ReminderStaysOutOfHistory 钉住**重复提醒不进 append-only 会话历史**。
//
// 提醒是从历史里已有的东西（那几次工具调用本身）完全可重算的派生物，所以打
// [agent_v2.DerivedInputAnnotation]。但打 marker 不等于有人读它——修复前
// `isDerivedInput` 的三个消费点（编排边界的 input/final、preamble 的 user input）
// **都不在 tools 那条路上**，而 tools 节点整批无过滤入库。
//
// 后果不是多存一条：提醒以 `role=user` 留在历史里，**下一个回合被原样重放进新回合的
// prompt**，此后每轮都喂给模型。多轮对话里它永久贴着。
//
// ⚠️ 断言分两路：历史里没有它（直接证据）+ 下一个回合的 prompt 里没有它（那才是
// 真正会伤到业务的地方）。
func TestRepeatTool_ReminderStaysOutOfHistory(t *testing.T) {
	rt := &recordTool{name: "biz_tool"}
	tk := agent_v2_newToolkit()
	tk.Register(rt)

	hs := agent_v2.NewInMemoryHistoryStore()
	var seen []*schema.Message
	mkAgent := func(resps []*schema.Message) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("worker"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(8),
			agent_v2.WithHistory(hs, "tenant-a"),
			agent_v2.WithRepeatToolReminder(&agent_v2.RepeatToolConfig{
				Enabled: true, Thresholds: []int{3},
			}),
			agent_v2.WithModel(newStubModelCapturing(resps, &seen)),
		)
	}

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())

	// 回合一：模型一根筋捶同一个调用，必然触发提醒。
	r1 := make([]*schema.Message, 0, 6)
	for i := 0; i < 5; i++ {
		r1 = append(r1, oneToolCall("biz_tool"))
	}
	r1 = append(r1, schema.AssistantMessage("好吧我停下"))
	sess := agent_v2.NewRunSession()
	if _, err := mkAgent(r1).Run(ctx, sess, schema.UserMessage("干活")); err != nil {
		t.Fatalf("Run 1: %v", err)
	}

	const reminderMark = "[system_reminder]"
	// 前置假设：提醒真的产生了。没产生的话下面两条恒真、什么都没测。
	fired := false
	for _, m := range seen {
		if m != nil && strings.Contains(m.Content, reminderMark) {
			fired = true
			break
		}
	}
	if !fired {
		t.Fatal("前置假设不成立：这一轮压根没触发重复提醒 ⟹ 下面的「历史里没有它」恒真，什么都没测")
	}

	// 证据一：append-only 历史里不该有它。
	recs, err := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-1"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, m := range recs {
		if m != nil && strings.Contains(m.Content, reminderMark) {
			t.Errorf("重复提醒进了 append-only 会话历史：%.60q\n"+
				"  → 它打了 DerivedInputAnnotation，但 tools 那条路没人读这个 marker", m.Content)
		}
	}

	// 证据二（真正伤到业务的那个）：下一个回合的 prompt 里不该有它。
	seen = nil
	sess2 := agent_v2.NewRunSession()
	if _, err := mkAgent([]*schema.Message{schema.AssistantMessage("第二回合答复")}).
		Run(ctx, sess2, schema.UserMessage("换个话题")); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	for _, m := range seen {
		if m != nil && strings.Contains(m.Content, reminderMark) {
			t.Errorf("上一个回合的重复提醒被重放进了**新回合**的 prompt：%.60q\n"+
				"  → 多轮对话里它会永久贴着，此后每一轮都喂给模型", m.Content)
		}
	}
}
