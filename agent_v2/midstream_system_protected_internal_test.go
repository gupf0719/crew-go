package agent_v2

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"crew-go/schema"
)

// TestMidStreamSystem_ProtectedByBothCompactionLayers 钉住「调用方塞在**对话中段**的
// system 消息不被压缩层丢掉 / 摘要掉」。
//
// **bug 历史**：两层都靠「前导位置」认 system——从 `messages[0]` 起连续的才不动。
// 那个判据成立的前提是「system 只会出现在最前」，而调用方（或历史回放）完全可以把
// 一条 system 放在对话中段，它当场降级成一个普通可丢组。
//
// agent 自己的 system prompt 已经不在这条消息流里了（走 `BasicAgentPreambleOutput.System`），
// 所以这条从 🔴 掉到 🟡——**但判据形状没变**，剩下的受害者是调用方自己的 system。
//
// 判据改成「整组都是 system ⟹ 保护」，位置无关。两层各有一条断言：丢（紧急压缩）
// 与摘要（AutoCompact），少任何一条那一层都可能单独退化。
func TestMidStreamSystem_ProtectedByBothCompactionLayers(t *testing.T) {
	mid := schema.SystemMessage("CALLER-MIDSTREAM-RULE")
	build := func() []*schema.Message {
		return []*schema.Message{
			schema.SystemMessage("leading"),
			schema.UserMessage("q1 " + strings.Repeat("x", 4000)),
			schema.AssistantMessage("a1 " + strings.Repeat("y", 4000)),
			mid, // ← 中段，不在前导块里
			schema.UserMessage("q2 " + strings.Repeat("x", 4000)),
			schema.AssistantMessage("a2 " + strings.Repeat("y", 4000)),
			schema.UserMessage("当前问题"),
		}
	}

	// 组里混着 system 和别的：`RoleSystem` 恒开新组、但 `RoleAssistant` **不**开，
	// 所以「system 紧跟一条无 RunID 的 assistant」会混成同一组。判据写成「整组都是
	// system」时这种组漏保护——实测过，第一版就是这么错的。
	t.Run("system 紧跟无 RunID 的 assistant，混成一组也要保护", func(t *testing.T) {
		in := []*schema.Message{
			schema.SystemMessage("leading"),
			schema.UserMessage("q1 " + strings.Repeat("x", 4000)),
			mid,
			schema.AssistantMessage("紧跟其后、无 RunID"),
			schema.UserMessage("q2 " + strings.Repeat("x", 4000)),
			schema.UserMessage("当前问题"),
		}
		out := emergencyDropOldestGroups(in, 50, 100000, "[MARKER]")
		if len(out) >= len(in) {
			t.Fatalf("前提没搭对：一条都没丢（%d→%d）", len(in), len(out))
		}
		if !hasContent(out, "CALLER-MIDSTREAM-RULE") {
			t.Errorf("混合组里的 system 被丢掉了（判据写成「整组都是 system」就会这样）。出 %d 条：%s",
				len(out), renderRolesInternal(out))
		}
	})

	t.Run("紧急删组不丢它", func(t *testing.T) {
		in := build()
		// gap 给大，逼 emergencyDrop 尽量多丢——不然丢不到中段那一组，测了个寂寞。
		out := emergencyDropOldestGroups(in, 50, 100000, "[MARKER]")
		if len(out) >= len(in) {
			t.Fatalf("前提没搭对：一条都没丢（%d→%d），被测路径没跑到", len(in), len(out))
		}
		if !hasContent(out, "CALLER-MIDSTREAM-RULE") {
			t.Errorf("调用方塞在中段的 system 被丢掉了。出 %d 条：%s", len(out), renderRolesInternal(out))
		}
	})

	t.Run("摘要不把它摘成散文", func(t *testing.T) {
		cm := &capSummarizer{}
		cfg := &AutoCompactConfig{Enabled: true, Model: cm, ModelContextWindow: 40000}
		in := build()
		// 撑到阈值之上让摘要真跑
		for i := 0; i < 4; i++ {
			in = append(in, schema.UserMessage(fmt.Sprintf("填充%d %s", i, strings.Repeat("z", 20000))))
		}
		out, err := AutoCompactRun(context.Background(), cfg, in)
		if err != nil {
			t.Fatalf("AutoCompactRun: %v", err)
		}
		if len(cm.got) == 0 {
			t.Fatal("前提没搭对：摘要 LLM 没被调用，被测路径没跑到")
		}
		if !hasContent(out, "CALLER-MIDSTREAM-RULE") {
			t.Errorf("调用方塞在中段的 system 被摘要替换掉了。出 %d 条：%s", len(out), renderRolesInternal(out))
		}
	})
}

func hasContent(msgs []*schema.Message, want string) bool {
	for _, m := range msgs {
		if m != nil && strings.Contains(m.Content, want) {
			return true
		}
	}
	return false
}

func renderRolesInternal(msgs []*schema.Message) string {
	var b strings.Builder
	for i, m := range msgs {
		c := m.Content
		if len(c) > 20 {
			c = c[:20] + "…"
		}
		b.WriteString(fmt.Sprintf("\n  [%d] %s %q", i, m.Role, c))
	}
	return b.String()
}
