package compose_v2

import (
	"testing"
)

// 「写落地」与「清掉待补记录」必须是一件事。
//
// 曾经它们是两句：`ApplyAll(...)` 成功后再 `setPendingSendWrites(nil)`。两句之间
// 只要有一行能被掐断的代码，存档里就同时留下「写已落地」和「这批写还欠着」两份
// ——Resume 按后者补 apply 一次，Append/Sum 就多算一次，**每 Resume 一次翻一倍**。
// 当时靠一句「这两句之间不许有任何业务可达代码」的注释守着。
//
// 现在由 ApplyPendingSendsAndClear 在同一个临界区里做完。本用例验的是那个
// **后置条件**：提交成功 ⟹ 待补记录必须已经空了；提交失败 ⟹ 必须原样留着
// （没落地的写不能丢，Resume 还要靠它补）。
//
// 变异「提交成功后不清记录」时第一条红；变异「失败也清」时第二条红。
func TestApplyPendingSendsAndClear_SuccessClearsRecord(t *testing.T) {
	ch := Sum("pac_ok")
	cm := NewChannelManager()
	if err := RegisterChannel(cm, ch); err != nil {
		t.Fatalf("register: %v", err)
	}
	writes := []ChannelWrite{Write(ch, 7)}
	cm.setPendingSendWrites(writes)

	if err := cm.ApplyPendingSendsAndClear(); err != nil {
		t.Fatalf("提交该成功: %v", err)
	}
	// 正向：写真的落地了（否则下面那条否定断言在「什么都没发生」时恒真）
	cm.mu.RLock()
	landed, _ := cm.values["pac_ok"].(int)
	cm.mu.RUnlock()
	if landed != 7 {
		t.Fatalf("fixture 没搭对：写没落地（got %d 期望 7）", landed)
	}
	if left := cm.peekPendingSendWrites(); len(left) != 0 {
		t.Errorf("提交成功了，待补记录却还留着 %d 条——存档会同时有「已落地」和「还欠着」"+
			"两份，Resume 补一次多算一次", len(left))
	}
}

func TestApplyPendingSendsAndClear_FailureKeepsRecord(t *testing.T) {
	cm := NewChannelManager()
	// 故意不注册：提交必失败
	writes := []ChannelWrite{&rawChannelWrite{name: "pac_missing", val: 1}}
	cm.setPendingSendWrites(writes)

	if err := cm.ApplyPendingSendsAndClear(); err == nil {
		t.Fatal("channel 没注册，提交该失败")
	}
	if left := cm.peekPendingSendWrites(); len(left) != 1 {
		t.Errorf("提交失败却把待补记录清了（剩 %d 条，期望 1）——那批写既没落地又没人记着，"+
			"Resume 时 target 静默读零值", len(left))
	}
}
