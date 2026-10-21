package compose_v2

import (
	"testing"
)

// commitSuperstepPartial 写的五样必须原样过 Snapshot → Restore。
//
// 这五样是**同一个提交点一起生效**的：channel 值、frontier、代际、答案账本、成功成员先记在一边的
// 上行信号。少存哪一样，恢复出来的状态就自相矛盾——少了代际，续跑时同一个节点的新提问会算出
// 跟上一轮相同的中断身份，用户答过的旧答案把它静默满足；少了答案账本，同一个答案能
// 兑付两次；少了成功成员的上行信号，它做出的转交 / 路由决定续跑后丢失（它不再执行）。都是
// err==nil 的静默失效。
//
// 变异四次（Snapshot 分别不存 visitCount / consumedAnswers / frontier / completedSignals）各自精确
// 报红，且报出的是具体哪一样空了。
//
// ⚠️ 前置那条 Fatal（「快照里有空项」）是**正向断言**，不是防御：没有它的话，
// 一旦 fixture 腐化到「五样压根没写进去」，下面那些比对在「都是零值」的状态上恒真。
func TestCommitSuperstep_AllFieldsRoundTrip(t *testing.T) {
	ch := Sum("rt_acc")
	cm := NewChannelManager()
	if err := RegisterChannel(cm, ch); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := cm.commitSuperstepPartial(
		[]ChannelWrite{Write(ch, 42)},
		[]FrontierMember{{NodeID: "nX"}},
		[]FrontierMember{{NodeID: "nX"}},
		[]string{"ans-1", "ans-2"},
		[]SignalRecord{{Kind: "k", Dest: "d", Value: "billing", Origin: "call_1#0"}},
		nil, nil,
	); err != nil {
		t.Fatalf("commitSuperstepPartial: %v", err)
	}

	cp, err := cm.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// 正向：五样在快照里都真的有（否则下面的比对在「都是空」时恒真）
	if len(cp.Channels) == 0 || len(cp.CurrentFrontier) == 0 ||
		len(cp.NodeVisits) == 0 || len(cp.ConsumedAnswers) == 0 || len(cp.CompletedSignals) == 0 {
		t.Fatalf("fixture 没搭对：快照里有空项 channels=%d frontier=%d visits=%d answers=%d completedSignals=%d",
			len(cp.Channels), len(cp.CurrentFrontier), len(cp.NodeVisits), len(cp.ConsumedAnswers), len(cp.CompletedSignals))
	}
	// 部分失败时成功成员的信号先记在一边，不并入已提交的——整超步完成时才合并（同一超步是并发的）
	if len(cp.Signals) != 0 {
		t.Errorf("部分提交不该把成功成员的信号直接并入已提交的：%+v", cp.Signals)
	}

	cm2 := NewChannelManager()
	if err := RegisterChannel(cm2, ch); err != nil {
		t.Fatalf("register 2: %v", err)
	}
	if err := cm2.Restore(cp); err != nil {
		t.Fatalf("restore: %v", err)
	}

	cm2.mu.RLock()
	defer cm2.mu.RUnlock()
	if got, _ := cm2.values["rt_acc"]; got != 42 {
		t.Errorf("channel 值没过来: %v 期望 42", got)
	}
	if len(cm2.frontierAtExit) != 1 || cm2.frontierAtExit[0].NodeID != "nX" {
		t.Errorf("frontier 没过来: %+v", cm2.frontierAtExit)
	}
	if cm2.visitCount["nX"] != 1 {
		t.Errorf("代际没过来: %v 期望 1", cm2.visitCount["nX"])
	}
	if len(cm2.consumedAnswers) != 2 {
		t.Errorf("答案账本没过来: %v 期望 2 条", cm2.consumedAnswers)
	}
	if len(cm2.completedSignals) != 1 || cm2.completedSignals[0].Value != "billing" || cm2.completedSignals[0].Origin != "call_1#0" {
		t.Errorf("成功成员先记在一边的上行信号没过来: %+v 期望 1 条转给 billing", cm2.completedSignals)
	}
}
