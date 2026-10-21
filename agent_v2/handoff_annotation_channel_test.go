package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// TestHandoffAnnotation_NoLongerATransferChannel 钉住：**产出消息上的 handoff_target
// 不再是转交通道**。
//
// 它曾经是，而且绕过全部三道校验——实测一个连 `WithHandoffTransfer()` 都没配、
// 因此连 transfer 工具都不存在的候选，一行 annotation 就把对话转走了，err 全程 nil。
// 也就是说那个 opt-in 开关**只挡住了工具路径**。
//
// 承重的量是「接手方的模型有没有被真调」，不是最终文本——转交丢失时 front 会自己
// 答完收尾，最终文本同样是一句像样的回复。
func TestHandoffAnnotation_NoLongerATransferChannel(t *testing.T) {
	forged := schema.AssistantMessage("FRONT 自己答了")
	forged.Annotations = map[string]any{agent_v2.HandoffSignalKey: "billing"}

	front := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithModel(newScopeStub(forged)),
		// 注意：**没有** WithHandoffTransfer()——它连 transfer 工具都没有
		agent_v2.WithMaxRounds(2),
	)
	billing, billingModel := scopeLeaf("billing", "BILLING 接手了")

	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)

	events, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := billingModel.count(); n != 0 {
		t.Fatalf("未授予转交的候选靠一行 annotation 就把对话转走了（billing 被真调 %d 次）"+
			"——那条通道绕过「有没有被授予/目标合不合法/这一层有没有人接」三道校验，且 err 全程 nil", n)
	}
	if fm := agent_v2.FinalMessage(events); fm == nil || fm.Content != "FRONT 自己答了" {
		got := "<nil>"
		if fm != nil {
			got = fm.Content
		}
		t.Errorf("产出应原样来自 front 自己，实际 %q", got)
	}
}

// TestHandoffAnnotation_NotLeakedToCaller：那个字段也不该跟着产出漏给调用方。
//
// 它是框架内部路由用的（stepFn 写、条件边读）；漏出去会让外层嵌套编排误读——
// 而外层看到的是一个它无从判断真伪的字符串。
//
// ⚠️ **必须让最后一跳是「还想转交但被拦下」的那种**（这里用 maxHops 顶穿）。
// 第一版写成了正常收尾的两跳——那时最后一跳按定义不转交、`nextTarget == ""`、
// 清除分支压根不执行 ⟹ 这个 fixture **结构上观测不到泄漏**，撤掉清除逻辑照样全绿。
// 审计实测抓出来的：同一个注入下它和另一条哨兵**双双 PASS**，而泄漏是真的。
func TestHandoffAnnotation_NotLeakedToCaller(t *testing.T) {
	front := newProgAgent("front", "front 看了一眼", "billing")
	billing := newProgAgent("billing", "billing 也想转", "front") // 还想再转 → 被 maxHops 拦

	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
		agent_v2.WithHandoffMaxHops(1),
	).AddAgent("front", front).AddAgent("billing", billing)

	sess := agent_v2.NewRunSession()
	events, err := h.Run(context.Background(), sess, schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 正向前提：真的走到了「想转但被拦」那一步，否则下面那句恒真。
	if st := h.State(sess); !st.Exhausted {
		t.Fatalf("fixture 没搭对：没走到 maxHops 耗尽（state=%+v），"+
			"下面的否定断言在这种状态下观测不到泄漏", st)
	}
	fm := agent_v2.FinalMessage(events)
	if fm == nil {
		t.Fatal("nil final")
	}
	if _, ok := fm.Annotations[agent_v2.HandoffSignalKey]; ok {
		t.Errorf("路由用的内部字段漏给了调用方：%v", fm.Annotations)
	}
}
