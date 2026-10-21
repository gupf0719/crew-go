package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// annoTransferMock 每次都经 [agent_v2.HandoffTo] 转给 target——用来把 maxHops 顶穿。
//
// ⚠️ 它原先写产出消息的 handoff_target annotation。那条通道删掉之后，这个 mock 的
// 转交**一次都不会发生**，于是 maxHops 永远到不了、下面那条只有否定断言的用例变成
// 恒真（实测：两棵树对同一个注入，改前 FAIL、改后 PASS——判别力归零）。
type annoTransferMock struct {
	name   string
	target string
}

func (a *annoTransferMock) Run(ctx context.Context, _ *agent_v2.RunSession, _ *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	msg := schema.AssistantMessage("transferring to " + a.target)
	// 转不动就让它显形——静默失败正是这个 mock 上次坏掉的方式。
	if err := agent_v2.HandoffTo(ctx, opts, a, a.target); err != nil {
		return nil, err
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, msg)}, nil
}
func (a *annoTransferMock) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *annoTransferMock) Name() string        { return a.name }
func (a *annoTransferMock) Description() string { return "" }

// TestHandoff_MaxHops_HandoffTargetNotLeak 验证（Sequential review 牵出，同 loop break_loop
// 根因）：HandoffAgent maxHops 耗尽时 final 不得带 handoff_target annotation——否则嵌套时
// （外层 Handoff/Loop/Routing 把它当 lastOut）外层误读该控制信号。
//
// 复现：maxHops=1，front 转 billing（合法）→ nextHops=1>=maxHops 耗尽 done。agent_v2 stepFn
// 在 nextTarget!="" 时已写回 handoff_target，compose_v2 stepNode 判 maxHops done → final
// （handoffAgentOutputCh LastValue）带着 handoff_target=billing 泄漏。
func TestHandoff_MaxHops_HandoffTargetNotLeak(t *testing.T) {
	front := &annoTransferMock{name: "front", target: "billing"}
	billing := &annoTransferMock{name: "billing", target: "front"}
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
		agent_v2.WithHandoffMaxHops(1), // 1 hop 即 maxHops 耗尽
	).AddAgent("front", front).AddAgent("billing", billing)

	sess := agent_v2.NewRunSession()
	events, err := h.Run(context.Background(), sess, schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// **正向前提**：maxHops 必须真的被顶穿，否则下面那条否定断言恒真。
	// 这条用例栽过一次——fixture 的转交手段失效后转交一次都没发生，它照样全绿。
	if st := h.State(sess); !st.Exhausted {
		t.Fatalf("fixture 没搭对：maxHops 没被顶穿（state=%+v），"+
			"下面「final 不带 handoff_target」这句在这种状态下恒真", st)
	}

	final := agent_v2.FinalMessage(events)
	if final != nil && final.Annotations != nil {
		if v, ok := final.Annotations[agent_v2.HandoffSignalKey]; ok {
			t.Errorf("HandoffAgent final 泄漏 handoff_target=%v（maxHops 耗尽时应清除，防嵌套外层误读，同 loop break_loop 根因）", v)
		}
	}
}
