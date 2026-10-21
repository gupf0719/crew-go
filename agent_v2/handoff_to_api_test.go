package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestHandoffTo_ProgrammaticMemberCanTransfer：程序化成员经 HandoffTo 转交——
// 这是 annotation 通道被删之后，业务自实现 Agent 的**唯一合法出口**，必须真能用。
func TestHandoffTo_ProgrammaticMemberCanTransfer(t *testing.T) {
	front := newProgAgent("front", "front 看了一眼", "billing")
	billing, billingModel := scopeLeaf("billing", "BILLING 接手了")

	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)

	events, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if front.lastErr != nil {
		t.Fatalf("被授予的成员调 HandoffTo 不该报错：%v", front.lastErr)
	}
	// 承重的量是「billing 的模型有没有被真调」，不是最终文本。
	if n := billingModel.count(); n != 1 {
		t.Fatalf("程序化转交必须真的生效：billing 应被真调 1 次，实际 %d 次", n)
	}
	if fm := agent_v2.FinalMessage(events); fm == nil || !strings.Contains(fm.Content, "BILLING") {
		t.Errorf("最终产出应来自接手方，实际 %v", fm)
	}
}

// TestHandoffTo_RejectsUngrantedAndIllegal：三道校验各自都要真的拦住。
//
// 这三条是 annotation 通道**一条都没有**的——它当初绕过的正是它们。
func TestHandoffTo_RejectsUngrantedAndIllegal(t *testing.T) {
	t.Run("没被授予（顶层直接跑，头上没有 HandoffAgent）", func(t *testing.T) {
		solo := newProgAgent("solo", "自己跑", "billing")
		if _, err := solo.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if solo.lastErr == nil {
			t.Fatal("没有授予也能转交——那正是 annotation 通道的老毛病")
		}
		if !strings.Contains(solo.lastErr.Error(), "没有被授予") {
			t.Errorf("错误正文该点明缺的是授予，实际：%v", solo.lastErr)
		}
	})

	t.Run("目标不在候选里", func(t *testing.T) {
		front := newProgAgent("front", "front 看了一眼", "ghost") // ghost 没注册
		billing, billingModel := scopeLeaf("billing", "BILLING 接手了")
		h := agent_v2.NewHandoffAgent(
			agent_v2.WithHandoffName("router"),
			agent_v2.WithHandoffInitial("front"),
		).AddAgent("front", front).AddAgent("billing", billing)

		if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if front.lastErr == nil {
			t.Fatal("非法目标应被拒")
		}
		if !strings.Contains(front.lastErr.Error(), "不在可转交候选") {
			t.Errorf("错误正文该列出合法候选，实际：%v", front.lastErr)
		}
		// 且**什么都没写**——不是「写了再被下游忽略」。
		if n := billingModel.count(); n != 0 {
			t.Errorf("拒绝之后不该留下半个信号：billing 却被调了 %d 次", n)
		}
	})
}

// probeFailAgent 是把 HandoffTo 的失败**当致命错返回**的程序化成员。
type probeFailAgent struct {
	name   string
	target string
	runs   int
}

func (a *probeFailAgent) Name() string        { return a.name }
func (a *probeFailAgent) Description() string { return a.name }
func (a *probeFailAgent) Run(ctx context.Context, s *agent_v2.RunSession, in *schema.Message,
	opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.runs++
	if err := agent_v2.HandoffTo(ctx, opts, a, a.target); err != nil {
		return nil, err
	}
	return []*agent_v2.AgentEvent{agent_v2.NewOutputEvent(a.name, schema.AssistantMessage("ok"))}, nil
}
func (a *probeFailAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

// TestHandoffTo_FailureIsTerminalNotRetried：HandoffTo 拒绝的都是「重跑必然一样」的
// 情形（授予是上层发的、候选在注册时定的、拿不到槽是结构性的），所以它的 error 必须
// 带 [compose_v2.ErrRunAborted]。
//
// 不带的话 IsRetryable 返 true ⟹ 配了重试的编排会把一次确定性的协议违规跑满重试次数，
// **每次都重放这个成员的副作用**。实测过：MaxRetries=3 时成员被跑 4 次。
//
// 这条同时是那条规约的哨兵：新增 fail-fast 必问「这个错重跑一次可能就好，还是重跑
// 必然一样」——后者一律 ErrRunAborted。
func TestHandoffTo_FailureIsTerminalNotRetried(t *testing.T) {
	front := &probeFailAgent{name: "front", target: "ghost"} // ghost 不在候选里
	billing, _ := scopeLeaf("billing", "B")

	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
		agent_v2.WithHandoffRetryConfig(compose_v2.SimpleRetry(3)), // 显式接受框架保守谓词
	).AddAgent("front", front).AddAgent("billing", billing)

	_, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil {
		t.Fatal("非法目标应让本次 Run 失败")
	}
	if front.runs != 1 {
		t.Errorf("确定性的协议违规不该被重试：成员被跑了 %d 次（每次都重放它的副作用），期望 1 次", front.runs)
	}
	if compose_v2.IsRetryable(err) {
		t.Errorf("HandoffTo 的拒绝必须不可重试；IsRetryable 却返 true：%v", err)
	}
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("应带 ErrRunAborted 哨兵（业务据此判「已终局、别再 Resume」）：%v", err)
	}
}
