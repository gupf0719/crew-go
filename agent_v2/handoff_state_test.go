package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// TestHandoffState_BeforeRun 验证 Run 前 State() 返 initial。
func TestHandoffState_BeforeRun(t *testing.T) {
	a := agent_v2.NewBasicAgent(agent_v2.WithName("a"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("ok")})))

	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("a"),
	).AddAgent("a", a)

	sess := agent_v2.NewRunSession()
	state := h.State(sess)
	if state.Current != "a" {
		t.Errorf("expect Current=a before Run, got %q", state.Current)
	}
	if state.Hops != 0 || state.Done || state.Exhausted {
		t.Errorf("expect zero state before Run, got %+v", state)
	}
}

// TestHandoffState_AfterDoneFirstStep 验证不 handoff 时 Done=true。
func TestHandoffState_AfterDoneFirstStep(t *testing.T) {
	a := agent_v2.NewBasicAgent(agent_v2.WithName("a"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("answered")})))

	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("a"),
	).AddAgent("a", a)

	sess := agent_v2.NewRunSession()
	if _, err := h.Run(context.Background(), sess, schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	state := h.State(sess)
	if state.Current != "a" || state.Hops != 1 || !state.Done {
		t.Errorf("expect Current=a Hops=1 Done=true, got %+v", state)
	}
}

// TestHandoffState_AcrossHandoff 验证转交后 State 反映新 active。
func TestHandoffState_AcrossHandoff(t *testing.T) {
	// 程序化成员经 HandoffTo 转交（annotation 通道已删）。
	a := newProgAgent("a", "forward", "b")
	b := newProgAgent("b", "done", "")

	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("a"),
	).AddAgent("a", a).AddAgent("b", b)

	sess := agent_v2.NewRunSession()
	if _, err := h.Run(context.Background(), sess, schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	state := h.State(sess)
	if state.Current != "b" || state.Hops != 2 || !state.Done {
		t.Errorf("expect Current=b Hops=2 Done=true, got %+v", state)
	}
}

// TestHandoffState_IsolatedPerSession 验证不同 session 状态隔离。
func TestHandoffState_IsolatedPerSession(t *testing.T) {
	a := agent_v2.NewBasicAgent(agent_v2.WithName("a"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("done1"),
			schema.AssistantMessage("done2"),
		})))

	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("a"),
	).AddAgent("a", a)

	sess1 := agent_v2.NewRunSession()
	sess2 := agent_v2.NewRunSession()

	if _, err := h.Run(context.Background(), sess1, schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run sess1: %v", err)
	}
	// sess2 还没跑——State 应是 initial 默认值
	st2 := h.State(sess2)
	if st2.Hops != 0 || st2.Done {
		t.Errorf("sess2 should be untouched, got %+v", st2)
	}
	st1 := h.State(sess1)
	if st1.Hops != 1 || !st1.Done {
		t.Errorf("sess1 should be Done with 1 hop, got %+v", st1)
	}
}
