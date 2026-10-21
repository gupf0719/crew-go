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

// history.compact EventSink event 守护测试(sink-gated——无独立开关).
//
// 测覆盖:
//  1. 接了 OrchHooks + 配 HistoryCompactor → fire 1 history.compact event
//  2. 没配 HistoryCompactor → 0 event(compactor 根本没调)
//  3. HistoryCompactor 返 err → fire event 含 Ok=false + Err
//
// sanity-revert: 删 basic_agent.go::if hasEventSink 块内 EmitHistoryCompactEvent 调用 → 测试 fail.

// captureHistorySink 接 framework 转发 history event (OnNodeEvent 路径).
type captureHistorySink struct {
	compose_v2.NoOpOrchestrationHooks
	events []compose_v2.Event
}

func (s *captureHistorySink) OnNodeEvent(ctx context.Context, _ compose_v2.NodeID, ev compose_v2.Event) context.Context {
	s.events = append(s.events, ev)
	return ctx
}

func (s *captureHistorySink) historyActions() []*agent_v2.HistoryCompactAction {
	var out []*agent_v2.HistoryCompactAction
	for _, ev := range s.events {
		if ev.Type != agent_v2.HistoryCompactEventType {
			continue
		}
		if a, ok := ev.Metadata["action"].(*agent_v2.HistoryCompactAction); ok {
			out = append(out, a)
		}
	}
	return out
}

func TestPhase2Sub2_HistoryCompactor_EmitsEvent(t *testing.T) {
	// 业务 HistoryCompactor 把 prior 截短到 2 条
	hcFn := func(_ context.Context, msgs []*schema.Message, keep int) ([]*schema.Message, error) {
		if len(msgs) <= 2 {
			return msgs, nil
		}
		return msgs[len(msgs)-2:], nil
	}

	sink := &captureHistorySink{}
	ctx := compose_v2.WithOrchHooks(context.Background(), sink)

	// 用 RunSession 含 prior events 让 HistoryCompactor 真触发
	sess := agent_v2.NewRunSession()
	// 模拟历史 — agent name 必须跟 BasicAgent.Name 一致, prior 才取 (SelfHistoryCompacted)
	for i := 0; i < 5; i++ {
		sess.AddEvent(agent_v2.NewOutputEvent("test_history", schema.UserMessage("u"+string(rune('0'+i)))))
		sess.AddEvent(agent_v2.NewOutputEvent("test_history", schema.AssistantMessage("a"+string(rune('0'+i)))))
	}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("test_history"),
		agent_v2.WithModel(&stubAssistModel{}),
		agent_v2.WithHistoryCompactor(hcFn),
	)

	_, err := ag.Run(ctx, sess, schema.UserMessage("query"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	actions := sink.historyActions()
	if len(actions) == 0 {
		t.Fatalf("history event regression: 有 sink 应 fire history.compact event\n" +
			"sanity-revert: 删 basic_agent.go::if hasEventSink 块内 EmitHistoryCompactEvent 调用 → 此测 fail")
	}
	// 验首个 event 含正确字段
	a := actions[0]
	if a.InputMsgs == 0 {
		t.Errorf("InputMsgs 应 > 0 (sess prior 含 10 events), got %d", a.InputMsgs)
	}
	if a.OutputMsgs == 0 {
		t.Errorf("OutputMsgs 应 > 0 (compactor 保留至少 2), got %d", a.OutputMsgs)
	}
	if !a.Ok {
		t.Errorf("Ok 应 true, got false err=%q", a.Err)
	}
	if a.KeepHint == 0 {
		t.Errorf("KeepHint 应非 0 (Config.MaxHistoryPairs 或默认 20)")
	}
}

func TestPhase2Sub2_HistoryCompact_NoCompactorNoEvent(t *testing.T) {
	// sink-gated 新语义:没配 HistoryCompactor 时 compactor 根本不调 → 不 fire history
	// event(接了 OrchHooks 也一样)。证明 history event 只在 compactor 真跑时发。
	sink := &captureHistorySink{}
	ctx := compose_v2.WithOrchHooks(context.Background(), sink)
	sess := agent_v2.NewRunSession()
	sess.AddEvent(agent_v2.NewOutputEvent("user", schema.UserMessage("u1")))

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("test_no_compactor"),
		agent_v2.WithModel(&stubAssistModel{}),
		// 不配 HistoryCompactor
	)

	_, err := ag.Run(ctx, sess, schema.UserMessage("q"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}
	if got := len(sink.historyActions()); got != 0 {
		t.Errorf("没配 HistoryCompactor 应 0 history event, got %d", got)
	}
}

func TestPhase2Sub2_HistoryCompactorFailure_EmitsErrInEvent(t *testing.T) {
	hcFn := func(_ context.Context, _ []*schema.Message, _ int) ([]*schema.Message, error) {
		return nil, errors.New("simulated history compactor backend error")
	}
	sink := &captureHistorySink{}
	ctx := compose_v2.WithOrchHooks(context.Background(), sink)
	sess := agent_v2.NewRunSession()
	sess.AddEvent(agent_v2.NewOutputEvent("user", schema.UserMessage("u1")))

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("test_history_fail"),
		agent_v2.WithModel(&stubAssistModel{}),
		agent_v2.WithHistoryCompactor(hcFn),
	)

	// HistoryCompactor 返 err → BasicAgent.Run 应失败 propagate
	_, err := ag.Run(ctx, sess, schema.UserMessage("q"))
	if err == nil {
		t.Fatal("expect err propagate from history compactor")
	}

	actions := sink.historyActions()
	var sawErr bool
	for _, a := range actions {
		if !a.Ok && strings.Contains(a.Err, "simulated history compactor backend error") {
			sawErr = true
		}
	}
	if !sawErr {
		t.Errorf("失败 fire 应含 Ok=false + Err 含真实 msg, got actions %+v", actions)
	}
}
