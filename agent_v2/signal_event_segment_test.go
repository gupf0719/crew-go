package agent_v2_test

// 上行信号跟着做出它的那一段执行走（CaptureSignals 开的：一个 tool_call、一次嵌套调用、OnToolEnd 那次钩子
// 调用……）：失败的调用里做出的作废、暂停续跑之后随工具账 / 完成记录交出、OnToolEnd 看过结果之后的改道排在
// 工具体之后、同一批排在前面的生效（前五条）。经 EventSink 发事件、在 OnNodeEvent 里做出的不生效——
// OnNodeEvent 只作观测（最后一条）。曾经经事件做出的也算数，靠在各段入口重绑 sink 让它跟着发事件的那一段
// 走，一再漏出新的形态（业务包一层转发的 sink、钩子里再发一次事件）。

import (
	"context"
	"sync"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

var evSegKind = compose_v2.NewSignalKind("agent_v2_test.event_segment")

// evSegDecision 取发给 "d" 的那个决定，没有时返回 "<none>"。
func evSegDecision(s compose_v2.Signals) string {
	got, ok, _ := s.Take(evSegKind, "d")
	if !ok {
		return "<none>"
	}
	return got.Value
}

// evSegDecide 在当前这段执行里直接做出决定 v。
func evSegDecide(ctx context.Context, v string) {
	compose_v2.EmitSignal(ctx, evSegKind, compose_v2.Signal{Dest: "d", Value: v})
}

func evSegCall(id, name string) schema.ToolCall {
	return schema.ToolCall{ID: id, Type: "function", Function: schema.FunctionCall{Name: name, Arguments: `{}`}}
}

// evSegDrive 跑 ag（每次停下答「是」再续），每一次都包在 CaptureSignals 里，返回成功那次收到的信号与停下的次数。
// orch 非 nil 时经 ctx 挂上（嵌套的子 agent 一并继承）。
func evSegDrive(t *testing.T, ag agent_v2.Agent, orch agent_v2.OrchestrationHooks) (compose_v2.Signals, int) {
	t.Helper()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	if orch != nil {
		ctx = agent_v2.WithCtxOrchHooks(ctx, orch)
	}
	sess := agent_v2.NewRunSession()
	answers := map[string]*schema.Message{}
	pauses := 0
	sigs, err := compose_v2.CaptureSignals(ctx, func(c context.Context) error {
		_, e := ag.Run(c, sess, schema.UserMessage("go"))
		return e
	})
	for i := 0; i < 4 && err != nil; i++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			t.Fatalf("非中断错误：%v", err)
		}
		pauses++
		for _, q := range ie.AllQueries() {
			answers[q.InterruptID] = schema.UserMessage("是")
		}
		sigs, err = compose_v2.CaptureSignals(ctx, func(c context.Context) error {
			_, e := ag.Resume(c, sess.RunID, agent_v2.WithResumeUserInputs(answers), agent_v2.WithResumeSession(sess))
			return e
		})
	}
	if err != nil {
		t.Fatalf("没收敛：%v", err)
	}
	return sigs, pauses
}

func evSegAgent(tk *tool.Toolkit, h hook.Hooks, rounds ...*schema.Message) *agent_v2.BasicAgent {
	opts := []agent_v2.Option{agent_v2.WithName("ev_seg"), agent_v2.WithModel(newStubModel(rounds)),
		agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(5)}
	if h != nil {
		opts = append(opts, agent_v2.WithHooks(h))
	}
	return agent_v2.NewBasicAgent(opts...)
}

// TestSegmentDecision_ToolCall_SurvivesPause：同一批 [decide（工具体做决定 X）, ask（问人）]。decide 已成功、
// 记进工具账，续跑时不再执行——它做出的决定要随工具账记下、续跑时交出。
func TestSegmentDecision_ToolCall_SurvivesPause(t *testing.T) {
	tk := tool.NewToolkit()
	decideRuns := 0
	tk.Register(tool.NewFuncTool("decide", "d", map[string]any{"type": "object"}, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
		decideRuns++
		evSegDecide(ctx, "X")
		return tool.Success("ok"), nil
	}))
	tk.Register(tool.NewFuncTool("ask", "a", map[string]any{"type": "object"}, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"}); err != nil {
			return nil, err
		}
		return tool.Success("ok"), nil
	}))
	sigs, pauses := evSegDrive(t, evSegAgent(tk, nil,
		schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "decide"), evSegCall("c2", "ask")}),
		schema.AssistantMessage("done")), nil)
	if pauses != 1 || decideRuns != 1 {
		t.Fatalf("前提不成立：应停下 1 次、decide 执行 1 次，实际 %d / %d", pauses, decideRuns)
	}
	if got := evSegDecision(sigs); got != "X" {
		t.Errorf("续跑之后应交出 decide 当初做出的 X，实际 %s", got)
	}
}

// evSegEndHook：OnToolEnd（对 c1）看过结果后改道 Y。
type evSegEndHook struct {
	hook.NoOpHooks
}

func (h *evSegEndHook) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if out != nil && out.CallId == "c1" {
		evSegDecide(ctx, "Y")
	}
	return ctx, nil
}

// TestSegmentDecision_OnToolEndAfterToolBody：工具体定 X，OnToolEnd 看过结果之后改道 Y——后做的为准。
func TestSegmentDecision_OnToolEndAfterToolBody(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("decide", "d", map[string]any{"type": "object"}, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
		evSegDecide(ctx, "X")
		return tool.Success("ok"), nil
	}))
	sigs, _ := evSegDrive(t, evSegAgent(tk, &evSegEndHook{},
		schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "decide")}),
		schema.AssistantMessage("done")), nil)
	if got := evSegDecision(sigs); got != "Y" {
		t.Errorf("OnToolEnd 看过结果之后的改道应为准，实际 %s", got)
	}
}

// TestSegmentDecision_FailedToolCallVoided：工具体做决定之后返回 tool.Failure——失败的调用里做出的作废。
func TestSegmentDecision_FailedToolCallVoided(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("decide", "d", map[string]any{"type": "object"}, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
		evSegDecide(ctx, "X")
		return tool.Failure("biz", "没成"), nil
	}))
	sigs, _ := evSegDrive(t, evSegAgent(tk, nil,
		schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "decide")}),
		schema.AssistantMessage("done")), nil)
	if got := evSegDecision(sigs); got != "<none>" {
		t.Errorf("失败的调用里做出的决定应作废，实际交出了 %s", got)
	}
}

// TestSegmentDecision_SameBatchFirstWins：同一批 [c1 立即定 first, c2 稍后定 second]——排在前面的生效，后面
// 那个调用的结果换成「没有生效」的失败结果。
func TestSegmentDecision_SameBatchFirstWins(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("d1", "d", map[string]any{"type": "object"}, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
		evSegDecide(ctx, "first")
		return tool.Success("ok1"), nil
	}))
	tk.Register(tool.NewFuncTool("d2", "d", map[string]any{"type": "object"}, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
		time.Sleep(20 * time.Millisecond) // 让 c2 的决定晚到：先后不该影响谁生效
		evSegDecide(ctx, "second")
		return tool.Success("ok2"), nil
	}))
	var seen []*schema.Message
	m := newStubModelCapturing([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "d1"), evSegCall("c2", "d2")}),
		schema.AssistantMessage("done")}, &seen)
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("ev_seg"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(5))
	sigs, _ := evSegDrive(t, ag, nil)
	if got := evSegDecision(sigs); got != "first" {
		t.Errorf("同一批排在前面的应生效，实际 %s", got)
	}
	var c2 string
	for _, msg := range seen {
		if msg.Role == schema.RoleTool && schema.GetToolResultID(msg) == "c2" {
			c2 = msg.Content
		}
	}
	if want := "[" + agent_v2.SupersededDecisionErrCode + "]"; len(c2) < len(want) || c2[:len(want)] != want {
		t.Errorf("被盖掉的 c2 应拿到「没有生效」的失败结果，实际 %q", c2)
	}
}

// evSegStartHook：被调方的 agent 级 OnAgentStart 里做决定（被调方在自己图外的那一段）。
type evSegStartHook struct {
	hook.NoOpHooks
	runs *int
}

func (h *evSegStartHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	*h.runs++
	evSegDecide(ctx, "from_callee")
	return ctx, nil
}

// TestSegmentDecision_NestedCallHook_SurvivesPause：GraphAgent 节点里 RunSubAgent 调 callee（它的 OnAgentStart
// 做决定），随后节点问人。续跑时 callee 从完成记录拿回、不再执行——它做出的决定要随记录交出。
func TestSegmentDecision_NestedCallHook_SurvivesPause(t *testing.T) {
	runs := 0
	callee := agent_v2.NewBasicAgent(agent_v2.WithName("callee"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("callee 答")})),
		agent_v2.WithHooks(&evSegStartHook{runs: &runs}), agent_v2.WithMaxRounds(3))
	g := effGraph(t, "ev_seg_host", func(ctx context.Context, c string) (string, error) {
		if _, err := agent_v2.RunSubAgent(ctx, "callee", callee, schema.UserMessage(c)); err != nil {
			return "", err
		}
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"}); err != nil {
			return "", err
		}
		return "ok", nil
	})
	sigs, pauses := evSegDrive(t, g, nil)
	if pauses != 1 || runs != 1 {
		t.Fatalf("前提不成立：应停下 1 次、callee 的 OnAgentStart 跑 1 次，实际 %d / %d", pauses, runs)
	}
	if got := evSegDecision(sigs); got != "from_callee" {
		t.Errorf("续跑之后应交出 callee 当初做出的决定，实际 %s", got)
	}
}

// evSegObserveHooks：OnNodeEvent 收到 ev_seg_decide 事件就试着做决定，记下 EmitSignal 的返回值。
type evSegObserveHooks struct {
	agent_v2.NoOpOrchestrationHooks
	mu       sync.Mutex
	seen     int
	accepted bool
}

func (h *evSegObserveHooks) OnNodeEvent(ctx context.Context, _ string, ev agent_v2.Event) context.Context {
	if ev.Type == "ev_seg_decide" {
		ok := compose_v2.EmitSignal(ctx, evSegKind, compose_v2.Signal{Dest: "d", Value: ev.Content})
		h.mu.Lock()
		h.seen++
		h.accepted = h.accepted || ok
		h.mu.Unlock()
	}
	return ctx
}

// TestEventDecision_OnNodeEventIsObservationOnly：工具体经 EventSink 发事件、由 OnNodeEvent 做决定——不生效：
// EmitSignal 返回 false，调用方什么都收不到。决定要在发事件的那段代码里直接做（上面几条）。
func TestEventDecision_OnNodeEventIsObservationOnly(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("decide", "d", map[string]any{"type": "object"}, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
		if s := compose_v2.GetEventSink(ctx); s != nil {
			s.Send(compose_v2.Event{Type: "ev_seg_decide", Content: "X"})
		}
		return tool.Success("ok"), nil
	}))
	h := &evSegObserveHooks{}
	sigs, _ := evSegDrive(t, evSegAgent(tk, nil,
		schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "decide")}),
		schema.AssistantMessage("done")), h)
	h.mu.Lock()
	seen, accepted := h.seen, h.accepted
	h.mu.Unlock()
	if seen != 1 {
		t.Fatalf("fixture 没搭对：OnNodeEvent 应收到 1 次事件，实际 %d 次", seen)
	}
	if accepted {
		t.Error("OnNodeEvent 里 EmitSignal 应返回 false（只作观测），实际收下了")
	}
	if got := evSegDecision(sigs); got != "<none>" {
		t.Errorf("经事件做出的决定不该生效，实际交出了 %s", got)
	}
}
