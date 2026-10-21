package agent_v2

import (
	"context"
	"strings"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// flipFailureToSuccess 把失败的工具结果改写成成功（钩子判定这次产出可以接受）。
type flipFailureToSuccess struct{ hook.NoOpHooks }

func (h *flipFailureToSuccess) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if out != nil && out.Result != nil && !out.Result.Success {
		out.Result.Success = true
		out.Result.ErrorCode, out.Result.Error = "", ""
		out.Result.Output = "可以接受"
	}
	return ctx, nil
}

// hookRewriteDesk 跑一个两人的 HandoffAgent：前台调一次 decide 工具（工具体做出转交 billing 的决定，
// 结果由 toolBody 定），返回 billing 被调的次数。
func hookRewriteDesk(t *testing.T, toolBody func(ctx context.Context) (*tool.Result, error), withHook bool, extra ...Option) int {
	t.Helper()
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("decide", "定一个去处", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if dest, ok := getHandoffDest(ctx); ok {
				compose_v2.EmitSignal(ctx, handoffSignal, compose_v2.Signal{Dest: dest, Value: "billing"})
			}
			return toolBody(ctx)
		}))
	opts := []Option{WithName("front"), WithToolkit(tk), WithMaxRounds(3),
		WithModel(&routeStub{msgs: []*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "d1", Type: "function",
				Function: schema.FunctionCall{Name: "decide", Arguments: `{}`}}}),
			schema.AssistantMessage("前台自己处理")}})}
	opts = append(opts, extra...)
	if withHook {
		opts = append(opts, WithHooks(&flipFailureToSuccess{}))
	}
	front := NewBasicAgent(opts...)
	billingModel := &routeStub{msgs: []*schema.Message{schema.AssistantMessage("BILLING 已处理")}}
	billing := NewBasicAgent(WithName("billing"), WithModel(billingModel))
	h := NewHandoffAgent(WithHandoffName("desk"), WithHandoffInitial("front")).
		AddAgent("front", front).AddAgent("billing", billing)
	if _, err := h.Run(context.Background(), NewRunSession(), schema.UserMessage("退款")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return billingModel.i
}

// TestToolBatch_HookRewriteToSuccess_KeepsDecision：工具做出了转交决定、自己返回失败，钩子把结果改成
// 成功——模型看到的是「成功」，这次决定就算数。没有钩子时（对照）工具的失败让决定作废。
//
// 白盒：「做出决定又返回失败」的工具用公开 API 造不出来（transfer 工具成功才交决定），这里直接往收件人
// 交一条转交信号。曾经作废在钩子之前按工具自己的结果判：钩子改成成功，模型以为转成了，决定却已作废
// （审计实测）。
func TestToolBatch_HookRewriteToSuccess_KeepsDecision(t *testing.T) {
	failing := func(context.Context) (*tool.Result, error) { return tool.Failure("bad_output", "产出不合格"), nil }
	for _, withHook := range []bool{true, false} {
		name := map[bool]string{true: "钩子改成成功", false: "没有钩子（对照）"}[withHook]
		t.Run(name, func(t *testing.T) {
			want := map[bool]int{true: 1, false: 0}[withHook]
			if got := hookRewriteDesk(t, failing, withHook); got != want {
				t.Errorf("%s：billing 应被调 %d 次，实际 %d", name, want, got)
			}
		})
	}
}

// TestToolBatch_TimeoutRewrittenToSuccess_KeepsDecision：超时也按同一条规则判——框架把超时的调用收尾成
// 一条失败结果，钩子把它改成成功（比如换成缓存的结果），模型看到的是「成功」，这次调用做出的决定就算数；
// 没有钩子时作废。
//
// 曾经超时是例外、无条件作废，理由是「那时工具还在跑、决定收不全」——不成立：工具调用是同步的，走到
// 超时收尾时工具已经返回。例外只让「模型看到成功、决定却作废」在这一种失败上重新出现。
func TestToolBatch_TimeoutRewrittenToSuccess_KeepsDecision(t *testing.T) {
	late := func(context.Context) (*tool.Result, error) {
		time.Sleep(150 * time.Millisecond) // 不看 ctx、迟到返回成功
		return tool.Success("ok"), nil
	}
	for _, withHook := range []bool{true, false} {
		name := map[bool]string{true: "钩子改成成功", false: "没有钩子（对照）"}[withHook]
		t.Run(name, func(t *testing.T) {
			want := map[bool]int{true: 1, false: 0}[withHook]
			if got := hookRewriteDesk(t, late, withHook, WithToolTimeout(30*time.Millisecond)); got != want {
				t.Errorf("%s：billing 应被调 %d 次，实际 %d", name, want, got)
			}
		})
	}
}

// TestFinishToolBatchSignals_FirstWinsAndRewrites：同一个收件人上第一个生效；后面给出不同值的那个调用的
// 结果换成一条「没有生效」的失败结果（新对象，原对象不动——工具账里存的消息会被原样交出来）；值相同的
// 算一次、不改写。
func TestFinishToolBatchSignals_FirstWinsAndRewrites(t *testing.T) {
	sigs := func(v string) compose_v2.Signals {
		s, _ := compose_v2.CaptureSignals(context.Background(), func(c context.Context) error {
			compose_v2.EmitSignal(c, handoffSignal, compose_v2.Signal{Dest: "d", Value: v})
			return nil
		})
		return s
	}
	calls := []schema.ToolCall{
		{ID: "a", Function: schema.FunctionCall{Name: HandoffTransferToolName}},
		{ID: "b", Function: schema.FunctionCall{Name: HandoffTransferToolName}},
		{ID: "c", Function: schema.FunctionCall{Name: HandoffTransferToolName}},
	}
	orig := []*schema.Message{
		schema.ToolResultMessage("a", "", "已转交给 billing"),
		schema.ToolResultMessage("b", "", "已转交给 tech"),
		schema.ToolResultMessage("c", "", "已转交给 billing"),
	}
	results := append([]*schema.Message(nil), orig...)
	got, err := compose_v2.CaptureSignals(context.Background(), func(c context.Context) error {
		finishToolBatchSignals(c, calls, results, []compose_v2.Signals{sigs("billing"), sigs("tech"), sigs("billing")},
			[]bool{false, false, false})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if s, ok, _ := got.Take(handoffSignal, "d"); !ok || s.Value != "billing" || got.Len() != 1 {
		t.Errorf("应只交出排在前面的那个（billing），实际 %+v", got)
	}
	if !isFailedToolResult(results[1]) || results[1] == orig[1] {
		t.Errorf("被盖掉的那个调用应换成一条新的失败结果，实际 %+v", results[1])
	}
	if c := results[1].Content; !strings.Contains(c, "「tech」") || !strings.Contains(c, "「billing」") ||
		!strings.Contains(c, "已转交给 tech") {
		t.Errorf("失败结果应说清被盖掉的、生效的是哪个，并带上这次调用原本的结果，实际 %q", c)
	}
	if orig[1].Content != "已转交给 tech" {
		t.Errorf("原对象不该被就地改：%q", orig[1].Content)
	}
	if isFailedToolResult(results[0]) || isFailedToolResult(results[2]) {
		t.Errorf("生效的那个与值相同的那个都不该被改写")
	}

	// 钩子之后失败的作废：第一个作废时第二个生效
	results = append([]*schema.Message(nil), orig...)
	got, _ = compose_v2.CaptureSignals(context.Background(), func(c context.Context) error {
		finishToolBatchSignals(c, calls[:2], results[:2], []compose_v2.Signals{sigs("billing"), sigs("tech")},
			[]bool{true, false})
		return nil
	})
	if s, ok, _ := got.Take(handoffSignal, "d"); !ok || s.Value != "tech" {
		t.Errorf("第一个在钩子之后失败、作废，第二个应生效，实际 %+v", got)
	}

	// 被盖掉的是框架那件控制工具：失败结果里不带它原本那句「已转交给 tech」——跟「没有生效」自相矛盾
	ctl := append([]*schema.Message(nil), orig[:2]...)
	ctl[1] = schema.ToolResultMessage("b", "", "已转交给 tech")
	ctl[1].Annotations = map[string]any{ReturnDirectAnnotation: true, ReturnDirectMintedAnnotation: true}
	_, _ = compose_v2.CaptureSignals(context.Background(), func(c context.Context) error {
		finishToolBatchSignals(c, calls[:2], ctl, []compose_v2.Signals{sigs("billing"), sigs("tech")}, []bool{false, false})
		return nil
	})
	if c := ctl[1].Content; !isFailedToolResult(ctl[1]) || strings.Contains(c, "已转交给 tech") {
		t.Errorf("被盖掉的框架控制工具不该带上原来那句确认，实际 %q", c)
	}

	// 一个调用里一条决定被盖掉，它别的（没冲突的）决定也不生效——结果说失败，就整个不算
	mixed := func(handoffTo, routeTo string) compose_v2.Signals {
		s, _ := compose_v2.CaptureSignals(context.Background(), func(c context.Context) error {
			compose_v2.EmitSignal(c, handoffSignal, compose_v2.Signal{Dest: "d", Value: handoffTo})
			if routeTo != "" {
				compose_v2.EmitSignal(c, routeSignal, compose_v2.Signal{Dest: "r", Value: routeTo})
			}
			return nil
		})
		return s
	}
	results = append([]*schema.Message(nil), orig[:2]...)
	got, _ = compose_v2.CaptureSignals(context.Background(), func(c context.Context) error {
		finishToolBatchSignals(c, calls[:2], results, []compose_v2.Signals{mixed("billing", ""), mixed("tech", "x")},
			[]bool{false, false})
		return nil
	})
	if _, ok, _ := got.Take(routeSignal, "r"); ok || !isFailedToolResult(results[1]) {
		t.Errorf("被盖掉的那个调用的路由决定不该生效（它的结果是失败），实际 %+v", got)
	}

	// 一个调用做出的两个决定都被盖掉：合成一条失败结果，原本的结果只出现一次
	two := func(a, b string) compose_v2.Signals {
		s, _ := compose_v2.CaptureSignals(context.Background(), func(c context.Context) error {
			compose_v2.EmitSignal(c, handoffSignal, compose_v2.Signal{Dest: "d", Value: a})
			compose_v2.EmitSignal(c, routeSignal, compose_v2.Signal{Dest: "r", Value: b})
			return nil
		})
		return s
	}
	results = append([]*schema.Message(nil), orig[:2]...)
	_, _ = compose_v2.CaptureSignals(context.Background(), func(c context.Context) error {
		finishToolBatchSignals(c, calls[:2], results, []compose_v2.Signals{two("billing", "math"), two("tech", "chat")},
			[]bool{false, false})
		return nil
	})
	c := results[1].Content
	if !isFailedToolResult(results[1]) || !strings.Contains(c, "「tech」") || !strings.Contains(c, "「chat」") ||
		strings.Count(c, "已转交给 tech") != 1 || strings.Count(c, "没有生效") != 1 {
		t.Errorf("两个决定都被盖掉时应合成一条、原本的结果只出现一次，实际 %q", c)
	}
}

// rerouteOnToolEnd 在 OnToolEnd 里经嵌套调用让一个路由 agent 定路由（看过工具结果之后改道）。
type rerouteOnToolEnd struct {
	hook.NoOpHooks
	sub   Agent
	fired bool
}

func (h *rerouteOnToolEnd) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ToolOutput) (context.Context, error) {
	if h.fired {
		return ctx, nil
	}
	h.fired = true
	_, err := RunSubAgent(ctx, "reroute", h.sub, schema.UserMessage("看过结果之后改道"), RegrantFromNode(ctx, h.sub)...)
	return ctx, err
}

// TestToolBatch_OnToolEndDecisionComesAfterTheBatch：分类器 route_to(alpha)，它的 OnToolEnd 钩子看过
// 结果之后经嵌套调用改道 beta——钩子的决定是后做的，走 beta。
//
// 曾经批的决定在全部 OnToolEnd 之后才交，「后交的为准」让工具更早做出的 alpha 盖掉了钩子更晚做出的
// beta（审计实测）。对照组（分类器只调业务工具、只有钩子定路由）证明钩子这条路的决定送得到。
func TestToolBatch_OnToolEndDecisionComesAfterTheBatch(t *testing.T) {
	for _, toolRoutes := range []bool{true, false} {
		name := map[bool]string{true: "工具定 alpha、钩子改道 beta", false: "只有钩子定路由（对照）"}[toolRoutes]
		t.Run(name, func(t *testing.T) {
			sub := NewBasicAgent(WithName("rerouter"), WithRouteTool(), WithMaxRounds(3),
				WithModel(&routeStub{msgs: []*schema.Message{routeCallMsg("rr1", "beta"), schema.AssistantMessage("done")}}))
			tk := tool.NewToolkit()
			tk.Register(tool.NewFuncTool("lookup", "查资料", map[string]any{"type": "object"},
				func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("资料"), nil }))
			first := routeCallMsg("r1", "alpha")
			if !toolRoutes {
				first = schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "l1", Type: "function",
					Function: schema.FunctionCall{Name: "lookup", Arguments: `{}`}}})
			}
			clf := NewBasicAgent(WithName("clf"), WithToolkit(tk), WithRouteTool(), WithMaxRounds(3),
				WithModel(&routeStub{msgs: []*schema.Message{first, schema.AssistantMessage("none")}}),
				WithHooks(&rerouteOnToolEnd{sub: sub}))
			am := &routeStub{msgs: []*schema.Message{schema.AssistantMessage("ALPHA")}}
			bm := &routeStub{msgs: []*schema.Message{schema.AssistantMessage("BETA")}}
			r := NewRoutingAgent(WithRoutingName("router"), WithRoutingClassifier(clf)).
				AddBranch("alpha", NewBasicAgent(WithName("alpha"), WithModel(am))).
				AddBranch("beta", NewBasicAgent(WithName("beta"), WithModel(bm)))
			if _, err := r.Run(context.Background(), NewRunSession(), schema.UserMessage("问题")); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if am.i != 0 || bm.i != 1 {
				t.Errorf("钩子看过结果之后做的决定是后做的，应走 beta：alpha 被调 %d 次、beta %d 次", am.i, bm.i)
			}
		})
	}
}
