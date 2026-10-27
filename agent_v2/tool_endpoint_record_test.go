package agent_v2_test

// 中间件在 next 之后挂起（结果确认类中间件：工具执行完、问人确认再交给模型）时，续跑重跑这个 tool_call，工具不能
// 再执行一遍——工具账只在整个 tool_call 完成时记，这一段由最内层工具端点的记录兜住（agent_v2/tool_endpoint_record.go）。
// 曾经每次续跑都把工具再执行一遍：扣款工具扣两次款，err == nil。

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// confirmAfterNext 是「工具执行完、问人确认」的中间件：把工具产出与答复拼成交给模型的结果。
func confirmAfterNext(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
	return func(ctx context.Context, n string, a map[string]any) (*tool.Result, error) {
		r, err := next(ctx, n, a)
		if err != nil {
			return r, err
		}
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: fmt.Sprintf("%v 已完成，确认吗", r.Output)})
		if err != nil {
			return nil, err
		}
		return tool.Success(fmt.Sprintf("%v/%s", r.Output, ans.Content)), nil
	}
}

// driveAnswering 跑 ag，每次停下把所有挂起的问题答「是」再续，返回停下的次数。
func driveAnswering(t *testing.T, ag agent_v2.Agent) int {
	t.Helper()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	answers := map[string]*schema.Message{}
	_, err := ag.Run(ctx, sess, schema.UserMessage("go"))
	pauses := 0
	for ; pauses < 4 && err != nil; pauses++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			t.Fatalf("意外失败：%v", err)
		}
		for _, q := range ie.AllQueries() {
			answers[q.InterruptID] = schema.UserMessage("是")
		}
		_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers), agent_v2.WithResumeSession(sess))
	}
	if err != nil {
		t.Fatalf("续跑没收敛：%v", err)
	}
	return pauses
}

// TestToolEndpointRecord_MiddlewarePausesAfterNext_ToolRunsOnce：扣款工具 + 确认中间件。停一次、答完续跑：工具只扣一次款，
// 模型看到的是第一次扣款的产出加上答复。
func TestToolEndpointRecord_MiddlewarePausesAfterNext_ToolRunsOnce(t *testing.T) {
	var charges atomic.Int64
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("charge", "扣款", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return tool.Success(fmt.Sprintf("扣款#%d", charges.Add(1))), nil
		}))
	var seen []*schema.Message
	m := newStubModelCapturing([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "charge")}),
		schema.AssistantMessage("好了")}, &seen)
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("pay"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(4), agent_v2.WithToolMiddleware(confirmAfterNext))
	if n := driveAnswering(t, ag); n != 1 {
		t.Fatalf("应停一次（中间件问确认），实际 %d 次", n)
	}
	if n := charges.Load(); n != 1 {
		t.Fatalf("中间件在工具执行之后挂起，续跑不该再执行工具：扣款 %d 次", n)
	}
	if got := lastResultFor(t, seen, "c1").Content; got != "扣款#1/是" {
		t.Fatalf("模型应看到第一次扣款的产出加答复，实际 %q", got)
	}
}

// TestToolEndpointRecord_TwoCallsInBatch_EachKeepsItsOwn：同一批两个 tool_call 调同一个工具、参数不同，中间件都在
// next 之后问。续跑时两个各拿回自己的那一份——记录按 tool_call 分开，不串、不重做。
func TestToolEndpointRecord_TwoCallsInBatch_EachKeepsItsOwn(t *testing.T) {
	var runs atomic.Int64
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("pay", "付款", map[string]any{"type": "object"},
		func(_ context.Context, a map[string]any) (*tool.Result, error) {
			runs.Add(1)
			return tool.Success(fmt.Sprintf("付给%v", a["to"])), nil
		}))
	var seen []*schema.Message
	m := newStubModelCapturing([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "c1", Type: "function", Function: schema.FunctionCall{Name: "pay", Arguments: `{"to":"甲"}`}},
			{ID: "c2", Type: "function", Function: schema.FunctionCall{Name: "pay", Arguments: `{"to":"乙"}`}},
		}),
		schema.AssistantMessage("好了")}, &seen)
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("pay2"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(4), agent_v2.WithToolMiddleware(confirmAfterNext))
	driveAnswering(t, ag)
	if n := runs.Load(); n != 2 {
		t.Fatalf("两个调用各执行一次，共 2 次，实际 %d 次", n)
	}
	if got := lastResultFor(t, seen, "c1").Content; got != "付给甲/是" {
		t.Fatalf("c1 应拿回自己的产出，实际 %q", got)
	}
	if got := lastResultFor(t, seen, "c2").Content; got != "付给乙/是" {
		t.Fatalf("c2 应拿回自己的产出，实际 %q", got)
	}
}

// TestToolEndpointRecord_MiddlewareErrorsAfterNext_ResumeDoesNotRerun：中间件在 next 之后返回普通 error（第一次），这次 Run
// 失败、留下失败现场；不带答案续跑重跑这个 tool_call，next 交回记下的那一份、工具不再执行。
func TestToolEndpointRecord_MiddlewareErrorsAfterNext_ResumeDoesNotRerun(t *testing.T) {
	var charges, passes atomic.Int64
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("charge", "扣款", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return tool.Success(fmt.Sprintf("扣款#%d", charges.Add(1))), nil
		}))
	mw := func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
		return func(ctx context.Context, n string, a map[string]any) (*tool.Result, error) {
			r, err := next(ctx, n, a)
			if err != nil {
				return r, err
			}
			if passes.Add(1) == 1 {
				return nil, fmt.Errorf("下游记账服务暂时不可用")
			}
			return r, nil
		}
	}
	var seen []*schema.Message
	m := newStubModelCapturing([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "charge")}),
		schema.AssistantMessage("好了")}, &seen)
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("pay3"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(4), agent_v2.WithToolMiddleware(mw))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(ctx, sess, schema.UserMessage("go")); err == nil {
		t.Fatal("首跑中间件报错，Run 应失败")
	}
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess)); err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	if n := charges.Load(); n != 1 {
		t.Fatalf("中间件在工具执行之后报错，续跑不该再执行工具：扣款 %d 次", n)
	}
	if got := lastResultFor(t, seen, "c1").Content; got != "扣款#1" {
		t.Fatalf("模型应看到第一次扣款的产出，实际 %q", got)
	}
}

// epOrder 是没注册的工具产出类型。
type epOrder struct {
	OrderID int64
	Status  string
}

// TestToolEndpointRecord_UnregisteredOutputSameText：没注册的产出（int64 订单号、结构体）停没停过，模型看到的是同一段
// 文本。曾按 JSON 形状存：订单号续跑后变成 1.8345678901234568e+18、结构体变成 map[...]（审计实测）。
func TestToolEndpointRecord_UnregisteredOutputSameText(t *testing.T) {
	for _, tc := range []struct {
		name string
		out  any
	}{
		{"int64", int64(1834567890123456789)},
		{"struct", epOrder{OrderID: 1834567890123456789, Status: "paid"}},
		{"empty", ""}, // 空串不能变成「没有产出」——那样渲染出来是 <nil>（复审实测）
	} {
		t.Run(tc.name, func(t *testing.T) {
			tk := tool.NewToolkit()
			tk.Register(tool.NewFuncTool("order", "下单", map[string]any{"type": "object"},
				func(context.Context, map[string]any) (*tool.Result, error) {
					return &tool.Result{Success: true, Output: tc.out}, nil
				}))
			var seen []*schema.Message
			m := newStubModelCapturing([]*schema.Message{
				schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "order")}),
				schema.AssistantMessage("好了")}, &seen)
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("od"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
				agent_v2.WithMaxRounds(4), agent_v2.WithToolMiddleware(confirmAfterNext))
			driveAnswering(t, ag)
			if got, want := lastResultFor(t, seen, "c1").Content, fmt.Sprintf("%v/是", tc.out); got != want {
				t.Fatalf("续跑后模型看到的应与没停过时同一段文本 %q，实际 %q", want, got)
			}
		})
	}
}

// TestToolEndpointRecord_ReturnDirectEmptyOutput：return-direct 工具产出空串、中间件 next 之后问确认再原样交回。续跑后最终
// 答复仍是空串，不是 "<nil>"；return-direct 标记（在 Metadata 里）随记录拿回。
func TestToolEndpointRecord_ReturnDirectEmptyOutput(t *testing.T) {
	var runs atomic.Int64
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("rd", "直接答", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			runs.Add(1)
			return &tool.Result{Success: true, Output: "", Metadata: map[string]any{tool.ReturnDirectMetaKey: true}}, nil
		}))
	mw := func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
		return func(ctx context.Context, n string, a map[string]any) (*tool.Result, error) {
			r, err := next(ctx, n, a)
			if err != nil {
				return r, err
			}
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认吗"}); err != nil {
				return nil, err
			}
			return r, nil
		}
	}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("rd"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithToolMiddleware(mw), agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "rd")}), schema.AssistantMessage("不该走到这里")})))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应停在确认上，实际 %v", err)
	}
	events, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("是")}))
	if err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	if n := runs.Load(); n != 1 {
		t.Fatalf("工具应只执行一次，实际 %d 次", n)
	}
	if got := agent_v2.FinalMessage(events); got == nil || got.Content != "" {
		t.Fatalf("return-direct 的最终答复应是工具产出的空串，实际 %#v", got)
	}
}

// TestToolEndpointRecord_LateSuccessStillTimesOut：工具不看 ctx、超过 WithToolTimeout 之后才返回成功，中间件在 next 之后问。
// 没停过时这次调用按超时丢弃；停过之后续跑拿回记录，照样按超时收尾、工具不再执行。
func TestToolEndpointRecord_LateSuccessStillTimesOut(t *testing.T) {
	var runs atomic.Int64
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("slow", "慢", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			runs.Add(1)
			time.Sleep(80 * time.Millisecond)
			return tool.Success("迟到的成功"), nil
		}))
	var seen []*schema.Message
	m := newStubModelCapturing([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "slow")}),
		schema.AssistantMessage("好了")}, &seen)
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("sl"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(4), agent_v2.WithToolTimeout(20*time.Millisecond), agent_v2.WithToolMiddleware(confirmAfterNext))
	driveAnswering(t, ag)
	if n := runs.Load(); n != 1 {
		t.Fatalf("工具应只执行一次，实际 %d 次", n)
	}
	if got := lastResultFor(t, seen, "c1").Content; !strings.HasPrefix(got, "["+agent_v2.ToolTimeoutErrCode+"]") {
		t.Fatalf("迟到的成功续跑后也应按超时收尾，实际模型看到 %q", got)
	}
}

// epReceipt 是注册过的工具产出类型：续跑时中间件拿回的应是原类型，不是 JSON 形状的 map。
type epReceipt struct {
	Amount int `json:"amount"`
}

func init() { compose_v2.RegisterValueType[epReceipt]("agent_v2_test.epReceipt") }

// TestToolEndpointRecord_RegisteredOutputTypeSurvives：工具的 Output 是注册过的类型时，续跑中间件拿回的仍是那个类型。
func TestToolEndpointRecord_RegisteredOutputTypeSurvives(t *testing.T) {
	var typed []bool
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("rcpt", "开票", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: epReceipt{Amount: 42}}, nil
		}))
	mw := func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
		return func(ctx context.Context, n string, a map[string]any) (*tool.Result, error) {
			r, err := next(ctx, n, a)
			if err != nil {
				return r, err
			}
			rc, ok := r.Output.(epReceipt)
			typed = append(typed, ok && rc.Amount == 42)
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认吗"}); err != nil {
				return nil, err
			}
			return tool.Success("ok"), nil
		}
	}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("rc"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithToolMiddleware(mw), agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "rcpt")}), schema.AssistantMessage("好了")})))
	driveAnswering(t, ag)
	if len(typed) != 2 || !typed[0] || !typed[1] {
		t.Fatalf("首跑与续跑中间件都应拿到 epReceipt{42}，实际 %v", typed)
	}
}

// TestToolEndpointRecord_SignalsSurvive：分类器的业务工具里经 RunSubAgent 定路由，中间件在 next 之后问确认。续跑时工具不再
// 执行，它定下的路由必须随记录一起交出。
func TestToolEndpointRecord_SignalsSurvive(t *testing.T) {
	rm := sigRouteModel("math")
	router := agent_v2.NewBasicAgent(agent_v2.WithName("router_llm"), agent_v2.WithModel(rm),
		agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(3))
	var decideRuns atomic.Int32
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("decide", "定路由", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			decideRuns.Add(1)
			if _, err := agent_v2.RunSubAgent(ctx, "route_llm", router, schema.UserMessage("算一下"), agent_v2.RegrantFromNode(ctx, router)...); err != nil {
				return nil, err
			}
			return tool.Success("decided"), nil
		}))
	cls := agent_v2.NewBasicAgent(agent_v2.WithName("cls_llm"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithToolMiddleware(confirmAfterNext), agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{evSegCall("c1", "decide")}), schema.AssistantMessage("分类完")})))
	rt, math, _ := effRouting(cls)
	if got := effDrive(t, rt, "算一下 1+1"); got != "MATH 已处理" || math.runs.Load() != 1 {
		t.Errorf("工具里定下的路由丢了：最终 %q（math 执行 %d 次）", got, math.runs.Load())
	}
	if n := decideRuns.Load(); n != 1 {
		t.Errorf("工具已经执行完、中间件在之后挂起，续跑不该再执行它，实际 %d 次", n)
	}
}
