package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// TestReviewV6_H02_ToolsFnFanoutPanicRecover 验 review-v6 H-02 fix —
// ISSUE-81 N>=2 fanout 路径下业务 Tool.Call panic 被 defer recover 转 outcome.err,
// 不让 goroutine panic 冒泡到 Go runtime crash 整 test process.
//
// 历史: BasicAgent.makeToolsFn N=1 inline 路径走 safeNodeRun (graph_loop.go:864)
// 有 defer recover 兜底; N>=2 fanout 路径用 `go func() { runOne(...) }()` 绕开
// safeNodeRun, 业务 Tool.Call panic 时 Go runtime 直接 fatal crash.
//
// sanity-revert: 删 basic_agent.go::runOne 内 defer recover → 整 test process
// crash (Go runtime "fatal: panic in goroutine") → test framework 报
// "test process exited unexpectedly" FAIL.
func TestReviewV6_H02_ToolsFnFanoutPanicRecover(t *testing.T) {
	// 第 1 个 tool 正常
	okTool := tool.NewFuncTool(
		"ok_tool",
		"normal tool",
		map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}, "required": []string{"q"}},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Success("ok_result"), nil
		},
	)
	// 第 2 个 tool panic
	panicTool := tool.NewFuncTool(
		"panic_tool",
		"tool that panics",
		map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}, "required": []string{"q"}},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			panic("simulated business tool panic")
		},
	)
	tk := tool.NewToolkit()
	tk.Register(okTool)
	tk.Register(panicTool)

	// 同一轮 N>=2 tc 触发 fanout 路径
	tc1 := schema.ToolCall{ID: "tc_1", Function: schema.FunctionCall{Name: "ok_tool", Arguments: `{"q":"a"}`}}
	tc2 := schema.ToolCall{ID: "tc_2", Function: schema.FunctionCall{Name: "panic_tool", Arguments: `{"q":"b"}`}}
	resp1 := schema.AssistantToolCallMessage([]schema.ToolCall{tc1, tc2})
	m := newStubModel([]*schema.Message{resp1})

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("h02_test"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(3),
	)

	// fix 后期望: panic 转 outcome.err → graph_loop 收到业务错 → Run 返 error 不 crash 进程
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("trigger 2 tcs"))
	if err == nil {
		t.Fatal("review-v6 H-02 fix: 期望 Run 返非 nil error (panic 转 outcome.err 让 LLM 看到错误), 实际 nil — fix 漏了?")
	}
	// error message 应含 panic 信息让业务可观测
	if !strings.Contains(err.Error(), "panic") {
		t.Errorf("review-v6 H-02 fix: error message 应含 'panic' 让业务定位, 实际 = %q\n"+
			"sanity-revert: 删 runOne 内 defer recover → test process Go runtime 直接 fatal crash → 整测试进程异常退出",
			err.Error())
	}
	// 必须含 tool 名让业务定位是哪个 tool panic
	if !strings.Contains(err.Error(), "panic_tool") {
		t.Errorf("review-v6 H-02 fix: error message 应含 panic 的 tool 名 'panic_tool' 让业务定位, 实际 = %q",
			err.Error())
	}
}

// TestReviewV6_H02_ToolsFnInlinePanicRecover 验 N=1 inline 路径同款也走 runOne
// defer recover (与 fanout 协议一致 — N=1/N>=2 业务行为对称).
func TestReviewV6_H02_ToolsFnInlinePanicRecover(t *testing.T) {
	panicTool := tool.NewFuncTool(
		"solo_panic_tool",
		"single tool that panics",
		map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}, "required": []string{"q"}},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			panic("solo panic")
		},
	)
	tk := tool.NewToolkit()
	tk.Register(panicTool)

	tc := schema.ToolCall{ID: "tc_solo", Function: schema.FunctionCall{Name: "solo_panic_tool", Arguments: `{"q":"a"}`}}
	resp1 := schema.AssistantToolCallMessage([]schema.ToolCall{tc})
	m := newStubModel([]*schema.Message{resp1})

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("h02_inline_test"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(3),
	)

	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("trigger 1 tc"))
	if err == nil {
		t.Fatal("review-v6 H-02 fix: N=1 inline 路径同款 panic 应转 error 不 crash, 实际 nil error")
	}
	// N=1 路径: panic 可能被 runOne defer recover OR 被 safeNodeRun 兜底——两种都接受
	// 关键是不 crash 整进程 + error 非 nil
	if !strings.Contains(err.Error(), "panic") {
		t.Errorf("review-v6 H-02 fix: N=1 inline panic error message 应含 'panic', 实际 = %q",
			err.Error())
	}
}
