package agent_v2_test

// 工具调用失败在 hook 边界的可见性回归（issue-95 finding 3）。
//
// **承重语义**：`observability` 按 `!out.Result.Success` 判 status=error 并 fire
// `gen_ai_tool_errors`（24 个 framework auto-fire metric 之一）。若 toolsNode 构造
// `ToolCallResult` 时硬编码 `Success: true`，业务工具 `tool.Failure()` 明确声明的失败
// 就在边界被抹平 → 那个 metric 对工具失败**恒为 0**，业务照 metric 协议建的告警永不响。
//
// sanity-revert：把 basic_builder.go 里读 annotation 还原 Success/Error/ErrorCode 那段
// 删掉（退回硬编码 `Success: true`）→ 本文件 BizFailure / FrameworkRejection 两条 fail。

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

type toolEndRecorder struct {
	hook.NoOpHooks
	mu      sync.Mutex
	results []schema.ToolCallResult // 值拷贝——防 hook 返回后框架改写污染记录
	rewrite func(*schema.ToolCallResult)
}

func (h *toolEndRecorder) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if out == nil || out.Result == nil {
		return ctx, nil
	}
	h.mu.Lock()
	h.results = append(h.results, *out.Result)
	h.mu.Unlock()
	if h.rewrite != nil {
		h.rewrite(out.Result)
	}
	return ctx, nil
}

func (h *toolEndRecorder) snapshot() []schema.ToolCallResult {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]schema.ToolCallResult(nil), h.results...)
}

// runOneToolCall 跑一轮「LLM 调 toolName → 拿 tool_result → LLM 收尾」，返回 hook 记录 +
// 最终喂回 LLM 历史的 tool_result content。
func runOneToolCall(t *testing.T, tk *tool.Toolkit, toolName string, h *toolEndRecorder) []schema.ToolCallResult {
	t.Helper()
	tc := schema.ToolCall{ID: "tc_1", Function: schema.FunctionCall{Name: toolName, Arguments: `{}`}}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("tfv"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{tc}),
			schema.AssistantMessage("done"),
		})),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(h),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return h.snapshot()
}

// TestToolFailureVisibility_BizFailure 是主证：业务工具 tool.Failure 声明的失败必须
// 原样到达 hook 边界（Success=false + 结构化 Error/ErrorCode）。
func TestToolFailureVisibility_BizFailure(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("flaky", "flaky api", map[string]any{"type": "object"},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Failure("rate_limited", "upstream 429"), nil
		}))

	h := &toolEndRecorder{}
	got := runOneToolCall(t, tk, "flaky", h)
	if len(got) != 1 {
		t.Fatalf("应恰 1 次 OnToolEnd, got %d", len(got))
	}
	r := got[0]
	if r.Success {
		t.Errorf("业务工具返回 tool.Failure 时 OnToolEnd 应收到 Success=false, got true\n" +
			"  → observability 按 !Success 计 gen_ai_tool_errors，恒 true 意味着该 metric 对工具失败恒为 0")
	}
	if r.ErrorCode != "rate_limited" {
		t.Errorf("ErrorCode: want %q, got %q（结构化错误信息在 hook 边界丢失）", "rate_limited", r.ErrorCode)
	}
	if r.Error != "upstream 429" {
		t.Errorf("Error: want %q, got %q", "upstream 429", r.Error)
	}
	// 消息流无损：错误文本仍进 tool_result content 喂 LLM
	if !strings.Contains(r.Output, "rate_limited") || !strings.Contains(r.Output, "upstream 429") {
		t.Errorf("Output 应含错误文本（LLM 要看到才能重试/换路），got %q", r.Output)
	}
}

// TestToolFailureVisibility_SuccessUnaffected 守没回归：成功工具照旧 Success=true、
// 不带 Error/ErrorCode。
func TestToolFailureVisibility_SuccessUnaffected(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("ok_tool", "works", map[string]any{"type": "object"},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Success("all good"), nil
		}))

	h := &toolEndRecorder{}
	got := runOneToolCall(t, tk, "ok_tool", h)
	if len(got) != 1 {
		t.Fatalf("应恰 1 次 OnToolEnd, got %d", len(got))
	}
	r := got[0]
	if !r.Success || r.Error != "" || r.ErrorCode != "" {
		t.Errorf("成功工具应 Success=true 且无 Error/ErrorCode, got %+v", r)
	}
	if r.Output != "all good" {
		t.Errorf("Output: want %q, got %q", "all good", r.Output)
	}
}

// TestToolFailureVisibility_FrameworkRejection：框架级拒绝（工具压根没跑起来）同样是
// 失败的 tool call。这里用 invalid_arguments（LLM 吐坏 JSON）。
func TestToolFailureVisibility_FrameworkRejection(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("any_tool", "x", map[string]any{"type": "object"},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			t.Error("坏 JSON 时工具不该被执行")
			return tool.Success("nope"), nil
		}))

	// Arguments 不是合法 JSON → makeToolsFn 在调工具前就拒绝
	tc := schema.ToolCall{ID: "tc_bad", Function: schema.FunctionCall{Name: "any_tool", Arguments: `{"broken`}}
	h := &toolEndRecorder{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("tfv_reject"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{tc}),
			schema.AssistantMessage("done"),
		})),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(h),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := h.snapshot()
	if len(got) != 1 {
		t.Fatalf("应恰 1 次 OnToolEnd, got %d", len(got))
	}
	if got[0].Success {
		t.Errorf("框架拒绝（坏 JSON，工具没跑起来）应 Success=false, got true —— 这是失败的 tool call")
	}
	if got[0].ErrorCode != "invalid_arguments" {
		t.Errorf("ErrorCode: want invalid_arguments, got %q", got[0].ErrorCode)
	}
}

// TestToolFailureVisibility_Timeout：**框架自己收尾**的失败——工具超过
// WithToolTimeout 未返回时，框架丢弃结果、合成一条 tool_result。它同样必须带失败
// marker。
//
// 这条曾是唯一漏标的框架合成路径（合成点写成裸 ToolResultMessage，marker 忘了打）：
// OnToolEnd 收到 Success=true ⟹ gen_ai.tool.error.total 不计超时、span 标成功——
// **最需要被观测到的工具失败反而不可见**。合成入口现已收敛到 failedToolResult。
//
// sanity-revert：把 basic_agent.go 超时分支换回裸 schema.ToolResultMessage ⟹ 本条红。
func TestToolFailureVisibility_Timeout(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("slow_tool", "慢工具", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(2 * time.Second):
				return tool.Success("late"), nil
			}
		}))

	tc := schema.ToolCall{ID: "tc_slow", Function: schema.FunctionCall{Name: "slow_tool", Arguments: `{}`}}
	h := &toolEndRecorder{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("tfv_timeout"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{tc}),
			schema.AssistantMessage("done"),
		})),
		agent_v2.WithToolkit(tk),
		agent_v2.WithToolTimeout(50*time.Millisecond),
		agent_v2.WithHooks(h),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := h.snapshot()
	if len(got) != 1 {
		t.Fatalf("应恰 1 次 OnToolEnd, got %d", len(got))
	}
	if got[0].Success {
		t.Errorf("工具超时应 Success=false, got true —— 观测层按 !Success 计 gen_ai.tool.error.total，"+
			"漏标时超时这类失败恒不计、span 还标成功。Output=%q", got[0].Output)
	}
	if got[0].ErrorCode == "" {
		t.Errorf("超时应带 ErrorCode 供观测层分类, got 空")
	}
	if !strings.Contains(got[0].Output, "超时") {
		t.Errorf("Output 应说明超时（LLM 要看到才能换路），got %q", got[0].Output)
	}
}

// capturingStub 是会**读 input 做断言**的 stub model——记下每轮收到的 messages，
// 让测试能看到最终喂回 LLM 的 tool_result content 长什么样。
type capturingStub struct {
	mu        sync.Mutex
	seen      [][]*schema.Message
	responses []*schema.Message
	idx       int
}

func (m *capturingStub) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	m.seen = append(m.seen, append([]*schema.Message(nil), msgs...))
	out := schema.AssistantMessage("[exhausted]")
	if m.idx < len(m.responses) {
		out = m.responses[m.idx]
		m.idx++
	}
	m.mu.Unlock()
	return out, nil
}

func (m *capturingStub) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *capturingStub) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// lastToolContent 返回最后一轮 LLM 输入里 tool_result 的 content——即真正喂回模型的东西。
func (m *capturingStub) lastToolContent(t *testing.T) string {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.seen) < 2 {
		t.Fatalf("应至少 2 轮 LLM 调用（tool_call → tool_result 回炉），got %d", len(m.seen))
	}
	last := m.seen[len(m.seen)-1]
	for i := len(last) - 1; i >= 0; i-- {
		if last[i].Role == schema.RoleTool {
			return last[i].Content
		}
	}
	t.Fatal("最后一轮 LLM 输入里没有 tool_result")
	return ""
}

func runWithCapturingStub(t *testing.T, h *toolEndRecorder) *capturingStub {
	t.Helper()
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("flaky2", "x", map[string]any{"type": "object"},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Failure("rate_limited", "upstream 429"), nil
		}))
	tc := schema.ToolCall{ID: "tc_rw", Function: schema.FunctionCall{Name: "flaky2", Arguments: `{}`}}
	stub := &capturingStub{responses: []*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{tc}),
		schema.AssistantMessage("done"),
	}}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("tfv_rw"),
		agent_v2.WithModel(stub),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(h),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return stub
}

// TestToolFailureVisibility_HookNoRewriteKeepsContent 守**回写不变式**：hook 什么都不改
// 时，喂回 LLM 的 tool_result content 必须与工具原本产出的完全一致。
//
// 为什么容易坏：toolsNode 在 OnToolEnd 之后会按 `[ErrorCode] Error` **重算** content 写回
// （这是消费 hook 改写的机制）。填 Success/Error/ErrorCode 时若与 Output 拼不回原样，
// **hook 明明没改，tool_result 也会被悄悄改掉**——本 fix 正好动这三个字段。
func TestToolFailureVisibility_HookNoRewriteKeepsContent(t *testing.T) {
	stub := runWithCapturingStub(t, &toolEndRecorder{}) // 被动 hook：只观测
	if got := stub.lastToolContent(t); got != "[rate_limited] upstream 429" {
		t.Errorf("hook 未改写时喂回 LLM 的 tool_result content 应原样保持 %q, got %q\n"+
			"  → 填 Success/Error/ErrorCode 与 Output 拼不回原样，回写把内容改掉了",
			"[rate_limited] upstream 429", got)
	}
}

// TestToolFailureVisibility_HookRewriteTakesEffect 守 [消费] 档语义没退化：hook 改写
// Result.Error（结果脱敏）必须真生效到喂回 LLM 的 content。
//
// 本 fix 之前这条**做不到**——hook 收到的 Success 恒 true，回写分支 `!Success && Error != ""`
// 永远进不去，改 Error 等于白改。
func TestToolFailureVisibility_HookRewriteTakesEffect(t *testing.T) {
	h := &toolEndRecorder{rewrite: func(r *schema.ToolCallResult) {
		r.Error = "REDACTED" // 结果脱敏：把上游错误详情抹掉再喂 LLM
	}}
	stub := runWithCapturingStub(t, h)
	if got := stub.lastToolContent(t); got != "[rate_limited] REDACTED" {
		t.Errorf("hook 对失败 tool_result 的脱敏改写应生效, want %q got %q\n"+
			"  → Success 恒 true 时回写分支 `!Success && Error != \"\"` 进不去，改写被静默丢弃",
			"[rate_limited] REDACTED", got)
	}
}
