// agent span error.message 完整性测试 — 验证 BasicAgent 因**非通信类编排错误**
// (graph maxVisit 触顶 MaxIterationsError) 失败时, agent span 是否记到 error.message.
//
// ────────────────────────────────────────────────────────────────────────
// 怀疑的 gap (读代码推断, 本测试坐实/推翻):
//
// observability 的 agent span error.message 不是 OnAgentEnd 直接写的 — OnAgentEnd
// 用 GetEndError 拿顶层 retErr **只判 status**, 给 endSpan 传的 errMsg 是空 (hooks.go
// :470). agent span 的 error.message 完全靠 endSpan 内部读 OnError 记的 lastError 补
// (observability.go:856-867).
//
// 而 OnError 入参是 schema.CommError (通信错误) — BasicAgent + 6 编排的主体执行错误
// 直接 `return nil, err` 不 fire OnError (basic_agent.go:244-254). 只有 model/tool
// 通信层 / GraphAgent·PlanAgent 主体路径 fire OnError.
//
// 推断: BasicAgent 因纯编排错误 (MaxIterations) 失败 → 无任何 OnError fire →
// endSpan 读 lastErrors[agent key] miss → agent span status=error 但 error.message 空.
//
// 对照组 (防假绿, 仿 span_lifecycle_test 的 PlainTool 对照): 通信错误 (model 返 error)
// 走 OnError → error.message 应非空. 证明测试 harness 本身能拿到 error.message,
// 是特定错误类型 (非 CommError 编排错误) 才落空.
// ────────────────────────────────────────────────────────────────────────
package observability

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// mockChatLoop: 永远返 tool_call 从不终止 → BasicAgent ReAct 循环跑到 graph maxVisit
// 触顶 MaxIterationsError (非 CommError 编排层错误; model/tool 本身都成功不 fire OnError).
type mockChatLoop struct{ tools []*schema.ToolInfo }

func (m *mockChatLoop) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	cp := *m
	cp.tools = tools
	return &cp, nil
}

func (m *mockChatLoop) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:       "call_loop",
		Type:     "function",
		Function: schema.FunctionCall{Name: "get_weather", Arguments: `{}`},
	}}), nil
}

func (m *mockChatLoop) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(resp)
	schema.CloseSend(sr)
	return sr, nil
}

var _ model.ChatModel = (*mockChatLoop)(nil)

// mockChatErr: Generate / Stream 都返通信错误 → 触发 model_call_error OnError (对照组).
type mockChatErr struct{ tools []*schema.ToolInfo }

func (m *mockChatErr) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	cp := *m
	cp.tools = tools
	return &cp, nil
}

func (m *mockChatErr) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return nil, errors.New("model boom: upstream 503 service unavailable")
}

func (m *mockChatErr) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, errors.New("model boom: upstream 503 service unavailable")
}

var _ model.ChatModel = (*mockChatErr)(nil)

// TestBasicAgent_OrchestrationError_ErrorMessageGap — 主体: BasicAgent 编排错误
// (MaxIterations) 时 agent span error.message 是否落空.
func TestBasicAgent_OrchestrationError_ErrorMessagePresent(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	tk := tool.NewToolkit()
	tk.Register(&weatherToolRegression{})
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("loop_agent"),
		agent_v2.WithModel(&mockChatLoop{}),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(obs.AgentHooks()),
		agent_v2.WithMaxRounds(3), // 小值快速触顶
	)

	ctx := compose_v2.WithSessionIDCtx(context.Background(), "orcherr_sid")
	ctx = agent_v2.WithCtxOrchHooks(ctx, obs.OrchHooks())
	_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("loop?"))
	obs.CleanupSession("orcherr_sid")

	t.Logf("Run 顶层 retErr = %v", err)

	time.Sleep(500 * time.Millisecond)
	spans := srv.Spans()

	agentSpan := findSpanByOperation(spans, "agent")
	if agentSpan == nil {
		t.Fatalf("应有 agent span, got ops=%v", spanOps(spans))
	}
	status := findAttrString(agentSpan, "gen_ai.status")
	errType := findAttrString(agentSpan, "error.type")
	errMsg := findAttrString(agentSpan, "error.message")
	t.Logf("agent span: status=%q error.type=%q error.message=%q", status, errType, errMsg)

	// 修复后正向断言 (sanity-revert: revert endSpan fallback fix 后这些 FAIL 精确指向 bug).
	if err == nil {
		t.Fatalf("mockChatLoop 永不终止, 应触发 MaxIterations 错误, got nil")
	}
	if status != "error" {
		t.Errorf("MaxIterations 应记 status=error, got %q", status)
	}
	if errMsg == "" {
		t.Errorf("【回归】编排错误 agent span error.message 为空 — endSpan fallback 兜底失效; "+
			"顶层 retErr=%q 未记入 span", err.Error())
	}
	// message 应来自顶层 retErr (MaxIterations), 验证 fallback 取的是正确来源 (不止非空).
	if errMsg != "" && !strings.Contains(errMsg, "迭代") && !strings.Contains(errMsg, "max") {
		t.Errorf("error.message=%q 不含 MaxIterations 特征 (迭代/max), fallback 来源可能错", errMsg)
	}
}

// TestBasicAgent_CommError_ErrorMessagePresent — 对照组: 通信错误 (model 返 error)
// 走 OnError → agent span error.message 应非空. 证明 harness 能拿到 error.message.
func TestBasicAgent_CommError_ErrorMessagePresent(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("err_agent"),
		agent_v2.WithModel(&mockChatErr{}),
		agent_v2.WithHooks(obs.AgentHooks()),
		agent_v2.WithMaxRounds(3),
	)

	ctx := compose_v2.WithSessionIDCtx(context.Background(), "commerr_sid")
	ctx = agent_v2.WithCtxOrchHooks(ctx, obs.OrchHooks())
	_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi?"))
	obs.CleanupSession("commerr_sid")

	t.Logf("Run 顶层 retErr = %v", err)

	time.Sleep(500 * time.Millisecond)
	spans := srv.Spans()

	agentSpan := findSpanByOperation(spans, "agent")
	if agentSpan == nil {
		t.Fatalf("应有 agent span, got ops=%v", spanOps(spans))
	}
	status := findAttrString(agentSpan, "gen_ai.status")
	errType := findAttrString(agentSpan, "error.type")
	errMsg := findAttrString(agentSpan, "error.message")
	t.Logf("agent span: status=%q error.type=%q error.message=%q", status, errType, errMsg)

	// 对照断言: 通信错误经 OnError → error.message 应含 model 错误内容 (非空).
	if status == "error" && errMsg == "" {
		t.Errorf("对照组失败: 通信错误 (model boom) agent span error.message 仍空 — " +
			"说明 harness 拿不到 error.message, 测试本身有问题")
	}
}

// TestGraphAgent_BusinessInputWriterPanic_ErrorMessagePresent 端到端坐实 gap(2) 业务
// 可触发面:GraphAgent 的业务 inputWriter(必填回调)panic 时——它在 runGraph 的
// compose_v2.Run **之前**调、OnAgentEnd defer **之后**、runGraph 自身**无 recover** →
// 逃逸到 OnAgentEnd defer——agent span error.message 应非空(经 OnAgentEnd defer 内
// recoverPanicToErr 设 retErr)。比 panic_recover_internal_test.go 契约测试(模拟 defer
// 结构)更强:走**真实 GraphAgent.Run 路径**,坐实"业务可触发 + fix 兜住"。
// sanity-revert:移除 graph_agent.go 的 recoverPanicToErr → error.message 空 → 本测试 FAIL。
func TestGraphAgent_BusinessInputWriterPanic_ErrorMessagePresent(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	inputCh := compose_v2.LastValue[*schema.Message]("__panic_in__")
	outputCh := compose_v2.LastValue[*schema.Message]("__panic_out__")
	bld := compose_v2.NewBuilder()
	compose_v2.WithChannel(bld, inputCh)
	compose_v2.WithChannel(bld, outputCh)
	bld.AddNode("echo", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inputCh)
		return []compose_v2.ChannelWrite{compose_v2.Write(outputCh, in)}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "echo")
	bld.AddEdge("echo", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	ga := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("panic_iw_agent"),
		// 业务 inputWriter panic —— gap(2) 业务可触发面:在 runGraph compose_v2.Run 之前调,
		// OnAgentEnd defer 之后, runGraph 无自己的 recover → 逃逸到 OnAgentEnd defer.
		agent_v2.WithGraphInputWriter(func(_ *schema.Message) []compose_v2.ChannelWrite {
			panic(errors.New("business inputWriter boom: bad type assertion"))
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outputCh)
			return out
		}),
		agent_v2.WithGraphAgentHooks(obs.AgentHooks()),
	)

	ctx := compose_v2.WithSessionIDCtx(context.Background(), "graphpanic_sid")
	ctx = agent_v2.WithCtxOrchHooks(ctx, obs.OrchHooks())
	_, runErr := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	obs.CleanupSession("graphpanic_sid")

	// panic 被 recoverToErr/recoverPanicToErr 兜成 err 返业务(不 crash 进程)
	if runErr == nil {
		t.Fatalf("业务 inputWriter panic 应被兜成 err 返回(不 crash), got nil")
	}
	t.Logf("Run 返回(panic 转 err) = %v", runErr)

	time.Sleep(500 * time.Millisecond)
	spans := srv.Spans()
	agentSpan := findSpanByOperation(spans, "agent")
	if agentSpan == nil {
		t.Fatalf("应有 agent span, got ops=%v", spanOps(spans))
	}
	status := findAttrString(agentSpan, "gen_ai.status")
	errMsg := findAttrString(agentSpan, "error.message")
	t.Logf("agent span: status=%q error.message=%q", status, errMsg)

	// 核心断言(gap(2) 业务可触发 + fix 兜住): 业务 inputWriter panic 时 agent span
	// error.message 非空(经 OnAgentEnd defer recoverPanicToErr). sanity-revert:移除
	// graph_agent.go recoverPanicToErr → error.message 空 → 此处 FAIL 精确指向 bug.
	if errMsg == "" {
		t.Errorf("【gap(2) 回归】业务 inputWriter panic 时 agent span error.message 空 — "+
			"OnAgentEnd defer recoverPanicToErr 未兜住框架 wrapper panic(runErr=%v)", runErr)
	}
	if errMsg != "" && !strings.Contains(errMsg, "boom") {
		t.Errorf("error.message=%q 不含 panic 内容 'boom', fallback 来源可能错", errMsg)
	}
}
