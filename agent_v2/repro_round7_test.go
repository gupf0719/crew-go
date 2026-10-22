package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
)

// panicHook 在 OnAgentEnd 中故意 panic——验证 Bug 1 BasicAgent defer 必须 recover。
type panicHook struct {
	hook.NoOpHooks
	endCalled atomic.Int64
}

func (h *panicHook) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	h.endCalled.Add(1)
	panic("simulated business hook panic in OnAgentEnd")
}

// TestRepro_R7_Bug1_BasicAgentOnAgentEndPanicCrashesRun 验证 audit Round 7 Bug 1：
// agent_v2/basic_agent.go::Run defer 直接调 a.config.Hooks.OnAgentEnd，业务 hook
// panic 整 Run goroutine 崩；嵌套场景父 cp save 路径被打断。
//
// **预期行为**：业务 hook panic 被兜住、不冒泡到调用方；这次调用已经成功时它不改变结果（记一条带栈的 ERROR，Run 正常返回），
// 调用失败时 panic 并进那次调用的错误里。
func TestRepro_R7_Bug1_BasicAgentOnAgentEndPanicCrashesRun(t *testing.T) {
	stub := newStubModel([]*schema.Message{schema.AssistantMessage("ok")})
	hook1 := &panicHook{}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("bug1_agent"),
		agent_v2.WithModel(stub),
		agent_v2.WithHooks(hook1),
	)

	// 业务 hook panic 不应让 Run 整体崩溃
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Bug 1 reproduced: BasicAgent.Run defer 没 recover business hook panic\n"+
				"caller 看到 panic 冒泡: %v\n"+
				"修复方向：framework Run 入口 outer defer recover 兜底 panic 跟 eino 对齐 (audit² 删 SafeOnAgentEnd wrapper)", r)
		}
	}()

	sess := &agent_v2.RunSession{RunID: "bug1_sid"}
	_, err := ag.Run(context.Background(), sess, schema.UserMessage("hi"))
	// 这次调用已经成功：OnAgentEnd 是观测钩子，它 panic 不把成功报成失败（就地兜住、记 ERROR，见 observer_hook_panic_test.go）
	if err != nil {
		t.Fatalf("调用成功时 OnAgentEnd panic 不该让 Run 失败，got %v", err)
	}
	if hook1.endCalled.Load() == 0 {
		t.Fatal("OnAgentEnd should have been called once")
	}
}

// errorRecorder 记录所有 OnError 调用。
type errorRecorder struct {
	hook.NoOpHooks
	errors []*schema.CommError
}

func (h *errorRecorder) OnError(ctx context.Context, _ *hook.AgentInfo, e *schema.CommError) context.Context {
	h.errors = append(h.errors, e)
	return ctx
}

// TestRepro_R7_Bug3_InterruptErrorReportedToOnError 验证 audit Round 7 Bug 3：
// toolsNode 把 InterruptError 包成 tool_call_error 上报 OnError → 业务监控
// / billing hook 看到 tool_call_error 计数被 HITL 假阳性增。
//
// **预期行为**：InterruptError 不触发 OnError（控制流不是错误）。
//
// **当前 bug**：OnError 被调，CommError.Code = "tool_call_error"。
func TestRepro_R7_Bug3_InterruptErrorReportedToOnError(t *testing.T) {
	rec := &errorRecorder{}

	// 用 agent_v2.NewBasicAgentBuilder 构造 graph 直接验证 toolsNode 行为
	modelFn := func(_ context.Context, _, _ []*schema.Message) (*schema.Message, error) {
		// 返一个含 tool_call 的 assistant message 让 graph 进 toolsNode
		return &schema.Message{
			Role: schema.RoleAssistant,
			ContentBlocks: []*schema.ContentBlock{{
				Type: "tool_call",
				Toolcall: &schema.ToolcallContentBlock{
					Id:        "tc_1",
					Name:      "ask_user_stub",
					Arguments: "{}",
				},
			}},
		}, nil
	}
	toolsFn := func(ctx context.Context, calls []schema.ToolCall, _ []context.Context) ([]*schema.Message, error) {
		// 模拟 ask_user 抛 InterruptError
		return nil, &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{
				Question:    "test",
				InterruptID: "stub_q",
			},
		}
	}

	g, err := agent_v2.NewBasicAgentBuilder(modelFn, toolsFn).
		WithName("bug3_agent").
		WithAgentHooks(rec).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	runErr := compose_v2.Run(context.Background(), g, cm)
	if _, isInt := compose_v2.IsInterrupt(runErr); !isInt {
		t.Fatalf("expected InterruptError, got %v", runErr)
	}

	if len(rec.errors) > 0 {
		codes := make([]string, len(rec.errors))
		for i, e := range rec.errors {
			codes[i] = e.Code
		}
		t.Fatalf("Bug 3 reproduced: InterruptError 误报 OnError\n"+
			"OnError 被调 %d 次，CommError.Codes=%v\n"+
			"期望：HITL 控制流不应触发 OnError\n"+
			"修复方向：safeOnError 调用前先 IsInterrupt 跳过",
			len(rec.errors), codes)
	}
}

// hangingStreamModel.Stream 返非 nil reader 但 producer 永不 send/close——
// 模拟第三方 LLM SDK producer 不感知 ctx 的情况。
type hangingStreamModel struct{}

func (m *hangingStreamModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return nil, fmt.Errorf("not impl")
}
func (m *hangingStreamModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	sr, _ := schema.NewStreamReader[*schema.Message](1)
	// **故意不调 send 也不调 CloseSend**——producer 永远不退出
	return sr, nil
}
func (m *hangingStreamModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

var _ model.ChatModel = (*hangingStreamModel)(nil)

// TestRepro_R7_Bug4_StreamReaderCtxCancel 验证 audit Round 7 Bug 4：
// schema.StreamReader.Recv 不响应 ctx → 第三方 SDK producer 不感知 ctx 时
// caller ctx cancel 完全无效，Recv 永久阻塞。
//
// **预期行为**：ctx cancel 时 Recv 立即返 ctx.Err()。
//
// **当前 bug**：Recv 永久阻塞，ctx cancel 无效。
func TestRepro_R7_Bug4_StreamReaderCtxCancel(t *testing.T) {
	sr, _ := schema.NewStreamReader[string](1)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := sr.Recv(ctx)
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Bug 4 reproduced: Recv 不响应 ctx cancel\n"+
			"期望返 context.Canceled，实际 err=%v\n"+
			"修复方向：Recv signature 加 ctx.Context 参数 + select case <-ctx.Done()",
			err)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("Bug 4: Recv 响应 ctx cancel 过慢 %v（应 ~50ms）", elapsed)
	}
}

// TestRepro_R7_Bug4_StreamOrGenerateCtxCancel 端到端：streamOrGenerate 内部
// reader.Recv(ctx) 应让整个 streamOrGenerate 调用响应 ctx cancel。
func TestRepro_R7_Bug4_StreamOrGenerateCtxCancel(t *testing.T) {
	stub := &hangingStreamModel{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("bug4_agent"),
		agent_v2.WithModel(stub),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := ag.Run(ctx, nil, schema.UserMessage("hi"))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected ctx cancel error, got nil")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Bug 4 e2e: streamOrGenerate 响应 ctx cancel 过慢 %v\n"+
			"修复方向：reader.Recv(ctx) 让 ctx cancel 立刻退出 stream loop\n"+
			"err=%v", elapsed, err)
	}
}

// 注：Bug 5 (empty tool_call ID) repro 在 streaming_internal_test.go 内（white-box
// 直接调 streamOrGenerate）— BasicAgent.Run 黑盒路径无 Toolkit 时不 emit 中间
// assistant 到 sess.Events，无法验证 tool_call 合并问题。
