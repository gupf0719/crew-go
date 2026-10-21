package tutorial_test

// adv_10 mock：Hooks 三件套（timing + error + lifecycle）+ ChainHooks

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

func TestAdv_10_Hooks_Mock(t *testing.T) {
	pingTool := tool.NewFuncTool("ping", "ping host",
		map[string]any{"type": "object", "properties": map[string]any{
			"host": map[string]any{"type": "string"},
		}, "required": []string{"host"}},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Success("延迟 12ms 丢包 0%"), nil
		})
	tk := tool.NewToolkit()
	tk.Register(pingTool)

	round := int32(0)
	stub := &stubModel{reply: func(_, _ string, _ []*schema.Message) *schema.Message {
		n := atomic.AddInt32(&round, 1)
		if n == 1 {
			return schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID: "tc1", Type: "function",
				Function: schema.FunctionCall{Name: "ping", Arguments: `{"host":"192.168.1.1"}`},
			}})
		}
		return schema.AssistantMessage("诊断报告：网络正常 12ms 延迟")
	}}

	th := newTimingHookMock(t)
	errHook := &errorCaptureHookMock{}
	lcHook := &lifecycleLogHookMock{}
	chained := hook.NewChainHooks(th, errHook, lcHook)
	a := agent_v2.NewBasicAgent(
		agent_v2.WithName("network_diagnostics"),
		agent_v2.WithSystemPrompt("网络诊断"),
		agent_v2.WithModel(stub),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(chained),
		agent_v2.WithMaxRounds(6),
	)
	events, err := a.Run(ctx60sMock(t), nil, schema.UserMessage("ping 192.168.1.1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := agent_v2.FinalMessage(events)
	t.Logf("model=%d tool=%d 生命周期=%v errs=%v 最终=%s",
		th.modelCalls, th.toolStarts, lcHook.log, errHook.records, out.Content)
	if th.modelCalls < 2 {
		t.Errorf("期望 ≥2 LLM 调用（含 tool round-trip），实际 %d", th.modelCalls)
	}
	if th.toolStarts != 1 {
		t.Errorf("期望 1 次 tool 调用，实际 %d", th.toolStarts)
	}
	if len(lcHook.log) < 2 {
		t.Errorf("期望 lifecycle log 至少 2 条（START + END）, 实际 %d", len(lcHook.log))
	}
}

type errorCaptureHookMock struct {
	hook.NoOpHooks
	mu      sync.Mutex
	records []string
}

func (h *errorCaptureHookMock) OnError(ctx context.Context, info *hook.AgentInfo, err *schema.CommError) context.Context {
	if err == nil {
		return ctx
	}
	h.mu.Lock()
	h.records = append(h.records, fmt.Sprintf("[%s] %s", info.Name, err.Message))
	h.mu.Unlock()
	return ctx
}

type lifecycleLogHookMock struct {
	hook.NoOpHooks
	mu  sync.Mutex
	log []string
}

func (h *lifecycleLogHookMock) OnAgentStart(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	h.mu.Lock()
	h.log = append(h.log, fmt.Sprintf("START %s", info.Name))
	h.mu.Unlock()
	return ctx, nil
}

func (h *lifecycleLogHookMock) OnAgentEnd(ctx context.Context, info *hook.AgentInfo, output *hook.AgentOutput) context.Context {
	h.mu.Lock()
	h.log = append(h.log, fmt.Sprintf("END %s msgs=%d", info.Name, len(output.Messages)))
	h.mu.Unlock()
	return ctx
}
