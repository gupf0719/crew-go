package hook_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/hook"
	"crew-go/modules/common"
	specModel "crew-go/modules/model"
	"crew-go/schema"
)

// audit³ A4 fix 守护测试.
//
// A4: ChainHooks.OnModelEndStream hook panic 跳过 cleanup() 让 wrappedStack
// stream 泄漏. 修法: 函数顶部 defer recover + cleanup + 重抛.
//
// sanity-revert: 删 ChainHooks.OnModelEndStream 顶部 defer recover → 此测 panic
// 时 wrappedStack 内 stream 不被 Close → assertions fail.

// trackedStream 是 fakeStream 升级版——记录是否被 Close.
type trackedStream struct {
	id     string
	closed bool
}

func (s *trackedStream) Recv(_ context.Context) (*common.Message, error) {
	return nil, errors.New("trackedStream.Recv 未使用")
}

func (s *trackedStream) Close() error {
	s.closed = true
	return nil
}

// wrappingHook 把入参 stream wrap 成新 trackedStream + 选择性 panic.
type wrappingHook struct {
	hook.NoOpHooks
	id      string
	wrapped *trackedStream // wrap 出的新 stream — 测试用
	doPanic bool
}

func (h *wrappingHook) OnModelEndStream(ctx context.Context, _ *hook.AgentInfo, sr specModel.MessageStreamReader) (context.Context, specModel.MessageStreamReader, error) {
	if h.doPanic {
		panic("audit3 A4: OnModelEndStream panic")
	}
	h.wrapped = &trackedStream{id: h.id + "-wrap"}
	return ctx, h.wrapped, nil
}

// TestAudit3_A4_ChainHooks_OnModelEndStreamPanic_CleanupRuns 验:
// hook B panic 时, hook A 已 wrap 的 stream 应被 Close (不泄漏).
func TestAudit3_A4_ChainHooks_OnModelEndStreamPanic_CleanupRuns(t *testing.T) {
	h1 := &wrappingHook{id: "h1"} // wrap 出 h1-wrap
	h2 := &wrappingHook{id: "h2", doPanic: true}
	chain := hook.NewChainHooks(h1, h2)

	original := &trackedStream{id: "original"}

	// 期 panic 冒泡——audit² fail-fast 协议
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("audit² panic 协议: ChainHooks.OnModelEndStream 内 hook panic 应冒泡到 caller")
		}
		// 关键断言: hook A wrapped 的 stream 被 Close (cleanup 跑了)
		if h1.wrapped == nil {
			t.Fatal("h1 应已 wrap 出 stream (panic 发生前)")
		}
		if !h1.wrapped.closed {
			t.Errorf("audit³ A4 fix regression: hook B panic 后 hook A 的 wrapped stream 未被 Close (cleanup 未跑 → 泄漏)\n" +
				"sanity-revert: 删 ChainHooks.OnModelEndStream 顶部 defer recover + cleanup + 重抛 → 此测 fail")
		}
		// original 也应被 Close
		if !original.closed {
			t.Errorf("audit³ A4 fix regression: original stream 未被 Close")
		}
	}()

	_, _, _ = chain.OnModelEndStream(context.Background(), &hook.AgentInfo{}, original)
}

// ── 补 ChainHooks 8 method panic 守护 (audit³ task #14) ──
//
// 已有: TestChainHooks_PanicBubblesUp (OnAgentEnd).
// 补 6 method: OnAgentStart / OnModelStart / OnModelEnd / OnToolStart / OnToolEnd / OnError.
// 协议: 任一 hook 方法 panic 应冒泡到 caller (不在 ChainHooks 内 silent isolate),
// framework outer recover (compose_v2.Run / agent_v2 各编排 Run) 兜底转 wrapped err.

type fullPanicHook struct {
	hook.NoOpHooks
	method string // 哪个方法 panic
}

func (h *fullPanicHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if h.method == "start" {
		panic("boom-start")
	}
	return ctx, nil
}
func (h *fullPanicHook) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	if h.method == "modelStart" {
		panic("boom-modelStart")
	}
	return ctx, nil
}
func (h *fullPanicHook) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
	if h.method == "modelEnd" {
		panic("boom-modelEnd")
	}
	return ctx, nil
}
func (h *fullPanicHook) OnToolStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ToolInput) (context.Context, error) {
	if h.method == "toolStart" {
		panic("boom-toolStart")
	}
	return ctx, nil
}
func (h *fullPanicHook) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ToolOutput) (context.Context, error) {
	if h.method == "toolEnd" {
		panic("boom-toolEnd")
	}
	return ctx, nil
}
func (h *fullPanicHook) OnError(ctx context.Context, _ *hook.AgentInfo, _ *schema.CommError) context.Context {
	if h.method == "error" {
		panic("boom-error")
	}
	return ctx
}

func TestAudit3_ChainHooks_AllMethodsPanicBubble(t *testing.T) {
	// 7 method panic 守护 — 加 OnAgentEnd (audit² 已守) 共 7 method (OnModelEndStream wrap 语义另测).
	type runFn func(c *hook.ChainHooks)
	cases := []struct {
		method string
		run    runFn
	}{
		{"start", func(c *hook.ChainHooks) {
			_, _ = c.OnAgentStart(context.Background(), &hook.AgentInfo{}, &hook.AgentInput{})
		}},
		{"modelStart", func(c *hook.ChainHooks) {
			_, _ = c.OnModelStart(context.Background(), &hook.AgentInfo{}, &schema.ModelInput{})
		}},
		{"modelEnd", func(c *hook.ChainHooks) {
			_, _ = c.OnModelEnd(context.Background(), &hook.AgentInfo{}, &schema.ModelOutput{})
		}},
		{"toolStart", func(c *hook.ChainHooks) {
			_, _ = c.OnToolStart(context.Background(), &hook.AgentInfo{}, &schema.ToolInput{})
		}},
		{"toolEnd", func(c *hook.ChainHooks) {
			_, _ = c.OnToolEnd(context.Background(), &hook.AgentInfo{}, &schema.ToolOutput{})
		}},
		{"error", func(c *hook.ChainHooks) {
			c.OnError(context.Background(), &hook.AgentInfo{}, &schema.CommError{})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.method, func(t *testing.T) {
			chain := hook.NewChainHooks(&fullPanicHook{method: tc.method})
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("audit² panic 协议: ChainHooks.%s panic 应冒泡到 caller", tc.method)
				}
			}()
			tc.run(chain)
		})
	}
}
