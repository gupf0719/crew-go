package hook_test

// 只作观测的 OnAgentEnd / OnError 上，链里一个钩子 panic，排在后面的照样调到（计时、计费、错误计数不静默丢），之后第一个
// panic 原样交给调用点——原值不变，调用点在 defer 里取的栈看得到 panic 的钩子。

import (
	"context"
	"errors"
	"runtime/debug"
	"strings"
	"testing"

	"crew-go/hook"
	"crew-go/internal/panicerr"
	"crew-go/schema"
)

type endPanicHook struct {
	hook.NoOpHooks
}

func (endPanicHook) OnAgentEnd(context.Context, *hook.AgentInfo, *hook.AgentOutput) context.Context {
	panic("boom-agent-end")
}

func (endPanicHook) OnError(context.Context, *hook.AgentInfo, *schema.CommError) context.Context {
	panic("boom-on-error")
}

type endCounter struct {
	hook.NoOpHooks
	agentEnd, onError int
}

func (c *endCounter) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	c.agentEnd++
	return ctx
}

func (c *endCounter) OnError(ctx context.Context, _ *hook.AgentInfo, _ *schema.CommError) context.Context {
	c.onError++
	return ctx
}

var errHookSentinel = errors.New("hook sentinel")

type sentinelPanicHook struct {
	hook.NoOpHooks
	v any
}

func (h sentinelPanicHook) OnAgentEnd(context.Context, *hook.AgentInfo, *hook.AgentOutput) context.Context {
	panic(h.v)
}

// 业务自己调钩子链、自己 recover：拿到的就是钩子 panic 的那个值（类型、errors.Is、== 都对得上），在 defer 里取的栈看得到
// panic 的钩子——跟没有这条链时一样。曾经抛的是一个不导出的包装，这几样全对不上（审计实测）。
func TestChainHooks_BusinessRecoverSeesOriginalPanic(t *testing.T) {
	for _, v := range []any{errHookSentinel, "boom-str"} {
		var raw any
		var stack string
		func() {
			defer func() {
				raw = recover()
				stack = string(debug.Stack())
			}()
			hook.NewChainHooks(sentinelPanicHook{v: v}, &endCounter{}).OnAgentEnd(context.Background(), &hook.AgentInfo{}, &hook.AgentOutput{})
		}()
		if raw != v {
			t.Fatalf("业务 recover 到的应是原值 %v，实际 %T %v", v, raw, raw)
		}
		if err, ok := v.(error); ok {
			if got, isErr := raw.(error); !isErr || !errors.Is(got, err) {
				t.Fatalf("panic 值是 error 时业务应能 errors.Is 认出它，实际 %T", raw)
			}
		}
		if !strings.Contains(stack, "sentinelPanicHook.OnAgentEnd") {
			t.Fatalf("业务在 defer 里取的栈应看得到 panic 的钩子：\n%s", stack)
		}
	}
}

func TestChainHooks_ObserverPanicDoesNotSkipLaterHooks(t *testing.T) {
	cases := []struct {
		name, value, frame string
		call               func(c *hook.ChainHooks)
		count              func(c *endCounter) int
	}{
		{"OnAgentEnd", "boom-agent-end", "endPanicHook.OnAgentEnd",
			func(c *hook.ChainHooks) { c.OnAgentEnd(context.Background(), &hook.AgentInfo{}, &hook.AgentOutput{}) },
			func(c *endCounter) int { return c.agentEnd }},
		{"OnError", "boom-on-error", "endPanicHook.OnError",
			func(c *hook.ChainHooks) { c.OnError(context.Background(), &hook.AgentInfo{}, &schema.CommError{}) },
			func(c *endCounter) int { return c.onError }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			counter := &endCounter{}
			chain := hook.NewChainHooks(endPanicHook{}, endPanicHook{}, counter)
			var pe *panicerr.Error
			func() {
				defer func() {
					if r := recover(); r != nil {
						pe = panicerr.New(r)
					}
				}()
				tc.call(chain)
			}()
			if got := tc.count(counter); got != 1 {
				t.Fatalf("排在两个 panic 后面的钩子应被调一次，实际 %d 次", got)
			}
			if pe == nil {
				t.Fatal("调完整条链之后应抛出第一个 panic")
			}
			if pe.Value != tc.value || !strings.Contains(pe.Stack, tc.frame) {
				t.Fatalf("应是第一个 panic 的原值与它当初的栈，实际 value=%v\n%s", pe.Value, pe.Stack)
			}
		})
	}
}
