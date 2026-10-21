package compose_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/compose_v2"
)

// TestAudit_TypedNil_AllEntries 综合验 audit²-²⁷ 加 isFrameworkNil/nil 防护的
// compose_v2 入口 sanity-revert. **每个入口都应在传 nil 时 panic-fast** — 删 fix
// 后测试 fail (业务踩 nil 远 stack panic 难定位).
//
// 历史: audit⁷ HIGH-1/2/3 (Worker/AddNode/AddConditionalEdge) + audit⁹ HIGH-3/4/5
// (Custom/DynamicChannel/RegisterChannel) +
// audit¹¹ HIGH-1/2 (MergeInterrupts/WithCheckpointer) + audit¹³ HIGH-1 (WithChannel)
// + audit²¹ HIGH-4 (WithOrchHooks) + audit²³ HIGH-1 (RunWithRetry) + audit²⁴ HIGH-1/2
// (Get/Write) + audit²⁵ HIGH-1/2 (MustGet/Has) 累计 ~17 处入口.
//
// **sanity-revert**: 删任一入口 nil check → 对应 sub-test fail (recover 拿 nil).
func TestAudit_TypedNil_AllEntries(t *testing.T) {
	type testCase struct {
		name      string
		fn        func()
		wantPanic bool   // false 表示应返 nil/error 不 panic
		wantMsg   string // panic msg / error msg 关键字
	}

	noopFn := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	}
	_ = noopFn // 防 unused import

	tests := []testCase{
		// compose_v2/graph.go builder typed nil
		{
			name: "AddNode nil fn",
			fn: func() {
				b := compose_v2.NewBuilder()
				b.AddNode("a", nil)
			},
			wantPanic: true,
			wantMsg:   "AddNode: nil fn",
		},
		{
			name: "AddConditionalEdge nil cond",
			fn: func() {
				b := compose_v2.NewBuilder()
				b.AddConditionalEdge("a", nil)
			},
			wantPanic: true,
			wantMsg:   "AddConditionalEdge: nil cond",
		},
		// (PlanBuilder.Worker nil fn case 删除: plan builder 已迁出 compose_v2 到
		// agent_v2, 低层 PlanBuilder 不再存在; agent_v2 worker 注册的 typed-nil 防护
		// 由 AddSubAgent/SetExecutor 的 isAgentNil 覆盖, 见 agent_v2 测试)
		// compose_v2/channel.go reducer
		{
			name: "Custom nil reducer",
			fn: func() {
				_ = compose_v2.Custom[int]("x", nil, 0)
			},
			wantPanic: true,
			wantMsg:   "Custom: nil reducer",
		},
		{
			name: "DynamicChannel nil reducer",
			fn: func() {
				_ = compose_v2.DynamicChannel[int]("x", nil, 0)
			},
			wantPanic: true,
			wantMsg:   "DynamicChannel: nil reducer",
		},
		// compose_v2/channel_manager.go - RegisterChannel 走 error 路径单独测见 TestAudit_TypedNil_ErrorReturning
		// compose_v2/graph.go WithChannel
		{
			name: "WithChannel nil ch",
			fn: func() {
				b := compose_v2.NewBuilder()
				compose_v2.WithChannel[int](b, nil)
			},
			wantPanic: true,
			wantMsg:   "WithChannel: nil Channel",
		},
		// compose_v2/access.go Get / Write / MustGet / Has
		{
			name: "Get nil ch panic",
			fn: func() {
				cm := compose_v2.NewChannelManager()
				_, _ = compose_v2.Get(cm.Channels(), (*compose_v2.Channel[int])(nil))
			},
			wantPanic: true,
			wantMsg:   "Get: nil *Channel",
		},
		{
			name: "Write nil ch panic",
			fn: func() {
				_ = compose_v2.Write[int](nil, 0)
			},
			wantPanic: true,
			wantMsg:   "Write: nil *Channel",
		},
		{
			name: "MustGet nil ch panic",
			fn: func() {
				cm := compose_v2.NewChannelManager()
				_ = compose_v2.MustGet[int](cm.Channels(), nil)
			},
			wantPanic: true,
			wantMsg:   "MustGet: nil *Channel",
		},
		{
			name: "Has nil ch returns false (no panic)",
			fn: func() {
				cm := compose_v2.NewChannelManager()
				if compose_v2.Has[int](cm.Channels(), nil) {
					panic("expected false got true")
				}
			},
			wantPanic: false,
		},
		// compose_v2/retry.go - RunWithRetry 走 error 路径单独测见 TestAudit_TypedNil_ErrorReturning
		// compose_v2/interrupt.go
		{
			name: "MergeInterrupts nil elements filtered (no panic)",
			fn: func() {
				// 含 nil 元素不应 panic
				_ = compose_v2.MergeInterrupts([]*compose_v2.InterruptError{nil, nil})
			},
			wantPanic: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if tt.wantPanic && r == nil {
					t.Errorf("audit²-²⁷ sanity-revert: %s 应 panic, got no panic\n"+
						"sanity-revert: 删该入口 nil check → 测试 fail", tt.name)
					return
				}
				if !tt.wantPanic && r != nil {
					t.Errorf("%s 不应 panic, got %v", tt.name, r)
					return
				}
				if tt.wantPanic && tt.wantMsg != "" {
					msg := ""
					switch v := r.(type) {
					case string:
						msg = v
					case error:
						msg = v.Error()
					}
					if !strings.Contains(msg, tt.wantMsg) {
						t.Errorf("%s panic msg 应含 %q, got %q\n"+
							"sanity-revert: 删该入口 nil check 或改 msg → 测试 fail",
							tt.name, tt.wantMsg, msg)
					}
				}
			}()
			tt.fn()
		})
	}
}

// TestAudit_TypedNil_ChainOrchHooks 验 audit²⁸ HIGH-1 NewChainOrchHooks
// typed-nil 元素过滤 sanity-revert.
//
// **sanity-revert**: 改 hooks.go::NewChainOrchHooks 回 `if h != nil` → typed nil
// 元素留下. 调时业务 OnGraphStart override 触发 m.field 访问 → typed nil deref
// → safeOnGraphStart panic recover 转 wrap error "OnGraphStart panic recovered".
// 本测试断: 过滤生效则 OnGraphStart 返 nil (typed nil 不被调). 未过滤则返非 nil
// wrap error.
func TestAudit_TypedNil_ChainOrchHooks(t *testing.T) {
	// 业务自定义 override OnGraphStart 访问 field — typed nil 调时 deref panic.
	// 测试不嵌入 NoOpOrchestrationHooks fall-through (会让 typed nil 调 NoOp value
	// receiver 不 deref 假绿).
	chain := compose_v2.NewChainOrchHooks(nil, (*derefOrchHook)(nil), &derefOrchHook{name: "real"})

	_, err := chain.OnGraphStart(context.Background(), compose_v2.Channels{})
	if err != nil {
		t.Errorf("audit²⁸ HIGH-1: NewChainOrchHooks typed nil 元素未过滤 → 调 OnGraphStart 远 stack deref panic 被 safeOn* recover → 拿 wrap err: %v\n"+
			"sanity-revert: hooks.go::NewChainOrchHooks 改回 'if h != nil' → 测试 fail (typed nil 留下被调时 deref m.name → panic recover 转 wrap error)", err)
	}
}

// derefOrchHook 业务自定义 OrchHooks impl — OnGraphStart 访问 m.name field 让
// typed nil 调时 deref panic (用于验 typed-nil 过滤是否真生效).
type derefOrchHook struct {
	compose_v2.NoOpOrchestrationHooks
	name string
}

func (m *derefOrchHook) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	_ = m.name // typed nil 时 nil deref panic
	return ctx, nil
}

// TestAudit_TypedNil_ErrorReturning 走 error 返回路径的入口 — 不是 panic.
// 验 nil 入参返 error + msg 含期望关键字.
func TestAudit_TypedNil_ErrorReturning(t *testing.T) {
	// RegisterChannel - audit⁹ HIGH-5
	t.Run("RegisterChannel nil cm", func(t *testing.T) {
		err := compose_v2.RegisterChannel((*compose_v2.ChannelManager)(nil), compose_v2.LastValue[int]("x"))
		if err == nil || !strings.Contains(err.Error(), "nil ChannelManager") {
			t.Errorf("audit⁹ HIGH-5: RegisterChannel nil cm 应返 error 含 'nil ChannelManager', got %v\n"+
				"sanity-revert: 删 channel_manager.go RegisterChannel nil cm check → 测试 fail", err)
		}
	})
	t.Run("RegisterChannel nil ch", func(t *testing.T) {
		cm := compose_v2.NewChannelManager()
		err := compose_v2.RegisterChannel(cm, (*compose_v2.Channel[int])(nil))
		if err == nil || !strings.Contains(err.Error(), "nil Channel") {
			t.Errorf("audit⁹ HIGH-5: RegisterChannel nil ch 应返 error 含 'nil Channel', got %v\n"+
				"sanity-revert: 删 channel_manager.go RegisterChannel nil ch check → 测试 fail", err)
		}
	})

	// RunWithRetry - audit²³ HIGH-1
	t.Run("RunWithRetry nil fn", func(t *testing.T) {
		_, err := compose_v2.RunWithRetry[int](
			context.Background(),
			compose_v2.SimpleRetry(0),
			nil,
		)
		if err == nil || !strings.Contains(err.Error(), "nil fn") {
			t.Errorf("audit²³ HIGH-1: RunWithRetry nil fn 应返 error 含 'nil fn', got %v\n"+
				"sanity-revert: 删 retry.go RunWithRetry nil fn check → 测试 fail", err)
		}
	})
}
