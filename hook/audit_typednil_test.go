package hook_test

import (
	"testing"

	"crew-go/hook"
)

// TestAudit_TypedNil_HookChain 验 audit⁷ HIGH-5 NewChainHooks 自动过滤 nil / typed nil 元素.
//
// **sanity-revert**: 删 hook/hooks.go NewChainHooks isHooksNil 过滤 → 调 ChainHooks
// 方法时 typed nil 元素 remote stack nil deref panic.
func TestAudit_TypedNil_HookChain(t *testing.T) {
	// typed nil hook
	type myHooks struct{ hook.NoOpHooks }
	var typedNil *myHooks

	t.Run("NewChainHooks 静默过滤 typed nil 元素", func(t *testing.T) {
		// 含 nil + typed nil + real — 应过滤前两个, 保留 real
		real := &myHooks{}
		chain := hook.NewChainHooks(nil, typedNil, real)

		// 调任一方法不 panic — typed nil 元素已被过滤
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("audit⁷ HIGH-5: ChainHooks 调方法 typed nil 元素已过滤不应 panic, got %v\n"+
					"sanity-revert: 删 NewChainHooks isHooksNil 过滤 → typed nil 元素留下, 调 OnAgentEnd 远 stack nil deref",
					r)
			}
		}()
		chain.OnAgentEnd(nil, nil, nil) // 不应 panic
	})
}
