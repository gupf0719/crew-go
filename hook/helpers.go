package hook

import "reflect"

// isHooksNil 检 Hooks 是否真 nil — 防 Go interface "typed nil" 经典陷阱.
//
// audit⁷ HIGH-5 fix: 协议七复证扩展到 hook 包. 业务 `var h *MyHooks;
// NewChainHooks(h, observer)` 时 h 是 typed nil pointer 但 Hooks interface header
// 持非 nil type tag → `h == nil` false → ChainHooks 调时 h.OnAgentStart 远 stack
// nil deref panic.
func isHooksNil(h Hooks) bool {
	if h == nil {
		return true
	}
	v := reflect.ValueOf(h)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return v.IsNil()
	}
	return false
}
