package tool

import "reflect"

// isToolNil 检 Tool 是否真 nil — 防 Go interface "typed nil" 经典陷阱.
//
// **业务陷阱**: `var t *MyTool; tk.Register(t)` 时 t 是 typed nil pointer 但 Tool
// interface header 持非 nil type tag → `t == nil` false 不触发 fail-fast → 后续
// t.Name() 远 stack nil deref panic.
//
// audit⁷ 协议七复证扩展到 tool 包 (跨包 mirror agent_v2/agent_helpers.go +
// compose_v2/helpers.go).
func isToolNil(t Tool) bool {
	if t == nil {
		return true
	}
	v := reflect.ValueOf(t)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return v.IsNil()
	}
	return false
}
