package compose_v2

import "reflect"

// isFrameworkNil 是包级通用 typed nil 检测 — 防 Go interface "typed nil" 经典陷阱.
//
// **业务陷阱**: `var fn NodeFunc; AddNode(id, fn)` 时 fn 是 typed nil func 但
// `NodeFunc` interface header 持非 nil type tag → `fn == nil` false 不触发 fail-fast →
// 业务后续 fn(ctx, chs) 远 stack nil deref panic 难定位.
//
// **覆盖 kind**: Ptr / Interface / Map / Slice / Chan / Func / UnsafePointer
// (Go reflect 可 IsNil 的全部 kind). struct value 实现 (非 ptr receiver) zero value
// 不算 nil 是 Go 语义, 业务自负.
//
// 与 agent_v2/agent_helpers.go::isFrameworkNil 同款逻辑 (跨包 mirror) — audit⁷
// 协议七复证扩展到 compose_v2 builder 接收点 (Worker / AddNode / AddConditionalEdge /
// EdgeCondition / NodeFunc 等).
func isFrameworkNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return rv.IsNil()
	}
	return false
}
