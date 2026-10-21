package mcp

import "reflect"

// isClientNil 检 Client 是否真 nil — 防 Go interface "typed nil" 经典陷阱.
//
// 业务 `var c *MyClient; WithClient(c)` 时 c 是 typed nil pointer 但 Client interface
// header 持非 nil type tag → `c == nil` false → 后续 c.ListTools / c.CallTool 远 stack nil deref.
//
// audit¹⁸ 协议十八复证扩展到 mcp 包.
func isClientNil(c Client) bool {
	if c == nil {
		return true
	}
	v := reflect.ValueOf(c)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return v.IsNil()
	}
	return false
}
