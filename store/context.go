package store

import (
	"context"
	"reflect"
)

// ctxKey 是 Store ctx 注入的私有 key 类型——遵循 CLAUDE.md "context.WithValue
// 的 key 用私有类型，避免碰撞"约定。
type ctxKey struct{}

// Environment 让 ctxKey 满足 `compose_v2.EnvironmentKey`（接口按方法集匹配，本包不必引用它）：
// store 是调用方给整个请求配的环境。框架采纳业务钩子返回的 ctx 时，那个 ctx 是外来的（别处缓存的、
// `context.Background()`）就取这次请求的 store——曾经跟着钩子返回的走，一个钩子挂在两个各自配了
// store 的 agent 上、沿用存下的 ctx 时，后一个 agent 的长期记忆写进了前一个的 store（实测）。
func (ctxKey) Environment() {}

// WithContext 注入 store 到 ctx——caller-facing helper。
//
// 业务（如 agent_v2.WithStore option 实现）想让下游 Run 调用从 ctx 取 store 而
// 不是显式传 RunOption 时用此 helper。跟 compose_v2.WithCheckpointerCtx 协议对
// 称（[compose_v2/checkpointer.go::WithCheckpointerCtx]）。
//
// **typed nil 防护**：业务 `var nilSt *MyStore; WithContext(ctx, nilSt)` 时
// nilSt != nil 但 underlying nil → 后续 st.Get() 远 stack panic。isFrameworkNil
// 协议对齐 compose_v2.WithCheckpointerCtx audit¹¹ HIGH-2 fix。
//
// 典型用法：
//
//	ctx = store.WithContext(ctx, st)
//	events, err := agent.Run(ctx, sess, input)  // helper / hook / tool 内 FromContext 拿 st
func WithContext(ctx context.Context, st Store) context.Context {
	if isFrameworkNil(st) {
		// 等价 untyped nil 注 nil——不破坏"无 store"idiom，让 FromContext 返 nil
		// 让 helper 自查 ErrStoreNotConfigured fail-fast。
		return context.WithValue(ctx, ctxKey{}, Store(nil))
	}
	return context.WithValue(ctx, ctxKey{}, st)
}

// FromContext 从 ctx 取 store——业务节点 / hook / tool 调用。
//
// 未注入时返 nil（业务自己 nil check 决定降级行为 / 抛 ErrStoreNotConfigured）。
//
//	st := store.FromContext(ctx)
//	if st == nil {
//	    return store.ErrStoreNotConfigured
//	}
//	item, err := st.Get(ctx, store.NS("users", uid), "theme")
func FromContext(ctx context.Context) Store {
	st, _ := ctx.Value(ctxKey{}).(Store)
	return st
}

// isFrameworkNil 是 typed nil 检测——防 Go interface "typed nil" 经典陷阱。
//
// 跟 compose_v2/helpers.go::isFrameworkNil + agent_v2/agent_helpers.go::
// isFrameworkNil 同款逻辑（跨包 mirror）——audit⁷ 协议七复证扩展到 store 包 ctx
// 注入接收点。
//
// **覆盖 kind**：Ptr / Interface / Map / Slice / Chan / Func / UnsafePointer
// （Go reflect 可 IsNil 的全部 kind）。
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
