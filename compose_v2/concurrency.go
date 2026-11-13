package compose_v2

import "context"

type maxConcurrentNodesKey struct{}

// WithMaxConcurrentNodes 限制每张图的同一超步中同时执行的成员数。
// 0 为不设上限（默认），负数会 panic。嵌套图继承配置，每张图独立调度。
// 成员按声明顺序派发，等待空位时响应 ctx 取消。
func WithMaxConcurrentNodes(ctx context.Context, limit int) context.Context {
	if limit < 0 {
		panic("compose_v2: max concurrent nodes must be non-negative")
	}
	return context.WithValue(ctx, maxConcurrentNodesKey{}, limit)
}
