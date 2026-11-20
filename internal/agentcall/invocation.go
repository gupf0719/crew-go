package agentcall

import "context"

type invocationKey struct{}

func (invocationKey) ExecutionState() {}

// Invocation 标识一次进入 Agent 的调用。使用非零大小的值，地址可作为身份。
type Invocation byte

// Begin 为本次调用分配身份，入口和结束钩子共同使用。
func Begin(ctx context.Context) context.Context {
	return context.WithValue(ctx, invocationKey{}, new(Invocation))
}

// From 取得调用身份。宿主直接调用钩子时可能没有身份。
func From(ctx context.Context) *Invocation {
	id, _ := ctx.Value(invocationKey{}).(*Invocation)
	return id
}
