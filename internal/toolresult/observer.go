// Package toolresult 在 BasicAgent 与宿主之间传递已提交的工具结果。
package toolresult

import (
	"context"
	"sync/atomic"

	"crew-go/schema"
)

type observerKey struct{}

func (observerKey) Environment() {}

type deferredKey struct{}

func (deferredKey) ExecutionState() {}

type startScopeKey struct{}

func (startScopeKey) ExecutionState() {}

type startScope struct {
	ctx    context.Context
	in     *schema.ToolInput
	callID string
}

// Observer 属于一次宿主执行，只有这个订阅方可以登记等待提交的展示。
type Observer struct {
	Deliver func(context.Context, *schema.ToolOutput)
}

// WithObserver 为本次执行注册已提交工具结果的观察者。
func WithObserver(ctx context.Context, observe *Observer) context.Context {
	return context.WithValue(ctx, observerKey{}, observe)
}

// ObserverFrom 读取宿主注册的观察者。
func ObserverFrom(ctx context.Context) *Observer {
	observe, _ := ctx.Value(observerKey{}).(*Observer)
	return observe
}

// Delivery 描述已经过实际结束钩子的展示。默认只记录是否展示，正文取提交结果。
type Delivery struct {
	Projected bool                   `json:"projected,omitempty"`
	Result    *schema.ToolCallResult `json:"result,omitempty"`
	ErrorCode *string                `json:"error_code,omitempty"`
}

// Project 将调用方显式提供的展示副本用于显示。批内裁决等后续产生的新失败使用提交结果。
func (d *Delivery) Project(result *schema.ToolCallResult) *schema.ToolCallResult {
	if !d.Projected || result != nil && !result.Success && (d.ErrorCode == nil || *d.ErrorCode != result.ErrorCode) {
		return result
	}
	return d.Result
}

// DeferredOutput 收集这次结束钩子选定的展示，钩子被拒绝或挂起时随尝试弃掉。
type DeferredOutput struct {
	observer *Observer
	source   *schema.ToolOutput
	callID   string
	delivery atomic.Pointer[Delivery]
}

// WithDeferredOutput 标记此钩子正在处理等待提交的结果。
func WithDeferredOutput(ctx context.Context, observer *Observer, out *schema.ToolOutput, callID string) (context.Context, *DeferredOutput) {
	d := &DeferredOutput{observer: observer, source: out, callID: callID}
	return context.WithValue(ctx, deferredKey{}, d), d
}

// DeferDelivery 接受当前调用的展示。装饰钩子可以为同一个调用提供一份展示副本。
func (o *Observer) DeferDelivery(ctx context.Context, out *schema.ToolOutput, callID string) bool {
	d, _ := ctx.Value(deferredKey{}).(*DeferredOutput)
	if d == nil || d.observer != o || d.source != out && !sameCall(d.callID, callID) {
		return false
	}
	if d.delivery.Load() != nil {
		return true
	}
	before, display := d.source.Result, out.Result
	delivery := &Delivery{Projected: d.source != out && resultChanged(before, display)}
	if delivery.Projected {
		if display != nil {
			// 存档只需要供宿主显示的字段，不再次保存模型侧的错误正文。
			delivery.Result = &schema.ToolCallResult{Success: display.Success}
			if display.Success {
				delivery.Result.Output = display.Output
			} else {
				delivery.Result.ErrorCode = display.ErrorCode
			}
		}
		if before != nil && !before.Success {
			code := before.ErrorCode
			delivery.ErrorCode = &code
		}
	}
	d.delivery.CompareAndSwap(nil, delivery)
	return true
}

// Delivery 返回本次结束钩子选定的展示。nil 表示订阅方没有收到本次结束钩子。
func (d *DeferredOutput) Delivery() *Delivery {
	return d.delivery.Load()
}

func resultChanged(before, display *schema.ToolCallResult) bool {
	if before == display {
		return false
	}
	if before == nil || display == nil || before.Success != display.Success {
		return true
	}
	if before.Success {
		return before.Output != display.Output
	}
	return before.ErrorCode != display.ErrorCode
}

// WithStartScope 记录框架的开始钩子相位。业务钩子再派生相位时，发送记录仍写到此处。
func WithStartScope(ctx context.Context, in *schema.ToolInput, callID string) context.Context {
	return context.WithValue(ctx, startScopeKey{}, startScope{ctx: ctx, in: in, callID: callID})
}

// StartScope 返回这次调用的框架相位。自研工具发送入口沿用调用方的 ctx。
func StartScope(ctx context.Context, in *schema.ToolInput, callID string) (context.Context, bool) {
	if scope, ok := ctx.Value(startScopeKey{}).(startScope); ok && (scope.in == in || sameCall(scope.callID, callID)) {
		return scope.ctx, true
	}
	return ctx, false
}

func sameCall(first, second string) bool { return first != "" && first == second }
