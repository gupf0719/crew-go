// Package inference 在编排、模型调用和观测层之间传递模型调用的用途。
package inference

import (
	"context"

	"crew-go/model"
	"crew-go/schema"
)

// Purpose 是框架内决策调用的有限取值，零值用于普通业务调用。
type Purpose string

const (
	Routing    Purpose = "routing"
	Evaluation Purpose = "evaluation"
	Reflection Purpose = "reflection"
)

type purposeKey struct{}

func (purposeKey) ExecutionState() {}

// WithPurpose 为决策及其嵌套调用标记用途。独立任务经 DetachTask 清除此标记。
func WithPurpose(ctx context.Context, purpose Purpose) context.Context {
	return context.WithValue(ctx, purposeKey{}, purpose)
}

// PurposeFrom 读取当前执行的模型用途。
func PurposeFrom(ctx context.Context) Purpose {
	v, _ := ctx.Value(purposeKey{}).(Purpose)
	return v
}

type generateObserverKey struct{}

func (generateObserverKey) ExecutionState() {}

// GenerateFunc 发出已装配的模型请求。
type GenerateFunc func(context.Context) (*schema.Message, error)

// GenerateObserver 围绕同一次实际请求记录推理数据。调用 next 一次，并原样返回结果。
type GenerateObserver func(ctx context.Context, agentName string, in *schema.ModelInput, next GenerateFunc) (*schema.Message, error)

// GenerateObserverFrom 从可选的编排钩子能力取得观察方。
func GenerateObserverFrom(hooks any) GenerateObserver {
	if source, ok := hooks.(interface{ GenerateObserver() GenerateObserver }); ok {
		return source.GenerateObserver()
	}
	return nil
}

// ChainGenerateObservers 按钩子顺序包裹请求，同一条链只发一次实际请求。
func ChainGenerateObservers(observers []GenerateObserver) GenerateObserver {
	switch len(observers) {
	case 0:
		return nil
	case 1:
		return observers[0]
	}
	return func(ctx context.Context, agentName string, in *schema.ModelInput, next GenerateFunc) (*schema.Message, error) {
		for i := len(observers) - 1; i >= 0; i-- {
			observe, call := observers[i], next
			next = func(ctx context.Context) (*schema.Message, error) {
				return observe(ctx, agentName, in, call)
			}
		}
		return next(ctx)
	}
}

type generateBinding struct {
	agentName string
	observe   GenerateObserver
}

// WithGenerateObserver 为当前决策范围绑定实际选中的观测方。nil 清除继承的观测方。
func WithGenerateObserver(ctx context.Context, agentName string, observe GenerateObserver) context.Context {
	if observe == nil && ctx.Value(generateObserverKey{}) == nil {
		return ctx
	}
	return context.WithValue(ctx, generateObserverKey{}, generateBinding{agentName: agentName, observe: observe})
}

// Generate 用于框架内直接调模型的决策方。安装观测时，经观测层调用同一个模型和同一份选项。
func Generate(ctx context.Context, m model.ChatModel, msgs []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	if binding, _ := ctx.Value(generateObserverKey{}).(generateBinding); binding.observe != nil && PurposeFrom(ctx) == Routing {
		in := &schema.ModelInput{Config: model.RequestConfig(m, opts...)}
		return binding.observe(ctx, binding.agentName, in, func(callCtx context.Context) (*schema.Message, error) {
			return m.Generate(callCtx, msgs, opts...)
		})
	}
	return m.Generate(ctx, msgs, opts...)
}
