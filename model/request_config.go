package model

import (
	"math"

	"crew-go/schema"
)

// ModelNamer 由知道自己请求哪个模型的 ChatModel 实现：ModelName 返回不带按次覆盖时请求的模型名（构造客户端时
// 配的那个）。框架据此把本次调用的请求模型交给钩子（ModelInput.Config.ModelName，观测层记成
// gen_ai.request.model）。不实现时请求模型记不上，只剩响应里带回来的模型名。
//
// 包装别的 ChatModel 的实现（装饰器、故障切换）应当转发里面那个的模型名。
type ModelNamer interface {
	ModelName() string
}

// RequestConfig 返回一次调用的请求配置，给钩子看（ModelInput.Config）：模型名取按次的 WithModel 覆盖，没有时取
// m 实现的 ModelNamer；温度、max_tokens、top_p 取按次选项里设了的。什么都没有时返回 nil。
//
// 只反映「调用方这次请求了什么」：客户端构造时配的默认参数（默认温度之类）不在内，ModelConfig 的零值也分不出
// 「设成 0」与「没设」——温度 0 不会出现在这里。
func RequestConfig(m ChatModel, opts ...Option) *schema.ModelConfig {
	o := Apply(opts...)
	cfg := &schema.ModelConfig{}
	switch {
	case o.Model != nil && *o.Model != "":
		cfg.ModelName = *o.Model
	case m != nil:
		if n, ok := m.(ModelNamer); ok {
			cfg.ModelName = n.ModelName()
		}
	}
	if o.Temperature != nil {
		cfg.Temperature = *o.Temperature
	}
	if o.MaxTokens != nil {
		cfg.MaxTokens = int32(min(*o.MaxTokens, math.MaxInt32))
	}
	if o.TopP != nil {
		cfg.TopP = *o.TopP
	}
	if cfg.ModelName == "" && cfg.Temperature == 0 && cfg.MaxTokens == 0 && cfg.TopP == 0 {
		return nil
	}
	return cfg
}
