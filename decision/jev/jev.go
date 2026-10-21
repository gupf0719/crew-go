// Package jev 接入 TypeSafe 的决策模型 Jev：直连 TypeSafe 的 System One API。
//
// 构造出的是 decision.Client，请求、答案与路由决策方都在 decision 包里：
//
//	c := jev.NewClient(os.Getenv("TYPESAFE_API_KEY"))
//	decider := decision.NewRouteDecider(c)
package jev

import "crew-go/decision"

const (
	// BaseURL 是 TypeSafe API 的官方地址。
	BaseURL = "https://api.typesafe.ai"
	// Model 是默认模型别名，指向当前稳定版本。
	Model = "jev-latest"
)

// preset 是直连 TypeSafe 的服务方描述。
var preset = decision.Preset{
	Provider:        "typesafe",
	BaseURL:         BaseURL,
	DefaultModel:    Model,
	RequestIDHeader: "x-typesafe-request-id",
}

// NewClient 用 TypeSafe 的 API key 构造直连 Jev 的客户端，默认模型是 [Model]。opts 是
// decision 包的选项，比如用 decision.WithModel 固定到某个版本、用 decision.WithBaseURL 经
// 网关转发。key 为空或含控制字符时 panic。
func NewClient(apiKey string, opts ...decision.Option) *decision.Client {
	return decision.NewClient(apiKey, preset, opts...)
}
