package model_test

import (
	"context"
	"reflect"
	"testing"

	"crew-go/model"
	"crew-go/schema"
)

// namedModel 只为测 RequestConfig：报自己的模型名。
type namedModel struct{ name string }

func (namedModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, nil
}
func (namedModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m namedModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }
func (m namedModel) ModelName() string                                     { return m.name }

// anonModel 不报模型名。
type anonModel struct{ namedModel }

func (anonModel) ModelName() {} // 签名不同，不满足 ModelNamer

func TestRequestConfig(t *testing.T) {
	cases := []struct {
		name string
		m    model.ChatModel
		opts []model.Option
		want *schema.ModelConfig
	}{
		{"取客户端的模型名", namedModel{"qwen-plus"}, nil, &schema.ModelConfig{ModelName: "qwen-plus"}},
		{"按次覆盖优先", namedModel{"qwen-plus"}, []model.Option{model.WithModel("qwen-max")}, &schema.ModelConfig{ModelName: "qwen-max"}},
		{"空的覆盖不算", namedModel{"qwen-plus"}, []model.Option{model.WithModel("")}, &schema.ModelConfig{ModelName: "qwen-plus"}},
		{"按次参数", namedModel{"m"}, []model.Option{model.WithTemperature(0.3), model.WithMaxTokens(512), model.WithTopP(0.9)},
			&schema.ModelConfig{ModelName: "m", Temperature: 0.3, MaxTokens: 512, TopP: 0.9}},
		{"不报模型名也没参数", anonModel{}, nil, nil},
		{"不报模型名但有覆盖", anonModel{}, []model.Option{model.WithModel("x")}, &schema.ModelConfig{ModelName: "x"}},
		{"仅设 JSON 对象", anonModel{}, []model.Option{model.WithJSONObject()}, &schema.ModelConfig{OutputType: "json"}},
		{"仅设 JSON schema", anonModel{}, []model.Option{model.WithJSONSchema("answer", nil, false)}, &schema.ModelConfig{OutputType: "json"}},
		{"请求 JSON 后改文本", anonModel{}, []model.Option{model.WithJSONObject(), model.WithResponseFormat(&model.ResponseFormat{Type: "text"})},
			&schema.ModelConfig{OutputType: "text"}},
		{"清除请求格式", anonModel{}, []model.Option{model.WithJSONObject(), model.WithResponseFormat(nil)}, nil},
		{"nil 模型", nil, nil, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := model.RequestConfig(c.m, c.opts...)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("got %+v, want %+v", got, c.want)
			}
		})
	}
}
