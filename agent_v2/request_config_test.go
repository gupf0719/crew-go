package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
)

// reqCfgModel 报自己的模型名，直接给最终答复。
type reqCfgModel struct{ name string }

func (m *reqCfgModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage("ok"), nil
}

func (m *reqCfgModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return schema.NewSingleChunkReader(schema.AssistantMessage("ok")), nil
}

func (m *reqCfgModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }
func (m *reqCfgModel) ModelName() string                                     { return m.name }

// reqCfgHooks 记下 OnModelStart 收到的 Config。
type reqCfgHooks struct {
	hook.NoOpHooks
	got []*schema.ModelConfig
}

func (h *reqCfgHooks) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	h.got = append(h.got, in.Config)
	return ctx, nil
}

// TestBasicAgent_ModelInputConfig：OnModelStart 收到的 ModelInput.Config 带本次请求的模型名与按次参数——模型名取
// 按次的 model.WithModel 覆盖，没有时取客户端报的；按次换模型（WithRunModel）时报换上的那个。
func TestBasicAgent_ModelInputConfig(t *testing.T) {
	run := func(t *testing.T, opts ...agent_v2.RunOption) *schema.ModelConfig {
		t.Helper()
		h := &reqCfgHooks{}
		ag := agent_v2.NewBasicAgent(agent_v2.WithName("cfg"), agent_v2.WithModel(&reqCfgModel{name: "qwen-plus"}), agent_v2.WithHooks(h))
		if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"), opts...); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if len(h.got) != 1 || h.got[0] == nil {
			t.Fatalf("OnModelStart 应收到 1 次非空 Config，got %v", h.got)
		}
		return h.got[0]
	}
	t.Run("客户端的模型名", func(t *testing.T) {
		if c := run(t); c.ModelName != "qwen-plus" {
			t.Errorf("ModelName=%q 应=qwen-plus", c.ModelName)
		}
	})
	t.Run("按次覆盖与参数", func(t *testing.T) {
		c := run(t, agent_v2.WithRunModelOptions(model.WithModel("qwen-max"), model.WithTemperature(0.3), model.WithMaxTokens(256)))
		if c.ModelName != "qwen-max" || c.Temperature != 0.3 || c.MaxTokens != 256 {
			t.Errorf("got %+v，应是 qwen-max / 0.3 / 256", *c)
		}
	})
	t.Run("按次换模型", func(t *testing.T) {
		if c := run(t, agent_v2.WithRunModel(&reqCfgModel{name: "deepseek-v4"})); c.ModelName != "deepseek-v4" {
			t.Errorf("ModelName=%q 应=deepseek-v4", c.ModelName)
		}
	})
}
