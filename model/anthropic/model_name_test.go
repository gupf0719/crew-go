package anthropic_test

import (
	"context"
	"testing"

	"crew-go/model"
	"crew-go/model/anthropic"
	"crew-go/schema"
)

// 客户端报构造时配的模型名，WithTools 派生的实例也报同一个。
func TestClient_ModelName(t *testing.T) {
	c := anthropic.NewClient("k", "claude-x")
	if c.ModelName() != "claude-x" {
		t.Fatalf("ModelName=%q", c.ModelName())
	}
	bound, err := c.WithTools([]*schema.ToolInfo{{Name: "t"}})
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := bound.(model.ModelNamer); !ok || n.ModelName() != "claude-x" {
		t.Errorf("WithTools 之后应仍报 claude-x，ok=%v", ok)
	}
}

// TestClient_EmptyModelOverrideIgnored：按次的 model.WithModel("") 不算覆盖，照发构造时的模型名
// ——跟 model.RequestConfig 报给钩子的一致。
//
// ⚠️ 这条**必须单独存在**，而且必须自建带模型名的 client（helpers 里的 newClient 构造期模型名是
// 空串，拿它验不出空覆盖与非空覆盖的差别）。openai 侧有同名哨兵，而这条守卫在两家各有一份实现：
// 实测过把本家的 `*o.Model != ""` 去掉，整个 model/anthropic 照样全绿，只有 openai 那条会红。
func TestClient_EmptyModelOverrideIgnored(t *testing.T) {
	srv, rec := newServer(t, textReply)
	c := anthropic.NewClient("k", "base-model",
		anthropic.WithBaseURL(srv.URL), anthropic.WithHTTPClient(srv.Client()))
	msgs := []*schema.Message{schema.UserMessage("q")}
	for _, opts := range [][]model.Option{{model.WithModel("")}, {model.WithModel("other")}} {
		if _, err := c.Generate(context.Background(), msgs, opts...); err != nil {
			t.Fatalf("Generate: %v", err)
		}
	}
	if got := str(rec.at(t, 0).body, "model"); got != "base-model" {
		t.Errorf("空覆盖应发构造期的 base-model，got %q", got)
	}
	if got := str(rec.at(t, 1).body, "model"); got != "other" {
		t.Errorf("非空覆盖应发 other，got %q", got)
	}
	if got := model.RequestConfig(c, model.WithModel("")); got == nil || got.ModelName != "base-model" {
		t.Errorf("RequestConfig 报的应跟实际发的一致（base-model），got %+v", got)
	}
}
