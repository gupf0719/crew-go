package openai_test

import (
	"testing"

	"crew-go/model"
	"crew-go/model/openai"
	"crew-go/schema"
)

// 客户端报构造时配的模型名，WithTools 派生的实例也报同一个——框架拿它当请求模型交给钩子。
func TestClient_ModelName(t *testing.T) {
	c := openai.NewClient("k", "gpt-x")
	if c.ModelName() != "gpt-x" {
		t.Fatalf("ModelName=%q", c.ModelName())
	}
	bound, err := c.WithTools([]*schema.ToolInfo{{Name: "t"}})
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := bound.(model.ModelNamer); !ok || n.ModelName() != "gpt-x" {
		t.Errorf("WithTools 之后应仍报 gpt-x，ok=%v", ok)
	}
}
