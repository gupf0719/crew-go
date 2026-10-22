package anthropic_test

import (
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
