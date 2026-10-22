package fallback_test

import (
	"context"
	"testing"
	"time"

	"crew-go/fallback"
	"crew-go/model"
	"crew-go/schema"
)

// namedStub 复用包内 stubModel，加上模型名自报。
type namedStub struct {
	*stubModel
	model string
}

func (s *namedStub) ModelName() string { return s.model }

// TestFallback_ModelName：报这次调用起手请求的那个候选——断路器没跳开的第一个；首选熔断之后报下一个。
func TestFallback_ModelName(t *testing.T) {
	primary := &namedStub{stubModel: &stubModel{name: "p", respond: func(int, []*schema.Message) (*schema.Message, error) {
		return nil, &model.APIError{Provider: "test", StatusCode: 503}
	}}, model: "qwen-max"}
	backup := &namedStub{stubModel: &stubModel{name: "b"}, model: "qwen-plus"}
	fb := fallback.New([]model.ChatModel{primary, backup}, fallback.WithBreaker(1, time.Hour))
	if got := fb.ModelName(); got != "qwen-max" {
		t.Fatalf("首选没熔断时应报首选，got %q", got)
	}
	if _, err := fb.Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")}); err != nil {
		t.Fatalf("应切到备选成功：%v", err)
	}
	if got := fb.ModelName(); got != "qwen-plus" {
		t.Errorf("首选熔断之后应报起手请求的备选，got %q", got)
	}
	var _ model.ModelNamer = fb
}
