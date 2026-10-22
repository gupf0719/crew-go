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

// TestFallback_ModelName_GateClosed：首选收到很长的 Retry-After（服务端让等）时门关上、断路器不记失败；门关着的窗口内
// 每次调用都先请求备选，ModelName 也应报备选。
func TestFallback_ModelName_GateClosed(t *testing.T) {
	primary := &namedStub{stubModel: &stubModel{name: "p", respond: func(int, []*schema.Message) (*schema.Message, error) {
		return nil, &model.APIError{Provider: "test", StatusCode: 429, RetryAfter: 10 * time.Minute}
	}}, model: "qwen-max"}
	backup := &namedStub{stubModel: &stubModel{name: "b"}, model: "qwen-plus"}
	fb := fallback.New([]model.ChatModel{primary, backup})
	if _, err := fb.Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")}); err != nil {
		t.Fatalf("应切到备选成功：%v", err)
	}
	if got := fb.ModelName(); got != "qwen-plus" {
		t.Errorf("首选的门关着时下一次调用起手请求备选，应报 qwen-plus，got %q", got)
	}
}

// TestFallback_ModelName_HalfOpenProbe：首选熔断、cooldown 过后，下一次调用会先去探测首选——ModelName 应报首选。
func TestFallback_ModelName_HalfOpenProbe(t *testing.T) {
	fail := true
	primary := &namedStub{stubModel: &stubModel{name: "p", respond: func(int, []*schema.Message) (*schema.Message, error) {
		if fail {
			return nil, &model.APIError{Provider: "test", StatusCode: 503}
		}
		return nil, nil
	}}, model: "qwen-max"}
	backup := &namedStub{stubModel: &stubModel{name: "b"}, model: "qwen-plus"}
	fb := fallback.New([]model.ChatModel{primary, backup}, fallback.WithBreaker(1, 30*time.Millisecond))
	if _, err := fb.Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")}); err != nil {
		t.Fatalf("应切到备选成功：%v", err)
	}
	if got := fb.ModelName(); got != "qwen-plus" {
		t.Fatalf("cooldown 内应报备选，got %q", got)
	}
	time.Sleep(50 * time.Millisecond)
	if got := fb.ModelName(); got != "qwen-max" {
		t.Errorf("cooldown 过后下一次调用先探测首选，应报 qwen-max，got %q", got)
	}
	if got := fb.ModelName(); got != "qwen-max" {
		t.Errorf("ModelName 不该占掉探测名额，再问一次仍应报 qwen-max，got %q", got)
	}
}
