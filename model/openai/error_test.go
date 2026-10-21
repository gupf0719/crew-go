package openai_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/model/openai"
	"crew-go/schema"
)

// TestClient_Generate_APIError 验证非 200 响应被包成结构化 *model.APIError，
// StatusCode / Provider / Body 正确——对齐 model-fallback.md P0 验收
// （构造 400 / 429 / 503 响应断言 errors.As 拿到正确 StatusCode）。
func TestClient_Generate_APIError(t *testing.T) {
	for _, code := range []int{http.StatusBadRequest, http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		code := code
		t.Run(http.StatusText(code), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
				_, _ = w.Write([]byte(`{"error":{"message":"boom"}}`))
			}))
			defer srv.Close()

			c := openai.NewClient("k", "m", openai.WithBaseURL(srv.URL), openai.WithSystem("openai"))
			_, err := c.Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")})

			var apiErr *model.APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("err 不是 *model.APIError: %T %v", err, err)
			}
			if apiErr.StatusCode != code {
				t.Fatalf("StatusCode = %d, want %d", apiErr.StatusCode, code)
			}
			if apiErr.Provider != "openai" {
				t.Fatalf("Provider = %q, want openai", apiErr.Provider)
			}
			if !strings.Contains(apiErr.Body, "boom") {
				t.Fatalf("Body 未含响应体: %q", apiErr.Body)
			}
		})
	}
}

// TestClient_Stream_APIError 验证 Stream 建流阶段非 200 也包成 *model.APIError，
// 且 provider 取 WithSystem 注入值（这里 qwen 复用 openai.Client 的场景）。
func TestClient_Stream_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("service unavailable"))
	}))
	defer srv.Close()

	c := openai.NewClient("k", "m", openai.WithBaseURL(srv.URL), openai.WithSystem("qwen"))
	_, err := c.Stream(context.Background(), []*schema.Message{schema.UserMessage("hi")})

	var apiErr *model.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err 不是 *model.APIError: %T %v", err, err)
	}
	if apiErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("StatusCode = %d, want 503", apiErr.StatusCode)
	}
	if apiErr.Provider != "qwen" {
		t.Fatalf("Provider = %q, want qwen（WithSystem 注入）", apiErr.Provider)
	}
}

// TestClient_NetworkError_APIError 验证网络层失败包成 StatusCode==0 的 *model.APIError，
// 让 FailoverPolicy 能区分"接入点不可达（该切）"与控制流信号；Err 非空可继续 errors.Is。
func TestClient_NetworkError_APIError(t *testing.T) {
	// 指向保留地址 127.0.0.1:1（无监听）触发 Do 失败
	c := openai.NewClient("k", "m", openai.WithBaseURL("http://127.0.0.1:1"), openai.WithSystem("openai"))
	_, err := c.Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")})

	var apiErr *model.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err 不是 *model.APIError: %T %v", err, err)
	}
	if apiErr.StatusCode != 0 {
		t.Fatalf("StatusCode = %d, want 0（网络错）", apiErr.StatusCode)
	}
	if apiErr.Err == nil {
		t.Fatal("网络错的 APIError.Err 应非 nil")
	}
}

// TestClient_FillsRetryAfterFromResponseHeader 钉住**客户端真的把响应头读进了
// APIError.RetryAfter**——Generate 与 Stream 两条路径各一次。
//
// **为什么这条必须独立存在**：这个特性的其余测试都绕开了这一环——`parseRetryAfter`
// 是拿字符串喂的纯函数单测，重试层那两条是拿**手搓的** `*model.APIError` 喂的。也就是
// 说把 client 里那行 `RetryAfter: parseRetryAfter(...)` 整个删掉，**全仓一条测试都不会红**，
// 而线上的表现是退避悄悄退回瞎猜（服务端明明说了等多久）。
//
// 用 429 + `Retry-After: 3`：秒数形式是 provider 实际最常发的那种。
func TestClient_FillsRetryAfterFromResponseHeader(t *testing.T) {
	newSrv := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "3")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"message":"rate limited"}}`))
		}))
	}

	call := map[string]func(c *openai.Client) error{
		"Generate": func(c *openai.Client) error {
			_, err := c.Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")})
			return err
		},
		"Stream": func(c *openai.Client) error {
			_, err := c.Stream(context.Background(), []*schema.Message{schema.UserMessage("hi")})
			return err
		},
	}

	for name, do := range call {
		t.Run(name, func(t *testing.T) {
			srv := newSrv()
			defer srv.Close()

			c := openai.NewClient("k", "m", openai.WithBaseURL(srv.URL), openai.WithSystem("openai"))
			err := do(c)

			var apiErr *model.APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("err 不是 *model.APIError: %T %v", err, err)
			}
			if apiErr.RetryAfter != 3*time.Second {
				t.Errorf("RetryAfter = %v, want 3s——响应头没被读进来，"+
					"重试层会退回按自己猜的指数退避等待", apiErr.RetryAfter)
			}
			// 顺带钉住它确实经接口被重试层取得到（两者之间还隔着一个方法）
			if got := compose_v2.RetryAfterFromError(err); got != 3*time.Second {
				t.Errorf("RetryAfterFromError = %v, want 3s——字段填了但重试层取不到", got)
			}
		})
	}
}

// TestClient_StatusIsWiredToRetryClassification 钉住**客户端真的把状态码接到了重试分类上**。
//
// **为什么这条必须独立存在**：分类逻辑（compose_v2.IsRetryable / IsRateLimited）有自己
// 的单测，但那些用的是测试桩。把 `model.APIError.HTTPStatus()` 整个改成恒返 0，**全仓
// 测试照样全绿**（实测），而线上表现是 400/401 这类重发必然同样失败的请求重新被重试到
// 次数用满、429 重新跟 5xx 抢同一份预算——两者都 err == nil。
//
// 这跟同文件那条 Retry-After 的接线测试是同一个缺口形状：纯函数单测 + 手搓 error 的
// 消费侧单测，中间那一环没人管。
func TestClient_StatusIsWiredToRetryClassification(t *testing.T) {
	cases := []struct {
		code        int
		wantRetry   bool
		wantLimited bool
		why         string
	}{
		{http.StatusBadRequest, false, false, "参数错，重发必然同样失败"},
		{http.StatusUnauthorized, false, false, "鉴权失败，重发必然同样失败"},
		{http.StatusNotFound, false, false, "模型不存在，重发必然同样失败"},
		{http.StatusTooManyRequests, true, true, "限流，等一会儿就好——且要走独立的限流预算"},
		{http.StatusServiceUnavailable, true, false, "provider 故障，值得重试"},
	}
	for _, c := range cases {
		t.Run(http.StatusText(c.code), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(c.code)
				_, _ = w.Write([]byte(`{"error":{"message":"boom"}}`))
			}))
			defer srv.Close()

			cl := openai.NewClient("k", "m", openai.WithBaseURL(srv.URL), openai.WithSystem("openai"))
			_, err := cl.Generate(context.Background(), []*schema.Message{schema.UserMessage("hi")})
			if err == nil {
				t.Fatal("非 200 应返错")
			}
			if got := compose_v2.IsRetryable(err); got != c.wantRetry {
				t.Errorf("IsRetryable = %v, want %v（%s）——状态码没接到分类上", got, c.wantRetry, c.why)
			}
			if got := compose_v2.IsRateLimited(err); got != c.wantLimited {
				t.Errorf("IsRateLimited = %v, want %v（%s）", got, c.wantLimited, c.why)
			}
		})
	}
}
