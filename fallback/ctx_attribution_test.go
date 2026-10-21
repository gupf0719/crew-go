package fallback_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/fallback"
	"crew-go/model"
)

// TestFailover_BackendInternalTimeoutStillSwitches 钉住**「错误链上有 ctx 错」不等于
// 「我的 ctx 死了」**——后者才是「别再试了」的理由。
//
// 场景是标准做法：给模型客户端配 `http.Client{Timeout}`。provider 变慢时它产出的错
// `errors.Is(err, context.DeadlineExceeded)` **为真**，而 **caller 的 ctx 全程健康**。
// 把它判成控制流信号 ⟹ 不重试、不切候选 ⟹ **fallback 在它最该发挥作用的那个场景永远
// 不工作**。
//
// ⚠️ 最阴的一档：`net.Dialer` 超时的错误串是 `i/o timeout`，字面上跟 context 无关，
// 但 `errors.Is(err, context.DeadlineExceeded)` 照样是 true——标准库 net 包给它的超时错
// 实现了 Is 方法、显式声明与 DeadlineExceeded 相等。所以连「grep 错误串里有没有 context」
// 都救不了，只能问 ctx。
func TestFailover_BackendInternalTimeoutStillSwitches(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer srv.Close()

	callerCtx := context.Background() // caller 的 ctx 全程健康
	cli := &http.Client{Timeout: 30 * time.Millisecond}
	req, _ := http.NewRequestWithContext(callerCtx, "GET", srv.URL, nil)
	_, rawErr := cli.Do(req)
	if rawErr == nil {
		t.Fatal("fixture 没搭对：请求该超时")
	}
	// 复刻 model/openai/client.go 的包装形状
	apiErr := &model.APIError{Err: fmt.Errorf("request failed: %w", rawErr)}

	if !errors.Is(apiErr, context.DeadlineExceeded) {
		t.Fatalf("fixture 没搭对：这个错该匹配 DeadlineExceeded，实际 %v", apiErr)
	}
	if callerCtx.Err() != nil {
		t.Fatalf("fixture 没搭对：caller ctx 该是活的")
	}

	if compose_v2.IsRetryFutile(callerCtx, apiErr) {
		t.Errorf("caller ctx 活着、只是 provider 超时，却被判成「再做一次没用」——" +
			"重跑一次很可能就好了，而框架认定「重试立即再失败」")
	}
	if !(fallback.DefaultFailoverPolicy{}).ShouldFailover(callerCtx, apiErr) {
		t.Errorf("caller ctx 活着、只是 provider 超时，却不切候选——" +
			"这正是 fallback 存在的理由，而它在这个场景永不工作")
	}

	// 对照：caller 自己的 ctx 真死了 ⟹ 两个都该说「别试了」
	dead, cancel := context.WithCancel(context.Background())
	cancel()
	if !compose_v2.IsRetryFutile(dead, apiErr) {
		t.Errorf("caller ctx 已死却仍判「再做一次可能有用」——重试必然立即再失败")
	}
	if (fallback.DefaultFailoverPolicy{}).ShouldFailover(dead, apiErr) {
		t.Errorf("caller ctx 已死却仍切候选——换个 provider 也一样死")
	}
}
