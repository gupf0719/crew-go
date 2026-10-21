package fallback_test

import (
	"context"
	"testing"

	"crew-go/compose_v2"
	"crew-go/fallback"
	"crew-go/model"
)

// TestDefaultFailoverPolicy_QuotaExhausted 验额度/配额耗尽类 4xx（dashscope 403 / 余额不足）
// 触发切换，而普通 4xx（400 参数 / 401 鉴权 / 403 权限）不切——同 StatusCode 靠 body 关键词区分。
func TestDefaultFailoverPolicy_QuotaExhausted(t *testing.T) {
	p := fallback.DefaultFailoverPolicy{}
	cases := []struct {
		name string
		err  *model.APIError
		want bool
	}{
		// 关键区分：同为 403，额度耗尽切、权限拒绝不切
		{"dashscope 403 额度耗尽→切", &model.APIError{StatusCode: 403, Body: `{"error":{"message":"The free quota has been exhausted","code":"AllocationQuota.FreeTierOnly"}}`}, true},
		{"403 权限拒绝→不切", &model.APIError{StatusCode: 403, Body: `{"error":{"message":"permission denied"}}`}, false},
		{"400 参数错→不切", &model.APIError{StatusCode: 400, Body: `{"error":"invalid parameter foo"}`}, false},
		{"400 含 quota_limit_override 不误切", &model.APIError{StatusCode: 400, Body: `{"error":{"message":"cannot set both max_tokens and quota_limit_override in the same request"}}`}, false},
		{"401 鉴权错→不切", &model.APIError{StatusCode: 401, Body: `{"error":"invalid api key"}`}, false},
		{"402 余额不足→切", &model.APIError{StatusCode: 402, Body: "insufficient balance"}, true},
		{"429 限流→切", &model.APIError{StatusCode: 429, Body: "rate limit exceeded"}, true},
		{"500 provider 故障→切", &model.APIError{StatusCode: 500, Body: "internal error"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := p.ShouldFailover(context.Background(), c.err); got != c.want {
				t.Errorf("ShouldFailover = %v, want %v (body=%q)", got, c.want, c.err.Body)
			}
		})
	}
}

// TestDefaultFailoverPolicy_ControlFlow 验控制流信号不切（回归——增强额度检查不应影响这些）。
func TestDefaultFailoverPolicy_ControlFlow(t *testing.T) {
	p := fallback.DefaultFailoverPolicy{}
	// InterruptError 豁免锁定导出分类器契约——DefaultFailoverPolicy 内保留的控制流
	// 冗余分支（框架 guardedPolicy 已兜底）必须让直接调用者拿到"不切"。删该分支时
	// 本断言 fail（否则只有 fallback 集成测试因 guard 兜底而无法暴露契约破坏）。
	if p.ShouldFailover(context.Background(), &compose_v2.InterruptError{Reason: "hitl"}) {
		t.Error("InterruptError 不应切")
	}
	// ⚠️ **ctx 那两档按「谁的 ctx」分叉，不按错误长什么样分叉**（2026-08-26 改）。
	//
	// 原来这里写的是「`context.Canceled` 恒不切」。那个期望是错的，而且错得有代价：
	// `http.Client{Timeout}` / `net.Dialer{Timeout}`（**标准配法**）产出的错同样匹配
	// `DeadlineExceeded`，于是 **provider 变慢时 fallback 永不切换**——正是它存在的
	// 理由那个场景。判据换成 `ctx.Err()` 之后，两个方向都要钉住：
	dead, cancel := context.WithCancel(context.Background())
	cancel()
	for _, e := range []error{context.Canceled, context.DeadlineExceeded} {
		if p.ShouldFailover(dead, e) {
			t.Errorf("caller ctx 已死时不该切（换个候选也一样死）：%v", e)
		}
		if !p.ShouldFailover(context.Background(), e) {
			t.Errorf("caller ctx 活着时该切——这个 ctx 错来自别人（后端内部的子请求），"+
				"换个候选很可能就好了：%v", e)
		}
	}
	if !p.ShouldFailover(context.Background(), &model.APIError{StatusCode: 0, Body: ""}) {
		t.Error("StatusCode=0（网络/超时）应切")
	}
}
