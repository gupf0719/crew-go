package compose_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
)

// TestRetryConfig_PredicateRequiredWhenRetrying：**配了重试就得说重试什么。**
//
// 重试的对象是整个成员调用——里面可能有扣款、发消息、多次模型调用，重试一次全部
// 重放一次。而框架在这一层看到的只是 `error`，分不出「网络抖了一下」和「参数非法，
// 重跑一万次也一样」——后者在 Go 里的写法就是普通的 fmt.Errorf。
//
// 曾经的默认是「没标特殊哨兵就重试」，实测代价：一次确定性的协议违规被跑了 4 次、
// 副作用重放 4 次，而 err 全程只是一句普通错误。
func TestRetryConfig_PredicateRequiredWhenRetrying(t *testing.T) {
	t.Run("配了重试却没给谓词 → 装配期就拦下", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("应在装配期 panic：配重试却不说重试什么，等于让框架去猜一个它答不上的问题")
			}
		}()
		compose_v2.ValidateRetryConfig(compose_v2.RetryConfig{MaxRetries: 3}, "WithXxxRetryConfig")
	})

	t.Run("不重试的配置不受影响", func(t *testing.T) {
		compose_v2.ValidateRetryConfig(compose_v2.RetryConfig{}, "WithXxxRetryConfig")
		compose_v2.ValidateRetryConfig(compose_v2.RetryConfig{MaxRetries: 0}, "WithXxxRetryConfig")
	})

	t.Run("便捷构造自带谓词（显式接受框架默认）", func(t *testing.T) {
		compose_v2.ValidateRetryConfig(compose_v2.SimpleRetry(3), "WithXxxRetryConfig")
		compose_v2.ValidateRetryConfig(compose_v2.ExponentialBackoff(3, 0, 0), "WithXxxRetryConfig")
	})
}

// TestRetryConfig_PredicateDecidesRetry：谓词说了算，且控制流信号恒不受它影响。
func TestRetryConfig_PredicateDecidesRetry(t *testing.T) {
	deterministic := errors.New("参数非法，重跑也一样")

	t.Run("谓词说不重试 → 只跑 1 次", func(t *testing.T) {
		calls := 0
		cfg := compose_v2.RetryConfig{MaxRetries: 3, ShouldRetry: func(error) bool { return false }}
		_, err := compose_v2.RunWithRetry(context.Background(), cfg, func(context.Context) (int, error) {
			calls++
			return 0, deterministic
		})
		if calls != 1 {
			t.Errorf("谓词否决时不该重试，实际跑了 %d 次", calls)
		}
		if !errors.Is(err, deterministic) {
			t.Errorf("原错误应原样返回：%v", err)
		}
	})

	t.Run("谓词说重试 → 跑满", func(t *testing.T) {
		calls := 0
		cfg := compose_v2.RetryConfig{MaxRetries: 2, ShouldRetry: func(error) bool { return true }}
		_, _ = compose_v2.RunWithRetry(context.Background(), cfg, func(context.Context) (int, error) {
			calls++
			return 0, deterministic
		})
		if calls != 3 {
			t.Errorf("谓词放行时应跑满 MaxRetries+1=3 次，实际 %d 次", calls)
		}
	})

	t.Run("谓词放行也拦不住控制流信号", func(t *testing.T) {
		calls := 0
		cfg := compose_v2.RetryConfig{MaxRetries: 3, ShouldRetry: func(error) bool { return true }}
		_, _ = compose_v2.RunWithRetry(context.Background(), cfg, func(context.Context) (int, error) {
			calls++
			return 0, &compose_v2.InterruptError{
				Reason: "问一句",
				Query:  &compose_v2.AskQuery{InterruptID: "q1", Question: "确认吗"},
			}
		})
		if calls != 1 {
			t.Fatalf("中断不是「失败了再试试」，它在等答案；业务谓词写成恒真也不该吞掉它，实际跑了 %d 次", calls)
		}
	})
}
