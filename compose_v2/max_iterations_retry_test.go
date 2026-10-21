package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
)

// TestMaxIterations_NotRetried 钉住**轮次耗尽不该被重试**。
//
// `maxVisit = maxRounds + 1`（见 agent_v2/basic_builder.go），所以**一次触顶已经是烧满
// 整个 ReAct 预算的 LLM 调用**。被重试 N 次 = 把满预算烧 N+1 遍、工具副作用全部重放。
//
// ⚠️ 这条不依赖「maxRounds 是保险丝还是预算」这个产品判断——**两种读法都指向不该重试**：
//   - 当**保险丝**用：触顶是异常，重跑必然再熔断
//   - 当**预算**用：用**同样的预算**重跑还是烧满。只有「重试时给更大预算」才有意义，
//     而重试机制本身给不了这个（`RunWithRetry` 每次传的是同一个 cfg）
//
// 断言选在**调用次数**上而不是「最终返不返错」——重试之后最终照样返错，那个维度不承重。
func TestMaxIterations_NotRetried(t *testing.T) {
	calls := 0
	_, err := compose_v2.RunWithRetry(context.Background(), compose_v2.SimpleRetry(3),
		func(ctx context.Context) (int, error) {
			calls++
			return 0, &compose_v2.MaxIterationsError{Iterations: 21}
		})
	if calls != 1 {
		t.Errorf("轮次耗尽被跑了 %d 次，该只跑 1 次——每一次都是烧满整个 ReAct 预算"+
			"（maxVisit = maxRounds+1）且工具副作用全部重放", calls)
	}
	var mie *compose_v2.MaxIterationsError
	if !errors.As(err, &mie) {
		t.Errorf("错误该原样返回（typed，调用方用 errors.As 判），实际 %v", err)
	}

	// **两个抛出点都要覆盖**——我第一次只想到 ReAct 深度那个，漏了 fan-out 宽度失控
	// （业务终止条件写错导致的指数爆炸，那个更该一次就停）。生产路径上是 wrap 过的。
	calls = 0
	_, _ = compose_v2.RunWithRetry(context.Background(), compose_v2.SimpleRetry(3),
		func(ctx context.Context) (int, error) {
			calls++
			return 0, fmt.Errorf("superstep frontier width %d exceeds max: %w", 999,
				&compose_v2.MaxIterationsError{Iterations: 999})
		})
	if calls != 1 {
		t.Errorf("fan-out 宽度失控被跑了 %d 次，该只跑 1 次（wrap 过也要认）", calls)
	}
}

// TestBusinessErrors_StillRetried 钉住**框架只排除自己抛的那一类，不替业务猜**。
//
// 业务错（参数非法 / 鉴权失败）的重放代价框架不知道——那正是 `RetryConfig.ShouldRetry`
// 被设成**必填**、且它的 godoc 明写「那个默认确实偏宽」的原因。
//
// ⚠️ 这条断言的是**现状而非理想**：`SimpleRetry` 的默认会把这些跑满次数。写下来是为了
// 让「框架该管到哪」这条线可见——想收窄业务错就自己写谓词。删掉这条用例之前先想清楚
// 是不是真的要让框架去猜业务错的重放代价。
func TestBusinessErrors_StillRetried(t *testing.T) {
	for _, e := range []error{
		errors.New("invalid argument: amount must be positive"),
		errors.New("401 unauthorized: bad api key"),
	} {
		calls := 0
		_, _ = compose_v2.RunWithRetry(context.Background(), compose_v2.SimpleRetry(2),
			func(ctx context.Context) (int, error) { calls++; return 0, e })
		if calls != 3 {
			t.Errorf("业务错 %v 被跑 %d 次，期望 3（= 保守默认全跑满）——"+
				"若这里变了，说明框架开始替业务猜重放代价了，需要重新论证", e, calls)
		}
	}
}

// TestNonRetryable_CrossesPackageBoundary 钉住**接口跨得过分层，而哨兵清单跨不过**。
//
// `agent_v2.StructuredOutputError` 是 agent_v2 的类型，而判据在 compose_v2——依赖方向上
// compose_v2 **import 不了** agent_v2。用哨兵清单表达就只能让它去 wrap `ErrRunAborted`，
// 而那捆绑了「清 cp 子树」的语义，对不该清档的错是有害的。
//
// 这条测试住在 compose_v2 的黑盒测试包里（能 import agent_v2），所以它同时验证了
// **方向是对的**：下游包实现上游包的接口，编译得过就是证明。
func TestNonRetryable_CrossesPackageBoundary(t *testing.T) {
	calls := 0
	_, _ = compose_v2.RunWithRetry(context.Background(), compose_v2.SimpleRetry(3),
		func(ctx context.Context) (int, error) {
			calls++
			return 0, &agent_v2.StructuredOutputError{SchemaName: "x", Attempts: 3}
		})
	if calls != 1 {
		t.Errorf("跨包的 NonRetryable 被跑了 %d 次，该只跑 1 次——"+
			"抛出时这次调用已经烧完 maxRetries+1 轮 LLM，外层再重试等于乘以 MaxRetries+1", calls)
	}
}
