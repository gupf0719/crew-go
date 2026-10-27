package compose_v2_test

// 一个失败只被重试一层（compose_v2.RetriesExhaustedError）：下层把预算用完之后交出的错误带标记，上层的重试不再重试它，
// 业务谓词放得再宽也一样。不这样时各层次数相乘——两层各重试 2 次就是 9 次调用。

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
)

var errFlaky = errors.New("upstream 503")

func alwaysRetry(error) bool { return true }

// 两层都配重试、都放宽谓词：最里层用完 3 次之后，外层一次也不再重做。
func TestRetriesExhausted_NestedRetriesDoNotMultiply(t *testing.T) {
	calls := 0
	inner := func(ctx context.Context) (int, error) {
		return compose_v2.RunWithRetry(ctx, compose_v2.RetryConfig{MaxRetries: 2, ShouldRetry: alwaysRetry},
			func(context.Context) (int, error) { calls++; return 0, errFlaky })
	}
	_, err := compose_v2.RunWithRetry(context.Background(), compose_v2.RetryConfig{MaxRetries: 2, ShouldRetry: alwaysRetry}, inner)
	if calls != 3 {
		t.Fatalf("最里层重试用完之后外层不该再重做：期望 3 次调用，实际 %d 次", calls)
	}
	if !errors.Is(err, errFlaky) || err.Error() != errFlaky.Error() {
		t.Fatalf("错误文本与 errors.Is 应保持原样，实际 %q", err)
	}
	var re *compose_v2.RetriesExhaustedError
	if !errors.As(err, &re) || re.Attempts != 3 {
		t.Fatalf("应带标记且记下这一层尝试了 3 次，实际 %#v", re)
	}
	if compose_v2.IsRetryable(err) {
		t.Fatal("默认谓词也该认这个标记")
	}
}

// 第一次就被判成不可重试的错不带标记：这一层没重试过，上层照自己的判据处理。
func TestRetriesExhausted_NotMarkedWhenNotRetried(t *testing.T) {
	calls := 0
	inner := func(ctx context.Context) (int, error) {
		return compose_v2.RunWithRetry(ctx, compose_v2.RetryConfig{MaxRetries: 2, ShouldRetry: func(error) bool { return false }},
			func(context.Context) (int, error) { calls++; return 0, errFlaky })
	}
	_, err := compose_v2.RunWithRetry(context.Background(), compose_v2.RetryConfig{MaxRetries: 2, ShouldRetry: alwaysRetry}, inner)
	if calls != 3 {
		t.Fatalf("里层没重试过，外层照常重试：期望 3 次调用，实际 %d 次", calls)
	}
	var re *compose_v2.RetriesExhaustedError
	if errors.As(err, &re) && re.Attempts != 3 {
		t.Fatalf("标记只能由真的重试过的那一层（外层，3 次）打，实际 %d", re.Attempts)
	}
}

// 不重试的配置（MaxRetries 0）不打标记。
func TestRetriesExhausted_ZeroRetriesUnmarked(t *testing.T) {
	_, err := compose_v2.RunWithRetry(context.Background(), compose_v2.RetryConfig{},
		func(context.Context) (int, error) { return 0, errFlaky })
	var re *compose_v2.RetriesExhaustedError
	if errors.As(err, &re) {
		t.Fatal("没重试过的错误不该带标记")
	}
	if compose_v2.MarkRetriesExhausted(nil, 3) != nil {
		t.Fatal("nil 原样返回")
	}
}

// 同一超步两个分支都失败，声明在前的是普通错误、另一个下层已经重试用完：交出的错误带「重试用完」标记（文本仍是声明在前
// 那个分支的），外层不再重做——否则用完的那个分支被再重试一轮、次数相乘。曾经带不带标记取决于失败分支谁声明在前。
func TestRetriesExhausted_FanoutCarriesMarkFromAnyBranch(t *testing.T) {
	plain := errors.New("plain failure")
	b := compose_v2.NewBuilder()
	b.AddNode("a", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, plain })
	b.AddNode("b", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, compose_v2.MarkRetriesExhausted(errFlaky, 3)
	})
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddEdge(compose_v2.StartNode, "b")
	b.AddEdge("a", compose_v2.EndNode)
	b.AddEdge("b", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	runErr := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager())
	var re *compose_v2.RetriesExhaustedError
	if !errors.Is(runErr, plain) || !errors.As(runErr, &re) || re.Attempts != 3 {
		t.Fatalf("应交出声明在前那个分支的错误并带上另一个分支的标记，实际 %v（标记 %#v）", runErr, re)
	}
	if compose_v2.IsRetryable(runErr) {
		t.Fatal("默认谓词应认这个标记、不再重试")
	}
	// 扇出仲裁的公开入口同一个判据
	if picked := compose_v2.PickFanoutError([]error{plain, compose_v2.MarkRetriesExhausted(errFlaky, 2)}); !errors.As(picked, &re) || !errors.Is(picked, plain) {
		t.Fatalf("PickFanoutError 也应带上标记，实际 %v", picked)
	}
}
