package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crew-go/compose_v2"
)

func ie(id string) error {
	return &compose_v2.InterruptError{
		Query: &compose_v2.AskQuery{InterruptID: id, Question: "q-" + id},
	}
}

// TestPickFanoutError_InterruptBeatsBusinessError 钉住判据本体：中断 > 第一个业务错。
//
// 「先到先得」在这儿是错的：调用方拿到业务错就按「失败了」处理、不会去 Resume，而挂起
// 的问题永远没人答。理由全文见 frontierResult.exitErr 的 godoc。
func TestPickFanoutError_InterruptBeatsBusinessError(t *testing.T) {
	biz := errors.New("db connection refused")
	got := compose_v2.PickFanoutError([]error{biz, ie("a")})
	if _, ok := compose_v2.IsInterrupt(got); !ok {
		t.Fatalf("先到的业务错盖掉了中断：%v", got)
	}
}

// TestPickFanoutError_SingleInterruptKeepsSecondSentinel 钉住「1 个中断返**原始 error**」。
//
// 不能返 MergeInterrupts 抠出来的裸 ie——链上并列挂的第二 sentinel 会被剥掉。这里用
// ErrCheckpointStale 当探针：下游有三处专门判它。
func TestPickFanoutError_SingleInterruptKeepsSecondSentinel(t *testing.T) {
	raw := errors.Join(ie("a"), compose_v2.ErrCheckpointStale)
	got := compose_v2.PickFanoutError([]error{raw})
	if _, ok := compose_v2.IsInterrupt(got); !ok {
		t.Fatalf("中断没透出来：%v", got)
	}
	if !errors.Is(got, compose_v2.ErrCheckpointStale) {
		t.Errorf("并列挂的第二 sentinel 被剥掉了——说明返的是抠出来的裸 ie 而不是原始 error")
	}
}

// TestPickFanoutError_MultipleInterruptsCollectAllQueries 钉住「多个中断收全问题清单」。
//
// 少收一条 = 少一次交互，而那条挂起还在存档里等着。
func TestPickFanoutError_MultipleInterruptsCollectAllQueries(t *testing.T) {
	got := compose_v2.PickFanoutError([]error{ie("a"), errors.New("biz"), ie("b")})
	merged, ok := compose_v2.IsInterrupt(got)
	if !ok {
		t.Fatalf("没识别成中断：%v", got)
	}
	qs := merged.AllQueries()
	if len(qs) != 2 {
		t.Fatalf("问题清单没收全：want 2 got %d (%v)", len(qs), qs)
	}
	seen := map[string]bool{}
	for _, q := range qs {
		seen[q.InterruptID] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Errorf("收到的是 %v，两条都该在", seen)
	}
}

// TestPickFanoutError_BusinessErrorsNotJoined 钉住「业务错**不**聚合成多错误树」。
//
// errors.Join 会让 errors.Is 的语义从「这个错是不是 X」静默变成「有没有哪个分支是 X」。
// 实测过的后果：2 个可重试的业务错 + 1 个 wrap 了自己下游 DeadlineExceeded 的分支，
// 聚合后 IsControlFlowSignal 返 true ⟹ IsRetryable 返 false ⟹ 那 2 个再也不重试。
func TestPickFanoutError_BusinessErrorsNotJoined(t *testing.T) {
	retryable := errors.New("temporary: quota")
	poisoned := errors.Join(errors.New("下游超时"), context.DeadlineExceeded)
	got := compose_v2.PickFanoutError([]error{retryable, errors.New("另一个"), poisoned})
	if compose_v2.IsControlFlowSignal(got) {
		t.Fatalf("聚合把某个分支的 DeadlineExceeded 带进了返回值：%v——"+
			"调用方会把一个可重试的业务错当成控制流信号，不再重试", got)
	}
	if !errors.Is(got, retryable) {
		t.Errorf("应返第一个业务错，实际 %v", got)
	}
}

// TestPickFanoutError_AllNil 钉住全成功时返 nil（nil 元素要跳过，不能被当成"第一个错"）。
func TestPickFanoutError_AllNil(t *testing.T) {
	if got := compose_v2.PickFanoutError([]error{nil, nil, nil}); got != nil {
		t.Errorf("全成功应返 nil，实际 %v", got)
	}
	if got := compose_v2.PickFanoutError(nil); got != nil {
		t.Errorf("空输入应返 nil，实际 %v", got)
	}
}

// TestPickFanoutError_AbortDominatesInterrupt 钉住「终局失败严格支配中断」。
//
// 这一档曾经外包给调用方（godoc 写「自己在调用之前先判」）。实测那是错的，而且**框架自己
// 的第一个调用方就没照做**：返中断 ⟹ 引擎落 Pending 档 ⟹ **人被问了、人答了** ⟹ Resume
// 时 abort 赢 ⟹ 出口 purge 把刚给的答案连同兄弟分支的工作一起清掉。一次白问 + 一次销毁，
// 换来的信息量为零。
//
// 顺序两向都测：abort 在中断**之后**出现时也必须赢——「先到先得」在这一档同样是错的。
func TestPickFanoutError_AbortDominatesInterrupt(t *testing.T) {
	abort := fmt.Errorf("子调用违反协议: %w", compose_v2.ErrRunAborted)

	for name, errs := range map[string][]error{
		"abort 在前": {abort, ie("a")},
		"abort 在后": {ie("a"), errors.New("biz"), abort},
		"多中断也让位":   {ie("a"), ie("b"), abort},
	} {
		got := compose_v2.PickFanoutError(errs)
		if !errors.Is(got, compose_v2.ErrRunAborted) {
			t.Errorf("%s：终局失败没赢，返的是 %v——人会被问一个注定答不成的问题", name, got)
		}
		if _, isInt := compose_v2.IsInterrupt(got); isInt {
			t.Errorf("%s：返回值仍被识别成中断 ⟹ 调用方会去落 Pending 档等回答", name)
		}
	}
}
