package compose_v2_test

// 每个框架 sentinel 在 `IsRetryable` 下的判定，逐条钉死。
//
// # 为什么要整表钉而不是只钉出问题的那两条
//
// 这类缺陷的形状是"新加了一个 sentinel，但没人想起来它跟重试的关系"——`soft_cancel.go`
// 定义了信号、`retry.go` 不认识它，两个文件各自都对，错在它们之间。整表钉住之后，
// 下次加 sentinel 时这张表会逼着作者回答"它该不该重试"。
//
// 实测过的两条（修复前 `SimpleRetry(3)` 下子任务被调 4 次）：
//
//   - `ErrSoftCanceled`：业务在另一 goroutine 说了"停"，而且已经存档了。重跑会从断点
//     Resume 再推进一个超步——每次 retry 多跑一步真实工作（LLM 调用、工具副作用），
//     然后又撞上同一个已触发的信号。用户点停，框架反而多跑几步
//   - `ErrRunAborted`：终局失败，框架**已经把 cp 子树删了**。重跑不是"再试一次"是
//     "从头再来一遍"：cp 没了所以 fresh run，副作用全部重放，然后撞同一个确定性的
//     坏数据再 abort

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crew-go/compose_v2"
)

func TestIsRetryable_AllFrameworkSentinels(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		retryable bool
		why       string
	}{
		{
			"InterruptError", &compose_v2.InterruptError{
				Reason: "r", Query: &compose_v2.AskQuery{InterruptID: "i", Question: "q"},
			}, false, "在等人答，重跑白跑",
		},
		{"ctx.Canceled", context.Canceled, false, "调用方要停"},
		{"ctx.DeadlineExceeded", context.DeadlineExceeded, false, "超时了，重跑还是超"},
		{
			"ErrSoftCanceled", compose_v2.ErrSoftCanceled, false,
			"业务明确说停且已存档；重跑会从断点再推进一个超步，多做一步真实工作",
		},
		{
			"ErrCheckpointStale", compose_v2.ErrCheckpointStale, false,
			"另一个执行者已推进会话，重跑不会让账记上、只会重造副作用",
		},
		{
			"ErrRunAborted", compose_v2.ErrRunAborted, false,
			"终局失败且 cp 子树已删；重跑=fresh run 从头再来，副作用全部重放",
		},

		// 下面这些**可以**重试——它们是"当下没查到"，不是"确定性的否定"
		{
			"ErrNoPendingCheckpoint", compose_v2.ErrNoPendingCheckpoint, true,
			"Durability=Async 下后台 worker 可能还没写完，重试确实可能拿到",
		},
		{
			"ErrNoSubgraphCheckpoint", compose_v2.ErrNoSubgraphCheckpoint, true,
			"同上",
		},
		{"普通业务错", errors.New("model api 503"), true, "临时故障正是 retry 的用武之地"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 裸的和 wrap 过的都要判得出（生产里错误总是被 %w 包了好几层）
			for _, err := range []error{tc.err, fmt.Errorf("outer: %w", tc.err)} {
				if got := compose_v2.IsRetryable(err); got != tc.retryable {
					t.Errorf("IsRetryable(%v) = %v, want %v\n  理由: %s",
						err, got, tc.retryable, tc.why)
				}
			}
		})
	}
}

// TestRunWithRetry_DoesNotRerunOnStopSignals：谓词判对了还不够，得看 `RunWithRetry`
// 真的没重跑——这两条的危害全在"多跑的那几次"上。
func TestRunWithRetry_DoesNotRerunOnStopSignals(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		harm string
	}{
		{"ErrSoftCanceled", compose_v2.ErrSoftCanceled,
			"每次重跑从断点 Resume 再推进一个超步——用户点了停，框架反而多做几步真实工作"},
		{"ErrRunAborted", compose_v2.ErrRunAborted,
			"cp 子树已删，重跑是 fresh run 从头来——副作用全部重放一遍"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			_, err := compose_v2.RunWithRetry(context.Background(), compose_v2.SimpleRetry(3),
				func(context.Context) (int, error) {
					calls++
					return 0, fmt.Errorf("wrapped: %w", tc.err)
				})
			if calls != 1 {
				t.Errorf("子任务被调 %d 次，应只调 1 次\n  %s", calls, tc.harm)
			}
			if !errors.Is(err, tc.err) {
				t.Errorf("原始 sentinel 必须能穿透 retry 层被业务判出: %v", err)
			}
		})
	}

	// 对照：普通故障仍要照常重试——上面的收紧不能波及 retry 的本职
	var calls int
	_, _ = compose_v2.RunWithRetry(context.Background(), compose_v2.SimpleRetry(3),
		func(context.Context) (int, error) {
			calls++
			return 0, errors.New("model api 503")
		})
	if calls != 4 {
		t.Errorf("普通故障应重试满 3 次（共 4 次调用），实际 %d 次——收紧波及了正常路径", calls)
	}
}
