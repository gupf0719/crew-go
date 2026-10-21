package compose_v2_test

import (
	"errors"
	"fmt"
	"testing"

	"crew-go/compose_v2"
)

// mkInterrupt 造一个带稳定 InterruptID 的 InterruptError（模拟 ask_user）。
func mkInterrupt(id string) *compose_v2.InterruptError {
	return &compose_v2.InterruptError{
		Reason: "ask_user",
		Query:  &compose_v2.AskQuery{InterruptID: id, Question: "q?"},
	}
}

// TestIsInterrupt_ErrorsJoin_MultiErrorTree 钉死 graph_loop.go:695 的场景：
// fan-out 分支抛 InterruptError(res.FirstErr) + 同超步成功分支 ApplyAll 失败(applyErr)
// → return errors.Join(res.FirstErr, applyErr)。修复前 asInterrupt 只走单链 Unwrap()
// error 检测不到 errors.Join 的 Unwrap() []error → IsInterrupt=false → HITL 中断被误判
// 成致命错 Resume 闭环静默失效。修复后对齐 stdlib errors.As 认多错误树。
//
// **sanity**：revert asInterrupt 的 []error 分支后 case "join_first"/"join_second"/
// "nested_join" 会 FAIL（IsInterrupt 返 false），精确指向 bug。
func TestIsInterrupt_ErrorsJoin_MultiErrorTree(t *testing.T) {
	ieA := mkInterrupt("id-A")
	ieB := mkInterrupt("id-B")
	plain := errors.New("channel \"v\" not registered") // 模拟 ApplyAll 失败

	tests := []struct {
		name     string
		err      error
		wantOK   bool
		wantSame *compose_v2.InterruptError // 期望取回的 InterruptError（first-win 顺序）
	}{
		{
			// 现有单链行为——必须不破坏（fmt.Errorf %w，runNodeWithHooks:1265 形态）
			name:     "single_chain_wrap",
			err:      fmt.Errorf("node %q: %w", "worker", ieA),
			wantOK:   true,
			wantSame: ieA,
		},
		{
			// 核心修复：errors.Join 第一个成员含 InterruptError（graph_loop.go:695 真实形态）
			name:     "join_first",
			err:      errors.Join(fmt.Errorf("node %q: %w", "w", ieA), fmt.Errorf("apply writes: %w", plain)),
			wantOK:   true,
			wantSame: ieA,
		},
		{
			// InterruptError 在 errors.Join 第二个成员——DFS 也应命中
			name:     "join_second",
			err:      errors.Join(fmt.Errorf("apply writes: %w", plain), fmt.Errorf("node: %w", ieB)),
			wantOK:   true,
			wantSame: ieB,
		},
		{
			// first-win：两个 InterruptError，返回 DFS 从左到右第一个（ieA）
			name:     "join_first_win",
			err:      errors.Join(fmt.Errorf("%w", ieA), fmt.Errorf("%w", ieB)),
			wantOK:   true,
			wantSame: ieA,
		},
		{
			// 嵌套：单链 wrap 一个 errors.Join——混合链要穿透
			name:     "nested_join",
			err:      fmt.Errorf("outer: %w", errors.Join(plain, fmt.Errorf("%w", ieA))),
			wantOK:   true,
			wantSame: ieA,
		},
		{
			// 不误报：errors.Join 全是非中断错 → false
			name:   "join_no_interrupt",
			err:    errors.Join(plain, errors.New("other")),
			wantOK: false,
		},
		{
			// 不误报：普通单链错 → false
			name:   "plain_single",
			err:    fmt.Errorf("wrap: %w", plain),
			wantOK: false,
		},
		{
			name:   "nil",
			err:    nil,
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := compose_v2.IsInterrupt(tc.err)
			if ok != tc.wantOK {
				t.Fatalf("IsInterrupt ok=%v, want %v (err=%v)", ok, tc.wantOK, tc.err)
			}
			if tc.wantOK && got != tc.wantSame {
				t.Fatalf("IsInterrupt 取回的 InterruptError 身份不符 (first-win 顺序错)：got id=%v",
					got.Query.InterruptID)
			}
			// 对齐 stdlib errors.As：框架 IsInterrupt 结果必须与 stdlib 一致
			var viaStdlib *compose_v2.InterruptError
			if okStdlib := errors.As(tc.err, &viaStdlib); okStdlib != tc.wantOK {
				t.Fatalf("与 stdlib errors.As 不一致：IsInterrupt=%v stdlib errors.As=%v", tc.wantOK, okStdlib)
			}
		})
	}
}
