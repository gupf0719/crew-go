package compose_v2_test

import (
	"errors"
	"fmt"
	"testing"

	"crew-go/compose_v2"
)

// multiErr 自造 Unwrap() []error 类型——构造 errors.Join 无法直接构造的病态输入
// （空 []error / 含 nil 成员 / 嵌套多错误树），钉死 asInterrupt 的 []error 分支
// 与 stdlib errors.As 在各边界上行为一致。
type multiErr struct{ errs []error }

func (m *multiErr) Error() string   { return "multi" }
func (m *multiErr) Unwrap() []error { return m.errs }

// TestAsInterrupt_MultiErrorBoundaries_MatchStdlib 校验 asInterrupt 新增的 Unwrap() []error
// 分支在边界输入上与 stdlib errors.As 完全对齐——覆盖 interrupt_join_test.go 未触及的
// 空/nil 成员/深嵌套场景。每个 case 都断言 IsInterrupt == errors.As（不允许分叉）。
func TestAsInterrupt_MultiErrorBoundaries_MatchStdlib(t *testing.T) {
	ie := &compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "x", Question: "?"}}

	// 1000 层单链 wrap 包一个最内层 Join(plain, ie)——验证深嵌套混合链不栈溢出且穿透
	deep := error(errors.Join(errors.New("p"), fmt.Errorf("%w", ie)))
	for i := 0; i < 1000; i++ {
		deep = fmt.Errorf("layer %d: %w", i, deep)
	}

	cases := map[string]error{
		"nil_err":              nil,
		"empty_slice_nil":      &multiErr{errs: nil},
		"empty_slice_len0":     &multiErr{errs: []error{}},
		"all_nil_members":      &multiErr{errs: []error{nil, nil, nil}},
		"nil_members_then_ie":  &multiErr{errs: []error{nil, ie, nil}},
		"nested_multi_no_ie":   &multiErr{errs: []error{&multiErr{errs: []error{errors.New("a")}}, nil}},
		"nested_multi_with_ie": &multiErr{errs: []error{&multiErr{errs: []error{nil, ie}}}},
		"deep_1000_layer_join": deep,
		"join_empty":           errors.Join(),         // 返 nil
		"join_all_nil":         errors.Join(nil, nil), // 返 nil
	}

	for name, err := range cases {
		t.Run(name, func(t *testing.T) {
			_, gotOK := compose_v2.IsInterrupt(err)
			var viaStd *compose_v2.InterruptError
			stdOK := errors.As(err, &viaStd)
			if gotOK != stdOK {
				t.Fatalf("IsInterrupt=%v 与 stdlib errors.As=%v 不对齐 (err=%v)", gotOK, stdOK, err)
			}
		})
	}
}
