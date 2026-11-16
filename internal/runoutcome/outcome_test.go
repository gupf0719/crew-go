package runoutcome_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crew-go/compose_v2"
	"crew-go/internal/runoutcome"
)

func TestClassify_LostLeaseCauseProtectsCheckpoint(t *testing.T) {
	lost, lose := context.WithCancelCause(t.Context())
	lose(fmt.Errorf("renewal failed: %w", compose_v2.ErrTaskLeaseLost))
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	fail := errors.New("business failure")
	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want runoutcome.Kind
	}{
		{"丢失租约与普通失败并存", lost, fail, runoutcome.CheckpointStale},
		{"调用方取消与普通失败并存", canceled, fail, runoutcome.Canceled},
		{"已知的存档身份冲突优先", lost, compose_v2.ErrGraphIDConflict, runoutcome.GraphIDConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if kind, _ := runoutcome.Classify(tc.ctx, tc.err); kind != tc.want {
				t.Fatalf("分类应为 %q，实际 %q", tc.want, kind)
			}
		})
	}
}
