package compose_v2

// 逐个采纳（一批 tool_call 的工具钩子就是这样）时，执行状态的查找源不随采纳次数变深；但压平只跳过「上一次采纳
// + 这次的标记」，两次采纳之间框架自己推的执行状态照样生效。

import (
	"context"
	"testing"
)

type flattenIdxKey int

// TestCallHook_ExecLookupFlatAcrossAdoptions：64 次逐个采纳之后，执行状态（这里取 ns）直接从最初那个 ctx 查，不穿过
// 64 层；每个钩子推的业务值都还在（业务值本来就要穿过各层）。曾经每次采纳都在上一次的结果外面再包一层，查一次
// 位置要穿过 64 层（实测 10ns → 1.9µs）。
func TestCallHook_ExecLookupFlatAcrossAdoptions(t *testing.T) {
	base := WithGraphNS(context.Background(), "ns")
	ctx := base
	for i := 0; i < 64; i++ {
		ctx, _ = CallHook(ctx, func(c context.Context) (context.Context, error) {
			return context.WithValue(WithGraphNS(c, "hook-pushed"), flattenIdxKey(i), i), nil
		})
	}
	a, ok := ctx.(adoptedCtx)
	if !ok {
		t.Fatalf("采纳之后应是 adoptedCtx，实际 %T", ctx)
	}
	if a.exec != base {
		t.Errorf("执行状态的查找源应是最初那个 ctx，实际 %T（每次采纳又包了一层）", a.exec)
	}
	if ns := GetGraphNS(ctx); ns != "ns" {
		t.Errorf("位置应取框架的，实际 %q", ns)
	}
	for _, i := range []int{0, 31, 63} {
		if v, _ := ctx.Value(flattenIdxKey(i)).(int); v != i {
			t.Errorf("第 %d 个钩子推的业务值应还在，实际 %v", i, ctx.Value(flattenIdxKey(i)))
		}
	}
}

// TestCallHook_FlattenKeepsFrameworkPushesBetweenAdoptions：两次采纳之间框架自己推了执行状态（这里换了 ns），之后
// 那次采纳照样以它为准——压平只跳过「上一次采纳 + 标记」，不跳过中间推的东西。
func TestCallHook_FlattenKeepsFrameworkPushesBetweenAdoptions(t *testing.T) {
	derived := func(c context.Context) (context.Context, error) {
		return context.WithValue(c, flattenIdxKey(0), 0), nil
	}
	first, _ := CallHook(WithGraphNS(context.Background(), "outer"), derived)
	second, _ := CallHook(WithGraphNS(first, "inner"), derived)
	if ns := GetGraphNS(second); ns != "inner" {
		t.Errorf("两次采纳之间框架推的 ns 应生效，实际 %q", ns)
	}
	if ns := GetGraphNS(first); ns != "outer" {
		t.Errorf("前一次采纳的结果不该被影响，实际 %q", ns)
	}
}
