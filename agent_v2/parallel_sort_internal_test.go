package agent_v2

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// TestParallelAgent_CompileNilHooksDefaultsToNoOp 验证 ParallelAgentBuilder.Compile
// 在业务**不调** WithAgentHooks 时 (hooks 是 typed nil hook.Hooks interface), 走
// isFrameworkNil 防护退化 NoOpHooks——业务错路径触达 `hooks.OnError` 不 panic。
//
// 白盒（构造时刻意不调 WithAgentHooks，让 b.hooks 保持 zero value；分支故意返回 error
// 触发 workerFn error 路径 line 206 `hooks.OnError` 调用）。
//
// sanity-revert: 删 Compile() 里 isFrameworkNil(hooks) 检查块（保留 `hooks := b.hooks`），
// 分支 error 路径会 nil deref panic——本测试应 fail 报 "nil pointer dereference"。
func TestParallelAgent_CompileNilHooksDefaultsToNoOp(t *testing.T) {
	// 刻意不调 WithAgentHooks——b.hooks 保持 zero value (typed nil hook.Hooks interface)。
	branchErr := errors.New("branch boom")
	g, err := NewParallelAgentBuilder().
		WithName("p_nilhooks").
		AddBranch("a", func(_ context.Context, _ *schema.Message) (*schema.Message, error) {
			// 返业务 error (非 compose_v2.InterruptError / 非 ctx.Err) 触发 workerFn line 206
			// `hooks.OnError` 调用——若 hooks 是 typed nil 此处直接 nil deref panic。
			return nil, branchErr
		}).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	// compose_v2.Run 应返业务 error wrap，不应 panic。
	runErr := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(compose_v2.Write(parAgentOriginalCh, schema.UserMessage("input"))))
	if runErr == nil {
		t.Fatalf("compose_v2.Run 应返 branch error, got nil")
	}
	if !errors.Is(runErr, branchErr) {
		t.Fatalf("compose_v2.Run 应 wrap branchErr, got: %v", runErr)
	}
}

// TestParallelAgent_CompileNilHooksDefaultsToNoOp_MergerPath 补 audit follow-up MED-1
// 覆盖缺口: 验证 finalizeFn merger error 路径 (line 242 `hooks.OnError`) 在业务不调
// WithAgentHooks 时也走 NoOpHooks 退化不 panic——之前 `TestParallelAgent_CompileNilHooksDefaultsToNoOp`
// 只触发 workerFn 路径 (line 207)。
//
// 白盒: WithMerger 注入返 error 的 merger + 分支全成功 → 进 finalizeFn line 242 路径。
//
// sanity-revert: 删 isFrameworkNil 块, 测试 fail 报 "nil pointer dereference"——确保
// finalizeFn 路径独立锁死, 未来 fix 即使被拆分也不漏防。
func TestParallelAgent_CompileNilHooksDefaultsToNoOp_MergerPath(t *testing.T) {
	mergerErr := errors.New("merger boom")
	g, err := NewParallelAgentBuilder().
		WithName("p_nilhooks_merger").
		AddBranch("a", func(_ context.Context, _ *schema.Message) (*schema.Message, error) {
			return schema.AssistantMessage("A"), nil // 分支成功, 让 finalize 跑
		}).
		WithMerger(func(_ context.Context, _ []ParallelBranchResult) (*schema.Message, error) {
			return nil, mergerErr // merger 故意返业务 error 触发 line 242 hooks.OnError
		}).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	runErr := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(compose_v2.Write(parAgentOriginalCh, schema.UserMessage("input"))))
	if runErr == nil {
		t.Fatalf("compose_v2.Run 应返 merger error, got nil")
	}
	if !errors.Is(runErr, mergerErr) {
		t.Fatalf("compose_v2.Run 应 wrap mergerErr, got: %v", runErr)
	}
}

// TestParallelAgent_FinalizeSortsResultsByName 验证 finalize 调 merger 前按分支 Name
// 稳定排序 results——消除"部分失败 Resume 只重跑失败分支"导致的顺序抖动，对所有 merger 生效。
//
// 白盒（用内部 channel parAgentOriginalCh 注入 input）。
//
// sanity-revert：删 finalizeFn 里的 `sort.SliceStable(results, ...by Name)` 行，本测试应
// fail——merger 会拿到 append 顺序（= AddBranch 顺序 c/a/b），而非 Name 序 a/b/c。
func TestParallelAgent_FinalizeSortsResultsByName(t *testing.T) {
	var gotOrder []string
	captureMerger := func(_ context.Context, results []ParallelBranchResult) (*schema.Message, error) {
		for _, r := range results {
			gotOrder = append(gotOrder, r.Name)
		}
		return schema.AssistantMessage("merged"), nil
	}

	// 故意乱序 AddBranch（c, a, b）：append 顺序 = frontier/AddBranch 序 = c,a,b；
	// finalize 按 Name 排序后 merger 应拿到 a,b,c。
	g, err := NewParallelAgentBuilder().
		WithName("p").
		WithAgentHooks(hook.NoOpHooks{}).
		AddBranch("c", func(_ context.Context, _ *schema.Message) (*schema.Message, error) {
			return schema.AssistantMessage("C"), nil
		}).
		AddBranch("a", func(_ context.Context, _ *schema.Message) (*schema.Message, error) {
			return schema.AssistantMessage("A"), nil
		}).
		AddBranch("b", func(_ context.Context, _ *schema.Message) (*schema.Message, error) {
			return schema.AssistantMessage("B"), nil
		}).
		WithMerger(captureMerger).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(compose_v2.Write(parAgentOriginalCh, schema.UserMessage("input")))); err != nil {
		t.Fatalf("compose_v2.Run: %v", err)
	}

	want := []string{"a", "b", "c"}
	if len(gotOrder) != len(want) {
		t.Fatalf("merger 应拿到 3 个 result，实际 %d: %v", len(gotOrder), gotOrder)
	}
	for i := range want {
		if gotOrder[i] != want[i] {
			t.Errorf("merger results 应按 Name 序 %v，实际 %v", want, gotOrder)
			break
		}
	}
}
