package compose_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
)

// Phase 5.1.a Step 2 测试：Checkpointer 接口 + InMemoryCheckpointer
// （NS-aware 物理分离）。

// ── SaveNS / LoadNS round-trip ──

func TestCheckpointer_SaveLoad_BasicRoundTrip(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	src := &compose_v2.Checkpoint{
		Status:  compose_v2.CPStatusPending,
		GraphID: "g1",
	}
	if err := cp.SaveNS(ctx, "sid_1", "", src); err != nil {
		t.Fatalf("SaveNS: %v", err)
	}

	got, err := cp.LoadNS(ctx, "sid_1", "")
	if err != nil {
		t.Fatalf("LoadNS: %v", err)
	}
	if got == nil {
		t.Fatal("LoadNS returned nil")
	}
	// SaveNS 自动填 ID/NS（覆盖入参 cp）
	if got.ID != "sid_1" {
		t.Fatalf("ID 应自动填 sid_1，实际 %q", got.ID)
	}
	if got.NS != "" {
		t.Fatalf("NS 应是 ''，实际 %q", got.NS)
	}
	if got.GraphID != "g1" {
		t.Fatalf("GraphID 应保留 g1，实际 %q", got.GraphID)
	}
}

// ── 多 ns 嵌套场景 ──

func TestCheckpointer_NestedNS(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// 同 sessionID 下 3 层 ns
	for _, ns := range []string{"", "sub_a", "sub_a/inner"} {
		if err := cp.SaveNS(ctx, "sid", ns, &compose_v2.Checkpoint{Status: compose_v2.CPStatusPending}); err != nil {
			t.Fatalf("SaveNS %q: %v", ns, err)
		}
	}

	// 验证各 ns 独立可 Load
	for _, ns := range []string{"", "sub_a", "sub_a/inner"} {
		got, _ := cp.LoadNS(ctx, "sid", ns)
		if got == nil {
			t.Errorf("LoadNS ns=%q 应非 nil", ns)
		}
		if got != nil && got.NS != ns {
			t.Errorf("ns=%q got.NS=%q", ns, got.NS)
		}
	}

	// 验证 ParentNS 自动填
	innerCp, _ := cp.LoadNS(ctx, "sid", "sub_a/inner")
	if innerCp.ParentNS != "sub_a" {
		t.Fatalf("inner 的 ParentNS 应 = sub_a，实际 %q", innerCp.ParentNS)
	}
}

// ── DeleteSubtree 删自身与经 `/` 往下的后代 ──

func TestCheckpointer_DeleteSubtree(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// 5 个 cp：根 + sub_a + sub_a/inner + sub_b + sub_b/inner
	for _, ns := range []string{"", "sub_a", "sub_a/inner", "sub_b", "sub_b/inner"} {
		_ = cp.SaveNS(ctx, "sid", ns, &compose_v2.Checkpoint{})
	}

	// 删 sub_a 子树（含 sub_a 自身 + sub_a/inner）
	deleted, err := cp.DeleteSubtree(ctx, "sid", "sub_a")
	if err != nil {
		t.Fatalf("DeleteSubtree: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("应删 2 个（sub_a + sub_a/inner），实际 %d", deleted)
	}

	// 剩下应只有根 + sub_b + sub_b/inner
	for _, ns := range []string{"", "sub_b", "sub_b/inner"} {
		exists, _ := cp.ExistsNS(ctx, "sid", ns)
		if !exists {
			t.Errorf("ns=%q 应仍存在", ns)
		}
	}
	for _, ns := range []string{"sub_a", "sub_a/inner"} {
		exists, _ := cp.ExistsNS(ctx, "sid", ns)
		if exists {
			t.Errorf("ns=%q 应已删", ns)
		}
	}
}

// ── ListPending 仅返 Pending 状态 ──

func TestCheckpointer_ListPending(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	_ = cp.SaveNS(ctx, "sid", "", &compose_v2.Checkpoint{Status: compose_v2.CPStatusPending})
	_ = cp.SaveNS(ctx, "sid", "ns1", &compose_v2.Checkpoint{Status: compose_v2.CPStatusUnset})
	_ = cp.SaveNS(ctx, "sid", "ns2", &compose_v2.Checkpoint{Status: compose_v2.CPStatusPending})
	_ = cp.SaveNS(ctx, "sid", "ns3", &compose_v2.Checkpoint{Status: compose_v2.CPStatusUnset})

	pending, err := cp.ListPending(ctx, "sid")
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("应 2 个 Pending（根 + ns2），实际 %d", len(pending))
	}
	// 验证返的都是 Pending
	nsSet := map[string]bool{}
	for _, p := range pending {
		nsSet[p.NS] = true
		if p.Checkpoint.Status != compose_v2.CPStatusPending {
			t.Errorf("ListPending 返非 Pending：ns=%q status=%v", p.NS, p.Checkpoint.Status)
		}
	}
	if !nsSet[""] || !nsSet["ns2"] {
		t.Fatalf("应含 '' 和 'ns2'，实际 %v", nsSet)
	}
}

// ── GraphID 防串台（ISSUE-50 P0）──

func TestCheckpointer_GraphIDConflict(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// 第 1 次 SaveNS GraphID="g_a"
	if err := cp.SaveNS(ctx, "sid", "ns1", &compose_v2.Checkpoint{GraphID: "g_a"}); err != nil {
		t.Fatalf("first SaveNS: %v", err)
	}

	// 第 2 次同 (sid, ns) 但 GraphID="g_b" → 应报 ErrGraphIDConflict
	err := cp.SaveNS(ctx, "sid", "ns1", &compose_v2.Checkpoint{GraphID: "g_b"})
	if err == nil {
		t.Fatal("GraphID 冲突应报错")
	}
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) {
		t.Fatalf("err 应是 ErrGraphIDConflict，实际 %v", err)
	}

	// 同 GraphID 重复 SaveNS 不报错（覆盖）
	if err := cp.SaveNS(ctx, "sid", "ns1", &compose_v2.Checkpoint{GraphID: "g_a"}); err != nil {
		t.Fatalf("same GraphID re-save: %v", err)
	}
}

// ── ExistsNS 快速判存在 ──

func TestCheckpointer_ExistsNS(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	exists, _ := cp.ExistsNS(ctx, "sid", "")
	if exists {
		t.Fatal("空 cp 不应存在")
	}

	_ = cp.SaveNS(ctx, "sid", "", &compose_v2.Checkpoint{})
	exists, _ = cp.ExistsNS(ctx, "sid", "")
	if !exists {
		t.Fatal("Save 后应存在")
	}
	exists, _ = cp.ExistsNS(ctx, "sid", "non_existent_ns")
	if exists {
		t.Fatal("不存在的 ns 不应 true")
	}
}

// ── 并发安全（4 goroutines 同时写 + 读）──

func TestCheckpointer_ConcurrentSafe(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	done := make(chan struct{}, 8)
	// 4 writers
	for i := 0; i < 4; i++ {
		i := i
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 100; j++ {
				_ = cp.SaveNS(ctx, "sid", "ns_w", &compose_v2.Checkpoint{})
				_ = i
				_ = j
			}
		}()
	}
	// 4 readers
	for i := 0; i < 4; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 100; j++ {
				_, _ = cp.LoadNS(ctx, "sid", "ns_w")
			}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	// 仅验证不 race / panic（go test -race 跑会更严格）
}

// ── ParentNS 自动填 ──

func TestCheckpointer_ParentNS_AutoFill(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	cases := []struct{ ns, wantParent string }{
		{"", ""},
		{"a", ""},
		{"a/b", "a"},
		{"a/b/c", "a/b"},
		{"a#0", "a"},
		{"a/b#1/c", "a/b#1"},
	}

	for _, c := range cases {
		_ = cp.SaveNS(ctx, "sid", c.ns, &compose_v2.Checkpoint{})
		got, _ := cp.LoadNS(ctx, "sid", c.ns)
		if got.ParentNS != c.wantParent {
			t.Errorf("ns=%q ParentNS 应 %q 实际 %q", c.ns, c.wantParent, got.ParentNS)
		}
	}
}
