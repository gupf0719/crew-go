package checkpointer_conformance

import (
	"errors"
	"testing"

	"crew-go/compose_v2"
)

func testDeleteCondition(t *testing.T, factory CheckpointerFactory) {
	ctx := t.Context()
	store := factory()
	const sid, root, child = "conditional-delete", "branch", "branch/child"
	siblings := []string{"branch#0", "branch@1"}
	save := func(ns, epoch string, status compose_v2.CheckpointStatus) {
		t.Helper()
		cp := &compose_v2.Checkpoint{GraphID: "graph", TaskID: "task", Epoch: epoch, Status: status}
		if err := store.SaveNS(ctx, sid, ns, cp); err != nil {
			t.Fatal(err)
		}
	}
	for _, ns := range append([]string{root, child}, siblings...) {
		save(ns, "initial", compose_v2.CPStatusPending)
	}
	condition := func() compose_v2.CheckpointDeleteCondition {
		t.Helper()
		slots, err := store.LoadAllInSession(ctx, sid)
		if err != nil {
			t.Fatal(err)
		}
		var check compose_v2.CheckpointDeleteCondition
		for _, slot := range slots {
			if compose_v2.IsNSInSubtree(root, slot.NS) {
				check.Expected = append(check.Expected, slot)
			}
		}
		return check
	}
	assertPreserved := func(check compose_v2.CheckpointDeleteCondition, wantPending int) {
		t.Helper()
		n, err := store.DeleteSubtree(ctx, sid, root, check)
		if n != 0 || !errors.Is(err, compose_v2.ErrCheckpointStale) {
			t.Fatalf("条件不匹配须拒绝整棵子树：deleted=%d err=%v", n, err)
		}
		all, err := store.LoadAllInSession(ctx, sid)
		if err != nil || len(all) != wantPending+1 {
			t.Fatalf("失败清理改动了子树：%v, %v", all, err)
		}
		pending, err := store.ListPending(ctx, sid)
		if err != nil || len(pending) != wantPending {
			t.Fatalf("失败清理改动了待答索引：%v, %v", pending, err)
		}
	}
	before := condition()
	save(child, "newer", compose_v2.CPStatusResuming)
	assertPreserved(before, 3)
	before = condition()
	save(root+"/late", "late", compose_v2.CPStatusPending)
	assertPreserved(before, 4)
	if n, err := store.DeleteSubtree(ctx, sid, root, condition()); err != nil || n != 3 {
		t.Fatalf("条件匹配应只删除自身及后代：deleted=%d err=%v", n, err)
	}
	left, err := store.ListPending(ctx, sid)
	if err != nil || len(left) != len(siblings) {
		t.Fatalf("两个兄弟执行都须保留：%v, %v", left, err)
	}
}
