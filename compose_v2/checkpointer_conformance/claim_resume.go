package checkpointer_conformance

import (
	"errors"
	"testing"

	"crew-go/compose_v2"
)

func testClaimResume(t *testing.T, factory CheckpointerFactory) {
	ctx := t.Context()
	store := factory()
	const sid, ns = "claim-resume", "child"
	cp := &compose_v2.Checkpoint{
		Status: compose_v2.CPStatusPending, GraphID: "graph", TaskID: "task",
		PendingInterrupts: []*compose_v2.AskQuery{{InterruptID: "ask", Question: "继续？"}},
		CurrentFrontier:   []compose_v2.FrontierMember{{NodeID: "ask"}},
	}
	if err := store.SaveNS(ctx, sid, ns, cp); err != nil {
		t.Fatal(err)
	}
	expected, err := store.LoadNS(ctx, sid, ns)
	if err != nil || expected == nil {
		t.Fatalf("载入待答档: %v, %v", expected, err)
	}
	start := make(chan struct{})
	type result struct {
		cp  *compose_v2.Checkpoint
		err error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			<-start
			claimed, err := store.ClaimResume(ctx, sid, ns, expected, expected)
			results <- result{claimed, err}
		}()
	}
	close(start)
	var winner *compose_v2.Checkpoint
	for range 2 {
		out := <-results
		if out.err != nil {
			if !errors.Is(out.err, compose_v2.ErrCheckpointResumeUnconfirmed) && !errors.Is(out.err, compose_v2.ErrCheckpointStale) {
				t.Errorf("另一次申请须被拒绝: %v", out.err)
			}
			continue
		}
		if winner != nil || out.cp == nil {
			t.Fatalf("只允许一次续跑准入: %+v", out)
		}
		winner = out.cp
	}
	pending, err := store.ListPending(ctx, sid)
	if err != nil || len(pending) != 0 {
		t.Fatalf("已准入世代不能出现在待答索引: %v, %v", pending, err)
	}
	stored, err := store.LoadNS(ctx, sid, ns)
	if err != nil || winner == nil || stored == nil || stored.Epoch != winner.Epoch || stored.Epoch == "" ||
		stored.Status != compose_v2.CPStatusResuming || expected.Status != compose_v2.CPStatusPending ||
		len(stored.PendingInterrupts) != 1 || stored.PendingInterrupts[0].InterruptID != "ask" {
		t.Fatalf("准入须保留快照并提交唯一新世代: stored=%+v winner=%+v err=%v", stored, winner, err)
	}
	// 导入空槽也要在执行前提交新世代，原来的源档不受影响。
	const forkSid, forkNS = "claim-fork", "fork/child"
	fork, err := store.ClaimResume(ctx, forkSid, forkNS, nil, expected)
	if err != nil || fork == nil || fork.Status != compose_v2.CPStatusResuming ||
		fork.ID != forkSid || fork.NS != forkNS || fork.ParentNS != "fork" {
		t.Fatalf("空槽应持久领取导出的快照: %+v, %v", fork, err)
	}
	if _, err := store.ClaimResume(ctx, forkSid, forkNS, nil, expected); !errors.Is(err, compose_v2.ErrCheckpointResumeUnconfirmed) {
		t.Fatalf("同一个目标槽不可重放导入快照: %v", err)
	}
	source, err := store.LoadNS(ctx, sid, ns)
	if err != nil || source == nil || source.Epoch != winner.Epoch {
		t.Fatalf("导入不能改变原来的槽: %+v, %v", source, err)
	}

	next := *expected
	next.Epoch = "new-pause"
	if err := store.SaveNS(ctx, sid, ns, &next); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimResume(ctx, sid, ns, expected, expected); !errors.Is(err, compose_v2.ErrCheckpointStale) {
		t.Fatalf("旧快照不能领取新一轮: %v", err)
	}
	if _, err := store.ClaimResume(ctx, sid, ns, &next, &next); err != nil {
		t.Fatalf("新一轮待答档应重新开放: %v", err)
	}
}
