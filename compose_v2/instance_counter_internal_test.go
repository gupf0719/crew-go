package compose_v2

// 实例身份发号器跨进程重启的续号（ISSUE-96 audit）。
//
// instanceCounter 是进程级的，重启后从 0 重新发号；而 cp 里存着重启前发出去的
// token。不续号就会**逐位撞上**——两个互不相干的任务顶同一个身份：InterruptID
// 相同 ⟹ 后一个静默吃掉前一个的用户答案、自己从不提问、Run 还报成功；childNS
// 相同 ⟹ 它还会自动 load 走前一个的子 cp。

import (
	"math"
	"strconv"
	"testing"
)

// TestInstanceCounter_BumpsAcrossRestart：Restore 一份带 token 的 cp 后，新发的号
// 必须越过 cp 里出现过的所有旧号。
//
// sanity-revert：删 Restore 尾部的 bumpInstanceCounterFromCheckpoint 调用 → 重启后
// 第一个新号退回 "i1"，与 cp 里的 i1 撞车。
func TestInstanceCounter_BumpsAcrossRestart(t *testing.T) {
	// 模拟"重启前"发出去的号，存进 cp 的四个身份位置
	instanceCounter.Store(0)
	front, completed := newInstanceID(), newInstanceID()
	target, owner := newInstanceID(), newInstanceID()
	cp := &Checkpoint{
		CurrentFrontier:   []FrontierMember{{NodeID: "a", InstanceID: front}},
		CompletedFrontier: []FrontierMember{{NodeID: "b", InstanceID: completed}},
		DeferredSends: []SerializedSend{
			{Target: "c", InstanceID: target, Owner: "b", OwnerInstanceID: owner},
		},
	}
	old := map[string]bool{front: true, completed: true, target: true, owner: true}

	instanceCounter.Store(0) // ← 进程重启

	// **必须走 Restore 而不是直接调 bump helper**：直接调只能证明 helper 自己会算，
	// 证不了 Restore 真的接了它——把 Restore 里那行删掉，直调版本照样 PASS。
	cm := NewChannelManager()
	if err := cm.Restore(cp); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// 重启后连发几个号，一个都不许撞上 cp 里的旧号
	for i := 0; i < 4; i++ {
		got := newInstanceID()
		if old[got] {
			t.Fatalf("重启后新发的号 %q 撞上 cp 里已有的 token（cp 里有 %v）\n"+
				"  撞号 = 两个互不相干的任务同身份：后者静默吃掉前者的用户答案、"+
				"自己从不提问、Run 还报成功", got, old)
		}
	}
}

// TestInstanceCounter_BumpScansAllFourSites 钉住"扫哪些位置"——漏扫一处，那处的
// 旧 token 就会被重新发出去。四个位置各单独构造一次。
func TestInstanceCounter_BumpScansAllFourSites(t *testing.T) {
	mk := func(seed uint64) string { return "i" + strconv.FormatUint(seed, 36) }
	const big = 5000
	sites := map[string]*Checkpoint{
		"CurrentFrontier.InstanceID":   {CurrentFrontier: []FrontierMember{{NodeID: "a", InstanceID: mk(big)}}},
		"CompletedFrontier.InstanceID": {CompletedFrontier: []FrontierMember{{NodeID: "b", InstanceID: mk(big)}}},
		"DeferredSends.InstanceID": {DeferredSends: []SerializedSend{
			{Target: "x", InstanceID: mk(big)}}},
		"DeferredSends.OwnerInstanceID": {DeferredSends: []SerializedSend{
			{Target: "x", OwnerInstanceID: mk(big)}}},
	}
	for name, cp := range sites {
		instanceCounter.Store(0)
		bumpInstanceCounterFromCheckpoint(cp)
		if got := instanceCounter.Load(); got < big {
			t.Errorf("%s 里的 token 没被扫到：发号器停在 %d（应 ≥ %d）⟹ 该位置的旧号会被重发",
				name, got, big)
		}
	}
}

// TestInstanceFanoutIndex_NoSilentDegradeAboveInt32：超出 int32 的 token 仍要给出
// **能区分实例**的 ns 下标。
//
// 越界返 (0,false) 时调用方退回 JoinNS(parentNS, nodeID) —— 该节点全部实例撞同一个
// childNS、子 cp 互覆，且一声不吭。
func TestInstanceFanoutIndex_NoSilentDegradeAboveInt32(t *testing.T) {
	// 边界内：下标就是投递序号，ns 可读
	if idx, ok := instanceFanoutIndex("i" + strconv.FormatUint(42, 36)); !ok || idx != 42 {
		t.Errorf("边界内应直接返回原值: idx=%d ok=%v", idx, ok)
	}
	// 越界：不能退化成"没有 fanout 维度"
	a := "i" + strconv.FormatUint(math.MaxInt32+1, 36)
	b := "i" + strconv.FormatUint(math.MaxInt32+2, 36)
	ia, oka := instanceFanoutIndex(a)
	ib, okb := instanceFanoutIndex(b)
	if !oka || !okb {
		t.Fatalf("超 int32 的 token 不能返 ok=false（调用方会退回无 fanout 维度的 ns，"+
			"全部实例撞同一个 childNS 且无告警）: %v %v", oka, okb)
	}
	if ia == ib {
		t.Errorf("两个不同的越界 token 派生出同一个下标 %d —— 实例无法区分", ia)
	}
	if ia < 0 || ib < 0 {
		t.Errorf("下标必须非负（负数会让 ns 变成 name#-N 破坏解析）: %d %d", ia, ib)
	}
}
