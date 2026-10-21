package compose_v2

// 节点私有中断态（node_state.go）的可见性边界。
//
// 判据只有一条，两类成员共用：**这一步成没成**。
//
//   - 成了（NodeFunc 通过 + 出边算得出来 + 写入提交成功）⟹ 提交点推进 visit ⟹ 下次访问
//     算出的是新键 ⟹ 旧 memo 自然不可达。这就是「不设清除点」那条不变量。
//   - 没成（节点报错 / 中断 / cond 算不出出边 / 提交失败）⟹ visit 不动 ⟹ 键与上次重合
//     ⟹ 重跑时读得到自己上次的进度。**这正是 memo 存在的理由**——已经下单/扣款/发消息
//     的那几件不再重放。
//
// 另有两条纯确认：FreshRun / 弃用挂起后 memo 不可见（那是「换任务」不是「重跑」）。
//
// # 这里曾经有过一个清除点
//
// 出边还是提交之后的独立相位时，一个成员可以「已经成功、写入已提交、visit 已推进」，
// 然后因为 cond 报错被拉回来重跑——那时它的键跟上次重合（实例侧的 visit 由
// `nodeStateKey` 置 0），却又被要求「重新产出」，于是加了一道按实例精确清 memo 的 purge。
// 出边归成员之后那个状态不可达，purge 连同它依赖的 `mergeCompletedSiblings` 已删。

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type nsProbeState struct{ Done []string }

func init() { RegisterValueType[*nsProbeState]("*compose_v2.nsProbeState") }

// TestSuspect_FreshRunSeesStaleMemo 怀疑一：WithFreshRun 语义是「忽略 cp 从头跑」，
// 但 memo 若被 Restore 进来，节点会读到上次的进度、跳过本该重做的事。
func TestNodeState_FreshRunSeesNoStaleMemo(t *testing.T) {
	var seenPrev []string
	var runs int
	b := NewBuilder().WithName("suspect_fresh")
	b.AddNode("n", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		runs++
		if prev, ok := GetNodeState[*nsProbeState](ctx); ok && prev != nil {
			seenPrev = append(seenPrev, fmt.Sprint(prev.Done))
		} else {
			seenPrev = append(seenPrev, "<无>")
		}
		_ = SaveNodeState(ctx, &nsProbeState{Done: []string{"step1"}})
		if runs == 1 {
			return nil, &InterruptError{Query: &AskQuery{InterruptID: "sq", Question: "?"}}
		}
		return nil, nil
	})
	b.AddEdge(StartNode, "n")
	b.AddEdge("n", EndNode)
	g, _ := b.Compile()

	cper := NewInMemoryCheckpointer()
	ctx := context.Background()
	if err := Run(ctx, g, NewChannelManager(), WithCheckpointer(cper), WithSessionID("fr")); err == nil {
		t.Fatal("应中断")
	}
	// WithFreshRun：语义是忽略 cp 从头跑
	err := Run(ctx, g, NewChannelManager(), WithCheckpointer(cper), WithSessionID("fr"), WithFreshRun())
	t.Logf("FreshRun err=%v", err)
	t.Logf("节点每次看到的上次进度 = %v", seenPrev)
	if len(seenPrev) >= 2 && seenPrev[1] != "<无>" {
		t.Errorf("❌ WithFreshRun 下节点读到了上次的 memo（%s）——语义应是从头跑", seenPrev[1])
	} else {
		t.Logf("✅ FreshRun 下 memo 不可见")
	}
}

// TestSuspect_AbortThenNewRunSeesStaleMemo 怀疑一续：Abort 弃用挂起后，同 sid 新 Run
// 会不会读到上次的 memo。
func TestNodeState_AbandonedThenNewRunSeesNoStaleMemo(t *testing.T) {
	var seenPrev []string
	var runs int
	b := NewBuilder().WithName("suspect_abort")
	b.AddNode("n", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		runs++
		if prev, ok := GetNodeState[*nsProbeState](ctx); ok && prev != nil {
			seenPrev = append(seenPrev, fmt.Sprint(prev.Done))
		} else {
			seenPrev = append(seenPrev, "<无>")
		}
		_ = SaveNodeState(ctx, &nsProbeState{Done: []string{"step1"}})
		if runs == 1 {
			return nil, &InterruptError{Query: &AskQuery{InterruptID: "aq", Question: "?"}}
		}
		return nil, nil
	})
	b.AddEdge(StartNode, "n")
	b.AddEdge("n", EndNode)
	g, _ := b.Compile()

	cper := NewInMemoryCheckpointer()
	ctx := context.Background()
	if err := Run(ctx, g, NewChannelManager(), WithCheckpointer(cper), WithSessionID("ab")); err == nil {
		t.Fatal("应中断")
	}
	cp, _ := cper.LoadNS(ctx, "ab", "")
	t.Logf("中断后 cp.NodeStates 条数 = %d", len(cp.NodeStates))
	// 模拟业务弃用：删 cp 子树（agent 层 ResumeActionAbort 的底层动作）
	n, _ := cper.DeleteSubtree(ctx, "ab", "")
	t.Logf("DeleteSubtree 删了 %d 份 cp", n)
	if err := Run(ctx, g, NewChannelManager(), WithCheckpointer(cper), WithSessionID("ab")); err != nil {
		t.Fatalf("弃用后新 Run: %v", err)
	}
	t.Logf("节点每次看到的上次进度 = %v", seenPrev)
	if len(seenPrev) >= 2 && seenPrev[1] != "<无>" {
		t.Errorf("❌ 弃用挂起后新 Run 读到了上次 memo（%s）", seenPrev[1])
	} else {
		t.Logf("✅ 弃用后 memo 不可见")
	}
}

// TestNodeState_CondFailedInstance_KeepsOwnMemo 钉 `SendFrom` 派生实例那半：cond 算不出
// 出边 ⟹ **这个实例这一步没成** ⟹ 重跑时读得到自己上次的进度。
//
// 这是要的行为，不是漏网：实例在 NodeFunc 里已经做过的带副作用的事（下单 / 扣款 / 发消息）
// 不该因为「出边没算出来」而重放一遍。判据跟中断重跑完全一致——两者都是「这一步没成」。
//
// **反向的那半**（成了就读不到）由 TestNodeState_FreshRunSeesNoStaleMemo 与
// 「成功后 visit 递增」共同守。
func TestNodeState_CondFailedInstance_KeepsOwnMemo(t *testing.T) {
	type seenRec struct {
		inst string
		prev string
	}
	var seen []seenRec
	// **fanout 下 w 有两个实例并发跑**——记录用的 slice 必须加锁（`-race` 抓到过裸 append）。
	// 顺序不确定是 by-design（超步内并发），所以断言只看「每次执行读到什么」不看顺序。
	var seenMu sync.Mutex
	var condCalls atomic.Int32
	outCh := LastValue[string]("__sus_out__")

	b := NewBuilder().WithName("suspect_rerun")
	b = WithChannel(b, outCh)
	b.AddNode("split", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		SendFrom(ctx, SendDirective{Target: "w"}, SendDirective{Target: "w"})
		return nil, nil
	})
	b.AddNode("w", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		inst := GetCurrentInstanceID(ctx)
		prev := "<无>"
		if p, ok := GetNodeState[*nsProbeState](ctx); ok && p != nil {
			prev = fmt.Sprint(p.Done)
		}
		seenMu.Lock()
		seen = append(seen, seenRec{inst: inst, prev: prev})
		seenMu.Unlock()
		_ = SaveNodeState(ctx, &nsProbeState{Done: []string{"did:" + inst}})
		return []ChannelWrite{Write(outCh, "ok")}, nil
	})
	b.AddNode("tail", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) { return nil, nil })
	b.AddEdge(StartNode, "split")
	// w 的出边是 cond：前两次报错，让两个实例都失败、都进重跑集合。
	//
	// **计数器必须是 atomic**：cond 现在跑在**各成员自己的 goroutine 里**（出边归成员），
	// 多实例节点的 cond 因此并发执行——裸 int 会被 `-race` 抓到（实测抓到过）。
	b.AddConditionalEdge("w", func(_ context.Context, _ Channels) (NodeID, error) {
		if n := condCalls.Add(1); n <= 2 {
			return "", fmt.Errorf("cond 第 %d 次故意失败", n)
		}
		return "tail", nil
	}, WithCondFailPolicy(CondFailRerunFrom))
	b.AddEdge("tail", EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cper := NewInMemoryCheckpointer()
	ctx := context.Background()
	e1 := Run(ctx, g, NewChannelManager(), WithCheckpointer(cper), WithSessionID("rr"))
	t.Logf("Run1 err=%v", e1)
	cp, _ := cper.LoadNS(ctx, "rr", "")
	if cp != nil {
		t.Logf("cp frontier=%+v  NodeStates 条数=%d", cp.CurrentFrontier, len(cp.NodeStates))
	}
	e2 := Run(ctx, g, NewChannelManager(), WithCheckpointer(cper), WithSessionID("rr"))
	t.Logf("Run2(Resume) err=%v", e2)
	seenMu.Lock()
	snapshot := append([]seenRec(nil), seen...)
	seenMu.Unlock()
	for i, r := range snapshot {
		t.Logf("  w 第 %d 次执行: inst=%q 读到上次进度=%s", i+1, r.inst, r.prev)
	}
	// 首跑两个实例各读不到（本来就没存过）；重跑时各自读得到**自己**那份。
	if len(snapshot) != 4 {
		t.Fatalf("w 应跑 4 次（2 实例 × (首跑 + 重跑)），实际 %d 次：%+v", len(snapshot), snapshot)
	}
	byInst := map[string][]string{}
	for _, r := range snapshot {
		byInst[r.inst] = append(byInst[r.inst], r.prev)
	}
	if len(byInst) != 2 {
		t.Fatalf("应是 2 个不同实例，实际 %d：%+v", len(byInst), byInst)
	}
	for inst, prevs := range byInst {
		if len(prevs) != 2 {
			t.Fatalf("实例 %q 应跑 2 次，实际 %d：%v", inst, len(prevs), prevs)
		}
		if prevs[0] != "<无>" {
			t.Errorf("实例 %q 首跑不该读到任何 memo, got %s", inst, prevs[0])
		}
		// **承重断言**：重跑读得到**自己**上次那份（不是兄弟的）——键含 InstanceID
		want := "[did:" + inst + "]"
		if prevs[1] != want {
			t.Errorf("实例 %q 重跑应读到自己上次的进度 %s, got %s\n"+
				"  读不到 = 这一步「没成」却推进了 visit（键漂了），已完成的副作用会被重放；\n"+
				"  读到别人的 = 键漏了 InstanceID，实例互相冒充。\n"+
				"  sanity-revert: 把 advanceVisitsLocked 的 visit 自增挪回 runNodeWithHooks 成功路径 → 本断言 fail",
				inst, want, prevs[1])
		}
	}
}

// TestNodeState_StaticMemberCondFailed_KeepsMemoAndVisit 钉静态边成员那半——**跟实例那半
// 同一条规则**，这正是本次改动带来的统一。
//
// 曾经两半由不同机制保护（静态靠 visit 递增自然不可达、实例靠一道 purge），因为那时
// 「已成功成员被拉回重跑」是可达状态，而 visit 在节点函数一返回就推进了。现在 visit 在
// **提交点**推进，「没成」的成员一律 visit 不动 ⟹ 两半的行为一样。
//
// 本条同时是 visit 推进时刻的哨兵：断言 cond 失败那轮 visit **没有**推进。
func TestNodeState_StaticMemberCondFailed_KeepsMemoAndVisit(t *testing.T) {
	var seen []string
	var condCalls int
	outCh := LastValue[string]("__ns_static_out__")

	b := NewBuilder().WithName("ns_static")
	b = WithChannel(b, outCh)
	b.AddNode("w", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		prev := "<无>"
		if p, ok := GetNodeState[*nsProbeState](ctx); ok && p != nil {
			prev = fmt.Sprint(p.Done)
		}
		seen = append(seen, fmt.Sprintf("visit=%d prev=%s", GetCurrentNodeVisit(ctx), prev))
		_ = SaveNodeState(ctx, &nsProbeState{Done: []string{"did"}})
		return []ChannelWrite{Write(outCh, "ok")}, nil
	})
	b.AddNode("tail", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) { return nil, nil })
	b.AddEdge(StartNode, "w") // 静态边 ⟹ w 是空身份成员
	b.AddConditionalEdge("w", func(_ context.Context, _ Channels) (NodeID, error) {
		condCalls++
		if condCalls == 1 {
			return "", fmt.Errorf("cond 故意失败一次")
		}
		return "tail", nil
	}, WithCondFailPolicy(CondFailRerunFrom))
	b.AddEdge("tail", EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cper := NewInMemoryCheckpointer()
	ctx := context.Background()
	if err := Run(ctx, g, NewChannelManager(), WithCheckpointer(cper), WithSessionID("nss")); err == nil {
		t.Fatal("Run1 应因 cond 失败退出")
	}
	if err := Run(ctx, g, NewChannelManager(), WithCheckpointer(cper), WithSessionID("nss")); err != nil {
		t.Fatalf("Run2(Resume): %v", err)
	}
	t.Logf("w 每次执行: %v", seen)
	if len(seen) != 2 {
		t.Fatalf("w 应跑 2 次（首跑 + 降级重跑），实际 %d 次：%v", len(seen), seen)
	}
	// **承重断言 1**：cond 失败那轮不算成功 ⟹ visit 没推进 ⟹ 两轮都是 visit=0
	if !strings.Contains(seen[0], "visit=0") || !strings.Contains(seen[1], "visit=0") {
		t.Errorf("cond 算不出出边 = 这一步没成 ⟹ visit 不该推进，两轮都该是 visit=0，实际 %v\n"+
			"  出现 visit=1 说明推进点又跑到「节点函数一返回」去了——那会让中断身份跨重跑漂移\n"+
			"  sanity-revert: 把 advanceVisitsLocked 的 visit 自增挪回 runNodeWithHooks 成功路径 → 本断言 fail", seen)
	}
	// **承重断言 2**：键没漂 ⟹ 重跑读得到自己上次的进度（已完成的副作用不重放）
	if !strings.Contains(seen[1], "prev=[did]") {
		t.Errorf("重跑应读到自己上次的进度（memo 的全部意义），实际 %v", seen)
	}
}

// TestNodeState_RestoreRollbackIncludesNodeStates Restore 的 error 回滚必须带上 nodeStates。
//
// 它的赋值排在 DeferredSends / PendingSendWrites 两个会 rollbackOnErr 的步骤**之前**，
// 漏进快照清单时那两步失败会留下「旧 cm 上盖了新 cp 的 memo」的半还原状态——而
// `Restore` 的注释承诺的是 atomic 语义。漏了不报错，只是状态半烂。
func TestNodeState_RestoreRollbackIncludesNodeStates(t *testing.T) {
	cm := NewChannelManager()

	good, err := marshalValue(&nsProbeState{Done: []string{"GOOD"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := cm.Restore(&Checkpoint{NodeStates: map[string]taggedValue{"k": good}}); err != nil {
		t.Fatalf("先铺一份好的: %v", err)
	}

	// 坏 cp：同键换成另一份 memo + DeferredSends 用未注册类型 ⟹ 走 rollbackOnErr
	polluted, err := marshalValue(&nsProbeState{Done: []string{"POLLUTED"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	bad := &Checkpoint{
		NodeStates: map[string]taggedValue{"k": polluted},
		DeferredSends: []SerializedSend{{
			Target: "x",
			Payload: []SerializedWrite{
				{Channel: "c", Type: "no.such.Type", Data: []byte(`"v"`)},
			},
		}},
	}
	if err := cm.Restore(bad); err == nil {
		t.Fatal("坏 cp 应返 error（DeferredSends 类型未注册）")
	}

	tv, ok := cm.getNodeState("k")
	if !ok {
		t.Fatal("回滚后 memo 应还在")
	}
	v, uerr := unmarshalValue(tv)
	if uerr != nil {
		t.Fatalf("unmarshal: %v", uerr)
	}
	st, _ := v.(*nsProbeState)
	if st == nil || len(st.Done) != 1 || st.Done[0] != "GOOD" {
		t.Errorf("memo 没随 error 回滚：want [GOOD]，got %+v\n"+
			"  = rollbackOnErr / panic defer 的快照清单漏了 nodeStates，\n"+
			"  而它的赋值在两个会 rollback 的步骤之前 ⟹ 半还原状态", st)
	}
}

// TestNodeState_MarshalledBytesNeverAliased Snapshot 浅拷贝安全性的哨兵。
//
// `Snapshot` 只拷 nodeStates 这张 map、**不深拷 `taggedValue.Data`**。这么做安全的唯一
// 依据是「`marshalValue` 每次都新分配一份、从不原地改写或复用 buffer」——一条隐式约定，
// 破了不报错：async durability 的后台 writer 会拿着已交出的 cp 读到被后续超步改掉的
// 字节，静默写出撕裂的 cp。
//
// 本测试钉住那条约定：连续两次 marshal 的底层数组不得共享，且先前那份的内容不得被后
// 一次改动。给 `marshalValue` 加 buffer pool（性能条款正会引向这个）时它会红。
func TestNodeState_MarshalledBytesNeverAliased(t *testing.T) {
	first, err := marshalValue(&nsProbeState{Done: []string{"AAAAAAAAAAAA"}})
	if err != nil {
		t.Fatalf("marshal 1: %v", err)
	}
	// 扣住第一份的字节（模拟已交给后台 writer 的那份 cp）
	snapshot := string(first.Data)

	second, err := marshalValue(&nsProbeState{Done: []string{"BBBBBBBBBBBB"}})
	if err != nil {
		t.Fatalf("marshal 2: %v", err)
	}

	if string(first.Data) != snapshot {
		t.Errorf("第一份 marshal 的字节被第二次调用改掉了：\n  before=%q\n  after =%q\n"+
			"  ——marshalValue 复用了 buffer。Snapshot 的浅拷贝立刻不再安全：\n"+
			"  async durability 的后台 writer 会写出撕裂的 cp，且不报错",
			snapshot, string(first.Data))
	}
	if len(first.Data) > 0 && len(second.Data) > 0 && &first.Data[0] == &second.Data[0] {
		t.Errorf("两次 marshal 的底层数组是同一块——marshalValue 复用了 buffer")
	}
	if strings.Contains(string(first.Data), "BBBB") {
		t.Errorf("第一份里出现了第二份的内容：%q", string(first.Data))
	}
}
