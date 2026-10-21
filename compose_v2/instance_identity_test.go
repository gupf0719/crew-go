package compose_v2_test

// ISSUE-96 验收标尺：branch 实例身份（FrontierMember.InstanceID）。
//
// 对应 issue-96 侧面 1/2/3——**都是静默丢数据**（不报错、结果错）。实施前必须红，
// 实施后必须绿，且 sanity-revert 能让它们重新变红。
//
// 根因：同 NodeID 可有 N 个并发实例（唯一来源 SendFrom 同 Target 投递），但身份此前
// 靠 branchIdx（frontier 里的**位置**）——部分失败时 frontier 缩成失败子集、位置重排。
// 详见 doc/issue/issue-96-branch-instance-identity.md。

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// idRecorder 并发安全地记录每轮观察到的 InstanceID（fanout 是并发的）。
type idRecorder struct {
	mu  sync.Mutex
	ids []string
}

func (r *idRecorder) add(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = append(r.ids, id)
}

func (r *idRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ids...)
}

func (r *idRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = nil
}

func distinctCount(ids []string) int {
	set := map[string]struct{}{}
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return len(set)
}

func containsStr(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

// TestISSUE96_SendFromInstancesHaveDistinctStableIDs：SendFrom 派生的 N 个实例
// ① 各有 distinct 的 InstanceID；② 部分失败后 Resume，重跑实例的 ID 与首轮一致。
//
// ② 是关键——branchIdx 正是在这里漂的（frontier 从 3 缩到 1，位置 2→0）。
func TestISSUE96_SendFromInstancesHaveDistinctStableIDs(t *testing.T) {
	var rec idRecorder
	var failWho string // 首轮记下要失败的那个实例 ID
	var mu sync.Mutex

	b := compose_v2.NewBuilder().WithName("i96_stable")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "w"},
			compose_v2.SendDirective{Target: "w"},
			compose_v2.SendDirective{Target: "w"},
		)
		return nil, nil
	})
	b.AddNode("w", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		id := compose_v2.GetCurrentInstanceID(ctx)
		rec.add(id)
		// 首轮：第一个进来的实例失败，制造部分失败超步
		mu.Lock()
		defer mu.Unlock()
		if failWho == "" {
			failWho = id
			return nil, errI96Transient
		}
		if id == failWho {
			return nil, nil // Resume 重跑它，这次成功
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("w", compose_v2.EndNode)

	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	_ = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("i96_sid"))
	run1 := rec.snapshot()
	t.Logf("Run1 实例 IDs: %v（失败的是 %q）", run1, failWho)
	if len(run1) != 3 {
		t.Fatalf("Run1 应跑 3 个实例, got %d: %v", len(run1), run1)
	}
	if distinctCount(run1) != 3 {
		t.Errorf("3 个 SendFrom 派生实例的 InstanceID 应全 distinct, got %v", run1)
	}

	// Resume：只重跑失败的那个 —— 它的 InstanceID 必须与 Run1 一致。
	//
	// 用 WithResumeFrom **显式**恢复：上一次是业务错退出，存档标了 CPStatusFailed。
	// 显式指名是为了让这条用例只测「重跑的那个实例身份对不对」，不依赖入口的捡档行为
	//（2026-08-13 起续跑闸门按内容判（`HasResumableWork`）、不读 Status，失败档本来也会被捡起）。
	rec.reset()
	failedCp, loadErr := cper.LoadNS(ctx, "i96_sid", "")
	if loadErr != nil || failedCp == nil {
		t.Fatalf("失败存档应当保留（只是不进 ListPending）, err=%v cp=%v", loadErr, failedCp)
	}
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("i96_sid"),
		compose_v2.WithResumeFrom(failedCp)); err != nil {
		t.Fatalf("Resume 应成功, got %v", err)
	}
	run2 := rec.snapshot()
	t.Logf("Resume 重跑的实例 IDs: %v", run2)
	for _, id := range run2 {
		if !containsStr(run1, id) {
			t.Errorf("Resume 重跑实例 InstanceID=%q 在 Run1 里不存在（**身份漂移**）\n"+
				"  Run1=%v  Resume=%v\n"+
				"  branchIdx 正是在这漂的：frontier 从 3 缩成失败子集 1，位置 2→0",
				id, run1, run2)
		}
	}
}

// TestISSUE96_StaticEdgeMemberHasEmptyInstanceID：静态边节点恒单实例 → InstanceID 空。
// 它们的跨访问代际由 visit 承担（**不能**也不需要 mint token，因为静态边根本不产
// SendDirective、没有 mint 点）。
func TestISSUE96_StaticEdgeMemberHasEmptyInstanceID(t *testing.T) {
	var got string
	var seen bool

	b := compose_v2.NewBuilder().WithName("i96_static")
	b.AddNode("solo", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		got, seen = compose_v2.GetCurrentInstanceID(ctx), true
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "solo")
	b.AddEdge("solo", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !seen {
		t.Fatal("节点没跑")
	}
	if got != "" {
		t.Errorf("静态边节点的 InstanceID 应为空串（恒单实例，身份走 NodeID + visit）, got %q", got)
	}
}

var errI96Transient = &i96Err{}

type i96Err struct{}

func (*i96Err) Error() string { return "transient failure for issue-96 probe" }

// TestISSUE96_InstanceIDWireName 钉住 frontier 成员在 JSON 里的**键名与内联语义**。
//
// **为什么单列**：`InMemoryCheckpointer` 直存 *Checkpoint 指针、**不过 JSON**，
// 所以 conformance 里那条断言对它恒真——抓不到"业务 backend 漏序列化"。真正会坏的
// 是跨进程消费者（PG JSONB `->'current_frontier'->>'instance_id'` / 其它语言 runtime）。
// 同 SerializedSend.Owner 当初踩的坑。
//
// **本测试随 cp 换成内联身份改写**：原来钉的是「两条平行数组等长」，现在钉的是
// 「每条记录自带 node_id + instance_id」。等长那条不变量随平行数组一起消失了——
// 那正是这次改动的目的：约束不存在就无需测试去守。
func TestISSUE96_InstanceIDWireName(t *testing.T) {
	data, err := compose_v2.MarshalCheckpoint(&compose_v2.Checkpoint{
		ID: "sid", GraphID: "g",
		CurrentFrontier:   []compose_v2.FrontierMember{{NodeID: "a"}, {NodeID: "b", InstanceID: "i7"}},
		CompletedFrontier: []compose_v2.FrontierMember{{NodeID: "c"}, {NodeID: "d", InstanceID: "i9"}},
		DeferredSends: []compose_v2.SerializedSend{
			{Target: "t", Owner: "d", OwnerInstanceID: "i9", InstanceID: "i12"},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	type wireMember struct {
		NodeID   string `json:"node_id"`
		Instance string `json:"instance_id"`
	}
	var raw struct {
		Frontier  []wireMember `json:"current_frontier"`
		Completed []wireMember `json:"completed_frontier"`
		Deferred  []struct {
			Owner     string `json:"owner"`
			OwnerInst string `json:"owner_instance_id"`
			Instance  string `json:"instance_id"`
		} `json:"deferred_sends"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	wantFrontier := []wireMember{{NodeID: "a"}, {NodeID: "b", Instance: "i7"}}
	if len(raw.Frontier) != 2 || raw.Frontier[0] != wantFrontier[0] || raw.Frontier[1] != wantFrontier[1] {
		t.Errorf("cp wire 的 current_frontier 应为 %+v, got %+v\n"+
			"  跨进程消费者（业务 PG JSONB 按 node_id/instance_id 取 / 其它语言 runtime）按这两个名解——"+
			"改名或漏序列化即静默读不到：漏 node_id 无从寻址、漏 instance_id 多实例 Resume 后互相冒充。\n"+
			"  **无身份的那条也必须在**（`{node_id:a}` 一条不能整个消失）——它表达「该成员是静态边节点」",
			wantFrontier, raw.Frontier)
	}
	wantCompleted := []wireMember{{NodeID: "c"}, {NodeID: "d", Instance: "i9"}}
	if len(raw.Completed) != 2 || raw.Completed[0] != wantCompleted[0] || raw.Completed[1] != wantCompleted[1] {
		t.Errorf("cp wire 的 completed_frontier 应为 %+v, got %+v（purge 拿它跟重跑成员精确匹配）",
			wantCompleted, raw.Completed)
	}
	// send 的发出者身份：owner(NodeID) + owner_instance_id 配对才拼得出完整成员身份
	// **目标**实例的身份（区别于 owner_instance_id 的"谁发的"）——deferred send 从 cp
	// 重放时按它重建实例，漏了 N 个重放实例全拿空串彼此无法区分
	if len(raw.Deferred) != 1 || raw.Deferred[0].Instance != "i12" {
		t.Errorf("cp wire 的 deferred_sends[].instance_id 应为 \"i12\", got %+v\n"+
			"  漏了 Resume 重放出的 N 个实例全空身份 → HITL InterruptID 撞车 / 子 cp 互覆 / 观测 span 共槽",
			raw.Deferred)
	}
	if len(raw.Deferred) != 1 || raw.Deferred[0].OwnerInst != "i9" {
		t.Errorf("cp wire 的 deferred_sends[].owner_instance_id 应为 \"i9\", got %+v\n"+
			"  漏了 purge 匹配键恒为 {owner,\"\"} → 跟带实例身份的重跑成员永远匹配不上 → Target 双发",
			raw.Deferred)
	}

	// 全静态（无 SendFrom 实例）时 instance_id 键**逐条省略**——绝大多数图是这样，
	// wire 上不该出现一堆空串。省略与「显式空串」在读回时等价（都是零值），故安全。
	data2, err := compose_v2.MarshalCheckpoint(&compose_v2.Checkpoint{
		ID: "sid", GraphID: "g",
		CurrentFrontier: []compose_v2.FrontierMember{{NodeID: "a"}},
	})
	if err != nil {
		t.Fatalf("marshal2: %v", err)
	}
	if strings.Contains(string(data2), "instance_id") {
		t.Errorf("全静态 cp 不该出现 instance_id 键（omitempty 逐条省略）: %s", data2)
	}
	if !strings.Contains(string(data2), `"node_id":"a"`) {
		t.Errorf("全静态 cp 的成员记录仍须带 node_id: %s", data2)
	}
}

// TestISSUE96_Side1_AutoFillIDStableAcrossResume 直接验**侧面 1**：多实例部分成功后，
// 框架按执行位置算出的 InterruptID 跨 Resume 稳定 → 用户答案配得上、一轮收敛。
//
// 这是 issue-96 侧面 1 的正面回归（此前实测：3 实例部分成功 → Resume 重算 ID 与原集合
// 重叠 0/2、且 Resume2 仍不收敛）。
func TestISSUE96_Side1_AutoFillIDStableAcrossResume(t *testing.T) {
	var answered atomic.Int32
	var interrupted atomic.Bool

	b := compose_v2.NewBuilder().WithName("i96_side1")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "w"},
			compose_v2.SendDirective{Target: "w"},
			compose_v2.SendDirective{Target: "w"},
		)
		return nil, nil
	})
	b.AddNode("w", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// **一次 interrupt 只调一次** CurrentInterruptSite——它内部 NextInterruptIndex
		// 每调递增，调两次会算出两个不同 ID（本测试初版就踩了这个坑）
		id := compose_v2.CurrentInterruptSite(ctx).InterruptID()
		if _, ok := compose_v2.GetResumeAnswer(ctx, id); ok {
			answered.Add(1)
			return nil, nil
		}
		// 只让抢到 CAS 的那个实例中断，另两个成功 → 部分失败超步
		if interrupted.CompareAndSwap(false, true) {
			return nil, &compose_v2.InterruptError{
				Reason: "side1",
				Query:  &compose_v2.AskQuery{InterruptID: id, Question: "?"},
			}
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("w", compose_v2.EndNode)

	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	runErr := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("side1_sid"))
	ie, ok := compose_v2.IsInterrupt(runErr)
	if !ok {
		t.Fatalf("Run1 应中断, got %v", runErr)
	}
	askID := ie.AllQueries()[0].InterruptID
	t.Logf("Run1 中断 ID = %s", askID)

	// 带上答案 Resume —— 一轮就该收敛（此前重叠 0/2、要 ~N 轮）
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("side1_sid"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			askID: schema.UserMessage("go"),
		})); err != nil {
		t.Fatalf("侧面 1 未修好：带对答案 Resume 仍失败 %v\n"+
			"  症状=ID 跨 Resume 漂移 → GetResumeAnswer miss → 二次中断、不收敛", err)
	}
	if answered.Load() != 1 {
		t.Errorf("重跑实例应认领到自己的答案（answered=1），实际 %d —— ID 漂了", answered.Load())
	}
}
