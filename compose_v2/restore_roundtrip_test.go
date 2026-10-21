package compose_v2_test

// Snapshot↔Restore round-trip 完备性回归（引擎深审）。
//
// **承重不变量**：`Snapshot()` 读的每个 cm 字段，`Restore()` 都必须填回。
//
// 为什么这条不变量是承重的：Run 出口 defer 对**所有** runErr != nil 都 Snapshot+SaveNS
// （业务错也存 Pending cp 让 Resume 能重试——这是特性不是 bug）。而 Resume 路径是
// 「Restore(cp) → …一段… → 主循环 updateFrontier」。任何 Restore 不填的字段，在这段
// 窗口里就是零值；窗口内任何 early return → Snapshot 拿零值 → **覆盖掉原本好的 cp**。
//
// `frontierAtExit` 曾漏填（只被 updateFrontier 写）→ 静默丢 HITL 恢复点。
//
// sanity-revert：删 serialization.go Restore 末尾的 `cm.frontierAtExit = ...` 那块
// → 本文件两个测试都 fail。

import (
	"context"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
)

// TestRestore_FrontierAtExitRoundTrips 直接钉字段级契约——不依赖 graph_loop 的具体
// early return 位置（那些会随重构长出/消失），只钉「Snapshot 读的字段 Restore 必须填」。
//
// **fixture 必须含一个带实例身份的成员**：原来两个成员都是静态边（身份为空），断言也
// 只比 NodeID ⟹ Restore 把实例身份剥掉时**全仓无人报警**（审计实测：新旧两棵树各自
// 注入该退化，`compose_v2` + `agent_v2` + `sql` 全绿）。危害窗口窄但真实——
// `frontierAtExit` 在「Restore 之后、`updateFrontier` 之前 early return」那一刻会被
// Snapshot 读走，那时写回的 cp 里实例身份已经没了，多实例 Resume 后互相冒充。
func TestRestore_FrontierAtExitRoundTrips(t *testing.T) {
	ch := compose_v2.LastValue[string]("__rt2__")
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.RegisterChannel(cm, ch); err != nil {
		t.Fatalf("register: %v", err)
	}

	want := []compose_v2.FrontierMember{
		{NodeID: "n1"},                    // 静态边成员：身份空
		{NodeID: "n2", InstanceID: "i42"}, // SendFrom 派生实例：带 token
	}
	if err := cm.Restore(&compose_v2.Checkpoint{CurrentFrontier: want}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Restore 之后立刻 Snapshot（模拟"窗口内 early return → Run-defer Snapshot"）
	snap, err := cm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	// **整条比不是只比 NodeID**——身份内联后两个字段一起是一条记录，分开比就是
	// 把已经消除掉的「下标对齐」思路又搬回测试里
	if len(snap.CurrentFrontier) != len(want) ||
		snap.CurrentFrontier[0] != want[0] || snap.CurrentFrontier[1] != want[1] {
		t.Errorf("Restore→Snapshot 未 round-trip CurrentFrontier: got %+v want %+v\n"+
			"  Snapshot 读 cm.frontierAtExit，Restore 必须**整条**填它——只填 NodeID 时，\n"+
			"  窗口内 Snapshot 写回的 cp 实例身份被剥空 → 多实例 Resume 后互相冒充",
			snap.CurrentFrontier, want)
	}
}

// TestRestore_FrontierCopyProtectsCallerCheckpoint 钉住 Restore 回填 frontier 时
// **必须拷贝、不能别名**。
//
// # 为什么需要这条哨兵（它是本轮内联改动的代价）
//
// 内联之前 `cp.CurrentFrontier` 是 `[]NodeID`、`cm.frontierAtExit` 是 `[]FrontierMember`,
// 两个方向都得过元素类型转换 ⟹ **别名在类型层就不可表达**，不变量由编译器守。内联之后
// 两侧同类型，`cm.frontierAtExit = cp.CurrentFrontier` 能干净编译且静默错——不变量降级
// 成手写 `make`+`copy` 加注释。**内联换掉了「下标对齐」那条约束，同时新引入了这条**，
// 诚实记在这儿。
//
// # 为什么它天生难抓
//
// `make([]T, n)` 给出 cap==len，而 `updateFrontier` 是 `append(frontierAtExit[:0], ...)`
// **就地复用底层数组** ⟹ 别名只在 frontier **收缩或等长**时才写回原数组，**变长会
// realloc 自动躲开**。库里既有的 Resume 用例恰好都是等长或变长，所以整套 -race 全绿。
// 本用例专门构造收缩：两个动态实例 → 静态边汇到同一个后继 → nextSet 去重 → 2 收缩到 1。
//
// # 受损的是谁
//
// 引擎自己在进循环前就把 cp 读完了，本次 Run 不受影响；坏的是**调用方手里那份对象**
// ——正是 godoc 推荐的跨进程用法（Marshal → wire → Unmarshal → WithResumeFrom）和业务
// 拿 ListPending / LoadAllInSession 结果做的任何事。`InMemoryCheckpointer` 直接存/返
// 同一指针，是唯一暴露面（redis / sql 过 JSON 天然免疫），而它正是测试 / example /
// 未配持久后端时的默认。
func TestRestore_FrontierCopyProtectsCallerCheckpoint(t *testing.T) {
	var resumed atomic.Bool

	ask := func(id string) compose_v2.NodeFunc {
		return func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if resumed.Load() {
				return nil, nil
			}
			return nil, &compose_v2.InterruptError{
				Reason: "alias_probe",
				Query:  &compose_v2.AskQuery{InterruptID: "approve_" + id, Question: "approve " + id + "?"},
			}
		}
	}

	b := compose_v2.NewBuilder().WithName("frontier_alias_probe")
	b.AddNode("gate", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "A"},
			compose_v2.SendDirective{Target: "B"},
		)
		return nil, nil
	})
	b.AddNode("A", ask("A"))
	b.AddNode("B", ask("B"))
	b.AddNode("C", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "gate")
	// A、B 静态边汇到同一个 C —— computeNextTasks 的 nextSet 去重 ⟹ 下一超步 frontier
	// 从 2 个成员**收缩**到 1 个，这是触发就地写回的必要条件
	b.AddEdge("A", "C")
	b.AddEdge("B", "C")
	b.AddEdge("C", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// Run 1：A、B 各自带实例身份进 frontier，双双中断 → cp.CurrentFrontier len2 cap2
	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("alias_sid"))); !ok {
		t.Fatal("Run1 应双双中断")
	}

	// 拿到**调用方手里那份 cp**（InMemory 返的就是存着的那个指针），并留一份内容副本
	held, _ := cper.LoadNS(ctx, "alias_sid", "")
	if held == nil || len(held.CurrentFrontier) != 2 {
		t.Fatalf("Run1 应存下 2 个成员的 frontier，实际 %+v", held)
	}
	before := append([]compose_v2.FrontierMember(nil), held.CurrentFrontier...)

	// Resume：A、B 成功 → 出边去重 → frontier 收缩到 [C] → updateFrontier 就地写
	resumed.Store(true)
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("alias_sid")); err != nil {
		t.Fatalf("Resume 应成功, got %v", err)
	}

	for i := range before {
		if held.CurrentFrontier[i] != before[i] {
			t.Fatalf("Resume 改写了调用方手里那份 cp：\n  前 %+v\n  后 %+v\n"+
				"  Restore 把 cp.CurrentFrontier 别名给了 cm.frontierAtExit，而 updateFrontier\n"+
				"  是 append(frontierAtExit[:0], ...) 就地复用底层数组 ⟹ frontier 收缩时原地\n"+
				"  写回调用方的数组，得到一个**从未存在过的 frontier**（新后继 + 陈旧的兄弟实例）。\n"+
				"  拿那份 cp 再 Resume 会重跑新后继 + 陈旧实例（副作用双跑）并丢掉另一个",
				before, held.CurrentFrontier)
		}
	}
}
