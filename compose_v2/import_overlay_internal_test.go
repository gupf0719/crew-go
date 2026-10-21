package compose_v2

// 包内白盒：这两条测试验的是**不导出**的 `withImportOverlay`（两条路径都 apply）。
//
// # 为什么必须白盒
//
// 那个语义刻意不对外——它跟公开的 `WithInitialWrites` 是包含关系（both ⊃ fresh），
// 而包含关系下「该用窄的却用了宽的」在调用点上跟正当用法**观测等价**，框架分不出来，
// 所以只留一个公开入口。但它的两条不变量（续跑时确实 apply、失败时不降级恢复点）
// 必须有测试钉着。
//
// ⚠️ **不要为了让外部测试包能调而加导出 shim**——项目规约明写「不要为『能测』把内部
// 函数 export」（CLAUDE.md 测试节 / testing-conventions.md §白盒）。曾经加过一个
// `ExportWithImportOverlayForTest`，两轮独立审计都把它报了出来，已删。

import (
	"context"
	"sync/atomic"
	"testing"

	"crew-go/schema"
)

// import value——父图 Resume 时父节点可能改了 import channel，子图应读最新值。
//
// sanity-revert：把 subgraph.go 改回 `if savedSubCp != nil { 跳过 import 同步 }`
// 旧版逻辑，本测试应 fail（子图读到旧值）。
func TestF5_ResumeImportSyncsParentCm(t *testing.T) {
	importedCh := LastValue[string]("f5_imported")
	subSawCh := LastValue[string]("f5_sub_saw")

	var subRunCount atomic.Int32

	subB := NewBuilder().
		AddNode("inner", func(ctx context.Context, chs Channels) ([]ChannelWrite, error) {
			subRunCount.Add(1)
			val, _ := Get(chs, importedCh)
			if ans, ok := GetResumeAnswerText(ctx, "f5_ask"); ok {
				// Resume 路径：把当前看到的 imported value 写到 subSawCh
				_ = ans
				return []ChannelWrite{
					Write(subSawCh, "resume_saw:"+val),
				}, nil
			}
			// 首次执行：写第一次看到的值，然后中断
			return []ChannelWrite{
					Write(subSawCh, "first_saw:"+val),
				},
				&InterruptError{
					Query: &AskQuery{InterruptID: "f5_ask", Question: "?"},
				}
		}).
		AddEdge(StartNode, "inner").
		AddEdge("inner", EndNode)
	subB = WithChannel(subB, importedCh)
	subB = WithChannel(subB, subSawCh)
	sub, _ := subB.Compile()

	makeParent := func() *Graph {
		parentB := NewBuilder().
			AddSubgraphNode("sub_node", sub,
				WithImport(importedCh),
				WithExport(subSawCh),
			).
			AddEdge(StartNode, "sub_node").
			AddEdge("sub_node", EndNode)
		parentB = WithChannel(parentB, importedCh)
		parentB = WithChannel(parentB, subSawCh)
		parent, _ := parentB.Compile()
		return parent
	}

	cper := NewInMemoryCheckpointer()

	// === Run 1: 父图 imported="v1" → 子图首次执行 → 中断 ===
	cm1 := NewChannelManager()
	err := Run(context.Background(), makeParent(), cm1,
		WithCheckpointer(cper),
		WithSessionID("f5_sid"),
		withImportOverlay(Write(importedCh, "v1")),
	)
	if _, isInt := IsInterrupt(err); !isInt {
		t.Fatalf("Run 1 应中断，实际 %v", err)
	}
	if subRunCount.Load() != 1 {
		t.Fatalf("Run 1 子图 inner 应跑 1 次，实际 %d", subRunCount.Load())
	}

	// === Run 2: 父图 imported 改成 "v2" → 子图 Resume 应读到 "v2" 而非 saved cp 中的 "v1" ===
	cm2 := NewChannelManager()
	if err := RegisterChannel(cm2, importedCh); err != nil {
		t.Fatalf("register importedCh: %v", err)
	}
	if err := RegisterChannel(cm2, subSawCh); err != nil {
		t.Fatalf("register subSawCh: %v", err)
	}
	// 关键：父图重新 init 时 imported 改 "v2"
	err = Run(context.Background(), makeParent(), cm2,
		WithCheckpointer(cper),
		WithSessionID("f5_sid"),
		withImportOverlay(Write(importedCh, "v2")),
		WithResumeAnswers(map[string]*schema.Message{"f5_ask": schema.UserMessage("yes")}),
	)
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}

	// **关键不变量 F5**：subSawCh 最终应是 "resume_saw:v2"——子图 Resume 路径
	// 读到父图最新写入的 imported="v2"，不是 saved cp 中的旧 "v1"
	got, _ := Get(cm2.Channels(), subSawCh)
	if got != "resume_saw:v2" {
		t.Fatalf("F5 修复后 Resume 应读父图最新 imported='v2'，实际 subSaw=%q\n"+
			"sanity-revert：subgraph.go Resume 路径改回 silent 跳过 import 同步，本测试 fail 应显示 resume_saw:v1",
			got)
	}
}

// TestRestore_FailedResumeDoesNotDowngradeCheckpoint 是主证：Resume 在跑起来之前就
// 失败时，已存的恢复点必须原样保住。
//
// 触发（现存唯一的那个窗口）：HITL 存档 + Resume 带的 import overlay 里有**未注册**的
// channel → `setBaseline` 拒 → graph_loop 在 OnGraphStart 之前的 `import overlay` 处
// early return（此时还没 Restore、更没 updateFrontier）。
//
// ⚠️ 这个触发器曾经是「overlay 的 reducer panic」——import 改成不经 reducer 的 baseline
// 之后那条路不存在了（baseline 不跑 reducer）。窗口本身没变，只是换了把它打开的钥匙。
func TestRestore_FailedResumeDoesNotDowngradeCheckpoint(t *testing.T) {
	var aRuns atomic.Int32

	// 故意**不**在图上注册：setBaseline 对未注册的 channel 返错，这就是触发器。
	strayCh := LastValue[int]("__rt_stray_unregistered__")

	b := NewBuilder().WithName("restore_roundtrip")
	b.AddNode("A", func(ctx context.Context, _ Channels) ([]ChannelWrite, error) {
		aRuns.Add(1)
		if _, ok := GetResumeAnswer(ctx, "rt_q"); !ok {
			return nil, &InterruptError{
				Reason: "rt",
				Query:  &AskQuery{InterruptID: "rt_q", Question: "approve?"},
			}
		}
		return nil, nil
	})
	b.AddEdge(StartNode, "A")
	b.AddEdge("A", EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cper := NewInMemoryCheckpointer()
	ctx := context.Background()
	ans := map[string]*schema.Message{"rt_q": schema.UserMessage("yes")}

	// Run 1：A 中断 → 存下恢复点
	if _, ok := IsInterrupt(Run(ctx, g, NewChannelManager(),
		WithCheckpointer(cper), WithSessionID("rt_sid"))); !ok {
		t.Fatal("Run1 应中断")
	}

	// Resume 1：import overlay 里有未注册的 channel → setBaseline 拒 → 在 updateFrontier
	// 之前 early return
	err = Run(ctx, g, NewChannelManager(),
		WithCheckpointer(cper), WithSessionID("rt_sid"),
		WithResumeAnswers(ans),
		withImportOverlay(Write(strayCh, 1)))
	if err == nil {
		t.Fatal("Resume1 应因 import overlay 含未注册 channel 失败")
	}

	// 核心断言：失败的 Resume **不能**降级已存恢复点
	cp2, _ := cper.LoadNS(ctx, "rt_sid", "")
	if cp2 == nil {
		t.Fatal("恢复点被整个删了")
	}
	if len(cp2.CurrentFrontier) != 1 || cp2.CurrentFrontier[0].NodeID != "A" {
		t.Errorf("失败的 Resume 把恢复点降级了：CurrentFrontier=%v（want [A]）\n"+
			"  Restore 不填 frontierAtExit → 该字段在「Restore 之后、updateFrontier 之前」是零值\n"+
			"  → early return 时 Run-defer 的 Snapshot 拿空 frontier 覆盖原 cp",
			cp2.CurrentFrontier)
	}
	if len(cp2.PendingInterrupts) != 1 {
		t.Errorf("HITL 待答 query 丢了：PendingInterrupts=%v", cp2.PendingInterrupts)
	}

	// 后果验证：去掉那条坏 overlay 后必须能真正续跑（而非静默成功）
	aRuns.Store(0)
	if err := Run(ctx, g, NewChannelManager(),
		WithCheckpointer(cper), WithSessionID("rt_sid"),
		WithResumeAnswers(ans)); err != nil {
		t.Fatalf("修好 reducer 后 Resume 应成功, got %v", err)
	}
	if aRuns.Load() != 1 {
		t.Errorf("A 应跑 1 次，实际 %d 次\n"+
			"  0 次 = 恢复点被空 frontier 覆盖后，Resume 撞「上次已跑完」分支**静默成功**——"+
			"不报错、结果错，用户的 HITL 问题永久丢失", aRuns.Load())
	}
}
