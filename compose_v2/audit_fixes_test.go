package compose_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// Phase 5.1.a Step 4 audit Round 1 fixes 回归测试。

// TestF2_SnapErrNotSilent 验证 F2 修复：subCM.Snapshot 失败时**不静默吞**子图
// 中断 cp 持久化——typical 触发是 channel value 类型未注册。
//
// 设计：两节点子图——node1 成功写入未注册类型 channel；node2 抛 ask_user 中断。
// node1 的 writes 已 ApplyAll 进 cm.values（含未注册类型）→ cm.Snapshot 调
// marshalValue 报 "not registered"。F2 修复确保此 err 不被静默吞。
//
// sanity-revert：把 subgraph.go 中 `if snapErr != nil { return ... }` 改回
// silent 跳过（旧版逻辑），本测试应 fail 并精确指向 cp 未持久化。
func TestF2_SnapErrNotSilent(t *testing.T) {
	// 用一个 channel 业务**没有** RegisterValueType 的 type
	type unregisteredCustomType struct {
		Foo int
	}
	customCh := compose_v2.LastValue[unregisteredCustomType]("audit_f2_unreg")

	subB := compose_v2.NewBuilder().
		AddNode("write_unreg", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			// node1：成功写未注册类型——此 write 被 ApplyAll 进 cm.values
			return []compose_v2.ChannelWrite{compose_v2.Write(customCh, unregisteredCustomType{Foo: 42})}, nil
		}).
		AddNode("ask_then_fail", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			// node2：抛 ask_user → 触发 cm.Snapshot 时 cm.values 含未注册类型
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "f2_ask"); ok {
				_ = ans
				return nil, nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "f2_ask", Question: "?"},
			}
		}).
		AddEdge(compose_v2.StartNode, "write_unreg").
		AddEdge("write_unreg", "ask_then_fail").
		AddEdge("ask_then_fail", compose_v2.EndNode)
	subB = compose_v2.WithChannel(subB, customCh)
	sub, _ := subB.Compile()

	parentB := compose_v2.NewBuilder().
		AddSubgraphNode("sub_node", sub).
		AddEdge(compose_v2.StartNode, "sub_node").
		AddEdge("sub_node", compose_v2.EndNode)
	parent, _ := parentB.Compile()

	cper := compose_v2.NewInMemoryCheckpointer()
	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), parent, cm,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("f2_sid"),
	)
	if err == nil {
		t.Fatal("应失败")
	}

	// **R27a audit LOW-1 修订**：subgraph.go 删除了冗余 SaveNS（双 SaveNS 改单
	// 由 graph_loop defer 独家负责），所以 snapErr wrap 现在走 graph_loop.go:243-249
	// 的 "root cp snapshot failed" 路径而非旧 subgraph.go:202 "snapshot for cp save failed"。
	// snapErr 不静默的 F2 invariant 不变（graph_loop wrap 同样保 snapErr 进 returned err）。
	//
	// 改用 graph_loop 专属字符串 "root cp snapshot failed"，sanity-revert 路径：
	//   - revert R27a LOW-1 fix（恢复 subgraph.go SaveNS 块）→ 测试应通过（subgraph
	//     wrap 包含 "root cp snapshot failed" 子串吗？不——subgraph wrap 是 "snapshot for
	//     cp save failed"，所以 revert 后测试 fail，精确指向"双 SaveNS 改单"
	//     调整未同步测试这一缺口）
	//   - revert F2 fix（让 graph_loop snapErr 改回 silent）→ 测试 fail，因为
	//     "root cp snapshot failed" 不在 err msg 中。
	msg := err.Error()
	if !strings.Contains(msg, "root cp snapshot failed") {
		t.Fatalf("F2 修复后 err 应含 'root cp snapshot failed'（graph_loop.go:246 专属 wrap），实际 %q\n"+
			"sanity-revert（删 graph_loop.go snapErr wrap）→ 本测试 fail 应精确指向 F2 缺失",
			msg)
	}
	// 快照不出来就写不了档：跟写档失败同样处置，不再报告成中断（见 ErrCheckpointNotPersisted）。
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Fatalf("快照失败、档没写成，不该再报告成中断：%v", err)
	}
	if !errors.Is(err, compose_v2.ErrCheckpointNotPersisted) || !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("应返 ErrCheckpointNotPersisted + ErrRunAborted：%v", err)
	}
}

// TestF5_ResumeImportSyncsParentCm 验证 F5 修复：Resume 路径**也**从父 cm 读

// TestF4_RootCPAutoSaveLoadCleanup 验证 F4 修复：根 graph cp 路径与子图 cp 路径
// 对称——caller 注入 WithCheckpointer+WithSessionID 后框架自动 SaveNS（中断时）
// + LoadNS（启动时尝试 Resume）+ DeleteSubtree（成功时）。
//
// sanity-revert：把 graph_loop.go defer 块（auto SaveNS / DeleteSubtree） + 入
// 口 auto LoadNS 块都改回旧版（业务自管 cm.Snapshot），本测试应 fail——
//   - Run 1 中断后 LoadNS(sid, "") 返 nil（旧版没 SaveNS）
//   - Run 2 fresh 跑（旧版没 LoadNS）→ Resume 不工作
func TestF4_RootCPAutoSaveLoadCleanup(t *testing.T) {
	answerCh := compose_v2.LastValue[string]("f4_answer")

	var runCount atomic.Int32

	makeGraph := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder().
			AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				runCount.Add(1)
				if ans, ok := compose_v2.GetResumeAnswerText(ctx, "f4_ask"); ok {
					return []compose_v2.ChannelWrite{compose_v2.Write(answerCh, "got:"+ans)}, nil
				}
				return nil, &compose_v2.InterruptError{
					Query: &compose_v2.AskQuery{InterruptID: "f4_ask", Question: "?"},
				}
			}).
			AddEdge(compose_v2.StartNode, "ask").
			AddEdge("ask", compose_v2.EndNode)
		b = compose_v2.WithChannel(b, answerCh)
		g, _ := b.Compile()
		return g
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	sid := "f4_sid"

	// === Run 1: 触发中断 → F4 应自动 SaveNS(sid, "") ===
	cm1 := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), makeGraph(), cm1,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sid),
	)
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("Run 1 应中断，实际 %v", err)
	}
	if runCount.Load() != 1 {
		t.Fatalf("Run 1 ask 应跑 1 次，实际 %d", runCount.Load())
	}

	// **关键不变量 F4-Save**：根 cp 应已自动写到 (sid, "")
	saved, loadErr := cper.LoadNS(context.Background(), sid, "")
	if loadErr != nil {
		t.Fatalf("LoadNS 后中断: %v", loadErr)
	}
	if saved == nil {
		t.Fatalf("F4 修复后根 cp 应自动 SaveNS(sid, '')，实际 nil\n" +
			"sanity-revert：graph_loop.go defer 块的 auto SaveNS 改回旧版业务自管，本断言 fail")
	}
	if saved.Status != compose_v2.CPStatusPending {
		t.Fatalf("根 cp Status 应 Pending，实际 %v", saved.Status)
	}
	if len(saved.PendingInterrupts) == 0 {
		t.Fatalf("根 cp 应含 PendingInterrupts，实际空")
	}

	// === Run 2: 同 sid 重跑 → F4 应自动 LoadNS 恢复，Resume 跑完 ===
	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(context.Background(), makeGraph(), cm2,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{"f4_ask": schema.UserMessage("yes")}),
	)
	if err != nil {
		t.Fatalf("Run 2 应成功（F4 自动 LoadNS Resume），实际 %v\n"+
			"sanity-revert：graph_loop.go 入口 auto-load 改回旧版，Run 2 fresh 跑会再中断，本断言 fail",
			err)
	}

	// **关键不变量 F4-Load**：Resume 路径应读到答案 → answerCh = "got:yes"
	gotAns, _ := compose_v2.Get(cm2.Channels(), answerCh)
	if gotAns != "got:yes" {
		t.Fatalf("Resume 应读到答案 'got:yes'，实际 %q", gotAns)
	}

	// **关键不变量 F4-Cleanup**：成功后 DeleteSubtree(sid, "") 清根 cp
	afterClean, _ := cper.LoadNS(context.Background(), sid, "")
	if afterClean != nil {
		t.Fatalf("F4 修复后 Run 成功应 DeleteSubtree 清根 cp，实际仍存 %+v\n"+
			"sanity-revert：graph_loop.go defer 块成功路径 DeleteSubtree 改回旧版，本断言 fail",
			afterClean)
	}
}

// === Phase 5.1.a Step 5 audit Round 2 ===
//
// audit Round 2 由独立 agent 报告 3 真 bug：
//   - Bug A：DeleteSubtree(sid, "") 误删同 sid 下其他 cp（**确认是 design intent
//     不改行为**——sid lifetime = 根 graph Run lifetime；godoc 强调 + 测试固化语义）
//   - Bug B：caller 想用同 sid 跑不同 graph 时无逃生口（修：加 WithFreshRun opt）
//   - Bug C：DeleteSubtree silent error swallow（修：slog.Warn 兑现注释承诺）
//
// 本节加 Round 2 回归测试。

// TestAuditR2_WithFreshRunSkipsAutoLoad 验证 Bug B 修复：caller 传 WithFreshRun
// 后 F4 不自动加载 saved cp——业务用同 sid 跑**不同 graph**（拓扑不一样）不踩
// "陈旧 frontier 注入当前 graph 触发 node not registered"陷阱。
//
// **测试设计**：Run 1 跑 graph A（含节点 nodeA）触发 nodeA 中断 → cp 存
// frontier=[nodeA]。Run 2 用同 sid 跑 graph B（含节点 nodeB，**没 nodeA**）：
//   - 不传 WithFreshRun：auto-load 把 frontier=[nodeA] 注入 graph B → executeFrontier
//     找不到 nodeA → "node 'nodeA' not registered" 错
//   - 传 WithFreshRun：跳过 auto-load → fresh 跑 nodeB → 成功
//
// sanity-revert：把 graph_loop.go 入口 auto-load 块的 `!cfg.freshRun` 条件移除，
// 本测试 Run 2 应 fail with "not registered" 错。
func TestAuditR2_WithFreshRunSkipsAutoLoad(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	sid := "r2_sid"

	// === Run 1: graph A 含 nodeA 触发中断 ===
	graphA, _ := compose_v2.NewBuilder().
		AddNode("nodeA", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "ask_A", Question: "?"},
			}
		}).
		AddEdge(compose_v2.StartNode, "nodeA").
		AddEdge("nodeA", compose_v2.EndNode).
		Compile()
	err := compose_v2.Run(context.Background(), graphA, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sid),
	)
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("Run 1 应中断，实际 %v", err)
	}

	// 确认 cp 存了 frontier=[nodeA]
	saved, _ := cper.LoadNS(context.Background(), sid, "")
	if saved == nil || len(saved.CurrentFrontier) != 1 || saved.CurrentFrontier[0].NodeID != "nodeA" {
		t.Fatalf("Run 1 后 cp.frontier 应是 [nodeA]，实际 %+v", saved)
	}

	// === Run 2: graph B 含 nodeB（**没 nodeA**） + WithFreshRun → 应跑 fresh 成功 ===
	graphB, _ := compose_v2.NewBuilder().
		AddNode("nodeB", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil // 成功完成
		}).
		AddEdge(compose_v2.StartNode, "nodeB").
		AddEdge("nodeB", compose_v2.EndNode).
		Compile()
	err = compose_v2.Run(context.Background(), graphB, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sid),
		compose_v2.WithFreshRun(), // ← 关键：跳过 auto-load
	)

	// **关键不变量 R2-Fresh**：WithFreshRun 跳过 auto-load → graph B 跑 nodeB 成功
	if err != nil {
		t.Fatalf("WithFreshRun 应跳过 auto-load 让 graph B fresh 跑成功，实际 err=%v\n"+
			"sanity-revert：graph_loop.go 入口 auto-load 条件去掉 `!cfg.freshRun`，本断言 fail with"+
			" 'node \"nodeA\" not registered'（auto-load 把 saved frontier=[nodeA] 注入 graph B 但 graph B 没 nodeA）",
			err)
	}
}

// === Phase 5.1.b ISSUE-50 P0 GraphID 防串台守卫测试 ===

// TestPhase51b_GraphID_NameOverride 验证 Graph.ID() 优先用 WithName 设置的 name。
func TestPhase51b_GraphID_NameOverride(t *testing.T) {
	g, _ := compose_v2.NewBuilder().
		AddNode("node1", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "node1").
		AddEdge("node1", compose_v2.EndNode).
		WithName("my_graph").
		Compile()

	if got := g.ID(); got != "my_graph" {
		t.Fatalf("WithName 设的 name 应优先，实际 %q", got)
	}
}

// TestPhase51b_GraphID_FingerprintFallback 验证 Graph.ID() 无 WithName 时按
// fingerprint 兜底——拓扑相同 ID 相同；nodeID 改一个 / channel schema 改 → ID 变
func TestPhase51b_GraphID_FingerprintFallback(t *testing.T) {
	makeNoop := func() compose_v2.NodeFunc {
		return func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}
	}
	makeGraph := func(node1ID compose_v2.NodeID, withCh bool) *compose_v2.Graph {
		b := compose_v2.NewBuilder().
			AddNode(node1ID, makeNoop()).
			AddEdge(compose_v2.StartNode, node1ID).
			AddEdge(node1ID, compose_v2.EndNode)
		if withCh {
			ch := compose_v2.LastValue[string]("audit_b1_ch")
			b = compose_v2.WithChannel(b, ch)
		}
		g, _ := b.Compile()
		return g
	}

	// 同样拓扑 → 同 fingerprint
	g1a := makeGraph("node_x", false)
	g1b := makeGraph("node_x", false)
	if g1a.ID() != g1b.ID() {
		t.Fatalf("拓扑相同应 ID 相同，g1a=%q g1b=%q", g1a.ID(), g1b.ID())
	}

	// nodeID 变 → fingerprint 变
	g2 := makeGraph("node_y", false)
	if g1a.ID() == g2.ID() {
		t.Fatalf("nodeID 不同应 ID 不同，但都是 %q", g1a.ID())
	}

	// channel schema 变 → fingerprint 变
	g3 := makeGraph("node_x", true)
	if g1a.ID() == g3.ID() {
		t.Fatalf("channel schema 不同应 ID 不同，但都是 %q", g1a.ID())
	}

	// fingerprint 是 16 hex chars
	if len(g1a.ID()) != 16 {
		t.Fatalf("fingerprint 应是 16 hex chars，实际 len=%d %q", len(g1a.ID()), g1a.ID())
	}
}

// TestPhase51b_GraphID_GuardOnAutoLoad 验证 F4 auto-load 加 GraphID 守卫——
// caller 用同 sid 跑两个不同 graph 时，第二个 graph 加载到第一个 graph 的 cp
// 应 fail-fast with ErrGraphIDConflict（除非显式 WithFreshRun）。
//
// sanity-revert：把 graph_loop.go auto-load 块的 GraphID 校验去掉，本测试应
// fail——Run 2 不再报 conflict 而是被强行 Resume 然后 "node X not registered"。
func TestPhase51b_GraphID_GuardOnAutoLoad(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	sid := "b1_sid"

	// Run 1: graph A（WithName "g_A"）触发中断
	graphA, _ := compose_v2.NewBuilder().
		AddNode("nodeA", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "ask_A", Question: "?"},
			}
		}).
		AddEdge(compose_v2.StartNode, "nodeA").
		AddEdge("nodeA", compose_v2.EndNode).
		WithName("g_A").
		Compile()
	err := compose_v2.Run(context.Background(), graphA, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sid),
	)
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("Run 1 应中断，实际 %v", err)
	}

	// 验证 cp 含 GraphID
	saved, _ := cper.LoadNS(context.Background(), sid, "")
	if saved == nil {
		t.Fatalf("Run 1 后应有 saved cp")
	}
	if saved.GraphID != "g_A" {
		t.Fatalf("saved cp.GraphID 应是 'g_A'，实际 %q", saved.GraphID)
	}

	// Run 2: graph B（WithName "g_B"）同 sid → 应 fail-fast ErrGraphIDConflict
	graphB, _ := compose_v2.NewBuilder().
		AddNode("nodeB", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "nodeB").
		AddEdge("nodeB", compose_v2.EndNode).
		WithName("g_B").
		Compile()
	err = compose_v2.Run(context.Background(), graphB, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sid),
	)
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) {
		t.Fatalf("Run 2 不同 graph 同 sid 应 ErrGraphIDConflict（fail-fast），实际 %v\n"+
			"sanity-revert：graph_loop.go auto-load 块去掉 GraphID 校验，本断言 fail with 'node \"nodeA\" not registered'",
			err)
	}

	// Run 2 重试: 加 WithFreshRun 应 bypass 守卫，跑成功
	err = compose_v2.Run(context.Background(), graphB, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sid),
		compose_v2.WithFreshRun(),
	)
	if err != nil {
		t.Fatalf("WithFreshRun 应 bypass GraphID 守卫，实际 %v", err)
	}
}

// TestPhase51b_GraphID_GuardOnSubgraph 验证子图 Resume 路径也加 GraphID 守卫——
// 业务用同 (sid, childNS) 跑两个不同子 graph 应 fail-fast。
func TestPhase51b_GraphID_GuardOnSubgraph(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	sid := "b1_sub_sid"

	// 先手动写一个"假"的子图 cp，GraphID 是 "fake_sub_id"
	fakeCp := &compose_v2.Checkpoint{
		Status:          compose_v2.CPStatusPending,
		GraphID:         "fake_sub_id",
		CurrentFrontier: []compose_v2.FrontierMember{{NodeID: "nodeFake"}},
	}
	if err := cper.SaveNS(context.Background(), sid, "sub_node", fakeCp); err != nil {
		t.Fatalf("SaveNS fake: %v", err)
	}

	// 真正的子图 with WithName "real_sub_id"
	realSub, _ := compose_v2.NewBuilder().
		AddNode("nodeReal", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "nodeReal").
		AddEdge("nodeReal", compose_v2.EndNode).
		WithName("real_sub_id").
		Compile()

	// 父图嵌入 realSub
	parent, _ := compose_v2.NewBuilder().
		AddSubgraphNode("sub_node", realSub).
		AddEdge(compose_v2.StartNode, "sub_node").
		AddEdge("sub_node", compose_v2.EndNode).
		WithName("parent_g").
		Compile()

	// Run → 子图 LoadNS 拿到 fake_sub_id 的 cp，与 real_sub_id 不匹配 → fail-fast
	err := compose_v2.Run(context.Background(), parent, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sid),
		// **不能用 WithFreshRun 来隔离父层**——它现在会往下传（子图跟着跳过 auto-load），
		// 那样子图根本载不到那份假 cp、守卫无从触发。父图自己这一槽本来就没 cp，不隔离也不会踩。
	)
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) {
		t.Fatalf("子图 GraphID 不匹配应 ErrGraphIDConflict，实际 %v", err)
	}
}

// TestAuditR2_BugA_SidLifetime_DocumentedSemantic 固化 Bug A 的设计语义：
// 根 graph Run 成功后清整 sid 命名空间——业务在同 sid 下保存的无关 cp 也会
// 被一并清掉。**这是设计 intent 不是 bug**：sid lifetime = Run lifetime，
// 业务跨 Run 持久化用不同 sid。
//
// 本测试**固化此语义**——若未来改成"只清本 graph 子树不碰业务无关 ns"会
// fail，提醒决策者先看 godoc + 三方对照文档再改。
func TestAuditR2_BugA_SidLifetime_DocumentedSemantic(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	sid := "shared_sid"

	// 业务自存一个完全无关的 cp（不在任何 graph tree 内）
	bizCp := &compose_v2.Checkpoint{
		Status:  compose_v2.CPStatusPending,
		GraphID: "biz_unrelated",
	}
	if err := cper.SaveNS(context.Background(), sid, "biz_data", bizCp); err != nil {
		t.Fatalf("SaveNS biz: %v", err)
	}

	// 跑根 graph 成功（极简：StartNode → noop → EndNode）
	g, err := compose_v2.NewBuilder().
		AddNode("noop", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "noop").
		AddEdge("noop", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sid),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// **关键不变量 Bug A 语义**：业务无关 cp 也被一并清掉（sid lifetime = Run lifetime）
	got, _ := cper.LoadNS(context.Background(), sid, "biz_data")
	if got != nil {
		t.Fatalf("Bug A 设计语义：sid lifetime = Run lifetime，业务无关 cp 应被一并清掉，实际仍存 %+v\n"+
			"若改了 cleanup 行为（如改为只清本 graph 子树），先看 WithSessionID godoc + Phase 5.1.b GraphID 计划再决策",
			got)
	}
}
