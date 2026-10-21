package compose_v2_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestRepro_R8_Bug1_RegisterValueTypeAliasNoCollision 验证 audit Round 8 Bug 1：
// schema.Message 是 common.Message 的 type alias；agent_v2 init 用
// "[]*common.Message" 注册同 type；业务 example 用 "[]*schema.Message" 注册——
// 旧实现同 type 不同 name 时 panic，新实现允许多名 alias 不 panic。
//
// **预期行为**：同 type 注册多个 name 不 panic，所有 name 都能 marshal/unmarshal。
//
// **当前 bug**（修前）：第二次注册不同 name panic init。
type aliasTypeT struct{ X int }

func TestRepro_R8_Bug1_RegisterValueTypeAliasNoCollision(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Bug 1 reproduced: 同 type 不同 name 二次注册 panic\n"+
				"修复方向：RegisterValueType 同 type 不同 name 时按 alias 不 panic\n"+
				"实际 panic: %v", r)
		}
	}()

	// 第一次注册
	compose_v2.RegisterValueType[*aliasTypeT]("compose_v2_test.aliasTypeT_canonical")
	// 第二次同 type 不同 name——应静默 alias，不 panic
	compose_v2.RegisterValueType[*aliasTypeT]("compose_v2_test.aliasTypeT_alias")
	// 第三次同 name 同 type——幂等，不 panic
	compose_v2.RegisterValueType[*aliasTypeT]("compose_v2_test.aliasTypeT_canonical")
}

// TestRepro_R8_Bug2_RestoreRecoversPendingInterrupts 验证 audit Round 8 Bug 2：
// cm.Restore 不恢复 cp.PendingInterrupts → 跨进程 Restore 后 cm.PendingInterrupts()
// API 与 cp.PendingInterrupts 字段不一致。
//
// **预期行为**：Restore 把 cp.PendingInterrupts 写回 cm.pendingInterrupts，
// cm.PendingInterrupts() 返同样数量。
//
// **当前 bug**（修前）：cm.PendingInterrupts() 返空。
func TestRepro_R8_Bug2_RestoreRecoversPendingInterrupts(t *testing.T) {
	// 构造 cp 含 2 条 PendingInterrupts
	cp := &compose_v2.Checkpoint{
		SchemaVer: 6,
		PendingInterrupts: []*compose_v2.AskQuery{
			{Question: "q1", InterruptID: "id1"},
			{Question: "q2", InterruptID: "id2", Options: []string{"yes", "no"}},
		},
	}

	// 模拟跨进程：marshal → unmarshal
	data, err := compose_v2.MarshalCheckpoint(cp)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	cp2, err := compose_v2.UnmarshalCheckpoint(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(cp2.PendingInterrupts) != 2 {
		t.Fatalf("cp wire round-trip lost: got %d", len(cp2.PendingInterrupts))
	}

	// 新 cm Restore
	cm := compose_v2.NewChannelManager()
	if err := cm.Restore(cp2); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	pending := cm.PendingInterrupts()
	if len(pending) != 2 {
		t.Fatalf("Bug 2 reproduced: cm.Restore 不恢复 PendingInterrupts\n"+
			"cp 含 2 条 PendingInterrupts，Restore 后 cm.PendingInterrupts() 返 %d 条\n"+
			"修复方向：Restore 把 cp.PendingInterrupts 深拷贝到 cm.pendingInterrupts",
			len(pending))
	}
	// 验证字段完整
	if pending[0].InterruptID != "id1" || pending[1].InterruptID != "id2" {
		t.Fatalf("PendingInterrupts 字段错位: %+v", pending)
	}
	if len(pending[1].Options) != 2 {
		t.Fatalf("AskQuery.Options 丢失: %+v", pending[1].Options)
	}
}

// TestRepro_R8_Bug3_EmptyInterruptIDRejected 空 InterruptID 必须被**拒绝**。
//
// # 这条的立意翻过一次，值得说清
//
// 它原名 `..._AutoFillEmptyInterruptID`，钉的是 audit Round 8 Bug 3 的修复：业务不填 ID 时
// 框架**自动补**一个。那个兜底后来被实测拆掉了——它读的是**节点级 ctx**，看不见节点内部的
// 分支结构（分支下标那一维恒 0、序号取合并列表里的位置），而分支自己算的是「本分支计数器
// 的下一个」。两套坐标只在「一次性批量抛、抛完不回查」时碰巧对上，其余形态：
//
//	单工具连问两次        12 轮不收敛，副作用重放 12 次（随轮数线性增长）
//	单工具单问 + 开审批   永不收敛
//	两工具各问一次        第二个永远执行不了
//
// 而且它会把**新问题盖上别人已经答过的 ID**——用户看到的问题与 payload 里的 ID 指向两件
// 不同的事。R8 当年要解决的「多 pending 路由失败」，被这个兜底换了个更隐蔽的形态。
//
// ⚠️ **不能只是「不补」**：空 ID 的挂起彼此不可区分，多条并存时 `GetResumeAnswer(ctx, "")`
// 只拿得到第一条——调用方看到 N 个问题、却只有一个地址能投递答案。所以是**拒绝**：
// `validateInterrupt` 在落档之前就拒掉，空 ID 根本进不了挂起列表。
//
// R8 的原始诉求（多 pending 必须能各自路由）现在由「**每条挂起都有非空身份**」这条不变量
// 直接保证，而它由本用例钉着。
func TestRepro_R8_Bug3_EmptyInterruptIDRejected(t *testing.T) {
	throwNode := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{Question: "what is your name?"}, // 故意不填 ID
		}
	}
	b := compose_v2.NewBuilder()
	b.AddNode("ask", throwNode)
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	runErr := compose_v2.Run(context.Background(), g, cm)

	// 承重①：必须报错，而且**不是**中断——空 ID 的中断没人能答，放它过去等于制造一条死路
	if runErr == nil {
		t.Fatal("空 InterruptID 应被拒绝，实际 err=nil")
	}
	if _, isInt := compose_v2.IsInterrupt(runErr); isInt {
		t.Fatalf("拒绝不该是中断（没人能答它）：%v", runErr)
	}
	// 承重②：错误信息要**给得出出路**——业务读到它得知道下一步做什么
	if !strings.Contains(runErr.Error(), "compose_v2.Ask") {
		t.Fatalf("错误信息应指向 compose_v2.Ask，实际：%v", runErr)
	}
	// 承重③：**没有半条空 ID 的挂起落进记录**——那正是「不可区分」的来源
	for _, q := range cm.PendingInterrupts() {
		if q.InterruptID == "" {
			t.Fatalf("空 ID 的挂起被记录了：%+v", q)
		}
	}
}

// TestRepro_R8_Bug4_WithResumeFromGraphIDGuard 验证 audit Round 8 Bug 4：
// WithResumeFrom 显式路径不做 GraphID 冲突守卫 → 业务 mix 错版本 graph 时
// silent corruption（auto-load 路径有守卫，WithResumeFrom 路径没有）。
//
// **预期行为**：cp.GraphID 与当前 g.ID() 不匹配时返 ErrGraphIDConflict。
//
// **当前 bug**（修前）：silent restore 让 frontier/channel 错位。
func TestRepro_R8_Bug4_WithResumeFromGraphIDGuard(t *testing.T) {
	// 构造两个不同身份的 graph
	noopFn := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	}
	build := func(name string) *compose_v2.Graph {
		b := compose_v2.NewBuilder()
		b.WithName(name)
		b.AddNode("x", noopFn)
		b.AddEdge(compose_v2.StartNode, "x")
		b.AddEdge("x", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatalf("compile %s: %v", name, err)
		}
		return g
	}
	g1 := build("g1")
	g2 := build("g2")

	// 构造一个属于 g1 的 cp（含 GraphID）
	cp := &compose_v2.Checkpoint{
		SchemaVer:       6,
		GraphID:         g1.ID(),
		Status:          compose_v2.CPStatusPending,
		CurrentFrontier: []compose_v2.FrontierMember{{NodeID: "x"}},
	}

	// 用 g2 + WithResumeFrom(cp) 应 fail-fast 报 ErrGraphIDConflict
	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g2, cm, compose_v2.WithResumeFrom(cp))
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) {
		t.Fatalf("Bug 4 reproduced: WithResumeFrom 不做 GraphID 冲突校验\n"+
			"期望 ErrGraphIDConflict，实际 err=%v\n"+
			"修复方向：WithResumeFrom 路径加 cp.GraphID != g.ID() 守卫", err)
	}
}

// TestRepro_R8_Bug5_SnapshotForGraphAutoFillsMetadata 验证 audit Round 8 Bug 5：
// 业务用裸 cm.Snapshot 时 cp 缺 GraphID/Status，跨进程接收方无法守卫；新
// SnapshotForGraph 自动填两字段。
//
// **预期行为**：SnapshotForGraph(g) 产 cp.GraphID == g.ID() + cp.Status == Pending。
func TestRepro_R8_Bug5_SnapshotForGraphAutoFillsMetadata(t *testing.T) {
	noopFn := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	}
	b := compose_v2.NewBuilder()
	b.WithName("g_bug5")
	b.AddNode("x", noopFn)
	b.AddEdge(compose_v2.StartNode, "x")
	b.AddEdge("x", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	cp, err := cm.SnapshotForGraph(g)
	if err != nil {
		t.Fatalf("SnapshotForGraph: %v", err)
	}
	if cp.GraphID == "" || cp.GraphID != g.ID() {
		t.Fatalf("Bug 5 reproduced: SnapshotForGraph 不填 GraphID\n"+
			"期望 cp.GraphID = %q，实际 %q\n"+
			"修复方向：SnapshotForGraph 在 Snapshot 后填 cp.GraphID = g.ID()",
			g.ID(), cp.GraphID)
	}
	// Status **按内容算**：这个 cm 从没跑过任何东西——没有 frontier、没有挂起提问，
	// 也就没有任何可执行的东西，标成「挂起等 Resume」是假的。
	//
	// **为什么这条重要**：档位是这份快照对自己的描述。把一份「没东西可跑」的快照标成
	// Pending，它就会混进 `ListPending` 的「有人在等答案」清单，而对端真去续跑时只会拿到
	// 「存档在但没有可续的内容」——2026-08-13 起续跑闸门按内容判（`HasResumableWork`）、不读 Status。
	if cp.Status != compose_v2.CPStatusUnset {
		t.Fatalf("空 cm 的快照没有任何可执行的东西，Status 该留 Unset，实际 %s", cp.Status)
	}

	// 端到端：跨进程 wire-push 后 WithResumeFrom 接收方能用 GraphID 守卫
	data, err := compose_v2.MarshalCheckpoint(cp)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	cp2, err := compose_v2.UnmarshalCheckpoint(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if cp2.GraphID != g.ID() {
		t.Fatalf("wire round-trip 丢 GraphID: %q", cp2.GraphID)
	}
}

// TestSnapshotForGraph_StatusFromContent 守「Status 按内容算」的**两面**——只验空 cm
// 留 Unset 是不够的：把判据写死成「永远不盖」同样能让那半绿，而真中断的快照必须是 Pending，
// 否则跨进程回灌的存档在对端的挂起清单里查不到，HITL 的「谁在等答案」就断了。
//
// sanity-revert：把 SnapshotForGraph 里的 `if len(...)>0` 改回无条件 `cp.Status =
// CPStatusPending` → 「跑完」那条红；改成永不盖 → 「中断中」那条红。
func TestSnapshotForGraph_StatusFromContent(t *testing.T) {
	b := compose_v2.NewBuilder().WithName("g_status")
	b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, ok := compose_v2.GetResumeAnswer(ctx, "q"); !ok {
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "q", Question: "批准吗"}}
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// ① 中断中的 cm → 有 frontier + 有挂起提问 → Pending
	cm := compose_v2.NewChannelManager()
	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(context.Background(), g, cm)); !ok {
		t.Fatal("fixture 没搭对：这一跑该中断")
	}
	cp, err := cm.SnapshotForGraph(g)
	if err != nil {
		t.Fatalf("SnapshotForGraph: %v", err)
	}
	if cp.Status != compose_v2.CPStatusPending {
		t.Errorf("中断中的快照该是 Pending（否则对端的挂起清单里查不到它，HITL 断），实际 %s：frontier=%d pending=%d",
			cp.Status, len(cp.CurrentFrontier), len(cp.PendingInterrupts))
	}

	// ② 跑完的 cm → 什么都不剩 → 不盖戳
	cm2 := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm2,
		compose_v2.WithResumeAnswers(map[string]*schema.Message{"q": schema.UserMessage("yes")})); err != nil {
		t.Fatalf("答完这一跑该成功: %v", err)
	}
	cp2, err := cm2.SnapshotForGraph(g)
	if err != nil {
		t.Fatalf("SnapshotForGraph: %v", err)
	}
	if cp2.Status != compose_v2.CPStatusUnset {
		t.Errorf("跑完的快照没有任何可执行的东西，Status 该留 Unset，实际 %s：frontier=%d pending=%d",
			cp2.Status, len(cp2.CurrentFrontier), len(cp2.PendingInterrupts))
	}
}
