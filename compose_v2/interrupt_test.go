package compose_v2_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/compose_v2"

	"crew-go/schema"
)

// Phase 1.0.c 端到端测试：HITL InterruptError + ask_user + Resume with answer。
//
// 撞 audit F8 [MED] 的简化版本：暂不接 OrchHooks（hook 集成留 Phase 1.1），
// 但验证 channel-based 模型的 HITL 协议核心闭环——节点抛中断 + cp save +
// caller 提供 answer + Resume 注入 → 节点拿到 answer 继续。

// TestInterrupt_AskUser_ResumeWithAnswer 是 Phase 1.0.c 旗舰测试：
//
// 场景：approval workflow
//  1. validator 节点检查输入，需要 user 确认 → 抛 InterruptError(ask_1)
//  2. Run 1 返 InterruptError，cm 自动记录 pending interrupt + frontier
//  3. caller 拿到 cm.PendingInterrupts() → 向 user 询问 → 拿到 "yes"
//  4. Snapshot + 模拟跨进程
//  5. Restore + Run 2 with WithResumeAnswers(ask_1: "yes")
//  6. validator 重跑时 GetResumeAnswer 拿到 "yes" → 不再抛中断 → 写 approved
//  7. 后续节点正常推进到 End
func TestInterrupt_AskUser_ResumeWithAnswer(t *testing.T) {
	approvedCh := compose_v2.LastValue[string]("approved")

	makeGraph := func() *compose_v2.Graph {
		validator := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			// 检查是否已有 user answer
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "ask_approval"); ok {
				if ans == "yes" {
					return []compose_v2.ChannelWrite{
						compose_v2.Write(approvedCh, "ok"),
					}, nil
				}
				return []compose_v2.ChannelWrite{
					compose_v2.Write(approvedCh, "rejected:"+ans),
				}, nil
			}
			// 首次执行：抛中断等 user
			return nil, &compose_v2.InterruptError{
				Reason: "需要 user 批准",
				Query: &compose_v2.AskQuery{
					InterruptID: "ask_approval",
					Question:    "批准本次操作？",
					Options:     []string{"yes", "no"},
				},
			}
		}

		b := compose_v2.NewBuilder().
			AddNode("validator", validator).
			AddEdge(compose_v2.StartNode, "validator").
			AddEdge("validator", compose_v2.EndNode)
		b = compose_v2.WithChannel(b, approvedCh)
		g, err := b.Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		return g
	}

	// === Run 1：触发 ask_user 中断 ===
	g1 := makeGraph()
	cm1 := compose_v2.NewChannelManager()

	runErr := compose_v2.Run(context.Background(), g1, cm1)
	if runErr == nil {
		t.Fatal("Run 1 应被 InterruptError 中断")
	}
	ie, isInt := compose_v2.IsInterrupt(runErr)
	if !isInt {
		t.Fatalf("应是 InterruptError，实际 %v", runErr)
	}
	if ie.Query == nil || ie.Query.InterruptID != "ask_approval" {
		t.Fatalf("Query 应是 ask_approval，实际 %+v", ie.Query)
	}
	t.Logf("Run 1 触发 ask_user：%q (id=%s, options=%v)",
		ie.Query.Question, ie.Query.InterruptID, ie.Query.Options)

	// 验证 cm 自动记录了 pending interrupt
	pending := cm1.PendingInterrupts()
	if len(pending) != 1 || pending[0].InterruptID != "ask_approval" {
		t.Fatalf("cm 应记录 1 个 ask_approval pending，实际 %+v", pending)
	}

	// === Snapshot + 序列化 ===
	snap1, err := cm1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap1.PendingInterrupts) != 1 {
		t.Fatalf("snapshot 应含 1 个 pending interrupt，实际 %d", len(snap1.PendingInterrupts))
	}
	if len(snap1.CurrentFrontier) != 1 || snap1.CurrentFrontier[0].NodeID != "validator" {
		t.Fatalf("snapshot frontier 应是 [validator]（重跑），实际 %v", snap1.CurrentFrontier)
	}

	data, err := compose_v2.MarshalCheckpoint(snap1)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// === caller 向 user 询问后拿到 "yes"——模拟跨进程 ===
	snap2, err := compose_v2.UnmarshalCheckpoint(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	g2 := makeGraph()
	cm2 := compose_v2.NewChannelManager()
	if err := compose_v2.RegisterChannel(cm2, approvedCh); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := cm2.Restore(snap2); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// === Run 2：注入 user 答案 ===
	if err := compose_v2.Run(context.Background(), g2, cm2,
		compose_v2.WithResumeFrom(snap2),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{"ask_approval": schema.UserMessage("yes")}),
	); err != nil {
		t.Fatalf("Run 2 应成功，实际 %v", err)
	}

	// 验证 approved channel 写入正确
	got := compose_v2.MustGet(cm2.Channels(), approvedCh)
	if got != "ok" {
		t.Fatalf("approved 应是 'ok'，实际 %q", got)
	}

	// 验证 cm 的 pending interrupts 已清空
	if len(cm2.PendingInterrupts()) != 0 {
		t.Fatal("Resume 后 pending interrupts 应清空")
	}
}

// TestInterrupt_RejectAnswer_ContinuesWithRejection 验证 user 答 "no" 时
// 节点拿到正确答案，按业务逻辑分支（不重抛中断）。
func TestInterrupt_RejectAnswer_ContinuesWithRejection(t *testing.T) {
	approvedCh := compose_v2.LastValue[string]("approval_v2")

	makeGraph := func() *compose_v2.Graph {
		validator := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "ask_v2"); ok {
				return []compose_v2.ChannelWrite{
					compose_v2.Write(approvedCh, "user_said:"+ans),
				}, nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "ask_v2", Question: "go?"},
			}
		}
		b := compose_v2.NewBuilder().
			AddNode("v", validator).
			AddEdge(compose_v2.StartNode, "v").
			AddEdge("v", compose_v2.EndNode)
		b = compose_v2.WithChannel(b, approvedCh)
		g, _ := b.Compile()
		return g
	}

	// Run 1: 中断
	cm1 := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), makeGraph(), cm1); err == nil {
		t.Fatal("应中断")
	}
	snap, _ := cm1.Snapshot()

	// Run 2: 答 "no"
	cm2 := compose_v2.NewChannelManager()
	_ = compose_v2.RegisterChannel(cm2, approvedCh)
	_ = cm2.Restore(snap)
	if err := compose_v2.Run(context.Background(), makeGraph(), cm2,
		compose_v2.WithResumeFrom(snap),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{"ask_v2": schema.UserMessage("no")}),
	); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if got := compose_v2.MustGet(cm2.Channels(), approvedCh); got != "user_said:no" {
		t.Fatalf("应是 user_said:no，实际 %q", got)
	}
}

// TestInterrupt_MissingAnswer_ReInterrupts 验证 Resume 时未提供答案则节点
// 再次抛中断（不会假装继续）。
func TestInterrupt_MissingAnswer_ReInterrupts(t *testing.T) {
	makeGraph := func() *compose_v2.Graph {
		v := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if _, ok := compose_v2.GetResumeAnswerText(ctx, "ask_3"); ok {
				return nil, nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "ask_3", Question: "?"},
			}
		}
		b := compose_v2.NewBuilder().
			AddNode("v", v).
			AddEdge(compose_v2.StartNode, "v").
			AddEdge("v", compose_v2.EndNode)
		g, _ := b.Compile()
		return g
	}

	cm1 := compose_v2.NewChannelManager()
	err1 := compose_v2.Run(context.Background(), makeGraph(), cm1)
	if _, isInt := compose_v2.IsInterrupt(err1); !isInt {
		t.Fatalf("Run 1 应中断，实际 %v", err1)
	}
	snap, _ := cm1.Snapshot()

	// Run 2 不带 answer
	cm2 := compose_v2.NewChannelManager()
	_ = cm2.Restore(snap)
	err2 := compose_v2.Run(context.Background(), makeGraph(), cm2,
		compose_v2.WithResumeFrom(snap),
		// 故意不传 WithResumeAnswers
	)
	if _, isInt := compose_v2.IsInterrupt(err2); !isInt {
		t.Fatalf("Run 2 无 answer 时应再次中断，实际 %v", err2)
	}
}

// TestInterrupt_WrongInterruptID_ReInterrupts 验证 Resume 提供错 ID 答案时
// 节点拿不到正确答案，仍抛中断（不会误读其它 ID 的答案）。
func TestInterrupt_WrongInterruptID_ReInterrupts(t *testing.T) {
	makeGraph := func() *compose_v2.Graph {
		v := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if _, ok := compose_v2.GetResumeAnswerText(ctx, "ask_target"); ok {
				return nil, nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "ask_target", Question: "?"},
			}
		}
		b := compose_v2.NewBuilder().AddNode("v", v).
			AddEdge(compose_v2.StartNode, "v").
			AddEdge("v", compose_v2.EndNode)
		g, _ := b.Compile()
		return g
	}

	cm1 := compose_v2.NewChannelManager()
	_ = compose_v2.Run(context.Background(), makeGraph(), cm1)
	snap, _ := cm1.Snapshot()

	cm2 := compose_v2.NewChannelManager()
	_ = cm2.Restore(snap)
	err := compose_v2.Run(context.Background(), makeGraph(), cm2,
		compose_v2.WithResumeFrom(snap),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{"ask_other": schema.UserMessage("yes")}), // 错 ID
	)
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("Run 2 提供错 ID 答案时应再次中断，实际 %v", err)
	}
}

// TestInterrupt_IsInterrupt_NilSafe 验证 IsInterrupt nil 安全。
func TestInterrupt_IsInterrupt_NilSafe(t *testing.T) {
	if _, ok := compose_v2.IsInterrupt(nil); ok {
		t.Fatal("nil err 不应是 interrupt")
	}
	if _, ok := compose_v2.IsInterrupt(errors.New("plain")); ok {
		t.Fatal("plain err 不应是 interrupt")
	}
	ie := &compose_v2.InterruptError{Reason: "test"}
	if got, ok := compose_v2.IsInterrupt(ie); !ok || got != ie {
		t.Fatalf("InterruptError 应识别，got=%v ok=%v", got, ok)
	}
}

// ── audit #5 视角 8 三 finding 测试 ──

// TestMergeInterrupts_PerQueryReasonPreserved 验证 F3: MergeInterrupts 聚合时
// 每 ie 的 Reason 拷贝到对应 query.Reason, fanout 多 tc 业务可区分 per-query
// 上下文 (vs 旧实现只保留 "merged N interrupts" 占位).
//
// sanity-revert: 删 MergeInterrupts 内 for q 循环的 q.Reason = ieReason 块,
// 本测试 fail.
func TestMergeInterrupts_PerQueryReasonPreserved(t *testing.T) {
	ie1 := &compose_v2.InterruptError{
		Reason: "tool_X 想确认",
		Query:  &compose_v2.AskQuery{Question: "X?", InterruptID: "id_x"},
	}
	ie2 := &compose_v2.InterruptError{
		Reason: "tool_Y 想确认",
		Query:  &compose_v2.AskQuery{Question: "Y?", InterruptID: "id_y"},
	}
	merged := compose_v2.MergeInterrupts([]*compose_v2.InterruptError{ie1, ie2})
	if merged == nil {
		t.Fatal("merged 应非 nil")
	}
	queries := merged.AllQueries()
	if len(queries) != 2 {
		t.Fatalf("应聚合 2 queries, 实际 %d", len(queries))
	}
	// 每 query.Reason 应来自其 ie.Reason
	gotReasons := map[string]string{}
	for _, q := range queries {
		gotReasons[q.InterruptID] = q.Reason
	}
	if gotReasons["id_x"] != "tool_X 想确认" {
		t.Errorf("id_x.Reason 应保留 ie1.Reason='tool_X 想确认', 实际 %q", gotReasons["id_x"])
	}
	if gotReasons["id_y"] != "tool_Y 想确认" {
		t.Errorf("id_y.Reason 应保留 ie2.Reason='tool_Y 想确认', 实际 %q", gotReasons["id_y"])
	}
}

// TestMergeInterrupts_BusinessFilledQueryReason 验证业务自填 query.Reason 时
// MergeInterrupts 不覆盖 (业务自填优先).
func TestMergeInterrupts_BusinessFilledQueryReason(t *testing.T) {
	ie1 := &compose_v2.InterruptError{
		Reason: "ie 级 reason",
		Query: &compose_v2.AskQuery{
			Question:    "Q?",
			Reason:      "业务自填 query reason",
			InterruptID: "id_1",
		},
	}
	ie2 := &compose_v2.InterruptError{
		Reason: "另一 ie",
		Query:  &compose_v2.AskQuery{Question: "Q2?", InterruptID: "id_2"},
	}
	merged := compose_v2.MergeInterrupts([]*compose_v2.InterruptError{ie1, ie2})
	queries := merged.AllQueries()
	for _, q := range queries {
		if q.InterruptID == "id_1" && q.Reason != "业务自填 query reason" {
			t.Errorf("业务自填 query.Reason 应保留, 实际 %q", q.Reason)
		}
	}
}

// TestInterrupt_RejectsDuplicateIDsInSameBatch 同一个中断错误对象里两条 query 撞 ID
// 必须**当场拒**。
//
// # 为什么不能放过
//
// 调用方回答用的就是这个 ID（`WithResumeUserInputs(map[InterruptID]*Message)`）——两条
// 同址挂起无从分别投递。放过的话，用户按「批准转账」那条答，而读同一个 ID 的代码可能
// 执行的是「删除历史」。
//
// ⚠️ 这条只管**一个错误对象内部**；两个兄弟分支各抛一条、ID 相同那半由
// `TestInterrupt_RejectsDuplicateIDsAcrossBranches` 守（校验点在整超步汇总那里，
// 因为落档单位是「一个超步的全部挂起」）。
//
// ⚠️ 这个用例原先钉的是「同一批撞 ID 会被去重成 1 条」——那是**在描述缺陷本身**。
func TestInterrupt_RejectsDuplicateIDsInSameBatch(t *testing.T) {
	g, err := compose_v2.NewBuilder().
		AddNode("n", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, &compose_v2.InterruptError{
				Reason: "test dup",
				Queries: []*compose_v2.AskQuery{
					{Question: "批准转账 5000 吗", InterruptID: "same_id"},
					{Question: "批准删除全部历史吗", InterruptID: "same_id"},
				},
			}
		}).
		AddEdge(compose_v2.StartNode, "n").
		AddEdge("n", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	cper := compose_v2.NewInMemoryCheckpointer()
	runErr := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("dup"))

	// 承重①：**必须不是中断**——放过的话调用方会以为这是一次正常 HITL 去问人
	if _, isInt := compose_v2.IsInterrupt(runErr); isInt {
		t.Fatalf("批内撞 ID 应被拒，却当成了正常中断：%v\n"+
			"（那样第二条被静默丢弃，用户答一条却可能触发另一条）", runErr)
	}
	if runErr == nil {
		t.Fatal("批内撞 ID 应报错，实际成功收尾")
	}
	// 承重②：**两条问题都要出现在错误里**——只报「撞了」的话业务不知道是哪两条撞
	for _, want := range []string{"批准转账 5000 吗", "批准删除全部历史吗", "same_id"} {
		if !strings.Contains(runErr.Error(), want) {
			t.Errorf("错误信息里没有 %q，业务无从定位：%v", want, runErr)
		}
	}
	// 承重③：**不可重试**——这是确定性的代码 bug，重跑必然同样违规。
	// 实测过没有这条时的代价：配了 retry 的编排下一次这样的 bug 扣了 4 次款。
	if compose_v2.IsRetryable(runErr) {
		t.Error("协议违规不该被判为可重试——重跑必然同样违规，只会把副作用再放一遍")
	}
	if !errors.Is(runErr, compose_v2.ErrRunAborted) {
		t.Errorf("应 wrap ErrRunAborted（业务据此判「已终局、别再 Resume」）：%v", runErr)
	}
	// 承重④：**不该留下存档**。
	//
	// ⚠️ 别把这条读成「验了『写了又清』」——这个 fixture 里 cp **从没被写过**（单节点、
	// 校验在任何写入之前就拒了）。实测把出口那句 `purgeExitSlot` 关掉，本条照样绿。
	// 它实际守的是「有没有 wrap 上终局哨兵」（漏 wrap 时会走 SaveNS 存下一份 cp → 本条
	// 报红；那份档实测是 **Status=failed**，因为去掉 wrap 之后这个错已不是控制流信号，
	// `shouldStayPending` 判 false），跟承重③ 是同一件事的两个观测面。
	//
	// **「协议违规 + 有前置 cp ⟹ 真的删掉」那条合成路径**由本文件下面那条
	// TestInterrupt_AbortPurgesPreExistingCheckpoint 单独守。
	if exists, _ := cper.ExistsNS(context.Background(), "dup", ""); exists {
		t.Error("终局失败后存档仍在——下一次同 sid 的调用会把它捡起来，重撞同一个确定性错误")
	}
	if n := len(cm.PendingInterrupts()); n != 0 {
		t.Errorf("被拒的批次不该留下挂起，实际 %d 条", n)
	}
}

// TestInterruptError_Validate_QueryAndQueriesMutex 验证 F8: ie.Validate() 检
// Query/Queries 互斥, 同时填时返 error.
func TestInterruptError_Validate_QueryAndQueriesMutex(t *testing.T) {
	// 单 Query OK
	ie1 := &compose_v2.InterruptError{Query: &compose_v2.AskQuery{Question: "Q?"}}
	if err := ie1.Validate(); err != nil {
		t.Errorf("单 Query 应通过, 实际 %v", err)
	}

	// 单 Queries OK
	ie2 := &compose_v2.InterruptError{Queries: []*compose_v2.AskQuery{{Question: "Q?"}}}
	if err := ie2.Validate(); err != nil {
		t.Errorf("单 Queries 应通过, 实际 %v", err)
	}

	// 同时填 → fail
	ie3 := &compose_v2.InterruptError{
		Query:   &compose_v2.AskQuery{Question: "single?"},
		Queries: []*compose_v2.AskQuery{{Question: "multi?"}},
	}
	if err := ie3.Validate(); err == nil {
		t.Fatal("Query+Queries 同时填应返 error")
	}

	// nil 安全
	var ie *compose_v2.InterruptError
	if err := ie.Validate(); err != nil {
		t.Errorf("nil ie 应通过, 实际 %v", err)
	}
}

// TestGraph_InterruptError_ValidateFailFast 验证 F8: graph_loop ie 路径在
// record 前调 Validate, 同时填 Query+Queries 时 fail-fast 返 wrap error.
//
// sanity-revert: 删 graph_loop.go 内 `if vErr := ie.Validate(); vErr != nil`
// 块, 本测试改报 nil 或 silent 走 Queries 优先, 应 fail.
func TestGraph_InterruptError_ValidateFailFast(t *testing.T) {
	g, err := compose_v2.NewBuilder().
		AddNode("n", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, &compose_v2.InterruptError{
				Reason:  "test validate",
				Query:   &compose_v2.AskQuery{Question: "single?", InterruptID: "id_1"},
				Queries: []*compose_v2.AskQuery{{Question: "multi?", InterruptID: "id_2"}},
			}
		}).
		AddEdge(compose_v2.StartNode, "n").
		AddEdge("n", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	runErr := compose_v2.Run(context.Background(), g, cm)
	if runErr == nil {
		t.Fatal("应返 validate error")
	}
	if _, isInt := compose_v2.IsInterrupt(runErr); isInt {
		t.Fatalf("不应作为 Interrupt 透传, 应转 fail-fast error: %v", runErr)
	}
	// **audit #5 阶段 1 follow-up MED-B fix**: 之前断言 `!errors.Is(runErr, runErr) ||
	// runErr.Error() == ""` 是 5 类炸弹 #3 (error 字符串不匹配 regex)——errors.Is(x,x)
	// 自反恒 true 让 !true=false 永不进 branch, 实际只验 error 非空, 不验 fix 路径
	// 专属关键词. 改 strings.Contains 精确锁 Validate 路径 (非"Query is nil" 等其他
	// fail-fast 路径).
	if !strings.Contains(runErr.Error(), "mutually exclusive") {
		t.Fatalf("error msg 应含 'mutually exclusive' 关键词锁定 Validate 路径, 实际 %v", runErr)
	}
}

// 注：cond fn 路径的 Validate fail-fast 测试已随「cond 禁止 interrupt」协议删除——
// cond 抛任何 InterruptError（含 Query+Queries 双填）现在一律被拒，由
// issue86_cond_path_internal_test.go::TestCondInterrupt_AlwaysRejected 覆盖。
// NodeFunc 路径的 Validate fail-fast（上方测试）仍在，Validate helper 保留。

// hookAskingNoID 在 OnNodeStart 里抛一个没有 ID 的中断。
type hookAskingNoID struct {
	compose_v2.NoOpOrchestrationHooks
}

func (hookAskingNoID) OnNodeStart(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	return ctx, &compose_v2.InterruptError{
		Query: &compose_v2.AskQuery{Question: "钩子里的暂停（没填 ID）"}}
}

// TestInterrupt_HookPathAlsoValidated hook 抛的中断必须走**同一套**形状校验。
//
// # 为什么单列一条
//
// 校验原先只长在「节点返回」那条路上，而 `OnNodeStart` 的早退在它上面几十行 ——
// hook 抛的中断整条绕过去。实测：空 ID 的挂起照样落进存档（`ID=""`），两个分支各抛一次
// 就是两条彼此不可区分的挂起。**要拒的那个形态，从另一个入口原样存活。**
//
// 同一份判断散在两处，漏掉一处就是一个静默入口——所以这条钉的不是「校验逻辑对不对」
// （那由批内撞 ID 那条钉），而是「这个入口有没有接上」。
func TestInterrupt_HookPathAlsoValidated(t *testing.T) {
	g, err := compose_v2.NewBuilder().
		AddNode("n", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "n").
		AddEdge("n", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	runErr := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithHooks(hookAskingNoID{}))

	// 承重①：**不能当成正常中断**——那样调用方会拿着一个空 ID 去问人，答案回不来
	//
	// ⚠️ 这里用 Errorf 不是 Fatalf：唯一已知的回归形态下①②会同时坏，而②的信息
	// （存档里真的落下了不可区分的挂起）比①更接近缺陷的实质——Fatalf 会把它盖掉。
	if _, isInt := compose_v2.IsInterrupt(runErr); isInt {
		t.Errorf("hook 抛的空 ID 中断应被拒，却当成了正常中断：%v", runErr)
	}
	// 承重②：**存档里不能留下空 ID 的挂起**——这才是缺陷的实质
	// （只断「报错了」的话，将来若在别处报错、挂起照旧落盘，这条仍会绿）
	for _, q := range cm.PendingInterrupts() {
		if q.InterruptID == "" {
			// 同样用 Errorf——③（wrap 了哨兵没有）在它后面，三条各自独立承重
			t.Errorf("存档里落下了空 ID 的挂起（问题 %q）——它跟任何其他空 ID 挂起不可区分",
				q.Question)
		}
	}
	if !errors.Is(runErr, compose_v2.ErrRunAborted) {
		t.Errorf("应 wrap ErrRunAborted：%v", runErr)
	}
}

// TestInterrupt_AbortPurgesPreExistingCheckpoint 协议违规发生在**已经有存档之后**时，
// 那份存档必须被真的删掉。
//
// # 为什么单列
//
// 「空 ID 被拒 ⟹ 存档不留」这句承诺是**两段拼起来的**：`validateInterrupt` 给错 wrap 上
// 终局哨兵 + Run 出口的 defer 据此 purge。而批内撞 ID 那条用例里 cp **从没被写过**
// （单节点、校验在任何写入之前就拒了），所以它验不到第二段；`abort_purge_test.go` 走的
// 又是 cond 违规那条来源。**这条合成路径此前谁都没跑通。**
//
// 承重量是「存档没了」而不是「报错了」：留着的话它占着 (sid,ns) 那个单槽，而且2026-08-13 起续跑闸门按内容判（`HasResumableWork`）、不读 Status
// ⟹ 下一次同 sid 的调用会把这份失败档捡起来，一次次重撞同一个确定性错误。
func TestInterrupt_AbortPurgesPreExistingCheckpoint(t *testing.T) {
	ctx := context.Background()
	cper := compose_v2.NewInMemoryCheckpointer()
	const sid = "abort_after_cp"

	b := compose_v2.NewBuilder()
	// 第一步：正常 HITL 提问 —— 它会存下一份 Pending cp
	b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续吗"})
		return nil, err
	})
	// 第二步：业务在这儿违反协议（空 ID）
	b.AddNode("bad", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, &compose_v2.InterruptError{Query: &compose_v2.AskQuery{Question: "忘了填 ID"}}
	})
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", "bad")
	b.AddEdge("bad", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// Run 1：停在合法中断，存下 Pending cp
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("第一步应中断，got %v", err)
	}
	// 前置条件：**存档真的在**（不验这条的话，后面「没了」可能只是「从来没有过」）
	if exists, _ := cper.ExistsNS(ctx, sid, ""); !exists {
		t.Fatal("第一步之后应有存档——没有的话这条用例验不到「写了又清」")
	}

	// Run 2：答完第一问，走到违规的第二步
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("继续")}))

	// ⚠️ Errorf 不是 Fatalf：下面那条才是本例的承重量，漏 wrap 哨兵时两件事一起坏，
	// Fatalf 会把更重要的那条盖掉（本文件另一处刚因为同样的理由改过）。
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("协议违规应终局失败，got %v", err)
	}
	// 承重：**先前那份存档必须没了**。
	//
	// ⚠️ **危害是哪一种，取决于残留档的 Status，不能写死一句**。这条用例有两种坏法，
	// 各自留下的残留状态不同（都实测过）：
	//   - 出口那句 purge 被删 ⟹ 残留的是 Run 1 那份 **Status=pending** 的挂起档 ⟹
	//     下一次同 sid 调用 auto-load **真的把它捡起来**，已答过的那一问被用**同一个
	//     InterruptID 原样又问一遍**、节点跟着重跑（实测 askRuns 2→3，ID 逐字相同）
	//   - 终局哨兵漏 wrap ⟹ 这个错不再是控制流信号 ⟹ 出口存下 **Status=failed** ⟹
	//     下一次同 sid 的调用**照样把它捡起来**（闸门按内容判、不读 Status），一次次
	//     重撞同一个确定性错误，而且它还占着 (sid,ns) 单槽
	//
	// 我在这儿写死过前一种（措辞是从批内撞 ID 那条抄来的，**那边残留是 failed、抄过来
	// 就反了**），而且信息里打印的 Status 就在旁边直接反驳它。所以现在从实际读到的
	// Status 推——两种都说对，且不会因为将来多一种坏法而漂。
	if cp, _ := cper.LoadNS(ctx, sid, ""); cp != nil {
		harm := "它会被下一次同 sid 的调用捡起来、重撞同一个确定性错误，还占着 (sid,ns) 单槽"
		if cp.Status == compose_v2.CPStatusPending {
			harm = "它会被下一次同 sid 的调用 auto-load 捡起来接着跑：" +
				"已答过的问题被原样重问、节点重跑、副作用重放"
		}
		t.Fatalf("终局失败后先前的存档仍在（Status=%v frontier=%v pending=%d）——%s\n"+
			"sanity-revert: 删 graph_loop.go 出口 defer 里 errors.Is(runErr, ErrRunAborted) "+
			"的 purge 分支 → 本测试 fail",
			cp.Status, cp.CurrentFrontier, len(cp.PendingInterrupts), harm)
	}
}

// TestInterrupt_RejectsDuplicateIDsAcrossBranches 同一超步**跨分支**撞 ID 必须当场拒。
//
// # 为什么单列（批内那条挡不住它）
//
// 批内撞 ID（一个 InterruptError 里两条 query 同 ID）由 `validateInterrupt` 拒。但两个
// **兄弟分支**各抛一条**各自合法**的单 query 中断、ID 相同时，两条各自过校验 ⟹ 汇总时
// 才撞上。校验单位必须等于落档单位——落档单位是「一个超步的全部挂起」。
//
// 修前实测：调用方看到 2 条、存档里只有 1 条（first-write-wins 静默丢第二条，只 warn）
// ⟹ 用户答了看得见的那条，**另一件事也执行了**，整个 Run 成功收尾（err==nil）。
//
// ⚠️ **撞了只能拒，不能「都留下」**：调用方回答用的就是这个 ID
// （`WithResumeUserInputs(map[InterruptID]*Message)`），两条同址挂起无从分别投递。
func TestInterrupt_RejectsDuplicateIDsAcrossBranches(t *testing.T) {
	mk := func(id, q string) compose_v2.NodeFunc {
		return func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: id, Question: q}}
		}
	}
	b := compose_v2.NewBuilder().WithName("dup_branch")
	b.AddNode("transfer", mk("confirm", "批准转账 5000 吗"))
	b.AddNode("purge", mk("confirm", "确认删除全部历史吗"))
	b.AddEdge(compose_v2.StartNode, "transfer")
	b.AddEdge(compose_v2.StartNode, "purge")
	b.AddEdge("transfer", compose_v2.EndNode)
	b.AddEdge("purge", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cm := compose_v2.NewChannelManager()
	runErr := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()),
		compose_v2.WithSessionID("dup_branch"))

	// 承重①：**不能伪装成一次正常 HITL**——那样调用方会拿着一个地址去问人，
	// 而另一条挂起没有地址可投递
	if _, isInt := compose_v2.IsInterrupt(runErr); isInt {
		t.Fatalf("跨分支撞 ID 应被拒，却当成了正常中断：%v", runErr)
	}
	if !errors.Is(runErr, compose_v2.ErrRunAborted) {
		t.Fatalf("应终局失败（业务自身矛盾、重跑必然一样）：%v", runErr)
	}
	// 承重②：**不可重试**——重跑必然同样撞
	if compose_v2.IsRetryable(runErr) {
		t.Error("撞 ID 是确定性的业务 bug，不该被判为可重试")
	}
	// 承重③：**两条问题都要出现在错误里**——只说「撞了」业务不知道是哪两条
	for _, want := range []string{"批准转账 5000 吗", "确认删除全部历史吗", "confirm"} {
		if !strings.Contains(runErr.Error(), want) {
			t.Errorf("错误里没有 %q，业务无从定位：%v", want, runErr)
		}
	}
	// 承重④：**不留半份挂起**——留着会让 LoadAllInSession 的全景查询看到不完整的清单
	if n := len(cm.PendingInterrupts()); n != 0 {
		t.Errorf("被拒的批次不该留下挂起，实际 %d 条", n)
	}
}

// TestInterrupt_DupRejectStillCommitsSiblingWrites 拒绝撞 ID 时，**成功兄弟的 writes 照常落地**。
//
// 这条钉的是拒绝的**落点**：撞 ID 的事实在记录循环里发现，但那里往下还有
// `ApplyAll`（成功分支的 writes）和 `appendCompletedSiblingsLocked`（成功成员身份 + sends）。
// 从记录循环直接 return 会跳过它们，留下半应用状态。
func TestInterrupt_DupRejectStillCommitsSiblingWrites(t *testing.T) {
	ch := compose_v2.LastValue[string]("dup_sib_ch")
	b := compose_v2.NewBuilder().WithName("dup_sib")
	b = compose_v2.WithChannel(b, ch)
	b.AddNode("good", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(ch, "committed")}, nil
	})
	for i, n := range []compose_v2.NodeID{"d1", "d2"} {
		q := "q" + string(rune('1'+i))
		b.AddNode(n, func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "same", Question: q}}
		})
	}
	for _, n := range []compose_v2.NodeID{"good", "d1", "d2"} {
		b.AddEdge(compose_v2.StartNode, n)
		b.AddEdge(n, compose_v2.EndNode)
	}
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cm := compose_v2.NewChannelManager()
	runErr := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()),
		compose_v2.WithSessionID("dup_sib"))
	if !errors.Is(runErr, compose_v2.ErrRunAborted) {
		t.Fatalf("应终局失败：%v", runErr)
	}
	if v, _ := compose_v2.Get(cm.Channels(), ch); v != "committed" {
		t.Fatalf("成功兄弟的 write 没落地（实际 %q）——拒绝的落点太早，"+
			"跳过了 ApplyAll，留下半应用状态", v)
	}
}
