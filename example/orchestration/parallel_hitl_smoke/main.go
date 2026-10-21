// parallel_hitl_smoke 演示 ParallelAgent fan-out 多分支并发 ask_user 中断 +
// **AllQueries 多 query pattern** 的端到端 HITL Resume 闭环。
//
// **场景**: ParallelAgent 派 3 个 branch 并发，每个 branch 内嵌 BasicAgent 各自调
// ask_user → 同超步 3 个 branch 同时抛 InterruptError → 框架全部收集到单个 cp 的
// PendingInterrupts。业务 Resume 时遍历**所有** query 配对答案。
//
// **核心教学点——为什么不能直接读 ie.Query.X**（doc/v2/hitl-resume.md §2.4 + §2.5）:
//
//	fan-out 多分支并发中断时，框架把 N 条中断经 MergeInterrupts 合成**单一**
//	*InterruptError 返回，所以 ie.AllQueries() **含本超步全部 query**（本例实测 3 条，
//	与 cp.PendingInterrupts 数量一致）。而 ie.Query 只是单 query 那档的字段，
//	fan-out 下读它会漏掉 N-1 个兄弟分支 ⟹ 对应 branch Resume 续不上。
//
//	⟹ **统一用 ie.AllQueries() 遍历，绝不写 ie.Query.X**。业务不需要为了拿全集去
//	读裸 cp。（本例仍然 LoadNS 一次，是为了**展示两个视图数量一致**这个不变量；
//	曾经这段注释写成「AllQueries 只含 1 条、必须 LoadNS 补全」，是错的——
//	本文件自己的运行输出就打脸。）
//
// **关键 API**:
//   - agent_v2.NewParallelAgent + AddBranch（fan-out N 分支并发）
//   - ie.AllQueries()（统一 accessor——单/多 query 模式都安全，不写 ie.Query.X）
//   - cper.LoadNS(ctx, sid, "") → cp.PendingInterrupts（多分支聚合视图）
//   - agent_v2.WithResumeUserInputs(map[InterruptID]*Message)（按 ID 精确配对答案）
//   - 每个 branch 独立 graphNS → InterruptID 5 维 distinct（不串台）
//
// **不依赖真 LLM**——askOnceModel 第一次返 ask_user 触发中断，Resume 后返最终文本。
//
// 跑法:
//
//	go run ./example/orchestration/parallel_hitl_smoke
//
// 全场景 PASS + 退出码 0 表示通过。
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// askOnceModel 第一次（无 tool_result）返 ask_user tool_call 触发 InterruptError，
// Resume 后（有 tool_result）返最终文本（含拿到的答案，便于断言端到端流通）。
type askOnceModel struct {
	question string
	branch   string
}

func (m *askOnceModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for _, msg := range msgs {
		if msg.Role == schema.RoleTool {
			// tool_result 的 Content 是用户答案——回显证明答案端到端流通到对应 branch
			return schema.AssistantMessage(fmt.Sprintf("[%s] 收到答复: %s", m.branch, msg.Content)), nil
		}
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:   "tc_" + m.branch,
		Type: "function",
		Function: schema.FunctionCall{
			Name:      agent_v2.AskUserToolName,
			Arguments: fmt.Sprintf(`{"question":%q}`, m.question),
		},
	}}), nil
}
func (m *askOnceModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *askOnceModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\nparallel_hitl_smoke PASS")
}

func run() error {
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// 3 个 branch，每个内嵌一个 BasicAgent 各自 ask_user 一个不同问题
	branchQ := map[string]string{
		"weather": "目的地天气如何？",
		"budget":  "预算多少？",
		"dates":   "出行日期？",
	}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("trip_planner"))
	for name, q := range branchQ {
		par.AddBranch(name, agent_v2.NewBasicAgent(
			agent_v2.WithName(name),
			agent_v2.WithModel(&askOnceModel{question: q, branch: name}),
			agent_v2.WithAskUser(),
		))
	}

	// === Run: fan-out 3 branch 并发 → 同超步 3 个 InterruptError → 框架全部收集 ===
	_, err := par.Run(ctx, sess, schema.UserMessage("帮我规划旅行"))
	if err == nil {
		return errors.New("Run 应抛 InterruptError（3 branch 并发 ask_user），got nil")
	}
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		return fmt.Errorf("应返 *InterruptError，got %T: %w", err, err)
	}

	// ── 反面教材警示 ────────────────────────────────────────────────────────
	// ie.AllQueries() 只含**返回的这个 ie 自己**的 query（fan-out 场景框架返单一
	// res.FirstErr）——不是全部 3 个 branch 的 query。直接据此 Resume 会漏兄弟分支。
	ieQueries := ie.AllQueries()
	fmt.Printf("Run 抛中断：ie.AllQueries() 含 %d 个 query（MergeInterrupts 已把 3 个 branch 的中断合成一条 ie——这就是全集）\n", len(ieQueries))

	// ── 正确做法：LoadNS 读 cp.PendingInterrupts 拿跨所有 branch 的聚合视图 ────
	cp, err := cper.LoadNS(ctx, sess.RunID, "")
	if err != nil {
		return fmt.Errorf("LoadNS: %w", err)
	}
	if cp == nil {
		return errors.New("LoadNS 返 nil cp（应有 Pending cp）")
	}
	pending := cp.PendingInterrupts
	if len(pending) != 3 {
		return fmt.Errorf("cp.PendingInterrupts 应含 3 个 query（3 branch 各 ask_user 一次），got %d: %+v", len(pending), pending)
	}
	fmt.Printf("cp.PendingInterrupts 含 %d 个 query（跨 3 branch 聚合视图——这才是 Resume 该遍历的全集）\n", len(pending))

	// 验证 3 个 InterruptID 全 distinct（各 branch 独立 graphNS → 5 维 ID 不撞）
	idSet := make(map[string]bool, len(pending))
	gotQuestions := make([]string, 0, len(pending))
	for _, q := range pending {
		if q.InterruptID == "" {
			return fmt.Errorf("query InterruptID 应非空（框架按 5 维自动算），got 空: %+v", q)
		}
		idSet[q.InterruptID] = true
		gotQuestions = append(gotQuestions, q.Question)
	}
	if len(idSet) != 3 {
		return fmt.Errorf("3 个 InterruptID 应全 distinct（branch graphNS 维度区分），实际 %d distinct: %v", len(idSet), idSet)
	}
	sort.Strings(gotQuestions)
	fmt.Printf("3 个 InterruptID 全 distinct；问题集（按字典序）: %v\n", gotQuestions)

	// === Resume: 遍历**所有** pending query 配对答案（不是只答 ie.Query 那一个）===
	// 业务前端持有所有 query → 收集全部答案 → 一次性传。
	//
	// **不是「累积」模式**：这里答的是**本轮**的全部 pending，一轮答完就没有剩下的了，
	// 不会产生上几轮的 stale ID。真正的累积形态（一个 map 跨轮攒着、每次全传）在
	// identity_guard_smoke 和 nested_smoke，那两个会 fire "unmatched InterruptIDs" warn。
	answers := make(map[string]*schema.Message, len(pending))
	for _, q := range pending {
		// 按 Question 决定答什么（演示用——真实场景是前端表单逐个收集）
		var ans string
		switch {
		case strings.Contains(q.Question, "天气"):
			ans = "晴"
		case strings.Contains(q.Question, "预算"):
			ans = "5000 元"
		case strings.Contains(q.Question, "日期"):
			ans = "下周一"
		default:
			ans = "ok"
		}
		answers[q.InterruptID] = schema.UserMessage(ans)
	}
	events, err := par.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers))
	if err != nil {
		return fmt.Errorf("Resume（遍历全部 query 配对答案后）: %w", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		return errors.New("Resume 应返非 nil final（merger 拼 3 branch 结果）")
	}
	// 验证 3 branch 的答复都流通进了 final（merger 聚合）
	for _, want := range []string{"晴", "5000 元", "下周一"} {
		if !strings.Contains(final.Content, want) {
			return fmt.Errorf("final 应含 branch 答复 %q（证明 3 branch 答案都端到端流通），实际 final=%q", want, final.Content)
		}
	}
	fmt.Printf("Resume 成功：3 branch 答案全部流通进 final\n  final=%q\n", final.Content)

	// 验证 cleanup：成功 Resume 后 cp 应被清（不污染后续同 sid Run）
	all, _ := cper.LoadAllInSession(ctx, sess.RunID)
	if len(all) > 0 {
		return fmt.Errorf("成功 Resume 后 cp 应被 cleanupOnSuccess 清，实有 %d cp", len(all))
	}
	fmt.Println("成功 Resume 后 cp 已清（cleanupOnSuccess work）")

	return nil
}
