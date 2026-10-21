package agent_v2_test

// 「同轮已跑成功的 tool_call 在 Resume 时不重执行」的回归。
//
// 核心断言是**副作用发生次数**（工具函数被调了几次），不是历史条数——历史双记只是这个
// 问题的一个可见症状，真正的害处是下单 / 发消息 / 扣款各来第二遍。
//
// 载体是 compose_v2 的节点私有中断态原语（node_state.go）。设计要点见那边的类型 godoc，
// 两条从对端挖来的关键决策：memo 键含 visit ⟹ 成功后自然不可达、**不设清除点**（撤回的
// 那版 7 条 HIGH 里 4 条由清除点引出）；写进本层 cm ⟹ 只落本层 cp，不向上冒泡。

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// memoAgent 造一个 BasicAgent：一轮里两个 tool_call —— 一个带副作用计数、一个抛中断。
func memoAgent(t *testing.T, hs agent_v2.HistoryStore, sideEffects *atomic.Int32, askID string) *agent_v2.BasicAgent {
	return memoAgentWith(t, hs, sideEffects, askID, false)
}

// memoAgentWith 的 replayFinalOnly 供「模拟新进程」用——见 TestToolMemo_CrossProcessResume
// 里那段 fixture 说明。
func memoAgentWith(t *testing.T, hs agent_v2.HistoryStore, sideEffects *atomic.Int32, askID string, replayFinalOnly bool) *agent_v2.BasicAgent {
	t.Helper()
	payTool := tool.NewFuncTool("pay", "扣款", map[string]any{"type": "object",
		"properties": map[string]any{"amt": map[string]any{"type": "string"}}},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			sideEffects.Add(1) // ← 真副作用：这个数字是本用例的全部意义
			return tool.Success("已扣款"), nil
		})
	askTool := tool.NewFuncTool("confirm", "要确认", map[string]any{"type": "object",
		"properties": map[string]any{"q": map[string]any{"type": "string"}}},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, askID); ok {
				return tool.Success("用户确认：" + ans), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: askID, Question: "确认扣款?"},
			}
		})
	tk := tool.NewToolkit()
	tk.Register(payTool)
	tk.Register(askTool)

	replies := []*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "tc_pay", Function: schema.FunctionCall{Name: "pay", Arguments: `{"amt":"100"}`}},
			{ID: "tc_ask", Function: schema.FunctionCall{Name: "confirm", Arguments: `{"q":"x"}`}},
		}),
		schema.AssistantMessage("办完了"),
	}
	if replayFinalOnly {
		replies = replies[1:]
	}
	opts := []agent_v2.Option{
		agent_v2.WithName("memoAg"),
		agent_v2.WithModel(newStubModel(replies)),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5),
	}
	if hs != nil {
		opts = append(opts, agent_v2.WithHistory(hs, "t"))
	}
	return agent_v2.NewBasicAgent(opts...)
}

// TestToolMemo_SucceededToolNotReexecutedOnResume 主用例：扣款只发生一次。
func TestToolMemo_SucceededToolNotReexecutedOnResume(t *testing.T) {
	var pays atomic.Int32
	ag := memoAgent(t, nil, &pays, "memo_q1")

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("给我扣 100"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("Run 应中断（confirm 工具），got %v", err)
	}
	if got := pays.Load(); got != 1 {
		t.Fatalf("首跑扣款应恰 1 次，实际 %d", got)
	}

	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("确认")})); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	if got := pays.Load(); got != 1 {
		t.Errorf("❌ Resume 后扣款共 %d 次，应仍是 1 次\n"+
			"  2 次 = 已成功的 tool_call 被重执行——tools 节点原子、整节点重跑，\n"+
			"  memo（节点私有中断态）没生效。真实业务里这是重复下单 / 重复扣款", got)
	}
}

// TestToolMemo_HistoryNoDoubleRecord 同一形状的可见症状：历史里 tool_result 不双记。
func TestToolMemo_HistoryNoDoubleRecord(t *testing.T) {
	var pays atomic.Int32
	hs := agent_v2.NewInMemoryHistoryStore()
	ag := memoAgent(t, hs, &pays, "memo_q2")

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_memo")
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("给我扣 100"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("Run 应中断, got %v", err)
	}
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("确认")})); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	msgs, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "c_memo"})
	n := 0
	for _, m := range msgs {
		if m.Role == schema.RoleTool && strings.Contains(m.Content, "已扣款") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("已扣款的 tool_result 应恰 1 条，实际 %d 条", n)
		for i, m := range msgs {
			t.Logf("  [%d] %s %s", i, m.Role, strings.TrimSpace(m.Content))
		}
	}
}

// TestToolMemo_CrossProcessResume memo 必须跨进程活着（cp 序列化 round-trip）。
//
// 这条钉的是「memo 真的进了 cp」而不是「靠同进程内存侥幸命中」——换全新
// checkpointer + 全新 agent 对象。
func TestToolMemo_CrossProcessResume(t *testing.T) {
	var pays atomic.Int32
	ag1 := memoAgent(t, nil, &pays, "memo_q3")

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()
	_, err := ag1.Run(ctx, sess, schema.UserMessage("扣款"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("Run 应中断, got %v", err)
	}

	// 模拟跨进程：cp 过 wire、换新 checkpointer、换新 agent 对象
	saved, _ := cper.LoadNS(ctx, sess.RunID, "")
	if saved == nil {
		t.Fatal("应有 cp")
	}
	if len(saved.NodeStates) == 0 {
		t.Fatalf("❌ cp 里没有 NodeStates —— memo 没进 cp，跨进程必然重跑")
	}
	blob, merr := compose_v2.MarshalCheckpoint(saved)
	if merr != nil {
		t.Fatalf("marshal: %v", merr)
	}
	revived, uerr := compose_v2.UnmarshalCheckpoint(blob)
	if uerr != nil {
		t.Fatalf("unmarshal: %v", uerr)
	}
	cper2 := compose_v2.NewInMemoryCheckpointer()
	if err := cper2.SaveNS(context.Background(), sess.RunID, "", revived); err != nil {
		t.Fatalf("SaveNS: %v", err)
	}

	// ⚠️ **fixture 陷阱，值得记**：新 agent 的 stub model 索引从 0 重放，若给它整份
	// replies，Resume 后 tools 成功 → 回到 model → 它**又发一遍同样的 tool_call** →
	// tools 进第二轮（visit+1、新 memo 键、正确未命中）→ 工具被调 → 看起来像 memo 失效。
	// 那是 fixture 的假失败：真实跨进程场景里模型拿到完整历史（含 tool 结果）会出终稿。
	// 所以新"进程"只给终稿。（第一版没这么写，探针实测 probe 在 visit=1 被调才定位到。）
	var pays2 atomic.Int32 // 全新计数器：新"进程"里工具若被调会 +1
	ag2 := memoAgentWith(t, nil, &pays2, "memo_q3", true)
	ctx2 := compose_v2.WithCheckpointerCtx(context.Background(), cper2)
	if _, err := ag2.Resume(ctx2, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("确认")})); err != nil {
		t.Fatalf("跨进程 Resume: %v", err)
	}
	if got := pays2.Load(); got != 0 {
		t.Errorf("❌ 跨进程 Resume 后新进程里扣款被调 %d 次，应 0 次（memo 从 cp 读回）\n"+
			"  非 0 = memo 没跨 wire 活下来", got)
	}
}

// TestToolMemo_NextRoundNotBlocked 反面：memo 不能把**下一轮**同名工具挡掉。
//
// 键含 visit（成功后递增）⟹ 下一轮是新键、读不到上轮 memo。这条正是「不设清除点」
// 能成立的依据；若键漏了 visit，第二轮的 pay 会被第一轮的 memo 命中、静默不执行。
func TestToolMemo_NextRoundNotBlocked(t *testing.T) {
	var pays atomic.Int32
	payTool := tool.NewFuncTool("pay", "扣款", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			pays.Add(1)
			return tool.Success("已扣款"), nil
		})
	tk := tool.NewToolkit()
	tk.Register(payTool)

	// 连续两轮都发同一个 tc.ID 的 pay（LLM 完全可能重复用同样的 id），第三轮收尾
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("twoRound"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "tc_same", Function: schema.FunctionCall{Name: "pay", Arguments: `{}`}},
			}),
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "tc_same", Function: schema.FunctionCall{Name: "pay", Arguments: `{}`}},
			}),
			schema.AssistantMessage("两轮都办完了"),
		})),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(6))

	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("扣两次")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := pays.Load(); got != 2 {
		t.Errorf("❌ 两轮各扣一次应共 2 次，实际 %d 次\n"+
			"  1 次 = memo 键漏了 visit 维，第二轮被第一轮的 memo 命中、工具静默没执行\n"+
			"  ——这比重复执行更坏（业务以为扣了，实际没扣，且不报错）", got)
	}
}
