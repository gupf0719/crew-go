package agent_v2_test

// memo 的两条边界（审计抓出、实测复现后修）：
//
//   - **框架元工具的产出不止 tool_result**：`transfer_to_agent` 的真决定是一条交给上层
//     HandoffAgent 的上行信号，`tool_search` 写的是揭示集。memo 命中跳过工具体时，这些
//     得由框架补上——否则**静默失效**（err=nil、tool_result 齐全，只是转交没发生 / 工具没
//     解锁）。转交那半现在随 memo 记下、命中时原样交出（toolMemoEntry.Signals）；
//     tool_search 仍排除在 memo 外、靠函数体重跑。
//   - **memo 键必须带 tool_call 下标**：同一批里两个 tool_call 用同一个 `tc.ID`（框架别处
//     不假设它唯一——`WithCurrentToolCallContext` 就同时带 idx 维）时，后跑的那个会命中前
//     一个的条目、静默不执行。
//
// 两条的共同点：失效都是**静默**的，业务侧看不出任何异常。

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// TestToolMemoFw_HandoffTransferStillHappensAfterResume F1：转交必须发生。
//
// **不传 `WithResumeSession`** 是 CLAUDE.md 的 HITL 范式写法、nested_smoke 5 处 Resume 全是
// 这个形状——所以这不是边角配置。
func TestToolMemoFw_HandoffTransferStillHappensAfterResume(t *testing.T) {
	var billingRan atomic.Int32
	holdTool := tool.NewFuncTool("hold", "中断", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if _, ok := compose_v2.GetResumeAnswerText(ctx, "fw_q"); ok {
				return tool.Success("ok"), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "fw_q", Question: "?"},
			}
		})
	tk := tool.NewToolkit()
	tk.Register(holdTool)

	front := agent_v2.NewBasicAgent(
		agent_v2.WithName("frontdesk"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "t_xfer", Function: schema.FunctionCall{
					Name: agent_v2.HandoffTransferToolName, Arguments: `{"agent_name":"billing"}`}},
				{ID: "t_hold", Function: schema.FunctionCall{Name: "hold", Arguments: `{}`}},
			}),
			schema.AssistantMessage("front 收尾"),
		})),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(5))
	billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithModel(&countingStub{n: &billingRan, reply: "账单办好了"}),
		agent_v2.WithMaxRounds(3))

	ho := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("ho_fw"),
		agent_v2.WithHandoffInitial("frontdesk"),
	).AddAgent("frontdesk", front).AddAgent("billing", billing)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()
	_, err := ho.Run(ctx, sess, schema.UserMessage("我要退款"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("Run 应中断, got %v", err)
	}

	// **不传 WithResumeSession**——框架范式写法
	evs, rerr := ho.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("y")}))
	if rerr != nil {
		t.Fatalf("Resume: %v", rerr)
	}
	if billingRan.Load() == 0 {
		var names []string
		for _, e := range evs {
			if e != nil && e.AgentName != "" {
				names = append(names, e.AgentName)
			}
		}
		t.Errorf("转交没发生——billing 一次都没跑。事件流里的 agent: %v\n"+
			"  memo 命中跳过了 transfer_to_agent 的工具体，而它的真决定是交给上层的\n"+
			"  上行信号（不是 annotation）⟹ memo 没记下它、或命中时没交出 ⟹ 静默失效",
			names)
	}
}

// countingStub 数被调次数的 stub model。
type countingStub struct {
	n     *atomic.Int32
	reply string
}

func (s *countingStub) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	s.n.Add(1)
	return schema.AssistantMessage(s.reply), nil
}

func (s *countingStub) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (s *countingStub) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return s, nil }

// TestToolMemoFw_ToolSearchRevealStillEffectiveAfterResume F1b：tool_search 解锁的工具
// Resume 后仍可调。
func TestToolMemoFw_ToolSearchRevealStillEffectiveAfterResume(t *testing.T) {
	var payCalls atomic.Int32
	payTool := tool.NewFuncTool("deferred_pay", "延迟披露的工具", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			payCalls.Add(1)
			return tool.Success("扣款完成"), nil
		})
	holdTool := tool.NewFuncTool("hold2", "中断", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if _, ok := compose_v2.GetResumeAnswerText(ctx, "ts_q"); ok {
				return tool.Success("ok"), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "ts_q", Question: "?"},
			}
		})
	tk := tool.NewToolkit()
	tk.Register(payTool)
	tk.Register(holdTool)

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("tsAg"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			// 第 1 轮：检索解锁 + 一个中断工具
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "t_search", Function: schema.FunctionCall{
					Name: "tool_search", Arguments: `{"query":"pay"}`}},
				{ID: "t_hold", Function: schema.FunctionCall{Name: "hold2", Arguments: `{}`}},
			}),
			// 第 2 轮：调被解锁的工具
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "t_pay", Function: schema.FunctionCall{Name: "deferred_pay", Arguments: `{}`}},
			}),
			schema.AssistantMessage("完成"),
		})),
		agent_v2.WithToolkit(tk),
		agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{
			Enabled:  true,
			Deferred: func(tl tool.Tool) bool { return tl.Name() == "deferred_pay" },
		}),
		agent_v2.WithMaxRounds(6))

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("帮我扣款"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("Run 应中断, got %v", err)
	}
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("y")})); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if payCalls.Load() != 1 {
		t.Errorf("被解锁的工具应恰调 1 次，实际 %d 次\n"+
			"  0 次 = memo 命中跳过 tool_search 的工具体 ⟹ 这一批的解锁记录是空的（解锁记在这次执行上、"+
			"不进 cp）⟹ 下一轮拿到 [tool_not_revealed]", payCalls.Load())
	}
}

// TestToolMemoFw_DuplicateCallIDBothExecute F2：同一批里重复 tc.ID 时两个都要执行。
//
// 键只用裸 `tc.ID` 时，fanout 下后跑的那个会命中前一个的条目、静默不执行（审计实测
// 30 轮里 25 轮只执行 1 次，纯调度决定）。框架别处不假设 tc.ID 唯一——
// `WithCurrentToolCallContext(ctx, tc.ID, i)` 就同时带了 idx 维。
func TestToolMemoFw_DuplicateCallIDBothExecute(t *testing.T) {
	for round := 0; round < 12; round++ {
		var got []string
		var mu atomic.Int32
		seenN := make(chan string, 4)
		echoTool := tool.NewFuncTool("echo", "回显", map[string]any{"type": "object",
			"properties": map[string]any{"n": map[string]any{"type": "string"}}},
			func(ctx context.Context, args map[string]any) (*tool.Result, error) {
				n, _ := args["n"].(string)
				mu.Add(1)
				seenN <- n
				return tool.Success("echo:" + n), nil
			})
		tk := tool.NewToolkit()
		tk.Register(echoTool)

		ag := agent_v2.NewBasicAgent(
			agent_v2.WithName("dupAg"),
			agent_v2.WithModel(newStubModel([]*schema.Message{
				// **两个 tool_call 同一个 ID、args 不同**
				schema.AssistantToolCallMessage([]schema.ToolCall{
					{ID: "same_id", Function: schema.FunctionCall{Name: "echo", Arguments: `{"n":"1"}`}},
					{ID: "same_id", Function: schema.FunctionCall{Name: "echo", Arguments: `{"n":"2"}`}},
				}),
				schema.AssistantMessage("done"),
			})),
			agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4))

		if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
			t.Fatalf("round %d Run: %v", round, err)
		}
		close(seenN)
		for n := range seenN {
			got = append(got, n)
		}
		if mu.Load() != 2 {
			t.Fatalf("round %d: 两个 tool_call（同 ID、不同 args）都该执行，实际执行 %d 次（args=%v）\n"+
				"  1 次 = memo 键只用裸 tc.ID，后跑的命中了前一个的条目、静默不执行\n"+
				"  ——业务以为发了两条实际一条，而 tool_result 条数/配对/err 全部正常",
				round, mu.Load(), got)
		}
		if len(got) == 2 && got[0] == got[1] {
			t.Fatalf("round %d: 两次执行的 args 相同（%v）——后一个复用了前一个的产出", round, got)
		}
		_ = strings.Join(got, ",")
	}
}

// renamingHook 在 OnToolStart 里改写工具名——`Name` 是[消费]档，框架会把改写值写回
// toolCalls 并用它调用工具。
type renamingHook struct {
	hook.NoOpHooks
	to string
}

func (h *renamingHook) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	// **只改 tc_x 那一个**：两个都改的话 tc_hold 也会去跑 refund，它贡献的执行次数会
	// 盖住 tc_x 到底命没命中 memo——那样 sanity-revert 不红（实测踩过）。
	if h.to != "" && in.CallId == "tc_x" {
		in.Name = h.to
	}
	return ctx, nil
}

// TestToolMemoFw_ToolRenamedOnResume_KeepsFirstTool 续跑时改名不生效的哨兵（工具账那一侧）。
//
// 两次 Resume 之间 hook 的改写逻辑变了：首跑时 tc_x 走 pay 并成功，续跑时钩子把它改名成 refund。
// 这次调用的内容在这一批第一次执行时就定下了（fixToolCallContent）⟹ 续跑仍是 pay，工具账命中，
// pay 不重跑，refund 一次都不跑。改名生效的话 refund 会被执行——一笔已经付过的款被退掉。
func TestToolMemoFw_ToolRenamedOnResume_KeepsFirstTool(t *testing.T) {
	var payRan, refundRan atomic.Int32
	mk := func(name string, n *atomic.Int32) tool.Tool {
		return tool.NewFuncTool(name, name, map[string]any{"type": "object"},
			func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
				n.Add(1)
				return tool.Success(name + " 完成"), nil
			})
	}
	holdTool := tool.NewFuncTool("hold3", "中断", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if _, ok := compose_v2.GetResumeAnswerText(ctx, "rn_q"); ok {
				return tool.Success("ok"), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "rn_q", Question: "?"},
			}
		})

	// finalOnly：Resume 那个 agent 只给终稿。新 agent 的 stub model 索引从 0 重放，
	// 给整份 replies 的话 tools 成功后回 model 会**又发一遍同样的 tool_call** → 进第二轮
	// → 工具再跑一次（refund=2）。那是 fixture 的假失败——真实场景里模型拿到完整历史
	// （含 tool 结果）会出终稿。同款陷阱在 TestToolMemo_CrossProcessResume 记过一次。
	build := func(renameTo string, finalOnly bool) *agent_v2.BasicAgent {
		tk := tool.NewToolkit()
		tk.Register(mk("pay", &payRan))
		tk.Register(mk("refund", &refundRan))
		tk.Register(holdTool)
		replies := []*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "tc_x", Function: schema.FunctionCall{Name: "pay", Arguments: `{}`}},
				{ID: "tc_hold", Function: schema.FunctionCall{Name: "hold3", Arguments: `{}`}},
			}),
			schema.AssistantMessage("done"),
		}
		if finalOnly {
			replies = replies[1:]
		}
		opts := []agent_v2.Option{
			agent_v2.WithName("rnAg"),
			agent_v2.WithModel(newStubModel(replies)),
			agent_v2.WithToolkit(tk),
			agent_v2.WithMaxRounds(5),
		}
		if renameTo != "" {
			opts = append(opts, agent_v2.WithHooks(&renamingHook{to: renameTo}))
		}
		return agent_v2.NewBasicAgent(opts...)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// 首跑：tc_x 走 pay
	_, err := build("", false).Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("Run 应中断, got %v", err)
	}
	if payRan.Load() != 1 {
		t.Fatalf("首跑 pay 应 1 次，实际 %d", payRan.Load())
	}

	// Resume：hook 把 tc_x 的工具名改写成 refund
	_, rerr := build("refund", true).Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("y")}))
	if rerr != nil {
		t.Fatalf("Resume: %v", rerr)
	}
	if refundRan.Load() != 0 || payRan.Load() != 1 {
		t.Errorf("续跑时的改名不该生效：refund 应 0 次、pay 应只在首跑 1 次，实际 refund=%d pay=%d",
			refundRan.Load(), payRan.Load())
	}
}
