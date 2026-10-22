package agent_v2_test

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

// countingFuncTool 是只计数的函数工具，readOnly 决定它有没有声明只读。
func countingFuncTool(name string, calls *atomic.Int32, readOnly bool) *tool.FuncTool {
	var opts []tool.FuncToolOption
	if readOnly {
		opts = append(opts, tool.WithReadOnly())
	}
	return tool.NewFuncTool(name, name, map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			calls.Add(1)
			return tool.Success("ran_" + name), nil
		}, opts...)
}

func toolCallsMessage(names ...string) *schema.Message {
	tcs := make([]schema.ToolCall, 0, len(names))
	for i, n := range names {
		tcs = append(tcs, schema.ToolCall{ID: "tc_" + n + string(rune('0'+i)), Type: "function",
			Function: schema.FunctionCall{Name: n, Arguments: `{}`}})
	}
	return schema.AssistantToolCallMessage(tcs)
}

// 只读放行档：声明只读的直接执行，没声明的（按有副作用处理）执行前问人；续跑批准之后只读那件不重跑。
func TestApprovalAskWrites_ReadOnlyRunsWritesAsk(t *testing.T) {
	var lookups, sends atomic.Int32
	tk := tool.NewToolkit()
	tk.Register(countingFuncTool("lookup", &lookups, true))
	tk.Register(countingFuncTool("send", &sends, false))
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ro_agent"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAskWrites),
		agent_v2.WithModel(newStubModel([]*schema.Message{toolCallsMessage("lookup", "send"), schema.AssistantMessage("done")})),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("send 没声明只读，该停下来问人，实际 %v", err)
	}
	qs := ie.AllQueries()
	if len(qs) != 1 || qs[0].ToolName != "send" || qs[0].Reason != agent_v2.ApprovalInterruptReason {
		t.Fatalf("只该问 send 这一件，实际 %+v", qs)
	}
	if lookups.Load() != 1 || sends.Load() != 0 {
		t.Fatalf("lookup 该直接执行、send 等批准，实际 lookup=%d send=%d", lookups.Load(), sends.Load())
	}
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		qs[0].InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApprove)})); err != nil {
		t.Fatalf("批准之后该跑完，实际 %v", err)
	}
	if lookups.Load() != 1 || sends.Load() != 1 {
		t.Errorf("续跑：lookup 不重跑、send 执行一次，实际 lookup=%d send=%d", lookups.Load(), sends.Load())
	}
}

// 业务一个工具都没声明只读时，只读放行档等同 Ask——不会意外放行。工具用的是根本没实现
// tool.ReadOnlyDeclarer 的类型（recordTool）：没声明 ≠ 声明了 false，两种都得问。
func TestApprovalAskWrites_UndeclaredAsks(t *testing.T) {
	rt := &recordTool{name: "query"}
	calls := &rt.calls
	tk := tool.NewToolkit()
	tk.Register(rt)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ro_undeclared"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAskWrites),
		agent_v2.WithModel(newStubModel([]*schema.Message{toolCallsMessage("query"), schema.AssistantMessage("done")})),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("没声明只读的该问人，实际 %v", err)
	}
	if calls.Load() != 0 {
		t.Errorf("批准之前不该执行，实际 %d", calls.Load())
	}
}

// 启发式的明确结论压过只读放行：Ask 连只读工具也问、Deny 连只读工具也拒；Default 才落到只读判定。
func TestApprovalAskWrites_HeuristicOverridesReadOnly(t *testing.T) {
	for _, tc := range []struct {
		name       string
		decision   agent_v2.ApprovalDecision
		wantAsk    bool
		wantCalls  int32
		wantDenied bool
	}{
		{"default 落到只读放行", agent_v2.ApprovalDecisionDefault, false, 1, false},
		{"ask 连只读也问", agent_v2.ApprovalDecisionAsk, true, 0, false},
		{"deny 连只读也拒", agent_v2.ApprovalDecisionDeny, false, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			tk := tool.NewToolkit()
			tk.Register(countingFuncTool("lookup", &calls, true))
			var seen []*schema.Message
			ag := agent_v2.NewBasicAgent(
				agent_v2.WithName("ro_heuristic"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
				agent_v2.WithApprovalMode(agent_v2.ApprovalAskWrites),
				agent_v2.WithApprovalHeuristic(func(context.Context, string, map[string]any) agent_v2.ApprovalDecision {
					return tc.decision
				}),
				agent_v2.WithModel(newStubModelCapturing([]*schema.Message{toolCallsMessage("lookup"), schema.AssistantMessage("done")}, &seen)),
			)
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
			_, asked := compose_v2.IsInterrupt(err)
			if asked != tc.wantAsk {
				t.Fatalf("问没问人 = %v, want %v（err=%v）", asked, tc.wantAsk, err)
			}
			if !tc.wantAsk && err != nil {
				t.Fatalf("不问人时该跑完，实际 %v", err)
			}
			if calls.Load() != tc.wantCalls {
				t.Errorf("工具执行 %d 次, want %d", calls.Load(), tc.wantCalls)
			}
			denied := false
			for _, m := range seen {
				if m.Role == schema.RoleTool && strings.Contains(m.Content, "[tool_denied]") {
					denied = true
				}
			}
			if denied != tc.wantDenied {
				t.Errorf("模型看到拒绝结果 = %v, want %v", denied, tc.wantDenied)
			}
		})
	}
}

// flipTool 的只读声明可以在两次执行之间改；它第一次执行时自己提问。
type flipTool struct {
	ro    *atomic.Bool
	calls atomic.Int32
}

func (t *flipTool) Name() string                { return "lookup" }
func (t *flipTool) Description() string         { return "lookup" }
func (t *flipTool) InputSchema() map[string]any { return map[string]any{"type": "object"} }
func (t *flipTool) ReadOnly() bool              { return t.ro.Load() }
func (t *flipTool) Call(ctx context.Context, _ map[string]any) (*tool.Result, error) {
	ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "查哪个范围？"})
	if err != nil {
		return nil, err
	}
	t.calls.Add(1)
	return tool.Success("scope=" + ans.Content), nil
}

// 每次调用只判一次：放行的结论随这次调用记下，续跑时工具改了声明也不推翻（不会在用户答完工具自己的问题后
// 又冒出一个审批问题）。
func TestApprovalAskWrites_DecisionFormedOnce(t *testing.T) {
	ro := &atomic.Bool{}
	ro.Store(true)
	ft := &flipTool{ro: ro}
	tk := tool.NewToolkit()
	tk.Register(ft)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ro_once"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAskWrites),
		agent_v2.WithModel(newStubModel([]*schema.Message{toolCallsMessage("lookup"), schema.AssistantMessage("done")})),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.AllQueries()[0].Reason == agent_v2.ApprovalInterruptReason {
		t.Fatalf("第一次停下该是工具自己的提问（只读放行、不审批），实际 %v", err)
	}
	ro.Store(false) // 两次执行之间改了声明
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage("全部")})); err != nil {
		t.Fatalf("放行的结论记下了，续跑不该再问审批，实际 %v", err)
	}
	if ft.calls.Load() != 1 {
		t.Errorf("工具该执行完一次，实际 %d", ft.calls.Load())
	}
}

// 审批生效时，当前审批方式写进系统提示；不生效时不写；WithApprovalPrompt 可替换、传空串不写。
func TestApprovalPrompt_InSystemPreamble(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    []agent_v2.Option
		want    []string
		notWant []string
	}{
		{"auto 不写", nil, nil, []string{"【工具审批】"}},
		{"ask", []agent_v2.Option{agent_v2.WithApprovalMode(agent_v2.ApprovalAsk)},
			[]string{"【工具审批】", "执行前都要经过用户批准", "[tool_denied]"}, nil},
		{"ask writes", []agent_v2.Option{agent_v2.WithApprovalMode(agent_v2.ApprovalAskWrites)},
			[]string{"【工具审批】", "只读的工具"}, nil},
		{"只配启发式", []agent_v2.Option{agent_v2.WithApprovalHeuristic(func(context.Context, string, map[string]any) agent_v2.ApprovalDecision {
			return agent_v2.ApprovalDecisionDefault
		})}, []string{"【工具审批】", "可能需要用户批准"}, nil},
		{"替换文案", []agent_v2.Option{agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), agent_v2.WithApprovalPrompt("自定义审批说明")},
			[]string{"自定义审批说明"}, []string{"【工具审批】"}},
		{"传空串不写", []agent_v2.Option{agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), agent_v2.WithApprovalPrompt("")},
			nil, []string{"【工具审批】"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var seen []*schema.Message
			opts := append([]agent_v2.Option{
				agent_v2.WithName("prompt_agent"), agent_v2.WithSystemPrompt("你是助手。"),
				agent_v2.WithModel(newStubModelCapturing([]*schema.Message{schema.AssistantMessage("hi")}, &seen)),
			}, tc.opts...)
			if _, err := agent_v2.NewBasicAgent(opts...).Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
				t.Fatal(err)
			}
			var sys strings.Builder
			for _, m := range seen {
				if m.Role == schema.RoleSystem {
					sys.WriteString(m.Content)
				}
			}
			if !strings.Contains(sys.String(), "你是助手。") {
				t.Errorf("业务自己的系统提示该保留，实际 %q", sys.String())
			}
			for _, w := range tc.want {
				if !strings.Contains(sys.String(), w) {
					t.Errorf("系统提示里该有 %q，实际 %q", w, sys.String())
				}
			}
			for _, w := range tc.notWant {
				if strings.Contains(sys.String(), w) {
					t.Errorf("系统提示里不该有 %q，实际 %q", w, sys.String())
				}
			}
		})
	}
}

// 用户拒绝之后给模型的结果带着「别原样重试」的指引。
func TestApproval_DenyResultGuidesModel(t *testing.T) {
	var calls atomic.Int32
	tk := tool.NewToolkit()
	tk.Register(countingFuncTool("send", &calls, false))
	var seen []*schema.Message
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("deny_agent"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithModel(newStubModelCapturing([]*schema.Message{toolCallsMessage("send"), schema.AssistantMessage("ok")}, &seen)),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("该问人，实际 %v", err)
	}
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage(agent_v2.ApprovalOptionDeny)})); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range seen {
		if m.Role == schema.RoleTool && strings.Contains(m.Content, "用户拒绝执行工具") && strings.Contains(m.Content, "不要用同样的参数再试") {
			found = true
		}
	}
	if !found {
		t.Error("拒绝结果里该带「不要用同样的参数再试」的指引")
	}
}

// PlanAgent 现造的执行体（TmpSubAgent）经 WithPlanTmpSubAgentOptions 配审批：拿到写工具时停下来问人。
func TestPlanTmpSubAgentOptions_ApprovalApplies(t *testing.T) {
	var sends atomic.Int32
	planTK := tool.NewToolkit()
	planTK.Register(countingFuncTool("send", &sends, false))
	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) == 0 {
			return []agent_v2.PlanStep{{
				Description: "发通知",
				TmpSubAgent: &agent_v2.TmpSubAgentConfig{Name: "tmp_sender", System: "你负责发通知", ToolNames: []string{"send"}},
			}}, "", nil
		}
		return nil, "completed", nil
	}
	stub := newStubModel([]*schema.Message{toolCallsMessage("send"), schema.AssistantMessage("sent")})
	dummy := agent_v2.NewBasicAgent(agent_v2.WithName("dummy"), agent_v2.WithModel(newStubModel(nil)))
	pa := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("plan_tmp_approval"),
		agent_v2.WithPlanModel(stub),
		agent_v2.WithPlanToolkit(planTK),
		agent_v2.WithPlanTmpSubAgentOptions(agent_v2.WithApprovalMode(agent_v2.ApprovalAskWrites)),
	).SetExecutor(dummy)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	_, err := pa.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("task"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || ie.AllQueries()[0].Reason != agent_v2.ApprovalInterruptReason {
		t.Fatalf("TmpSubAgent 拿到写工具该停下来审批，实际 %v", err)
	}
	if sends.Load() != 0 {
		t.Errorf("批准之前 send 不该执行，实际 %d", sends.Load())
	}
}
