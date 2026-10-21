package agent_v2_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// 同一批 tool_call 做出的决定在 OnToolEnd 之后才定（见 tool_batch_signals.go）。

// flipTransferToFailure 把 transfer_to_agent 的成功结果改写成失败（比如策略拦下）。
type flipTransferToFailure struct{ hook.NoOpHooks }

func (h *flipTransferToFailure) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if out != nil && out.Result != nil && out.Result.Success {
		out.Result.Success = false
		out.Result.ErrorCode = "policy_blocked"
		out.Result.Error = "转交被策略拦下"
	}
	return ctx, nil
}

// TestToolBatch_OnToolEndRewriteToFailure_VoidsTransfer：钩子把转交结果改成失败，这次转交就不算；
// 而且 ReAct 不因为 transfer 是 return-direct 就短路收尾——模型被告知这一次没成，接着作答。
//
// 曾经作废在钩子之前判：模型看到「失败」、对话照样转走。只修作废不修短路的话，转交作废了而最终答复
// 是空的（审计实测）。
func TestToolBatch_OnToolEndRewriteToFailure_VoidsTransfer(t *testing.T) {
	frontModel := newScopeStub(
		schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "t1", Type: "function", Function: schema.FunctionCall{
			Name: agent_v2.HandoffTransferToolName, Arguments: `{"agent_name":"billing"}`}}}),
		schema.AssistantMessage("前台自己处理"))
	front := agent_v2.NewBasicAgent(agent_v2.WithName("front"), agent_v2.WithModel(frontModel),
		agent_v2.WithHandoffTransfer(), agent_v2.WithHooks(&flipTransferToFailure{}), agent_v2.WithMaxRounds(3))
	billing, billingModel := scopeLeaf("billing", "BILLING 已处理")
	h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("desk"), agent_v2.WithHandoffInitial("front")).
		AddAgent("front", front).AddAgent("billing", billing)

	events, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("退款"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := billingModel.count(); n != 0 {
		t.Errorf("钩子把转交改成了失败，模型看到的是「失败」，对话不该被转走（billing 被调 %d 次）", n)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || final.Content != "前台自己处理" {
		t.Errorf("转交没成，模型应接着作答，实际最终答复 %+v（前台模型被调 %d 次）", final, frontModel.count())
	}
}

// TestToolBatch_FirstWinsAfterSiblingAsk：同一批里一个工具问人、两个转交目标不同。答完续跑时两个转交从
// 工具账重放，第一个生效——不再报冲突，答完之后任务往下走。
//
// 曾经两个转交在批末报冲突：续跑时这批 tool_call 原样重放、必然再撞，每次都是同一个冲突（审计实测
// 续跑 3 次）；而冲突要等兄弟的问题答完才撞得上，答完了任务照样卡死。
func TestToolBatch_FirstWinsAfterSiblingAsk(t *testing.T) {
	batch := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "q1", Type: "function", Function: schema.FunctionCall{
			Name: agent_v2.AskUserToolName, Arguments: `{"question":"确认退款？"}`}},
		{ID: "t1", Type: "function", Function: schema.FunctionCall{
			Name: agent_v2.HandoffTransferToolName, Arguments: `{"agent_name":"billing"}`}},
		{ID: "t2", Type: "function", Function: schema.FunctionCall{
			Name: agent_v2.HandoffTransferToolName, Arguments: `{"agent_name":"tech"}`}},
	})
	frontModel := newScopeStub(batch, schema.AssistantMessage("不该走到这里"))
	front := agent_v2.NewBasicAgent(agent_v2.WithName("front"), agent_v2.WithModel(frontModel),
		agent_v2.WithHandoffTransfer(), agent_v2.WithAskUser(), agent_v2.WithMaxRounds(3))
	billing, billingModel := scopeLeaf("billing", "BILLING 已处理")
	tech, techModel := scopeLeaf("tech", "TECH 已处理")
	h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("desk"), agent_v2.WithHandoffInitial("front")).
		AddAgent("front", front).AddAgent("billing", billing).AddAgent("tech", tech)

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := h.Run(ctx, sess, schema.UserMessage("退款"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应先停在问人上：%v", err)
	}
	answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("确认")}
	if _, err := h.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers)); err != nil {
		t.Fatalf("答完续跑不该失败（曾经每次都撞同一个冲突）：%v", err)
	}
	if b, tc := billingModel.count(), techModel.count(); b != 1 || tc != 0 {
		t.Errorf("排在前面的转交（billing）应生效：billing 被调 %d 次、tech %d 次", b, tc)
	}
	if n := frontModel.count(); n != 1 {
		t.Errorf("两个转交从工具账重放，前台模型不该再被调（实际 %d 次）", n)
	}
}

// seenStub 按序吐消息，并记下每次被调时看到的最后一条 tool_result。
type seenStub struct {
	mu       sync.Mutex
	msgs     []*schema.Message
	i        int
	lastTool []string
}

func (m *seenStub) Generate(_ context.Context, in []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	last := ""
	for j := len(in) - 1; j >= 0; j-- {
		if in[j].Role == schema.RoleTool {
			last = in[j].Content
			break
		}
	}
	m.lastTool = append(m.lastTool, last)
	k := m.i
	m.i++
	if k >= len(m.msgs) {
		k = len(m.msgs) - 1
	}
	out := *m.msgs[k]
	return &out, nil
}
func (m *seenStub) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *seenStub) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }
func (m *seenStub) calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.i
}

// exitToolCall 是一个调业务 exit 工具的 tool_call。
func exitToolCall() *schema.Message {
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "e1", Type: "function",
		Function: schema.FunctionCall{Name: "exit", Arguments: `{}`}}})
}

func exitToolkit(res func() *tool.Result) *tool.Toolkit {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("exit", "给出答复并结束", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { return res(), nil }))
	return tk
}

func rdMeta(r *tool.Result) *tool.Result {
	r.Metadata = map[string]any{tool.ReturnDirectMetaKey: true}
	return r
}

// TestToolBatch_BusinessReturnDirectFailureStillExits：业务自己的 return-direct 工具以失败收尾（「无法
// 处理，请联系人工」）——失败内容就是它给出的答复，照常短路，模型不再被调。工具自己声明失败、钩子把
// 成功改成失败，两种都一样。
//
// 曾经「结果在钩子之后是失败就不短路」对所有工具生效：业务 exit 工具一声明失败，模型就被再调一次、
// 答复被顶掉（审计实测）。不短路只给框架那件控制工具（transfer_to_agent / route_to）：它的决定作废了，
// 短路收尾的话最终答复是空的。
func TestToolBatch_BusinessReturnDirectFailureStillExits(t *testing.T) {
	cases := map[string]struct {
		res  func() *tool.Result
		hook hook.Hooks
	}{
		"工具自己声明失败": {res: func() *tool.Result { return rdMeta(tool.Failure("abort", "无法继续，请联系人工")) }},
		"钩子改成失败":   {res: func() *tool.Result { return rdMeta(tool.Success("答复A")) }, hook: &flipToolEndToFailure{}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			m := &seenStub{msgs: []*schema.Message{exitToolCall(), schema.AssistantMessage("模型又说话了")}}
			opts := []agent_v2.Option{agent_v2.WithName("solo"), agent_v2.WithModel(m),
				agent_v2.WithToolkit(exitToolkit(c.res)), agent_v2.WithMaxRounds(3)}
			if c.hook != nil {
				opts = append(opts, agent_v2.WithHooks(c.hook))
			}
			events, err := agent_v2.NewBasicAgent(opts...).Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("办不了的事"))
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			final := agent_v2.FinalMessage(events)
			if m.calls() != 1 || final == nil || !strings.HasPrefix(final.Content, "[") {
				t.Errorf("应以失败内容短路收尾：模型被调 %d 次，最终 %+v", m.calls(), final)
			}
		})
	}
}

// flipToolEndToFailure 把任何成功的工具结果就地改成失败（带说明）。
type flipToolEndToFailure struct{ hook.NoOpHooks }

func (h *flipToolEndToFailure) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if out != nil && out.Result != nil && out.Result.Success {
		out.Result.Success = false
		out.Result.ErrorCode = "policy_blocked"
		out.Result.Error = "被策略拦下"
	}
	return ctx, nil
}

// replaceResultForgetSuccess 整体换掉 Result、只填 Output——Success 是零值 false。
type replaceResultForgetSuccess struct{ hook.NoOpHooks }

func (replaceResultForgetSuccess) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if out != nil && out.Result != nil {
		out.Result = &schema.ToolCallResult{Output: "[已脱敏] " + out.Result.Output}
	}
	return ctx, nil
}

// TestToolBatch_HookReplacedResult_ModelSeesWhatFrameworkDecides：钩子整体换掉 Result 忘了填 Success
// （零值 false）——框架按失败处理，**模型看到的也是失败**：转交作废、前台模型下一轮看到的是失败格式；
// 业务 exit 工具以失败内容短路收尾。钩子给的内容不丢。
//
// 曾经判失败只看 Success、渲染失败格式却还要 Error 非空：模型看到的是「已转交」，转交却作废了（审计
// 实测）。两边必须是同一个结论；零值陷阱写在 hook.Hooks 的消费语义里。
func TestToolBatch_HookReplacedResult_ModelSeesWhatFrameworkDecides(t *testing.T) {
	t.Run("框架转交", func(t *testing.T) {
		frontModel := &seenStub{msgs: []*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "t1", Type: "function", Function: schema.FunctionCall{
				Name: agent_v2.HandoffTransferToolName, Arguments: `{"agent_name":"billing"}`}}}),
			schema.AssistantMessage("前台自己处理")}}
		front := agent_v2.NewBasicAgent(agent_v2.WithName("front"), agent_v2.WithModel(frontModel),
			agent_v2.WithHandoffTransfer(), agent_v2.WithHooks(replaceResultForgetSuccess{}), agent_v2.WithMaxRounds(3))
		billing, billingModel := scopeLeaf("billing", "BILLING 已处理")
		h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("desk"), agent_v2.WithHandoffInitial("front")).
			AddAgent("front", front).AddAgent("billing", billing)
		if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("退款")); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if n := billingModel.count(); n != 0 {
			t.Errorf("Result 说失败，转交该作废（billing 被调 %d 次）", n)
		}
		frontModel.mu.Lock()
		defer frontModel.mu.Unlock()
		if len(frontModel.lastTool) < 2 || !strings.HasPrefix(frontModel.lastTool[1], "[] ") ||
			!strings.Contains(frontModel.lastTool[1], "[已脱敏]") {
			t.Errorf("前台模型下一轮应看到失败格式、且钩子给的内容还在，实际 %q", frontModel.lastTool)
		}
	})
	t.Run("业务exit工具", func(t *testing.T) {
		m := &seenStub{msgs: []*schema.Message{exitToolCall(), schema.AssistantMessage("模型又说话了")}}
		ag := agent_v2.NewBasicAgent(agent_v2.WithName("solo"), agent_v2.WithModel(m),
			agent_v2.WithToolkit(exitToolkit(func() *tool.Result { return rdMeta(tool.Success("答复A")) })),
			agent_v2.WithHooks(replaceResultForgetSuccess{}), agent_v2.WithMaxRounds(3))
		events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("问"))
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		final := agent_v2.FinalMessage(events)
		if m.calls() != 1 || final == nil || final.Content != "[] [已脱敏] 答复A" {
			t.Errorf("应以失败格式的内容短路收尾：模型被调 %d 次，最终 %+v", m.calls(), final)
		}
	})
}

// TestToolResult_EmptyMessageFailureNotDoublePrefixed：工具声明了失败、说明为空（tool.Failure(code, "")），
// 钩子什么都没改——模型看到的逐字是「[code] 」，不会把已经渲染好的那段再拼一次成双前缀。
//
// 「Success 为假时没给 Error 就拿 Output 当说明」只该用在钩子给了新内容的时候；钩子之前 Output 就是
// 渲染好的失败文本，再拼一次就是 `[x] [x] `（这个仓库踩过同一个双前缀）。
func TestToolResult_EmptyMessageFailureNotDoublePrefixed(t *testing.T) {
	m := &seenStub{msgs: []*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "e1", Type: "function",
			Function: schema.FunctionCall{Name: "exit", Arguments: `{}`}}}),
		schema.AssistantMessage("收到")}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("solo"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(exitToolkit(func() *tool.Result { return tool.Failure("bad_output", "") })),
		agent_v2.WithMaxRounds(3))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("问")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.lastTool) < 2 || m.lastTool[1] != "[bad_output] " {
		t.Errorf("模型应逐字看到「[bad_output] 」，实际 %q", m.lastTool)
	}
}
