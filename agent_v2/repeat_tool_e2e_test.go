package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// TestRepeatTool_DeniedCallsStillCount 钉住**被审批拒绝的调用也计入重复链**。
//
// 这是这个特性最实的一个场景，也是「在执行之后数、不在之前数」这条判据的全部理由：
// 模型死命捶一个被拒的工具时，今天框架一声不响（业务连拒 20 次、框架毫无反应）。
// 如果计数挂在「工具真的跑了」上，被拒的调用一次都不算，这条循环永远打不断。
//
// ⚠️ 断言选在「提醒有没有真的进消息流」上，不是「链的内部计数对不对」——后者白盒测已经
// 覆盖了，而它测不出「接线接没接上」。
func TestRepeatTool_DeniedCallsStillCount(t *testing.T) {
	rt := &recordTool{name: "transfer_money"}
	tk := agent_v2_newToolkit()
	tk.Register(rt)

	// 模型一根筋：每轮都发同一个工具、同样参数。
	resps := make([]*schema.Message, 0, 8)
	for i := 0; i < 7; i++ {
		resps = append(resps, oneToolCall("transfer_money"))
	}
	resps = append(resps, schema.AssistantMessage("好吧，我停下"))

	var seen []*schema.Message
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(10),
		// 每个业务工具执行前都问一次；下面每次都答 deny。
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithRepeatToolReminder(&agent_v2.RepeatToolConfig{
			Enabled: true, Thresholds: []int{3},
		}),
		agent_v2.WithModel(newStubModelCapturing(resps, &seen)),
	)

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("转钱"))
	// 一路拒下去：每次审批中断都答 deny，工具体一次都不执行。
	denies := 0
	for i := 0; i < 8; i++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			break
		}
		denies++
		_, err = ag.Resume(ctx, sess.RunID,
			agent_v2.WithResumeSession(sess),
			agent_v2.WithResumeUserInputs(map[string]*schema.Message{
				ie.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionDeny),
			}))
	}
	if err != nil {
		if _, isInt := compose_v2.IsInterrupt(err); !isInt {
			t.Fatalf("Run/Resume: %v", err)
		}
	}

	// 承重①：前提——确实连拒了至少 3 次，且工具体**一次都没真跑**。
	// 没有这条，下面那个断言可能是「执行了 3 次」才触发的，跟「被拒也计数」无关。
	if denies < 3 {
		t.Fatalf("前提没搭对：只拒了 %d 次，用例要的是至少 3 次", denies)
	}
	if got := rt.calls.Load(); got != 0 {
		t.Fatalf("前提没搭对：工具真跑了 %d 次，用例要的是全被拒（一次都不跑）", got)
	}

	// 承重②：模型确实看到了提醒。
	found := false
	for _, m := range seen {
		if m == nil {
			continue
		}
		if v, _ := m.Annotations[agent_v2.RepeatToolReminderAnnotation].(bool); v {
			found = true
			break
		}
	}
	if !found {
		t.Error("连拒 3 次同一个调用，模型没收到任何提醒——" +
			"计数多半挂在「工具真的跑了」上，被拒的一次都没算进去")
	}
}

// TestRepeatTool_FrameworkToolsAreTransparent 钉住框架工具对链**既不计数也不重置**。
//
// 不透明的话，模型只要在两次重复之间插一个框架工具（activate_skill / ask_user…）
// 就能把链洗白，而它其实还在原地打转。
func TestRepeatTool_FrameworkToolsAreTransparent(t *testing.T) {
	rt := &recordTool{name: "biz_tool"}
	tk := agent_v2_newToolkit()
	tk.Register(rt)

	// 业务工具 → ask_user → 业务工具 → ask_user → 业务工具：
	// 业务那个连续调了 3 次（中间夹的框架工具应当透明）。
	resps := []*schema.Message{
		oneToolCall("biz_tool"),
		oneToolCallNamed(agent_v2.AskUserToolName, `{"question":"继续吗"}`),
		oneToolCall("biz_tool"),
		oneToolCallNamed(agent_v2.AskUserToolName, `{"question":"还继续吗"}`),
		oneToolCall("biz_tool"),
		schema.AssistantMessage("停"),
	}
	var seen []*schema.Message
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(10),
		agent_v2.WithAskUser(),
		agent_v2.WithRepeatToolReminder(&agent_v2.RepeatToolConfig{
			Enabled: true, Thresholds: []int{3},
		}),
		agent_v2.WithModel(newStubModelCapturing(resps, &seen)),
	)

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("干活"))
	// ask_user 会抛中断——一路答下去直到跑完。
	for i := 0; i < 4; i++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			break
		}
		_, err = ag.Resume(ctx, sess.RunID,
			agent_v2.WithResumeSession(sess),
			agent_v2.WithResumeUserInputs(map[string]*schema.Message{
				ie.Query.InterruptID: schema.UserMessage("继续"),
			}))
	}
	if err != nil {
		if _, isInt := compose_v2.IsInterrupt(err); !isInt {
			t.Fatalf("Run/Resume: %v", err)
		}
	}

	found := false
	for _, m := range seen {
		if m == nil {
			continue
		}
		if v, _ := m.Annotations[agent_v2.RepeatToolReminderAnnotation].(bool); v {
			found = true
			break
		}
	}
	if !found {
		t.Error("业务工具连续调了 3 次（中间夹着框架工具），却没喊——" +
			"框架工具把链重置了，等于给模型提供了洗白的办法")
	}
}

// oneToolCallNamed 造一个带指定参数的 tool_call 消息。
func oneToolCallNamed(name, args string) *schema.Message {
	return schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tc_" + name + "_" + strings.TrimPrefix(args, "{"), Type: "function",
			Function: schema.FunctionCall{Name: name, Arguments: args}},
	})
}

// capturingStubModel 按序返回固定输出，同时**捕获每轮真正喂给模型的消息流**。
//
// 捕获是承重的：这两条用例要断言的是「提醒有没有到模型眼前」，而那只能从模型侧看到
// ——从 agent 的返回值里看不见（提醒不是最终答复、也不进历史）。
type capturingStubModel struct {
	responses []*schema.Message
	idx       int
	seen      *[]*schema.Message
}

func newStubModelCapturing(responses []*schema.Message, seen *[]*schema.Message) *capturingStubModel {
	return &capturingStubModel{responses: responses, seen: seen}
}

func (m *capturingStubModel) Generate(_ context.Context, in []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	*m.seen = append(*m.seen, in...)
	if m.idx >= len(m.responses) {
		return schema.AssistantMessage("[stub exhausted]"), nil
	}
	out := m.responses[m.idx]
	m.idx++
	return out, nil
}

func (m *capturingStubModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *capturingStubModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*capturingStubModel)(nil)
