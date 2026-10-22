package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

func lastIsGateFeedback(msgs []*schema.Message, contains string) bool {
	if len(msgs) == 0 {
		return false
	}
	m := msgs[len(msgs)-1]
	return m.Role == schema.RoleUser && m.Annotations[agent_v2.TurnGateFeedbackAnnotation] == true &&
		strings.Contains(m.Content, contains)
}

// 闸门否决：反馈作为 user 消息接在答复后面、模型接着干；第二次放行就结束。闸门看得到这是第几次否决。
func TestTurnGate_VetoContinuesWithFeedback(t *testing.T) {
	var vetoesSeen []int
	gate := func(_ context.Context, in agent_v2.TurnGateInput) (string, error) {
		vetoesSeen = append(vetoesSeen, in.Vetoes)
		if strings.Contains(in.Final.Content, "价格") {
			return "", nil
		}
		return "答复里漏了价格", nil
	}
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		reply(schema.AssistantMessage("这是方案")),
		reply(schema.AssistantMessage("这是方案，价格 100 元")),
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("gate_basic"), agent_v2.WithModel(m), agent_v2.WithTurnGate(gate))
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("给个方案"))
	if err != nil {
		t.Fatal(err)
	}
	if m.count() != 2 {
		t.Fatalf("否决一次之后模型该再被调一次，实际 %d 次", m.count())
	}
	if !lastIsGateFeedback(m.call(1), "答复里漏了价格") {
		t.Errorf("第二次调模型时最后一条该是闸门反馈，实际 %+v", m.call(1))
	}
	if len(vetoesSeen) != 2 || vetoesSeen[0] != 0 || vetoesSeen[1] != 1 {
		t.Errorf("闸门两次看到的否决次数该是 [0 1]，实际 %v", vetoesSeen)
	}
	if final := agent_v2.FinalMessage(events); final == nil || !strings.Contains(final.Content, "价格") {
		t.Errorf("最终答复该是放行的那条，实际 %+v", final)
	}
}

// 否决次数到上限：最后一次否决的反馈带「最后一次」，之后不再问、直接放行。
func TestTurnGate_MaxVetoesStops(t *testing.T) {
	var gateCalls atomic.Int32
	gate := func(context.Context, agent_v2.TurnGateInput) (string, error) {
		gateCalls.Add(1)
		return "还不够好", nil // 永远不满意
	}
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		reply(schema.AssistantMessage("v1")), reply(schema.AssistantMessage("v2")), reply(schema.AssistantMessage("v3")),
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("gate_max"), agent_v2.WithModel(m),
		agent_v2.WithTurnGate(gate), agent_v2.WithTurnGateMaxVetoes(2))
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("写"))
	if err != nil {
		t.Fatal(err)
	}
	if gateCalls.Load() != 2 || m.count() != 3 {
		t.Fatalf("上限 2：闸门问 2 次、模型调 3 次，实际闸门 %d 次、模型 %d 次", gateCalls.Load(), m.count())
	}
	if !lastIsGateFeedback(m.call(2), "最后一次") {
		t.Errorf("最后一次否决的反馈该说明「最后一次」，实际 %+v", m.call(2))
	}
	if lastIsGateFeedback(m.call(1), "最后一次") {
		t.Error("第一次否决不该带「最后一次」")
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "v3" {
		t.Errorf("到上限之后放行 v3，实际 %+v", final)
	}
}

// 轮次预算的最后一轮不问闸门：否决了也没有下一轮（maxRounds 优先）。
func TestTurnGate_NotConsultedOnFinalRound(t *testing.T) {
	var gateCalls atomic.Int32
	tk := tool.NewToolkit()
	var n atomic.Int32
	tk.Register(countingFuncTool("step", &n, true))
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		reply(toolCallsMessage("step")),
		reply(schema.AssistantMessage("final")),
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("gate_final_round"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(1),
		agent_v2.WithTurnGate(func(context.Context, agent_v2.TurnGateInput) (string, error) {
			gateCalls.Add(1)
			return "再来", nil
		}))
	// 用满轮次预算本来就带着 MaxIterationsError 返回（答复在 events 里）——跟闸门无关，这里只看闸门问没问
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("做"))
	var mie *compose_v2.MaxIterationsError
	if err != nil && !errors.As(err, &mie) {
		t.Fatalf("只该是用满轮次的那个错，实际 %v", err)
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "final" {
		t.Errorf("最后一轮的答复该照常交出，实际 %+v", final)
	}
	if gateCalls.Load() != 0 {
		t.Errorf("最后一轮不该问闸门，实际问了 %d 次", gateCalls.Load())
	}
}

// 闸门里可以问人：停在闸门上，续跑只重跑闸门、不重调模型；对新答复再问时是一个新问题。
func TestTurnGate_HITLAskInGate(t *testing.T) {
	gate := func(ctx context.Context, in agent_v2.TurnGateInput) (string, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "这样发出去可以吗：" + in.Final.Content})
		if err != nil {
			return "", err
		}
		if ans.Content == "可以" {
			return "", nil
		}
		return "用户要求：" + ans.Content, nil
	}
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		reply(schema.AssistantMessage("草稿 v1")),
		reply(schema.AssistantMessage("草稿 v2")),
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("gate_hitl"), agent_v2.WithModel(m), agent_v2.WithTurnGate(gate))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("写个通知"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || !strings.Contains(ie.AllQueries()[0].Question, "草稿 v1") {
		t.Fatalf("该停在闸门对 v1 的提问上，实际 %v", err)
	}
	first := ie.AllQueries()[0].InterruptID
	_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{first: schema.UserMessage("语气再客气点")}))
	ie, ok = compose_v2.IsInterrupt(err)
	if !ok || !strings.Contains(ie.AllQueries()[0].Question, "草稿 v2") {
		t.Fatalf("改完之后该停在对 v2 的提问上，实际 %v", err)
	}
	if ie.AllQueries()[0].InterruptID == first {
		t.Fatal("对新答复的提问不该沿用上一问的 ID")
	}
	if m.count() != 2 {
		t.Fatalf("续跑只重跑闸门、不重调模型：此刻模型该被调 2 次，实际 %d", m.count())
	}
	events, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage("可以")}))
	if err != nil {
		t.Fatalf("批准之后该收尾，实际 %v", err)
	}
	if m.count() != 2 {
		t.Errorf("放行时不该再调模型，实际 %d 次", m.count())
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "草稿 v2" {
		t.Errorf("最终答复该是 v2，实际 %+v", final)
	}
}

// 闸门报错照常往上传，不被吞成「放行」。
func TestTurnGate_ErrorPropagates(t *testing.T) {
	boom := errors.New("评审服务挂了")
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage("x"))}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("gate_err"), agent_v2.WithModel(m),
		agent_v2.WithTurnGate(func(context.Context, agent_v2.TurnGateInput) (string, error) { return "", boom }))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("x")); !errors.Is(err, boom) {
		t.Fatalf("闸门的错该原样带出来，实际 %v", err)
	}
}

// 以工具直接收尾的回合不经过闸门（那是工具定义的结束）。
func TestTurnGate_ReturnDirectNotGated(t *testing.T) {
	var gateCalls atomic.Int32
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("respond", "respond", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: "直接答复", Metadata: map[string]any{tool.ReturnDirectMetaKey: true}}, nil
		}))
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(toolCallsMessage("respond"))}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("gate_rd"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithTurnGate(func(context.Context, agent_v2.TurnGateInput) (string, error) {
			gateCalls.Add(1)
			return "再来", nil
		}))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("x")); err != nil {
		t.Fatal(err)
	}
	if gateCalls.Load() != 0 {
		t.Errorf("return-direct 收尾不该问闸门，实际问了 %d 次", gateCalls.Load())
	}
}

// 闸门否决之后、模型下一轮之前又到的插话照常接进来；模型答复之后到的插话不算一次否决。
func TestTurnGate_LateInterjectionNotCountedAsVeto(t *testing.T) {
	inbox := agent_v2.NewInbox()
	var gateCalls atomic.Int32
	gate := func(_ context.Context, in agent_v2.TurnGateInput) (string, error) {
		gateCalls.Add(1)
		return "", nil
	}
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		func([]*schema.Message) (*schema.Message, error) {
			mustPush(t, inbox, "再加一条注意事项")
			return schema.AssistantMessage("初版"), nil
		},
		reply(schema.AssistantMessage("加上了")),
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("gate_late_inter"), agent_v2.WithModel(m), agent_v2.WithTurnGate(gate))
	events, err := ag.Run(agent_v2.WithInboxCtx(context.Background(), inbox), agent_v2.NewRunSession(), schema.UserMessage("写"))
	if err != nil {
		t.Fatal(err)
	}
	if m.count() != 2 || gateCalls.Load() != 1 {
		t.Fatalf("插话让模型再调一次、闸门只在最后那次答复上问一次，实际模型 %d 次、闸门 %d 次", m.count(), gateCalls.Load())
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "加上了" {
		t.Errorf("最终答复不对：%+v", final)
	}
}

// pushAfterModelHook 在 model 节点第一次执行结束之后（模型节点自己的迟到检查已经过了）往收件箱推一条插话。
type pushAfterModelHook struct {
	agent_v2.NoOpOrchestrationHooks
	inbox *agent_v2.Inbox
	text  string
	done  atomic.Bool
}

func (h *pushAfterModelHook) OnNodeEnd(ctx context.Context, nodeID string, _ error) context.Context {
	if nodeID == "model" && h.done.CompareAndSwap(false, true) {
		_ = h.inbox.Push(schema.UserMessage(h.text))
	}
	return ctx
}

// 模型答复之后、闸门之前到的插话由闸门节点接进来：回去回应它，这一次不问闸门、**不算一次否决**。
//
// 判别式：否决上限 1、闸门永远不满意。插话不算否决时闸门问 1 次、模型调 3 次；算的话闸门 0 次、模型 2 次。
func TestTurnGate_InterjectionAtGateNotCountedAsVeto(t *testing.T) {
	inbox := agent_v2.NewInbox()
	var gateCalls atomic.Int32
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		reply(schema.AssistantMessage("a1")), reply(schema.AssistantMessage("a2")), reply(schema.AssistantMessage("a3")),
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("gate_inter_at_gate"), agent_v2.WithModel(m),
		agent_v2.WithTurnGateMaxVetoes(1),
		agent_v2.WithTurnGate(func(context.Context, agent_v2.TurnGateInput) (string, error) {
			gateCalls.Add(1)
			return "还不行", nil
		}))
	ctx := agent_v2.WithCtxOrchHooks(agent_v2.WithInboxCtx(context.Background(), inbox),
		&pushAfterModelHook{inbox: inbox, text: "补充：截止周五"})
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(ctx, sess, schema.UserMessage("写")); err != nil {
		t.Fatal(err)
	}
	if gateCalls.Load() != 1 || m.count() != 3 {
		t.Fatalf("插话不算否决：闸门问 1 次、模型调 3 次，实际闸门 %d 次、模型 %d 次", gateCalls.Load(), m.count())
	}
	if !hasInterjection(m.call(1), "补充：截止周五") {
		t.Errorf("第二次调模型该看到闸门节点接进来的插话，实际 %+v", m.call(1))
	}
	// 闸门节点接进来的插话同样进会话事件（sanity-revert：闸门节点不登记它 ⟹ 事件里没有，这里红）
	recorded := false
	for _, ev := range sess.Events() {
		if ev.Output != nil && ev.Output.Message != nil && ev.Output.Message.Content == "补充：截止周五" {
			recorded = true
		}
	}
	if !recorded {
		t.Errorf("闸门节点接进来的插话该进会话事件，实际 %d 条事件里没有", len(sess.Events()))
	}
}

// 交给闸门的现场是拷贝：闸门改了答复、改了消息流，交给调用方的答复与记进历史的都不受影响。
//
// sanity-revert：把 channel 里的消息指针直接交给闸门 ⟹ 调用方拿到的答复被闸门改掉了，本用例红。
func TestTurnGate_InputIsACopy(t *testing.T) {
	ctx, hs, withHist := basicHistory("conv-gate-copy")
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage("original answer"))}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("gate_copy"), agent_v2.WithModel(m), withHist,
		agent_v2.WithTurnGate(func(_ context.Context, in agent_v2.TurnGateInput) (string, error) {
			in.Final.Content = "MUTATED_BY_GATE"
			for _, msg := range in.Messages {
				msg.Content = "MUTATED_BY_GATE"
			}
			return "", nil
		}))
	events, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话"))
	if err != nil {
		t.Fatal(err)
	}
	if f := agent_v2.FinalMessage(events); f == nil || f.Content != "original answer" {
		t.Errorf("闸门改它拿到的现场不该影响交给调用方的答复，实际 %+v", f)
	}
	for _, msg := range loadHist(t, hs, ctx, "conv-gate-copy") {
		if msg.Content == "MUTATED_BY_GATE" {
			t.Errorf("闸门改它拿到的现场不该影响会话历史，实际有 %s %q", msg.Role, msg.Content)
		}
	}
}

// 最后一次否决时框架补的那句「这是最后一次被要求继续」只给这一回合的模型看：它说的是这一回合的否决次数，
// 不进会话历史（下一个任务从 0 数，那句话对它不成立）；闸门的反馈本身照常进历史。
//
// sanity-revert：把那句话拼进反馈那条消息 ⟹ 历史里有它，本用例红。
func TestTurnGate_LastChanceNoteNotInHistory(t *testing.T) {
	ctx, hs, withHist := basicHistory("conv-gate-last")
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		reply(schema.AssistantMessage("a1")), reply(schema.AssistantMessage("a2")),
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("gate_last"), agent_v2.WithModel(m), withHist,
		agent_v2.WithTurnGateMaxVetoes(1),
		agent_v2.WithTurnGate(func(context.Context, agent_v2.TurnGateInput) (string, error) { return "漏了价格", nil }))
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("报价")); err != nil {
		t.Fatal(err)
	}
	const note = "这是最后一次被要求继续"
	sawNoteInTurn := false
	for _, msg := range m.call(1) {
		if strings.Contains(msg.Content, note) {
			sawNoteInTurn = true
		}
	}
	if !sawNoteInTurn {
		t.Fatalf("这一回合的模型该看到「最后一次」那句话，实际 %+v", m.call(1))
	}
	sawFeedback := false
	for _, msg := range loadHist(t, hs, ctx, "conv-gate-last") {
		if strings.Contains(msg.Content, note) {
			t.Errorf("「最后一次」那句话不该进会话历史，实际 %s %q", msg.Role, msg.Content)
		}
		if msg.Content == "漏了价格" {
			sawFeedback = true
		}
	}
	if !sawFeedback {
		t.Errorf("闸门的反馈本身该照常进会话历史（原文、不带那句话）")
	}
}
