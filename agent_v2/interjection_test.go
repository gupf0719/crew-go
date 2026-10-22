package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// scriptModel 是按次脚本化的模型桩：第 i 次调用跑 steps[i]（拿到这次的输入、返回答复），并逐次记下输入。
// 调用次数超过脚本时返回 "[script exhausted]"。
type scriptModel struct {
	mu    sync.Mutex
	steps []func(in []*schema.Message) (*schema.Message, error)
	calls [][]*schema.Message
}

func (m *scriptModel) Generate(_ context.Context, in []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	i := len(m.calls)
	m.calls = append(m.calls, append([]*schema.Message(nil), in...))
	m.mu.Unlock()
	if i >= len(m.steps) {
		return schema.AssistantMessage("[script exhausted]"), nil
	}
	return m.steps[i](in)
}

func (m *scriptModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *scriptModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *scriptModel) call(i int) []*schema.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	if i >= len(m.calls) {
		return nil
	}
	return m.calls[i]
}

func (m *scriptModel) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func reply(msg *schema.Message) func([]*schema.Message) (*schema.Message, error) {
	return func([]*schema.Message) (*schema.Message, error) { return msg, nil }
}

func hasInterjection(msgs []*schema.Message, text string) bool {
	for _, m := range msgs {
		if m.Role == schema.RoleUser && m.Content == text && m.Annotations[agent_v2.InterjectionAnnotation] == true {
			return true
		}
	}
	return false
}

func mustPush(t *testing.T, b *agent_v2.Inbox, text string) {
	t.Helper()
	if err := b.Push(schema.UserMessage(text)); err != nil {
		t.Fatalf("Push: %v", err)
	}
}

// 工具执行期间到的插话：下一次调模型之前接进消息流，排在工具结果之后。
func TestInterjection_DeliveredBeforeNextModelCall(t *testing.T) {
	inbox := agent_v2.NewInbox()
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("search", "search", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			mustPush(t, inbox, "只看 2026 年的") // 用户在工具跑着的时候补了一句
			return tool.Success("results"), nil
		}))
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		reply(toolCallsMessage("search")),
		reply(schema.AssistantMessage("按 2026 年筛好了")),
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("inter_basic"), agent_v2.WithToolkit(tk), agent_v2.WithModel(m))
	events, err := ag.Run(agent_v2.WithInboxCtx(context.Background(), inbox), agent_v2.NewRunSession(), schema.UserMessage("查一下"))
	if err != nil {
		t.Fatal(err)
	}
	second := m.call(1)
	if len(second) == 0 || !hasInterjection(second[len(second)-1:], "只看 2026 年的") {
		t.Fatalf("第二次调模型时插话该排在最后（工具结果之后），实际 %+v", second)
	}
	if second[len(second)-2].Role != schema.RoleTool {
		t.Errorf("插话前面该是工具结果，实际 %+v", second[len(second)-2])
	}
	if inbox.Len() != 0 {
		t.Errorf("插话该被取走，收件箱还剩 %d 条", inbox.Len())
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "按 2026 年筛好了" {
		t.Errorf("最终答复不对：%+v", final)
	}
}

// 模型答复期间到的插话：不留到下一次 Run，接在答复后面再调一次模型。
func TestInterjection_LateArrivalContinuesTurn(t *testing.T) {
	inbox := agent_v2.NewInbox()
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		func([]*schema.Message) (*schema.Message, error) {
			mustPush(t, inbox, "顺便换成英文") // 模型生成的时候用户又说了一句
			return schema.AssistantMessage("好的，这是中文版"), nil
		},
		reply(schema.AssistantMessage("Here is the English version")),
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("inter_late"), agent_v2.WithModel(m))
	events, err := ag.Run(agent_v2.WithInboxCtx(context.Background(), inbox), agent_v2.NewRunSession(), schema.UserMessage("写一段介绍"))
	if err != nil {
		t.Fatal(err)
	}
	if m.count() != 2 {
		t.Fatalf("迟到的插话该让模型再被调一次，实际调了 %d 次", m.count())
	}
	second := m.call(1)
	if !hasInterjection(second[len(second)-1:], "顺便换成英文") || second[len(second)-2].Content != "好的，这是中文版" {
		t.Errorf("第二次调用该看到 [.., 第一次的答复, 插话]，实际 %+v", second)
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "Here is the English version" {
		t.Errorf("最终答复该是回应插话的那条，实际 %+v", final)
	}
}

// 被当成工具调用的子 agent 不取插话（用户在跟父 agent 说话）：父 agent 下一次调模型时才拿到。
//
// sanity-revert：runNestedCall 不摘收件箱 ⟹ 子 agent 的模型先拿走插话，本用例红。
func TestInterjection_NotTakenByNestedToolAgent(t *testing.T) {
	inbox := agent_v2.NewInbox()
	sub := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage("sub done"))}}
	helper := agent_v2.NewBasicAgent(agent_v2.WithName("helper"), agent_v2.WithDescription("helper"), agent_v2.WithModel(sub))
	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(helper))
	parent := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		func([]*schema.Message) (*schema.Message, error) {
			mustPush(t, inbox, "另外记得抄送我") // 父模型这一轮之后、子 agent 跑之前到的插话
			return toolCallsMessage("helper"), nil
		},
		reply(schema.AssistantMessage("parent done")),
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("inter_parent"), agent_v2.WithToolkit(tk), agent_v2.WithModel(parent))
	if _, err := ag.Run(agent_v2.WithInboxCtx(context.Background(), inbox), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatal(err)
	}
	if hasInterjection(sub.call(0), "另外记得抄送我") {
		t.Error("子 agent 不该拿到用户的插话")
	}
	if !hasInterjection(parent.call(1), "另外记得抄送我") {
		t.Error("父 agent 下一次调模型时该拿到插话")
	}
}

// 编排的内部决策 LLM（路由分类器）不取插话；正在对话的成员取。
//
// sanity-revert：runDecisionAgent 不摘收件箱 ⟹ 分类器先拿走插话，本用例红。
func TestInterjection_NotTakenByDecisionAgent(t *testing.T) {
	inbox := agent_v2.NewInbox()
	mustPush(t, inbox, "用正式的语气")
	clsModel := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage("writer"))}}
	classifier := agent_v2.NewBasicAgent(agent_v2.WithName("classifier"), agent_v2.WithModel(clsModel))
	writerModel := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage("written"))}}
	writer := agent_v2.NewBasicAgent(agent_v2.WithName("writer"), agent_v2.WithDescription("写东西"), agent_v2.WithModel(writerModel))
	ra := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("inter_router"), agent_v2.WithRoutingClassifier(classifier)).
		AddBranch("writer", writer)
	if _, err := ra.Run(agent_v2.WithInboxCtx(context.Background(), inbox), agent_v2.NewRunSession(), schema.UserMessage("写一封信")); err != nil {
		t.Fatal(err)
	}
	if hasInterjection(clsModel.call(0), "用正式的语气") {
		t.Error("路由分类器不该拿到用户的插话")
	}
	if !hasInterjection(writerModel.call(0), "用正式的语气") {
		t.Error("被路由到的成员该拿到插话")
	}
}

// 取走之后那次模型调用失败：插话已记进节点私有态，续跑时原样接回去（收件箱已经空了）。
func TestInterjection_SurvivesFailureResume(t *testing.T) {
	inbox := agent_v2.NewInbox()
	var fail atomic.Bool
	fail.Store(true)
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		func([]*schema.Message) (*schema.Message, error) {
			if fail.Load() {
				return nil, errors.New("upstream 503")
			}
			return schema.AssistantMessage("unused"), nil
		},
		reply(schema.AssistantMessage("ok")),
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("inter_resume"), agent_v2.WithModel(m))
	ctx := agent_v2.WithInboxCtx(compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer()), inbox)
	sess := agent_v2.NewRunSession()
	mustPush(t, inbox, "别用表格")
	if _, err := ag.Run(ctx, sess, schema.UserMessage("总结一下")); err == nil {
		t.Fatal("第一次调模型该失败")
	}
	if inbox.Len() != 0 {
		t.Fatalf("留下了可续的存档，插话随存档走、不该放回收件箱（放回了续跑时会接进两份），实际 %d", inbox.Len())
	}
	if _, err := ag.Resume(ctx, sess.RunID); err != nil {
		t.Fatalf("续跑该成功，实际 %v", err)
	}
	if got := countInterjection(m.call(1), "别用表格"); got != 1 {
		t.Errorf("续跑那次调模型该原样接回取走过的插话、恰好一次，实际 %d 次：%+v", got, m.call(1))
	}
}

// 插话重置工具重复调用的计数链：用户换了方向，之后同样的调用不算原地打转。
//
// sanity-revert：model 节点接进插话时不写空链 ⟹ 第二次 lookup 算第 2 次重复、喊出提醒，本用例红。
func TestInterjection_ResetsRepeatToolChain(t *testing.T) {
	inbox := agent_v2.NewInbox()
	var lookups atomic.Int32
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("lookup", "lookup", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			if lookups.Add(1) == 1 {
				mustPush(t, inbox, "换个关键词再查") // 第一次调用期间用户插话
			}
			return tool.Success("r"), nil
		}))
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		reply(toolCallsMessage("lookup")),
		reply(toolCallsMessage("lookup")),
		reply(schema.AssistantMessage("done")),
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("inter_repeat"), agent_v2.WithToolkit(tk), agent_v2.WithModel(m),
		agent_v2.WithRepeatToolReminder(&agent_v2.RepeatToolConfig{Enabled: true, Thresholds: []int{2}}))
	if _, err := ag.Run(agent_v2.WithInboxCtx(context.Background(), inbox), agent_v2.NewRunSession(), schema.UserMessage("查")); err != nil {
		t.Fatal(err)
	}
	for _, msg := range m.call(2) {
		if msg.Annotations[agent_v2.RepeatToolReminderAnnotation] == true {
			t.Fatal("插话之后的那次调用不该算重复、不该喊重复提醒")
		}
	}
}

// 插话只能是 user 角色。
func TestInbox_PushRejectsNonUser(t *testing.T) {
	b := agent_v2.NewInbox()
	if err := b.Push(schema.AssistantMessage("x")); err == nil {
		t.Error("assistant 角色的插话该被拒")
	}
	if err := b.Push(nil); err == nil {
		t.Error("nil 该被拒")
	}
	orig := schema.UserMessage("hi")
	if err := b.Push(orig); err != nil {
		t.Fatal(err)
	}
	orig.Content = "changed"
	got := b.Drain()
	if len(got) != 1 || got[0].Content != "hi" || got[0].Annotations[agent_v2.InterjectionAnnotation] != true {
		t.Errorf("收进来的该是打了标记的拷贝，实际 %+v", got)
	}
	if strings.Contains(orig.Content, "hi") || orig.Annotations[agent_v2.InterjectionAnnotation] == true {
		t.Error("不该改调用方的对象")
	}
}

// 并发的成员（Parallel 的分支）谁先到谁取：一条插话只被取一次。用 -race 跑查收件箱的并发安全。
func TestInterjection_ParallelBranchesTakeOnce(t *testing.T) {
	inbox := agent_v2.NewInbox()
	mustPush(t, inbox, "注意保密")
	mk := func(name string) (*agent_v2.BasicAgent, *scriptModel) {
		m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){reply(schema.AssistantMessage(name + " done"))}}
		return agent_v2.NewBasicAgent(agent_v2.WithName(name), agent_v2.WithModel(m)), m
	}
	a, am := mk("branch_a")
	b, bm := mk("branch_b")
	pa := agent_v2.NewParallelAgent(agent_v2.WithParallelName("inter_parallel")).AddBranch("a", a).AddBranch("b", b)
	if _, err := pa.Run(agent_v2.WithInboxCtx(context.Background(), inbox), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, m := range []*scriptModel{am, bm} {
		if hasInterjection(m.call(0), "注意保密") {
			seen++
		}
	}
	if seen != 1 {
		t.Errorf("一条插话只该被一个分支取走，实际 %d 个分支看到", seen)
	}
}

// countInterjection 数 msgs 里 text 那条插话出现了几次。
func countInterjection(msgs []*schema.Message, text string) int {
	n := 0
	for _, m := range msgs {
		if m.Role == schema.RoleUser && m.Content == text && m.Annotations[agent_v2.InterjectionAnnotation] == true {
			n++
		}
	}
	return n
}

// 没配存档时，取走插话的那次模型调用失败、Run 以错误结束：这次 Run 的进度整个作废，插话回到收件箱，
// 下一次 Run 再取到它。
//
// sanity-revert：Run 出口不放回 ⟹ 收件箱是空的、插话没了，本用例红。
func TestInterjection_GivenBackWhenRunCannotResume(t *testing.T) {
	inbox := agent_v2.NewInbox()
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		func([]*schema.Message) (*schema.Message, error) { return nil, errors.New("upstream 503") },
		reply(schema.AssistantMessage("ok")),
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("inter_giveback"), agent_v2.WithModel(m))
	ctx := agent_v2.WithInboxCtx(context.Background(), inbox)
	mustPush(t, inbox, "别用表格")
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("总结一下")); err == nil {
		t.Fatal("第一次调模型该失败")
	}
	if !hasInterjection(m.call(0), "别用表格") {
		t.Fatalf("失败的那次调用该已经取走插话（否则本用例测不到放回），实际 %+v", m.call(0))
	}
	if inbox.Len() != 1 {
		t.Fatalf("没有存档可续，取走的插话该回到收件箱，实际剩 %d 条", inbox.Len())
	}
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("总结一下")); err != nil {
		t.Fatal(err)
	}
	if got := countInterjection(m.call(1), "别用表格"); got != 1 {
		t.Errorf("重跑那次调模型该恰好看到一次插话，实际 %d 次：%+v", got, m.call(1))
	}
}

// 插话已经接进消息流、模型也回应过了，之后的一步失败、Run 又没有存档可续：这次 Run 连同那些插话一起作废，
// 取走的按原顺序放回收件箱最前面（排在取走之后才到的前面）。三条文本各不相同：第一次工具调用时到 FIRST、第二次
// 到 SECOND，都被接进消息流；失败那次模型调用里到 THIRD，没被取走。
//
// sanity-revert：只放回没提交的 / 只放回最后一次取的 / 放到收件箱末尾 / 账覆盖不追加 ⟹ 收件箱不等于
// [FIRST SECOND THIRD]，本用例红。
func TestInterjection_CommittedButRunDiscardedGivenBack(t *testing.T) {
	inbox := agent_v2.NewInbox()
	var searches atomic.Int32
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("search", "search", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			if searches.Add(1) == 1 {
				mustPush(t, inbox, "FIRST")
			} else {
				mustPush(t, inbox, "SECOND")
			}
			return tool.Success("results"), nil
		}))
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		reply(toolCallsMessage("search")),
		reply(toolCallsMessage("search")), // 看到 FIRST、又调了一次工具
		func([]*schema.Message) (*schema.Message, error) { // 看到 SECOND；这次调用期间又到 THIRD，然后失败
			mustPush(t, inbox, "THIRD")
			return nil, errors.New("upstream 503")
		},
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("inter_discarded"), agent_v2.WithToolkit(tk), agent_v2.WithModel(m))
	if _, err := ag.Run(agent_v2.WithInboxCtx(context.Background(), inbox), agent_v2.NewRunSession(), schema.UserMessage("查")); err == nil {
		t.Fatal("第三次调模型该失败")
	}
	if !hasInterjection(m.call(1), "FIRST") || !hasInterjection(m.call(2), "SECOND") {
		t.Fatalf("FIRST、SECOND 该分别接进第二、三次调用（否则本用例测不到「提交之后」），实际 %+v / %+v", m.call(1), m.call(2))
	}
	var got []string
	for _, msg := range inbox.Drain() {
		got = append(got, msg.Content)
	}
	if strings.Join(got, ",") != "FIRST,SECOND,THIRD" {
		t.Errorf("这次 Run 整个作废，取走的按原顺序放回最前面、排在没取走的前面，收件箱该是 [FIRST SECOND THIRD]，实际 %v", got)
	}
}

// 开了会话历史、没配存档时编排的成员重试：第一次尝试已经把插话接进消息流、记进了会话历史，后面一步失败——重来
// 那次从历史上文里看得到它，不该再放回收件箱（放回的话上文一份、新取一份，模型看到两遍）。
//
// sanity-revert：放回时不看会话历史 ⟹ 重来那次的模型输入里插话出现两次，本用例红。
func TestInterjection_RecordedInHistoryNotGivenBack(t *testing.T) {
	inbox := agent_v2.NewInbox()
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("search", "search", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("results"), nil }))
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		reply(toolCallsMessage("search")), // 第一次尝试：取走插话、调工具
		func([]*schema.Message) (*schema.Message, error) { return nil, errors.New("upstream 503") },
		reply(schema.AssistantMessage("done")), // 重试
	}}
	step := agent_v2.NewBasicAgent(agent_v2.WithName("hist_retry_step"), agent_v2.WithToolkit(tk), agent_v2.WithModel(m))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("hist_retry_seq"), agent_v2.WithSequentialMaxRetries(1)).
		AddStep("only", step)
	ctx, _ := historyCtx("conv-inter-retry")
	ctx = agent_v2.WithInboxCtx(ctx, inbox)
	mustPush(t, inbox, "用英文")
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("查")); err != nil {
		t.Fatalf("重试之后该成功，实际 %v", err)
	}
	if !hasInterjection(m.call(0), "用英文") {
		t.Fatalf("第一次尝试该已取走插话，实际 %+v", m.call(0))
	}
	if got := countInterjection(m.call(2), "用英文"); got != 1 {
		t.Errorf("重来那次的模型输入里插话该恰好一次（来自会话历史），实际 %d 次：%+v", got, m.call(2))
	}
	if inbox.Len() != 0 {
		t.Errorf("记进会话历史的插话不该放回收件箱，实际剩 %d 条", inbox.Len())
	}
}

// 开了会话历史时，取走之后那次模型调用就失败了的插话没进过历史，照样放回收件箱（只跳过记进历史的那些）。
//
// sanity-revert：开了历史就一概不放回 ⟹ 收件箱是空的、这条插话没了，本用例红。
func TestInterjection_UnrecordedGivenBackWithHistory(t *testing.T) {
	inbox := agent_v2.NewInbox()
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		func([]*schema.Message) (*schema.Message, error) { return nil, errors.New("upstream 503") },
	}}
	ctx, _, withHist := basicHistory("conv-inter-unrecorded")
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("inter_unrecorded"), agent_v2.WithModel(m), withHist)
	mustPush(t, inbox, "别用表格")
	if _, err := ag.Run(agent_v2.WithInboxCtx(ctx, inbox), agent_v2.NewRunSession(), schema.UserMessage("总结一下")); err == nil {
		t.Fatal("调模型该失败")
	}
	if !hasInterjection(m.call(0), "别用表格") {
		t.Fatalf("失败的那次调用该已经取走插话，实际 %+v", m.call(0))
	}
	if inbox.Len() != 1 {
		t.Errorf("没进过会话历史的插话该放回收件箱，实际剩 %d 条", inbox.Len())
	}
}

// 编排的成员重试（没配存档，第二次尝试从头跑）：第一次尝试取走插话之后失败，第二次尝试照样看得到它。
//
// sanity-revert：Run 出口不放回 ⟹ 第二次尝试的模型输入里没有插话，本用例红。
func TestInterjection_MemberRetryWithoutCheckpointerRetakes(t *testing.T) {
	inbox := agent_v2.NewInbox()
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		func([]*schema.Message) (*schema.Message, error) { return nil, errors.New("upstream 503") },
		reply(schema.AssistantMessage("ok")),
	}}
	step := agent_v2.NewBasicAgent(agent_v2.WithName("inter_retry_step"), agent_v2.WithModel(m))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("inter_retry_seq"), agent_v2.WithSequentialMaxRetries(1)).
		AddStep("only", step)
	mustPush(t, inbox, "别用表格")
	if _, err := seq.Run(agent_v2.WithInboxCtx(context.Background(), inbox), agent_v2.NewRunSession(), schema.UserMessage("总结一下")); err != nil {
		t.Fatalf("重试之后该成功，实际 %v", err)
	}
	if m.count() != 2 {
		t.Fatalf("该调两次模型（失败一次、重试一次），实际 %d 次", m.count())
	}
	if got := countInterjection(m.call(1), "别用表格"); got != 1 {
		t.Errorf("重试那次调模型该恰好看到一次插话，实际 %d 次：%+v", got, m.call(1))
	}
	if inbox.Len() != 0 {
		t.Errorf("重试成功之后插话该被取走，收件箱还剩 %d 条", inbox.Len())
	}
}

// 配了存档但这次 Run 终局失败（ErrRunAborted：存档清掉、不能续）：取走的插话回到收件箱。
//
// sanity-revert：出口只看「有没有 checkpointer」⟹ 终局失败时插话随清掉的存档一起没了，本用例红。
func TestInterjection_TerminalFailureGivesBack(t *testing.T) {
	inbox := agent_v2.NewInbox()
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		func([]*schema.Message) (*schema.Message, error) {
			return nil, fmt.Errorf("bad request: %w", compose_v2.ErrRunAborted)
		},
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("inter_terminal"), agent_v2.WithModel(m))
	ctx := agent_v2.WithInboxCtx(compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer()), inbox)
	mustPush(t, inbox, "别用表格")
	_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("总结一下"))
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("该终局失败，实际 %v", err)
	}
	if !hasInterjection(m.call(0), "别用表格") {
		t.Fatalf("失败的那次调用该已经取走插话，实际 %+v", m.call(0))
	}
	if inbox.Len() != 1 {
		t.Fatalf("终局失败、存档清掉，取走的插话该回到收件箱，实际剩 %d 条", inbox.Len())
	}
}

// 不开会话历史时，插话照样进会话事件：下一个任务从事件里读到的上文有它，而且排在它回应的那条答复之前。
//
// sanity-revert：收尾时不把插话写进事件 ⟹ 第二个任务的上文里只有答复、没有它回应的那句话，本用例红。
func TestInterjection_RecordedInSessionEvents(t *testing.T) {
	inbox := agent_v2.NewInbox()
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("search", "search", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			mustPush(t, inbox, "以后都用英文回答")
			return tool.Success("results"), nil
		}))
	m := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		reply(toolCallsMessage("search")),
		reply(schema.AssistantMessage("Here is the English answer")),
		reply(schema.AssistantMessage("Again in English")),
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("inter_events"), agent_v2.WithToolkit(tk), agent_v2.WithModel(m))
	ctx := agent_v2.WithInboxCtx(context.Background(), inbox)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(ctx, sess, schema.UserMessage("介绍一下你自己")); err != nil {
		t.Fatal(err)
	}
	sess.NextTask()
	if _, err := ag.Run(ctx, sess, schema.UserMessage("再说一遍")); err != nil {
		t.Fatal(err)
	}
	prior := m.call(2)
	interAt, answerAt := -1, -1
	for i, msg := range prior {
		switch {
		case msg.Role == schema.RoleUser && msg.Content == "以后都用英文回答":
			interAt = i
		case msg.Role == schema.RoleAssistant && msg.Content == "Here is the English answer":
			answerAt = i
		}
	}
	if interAt < 0 {
		t.Fatalf("第二个任务的上文里该有上一个任务的插话，实际 %+v", prior)
	}
	if answerAt < 0 || interAt > answerAt {
		t.Errorf("插话该排在它回应的那条答复之前：插话 #%d、答复 #%d，%+v", interAt, answerAt, prior)
	}
}

// Push 收进来的是独立的一份：调用方之后改原消息的内容块，收件箱里那份不变。
//
// sanity-revert：只浅拷结构体 ⟹ 内容块指针共享、改原件跟着变，本用例红。
func TestInbox_PushCopiesContentBlocks(t *testing.T) {
	inbox := agent_v2.NewInbox()
	msg := schema.UserMessage("看图")
	msg.ContentBlocks = []*schema.ContentBlock{{Type: "text", Text: &schema.TextContentBlock{Type: "text", Text: "原文"}}}
	if err := inbox.Push(msg); err != nil {
		t.Fatal(err)
	}
	msg.ContentBlocks[0].Text.Text = "改过了"
	got := inbox.Drain()
	if len(got) != 1 || got[0].ContentBlocks[0].Text.Text != "原文" {
		t.Fatalf("收件箱里那份该不受原件改动影响，实际 %+v", got[0].ContentBlocks[0].Text)
	}
}

// Push 拒收存不进存档的插话（标注里有 JSON 编不了的值）：当场报错，而不是让之后每次 Run 都失败在取插话那一步。
//
// sanity-revert：Push 不校验 ⟹ 这里返回 nil，本用例红。
func TestInbox_PushRejectsUnencodable(t *testing.T) {
	inbox := agent_v2.NewInbox()
	msg := schema.UserMessage("x")
	msg.Annotations = map[string]any{"score": math.NaN()}
	if err := inbox.Push(msg); err == nil {
		t.Fatal("标注里有 NaN（JSON 编不了）该被拒收")
	}
	if inbox.Len() != 0 {
		t.Errorf("拒收的不该进收件箱，实际 %d 条", inbox.Len())
	}
}
