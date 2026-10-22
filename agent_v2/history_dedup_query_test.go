package agent_v2_test

// 去重按条件问后端（见 history_read.go 的 findHistory）里，别的用例没钉住的两处：原始输入落在下一跳读 prior 的
// 那一段之外时，靠按消息 ID 查挡住重记；放弃只补被放弃的这个任务的悬空调用、不碰同一会话里还挂着的别的任务。

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestHistory_Handoff_InputOutsideReadWindow_ResumeNoDup：前一跳调了很多次工具（原始输入早已落在下一跳读 prior
// 的那一段之外），中途问了用户、续跑之后才转交。下一跳拿到同一条原始输入：它读回的 prior 里没有这条，续跑这次
// Run 的认领集也是新建的，挡住重记的只剩「按消息 ID 在本任务的记录里查」。用户原话只该有一份。
func TestHistory_Handoff_InputOutsideReadWindow_ResumeNoDup(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	rt := &recordTool{name: "lookup"}
	tk := agent_v2_newToolkit()
	tk.Register(rt)
	var script []*schema.Message
	for i := 0; i < 80; i++ {
		script = append(script, schema.AssistantToolCallMessage([]schema.ToolCall{{ID: fmt.Sprintf("l%d", i), Type: "function",
			Function: schema.FunctionCall{Name: "lookup", Arguments: fmt.Sprintf(`{"i":%d}`, i)}}}))
	}
	script = append(script,
		schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "ask", Type: "function",
			Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"转给账单吗"}`}}}),
		transferToolCall("tx", "billing"))
	front := agent_v2.NewBasicAgent(agent_v2.WithName("front"), agent_v2.WithToolkit(tk),
		agent_v2.WithModel(newStubModel(script)), agent_v2.WithAskUser(), agent_v2.WithHandoffTransfer(),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(200))
	billing := agent_v2.NewBasicAgent(agent_v2.WithName("billing"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("账单完成")})),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(5))
	ho := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("ho"), agent_v2.WithHandoffInitial("front")).
		AddAgent("front", front).AddAgent("billing", billing)
	ctx := compose_v2.WithCheckpointerCtx(compose_v2.WithThreadIDCtx(context.Background(), "conv-window"),
		compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ho.Run(ctx, sess, schema.UserMessage("USER_MARK"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应在 ask_user 中断，got %v", err)
	}
	if _, err := ho.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("转吧")})); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	got, err := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-window"})
	if err != nil {
		t.Fatal(err)
	}
	n, billingSpoke := 0, false
	for _, m := range got {
		if m.Role == schema.RoleUser && m.Content == "USER_MARK" {
			n++
		}
		if m.Content == "账单完成" {
			billingSpoke = true
		}
	}
	if !billingSpoke {
		t.Fatalf("fixture 没搭对：续跑之后应转交到 billing 并答复\n%s", dumpHist(got))
	}
	if len(got) < 160 {
		t.Fatalf("fixture 没搭对：前一跳的记录应远超下一跳读 prior 的那一段，实际 %d 条", len(got))
	}
	if n != 1 {
		t.Errorf("原始输入 %d 份, want 1\n%s", n, dumpHist(got))
	}
}

// TestHistory_AbortClosesOnlyOwnTaskCalls：同一会话里两个任务各挂着一个等审批的调用；放弃任务 B 只补 B 的那个。
// A 还活着——给它补一条「任务已被放弃」的结果，A 批准续跑之后同一个调用就有两条结果，配对坏了，模型下一轮读到
// 一个调用既被放弃又执行成功。
func TestHistory_AbortClosesOnlyOwnTaskCalls(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	rt := &recordTool{name: "do_thing"}
	tk := agent_v2_newToolkit()
	tk.Register(rt)
	tc := func(id string) *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: id, Type: "function",
			Function: schema.FunctionCall{Name: "do_thing", Arguments: `{"x":1}`}}})
	}
	mk := func(id string) agent_v2.Agent { // 两个任务各用一个实例，桩模型的脚本各走各的
		return agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithToolkit(tk),
			agent_v2.WithMaxRounds(4), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
			agent_v2.WithHistory(hs, "tenant1"),
			agent_v2.WithModel(newStubModel([]*schema.Message{tc(id), schema.AssistantMessage("final-" + id)})))
	}
	agA, agB := mk("tcA"), mk("tcB")
	ctx := compose_v2.WithCheckpointerCtx(compose_v2.WithThreadIDCtx(context.Background(), "c_abort_own"),
		compose_v2.NewInMemoryCheckpointer())
	key := agent_v2.HistoryKey{Owner: "tenant1", ConvID: "c_abort_own"}
	sessA, sessB := agent_v2.NewRunSession(), agent_v2.NewRunSession()
	_, err := agA.Run(ctx, sessA, schema.UserMessage("任务 A"))
	ieA, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("任务 A 应在审批中断，got %v", err)
	}
	if _, err := agB.Run(ctx, sessB, schema.UserMessage("任务 B")); err == nil {
		t.Fatal("任务 B 应在审批中断")
	}
	if _, err := agB.Resume(ctx, sessB.RunID, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort)); err != nil {
		t.Fatalf("放弃 B: %v", err)
	}
	msgs := spanLoadAll(t, ctx, hs, key)
	if n := toolResultsFor(msgs, "tcB"); n != 1 {
		t.Fatalf("放弃 B 之后它的调用应补上 1 条结果，实际 %d 条", n)
	}
	if n := toolResultsFor(msgs, "tcA"); n != 0 {
		t.Fatalf("放弃 B 给还挂着的任务 A 的调用补了 %d 条结果", n)
	}
	if _, err := agA.Resume(ctx, sessA.RunID, agent_v2.WithResumeSession(sessA),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ieA.AllQueries()[0].InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApprove),
		})); err != nil {
		t.Fatalf("续跑 A: %v", err)
	}
	if n := rt.calls.Load(); n != 1 {
		t.Fatalf("A 批准之后工具应执行 1 次，实际 %d 次", n)
	}
	msgs = spanLoadAll(t, ctx, hs, key)
	if n := toolResultsFor(msgs, "tcA"); n != 1 {
		t.Errorf("A 的调用有 %d 条结果, want 1", n)
		spanDump(t, msgs)
	}
}

// toolResultsFor 数历史里配对到 callID 的工具结果条数。
func toolResultsFor(msgs []*schema.Message, callID string) int {
	n := 0
	for _, m := range msgs {
		if m.Role == schema.RoleTool && schema.GetToolResultID(m) == callID {
			n++
		}
	}
	return n
}

// TestHistory_ReusedOutputInLaterTaskAtOtherSlotRecorded：执行体每次返回同一个消息对象（框架认可的写法），任务 1 路由
// 到分支 x、任务 2 路由到分支 y。任务 2 里 y 的那句回复跟任务 1 里 x 记下的是同一个消息 ID，但它是 y 这一次的
// 回复、不是透传，要以 y 的归属记下来。「这条产出是不是已经在别的归属下记过」按整场会话查时把它当成了透传：
// 会话停在任务 2 的提问上、没有回复（审计实测）。
func TestHistory_ReusedOutputInLaterTaskAtOtherSlotRecorded(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	canned := schema.AssistantMessage("固定回复")
	body := &constMsgAgent{name: "canned", out: canned}
	rt := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("rt"),
		agent_v2.WithRouter(func(_ context.Context, in *schema.Message) (compose_v2.NodeID, error) {
			if strings.Contains(in.Content, "x") {
				return "x", nil
			}
			return "y", nil
		})).AddBranch("x", body).AddBranch("y", body)
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-reuse-slot")
	ctx = agent_v2.WithHistoryCtx(ctx, hs, "t")
	if _, err := rt.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("问题一 x")); err != nil {
		t.Fatalf("任务 1: %v", err)
	}
	if _, err := rt.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("问题二 y")); err != nil {
		t.Fatalf("任务 2: %v", err)
	}
	msgs := spanLoadAll(t, ctx, hs, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-reuse-slot"})
	byOwner := map[string]int{}
	for _, m := range msgs {
		if m.Content == "固定回复" {
			o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
			byOwner[o]++
		}
	}
	if byOwner["x"] != 1 || byOwner["y"] != 1 {
		t.Errorf("固定回复按归属 %v, want x、y 各 1 份——任务 2 里 y 的回复被当成了透传", byOwner)
		spanDump(t, msgs)
	}
	if last := msgs[len(msgs)-1]; last.Role != schema.RoleAssistant {
		t.Errorf("会话以 %s %q 结尾——任务 2 的提问没有回复", last.Role, last.Content)
	}
}
