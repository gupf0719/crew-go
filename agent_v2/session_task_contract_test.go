package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// TestNextTask_KeepsSessionStateRotatesRunID 钉 `RunSession.RunID` 的契约：
// **一枚 RunID 只服务一个任务，而跨任务要攒的东西挂在对象上、不挂在 RunID 上**。
//
// 这两半必须同时成立，`NextTask` 才是一条可走的路——只换 ID 会让业务丢掉一路攒的
// 状态（于是它宁可不换），只保状态不换 ID 则根本没解决问题。
func TestNextTask_KeepsSessionStateRotatesRunID(t *testing.T) {
	sess := agent_v2.NewRunSession()
	first := sess.RunID
	sess.SetValue("k", "v")
	sess.AddEvent(agent_v2.NewOutputEvent("ag", schema.AssistantMessage("第一个任务说的话")))

	sess.NextTask()

	if sess.RunID == first {
		t.Fatal("RunID 没换——新任务会共用上个任务的存档槽，收尾时把对方的挂起档删掉")
	}
	if got, _ := sess.GetValue("k"); got != "v" {
		t.Errorf("values 被清了（got %v）。跨任务累积是 NextTask 存在的理由；"+
			"清掉的话业务只能退回「复用同一枚 RunID」，契约就没人遵守了", got)
	}
	if n := len(sess.History()); n != 1 {
		t.Errorf("events 被清了（剩 %d 条）。同上——这是业务复用 sess 对象的目的", n)
	}
}

// TestNextTask_PreviousTaskCheckpointSurvives 是这条契约的**承重**用例：
// 遵守它之后，上一个被放弃的任务的挂起存档不再被下一个任务的成功收尾毁掉。
//
// ⚠️ 任务 A **停在中断且不 Resume**（被放弃）是 fixture 的关键——跑完的话
// `cleanupOnSuccess` 会清掉 cp，就没有「残留可被毁掉」这回事，用例恒真。
func TestNextTask_PreviousTaskCheckpointSurvives(t *testing.T) {
	// script 让调用方决定这次 Run 的模型剧本。
	//
	// **Resume 那次必须只给 "done"**：`mk` 每次建新 stub，脚本从头开始——不区分的话
	// Resume 时模型会**再发一次同样的 tool_call**，而工具用的是自填常量 ID，第二次调用
	// 就落在同一个答案位上、被「一个答案只能满足一个提问」拦下。真实 LLM 在 Resume 时
	// 看到 tool_result 会给最终答复，不会重发同样的调用——这是 stub 被重置的假象。
	mkWith := func(id, q string, script []*schema.Message) *agent_v2.BasicAgent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("hold", "要人工确认", map[string]any{"type": "object"},
			func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
				if _, ok := compose_v2.GetResumeAnswerText(ctx, id); ok {
					return tool.Success("ok"), nil
				}
				return nil, &compose_v2.InterruptError{
					Query: &compose_v2.AskQuery{InterruptID: id, Question: q}}
			}))
		return agent_v2.NewBasicAgent(agent_v2.WithName("taskAg"),
			agent_v2.WithModel(newStubModel(script)),
			agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(5))
	}
	askThenDone := []*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "tc",
			Function: schema.FunctionCall{Name: "hold", Arguments: `{}`}}}),
		schema.AssistantMessage("done"),
	}
	mk := func(id, q string) *agent_v2.BasicAgent { return mkWith(id, q, askThenDone) }
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// 任务 A：停在中断，用户走了，不 Resume
	if _, err := mk("qA", "确认转账 5000？").Run(ctx, sess, schema.UserMessage("转账")); err == nil {
		t.Fatal("fixture 没搭对：A 该中断（不中断就没有残留可谈）")
	}
	taskA := sess.RunID
	if pend, _ := cper.ListPending(ctx, taskA); len(pend) != 1 {
		t.Fatalf("fixture 没搭对：A 该留下 1 条挂起档，实际 %d 条", len(pend))
	}

	// 按契约开新任务
	sess.NextTask()
	if _, err := mk("qB", "确认删除历史？").Run(ctx, sess, schema.UserMessage("查天气")); err == nil {
		t.Fatal("fixture 没搭对：B 也该中断")
	}
	// B 也放弃，换第三个任务并让它**跑成功**——成功收尾才会触发按 sid 清子树
	sess.NextTask()
	okAg := agent_v2.NewBasicAgent(agent_v2.WithName("taskAg"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("C done")})),
		agent_v2.WithMaxRounds(5))
	if _, err := okAg.Run(ctx, sess, schema.UserMessage("闲聊")); err != nil {
		t.Fatalf("C 该跑成功: %v", err)
	}

	// **承重的正向断言**：A 还等得到它的答案。
	pend, _ := cper.ListPending(ctx, taskA)
	if len(pend) != 1 {
		t.Fatalf("任务 A 的挂起档没了（剩 %d 条）——遵守了契约却仍被后来的任务毁掉，"+
			"那这条契约就是白钉的", len(pend))
	}
	ie, err := mkWith("qA", "确认转账 5000？", []*schema.Message{schema.AssistantMessage("done")}).Resume(ctx, taskA,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{"qA": schema.UserMessage("批准")}))
	if err != nil {
		t.Fatalf("A 该能被答完: %v", err)
	}
	if agent_v2.FinalMessage(ie) == nil {
		t.Error("A 续跑完该有最终产出——只断言「没报错」的话，静默 no-op 也会绿")
	}
}
