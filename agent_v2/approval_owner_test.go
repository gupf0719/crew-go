package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// 不变量：approve_always 只对答应它的那个 agent 生效（范围仍是一个任务）。工具名只在一个 agent 的工具表里唯一，别的
// agent 同名的可能是另一个工具；嵌套调用的被调方与编排成员跟父共用一个任务和会话 KV，规则不带 agent 身份时互相放行：
// 用户在沙箱子 agent 里对 delete 答的「一直允许」，父 agent 自己那个生产 delete 同样参数时静默执行（实测）；Sequential
// 里 a 答的「一直允许」，b 的同名工具不经询问就执行（实测）。

// askingAgent 是开了审批、有一个叫 delete 的工具、先调它再收尾的 BasicAgent。
func askingAgent(name string, del *recordTool) *agent_v2.BasicAgent {
	tk := agent_v2_newToolkit()
	tk.Register(del)
	return agent_v2.NewBasicAgent(
		agent_v2.WithName(name), agent_v2.WithDescription(name+" agent"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithModel(newStubModel([]*schema.Message{oneToolCall("delete"), schema.AssistantMessage(name + " done")})),
	)
}

// Sequential 两个成员各有一个叫 delete 的工具：a 里答的「一直允许」不放行 b 的 delete，b 照样要问。
func TestApproveAlways_SameNamedToolOfAnotherMemberAsksAgain(t *testing.T) {
	sandboxDel := &recordTool{name: "delete"}
	prodDel := &recordTool{name: "delete"}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("owner_seq")).
		AddStep("a", askingAgent("a", sandboxDel)).
		AddStep("b", askingAgent("b", prodDel))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := seq.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("前置：a 的 delete 应先问审批，got %v", err)
	}
	_, err = seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApproveAlways)}))
	if sandboxDel.calls.Load() != 1 {
		t.Fatalf("前置：a 的 delete 批准后应执行 1 次，实际 %d", sandboxDel.calls.Load())
	}
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("b 的 delete 是另一个工具，a 里答的「一直允许」不该放行它，应再问一次；got err=%v，b 的 delete 执行了 %d 次",
			err, prodDel.calls.Load())
	}
	if prodDel.calls.Load() != 0 {
		t.Errorf("b 的 delete 不该未经审批就执行，实际 %d 次", prodDel.calls.Load())
	}
}

// 嵌套调用（AgentAsTool）的被调方里答的「一直允许」不放行父 agent 自己的同名工具。
func TestApproveAlways_SubAgentRuleDoesNotApplyToParent(t *testing.T) {
	subDel := &recordTool{name: "delete"}
	sub := askingAgent("sandbox", subDel)
	parentDel := &recordTool{name: "delete"}
	tk := agent_v2_newToolkit()
	tk.Register(parentDel)
	tk.Register(agent_v2.AgentAsTool(sub))
	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "tc_sub", Type: "function",
				Function: schema.FunctionCall{Name: sub.Name(), Arguments: `{"input":"清理沙箱"}`}}}),
			oneToolCall("delete"),
			schema.AssistantMessage("parent done"),
		})),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := parent.Run(ctx, sess, schema.UserMessage("go"))
	answers := map[string]*schema.Message{}
	// 依次答：父 agent 调子 agent 那一次的审批答 approve，子 agent 里 delete 的审批答 approve_always；之后父 agent
	// 自己的 delete 再来问时停下检查
	for round := 0; round < 4; round++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			break
		}
		q := ie.AllQueries()[0]
		if subDel.calls.Load() == 1 {
			break // 子 agent 的 delete 已经执行过：这一问是父 agent 自己的 delete
		}
		ans := agent_v2.ApprovalOptionApprove
		if strings.Contains(q.Question, "delete") {
			ans = agent_v2.ApprovalOptionApproveAlways
		}
		answers[q.InterruptID] = schema.UserMessage(ans)
		_, err = parent.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	}
	if subDel.calls.Load() != 1 {
		t.Fatalf("前置：子 agent 的 delete 批准后应执行 1 次，实际 %d（err=%v）", subDel.calls.Load(), err)
	}
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("父 agent 的 delete 是另一个工具，子 agent 里答的「一直允许」不该放行它，应再问一次；got err=%v", err)
	}
	if parentDel.calls.Load() != 0 {
		t.Errorf("父 agent 的 delete 不该未经审批就执行，实际 %d 次", parentDel.calls.Load())
	}
}

// 同一个 agent 在同一个任务里照样记得：答过「一直允许」之后同工具同参数不再问。
func TestApproveAlways_SameAgentStillRemembers(t *testing.T) {
	del := &recordTool{name: "delete"}
	tk := agent_v2_newToolkit()
	tk.Register(del)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
		agent_v2.WithModel(newStubModel([]*schema.Message{oneToolCall("delete"), oneToolCall("delete"), schema.AssistantMessage("done")})),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("前置：应先问审批，got %v", err)
	}
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApproveAlways)})); err != nil {
		t.Fatalf("同一个 agent 答过「一直允许」之后同工具同参数不该再问：%v", err)
	}
	if del.calls.Load() != 2 {
		t.Errorf("两次 delete 都应执行，实际 %d 次", del.calls.Load())
	}
}

// 业务用整份覆盖 Config 的模板选项（`*c = tmpl`）建出来的两个 agent 仍是两个身份：a 里答的「一直允许」不放行 b。
// 身份曾在选项之前铸，模板选项把它冲成模板里的值（字面量就是空串），两个 agent 共用一份规则（审计实测）。
func TestApproveAlways_TemplateOptionDoesNotShareIdentity(t *testing.T) {
	sandboxDel := &recordTool{name: "delete"}
	prodDel := &recordTool{name: "delete"}
	// 几个 agent 共用的模板，先整份套上、再各自补名字与工具
	tmpl := agent_v2.Config{MaxRounds: 4, Hooks: hook.NoOpHooks{}, ApprovalMode: agent_v2.ApprovalAsk}
	fromTemplate := func(name string, del *recordTool) *agent_v2.BasicAgent {
		tk := agent_v2_newToolkit()
		tk.Register(del)
		return agent_v2.NewBasicAgent(
			func(c *agent_v2.Config) { *c = tmpl },
			agent_v2.WithName(name), agent_v2.WithToolkit(tk),
			agent_v2.WithModel(newStubModel([]*schema.Message{oneToolCall("delete"), schema.AssistantMessage(name + " done")})),
		)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("tmpl_seq")).
		AddStep("a", fromTemplate("a", sandboxDel)).
		AddStep("b", fromTemplate("b", prodDel))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := seq.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("前置：a 的 delete 应先问审批，got %v", err)
	}
	_, err = seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApproveAlways)}))
	if sandboxDel.calls.Load() != 1 {
		t.Fatalf("前置：a 的 delete 批准后应执行 1 次，实际 %d", sandboxDel.calls.Load())
	}
	if _, isInt := compose_v2.IsInterrupt(err); !isInt || prodDel.calls.Load() != 0 {
		t.Errorf("模板选项建的 b 不该拿到 a 的「一直允许」：err=%v，b 的 delete 执行了 %d 次", err, prodDel.calls.Load())
	}
}
