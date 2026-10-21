package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// AgentAsTool 不看这次 Call 是被谁调起的（tools 节点直接调、组合工具体里调、装饰器 / 中间件转发、
// 钩子里调——调用栈上的名字可以被改写、转发、复用，框架分不可靠）：一律记下完成结果（只记最终
// 那条消息）、全历史输入只按位置定身份、不补记历史。tools 节点的工具账只在整个 tool_call 完成时
// 记，AgentAsTool 返回之后同一个 tool_call 还可能挂起——那时只有这份完成记录挡得住子 agent 重跑。

func resultText(r *tool.Result) string {
	if r == nil {
		return ""
	}
	s, _ := r.Output.(string)
	return s
}

// TestAgentAsTool_InsideCompositeTool_ApprovedDraftIsTheOneUsed：组合工具先经 AgentAsTool 起草、再问
// 用户采不采用。续跑时起草那次必须拿回，工具用的必须是用户看到并批准的那一版——重跑的话
// 模型型的子 agent 产出下一版，用户批准的是 v1、执行的是 v2，`err == nil`。
func TestAgentAsTool_InsideCompositeTool_ApprovedDraftIsTheOneUsed(t *testing.T) {
	var drafts atomic.Int32
	drafter := newTwoNodeAgent(t, "drafter",
		func(_ context.Context, c string) (string, error) { return c, nil },
		func(_ context.Context, _, _ string) (string, error) {
			return "草稿v" + string(rune('0'+drafts.Add(1))), nil
		})
	at := agent_v2.AgentAsTool(drafter, agent_v2.WithAgentToolDescription("起草"))
	var asked, used string
	tk := agent_v2_newToolkit()
	tk.Register(tool.NewFuncTool("draft_then_confirm", "起草并确认", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			res, err := at.Call(ctx, map[string]any{"input": "合同"})
			if err != nil {
				return nil, err
			}
			q := "采用 " + resultText(res) + " 吗？"
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: q})
			if err != nil {
				asked = q
				return nil, err
			}
			used = resultText(res)
			return tool.Success(used + "/" + ans.Content), nil
		}))
	host := agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithModel(newStubModel([]*schema.Message{oneToolCall("draft_then_confirm"), schema.AssistantMessage("ok")})))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, schema.UserMessage("起草合同"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应停在确认上，got %v", err)
	}
	if _, err := host.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("采用")})); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if n := drafts.Load(); n != 1 {
		t.Errorf("起草只该做一次（续跑时拿回），实际 %d 次", n)
	}
	if !strings.Contains(asked, used) || used == "" {
		t.Errorf("用户批准的是 %q，工具用的却是 %q", asked, used)
	}
}

// TestAgentAsTool_InsideCompositeTool_CompletedFirstCallRecalled：组合工具依次经两个 AgentAsTool 调
// 两个子 agent，第二个中断。续跑时第一个（已完成）拿回、第二个续自己的存档。
func TestAgentAsTool_InsideCompositeTool_CompletedFirstCallRecalled(t *testing.T) {
	runs := &countLog{}
	researcher := newTwoNodeAgent(t, "researcher",
		func(_ context.Context, c string) (string, error) { runs.inc("researcher"); return c, nil },
		func(_ context.Context, c, _ string) (string, error) { return "facts(" + c + ")", nil })
	approver := newTwoNodeAgent(t, "approver",
		func(_ context.Context, c string) (string, error) { runs.inc("approver"); return c, nil },
		func(ctx context.Context, c, _ string) (string, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准 " + c + "？"})
			if err != nil {
				return "", err
			}
			return "approved:" + ans.Content, nil
		})
	ta := agent_v2.AgentAsTool(researcher, agent_v2.WithAgentToolDescription("查"))
	tb := agent_v2.AgentAsTool(approver, agent_v2.WithAgentToolDescription("批"))
	tk := agent_v2_newToolkit()
	tk.Register(tool.NewFuncTool("pipeline", "查完再批", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			r1, err := ta.Call(ctx, map[string]any{"input": "Q"})
			if err != nil {
				return nil, err
			}
			r2, err := tb.Call(ctx, map[string]any{"input": resultText(r1)})
			if err != nil {
				return nil, err
			}
			return tool.Success(resultText(r2)), nil
		}))
	host := agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithModel(newStubModel([]*schema.Message{oneToolCall("pipeline"), schema.AssistantMessage("ok")})))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应停在批准上，got %v", err)
	}
	if _, err := host.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("yes")})); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if n := runs.get("researcher"); n != 1 {
		t.Errorf("已完成的第一个子 agent 在续跑时被重跑（%d 次）", n)
	}
	if n := runs.get("approver"); n != 1 {
		t.Errorf("挂起的第二个子 agent 没续上自己的存档（第一个节点执行 %d 次）", n)
	}
}

// agentToolInHook 在 name 这个 agent 的 OnAgentStart 里直接调一个 AgentAsTool。
type agentToolInHook struct {
	hook.NoOpHooks
	name string
	at   tool.Tool
}

func (h *agentToolInHook) OnAgentStart(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if info.Name != h.name {
		return ctx, nil
	}
	_, err := h.at.Call(ctx, map[string]any{"input": "预检"})
	return ctx, err
}

// TestAgentAsTool_InCalleeHookUnderToolCall_Recalled：父 BasicAgent 的 tool_call 调 AgentAsTool(worker)，
// worker 的 OnAgentStart 里再调一个 AgentAsTool(checker)——它不是 tool_call 调起的工具，只是祖先里
// 有 tool_call。worker 之后中断；续跑时 checker 已完成的那次必须拿回。
func TestAgentAsTool_InCalleeHookUnderToolCall_Recalled(t *testing.T) {
	runs := &countLog{}
	checker := newTwoNodeAgent(t, "checker",
		func(_ context.Context, c string) (string, error) { runs.inc("checker"); return c, nil },
		func(_ context.Context, _, _ string) (string, error) { return "checked", nil })
	pauseTK := agent_v2_newToolkit()
	pauseTK.Register(tool.NewFuncTool("pause", "pause", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"})
			if err != nil {
				return nil, err
			}
			return tool.Success(ans.Content), nil
		}))
	worker := agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithDescription("w"),
		agent_v2.WithHooks(&agentToolInHook{name: "worker",
			at: agent_v2.AgentAsTool(checker, agent_v2.WithAgentToolDescription("c"))}),
		agent_v2.WithToolkit(pauseTK), agent_v2.WithMaxRounds(4),
		agent_v2.WithModel(newStubModel([]*schema.Message{oneToolCall("pause"), schema.AssistantMessage("worker done")})))
	tk := agent_v2_newToolkit()
	tk.Register(agent_v2.AgentAsTool(worker))
	host := agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("w1", "worker", "做事")}),
			schema.AssistantMessage("host done")})))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应中断，got %v", err)
	}
	if _, err := host.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("是")})); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if n := runs.get("checker"); n != 1 {
		t.Errorf("worker 钩子里已完成的 checker 调用在续跑时被重跑（%d 次）", n)
	}
}

// newDrafter 造一个起草子 agent：每次执行产出下一版草稿（草稿v1、草稿v2……）。
func newDrafter(t *testing.T, drafts *atomic.Int32) agent_v2.Agent {
	return newTwoNodeAgent(t, "drafter",
		func(_ context.Context, c string) (string, error) { return c, nil },
		func(_ context.Context, _, _ string) (string, error) {
			return fmt.Sprintf("草稿v%d", drafts.Add(1)), nil
		})
}

// TestAgentAsTool_SameNameDecoratorAsksAfterInnerCall_DraftRecalled：同名装饰器（名字跟被包的 AgentAsTool
// 一样）转发之后问人采不采用。这次 tool_call 在装饰器里挂起、没完成，tools 节点的账里没有它——续跑时
// 起草那次必须由 AgentAsTool 自己的完成记录拿回。不记的话子 agent 重新起草：用户批准的是 v1，执行的
// 是 v2，`err == nil`（实测）。
func TestAgentAsTool_SameNameDecoratorAsksAfterInnerCall_DraftRecalled(t *testing.T) {
	var drafts atomic.Int32
	inner := agent_v2.AgentAsTool(newDrafter(t, &drafts), agent_v2.WithAgentToolDescription("起草"))
	var asked, used string
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool(inner.Name(), inner.Description(), inner.InputSchema(),
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			res, err := inner.Call(ctx, args) // 装饰器：转发，再让人确认
			if err != nil {
				return nil, err
			}
			q := "采用 " + resultText(res) + " 吗？"
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: q})
			if err != nil {
				asked = q
				return nil, err
			}
			used = resultText(res)
			return tool.Success(used + "/" + ans.Content), nil
		}))
	host := agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("c1", "drafter", "合同")}),
			schema.AssistantMessage("ok")})))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, schema.UserMessage("起草合同"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应停在确认上，got %v", err)
	}
	if _, err := host.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("采用")})); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if n := drafts.Load(); n != 1 {
		t.Errorf("起草只该做一次（续跑时拿回），实际 %d 次", n)
	}
	if used == "" || !strings.Contains(asked, used) {
		t.Errorf("用户批准的是 %q，工具用的却是 %q", asked, used)
	}
}

// TestAgentAsTool_MiddlewareAsksAfterNext_DraftRecalled：同一个形状换成工具中间件——next 之后问人采不
// 采用（中断是中间件的正当用途）。中间件包住的就是 AgentAsTool 本身，续跑时起草那次同样要拿回。
func TestAgentAsTool_MiddlewareAsksAfterNext_DraftRecalled(t *testing.T) {
	var drafts atomic.Int32
	var asked, used string
	confirm := func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
		return func(ctx context.Context, name string, args map[string]any) (*tool.Result, error) {
			res, err := next(ctx, name, args)
			if err != nil {
				return nil, err
			}
			q := "采用 " + resultText(res) + " 吗？"
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: q})
			if err != nil {
				asked = q
				return nil, err
			}
			used = resultText(res)
			return tool.Success(used + "/" + ans.Content), nil
		}
	}
	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(newDrafter(t, &drafts), agent_v2.WithAgentToolDescription("起草")))
	host := agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithToolMiddleware(confirm),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("c1", "drafter", "合同")}),
			schema.AssistantMessage("ok")})))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, schema.UserMessage("起草合同"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应停在确认上，got %v", err)
	}
	if _, err := host.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("采用")})); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if n := drafts.Load(); n != 1 {
		t.Errorf("起草只该做一次（续跑时拿回），实际 %d 次", n)
	}
	if used == "" || !strings.Contains(asked, used) {
		t.Errorf("用户批准的是 %q，工具用的却是 %q", asked, used)
	}
}

// TestAgentAsTool_EmptyToolCallIDs_CallsStayApart：有的 provider 不回 tool_call ID。同一批两个
// AgentAsTool(helper) 调用、输入相同（模型让同一个帮手把同一件事做两遍）、ID 都是空串——两次调用
// 必须各落各的存档槽、各问各的问题，靠批内下标区分。只在 ID 非空时才认「在 tool_call 里」的话，
// 两次都按「节点体里同一份输入的第 0 次」定身份（每个 tool_call 各有自己的计数器），落进同一个槽，
// 两条提问撞成同一个 ID，整个 Run 终局失败（实测）。
func TestAgentAsTool_EmptyToolCallIDs_CallsStayApart(t *testing.T) {
	var mu sync.Mutex
	var nss []string
	sub := newSingleNodeAgent(t, "helper", "work", nil, func(ctx context.Context, c string) (string, error) {
		mu.Lock()
		nss = append(nss, compose_v2.GetGraphNS(ctx))
		mu.Unlock()
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认 " + c + "？"})
		if err != nil {
			return "", err
		}
		return "done:" + c + "/" + ans.Content, nil
	})
	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(sub, agent_v2.WithAgentToolDescription("帮手")))
	host := agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("", "helper", "转账"), toolCall("", "helper", "转账")}),
			schema.AssistantMessage("好了"),
		})))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("两次调用各问一句，应中断，got %v", err)
	}
	if n := len(ie.AllQueries()); n != 2 {
		t.Fatalf("应有两条挂起的提问，got %d", n)
	}
	mu.Lock()
	got := append([]string(nil), nss...)
	mu.Unlock()
	if len(got) != 2 || got[0] == got[1] {
		t.Errorf("同一批的两次调用应各有自己的存档槽：%q", got)
	}
	answers := map[string]*schema.Message{}
	for _, q := range ie.AllQueries() {
		answers[q.InterruptID] = schema.UserMessage("是")
	}
	if _, err := host.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers)); err != nil {
		t.Fatalf("两条都答了，续跑应收敛：%v", err)
	}
}

// TestAgentAsTool_FullHistoryInsideComposite_ResumeConverges：组合工具体里调全历史输入的 AgentAsTool，
// 子 agent 问人中断。全历史输入由框架从 session 事件现拼，续跑不带 WithResumeSession 时拼出来的不一样
// ——按内容定身份就换槽：子 agent 从头跑、换个 ID 重问，最后在空输入上算出结果（实测）。它只能按位置
// 定身份，跟是不是 tools 节点直接调起的无关。
func TestAgentAsTool_FullHistoryInsideComposite_ResumeConverges(t *testing.T) {
	firsts := &countLog{}
	sub := newTwoNodeAgent(t, "lookup",
		func(_ context.Context, c string) (string, error) { firsts.inc("first"); return c, nil },
		func(ctx context.Context, c, _ string) (string, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认执行吗？"})
			if err != nil {
				return "", err
			}
			return "done(" + ans.Content + ") 处理的输入=[" + c + "]", nil
		})
	at := agent_v2.AgentAsTool(sub, agent_v2.WithAgentToolDescription("查一件事"), agent_v2.WithAgentToolFullHistoryInput())
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("lookup_and_log", "查一件事并记日志", map[string]any{"type": "object"},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			res, err := at.Call(ctx, args)
			if err != nil {
				return nil, err
			}
			return tool.Success("logged:" + resultText(res)), nil
		}))
	m := &scriptedToolModel{replies: []*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("call_0", "lookup_and_log", "x")}),
		schema.AssistantMessage("all done"),
	}}
	parent := agent_v2.NewBasicAgent(agent_v2.WithName("parent"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := parent.Run(ctx, sess, schema.UserMessage("帮我查一下订单"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在子 agent 的确认，got %v", err)
	}
	// 续跑故意不带 WithResumeSession：全历史输入拼出来的跟首跑不同
	if _, err := parent.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("是")})); err != nil {
		t.Fatalf("用户已回答，一次续跑就该收敛：%v", err)
	}
	if n := firsts.get("first"); n != 1 {
		t.Errorf("子 agent 第一个节点应只执行 1 次（续上自己的存档），实际 %d 次", n)
	}
	if r := m.lastResults(); len(r) != 1 || !strings.Contains(r[0], "处理的输入=[user: 帮我查一下订单]") {
		t.Errorf("父模型拿到的工具结果应基于首跑时的输入，got %q", r)
	}
}

// TestAgentAsTool_SameNameInsideCallee_Recalled：tool_call 调起的 AgentAsTool 叫 helper，它的被调方在
// OnAgentStart 里又经另一个 AgentAsTool 调了一个**也叫 helper** 的 agent。里面那次已完成的调用在被调方
// 续跑时要拿回——按名字判「是不是这次 tool_call 本身」的话，里面那次被认成外面那次、不记完成结果，
// 被调方续跑时它整个重跑。
func TestAgentAsTool_SameNameInsideCallee_Recalled(t *testing.T) {
	runs := &countLog{}
	innerHelper := newTwoNodeAgent(t, "helper",
		func(_ context.Context, c string) (string, error) { runs.inc("inner"); return c, nil },
		func(_ context.Context, _, _ string) (string, error) { return "checked", nil })
	pauseTK := agent_v2_newToolkit()
	pauseTK.Register(tool.NewFuncTool("pause", "pause", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"})
			if err != nil {
				return nil, err
			}
			return tool.Success(ans.Content), nil
		}))
	outerHelper := agent_v2.NewBasicAgent(agent_v2.WithName("helper"), agent_v2.WithDescription("h"),
		agent_v2.WithHooks(&agentToolInHook{name: "helper",
			at: agent_v2.AgentAsTool(innerHelper, agent_v2.WithAgentToolDescription("inner"))}),
		agent_v2.WithToolkit(pauseTK), agent_v2.WithMaxRounds(4),
		agent_v2.WithModel(newStubModel([]*schema.Message{oneToolCall("pause"), schema.AssistantMessage("helper done")})))
	tk := agent_v2_newToolkit()
	tk.Register(agent_v2.AgentAsTool(outerHelper))
	host := agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("h1", "helper", "做事")}),
			schema.AssistantMessage("host done")})))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应中断，got %v", err)
	}
	if _, err := host.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("是")})); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if n := runs.get("inner"); n != 1 {
		t.Errorf("被调方钩子里那次同名调用已完成，续跑时应拿回，实际执行 %d 次", n)
	}
}
