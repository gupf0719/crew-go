package agent_v2_test

import (
	"context"
	"errors"
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

// ── 工具钩子不是节点体：tools 节点那一格（工具账）不归它 ──────────────────────────────

// toolStartLedgerHook 在 OnToolStart 里既用 SaveNodeState 记「最近开始的工具」，又按 tool_call 用
// SaveStepResult 记「这个 tool_call 的预处理做过了」（业务 helper 在节点体、工具里也这么写）。
type toolStartLedgerHook struct {
	hook.NoOpHooks
	prepWork atomic.Int32 // 预处理真正做了几次（拿回记录时不算）
}

func (h *toolStartLedgerHook) OnToolStart(ctx context.Context, _ *hook.AgentInfo, ti *schema.ToolInput) (context.Context, error) {
	if err := compose_v2.SaveNodeState(ctx, "last tool: "+ti.Name); err != nil {
		return ctx, err
	}
	if _, ok := compose_v2.GetStepResult[string](ctx, "prep"); !ok {
		h.prepWork.Add(1)
		if err := compose_v2.SaveStepResult(ctx, "prep", "done"); err != nil {
			return ctx, err
		}
	}
	return ctx, nil
}

// TestBasicAgent_ToolHookStateDoesNotClobberToolLedger：一批两个 tool_call，扣款完成、确认中断。
// OnToolStart 钩子里的 SaveNodeState 写的是钩子自己那一格——续跑时 tools 节点的工具账还在，扣款
// 不重放；钩子按 tool_call 记的步骤续跑时读得回来（每个 tool_call 的预处理只做一次）。
func TestBasicAgent_ToolHookStateDoesNotClobberToolLedger(t *testing.T) {
	var charged atomic.Int32
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("charge", "扣款", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			charged.Add(1)
			return tool.Success("charged"), nil
		}))
	tk.Register(tool.NewFuncTool("confirm", "问人确认", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认？"})
			if err != nil {
				return nil, err
			}
			return tool.Success(ans.Content), nil
		}))
	h := &toolStartLedgerHook{}
	ba := agent_v2.NewBasicAgent(agent_v2.WithName("b"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3), agent_v2.WithHooks(h),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("c1", "charge", "x"), toolCall("c2", "confirm", "y")}),
			schema.AssistantMessage("好了"),
		})))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ba.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("want interrupt, got %v", err)
	}
	if _, err := ba.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("好")})); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if n := charged.Load(); n != 1 {
		t.Errorf("已完成的扣款在续跑时被重放（%d 次）——钩子里的 SaveNodeState 覆盖了 tools 节点的工具账", n)
	}
	if n := h.prepWork.Load(); n != 2 {
		t.Errorf("钩子按 tool_call 记的预处理续跑时应读得回来（两个 tool_call 各做 1 次，共 2 次），实际 %d 次", n)
	}
}

// renameOnResumeHook 续跑时把 send_sms 改写成 send_email（OnToolStart 的 Name 是[消费]档）。
type renameOnResumeHook struct {
	hook.NoOpHooks
	resumed atomic.Bool
}

func (h *renameOnResumeHook) OnToolStart(ctx context.Context, _ *hook.AgentInfo, ti *schema.ToolInput) (context.Context, error) {
	if h.resumed.Load() && ti.Name == "send_sms" {
		ti.Name = "send_email"
	}
	return ctx, nil
}

// TestBasicAgent_ToolRenamedOnResume_KeepsFirstTool_StepRecords：同一个 tool_call 续跑时被钩子改名到另一个
// 工具。这次调用的内容在这一批第一次执行时就定下了（fixToolCallContent），改名不生效：续跑的仍是 send_sms，
// 它记的「发过了」读得回来、不再发第二条，send_email 一次都不跑——用户回答的是短信的回执问题，改名生效的话
// 这个回答会落到发邮件上。
func TestBasicAgent_ToolRenamedOnResume_KeepsFirstTool_StepRecords(t *testing.T) {
	var sms, email atomic.Int32
	idempotentSend := func(ctx context.Context, do func()) error {
		if _, ok := compose_v2.GetStepResult[string](ctx, "sent"); ok {
			return nil
		}
		do()
		return compose_v2.SaveStepResult(ctx, "sent", "yes")
	}
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("send_sms", "发短信", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if err := idempotentSend(ctx, func() { sms.Add(1) }); err != nil {
				return nil, err
			}
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "短信发了，要回执吗？"}); err != nil {
				return nil, err
			}
			return tool.Success("sms ok"), nil
		}))
	tk.Register(tool.NewFuncTool("send_email", "发邮件", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if err := idempotentSend(ctx, func() { email.Add(1) }); err != nil {
				return nil, err
			}
			return tool.Success("email ok"), nil
		}))
	h := &renameOnResumeHook{}
	ba := agent_v2.NewBasicAgent(agent_v2.WithName("b"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3), agent_v2.WithHooks(h),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("c1", "send_sms", "x")}),
			schema.AssistantMessage("好了"),
		})))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ba.Run(ctx, sess, schema.UserMessage("通知用户"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("want interrupt, got %v", err)
	}
	h.resumed.Store(true)
	if _, err := ba.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("不用")})); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if n := email.Load(); n != 0 {
		t.Errorf("续跑时的改名不该生效，send_email 不该跑，实际发了 %d 封", n)
	}
	if n := sms.Load(); n != 1 {
		t.Errorf("短信只该发 1 条（续跑时读回自己记的「发过了」），实际 %d 条", n)
	}
}

// ── 被调方拿到的输入是一份独立的拷贝，连业务类型的标注值也是 ─────────────────────────

// typedTagRedactHook 在被调方的 OnAgentStart 里原地改写一个 []string 标注（脱敏类钩子的常见写法）。
type typedTagRedactHook struct {
	hook.NoOpHooks
	name string
}

func (h *typedTagRedactHook) OnAgentStart(ctx context.Context, info *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	if info.Name != h.name || len(in.Messages) == 0 {
		return ctx, nil
	}
	switch v := in.Messages[len(in.Messages)-1].Annotations["tags"].(type) {
	case []string:
		v[0] = "redacted"
	case []any:
		v[0] = "redacted"
	}
	return ctx, nil
}

// TestRunSubAgent_CalleeHookMutatingTypedAnnotationDoesNotLeak：调用方把一条带 []string 标注的消息交给
// RunSubAgent，被调方的钩子原地改了它。改写不能写回调用方——写回的话调用方存档里那条跟着变，续跑时
// 输入摘要不同、同一次调用被当成另一次：子 agent 从头跑、重问一遍。
func TestRunSubAgent_CalleeHookMutatingTypedAnnotationDoesNotLeak(t *testing.T) {
	firsts := &countLog{}
	cb := compose_v2.NewBuilder()
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(cb, outCh)
	cb.AddNode("first", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		firsts.inc("first")
		return nil, nil
	})
	cb.AddNode("second", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "ok?"})
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("done:"+ans.Content))}, nil
	})
	cb.AddEdge(compose_v2.StartNode, "first")
	cb.AddEdge("first", "second")
	cb.AddEdge("second", compose_v2.EndNode)
	cg, err := cb.Compile()
	if err != nil {
		t.Fatal(err)
	}
	callee := agent_v2.NewGraphAgent(cg, agent_v2.WithGraphAgentName("callee"),
		agent_v2.WithGraphAgentHooks(&typedTagRedactHook{name: "callee"}),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))

	var handed *schema.Message
	pb := compose_v2.NewBuilder()
	msgCh := compose_v2.LastValue[*schema.Message]("msg")
	resCh := compose_v2.LastValue[*schema.Message]("res")
	compose_v2.WithChannel(pb, msgCh)
	compose_v2.WithChannel(pb, resCh)
	pb.AddNode("prep", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		handed = &schema.Message{Role: schema.RoleUser, Content: "x", Annotations: map[string]any{"tags": []string{"orig"}}}
		return []compose_v2.ChannelWrite{compose_v2.Write(msgCh, handed)}, nil
	})
	pb.AddNode("call", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		m, _ := compose_v2.Get(chs, msgCh)
		if m == nil {
			return nil, errors.New("nil msg")
		}
		evs, err := agent_v2.RunSubAgent(ctx, "sub", callee, m)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(resCh, agent_v2.FinalMessage(evs))}, nil
	})
	pb.AddEdge(compose_v2.StartNode, "prep")
	pb.AddEdge("prep", "call")
	pb.AddEdge("call", compose_v2.EndNode)
	pg, err := pb.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parent := agent_v2.NewGraphAgent(pg, agent_v2.WithGraphAgentName("parent"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, resCh)
			return out
		}))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err = parent.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应中断：%v", err)
	}
	if got := handed.Annotations["tags"].([]string)[0]; got != "orig" {
		t.Errorf("被调方钩子的改写写回了调用方手里那条：tags[0] = %q", got)
	}
	evs, err := parent.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.Query.InterruptID: schema.UserMessage("yes")}))
	if err != nil {
		t.Fatalf("续跑应收敛（没有重问）：%v", err)
	}
	if got := nestedFinal(evs); got != "done:yes" {
		t.Errorf("final = %q，want done:yes", got)
	}
	if n := firsts.get("first"); n != 1 {
		t.Errorf("被调方应续上自己的存档（第一个节点只执行 1 次），实际 %d 次", n)
	}
}

// renameAndAuditHook：续跑时把 send_sms 改写成 send_email（OnToolStart）；OnToolEnd 对每个结果做一次
// 「审计」并按 key 记下（审过就不再审），c2 的 OnToolEnd 问人。
type renameAndAuditHook struct {
	hook.NoOpHooks
	resumed atomic.Bool
	mu      sync.Mutex
	audited []string
}

func (h *renameAndAuditHook) OnToolStart(ctx context.Context, _ *hook.AgentInfo, ti *schema.ToolInput) (context.Context, error) {
	if h.resumed.Load() && ti.Name == "send_sms" {
		ti.Name = "send_email"
	}
	return ctx, nil
}

func (h *renameAndAuditHook) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if _, ok := compose_v2.GetStepResult[string](ctx, "audited"); !ok {
		h.mu.Lock()
		h.audited = append(h.audited, out.CallId+":"+out.Result.Output)
		h.mu.Unlock()
		if err := compose_v2.SaveStepResult(ctx, "audited", out.Result.Output); err != nil {
			return ctx, err
		}
	}
	if out.CallId == "c2" {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "放行？"}); err != nil {
			return ctx, err
		}
	}
	return ctx, nil
}

// TestBasicAgent_ToolRenamedOnResume_KeepsFirstTool_ToolEndRecords：同一个 tool_call 在续跑时被 OnToolStart
// 改名到另一个工具。改名不生效（fixToolCallContent）：c1 仍是 send_sms，结果是首跑那份，OnToolEnd 在首跑时对它
// 记的「审过了」读得回来，不重审；新工具一次都不跑、没有要审的新结果。改名生效的话，新工具的结果要经 OnToolEnd
// 的另一个分域重审（审过的里出现 c1:email ok）。
func TestBasicAgent_ToolRenamedOnResume_KeepsFirstTool_ToolEndRecords(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("send_sms", "发短信", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("sms ok"), nil }))
	tk.Register(tool.NewFuncTool("send_email", "发邮件", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("email ok"), nil }))
	tk.Register(tool.NewFuncTool("noop", "noop", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("noop ok"), nil }))
	h := &renameAndAuditHook{}
	ba := agent_v2.NewBasicAgent(agent_v2.WithName("b"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3), agent_v2.WithHooks(h),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("c1", "send_sms", "x"), toolCall("c2", "noop", "y")}),
			schema.AssistantMessage("好了"),
		})))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ba.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("want interrupt, got %v", err)
	}
	h.resumed.Store(true)
	if _, err := ba.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("放行")})); err != nil {
		t.Fatalf("resume: %v", err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if got := strings.Join(h.audited, ","); got != "c1:sms ok,c2:noop ok" {
		t.Errorf("续跑时的改名不该生效：只应有首跑那两次审计，实际审过的 %q", h.audited)
	}
}

// TestBasicAgent_EmptyToolCallIDs_CompletedToolNotRerun：provider 不回 tool_call ID（空串）。同一批里一个
// 工具完成、另一个问人；续跑时完成的那个不能再执行一遍——工具账的键带着批内下标，空 ID 照样分得开，
// 只是曾经对空 ID 整个不记（实测扣款两次）。
func TestBasicAgent_EmptyToolCallIDs_CompletedToolNotRerun(t *testing.T) {
	var charges atomic.Int32
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("charge", "扣款", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			charges.Add(1)
			return tool.Success("charged"), nil
		}))
	tk.Register(tool.NewFuncTool("confirm", "问人", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"})
			if err != nil {
				return nil, err
			}
			return tool.Success(ans.Content), nil
		}))
	ba := agent_v2.NewBasicAgent(agent_v2.WithName("b"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("", "charge", "x"), toolCall("", "confirm", "y")}),
			schema.AssistantMessage("好了"),
		})))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ba.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("want interrupt, got %v", err)
	}
	if _, err := ba.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("好")})); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if n := charges.Load(); n != 1 {
		t.Errorf("已完成的扣款在续跑时不该再执行，实际扣了 %d 次", n)
	}
}
