package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// 已完成的嵌套调用被拿回（父节点中断之后重跑）时，被调方当初交给上层 HandoffAgent / RoutingAgent
// 的转交与路由决定要原样交出——它们不在 events 里（是上行信号，见 side_signal.go），拿回时子 agent
// 不执行、不会再做一次，不随完成记录记下就静默丢了，`err == nil`。
//
// 断言两件事：最终走对了分支（承重的量），以及被调方确实只执行了一次（确认走的是「拿回 + 重放」
// 这条路，而不是被调方重跑一遍碰巧把信号又写了一次）。

// escHost 单节点 GraphAgent：节点读输入文本交给 handle，把返回写成产出。
func escHost(t *testing.T, name string, handle func(ctx context.Context, c string) (string, error)) *agent_v2.GraphAgent {
	t.Helper()
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("host", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		if in == nil {
			return nil, errors.New("nil input")
		}
		out, err := handle(ctx, in.Content)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage(out))}, nil
	})
	b.AddEdge(compose_v2.StartNode, "host")
	b.AddEdge("host", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName(name),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			o, _ := compose_v2.Get(chs, outCh)
			return o
		}),
	)
}

// escDrive 跑到收敛（最多 maxResumes 次续跑），每次停下都答「是」（答案累积传）。
func escDrive(t *testing.T, ag agent_v2.Agent, input string, maxResumes int) (string, error) {
	t.Helper()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	answers := map[string]*schema.Message{}
	events, err := ag.Run(ctx, sess, schema.UserMessage(input))
	for i := 0; i < maxResumes && err != nil; i++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			break
		}
		for _, q := range ie.AllQueries() {
			answers[q.InterruptID] = schema.UserMessage("是")
		}
		events, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers), agent_v2.WithResumeSession(sess))
	}
	final := "<no final>"
	if m := agent_v2.FinalMessage(events); m != nil {
		final = m.Content
	}
	return final, err
}

// escReplyAgent 固定回复（产 ExitEvent），记被调次数。
type escReplyAgent struct {
	name  string
	reply string
	runs  atomic.Int32
}

func (a *escReplyAgent) Name() string        { return a.name }
func (a *escReplyAgent) Description() string { return "reply " + a.name }
func (a *escReplyAgent) Run(_ context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.runs.Add(1)
	ev := agent_v2.NewExitEvent(a.name, schema.AssistantMessage(a.reply))
	if sess != nil {
		sess.AddEvent(ev)
	}
	return []*agent_v2.AgentEvent{ev}, nil
}
func (a *escReplyAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

// escToolCallModel 每次都吐同一个 tool_call（看到 tool 结果后给文本），记被调次数。
type escToolCallModel struct {
	tool, args string
	calls      atomic.Int32
}

func (m *escToolCallModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.calls.Add(1)
	if len(msgs) > 0 && msgs[len(msgs)-1].Role == schema.RoleTool {
		return schema.AssistantMessage("done"), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "call_1", Type: "function",
		Function: schema.FunctionCall{Name: m.tool, Arguments: m.args}}}), nil
}
func (m *escToolCallModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *escToolCallModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// escFrontdesk 是 HandoffAgent 的一个候选（自研 agent）：把转交能力转授给内层分诊、经 RunSubAgent
// 调它，然后在同一次成员执行里请人确认。
type escFrontdesk struct {
	triage agent_v2.Agent
	runs   atomic.Int32
}

func (a *escFrontdesk) Name() string        { return "frontdesk" }
func (a *escFrontdesk) Description() string { return "frontdesk" }
func (a *escFrontdesk) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *escFrontdesk) Run(ctx context.Context, _ *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.runs.Add(1)
	if _, err := agent_v2.RunSubAgent(ctx, "triage", a.triage, in, agent_v2.RegrantHandoffTo(opts, a, a.triage)...); err != nil {
		return nil, err
	}
	if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认转接？"}); err != nil {
		return nil, err
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent("frontdesk", schema.AssistantMessage("前台自己答了"))}, nil
}

// TestNestedCall_RecalledTransferToolStillHandsOff：内层 LLM 经 transfer_to_agent 转交给账单，
// 前台随后请人确认（中断）。续跑时那次分诊调用被拿回，转交必须照样生效。
func TestNestedCall_RecalledTransferToolStillHandsOff(t *testing.T) {
	triageModel := &escToolCallModel{tool: agent_v2.HandoffTransferToolName, args: `{"agent_name":"billing"}`}
	triage := agent_v2.NewBasicAgent(
		agent_v2.WithName("triage_llm"),
		agent_v2.WithModel(triageModel),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(3),
	)
	fd := &escFrontdesk{triage: triage}
	billing := &escReplyAgent{name: "billing", reply: "BILLING 已处理"}
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("desk"),
		agent_v2.WithHandoffInitial("frontdesk"),
	).AddAgent("frontdesk", fd).AddAgent("billing", billing)
	final, err := escDrive(t, h, "我要退款", 3)
	if err != nil {
		t.Fatalf("应收敛：%v", err)
	}
	if !strings.Contains(final, "BILLING") {
		t.Errorf("转交丢了：模型调了 transfer_to_agent(billing)、用户确认了，最终却是 %q", final)
	}
	if n := triageModel.calls.Load(); n != 1 {
		t.Errorf("分诊那次调用应在续跑时被拿回、不再执行，分诊模型实际被调 %d 次（want 1）", n)
	}
	if n := billing.runs.Load(); n != 1 {
		t.Errorf("账单应恰好接手一次，got %d", n)
	}
}

// escTriage 程序化转交（HandoffTo）给账单。
type escTriage struct{ runs atomic.Int32 }

func (a *escTriage) Name() string        { return "triage" }
func (a *escTriage) Description() string { return "triage" }
func (a *escTriage) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *escTriage) Run(ctx context.Context, _ *agent_v2.RunSession, _ *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.runs.Add(1)
	if err := agent_v2.HandoffTo(ctx, opts, a, "billing"); err != nil {
		return nil, err
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent("triage", schema.AssistantMessage("转给账单"))}, nil
}

// TestNestedCall_RecalledProgrammaticHandoffStillHandsOff：同上，转交走 HandoffTo（程序化）。
func TestNestedCall_RecalledProgrammaticHandoffStillHandsOff(t *testing.T) {
	tri := &escTriage{}
	fd := &escFrontdesk{triage: tri}
	billing := &escReplyAgent{name: "billing", reply: "BILLING 已处理"}
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("desk"),
		agent_v2.WithHandoffInitial("frontdesk"),
	).AddAgent("frontdesk", fd).AddAgent("billing", billing)
	final, err := escDrive(t, h, "我要退款", 3)
	if err != nil {
		t.Fatalf("应收敛：%v", err)
	}
	if !strings.Contains(final, "BILLING") {
		t.Errorf("转交丢了：分诊转给了 billing、用户也确认了，最终却是 %q", final)
	}
	if n := tri.runs.Load(); n != 1 {
		t.Errorf("分诊那次调用应在续跑时被拿回、不再执行，实际执行 %d 次（want 1）", n)
	}
}

// TestNestedCall_RecalledRouteDecisionStillRoutes：classifier 是一张图，同一个节点里先经
// RunSubAgent 让 LLM 调 route_to 选分支、再请人确认，产出是重新合成的文本（不带那条 tool_call，
// 从产出里解析不出目标）。续跑时那次路由调用被拿回，路由决定必须照样生效。
func TestNestedCall_RecalledRouteDecisionStillRoutes(t *testing.T) {
	routerModel := &escToolCallModel{tool: agent_v2.RouteTool, args: `{"target":"math"}`}
	router := agent_v2.NewBasicAgent(
		agent_v2.WithName("router_llm"),
		agent_v2.WithModel(routerModel),
		agent_v2.WithRouteTool(),
		agent_v2.WithMaxRounds(3),
	)
	classifier := escHost(t, "classifier", func(ctx context.Context, c string) (string, error) {
		if _, err := agent_v2.RunSubAgent(ctx, "route_llm", router, schema.UserMessage(c), agent_v2.RegrantFromNode(ctx, router)...); err != nil {
			return "", err
		}
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "按模型的路由走？"}); err != nil {
			return "", err
		}
		return "已确认", nil
	})
	math := &escReplyAgent{name: "math", reply: "MATH 已处理"}
	fb := &escReplyAgent{name: "fb", reply: "FALLBACK 已处理"}
	rt := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("rt"),
		agent_v2.WithRoutingClassifier(classifier),
		agent_v2.WithRoutingFallback(fb),
	).AddBranch("math", math)
	final, err := escDrive(t, rt, "算一下 1+1", 3)
	if err != nil {
		t.Fatalf("应收敛：%v", err)
	}
	if !strings.Contains(final, "MATH") {
		t.Errorf("路由决定丢了：模型选了 math、用户确认了，最终却是 %q（fallback 被调 %d 次）", final, fb.runs.Load())
	}
	if n := routerModel.calls.Load(); n != 1 {
		t.Errorf("路由那次调用应在续跑时被拿回、不再执行，路由模型实际被调 %d 次（want 1）", n)
	}
}
