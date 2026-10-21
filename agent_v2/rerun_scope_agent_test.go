package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// 同一批 tool_call 里，一个 tool_call 调的嵌套图入口被否决、错被 AgentAsTool 吞成工具的失败结果：
// 那个 tool_call 已经算完（不会重做），重做代际一格也不该推进——兄弟 tool_call 挂着的提问、挂起中的
// 可重做调用都照常续。曾经否决当场推进整次 tools 节点执行的代际：兄弟的提问换 ID 再问（不加同步时
// 30 次里 29 次），挂起的可重做调用换槽重来、重新起草（实测）。

// rejectOnceRouter 是一个入口条件边第一次求值按 CondFailRerunFrom 否决的 GraphAgent。
func rejectOnceRouter(t *testing.T, evals *atomic.Int32) agent_v2.Agent {
	t.Helper()
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("x", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("routed-x"))}, nil
	})
	b.AddConditionalEdge(compose_v2.StartNode, func(context.Context, compose_v2.Channels) (compose_v2.NodeID, error) {
		if evals.Add(1) == 1 {
			return "", errors.New("router 一时判不出分支")
		}
		return "x", nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	b.AddEdge("x", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("router_graph"),
		agent_v2.WithGraphAgentDescription("按请求路由"),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
}

// driveRerunScope 跑到收敛：累积答案续跑，返回问过的问题。
func driveRerunScope(t *testing.T, ag agent_v2.Agent, ctx context.Context, input string) (asked []string, err error) {
	t.Helper()
	sess := agent_v2.NewRunSession()
	answers := map[string]*schema.Message{}
	_, err = ag.Run(ctx, sess, schema.UserMessage(input))
	for i := 0; i < 8 && err != nil; i++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			return asked, err
		}
		for _, q := range ie.AllQueries() {
			asked = append(asked, q.Question)
			answers[q.InterruptID] = schema.UserMessage("approve")
		}
		_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers), agent_v2.WithResumeSession(sess))
	}
	return asked, err
}

// TestRerunScope_SiblingAgentToolReject_QuestionAskedOnce：一轮两个 tool_call——A 在工具体里问人，B 是
// AgentAsTool(GraphAgent)，其入口条件边否决一次。不加同步、跑 20 次：A 的问题每次都只问一次。
func TestRerunScope_SiblingAgentToolReject_QuestionAskedOnce(t *testing.T) {
	for i := 0; i < 20; i++ {
		var evals, aExec atomic.Int32
		innerTool := agent_v2.AgentAsTool(rejectOnceRouter(t, &evals))
		askA := tool.NewFuncTool("tool_a", "要人确认的工具", map[string]any{"type": "object"},
			func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
				ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准工具A吗"})
				if err != nil {
					return nil, err
				}
				aExec.Add(1)
				return tool.Success("A 已执行：" + ans.Content), nil
			})
		tk := tool.NewToolkit()
		tk.Register(askA)
		tk.Register(innerTool)
		stub := newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "call_a", Type: "function", Function: schema.FunctionCall{Name: "tool_a", Arguments: `{}`}},
				{ID: "call_b", Type: "function", Function: schema.FunctionCall{Name: "router_graph", Arguments: `{"input":"go"}`}},
			}),
			schema.AssistantMessage("done"),
		})
		host := agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithModel(stub), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4))
		ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
		asked, err := driveRerunScope(t, host, ctx, "办事")
		if err != nil {
			t.Fatalf("第 %d 次没收敛：%v", i, err)
		}
		if len(asked) != 1 || aExec.Load() != 1 {
			t.Fatalf("第 %d 次：A 的问题应只问一次、执行一次，实际 asked=%v aExec=%d", i, asked, aExec.Load())
		}
	}
}

// draftingGraphAgent 是两节点 GraphAgent：draft 节点产出草稿（计数），confirm 节点问「批准草稿 vN 吗」。
func draftingGraphAgent(t *testing.T, drafts *atomic.Int32) agent_v2.Agent {
	t.Helper()
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	draftCh := compose_v2.LastValue[string]("draft")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, draftCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("draft", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		n := drafts.Add(1)
		return []compose_v2.ChannelWrite{compose_v2.Write(draftCh, fmt.Sprintf("草稿v%d", n))}, nil
	})
	b.AddNode("confirm", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		d, _ := compose_v2.Get(chs, draftCh)
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准" + d + "吗"})
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage(d+"("+ans.Content+")"))}, nil
	})
	b.AddEdge(compose_v2.StartNode, "draft")
	b.AddEdge("draft", "confirm")
	b.AddEdge("confirm", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("drafter"),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
}

// TestRerunScope_SiblingAgentToolReject_PendingRerunnableCallContinues：A 的工具体经
// RunSubAgent(drafter, WithRerunnable()) 起草并问人批准（挂起在被调方里），B 是入口否决一次的
// AgentAsTool(GraphAgent)。A 的调用从没被否决过：用户答了「批准草稿v1」之后续跑，A 续上自己的存档，
// 不重新起草、不再问一遍。B 等 A 起过草再跑，让「A 已挂起、B 才否决」这个时序每次都出现。
func TestRerunScope_SiblingAgentToolReject_PendingRerunnableCallContinues(t *testing.T) {
	var evals, drafts atomic.Int32
	aStarted := make(chan struct{}, 16)
	drafter := draftingGraphAgent(t, &drafts)
	innerTool := agent_v2.AgentAsTool(rejectOnceRouter(t, &evals))
	draftTool := tool.NewFuncTool("draft_tool", "起草并请人批准", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			ev, err := agent_v2.RunSubAgent(ctx, "drafter", drafter, schema.UserMessage("写个方案"), agent_v2.WithRerunnable())
			select {
			case aStarted <- struct{}{}:
			default:
			}
			if err != nil {
				return nil, err
			}
			return tool.Success(nestedFinal(ev)), nil
		})
	routeB := tool.NewFuncTool("router_graph", "路由", innerTool.InputSchema(),
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			select {
			case <-aStarted:
			case <-time.After(2 * time.Second):
			}
			return innerTool.Call(ctx, args)
		})
	tk := tool.NewToolkit()
	tk.Register(draftTool)
	tk.Register(routeB)
	stub := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "call_a", Type: "function", Function: schema.FunctionCall{Name: "draft_tool", Arguments: `{}`}},
			{ID: "call_b", Type: "function", Function: schema.FunctionCall{Name: "router_graph", Arguments: `{"input":"go"}`}},
		}),
		schema.AssistantMessage("done"),
	})
	host := agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithModel(stub), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	asked, err := driveRerunScope(t, host, ctx, "办事")
	if err != nil {
		t.Fatalf("没收敛：%v", err)
	}
	if drafts.Load() != 1 || len(asked) != 1 {
		t.Errorf("A 应续上自己的存档：起草 %d 次（应 1）、问了 %v（应只问一次）", drafts.Load(), asked)
	}
}

// TestDetachTask_IndependentTaskInNode_DoesNotRecallOuterRecords：节点里经 DetachTask 开一个独立任务
// （GraphAgent，入口条件边记路由），节点第一次跑完它之后失败、续跑。独立任务每次都从头跑、重新判路由，
// 不拿回外层节点执行名下的记录。曾经在节点里给新 session 调时，入口条件边拿回了外层节点上一次记下的
// 路由（实测 results=[x x]）；现在那个写法被入口拒掉，开独立任务要 DetachTask。
func TestDetachTask_IndependentTaskInNode_DoesNotRecallOuterRecords(t *testing.T) {
	var routes atomic.Int32
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	for _, id := range []string{"x", "y"} {
		id := id
		b.AddNode(compose_v2.NodeID(id), func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage(id))}, nil
		})
		b.AddEdge(compose_v2.NodeID(id), compose_v2.EndNode)
	}
	b.AddConditionalEdge(compose_v2.StartNode, func(ctx context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		r, ok := compose_v2.GetStepResult[string](ctx, "route")
		if !ok {
			r = "x"
			if routes.Add(1) > 1 {
				r = "y"
			}
			if err := compose_v2.SaveStepResult(ctx, "route", r); err != nil {
				return "", err
			}
		}
		return compose_v2.NodeID(r), nil
	})
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	innerTask := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("inner_task"),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
	var results []string
	attempt := 0
	ob := compose_v2.NewBuilder()
	ob.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		attempt++
		ev, err := innerTask.Run(agent_v2.DetachTask(ctx), agent_v2.NewRunSession(), schema.UserMessage("独立任务"))
		if err != nil {
			return nil, err
		}
		results = append(results, nestedFinal(ev))
		if attempt == 1 {
			return nil, errors.New("下游暂时不可用")
		}
		return nil, nil
	})
	ob.AddEdge(compose_v2.StartNode, "n")
	ob.AddEdge("n", compose_v2.EndNode)
	og, err := ob.Compile()
	if err != nil {
		t.Fatal(err)
	}
	host := agent_v2.NewGraphAgent(og, agent_v2.WithGraphAgentName("host"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return schema.AssistantMessage("ok") }))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	if _, err := host.Run(ctx, sess, schema.UserMessage("go")); err == nil {
		t.Fatal("首跑应失败（下游暂时不可用）")
	}
	if _, err := host.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess)); err != nil {
		t.Fatalf("续跑：%v", err)
	}
	if routes.Load() != 2 || len(results) != 2 || results[0] != "x" || results[1] != "y" {
		t.Errorf("独立任务每次都该重新判路由：routes=%d results=%v", routes.Load(), results)
	}
}
