package agent_v2_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// TestAgentAsTool_FullHistoryInputResumesByPosition：AgentAsTool 开了全历史输入时，子 agent 的输入由
// 框架从父 session 的事件现拼，而事件不进存档——Resume 不带 WithResumeSession（CLAUDE.md 示例的写法）
// 时续跑拼出来的是空串。一个 tool_call 就是一次调用，身份只看位置：续跑续上自己的存档、用户的回答
// 配得上，子 agent 已完成的那一步不重放。按内容定身份时这里换槽、从头跑、换 ID 重问，最后在空输入
// 上算出结果交给父模型，`err == nil`（实测）。
func TestAgentAsTool_FullHistoryInputResumesByPosition(t *testing.T) {
	for _, keepSession := range []bool{false, true} {
		t.Run(fmt.Sprintf("WithResumeSession=%v", keepSession), func(t *testing.T) {
			firsts := &countLog{}
			seen := &inputLog{}
			sub := newTwoNodeAgent(t, "lookup",
				func(_ context.Context, c string) (string, error) {
					firsts.inc("first")
					seen.add(c)
					return "prep", nil
				},
				func(ctx context.Context, c, _ string) (string, error) {
					ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认执行吗？"})
					if err != nil {
						return "", err
					}
					return "done(" + ans.Content + ") 处理的输入=[" + c + "]", nil
				})
			m := &scriptedToolModel{replies: []*schema.Message{
				schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("call_0", "lookup", "x")}),
				schema.AssistantMessage("all done"),
			}}
			tk := tool.NewToolkit()
			tk.Register(agent_v2.AgentAsTool(sub,
				agent_v2.WithAgentToolDescription("查一件事"),
				agent_v2.WithAgentToolFullHistoryInput()))
			parent := agent_v2.NewBasicAgent(
				agent_v2.WithName("parent"),
				agent_v2.WithModel(m),
				agent_v2.WithToolkit(tk),
				agent_v2.WithMaxRounds(6),
			)
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			sess := agent_v2.NewRunSession()
			_, err := parent.Run(ctx, sess, schema.UserMessage("帮我查一下订单"))
			ie, ok := compose_v2.IsInterrupt(err)
			if !ok {
				t.Fatalf("首跑应停在子 agent 的确认，got %v", err)
			}
			ropts := []agent_v2.ResumeOption{agent_v2.WithResumeUserInputs(map[string]*schema.Message{
				ie.Query.InterruptID: schema.UserMessage("是")})}
			if keepSession {
				ropts = append(ropts, agent_v2.WithResumeSession(sess))
			}
			if _, err := parent.Resume(ctx, sess.RunID, ropts...); err != nil {
				t.Fatalf("用户已回答，一次续跑就该收敛，got %v", err)
			}
			if n := firsts.get("first"); n != 1 {
				t.Errorf("子 agent 第一个节点应只执行 1 次（续上自己的存档），实际 %d 次，依次看到的输入 %q", n, seen.snapshot())
			}
			if r := m.lastResults(); len(r) != 1 || !strings.Contains(r[0], "处理的输入=[user: 帮我查一下订单]") {
				t.Errorf("父模型拿到的工具结果应基于首跑时的输入，got %q", r)
			}
		})
	}
}

// TestAgentAsTool_CompletionRecordHoldsOnlyTheFinalMessage：tool_call 里的 AgentAsTool 完成之后记下
// 完成结果（同一个 tool_call 之后还可能挂起，那时只有它挡得住子 agent 重跑），但只记最终那条消息
// ——AgentAsTool 只用得到它。记整组事件的话，子 agent 的整段事件（这里是它记下的那份很大的输入）
// 跟着进父层的中断档，续跑时一次都不会被读（实测两百多倍）。
func TestAgentAsTool_CompletionRecordHoldsOnlyTheFinalMessage(t *testing.T) {
	bulk := strings.Repeat("原文", 2000)
	var subRuns atomic.Int32
	sub := newSingleNodeAgent(t, "researcher", "work", nil, func(context.Context, string) (string, error) {
		subRuns.Add(1)
		return "摘要", nil
	})
	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(sub, agent_v2.WithAgentToolDescription("查资料")))
	tk.Register(tool.NewFuncTool("confirm", "问人", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"})
			if err != nil {
				return nil, err
			}
			return tool.Success(ans.Content), nil
		}))
	ba := agent_v2.NewBasicAgent(
		agent_v2.WithName("b"),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(3),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("c1", "researcher", bulk), toolCall("c2", "confirm", "y")}),
			schema.AssistantMessage("好了"),
		})),
	)
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()
	_, err := ba.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("want interrupt, got %v", err)
	}
	cp, err := cper.LoadNS(ctx, sess.RunID, "")
	if err != nil || cp == nil {
		t.Fatalf("应留下父层中断档：cp=%v err=%v", cp, err)
	}
	raw, err := json.Marshal(cp.NodeStates)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "原文原文原文") {
		t.Errorf("父层中断档的节点私有态里出现了子 agent 的整段事件（%d 字节）——完成记录应只留最终那条消息", len(raw))
	}
	if !strings.Contains(string(raw), "摘要") {
		t.Errorf("完成记录应在父层中断档里（同一个 tool_call 之后还可能挂起，只有它挡得住重跑）")
	}
	if _, err := ba.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("好")}),
		agent_v2.WithResumeSession(sess)); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if n := subRuns.Load(); n != 1 {
		t.Errorf("完成的 AgentAsTool 在续跑时应拿回、不重跑，子 agent 执行了 %d 次", n)
	}
}

// inPlaceRedactHook 是幂等的原地脱敏钩子（最常见的 OnAgentStart 用法之一）：把手机号换成 ***。
type inPlaceRedactHook struct{ hook.NoOpHooks }

func (inPlaceRedactHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	for _, m := range in.Messages {
		if m != nil {
			m.Content = strings.ReplaceAll(m.Content, "13800001111", "***")
		}
	}
	return ctx, nil
}

// TestNestedCall_CalleeHookInPlaceEditDoesNotLeakIntoCaller：父节点把从 channel 读出来的那条消息原样
// 交给 RunSubAgent（最自然的写法），被调方的 OnAgentStart 原地改写输入（钩子契约允许）。改写不能
// 顺着指针写进父 channel、随父存档落盘——否则续跑时父节点读到的已是改写过的消息，同一次调用被当成
// 另一次：每次加后缀的钩子永不收敛（每次换一个提问 ID 重问），连幂等的脱敏钩子也多问一次。
func TestNestedCall_CalleeHookInPlaceEditDoesNotLeakIntoCaller(t *testing.T) {
	for _, v := range []struct {
		name  string
		hooks func() hook.Hooks
	}{
		{"每次加后缀", func() hook.Hooks { return &volatileStartHook{} }},
		{"幂等脱敏", func() hook.Hooks { return inPlaceRedactHook{} }},
	} {
		t.Run(v.name, func(t *testing.T) {
			var works atomic.Int32
			b := compose_v2.NewBuilder()
			inCh := compose_v2.LastValue[*schema.Message]("in")
			outCh := compose_v2.LastValue[*schema.Message]("out")
			compose_v2.WithChannel(b, inCh)
			compose_v2.WithChannel(b, outCh)
			b.AddNode("work", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				works.Add(1)
				ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认？"})
				if err != nil {
					return nil, err
				}
				return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("ok:"+ans.Content))}, nil
			})
			b.AddEdge(compose_v2.StartNode, "work")
			b.AddEdge("work", compose_v2.EndNode)
			g, err := b.Compile()
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			sub := agent_v2.NewGraphAgent(g,
				agent_v2.WithGraphAgentName("sub"),
				agent_v2.WithGraphAgentHooks(v.hooks()),
				agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
					return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
				}),
				agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
					out, _ := compose_v2.Get(chs, outCh)
					return out
				}),
			)
			var hostSaw []string
			hb := compose_v2.NewBuilder()
			hin := compose_v2.LastValue[*schema.Message]("in")
			hout := compose_v2.LastValue[*schema.Message]("out")
			compose_v2.WithChannel(hb, hin)
			compose_v2.WithChannel(hb, hout)
			hb.AddNode("host", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				in, _ := compose_v2.Get(chs, hin)
				hostSaw = append(hostSaw, in.Content)
				events, err := agent_v2.RunSubAgent(ctx, "sub", sub, in) // 原样交出 channel 里那条
				if err != nil {
					return nil, err
				}
				return []compose_v2.ChannelWrite{compose_v2.Write(hout, agent_v2.FinalMessage(events))}, nil
			})
			hb.AddEdge(compose_v2.StartNode, "host")
			hb.AddEdge("host", compose_v2.EndNode)
			hg, err := hb.Compile()
			if err != nil {
				t.Fatalf("compile host: %v", err)
			}
			host := agent_v2.NewGraphAgent(hg,
				agent_v2.WithGraphAgentName("host"),
				agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
					return []compose_v2.ChannelWrite{compose_v2.Write(hin, m)}
				}),
				agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
					out, _ := compose_v2.Get(chs, hout)
					return out
				}),
			)
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			sess := agent_v2.NewRunSession()
			_, err = host.Run(ctx, sess, schema.UserMessage("给 13800001111 发短信"))
			ie, ok := compose_v2.IsInterrupt(err)
			if !ok {
				t.Fatalf("首跑应停在提问，got %v", err)
			}
			events, err := host.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
				ie.Query.InterruptID: schema.UserMessage("好")}), agent_v2.WithResumeSession(sess))
			if err != nil {
				t.Fatalf("一次续跑就该收敛，got %v", err)
			}
			if got := nestedFinal(events); got != "ok:好" {
				t.Errorf("final = %q", got)
			}
			if n := works.Load(); n != 2 {
				t.Errorf("子 agent 的 work 节点应执行 2 次（首跑问 + 续跑拿答案），实际 %d 次", n)
			}
			for _, s := range hostSaw {
				if s != "给 13800001111 发短信" {
					t.Errorf("父节点续跑时读到的输入被被调方的钩子改写了：%q", hostSaw)
				}
			}
		})
	}
}

// TestNestedCall_CondFailRerunFromRedoesNestedPlanner：CondFailRerunFrom 的上游节点经 RunSubAgent 调
// LLM 型 planner，这次调用声明了 WithRerunnable。cond 否决了 planner 的产出、要求重新产出——续跑时
// 那次嵌套调用作废、planner 重新执行。按普通完成记录拿回的话，重跑原样拿回被否决的产出，续跑多少次
// 都是同一个 cond 错（实测）。
func TestNestedCall_CondFailRerunFromRedoesNestedPlanner(t *testing.T) {
	var calls atomic.Int32
	planner := newSingleNodeAgent(t, "planner", "p", nil, func(_ context.Context, _ string) (string, error) {
		if calls.Add(1) == 1 {
			return "BAD-plan-v1", nil
		}
		return fmt.Sprintf("plan-v%d", calls.Load()), nil
	})
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("plan", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		ev, err := agent_v2.RunSubAgent(ctx, "planner", planner, in, agent_v2.WithRerunnable())
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage(nestedFinal(ev)))}, nil
	})
	b.AddEdge(compose_v2.StartNode, "plan")
	b.AddConditionalEdge("plan", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		out, _ := compose_v2.Get(chs, outCh)
		if out == nil || strings.HasPrefix(out.Content, "BAD") {
			return "", fmt.Errorf("plan 不合法: %v", out)
		}
		return compose_v2.EndNode, nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	host := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("host"),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	if _, err := host.Run(ctx, sess, schema.UserMessage("做个计划")); err == nil {
		t.Fatal("首跑的计划不合格，cond 应报错")
	}
	events, err := host.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess))
	if err != nil {
		t.Fatalf("续跑应让 planner 重新产出、收敛；got %v（planner 执行 %d 次）", err, calls.Load())
	}
	if got := nestedFinal(events); got != "plan-v2" {
		t.Errorf("final = %q，want plan-v2", got)
	}
	if n := calls.Load(); n != 2 {
		t.Errorf("planner 应执行 2 次（首跑 + 被否决后重新产出），got %d", n)
	}
}

// calleeStateWritingHook 在被调 agent 的 OnAgentStart 里调 SaveNodeState（业务 helper 在节点体里也用它）。
type calleeStateWritingHook struct {
	hook.NoOpHooks
	delay time.Duration
	fired atomic.Int32
}

func (h *calleeStateWritingHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	h.fired.Add(1)
	if h.delay > 0 {
		time.Sleep(h.delay)
	}
	_ = compose_v2.SaveNodeState(ctx, "hook wrote this")
	return ctx, nil
}

// askingSub 是一个问一句就结束的 GraphAgent，挂着 hooks。
func askingSub(t *testing.T, name string, hooks hook.Hooks) agent_v2.Agent {
	t.Helper()
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("work", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认？"})
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("sub:"+ans.Content))}, nil
	})
	b.AddEdge(compose_v2.StartNode, "work")
	b.AddEdge("work", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName(name),
		agent_v2.WithGraphAgentHooks(hooks),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}),
	)
}

// TestNestedCall_CalleeAgentHookCannotOverwriteCallerNodeState：被调 agent 的 OnAgentStart 跑在它自己的
// 图之外、调用方节点的执行里。它调 SaveNodeState 不能覆盖调用方节点自己记的进度。
func TestNestedCall_CalleeAgentHookCannotOverwriteCallerNodeState(t *testing.T) {
	h := &calleeStateWritingHook{}
	// 被调方：一个不问人的单节点图，挂着会写 SaveNodeState 的钩子
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("work", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("done:"+in.Content))}, nil
	})
	b.AddEdge(compose_v2.StartNode, "work")
	b.AddEdge("work", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	sub := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("sub"),
		agent_v2.WithGraphAgentHooks(h),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}),
	)
	var after string
	host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
		if err := compose_v2.SaveNodeState(ctx, "host progress"); err != nil {
			return "", err
		}
		if _, err := agent_v2.RunSubAgent(ctx, "w", sub, schema.UserMessage("x")); err != nil {
			return "", err
		}
		after, _ = compose_v2.GetNodeState[string](ctx)
		return "ok", nil
	})
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	if _, err := host.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("run: %v", err)
	}
	if h.fired.Load() != 1 {
		t.Fatalf("前提没搭对：被调方的 OnAgentStart 触发 %d 次", h.fired.Load())
	}
	if after != "host progress" {
		t.Errorf("调用方节点的私有态被被调方的 OnAgentStart 覆盖成了 %q", after)
	}
}

// TestNestedCall_CalleeAgentHookCannotClobberToolMemo：一批两个 tool_call——charge（有副作用、进
// tools 节点的账）+ AgentAsTool(sub)。sub 的 OnAgentStart 调 SaveNodeState、然后 sub 问人中断。续跑时
// tools 节点的账必须完好，charge 不重放（实测没封口时 2 次）。
func TestNestedCall_CalleeAgentHookCannotClobberToolMemo(t *testing.T) {
	var charged atomic.Int32
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("charge", "扣款", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			charged.Add(1)
			return tool.Success("charged"), nil
		}))
	// 延迟写入：让 charge 先把账落下，钩子再写——覆盖要在「账已经记下之后」才看得出来
	sub := askingSub(t, "confirmer", &calleeStateWritingHook{delay: 50 * time.Millisecond})
	tk.Register(agent_v2.AgentAsTool(sub, agent_v2.WithAgentToolDescription("确认")))
	ba := agent_v2.NewBasicAgent(
		agent_v2.WithName("b"),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(3),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{toolCall("c1", "charge", "x"), toolCall("c2", "confirmer", "y")}),
			schema.AssistantMessage("好了"),
		})),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ba.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("want interrupt, got %v", err)
	}
	if _, err := ba.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("好")}),
		agent_v2.WithResumeSession(sess)); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if n := charged.Load(); n != 1 {
		t.Errorf("已完成的 charge 在续跑时被重放：%d 次（tools 节点的账被被调方钩子覆盖了）", n)
	}
}

// TestNodeState_ToolFunctionsGetTheirOwnScope：工具函数里的 SaveNodeState / SaveStepResult 按
// tool_call 分域——
//
//   - SaveNodeState 不覆盖 tools 节点自己记的账（实测没分域时已完成的 charge 在续跑时重放）；
//   - 同批两个 tool_call 用同一个 key 记步骤，各记各的（没分域时第二个读到第一个记下的值）；
//   - 工具自己记的进度在续跑时读得回（记在这个 tool_call 名下，随存档走）。
func TestNodeState_ToolFunctionsGetTheirOwnScope(t *testing.T) {
	var charged atomic.Int32
	var stamps []string
	var noterProgress []string
	firstStamped := make(chan struct{})
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("charge", "扣款", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			charged.Add(1)
			return tool.Success("charged"), nil
		}))
	tk.Register(tool.NewFuncTool("stamp", "盖章", map[string]any{"type": "object"},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			mine, _ := args["input"].(string)
			if mine == "B" {
				<-firstStamped // 确定性地排在 A 记完之后读
			}
			v, ok := compose_v2.GetStepResult[string](ctx, "stamp")
			if !ok {
				v = mine
				if err := compose_v2.SaveStepResult(ctx, "stamp", v); err != nil {
					return nil, err
				}
			}
			if mine == "A" {
				close(firstStamped)
			}
			return tool.Success(mine + "->" + v), nil
		}))
	tk.Register(tool.NewFuncTool("noter", "记进度再问", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if p, ok := compose_v2.GetNodeState[string](ctx); ok {
				noterProgress = append(noterProgress, p)
			} else if err := compose_v2.SaveNodeState(ctx, "noter 的进度"); err != nil {
				return nil, err
			}
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"})
			if err != nil {
				return nil, err
			}
			return tool.Success(ans.Content), nil
		}))
	m := &scriptedToolModel{replies: []*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{
			toolCall("c1", "charge", "x"), toolCall("c2", "stamp", "A"), toolCall("c3", "stamp", "B"), toolCall("c4", "noter", "y")}),
		schema.AssistantMessage("好了"),
	}}
	ba := agent_v2.NewBasicAgent(agent_v2.WithName("b"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3), agent_v2.WithModel(m))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ba.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("want interrupt, got %v", err)
	}
	if _, err := ba.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("好")}),
		agent_v2.WithResumeSession(sess)); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if n := charged.Load(); n != 1 {
		t.Errorf("已完成的 charge 在续跑时被重放：%d 次（工具里的 SaveNodeState 覆盖了 tools 节点的账）", n)
	}
	for _, r := range m.lastResults() {
		if strings.Contains(r, "->") {
			stamps = append(stamps, r)
		}
	}
	if strings.Join(stamps, ",") != "A->A,B->B" {
		t.Errorf("同批两个 tool_call 用同一个 key 应各记各的，got %v", stamps)
	}
	if len(noterProgress) != 1 || noterProgress[0] != "noter 的进度" {
		t.Errorf("工具在续跑时应读回自己记的进度，got %q", noterProgress)
	}
}
