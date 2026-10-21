package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// 转交 / 路由这两条旁路信号跟着写入者的提交走：写入者那一步提交了，信号就随它进存档；那一步没成
// （失败、被否决、被中断）信号就跟着作废。下面每条都跑「不中断」与「中断后续跑」两遍，两遍的结果
// 必须一样——不一样说明中断改变了控制流。
//
// 曾经信号写进消费方的活通道（HandoffAgent 每次尝试的收件槽、RoutingAgent 的 session KV），写入当场
// 生效、不等写入者提交，于是：写入者那一步已提交、续跑不再调它时信号丢失；失败那次尝试写下的信号照样
// 生效；并发写同一格不报冲突、谁赢随调度。全部 err == nil（均实测）。

func sigTransferModel(target string) *effModel {
	return &effModel{tc: schema.ToolCall{ID: "call_1", Type: "function",
		Function: schema.FunctionCall{Name: agent_v2.HandoffTransferToolName, Arguments: `{"agent_name":"` + target + `"}`}}}
}

func sigRouteModel(target string) *effModel {
	return &effModel{tc: schema.ToolCall{ID: "call_1", Type: "function",
		Function: schema.FunctionCall{Name: agent_v2.RouteTool, Arguments: `{"target":"` + target + `"}`}}}
}

// TestSideSignal_TransferInCommittedStep_SurvivesPause：候选是「先分诊（转交）、再问人确认」两步。
// 分诊那一步已经提交，确认那一步问人；续跑时分诊不再执行，它的转交必须还在。
func TestSideSignal_TransferInCommittedStep_SurvivesPause(t *testing.T) {
	for _, ask := range []bool{false, true} {
		t.Run(fmt.Sprintf("ask=%v", ask), func(t *testing.T) {
			m := sigTransferModel("billing")
			triage := agent_v2.NewBasicAgent(agent_v2.WithName("triage_llm"), agent_v2.WithModel(m),
				agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3))
			confirm := &effFunc{name: "confirm"}
			confirm.fn = func(ctx context.Context, _ agent_v2.Agent, _ *schema.Message, _ []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
				if err := effAsk(ctx, ask, "确认？"); err != nil {
					return nil, err
				}
				return effExit("confirm", "前台自己答了"), nil
			}
			seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("fdseq")).
				AddStep("triage", triage).AddStep("confirm", confirm)
			h, billing := effDesk(seq)
			if got := effDrive(t, h, "我要退款"); got != "BILLING 已处理" || billing.runs.Load() != 1 {
				t.Errorf("分诊的转交丢了：最终 %q（billing 执行 %d 次）", got, billing.runs.Load())
			}
			if n := m.calls.Load(); n != 1 {
				t.Errorf("分诊那一步已经提交，续跑不该再调它的模型，实际 %d 次", n)
			}
		})
	}
}

// TestSideSignal_RouteInCommittedStep_SurvivesPause：分类器是两步图，第一步经 RunSubAgent 调路由
// agent，第二步问人确认。续跑时第一步不再执行，它定下的路由必须还在。
func TestSideSignal_RouteInCommittedStep_SurvivesPause(t *testing.T) {
	for _, ask := range []bool{false, true} {
		t.Run(fmt.Sprintf("ask=%v", ask), func(t *testing.T) {
			m := sigRouteModel("math")
			router := agent_v2.NewBasicAgent(agent_v2.WithName("router_llm"), agent_v2.WithModel(m),
				agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(3))
			cls := newTwoNodeAgent(t, "classifier",
				func(ctx context.Context, c string) (string, error) {
					if _, err := agent_v2.RunSubAgent(ctx, "route_llm", router, schema.UserMessage(c), agent_v2.RegrantFromNode(ctx, router)...); err != nil {
						return "", err
					}
					return "routed", nil
				},
				func(ctx context.Context, _, _ string) (string, error) {
					return "已确认", effAsk(ctx, ask, "确认？")
				})
			rt, math, _ := effRouting(cls)
			if got := effDrive(t, rt, "算一下 1+1"); got != "MATH 已处理" || math.runs.Load() != 1 {
				t.Errorf("第一步定下的路由丢了：最终 %q（math 执行 %d 次）", got, math.runs.Load())
			}
			if n := m.calls.Load(); n != 1 {
				t.Errorf("定路由那一步已经提交，续跑不该再调路由模型，实际 %d 次", n)
			}
		})
	}
}

// TestSideSignal_RouteFromCompletedToolCall_SurvivesSiblingPause：分类器同一批两个 tool_call——业务
// 工具里经 RunSubAgent 定路由，另一个工具问人。续跑时业务工具命中 tools 节点的账、函数体不跑，
// 它里面定下的路由必须随账一起回来。
func TestSideSignal_RouteFromCompletedToolCall_SurvivesSiblingPause(t *testing.T) {
	for _, ask := range []bool{false, true} {
		t.Run(fmt.Sprintf("ask=%v", ask), func(t *testing.T) {
			rm := sigRouteModel("math")
			router := agent_v2.NewBasicAgent(agent_v2.WithName("router_llm"), agent_v2.WithModel(rm),
				agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(3))
			var decideRuns atomic.Int32
			tk := agent_v2_newToolkit()
			tk.Register(tool.NewFuncTool("decide", "定路由", map[string]any{"type": "object"},
				func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
					decideRuns.Add(1)
					if _, err := agent_v2.RunSubAgent(ctx, "route_llm", router, schema.UserMessage("算一下"), agent_v2.RegrantFromNode(ctx, router)...); err != nil {
						return nil, err
					}
					return tool.Success("decided"), nil
				}))
			tk.Register(tool.NewFuncTool("confirm", "问人", map[string]any{"type": "object"},
				func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
					if err := effAsk(ctx, ask, "确认？"); err != nil {
						return nil, err
					}
					return tool.Success("confirmed"), nil
				}))
			batch := schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "c1", Type: "function", Function: schema.FunctionCall{Name: "decide", Arguments: `{}`}},
				{ID: "c2", Type: "function", Function: schema.FunctionCall{Name: "confirm", Arguments: `{}`}},
			})
			cls := agent_v2.NewBasicAgent(agent_v2.WithName("cls_llm"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
				agent_v2.WithModel(newStubModel([]*schema.Message{batch, schema.AssistantMessage("分类完")})))
			rt, math, _ := effRouting(cls)
			if got := effDrive(t, rt, "算一下 1+1"); got != "MATH 已处理" || math.runs.Load() != 1 {
				t.Errorf("业务工具里定下的路由丢了：最终 %q（math 执行 %d 次）", got, math.runs.Load())
			}
			if n := decideRuns.Load(); n != 1 {
				t.Errorf("定路由的工具已经跑完，续跑不该再执行它，实际 %d 次", n)
			}
		})
	}
}

// TestSideSignal_FailedAttemptTransferDiscarded：候选里用 RunWithRetry 包 RunSubAgent，第一次尝试
// HandoffTo(billing) 之后失败，第二次成功且不转交。失败那次尝试交不出目标。
func TestSideSignal_FailedAttemptTransferDiscarded(t *testing.T) {
	var attempts atomic.Int32
	inner := &effFunc{name: "inner"}
	inner.fn = func(ctx context.Context, self agent_v2.Agent, _ *schema.Message, opts []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		if attempts.Add(1) == 1 {
			if err := agent_v2.HandoffTo(ctx, opts, self, "billing"); err != nil {
				return nil, err
			}
			return nil, errors.New("transient failure after handoff")
		}
		return effExit("inner", "inner 自己答了"), nil
	}
	fd := &effFunc{name: "frontdesk"}
	fd.fn = func(ctx context.Context, self agent_v2.Agent, in *schema.Message, opts []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		cfg := compose_v2.RetryConfig{MaxRetries: 1, ShouldRetry: func(error) bool { return true }}
		if _, err := compose_v2.RunWithRetry(ctx, cfg, func(ctx context.Context) ([]*agent_v2.AgentEvent, error) {
			return agent_v2.RunSubAgent(ctx, "inner", inner, in, agent_v2.RegrantHandoffTo(opts, self, inner)...)
		}); err != nil {
			return nil, err
		}
		return effExit("frontdesk", "前台自己答了"), nil
	}
	h, billing := effDesk(fd)
	if got := effDrive(t, h, "我要退款"); got != "前台自己答了" || billing.runs.Load() != 0 {
		t.Errorf("失败那次尝试的转交生效了：最终 %q（billing 执行 %d 次，inner 尝试 %d 次）",
			got, billing.runs.Load(), attempts.Load())
	}
}

// TestSideSignal_SwallowedFailedNestedCallTransferDiscarded：候选经 RunSubAgent 调一个先 HandoffTo
// 再失败的成员，自己吞掉这个错、照常作答。失败那次调用交不出目标——不靠重试那层，嵌套调用自己就
// 得把它作废。
func TestSideSignal_SwallowedFailedNestedCallTransferDiscarded(t *testing.T) {
	inner := &effFunc{name: "inner"}
	inner.fn = func(ctx context.Context, self agent_v2.Agent, _ *schema.Message, opts []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		if err := agent_v2.HandoffTo(ctx, opts, self, "billing"); err != nil {
			return nil, err
		}
		return nil, errors.New("failure after handoff")
	}
	fd := &effFunc{name: "frontdesk"}
	fd.fn = func(ctx context.Context, self agent_v2.Agent, in *schema.Message, opts []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		_, _ = agent_v2.RunSubAgent(ctx, "inner", inner, in, agent_v2.RegrantHandoffTo(opts, self, inner)...) // 有意吞掉
		return effExit("frontdesk", "前台自己答了"), nil
	}
	h, billing := effDesk(fd)
	if got := effDrive(t, h, "我要退款"); got != "前台自己答了" || billing.runs.Load() != 0 {
		t.Errorf("失败那次嵌套调用的转交生效了：最终 %q（billing 执行 %d 次）", got, billing.runs.Load())
	}
}

// TestSideSignal_ConcurrentConflictingTransfersReported：候选是 GraphAgent，同一超步两个节点各自显式转授给
// 一个带 transfer 工具的 agent，两者转给不同的目标，而两个 tool_call 恰好同 ID、同下标（有的 provider 每轮
// 都叫 call_0）。两个并发的决定方必须报冲突（终局），不能谁后写谁赢。
func TestSideSignal_ConcurrentConflictingTransfersReported(t *testing.T) {
	for i := 0; i < 10; i++ {
		a := agent_v2.NewBasicAgent(agent_v2.WithName("a_llm"), agent_v2.WithModel(sigTransferModel("billing")),
			agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3))
		b := agent_v2.NewBasicAgent(agent_v2.WithName("b_llm"), agent_v2.WithModel(sigTransferModel("sales")),
			agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3))
		front := concurrentDecidersGraph(t, "front", a, b)
		billing := &effReply{name: "billing", reply: "BILLING 已处理"}
		sales := &effReply{name: "sales", reply: "SALES 已处理"}
		h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("desk"), agent_v2.WithHandoffInitial("frontdesk")).
			AddAgent("frontdesk", front).AddAgent("billing", billing).AddAgent("sales", sales)
		ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
		_, err := h.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
		var conflict *compose_v2.SignalConflictError
		if !errors.As(err, &conflict) || !errors.Is(err, compose_v2.ErrRunAborted) ||
			billing.runs.Load()+sales.runs.Load() != 0 {
			t.Fatalf("第 %d 次：两个并发节点给出不同的转交目标应报冲突并终局，实际 err=%v（billing %d 次，sales %d 次）",
				i, err, billing.runs.Load(), sales.runs.Load())
		}
	}
}

// TestSideSignal_ConcurrentConflictingRoutesReported：分类器是并行的两步图，两个节点各经
// RunSubAgent 调一个路由 agent、给出不同的目标。必须报冲突，不能谁后写谁赢——也不能被分类器
// 失败的降级吞掉、落到兜底（那会把「模型自相矛盾」变成「没识别出目标」）。
func TestSideSignal_ConcurrentConflictingRoutesReported(t *testing.T) {
	for i := 0; i < 10; i++ {
		ra := agent_v2.NewBasicAgent(agent_v2.WithName("ra_llm"), agent_v2.WithModel(sigRouteModel("math")),
			agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(3))
		rb := agent_v2.NewBasicAgent(agent_v2.WithName("rb_llm"), agent_v2.WithModel(sigRouteModel("chat")),
			agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(3))
		b := compose_v2.NewBuilder().WithName("par_cls")
		inCh := compose_v2.LastValue[*schema.Message]("in")
		compose_v2.WithChannel(b, inCh)
		for _, br := range []struct {
			id string
			ag agent_v2.Agent
		}{{"na", ra}, {"nb", rb}} {
			br := br
			b.AddNode(compose_v2.NodeID(br.id), func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				in, _ := compose_v2.Get(chs, inCh)
				_, err := agent_v2.RunSubAgent(ctx, br.id, br.ag, in, agent_v2.RegrantFromNode(ctx, br.ag)...)
				return nil, err
			})
			b.AddEdge(compose_v2.StartNode, compose_v2.NodeID(br.id))
			b.AddEdge(compose_v2.NodeID(br.id), compose_v2.EndNode)
		}
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		cls := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("cls"),
			agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
				return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
			}),
			agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return schema.AssistantMessage("") }))
		math := &effReply{name: "math", reply: "MATH 已处理"}
		chat := &effReply{name: "chat", reply: "CHAT 已处理"}
		rt := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("rt"), agent_v2.WithRoutingClassifier(cls)).
			AddBranch("math", math).AddBranch("chat", chat)
		ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
		_, err = rt.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
		var conflict *compose_v2.SignalConflictError
		if !errors.As(err, &conflict) || !errors.Is(err, compose_v2.ErrRunAborted) ||
			math.runs.Load()+chat.runs.Load() != 0 {
			t.Fatalf("第 %d 次：两个节点给出不同的路由目标应报冲突并终局，实际 err=%v（math %d 次，chat %d 次）",
				i, err, math.runs.Load(), chat.runs.Load())
		}
	}
}

// TestSideSignal_ClassifierJoinSignalsConflict_TerminalNotDegraded：分类器是业务自研 agent，自己起两个
// goroutine 各经 RunSubAgent 调一个被转授的路由 agent，用 CaptureSignals 分路收、JoinSignals 合并
// （compose_v2/signal.go「并发合并要显式做」），冲突时往上交——原样返回，或用 errors.As 取出结构体再返回。
// 两路给出不同目标：
//
//   - RoutingAgent 必须原样往上交，不能降级落兜底——那会把一次「两路自相矛盾」悄悄变成一次「没识别出目标」；
//   - 冲突是终局（带 ErrRunAborted）：存档清掉，续跑返回 ErrNoPendingCheckpoint，什么都不再跑。曾经不带
//     终局标记、留着可续的档：续跑时分类器重跑、两个路由 agent 的决定从完成记录原样拿回，次次再撞
//     （审计实测续跑 3 次 3 次冲突）。后来标记只挂在外面那层包装上：取出来的结构体在这里被降级成兜底、
//     `err == nil`（审计实测）——所以两种交法都要测。
func TestSideSignal_ClassifierJoinSignalsConflict_TerminalNotDegraded(t *testing.T) {
	for _, mode := range []string{"原样返回", "取出结构体返回"} {
		t.Run(mode, func(t *testing.T) { classifierJoinConflictCase(t, mode == "取出结构体返回") })
	}
}

func classifierJoinConflictCase(t *testing.T, returnStruct bool) {
	ra, raModel := effRouter("ra_llm", "math")
	rb, rbModel := effRouter("rb_llm", "chat")
	cls := &effFunc{name: "cls"}
	cls.fn = func(ctx context.Context, self agent_v2.Agent, in *schema.Message, opts []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		sigs := make([]compose_v2.Signals, 2)
		errs := make([]error, 2)
		var wg sync.WaitGroup
		for i, r := range []agent_v2.Agent{ra, rb} {
			i, r := i, r
			wg.Add(1)
			go func() {
				defer wg.Done()
				sigs[i], errs[i] = compose_v2.CaptureSignals(ctx, func(c context.Context) error {
					_, err := agent_v2.RunSubAgent(c, r.Name(), r, in, agent_v2.RegrantRouteTo(opts, self, r)...)
					return err
				})
			}()
		}
		wg.Wait()
		if err := errors.Join(errs...); err != nil {
			return nil, err
		}
		joined, err := compose_v2.JoinSignals(sigs...)
		var conflict *compose_v2.SignalConflictError
		if returnStruct && errors.As(err, &conflict) {
			return nil, conflict
		}
		if err != nil {
			return nil, err
		}
		compose_v2.EmitSignals(ctx, joined)
		return effExit("cls", "分完了"), nil
	}
	rt, math, fb := effRouting(cls)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := rt.Run(ctx, sess, schema.UserMessage("hi"))
	var conflict *compose_v2.SignalConflictError
	if !errors.As(err, &conflict) || !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("两路给出不同目标，JoinSignals 的冲突应带 ErrRunAborted 原样往上交，实际 err=%v（math %d 次，兜底 %d 次）",
			err, math.runs.Load(), fb.runs.Load())
	}
	if math.runs.Load()+fb.runs.Load() != 0 {
		t.Errorf("冲突时不该路由到任何分支，实际 math %d 次、兜底 %d 次", math.runs.Load(), fb.runs.Load())
	}
	if _, err := rt.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess)); !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
		t.Errorf("冲突是终局，存档应已清掉、续跑返回 ErrNoPendingCheckpoint，实际 %v", err)
	}
	if c, a, b := cls.runs.Load(), raModel.calls.Load(), rbModel.calls.Load(); c != 1 || a != 1 || b != 1 {
		t.Errorf("终局之后什么都不该再跑：分类器 %d 次、两个路由模型 %d / %d 次（都应是 1）", c, a, b)
	}
}

// TestSideSignal_HandoffAgentForwardsDecisionsNotForIt：RoutingAgent 的分类器是一个 HandoffAgent，
// 它的候选经 RunSubAgent 调路由 agent 定路由、自己另答一句话。那条路由发给外层的 RoutingAgent，
// HandoffAgent 取走发给自己的转交之后要把它原样往上交——不交的话外层收不到、落到兜底。
//
// 候选自己的最终答复不提路由目标：否则 RoutingAgent 从最终消息里解析目标的那道兜底会把它救回来，
// 这条用例就测不到转交本身。
func TestSideSignal_HandoffAgentForwardsDecisionsNotForIt(t *testing.T) {
	router := agent_v2.NewBasicAgent(agent_v2.WithName("router_llm"), agent_v2.WithModel(sigRouteModel("math")),
		agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(3))
	front := &effFunc{name: "front"}
	// HandoffAgent 把它收到的路由授予转给候选，候选再显式转给它调的路由 agent。
	front.fn = func(ctx context.Context, self agent_v2.Agent, in *schema.Message, opts []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		if _, err := agent_v2.RunSubAgent(ctx, "router", router, in, agent_v2.RegrantRouteTo(opts, self, router)...); err != nil {
			return nil, err
		}
		return effExit("front", "已分诊"), nil
	}
	desk := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("desk"), agent_v2.WithHandoffInitial("front")).
		AddAgent("front", front)
	rt, math, _ := effRouting(desk)
	if got := effDrive(t, rt, "算一下 1+1"); got != "MATH 已处理" || math.runs.Load() != 1 {
		t.Errorf("HandoffAgent 没把发给外层的路由往上交：最终 %q（math 执行 %d 次）", got, math.runs.Load())
	}
}
