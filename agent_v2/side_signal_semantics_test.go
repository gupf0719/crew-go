package agent_v2_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// 转交 / 路由决定的裁决语义（见 side_signal.go）：一个收件人上是一个单值格子——先后做出的后者为准，
// 同一超步只能有一个成员做决定（两个都做了按设计错误终局）；结果是「没成」的那段执行做出的作废；
// 收件人别的代码算不出来。每条用例都对应审计实测过的一个静默错误（err == nil）或一次误报。

// sigDesk：前台 fd + billing + sales 三个候选。
func sigDesk(fd agent_v2.Agent) (*agent_v2.HandoffAgent, *effReply, *effReply) {
	billing := &effReply{name: "billing", reply: "BILLING 已处理"}
	sales := &effReply{name: "sales", reply: "SALES 已处理"}
	return agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("desk"), agent_v2.WithHandoffInitial("frontdesk")).
		AddAgent("frontdesk", fd).AddAgent("billing", billing).AddAgent("sales", sales), billing, sales
}

// TestSideSignal_SequentialRefinement_LaterWins：候选是 Sequential，两步各转一次、目标不同；第二步
// 可以先问人。以后一步为准，停没停过一样。
//
// 曾经「同一次调用里目标不一致」一律报冲突：第二步修正第一步是先后关系，却被当成矛盾、整个 Run 失败
// （审计实测；改动之前是后者为准）。
func TestSideSignal_SequentialRefinement_LaterWins(t *testing.T) {
	for _, ask := range []bool{false, true} {
		t.Run(fmt.Sprintf("ask=%v", ask), func(t *testing.T) {
			first := agent_v2.NewBasicAgent(agent_v2.WithName("first_llm"), agent_v2.WithModel(sigTransferModel("billing")),
				agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3))
			second := &effFunc{name: "second"}
			second.fn = func(ctx context.Context, self agent_v2.Agent, _ *schema.Message, opts []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
				if err := effAsk(ctx, ask, "改转 sales？"); err != nil {
					return nil, err
				}
				if err := agent_v2.HandoffTo(ctx, opts, self, "sales"); err != nil {
					return nil, err
				}
				return effExit("second", "改转 sales"), nil
			}
			seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("fdseq")).
				AddStep("first", first).AddStep("second", second)
			h, billing, sales := sigDesk(seq)
			if got := effDrive(t, h, "我要退款"); got != "SALES 已处理" || billing.runs.Load() != 0 || sales.runs.Load() != 1 {
				t.Errorf("先后两步各转一次应以后一步为准：最终 %q（billing %d 次，sales %d 次）",
					got, billing.runs.Load(), sales.runs.Load())
			}
		})
	}
}

// TestSideSignal_SequentialClassifier_LaterWins：分类器是 Sequential（先粗分 math、再细分 chat），
// 以后一步为准。
func TestSideSignal_SequentialClassifier_LaterWins(t *testing.T) {
	coarse, _ := effRouter("coarse_llm", "math")
	fine, _ := effRouter("fine_llm", "chat")
	cls := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("cls")).AddStep("coarse", coarse).AddStep("fine", fine)
	math := &effReply{name: "math", reply: "MATH 已处理"}
	chat := &effReply{name: "chat", reply: "CHAT 已处理"}
	rt := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("rt"), agent_v2.WithRoutingClassifier(cls)).
		AddBranch("math", math).AddBranch("chat", chat)
	if got := effDrive(t, rt, "随便聊聊"); got != "CHAT 已处理" || math.runs.Load() != 0 {
		t.Errorf("分类器先后两步各定一次路由应以后一步为准：最终 %q（math %d 次）", got, math.runs.Load())
	}
}

// TestSideSignal_LoopRedecision_LaterWins：候选是 Loop，两轮各转一次、目标不同，以后一轮为准。
func TestSideSignal_LoopRedecision_LaterWins(t *testing.T) {
	body := &effFunc{name: "body"}
	body.fn = func(ctx context.Context, self agent_v2.Agent, _ *schema.Message, opts []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		target := "billing"
		if body.runs.Load() >= 2 {
			target = "sales"
		}
		if err := agent_v2.HandoffTo(ctx, opts, self, target); err != nil {
			return nil, err
		}
		return effExit("body", "转 "+target), nil
	}
	loop := agent_v2.NewLoopAgent(agent_v2.WithLoopName("fdloop"), agent_v2.WithLoopMaxIterations(2)).WithBody(body)
	h, billing, sales := sigDesk(loop)
	if got := effDrive(t, h, "我要退款"); got != "SALES 已处理" || billing.runs.Load() != 0 || sales.runs.Load() != 1 {
		t.Errorf("两轮各转一次应以后一轮为准：最终 %q（billing %d 次，sales %d 次）",
			got, billing.runs.Load(), sales.runs.Load())
	}
}

// TestSideSignal_ToolThenProgrammatic_LaterWins：内层 LLM 经 transfer 工具转交之后，业务代码再
// HandoffTo 修正——以后者为准。两条路做出的是同一类决定。
//
// 曾经「工具路径优先」：先做出的工具转交赢、业务代码的修正被静默忽略。
func TestSideSignal_ToolThenProgrammatic_LaterWins(t *testing.T) {
	llm := agent_v2.NewBasicAgent(agent_v2.WithName("llm"), agent_v2.WithModel(sigTransferModel("billing")),
		agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3))
	fixer := &effFunc{name: "fixer"}
	fixer.fn = func(ctx context.Context, self agent_v2.Agent, _ *schema.Message, opts []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		if err := agent_v2.HandoffTo(ctx, opts, self, "sales"); err != nil {
			return nil, err
		}
		return effExit("fixer", "业务规则：改转 sales"), nil
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("fdseq")).AddStep("llm", llm).AddStep("fixer", fixer)
	h, billing, sales := sigDesk(seq)
	if got := effDrive(t, h, "我要退款"); got != "SALES 已处理" || billing.runs.Load() != 0 {
		t.Errorf("业务代码在工具转交之后修正，应以后者为准：最终 %q（billing %d 次，sales %d 次）",
			got, billing.runs.Load(), sales.runs.Load())
	}
}

// TestSideSignal_ConcurrentProgrammaticDeciders_TerminalRegardlessOfPause：候选是 GraphAgent，同一超步两个
// 节点各自显式转授给一个经 HandoffTo 转交的成员（成员 a 可以先问人）。两个成员都做了决定——目标同不同、
// 停没停过都一样——是图的设计错误：报冲突并带 ErrRunAborted，谁也不接手。
//
// 曾经程序化转交取「最后一个」：赢家由声明顺序决定，a 停过一次之后翻转（审计实测 SALES ⟶ BILLING）。
// 后来改成「值不同才报、不清档」：值相同的两个并发决定方被放过，分歧时续跑又原样再撞。
func TestSideSignal_ConcurrentProgrammaticDeciders_TerminalRegardlessOfPause(t *testing.T) {
	for _, tc := range []struct{ name, ta, tb string }{{"differ", "billing", "sales"}, {"agree", "billing", "billing"}} {
		for _, ask := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s_a_asks=%v", tc.name, ask), func(t *testing.T) {
				mk := func(name, target string, asks bool) *effFunc {
					f := &effFunc{name: name}
					f.fn = func(ctx context.Context, self agent_v2.Agent, _ *schema.Message, opts []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
						if err := effAsk(ctx, asks, "确认？"); err != nil {
							return nil, err
						}
						if err := agent_v2.HandoffTo(ctx, opts, self, target); err != nil {
							return nil, err
						}
						return effExit(name, "转 "+target), nil
					}
					return f
				}
				front := concurrentDecidersGraph(t, "fdgraph", mk("a", tc.ta, ask), mk("b", tc.tb, false))
				h, billing, sales := sigDesk(front)
				ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
				sess := agent_v2.NewRunSession()
				_, err := h.Run(ctx, sess, schema.UserMessage("hi"))
				if ie, ok := compose_v2.IsInterrupt(err); ok {
					answers := map[string]*schema.Message{}
					for _, q := range ie.AllQueries() {
						answers[q.InterruptID] = schema.UserMessage("是")
					}
					_, err = h.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers), agent_v2.WithResumeSession(sess))
				} else if ask {
					t.Fatalf("前提：a 应先问人，实际 err=%v", err)
				}
				var conflict *compose_v2.SignalConflictError
				if !errors.As(err, &conflict) || !errors.Is(err, compose_v2.ErrRunAborted) {
					t.Fatalf("两个并发成员都做了转交决定应报冲突并终局，实际 err=%v（billing %d 次，sales %d 次）",
						err, billing.runs.Load(), sales.runs.Load())
				}
				if billing.runs.Load()+sales.runs.Load() != 0 {
					t.Errorf("冲突时谁也不该接手，实际 billing %d 次，sales %d 次", billing.runs.Load(), sales.runs.Load())
				}
			})
		}
	}
}

// TestSideSignal_RetryFailedAttemptDirectTransferDiscarded：候选自己用 RunWithRetry 包一段逻辑，第一次
// 尝试直接 HandoffTo 之后失败、第二次自己作答——失败那次的转交作废。
//
// ⚠️ 转交必须在重试的那段逻辑里**直接**做出：经 RunSubAgent 调一个先转交再失败的被调方时，嵌套调用自己
// 那层就把它作废了，测不到重试这一层（`TestSideSignal_FailedAttemptTransferDiscarded` 是那个形状——去掉
// 重试按尝试收的那一层，它照样全绿，实测）。
func TestSideSignal_RetryFailedAttemptDirectTransferDiscarded(t *testing.T) {
	fd := &effFunc{name: "frontdesk"}
	fd.fn = func(ctx context.Context, self agent_v2.Agent, _ *schema.Message, opts []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
		attempt := 0
		cfg := compose_v2.RetryConfig{MaxRetries: 1, ShouldRetry: func(error) bool { return true }}
		if _, err := compose_v2.RunWithRetry(ctx, cfg, func(ctx context.Context) (int, error) {
			attempt++
			if attempt == 1 {
				if err := agent_v2.HandoffTo(ctx, opts, self, "billing"); err != nil {
					return 0, err
				}
				return 0, errors.New("转交之后这次尝试失败了")
			}
			return 1, nil
		}); err != nil {
			return nil, err
		}
		return effExit("frontdesk", "前台自己答了"), nil
	}
	h, billing, _ := sigDesk(fd)
	if got := effDrive(t, h, "我要退款"); got != "前台自己答了" || billing.runs.Load() != 0 {
		t.Errorf("失败那次尝试的转交生效了：最终 %q（billing %d 次）", got, billing.runs.Load())
	}
}

// forgeTool 是一件业务工具：按 ns 的每个前缀拼出「旧格式」的收件人，手写一份信号 JSON、反序列化后
// EmitSignals——审计用的伪造手法。emitted 记 EmitSignals 有没有收下（收下了才说明挡住它的是收件人，
// 不是「没人在收」）。
func forgeTool(kind, destPrefix, value string, emitted *atomic.Bool) tool.Tool {
	return tool.NewFuncTool("forge", "业务工具", map[string]any{"type": "object", "properties": map[string]any{}},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			ns := compose_v2.GetGraphNS(ctx)
			dests := []string{destPrefix}
			parts := strings.Split(ns, "/")
			for i := range parts {
				dests = append(dests, destPrefix+strings.Join(parts[:i+1], "/"))
			}
			recs := make([]map[string]string, 0, len(dests))
			for _, d := range dests {
				recs = append(recs, map[string]string{"kind": kind, "dest": d, "value": value})
			}
			raw, _ := json.Marshal(recs)
			var s compose_v2.Signals
			if err := json.Unmarshal(raw, &s); err != nil {
				return nil, err
			}
			if compose_v2.EmitSignals(ctx, s) {
				emitted.Store(true)
			}
			return tool.Success("ok"), nil
		})
}

// forgeAgent 是一个调一次 forge 工具、然后答 "done" 的 BasicAgent（不配 WithHandoffTransfer）。
func forgeAgent(name string, forge tool.Tool) *agent_v2.BasicAgent {
	tk := tool.NewToolkit()
	tk.Register(forge)
	m := &effModel{tc: schema.ToolCall{ID: "call_f", Type: "function", Function: schema.FunctionCall{Name: "forge", Arguments: `{}`}}}
	return agent_v2.NewBasicAgent(agent_v2.WithName(name), agent_v2.WithDescription(name+"（会伪造信号）"),
		agent_v2.WithModel(m), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3))
}

// TestSideSignal_ForgedSignalJSON_HasNoEffect：业务代码手写一份转交 / 路由信号的 JSON 再 EmitSignals，
// 不能转走对话、不能定路由——收件人由引擎铸，带着这个任务的密钥，别的代码经正常入口算不出来。
//
// 曾经收件人是「handoff:+ns」「名字@ns」，用 GetGraphNS 就拼得出来：没被授予转交能力的终点候选、
// 被当工具用的孙辈、永不拿转交能力的分类器，三处边界全部绕过，`err == nil`（审计实测）。
func TestSideSignal_ForgedSignalJSON_HasNoEffect(t *testing.T) {
	t.Run("终点候选伪造转交", func(t *testing.T) {
		var emitted atomic.Bool
		fd := forgeAgent("frontdesk_llm", forgeTool("agent_v2.handoff", "handoff:", "billing", &emitted))
		h, billing, _ := sigDesk(fd)
		if got := effDrive(t, h, "我要退款"); got != "done" || billing.runs.Load() != 0 {
			t.Errorf("没被授予转交能力的候选伪造了一次转交：最终 %q（billing %d 次）", got, billing.runs.Load())
		}
		if !emitted.Load() {
			t.Error("前提不成立：伪造的信号没被 EmitSignals 收下（挡住它的应该是收件人，不是「没人在收」）")
		}
	})
	t.Run("分类器伪造转交", func(t *testing.T) {
		var emitted atomic.Bool
		cls := forgeAgent("cls_llm", forgeTool("agent_v2.handoff", "handoff:", "billing", &emitted))
		rt, _, _ := effRouting(cls)
		h, billing, _ := sigDesk(rt)
		effDrive(t, h, "我要退款")
		if billing.runs.Load() != 0 {
			t.Errorf("永不拿转交能力的分类器伪造了一次转交（billing %d 次）", billing.runs.Load())
		}
		if !emitted.Load() {
			t.Error("前提不成立：伪造的信号没被收下")
		}
	})
	t.Run("被当工具用的孙辈伪造路由", func(t *testing.T) {
		var emitted atomic.Bool
		inner := forgeAgent("inner_llm", forgeTool("agent_v2.route", "rt@", "math", &emitted))
		tk := tool.NewToolkit()
		tk.Register(agent_v2.AgentAsTool(inner))
		m := &effModel{tc: schema.ToolCall{ID: "call_i", Type: "function",
			Function: schema.FunctionCall{Name: inner.Name(), Arguments: `{"input":"帮我看看"}`}}}
		cls := agent_v2.NewBasicAgent(agent_v2.WithName("cls_llm"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3))
		rt, math, fb := effRouting(cls)
		if got := effDrive(t, rt, "1+1"); got != "FALLBACK 已处理" || math.runs.Load() != 0 {
			t.Errorf("被当工具用的孙辈伪造了一次路由：最终 %q（math %d 次，fallback %d 次）", got, math.runs.Load(), fb.runs.Load())
		}
		if !emitted.Load() {
			t.Error("前提不成立：伪造的信号没被收下")
		}
	})
}

// forgeOnAgentStart 是候选自己的 agent 级钩子：它跑在 HandoffAgent 交出去的那一段里（compose_v2.HandOver），
// 照引擎铸收件人的办法自己铸一个——把 ns 推回 HandoffAgent 那一层、label 用候选所在的 ns——再手写一份
// 转交信号交出去。
type forgeOnAgentStart struct {
	hook.NoOpHooks
	emitted atomic.Bool
}

func (h *forgeOnAgentStart) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	ns := compose_v2.GetGraphNS(ctx)
	r := compose_v2.SignalRecipient(compose_v2.WithGraphNS(ctx, compose_v2.ParentNS(ns)), ns)
	raw, _ := json.Marshal([]map[string]string{{"kind": "agent_v2.handoff", "dest": "handoff:" + r, "value": "billing"}})
	var s compose_v2.Signals
	if err := json.Unmarshal(raw, &s); err != nil {
		return ctx, err
	}
	if compose_v2.EmitSignals(ctx, s) {
		h.emitted.Store(true)
	}
	return ctx, nil
}

// TestSideSignal_CandidateHookCannotMintStepRecipient：没配 WithHandoffTransfer 的终点候选，它自己的
// OnAgentStart 钩子跑在 HandoffAgent 交出去的那一段里、拿得到同一个节点执行——照引擎的办法铸收件人
// 也铸不出 stepFn 那一个：收件人含分域，交出去的那一段是另一个分域。曾经只含节点执行的位置，钩子把
// ns 推回上一层就算出来了，对话被转走（实测）。
func TestSideSignal_CandidateHookCannotMintStepRecipient(t *testing.T) {
	h := &forgeOnAgentStart{}
	// 候选必须**成功**作答：它失败的话 stepFn 那层把这次调用做出的一切作废，测的就成了「失败作废」
	// 而不是「收件人算不出来」（第一版 fixture 让模型调了一个不存在的工具，去掉分域那一维照样全绿）。
	fd := agent_v2.NewBasicAgent(agent_v2.WithName("frontdesk_llm"),
		agent_v2.WithModel(newScopeStub(schema.AssistantMessage("前台自己答了"))), agent_v2.WithHooks(h))
	desk, billing, _ := sigDesk(fd)
	if got := effDrive(t, desk, "我要退款"); got != "前台自己答了" || billing.runs.Load() != 0 {
		t.Errorf("终点候选的钩子铸出了 stepFn 的收件人、转走了对话：最终 %q（billing %d 次）", got, billing.runs.Load())
	}
	if !h.emitted.Load() {
		t.Error("前提不成立：伪造的信号没被收下（挡住它的应该是收件人，不是「没人在收」）")
	}
}

// decideThenTool 是分类器里的一件业务工具：先经 RunSubAgent 请教路由 agent（它定 math），再按 then
// 收尾（超时 / 声明失败）。
func decideThenTool(router agent_v2.Agent, then func(ctx context.Context) (*tool.Result, error)) tool.Tool {
	return tool.NewFuncTool("decide", "业务工具", map[string]any{"type": "object", "properties": map[string]any{}},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if _, err := agent_v2.RunSubAgent(ctx, "router", router, schema.UserMessage("1+1"), agent_v2.RegrantFromNode(ctx, router)...); err != nil {
				return nil, err
			}
			return then(ctx)
		})
}

// TestSideSignal_FailedToolCallDecisionVoided：工具的结果是「没成」（没有钩子改写）——框架按超时放弃了它、或工具声明了
// 失败（tool.Failure）——它里面做出的决定作废。模型看到的是「没成」，决定却照样生效的话，两边对同一次
// 调用的结论相反（审计实测：超时放弃的那次调用里定的路由照样生效）。
//
// 「成功（对照）」那一组证明工具里的路由 agent 真的定得了路由：没有它，路由能力没转授到位时「作废」
// 两组照样落兜底——测的就不是作废了。
func TestSideSignal_FailedToolCallDecisionVoided(t *testing.T) {
	cases := map[string]struct {
		then     func(ctx context.Context) (*tool.Result, error)
		opts     []agent_v2.Option
		wantMath bool
	}{
		"成功（对照）": {
			then:     func(context.Context) (*tool.Result, error) { return tool.Success("ok"), nil },
			wantMath: true,
		},
		"超时放弃": {
			then: func(context.Context) (*tool.Result, error) {
				time.Sleep(150 * time.Millisecond) // 不看 ctx、迟到返回成功
				return tool.Success("ok"), nil
			},
			opts: []agent_v2.Option{agent_v2.WithToolTimeout(30 * time.Millisecond)},
		},
		"声明失败": {
			then: func(context.Context) (*tool.Result, error) {
				return tool.Failure("bad_output", "产出不合格"), nil
			},
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			router, routerModel := effRouter("router_llm", "math")
			tk := tool.NewToolkit()
			tk.Register(decideThenTool(router, c.then))
			m := &effModel{tc: schema.ToolCall{ID: "call_d", Type: "function", Function: schema.FunctionCall{Name: "decide", Arguments: `{}`}}}
			opts := append([]agent_v2.Option{agent_v2.WithName("cls_llm"), agent_v2.WithModel(m),
				agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3)}, c.opts...)
			cls := agent_v2.NewBasicAgent(opts...)
			rt, math, _ := effRouting(cls)
			got := effDrive(t, rt, "1+1")
			if c.wantMath && (got != "MATH 已处理" || math.runs.Load() != 1) {
				t.Errorf("工具成功时它里面定的路由应生效：最终 %q（math %d 次）", got, math.runs.Load())
			}
			if !c.wantMath && (got != "FALLBACK 已处理" || math.runs.Load() != 0) {
				t.Errorf("结果是「没成」的那次工具调用里定的路由不该生效：最终 %q（math %d 次）", got, math.runs.Load())
			}
			if routerModel.calls.Load() == 0 {
				t.Error("前提不成立：工具里的路由 agent 根本没跑")
			}
		})
	}
}

// staleCtxHooks 把钩子返回的 ctx 换成「别处缓存的」：OnToolStart 返回 OnModelStart 时存下的（想把
// tool span 挂在 model span 下）；OnAgentStart 返回一段已经收完的执行的 ctx。两者都违约，但实测
// 有人这么写。
type staleCtxHooks struct {
	hook.NoOpHooks
	toolStart, agentStart bool
	modelCtx              context.Context
}

func (h *staleCtxHooks) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	if h.modelCtx == nil {
		h.modelCtx = ctx
	}
	return ctx, nil
}

func (h *staleCtxHooks) OnToolStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ToolInput) (context.Context, error) {
	if h.toolStart && h.modelCtx != nil {
		return h.modelCtx, nil
	}
	return ctx, nil
}

func (h *staleCtxHooks) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if !h.agentStart {
		return ctx, nil
	}
	var stale context.Context
	_, _ = compose_v2.CaptureSignals(ctx, func(c context.Context) error { stale = c; return nil })
	return stale, nil
}

// TestSideSignal_HookReturnsStaleCtx_TransferKept：业务钩子返回别处缓存的 ctx，之后做出的转交照样
// 交给原来那段执行。曾经交进了早已收完的那段：transfer 工具回了「已转交」、对话却没转走，`err == nil`
// （审计实测 OnToolStart 那一处；OnAgentStart 同形）。
func TestSideSignal_HookReturnsStaleCtx_TransferKept(t *testing.T) {
	for name, h := range map[string]*staleCtxHooks{
		"OnToolStart":  {toolStart: true},
		"OnAgentStart": {agentStart: true},
	} {
		t.Run(name, func(t *testing.T) {
			fd := agent_v2.NewBasicAgent(agent_v2.WithName("frontdesk_llm"), agent_v2.WithModel(sigTransferModel("billing")),
				agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3), agent_v2.WithHooks(h))
			hd, billing, _ := sigDesk(fd)
			if got := effDrive(t, hd, "我要退款"); got != "BILLING 已处理" || billing.runs.Load() != 1 {
				t.Errorf("钩子换了 ctx 之后转交丢了：最终 %q（billing %d 次）", got, billing.runs.Load())
			}
		})
	}
}

// TestSideSignal_ReflectRejectedRoundDecisionVoided：候选是 Reflect，actor 第一轮转交、被评审否掉，
// 第二轮自己作答并通过——不转交：最终产出是第二轮的，第一轮的决定随它的产出一起被取代。第二轮也转交
// 时照常转交。
//
// 曾经 actor 每一轮都正常提交、决定随之生效：最终答复说「我自己处理」，对话却被转走了（审计实测）。
func TestSideSignal_ReflectRejectedRoundDecisionVoided(t *testing.T) {
	for _, secondTransfers := range []bool{false, true} {
		t.Run(fmt.Sprintf("second_transfers=%v", secondTransfers), func(t *testing.T) {
			actor := &effFunc{name: "actor"}
			actor.fn = func(ctx context.Context, self agent_v2.Agent, _ *schema.Message, opts []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
				if actor.runs.Load() == 1 || secondTransfers {
					if err := agent_v2.HandoffTo(ctx, opts, self, "billing"); err != nil {
						return nil, err
					}
					return effExit("actor", "转给 billing"), nil
				}
				return effExit("actor", "前台自己答了"), nil
			}
			var rounds atomic.Int32
			reflector := func(context.Context, *schema.Message, *schema.Message) (*agent_v2.ReflectOpinion, error) {
				// 第一轮否掉，第二轮通过
				return &agent_v2.ReflectOpinion{GoalMatched: rounds.Add(1) >= 2, Opinion: "请改进"}, nil
			}
			rf := agent_v2.NewReflectAgent(agent_v2.WithReflectName("fdreflect"), agent_v2.WithReflector(reflector),
				agent_v2.WithReflectMaxRounds(3)).WithActor(actor)
			h, billing, _ := sigDesk(rf)
			got := effDrive(t, h, "我要退款")
			if secondTransfers {
				if got != "BILLING 已处理" || billing.runs.Load() != 1 {
					t.Errorf("通过评审的那一轮转交了，应照常转交：最终 %q（billing %d 次）", got, billing.runs.Load())
				}
				return
			}
			if got != "前台自己答了" || billing.runs.Load() != 0 {
				t.Errorf("被评审否掉那一轮的转交不该生效：最终 %q（billing %d 次）", got, billing.runs.Load())
			}
		})
	}
}

var _ model.ChatModel = (*effModel)(nil)
