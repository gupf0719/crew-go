package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// 条件边按 CondFailRerunFrom 否决父节点的产出时，作废的只有调用方点名的那几次嵌套调用
// （agent_v2.WithRerunnable）；别的已完成调用——扣款、问过人的把关——照常拿回。条件边没判完就
// 停下（软取消）不算否决，连点名的那次也照常拿回。

// planHost 造一个「plan 节点先调 charger、再调 planner，条件边校验计划」的 GraphAgent。judge 非空时
// 条件边里经 RunSubAgent 调判官；plannerOpts 交给 planner 那次调用。
func planHost(t *testing.T, charger, planner, judge agent_v2.Agent, plannerOpts ...agent_v2.RunOption) *agent_v2.GraphAgent {
	t.Helper()
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("plan", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		if _, err := agent_v2.RunSubAgent(ctx, "charger", charger, in); err != nil {
			return nil, err
		}
		ev, err := agent_v2.RunSubAgent(ctx, "planner", planner, in, plannerOpts...)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage(nestedFinal(ev)))}, nil
	})
	b.AddEdge(compose_v2.StartNode, "plan")
	b.AddConditionalEdge("plan", func(ctx context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		out, _ := compose_v2.Get(chs, outCh)
		if judge != nil {
			if _, err := agent_v2.RunSubAgent(ctx, "judge", judge, out); err != nil {
				return "", err
			}
		}
		if out == nil || strings.HasPrefix(out.Content, "BAD") {
			return "", fmt.Errorf("plan 不合法: %v", out)
		}
		return compose_v2.EndNode, nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("host"),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
}

// TestNestedCall_CondRejectRedoesOnlyTheDeclaredCall：同一个节点里先扣款、再调 planner，条件边否决了
// 计划。续跑时 planner（声明了 WithRerunnable）重新产出，扣款照常拿回——框架分不出哪次调用是
// 「被判定的产出」、哪次是「顺带做成的事」，一律作废就是重复扣款。
func TestNestedCall_CondRejectRedoesOnlyTheDeclaredCall(t *testing.T) {
	var charges, plans atomic.Int32
	charger := newSingleNodeAgent(t, "charger", "c", nil, func(context.Context, string) (string, error) {
		charges.Add(1)
		return "charged", nil
	})
	planner := newSingleNodeAgent(t, "planner", "p", nil, func(context.Context, string) (string, error) {
		if plans.Add(1) == 1 {
			return "BAD-plan", nil
		}
		return "plan-ok", nil
	})
	host := planHost(t, charger, planner, nil, agent_v2.WithRerunnable())
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	if _, err := host.Run(ctx, sess, schema.UserMessage("下单并做计划")); err == nil {
		t.Fatal("首跑的计划不合格，条件边应报错")
	}
	events, err := host.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess))
	if err != nil {
		t.Fatalf("续跑应收敛：%v", err)
	}
	if got := nestedFinal(events); got != "plan-ok" {
		t.Errorf("final = %q，want plan-ok", got)
	}
	if n := plans.Load(); n != 2 {
		t.Errorf("planner 声明了 WithRerunnable，被否决后应重新产出（共 2 次），实际 %d 次", n)
	}
	if n := charges.Load(); n != 1 {
		t.Errorf("扣款没声明 WithRerunnable，否决计划时不该再扣一次：扣了 %d 次", n)
	}
}

// TestNestedCall_CondRejectKeepsHookGateAnswer：OnNodeStart 钩子里经 RunSubAgent 问人把关、人批准了；
// 之后节点产出被条件边否决、节点重跑。把关那次调用没声明 WithRerunnable，照常拿回——不再问人。
func TestNestedCall_CondRejectKeepsHookGateAnswer(t *testing.T) {
	var asked inputLog
	var plans atomic.Int32
	hooks := &hpGuardHook{guard: hpGuard(t, &asked), targets: map[string]bool{"plan": true},
		input: func(n string) string { return "开始 " + n }}
	b := compose_v2.NewBuilder()
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(b, outCh)
	b.AddNode("plan", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		p := "plan-ok"
		if plans.Add(1) == 1 {
			p = "BAD-plan"
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage(p))}, nil
	})
	b.AddEdge(compose_v2.StartNode, "plan")
	b.AddConditionalEdge("plan", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		if out, _ := compose_v2.Get(chs, outCh); out == nil || strings.HasPrefix(out.Content, "BAD") {
			return "", errors.New("plan 不合法")
		}
		return compose_v2.EndNode, nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	host := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("host"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err = host.Run(ctx, sess, schema.UserMessage("go"), agent_v2.WithOrchHooks(hooks))
	answers := map[string]*schema.Message{}
	for round := 0; round < 6 && err != nil; round++ {
		if ie, ok := compose_v2.IsInterrupt(err); ok {
			for _, q := range ie.AllQueries() {
				answers[q.InterruptID] = schema.UserMessage("批准")
			}
		}
		_, err = host.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers),
			agent_v2.WithResumeOrchHooks(hooks), agent_v2.WithResumeSession(sess))
	}
	if err != nil {
		t.Fatalf("没能收敛：%v", err)
	}
	if n := plans.Load(); n != 2 {
		t.Fatalf("前提：节点应重新产出一次（共 2 次），实际 %d 次", n)
	}
	if got := asked.snapshot(); len(got) != 1 {
		t.Errorf("把关只该问一次（人已经批准过这次执行），实际问了 %d 次：%v", len(got), got)
	}
}

// TestNestedCall_SoftCancelDuringJudgeKeepsDeclaredCall：条件边里的判官跑着时用户按了停止。产出根本
// 没被判过：续跑时连声明了 WithRerunnable 的 planner 也照常拿回，扣款更不重做。
func TestNestedCall_SoftCancelDuringJudgeKeepsDeclaredCall(t *testing.T) {
	var plans, charges atomic.Int32
	sig := compose_v2.NewSoftCancelSignal()
	charger := newSingleNodeAgent(t, "charger", "c", nil, func(context.Context, string) (string, error) {
		charges.Add(1)
		return "charged", nil
	})
	planner := newSingleNodeAgent(t, "planner", "p", nil, func(context.Context, string) (string, error) {
		n := plans.Add(1)
		if n == 1 {
			sig.Trigger("用户按了停止") // planner 刚产出，判官一进图就看到这个信号
		}
		return fmt.Sprintf("plan-v%d", n), nil
	})
	var judged atomic.Int32
	judge := newSingleNodeAgent(t, "judge", "j", nil, func(_ context.Context, c string) (string, error) {
		judged.Add(1)
		return "ok:" + c, nil
	})
	host := planHost(t, charger, planner, judge, agent_v2.WithRerunnable())
	base := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := host.Run(compose_v2.WithSoftCancelCtx(base, sig), sess, schema.UserMessage("下单并做计划"))
	if !errors.Is(err, compose_v2.ErrSoftCanceled) {
		t.Fatalf("前提：首跑应停在软取消上，got %v", err)
	}
	events, err := host.Resume(base, sess.RunID, agent_v2.WithResumeSession(sess))
	if err != nil {
		t.Fatalf("续跑应收敛：%v", err)
	}
	if got := nestedFinal(events); got != "plan-v1" {
		t.Errorf("final = %q，want plan-v1（产出没被判过，应拿回原来那份）", got)
	}
	if n := plans.Load(); n != 1 {
		t.Errorf("判官被打断不算否决：planner 应只产出 1 次，实际 %d 次", n)
	}
	if n := charges.Load(); n != 1 {
		t.Errorf("用户只按了停止，扣款不该重做：扣了 %d 次", n)
	}
	if n := judged.Load(); n != 1 {
		t.Errorf("判官应在续跑时判一次（首跑被打断时没判完），实际判了 %d 次", n)
	}
}

// ── 声明了 WithRerunnable 的调用，否决之后重做的是整次调用 ──────────────────────────

// rerunHost 造一个 GraphAgent：plan 节点经 call 拿产出，条件边否决含 BAD 的产出（CondFailRerunFrom）。
func rerunHost(t *testing.T, call func(ctx context.Context, in *schema.Message) (string, error)) *agent_v2.GraphAgent {
	t.Helper()
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("plan", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		out, err := call(ctx, in)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage(out))}, nil
	})
	b.AddEdge(compose_v2.StartNode, "plan")
	b.AddConditionalEdge("plan", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		if out, _ := compose_v2.Get(chs, outCh); out == nil || strings.Contains(out.Content, "BAD") {
			return "", fmt.Errorf("plan 不合法: %v", out)
		}
		return compose_v2.EndNode, nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("host"),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
}

// resumeOnceAfterReject 跑一遍（首跑的产出应被否决），再续跑一次，返回续跑的最终消息与错误。
func resumeOnceAfterReject(t *testing.T, host agent_v2.Agent) (string, error) {
	t.Helper()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	if _, err := host.Run(ctx, sess, schema.UserMessage("做计划")); err == nil {
		t.Fatal("前提：首跑的计划不合格，条件边应报错")
	}
	events, err := host.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess))
	return nestedFinal(events), err
}

// shellAgent 是业务自研的壳：Run 里经 RunSubAgent 调真正干活的 inner（不转发调用方给的选项）。
type shellAgent struct{ inner agent_v2.Agent }

func (s *shellAgent) Name() string        { return "shell" }
func (s *shellAgent) Description() string { return "shell" }
func (s *shellAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, errors.New("shell: 续跑走父节点重跑")
}
func (s *shellAgent) Run(ctx context.Context, _ *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	ev, err := agent_v2.RunSubAgent(ctx, "inner", s.inner, in)
	if err != nil {
		return nil, err
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent("shell", schema.AssistantMessage("shell:"+nestedFinal(ev)))}, nil
}

// TestNestedCall_WithRerunnable_RedoesCustomAgentInternals：点名了 WithRerunnable 的被调方是业务自研的壳，
// 真正产出方案的 planner 是壳在自己的 Run 里经 RunSubAgent 调的。否决之后重做的必须是整次调用——壳的
// 那次调用记在调用方节点名下，只作废外层那条记录的话，壳重新执行了、里面的 planner 却原样拿回，产出
// 不变，续跑多少次都一样（实测）。被调方是图时它本来就整个重跑，两种被调方得是同一个语义。
func TestNestedCall_WithRerunnable_RedoesCustomAgentInternals(t *testing.T) {
	var plans atomic.Int32
	planner := newSingleNodeAgent(t, "planner", "p", nil, func(context.Context, string) (string, error) {
		if plans.Add(1) == 1 {
			return "BAD-plan", nil
		}
		return "plan-ok", nil
	})
	host := rerunHost(t, func(ctx context.Context, in *schema.Message) (string, error) {
		ev, err := agent_v2.RunSubAgent(ctx, "planner", &shellAgent{inner: planner}, in, agent_v2.WithRerunnable())
		if err != nil {
			return "", err
		}
		return nestedFinal(ev), nil
	})
	final, err := resumeOnceAfterReject(t, host)
	if err != nil || final != "shell:plan-ok" {
		t.Fatalf("否决之后一次续跑就该重做并收敛：final=%q err=%v", final, err)
	}
	if n := plans.Load(); n != 2 {
		t.Errorf("壳里的 planner 应重新产出（共 2 次），实际 %d 次", n)
	}
}

// TestNestedCall_WithRerunnable_RedoesCalleeEntryCondRouter：点名了 WithRerunnable 的被调方是一张图，它的
// 入口条件边经 RunSubAgent 调 router 选分支（router 选错了分支，产出不合格）。入口条件边记在调用方
// 节点名下、这次调用的分域里——重做整次调用时它也得重新判。
func TestNestedCall_WithRerunnable_RedoesCalleeEntryCondRouter(t *testing.T) {
	var routes atomic.Int32
	router := newSingleNodeAgent(t, "router", "r", nil, func(context.Context, string) (string, error) {
		if routes.Add(1) == 1 {
			return "bad", nil
		}
		return "good", nil
	})
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	for id, out := range map[string]string{"bad": "BAD-plan", "good": "plan-ok"} {
		out := out
		b.AddNode(compose_v2.NodeID(id), func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage(out))}, nil
		})
		b.AddEdge(compose_v2.NodeID(id), compose_v2.EndNode)
	}
	b.AddConditionalEdge(compose_v2.StartNode, func(ctx context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		in, _ := compose_v2.Get(chs, inCh)
		ev, err := agent_v2.RunSubAgent(ctx, "router", router, in)
		if err != nil {
			return "", err
		}
		return compose_v2.NodeID(nestedFinal(ev)), nil
	})
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	callee := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("plannerg"),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
	host := rerunHost(t, func(ctx context.Context, in *schema.Message) (string, error) {
		ev, err := agent_v2.RunSubAgent(ctx, "planner", callee, in, agent_v2.WithRerunnable())
		if err != nil {
			return "", err
		}
		return nestedFinal(ev), nil
	})
	final, err := resumeOnceAfterReject(t, host)
	if err != nil || final != "plan-ok" {
		t.Fatalf("否决之后一次续跑就该重做并收敛：final=%q err=%v", final, err)
	}
	if n := routes.Load(); n != 2 {
		t.Errorf("被调方入口条件边里的 router 应重新判（共 2 次），实际 %d 次", n)
	}
}

// TestNestedCall_RedoneCallAsksAgain：点名了 WithRerunnable 的 planner 在自己的图里先问「批准方案 vN 吗」
// 再产出，条件边否决 v1。调用方累积传历次答案。重做的那一次问的是 v2——必须是个新问题；跟 v1 那问
// 同一个 ID 的话，用户对 v1 的批准直接兑付给 v2，v2 的问句从头到尾没出现（实测）。
func TestNestedCall_RedoneCallAsksAgain(t *testing.T) {
	var plans atomic.Int32
	planner := newSingleNodeAgent(t, "planner", "p", nil, func(ctx context.Context, _ string) (string, error) {
		n := plans.Load() + 1
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: fmt.Sprintf("批准方案 v%d 吗", n)})
		if err != nil {
			return "", err
		}
		plans.Add(1)
		if n == 1 {
			return "BAD-plan(" + ans.Content + ")", nil
		}
		return fmt.Sprintf("plan-ok v%d (%s)", n, ans.Content), nil
	})
	host := rerunHost(t, func(ctx context.Context, in *schema.Message) (string, error) {
		ev, err := agent_v2.RunSubAgent(ctx, "planner", planner, in, agent_v2.WithRerunnable())
		if err != nil {
			return "", err
		}
		return nestedFinal(ev), nil
	})
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	answers := map[string]*schema.Message{}
	var asked []string
	events, err := host.Run(ctx, sess, schema.UserMessage("做计划"))
	for i := 0; i < 8 && err != nil; i++ {
		if ie, ok := compose_v2.IsInterrupt(err); ok {
			for _, q := range ie.AllQueries() {
				asked = append(asked, q.Question)
				answers[q.InterruptID] = schema.UserMessage(fmt.Sprintf("批准#%d", len(asked)))
			}
		}
		events, err = host.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers), agent_v2.WithResumeSession(sess))
	}
	if err != nil {
		t.Fatalf("没能收敛：%v", err)
	}
	if got := nestedFinal(events); got != "plan-ok v2 (批准#2)" {
		t.Errorf("执行的 v2 必须是被单独问过、单独批准的：final=%q，问过的=%q", got, asked)
	}
}
