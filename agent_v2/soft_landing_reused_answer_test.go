package agent_v2_test

// 软降落与「沿用原答复续跑」的组合：OnModelEnd 停下等人之后续跑，交给 tools 节点的是记录里那份答复，「这批调用执不执行」
// 要按那份答复生成时是不是软降落轮判，不能按续跑时的预算现算（续跑可以用 WithResumeMaxRounds 调预算）。

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// askOnRoundEnd 在第 round 轮的 OnModelEnd 里问一次用户：第一遍停下，续跑那一遍拿到答案放行。
type askOnRoundEnd struct {
	hook.NoOpHooks
	round int32
}

func (h askOnRoundEnd) OnModelEnd(ctx context.Context, info *hook.AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
	if info == nil || info.Round != h.round {
		return ctx, nil
	}
	if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "这份答复行吗？"}); err != nil {
		return ctx, err
	}
	return ctx, nil
}

// runAndStopOnModelEnd 跑到第 stopRound 轮的 OnModelEnd 停下，返回续跑要用的东西。
func runAndStopOnModelEnd(t *testing.T, m *landingModel, n *atomic.Int32, maxRounds int, stopRound int32) (agent_v2.Agent, context.Context, *agent_v2.RunSession, string) {
	t.Helper()
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("sl"), agent_v2.WithModel(m), agent_v2.WithHooks(askOnRoundEnd{round: stopRound}),
		agent_v2.WithToolkit(kitOf(namedBizTool("noop", n, "ok"))), agent_v2.WithMaxRounds(maxRounds))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("干活"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture: 应停在第 %d 轮 OnModelEnd 的提问上，got %v", stopRound, err)
	}
	return ag, ctx, sess, ie.AllQueries()[0].InterruptID
}

// TestSoftLanding_ShrunkBudgetResumeOnReusedAnswer：第 3 轮（可以调工具）的 OnModelEnd 停下，续跑把预算缩到 1。沿用的那份
// 答复是在可以调工具的一轮里生成的，它的工具调用照常执行；之后给一次软降落（none）、拿到收尾答复——缩预算续跑照常软降落。
func TestSoftLanding_ShrunkBudgetResumeOnReusedAnswer(t *testing.T) {
	var n atomic.Int32
	m := &landingModel{}
	ag, ctx, sess, id := runAndStopOnModelEnd(t, m, &n, 5, 3)
	before, _ := m.seen()
	events, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeMaxRounds(1),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{id: schema.UserMessage("行")}))
	after, choices := m.seen()
	resumed := choices[len(before):]
	if len(after)-len(before) != 1 || !isNone(resumed[0]) {
		t.Fatalf("缩预算续跑应恰好调一次模型且带 tool_choice=none（软降落），got %d 次、choices=%v（err=%v）",
			len(after)-len(before), choiceNames(resumed), err)
	}
	if fm := agent_v2.FinalMessage(events); fm == nil || fm.Content != "收尾答复" {
		t.Errorf("软降落之后应拿到收尾答复，got %v", fm)
	}
}

// TestSoftLanding_RaisedBudgetResumeOnReusedSoftLandingAnswer：软降落轮（第 2 轮，预算 2）的 OnModelEnd 停下，那份答复里模型
// 不认 none、硬调了 noop。续跑把预算调到 3：沿用的答复仍是软降落轮生成的，硬调的工具不执行。
func TestSoftLanding_RaisedBudgetResumeOnReusedSoftLandingAnswer(t *testing.T) {
	var n atomic.Int32
	m := &landingModel{hardCall: "noop"}
	ag, ctx, sess, id := runAndStopOnModelEnd(t, m, &n, 2, 2)
	nBefore := n.Load()
	_, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeMaxRounds(3),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{id: schema.UserMessage("行")}))
	if got := n.Load() - nBefore; got != 0 {
		t.Errorf("软降落轮里硬调的工具续跑时不该执行，执行了 %d 次（err=%v）", got, err)
	}
}

// TestSoftLanding_SameBudgetResumeOnReusedSoftLandingAnswer：预算不变时，软降落轮停下再续跑——不再调模型、硬调不执行，
// 以 MaxIterationsError 结束。
func TestSoftLanding_SameBudgetResumeOnReusedSoftLandingAnswer(t *testing.T) {
	var n atomic.Int32
	m := &landingModel{hardCall: "noop"}
	ag, ctx, sess, id := runAndStopOnModelEnd(t, m, &n, 2, 2)
	nBefore := n.Load()
	before, _ := m.seen()
	_, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{id: schema.UserMessage("行")}))
	after, _ := m.seen()
	if len(after)-len(before) != 0 || n.Load()-nBefore != 0 {
		t.Errorf("同预算续跑不该再调模型、不该执行硬调的工具，got 模型 %d 次、工具 %d 次", len(after)-len(before), n.Load()-nBefore)
	}
	var mie *compose_v2.MaxIterationsError
	if !errors.As(err, &mie) {
		t.Errorf("应以 MaxIterationsError 结束，got %v", err)
	}
}
