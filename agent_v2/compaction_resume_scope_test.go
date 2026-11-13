package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
)

func compactionTestSession() *agent_v2.RunSession {
	sess := agent_v2.NewRunSession()
	for range 5 {
		sess.AddEvent(agent_v2.NewOutputEvent("compact", schema.UserMessage(strings.Repeat("old question ", 100))))
		sess.AddEvent(agent_v2.NewOutputEvent("compact", schema.AssistantMessage(strings.Repeat("old answer ", 100))))
	}
	return sess
}

func compactionTestConfig(m model.ChatModel) *agent_v2.AutoCompactConfig {
	return &agent_v2.AutoCompactConfig{Enabled: true, Model: m, ModelContextWindow: 300,
		SummaryMaxTokens: 50, SummaryBufferTokens: 50, MinCompactableTokens: 1}
}

func TestAutoCompact_HalfOpenRecovers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var summaryCalls, mainCalls int
		summary := &fnModel{fn: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
			summaryCalls++
			if countUserContent(msgs, "old question") == 0 {
				return nil, errors.New("missing history in summary input")
			}
			switch summaryCalls {
			case 1:
				return nil, errors.New("summary provider unavailable")
			case 2:
				return nil, nil // 半开拿到无效的答复，重新进入冷却
			default:
				return schema.AssistantMessage("<summary>history compressed</summary>"), nil
			}
		}}
		main := &fnModel{fn: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
			mainCalls++
			switch {
			case countUserContent(msgs, "history compressed") == 1:
				return schema.AssistantMessage("摘要已送入主模型"), nil
			case countUserContent(msgs, "old question") > 0:
				return schema.AssistantMessage("原历史已送入主模型"), nil
			default:
				return nil, errors.New("main model received neither history nor summary")
			}
		}}
		cfg := compactionTestConfig(summary)
		cfg.MaxConsecutiveFailures = 1
		ag := agent_v2.NewBasicAgent(agent_v2.WithName("compact"), agent_v2.WithModel(main), agent_v2.WithAutoCompact(cfg))
		sess := compactionTestSession()
		for i, tc := range []struct {
			advance time.Duration
			calls   int
			output  string
		}{
			{0, 1, "原历史已送入主模型"},
			{0, 1, "原历史已送入主模型"},
			{5 * time.Minute, 2, "原历史已送入主模型"},
			{0, 2, "原历史已送入主模型"},
			{5 * time.Minute, 3, "摘要已送入主模型"},
			{0, 4, "摘要已送入主模型"},
		} {
			time.Sleep(tc.advance)
			events, err := ag.Run(t.Context(), sess, schema.UserMessage("new question"))
			if err != nil {
				t.Fatal(err)
			}
			final := agent_v2.FinalMessage(events)
			if summaryCalls != tc.calls || mainCalls != i+1 || final == nil || final.Content != tc.output {
				t.Fatalf("第 %d 次运行，摘要调用 %d（预期 %d），主模型调用 %d，输出 %+v（预期 %q）",
					i+1, summaryCalls, tc.calls, mainCalls, final, tc.output)
			}
		}
	})
}

func TestAutoCompact_SummaryAndMainAskResumeSeparately(t *testing.T) {
	var summaryAnswered, mainAnswered int
	summary := &fnModel{fn: func(ctx context.Context, msgs []*schema.Message) (*schema.Message, error) {
		if countUserContent(msgs, "old question") == 0 {
			return nil, errors.New("missing history in summary input")
		}
		for _, view := range []context.Context{ctx, compose_v2.TaskCommitContext(ctx)} {
			_, hasText := compose_v2.GetResumeUserText(view)
			if _, ok := compose_v2.GetResumeContext(view); ok || hasText {
				return nil, errors.New("summary can read unrestricted resume data")
			}
			if _, ok := compose_v2.GetResumeAnswer(view, "unrelated"); ok {
				return nil, errors.New("summary received an unrelated answer")
			}
		}
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准摘要？"})
		if err != nil {
			return nil, err
		}
		if ans.Content != "summary" {
			return nil, errors.New("wrong summary answer")
		}
		summaryAnswered++
		return schema.AssistantMessage("<summary>approved</summary>"), nil
	}}
	main := &fnModel{fn: func(ctx context.Context, msgs []*schema.Message) (*schema.Message, error) {
		if countUserContent(msgs, "Previous Conversation Summary") != 1 || countUserContent(msgs, "new question") != 1 {
			return nil, errors.New("main model did not receive summary and current question")
		}
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准主模型？"})
		if err != nil {
			return nil, err
		}
		if ans.Content != "main" {
			return nil, errors.New("wrong main answer")
		}
		mainAnswered++
		return schema.AssistantMessage("完成"), nil
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("compact"), agent_v2.WithModel(main),
		agent_v2.WithAutoCompact(compactionTestConfig(summary)))
	sess := compactionTestSession()
	cp := compose_v2.NewInMemoryCheckpointer()
	deadlineCtx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	ctx := compose_v2.WithCheckpointerCtx(deadlineCtx, cp)
	_, err := ag.Run(ctx, sess, schema.UserMessage("new question"))
	summaryID := compactionQuestionID(t, err, "批准摘要？")
	answers := map[string]*schema.Message{
		summaryID:   schema.UserMessage("summary"),
		"unrelated": schema.UserMessage("other answer"),
	}
	_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(answers), agent_v2.WithResumeUserText("other text"))
	mainID := compactionQuestionID(t, err, "批准主模型？")
	if mainID == summaryID || summaryAnswered != 1 || mainAnswered != 0 {
		t.Fatalf("摘要和主模型提问未隔离，summary=%q/%d main=%q/%d", summaryID, summaryAnswered, mainID, mainAnswered)
	}
	answers[mainID] = schema.UserMessage("main")
	events, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	if err != nil {
		t.Fatal(err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || final.Content != "完成" || summaryAnswered != 2 || mainAnswered != 1 {
		t.Fatalf("续跑产出或答复次数不符，final=%+v summary=%d main=%d", final, summaryAnswered, mainAnswered)
	}
	if pending, err := cp.ListPending(ctx, sess.RunID); err != nil || len(pending) != 0 {
		t.Fatalf("成功后仍有挂起问题 %v, err=%v", pending, err)
	}
}

func compactionQuestionID(t *testing.T, err error, question string) string {
	t.Helper()
	interrupt, ok := compose_v2.IsInterrupt(err)
	if !ok || len(interrupt.AllQueries()) != 1 || interrupt.AllQueries()[0].Question != question {
		t.Fatalf("期望问题 %q，实际 %v", question, err)
	}
	return interrupt.AllQueries()[0].InterruptID
}

func TestAutoCompact_TerminalErrorStopsMainModel(t *testing.T) {
	var summaryCalls int
	summary := &fnModel{fn: func(context.Context, []*schema.Message) (*schema.Message, error) {
		summaryCalls++
		return nil, compose_v2.ErrRunAborted
	}}
	main := &compactLoopModel{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("compact"), agent_v2.WithModel(main),
		agent_v2.WithAutoCompact(compactionTestConfig(summary)))
	_, err := ag.Run(t.Context(), compactionTestSession(), schema.UserMessage("new question"))
	if !errors.Is(err, compose_v2.ErrRunAborted) || summaryCalls != 1 || main.calls != 0 {
		t.Fatalf("终局错误未停止主链路，err=%v summary=%d main=%d", err, summaryCalls, main.calls)
	}
}

func TestAutoCompact_CancelledSummaryStopsMainModel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	summaryCalls := 0
	summary := &fnModel{fn: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
		summaryCalls++
		if countUserContent(msgs, "old question") == 0 {
			return nil, errors.New("missing history in summary input")
		}
		cancel()
		return schema.AssistantMessage("<summary>compressed</summary>"), nil
	}}
	main := &compactLoopModel{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("compact"), agent_v2.WithModel(main),
		agent_v2.WithAutoCompact(compactionTestConfig(summary)))
	_, err := ag.Run(ctx, compactionTestSession(), schema.UserMessage("new question"))
	if !errors.Is(err, context.Canceled) || summaryCalls != 1 || main.calls != 0 {
		t.Fatalf("取消后应停止主链路，err=%v summary=%d main=%d", err, summaryCalls, main.calls)
	}
}

type compactSiblingHook struct {
	hook.NoOpHooks
	askStarted <-chan struct{}
}

func (h compactSiblingHook) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	if compose_v2.GetGraphNS(ctx) == "fail" {
		select {
		case <-h.askStarted:
		case <-ctx.Done():
			return ctx, ctx.Err()
		}
	}
	return ctx, nil
}

func TestAutoCompact_PendingSummarySurvivesSiblingTrip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var askCalls, failCalls, askDelivered, mainCalls, mainDelivered atomic.Int32
		askStarted := make(chan struct{})
		var markAsk sync.Once
		summary := &fnModel{fn: func(ctx context.Context, msgs []*schema.Message) (*schema.Message, error) {
			if countUserContent(msgs, "old question") == 0 {
				return nil, errors.New("missing history in summary input")
			}
			switch ns := compose_v2.GetGraphNS(ctx); ns {
			case "wait":
				askCalls.Add(1)
				markAsk.Do(func() { close(askStarted) })
				answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "approve shared summary?"})
				if err != nil {
					return nil, err
				}
				if answer == nil || answer.Content != "approved" {
					return nil, errors.New("missing approval")
				}
				askDelivered.Add(1)
				return schema.AssistantMessage("<summary>approved shared summary</summary>"), nil
			case "fail":
				failCalls.Add(1)
				return nil, errors.New("summary provider unavailable")
			default:
				return nil, fmt.Errorf("unexpected summary namespace %q", ns)
			}
		}}
		main := &fnModel{fn: func(ctx context.Context, msgs []*schema.Message) (*schema.Message, error) {
			if compose_v2.GetGraphNS(ctx) != "wait" {
				return schema.AssistantMessage("failure branch completed"), nil
			}
			mainCalls.Add(1)
			if countUserContent(msgs, "approved shared summary") != 1 {
				return nil, errors.New("main model ran without approved summary")
			}
			answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "approve main output?"})
			if err != nil {
				return nil, err
			}
			if answer == nil || answer.Content != "main yes" {
				return nil, errors.New("main model received wrong answer")
			}
			mainDelivered.Add(1)
			return schema.AssistantMessage("branch completed"), nil
		}}
		cfg := compactionTestConfig(summary)
		cfg.MaxConsecutiveFailures = 1
		child := agent_v2.NewBasicAgent(agent_v2.WithName("compact"), agent_v2.WithModel(main),
			agent_v2.WithHooks(compactSiblingHook{askStarted: askStarted}), agent_v2.WithAutoCompact(cfg))
		par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).AddBranch("wait", child).AddBranch("fail", child)
		sess := compactionTestSession()
		cp := compose_v2.NewInMemoryCheckpointer()
		deadline, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		ctx := compose_v2.WithCheckpointerCtx(deadline, cp)
		_, err := par.Run(ctx, sess, schema.UserMessage("same input"))
		id := compactionQuestionID(t, err, "approve shared summary?")
		if askCalls.Load() != 1 || failCalls.Load() != 1 || askDelivered.Load() != 0 {
			t.Fatalf("precondition failed: ask=%d fail=%d delivered=%d", askCalls.Load(), failCalls.Load(), askDelivered.Load())
		}
		pending, err := cp.ListPending(ctx, sess.RunID)
		if err != nil || len(pending) < 1 {
			t.Fatalf("question was not checkpointed: pending=%+v err=%v", pending, err)
		}
		_, err = par.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess))
		if again := compactionQuestionID(t, err, "approve shared summary?"); again != id || askCalls.Load() != 2 || mainCalls.Load() != 0 {
			t.Fatalf("without an answer should suspend at the same position, id=%q again=%q ask=%d main=%d", id, again, askCalls.Load(), mainCalls.Load())
		}
		answers := map[string]*schema.Message{id: schema.UserMessage("approved")}
		_, err = par.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
		mainID := compactionQuestionID(t, err, "approve main output?")
		if mainID == id || askDelivered.Load() != 1 || mainDelivered.Load() != 0 || failCalls.Load() != 1 {
			t.Fatalf("first answer mismatched, summary=%d main=%d fail=%d", askDelivered.Load(), mainDelivered.Load(), failCalls.Load())
		}
		answers[mainID] = schema.UserMessage("main yes")
		events, err := par.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
		if err != nil || agent_v2.FinalMessage(events) == nil || askCalls.Load() != 4 || failCalls.Load() != 1 ||
			askDelivered.Load() != 2 || mainCalls.Load() != 2 || mainDelivered.Load() != 1 {
			t.Fatalf("resume result=%+v err=%v ask=%d fail=%d delivered=%d main=%d mainDelivered=%d",
				agent_v2.FinalMessage(events), err, askCalls.Load(), failCalls.Load(), askDelivered.Load(), mainCalls.Load(), mainDelivered.Load())
		}
		pending, err = cp.ListPending(ctx, sess.RunID)
		if err != nil || len(pending) != 0 {
			t.Fatalf("successful resume left pending checkpoint: %v %v", pending, err)
		}
	})
}

func TestAutoCompact_SeparateCalls(t *testing.T) {
	msgs := make([]*schema.Message, 0, 11)
	for range 5 {
		msgs = append(msgs, schema.UserMessage(strings.Repeat("old question ", 100)),
			schema.AssistantMessage(strings.Repeat("old answer ", 100)))
	}
	msgs = append(msgs, schema.UserMessage("new question"))
	for _, distinct := range []bool{true, false} {
		t.Run(fmt.Sprintf("caller_phases_%t", distinct), func(t *testing.T) {
			var accepted [2]int
			newSummary := func(idx int, question, want string) *agent_v2.AutoCompactConfig {
				return compactionTestConfig(&fnModel{fn: func(ctx context.Context, input []*schema.Message) (*schema.Message, error) {
					if countUserContent(input, "old question") == 0 {
						return nil, errors.New("missing history in summary input")
					}
					ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: question})
					if err != nil {
						return nil, err
					}
					if ans.Content != want {
						return nil, errors.New("another summary's answer was delivered")
					}
					accepted[idx]++
					return schema.AssistantMessage("<summary>compressed</summary>"), nil
				}})
			}
			one := newSummary(0, "允许摘要甲？", "yes to first")
			two := newSummary(1, "允许摘要乙？", "yes to second")
			b := compose_v2.NewBuilder().WithName("separate_auto_compactions")
			b.AddNode("model", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				a, b := ctx, ctx
				if distinct {
					a = compose_v2.EnterPhase(ctx, "input-a")
					b = compose_v2.EnterPhase(ctx, "input-b")
				}
				if _, err := agent_v2.AutoCompactRun(a, one, msgs); err != nil {
					return nil, err
				}
				if _, err := agent_v2.AutoCompactRun(b, two, msgs); err != nil {
					return nil, err
				}
				return nil, nil
			})
			b.AddEdge(compose_v2.StartNode, "model")
			b.AddEdge("model", compose_v2.EndNode)
			g, err := b.Compile()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			cp := compose_v2.NewInMemoryCheckpointer()
			opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("two-summaries")}
			err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...)
			id1 := compactionQuestionID(t, err, "允许摘要甲？")
			answers := map[string]*schema.Message{id1: schema.UserMessage("yes to first")}
			err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), append(opts, compose_v2.WithResumeAnswers(answers))...)
			id2 := compactionQuestionID(t, err, "允许摘要乙？")
			if id2 == id1 {
				t.Fatalf("两处摘要的提问 ID 相同：%q", id2)
			}
			answers[id2] = schema.UserMessage("yes to second")
			err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), append(opts, compose_v2.WithResumeAnswers(answers))...)
			if err != nil || accepted != [2]int{2, 1} {
				t.Fatalf("两处摘要应收到各自的答复，实际 %v err=%v", accepted, err)
			}
			if pending, err := cp.ListPending(ctx, "two-summaries"); err != nil || len(pending) != 0 {
				t.Fatalf("成功后仍有挂起问题 %v，err=%v", pending, err)
			}
		})
	}
}
