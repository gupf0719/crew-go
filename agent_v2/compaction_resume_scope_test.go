package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

type compactionTestModel struct {
	generate func(context.Context) (*schema.Message, error)
}

func (m compactionTestModel) Generate(ctx context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return m.generate(ctx)
}
func (m compactionTestModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m compactionTestModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

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

func TestAutoCompact_SummaryAndMainAskResumeSeparately(t *testing.T) {
	var summaryAnswered, mainAnswered int
	summary := compactionTestModel{generate: func(ctx context.Context) (*schema.Message, error) {
		_, hasText := compose_v2.GetResumeUserText(ctx)
		if _, ok := compose_v2.GetResumeContext(ctx); ok || hasText {
			return nil, errors.New("summary can read unrestricted resume data")
		}
		if _, ok := compose_v2.GetResumeAnswer(ctx, "unrelated"); ok {
			return nil, errors.New("summary received an unrelated answer")
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
	main := compactionTestModel{generate: func(ctx context.Context) (*schema.Message, error) {
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
	ctx := compose_v2.WithCheckpointerCtx(t.Context(), cp)
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
	summary := compactionTestModel{generate: func(context.Context) (*schema.Message, error) {
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
