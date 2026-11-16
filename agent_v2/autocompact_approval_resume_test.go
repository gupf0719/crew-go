package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

func TestAutoCompact_ChangedRequestRevokesApproval(t *testing.T) {
	for _, failBeforeAsk := range []bool{false, true} {
		name := "asks_first_question"
		if failBeforeAsk {
			name = "fails_before_asking"
		}
		t.Run(name, func(t *testing.T) {
			const approveQuestion, finishQuestion = "approve input?", "finish summary?"
			const firstView, otherView = "document A", "document B"
			view := firstView
			stopped := errors.New("summary service unavailable")
			var authorized []string
			summary := &fnModel{fn: func(ctx context.Context, msgs []*schema.Message) (*schema.Message, error) {
				if countUserContent(msgs, view) != 1 {
					return nil, errors.New("unexpected summary input")
				}
				if view == otherView && failBeforeAsk {
					return nil, stopped
				}
				ctx = compose_v2.EnterPhase(ctx, "approval gate")
				approval, ok := compose_v2.GetStepResult[string](ctx, "approved")
				if !ok {
					answer, err := compose_v2.Ask(compose_v2.EnterPhase(ctx, "approve"), &compose_v2.AskQuery{Question: approveQuestion})
					if err != nil {
						return nil, err
					}
					approval = answer.Content
					authorized = append(authorized, view+": "+approval)
					if err := compose_v2.SaveStepResult(ctx, "approved", approval); err != nil {
						return nil, err
					}
				}
				answer, err := compose_v2.Ask(compose_v2.EnterPhase(ctx, "finish"), &compose_v2.AskQuery{Question: finishQuestion})
				if err != nil {
					return nil, err
				}
				if approval != "reapproved" || answer.Content != "finished" {
					return nil, errors.New("received stale summary authorization")
				}
				return schema.AssistantMessage("<summary>accepted history</summary>"), nil
			}}
			cfg := legacySummaryConfig(summary)
			builder := compose_v2.NewBuilder()
			builder.AddNode("summary", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				msgs := []*schema.Message{schema.UserMessage(view + strings.Repeat(" data", 32)), schema.UserMessage("latest")}
				compacted, err := agent_v2.AutoCompactRun(ctx, cfg, msgs)
				if err == nil && countUserContent(compacted, "accepted history") != 1 {
					return nil, errors.New("approved summary was not applied")
				}
				return nil, err
			})
			builder.AddEdge(compose_v2.StartNode, "summary")
			builder.AddEdge("summary", compose_v2.EndNode)
			graph, err := builder.Compile()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cp := compose_v2.NewInMemoryCheckpointer()
			answers := map[string]*schema.Message{}
			run := func() error {
				return compose_v2.Run(ctx, graph, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cp),
					compose_v2.WithSessionID("summary-revision"), compose_v2.WithResumeAnswers(answers))
			}
			firstID := compactionQuestionID(t, run(), approveQuestion)
			answers[firstID] = schema.UserMessage("approved A")
			oldFinishID := compactionQuestionID(t, run(), finishQuestion)
			answers[oldFinishID] = schema.UserMessage("retired finish")

			view = otherView
			err = run()
			if failBeforeAsk {
				if !errors.Is(err, stopped) {
					t.Fatalf("预期变更后的暂时失败，实际 %v", err)
				}
			} else if otherID := compactionQuestionID(t, err, approveQuestion); otherID == firstID {
				t.Fatalf("不同摘要请求共用问题 %q", otherID)
			}
			view = firstView
			newID := compactionQuestionID(t, run(), approveQuestion)
			if newID == firstID || len(authorized) != 1 {
				t.Fatalf("回到原输入也应重新审批，旧 ID %q 新 ID %q 已审批 %v", firstID, newID, authorized)
			}
			answers[newID] = schema.UserMessage("reapproved")
			newFinishID := compactionQuestionID(t, run(), finishQuestion)
			if newFinishID == oldFinishID {
				t.Fatal("摘要的后续问题不应复用旧答复")
			}
			answers[newFinishID] = schema.UserMessage("finished")
			if err := run(); err != nil || len(authorized) != 2 || authorized[1] != firstView+": reapproved" {
				t.Fatalf("应仅用重新审批后的结果完成摘要，已审批 %v 错误 %v", authorized, err)
			}
		})
	}
}

func TestAutoCompact_DistinctStructuredNumbersRequireDistinctApprovals(t *testing.T) {
	const question = "approve structured request?"
	value := uint64(1 << 53)
	var approved uint64
	summary := &fnModel{fn: func(ctx context.Context, msgs []*schema.Message) (*schema.Message, error) {
		for _, msg := range msgs {
			for _, block := range msg.ContentBlocks {
				if block.Data == nil || block.Data.Data != value {
					continue
				}
				if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: question}); err != nil {
					return nil, err
				}
				approved = value
				return schema.AssistantMessage("<summary>approved structured input</summary>"), nil
			}
		}
		return nil, errors.New("summary did not receive the structured number")
	}}
	cfg := legacySummaryConfig(summary)
	b := compose_v2.NewBuilder()
	b.AddNode("model", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		msg := schema.UserMessage(strings.Repeat("candidate record ", 32))
		msg.ContentBlocks = []*schema.ContentBlock{{Type: string(schema.ContentBlockTypeData),
			Data: &schema.DataContentBlock{Type: schema.ContentBlockTypeData, Data: value}}}
		compacted, err := agent_v2.AutoCompactRun(ctx, cfg, []*schema.Message{msg, schema.UserMessage("latest")})
		if err == nil && countUserContent(compacted, "approved structured input") != 1 {
			return nil, errors.New("approved summary was not applied")
		}
		return nil, err
	})
	b.AddEdge(compose_v2.StartNode, "model")
	b.AddEdge("model", compose_v2.EndNode)
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	answers := map[string]*schema.Message{}
	run := func() error {
		return compose_v2.Run(ctx, graph, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cp),
			compose_v2.WithSessionID("summary-structured-data"), compose_v2.WithResumeAnswers(answers))
	}
	firstID := compactionQuestionID(t, run(), question)
	answers[firstID] = schema.UserMessage("approve first number")
	value++
	nextID := compactionQuestionID(t, run(), question)
	if firstID == nextID || approved != 0 {
		t.Fatalf("相邻大整数应分属不同审批，ID %q/%q 已批准 %d", firstID, nextID, approved)
	}
	answers[nextID] = schema.UserMessage("approve second number")
	if err := run(); err != nil || approved != value {
		t.Fatalf("新审批应应用于 %d，实际 %d 错误 %v", value, approved, err)
	}
}

func TestAutoCompact_ReapproveOldSummaryKeepsBusinessQuestion(t *testing.T) {
	const summaryQuestion, businessQuestion = "approve generated summary?", "approve independent action?"
	oldSummaryID, saved := loadLegacySummaryCheckpoint(t, "de5a414-custom", businessQuestion)
	businessID := saved.PendingInterrupts[0].InterruptID
	var summaryApproved, businessApproved int
	summary := &fnModel{fn: func(ctx context.Context, in []*schema.Message) (*schema.Message, error) {
		if countUserContent(in, "file history:") != 1 {
			return nil, errors.New("summary history lost")
		}
		if err := checkLegacySummaryIsolation(ctx); err != nil {
			return nil, err
		}
		if err := legacySummaryApprove(ctx, summaryQuestion, "summary approved"); err != nil {
			return nil, err
		}
		summaryApproved++
		return schema.AssistantMessage("<summary>accepted history</summary>"), nil
	}}
	cfg := legacySummaryConfig(summary)
	msgs := []*schema.Message{schema.UserMessage(legacySummaryHistory), schema.AssistantMessage("notes"), schema.UserMessage("new request")}
	b := compose_v2.NewBuilder().WithName("legacy_dynamic_auto_compact")
	b.AddNode("model", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compacted, err := agent_v2.AutoCompactRun(ctx, cfg, msgs)
		if err != nil {
			return nil, err
		}
		if countUserContent(compacted, "accepted history") != 1 {
			return nil, errors.New("summary approval did not produce compacted history")
		}
		if err := legacySummaryApprove(compose_v2.EnterPhase(ctx, "auto_compact"), businessQuestion, "business approved"); err != nil {
			return nil, err
		}
		businessApproved++
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "model")
	b.AddEdge("model", compose_v2.EndNode)
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	if err := cp.SaveNS(ctx, saved.ID, "", saved); err != nil {
		t.Fatal(err)
	}
	answers := map[string]*schema.Message{oldSummaryID: schema.UserMessage("previously approved"), businessID: schema.UserMessage("business approved")}
	run := func() error {
		return compose_v2.Run(ctx, graph, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cp),
			compose_v2.WithSessionID(saved.ID), compose_v2.WithResumeAnswers(answers), compose_v2.WithResumeUserText("outside summary"))
	}
	newID := compactionQuestionID(t, run(), summaryQuestion)
	if newID == oldSummaryID || newID == businessID || summaryApproved != 0 || businessApproved != 0 {
		t.Fatalf("旧摘要与业务答复应隔离，实际问题 %q 摘要 %d 业务 %d", newID, summaryApproved, businessApproved)
	}
	answers[newID] = schema.UserMessage("summary approved")
	if err := run(); err != nil || summaryApproved != 1 || businessApproved != 1 {
		t.Fatalf("摘要重批后原业务答复仍应有效，摘要 %d 业务 %d 错误 %v", summaryApproved, businessApproved, err)
	}
}

type addSummaryReviewContext struct{ hook.NoOpHooks }

func (addSummaryReviewContext) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	in.Messages[len(in.Messages)-1].Content += " LOCAL-HOOK"
	if info.Round == 1 {
		in.Messages = append(in.Messages, schema.UserMessage("new context: "+strings.Repeat("review info ", 100)),
			schema.AssistantMessage("next review"), schema.UserMessage("latest change"))
	}
	return ctx, nil
}

func TestAutoCompact_ReapproveOldViewAfterMemoInvalidation(t *testing.T) {
	const previous, current = "approve incremental view?", "approve complete view?"
	_, saved := loadLegacySummaryCheckpoint(t, "de5a414-memo", previous)
	oldID := saved.PendingInterrupts[0].InterruptID
	sess := agent_v2.NewRunSession()
	sess.RunID = saved.ID
	var approved, mainCalls int
	summary := &fnModel{fn: func(ctx context.Context, msgs []*schema.Message) (*schema.Message, error) {
		text := inputText(msgs)
		if !strings.Contains(text, "raw history document") || strings.Contains(text, "condensed stable first pass") ||
			!strings.Contains(text, "new context:") {
			return nil, errors.New("summary did not receive the recalculated view")
		}
		if err := legacySummaryApprove(ctx, current, "complete view approved"); err != nil {
			return nil, err
		}
		approved++
		return schema.AssistantMessage("<summary>approved second pass</summary>"), nil
	}}
	main := &fnModel{fn: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
		mainCalls++
		if countUserContent(msgs, "approved second pass") != 1 {
			return nil, errors.New("main model did not receive the newly approved summary")
		}
		return schema.AssistantMessage("finished"), nil
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("compact"), agent_v2.WithModel(main), agent_v2.WithHooks(addSummaryReviewContext{}),
		agent_v2.WithAutoCompact(&agent_v2.AutoCompactConfig{Enabled: true, Model: summary, ModelContextWindow: 350,
			SummaryMaxTokens: 50, SummaryBufferTokens: 50, MinCompactableTokens: 1}))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	if err := cp.SaveNS(ctx, saved.ID, "", saved); err != nil {
		t.Fatal(err)
	}
	answers := map[string]*schema.Message{oldID: schema.UserMessage("incremental approved")}
	resume := func() ([]*agent_v2.AgentEvent, error) {
		return ag.Resume(ctx, saved.ID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	}
	_, err := resume()
	newID := compactionQuestionID(t, err, current)
	if newID == oldID || approved != 0 || mainCalls != 0 {
		t.Fatalf("重算视图应先询问再应用，旧 ID %q 新 ID %q 已批准 %d 主模型 %d", oldID, newID, approved, mainCalls)
	}
	answers[newID] = schema.UserMessage("complete view approved")
	events, err := resume()
	final := agent_v2.FinalMessage(events)
	if err != nil || final == nil || final.Content != "finished" || approved != 1 || mainCalls != 1 {
		t.Fatalf("重新审批后摘要应进入主模型，已批准 %d 主模型 %d 答复 %+v 错误 %v", approved, mainCalls, final, err)
	}
}
