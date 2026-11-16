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
	"crew-go/store"
)

type inPlaceModelRedaction struct{ hook.NoOpHooks }

func (inPlaceModelRedaction) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	out.Message.Content = "[redacted]"
	delete(out.Message.Annotations, compose_v2.RunIDAnnotationKey)
	return ctx, nil
}

func TestOnModelEnd_InPlaceRewriteKeepsStreamIdentity(t *testing.T) {
	capture := &captureEventsHook{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("redact"),
		agent_v2.WithModel(&chunkStreamModel{contents: []string{"secret", " value"}}),
		agent_v2.WithHooks(inPlaceModelRedaction{}))
	events, err := ag.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("hello"), agent_v2.WithOrchHooks(capture))
	if err != nil {
		t.Fatal(err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || final.Content != "[redacted]" {
		t.Fatalf("最终答复 = %+v", final)
	}
	runID, _ := final.Annotations[compose_v2.RunIDAnnotationKey].(string)
	var raw strings.Builder
	var resets int
	for _, ev := range capture.events {
		switch ev.Type {
		case "chunk":
			raw.WriteString(ev.Content)
		case "reset_stream":
			if ev.Metadata["replacement"] != "[redacted]" || ev.Metadata["run_id"] != runID {
				t.Errorf("替换事件未指向原调用，事件=%+v run_id=%q", ev, runID)
			}
			resets++
		}
	}
	if runID == "" || raw.String() != "secret value" || resets != 1 {
		t.Fatalf("未用原调用身份替换已发片段，run_id=%q chunks=%q resets=%d", runID, raw.String(), resets)
	}
}

type editModelMessages struct {
	hook.NoOpHooks
	replaceElement bool
}

func (h editModelMessages) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	if len(in.Messages) == 0 {
		return ctx, nil
	}
	i := len(in.Messages) - 1
	if h.replaceElement {
		cp := *in.Messages[i]
		in.Messages[i] = &cp
	}
	in.Messages[i].Content += " TRANSIENT-HOOK"
	return ctx, nil
}

func TestOnModelStart_EditedViewStaysOutOfCompactionHistory(t *testing.T) {
	for _, tc := range []struct {
		name           string
		replaceElement bool
	}{{"replace_element", true}, {"edit_message", false}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := compose_v2.WithThreadIDCtx(t.Context(), "conv")
			key := agent_v2.HistoryKey{Owner: "t", ConvID: "conv"}
			hs := agent_v2.NewInMemoryHistoryStore()
			summary := &countingSummarizer{}
			model := &compactLoopModel{rounds: 60}
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(model),
				agent_v2.WithToolkit(grepToolkit()), agent_v2.WithMaxRounds(100),
				agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxHistoryPairs(0),
				agent_v2.WithHooks(editModelMessages{replaceElement: tc.replaceElement}),
				agent_v2.WithAutoCompact(smallWindow(summary)))
			if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("ORIGINAL-INPUT")); err != nil {
				t.Fatal(err)
			}
			if summary.count() == 0 || !strings.Contains(inputText(model.inputs[0]), "TRANSIENT-HOOK") {
				t.Fatal("测试未同时触发本轮改写和压缩")
			}
			all, err := agent_v2.LoadAllHistory(ctx, hs, key)
			if err != nil {
				t.Fatal(err)
			}
			if n := countBoundaries(all); n != 0 {
				t.Fatalf("钩子改写的视图产生了 %d 份跨 Run 边界", n)
			}
			nextModel := &compactLoopModel{}
			next := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(nextModel),
				agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxHistoryPairs(0))
			if _, err := next.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("NEXT-INPUT")); err != nil {
				t.Fatal(err)
			}
			got := inputText(nextModel.inputs[0])
			if !strings.Contains(got, "ORIGINAL-INPUT") || strings.Contains(got, "TRANSIENT-HOOK") {
				t.Fatal("后续 Run 未恢复原始历史，或看到上一轮的瞬态改写")
			}
		})
	}
}

type markSkillBodyOnSecondRound struct{ hook.NoOpHooks }

func (markSkillBodyOnSecondRound) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	// 两轮都用瞬态视图。变的是旧工具结果的保护标注，最后一条消息始终在备忘前缀之外。
	in.Messages[len(in.Messages)-1].Content += " LOCAL-HOOK"
	if info.Round == 1 {
		for _, msg := range in.Messages {
			if schema.GetToolResultID(msg) == "prior-read" {
				if msg.Annotations == nil {
					msg.Annotations = make(map[string]any)
				}
				msg.Annotations[agent_v2.SkillBodyAnnotation] = true
			}
		}
	}
	return ctx, nil
}

func TestOnModelStart_ChangedSkillProtectionInvalidatesCompactionMemo(t *testing.T) {
	readCall := func(id, path string) *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: id, Type: "function",
			Function: schema.FunctionCall{Name: "Read", Arguments: `{"path":"` + path + `"}`}}})
	}
	const question = "继续第二轮？"
	oldBody := "old-body " + strings.Repeat("file data ", 150)
	sess := agent_v2.NewRunSession()
	for _, msg := range []*schema.Message{
		schema.UserMessage(strings.Repeat("prior problem ", 250)),
		readCall("prior-read", "earlier.txt"),
		schema.ToolResultMessage("prior-read", oldBody),
		schema.UserMessage("prior reply request"),
		schema.AssistantMessage(strings.Repeat("historical answer ", 50)),
	} {
		sess.AddEvent(agent_v2.NewOutputEvent("compact", msg))
	}
	summary := &countingSummarizer{}
	var firstRound, toolCalls int
	var seenProtected []bool
	main := &fnModel{fn: func(ctx context.Context, msgs []*schema.Message) (*schema.Message, error) {
		if firstRound == 0 {
			firstRound++
			if countUserContent(msgs, "SUMMARY-S1") != 1 || strings.Contains(inputText(msgs), "old-body") {
				return nil, errors.New("first summary did not replace the ordinary tool result")
			}
			return readCall("next-read", "latest.txt"), nil
		}
		var protected bool
		for _, msg := range msgs {
			if schema.GetToolResultID(msg) == "prior-read" {
				marked, _ := msg.Annotations[agent_v2.SkillBodyAnnotation].(bool)
				protected = marked && msg.Content == oldBody
			}
		}
		seenProtected = append(seenProtected, protected)
		answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: question})
		if err != nil {
			return nil, err
		}
		if answer.Content != "继续" {
			return nil, errors.New("main model received an unexpected reply")
		}
		return schema.AssistantMessage("完成"), nil
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("compact"), agent_v2.WithModel(main),
		agent_v2.WithHooks(markSkillBodyOnSecondRound{}),
		agent_v2.WithToolkit(readToolkit(func(path string) string {
			toolCalls++
			return "current tool result for " + path
		})),
		agent_v2.WithStore(store.NewInMemoryStore()),
		agent_v2.WithMicroCompact(&agent_v2.MicroCompactConfig{Enabled: true, KeepRecent: 1, OffloadEnabled: true}),
		agent_v2.WithAutoCompact(&agent_v2.AutoCompactConfig{Enabled: true, Model: summary, ModelContextWindow: 350,
			SummaryMaxTokens: 50, SummaryBufferTokens: 50, MinCompactableTokens: 1}))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	_, err := ag.Run(ctx, sess, schema.UserMessage("new question"))
	id := compactionQuestionID(t, err, question)
	events, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{id: schema.UserMessage("继续")}))
	final := agent_v2.FinalMessage(events)
	if err != nil || final == nil || final.Content != "完成" || toolCalls != 1 {
		t.Fatalf("续跑输出 %+v，工具调用 %d，错误 %v", final, toolCalls, err)
	}
	if len(seenProtected) != 2 || !seenProtected[0] || !seenProtected[1] {
		t.Fatalf("挂起前和续跑时都应收到带保护标注的原工具结果，实际 %v，摘要次数 %d", seenProtected, summary.count())
	}
}

type markTruncatedOnSecondRound struct{ hook.NoOpHooks }

func (markTruncatedOnSecondRound) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	in.Messages[len(in.Messages)-1].Content += " LOCAL-HOOK"
	if info.Round == 1 {
		for _, msg := range in.Messages {
			if schema.GetToolResultID(msg) == "prior-read" {
				if msg.Annotations == nil {
					msg.Annotations = make(map[string]any)
				}
				msg.Annotations[agent_v2.TruncatedOriginalBytesAnnotation] = 5000
			}
		}
	}
	return ctx, nil
}

func TestOnModelStart_ChangedTruncationInvalidatesCompactionMemo(t *testing.T) {
	readCall := func(id, path string) *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: id, Type: "function",
			Function: schema.FunctionCall{Name: "Read", Arguments: `{"path":"` + path + `"}`}}})
	}
	sess := agent_v2.NewRunSession()
	for _, msg := range []*schema.Message{
		schema.UserMessage(strings.Repeat("prior problem ", 250)),
		schema.AssistantMessage(strings.Repeat("historical answer ", 50)),
		schema.UserMessage("look at first old file"),
		readCall("prior-read", "earlier.txt"),
		schema.ToolResultMessage("prior-read", strings.Repeat("file data ", 150)),
		schema.UserMessage("look at second old file"),
		readCall("next-old-read", "later.txt"),
		schema.ToolResultMessage("next-old-read", "later contents"),
	} {
		sess.AddEvent(agent_v2.NewOutputEvent("compact", msg))
	}
	type offloadView struct{ key, preview string }
	var views []offloadView
	var modelCalls int
	main := &fnModel{fn: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
		modelCalls++
		for _, msg := range msgs {
			if schema.GetToolResultID(msg) == "prior-read" {
				key, _ := msg.Annotations[agent_v2.MicroCompactOffloadKeyAnnotation].(string)
				views = append(views, offloadView{key: key, preview: msg.Content})
			}
		}
		if modelCalls == 1 {
			if countUserContent(msgs, "SUMMARY-S1") != 1 {
				return nil, errors.New("first summary was not applied")
			}
			return readCall("new-read", "latest.txt"), nil
		}
		return schema.AssistantMessage("done"), nil
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("compact"), agent_v2.WithModel(main),
		agent_v2.WithHooks(markTruncatedOnSecondRound{}),
		agent_v2.WithToolkit(readToolkit(func(path string) string { return "current tool result for " + path })),
		agent_v2.WithStore(store.NewInMemoryStore()),
		agent_v2.WithMicroCompact(&agent_v2.MicroCompactConfig{Enabled: true, KeepRecent: 1, OffloadEnabled: true,
			OffloadReadToolName: "read_offload"}),
		agent_v2.WithAutoCompact(&agent_v2.AutoCompactConfig{Enabled: true, Model: &countingSummarizer{},
			ModelContextWindow: 350, SummaryMaxTokens: 50, SummaryBufferTokens: 50, MinCompactableTokens: 1}))
	events, err := ag.Run(t.Context(), sess, schema.UserMessage("new question"))
	final := agent_v2.FinalMessage(events)
	if err != nil || final == nil || final.Content != "done" || modelCalls != 2 || len(views) != 2 {
		t.Fatalf("预期两轮送模保留转存结果并完成，views=%+v 模型调用 %d，final=%+v 错误 %v", views, modelCalls, final, err)
	}
	if views[0].key == "" || !strings.Contains(views[0].preview, "读回全文") {
		t.Fatalf("第一轮应指向未截断的转存结果，实际 %+v", views[0])
	}
	if views[1].key == "" || views[1].key == views[0].key || !strings.Contains(views[1].preview, "读回已保存的截断内容") {
		t.Fatalf("截断标注改变后应更新转存结果和预览，实际 %+v", views)
	}
}
