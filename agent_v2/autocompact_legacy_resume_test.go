package agent_v2_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
)

const (
	legacySummaryHistory  = "file history: abcdef abcdef abcdef abcdef abcdef abcdef abcdef abcdef abcdef abcdef"
	legacySummaryQuestion = "accept previous history?"
)

func loadLegacySummaryCheckpoint(t *testing.T, fixture, question string) (string, *compose_v2.Checkpoint) {
	t.Helper()
	// 由对应提交实际执行生成，保留问题、任务、frontier 和私有状态。去掉运行时间和随机接收密钥。
	data, err := os.ReadFile(filepath.Join("testdata", "autocompact", fixture+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved struct {
		FirstAnswerID string                `json:"first_answer_id"`
		Checkpoint    compose_v2.Checkpoint `json:"checkpoint"`
	}
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if len(saved.Checkpoint.PendingInterrupts) != 1 || saved.Checkpoint.PendingInterrupts[0].Question != question {
		t.Fatalf("测试档没有预期摘要问题，实际 %+v", saved.Checkpoint.PendingInterrupts)
	}
	return saved.FirstAnswerID, &saved.Checkpoint
}

func legacySummaryConfig(m model.ChatModel) *agent_v2.AutoCompactConfig {
	return &agent_v2.AutoCompactConfig{Enabled: true, Model: m, ModelContextWindow: 74,
		SummaryMaxTokens: 40, SummaryBufferTokens: 8, MinCompactableTokens: 1}
}

func legacySummaryApprove(ctx context.Context, question, want string) error {
	answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: question})
	if err != nil {
		return err
	}
	if answer == nil || answer.Content != want {
		return errors.New("received unexpected answer")
	}
	return nil
}

func checkLegacySummaryIsolation(ctx context.Context) error {
	for _, view := range []context.Context{ctx, compose_v2.TaskCommitContext(ctx)} {
		if _, ok := compose_v2.GetResumeContext(view); ok {
			return errors.New("summary can read unrestricted resume context")
		}
		if _, ok := compose_v2.GetResumeAnswer(view, "unrelated"); ok {
			return errors.New("summary can read unrelated answer")
		}
		if _, ok := compose_v2.GetResumeUserText(view); ok {
			return errors.New("summary can read unaddressed resume text")
		}
	}
	return nil
}

func TestAutoCompact_ReapproveLegacyBasicAgentSummary(t *testing.T) {
	_, saved := loadLegacySummaryCheckpoint(t, "3de5276", legacySummaryQuestion)
	sess := agent_v2.NewRunSession()
	sess.RunID = saved.ID
	var approved, finished int
	summary := &fnModel{fn: func(ctx context.Context, msgs []*schema.Message) (*schema.Message, error) {
		if countUserContent(msgs, "file history:") != 1 {
			return nil, errors.New("summary input lost previous history")
		}
		if err := checkLegacySummaryIsolation(ctx); err != nil {
			return nil, err
		}
		if err := legacySummaryApprove(ctx, legacySummaryQuestion, "summary yes"); err != nil {
			return nil, err
		}
		approved++
		return schema.AssistantMessage("<summary>accepted history</summary>"), nil
	}}
	main := &fnModel{fn: func(ctx context.Context, msgs []*schema.Message) (*schema.Message, error) {
		if countUserContent(msgs, "accepted history") != 1 || countUserContent(msgs, "latest built-in question") != 1 {
			return nil, errors.New("main input lost approved summary or current question")
		}
		if err := legacySummaryApprove(ctx, "accept main answer?", "main yes"); err != nil {
			return nil, err
		}
		finished++
		return schema.AssistantMessage("finished old task"), nil
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("compact"), agent_v2.WithModel(main),
		agent_v2.WithAutoCompact(legacySummaryConfig(summary)))
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	if err := cp.SaveNS(ctx, saved.ID, "", saved); err != nil {
		t.Fatal(err)
	}
	oldID := saved.PendingInterrupts[0].InterruptID
	answers := map[string]*schema.Message{
		oldID:       schema.UserMessage("old approval"),
		"unrelated": schema.UserMessage("unrelated answer"),
	}
	_, err := ag.Resume(ctx, saved.ID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers),
		agent_v2.WithResumeUserText("outside summary"))
	summaryID := compactionQuestionID(t, err, legacySummaryQuestion)
	if summaryID == oldID || approved != 0 || finished != 0 {
		t.Fatalf("未绑定请求的旧审批应重问，旧 ID %q 新 ID %q 摘要 %d 主模型 %d", oldID, summaryID, approved, finished)
	}
	_, err = ag.Resume(ctx, saved.ID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	if got := compactionQuestionID(t, err, legacySummaryQuestion); got != summaryID {
		t.Fatalf("同一请求应保留新问题 ID %q，实际 %q", summaryID, got)
	}
	answers[summaryID] = schema.UserMessage("summary yes")
	_, err = ag.Resume(ctx, saved.ID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers),
		agent_v2.WithResumeUserText("outside summary"))
	mainID := compactionQuestionID(t, err, "accept main answer?")
	if mainID == oldID || mainID == summaryID || approved != 1 || finished != 0 {
		t.Fatalf("摘要答复和主模型未隔离，摘要 %d 主模型 %d", approved, finished)
	}
	answers[mainID] = schema.UserMessage("main yes")
	events, err := ag.Resume(ctx, saved.ID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	final := agent_v2.FinalMessage(events)
	if err != nil || final == nil || final.Content != "finished old task" || approved != 2 || finished != 1 {
		t.Fatalf("旧档续跑未完成，final=%+v 摘要 %d 主模型 %d err=%v", final, approved, finished, err)
	}
	if pending, err := cp.ListPending(ctx, saved.ID); err != nil || len(pending) != 0 {
		t.Fatalf("完成后仍有挂起存档 %+v err=%v", pending, err)
	}
}

func TestAutoCompact_ReapproveLegacyPublicSummary(t *testing.T) {
	firstID, saved := loadLegacySummaryCheckpoint(t, "b7c2241", legacySummaryQuestion)
	oldID := saved.PendingInterrupts[0].InterruptID
	msgs := []*schema.Message{schema.UserMessage(legacySummaryHistory), schema.AssistantMessage("file notes"), schema.UserMessage("latest public question")}
	var approved, nextApproved int
	summary := &fnModel{fn: func(ctx context.Context, in []*schema.Message) (*schema.Message, error) {
		ctx = compose_v2.EnterPhase(ctx, "summary review")
		if countUserContent(in, "file history:") != 1 {
			return nil, errors.New("summary input lost previous history")
		}
		if err := checkLegacySummaryIsolation(ctx); err != nil {
			return nil, err
		}
		if err := legacySummaryApprove(ctx, "accept summary format?", "format yes"); err != nil {
			return nil, err
		}
		if err := legacySummaryApprove(ctx, legacySummaryQuestion, "summary yes"); err != nil {
			return nil, err
		}
		approved++
		return schema.AssistantMessage("<summary>accepted history</summary>"), nil
	}}
	next := &fnModel{fn: func(ctx context.Context, in []*schema.Message) (*schema.Message, error) {
		ctx = compose_v2.EnterPhase(ctx, "summary review")
		if countUserContent(in, "file history:") != 1 {
			return nil, errors.New("next summary input lost previous history")
		}
		if err := checkLegacySummaryIsolation(ctx); err != nil {
			return nil, err
		}
		if err := legacySummaryApprove(ctx, "accept next summary?", "next yes"); err != nil {
			return nil, err
		}
		nextApproved++
		return schema.AssistantMessage("<summary>next history</summary>"), nil
	}}
	firstCfg, nextCfg := legacySummaryConfig(summary), legacySummaryConfig(next)
	builder := compose_v2.NewBuilder().WithName("legacy_public_auto_compact")
	builder.AddNode("model", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compacted, err := agent_v2.AutoCompactRun(ctx, firstCfg, msgs)
		if err != nil {
			return nil, err
		}
		if countUserContent(compacted, "accepted history") != 1 {
			return nil, errors.New("approved summary not used")
		}
		compacted, err = agent_v2.AutoCompactRun(ctx, nextCfg, msgs)
		if err == nil && countUserContent(compacted, "next history") != 1 {
			return nil, errors.New("next approved summary not used")
		}
		return nil, err
	})
	builder.AddEdge(compose_v2.StartNode, "model")
	builder.AddEdge("model", compose_v2.EndNode)
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	if err := cp.SaveNS(ctx, saved.ID, "", saved); err != nil {
		t.Fatal(err)
	}
	run := func(answers map[string]*schema.Message) error {
		return compose_v2.Run(ctx, graph, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cp),
			compose_v2.WithSessionID(saved.ID), compose_v2.WithResumeAnswers(answers), compose_v2.WithResumeUserText("outside summary"))
	}
	answers := map[string]*schema.Message{
		firstID:     schema.UserMessage("old format approval"),
		oldID:       schema.UserMessage("old summary approval"),
		"unrelated": schema.UserMessage("unrelated answer"),
	}
	newFirstID := compactionQuestionID(t, run(answers), "accept summary format?")
	if newFirstID == firstID || newFirstID == oldID || approved != 0 {
		t.Fatalf("未绑定请求的公开摘要应重新提问，旧 ID %q/%q 新 ID %q", firstID, oldID, newFirstID)
	}
	upgraded, err := cp.LoadNS(ctx, saved.ID, "")
	if err != nil || upgraded == nil || upgraded.SchemaVer != compose_v2.CheckpointSchemaVer {
		t.Fatalf("再次挂起应保存为当前版本，存档 %+v，错误 %v", upgraded, err)
	}
	if got := compactionQuestionID(t, run(answers), "accept summary format?"); got != newFirstID {
		t.Fatalf("未改请求应保留重新生成的问题 %q，实际 %q", newFirstID, got)
	}
	answers[newFirstID] = schema.UserMessage("format yes")
	newSecondID := compactionQuestionID(t, run(answers), legacySummaryQuestion)
	if newSecondID == oldID || newSecondID == newFirstID || approved != 0 {
		t.Fatalf("旧摘要第二问也需要重新批准，旧 ID %q 新 ID %q 摘要数 %d", oldID, newSecondID, approved)
	}
	answers[newSecondID] = schema.UserMessage("summary yes")
	nextID := compactionQuestionID(t, run(answers), "accept next summary?")
	if nextID == newFirstID || nextID == newSecondID || approved != 1 || nextApproved != 0 {
		t.Fatalf("旧摘要和后续摘要未隔离，旧摘要 %d 后续摘要 %d", approved, nextApproved)
	}
	answers[nextID] = schema.UserMessage("next yes")
	if err := run(answers); err != nil || approved != 2 || nextApproved != 1 {
		t.Fatalf("两次摘要未完成，旧摘要 %d 后续摘要 %d err=%v", approved, nextApproved, err)
	}
	if pending, err := cp.ListPending(ctx, saved.ID); err != nil || len(pending) != 0 {
		t.Fatalf("完成后仍有挂起存档 %+v err=%v", pending, err)
	}
	newID := compactionQuestionID(t, run(nil), "accept summary format?")
	if newID == newFirstID || newID == nextID || approved != 2 || nextApproved != 1 {
		t.Fatalf("新任务不应复用旧问题，ID %q，旧摘要 %d 后续摘要 %d", newID, approved, nextApproved)
	}
	if got := compactionQuestionID(t, run(answers), "accept summary format?"); got != newID {
		t.Fatalf("旧任务答复不应改变新任务问题，预期 %q 实际 %q", newID, got)
	}
}

type publicSummaryHook struct {
	hook.NoOpHooks
	cfg *agent_v2.AutoCompactConfig
}

func (h publicSummaryHook) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	msgs := []*schema.Message{
		schema.UserMessage(legacySummaryHistory), schema.AssistantMessage("hook notes"), schema.UserMessage("latest hook request"),
	}
	_, err := agent_v2.AutoCompactRun(ctx, h.cfg, msgs)
	return ctx, err
}

func TestAutoCompact_PublicSummaryHookKeepsBuiltInAnswer(t *testing.T) {
	for _, version := range []int{21, compose_v2.CheckpointSchemaVer} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			const hookQuestion, builtInQuestion = "accept hook summary?", "accept built-in summary?"
			var hookApproved, builtInApproved int
			summary := func(question, want string, approvals *int) *fnModel {
				return &fnModel{fn: func(ctx context.Context, _ []*schema.Message) (*schema.Message, error) {
					if err := legacySummaryApprove(ctx, question, want); err != nil {
						return nil, err
					}
					*approvals = *approvals + 1
					return schema.AssistantMessage("<summary>accepted history</summary>"), nil
				}}
			}
			main := &fnModel{fn: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
				if countUserContent(msgs, "accepted history") != 1 {
					return nil, errors.New("main model did not receive the approved built-in summary")
				}
				return schema.AssistantMessage("done"), nil
			}}
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("compact"), agent_v2.WithModel(main),
				agent_v2.WithHooks(publicSummaryHook{cfg: legacySummaryConfig(summary(hookQuestion, "hook yes", &hookApproved))}),
				agent_v2.WithAutoCompact(legacySummaryConfig(summary(builtInQuestion, "built-in yes", &builtInApproved))))
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cp := compose_v2.NewInMemoryCheckpointer()
			ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
			sess := agent_v2.NewRunSession()
			sess.AddEvent(agent_v2.NewOutputEvent("compact", schema.UserMessage(legacySummaryHistory)))
			sess.AddEvent(agent_v2.NewOutputEvent("compact", schema.AssistantMessage("built-in notes")))
			_, err := ag.Run(ctx, sess, schema.UserMessage("latest built-in request"))
			hookID := compactionQuestionID(t, err, hookQuestion)
			answers := map[string]*schema.Message{hookID: schema.UserMessage("hook yes")}
			_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
			builtInID := compactionQuestionID(t, err, builtInQuestion)
			if builtInID == hookID {
				t.Fatalf("模型钩子和内置摘要共用问题 ID %q", hookID)
			}
			if version != compose_v2.CheckpointSchemaVer {
				// 保留当前的审批绑定，只改版本号，核对版本变化时审批归属仍然稳定。
				saved, err := cp.LoadNS(ctx, sess.RunID, "")
				if err != nil || saved == nil {
					t.Fatalf("读取内置摘要存档 %+v，错误 %v", saved, err)
				}
				saved.SchemaVer = version
				if err := cp.SaveNS(ctx, sess.RunID, "", saved); err != nil {
					t.Fatal(err)
				}
			}
			answers[builtInID] = schema.UserMessage("built-in yes")
			events, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
			final := agent_v2.FinalMessage(events)
			if err != nil || final == nil || final.Content != "done" || hookApproved != 2 || builtInApproved != 1 {
				t.Fatalf("摘要答复串台，final=%+v 钩子获准 %d 内置获准 %d err=%v", final, hookApproved, builtInApproved, err)
			}
		})
	}
}

func TestAutoCompact_PublicSummaryKeepsCustomPhaseAnswer(t *testing.T) {
	const summaryQuestion, customQuestion = "accept summary?", "accept custom question?"
	var summaryApproved, customApproved int
	cfg := legacySummaryConfig(&fnModel{fn: func(ctx context.Context, _ []*schema.Message) (*schema.Message, error) {
		if err := legacySummaryApprove(ctx, summaryQuestion, "summary yes"); err != nil {
			return nil, err
		}
		summaryApproved++
		return schema.AssistantMessage("<summary>accepted history</summary>"), nil
	}})
	msgs := []*schema.Message{schema.UserMessage(legacySummaryHistory), schema.AssistantMessage("notes"), schema.UserMessage("latest request")}
	builder := compose_v2.NewBuilder()
	builder.AddNode("model", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, err := agent_v2.AutoCompactRun(ctx, cfg, msgs); err != nil {
			return nil, err
		}
		if err := legacySummaryApprove(compose_v2.EnterPhase(ctx, "auto_compact"), customQuestion, "custom yes"); err != nil {
			return nil, err
		}
		customApproved++
		return nil, nil
	})
	builder.AddEdge(compose_v2.StartNode, "model")
	builder.AddEdge("model", compose_v2.EndNode)
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	run := func(answers map[string]*schema.Message) error {
		return compose_v2.Run(ctx, graph, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cp),
			compose_v2.WithSessionID("public-summary-custom-phase"), compose_v2.WithResumeAnswers(answers))
	}
	summaryID := compactionQuestionID(t, run(nil), summaryQuestion)
	answers := map[string]*schema.Message{summaryID: schema.UserMessage("summary yes")}
	customID := compactionQuestionID(t, run(answers), customQuestion)
	answers[customID] = schema.UserMessage("custom yes")
	if err := run(answers); err != nil || summaryApproved != 2 || customApproved != 1 {
		t.Fatalf("摘要与当前版本的自定义提问串台，摘要获准 %d 自定义获准 %d err=%v", summaryApproved, customApproved, err)
	}
}
