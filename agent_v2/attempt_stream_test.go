package agent_v2_test

// 编排成员的一次尝试被下一次「从头再跑」的尝试取代时，它里面每次模型调用交给前端的流都撤回（reset_stream，按 run_id）；
// 下一次从存档续的不撤回——那些步骤不重跑，已经交出的就是留下来的进度。判据跟丢不丢会话事件是同一个（attemptLanes）。

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// seqStreamModel 第 n 次 Stream（从 1 数）交出 next(n) 的消息作为唯一片段；next 返回错误时这次流什么都没交出就失败。
type seqStreamModel struct {
	calls atomic.Int64
	next  func(n int) (*schema.Message, error)
}

func (m *seqStreamModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, errors.New("unused")
}

func (m *seqStreamModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.next(int(m.calls.Add(1)))
	sr, send := schema.NewStreamReader[*schema.Message](2)
	if err != nil {
		schema.SendError(sr, err)
	} else {
		send(msg)
	}
	schema.CloseSend(sr)
	return sr, nil
}

func (m *seqStreamModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// runAttemptDiscard：第 1 次调用流出文本 "A" 与一个工具调用、工具执行完；第 2 次什么都没流出就失败，成员重试；之后成功。
// 返回第 1 次调用的 run_id 与每个 run_id 被撤回的次数。
func runAttemptDiscard(t *testing.T, withCheckpointer bool) (string, map[string]int) {
	t.Helper()
	m := &seqStreamModel{next: func(n int) (*schema.Message, error) {
		switch n {
		case 1, 3:
			return &schema.Message{Role: schema.RoleAssistant, Content: "A", ContentBlocks: []*schema.ContentBlock{{Type: "tool_call",
				Toolcall: &schema.ToolcallContentBlock{Id: "c1", Name: "lookup", Arguments: `{}`}}}}, nil
		case 2:
			return nil, errConnReset
		default:
			return schema.AssistantMessage("full"), nil
		}
	}}
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("lookup", "查", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("r"), nil }))
	step := agent_v2.NewBasicAgent(agent_v2.WithName("s"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
		agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(2))).AddStep("s1", step)
	ctx := context.Background()
	if withCheckpointer {
		ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	}
	log := &eventLog{}
	// 挂在 ctx 上：嵌套成员流出的片段经 ctx 继承的编排钩子交出（agui 也是这样接的）
	ctx = agent_v2.WithCtxOrchHooks(ctx, log)
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	first := ""
	resets := map[string]int{}
	for _, ev := range log.events {
		runID, _ := ev.Metadata["run_id"].(string)
		switch {
		case ev.Type == "chunk" && ev.Content == "A" && first == "":
			first = runID
		case ev.Type == "reset_stream":
			resets[runID]++
		}
	}
	return first, resets
}

// preCallHook 在成员的 OnAgentStart 里经 RunSubAgent 调 inner（成员在自己的图之外那一段：完成记录存在编排节点上）。
type preCallHook struct {
	hook.NoOpHooks
	inner agent_v2.Agent
	runs  atomic.Int64
}

func (h *preCallHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	h.runs.Add(1)
	_, err := agent_v2.RunSubAgent(ctx, "pre", h.inner, schema.UserMessage("pre"))
	return ctx, err
}

// 没有存档、成员从头重来，但它在自己的图之外完成的嵌套调用被下一次尝试原样拿回、不再执行：那次调用交出的流不能撤——
// 前端得看得到实际被采用的产出。曾经在下一次尝试开始前就全部撤回（审计实测：inner 只执行一次，它的流却被撤了）。
func TestAttemptStreams_RecalledNestedCallKept(t *testing.T) {
	innerModel := &seqStreamModel{next: func(int) (*schema.Message, error) { return schema.AssistantMessage("INNER"), nil }}
	inner := agent_v2.NewBasicAgent(agent_v2.WithName("inner"), agent_v2.WithModel(innerModel), agent_v2.WithMaxRounds(2))
	h := &preCallHook{inner: inner}
	outerModel := &seqStreamModel{next: func(n int) (*schema.Message, error) {
		if n == 1 {
			return nil, &model.APIError{Provider: "stub", StatusCode: 400, Body: "bad"}
		}
		return schema.AssistantMessage("done"), nil
	}}
	step := agent_v2.NewBasicAgent(agent_v2.WithName("s"), agent_v2.WithModel(outerModel), agent_v2.WithHooks(h), agent_v2.WithMaxRounds(2))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
		agent_v2.WithSequentialRetryConfig(compose_v2.RetryConfig{MaxRetries: 2, ShouldRetry: func(error) bool { return true }})).
		AddStep("s1", step)
	log := &eventLog{}
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), log)
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if h.runs.Load() != 2 || innerModel.calls.Load() != 1 {
		t.Fatalf("fixture 没搭对：钩子应跑 2 次、inner 只执行 1 次（第二次拿回），实际 %d / %d", h.runs.Load(), innerModel.calls.Load())
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	innerRun := ""
	resets := map[string]int{}
	for _, ev := range log.events {
		runID, _ := ev.Metadata["run_id"].(string)
		switch {
		case ev.Type == "chunk" && ev.Content == "INNER":
			innerRun = runID
		case ev.Type == "reset_stream":
			resets[runID]++
		}
	}
	if innerRun == "" || resets[innerRun] != 0 {
		t.Fatalf("被拿回的嵌套调用（run_id %q）不该撤回，实际 %v", innerRun, resets)
	}
}

// chunkThenFailAgent 是业务自研的成员：经 EventSink 发一个片段（自己的 run_id），第一次之后报错。
type chunkThenFailAgent struct{ runs atomic.Int64 }

func (a *chunkThenFailAgent) Name() string        { return "custom" }
func (a *chunkThenFailAgent) Description() string { return "" }
func (a *chunkThenFailAgent) Run(ctx context.Context, _ *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	n := a.runs.Add(1)
	runID := "custom-" + string(rune('0'+n))
	if sink := compose_v2.GetEventSink(ctx); sink != nil {
		sink.Send(compose_v2.Event{Type: "chunk", Content: "片段", Metadata: map[string]any{"run_id": runID}})
	}
	if n == 1 {
		return nil, errConnReset
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent("custom", schema.AssistantMessage("好了"))}, nil
}
func (a *chunkThenFailAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, errors.New("unused")
}

// 不是 BasicAgent 的成员（业务自研、RemoteAgent）经 EventSink 发的片段，尝试作废时同样撤回——登记收在片段的出口上，不靠
// 生产方自己登记（曾经只在 BasicAgent 的模型调用处登记，审计实测撤不掉）。
func TestAttemptStreams_CustomMemberChunksRetracted(t *testing.T) {
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
		agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(2))).AddStep("s1", &chunkThenFailAgent{})
	log := &eventLog{}
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), log)
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	resets := map[string]int{}
	for _, ev := range log.events {
		if ev.Type == "reset_stream" {
			id, _ := ev.Metadata["run_id"].(string)
			resets[id]++
		}
	}
	if resets["custom-1"] != 1 || resets["custom-2"] != 0 {
		t.Fatalf("作废那次（custom-1）应撤回一次、重来那次（custom-2）不撤，实际 %v", resets)
	}
}

// 成员是 GraphAgent，它的节点直接经 EventSink 发片段：节点的 sink 是那张图新建的，只认 ctx 上的编排钩子链——由挂在链上的
// 记录钩子登记，尝试作废时同样撤回。
func TestAttemptStreams_GraphNodeChunksRetracted(t *testing.T) {
	var runs atomic.Int64
	g := effGraph(t, "g", func(ctx context.Context, _ string) (string, error) {
		n := runs.Add(1)
		if sink := compose_v2.GetEventSink(ctx); sink != nil {
			sink.Send(compose_v2.Event{Type: "chunk", Content: "片段", Metadata: map[string]any{"run_id": "g-" + string(rune('0'+n))}})
		}
		if n == 1 {
			return "", errConnReset
		}
		return "好了", nil
	})
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
		agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(2))).AddStep("s1", g)
	log := &eventLog{}
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), log)
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	resets := map[string]int{}
	for _, ev := range log.events {
		if ev.Type == "reset_stream" {
			id, _ := ev.Metadata["run_id"].(string)
			resets[id]++
		}
	}
	if resets["g-1"] != 1 || resets["g-2"] != 0 {
		t.Fatalf("作废那次（g-1）应撤回一次、重来那次（g-2）不撤，实际 %v", resets)
	}
}

// rejectOnceHook 第一次 OnModelEnd 报错（拒掉这份答复，续跑重新生成），之后放行。
type rejectOnceHook struct {
	hook.NoOpHooks
	n atomic.Int64
}

func (h *rejectOnceHook) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
	if h.n.Add(1) == 1 {
		return ctx, errors.New("内容不合规，拒掉")
	}
	return ctx, nil
}

// 配了存档、成员从存档续：被 OnModelEnd 钩子拒掉、续跑重新生成的那份答复，在作废那一刻撤回（曾经尝试层整份保留，前端留着
// 被拒掉的答复，审计实测）。
func TestAttemptStreams_RejectedAnswerRetractedOnResume(t *testing.T) { runRejectedAnswer(t, true) }

// 没有存档、成员从头重来：被拒掉的答复在作废那一刻撤过了，尝试作废时不再撤一次（一个 run_id 撤一次）。
func TestAttemptStreams_RejectedAnswerRetractedOnceOnRerun(t *testing.T) { runRejectedAnswer(t, false) }

func runRejectedAnswer(t *testing.T, withCheckpointer bool) {
	t.Helper()
	m := &seqStreamModel{next: func(n int) (*schema.Message, error) {
		return schema.AssistantMessage("ANS" + string(rune('0'+n))), nil
	}}
	step := agent_v2.NewBasicAgent(agent_v2.WithName("s"), agent_v2.WithModel(m), agent_v2.WithHooks(&rejectOnceHook{}),
		agent_v2.WithMaxRounds(2))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
		agent_v2.WithSequentialRetryConfig(compose_v2.RetryConfig{MaxRetries: 2, ShouldRetry: func(error) bool { return true }})).
		AddStep("s1", step)
	log := &eventLog{}
	ctx := context.Background()
	if withCheckpointer {
		ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	}
	ctx = agent_v2.WithCtxOrchHooks(ctx, log)
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	runOf := map[string]string{}
	resets := map[string]int{}
	for _, ev := range log.events {
		id, _ := ev.Metadata["run_id"].(string)
		switch ev.Type {
		case "chunk":
			runOf[ev.Content] = id
		case "reset_stream":
			resets[id]++
		}
	}
	if runOf["ANS1"] == "" || resets[runOf["ANS1"]] != 1 || resets[runOf["ANS2"]] != 0 {
		t.Fatalf("被拒掉的 ANS1 应撤回一次、ANS2 不撤，实际 run=%v resets=%v", runOf, resets)
	}
}

func TestAttemptStreams_DiscardedAttemptRetracted(t *testing.T) {
	first, resets := runAttemptDiscard(t, false)
	if first == "" || resets[first] != 1 {
		t.Fatalf("没有存档、尝试从头再跑：第一次尝试里完整流完的那次调用（%q）应撤回一次，实际 %v", first, resets)
	}
}

func TestAttemptStreams_ResumedAttemptKept(t *testing.T) {
	first, resets := runAttemptDiscard(t, true)
	if first == "" || resets[first] != 0 {
		t.Fatalf("有存档、下一次从存档续：已经完成的那次调用（%q）不该撤回，实际 %v", first, resets)
	}
}

// resetsOf 数每个 run_id 被撤回的次数，并找出内容为 content 的片段的 run_id。
func resetsOf(log *eventLog, content string) (string, map[string]int) {
	log.mu.Lock()
	defer log.mu.Unlock()
	run := ""
	resets := map[string]int{}
	for _, ev := range log.events {
		id, _ := ev.Metadata["run_id"].(string)
		switch {
		case ev.Type == "chunk" && ev.Content == content && run == "":
			run = id
		case ev.Type == "reset_stream":
			resets[id]++
		}
	}
	return run, resets
}

// flakyGraphSeq：顺序编排，唯一一步是 GraphAgent，它的节点发片段 g-<n> 后第一次报错；编排重试一次、没有存档、从头再跑。
func flakyGraphSeq(t *testing.T) agent_v2.Agent {
	var runs atomic.Int64
	g := effGraph(t, "g", func(ctx context.Context, _ string) (string, error) {
		n := runs.Add(1)
		id := "g-" + string(rune('0'+n))
		if sink := compose_v2.GetEventSink(ctx); sink != nil {
			sink.Send(compose_v2.Event{Type: "chunk", Content: id, Metadata: map[string]any{"run_id": id}})
		}
		if n == 1 {
			return "", errConnReset
		}
		return "好了", nil
	})
	return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
		agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(2))).AddStep("s1", g)
}

// 重试成员的编排嵌在节点里（经 RunSubAgent 调起）：作废那次尝试里 GraphAgent 节点发的片段照样撤回。登记靠挂在编排钩子链上的
// 记录钩子，它要在每一层取编排钩子的地方都挂上，不只最外层（曾经只在最外层的尝试里装，嵌在节点里的编排撤不掉，审计实测）。
func TestAttemptStreams_NestedInNodeRetracted(t *testing.T) {
	seq := flakyGraphSeq(t)
	outer := effGraph(t, "outer", func(ctx context.Context, c string) (string, error) {
		events, err := agent_v2.RunSubAgent(ctx, "sub", seq, schema.UserMessage(c))
		if err != nil {
			return "", err
		}
		return agent_v2.FinalMessage(events).Content, nil
	})
	log := &eventLog{}
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), log)
	if _, err := outer.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	_, resets := resetsOf(log, "g-1")
	if resets["g-1"] != 1 || resets["g-2"] != 0 {
		t.Fatalf("作废那次（g-1）应撤回一次、重来那次（g-2）不撤，实际 %v", resets)
	}
}

// 同上，编排被包成工具、由顶层 BasicAgent 调起。
func TestAttemptStreams_NestedInToolRetracted(t *testing.T) {
	seq := flakyGraphSeq(t)
	m := &seqStreamModel{next: func(n int) (*schema.Message, error) {
		if n == 1 {
			return &schema.Message{Role: schema.RoleAssistant, ContentBlocks: []*schema.ContentBlock{{Type: "tool_call",
				Toolcall: &schema.ToolcallContentBlock{Id: "c1", Name: "seq", Arguments: `{"input":"go"}`}}}}, nil
		}
		return schema.AssistantMessage("done"), nil
	}}
	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(seq, agent_v2.WithAgentToolDescription("跑一遍")))
	top := agent_v2.NewBasicAgent(agent_v2.WithName("top"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3))
	log := &eventLog{}
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), log)
	if _, err := top.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if m.calls.Load() != 2 {
		t.Fatalf("fixture 没搭对：顶层模型应调 2 次（工具调用 + 收尾），实际 %d", m.calls.Load())
	}
	_, resets := resetsOf(log, "g-1")
	if resets["g-1"] != 1 || resets["g-2"] != 0 {
		t.Fatalf("作废那次（g-1）应撤回一次、重来那次（g-2）不撤，实际 %v", resets)
	}
}

// composeEventLog 是挂在 compose_v2 ctx 上的前端钩子（compose_v2.WithOrchHooks），把事件转进 eventLog。
type composeEventLog struct {
	compose_v2.NoOpOrchestrationHooks
	log *eventLog
}

func (h composeEventLog) OnNodeEvent(ctx context.Context, id compose_v2.NodeID, ev compose_v2.Event) context.Context {
	return h.log.OnNodeEvent(ctx, string(id), agent_v2.Event{Type: ev.Type, Content: ev.Content, Metadata: ev.Metadata})
}

// 前端钩子挂在 compose_v2 的 ctx 上：作废那次尝试里的片段送到了前端，也要撤得掉（曾经引擎自己回落到这条链，链上没有记录
// 钩子，g-1 送到了却没撤，审计实测）。
func TestAttemptStreams_ComposeCtxHooksRetracted(t *testing.T) {
	log := &eventLog{}
	ctx := compose_v2.WithOrchHooks(context.Background(), composeEventLog{log: log})
	if _, err := flakyGraphSeq(t).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	run, resets := resetsOf(log, "g-1")
	if run != "g-1" || resets["g-1"] != 1 || resets["g-2"] != 0 {
		t.Fatalf("g-1 应送到前端并撤回一次、g-2 不撤，实际 chunk=%q resets=%v", run, resets)
	}
}

// detachingHook 的 OnNodeEvent 返回一个跟事件无关的 ctx（ChainOrchHooks 把它交给链上的下一个钩子）。
type detachingHook struct {
	agent_v2.NoOpOrchestrationHooks
}

func (detachingHook) OnNodeEvent(context.Context, string, agent_v2.Event) context.Context {
	return context.Background()
}

// 链上的业务钩子换掉了 ctx：记录钩子排在链的最前，照样拿到这次尝试的记录器（曾经排在末尾，拿到的是业务钩子返回的 ctx，
// 片段漏撤，审计实测）。
func TestAttemptStreams_BusinessHookCtxDoesNotHideRecorder(t *testing.T) {
	log := &eventLog{}
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), agent_v2.NewChainOrchHooks(log, detachingHook{}))
	if _, err := flakyGraphSeq(t).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	_, resets := resetsOf(log, "g-1")
	if resets["g-1"] != 1 || resets["g-2"] != 0 {
		t.Fatalf("作废那次（g-1）应撤回一次、重来那次（g-2）不撤，实际 %v", resets)
	}
}

// 外层编排的节点经 RunSubAgent 调一个重试成员的编排（成员是自研 agent），之后自己失败一次、外层从头再跑。自研成员经调用方
// 节点的 sink 发片段，同一个片段登记在里层尝试与外层两处；里层撤过的不再撤（曾经 custom-1 撤了两次，审计实测）。
func TestAttemptStreams_RetractedOnceAcrossLayers(t *testing.T) {
	inner := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("inner"),
		agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(2))).AddStep("c", &chunkThenFailAgent{})
	var runs atomic.Int64
	g := effGraph(t, "g", func(ctx context.Context, c string) (string, error) {
		if _, err := agent_v2.RunSubAgent(ctx, "sub", inner, schema.UserMessage(c)); err != nil {
			return "", err
		}
		if runs.Add(1) == 1 {
			return "", errConnReset
		}
		return "好了", nil
	})
	outer := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("outer"),
		agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(2))).AddStep("s1", g)
	log := &eventLog{}
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), log)
	if _, err := outer.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	_, resets := resetsOf(log, "")
	if resets["custom-1"] != 1 || resets["custom-2"] != 1 || resets["custom-3"] != 0 {
		t.Fatalf("custom-1（里层作废）与 custom-2（外层作废）各撤一次、custom-3 不撤，实际 %v", resets)
	}
}

// flakyPreCallHook：第 2 次 OnAgentStart 在调 inner 之前就报错，其余照常经 RunSubAgent 调 inner。
type flakyPreCallHook struct {
	hook.NoOpHooks
	inner agent_v2.Agent
	runs  atomic.Int64
}

func (h *flakyPreCallHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if h.runs.Add(1) == 2 {
		return ctx, errors.New("钩子暂时失败")
	}
	_, err := agent_v2.RunSubAgent(ctx, "pre", h.inner, schema.UserMessage("pre"))
	return ctx, err
}

// 第一次尝试完成了嵌套调用、之后失败；第二次在拿回它之前就失败；第三次拿回、成功。被拿回的那次调用的流不能撤：中间那次
// 尝试没登记它，不代表它没被采用（曾经第二次尝试一失败就撤，第三次拿回的产出前端已经看不到，审计实测）。
func TestAttemptStreams_RecalledAfterEarlyFailureKept(t *testing.T) {
	innerModel := &seqStreamModel{next: func(int) (*schema.Message, error) { return schema.AssistantMessage("INNER"), nil }}
	inner := agent_v2.NewBasicAgent(agent_v2.WithName("inner"), agent_v2.WithModel(innerModel), agent_v2.WithMaxRounds(2))
	h := &flakyPreCallHook{inner: inner}
	outerModel := &seqStreamModel{next: func(n int) (*schema.Message, error) {
		if n == 1 {
			return nil, &model.APIError{Provider: "stub", StatusCode: 400, Body: "bad"}
		}
		return schema.AssistantMessage("done"), nil
	}}
	step := agent_v2.NewBasicAgent(agent_v2.WithName("s"), agent_v2.WithModel(outerModel), agent_v2.WithHooks(h), agent_v2.WithMaxRounds(2))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
		agent_v2.WithSequentialRetryConfig(compose_v2.RetryConfig{MaxRetries: 3, ShouldRetry: func(error) bool { return true }})).
		AddStep("s1", step)
	log := &eventLog{}
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), log)
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if h.runs.Load() != 3 || innerModel.calls.Load() != 1 {
		t.Fatalf("fixture 没搭对：钩子应跑 3 次、inner 只执行 1 次（第三次拿回），实际 %d / %d", h.runs.Load(), innerModel.calls.Load())
	}
	innerRun, resets := resetsOf(log, "INNER")
	if innerRun == "" || resets[innerRun] != 0 {
		t.Fatalf("被拿回的嵌套调用（run_id %q）不该撤回，实际 %v", innerRun, resets)
	}
}

// retryNodeGraph 同 effGraph，节点声明 WithNodeRetry（重试 2 次）。
func retryNodeGraph(t *testing.T, step func(ctx context.Context, c string) (string, error)) *agent_v2.GraphAgent {
	t.Helper()
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[string]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("n", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		out, err := step(ctx, in.Content)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, out)}, nil
	}, compose_v2.WithNodeRetry(compose_v2.SimpleRetry(2)))
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("g"),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			o, _ := compose_v2.Get(chs, outCh)
			return schema.AssistantMessage(o)
		}))
}

// 节点按 WithNodeRetry 重试：失败那次尝试里节点发的片段撤回一次，重来那次的不撤（曾经节点重试不经撤回，作废那次的片段
// 留在前端）。
func TestAttemptStreams_NodeRetryChunksRetracted(t *testing.T) {
	var runs atomic.Int64
	g := retryNodeGraph(t, func(ctx context.Context, _ string) (string, error) {
		n := runs.Add(1)
		id := "n-" + string(rune('0'+n))
		if sink := compose_v2.GetEventSink(ctx); sink != nil {
			sink.Send(compose_v2.Event{Type: "chunk", Content: id, Metadata: map[string]any{"run_id": id}})
		}
		if n == 1 {
			return "", errConnReset
		}
		return "好了", nil
	})
	log := &eventLog{}
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), log)
	if _, err := g.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if runs.Load() != 2 {
		t.Fatalf("fixture 没搭对：节点应执行 2 次，实际 %d", runs.Load())
	}
	_, resets := resetsOf(log, "n-1")
	if resets["n-1"] != 1 || resets["n-2"] != 0 {
		t.Fatalf("作废那次（n-1）应撤回一次、重来那次（n-2）不撤，实际 %v", resets)
	}
}

// 节点体经 RunSubAgent 完成了一次嵌套调用之后失败、按 WithNodeRetry 重试：重来那次原样拿回这次调用，它交出的流不撤。
func TestAttemptStreams_NodeRetryRecalledCallKept(t *testing.T) {
	innerModel := &seqStreamModel{next: func(int) (*schema.Message, error) { return schema.AssistantMessage("INNER"), nil }}
	inner := agent_v2.NewBasicAgent(agent_v2.WithName("inner"), agent_v2.WithModel(innerModel), agent_v2.WithMaxRounds(2))
	var runs atomic.Int64
	g := retryNodeGraph(t, func(ctx context.Context, c string) (string, error) {
		if _, err := agent_v2.RunSubAgent(ctx, "sub", inner, schema.UserMessage(c)); err != nil {
			return "", err
		}
		if runs.Add(1) == 1 {
			return "", errConnReset
		}
		return "好了", nil
	})
	log := &eventLog{}
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), log)
	if _, err := g.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if runs.Load() != 2 || innerModel.calls.Load() != 1 {
		t.Fatalf("fixture 没搭对：节点应执行 2 次、inner 只执行 1 次（第二次拿回），实际 %d / %d", runs.Load(), innerModel.calls.Load())
	}
	innerRun, resets := resetsOf(log, "INNER")
	if innerRun == "" || resets[innerRun] != 0 {
		t.Fatalf("被拿回的嵌套调用（run_id %q）不该撤回，实际 %v", innerRun, resets)
	}
}

// 成员里的模型调用流出半截后断开，成员从头再跑：那次调用在失败点自己撤了一次，所在尝试作废时不再撤（一个 run_id 只撤
// 一次；曾经撤两次，agui 的自定义撤回事件发两遍）。
func TestAttemptStreams_MidStreamFailureRetractedOnce(t *testing.T) {
	m := &scriptedStreamModel{script: func(n int) ([]string, error) {
		if n == 0 {
			return []string{"half"}, errConnReset
		}
		return []string{"full"}, nil
	}}
	step := agent_v2.NewBasicAgent(agent_v2.WithName("s"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(2))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
		agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(2))).AddStep("s1", step)
	log := &eventLog{}
	ctx := agent_v2.WithCtxOrchHooks(context.Background(), log)
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	half, resets := resetsOf(log, "half")
	if half == "" || resets[half] != 1 {
		t.Fatalf("半截那次调用（run_id %q）应只撤一次，实际 %v", half, resets)
	}
}
