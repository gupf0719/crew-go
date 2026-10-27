package observability_test

// 一次模型 / 工具调用只按它自己的结局记一次：里面的钩子在结束一侧停下、续跑沿用原答复、从工具账重放、被审批拒绝。

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/observability"
	"crew-go/schema"
	"crew-go/tool"
)

// askOnModelEnd 在 OnModelEnd 里问一次人：第一遍抛中断，续跑那一遍拿到答案放行。
type askOnModelEnd struct {
	hook.NoOpHooks
	seen atomic.Int32
}

func (a *askOnModelEnd) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
	a.seen.Add(1)
	if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "这份答复可以发出去吗"}); err != nil {
		return ctx, err
	}
	return ctx, nil
}

// TestModelEndInterrupt_RecordedOnceAcrossResume：业务钩子在 OnModelEnd 里停下等人，续跑沿用那次答复、不再调模型。
// 那次模型调用的 chat span、耗时、用量各记一次——停下的那一遍按调用自己的结局收尾（用量只在这一遍交来），沿用的那一遍
// 不再开 span。
func TestModelEndInterrupt_RecordedOnceAcrossResume(t *testing.T) {
	h := newHarness(t)
	asker := &askOnModelEnd{}
	m := &scriptModel{}
	ctx, opts := h.obs.Install(compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer()),
		agent_v2.WithName("reviewer"),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(asker),
	)
	ag := agent_v2.NewBasicAgent(opts...)
	sess := agent_v2.NewRunSession()
	runCtx := compose_v2.WithSessionIDCtx(ctx, sess.RunID)
	_, err := ag.Run(runCtx, sess, schema.UserMessage("写一句问候"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应停在 OnModelEnd 的提问上，got %v", err)
	}
	if _, err := ag.Resume(runCtx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("可以")})); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if n := m.calls.Load(); n != 1 {
		t.Fatalf("续跑应沿用那次答复、不再调模型，模型被调了 %d 次", n)
	}
	if n := asker.seen.Load(); n != 2 {
		t.Fatalf("OnModelEnd 应触发两遍（停下的那遍 + 沿用答复的那遍），got %d", n)
	}
	h.assertAllEnded(t)
	var chats []sdktrace.ReadOnlySpan
	for _, s := range h.spans() {
		if strings.HasPrefix(s.Name(), "chat") {
			chats = append(chats, s)
		}
	}
	if len(chats) != 1 {
		t.Fatalf("一次模型调用应只有一个 chat span，got %d：%v", len(chats), names(h.spans()))
	}
	if s := strAttr(t, chats[0], observability.AttrOutcome); s != "success" || chats[0].Status().Code == codes.Error {
		t.Errorf("模型调用本身成功了，chat span 应记 success、不标 Error，got outcome=%s status=%v", s, chats[0].Status())
	}
	rm := h.collect(t)
	if n := histCount(t, rm, observability.MetricInferenceDuration, nil); n != 1 {
		t.Errorf("推理耗时样本=%d 应=1", n)
	}
	if n := sumInt(t, rm, observability.MetricUsageInputTokens, nil); n != 40 {
		t.Errorf("输入 token 应只记一次（40），got %d", n)
	}
}

// twoToolsModel 第一轮一次发两个 tool_call（a 先、b 后），拿到结果后给最终答复。
type twoToolsModel struct{ calls atomic.Int32 }

func (m *twoToolsModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *twoToolsModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.calls.Add(1)
	if msgs[len(msgs)-1].Role == schema.RoleTool {
		return schema.AssistantMessage("完成"), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "call_a", Type: "function", Function: schema.FunctionCall{Name: "tool_a", Arguments: `{}`}},
		{ID: "call_b", Type: "function", Function: schema.FunctionCall{Name: "tool_b", Arguments: `{}`}},
	}), nil
}

func (m *twoToolsModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(resp), nil
}

// TestToolReplayed_DurationRecordedOnce：同一批里 a 跑完、b 问人挂起；续跑时 a 从工具账重放（不再执行）、OnToolEnd 再触发
// 一次。a 的时长样本只记一次，重放那次的 span 标 crew.tool.replayed，不算进 agent 的调用次数。
func TestToolReplayed_DurationRecordedOnce(t *testing.T) {
	for _, layers := range []int{1, 2} {
		t.Run(fmt.Sprintf("layers=%d", layers), func(t *testing.T) { testToolReplayed(t, layers) })
	}
}

// testToolReplayed 跑一遍「a 跑完、b 挂起、续跑」；layers=2 时同一个 Observability 装两层（Install 两次），重放那次的计数
// 也只退一次。
func testToolReplayed(t *testing.T, layers int) {
	h := newHarness(t)
	var aRuns atomic.Int32
	tk := tool.NewToolkit()
	empty := map[string]any{"type": "object"}
	tk.Register(tool.NewFuncTool("tool_a", "a", empty, func(context.Context, map[string]any) (*tool.Result, error) {
		aRuns.Add(1)
		return &tool.Result{Success: true, Output: "a done"}, nil
	}))
	tk.Register(tool.NewFuncTool("tool_b", "b", empty, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "b 要继续吗"})
		if err != nil {
			return nil, err
		}
		return &tool.Result{Success: true, Output: "b " + ans.Content}, nil
	}))
	ctx, opts := h.obs.Install(compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer()),
		agent_v2.WithName("batcher"),
		agent_v2.WithModel(&twoToolsModel{}),
		agent_v2.WithToolkit(tk),
	)
	if layers == 2 {
		ctx, opts = h.obs.Install(ctx, opts...)
	}
	ag := agent_v2.NewBasicAgent(opts...)
	sess := agent_v2.NewRunSession()
	runCtx := compose_v2.WithSessionIDCtx(ctx, sess.RunID)
	_, err := ag.Run(runCtx, sess, schema.UserMessage("跑两个工具"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应停在 tool_b 的提问上，got %v", err)
	}
	if _, err := ag.Resume(runCtx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("继续")})); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if n := aRuns.Load(); n != 1 {
		t.Fatalf("tool_a 续跑时应从工具账重放、只执行一次，执行了 %d 次", n)
	}
	h.assertAllEnded(t)
	aSpans := h.spansNamed("execute_tool tool_a")
	var replayed int
	for _, s := range aSpans {
		if v, ok := attrOf(s, observability.AttrToolReplayed); ok && v.AsBool() {
			replayed++
		}
	}
	if len(aSpans) != 2 || replayed != 1 {
		t.Fatalf("tool_a 应有两个 span（执行的那次 + 重放的那次），其中一个标 crew.tool.replayed，got %d 个、标了 %d 个", len(aSpans), replayed)
	}
	rm := h.collect(t)
	if n := histCount(t, rm, observability.MetricExecuteToolDuration, map[string]string{"gen_ai.tool.name": "tool_a"}); n != 1 {
		t.Errorf("tool_a 的时长样本=%d 应=1（重放那次没有执行）", n)
	}
	if n := histCount(t, rm, observability.MetricExecuteToolDuration, map[string]string{"gen_ai.tool.name": "tool_b"}); n != 1 {
		t.Errorf("tool_b 的时长样本=%d 应=1", n)
	}
	// 续跑那次 agent 调用里真正执行的只有 tool_b（tool_a 是重放）
	agents := h.spansNamed("invoke_agent batcher")
	if len(agents) != 2 {
		t.Fatalf("Run + Resume 应各一个 agent span，got %d", len(agents))
	}
	resumed := agents[1]
	if resumed.StartTime().Before(agents[0].StartTime()) {
		resumed = agents[0]
	}
	if v, ok := attrOf(resumed, "crew.agent.tool_calls"); !ok || v.AsInt64() != 1 {
		t.Errorf("续跑那次 agent 的 crew.agent.tool_calls=%v 应=1（重放的不算）", v.AsInt64())
	}
}

// TestToolDenied_Blocked：被审批拒绝的调用没有执行——span 记 blocked、不标 Error、带 crew.tool.outcome=denied；时长样本
// 带同一个属性，跟执行成功的分得开。
func TestToolDenied_Blocked(t *testing.T) {
	h := newHarness(t)
	var runs atomic.Int32
	ctx, opts := h.obs.Install(context.Background(),
		agent_v2.WithName("guarded"),
		agent_v2.WithModel(&scriptModel{toolName: "get_weather", toolArgs: `{"city":"北京"}`}),
		agent_v2.WithToolkit(weatherToolkit(func(city string) *tool.Result {
			runs.Add(1)
			return sunny(city)
		})),
		agent_v2.WithApprovalHeuristic(func(context.Context, string, map[string]any) agent_v2.ApprovalDecision {
			return agent_v2.ApprovalDecisionDeny
		}),
	)
	ag := agent_v2.NewBasicAgent(opts...)
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("查天气")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := runs.Load(); n != 0 {
		t.Fatalf("被拒绝的工具不该执行，执行了 %d 次", n)
	}
	h.assertAllEnded(t)
	sp := h.span(t, "execute_tool get_weather")
	if s := strAttr(t, sp, observability.AttrOutcome); s != "blocked" {
		t.Errorf("被拒绝的调用 crew.outcome=%s 应=blocked", s)
	}
	if sp.Status().Code == codes.Error {
		t.Errorf("拒绝不是失败，status 不该是 Error，got %v", sp.Status())
	}
	if s := strAttr(t, sp, observability.AttrToolOutcome); s != agent_v2.ToolOutcomeDenied {
		t.Errorf("crew.tool.outcome=%s 应=%s", s, agent_v2.ToolOutcomeDenied)
	}
	rm := h.collect(t)
	want := map[string]string{"gen_ai.tool.name": "get_weather", "crew.tool.outcome": agent_v2.ToolOutcomeDenied}
	if n := histCount(t, rm, observability.MetricExecuteToolDuration, want); n != 1 {
		t.Errorf("带 crew.tool.outcome=denied 的时长样本=%d 应=1", n)
	}
}

// rewriteAnswer 在 OnModelEnd 里换掉答复（护栏钩子做 PII 过滤后改写的写法）。
type rewriteAnswer struct{ hook.NoOpHooks }

func (rewriteAnswer) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	if out != nil && out.Message != nil {
		out.Message = schema.AssistantMessage("REWRITTEN-PRIVATE-CONTENT 13800000000")
	}
	return ctx, nil
}

// TestRewrittenAnswer_ContentFollowsRecordingSwitch：钩子改写答复时框架发的 reset_stream 事件带着改写之后的完整答复
// （元数据 replacement）。它是内容：关着内容采集时不进 span，打开时记。
func TestRewrittenAnswer_ContentFollowsRecordingSwitch(t *testing.T) {
	for _, record := range []bool{false, true} {
		t.Run(fmt.Sprintf("record=%v", record), func(t *testing.T) {
			h := newHarness(t, observability.WithContentRecording(record))
			ctx, opts := h.obs.Install(context.Background(),
				agent_v2.WithName("a"), agent_v2.WithModel(&scriptModel{}), agent_v2.WithMaxRounds(3),
				agent_v2.WithHooks(rewriteAnswer{}))
			if _, err := agent_v2.NewBasicAgent(opts...).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
				t.Fatal(err)
			}
			found := false
			for _, s := range h.spans() {
				for _, e := range s.Events() {
					for _, a := range e.Attributes {
						if strings.Contains(a.Value.Emit(), "REWRITTEN-PRIVATE") {
							found = true
						}
					}
				}
			}
			if found != record {
				t.Errorf("内容采集=%v 时改写后的答复进没进 span=%v，应与开关一致", record, found)
			}
		})
	}
}

// panicOnAgentEnd 的 OnAgentEnd panic。
type panicOnAgentEnd struct{ hook.NoOpHooks }

func (panicOnAgentEnd) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	panic("业务的 OnAgentEnd 炸了")
}

// TestInnerOnAgentEndPanic_SpanStillEnds：里面的 OnAgentEnd 钩子 panic（框架兜住、调用照常成功）时，agent span 照样收尾。
func TestInnerOnAgentEndPanic_SpanStillEnds(t *testing.T) {
	h := newHarness(t)
	ctx, opts := h.obs.Install(context.Background(),
		agent_v2.WithName("a"), agent_v2.WithModel(&scriptModel{}), agent_v2.WithHooks(panicOnAgentEnd{}))
	if _, err := agent_v2.NewBasicAgent(opts...).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("OnAgentEnd panic 不该改变调用的结局，got %v", err)
	}
	h.assertAllEnded(t)
	if len(h.spansNamed("invoke_agent a")) != 1 {
		t.Errorf("agent span 应收尾一个，got %d", len(h.spansNamed("invoke_agent a")))
	}
}

// flakyStream 第一次建流吐一片后断开，之后每次等 delay 再吐完整答复。
type flakyStream struct {
	calls atomic.Int32
	delay time.Duration
}

func (m *flakyStream) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *flakyStream) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, model.ErrStreamUnsupported
}

func (m *flakyStream) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	n := m.calls.Add(1)
	sr, send := schema.NewStreamReader[*schema.Message](8)
	go func() {
		defer schema.CloseSend(sr)
		if n == 1 {
			send(&schema.Message{Role: schema.RoleAssistant, Content: "半截"})
			schema.SendError(sr, &model.APIError{Provider: "mock", Err: errors.New("stream broke")})
			return
		}
		time.Sleep(m.delay)
		send(&schema.Message{Role: schema.RoleAssistant, Content: "完整答复", Metadata: &schema.MessageMetadata{
			Model: "mock-model", System: "mock", FinishReason: "stop", Usage: &schema.MessageUsage{InputTokens: 10, OutputTokens: 3, TotalTokens: 13}}})
	}()
	return sr, nil
}

// TestModelRetry_TTFCFromDeliveredAttempt：模型调用整次重来（WithModelRetry）时，首片时长记的是交付出去的那份答复的首片，
// 不是被撤回那次尝试的。
func TestModelRetry_TTFCFromDeliveredAttempt(t *testing.T) {
	h := newHarness(t)
	m := &flakyStream{delay: 150 * time.Millisecond}
	ctx, opts := h.obs.Install(context.Background(),
		agent_v2.WithName("a"), agent_v2.WithModel(m), agent_v2.WithModelRetry(compose_v2.SimpleRetry(2)))
	if _, err := agent_v2.NewBasicAgent(opts...).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := m.calls.Load(); n != 2 {
		t.Fatalf("fixture：应建流两次（第一次断开、重来一次），got %d", n)
	}
	var chats []sdktrace.ReadOnlySpan
	for _, s := range h.spans() {
		if strings.HasPrefix(s.Name(), "chat") {
			chats = append(chats, s)
		}
	}
	if len(chats) != 1 {
		t.Fatalf("整次重来仍是一次模型调用，应只有一个 chat span，got %d", len(chats))
	}
	v, ok := attrOf(chats[0], observability.AttrResponseTTFC)
	if !ok || v.AsFloat64() < 0.15 {
		t.Errorf("首片时长=%v 应 ≥ 交付那次尝试首片之前等的 150ms", v.AsFloat64())
	}
}
