package agent_v2_test

// WithModelRetry：一次模型调用失败时整次重来（建流之前与流开始之后的错一样处理），失败那次已交出的片段在失败那一刻
// 撤回；一个失败只被重试一层——下层（fallback 的每候选重试）用完的不再重试，这里用完的外层（成员重试）不再重做。

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/fallback"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/model/openai"
	"crew-go/schema"
)

// scriptedStreamModel 第 n 次 Stream（从 0 数）按 script(n) 流出片段、再以 err 结束（err 为 nil 正常结束）；建流错误由
// openErr(n) 给。记下每次收到的输入条数。
type scriptedStreamModel struct {
	calls   atomic.Int64
	openErr func(n int) error
	script  func(n int) ([]string, error)
}

func (m *scriptedStreamModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, errors.New("unused")
}

func (m *scriptedStreamModel) Stream(_ context.Context, in []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	n := int(m.calls.Add(1) - 1)
	if m.openErr != nil {
		if err := m.openErr(n); err != nil {
			return nil, err
		}
	}
	chunks, end := m.script(n)
	sr, send := schema.NewStreamReader[*schema.Message](len(chunks) + 1)
	for _, c := range chunks {
		send(&schema.Message{Role: schema.RoleAssistant, Content: c})
	}
	if end != nil {
		schema.SendError(sr, end)
	} else {
		send(&schema.Message{Role: schema.RoleAssistant, Metadata: &schema.MessageMetadata{FinishReason: "stop"}})
	}
	schema.CloseSend(sr)
	return sr, nil
}

func (m *scriptedStreamModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// modelCallCounter 数 OnModelStart / OnModelEnd。
type modelCallCounter struct {
	hook.NoOpHooks
	starts, ends atomic.Int64
}

func (h *modelCallCounter) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	h.starts.Add(1)
	return ctx, nil
}

func (h *modelCallCounter) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
	h.ends.Add(1)
	return ctx, nil
}

// eventLog 收 OnNodeEvent。
type eventLog struct {
	agent_v2.NoOpOrchestrationHooks
	mu     sync.Mutex
	events []agent_v2.Event
}

func (h *eventLog) OnNodeEvent(ctx context.Context, _ string, ev agent_v2.Event) context.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, ev)
	return ctx
}

var errConnReset = errors.New("read tcp: connection reset by peer")

// 流出半截后连接断开、重来成功：模型调两次，最终答复是第二次的；作废那次的片段按它的 run_id 撤回；钩子看到的是
// 一次模型调用。
func TestModelRetry_MidStreamFailureRetriedWhole(t *testing.T) {
	m := &scriptedStreamModel{script: func(n int) ([]string, error) {
		if n == 0 {
			return []string{"半截"}, errConnReset
		}
		return []string{"完整", "答复"}, nil
	}}
	counter := &modelCallCounter{}
	log := &eventLog{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("mr"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(2),
		agent_v2.WithHooks(counter), agent_v2.WithModelRetry(compose_v2.SimpleRetry(2)))
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"), agent_v2.WithOrchHooks(log))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := agent_v2.FinalMessage(events).Content; got != "完整答复" {
		t.Fatalf("最终答复应是重来那次的，实际 %q", got)
	}
	if n := m.calls.Load(); n != 2 {
		t.Fatalf("模型应被调 2 次，实际 %d", n)
	}
	if counter.starts.Load() != 1 || counter.ends.Load() != 1 {
		t.Fatalf("重试在一对 OnModelStart / OnModelEnd 之内：实际 start=%d end=%d", counter.starts.Load(), counter.ends.Load())
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	var halfRun string
	resets := map[string]int{}
	for _, ev := range log.events {
		runID, _ := ev.Metadata["run_id"].(string)
		switch {
		case ev.Type == "chunk" && ev.Content == "半截":
			halfRun = runID
		case ev.Type == "reset_stream":
			resets[runID]++
		}
	}
	if halfRun == "" || resets[halfRun] != 1 || len(resets) != 1 {
		t.Fatalf("作废那次（run_id %q）的片段应恰好撤回一次，实际 %v", halfRun, resets)
	}
}

// 没配 WithModelRetry：行为不变，流中途出错这次 Run 失败。
func TestModelRetry_OffByDefault(t *testing.T) {
	m := &scriptedStreamModel{script: func(n int) ([]string, error) {
		if n == 0 {
			return []string{"半截"}, errConnReset
		}
		return []string{"完整"}, nil
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("mr0"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(2))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); !errors.Is(err, errConnReset) {
		t.Fatalf("默认不重试，应以流的错误结束，实际 %v", err)
	}
	if n := m.calls.Load(); n != 1 {
		t.Fatalf("默认只调 1 次，实际 %d", n)
	}
}

// 确定性的 HTTP 失败（400）不重试。
func TestModelRetry_DeterministicNotRetried(t *testing.T) {
	m := &scriptedStreamModel{
		openErr: func(int) error { return &model.APIError{Provider: "stub", StatusCode: 400, Body: "bad request"} },
		script:  func(int) ([]string, error) { return nil, nil },
	}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("mr4"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(2),
		agent_v2.WithModelRetry(compose_v2.SimpleRetry(3)))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err == nil {
		t.Fatal("应失败")
	}
	if n := m.calls.Load(); n != 1 {
		t.Fatalf("400 不该重试，实际调了 %d 次", n)
	}
}

// 模型套了 fallback 且配了每候选重试：建流之前的 503 已在候选上重试 3 次，这里不再重试——一共 3 次请求，不是 9 次。
func TestModelRetry_NotMultipliedWithFallbackRetry(t *testing.T) {
	m := &scriptedStreamModel{
		openErr: func(int) error { return &model.APIError{Provider: "stub", StatusCode: 503, Body: "busy"} },
		script:  func(int) ([]string, error) { return nil, nil },
	}
	fb := fallback.New([]model.ChatModel{m}, fallback.WithPerModelRetry(fallback.PerModelRetry{MaxRetries: 2}))
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("mrf"), agent_v2.WithModel(fb), agent_v2.WithMaxRounds(2),
		agent_v2.WithModelRetry(compose_v2.SimpleRetry(2)))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err == nil {
		t.Fatal("应失败")
	}
	if n := m.calls.Load(); n != 3 {
		t.Fatalf("一个失败只被重试一层：期望 3 次请求，实际 %d 次", n)
	}
}

// 这里重试用完之后，外层的成员重试不再重做这个失败：一共 3 次，不是 9 次。
func TestModelRetry_ExhaustedNotRetriedByMember(t *testing.T) {
	m := &scriptedStreamModel{script: func(int) ([]string, error) { return []string{"半截"}, errConnReset }}
	step := agent_v2.NewBasicAgent(agent_v2.WithName("mrm"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(2),
		agent_v2.WithModelRetry(compose_v2.SimpleRetry(2)))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
		agent_v2.WithSequentialRetryConfig(compose_v2.RetryConfig{MaxRetries: 2, ShouldRetry: func(error) bool { return true }})).
		AddStep("s1", step)
	if _, err := seq.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); !errors.Is(err, errConnReset) {
		t.Fatalf("应以流的错误结束，实际 %v", err)
	}
	if n := m.calls.Load(); n != 3 {
		t.Fatalf("模型调用层用完之后成员重试不该再重做：期望 3 次，实际 %d 次", n)
	}
}

// tooLongUntilCompacted 输入超过 limit 条时报上下文超窗（不带状态码的普通错误，自研或包装过的 ChatModel 多半是这种形态），
// 压缩之后成功。
type tooLongUntilCompacted struct {
	calls atomic.Int64
	limit int
}

func (m *tooLongUntilCompacted) Generate(_ context.Context, in []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.calls.Add(1)
	if len(in) > m.limit {
		return nil, errors.New("prompt is too long: 50000 tokens > 8192 maximum")
	}
	return schema.AssistantMessage("ok"), nil
}

func (m *tooLongUntilCompacted) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *tooLongUntilCompacted) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// 上下文超窗不在模型调用层重试：同一份过长的输入重发多少次都一样，直接交给 ReactiveCompact 压缩后重调——调用次数跟不配
// 模型重试时一样。曾经默认谓词只认 400 与 ErrContextWindowExceeded，别的形状的超窗先被重发 N 次（审计实测）。
func TestModelRetry_PromptTooLongGoesStraightToCompaction(t *testing.T) {
	run := func(retry bool) int64 {
		m := &tooLongUntilCompacted{limit: 4}
		opts := []agent_v2.Option{agent_v2.WithName("tl"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(2),
			agent_v2.WithReactiveCompact(&agent_v2.ReactiveCompactConfig{Enabled: true, MaxAttempts: 3})}
		if retry {
			opts = append(opts, agent_v2.WithModelRetry(compose_v2.SimpleRetry(2)))
		}
		sess := &agent_v2.RunSession{RunID: "tl_run"}
		for i := 0; i < 3; i++ {
			sess.AddEvent(agent_v2.NewOutputEvent("tl", schema.UserMessage("prior")))
			sess.AddEvent(agent_v2.NewOutputEvent("tl", schema.AssistantMessage("ans")))
		}
		if _, err := agent_v2.NewBasicAgent(opts...).Run(context.Background(), sess, schema.UserMessage("go")); err != nil {
			t.Fatalf("压缩后应成功（retry=%v），实际 %v", retry, err)
		}
		return m.calls.Load()
	}
	base, withRetry := run(false), run(true)
	if base < 2 {
		t.Fatalf("fixture 没搭对：不配重试时应先超窗、压缩后成功，实际只调了 %d 次", base)
	}
	if withRetry != base {
		t.Fatalf("超窗不该在模型调用层重试：不配重试 %d 次调用，配了 %d 次", base, withRetry)
	}
}

// 端到端：真的 openai 客户端，第一次请求流出半截后停住（空闲超时），第二次请求正常。配了 WithModelRetry 时这次 Run
// 成功、最终答复完整。
func TestModelRetry_StreamIdleTimeoutRecovered(t *testing.T) {
	var reqs atomic.Int64
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		if reqs.Add(1) == 1 {
			_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"半截\"}}]}\n\n")
			fl.Flush()
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"完整\"}}]}\n\n"+
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
		fl.Flush()
	}))
	t.Cleanup(func() { close(release); srv.Close() })
	client := openai.NewClient("k", "m", openai.WithBaseURL(srv.URL), openai.WithStreamIdleTimeout(150*time.Millisecond))
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("mri"), agent_v2.WithModel(client), agent_v2.WithMaxRounds(2),
		agent_v2.WithModelRetry(compose_v2.SimpleRetry(2)))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	events, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("空闲超时之后整次重来应成功，实际 %v", err)
	}
	if got := agent_v2.FinalMessage(events).Content; !strings.Contains(got, "完整") || strings.Contains(got, "半截") {
		t.Fatalf("最终答复应只有重来那次的内容，实际 %q", got)
	}
	if n := reqs.Load(); n != 2 {
		t.Fatalf("应请求 2 次，实际 %d", n)
	}
}

// runQuota429 让模型第一次建流报 body 那样的 429，之后正常答复；单模型配默认判据重试 3 次。返回模型被调的次数与 Run 的错误。
func runQuota429(t *testing.T, body string) (int64, error) {
	t.Helper()
	m := &scriptedStreamModel{
		openErr: func(n int) error {
			if n == 0 {
				return &model.APIError{Provider: "stub", StatusCode: 429, Body: body}
			}
			return nil
		},
		script: func(int) ([]string, error) { return []string{"ok"}, nil },
	}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("q"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(2),
		agent_v2.WithModelRetry(compose_v2.RetryConfig{MaxRetries: 3, ShouldRetry: compose_v2.IsRetryable}))
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	return m.calls.Load(), err
}

// 计费类额度用完（429 insufficient_quota）等多久都不会恢复：默认判据不重试，模型只调一次，错误认得出 ErrBillingExhausted
// （曾经 fallback 按响应体认、默认判据只看状态码，单模型配了 WithModelRetry 时白重试 n 次）。
func TestModelRetry_BillingExhaustedNotRetried(t *testing.T) {
	calls, err := runQuota429(t, `{"error":{"code":"insufficient_quota","message":"You exceeded your current quota"}}`)
	if calls != 1 || !errors.Is(err, model.ErrBillingExhausted) {
		t.Fatalf("额度用完应只调 1 次且错误认得出 ErrBillingExhausted，实际 %d 次、err=%v", calls, err)
	}
}

// 对照：按时间窗口的配额是限流、会自己恢复，照常重试。
func TestModelRetry_WindowQuotaStillRetried(t *testing.T) {
	calls, err := runQuota429(t, `{"error":{"message":"Quota exceeded for requests per minute"}}`)
	if err != nil || calls != 2 {
		t.Fatalf("按时间窗口的配额应重试一次后成功，实际 %d 次、err=%v", calls, err)
	}
}
