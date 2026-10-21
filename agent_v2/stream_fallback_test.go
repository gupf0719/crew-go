package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/fallback"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/model/openai"
	specModel "crew-go/modules/model"
	"crew-go/schema"
)

// 这组测试钉的是：BasicAgent 只在模型不支持流式时改调 Generate；建流失败、钩子报错都原样
// 交给上层，不另调一次 Generate。

// streamProbeModel 的 Stream 按配置返回错误、nil reader 或一条正常的流；Generate 计数后返回
// 固定答复。
type streamProbeModel struct {
	name        string
	streamErr   error
	nilReader   bool
	streamCalls atomic.Int32
	genCalls    atomic.Int32
}

func (m *streamProbeModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	m.genCalls.Add(1)
	return schema.AssistantMessage(m.name + " generate"), nil
}

func (m *streamProbeModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	m.streamCalls.Add(1)
	if m.nilReader {
		return nil, nil
	}
	if m.streamErr != nil {
		return nil, m.streamErr
	}
	return schema.NewSingleChunkReader(schema.AssistantMessage(m.name + " stream")), nil
}

func (m *streamProbeModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// runStreamProbe 用 m 跑一次 BasicAgent，返回最终答复。
func runStreamProbe(t *testing.T, m model.ChatModel, opts ...agent_v2.Option) (string, error) {
	t.Helper()
	all := append([]agent_v2.Option{agent_v2.WithName("stream_probe"), agent_v2.WithModel(m)}, opts...)
	events, err := agent_v2.NewBasicAgent(all...).Run(context.Background(), nil, schema.UserMessage("hi"))
	if err != nil {
		return "", err
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatal("没有最终答复")
	}
	return final.Content, nil
}

// 建流失败（这里是带 Retry-After 的 429）原样返回：改调 Generate 等于不看 Retry-After 立刻
// 再打一遍。
func TestStreamOrGenerate_StreamErrorReturnedWithoutGenerate(t *testing.T) {
	m := &streamProbeModel{name: "m", streamErr: &model.APIError{Provider: "p", StatusCode: 429, RetryAfter: 30 * time.Second}}
	_, err := runStreamProbe(t, m)
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 429 {
		t.Fatalf("建流的 429 应原样交给上层，实际 %v", err)
	}
	if n := m.genCalls.Load(); n != 0 {
		t.Fatalf("建流失败不该改调 Generate，实际 Generate 被调 %d 次", n)
	}
}

func TestStreamOrGenerate_UnsupportedFallsBackToGenerate(t *testing.T) {
	for _, tc := range []struct {
		name string
		m    *streamProbeModel
	}{
		{"ErrStreamUnsupported", &streamProbeModel{name: "m", streamErr: model.ErrStreamUnsupported}},
		{"包了一层的 ErrStreamUnsupported", &streamProbeModel{name: "m", streamErr: fmt.Errorf("wrapper: %w", model.ErrStreamUnsupported)}},
		{"nil reader", &streamProbeModel{name: "m", nilReader: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := runStreamProbe(t, tc.m)
			if err != nil || got != "m generate" {
				t.Fatalf("不支持流式应改调 Generate，实际答复 %q err %v", got, err)
			}
			if n := tc.m.genCalls.Load(); n != 1 {
				t.Fatalf("Generate 应被调 1 次，实际 %d", n)
			}
		})
	}
}

// failingEndStreamHook 的 OnModelEndStream 总是报错。
type failingEndStreamHook struct{ hook.NoOpHooks }

func (failingEndStreamHook) OnModelEndStream(ctx context.Context, _ *hook.AgentInfo, sr specModel.MessageStreamReader) (context.Context, specModel.MessageStreamReader, error) {
	return ctx, sr, errors.New("audit sink down")
}

// OnModelEndStream 报错跟 OnModelStart / OnModelEnd 报错一样让这次模型调用失败；改调
// Generate 会让模型被调两次。
func TestStreamOrGenerate_EndStreamHookErrorFailsCall(t *testing.T) {
	m := &streamProbeModel{name: "m"}
	_, err := runStreamProbe(t, m, agent_v2.WithHooks(failingEndStreamHook{}))
	if err == nil || !strings.Contains(err.Error(), "audit sink down") {
		t.Fatalf("钩子的错误应交给上层，实际 %v", err)
	}
	if n := m.genCalls.Load(); n != 0 {
		t.Fatalf("钩子报错不该改调 Generate，实际 Generate 被调 %d 次", n)
	}
}

// 主候选不支持流式时，fallback 不该把它当故障切到备选、记断路器失败：BasicAgent 改调
// Generate，仍由主候选回答。跑的次数超过默认断路器阈值（3），主候选也不会被熔断。
func TestStreamOrGenerate_FallbackKeepsOrderForNonStreamingPrimary(t *testing.T) {
	primary := &streamProbeModel{name: "primary", streamErr: model.ErrStreamUnsupported}
	backup := &streamProbeModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup})
	for i := 0; i < 5; i++ {
		got, err := runStreamProbe(t, fb)
		if err != nil || got != "primary generate" {
			t.Fatalf("第 %d 次：应由主候选经 Generate 回答，实际答复 %q err %v", i+1, got, err)
		}
	}
	if s, g := backup.streamCalls.Load(), backup.genCalls.Load(); s != 0 || g != 0 {
		t.Fatalf("备选不该被调用，实际 Stream %d 次、Generate %d 次", s, g)
	}
}

// 主候选支持流式但限流、备选不支持流式：一次模型调用里主候选只该被打一次。从头重走的话
// （改调 FallbackChatModel.Generate），主候选立刻又被打一次、断路器一次调用记两次失败，
// 答复也会来自主候选的 Generate。
func TestStreamOrGenerate_FallbackNonStreamingBackupDoesNotReHitPrimary(t *testing.T) {
	primary := &streamProbeModel{name: "primary", streamErr: &model.APIError{Provider: "p", StatusCode: 429, RetryAfter: 30 * time.Second}}
	backup := &streamProbeModel{name: "backup", streamErr: model.ErrStreamUnsupported}
	fb := fallback.New([]model.ChatModel{primary, backup}, fallback.WithBreaker(2, time.Hour))
	for i := 0; i < 2; i++ {
		got, err := runStreamProbe(t, fb)
		if err != nil || got != "backup generate" {
			t.Fatalf("第 %d 次：应由备选经 Generate 回答，实际答复 %q err %v", i+1, got, err)
		}
	}
	if s, g := primary.streamCalls.Load(), primary.genCalls.Load(); s != 2 || g != 0 {
		t.Fatalf("主候选每次模型调用只该被打一次（Stream），实际 Stream %d 次、Generate %d 次", s, g)
	}
}

// nilReaderHook 的 OnModelEndStream 返回 nil reader 且不报错（协议违例）。
type nilReaderHook struct{ hook.NoOpHooks }

func (nilReaderHook) OnModelEndStream(ctx context.Context, _ *hook.AgentInfo, _ specModel.MessageStreamReader) (context.Context, specModel.MessageStreamReader, error) {
	return ctx, nil, nil
}

// OnModelEndStream 返回 nil reader：单独挂与挂进钩子链用同一个判据（协议违例，这次模型调用失败）。
// 判据不同的话，同一个钩子单独挂能用、一接上观测（链式安装）就让每次模型调用失败。
func TestStreamOrGenerate_NilReaderFromHookIsRejectedConsistently(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []agent_v2.Option
	}{
		{"单独挂", []agent_v2.Option{agent_v2.WithHooks(nilReaderHook{})}},
		{"挂进钩子链", []agent_v2.Option{agent_v2.WithHooks(nilReaderHook{}), agent_v2.WithHooksChained(hook.NoOpHooks{})}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &streamProbeModel{name: "m"}
			_, err := runStreamProbe(t, m, tc.opts...)
			if err == nil || !strings.Contains(err.Error(), "nil reader") {
				t.Fatalf("钩子返回 nil reader 应让这次模型调用失败，实际 %v", err)
			}
			if n := m.genCalls.Load(); n != 0 {
				t.Fatalf("不该改调 Generate，实际 Generate 被调 %d 次", n)
			}
		})
	}
}

// 端点不接受 stream=true（建流回 400），非流式正常：声明 WithStreamUnsupported 之后走 Generate；
// 不声明时 400 照常当错误交出（框架不从错误去猜「是不是不支持流式」）。
func TestBasicAgent_StreamUnsupportedEndpoint(t *testing.T) {
	var streamReqs, genReqs atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if strings.Contains(string(body), `"stream":true`) {
			streamReqs.Add(1)
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":{"message":"stream is not supported for this model","param":"stream"}}`)
			return
		}
		genReqs.Add(1)
		_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}]}`)
	}))
	defer srv.Close()

	got, err := runStreamProbe(t, openai.NewClient("k", "m", openai.WithBaseURL(srv.URL), openai.WithStreamUnsupported()))
	if err != nil || got != "hello" {
		t.Fatalf("声明不支持流式之后应走 Generate，实际答复 %q err %v", got, err)
	}
	if s, g := streamReqs.Load(), genReqs.Load(); s != 0 || g != 1 {
		t.Fatalf("应只发 1 次非流式请求，实际流式 %d 次、非流式 %d 次", s, g)
	}

	_, err = runStreamProbe(t, openai.NewClient("k", "m", openai.WithBaseURL(srv.URL)))
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("没声明时建流的 400 应原样交出，实际 %v", err)
	}
}

// 端到端：provider 的流没收到 finish_reason 就断了，BasicAgent 必须以错误结束，不能把半截
// 内容当成最终答复。对照组是同一个服务端发完整的流。
func TestBasicAgent_TruncatedProviderStreamIsNotAFinalAnswer(t *testing.T) {
	const head = "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"答案的前半\"}}]}\n\n"
	for _, tc := range []struct {
		name     string
		body     string
		complete bool
	}{
		{"完整（对照）", head + "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", true},
		{"截断", head, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()
			got, err := runStreamProbe(t, openai.NewClient("k", "m", openai.WithBaseURL(srv.URL)))
			if tc.complete {
				if err != nil || got != "答案的前半" {
					t.Fatalf("完整的流应得到最终答复，实际 %q err %v", got, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("被截断的流被当成了最终答复：%q", got)
			}
			if !strings.Contains(err.Error(), "finish_reason") {
				t.Fatalf("错误应点出没收到 finish_reason：%v", err)
			}
		})
	}
}

// richToolCallMessage 是一条字段齐全的答复：两个工具调用的 ID 相同（增量拼接按 ID 合并，会把它们
// 并成一个），另有消息 ID、数据块、结束原因与创建时间——都是增量拼接不保留的字段——以及拒答。
func richToolCallMessage() *schema.Message {
	return &schema.Message{
		ID:      "msg-1",
		Role:    schema.RoleAssistant,
		Content: "调用两个工具",
		Refusal: "部分内容不予回答",
		ContentBlocks: []*schema.ContentBlock{
			{Type: "tool_call", Toolcall: &schema.ToolcallContentBlock{Id: "call_0", Name: "a", Arguments: `{"x":1}`}},
			{Type: "tool_call", Toolcall: &schema.ToolcallContentBlock{Id: "call_0", Name: "b", Arguments: `{"y":2}`}},
			{Type: "data", Data: &schema.DataContentBlock{Type: "data", Data: map[string]any{"k": "v"}}},
		},
		Metadata: &schema.MessageMetadata{FinishReason: "tool_calls", Created: 1700000000, Model: "m"},
	}
}

// wholeMessageModel 不支持流式，Generate 每次返回一条新的 richToolCallMessage。
type wholeMessageModel struct{}

func (wholeMessageModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return richToolCallMessage(), nil
}

func (wholeMessageModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m wholeMessageModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var errStopAfterModel = errors.New("stop after first model call")

// captureModelHook 记下第一次模型调用的产出，然后让这次 Run 结束；wrapStream 为 true 时把流原样
// 转一手，让 BasicAgent 走桥接那条路。
type captureModelHook struct {
	hook.NoOpHooks
	wrapStream bool
	got        *schema.Message
}

type passThroughReader struct{ sr specModel.MessageStreamReader }

func (r passThroughReader) Recv(ctx context.Context) (*schema.Message, error) { return r.sr.Recv(ctx) }
func (r passThroughReader) Close() error                                      { return r.sr.Close() }

func (h *captureModelHook) OnModelEndStream(ctx context.Context, _ *hook.AgentInfo, sr specModel.MessageStreamReader) (context.Context, specModel.MessageStreamReader, error) {
	if h.wrapStream {
		return ctx, passThroughReader{sr: sr}, nil
	}
	return ctx, sr, nil
}

func (h *captureModelHook) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	h.got = out.Message
	return ctx, errStopAfterModel
}

// withoutRunID 返回 m 的浅拷贝，去掉每次模型调用都不同的 run_id 标注。
func withoutRunID(m *schema.Message) *schema.Message {
	c := *m
	c.Annotations = nil
	for k, v := range m.Annotations {
		if k == "run_id" {
			continue
		}
		if c.Annotations == nil {
			c.Annotations = map[string]any{}
		}
		c.Annotations[k] = v
	}
	return &c
}

// 不支持流式的候选经 fallback 包成只有一个片段的流交过来：BasicAgent 拿到的消息跟直接调 Generate
// 逐字段相同。走增量拼接的话，两个 ID 相同的工具调用并成一个、参数首尾相接，消息 ID、数据块、
// 结束原因、创建时间全丢。
func TestStreamOrGenerate_SingleChunkIsTheWholeMessage(t *testing.T) {
	want := richToolCallMessage()
	for _, tc := range []struct {
		name       string
		m          model.ChatModel
		wrapStream bool
	}{
		{"直接调 Generate（对照）", wholeMessageModel{}, false},
		{"经 fallback", fallback.New([]model.ChatModel{wholeMessageModel{}}), false},
		{"经 fallback + 钩子转一手流", fallback.New([]model.ChatModel{wholeMessageModel{}}), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &captureModelHook{wrapStream: tc.wrapStream}
			_, err := runStreamProbe(t, tc.m, agent_v2.WithHooks(h))
			if !errors.Is(err, errStopAfterModel) {
				t.Fatalf("应在第一次模型调用之后停下，实际 %v", err)
			}
			if h.got == nil {
				t.Fatal("没记到模型产出")
			}
			if h.got.Annotations["run_id"] == nil {
				t.Fatal("产出应带 run_id 标注")
			}
			if got := withoutRunID(h.got); !reflect.DeepEqual(got, want) {
				t.Fatalf("产出跟候选的 Generate 不一致：\n got  %s\n want %s", describeMessage(got), describeMessage(want))
			}
		})
	}
}

// closingStreamHook 的 OnModelEndStream 包一层流：交出第一个片段之后关掉底层流，再把底层流
// 读到的结果原样交出（关掉之后读到的是 schema.ErrStreamCanceled）。护栏、超时保护这类钩子会这样
// 半路掐断流。
type closingStreamHook struct{ hook.NoOpHooks }

type closingReader struct {
	sr   specModel.MessageStreamReader
	recv int
}

func (r *closingReader) Recv(ctx context.Context) (*schema.Message, error) {
	r.recv++
	if r.recv == 2 {
		_ = r.sr.Close() // 关流的错误没有下文，忽略
	}
	return r.sr.Recv(ctx)
}

func (r *closingReader) Close() error { return r.sr.Close() }

func (closingStreamHook) OnModelEndStream(ctx context.Context, _ *hook.AgentInfo, sr specModel.MessageStreamReader) (context.Context, specModel.MessageStreamReader, error) {
	return ctx, &closingReader{sr: sr}, nil
}

// slowSecondChunkModel 的流先交出「前半」，等 release 关闭之后才交「后半」并结束：钩子关流时
// 生产方还没发完。
type slowSecondChunkModel struct{ release chan struct{} }

func (slowSecondChunkModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, errors.New("not used")
}

func (m slowSecondChunkModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	sr, send := schema.NewStreamReader[*schema.Message](2)
	go func() {
		defer schema.CloseSend(sr)
		send(schema.AssistantMessage("前半"))
		<-m.release
		send(schema.AssistantMessage("后半"))
	}()
	return sr, nil
}

func (m slowSecondChunkModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// 流被别人半路关掉（这里是钩子），不算正常结束：只有生产方交出 io.EOF 才算。曾经 streamOrGenerate
// 把 schema.ErrStreamCanceled 也当正常结束，半截内容「前半」成了最终答复、err == nil。钩子想在
// 这里截断并把已有内容当完整输出，要在自己的 Recv 里返回 io.EOF。
func TestStreamOrGenerate_StreamClosedByOthersIsNotACompletion(t *testing.T) {
	m := slowSecondChunkModel{release: make(chan struct{})}
	defer close(m.release)
	got, err := runStreamProbe(t, m, agent_v2.WithHooks(closingStreamHook{}))
	if err == nil {
		t.Fatalf("流被半路关掉，不该当成完整输出，实际最终答复 %q", got)
	}
	if !errors.Is(err, schema.ErrStreamCanceled) {
		t.Fatalf("错误应带着 schema.ErrStreamCanceled，实际 %v", err)
	}
}

// refusalStreamModel 的流交出两个片段，各带一段拒答增量（openai 客户端逐片段交出 delta.refusal）。
type refusalStreamModel struct{}

func (refusalStreamModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, errors.New("not used")
}

func (refusalStreamModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	sr, send := schema.NewStreamReader[*schema.Message](2)
	send(&schema.Message{Role: schema.RoleAssistant, Refusal: "无法"})
	send(&schema.Message{Refusal: "回答"})
	schema.CloseSend(sr)
	return sr, nil
}

func (m refusalStreamModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// 多片段的流里拒答是增量，跟内容一样拼起来；曾经拼接不收它，流式拿到的消息没有拒答、Generate 有。
func TestStreamOrGenerate_RefusalDeltasAreAggregated(t *testing.T) {
	h := &captureModelHook{}
	_, err := runStreamProbe(t, refusalStreamModel{}, agent_v2.WithHooks(h))
	if !errors.Is(err, errStopAfterModel) || h.got == nil {
		t.Fatalf("应在第一次模型调用之后停下并记到产出，实际 err %v", err)
	}
	if h.got.Refusal != "无法回答" {
		t.Fatalf("拒答增量应拼成「无法回答」，实际 %q", h.got.Refusal)
	}
}

// describeMessage 把消息里这组用例关心的字段写成一行，失败信息里对比用。
func describeMessage(m *schema.Message) string {
	var b strings.Builder
	fmt.Fprintf(&b, "id=%q role=%q content=%q refusal=%q", m.ID, m.Role, m.Content, m.Refusal)
	for _, cb := range m.ContentBlocks {
		switch {
		case cb.Toolcall != nil:
			fmt.Fprintf(&b, " tc{id=%s name=%s args=%s}", cb.Toolcall.Id, cb.Toolcall.Name, cb.Toolcall.Arguments)
		case cb.Data != nil:
			fmt.Fprintf(&b, " data{%v}", cb.Data.Data)
		}
	}
	if m.Metadata != nil {
		fmt.Fprintf(&b, " finish=%q created=%d model=%q", m.Metadata.FinishReason, m.Metadata.Created, m.Metadata.Model)
	}
	fmt.Fprintf(&b, " annotations=%v", m.Annotations)
	return b.String()
}
