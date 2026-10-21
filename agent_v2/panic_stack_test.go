package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// 兜住 panic 时的栈与日志（compose_v2.PanicError）——agent_v2 这一层的兜底点：工具体、编排外层的业务回调。
// 每条都断言 errors.As 取得到、栈里有 panic 发生处、错误文本不含栈、带这个栈的日志条数对。

var errAPSentinel = errors.New("ap sentinel boom")

//go:noinline
func apToolExplodes(context.Context, map[string]any) (*tool.Result, error) {
	panic(errAPSentinel)
}

//go:noinline
func apInputWriterExplodes(*schema.Message) []compose_v2.ChannelWrite {
	panic("input writer boom")
}

func apAssertPanicStack(t *testing.T, err error, site string) {
	t.Helper()
	if err == nil {
		t.Fatal("该返回错误")
	}
	var pe *compose_v2.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("errors.As 取不到 PanicError：%v", err)
	}
	if !strings.Contains(pe.Stack, "agent_v2_test."+site) {
		t.Fatalf("栈里没有 panic 发生处 %s：\n%s", site, pe.Stack)
	}
	if strings.Contains(err.Error(), "goroutine ") {
		t.Errorf("错误文本不该带栈：%q", err.Error())
	}
}

// TestPanicStack_ToolBody：工具 panic 让整个 tools 节点失败。两个 tool_call 并发时 Run 只返其中一个的错误，
// 另一个的栈只在日志里——每个 tool_call 各记一条。
func TestPanicStack_ToolBody(t *testing.T) {
	for _, calls := range []int{1, 2} {
		t.Run(fmt.Sprintf("calls=%d", calls), func(t *testing.T) {
			logs := apCaptureLogs(t)
			tk := tool.NewToolkit()
			tk.Register(tool.NewFuncTool("boom_tool", "panics", map[string]any{"type": "object"}, apToolExplodes))
			tcs := make([]schema.ToolCall, 0, calls)
			for i := 0; i < calls; i++ {
				tcs = append(tcs, schema.ToolCall{ID: fmt.Sprintf("tc_%d", i), Function: schema.FunctionCall{Name: "boom_tool", Arguments: `{}`}})
			}
			ag := agent_v2.NewBasicAgent(
				agent_v2.WithName("ap_tool"),
				agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantToolCallMessage(tcs), schema.AssistantMessage("done")})),
				agent_v2.WithToolkit(tk),
				agent_v2.WithMaxRounds(3),
			)
			_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go"))

			apAssertPanicStack(t, err, "apToolExplodes")
			if !strings.Contains(err.Error(), `tool "boom_tool" panic recovered: `+errAPSentinel.Error()) {
				t.Errorf("错误文本该与原来相同，实际 %q", err.Error())
			}
			// 工具兜底点原来用 %w 接上 panic 值：挂上记录之后仍认得出
			if !errors.Is(err, errAPSentinel) {
				t.Error("errors.Is 该认得出 panic 值")
			}
			if n := logs.countStack("apToolExplodes"); n != calls {
				t.Errorf("每个 tool_call 该各记一条带栈的日志（%d 条），实际 %d 条", calls, n)
			}
		})
	}
}

// TestPanicStack_GraphAgentCallback：GraphAgent 的 InputWriter 跑在图引擎之外，它 panic 由编排收尾那道兜底接住。
func TestPanicStack_GraphAgentCallback(t *testing.T) {
	logs := apCaptureLogs(t)
	outCh := compose_v2.LastValue[*schema.Message]("__ap_out__")
	b := compose_v2.NewBuilder()
	compose_v2.WithChannel(b, outCh)
	b.AddNode("n", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ga := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("ap_graph"),
		agent_v2.WithGraphInputWriter(apInputWriterExplodes),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}),
	)
	_, runErr := ga.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))

	apAssertPanicStack(t, runErr, "apInputWriterExplodes")
	if !strings.Contains(runErr.Error(), "GraphAgent.OnAgentEnd panic recovered") {
		t.Errorf("该由编排收尾那道兜底接住，实际 %q", runErr.Error())
	}
	if n := logs.countStack("apInputWriterExplodes"); n != 1 {
		t.Errorf("该恰好记一条带栈的日志，实际 %d 条", n)
	}
}

// apGoexitHandler 在 ERROR 记录上调 runtime.Goexit。
type apGoexitHandler struct{}

func (apGoexitHandler) Enabled(context.Context, slog.Level) bool { return true }
func (apGoexitHandler) Handle(_ context.Context, rec slog.Record) error {
	if rec.Level >= slog.LevelError {
		runtime.Goexit()
	}
	return nil
}
func (h apGoexitHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h apGoexitHandler) WithGroup(string) slog.Handler      { return h }

type apEndHooks struct {
	hook.NoOpHooks
	called *bool
	err    *error
}

func (h apEndHooks) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	*h.called = true
	*h.err = agent_v2.GetEndError(ctx)
	return ctx
}

// TestPanicStack_HandlerGoexitKeepsOnAgentEnd：编排收尾那道兜底在调 OnAgentEnd 之前记日志——handler 调了
// runtime.Goexit 也不能让 OnAgentEnd 被跳过（span 不闭合）、Run 不返回。
func TestPanicStack_HandlerGoexitKeepsOnAgentEnd(t *testing.T) {
	apCaptureLogs(t) // 只为了在用例结束时连 log 包输出一起还原；下面换成会 Goexit 的 handler
	slog.SetDefault(slog.New(apGoexitHandler{}))

	outCh := compose_v2.LastValue[*schema.Message]("__ap_goexit_out__")
	b := compose_v2.NewBuilder()
	compose_v2.WithChannel(b, outCh)
	b.AddNode("n", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	var endCalled, returned bool
	var endErr, runErr error
	ga := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("ap_goexit_graph"),
		agent_v2.WithGraphInputWriter(apInputWriterExplodes),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}),
		agent_v2.WithGraphAgentHooks(apEndHooks{called: &endCalled, err: &endErr}),
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, runErr = ga.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
		returned = true
	}()
	<-done
	if !endCalled || endErr == nil {
		t.Fatalf("OnAgentEnd 该照样触发并看到 panic 转成的错误，called=%v err=%v", endCalled, endErr)
	}
	if !returned || runErr == nil {
		t.Fatalf("Run 该照常返回错误，returned=%v err=%v", returned, runErr)
	}
}

// ── 日志捕获 ──

type apLogRecorder struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (r *apLogRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (r *apLogRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	r.recs = append(r.recs, rec.Clone())
	r.mu.Unlock()
	return nil
}
func (r *apLogRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *apLogRecorder) WithGroup(string) slog.Handler      { return r }

// countStack 数 stack 字段里含 site 的日志条数。
func (r *apLogRecorder) countStack(site string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, rec := range r.recs {
		rec.Attrs(func(a slog.Attr) bool {
			if a.Key == "stack" && strings.Contains(a.Value.String(), "agent_v2_test."+site) {
				n++
			}
			return true
		})
	}
	return n
}

// apCaptureLogs 换掉 slog 的默认 handler 收日志。log 包的输出要一起还原：slog.SetDefault 换成自定义 handler
// 时会把 log 包的输出改道过去，换回原来的默认 logger 时不会改回来。
func apCaptureLogs(t *testing.T) *apLogRecorder {
	t.Helper()
	rec := &apLogRecorder{}
	oldDefault := slog.Default()
	oldWriter, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	slog.SetDefault(slog.New(rec))
	t.Cleanup(func() {
		slog.SetDefault(oldDefault)
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	})
	return rec
}
