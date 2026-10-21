package agent_v2_test

// 钩子返回的 ctx 只作用于它包住的那次调用：OnToolStart(i) 放在 ctx 上的值只在调用 i 的执行（工具体、嵌套的被调方）
// 和 OnToolEnd(i) 里可见；OnToolEnd 的返回值不往后流；OnModelStart 的值只在这次模型调用和 OnModelEnd 里可见。

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

type scopeKey struct{}

func scopeOf(ctx context.Context) string {
	v, _ := ctx.Value(scopeKey{}).(string)
	return v
}

// scopeLog 记下各处看到的值，键形如 "OnToolStart(a)"。
type scopeLog struct {
	mu   sync.Mutex
	seen map[string]string
}

func (l *scopeLog) put(k, v string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.seen == nil {
		l.seen = map[string]string{}
	}
	l.seen[k] = v
}

func (l *scopeLog) get(k string) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	v, ok := l.seen[k]
	return v, ok
}

func (l *scopeLog) dump() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	keys := make([]string, 0, len(l.seen))
	for k := range l.seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k + "=" + l.seen[k] + " ")
	}
	return b.String()
}

// scopeToolHooks：OnToolStart 记下收到的值、放上 "start-<callID>"；OnToolEnd 记下收到的值、放上 "end-<callID>"。
type scopeToolHooks struct {
	hook.NoOpHooks
	log *scopeLog
}

func (h scopeToolHooks) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	h.log.put("OnToolStart("+in.CallId+")", scopeOf(ctx))
	return context.WithValue(ctx, scopeKey{}, "start-"+in.CallId), nil
}

func (h scopeToolHooks) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	h.log.put("OnToolEnd("+out.CallId+")", scopeOf(ctx))
	return context.WithValue(ctx, scopeKey{}, "end-"+out.CallId), nil
}

// scopeModel 第一轮发出给定的 tool_call，之后以文本收尾；记下每轮调用时 ctx 上的值。
type scopeModel struct {
	mu    sync.Mutex
	calls []schema.ToolCall
	n     int
	log   *scopeLog
}

func (m *scopeModel) Generate(ctx context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.n++
	if m.log != nil {
		m.log.put("模型调用("+string(rune('0'+m.n))+")", scopeOf(ctx))
	}
	if m.n == 1 && len(m.calls) > 0 {
		return schema.AssistantToolCallMessage(m.calls), nil
	}
	return schema.AssistantMessage("完成"), nil
}

func (m *scopeModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *scopeModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func scopeCall(id, name string) schema.ToolCall {
	return schema.ToolCall{ID: id, Type: "function", Function: schema.FunctionCall{Name: name, Arguments: `{"id":"` + id + `"}`}}
}

// recordingEcho 记下工具体收到的值。
func recordingEcho(log *scopeLog) tool.Tool {
	return tool.NewFuncTool("echo", "echo", map[string]any{"type": "object"},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			id, _ := args["id"].(string)
			log.put("工具体("+id+")", scopeOf(ctx))
			return tool.Success("ok"), nil
		})
}

func expectSeen(t *testing.T, log *scopeLog, want map[string]string) {
	t.Helper()
	for k, v := range want {
		got, ok := log.get(k)
		if !ok {
			t.Errorf("fixture 没搭对：%s 没被触发（实际：%s）", k, log.dump())
			continue
		}
		if got != v {
			t.Errorf("%s 收到的是 %q，应为 %q（全部：%s）", k, got, v, log.dump())
		}
	}
}

// TestToolHookCtx_ScopedPerCall：同一批两个 tool_call（并发执行）。每个调用的工具体与 OnToolEnd 看到的是它自己那次
// OnToolStart 放的值；OnToolStart 看不到别的调用放的值；OnToolEnd 看不到前一个 OnToolEnd 返回的值。曾经钩子的返回值
// 赋回整个节点的 ctx：b 的 OnToolStart 收到 a 放的值，整批工具体都拿到最后一个 OnToolStart 的值（a 的工具体带着 b
// 的值在跑），OnToolEnd 也拿到别的调用的值——业务钩子按惯例在 OnToolStart 开 span、在 OnToolEnd 结束它，a 的工作挂到
// b 的 span 下，b 的 span 结束两次（实测）。
func TestToolHookCtx_ScopedPerCall(t *testing.T) {
	log := &scopeLog{}
	kit := tool.NewToolkit()
	kit.Register(recordingEcho(log))
	m := &scopeModel{calls: []schema.ToolCall{scopeCall("a", "echo"), scopeCall("b", "echo")}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("scope"), agent_v2.WithModel(m), agent_v2.WithToolkit(kit),
		agent_v2.WithHooks(scopeToolHooks{log: log}), agent_v2.WithMaxRounds(3))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatal(err)
	}
	expectSeen(t, log, map[string]string{
		"OnToolStart(a)": "",
		"OnToolStart(b)": "",
		"工具体(a)":         "start-a",
		"工具体(b)":         "start-b",
		"OnToolEnd(a)":   "start-a",
		"OnToolEnd(b)":   "start-b",
	})
}

// TestToolHookCtx_SingleCall：只有一个 tool_call（内联路径，不起 goroutine）时同样：工具体与 OnToolEnd 看到 OnToolStart
// 放的值。
func TestToolHookCtx_SingleCall(t *testing.T) {
	log := &scopeLog{}
	kit := tool.NewToolkit()
	kit.Register(recordingEcho(log))
	m := &scopeModel{calls: []schema.ToolCall{scopeCall("a", "echo")}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("scope1"), agent_v2.WithModel(m), agent_v2.WithToolkit(kit),
		agent_v2.WithHooks(scopeToolHooks{log: log}), agent_v2.WithMaxRounds(3))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatal(err)
	}
	expectSeen(t, log, map[string]string{
		"OnToolStart(a)": "",
		"工具体(a)":         "start-a",
		"OnToolEnd(a)":   "start-a",
	})
}

// childAgentHooks 记下嵌套的被调方在 OnAgentStart 收到的值。
type childAgentHooks struct {
	hook.NoOpHooks
	log *scopeLog
}

func (h childAgentHooks) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	h.log.put("子 agent 的 OnAgentStart", scopeOf(ctx))
	return ctx, nil
}

// TestToolHookCtx_NestedCalleeSeesItsCall：调用 a 是 AgentAsTool 包的子 agent，调用 b 是普通工具。子 agent 看到的是
// 调用 a 的 OnToolStart 放的值——它的 span 该挂在 a 下面。曾经整批工具都拿到最后一个 OnToolStart 的 ctx，子 agent
// 看到的是 b 的值。
func TestToolHookCtx_NestedCalleeSeesItsCall(t *testing.T) {
	log := &scopeLog{}
	child := agent_v2.NewBasicAgent(agent_v2.WithName("child"), agent_v2.WithDescription("子 agent"),
		agent_v2.WithModel(&scopeModel{}), agent_v2.WithHooks(childAgentHooks{log: log}))
	kit := tool.NewToolkit()
	kit.Register(agent_v2.AgentAsTool(child))
	kit.Register(recordingEcho(log))
	childCall := schema.ToolCall{ID: "a", Type: "function", Function: schema.FunctionCall{Name: "child", Arguments: `{"input":"hi"}`}}
	m := &scopeModel{calls: []schema.ToolCall{childCall, scopeCall("b", "echo")}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("parent"), agent_v2.WithModel(m), agent_v2.WithToolkit(kit),
		agent_v2.WithHooks(scopeToolHooks{log: log}), agent_v2.WithMaxRounds(3))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatal(err)
	}
	expectSeen(t, log, map[string]string{
		"子 agent 的 OnAgentStart": "start-a",
		"工具体(b)":                 "start-b",
	})
}

// scopeModelHooks：OnModelStart 放上 "model-start"，OnModelEnd 记下收到的值、放上 "model-end"。
type scopeModelHooks struct {
	hook.NoOpHooks
	log *scopeLog
}

func (h scopeModelHooks) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	return context.WithValue(ctx, scopeKey{}, "model-start"), nil
}

func (h scopeModelHooks) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
	h.log.put("OnModelEnd", scopeOf(ctx))
	return context.WithValue(ctx, scopeKey{}, "model-end"), nil
}

// TestModelHookCtx_ScopedToCall：OnModelStart 放的值在这次模型调用和 OnModelEnd 里可见。
func TestModelHookCtx_ScopedToCall(t *testing.T) {
	log := &scopeLog{}
	m := &scopeModel{log: log}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("scope_model"), agent_v2.WithModel(m),
		agent_v2.WithHooks(scopeModelHooks{log: log}))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatal(err)
	}
	expectSeen(t, log, map[string]string{
		"模型调用(1)":    "model-start",
		"OnModelEnd": "model-start",
	})
}

// scopeErrHooks 在 OnToolStart 放上 "start-<callID>"，OnError 记下收到的值。
type scopeErrHooks struct {
	scopeToolHooks
}

func (h scopeErrHooks) OnError(ctx context.Context, _ *hook.AgentInfo, _ *schema.CommError) context.Context {
	h.log.put("OnError", scopeOf(ctx))
	return ctx
}

// TestToolHookCtx_BatchFailureOnErrorSeesItsCall：只有一个 tool_call、它让整批失败时（OnToolEnd 一个都不触发），OnError
// 收到的是这个调用的 ctx——钩子在 OnToolStart 里开的东西（span 等）只剩这一次机会收尾。好几个调用时分不出失败的是哪个，
// OnError 收节点的 ctx。
func TestToolHookCtx_BatchFailureOnErrorSeesItsCall(t *testing.T) {
	log := &scopeLog{}
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("fatal", "fatal", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return nil, fmt.Errorf("fatal: %w", compose_v2.ErrRunAborted)
		}))
	m := &scopeModel{calls: []schema.ToolCall{scopeCall("a", "fatal")}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("scope_err"), agent_v2.WithModel(m), agent_v2.WithToolkit(kit),
		agent_v2.WithHooks(scopeErrHooks{scopeToolHooks{log: log}}), agent_v2.WithMaxRounds(3))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err == nil {
		t.Fatal("fixture 没搭对：工具返回终局错误，Run 应失败")
	}
	expectSeen(t, log, map[string]string{"OnError": "start-a"})
}
