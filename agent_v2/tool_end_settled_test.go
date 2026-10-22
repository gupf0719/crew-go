package agent_v2_test

// 一批 tool_call 没整体成功时钩子看到什么（ToolCallErrCode 的 godoc）：
//   - 已经有结局的调用照样各自触发 OnToolEnd，带 ToolBatchFailedExtraKey：跑完的交它的结果，硬失败的交框架合成的失败
//     结果（ToolCallErrCode，它自己超时用 ToolTimeoutErrCode）；挂起等人回答的不触发；
//   - 成功路径上某个调用的 OnToolEnd 钩子中断（这一批随之作废）时，排在后面的调用同样这样收尾——续跑时每个调用都恰好
//     收到过一次不带重放标记的 OnToolEnd；
//   - 失败路径上钩子 panic 只记日志，不把这一批的中断变成失败；
//   - 续跑时从工具账重放的调用照常触发 OnToolStart / OnToolEnd，带 ToolReplayedExtraKey；这次真执行的不带；
//   - 模型调用失败时 OnError 的 Code 按错误分：自身超时 ModelCallTimeoutErrCode、内容过滤 ModelContentFilteredErrCode。

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// settledEndRecorder 记下每次 OnToolEnd 收到的 (CallId, 成没成, 错误码, 重放, 这一批没整体成功) 与每次 OnError 的 Code。
// askOn 非空时对这个 CallId 的 OnToolEnd 调 compose_v2.Ask（答过就放行）；panicFirst 为真时第一次 OnToolEnd panic。
type settledEndRecorder struct {
	hook.NoOpHooks
	mu         sync.Mutex
	ends       []string
	errors     []string
	askOn      string
	panicFirst bool
	n          int
}

func (r *settledEndRecorder) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	replayed, _ := out.Extra[agent_v2.ToolReplayedExtraKey].(bool)
	failed, _ := out.Extra[agent_v2.ToolBatchFailedExtraKey].(bool)
	r.mu.Lock()
	r.n++
	n := r.n
	r.ends = append(r.ends, fmt.Sprintf("%s ok=%v code=%s replayed=%v batch_failed=%v", out.CallId, out.Result.Success,
		out.Result.ErrorCode, replayed, failed))
	r.mu.Unlock()
	if r.panicFirst && n == 1 {
		panic("hook bug on the first OnToolEnd")
	}
	if out.CallId == r.askOn {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "结果给模型看之前确认一下？"}); err != nil {
			return ctx, err
		}
	}
	return ctx, nil
}

func (r *settledEndRecorder) OnError(ctx context.Context, _ *hook.AgentInfo, err *schema.CommError) context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors = append(r.errors, err.Code)
	return ctx
}

func (r *settledEndRecorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.ends
	r.ends = nil
	return out
}

// settledBatchModel：首轮一次发出 calls 里的全部 tool_call，见到 tool_result 后收尾。
type settledBatchModel struct{ calls []schema.ToolCall }

func (m settledBatchModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }
func (m settledBatchModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == schema.RoleUser {
			break
		}
		if msgs[i].Role == schema.RoleTool {
			return schema.AssistantMessage("done"), nil
		}
	}
	return schema.AssistantToolCallMessage(m.calls), nil
}
func (m settledBatchModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func settledTC(id, name, args string) schema.ToolCall {
	return schema.ToolCall{ID: id, Type: "function", Function: schema.FunctionCall{Name: name, Arguments: args}}
}

// settledTool：err 非空时返回它（硬失败），否则成功。
type settledTool struct {
	name string
	err  error
}

func (s settledTool) Name() string                { return s.name }
func (s settledTool) Description() string         { return s.name }
func (s settledTool) InputSchema() map[string]any { return map[string]any{"type": "object"} }
func (s settledTool) Call(context.Context, map[string]any) (*tool.Result, error) {
	if s.err != nil {
		return nil, s.err
	}
	return tool.Success("ok"), nil
}

// 一批 [lookup 成功, broken 硬失败]：两个调用各收到一次 OnToolEnd，都带 batch_failed——lookup 是它的成功结果，broken
// 是框架合成的失败（ToolCallErrCode；它自己超时时是 ToolTimeoutErrCode）。
func TestToolEnd_FailedBatch_EachSettledCallEnds(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code string
	}{
		{"hard_failure", errors.New("db connection reset"), agent_v2.ToolCallErrCode},
		{"own_timeout", fmt.Errorf("Post \"https://x\": %w", context.DeadlineExceeded), agent_v2.ToolTimeoutErrCode},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &settledEndRecorder{}
			tk := tool.NewToolkit()
			tk.Register(settledTool{name: "lookup"})
			tk.Register(settledTool{name: "broken", err: tc.err})
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("mixer"), agent_v2.WithMaxRounds(3), agent_v2.WithToolkit(tk),
				agent_v2.WithHooks(rec), agent_v2.WithModel(settledBatchModel{calls: []schema.ToolCall{
					settledTC("call_lookup", "lookup", "{}"), settledTC("call_broken", "broken", "{}")}}))
			if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err == nil {
				t.Fatal("fixture：broken 硬失败应让 Run 报错")
			}
			want := []string{
				"call_lookup ok=true code= replayed=false batch_failed=true",
				"call_broken ok=false code=" + tc.code + " replayed=false batch_failed=true",
			}
			if got := rec.take(); fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("OnToolEnd 收到 %q，want %q", got, want)
			}
		})
	}
}

// 一批 [lookup 成功, ask_user 挂起]：Run 里只有 lookup 收到 OnToolEnd（batch_failed；ask_user 没有结局）；续跑时 lookup
// 从工具账重放，带 replayed、不带 batch_failed，ask_user 这次真执行、两个都不带。
func TestToolEnd_InterruptedBatch_CompletedEndsThenReplayMarked(t *testing.T) {
	rec := &settledEndRecorder{}
	tk := tool.NewToolkit()
	tk.Register(settledTool{name: "lookup"})
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("asker"), agent_v2.WithAskUser(), agent_v2.WithMaxRounds(3),
		agent_v2.WithToolkit(tk), agent_v2.WithHooks(rec), agent_v2.WithModel(settledBatchModel{calls: []schema.ToolCall{
			settledTC("call_lookup", "lookup", "{}"), settledTC("call_ask", agent_v2.AskUserToolName, `{"question":"确认吗？"}`)}}))
	sess := agent_v2.NewRunSession()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture：应停在 ask_user 上，got %v", err)
	}
	if got, want := rec.take(), []string{"call_lookup ok=true code= replayed=false batch_failed=true"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("Run 里 OnToolEnd 收到 %q，want %q", got, want)
	}
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage("确认"),
	})); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	want := []string{"call_lookup ok=true code= replayed=true batch_failed=false", "call_ask ok=true code= replayed=false batch_failed=false"}
	if got := rec.take(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("续跑里 OnToolEnd 收到 %q，want %q", got, want)
	}
}

// 成功路径上 ca 的 OnToolEnd 钩子问人（这一批随之作废）：排在后面的 cb 按失败路径收尾（batch_failed），续跑时两个都从
// 工具账重放——每个调用恰好收到过一次不带重放标记的 OnToolEnd（曾经 cb 一次都没有，续跑时却带着重放标记，它唯一一次
// 真实执行就没人记了）。
func TestToolEnd_HookInterruptMidBatch_RestSettled(t *testing.T) {
	rec := &settledEndRecorder{askOn: "ca"}
	tk := tool.NewToolkit()
	tk.Register(settledTool{name: "a"})
	tk.Register(settledTool{name: "b"})
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("ender"), agent_v2.WithMaxRounds(3), agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(rec), agent_v2.WithModel(settledBatchModel{calls: []schema.ToolCall{
			settledTC("ca", "a", "{}"), settledTC("cb", "b", "{}")}}))
	sess := agent_v2.NewRunSession()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture：ca 的 OnToolEnd 应停在提问上，got %v", err)
	}
	run1 := rec.take()
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage("确认"),
	})); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	all := append(run1, rec.take()...)
	for _, id := range []string{"ca", "cb"} {
		n := 0
		for _, e := range all {
			if len(e) > len(id) && e[:len(id)+1] == id+" " && !contains(e, "replayed=true") {
				n++
			}
		}
		if n != 1 {
			t.Errorf("%s 应恰好收到一次不带重放标记的 OnToolEnd，got %d（全程 %q）", id, n, all)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// 一批 [lookup 跑完, ask_user 挂起]，钩子第一次 OnToolEnd（失败路径上给 lookup 收尾那次）panic：只记日志，Run 照样以
// 中断收尾、问题交给用户（曾经 panic 冒到节点，变成 node panic recovered，能回答的提问成了失败）。
func TestToolEnd_SettledHookPanic_KeepsInterrupt(t *testing.T) {
	rec := &settledEndRecorder{panicFirst: true}
	tk := tool.NewToolkit()
	tk.Register(settledTool{name: "lookup"})
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("asker"), agent_v2.WithAskUser(), agent_v2.WithMaxRounds(3),
		agent_v2.WithToolkit(tk), agent_v2.WithHooks(rec), agent_v2.WithModel(settledBatchModel{calls: []schema.ToolCall{
			settledTC("call_lookup", "lookup", "{}"), settledTC("call_ask", agent_v2.AskUserToolName, `{"question":"确认吗？"}`)}}))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err == nil {
		t.Fatal("应停在 ask_user 上")
	} else if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Errorf("失败路径上的钩子 panic 不该改变这一批的结局，Run 返回 %v", err)
	}
}

// settledErrModel：Generate 恒返 err。
type settledErrModel struct{ err error }

func (m settledErrModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }
func (m settledErrModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, m.err
}
func (m settledErrModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

// 模型调用失败时 OnError 的 Code 按错误分。
func TestModelCallError_CodeByCause(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code string
	}{
		{"own_timeout", fmt.Errorf("Post \"https://x\": %w", context.DeadlineExceeded), agent_v2.ModelCallTimeoutErrCode},
		{"content_filtered", fmt.Errorf("openai: %w", model.ErrContentFiltered), agent_v2.ModelContentFilteredErrCode},
		{"other", errors.New("provider 500"), "model_call_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &settledEndRecorder{}
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("mm"), agent_v2.WithMaxRounds(2), agent_v2.WithHooks(rec),
				agent_v2.WithModel(settledErrModel{err: tc.err}))
			if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err == nil {
				t.Fatal("fixture：模型失败应让 Run 报错")
			}
			rec.mu.Lock()
			got := rec.errors
			rec.mu.Unlock()
			if len(got) == 0 || got[0] != tc.code {
				t.Errorf("OnError 收到 %q，第一条应是 %q", got, tc.code)
			}
		})
	}
}
