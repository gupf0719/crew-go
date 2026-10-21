package agent_v2_test

import (
	"context"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// optCapModel 是捕获 per-request model.Option 的 mock——记录每次 Generate 收到的 opts
// 应用后的 ExtraBody（验 WithRunModelOptions/WithResumeModelOptions 真把 option 串到了
// model 调用，而非静默丢弃）。Stream 返 nil 让 streamOrGenerate 降级 Generate（同 perCallModel）。
type optCapModel struct {
	mu     sync.Mutex
	extra  []map[string]any // 每次 Generate 应用 opts 后的 ExtraBody（按调用序）
	respFn func(idx int) *schema.Message
}

func (m *optCapModel) Generate(_ context.Context, _ []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	o := model.Apply(opts...)
	m.mu.Lock()
	idx := len(m.extra)
	m.extra = append(m.extra, o.ExtraBody)
	m.mu.Unlock()
	if m.respFn != nil {
		return m.respFn(idx), nil
	}
	return schema.AssistantMessage("done"), nil
}

func (m *optCapModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *optCapModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *optCapModel) extraAt(i int) map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	if i < 0 || i >= len(m.extra) {
		return nil
	}
	return m.extra[i]
}

func (m *optCapModel) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.extra)
}

var _ model.ChatModel = (*optCapModel)(nil)

// TestWithRunModelOptions_ThreadedToModelCall 验 WithRunModelOptions 注入的 model.Option
// 被串到本次 Run 的 model 调用（Generate 收到对应 ExtraBody）。sanity：不传时 ExtraBody
// 为 nil——old/new 行为分叉，revert 掉 basic_agent 的 modelRunOpts 透传后本断言从 pass 变 fail。
func TestWithRunModelOptions_ThreadedToModelCall(t *testing.T) {
	m := &optCapModel{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("t"), agent_v2.WithModel(m))
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"),
		agent_v2.WithRunModelOptions(model.WithExtraBody(map[string]any{"thinking": "on"})))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if eb := m.extraAt(0); eb == nil || eb["thinking"] != "on" {
		t.Fatalf("WithRunModelOptions 未串到 model 调用：ExtraBody=%v", eb)
	}

	// sanity：不传 WithRunModelOptions → Generate opts 为空 → ExtraBody nil。
	m2 := &optCapModel{}
	ag2 := agent_v2.NewBasicAgent(agent_v2.WithName("t2"), agent_v2.WithModel(m2))
	if _, err := ag2.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("run2: %v", err)
	}
	if eb := m2.extraAt(0); eb != nil {
		t.Fatalf("baseline：无 WithRunModelOptions 应 ExtraBody nil，got %v", eb)
	}
}

// TestWithResumeModelOptions_ThreadedToResume 验 Run/Resume 各自的 per-request model.Option
// 分别串到对应阶段的 model 调用（对齐 WithRunModel/WithResumeModel 的 Run/Resume 对称）。
// Run 用 phase=run、Resume 用 phase=resume——两次 model 调用 ExtraBody 应各自对应。
func TestWithResumeModelOptions_ThreadedToResume(t *testing.T) {
	askTool := tool.NewFuncTool("ask", "ask user", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "ask_id"); ok {
				return tool.Success("user: " + ans), nil
			}
			return nil, &compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "ask_id", Question: "?"}}
		})
	tk := tool.NewToolkit()
	tk.Register(askTool)

	m := &optCapModel{respFn: func(idx int) *schema.Message {
		if idx == 0 { // Run：调 ask → 中断
			return schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "tc", Function: schema.FunctionCall{Name: "ask", Arguments: "{}"}},
			})
		}
		return schema.AssistantMessage("done") // Resume 续跑 final
	}}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("t"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5),
	)
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"),
		agent_v2.WithRunModelOptions(model.WithExtraBody(map[string]any{"phase": "run"})))
	if _, isInt := agent_v2.IsInterrupt(err); !isInt {
		t.Fatalf("expected interrupt from Run, got %v", err)
	}

	_, err = ag.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{"ask_id": schema.UserMessage("ok")}),
		agent_v2.WithResumeModelOptions(model.WithExtraBody(map[string]any{"phase": "resume"})),
		agent_v2.WithResumeSession(sess),
	)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if m.count() < 2 {
		t.Fatalf("expected >=2 model calls (run + resume), got %d", m.count())
	}
	if eb := m.extraAt(0); eb == nil || eb["phase"] != "run" {
		t.Fatalf("Run 阶段 model 调用应带 phase=run，got %v", eb)
	}
	last := m.extraAt(m.count() - 1)
	if last == nil || last["phase"] != "resume" {
		t.Fatalf("Resume 阶段 model 调用应带 phase=resume（Run 的覆盖不该泄漏到 Resume），got %v", last)
	}
}
