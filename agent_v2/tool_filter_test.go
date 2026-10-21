package agent_v2_test

import (
	"context"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// toolRecordingModel 记录每轮 WithTools 收到的工具名，用于验证 ToolFilter 真正裁了
// bind list。Stream 返 error 强制走 Generate fallback。
type toolRecordingModel struct {
	mu    sync.Mutex
	bound []string
	final *schema.Message
}

func (m *toolRecordingModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return m.final, nil
}

func (m *toolRecordingModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *toolRecordingModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bound = nil
	for _, t := range tools {
		if t != nil {
			m.bound = append(m.bound, t.Name)
		}
	}
	return m, nil
}

var _ model.ChatModel = (*toolRecordingModel)(nil)

func twoToolKit(t *testing.T) *tool.Toolkit {
	t.Helper()
	mk := func(name string) tool.Tool {
		return tool.NewFuncTool(name, name+" desc",
			map[string]any{"type": "object", "properties": map[string]any{}},
			func(_ context.Context, _ map[string]any) (*tool.Result, error) {
				return tool.Success("ok"), nil
			})
	}
	tk := tool.NewToolkit()
	tk.Register(mk("alpha"))
	tk.Register(mk("beta"))
	return tk
}

// TestWithToolFilter_RestrictsBoundTools 验证 ToolFilter 返回的子集真正裁掉了 bind list。
//
// bomb 防御（第 1 类·过滤后元素原位）：裁到 ["beta"]（第 2 个工具），sanity-revert 删
// ToolFilter 调用后 bound 变 ["alpha","beta"]——首元素 + 长度都变，必 fail 指向 bug。
func TestWithToolFilter_RestrictsBoundTools(t *testing.T) {
	m := &toolRecordingModel{final: schema.AssistantMessage("done")}
	var gotInput *schema.Message
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("tf"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(twoToolKit(t)),
		agent_v2.WithToolFilter(func(_ context.Context, input *schema.Message) []string {
			gotInput = input
			return []string{"beta"}
		}),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(m.bound) != 1 || m.bound[0] != "beta" {
		t.Fatalf("ToolFilter 应只 bind [beta]，实际 %v", m.bound)
	}
	// ToolFilter 必须收到本轮发给 LLM 的最后一条消息（首轮 = user 输入）。
	if gotInput == nil || gotInput.Content != "hi" {
		t.Fatalf("ToolFilter 应收到本轮 user 输入 'hi'，实际 %+v", gotInput)
	}
}

// TestWithToolFilter_NilMeansNoFilter 验证返回 nil = 不裁（全部 bind）。
func TestWithToolFilter_NilMeansNoFilter(t *testing.T) {
	m := &toolRecordingModel{final: schema.AssistantMessage("done")}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("tf"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(twoToolKit(t)),
		agent_v2.WithToolFilter(func(_ context.Context, _ *schema.Message) []string {
			return nil
		}),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(m.bound) != 2 {
		t.Fatalf("ToolFilter 返 nil 应全部 bind [alpha,beta]，实际 %v", m.bound)
	}
}

// TestWithToolFilter_KeepsFrameworkTools 验证 ToolFilter 豁免框架工具——白名单只含 beta、
// 不含 ask_user，但框架的 ask_user（WithAskUser 注入）仍被 bind（与审批层豁免一致，按实例判），
// alpha（非白名单业务工具）被裁。业务自己写一件叫 ask_user 的工具不在此列，照常受白名单约束
// （见 framework_tool_identity_gates_test.go）。
//
// sanity-revert：删 filterToolsByName 里的框架工具豁免后 ask_user 被裁、hasAskUser
// 变 false → 断言 fail 指向 bug。
func TestWithToolFilter_KeepsFrameworkTools(t *testing.T) {
	mk := func(name string) tool.Tool {
		return tool.NewFuncTool(name, name+" desc",
			map[string]any{"type": "object", "properties": map[string]any{}},
			func(_ context.Context, _ map[string]any) (*tool.Result, error) {
				return tool.Success("ok"), nil
			})
	}
	tk := tool.NewToolkit()
	tk.Register(mk("alpha"))
	tk.Register(mk("beta"))

	m := &toolRecordingModel{final: schema.AssistantMessage("done")}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("tf-fw"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithAskUser(),
		agent_v2.WithToolFilter(func(_ context.Context, _ *schema.Message) []string {
			return []string{"beta"} // 故意不含 ask_user / alpha
		}),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := map[string]bool{}
	for _, n := range m.bound {
		got[n] = true
	}
	if !got[agent_v2.AskUserToolName] {
		t.Fatalf("框架工具 ask_user 应豁免恒保留，实际 bound=%v", m.bound)
	}
	if !got["beta"] {
		t.Fatalf("白名单业务工具 beta 应保留，实际 bound=%v", m.bound)
	}
	if got["alpha"] {
		t.Fatalf("非白名单业务工具 alpha 应被裁，实际 bound=%v", m.bound)
	}
}
