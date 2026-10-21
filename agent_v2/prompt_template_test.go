package agent_v2_test

import (
	"context"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/prompt"
	"crew-go/schema"
)

// msgCapturingModel 记录 Generate 收到的 messages，验证 PromptTemplate 渲染进了对话。
type msgCapturingModel struct {
	mu    sync.Mutex
	seen  []*schema.Message
	final *schema.Message
}

func (m *msgCapturingModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	m.seen = msgs
	m.mu.Unlock()
	return m.final, nil
}

func (m *msgCapturingModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *msgCapturingModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*msgCapturingModel)(nil)

// TestWithPromptTemplate_RendersIntoConversation 验证模板（变量绑定 + 动态 varsFn）渲染进
// LLM 对话最前。
//
// sanity-revert：删 buildInitialMessages 里的模板渲染+prepend 后，m.seen[0] 变 user 'hi'
// （非渲染 system），断言 Content == 你是 客服助手。 必 fail 指向 bug。
func TestWithPromptTemplate_RendersIntoConversation(t *testing.T) {
	m := &msgCapturingModel{final: schema.AssistantMessage("done")}
	tmpl := prompt.FromMessages(prompt.System("你是 {{.role}}。"))
	var gotInput *schema.Message
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("pt"),
		agent_v2.WithModel(m),
		agent_v2.WithPromptTemplate(tmpl, func(_ context.Context, input *schema.Message) map[string]any {
			gotInput = input
			return map[string]any{"role": "客服助手"}
		}),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(m.seen) == 0 {
		t.Fatal("model 未收到 messages")
	}
	if m.seen[0].Role != schema.RoleSystem || m.seen[0].Content != "你是 客服助手。" {
		t.Fatalf("模板 system 应在最前且变量绑定，实际 %+v", m.seen[0])
	}
	if gotInput == nil || gotInput.Content != "hi" {
		t.Fatalf("varsFn 应收到当轮 input 'hi'，实际 %+v", gotInput)
	}
}
