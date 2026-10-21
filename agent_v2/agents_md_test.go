package agent_v2_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
)

// capturingModel 记录每次 Generate 收到的输入 messages——验证 AGENTS.md transient 注入。
type capturingModel struct {
	mu       sync.Mutex
	captured [][]*schema.Message
}

func (m *capturingModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	m.captured = append(m.captured, append([]*schema.Message(nil), msgs...))
	m.mu.Unlock()
	return schema.AssistantMessage("ok"), nil
}

func (m *capturingModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil // 走 Generate downgrade，在 Generate 里捕获
}

func (m *capturingModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*capturingModel)(nil)

// TestBasicAgent_WithAgentsMD 验 WithAgentsMD：AGENTS.md 作 system message transient
// 注入 LLM 输入（harness 层，排任务 system 之前），且不写进持久会话历史。
func TestBasicAgent_WithAgentsMD(t *testing.T) {
	dir := t.TempDir()
	mdPath := filepath.Join(dir, "AGENTS.md")
	if err := os.WriteFile(mdPath, []byte("PROJECT RULE: always Go."), 0o644); err != nil {
		t.Fatal(err)
	}

	cm := &capturingModel{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("md_test"),
		agent_v2.WithModel(cm),
		agent_v2.WithSystemPrompt("You are a coder."),
		agent_v2.WithAgentsMD(mdPath),
		agent_v2.WithMaxRounds(2),
	)
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(context.Background(), sess, schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 1. LLM 输入首条 = AGENTS.md system（harness 层），次条 = 任务 system
	if len(cm.captured) == 0 {
		t.Fatal("model 没被调用")
	}
	in := cm.captured[0]
	if len(in) < 2 {
		t.Fatalf("input 太短: %d", len(in))
	}
	if in[0].Role != schema.RoleSystem || !strings.Contains(in[0].Content, "PROJECT RULE") {
		t.Errorf("首条应是 AGENTS.md system; got role=%v content=%q", in[0].Role, in[0].Content)
	}
	if in[1].Role != schema.RoleSystem || !strings.Contains(in[1].Content, "You are a coder") {
		t.Errorf("次条应是任务 system; got role=%v content=%q", in[1].Role, in[1].Content)
	}

	// 2. transient: AGENTS.md 不进持久会话历史
	for _, msg := range sess.History() {
		if strings.Contains(msg.Content, "PROJECT RULE") {
			t.Errorf("AGENTS.md 不该进持久历史（transient）, 但 History 含: %q", msg.Content)
		}
	}
}
