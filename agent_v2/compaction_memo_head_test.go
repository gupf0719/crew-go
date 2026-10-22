package agent_v2_test

// 套用了上一次压缩结果之后，同一个 Run 里还要能接着压——跟 Head 里装了什么无关（审计 a96c2cba 的 HIGH）。
//
// 曾经把压缩输出（Head）原样当输入再喂给压缩：Head 里的摘要、文件重注入都是 user 角色，尾部判定把它们当成
// 「用户的最新一轮」；Head 里在摘要之前有受保护的组（激活过的 skill 正文）、或开了文件重注入时，摘要一次
// 之后就再也压不动，模型输入随轮数线性涨（60 轮涨到 20 万字符，`err == nil`）。现在压缩显式知道 Head 在哪
// （compactCut）：Head 与新增部分分开分组、尾部只在新增部分里找、上一次的重注入丢掉从原始消息流重算。
//
// 断言选在模型每次看到的输入大小上：不变量是「同一个 Run 里它有界，不随轮数涨」。

import (
	"context"
	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/skill"
	"crew-go/tool"
	"fmt"
	"strings"
	"sync"
	"testing"
)

type pzLoopModel struct {
	mu     sync.Mutex
	rounds int
	calls  int
	sizes  []int
}

func memoHeadText(msgs []*schema.Message) int {
	n := 0
	for _, m := range msgs {
		n += len(m.Content)
		for _, cb := range m.ContentBlocks {
			if cb != nil && cb.Toolresult != nil {
				n += len(cb.Toolresult.Content)
			}
		}
	}
	return n
}

func (m *pzLoopModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	m.sizes = append(m.sizes, memoHeadText(msgs))
	if m.calls > m.rounds {
		return schema.AssistantMessage("完成"), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: fmt.Sprintf("c%d", m.calls), Type: "function",
		Function: schema.FunctionCall{Name: "Read", Arguments: fmt.Sprintf(`{"path":"src/f%d.go"}`, m.calls%3)}}}), nil
}
func (m *pzLoopModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *pzLoopModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

type memoHeadSummarizer struct {
	mu sync.Mutex
	n  int
	// staleReinject 数「摘要输入里出现了上一次的文件重注入」的次数——它该被丢掉、从原始消息流重算，
	// 不该当成普通内容再摘一遍（摘要模型白读一遍旧文件内容）。
	staleReinject int
}

func (s *memoHeadSummarizer) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	for _, m := range msgs {
		if strings.Contains(m.Content, "# Recently Read Files") {
			s.staleReinject++
			break
		}
	}
	return schema.AssistantMessage(fmt.Sprintf("<summary>SUMMARY-S%d</summary>", s.n)), nil
}
func (s *memoHeadSummarizer) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (s *memoHeadSummarizer) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return s, nil }

func TestAutoCompactMemo_HeadWithReinjection_StillCompacts(t *testing.T) {
	for _, reinject := range []bool{false, true} {
		t.Run(fmt.Sprintf("reinject=%v", reinject), func(t *testing.T) {
			const rounds = 60
			m := &pzLoopModel{rounds: rounds}
			s := &memoHeadSummarizer{}
			tk := tool.NewToolkit()
			payload := strings.Repeat("src/a.go:12: func foo() { return bar }\n", 50) // ~2KB
			tk.Register(tool.NewFuncTool("Read", "read file", map[string]any{"type": "object"},
				func(context.Context, map[string]any) (*tool.Result, error) {
					return &tool.Result{Success: true, Output: payload}, nil
				}))
			cfg := &agent_v2.AutoCompactConfig{Enabled: true, Model: s, ModelContextWindow: 12000,
				SummaryMaxTokens: 1000, SummaryBufferTokens: 1000, MinCompactableTokens: 200,
				ReinjectRecentFiles: reinject}
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(m),
				agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(rounds+5), agent_v2.WithAutoCompact(cfg))
			if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("改代码")); err != nil {
				t.Fatal(err)
			}
			maxSize := 0
			for _, c := range m.sizes {
				if c > maxSize {
					maxSize = c
				}
			}
			t.Logf("reinject=%v summaries=%d maxInputChars=%d lastInputChars=%d sizes[0:8]=%v ... last5=%v",
				reinject, s.n, maxSize, m.sizes[len(m.sizes)-1], m.sizes[:8], m.sizes[len(m.sizes)-5:])
			if s.staleReinject > 0 {
				t.Errorf("有 %d 次摘要的输入里带着上一次的文件重注入——该丢掉重算", s.staleReinject)
			}
			if maxSize > 60_000 {
				t.Errorf("模型输入涨到 %d 字符——压缩停了", maxSize)
			}
		})
	}
}

type memoHeadSkillModel struct {
	mu     sync.Mutex
	rounds int
	calls  int
	sizes  []int
}

func (m *memoHeadSkillModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	m.sizes = append(m.sizes, memoHeadText(msgs))
	if m.calls == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "sk1", Type: "function",
			Function: schema.FunctionCall{Name: agent_v2.ActivateSkillToolName, Arguments: `{"skill":"coding","query":"改代码"}`}}}), nil
	}
	if m.calls > m.rounds {
		return schema.AssistantMessage("完成"), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: fmt.Sprintf("c%d", m.calls), Type: "function",
		Function: schema.FunctionCall{Name: "grep", Arguments: `{}`}}}), nil
}
func (m *memoHeadSkillModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *memoHeadSkillModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func TestAutoCompactMemo_HeadWithSkillGroup_StillCompacts(t *testing.T) {
	const rounds = 60
	m := &memoHeadSkillModel{rounds: rounds}
	s := &memoHeadSummarizer{}
	tk := tool.NewToolkit()
	payload := strings.Repeat("src/a.go:12: func foo() { return bar }\n", 50)
	tk.Register(tool.NewFuncTool("grep", "grep", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: payload}, nil
		}))
	box, err := skill.NewInMemorySkillBox("b", []skill.SkillContent{{
		Descriptor: skill.SkillDescriptor{Name: "coding", Description: "写代码"}, Body: "你是代码专家，按规范改代码。"}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := &agent_v2.AutoCompactConfig{Enabled: true, Model: s, ModelContextWindow: 12000,
		SummaryMaxTokens: 1000, SummaryBufferTokens: 1000, MinCompactableTokens: 200}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(rounds+5), agent_v2.WithAutoCompact(cfg),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("改代码")); err != nil {
		t.Fatal(err)
	}
	maxSize := 0
	for _, c := range m.sizes {
		if c > maxSize {
			maxSize = c
		}
	}
	t.Logf("summaries=%d maxInputChars=%d last5=%v", s.n, maxSize, m.sizes[len(m.sizes)-5:])
	if maxSize > 60_000 {
		t.Errorf("模型输入涨到 %d 字符——压缩停了", maxSize)
	}
}

type memoHeadSkillModel5 struct {
	mu     sync.Mutex
	rounds int
	calls  int
	sizes  []int
}

func (m *memoHeadSkillModel5) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	m.sizes = append(m.sizes, memoHeadText(msgs))
	if m.calls == 5 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "sk1", Type: "function",
			Function: schema.FunctionCall{Name: agent_v2.ActivateSkillToolName, Arguments: `{"skill":"coding","query":"改代码"}`}}}), nil
	}
	if m.calls > m.rounds {
		return schema.AssistantMessage("完成"), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: fmt.Sprintf("c%d", m.calls), Type: "function",
		Function: schema.FunctionCall{Name: "grep", Arguments: `{}`}}}), nil
}
func (m *memoHeadSkillModel5) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *memoHeadSkillModel5) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func TestAutoCompactMemo_SkillActivatedLater_StillCompacts(t *testing.T) {
	const rounds = 60
	m := &memoHeadSkillModel5{rounds: rounds}
	s := &memoHeadSummarizer{}
	tk := tool.NewToolkit()
	payload := strings.Repeat("src/a.go:12: func foo() { return bar }\n", 50)
	tk.Register(tool.NewFuncTool("grep", "grep", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: payload}, nil
		}))
	box, err := skill.NewInMemorySkillBox("b", []skill.SkillContent{{
		Descriptor: skill.SkillDescriptor{Name: "coding", Description: "写代码"}, Body: "你是代码专家，按规范改代码。"}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := &agent_v2.AutoCompactConfig{Enabled: true, Model: s, ModelContextWindow: 12000,
		SummaryMaxTokens: 1000, SummaryBufferTokens: 1000, MinCompactableTokens: 200}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(rounds+5), agent_v2.WithAutoCompact(cfg),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("改代码")); err != nil {
		t.Fatal(err)
	}
	maxSize := 0
	for _, c := range m.sizes {
		if c > maxSize {
			maxSize = c
		}
	}
	t.Logf("summaries=%d maxInputChars=%d last5=%v", s.n, maxSize, m.sizes[len(m.sizes)-5:])
	if maxSize > 60_000 {
		t.Errorf("模型输入涨到 %d 字符——压缩停了", maxSize)
	}
}
