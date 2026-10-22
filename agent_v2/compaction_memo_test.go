package agent_v2_test

import (
	"context"
	"fmt"
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

// compactLoopModel 前 rounds 次调用每次发一个工具调用，之后收尾；记下每次看到的输入。
type compactLoopModel struct {
	mu     sync.Mutex
	rounds int
	calls  int
	inputs [][]*schema.Message
	askAt  int // >0 时第 askAt 次调用改发 ask_user
}

func (m *compactLoopModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	m.inputs = append(m.inputs, msgs)
	if m.calls > m.rounds {
		return schema.AssistantMessage("完成"), nil
	}
	if m.askAt > 0 && m.calls == m.askAt {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: fmt.Sprintf("ask%d", m.calls), Type: "function",
			Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"继续吗"}`}}}), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: fmt.Sprintf("c%d", m.calls), Type: "function",
		Function: schema.FunctionCall{Name: "grep", Arguments: `{}`}}}), nil
}
func (m *compactLoopModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *compactLoopModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// countingSummarizer 每次返回编号不同的摘要（S1、S2……），记下每次的输入。
type countingSummarizer struct {
	mu     sync.Mutex
	inputs [][]*schema.Message
}

func (s *countingSummarizer) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inputs = append(s.inputs, msgs)
	return schema.AssistantMessage(fmt.Sprintf("<summary>SUMMARY-S%d</summary>", len(s.inputs))), nil
}
func (s *countingSummarizer) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (s *countingSummarizer) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return s, nil }

func (s *countingSummarizer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.inputs)
}

func grepToolkit() *tool.Toolkit {
	tk := tool.NewToolkit()
	payload := strings.Repeat("src/a.go:12: func foo() { return bar }\n", 50) // ~2KB
	tk.Register(tool.NewFuncTool("grep", "grep", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: payload}, nil
		}))
	return tk
}

// smallWindow 让阈值落在若干轮工具调用之内：窗口 12000 - 摘要预留 1000 - 缓冲 1000 = 10000 token，
// 每轮工具结果约 2KB，七八轮越过一次。
func smallWindow(s *countingSummarizer) *agent_v2.AutoCompactConfig {
	return &agent_v2.AutoCompactConfig{Enabled: true, Model: s, ModelContextWindow: 12000,
		SummaryMaxTokens: 1000, SummaryBufferTokens: 1000, MinCompactableTokens: 200}
}

func inputText(msgs []*schema.Message) string {
	var b strings.Builder
	for _, m := range msgs {
		b.WriteString(m.Content)
		b.WriteByte('\n')
	}
	return b.String()
}

// TestAutoCompact_ReusesSummaryWithinRun：同一个 Run 里摘要一次之后，下一轮接着用，不再把整段消息流
// 重新摘要；再次越过阈值时在「上一份摘要 + 之后新增的」上增量摘要。
//
// 断言选在两个量上：摘要被调的次数（每轮都重摘时它约等于越过阈值之后的轮数）；第二次摘要的输入里
// 有没有第一次的摘要（增量）、有没有最早那几条工具结果（重摘全量时一定有）。
func TestAutoCompact_ReusesSummaryWithinRun(t *testing.T) {
	const rounds = 60
	m := &compactLoopModel{rounds: rounds}
	s := &countingSummarizer{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(grepToolkit()), agent_v2.WithMaxRounds(rounds+5),
		agent_v2.WithAutoCompact(smallWindow(s)))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("改代码")); err != nil {
		t.Fatal(err)
	}
	n := s.count()
	if n < 2 {
		t.Fatalf("fixture 没搭对：摘要只被调了 %d 次，测不出复用", n)
	}
	if n > rounds/4 {
		t.Fatalf("%d 轮工具调用里摘要被调了 %d 次——每轮都在把整段消息流重新摘要", rounds, n)
	}
	second := inputText(s.inputs[1])
	if !strings.Contains(second, "SUMMARY-S1") {
		t.Fatalf("第二次摘要的输入里没有第一次的摘要——不是在上一份结果上接着摘：\n%.400s", second)
	}
	if strings.Count(second, "src/a.go:12") > strings.Count(inputText(s.inputs[0]), "src/a.go:12") {
		t.Fatalf("第二次摘要的输入比第一次还多旧工具结果——又把整段消息流摘了一遍")
	}
	// 每次模型调用看到的都在阈值附近，不随轮数涨；而且配对完整——每条 tool_result 前面都有声明它的
	// assistant（拼接备忘与新增消息时差一条就会在摘要后面留下一条孤儿 tool_result，真实 API 直接 400）
	for i, in := range m.inputs {
		if c := len(inputText(in)); c > 40_000 {
			t.Fatalf("第 %d 次模型调用输入 %d 字符——压缩结果没生效", i+1, c)
		}
		declared := map[string]bool{}
		for _, msg := range in {
			for _, tc := range schema.GetToolCalls(msg) {
				declared[tc.ID] = true
			}
			if id := schema.GetToolResultID(msg); msg.Role == schema.RoleTool && !declared[id] {
				t.Fatalf("第 %d 次模型调用的输入里 tool_result %s 前面没有声明它的 assistant（孤儿）", i+1, id)
			}
		}
	}
	t.Logf("%d 轮工具调用，摘要 %d 次", rounds, n)
}

// TestAutoCompact_SummaryMemoSurvivesResume：中断续跑之后接着用中断前的摘要结果，续跑的第一轮不重摘。
//
// 摘要结果随存档走（BasicAgent 图的一条 channel）。不随存档走的话，续跑那一轮读到的是原始全量、
// 仍超阈值，又摘要一次。
func TestAutoCompact_SummaryMemoSurvivesResume(t *testing.T) {
	m := &compactLoopModel{rounds: 40, askAt: 25}
	s := &countingSummarizer{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(grepToolkit()), agent_v2.WithMaxRounds(50), agent_v2.WithAskUser(),
		agent_v2.WithAutoCompact(smallWindow(s)))
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("改代码"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应在第 25 轮问用户，got %v", err)
	}
	before := s.count()
	if before == 0 {
		t.Fatal("fixture 没搭对：中断前一次摘要都没发生，测不出续跑后复用")
	}
	callsBefore := len(m.inputs)
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("继续")})); err != nil {
		t.Fatal(err)
	}
	// 摘要不在消息流里，只在那条 channel 里——续跑后第一次模型调用若看到的是中断前那份（S<before>），
	// 就是从存档里接着用的；没接着用时它会在原始全量上重摘、看到的是一份新编号的摘要。
	firstAfter := inputText(m.inputs[callsBefore])
	if !strings.Contains(firstAfter, fmt.Sprintf("SUMMARY-S%d", before)) {
		t.Fatalf("续跑后第一次模型调用没用上中断前的摘要（S%d）：\n%.300s", before, firstAfter)
	}
}

// prefixRewriteHook 每轮改写消息流里的第一条（用户的原始输入）：拷一份、保留 ID、只换正文，正文长度
// 每轮相同——像每轮按当前规则做一次脱敏。前缀的内容每轮都变，而 ID、条数、长度都不变。
type prefixRewriteHook struct {
	hook.NoOpHooks
	n int
}

func (h *prefixRewriteHook) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	h.n++
	if len(in.Messages) > 0 && in.Messages[0].Role == schema.RoleUser {
		cp := *in.Messages[0]
		cp.Content = fmt.Sprintf("改代码 第%02d轮", h.n)
		msgs := append([]*schema.Message{&cp}, in.Messages[1:]...)
		in.Messages = msgs
	}
	return ctx, nil
}

// prefixFieldRewriteHook 每轮只改消息流前缀里第一条工具调用消息的某一处送给模型的内容（条数、ID、正文、长度都不变）：
// field 为 "args" 时改那次调用的参数，为 "reasoning" 时改推理文本。改的是拷贝，不碰 channel 里的那条。
type prefixFieldRewriteHook struct {
	hook.NoOpHooks
	field string
	n     int
}

func (h *prefixFieldRewriteHook) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	h.n++
	for i, m := range in.Messages {
		if m == nil || len(schema.GetToolCalls(m)) == 0 {
			continue
		}
		cp := *m
		switch h.field {
		case "args":
			cp.ContentBlocks = make([]*schema.ContentBlock, len(m.ContentBlocks))
			for j, cb := range m.ContentBlocks {
				cbCopy := *cb
				if cb.Toolcall != nil {
					tc := *cb.Toolcall
					tc.Arguments = fmt.Sprintf(`{"round":%02d}`, h.n)
					cbCopy.Toolcall = &tc
				}
				cp.ContentBlocks[j] = &cbCopy
			}
		case "reasoning":
			cp.ReasoningContent = fmt.Sprintf("思考第%02d轮", h.n)
		}
		msgs := append([]*schema.Message(nil), in.Messages...)
		msgs[i] = &cp
		in.Messages = msgs
		break
	}
	return ctx, nil
}

// TestAutoCompact_PrefixToolArgsOrReasoningRewritten_MemoNotUsed：钩子每轮只改前缀里那次工具调用的参数（比如脱敏）、或推理文本，
// 正文与长度都不变——上一轮的摘要结果同样不能套到这一轮上：每次摘要的输入里都得是这一轮改写后的内容。指纹只算参数配对
// ID、推理文本只算长度时，备忘照用，这一轮模型看到的摘要是按改写之前的内容做的。
func TestAutoCompact_PrefixToolArgsOrReasoningRewritten_MemoNotUsed(t *testing.T) {
	for _, field := range []string{"args", "reasoning"} {
		t.Run(field, func(t *testing.T) {
			m := &compactLoopModel{rounds: 30}
			s := &roundSummarizer{model: m}
			cfg := &agent_v2.AutoCompactConfig{Enabled: true, Model: s, ModelContextWindow: 12000,
				SummaryMaxTokens: 1000, SummaryBufferTokens: 1000, MinCompactableTokens: 200}
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(m),
				agent_v2.WithToolkit(grepToolkit()), agent_v2.WithMaxRounds(40),
				agent_v2.WithHooks(&prefixFieldRewriteHook{field: field}), agent_v2.WithAutoCompact(cfg))
			if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("改代码")); err != nil {
				t.Fatal(err)
			}
			if s.count() < 2 {
				t.Fatalf("fixture 没搭对：摘要只发生了 %d 次", s.count())
			}
			for k, r := range s.rounds {
				found := false
				for _, mm := range s.inputs[k] {
					switch field {
					case "args":
						for _, tc := range schema.GetToolCalls(mm) {
							found = found || tc.Function.Arguments == fmt.Sprintf(`{"round":%02d}`, r)
						}
					case "reasoning":
						found = found || mm.ReasoningContent == fmt.Sprintf("思考第%02d轮", r)
					}
				}
				if !found {
					t.Fatalf("第 %d 轮的摘要输入里没有钩子这一轮改写后的%s——套用了上一轮前缀的摘要结果", r, field)
				}
			}
		})
	}
}

// roundSummarizer 记下每次摘要发生在第几轮（模型这一轮还没被调，所以是已调次数 + 1）。
type roundSummarizer struct {
	countingSummarizer
	model  *compactLoopModel
	rounds []int
}

func (s *roundSummarizer) Generate(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	s.model.mu.Lock()
	r := s.model.calls + 1
	s.model.mu.Unlock()
	s.mu.Lock()
	s.rounds = append(s.rounds, r)
	s.mu.Unlock()
	return s.countingSummarizer.Generate(ctx, msgs, opts...)
}

// TestAutoCompact_PrefixRewrittenByHook_MemoNotUsed：钩子每轮改写消息流前缀时，上一轮的摘要结果不能
// 套到这一轮上——那份结果代表的是上一轮的前缀。套上去之后这一轮的改写就丢了：被盖住的那一段换成了
// 旧摘要，改写后的内容既不在模型输入里、也不在之后的摘要输入里。
//
// 钩子保留了消息 ID、只换正文，条数与长度都不变——指纹里正文那一维必须真的起作用才认得出来。
func TestAutoCompact_PrefixRewrittenByHook_MemoNotUsed(t *testing.T) {
	m := &compactLoopModel{rounds: 30}
	s := &roundSummarizer{model: m}
	h := &prefixRewriteHook{}
	cfg := &agent_v2.AutoCompactConfig{Enabled: true, Model: s, ModelContextWindow: 12000,
		SummaryMaxTokens: 1000, SummaryBufferTokens: 1000, MinCompactableTokens: 200}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(grepToolkit()), agent_v2.WithMaxRounds(40), agent_v2.WithHooks(h),
		agent_v2.WithAutoCompact(cfg))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("改代码 原始")); err != nil {
		t.Fatal(err)
	}
	if s.count() < 2 {
		t.Fatalf("fixture 没搭对：摘要只发生了 %d 次", s.count())
	}
	for k, r := range s.rounds {
		want := fmt.Sprintf("第%02d轮", r)
		if in := inputText(s.inputs[k]); !strings.Contains(in, want) {
			t.Fatalf("第 %d 轮的摘要输入里没有钩子这一轮改写后的内容 %q——套用了上一轮前缀的摘要结果：\n%.200s", r, want, in)
		}
	}
}
