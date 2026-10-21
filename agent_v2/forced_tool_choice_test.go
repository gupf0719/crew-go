package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// enforcingModel 模拟 provider 执行 tool_choice=required：收到 required 就必须调工具（手上有 final_output 时
// 调它、参数合法，否则调 noop），否则以文本作答。记下每轮收到的 tool_choice。
type enforcingModel struct {
	mu      sync.Mutex
	bound   []string
	choices []*schema.ToolChoice
}

func (m *enforcingModel) Generate(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	choice := model.Apply(opts...).ToolChoice
	m.mu.Lock()
	m.choices = append(m.choices, choice)
	names := append([]string(nil), m.bound...)
	m.mu.Unlock()
	if choice == nil || *choice != schema.ToolChoiceRequired || len(names) == 0 {
		return schema.AssistantMessage("答复"), nil
	}
	name, args := "noop", "{}"
	if slices.Contains(names, agent_v2.FinalOutputToolName) {
		name, args = agent_v2.FinalOutputToolName, `{"answer":"结构化"}`
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "tc", Function: schema.FunctionCall{Name: name, Arguments: args},
	}}), nil
}

func (m *enforcingModel) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *enforcingModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	names := make([]string, 0, len(tools))
	for _, t := range tools {
		names = append(names, t.Name)
	}
	m.mu.Lock()
	m.bound = names
	m.mu.Unlock()
	return m, nil
}

func (m *enforcingModel) seen() []*schema.ToolChoice {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*schema.ToolChoice(nil), m.choices...)
}

func noopToolkit() *tool.Toolkit {
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("noop", "什么都不做", map[string]any{"type": "object"},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) { return tool.Success("ok"), nil }))
	return kit
}

func isRequired(c *schema.ToolChoice) bool { return c != nil && *c == schema.ToolChoiceRequired }

// choiceNames 把每轮的 tool_choice 格式化成可读的串（nil 记作 "nil"），给失败信息用。
func choiceNames(cs []*schema.ToolChoice) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		if c == nil {
			out = append(out, "nil")
			continue
		}
		out = append(out, string(*c))
	}
	return out
}

// TestForcedRequired_OnlyUntilFirstToolCall：业务经 WithRunModelOptions 强制 tool_choice=required——只管到模型
// 第一次调工具为止：第 1 轮带着 required（调了工具），第 2 轮复位，模型以文本收尾，一共 2 轮。曾经每轮都强制，
// 模型永远不能以文本收尾，一路调到软降落轮（这里 maxRounds=5 ⟹ 6 轮）。
func TestForcedRequired_OnlyUntilFirstToolCall(t *testing.T) {
	m := &enforcingModel{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("fr"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(noopToolkit()), agent_v2.WithMaxRounds(5))
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("干活"),
		agent_v2.WithRunModelOptions(model.WithToolChoice(schema.ToolChoiceRequired)))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := m.seen()
	if len(got) != 2 {
		t.Fatalf("模型应只被调 2 次（第 1 轮调工具、第 2 轮以文本收尾），实际 %d 次；每轮 tool_choice=%v", len(got), got)
	}
	if !isRequired(got[0]) || got[1] != nil {
		t.Errorf("第 1 轮应带着业务强制的 required、第 2 轮复位，实际 %v / %v", got[0], got[1])
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "答复" {
		t.Errorf("应以文本答复收尾，实际 %+v", final)
	}
}

// TestForcedRequired_StructuredOutputKeepsOwnRequired：配了结构化输出时，框架自己设的 required 每轮都在——复位
// 只针对业务强制的那份。第 1 轮业务强制 + 框架 required，第 2 轮仍是 required（框架那份），模型经 final_output
// 收尾。
func TestForcedRequired_StructuredOutputKeepsOwnRequired(t *testing.T) {
	m := &enforcingModel{}
	kit := noopToolkit()
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("fs"), agent_v2.WithModel(&firstNoopThen{inner: m}),
		agent_v2.WithToolkit(kit), agent_v2.WithOutputSchema("A", answerSchema()), agent_v2.WithMaxRounds(5))
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("干活"),
		agent_v2.WithRunModelOptions(model.WithToolChoice(schema.ToolChoiceRequired)))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := m.seen()
	if len(got) != 2 || !isRequired(got[0]) || !isRequired(got[1]) {
		t.Fatalf("两轮都应是 required（第 2 轮是框架为结构化输出设的），实际 %v", got)
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != `{"answer":"结构化"}` {
		t.Errorf("应经 final_output 以结构化 JSON 收尾，实际 %+v", final)
	}
}

// searchModel：第 1 次调用调 first（检索 lookup）；之后收到 required 就调 lookup，否则以文本作答。记下每次的 tool_choice。
type searchModel struct {
	mu      sync.Mutex
	first   string
	choices []*schema.ToolChoice
}

func (m *searchModel) Generate(_ context.Context, _ []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	choice := model.Apply(opts...).ToolChoice
	m.mu.Lock()
	n := len(m.choices)
	m.choices = append(m.choices, choice)
	m.mu.Unlock()
	switch {
	case n == 0:
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "s1", Function: schema.FunctionCall{Name: m.first, Arguments: `{"query":"lookup"}`}}}), nil
	case isRequired(choice):
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "l1", Function: schema.FunctionCall{Name: "lookup", Arguments: "{}"}}}), nil
	}
	return schema.AssistantMessage("凭自己知识答复"), nil
}

func (m *searchModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *searchModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *searchModel) seen() []*schema.ToolChoice {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*schema.ToolChoice(nil), m.choices...)
}

// TestForcedRequired_ToolSearchDoesNotConsume：开了渐进披露、业务强制 required——第 1 轮只能调 tool_search（lookup
// 被延迟），这不算「调过工具」；第 2 轮仍是 required，模型去调刚解锁的 lookup；第 3 轮才复位、以文本收尾。曾经
// tool_search 也算，第 2 轮就复位，lookup 一次没调、模型凭自己知识作答（审计实测）。
func TestForcedRequired_ToolSearchDoesNotConsume(t *testing.T) {
	var lookups atomic.Int32
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("lookup", "查资料", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			lookups.Add(1)
			return tool.Success("资料"), nil
		}))
	m := &searchModel{first: "tool_search"}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("ts_req"), agent_v2.WithModel(m), agent_v2.WithToolkit(kit),
		agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true,
			Deferred: func(tl tool.Tool) bool { return tl.Name() == "lookup" }}),
		agent_v2.WithMaxRounds(5))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("查一下"),
		agent_v2.WithRunModelOptions(model.WithToolChoice(schema.ToolChoiceRequired))); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := m.seen()
	if len(got) != 3 || !isRequired(got[0]) || !isRequired(got[1]) || got[2] != nil {
		t.Fatalf("应为 [required required nil]（第一批检索解锁了 lookup、不消耗强制额度），实际 %v", choiceNames(got))
	}
	if n := lookups.Load(); n != 1 {
		t.Errorf("强制额度应留给解锁后的 lookup，它该被调 1 次，实际 %d 次", n)
	}
}

// TestForcedRequired_BusinessToolNamedToolSearchCounts：业务自己写的工具恰好叫 tool_search——它是业务工具，调它照常
// 算「调过工具」（按派发的实例认，不按名字）：第 2 轮复位。
func TestForcedRequired_BusinessToolNamedToolSearchCounts(t *testing.T) {
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("tool_search", "业务自己的检索", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("结果"), nil }))
	m := &searchModel{first: "tool_search"}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("biz_ts_req"), agent_v2.WithModel(m), agent_v2.WithToolkit(kit),
		agent_v2.WithMaxRounds(5))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("查一下"),
		agent_v2.WithRunModelOptions(model.WithToolChoice(schema.ToolChoiceRequired))); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := m.seen(); len(got) != 2 || !isRequired(got[0]) || got[1] != nil {
		t.Fatalf("业务的 tool_search 调过就算，应为 [required nil]，实际 %v", choiceNames(got))
	}
}

// firstNoopThen 第 1 轮固定调 noop（制造「调过工具」），之后交给 inner；tool_choice 一律记在 inner 里。
type firstNoopThen struct {
	inner *enforcingModel
	mu    sync.Mutex
	n     int
}

func (f *firstNoopThen) Generate(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	f.mu.Lock()
	f.n++
	first := f.n == 1
	f.mu.Unlock()
	if !first {
		return f.inner.Generate(ctx, in, opts...)
	}
	f.inner.mu.Lock()
	f.inner.choices = append(f.inner.choices, model.Apply(opts...).ToolChoice)
	f.inner.mu.Unlock()
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "tc0", Function: schema.FunctionCall{Name: "noop", Arguments: "{}"},
	}}), nil
}

func (f *firstNoopThen) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (f *firstNoopThen) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	if _, err := f.inner.WithTools(tools); err != nil {
		return nil, err
	}
	return f, nil
}

// scriptedChoiceModel 按「第几次调用、这次是不是被强制」给出回复，记下每次的 tool_choice。
type scriptedChoiceModel struct {
	mu      sync.Mutex
	choices []*schema.ToolChoice
	script  func(n int, required bool) *schema.Message
}

func (m *scriptedChoiceModel) Generate(_ context.Context, _ []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	choice := model.Apply(opts...).ToolChoice
	m.mu.Lock()
	n := len(m.choices)
	m.choices = append(m.choices, choice)
	m.mu.Unlock()
	return m.script(n, isRequired(choice)), nil
}

func (m *scriptedChoiceModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *scriptedChoiceModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *scriptedChoiceModel) seen() []*schema.ToolChoice {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*schema.ToolChoice(nil), m.choices...)
}

// callsMsg 拼一条带若干 tool_call 的 assistant 消息，每项是 {工具名, 参数 JSON}。
func callsMsg(calls ...[2]string) *schema.Message {
	tcs := make([]schema.ToolCall, 0, len(calls))
	for i, c := range calls {
		tcs = append(tcs, schema.ToolCall{ID: fmt.Sprintf("c%d", i), Function: schema.FunctionCall{Name: c[0], Arguments: c[1]}})
	}
	return schema.AssistantToolCallMessage(tcs)
}

// runDeferredLookup 跑一个只有 lookup（被延迟）的 agent，业务强制 required；返回每轮的 tool_choice、lookup 被调次数、
// 最终答复与错误。
func runDeferredLookup(t *testing.T, script func(n int, required bool) *schema.Message) ([]*schema.ToolChoice, int32, string, error) {
	t.Helper()
	var lookups atomic.Int32
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("lookup", "查资料", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			lookups.Add(1)
			return tool.Success("资料"), nil
		}))
	m := &scriptedChoiceModel{script: script}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("deferred_lookup"), agent_v2.WithModel(m), agent_v2.WithToolkit(kit),
		agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true,
			Deferred: func(tl tool.Tool) bool { return tl.Name() == "lookup" }}),
		agent_v2.WithMaxRounds(5))
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("你好"),
		agent_v2.WithRunModelOptions(model.WithToolChoice(schema.ToolChoiceRequired)))
	final := ""
	if f := agent_v2.FinalMessage(events); f != nil {
		final = f.Content
	}
	return m.seen(), lookups.Load(), final, err
}

// TestForcedRequired_NoMatchSearchConsumes：检索什么都没解锁（检索不到）照算「调过工具」——第 2 轮复位、模型以文本
// 收尾。曾经 tool_search 一律不算：检索不到时模型每轮只能再检索，一路被逼到软降落轮、以 MaxIterationsError 收尾
// （审计实测 6 次模型调用）。
func TestForcedRequired_NoMatchSearchConsumes(t *testing.T) {
	got, _, final, err := runDeferredLookup(t, func(_ int, required bool) *schema.Message {
		if required {
			return callsMsg([2]string{"tool_search", `{"query":"zzzz"}`})
		}
		return schema.AssistantMessage("文本答复")
	})
	if err != nil {
		t.Fatalf("Run: %v（每轮 tool_choice=%v）", err, choiceNames(got))
	}
	if len(got) != 2 || !isRequired(got[0]) || got[1] != nil {
		t.Fatalf("检索不到应照算、第 2 轮复位，应为 [required nil]，实际 %v", choiceNames(got))
	}
	if final != "文本答复" {
		t.Errorf("应以文本答复收尾，实际 %q", final)
	}
}

// TestForcedRequired_SearchOfUnlockedToolConsumes：检索到的工具早已解锁（第 2 次检索同一个）——这一批什么也没新
// 解锁，照算：第 3 轮复位。
func TestForcedRequired_SearchOfUnlockedToolConsumes(t *testing.T) {
	got, lookups, _, err := runDeferredLookup(t, func(n int, required bool) *schema.Message {
		switch {
		case n <= 1:
			return callsMsg([2]string{"tool_search", `{"query":"lookup"}`})
		case required:
			return callsMsg([2]string{"lookup", "{}"})
		}
		return schema.AssistantMessage("文本答复")
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(got) != 3 || !isRequired(got[0]) || !isRequired(got[1]) || got[2] != nil {
		t.Fatalf("第 1 次检索解锁了 lookup 不算、第 2 次什么也没新解锁照算，应为 [required required nil]，实际 %v", choiceNames(got))
	}
	if lookups != 0 {
		t.Errorf("复位之后模型以文本作答，lookup 不该被调，实际 %d 次", lookups)
	}
}

// TestForcedRequired_DuplicateSearchesInOneBatchUnlock：同一批两次检索命中同一个工具——按整批判，这一批解锁了新
// 工具、不算；第 2 轮仍强制，模型去调 lookup。按单次检索判的话第二次「什么也没解锁」，强制额度在模型还没调 lookup
// 时就没了。
func TestForcedRequired_DuplicateSearchesInOneBatchUnlock(t *testing.T) {
	got, lookups, _, err := runDeferredLookup(t, func(n int, required bool) *schema.Message {
		switch {
		case n == 0:
			return callsMsg([2]string{"tool_search", `{"query":"lookup"}`}, [2]string{"tool_search", `{"query":"查资料 lookup"}`})
		case required:
			return callsMsg([2]string{"lookup", "{}"})
		}
		return schema.AssistantMessage("文本答复")
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(got) != 3 || !isRequired(got[0]) || !isRequired(got[1]) || got[2] != nil {
		t.Fatalf("应为 [required required nil]（这一批解锁了 lookup），实际 %v", choiceNames(got))
	}
	if lookups != 1 {
		t.Errorf("强制额度应留给解锁出来的 lookup，它该被调 1 次，实际 %d 次", lookups)
	}
}

// TestForcedRequired_FailedCallsCount：「调过工具」按模型发起过调用算、不看结果——直接调还没解锁的工具（被拒）、
// 调不存在的工具名，都算：第 2 轮复位。
func TestForcedRequired_FailedCallsCount(t *testing.T) {
	for _, name := range []string{"lookup", "no_such_tool"} {
		got, lookups, _, err := runDeferredLookup(t, func(_ int, required bool) *schema.Message {
			if required {
				return callsMsg([2]string{name, "{}"})
			}
			return schema.AssistantMessage("文本答复")
		})
		if err != nil {
			t.Fatalf("%s: Run: %v", name, err)
		}
		if len(got) != 2 || !isRequired(got[0]) || got[1] != nil {
			t.Errorf("%s: 失败的调用也算调过，应为 [required nil]，实际 %v", name, choiceNames(got))
		}
		if lookups != 0 {
			t.Errorf("%s: lookup 没解锁，不该被执行，实际 %d 次", name, lookups)
		}
	}
}

// namedKit 注册几个什么都不做的工具。
func namedKit(names ...string) *tool.Toolkit {
	kit := tool.NewToolkit()
	for _, n := range names {
		kit.Register(tool.NewFuncTool(n, "工具 "+n, map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("ok"), nil }))
	}
	return kit
}

// runAlwaysSearch 跑一个被强制时每轮都换个词检索、否则以文本作答的 agent（业务强制 required、轮次预算 6），
// 返回每轮的 tool_choice 与错误。
func runAlwaysSearch(t *testing.T, kit *tool.Toolkit, deferred map[string]bool,
	retriever func(context.Context, string, []tool.Tool) []string) ([]*schema.ToolChoice, error) {
	t.Helper()
	m := &scriptedChoiceModel{script: func(n int, required bool) *schema.Message {
		if required {
			return callsMsg([2]string{"tool_search", fmt.Sprintf(`{"query":"q%d"}`, n)})
		}
		return schema.AssistantMessage("文本答复")
	}}
	cfg := &agent_v2.ToolSearchConfig{Enabled: true, Deferred: func(tl tool.Tool) bool { return deferred[tl.Name()] },
		Retriever: retriever}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("always_search"), agent_v2.WithModel(m), agent_v2.WithToolkit(kit),
		agent_v2.WithToolSearch(cfg), agent_v2.WithMaxRounds(6))
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("你好"),
		agent_v2.WithRunModelOptions(model.WithToolChoice(schema.ToolChoiceRequired)))
	return m.seen(), err
}

// TestForcedRequired_OnlyFirstSearchBatchExempt：工具库比轮次预算大、每批检索都解锁一个新工具——只豁免第一批，
// 第二批起照算：[required required nil]。曾经每批解锁了新工具都不算，8 个被延迟工具、预算 6 轮时一路被逼到软降落、
// 以 MaxIterationsError 收尾（审计实测）。
func TestForcedRequired_OnlyFirstSearchBatchExempt(t *testing.T) {
	names := []string{"t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7"}
	deferred := map[string]bool{}
	for _, n := range names {
		deferred[n] = true
	}
	var next atomic.Int32
	got, err := runAlwaysSearch(t, namedKit(names...), deferred, func(_ context.Context, _ string, cands []tool.Tool) []string {
		i := int(next.Add(1)) - 1
		if i >= len(cands) {
			return nil
		}
		return []string{cands[i].Name()} // 每次解锁一个没解锁过的
	})
	if err != nil {
		t.Fatalf("Run: %v（每轮 tool_choice=%v）", err, choiceNames(got))
	}
	if len(got) != 3 || !isRequired(got[0]) || !isRequired(got[1]) || got[2] != nil {
		t.Fatalf("只豁免第一批，应为 [required required nil]，实际 %v", choiceNames(got))
	}
}

// TestForcedRequired_RetrieverNamesOutsideCandidatesIgnored：检索器返回候选以外的名字（共享索引、在全量工具上检索）
// ——框架丢掉它们，这一批什么都没解锁、照算：[required nil]。曾经原样揭示，每批都被判成解锁了新工具（审计实测一路
// 被逼到软降落），回给模型的「已解锁」清单里还列着调不了的工具。
func TestForcedRequired_RetrieverNamesOutsideCandidatesIgnored(t *testing.T) {
	got, err := runAlwaysSearch(t, namedKit("lookup", "visible"), map[string]bool{"lookup": true},
		func(_ context.Context, query string, _ []tool.Tool) []string {
			return []string{"ghost_" + query, "visible"} // 不存在的、本来就可见的
		})
	if err != nil {
		t.Fatalf("Run: %v（每轮 tool_choice=%v）", err, choiceNames(got))
	}
	if len(got) != 2 || !isRequired(got[0]) || got[1] != nil {
		t.Fatalf("候选以外的名字不算解锁，应为 [required nil]，实际 %v", choiceNames(got))
	}
}

// failOnToolEnd 让每次 OnToolEnd 报错：tools 节点在工具跑完之后失败，任务被放弃。
type failOnToolEnd struct{ hook.NoOpHooks }

func (failOnToolEnd) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ToolOutput) (context.Context, error) {
	return ctx, errors.New("guard boom")
}

// TestForcedRequired_AbandonedTaskTurnRecordDiscarded：任务 A 检索解锁了 lookup 之后失败、被放弃，同一个 session
// NextTask 开任务 B；B 的第一批什么都没解锁，照算：[required nil]。曾经 A 留在会话上的本轮解锁记录被 B 的第一批
// 取走，判成解锁了新工具、多强制一轮（审计实测），A 解锁的名字还写进了 B 的解锁集。
func TestForcedRequired_AbandonedTaskTurnRecordDiscarded(t *testing.T) {
	deferred := agent_v2.ToolSearchConfig{Enabled: true, Deferred: func(tl tool.Tool) bool { return tl.Name() == "lookup" }}
	sess := agent_v2.NewRunSession()
	mA := &scriptedChoiceModel{script: func(int, bool) *schema.Message {
		return callsMsg([2]string{"tool_search", `{"query":"lookup"}`})
	}}
	cfgA := deferred
	agA := agent_v2.NewBasicAgent(agent_v2.WithName("same_name"), agent_v2.WithModel(mA), agent_v2.WithToolkit(namedKit("lookup")),
		agent_v2.WithToolSearch(&cfgA), agent_v2.WithHooks(failOnToolEnd{}), agent_v2.WithMaxRounds(6))
	if _, err := agA.Run(context.Background(), sess, schema.UserMessage("查")); err == nil {
		t.Fatal("前提：任务 A 应在 tools 节点失败")
	}
	sess.NextTask()

	mB := &scriptedChoiceModel{script: func(_ int, required bool) *schema.Message {
		if required {
			return callsMsg([2]string{"tool_search", `{"query":"zzzz"}`})
		}
		return schema.AssistantMessage("文本答复")
	}}
	cfgB := deferred
	agB := agent_v2.NewBasicAgent(agent_v2.WithName("same_name"), agent_v2.WithModel(mB), agent_v2.WithToolkit(namedKit("lookup")),
		agent_v2.WithToolSearch(&cfgB), agent_v2.WithMaxRounds(6))
	if _, err := agB.Run(context.Background(), sess, schema.UserMessage("你好"),
		agent_v2.WithRunModelOptions(model.WithToolChoice(schema.ToolChoiceRequired))); err != nil {
		t.Fatalf("任务 B：%v", err)
	}
	if got := mB.seen(); len(got) != 2 || !isRequired(got[0]) || got[1] != nil {
		t.Fatalf("任务 B 的第一批什么都没解锁，应为 [required nil]，实际 %v", choiceNames(got))
	}
}
