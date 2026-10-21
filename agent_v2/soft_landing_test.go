package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// greedyModel 每轮都调工具，永不主动收尾——除非工具被摘掉。
type greedyModel struct {
	mu       sync.Mutex
	rounds   int
	sawTools []bool // 每轮 LLM 是否拿到了工具
}

func (g *greedyModel) Generate(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	g.mu.Lock()
	g.rounds++
	g.sawTools = append(g.sawTools, false) // 无 WithTools 调用 ⟹ 没拿到工具
	n := g.rounds
	g.mu.Unlock()
	return schema.AssistantMessage("第 " + string(rune('0'+n)) + " 轮的收尾答复"), nil
}

func (g *greedyModel) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (g *greedyModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return &greedyBound{parent: g}, nil
}

// greedyBound 是「拿到了工具」的那个分支——它每轮都调工具。
type greedyBound struct{ parent *greedyModel }

func (b *greedyBound) Generate(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	b.parent.mu.Lock()
	b.parent.rounds++
	b.parent.sawTools = append(b.parent.sawTools, true)
	b.parent.mu.Unlock()
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "tc1", Function: schema.FunctionCall{Name: "noop", Arguments: "{}"},
	}}), nil
}
func (b *greedyBound) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (b *greedyBound) WithTools(t []*schema.ToolInfo) (model.ChatModel, error) { return b, nil }

// TestSoftLanding_LastRoundHasNoTools 钉住**轮次耗尽时模型得到一次收尾机会**。
//
// ⚠️ 承重的量有两个，缺一不可：
//   - **最后一轮 LLM 有没有拿到工具**——软降落的机制就是摘掉它们。断言「有没有产出答案」
//     不够：一个什么都没改的退化下模型照样可能碰巧不调工具。
//   - **err 照返**——`maxRounds` 既可能是保险丝也可能是预算，框架不替产品决定。
//     err 消失了才是「静默截断」，那比原来的问题更糟。
func TestSoftLanding_LastRoundHasNoTools(t *testing.T) {
	g := &greedyModel{}
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("noop", "什么都不做",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			return tool.Success("ok"), nil
		}))
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("sl"), agent_v2.WithModel(g),
		agent_v2.WithToolkit(kit), agent_v2.WithMaxRounds(3))

	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("干活"))

	g.mu.Lock()
	saw := append([]bool(nil), g.sawTools...)
	g.mu.Unlock()
	if len(saw) == 0 {
		t.Fatal("fixture 没搭对：模型一次都没被调")
	}
	if saw[len(saw)-1] {
		t.Errorf("最后一轮 LLM **拿到了工具** %v——软降落没生效，模型没有收尾机会", saw)
	}
	for i := 0; i < len(saw)-1; i++ {
		if !saw[i] {
			t.Errorf("第 %d 轮就摘了工具 %v——只有最后一轮该摘，提前摘等于砍掉预算", i, saw)
		}
	}

	var mie *compose_v2.MaxIterationsError
	if !errors.As(err, &mie) {
		t.Errorf("err 该照返 MaxIterationsError——maxRounds 当保险丝用的调用方靠它；"+
			"err 消失就是静默截断，比原问题更糟。实际 %v", err)
	}
	if fm := agent_v2.FinalMessage(events); fm == nil || !strings.Contains(fm.Content, "收尾") {
		t.Errorf("events 里该有那条收尾答复——当预算用的调用方 errors.As 之后要能取到它。实际 %v", fm)
	}
}

// TestSoftLanding_StructuredOutputStillDelivers 钉住**软降落不摘 `final_output`**。
//
// ⚠️ 这条是审计抓到的回归：`toolkit = nil` 把框架工具一起摘了，而 `final_output` 是
// `WithOutputSchema` 下**唯一的答案投递通道**。后果有两层——请求体畸形
// （`tool_choice=required` 配「没有工具」），以及**熔断信号被替换**：模型只能自由文本
// ⟹ postamble 抛 `StructuredOutputError` ⟹ 补 `MaxIterationsError` 那段带 `err == nil`
// 前置、根本轮不到 ⟹ 业务检测预算耗尽，**配了 OutputSchema 就永远检测不到**。
//
// 承重的量是**最后一轮 LLM 手上还有没有 final_output**——只断言「有没有报错」抓不到，
// 两种坏法都会报错，只是报的不是同一个错。
func TestSoftLanding_StructuredOutputStillDelivers(t *testing.T) {
	g := &toolNameRecorder{}
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("noop", "什么都不做",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			return tool.Success("ok"), nil
		}))
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("so"), agent_v2.WithModel(g),
		agent_v2.WithToolkit(kit), agent_v2.WithMaxRounds(2),
		agent_v2.WithOutputSchema("ans", map[string]any{
			"type": "object", "properties": map[string]any{"v": map[string]any{"type": "string"}},
		}))
	_, _ = ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("干活"))

	g.mu.Lock()
	rounds := append([][]string(nil), g.perRound...)
	g.mu.Unlock()
	if len(rounds) == 0 {
		t.Fatal("fixture 没搭对：模型一次都没被调")
	}
	last := rounds[len(rounds)-1]
	var hasFinal bool
	for _, n := range last {
		if n == agent_v2.FinalOutputToolName {
			hasFinal = true
		}
	}
	if !hasFinal {
		t.Errorf("软降落轮把 final_output 也摘了（那轮手上的工具=%v）——"+
			"配了 WithOutputSchema 的 agent 从此无法收尾，而熔断信号会被 "+
			"StructuredOutputError 替换掉，业务再也检测不到预算耗尽", last)
	}
	for _, n := range last {
		if n != agent_v2.FinalOutputToolName {
			t.Errorf("软降落轮留下了 %q——只该留 final_output，其余（尤其 activate_skill / "+
				"tool_search 这些延长循环的）留着就废了软降落。那轮工具=%v", n, last)
		}
	}
}

// toolNameRecorder 记每轮 LLM 手上有哪些工具名。
type toolNameRecorder struct {
	mu       sync.Mutex
	perRound [][]string
	bound    []string
}

func (r *toolNameRecorder) Generate(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	r.mu.Lock()
	names := append([]string(nil), r.bound...)
	r.perRound = append(r.perRound, names)
	r.bound = nil
	r.mu.Unlock()
	// **一直调 noop 直到它被摘掉**——不这么做循环第 1 轮就退出，根本走不到软降落轮
	// （第一版 fixture 就栽在这儿，靠断言里那句「留下了 noop」才发现）。
	for _, n := range names {
		if n == "noop" {
			return schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID: "tc", Function: schema.FunctionCall{Name: "noop", Arguments: "{}"},
			}}), nil
		}
	}
	return schema.AssistantMessage("自由文本"), nil
}
func (r *toolNameRecorder) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (r *toolNameRecorder) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	names := make([]string, 0, len(tools))
	for _, t := range tools {
		if t != nil {
			names = append(names, t.Name)
		}
	}
	r.mu.Lock()
	r.bound = names
	r.mu.Unlock()
	return r, nil
}

// interruptOnceModel 第一轮抛中断，之后跟 greedyModel 一样一直调工具。
type interruptOnceModel struct {
	mu       sync.Mutex
	fired    bool
	rounds   int
	sawTools []bool
}

func (m *interruptOnceModel) Generate(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	m.rounds++
	m.sawTools = append(m.sawTools, false)
	n := m.rounds
	m.mu.Unlock()
	return schema.AssistantMessage("第 " + string(rune('0'+n)) + " 轮的收尾答复"), nil
}
func (m *interruptOnceModel) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *interruptOnceModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	return &interruptOnceBound{p: m}, nil
}

type interruptOnceBound struct{ p *interruptOnceModel }

func (b *interruptOnceBound) Generate(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	b.p.mu.Lock()
	first := !b.p.fired
	b.p.fired = true
	b.p.rounds++
	b.p.sawTools = append(b.p.sawTools, true)
	b.p.mu.Unlock()
	if first {
		return nil, &compose_v2.InterruptError{Reason: "hitl", Query: &compose_v2.AskQuery{
			InterruptID: "q-sl", Question: "继续吗"}}
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "tc", Function: schema.FunctionCall{Name: "noop", Arguments: "{}"},
	}}), nil
}
func (b *interruptOnceBound) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (b *interruptOnceBound) WithTools(t []*schema.ToolInfo) (model.ChatModel, error) { return b, nil }

// TestSoftLanding_ResumePathAlsoReportsExhaustion 钉住**Resume 路径同样报轮次耗尽**。
//
// ⚠️ 这条是审计抓到的回归：补错只补在了 Run 那一半，于是**一旦走过一次中断，`Resume`
// 就是那个终结调用，从此 maxRounds 当保险丝用的调用方再也看不见熔断**。而顶层是
// BasicAgent 的 HITL 产品（ask_user / 工具审批）恰恰是本仓最主流的形态。
//
// 承重的量**必须是 Resume 的 err**——断言「有没有答案」抓不到，答案两边都在。
func TestSoftLanding_ResumePathAlsoReportsExhaustion(t *testing.T) {
	m := &interruptOnceModel{}
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("noop", "什么都不做",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			return tool.Success("ok"), nil
		}))
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("slr"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(kit), agent_v2.WithMaxRounds(3))

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("干活"))
	ie, isInt := agent_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("fixture 没搭对：第一轮该抛中断，实际 %v", err)
	}

	events, rerr := ag.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("继续"),
		}),
		agent_v2.WithResumeSession(sess))

	var mie *compose_v2.MaxIterationsError
	if !errors.As(rerr, &mie) {
		t.Errorf("Resume 跑满轮次却没报 MaxIterationsError（实际 %v）——"+
			"走过一次中断之后 Resume 就是终结调用，保险丝用法从此看不见熔断", rerr)
	}
	if fm := agent_v2.FinalMessage(events); fm == nil || !strings.Contains(fm.Content, "收尾") {
		t.Errorf("Resume 的 events 里该有收尾答复（预算用法靠它），实际 %v", fm)
	}
}

// TestSoftLanding_BusinessFinalOutputWithoutSchemaIsRemoved：没配 WithOutputSchema 时 final_output 这个名字
// 是业务工具，软降落轮跟别的业务工具一起摘掉——只有框架那件（结构化输出的投递通道）才留。曾经按名字留：
// 业务那件在最后一轮照样绑着，模型调了它就跑满轮次、报原始的超限错误、拿不到收尾答复（审计实测）。
//
// 承重的量同上一条：最后一轮 LLM 手上还剩哪些工具。
func TestSoftLanding_BusinessFinalOutputWithoutSchemaIsRemoved(t *testing.T) {
	g := &toolNameRecorder{}
	kit := tool.NewToolkit()
	for _, name := range []string{"noop", agent_v2.FinalOutputToolName} {
		kit.Register(tool.NewFuncTool(name, "业务工具",
			map[string]any{"type": "object", "properties": map[string]any{}},
			func(ctx context.Context, args map[string]any) (*tool.Result, error) {
				return tool.Success("ok"), nil
			}))
	}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("sl"), agent_v2.WithModel(g),
		agent_v2.WithToolkit(kit), agent_v2.WithMaxRounds(2))
	_, _ = ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("干活"))

	g.mu.Lock()
	rounds := append([][]string(nil), g.perRound...)
	g.mu.Unlock()
	if len(rounds) < 2 {
		t.Fatalf("fixture 没搭对：模型只被调了 %d 次，走不到软降落轮", len(rounds))
	}
	if last := rounds[len(rounds)-1]; len(last) != 0 {
		t.Errorf("没配结构化输出时软降落轮该摘掉全部业务工具、同名的 final_output 也不例外，那轮手上的工具=%v", last)
	}
}

// toolChoiceRecorder 记每轮 LLM 手上的工具名与收到的 tool_choice；拿到工具时一直调 noop。
type toolChoiceRecorder struct {
	mu      sync.Mutex
	bound   []string
	rounds  [][]string
	choices []*schema.ToolChoice
}

func (r *toolChoiceRecorder) Generate(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	r.mu.Lock()
	names := append([]string(nil), r.bound...)
	r.rounds = append(r.rounds, names)
	r.choices = append(r.choices, model.Apply(opts...).ToolChoice)
	r.bound = nil
	r.mu.Unlock()
	if len(names) > 0 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc", Function: schema.FunctionCall{Name: "noop", Arguments: "{}"},
		}}), nil
	}
	return schema.AssistantMessage("收尾答复"), nil
}
func (r *toolChoiceRecorder) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (r *toolChoiceRecorder) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	names := make([]string, 0, len(tools))
	for _, t := range tools {
		if t != nil {
			names = append(names, t.Name)
		}
	}
	r.mu.Lock()
	r.bound = names
	r.mu.Unlock()
	return r, nil
}

// TestSoftLanding_ForcedToolChoiceDroppedWithTools：业务经 WithRunModelOptions 强制了 tool_choice=required——
// 软降落轮把工具摘光时 tool_choice 一并摘掉（「必须调工具」配「没有工具」provider 返 400），前面有工具的轮次照常
// 带着它。曾经照发，软降落那一轮的请求必然被拒。
func TestSoftLanding_ForcedToolChoiceDroppedWithTools(t *testing.T) {
	g := &toolChoiceRecorder{}
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("noop", "什么都不做",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			return tool.Success("ok"), nil
		}))
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("sl"), agent_v2.WithModel(g),
		agent_v2.WithToolkit(kit), agent_v2.WithMaxRounds(2))
	_, _ = ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("干活"),
		agent_v2.WithRunModelOptions(model.WithToolChoice(schema.ToolChoiceRequired)))

	g.mu.Lock()
	rounds, choices := g.rounds, g.choices
	g.mu.Unlock()
	if len(rounds) < 2 {
		t.Fatalf("fixture 没搭对：模型只被调了 %d 次，走不到软降落轮", len(rounds))
	}
	if c := choices[0]; c == nil || *c != schema.ToolChoiceRequired {
		t.Errorf("有工具的轮次应照常带着业务强制的 required，实际 %v", c)
	}
	last := len(rounds) - 1
	if len(rounds[last]) != 0 || choices[last] != nil {
		t.Errorf("软降落轮摘光了工具，业务强制的 tool_choice 也该摘掉：那轮工具=%v tool_choice=%v", rounds[last], choices[last])
	}
}
