package agent_v2_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/model/openai"
	"crew-go/schema"
	"crew-go/tool"
)

func isNone(c *schema.ToolChoice) bool { return c != nil && *c == schema.ToolChoiceNone }

// landingModel 记每轮绑到的工具名与收到的 tool_choice。手上有 noop 就一直调它、永不主动收尾；收到 tool_choice=none
// 时以文本收尾——跟强制执行 none 的 provider 一样。hardCall 非空时不认收尾要求：收到 none（或手上没有 noop）也照调
// 这个工具——不强制执行 none 的 provider、业务自己写的不认 tool_choice 的 ChatModel。
type landingModel struct {
	hardCall string

	mu      sync.Mutex
	bound   []string
	rounds  [][]string
	choices []*schema.ToolChoice
}

func (m *landingModel) Generate(_ context.Context, _ []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	choice := model.Apply(opts...).ToolChoice
	m.mu.Lock()
	names := m.bound
	m.bound = nil
	m.rounds = append(m.rounds, names)
	m.choices = append(m.choices, choice)
	m.mu.Unlock()
	switch {
	case !isNone(choice) && slices.Contains(names, "noop"):
		return callMsg(tcall("tc", "noop", "{}")), nil
	case m.hardCall != "":
		return callMsg(tcall("tc", m.hardCall, "{}")), nil
	default:
		return schema.AssistantMessage("收尾答复"), nil
	}
}

func (m *landingModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *landingModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	names := make([]string, 0, len(tools))
	for _, t := range tools {
		if t != nil {
			names = append(names, t.Name)
		}
	}
	m.mu.Lock()
	m.bound = names
	m.mu.Unlock()
	return m, nil
}

func (m *landingModel) seen() ([][]string, []*schema.ToolChoice) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.rounds), slices.Clone(m.choices)
}

// TestSoftLanding_LastRoundKeepsToolsSendsNone 钉住**轮次耗尽时模型得到一次收尾机会，且那一轮的工具集不变**。
//
// 没配结构化输出时，最后一轮绑的工具跟前几轮同一套、tool_choice=none——不是摘掉工具：工具集是 Anthropic 给 thinking
// 块签名时绑定的前缀之一，摘掉就让之前的 thinking 全部作废（见 withFinalRound）。
//
// ⚠️ 承重的量有三个，缺一不可：
//   - **最后一轮的工具集与前一轮相同**——摘掉工具同样能逼模型收尾，只断言「有没有产出答案」分不出两种机制。
//   - **只有最后一轮带 tool_choice=none**——它是这一轮唯一的收尾机制；提前带上等于砍掉预算。
//   - **err 照返**——`maxRounds` 既可能是保险丝也可能是预算，框架不替产品决定。
//     err 消失了才是「静默截断」，那比原来的问题更糟。
func TestSoftLanding_LastRoundKeepsToolsSendsNone(t *testing.T) {
	var n atomic.Int32
	m := &landingModel{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("sl"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(kitOf(namedBizTool("noop", &n, "ok"))), agent_v2.WithMaxRounds(3))

	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("干活"))

	rounds, choices := m.seen()
	if len(rounds) != 4 {
		t.Fatalf("fixture 没搭对：maxRounds=3 应调模型 4 次（3 个工具轮 + 软降落轮），实际 %d 次", len(rounds))
	}
	last := len(rounds) - 1
	if len(rounds[last]) == 0 || !slices.Equal(rounds[last], rounds[last-1]) {
		t.Errorf("最后一轮的工具集应跟前一轮相同（摘掉工具会让之前的 thinking 作废），实际前一轮 %v、最后一轮 %v",
			rounds[last-1], rounds[last])
	}
	for i, c := range choices {
		if isNone(c) != (i == last) {
			t.Errorf("tool_choice=none 只该出现在最后一轮，实际每轮 %v", choiceNames(choices))
			break
		}
	}
	if got := n.Load(); got != 3 {
		t.Errorf("noop 应执行 3 次（每个工具轮一次），实际 %d 次", got)
	}
	var mie *compose_v2.MaxIterationsError
	if !errors.As(err, &mie) {
		t.Errorf("err 该照返 MaxIterationsError——maxRounds 当保险丝用的调用方靠它；"+
			"err 消失就是静默截断，比原问题更糟。实际 %v", err)
	}
	if fm := agent_v2.FinalMessage(events); fm == nil || fm.Content != "收尾答复" {
		t.Errorf("events 里该有那条收尾答复——当预算用的调用方 errors.As 之后要能取到它。实际 %v", fm)
	}
}

// TestSoftLanding_HardToolCallNotExecuted：最后一轮发了 tool_choice=none，模型照样调工具（provider 不强制执行 none、
// 业务自己的 ChatModel 不认它）——框架不执行，以失败结果交回。执行的话就是在轮次预算之外多跑一批副作用，而结果再
// 没有一轮可以消化。
//
// 承重的量是**工具实际执行了几次**：断言「有没有报错」抓不到，执行与否都以 MaxIterationsError 收尾。
func TestSoftLanding_HardToolCallNotExecuted(t *testing.T) {
	var n atomic.Int32
	m := &landingModel{hardCall: "noop"}
	h := &toolEndRecorder{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("sl"), agent_v2.WithModel(m), agent_v2.WithHooks(h),
		agent_v2.WithToolkit(kitOf(namedBizTool("noop", &n, "ok"))), agent_v2.WithMaxRounds(2))

	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("干活"))

	if rounds, _ := m.seen(); len(rounds) != 3 {
		t.Fatalf("fixture 没搭对：maxRounds=2 应调模型 3 次，实际 %d 次", len(rounds))
	}
	if got := n.Load(); got != 2 {
		t.Errorf("软降落轮模型硬调的 noop 不该执行：应只在前两个工具轮各执行 1 次，实际执行 %d 次", got)
	}
	if res := h.snapshot(); len(res) != 3 || res[2].Success || res[2].ErrorCode != "round_budget_exhausted" {
		t.Errorf("软降落轮那次调用应以失败结果（round_budget_exhausted）交回，实际每次的结果 %+v", res)
	}
	var mie *compose_v2.MaxIterationsError
	if !errors.As(err, &mie) {
		t.Errorf("应以 MaxIterationsError 收尾，实际 %v", err)
	}
}

// askOnToolsRound 在 tools 节点第 round 轮那一批的 OnToolStart 里问一次用户：首跑停在中断，续跑拿到答复放行。
type askOnToolsRound struct {
	hook.NoOpHooks
	round int32
}

func (h askOnToolsRound) OnToolStart(ctx context.Context, info *hook.AgentInfo, _ *schema.ToolInput) (context.Context, error) {
	if info == nil || info.Round != h.round {
		return ctx, nil
	}
	if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"}); err != nil {
		return ctx, err
	}
	return ctx, nil
}

// TestSoftLanding_HardToolCallNotExecutedAfterResume：软降落轮那一批停在中断（这里是 OnToolStart 里问用户），续跑直接
// 进 tools 节点——产出这批调用的那次模型调用不再重跑，「是不是软降落轮」要从存档里读回来，硬调的工具照样不执行。这个
// 标记只放在内存里的话，续跑时读到的是零值，那次调用就被执行了。
func TestSoftLanding_HardToolCallNotExecutedAfterResume(t *testing.T) {
	var n atomic.Int32
	// maxRounds=1：第 1 轮调 noop（tools 节点 Round=1），第 2 轮是软降落轮、硬调 noop（tools 节点 Round=2）。
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("slr"), agent_v2.WithModel(&landingModel{hardCall: "noop"}),
		agent_v2.WithHooks(askOnToolsRound{round: 2}),
		agent_v2.WithToolkit(kitOf(namedBizTool("noop", &n, "ok"))), agent_v2.WithMaxRounds(1))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("干活"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture 没搭对：软降落轮那一批应停在 OnToolStart 的提问上，实际 %v", err)
	}
	if got := n.Load(); got != 1 {
		t.Fatalf("fixture 没搭对：中断前 noop 应只在第 1 轮执行过 1 次，实际 %d 次", got)
	}
	_, rerr := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("继续")}))
	if got := n.Load(); got != 1 {
		t.Errorf("续跑后软降落轮硬调的 noop 不该执行，实际 noop 一共执行了 %d 次", got)
	}
	var mie *compose_v2.MaxIterationsError
	if !errors.As(rerr, &mie) {
		t.Errorf("续跑应以 MaxIterationsError 收尾，实际 %v", rerr)
	}
}

// TestSoftLanding_AtMostOnePerTaskAcrossResume：每个任务至多一个软降落轮。不认 none 的 provider 在软降落轮硬调工具、
// 被执行闸拦下之后，续跑不再调模型。续跑时引擎的访问计数从 0 数、round 却接着数，不挡的话越过预算之后每一轮都被判成
// 软降落轮，模型被白调到本次续跑的访问上限（maxRounds=3 时续跑白调 4 次、没有收尾答复，审计实测；改动前摘工具，同样的
// 续跑调 1 次就收尾）。
//
// 承重的量是续跑时模型被调了几次。
func TestSoftLanding_AtMostOnePerTaskAcrossResume(t *testing.T) {
	var n atomic.Int32
	m := &landingModel{hardCall: "noop"}
	// maxRounds=3：第 3 个工具批（最后一个正常工具轮，tools 节点 Round=3）停在 OnToolStart 的提问上。
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("sl"), agent_v2.WithModel(m), agent_v2.WithHooks(askOnToolsRound{round: 3}),
		agent_v2.WithToolkit(kitOf(namedBizTool("noop", &n, "ok"))), agent_v2.WithMaxRounds(3))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("干活"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture 没搭对：第 3 个工具批应停在 OnToolStart 的提问上，实际 %v", err)
	}
	before, _ := m.seen()
	_, rerr := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("继续")}))
	after, choices := m.seen()
	if got := len(after) - len(before); got != 1 {
		t.Errorf("续跑应只调 1 次模型（那一次就是软降落轮），实际调了 %d 次，每次的 tool_choice %v",
			got, choiceNames(choices[len(before):]))
	}
	if got := n.Load(); got != 3 {
		t.Errorf("noop 应只在 3 个工具轮各执行 1 次，实际执行 %d 次", got)
	}
	var mie *compose_v2.MaxIterationsError
	if !errors.As(rerr, &mie) {
		t.Errorf("续跑应以 MaxIterationsError 收尾，实际 %v", rerr)
	}
}

// TestSoftLanding_RaisedBudgetOnResumeLandsAgain：撞上「每个任务至多一个软降落轮」之后存档照留，续跑把预算调大，新预算的
// 最后一轮另算、照常软降落。这里调到正好等于已用的轮数——下一次调用就是新预算的最后一轮（边界：闸判的是「越过」预算，
// 不是「到达」）。
func TestSoftLanding_RaisedBudgetOnResumeLandsAgain(t *testing.T) {
	var n atomic.Int32
	m := &landingModel{hardCall: "noop"}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("sl"), agent_v2.WithModel(m), agent_v2.WithHooks(askOnToolsRound{round: 3}),
		agent_v2.WithToolkit(kitOf(namedBizTool("noop", &n, "ok"))), agent_v2.WithMaxRounds(3))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("干活"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture 没搭对：第 3 个工具批应停在 OnToolStart 的提问上，实际 %v", err)
	}
	// 第一次续跑：软降落轮（round 3）硬调被拦，回到 model 节点时 round 4 越过预算 3 ⟹ 撞闸。
	_, rerr := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("继续")}))
	var mie *compose_v2.MaxIterationsError
	if !errors.As(rerr, &mie) {
		t.Fatalf("fixture 没搭对：第一次续跑应撞上闸、报 MaxIterationsError，实际 %v", rerr)
	}
	before, _ := m.seen()
	// 第二次续跑：预算调到 4（正好等于已用的轮数）⟹ 下一次调用是新预算的最后一轮。
	_, rerr = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeMaxRounds(4))
	after, choices := m.seen()
	if got := len(after) - len(before); got != 1 || !isNone(choices[len(choices)-1]) {
		t.Errorf("调大预算的续跑应调 1 次模型、那一次是新预算的软降落轮（tool_choice=none），实际调了 %d 次，每次的 tool_choice %v（err %v）",
			got, choiceNames(choices[len(before):]), rerr)
	}
}

// TestSoftLanding_ShrunkBudgetOnResumeStillLands：续跑时把预算调得比已用的轮数还小（WithResumeMaxRounds），续跑的第一次
// 模型调用就是软降落轮——照常给一次收尾机会。「每个任务至多一个软降落轮」那道闸只在软降落轮之后才拦；只看「round 越过
// 预算」的话，这里一次都不调就报错，正文还说「软降落轮也已用过」（审计实测）。
//
// 承重的量是续跑时模型被调了几次、那一次的 tool_choice。
func TestSoftLanding_ShrunkBudgetOnResumeStillLands(t *testing.T) {
	var n atomic.Int32
	m := &landingModel{}
	// maxRounds=5：第 3 个工具批（tools 节点 Round=3）停在 OnToolStart 的提问上；续跑把预算缩到 1。
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("sl"), agent_v2.WithModel(m), agent_v2.WithHooks(askOnToolsRound{round: 3}),
		agent_v2.WithToolkit(kitOf(namedBizTool("noop", &n, "ok"))), agent_v2.WithMaxRounds(5))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("干活"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture 没搭对：第 3 个工具批应停在 OnToolStart 的提问上，实际 %v", err)
	}
	before, _ := m.seen()
	events, rerr := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeMaxRounds(1),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("继续")}))
	after, choices := m.seen()
	if got := len(after) - len(before); got != 1 || !isNone(choices[len(choices)-1]) {
		t.Fatalf("缩了预算的续跑应调 1 次模型、那一次是软降落轮（tool_choice=none），实际调了 %d 次，每次的 tool_choice %v（err %v）",
			got, choiceNames(choices[len(before):]), rerr)
	}
	if fm := agent_v2.FinalMessage(events); fm == nil || fm.Content != "收尾答复" {
		t.Errorf("软降落轮的收尾答复应在 events 里，实际 %v", fm)
	}
	var mie *compose_v2.MaxIterationsError
	if !errors.As(rerr, &mie) {
		t.Errorf("跑到软降落轮，续跑仍应报 MaxIterationsError，实际 %v", rerr)
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

// structuredLandingModel 手上有 noop 就一直调它；软降落轮只剩 final_output 时，同一条答复里硬调 noop（那一轮没绑它）、
// 再经 final_output 交合法答复。
type structuredLandingModel struct {
	mu     sync.Mutex
	bound  []string
	rounds [][]string
}

func (m *structuredLandingModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	names := m.bound
	m.bound = nil
	m.rounds = append(m.rounds, names)
	m.mu.Unlock()
	if slices.Contains(names, "noop") {
		return callMsg(tcall("tc", "noop", "{}")), nil
	}
	return callMsg(tcall("hard", "noop", "{}"), tcall("fo", agent_v2.FinalOutputToolName, `{"answer":"结构化"}`)), nil
}

func (m *structuredLandingModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *structuredLandingModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	names := make([]string, 0, len(tools))
	for _, t := range tools {
		if t != nil {
			names = append(names, t.Name)
		}
	}
	m.mu.Lock()
	m.bound = names
	m.mu.Unlock()
	return m, nil
}

// TestSoftLanding_StructuredOutputFinalRoundDelivers：配了结构化输出时，软降落轮经 final_output 交的答复照常生效——
// 那一轮模型硬调的工具不执行，放行的只有框架那件 final_output（结构化答复的唯一投递通道）。同一条答复里硬调的 noop
// 不执行。
//
// 承重的量有两个：**最终答复是不是 final_output 校验过的 JSON**（放行坏了的话 final_output 也被拦下、没有答复）；
// **noop 执行了几次**（闸坏了的话软降落轮那次 noop 照样执行）。
func TestSoftLanding_StructuredOutputFinalRoundDelivers(t *testing.T) {
	var n atomic.Int32
	m := &structuredLandingModel{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("so"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(kitOf(namedBizTool("noop", &n, "ok"))), agent_v2.WithMaxRounds(2),
		agent_v2.WithOutputSchema("A", answerSchema()))

	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("干活"))

	m.mu.Lock()
	rounds := slices.Clone(m.rounds)
	m.mu.Unlock()
	if len(rounds) != 3 || !slices.Equal(rounds[2], []string{agent_v2.FinalOutputToolName}) {
		t.Fatalf("fixture 没搭对：应调模型 3 次、最后一轮只绑着 final_output，实际每轮的工具 %v", rounds)
	}
	if got := n.Load(); got != 2 {
		t.Errorf("软降落轮硬调的 noop 不该执行：应只在前两个工具轮各执行 1 次，实际执行 %d 次", got)
	}
	if fm := agent_v2.FinalMessage(events); fm == nil || fm.Content != `{"answer":"结构化"}` {
		t.Errorf("软降落轮经 final_output 交的答复应照常生效，实际 %+v（err %v）", fm, err)
	}
	var mie *compose_v2.MaxIterationsError
	if !errors.As(err, &mie) {
		t.Errorf("跑到了软降落轮，仍应报 MaxIterationsError，实际 %v", err)
	}
}

// interruptOnceModel 第一次拿到工具时抛中断，之后跟 landingModel 一样：一直调 noop，收到 tool_choice=none 时以文本收尾。
type interruptOnceModel struct {
	mu    sync.Mutex
	fired bool
}

func (m *interruptOnceModel) Generate(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage("收尾答复"), nil
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
	b.p.mu.Unlock()
	if first {
		return nil, &compose_v2.InterruptError{Reason: "hitl", Query: &compose_v2.AskQuery{
			InterruptID: "q-sl", Question: "继续吗"}}
	}
	if isNone(model.Apply(opts...).ToolChoice) {
		return schema.AssistantMessage("收尾答复"), nil
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

// TestSoftLanding_BusinessFinalOutputWithoutSchemaNotExempt：没配 WithOutputSchema 时 final_output 这个名字是业务工具，
// 软降落轮跟别的业务工具同等对待——照旧绑着、tool_choice=none，模型硬调了也不执行。放行的只有框架那件（结构化输出的
// 投递通道），按实例认、不按名字：按名字放行的话业务那件在最后一轮照样执行。摘工具那会儿踩过同一个坑：按名字留，
// 业务那件在最后一轮照样绑着，模型调了它就跑满轮次、报原始的超限错误、拿不到收尾答复（审计实测）。
//
// 承重的量是业务那件 final_output 在软降落轮执行了没有。
func TestSoftLanding_BusinessFinalOutputWithoutSchemaNotExempt(t *testing.T) {
	var noop, fo atomic.Int32
	m := &landingModel{hardCall: agent_v2.FinalOutputToolName}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("sl"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(kitOf(namedBizTool("noop", &noop, "ok"), namedBizTool(agent_v2.FinalOutputToolName, &fo, "业务答复"))),
		agent_v2.WithMaxRounds(2))
	_, _ = ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("干活"))

	rounds, choices := m.seen()
	if len(rounds) != 3 {
		t.Fatalf("fixture 没搭对：maxRounds=2 应调模型 3 次，实际 %d 次", len(rounds))
	}
	if !slices.Contains(rounds[2], agent_v2.FinalOutputToolName) || !isNone(choices[2]) {
		t.Errorf("没配结构化输出时业务那件 final_output 跟别的业务工具一样照旧绑着、tool_choice=none，"+
			"实际那轮工具=%v tool_choice=%v", rounds[2], choiceNames(choices[2:]))
	}
	if got := fo.Load(); got != 0 {
		t.Errorf("软降落轮硬调的业务 final_output 不该执行（它不是框架那件），实际执行 %d 次", got)
	}
}

// TestSoftLanding_ForcedToolChoiceOverriddenToNone：业务经 WithRunModelOptions 强制了 tool_choice=required——软降落轮
// 工具照旧绑着、tool_choice 换成 none（排在业务那份之后、盖掉它）。照发 required 的话模型被逼着调工具，而这一轮调的
// 工具不执行，收尾机会白白浪费。之前的轮次照常：第 1 轮 required，调过工具之后复位（见 WithRunModelOptions）。
func TestSoftLanding_ForcedToolChoiceOverriddenToNone(t *testing.T) {
	var n atomic.Int32
	m := &landingModel{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("sl"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(kitOf(namedBizTool("noop", &n, "ok"))), agent_v2.WithMaxRounds(2))
	_, _ = ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("干活"),
		agent_v2.WithRunModelOptions(model.WithToolChoice(schema.ToolChoiceRequired)))

	rounds, choices := m.seen()
	if len(rounds) != 3 {
		t.Fatalf("fixture 没搭对：maxRounds=2 应调模型 3 次，实际 %d 次", len(rounds))
	}
	if !isRequired(choices[0]) || choices[1] != nil || !isNone(choices[2]) {
		t.Errorf("每轮的 tool_choice 应为 [required nil none]（软降落轮盖掉业务强制的那份），实际 %v", choiceNames(choices))
	}
	if len(rounds[2]) == 0 || !slices.Equal(rounds[2], rounds[1]) {
		t.Errorf("软降落轮的工具集应跟前一轮相同，实际前一轮 %v、最后一轮 %v", rounds[1], rounds[2])
	}
}

// TestSoftLanding_NoToolsBoundSendsNoToolChoice：软降落轮 toolkit 这条路一件工具都没绑上（这里是 ToolFilter 在拿到工具
// 结果之后把业务工具滤光）时不设 none——没有工具可关，而有的 provider 对没有 tools 的 tool_choice 直接拒绝。
func TestSoftLanding_NoToolsBoundSendsNoToolChoice(t *testing.T) {
	var n atomic.Int32
	m := &landingModel{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("sl"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(kitOf(namedBizTool("noop", &n, "ok"))), agent_v2.WithMaxRounds(1),
		agent_v2.WithToolFilter(func(_ context.Context, in *schema.Message) []string {
			if in != nil && in.Role == schema.RoleTool {
				return []string{}
			}
			return nil
		}))
	_, _ = ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("干活"))

	rounds, choices := m.seen()
	if len(rounds) != 2 || len(rounds[0]) == 0 || len(rounds[1]) != 0 {
		t.Fatalf("fixture 没搭对：应调模型 2 次、第 1 轮绑着 noop、第 2 轮（软降落轮）一件工具都没有，实际每轮的工具 %v", rounds)
	}
	if choices[0] != nil || choices[1] != nil {
		t.Errorf("没绑上工具的软降落轮不该带 tool_choice，实际每轮 %v", choiceNames(choices))
	}
}

// TestSoftLanding_OpenAIWireKeepsToolsSendsNone：端到端走真的 openai client——软降落轮发出去的请求里工具跟前一轮
// 同一套、tool_choice 是 "none"，之前的轮次不带 tool_choice。mock 模型读的是框架交给它的选项；这条读的是线上的
// 请求体（client 自己还要按「请求里有没有工具」决定发不发 tool_choice）。
func TestSoftLanding_OpenAIWireKeepsToolsSendsNone(t *testing.T) {
	type wireReq struct {
		tools  []string
		choice any
	}
	var mu sync.Mutex
	var reqs []wireReq
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Tools []struct {
				Function struct {
					Name string `json:"name"`
				} `json:"function"`
			} `json:"tools"`
			ToolChoice any `json:"tool_choice"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("解析请求体: %v", err)
		}
		names := make([]string, 0, len(body.Tools))
		for _, tl := range body.Tools {
			names = append(names, tl.Function.Name)
		}
		mu.Lock()
		reqs = append(reqs, wireReq{tools: names, choice: body.ToolChoice})
		mu.Unlock()
		msg, finish := `{"role":"assistant","content":null,"tool_calls":[{"id":"c","type":"function",`+
			`"function":{"name":"noop","arguments":"{}"}}]}`, "tool_calls"
		if body.ToolChoice == "none" {
			msg, finish = `{"role":"assistant","content":"收尾答复"}`, "stop"
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"x","object":"chat.completion","created":0,"model":"m","choices":[{"index":0,`+
			`"message":`+msg+`,"finish_reason":"`+finish+`"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
	}))
	defer srv.Close()

	var n atomic.Int32
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("wire"),
		agent_v2.WithModel(openai.NewClient("k", "m", openai.WithBaseURL(srv.URL), openai.WithStreamUnsupported())),
		agent_v2.WithToolkit(kitOf(namedBizTool("noop", &n, "ok"))), agent_v2.WithMaxRounds(2))
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("干活"))

	mu.Lock()
	got := slices.Clone(reqs)
	mu.Unlock()
	if len(got) != 3 {
		t.Fatalf("fixture 没搭对：maxRounds=2 应发 3 次请求，实际 %d 次（err %v）", len(got), err)
	}
	if len(got[2].tools) == 0 || !slices.Equal(got[2].tools, got[1].tools) || got[2].choice != "none" {
		t.Errorf("软降落轮的请求应带着跟前一轮同一套工具、tool_choice 为 \"none\"，实际前一轮工具 %v、最后一轮工具 %v tool_choice %v",
			got[1].tools, got[2].tools, got[2].choice)
	}
	if got[0].choice != nil || got[1].choice != nil {
		t.Errorf("之前的轮次不该带 tool_choice，实际 %v / %v", got[0].choice, got[1].choice)
	}
	var mie *compose_v2.MaxIterationsError
	if fm := agent_v2.FinalMessage(events); fm == nil || fm.Content != "收尾答复" || !errors.As(err, &mie) {
		t.Errorf("应以收尾答复 + MaxIterationsError 结束，实际答复 %+v err %v", fm, err)
	}
}

// TestSoftLanding_HardCalledFrameworkToolNotExecuted：软降落轮硬调**框架工具**时同样不执行——放行的只有
// 框架那件 final_output（结构化输出的唯一投递通道），按实例认。
//
// ⚠️ 这条钉的是 makeToolsFn 那道闸用 isFrameworkFinalOutput 而**不是** isFrameworkMintedIn。两者就差一个词、
// 在相邻两行、形状相同，而注释反复讲「判的是实例不是名字」，极易被误改成后者。实测过：换成
// isFrameworkMintedIn，`go test ./agent_v2/` 全量照样全绿——既有用例一条都钉不住它，因为
// TestSoftLanding_BusinessFinalOutputWithoutSchemaNotExempt 验的是**业务**工具（本来就不是 minted，两种判据下
// 都被拦）。
//
// 断言选在「ask_user 有没有真执行」而不是「有没有报错」：ask_user 真执行的表现就是抛中断，于是「Run 没中断」
// 恰好是「它没执行」的可观测量（CLAUDE.md：验泄漏要断言工具有没有真执行，不是断言有没有中断——这里两者同一）。
// 误放行的后果是把用户停在一个注定答不成的问题上：答完 Resume 只换来预算耗尽 + FinalMessage 为 nil。
func TestSoftLanding_HardCalledFrameworkToolNotExecuted(t *testing.T) {
	var noop atomic.Int32
	m := &landingModel{hardCall: agent_v2.AskUserToolName}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("slf"), agent_v2.WithModel(m), agent_v2.WithAskUser(),
		agent_v2.WithToolkit(kitOf(namedBizTool("noop", &noop, "ok"))),
		agent_v2.WithMaxRounds(1))
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("干活"))

	rounds, choices := m.seen()
	if len(rounds) != 2 {
		t.Fatalf("fixture 没搭对：maxRounds=1 应调模型 2 次，实际 %d 次", len(rounds))
	}
	if !slices.Contains(rounds[1], agent_v2.AskUserToolName) || !isNone(choices[1]) {
		t.Fatalf("fixture 没搭对：软降落轮应照旧绑着 ask_user 并发 tool_choice=none，"+
			"实际那轮工具=%v tool_choice=%v", rounds[1], choiceNames(choices[1:]))
	}
	if ie, ok := compose_v2.IsInterrupt(err); ok {
		t.Errorf("软降落轮硬调的 ask_user 被执行了——Run 停在提问 %q 上，而这个问题答完也只换来预算耗尽",
			ie.Query.Question)
	}
	var mie *compose_v2.MaxIterationsError
	if !errors.As(err, &mie) {
		t.Errorf("软降落轮硬调工具、整批被拦后应以预算耗尽收尾，got err=%v", err)
	}
}

// TestSoftLanding_HardToolCallErrorMentionsSoftLanding：provider 不守 tool_choice=none 时，报出去的错误
// 正文要说清**发生了什么**——不能是裸的引擎消息。
//
// 这条路上图是失败的（整批被拦 ⟹ 条件边判「末条有 tool_call」回 model 节点 ⟹ 撞 maxVisit），所以 Run 出口
// 那段友好正文的旧判据 `err == nil` 永不成立，调用方拿到的是 `node "model" visited N times (max M)`——
// 一个字没提软降落，排查方向指不出去（审计实测）。而这条路上 postamble 没跑完，`FinalMessage` 可能是 nil，
// 所以文案也不能照抄成功那条的「答复在 events 里」。
//
// ⚠️ 断言挑的是正文里那几个**只有这条路才会出现**的词（软降落 / round_budget_exhausted / FinalMessage
// 可能为 nil），不是「正文非空」——后者在裸引擎消息下也成立，不承重。
func TestSoftLanding_HardToolCallErrorMentionsSoftLanding(t *testing.T) {
	var n atomic.Int32
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("sl"), agent_v2.WithModel(&landingModel{hardCall: "noop"}),
		agent_v2.WithToolkit(kitOf(namedBizTool("noop", &n, "ok"))), agent_v2.WithMaxRounds(2))
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("干活"))
	if err == nil {
		t.Fatal("fixture 没搭对：软降落轮硬调工具、整批被拦，应以错误收尾")
	}
	for _, want := range []string{"软降落轮", "FinalMessage 可能为 nil", "跑满 3 轮"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("错误正文应点出 %q，实际正文：%v", want, err)
		}
	}
	// 「跑满 N 轮」必须是**本 agent 自己的**轮数：正文的其余部分讲的是本 agent 的预算，数字跟着别人跑
	// 就是把假归属写进了头条（见 TestSoftLanding_BudgetMessageDoesNotMisattribute）。
	var mie *compose_v2.MaxIterationsError
	if !errors.As(err, &mie) {
		t.Errorf("仍应能 errors.As 出 MaxIterationsError（调用方据它判预算耗尽），实际 %v", err)
	}
}

// hookErrAtNth：第 n 次 OnToolStart 返回一个包着「别人的」MaxIterationsError 的错误——模拟业务钩子里
// 按 RegrantFromNode 范式调起的校验子 agent 自己预算耗尽。
type hookErrAtNth struct {
	hook.NoOpHooks
	nth   int32
	extra error // 非 nil 时与 MaxIterationsError 一起 Join（验「中断并存」那一档）
	seen  atomic.Int32
}

func (h *hookErrAtNth) OnToolStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ToolInput) (context.Context, error) {
	if h.seen.Add(1) != h.nth {
		return ctx, nil
	}
	mie := &compose_v2.MaxIterationsError{Iterations: 99} // 99 便于辨认归属
	if h.extra != nil {
		return ctx, errors.Join(h.extra, mie)
	}
	return ctx, fmt.Errorf("校验子 agent 失败: %w", mie)
}

// TestSoftLanding_BudgetMessageDoesNotMisattribute：预算耗尽的正文**只陈述本 agent 能确定的事**，不把
// 别人的 MaxIterationsError 说成「我的软降落轮模型硬调了工具」。
//
// 可达窗口（审计实测）：软降落轮的 tools 节点里业务 OnToolStart 对被拦下的那一批**照样 fire**，钩子返错
// 让节点失败，而那个错里可以包着钩子自己调起的子 agent 的 MaxIterationsError。此刻 round 恰好是 mr+1，
// 于是出口判据 `errors.As(err, &mie)` 成立 —— 但它问的是「链上任何位置有没有」，不足以支撑那句断言。
//
// ⚠️ 这条钉的是「正文不做假陈述」+「数字是自己的」两件事。断言挑的是**会随这个 bug 变**的量：
// 正文里不许出现「仍硬调了工具」这种对本 agent 的断言式陈述，而「跑满 N 轮」必须是本 agent 的 3 而不是
// 钩子里那个 99。af7c45a 的版本实测报出 `…模型仍硬调了工具…: 达到最大迭代：99`。
func TestSoftLanding_BudgetMessageDoesNotMisattribute(t *testing.T) {
	var n atomic.Int32
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"), agent_v2.WithModel(&landingModel{hardCall: "noop"}),
		agent_v2.WithHooks(&hookErrAtNth{nth: 3}), // maxRounds=2 ⟹ tools 跑 3 次，第 3 次是软降落轮
		agent_v2.WithToolkit(kitOf(namedBizTool("noop", &n, "ok"))),
		agent_v2.WithMaxRounds(2))
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("干活"))
	if err == nil {
		t.Fatal("fixture 没搭对：钩子在软降落轮返错，应以错误收尾")
	}
	msg := err.Error()
	if strings.Contains(msg, "仍硬调了工具") {
		t.Errorf("正文对本 agent 做了谓词不支撑的断言（真因是钩子里的子 agent 预算耗尽）：%v", err)
	}
	if !strings.Contains(msg, "跑满 3 轮") {
		t.Errorf("正文里的轮数应是本 agent 自己的 3，实际：%v", err)
	}
	if !strings.Contains(msg, "校验子 agent 失败") {
		t.Errorf("真正的错误应保留在链上可读，实际：%v", err)
	}
}

// TestSoftLanding_BudgetMessageSkipsInterrupt：错误链上同时带中断时**整个不改写**。
//
// a2a/server 与 agui 按 IsInterrupt 走中断分支、把问题呈现给用户请他作答，而同一个错误的头条说「预算
// 耗尽、FinalMessage 可能为 nil」是自相矛盾的。改写只放宽、不收窄——中断原样穿过。
func TestSoftLanding_BudgetMessageSkipsInterrupt(t *testing.T) {
	var n atomic.Int32
	ie := &compose_v2.InterruptError{Reason: "需要人工确认", Query: &compose_v2.AskQuery{
		InterruptID: "probe-id", Question: "批准吗",
	}}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("p"), agent_v2.WithModel(&landingModel{hardCall: "noop"}),
		agent_v2.WithHooks(&hookErrAtNth{nth: 3, extra: ie}),
		agent_v2.WithToolkit(kitOf(namedBizTool("noop", &n, "ok"))),
		agent_v2.WithMaxRounds(2))
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("干活"))
	got, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("fixture 没搭对：钩子返的错里带中断，IsInterrupt 应为真，err=%v", err)
	}
	if strings.Contains(err.Error(), "轮次预算耗尽") {
		t.Errorf("前端会按中断请用户回答 %q，正文却说预算耗尽、FinalMessage 可能为 nil：%v", got.Query.Question, err)
	}
}
