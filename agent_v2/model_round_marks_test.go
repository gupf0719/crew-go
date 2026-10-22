package agent_v2_test

// model 节点每轮给本轮打的两个标记（软降落轮、这个任务里调过工具）只属于这一轮——这一轮的模型回调
// （OnModelEnd、ToolFilter、ChatModel 本身）里调起的子 agent 不该继承。曾经只在为真时才推，子 agent 的
// model 节点为假时不推，于是沿用了父的真：父在软降落轮调起的子 agent 第 0 轮就被摘光工具（软降落那时摘工具，
// 现在是关掉工具调用），父调过工具之后调起的子 agent 自己要求的 required 在第 0 轮就被复位（审计实测，均
// err == nil）。软降落轮的标记 tools 节点也推（它据此不执行那一轮硬调的工具），同样每轮都推。

import (
	"context"
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

// firstCallProbe 记下第一次调用时绑到的工具与收到的 tool_choice，以文本答复。
type firstCallProbe struct {
	mu     sync.Mutex
	bound  []string
	calls  int
	first  []string
	choice *schema.ToolChoice
}

func (p *firstCallProbe) Generate(_ context.Context, _ []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.calls == 0 {
		p.first = append([]string(nil), p.bound...)
		p.choice = model.Apply(opts...).ToolChoice
	}
	p.calls++
	return schema.AssistantMessage("子答"), nil
}

func (p *firstCallProbe) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (p *firstCallProbe) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	names := make([]string, 0, len(tools))
	for _, t := range tools {
		names = append(names, t.Name)
	}
	p.mu.Lock()
	p.bound = names
	p.mu.Unlock()
	return p, nil
}

// spawnInModelEnd：父 agent 第 atRound 轮的 OnModelEnd 里经 RunSubAgent 调起子 agent。
type spawnInModelEnd struct {
	hook.NoOpHooks
	atRound int32
	child   agent_v2.Agent
	opts    []agent_v2.RunOption
	ran     atomic.Bool
	err     error
}

func (h *spawnInModelEnd) OnModelEnd(ctx context.Context, info *hook.AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
	if info != nil && info.Round == h.atRound && h.ran.CompareAndSwap(false, true) {
		_, h.err = agent_v2.RunSubAgent(ctx, "child", h.child, schema.UserMessage("子任务"), h.opts...)
	}
	return ctx, nil
}

func noopKit() *tool.Toolkit {
	var n atomic.Int32
	return kitOf(namedBizTool("noop", &n, "ok"))
}

// runParentSpawningChild 跑父 agent（第 0 轮调 noop、第 1 轮文本收尾），在它第 1 轮的 OnModelEnd 里调起子 agent。
func runParentSpawningChild(t *testing.T, parentMaxRounds int, childOpts ...agent_v2.RunOption) *firstCallProbe {
	t.Helper()
	probe := &firstCallProbe{}
	child := agent_v2.NewBasicAgent(agent_v2.WithName("child"), agent_v2.WithModel(probe), agent_v2.WithToolkit(noopKit()))
	h := &spawnInModelEnd{atRound: 1, child: child, opts: childOpts}
	parent := agent_v2.NewBasicAgent(agent_v2.WithName("parent"), agent_v2.WithHooks(h),
		agent_v2.WithModel(&gateModel{script: []*schema.Message{callMsg(tcall("c1", "noop", "{}"))}}),
		agent_v2.WithToolkit(noopKit()), agent_v2.WithMaxRounds(parentMaxRounds))
	_, err := parent.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if !h.ran.Load() || h.err != nil {
		t.Fatalf("fixture 没搭对：父第 1 轮的 OnModelEnd 应调起子 agent，ran=%v childErr=%v parentErr=%v", h.ran.Load(), h.err, err)
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if probe.calls == 0 {
		t.Fatal("fixture 没搭对：子 agent 的模型一次都没被调")
	}
	return probe
}

// TestModelRoundMarks_ToolsUsedNotInheritedByChild：父已经调过工具（第 1 轮），它的模型回调里调起的子 agent 自己
// 要求 tool_choice=required——子 agent 第 0 轮应照常带着 required。
func TestModelRoundMarks_ToolsUsedNotInheritedByChild(t *testing.T) {
	probe := runParentSpawningChild(t, 5, agent_v2.WithRunModelOptions(model.WithToolChoice(schema.ToolChoiceRequired)))
	if probe.choice == nil || *probe.choice != schema.ToolChoiceRequired {
		t.Errorf("子 agent 第 0 轮应带着自己要求的 required，实际 %v", probe.choice)
	}
}

// TestModelRoundMarks_FinalRoundNotInheritedByChild：父在软降落轮（maxRounds=1 的第 1 轮）的模型回调里调起子 agent
// ——子 agent 第 0 轮应照常拿到自己的工具、不带 tool_choice。承重的是 tool_choice：软降落轮的工具集照旧，继承了父的
// 标记的子 agent 照样拿得到工具，区别只在那一轮被关掉了工具调用（none）。
func TestModelRoundMarks_FinalRoundNotInheritedByChild(t *testing.T) {
	probe := runParentSpawningChild(t, 1)
	if len(probe.first) == 0 || probe.choice != nil {
		t.Errorf("子 agent 第 0 轮不是软降落轮，应拿到自己的工具、不带 tool_choice，实际工具 %v tool_choice %v",
			probe.first, choiceNames([]*schema.ToolChoice{probe.choice}))
	}
}

// TestModelRoundMarks_FinalRoundNotInheritedByChildTools：父在软降落轮的模型回调里调起的子 agent，自己调的工具照常
// 执行。子 agent 的 tools 节点也要推自己的值（为假也推）：只在为真时推的话它沿用父的真，子 agent 调的每个工具都被当成
// 软降落轮硬调的、一个不执行。
func TestModelRoundMarks_FinalRoundNotInheritedByChildTools(t *testing.T) {
	var childNoop atomic.Int32
	child := agent_v2.NewBasicAgent(agent_v2.WithName("child"),
		agent_v2.WithModel(&gateModel{script: []*schema.Message{callMsg(tcall("cc", "noop", "{}"))}}),
		agent_v2.WithToolkit(kitOf(namedBizTool("noop", &childNoop, "ok"))))
	h := &spawnInModelEnd{atRound: 1, child: child}
	parent := agent_v2.NewBasicAgent(agent_v2.WithName("parent"), agent_v2.WithHooks(h),
		agent_v2.WithModel(&gateModel{script: []*schema.Message{callMsg(tcall("c1", "noop", "{}"))}}),
		agent_v2.WithToolkit(noopKit()), agent_v2.WithMaxRounds(1))
	_, _ = parent.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if !h.ran.Load() || h.err != nil {
		t.Fatalf("fixture 没搭对：父软降落轮的 OnModelEnd 应调起子 agent，ran=%v childErr=%v", h.ran.Load(), h.err)
	}
	if got := childNoop.Load(); got != 1 {
		t.Errorf("子 agent 第 0 轮调的 noop 应照常执行 1 次（子 agent 不在软降落轮），实际执行 %d 次", got)
	}
}

// TestModelRoundMarks_RevealedNotInheritedByChild：父经 tool_search 解锁了 lookup，在之后一轮的模型回调里调起的
// 子 agent 也有一件延迟的 lookup——子 agent 没检索过，第 0 轮不该看到它（渐进披露按 agent 各算各的）。曾经
// model 节点只在本 agent 有解锁记录时才推，子 agent 沿用了父的，没检索就拿到了 lookup。
func TestModelRoundMarks_RevealedNotInheritedByChild(t *testing.T) {
	var n atomic.Int32
	deferLookup := &agent_v2.ToolSearchConfig{Enabled: true, Deferred: func(tl tool.Tool) bool { return tl.Name() == "lookup" }}
	probe := &firstCallProbe{}
	child := agent_v2.NewBasicAgent(agent_v2.WithName("child"), agent_v2.WithModel(probe),
		agent_v2.WithToolkit(kitOf(namedBizTool("lookup", &n, "child"))), agent_v2.WithToolSearch(deferLookup))
	h := &spawnInModelEnd{atRound: 1, child: child}
	pm := &gateModel{script: []*schema.Message{callMsg(tcall("s1", "tool_search", `{"query":"lookup"}`))}}
	parent := agent_v2.NewBasicAgent(agent_v2.WithName("parent"), agent_v2.WithHooks(h), agent_v2.WithModel(pm),
		agent_v2.WithToolkit(kitOf(namedBizTool("lookup", &n, "parent"))), agent_v2.WithToolSearch(deferLookup))
	if _, err := parent.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !h.ran.Load() || h.err != nil {
		t.Fatalf("fixture 没搭对：ran=%v childErr=%v", h.ran.Load(), h.err)
	}
	if bound, _ := pm.round(t, 1); !slices.Contains(bound, "lookup") {
		t.Fatalf("fixture 没搭对：父第 2 轮应已解锁 lookup，实际 %v", bound)
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if slices.Contains(probe.first, "lookup") {
		t.Errorf("子 agent 没检索过，第 0 轮不该看到延迟的 lookup，实际 %v", probe.first)
	}
}
