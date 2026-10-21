package agent_v2_test

// 任务级状态只活在任务自己的状态里（BasicAgent 图的 channel、tools 节点这次执行）：续跑带不带原 session 都一样、
// 中断的那一批不重复计、同一批里刚解锁的工具不放行。

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// seqRecordingModel 按顺序给出回复，记下每一轮收到的输入。
type seqRecordingModel struct {
	mu      sync.Mutex
	replies []*schema.Message
	inputs  [][]*schema.Message
}

func (m *seqRecordingModel) Generate(_ context.Context, in []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	i := len(m.inputs)
	m.inputs = append(m.inputs, append([]*schema.Message(nil), in...))
	if i >= len(m.replies) {
		i = len(m.replies) - 1
	}
	r := *m.replies[i]
	return &r, nil
}

func (m *seqRecordingModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *seqRecordingModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// reminders 返回第 round 轮输入里的重复调用提醒正文。
func (m *seqRecordingModel) reminders(round int) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if round >= len(m.inputs) {
		return nil
	}
	var out []string
	for _, msg := range m.inputs[round] {
		if msg.Annotations[agent_v2.RepeatToolReminderAnnotation] != nil {
			out = append(out, msg.Content)
		}
	}
	return out
}

func (m *seqRecordingModel) rounds() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.inputs)
}

func batch(calls ...[3]string) *schema.Message {
	tcs := make([]schema.ToolCall, 0, len(calls))
	for _, c := range calls {
		tcs = append(tcs, schema.ToolCall{ID: c[0], Type: "function", Function: schema.FunctionCall{Name: c[1], Arguments: c[2]}})
	}
	return schema.AssistantToolCallMessage(tcs)
}

func fooKit(calls *atomic.Int32) *tool.Toolkit {
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("foo", "foo", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			calls.Add(1)
			return tool.Success("ok"), nil
		}))
	return kit
}

// runRepeatWithAsk：第 1 轮调 foo，第 2 轮同一批调 foo 与 ask_user（中断），续跑后第 3 轮再调 foo，第 4 轮收尾。
// withSession 决定续跑带不带原 session。返回模型桩。
func runRepeatWithAsk(t *testing.T, thresholds []int, withSession bool) *seqRecordingModel {
	t.Helper()
	var calls atomic.Int32
	m := &seqRecordingModel{replies: []*schema.Message{
		batch([3]string{"c1", "foo", `{"x":1}`}),
		batch([3]string{"c2", "foo", `{"x":1}`}, [3]string{"q", agent_v2.AskUserToolName, `{"question":"继续吗"}`}),
		batch([3]string{"c3", "foo", `{"x":1}`}),
		schema.AssistantMessage("完成"),
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("rep_resume"), agent_v2.WithModel(m), agent_v2.WithToolkit(fooKit(&calls)),
		agent_v2.WithAskUser(), agent_v2.WithRepeatToolReminder(&agent_v2.RepeatToolConfig{Enabled: true, Thresholds: thresholds}),
		agent_v2.WithMaxRounds(6))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture 没搭对：第 2 轮应停在 ask_user 上，实际 %v", err)
	}
	opts := []agent_v2.ResumeOption{agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage("继续"),
	})}
	if withSession {
		opts = append(opts, agent_v2.WithResumeSession(sess))
	}
	if _, err := ag.Resume(ctx, sess.RunID, opts...); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if m.rounds() < 4 {
		t.Fatalf("fixture 没搭对：应跑到第 4 轮，实际 %d 轮", m.rounds())
	}
	return m
}

// TestRepeatTool_ChainSurvivesResumeWithoutSession：同一个任务里续跑不带原 session，计数照样接着算——第 3 次 foo 之后
// 收到提醒。曾经链挂在会话 KV 上、不进存档，换 session 续跑就从头计，第 3 次才算到第 2 次（审计实测）。
func TestRepeatTool_ChainSurvivesResumeWithoutSession(t *testing.T) {
	for _, withSession := range []bool{true, false} {
		m := runRepeatWithAsk(t, nil, withSession)
		if len(m.reminders(3)) == 0 {
			t.Errorf("withSession=%v：第 3 次同参数调用之后应收到提醒", withSession)
		}
	}
}

// TestRepeatTool_InterruptedBatchCountedOnce：一批中断、续跑重跑这一批，计数只算一次——第 3 次之后收到的是第一档
// 提醒，不是「连续 4 次」。曾经计数在这一批交出中断之前就记进了会话 KV，续跑重跑时同一批再算一遍。
func TestRepeatTool_InterruptedBatchCountedOnce(t *testing.T) {
	m := runRepeatWithAsk(t, []int{3, 4}, true)
	got := m.reminders(3)
	if len(got) == 0 {
		t.Fatal("第 3 次同参数调用之后应收到提醒")
	}
	for _, r := range got {
		if strings.Contains(r, "连续 4 次") {
			t.Errorf("中断的那一批被重复计数：收到的是 %q", r)
		}
	}
}

// TestToolSearch_SameBatchCallStaysLocked：同一批里检索并直调刚解锁的工具——执行闸只认这一批开始前已提交的
// 解锁集，那次直调一律被拒、函数体不执行，跑多少遍、批内怎么排都一样。刚解锁的工具从下一批起可调（下一轮 bind list
// 才有它，模型在这一批里本来就看不到它）。曾经执行闸读的是同一批里检索正在往里加的那份，放不放行看批内排列与调度：
// 实测 [lookup, tool_search] 50 次全放行、[tool_search, lookup, noop] 放行 40 次、[tool_search, lookup] 一次不放。
// 这条按排列跑多遍，旧实现在后两种排列下几乎必红；确定性的那条是白盒的 TestToolsGate_IgnoresSameBatchReveals。
func TestToolSearch_SameBatchCallStaysLocked(t *testing.T) {
	orders := map[string][][3]string{
		"[tool_search, lookup]":       {{"s", "tool_search", `{"query":"lookup"}`}, {"l", "lookup", `{}`}},
		"[lookup, tool_search]":       {{"l", "lookup", `{}`}, {"s", "tool_search", `{"query":"lookup"}`}},
		"[tool_search, lookup, noop]": {{"s", "tool_search", `{"query":"lookup"}`}, {"l", "lookup", `{}`}, {"n", "noop", `{}`}},
	}
	for name, calls := range orders {
		for i := 0; i < 30; i++ {
			var lookups atomic.Int32
			kit := tool.NewToolkit()
			kit.Register(tool.NewFuncTool("lookup", "查资料", map[string]any{"type": "object"},
				func(context.Context, map[string]any) (*tool.Result, error) {
					lookups.Add(1)
					return tool.Success("资料"), nil
				}))
			kit.Register(tool.NewFuncTool("noop", "noop", map[string]any{"type": "object"},
				func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("ok"), nil }))
			m := &seqRecordingModel{replies: []*schema.Message{
				batch(calls...),
				batch([3]string{"l2", "lookup", `{}`}),
				schema.AssistantMessage("完成"),
			}}
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("same_batch"), agent_v2.WithModel(m), agent_v2.WithToolkit(kit),
				agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true,
					Deferred: func(tl tool.Tool) bool { return tl.Name() == "lookup" }}),
				agent_v2.WithMaxRounds(5))
			if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("查")); err != nil {
				t.Fatalf("%s 第 %d 遍 Run: %v", name, i, err)
			}
			if n := lookups.Load(); n != 1 {
				t.Fatalf("%s 第 %d 遍：同一批的直调应被拒、下一批的调用放行，lookup 应执行 1 次，实际 %d 次", name, i, n)
			}
		}
	}
}

// boundRetryModel 按脚本回复（某一步可以报错），记下每次调用时绑定的工具与输入里的重复调用提醒条数。
type boundRetryModel struct {
	mu        sync.Mutex
	script    []*schema.Message // nil = 这一步报错
	n         int
	bound     []string
	boundLog  [][]string
	reminders []int
}

func (m *boundRetryModel) Generate(_ context.Context, in []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	i := m.n
	m.n++
	m.boundLog = append(m.boundLog, append([]string(nil), m.bound...))
	r := 0
	for _, msg := range in {
		if msg.Annotations[agent_v2.RepeatToolReminderAnnotation] != nil {
			r++
		}
	}
	m.reminders = append(m.reminders, r)
	if i >= len(m.script) {
		return schema.AssistantMessage("完成"), nil
	}
	if m.script[i] == nil {
		return nil, errors.New("模型报错")
	}
	out := *m.script[i]
	return &out, nil
}

func (m *boundRetryModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *boundRetryModel) WithTools(infos []*schema.ToolInfo) (model.ChatModel, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bound = m.bound[:0]
	for _, ti := range infos {
		m.bound = append(m.bound, ti.Name)
	}
	return m, nil
}

// TestToolSearch_RetryWithoutCheckpointerStartsEmpty：编排配了重试、没配 checkpointer 时，成员的第二次尝试是从头跑的
// 一张新图——解锁集与重复调用计数链都从空开始，失败那次尝试里检索解锁的工具、调过的次数都不带过来（配了 checkpointer
// 时第二次尝试从存档续上，照样认得）。曾经两样挂在会话 KV 上、按「任务 + 编排位置 + agent 名」分，同一个任务里两次
// 尝试共用一份：第二次尝试一开始就绑着 lookup、直调照样执行，提醒把被丢弃那次尝试里的调用也算进去（实测）。
func TestToolSearch_RetryWithoutCheckpointerStartsEmpty(t *testing.T) {
	var lookups, foos atomic.Int32
	kit := fooKit(&foos)
	kit.Register(tool.NewFuncTool("lookup", "查资料 lookup", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			lookups.Add(1)
			return tool.Success("资料"), nil
		}))
	m := &boundRetryModel{script: []*schema.Message{
		batch([3]string{"s", "tool_search", `{"query":"lookup"}`}), // 第一次尝试：检索解锁 lookup
		batch([3]string{"f1", "foo", `{"x":1}`}),
		batch([3]string{"f2", "foo", `{"x":1}`}),
		nil,                                      // 第一次尝试以模型报错失败
		batch([3]string{"f3", "foo", `{"x":1}`}), // 第二次尝试第一轮
		batch([3]string{"l", "lookup", `{}`}),    // 没检索直接调 lookup
		schema.AssistantMessage("完成"),
	}}
	step := agent_v2.NewBasicAgent(agent_v2.WithName("s1"), agent_v2.WithModel(m), agent_v2.WithToolkit(kit),
		agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true,
			Deferred: func(tl tool.Tool) bool { return tl.Name() == "lookup" }}),
		agent_v2.WithRepeatToolReminder(&agent_v2.RepeatToolConfig{Enabled: true, Thresholds: []int{3}}),
		agent_v2.WithMaxRounds(6))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
		agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(1))).AddStep("s1", step)
	if _, err := seq.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("查")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(m.boundLog) < 7 || !slices.Contains(m.boundLog[1], "lookup") || foos.Load() != 3 {
		t.Fatalf("fixture 没搭对：第一次尝试应解锁 lookup、foo 共执行 3 次、模型被调 7 次；实际 bound=%v foo=%d", m.boundLog, foos.Load())
	}
	if slices.Contains(m.boundLog[4], "lookup") {
		t.Errorf("第二次尝试第一轮就绑着 lookup——失败那次尝试的解锁带过来了：%v", m.boundLog[4])
	}
	if n := lookups.Load(); n != 0 {
		t.Errorf("第二次尝试没检索就直调 lookup，应被执行闸拒掉，实际执行 %d 次", n)
	}
	for i := 4; i < len(m.reminders); i++ {
		if m.reminders[i] != 0 {
			t.Errorf("第二次尝试第 %d 次模型调用的输入里有提醒——失败那次尝试里的调用被算进了计数链", i)
		}
	}
}
