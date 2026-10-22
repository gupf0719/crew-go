package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

var bgIDRe = regexp.MustCompile(`id="(bg_[0-9a-f]+)"`)

const bgNoteTag = "[后台任务通知]"

// bgScriptModel 按 steps 逐次给出回复（超出时收尾），记下每次的输入。step 读输入做决定——比如从上一条
// tool_result 里抠出任务 id。
type bgScriptModel struct {
	mu     sync.Mutex
	steps  []func(in []*schema.Message) (*schema.Message, error)
	inputs [][]*schema.Message
}

func (m *bgScriptModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	i := len(m.inputs)
	m.inputs = append(m.inputs, append([]*schema.Message(nil), msgs...))
	var step func([]*schema.Message) (*schema.Message, error)
	if i < len(m.steps) {
		step = m.steps[i]
	}
	m.mu.Unlock()
	if step == nil {
		return schema.AssistantMessage("完成"), nil
	}
	return step(msgs)
}
func (m *bgScriptModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *bgScriptModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *bgScriptModel) input(i int) []*schema.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	if i >= len(m.inputs) {
		return nil
	}
	return m.inputs[i]
}

func (m *bgScriptModel) calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.inputs)
}

var bgCallSeq atomic.Int64

func callStep(name, args string) func([]*schema.Message) (*schema.Message, error) {
	return func([]*schema.Message) (*schema.Message, error) {
		id := fmt.Sprintf("c%d", bgCallSeq.Add(1))
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: id, Type: "function",
			Function: schema.FunctionCall{Name: name, Arguments: args}}}), nil
	}
}

// bgActionStep 从消息流里最近出现的任务 id 上调 background_task。
func bgActionStep(action, extra string) func([]*schema.Message) (*schema.Message, error) {
	return func(in []*schema.Message) (*schema.Message, error) {
		id := lastTaskID(in)
		args := fmt.Sprintf(`{"action":%q,"id":%q%s}`, action, id, extra)
		return callStep(agent_v2.BackgroundTaskToolName, args)(in)
	}
}

func sayStep(text string) func([]*schema.Message) (*schema.Message, error) {
	return func([]*schema.Message) (*schema.Message, error) { return schema.AssistantMessage(text), nil }
}

func lastTaskID(msgs []*schema.Message) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		if mm := bgIDRe.FindStringSubmatch(msgs[i].Content); mm != nil {
			return mm[1]
		}
	}
	return ""
}

// gatedTool 是一件慢工具：放行之前一直不返回；ctx 被取消时返回 ctx 的错。
type gatedTool struct {
	release   chan struct{}
	runs      atomic.Int32
	cancelled atomic.Bool
	output    string
	panicMsg  string
}

func newGatedTool(output string) *gatedTool {
	return &gatedTool{release: make(chan struct{}), output: output}
}

func (g *gatedTool) open() { close(g.release) }

func (g *gatedTool) register(tk *tool.Toolkit, name string) {
	tk.Register(tool.NewFuncTool(name, "慢工具", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			g.runs.Add(1)
			select {
			case <-g.release:
				if g.panicMsg != "" {
					panic(g.panicMsg)
				}
				return tool.Success(g.output), nil
			case <-ctx.Done():
				g.cancelled.Store(true)
				return nil, ctx.Err()
			}
		}))
}

func bgConfig(mgr *agent_v2.BackgroundTaskManager) *agent_v2.BackgroundTasksConfig {
	return &agent_v2.BackgroundTasksConfig{Manager: mgr, YieldAfter: 50 * time.Millisecond,
		Eligible: func(t tool.Tool) bool { return t.Name() == "build" }}
}

func bgAgent(name string, m model.ChatModel, tk *tool.Toolkit, cfg *agent_v2.BackgroundTasksConfig, extra ...agent_v2.Option) *agent_v2.BasicAgent {
	opts := []agent_v2.Option{agent_v2.WithName(name), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(8), agent_v2.WithBackgroundTasks(cfg)}
	return agent_v2.NewBasicAgent(append(opts, extra...)...)
}

// waitTerminal 等任务结束（最多 3 秒）。
func waitTerminal(t *testing.T, mgr *agent_v2.BackgroundTaskManager, id string) agent_v2.BackgroundTask {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if bt, ok := mgr.Get(id); ok && bt.Status != agent_v2.BackgroundTaskRunning {
			return bt
		}
		time.Sleep(5 * time.Millisecond)
	}
	bt, _ := mgr.Get(id)
	t.Fatalf("任务 %s 3 秒内没结束，状态 %q", id, bt.Status)
	return bt
}

func closeMgr(t *testing.T, mgr *agent_v2.BackgroundTaskManager) {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := mgr.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
}

// TestBackgroundTask_SlowToolYieldsThenNotifiesOnce：慢工具跑过 YieldAfter 就转入后台——这一轮拿到任务 id、
// Run 照常结束（工具还没返回）；工具跑完之后，下一次 Run 的第一次模型调用里有它的结果；再下一次不再重发。
//
// 断言选在模型实际看到的东西上：Run 1 第二次调用看到的 tool_result（同步跑时是工具的输出，而那时工具根本还没
// 返回 ⟹ Run 1 会一直卡着）；Run 2 第一次调用里通知的条数与内容；Run 3 第一次调用里通知的条数（开了会话历史：
// Run 2 的那条从历史读回来是 1 条，重发了就是 2 条）。
func TestBackgroundTask_SlowToolYieldsThenNotifiesOnce(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	g := newGatedTool("BUILD-OK 共 42 个测试通过")
	tk := tool.NewToolkit()
	g.register(tk, "build")
	tk.Register(tool.NewFuncTool("noop", "快工具", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("ok"), nil }))
	hs := agent_v2.NewInMemoryHistoryStore()
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-bg")

	m1 := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("build", `{}`), sayStep("已开始构建")}}
	done := make(chan error, 1)
	go func() {
		_, err := bgAgent("coder", m1, tk, bgConfig(mgr), agent_v2.WithHistory(hs, "t")).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("构建一下"))
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		g.open()
		t.Fatal("工具还没返回时 Run 1 一直没结束——慢工具没转入后台")
	}
	handle := m1.input(1)
	last := handle[len(handle)-1].Content
	id := lastTaskID(handle)
	if !strings.Contains(last, "已转入后台") || id == "" {
		t.Fatalf("Run 1 第二次调用看到的 tool_result 应是转入后台的说明 + 任务 id：\n%s", last)
	}

	g.open()
	if bt := waitTerminal(t, mgr, id); bt.Status != agent_v2.BackgroundTaskCompleted {
		t.Fatalf("任务应完成，状态 %q", bt.Status)
	}

	// Run 2 跑两轮（先调一个快工具再收尾）：通知在第一轮送进来、写进消息流，第二轮还在、且只有一份；会话历史里
	// 也只记一份——没写进消息流的话，第二轮会因为「流里没有」再送一次，历史里记成两份。
	m2 := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("noop", `{}`), sayStep("构建通过了")}}
	if _, err := bgAgent("coder", m2, tk, bgConfig(mgr), agent_v2.WithHistory(hs, "t")).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("好了吗")); err != nil {
		t.Fatal(err)
	}
	in2 := inputText(m2.input(0))
	if strings.Count(in2, bgNoteTag) != 1 || !strings.Contains(in2, "BUILD-OK 共 42 个测试通过") || !strings.Contains(in2, id) {
		t.Fatalf("Run 2 第一次调用里应有 1 条带结果的通知：\n%s", in2)
	}
	if n := strings.Count(inputText(m2.input(1)), bgNoteTag); n != 1 {
		t.Fatalf("Run 2 第二轮里有 %d 条通知，应是 1 条（第一轮送进来的那条留在消息流里）", n)
	}
	all, _ := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-bg"})
	recorded := 0
	for _, msg := range all {
		if v, _ := msg.Annotations[agent_v2.BackgroundTaskNotificationAnnotation].(string); v == id {
			recorded++
		}
	}
	if recorded != 1 {
		t.Fatalf("会话历史里这条通知记了 %d 份，应是 1 份", recorded)
	}

	m3 := &bgScriptModel{}
	if _, err := bgAgent("coder", m3, tk, bgConfig(mgr), agent_v2.WithHistory(hs, "t")).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("再说一遍")); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(inputText(m3.input(0)), bgNoteTag); n != 1 {
		t.Fatalf("Run 3 第一次调用里有 %d 条通知——历史里那 1 条之外又重发了", n)
	}
	if g.runs.Load() != 1 {
		t.Fatalf("工具被执行了 %d 次", g.runs.Load())
	}
}

// TestBackgroundTask_FastToolUnaffected：在 YieldAfter 之内返回的可后台工具跟没开这个功能一样——结果当场
// 交回，管理器里不留任务。
func TestBackgroundTask_FastToolUnaffected(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	g := newGatedTool("FAST-RESULT")
	g.open()
	tk := tool.NewToolkit()
	g.register(tk, "build")
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("build", `{}`)}}
	if _, err := bgAgent("coder", m, tk, bgConfig(mgr)).Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("构建")); err != nil {
		t.Fatal(err)
	}
	in := m.input(1)
	if got := in[len(in)-1].Content; got != "FAST-RESULT" {
		t.Fatalf("快工具的结果应原样交回，模型看到：\n%s", got)
	}
	if strings.Contains(inputText(in), "已转入后台") {
		t.Fatal("快工具被转入了后台")
	}
}

// TestBackgroundTask_IneligibleToolStaysSync：没点名的工具再慢也同步跑完。
func TestBackgroundTask_IneligibleToolStaysSync(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("deploy", "慢", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			time.Sleep(200 * time.Millisecond)
			return tool.Success("DEPLOYED"), nil
		}))
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("deploy", `{}`)}}
	if _, err := bgAgent("coder", m, tk, bgConfig(mgr)).Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("部署")); err != nil {
		t.Fatal(err)
	}
	in := m.input(1)
	if got := in[len(in)-1].Content; got != "DEPLOYED" {
		t.Fatalf("没点名的工具应同步跑完，模型看到：\n%s", got)
	}
}

// TestBackgroundTask_WaitDeliversResultNoLaterNotification：模型用 wait 等到了结果——结果在 background_task
// 的结果里；之后不再作为通知重发。
func TestBackgroundTask_WaitDeliversResultNoLaterNotification(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	g := newGatedTool("WAITED-RESULT")
	tk := tool.NewToolkit()
	g.register(tk, "build")
	hs := agent_v2.NewInMemoryHistoryStore()
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-wait")
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		callStep("build", `{}`),
		func(in []*schema.Message) (*schema.Message, error) {
			go func() { time.Sleep(100 * time.Millisecond); g.open() }()
			return bgActionStep("wait", `,"timeout_seconds":5`)(in)
		},
		sayStep("构建完成"),
	}}
	if _, err := bgAgent("coder", m, tk, bgConfig(mgr), agent_v2.WithHistory(hs, "t")).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("构建并等结果")); err != nil {
		t.Fatal(err)
	}
	in := m.input(2)
	if got := in[len(in)-1].Content; !strings.Contains(got, "WAITED-RESULT") || !strings.Contains(got, "已完成") {
		t.Fatalf("wait 应交回结果：\n%s", got)
	}
	m2 := &bgScriptModel{}
	if _, err := bgAgent("coder", m2, tk, bgConfig(mgr), agent_v2.WithHistory(hs, "t")).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("下一件")); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(inputText(m2.input(0)), bgNoteTag) {
		t.Fatal("模型已经经 wait 拿到结果，下一次 Run 又收到了通知")
	}
}

// TestBackgroundTask_CancelStopsTool：cancel 让工具的 ctx 被取消、任务记为已取消，模型看到的是「已取消」。
func TestBackgroundTask_CancelStopsTool(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	g := newGatedTool("NEVER")
	tk := tool.NewToolkit()
	g.register(tk, "build")
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		callStep("build", `{}`), bgActionStep("cancel", ""), sayStep("已取消"),
	}}
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-cancel")
	if _, err := bgAgent("coder", m, tk, bgConfig(mgr)).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("构建")); err != nil {
		t.Fatal(err)
	}
	id := lastTaskID(m.input(1))
	in := m.input(2)
	if got := in[len(in)-1].Content; !strings.Contains(got, "已取消") {
		t.Fatalf("cancel 的结果应说已取消：\n%s", got)
	}
	if bt, _ := mgr.Get(id); bt.Status != agent_v2.BackgroundTaskCancelled {
		t.Fatalf("任务状态应是 cancelled，实际 %q", bt.Status)
	}
	if !g.cancelled.Load() {
		t.Fatal("工具的 ctx 没被取消")
	}
}

// TestBackgroundTask_OtherConversationCannotTouch：别的会话拿着任务 id 查不到、也取消不了。id 会出现在模型
// 可见的文本与会话历史里，不能靠它保密。
func TestBackgroundTask_OtherConversationCannotTouch(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	g := newGatedTool("A-RESULT")
	tk := tool.NewToolkit()
	g.register(tk, "build")
	mA := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("build", `{}`), sayStep("开始了")}}
	if _, err := bgAgent("coder", mA, tk, bgConfig(mgr)).Run(compose_v2.WithThreadIDCtx(context.Background(), "conv-A"),
		agent_v2.NewRunSession(), schema.UserMessage("构建")); err != nil {
		t.Fatal(err)
	}
	id := lastTaskID(mA.input(1))
	if id == "" {
		t.Fatal("fixture 没搭对：会话 A 的调用没转入后台")
	}
	steal := func(in []*schema.Message) (*schema.Message, error) {
		return callStep(agent_v2.BackgroundTaskToolName, fmt.Sprintf(`{"action":"cancel","id":%q}`, id))(in)
	}
	mB := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){steal, sayStep("好")}}
	if _, err := bgAgent("coder", mB, tk, bgConfig(mgr)).Run(compose_v2.WithThreadIDCtx(context.Background(), "conv-B"),
		agent_v2.NewRunSession(), schema.UserMessage("取消")); err != nil {
		t.Fatal(err)
	}
	in := mB.input(1)
	if got := in[len(in)-1].Content; !strings.Contains(got, "not_found") {
		t.Fatalf("会话 B 应查不到会话 A 的任务：\n%s", got)
	}
	if bt, _ := mgr.Get(id); bt.Status != agent_v2.BackgroundTaskRunning || g.cancelled.Load() {
		t.Fatalf("会话 B 取消了会话 A 的任务（状态 %q）", bt.Status)
	}
	g.open()
	waitTerminal(t, mgr, id)
}

// TestBackgroundTask_NestedAgentRunsSync：编排成员不转后台——它这一步结束之后没有一个确定会再来的模型调用能接住结果。
func TestBackgroundTask_NestedAgentRunsSync(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("build", "慢", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			time.Sleep(200 * time.Millisecond)
			return tool.Success("NESTED-RESULT"), nil
		}))
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("build", `{}`)}}
	inner := bgAgent("inner", m, tk, bgConfig(mgr))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("step", inner)
	if _, err := seq.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("构建")); err != nil {
		t.Fatal(err)
	}
	in := m.input(1)
	if got := in[len(in)-1].Content; got != "NESTED-RESULT" {
		t.Fatalf("编排成员里的可后台工具应同步跑完，模型看到：\n%s", got)
	}
	for _, msg := range m.input(0) {
		for _, tc := range schema.GetToolCalls(msg) {
			if tc.Function.Name == agent_v2.BackgroundTaskToolName {
				t.Fatal("编排成员不该拿到 background_task")
			}
		}
	}
}

// TestBackgroundTask_ResumeReplaysHandleWithoutRerun：同一批里别的工具中断——转入后台的那次调用命中工具账，
// 续跑时不再执行，模型看到的还是同一个任务 id。
func TestBackgroundTask_ResumeReplaysHandleWithoutRerun(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	g := newGatedTool("RESUMED-RESULT")
	tk := tool.NewToolkit()
	g.register(tk, "build")
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		func([]*schema.Message) (*schema.Message, error) {
			return schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "b1", Type: "function", Function: schema.FunctionCall{Name: "build", Arguments: `{}`}},
				{ID: "a1", Type: "function", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"要发布吗"}`}},
			}), nil
		},
		sayStep("好的"),
	}}
	ag := bgAgent("coder", m, tk, bgConfig(mgr), agent_v2.WithAskUser())
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(compose_v2.WithThreadIDCtx(context.Background(), "conv-resume"), cp)
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("构建并问我"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应在 ask_user 中断，got %v", err)
	}
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("发布")})); err != nil {
		t.Fatal(err)
	}
	if n := g.runs.Load(); n != 1 {
		t.Fatalf("续跑时转入后台的工具又执行了（共 %d 次）", n)
	}
	in := m.input(1)
	id := ""
	for _, msg := range in {
		if v, _ := msg.Annotations[agent_v2.BackgroundTaskAnnotation].(string); v != "" {
			id = v
		}
	}
	if id == "" || lastTaskID(in) != id {
		t.Fatalf("续跑后模型应看到转入后台时的那个任务 id：\n%s", inputText(in))
	}
	if bt, ok := mgr.Get(id); !ok || bt.Status != agent_v2.BackgroundTaskRunning {
		t.Fatalf("续跑之后任务应还在跑，状态 %q", bt.Status)
	}
	g.open()
	waitTerminal(t, mgr, id)
}

// TestBackgroundTask_CloseCancelsRunning：Close 取消还在跑的任务、等工具返回。
func TestBackgroundTask_CloseCancelsRunning(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	g := newGatedTool("NEVER")
	tk := tool.NewToolkit()
	g.register(tk, "build")
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("build", `{}`), sayStep("开始了")}}
	if _, err := bgAgent("coder", m, tk, bgConfig(mgr)).Run(compose_v2.WithThreadIDCtx(context.Background(), "conv-close"),
		agent_v2.NewRunSession(), schema.UserMessage("构建")); err != nil {
		t.Fatal(err)
	}
	id := lastTaskID(m.input(1))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := mgr.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if !g.cancelled.Load() {
		t.Fatal("Close 之后工具的 ctx 没被取消")
	}
	if bt, _ := mgr.Get(id); bt.Status != agent_v2.BackgroundTaskCancelled {
		t.Fatalf("Close 之后任务应是 cancelled，实际 %q", bt.Status)
	}
}

// TestBackgroundTask_CapacityFallsBackToSync：会话里在跑的任务到了上限，再来的可后台调用不转后台、照旧同步等。
func TestBackgroundTask_CapacityFallsBackToSync(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(&agent_v2.BackgroundTaskManagerConfig{MaxRunningPerConversation: 1})
	closeMgr(t, mgr)
	g := newGatedTool("SAME")
	tk := tool.NewToolkit()
	g.register(tk, "build")
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		func([]*schema.Message) (*schema.Message, error) {
			go func() { time.Sleep(300 * time.Millisecond); g.open() }()
			return schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "x1", Type: "function", Function: schema.FunctionCall{Name: "build", Arguments: `{"n":1}`}},
				{ID: "x2", Type: "function", Function: schema.FunctionCall{Name: "build", Arguments: `{"n":2}`}},
			}), nil
		},
	}}
	if _, err := bgAgent("coder", m, tk, bgConfig(mgr)).Run(compose_v2.WithThreadIDCtx(context.Background(), "conv-cap"),
		agent_v2.NewRunSession(), schema.UserMessage("构建两个")); err != nil {
		t.Fatal(err)
	}
	handles, results := 0, 0
	for _, msg := range m.input(1) {
		if msg.Role != schema.RoleTool {
			continue
		}
		switch {
		case strings.Contains(msg.Content, "已转入后台"):
			handles++
		case msg.Content == "SAME":
			results++
		}
	}
	if handles != 1 || results != 1 {
		t.Fatalf("上限 1：应 1 个转入后台、1 个同步等到结果，实际 %d / %d", handles, results)
	}
}

// TestBackgroundTask_NotificationOnlyToStarter：通知只发给开任务的那个 agent——同一场会话里别的 agent 收不到，
// 开它的 agent 下一次跑时收到。
func TestBackgroundTask_NotificationOnlyToStarter(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	g := newGatedTool("A-DONE")
	tk := tool.NewToolkit()
	g.register(tk, "build")
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-two")
	mA := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("build", `{}`), sayStep("开始了")}}
	if _, err := bgAgent("a", mA, tk, bgConfig(mgr)).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("构建")); err != nil {
		t.Fatal(err)
	}
	id := lastTaskID(mA.input(1))
	g.open()
	waitTerminal(t, mgr, id)

	mB := &bgScriptModel{}
	if _, err := bgAgent("b", mB, tk, bgConfig(mgr)).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("你好")); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(inputText(mB.input(0)), bgNoteTag) {
		t.Fatal("agent b 收到了 agent a 开的任务的通知")
	}
	mA2 := &bgScriptModel{}
	if _, err := bgAgent("a", mA2, tk, bgConfig(mgr)).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("好了吗")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(inputText(mA2.input(0)), "A-DONE") {
		t.Fatalf("agent a 下一次跑时应收到通知：\n%s", inputText(mA2.input(0)))
	}
}

// TestBackgroundTask_PanicBecomesFailed：后台工具 panic 不让进程崩，任务记为失败，通知里说失败了。
func TestBackgroundTask_PanicBecomesFailed(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	g := newGatedTool("")
	g.panicMsg = "构建脚本崩了"
	tk := tool.NewToolkit()
	g.register(tk, "build")
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-panic")
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("build", `{}`), sayStep("开始了")}}
	if _, err := bgAgent("coder", m, tk, bgConfig(mgr)).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("构建")); err != nil {
		t.Fatal(err)
	}
	id := lastTaskID(m.input(1))
	g.open()
	if bt := waitTerminal(t, mgr, id); bt.Status != agent_v2.BackgroundTaskFailed || bt.Err == nil {
		t.Fatalf("panic 的任务应记为失败，状态 %q err %v", bt.Status, bt.Err)
	}
	m2 := &bgScriptModel{}
	if _, err := bgAgent("coder", m2, tk, bgConfig(mgr)).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("怎么样")); err != nil {
		t.Fatal(err)
	}
	if in := inputText(m2.input(0)); !strings.Contains(in, "失败了") || !strings.Contains(in, "构建脚本崩了") {
		t.Fatalf("通知里应说失败与原因：\n%s", in)
	}
}

// TestBackgroundTask_FailedModelCallRedelivers：送进通知的那一轮模型调用失败了，通知不算送达——下一次照样有。
func TestBackgroundTask_FailedModelCallRedelivers(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	g := newGatedTool("REDELIVER-ME")
	tk := tool.NewToolkit()
	g.register(tk, "build")
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-redeliver")
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("build", `{}`), sayStep("开始了")}}
	if _, err := bgAgent("coder", m, tk, bgConfig(mgr)).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("构建")); err != nil {
		t.Fatal(err)
	}
	id := lastTaskID(m.input(1))
	g.open()
	waitTerminal(t, mgr, id)

	broken := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		func([]*schema.Message) (*schema.Message, error) { return nil, errors.New("provider 502") },
	}}
	if _, err := bgAgent("coder", broken, tk, bgConfig(mgr)).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("好了吗")); err == nil {
		t.Fatal("fixture 没搭对：模型调用应失败")
	}
	if !strings.Contains(inputText(broken.input(0)), "REDELIVER-ME") {
		t.Fatal("fixture 没搭对：失败的那一轮里本该有通知")
	}
	m3 := &bgScriptModel{}
	if _, err := bgAgent("coder", m3, tk, bgConfig(mgr)).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("好了吗")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(inputText(m3.input(0)), "REDELIVER-ME") {
		t.Fatal("上一轮模型调用失败了，通知却被当成已送达、这次没再发")
	}
}

// TestBackgroundTask_ToolTimeoutBeforeYield：同步等的那段里工具超时照常生效（YieldAfter 比超时长时）。
func TestBackgroundTask_ToolTimeoutBeforeYield(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	g := newGatedTool("LATE")
	tk := tool.NewToolkit()
	g.register(tk, "build")
	cfg := bgConfig(mgr)
	cfg.YieldAfter = 2 * time.Second
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("build", `{}`)}}
	if _, err := bgAgent("coder", m, tk, cfg, agent_v2.WithToolTimeout(50*time.Millisecond)).Run(context.Background(),
		agent_v2.NewRunSession(), schema.UserMessage("构建")); err != nil {
		t.Fatal(err)
	}
	in := m.input(1)
	if got := in[len(in)-1].Content; !strings.Contains(got, "tool_timeout") {
		t.Fatalf("应按工具超时收尾：\n%s", got)
	}
	if !g.cancelled.Load() {
		t.Fatal("超时之后工具的 ctx 应被取消")
	}
}

// TestBackgroundTask_ConfigValidation：Manager / Eligible 必填，构造期就报。
func TestBackgroundTask_ConfigValidation(t *testing.T) {
	mustPanic := func(name string, cfg *agent_v2.BackgroundTasksConfig) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s：应在构造期 panic", name)
			}
		}()
		agent_v2.NewBasicAgent(agent_v2.WithName("x"), agent_v2.WithModel(&bgScriptModel{}), agent_v2.WithBackgroundTasks(cfg))
	}
	mustPanic("缺 Manager", &agent_v2.BackgroundTasksConfig{Eligible: func(tool.Tool) bool { return true }})
	mustPanic("缺 Eligible", &agent_v2.BackgroundTasksConfig{Manager: agent_v2.NewBackgroundTaskManager(nil)})
}

// TestBackgroundTask_EligibleToolRunsDetached：点名的工具在脱离当前执行的 ctx 里跑——拿不到 session 与会话 ID、
// 提问得到错误而不是中断。不脱离的话它跟节点共用执行状态：转入后台之后节点早结束了，它还在往节点的私有态、
// 收信号处、提问计数器上写（并发、且写给一次已经结束的执行）。
func TestBackgroundTask_EligibleToolRunsDetached(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	var sawSession, sawThread, askInterrupted atomic.Bool
	var askErr atomic.Value
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("build", "构建", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			sawSession.Store(agent_v2.GetSession(ctx) != nil)
			sawThread.Store(compose_v2.GetThreadID(ctx) != "")
			_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续吗"})
			if _, ok := compose_v2.IsInterrupt(err); ok {
				askInterrupted.Store(true)
			}
			askErr.Store(fmt.Sprint(err))
			return tool.Success("ok"), nil
		}))
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("build", `{}`)}}
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-detached")
	if _, err := bgAgent("coder", m, tk, bgConfig(mgr)).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("构建")); err != nil {
		t.Fatal(err)
	}
	if sawSession.Load() || sawThread.Load() {
		t.Fatalf("点名的工具拿到了当前执行的 session（%v）或会话 ID（%v）", sawSession.Load(), sawThread.Load())
	}
	if askInterrupted.Load() || askErr.Load() == "<nil>" {
		t.Fatalf("点名的工具里提问应得到错误、不是中断：%v", askErr.Load())
	}
}

// TestBackgroundTask_CallerCancelDuringSyncWindow：同步等的那段里调用方撤了——任务跟着取消，Run 不等工具返回。
func TestBackgroundTask_CallerCancelDuringSyncWindow(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	g := newGatedTool("NEVER")
	tk := tool.NewToolkit()
	g.register(tk, "build")
	cfg := bgConfig(mgr)
	cfg.YieldAfter = 5 * time.Second
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("build", `{}`)}}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	start := time.Now()
	if _, err := bgAgent("coder", m, tk, cfg).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("构建")); err == nil {
		t.Fatal("调用方撤了，Run 应返回错误")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("调用方撤了之后 Run 过了 %s 才返回", d)
	}
	deadline := time.Now().Add(time.Second)
	for !g.cancelled.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !g.cancelled.Load() {
		t.Fatal("调用方撤了，工具的 ctx 没被取消")
	}
}

// TestBackgroundTask_NotificationRecordedBeforeCompactionBoundary：通知送进来的那一轮又压缩了一次时，会话历史里
// 通知排在压缩边界之前——边界里的替代历史已经含着它（它是这一轮输入的一部分），排在边界之后的话，下一次 Run
// 从边界读回时它出现两次。
func TestBackgroundTask_NotificationRecordedBeforeCompactionBoundary(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	g := newGatedTool(strings.Repeat("BIG-RESULT-LINE 0123456789abcdef\n", 1300)) // ~42KB，送进来就越过压缩阈值
	tk := grepToolkit()
	g.register(tk, "build")
	hs := agent_v2.NewInMemoryHistoryStore()
	key := agent_v2.HistoryKey{Owner: "t", ConvID: "conv-order"}
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-order")
	s := &countingSummarizer{}
	mk := func(m model.ChatModel) *agent_v2.BasicAgent {
		return bgAgent("coder", m, tk, bgConfig(mgr), agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxHistoryPairs(0),
			agent_v2.WithAutoCompact(smallWindow(s)))
	}
	steps := []func([]*schema.Message) (*schema.Message, error){callStep("build", `{}`)}
	for i := 0; i < 6; i++ {
		steps = append(steps, callStep("grep", `{}`))
	}
	m1 := &bgScriptModel{steps: append(steps, sayStep("先这样"))}
	if _, err := mk(m1).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("构建并查代码")); err != nil {
		t.Fatal(err)
	}
	if s.count() != 0 {
		t.Fatalf("fixture 没搭对：Run 1 不该压缩（摘要了 %d 次）", s.count())
	}
	id := lastTaskID(m1.input(1))
	g.open()
	waitTerminal(t, mgr, id)
	if _, err := mk(&bgScriptModel{}).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("好了吗")); err != nil {
		t.Fatal(err)
	}
	if s.count() == 0 {
		t.Fatal("fixture 没搭对：通知送进来的那一轮应越过压缩阈值")
	}
	all, _ := agent_v2.LoadAllHistory(ctx, hs, key)
	noteAt, boundaryAt := -1, -1
	for i, msg := range all {
		if v, _ := msg.Annotations[agent_v2.BackgroundTaskNotificationAnnotation].(string); v == id {
			noteAt = i
		}
		if _, ok := msg.Annotations[agent_v2.HistoryCompactionAnnotation]; ok && boundaryAt < 0 {
			boundaryAt = i
		}
	}
	if noteAt < 0 || boundaryAt < 0 {
		t.Fatalf("fixture 没搭对：历史里通知在 %d、边界在 %d", noteAt, boundaryAt)
	}
	if boundaryAt < noteAt {
		t.Fatalf("通知（第 %d 条）记在了压缩边界（第 %d 条）之后——下一次 Run 从边界读回时它会出现两次", noteAt, boundaryAt)
	}
}
