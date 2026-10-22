package agent_v2_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

// bgStartAndFinish：Run 1 开一个后台任务并让它结束，返回任务 id。
func bgStartAndFinish(t *testing.T, mgr *agent_v2.BackgroundTaskManager, ag func(m *bgScriptModel) *agent_v2.BasicAgent,
	ctx context.Context, sess *agent_v2.RunSession, g *gatedTool) string {
	t.Helper()
	m1 := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("build", `{}`), sayStep("已开始构建")}}
	if _, err := ag(m1).Run(ctx, sess, schema.UserMessage("构建一下")); err != nil {
		t.Fatal(err)
	}
	id := lastTaskID(m1.input(1))
	if id == "" {
		t.Fatalf("fixture 没搭对：Run 1 没转入后台")
	}
	g.open()
	waitTerminal(t, mgr, id)
	return id
}

// ToolFilter 的 lastInput——通知送进来的那一轮（下一次 Run 的第一轮），业务回调收到的「本轮输入」是通知，
// 不是用户这一轮说的话。
func TestBackgroundTask_ToolFilterSeesUserInputNotNotification(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	g := newGatedTool("BUILD-OK")
	tk := tool.NewToolkit()
	g.register(tk, "build")
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-tf")
	var mu sync.Mutex
	var seen []string
	filter := agent_v2.WithToolFilter(func(_ context.Context, in *schema.Message) []string {
		mu.Lock()
		defer mu.Unlock()
		if in != nil {
			seen = append(seen, in.Content)
		} else {
			seen = append(seen, "<nil>")
		}
		return nil
	})
	ag := func(m *bgScriptModel) *agent_v2.BasicAgent { return bgAgent("coder", m, tk, bgConfig(mgr), filter) }
	bgStartAndFinish(t, mgr, ag, ctx, agent_v2.NewRunSession(), g)
	mu.Lock()
	seen = nil
	mu.Unlock()
	m2 := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){sayStep("好的")}}
	if _, err := ag(m2).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("帮我查一下北京天气")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(inputText(m2.input(0)), bgNoteTag) {
		t.Fatalf("fixture 没搭对：Run 2 第一轮没有通知")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 || seen[0] != "帮我查一下北京天气" {
		t.Fatalf("Run 2 第一轮 ToolFilter 收到的输入应是用户这轮说的话，实际是：%q", seen)
	}
}

// 同一个 session 连续 Run（不调 NextTask——框架把它当新任务处理的写法）、没开会话历史：通知在 Run 2 的
// 唯一一轮送达之后，Run 3 又发一遍（「同一任务里记成送达、流里却没有」的复查把新的一次 Run 当成了同一个任务）。
func TestBackgroundTask_SameSessionRerunDoesNotRedeliver(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	g := newGatedTool("BUILD-OK-B")
	tk := tool.NewToolkit()
	g.register(tk, "build")
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-rerun")
	sess := agent_v2.NewRunSession()
	ag := func(m *bgScriptModel) *agent_v2.BasicAgent { return bgAgent("coder", m, tk, bgConfig(mgr)) }
	bgStartAndFinish(t, mgr, ag, ctx, sess, g)

	m2 := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){sayStep("构建通过了")}}
	if _, err := ag(m2).Run(ctx, sess, schema.UserMessage("好了吗")); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(inputText(m2.input(0)), bgNoteTag); n != 1 {
		t.Fatalf("fixture 没搭对：Run 2 应收到 1 条通知，got %d", n)
	}
	for i := 3; i <= 5; i++ {
		m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){sayStep("嗯")}}
		if _, err := ag(m).Run(ctx, sess, schema.UserMessage("还有别的吗")); err != nil {
			t.Fatal(err)
		}
		if n := strings.Count(inputText(m.input(0)), bgNoteTag); n != 0 {
			t.Fatalf("Run %d 第一轮又收到 %d 条已经在 Run 2 送达过的通知", i, n)
		}
	}
}

// TestBackgroundTask_RequiresExplicitName：通知按 agent 名路由——不显式起名时自动名每次构造都不同，按请求构造的
// agent（agui 的 AgentFactory 就是按连接构造）下一次请求收不到上一次开的任务的通知，也不报错。构造期就拒。
func TestBackgroundTask_RequiresExplicitName(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("WithBackgroundTasks 没配 WithName 应在构造期 panic")
		}
	}()
	agent_v2.NewBasicAgent(agent_v2.WithModel(&bgScriptModel{}), agent_v2.WithBackgroundTasks(bgConfig(agent_v2.NewBackgroundTaskManager(nil))))
}

// AutoCompact 的尾部按「最后一条 user 消息」定——通知排在用户这轮的提问之后，送进来的那一轮触发摘要时，
// 用户刚问的话被摘进摘要、原文不在模型输入里。对照组（任务还没结束、没有通知）用户原话保留。
func TestBackgroundTask_NotificationDoesNotPushUserQuestionIntoSummary(t *testing.T) {
	for _, withNote := range []bool{false, true} {
		name := "无通知"
		if withNote {
			name = "有通知"
		}
		t.Run(name, func(t *testing.T) {
			mgr := agent_v2.NewBackgroundTaskManager(nil)
			closeMgr(t, mgr)
			g := newGatedTool("BUILD-OK-D")
			tk := tool.NewToolkit()
			g.register(tk, "build")
			big := strings.Repeat("src/a.go:12: func foo() { return bar }\n", 200) // ~8KB
			tk.Register(tool.NewFuncTool("grep", "grep", map[string]any{"type": "object"},
				func(context.Context, map[string]any) (*tool.Result, error) {
					return &tool.Result{Success: true, Output: big}, nil
				}))
			hs := agent_v2.NewInMemoryHistoryStore()
			ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-ac-"+name)
			// Run 1：开后台任务 + 6 轮 grep 攒出长历史（这个 agent 不开压缩）
			steps := []func([]*schema.Message) (*schema.Message, error){callStep("build", `{}`)}
			for i := 0; i < 6; i++ {
				steps = append(steps, callStep("grep", `{}`))
			}
			steps = append(steps, sayStep("已开始"))
			m1 := &bgScriptModel{steps: steps}
			if _, err := bgAgent("coder", m1, tk, bgConfig(mgr), agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(20)).
				Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("构建并搜一下")); err != nil {
				t.Fatal(err)
			}
			id := lastTaskID(m1.input(1))
			if id == "" {
				t.Fatal("fixture 没搭对：没转入后台")
			}
			if withNote {
				g.open()
				waitTerminal(t, mgr, id)
			} else {
				defer g.open()
			}
			s := &countingSummarizer{}
			m2 := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){sayStep("好")}}
			const q = "QUESTION-请把第三个函数改名为 baz"
			if _, err := bgAgent("coder", m2, tk, bgConfig(mgr), agent_v2.WithHistory(hs, "t"),
				agent_v2.WithAutoCompact(smallWindow(s))).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(q)); err != nil {
				t.Fatal(err)
			}
			in := inputText(m2.input(0))
			s.mu.Lock()
			summarized := len(s.inputs)
			s.mu.Unlock()
			if summarized == 0 {
				t.Fatalf("fixture 没搭对：Run 2 第一轮没触发摘要")
			}
			if withNote && !strings.Contains(in, bgNoteTag) {
				t.Fatalf("fixture 没搭对：有通知组没收到通知")
			}
			if !strings.Contains(in, q) {
				t.Fatalf("[%s] 用户这一轮的提问原文不在模型输入里（被摘进了摘要）：\n%s", name, bgTruncateForLog(in))
			}
		})
	}
}

func bgTruncateForLog(s string) string {
	if len(s) > 600 {
		return s[:300] + " …… " + s[len(s)-300:]
	}
	return s
}

// 可后台的工具是一个被当工具用的 agent（长时间跑的子 agent 是最典型的慢工具）——它在后台 ctx 里跑，
// 环境（编排钩子）照留；顶层 Run 返回之后它还在往调用方那次请求的编排钩子上发事件。
func TestBackgroundTask_BackgroundToolDoesNotReachRequestHooks(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	release := make(chan struct{})
	inner := agent_v2.NewBasicAgent(agent_v2.WithName("researcher"), agent_v2.WithDescription("调研员"),
		agent_v2.WithModel(&bgSlowModel{release: release}), agent_v2.WithMaxRounds(2))
	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(inner))
	cfg := &agent_v2.BackgroundTasksConfig{Manager: mgr, YieldAfter: 50 * time.Millisecond,
		Eligible: func(tl tool.Tool) bool { return tl.Name() == "researcher" }}
	rec := &bgOrchRec{}
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-agenttool")
	ctx = agent_v2.WithCtxOrchHooks(ctx, rec)
	m1 := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("researcher", `{"input":"调研一下"}`), sayStep("已开始")}}
	if _, err := bgAgent("coder", m1, tk, cfg).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("调研")); err != nil {
		t.Fatal(err)
	}
	id := lastTaskID(m1.input(1))
	if id == "" {
		t.Fatalf("fixture 没搭对：没转入后台；Run 1 第二轮输入：\n%s", inputText(m1.input(1)))
	}
	rec.markReturned()
	close(release)
	bt := waitTerminal(t, mgr, id)
	t.Logf("任务结局 %s，结果 %+v，err %v", bt.Status, bt.Result, bt.Err)
	rec.mu.Lock()
	t.Logf("Run 返回之后收到的编排回调：%v", rec.kinds)
	rec.mu.Unlock()
	if n := rec.afterReturn(); n > 0 {
		t.Fatalf("顶层 Run 返回之后，后台子 agent 还在往这次请求的编排钩子上发了 %d 个回调（agui / a2a 的流此时已关）", n)
	}
}

type bgSlowModel struct{ release chan struct{} }

func (m *bgSlowModel) Generate(ctx context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	select {
	case <-m.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return schema.AssistantMessage("调研结论：X"), nil
}

func (m *bgSlowModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *bgSlowModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// bgOrchRec 记顶层 Run 返回之后还收到多少个编排回调。
type bgOrchRec struct {
	agent_v2.NoOpOrchestrationHooks
	mu       sync.Mutex
	returned bool
	after    int
	kinds    []string
}

func (r *bgOrchRec) markReturned() { r.mu.Lock(); r.returned = true; r.mu.Unlock() }
func (r *bgOrchRec) hit(kind string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.returned {
		r.after++
		r.kinds = append(r.kinds, kind)
	}
}
func (r *bgOrchRec) afterReturn() int { r.mu.Lock(); defer r.mu.Unlock(); return r.after }
func (r *bgOrchRec) OnGraphStart(ctx context.Context) (context.Context, error) {
	r.hit("graph_start")
	return ctx, nil
}
func (r *bgOrchRec) OnGraphEnd(ctx context.Context, err error) context.Context {
	r.hit("graph_end")
	return ctx
}
func (r *bgOrchRec) OnNodeStart(ctx context.Context, id string) (context.Context, error) {
	r.hit("node_start:" + id)
	return ctx, nil
}
func (r *bgOrchRec) OnNodeEnd(ctx context.Context, id string, err error) context.Context {
	r.hit("node_end:" + id)
	return ctx
}
func (r *bgOrchRec) OnNodeEvent(ctx context.Context, id string, ev agent_v2.Event) context.Context {
	r.hit("event:" + ev.Type)
	return ctx
}

// TestBackgroundTask_ReturnDirectRoundStillDelivers：通知送进来的那一轮以 return-direct 工具收尾（之后不再有模型节点）
// 时也记为送达——return-direct 收尾经过收尾节点，它读到的已提交消息流里有这条通知。不记的话下一次 Run 再发一遍。
func TestBackgroundTask_ReturnDirectRoundStillDelivers(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	g := newGatedTool("RD-RESULT")
	tk := tool.NewToolkit()
	g.register(tk, "build")
	tk.Register(tool.NewFuncTool("finish", "收尾", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: "已收尾", Metadata: map[string]any{tool.ReturnDirectMetaKey: true}}, nil
		}))
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-rd")
	sess := agent_v2.NewRunSession()
	ag := func(m *bgScriptModel) *agent_v2.BasicAgent { return bgAgent("coder", m, tk, bgConfig(mgr)) }
	bgStartAndFinish(t, mgr, ag, ctx, sess, g)
	m2 := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("finish", `{}`)}}
	if _, err := ag(m2).Run(ctx, sess, schema.UserMessage("收尾吧")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(inputText(m2.input(0)), "RD-RESULT") || m2.calls() != 1 {
		t.Fatalf("fixture 没搭对：Run 2 应只调一次模型、那一轮带着通知（调了 %d 次）", m2.calls())
	}
	m3 := &bgScriptModel{}
	if _, err := ag(m3).Run(ctx, sess, schema.UserMessage("还有吗")); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(inputText(m3.input(0)), bgNoteTag) {
		t.Fatal("通知在 Run 2 以 return-direct 收尾的那一轮已经提交，Run 3 又收到一遍")
	}
}

// TestBackgroundTask_CallerCancelWaitsForTool：同步等的那段里调用方撤了，Run 等工具真的返回之后才退出——跟同步执行
// 一样。不等的话，续跑时这次调用不在工具账里、会再执行一次，两次执行可能重叠。
func TestBackgroundTask_CallerCancelWaitsForTool(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	var exited atomic.Bool
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("build", "慢", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			<-ctx.Done()
			time.Sleep(200 * time.Millisecond) // 响应取消要一会儿
			exited.Store(true)
			return nil, ctx.Err()
		}))
	cfg := bgConfig(mgr)
	cfg.YieldAfter = 5 * time.Second
	m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){callStep("build", `{}`)}}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	if _, err := bgAgent("coder", m, tk, cfg).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("构建")); err == nil {
		t.Fatal("调用方撤了，Run 应返回错误")
	}
	if !exited.Load() {
		t.Fatal("Run 在工具返回之前就退出了")
	}
}

// TestBackgroundTask_SpilledResultNoteCarriesID：后台任务的结果大到要转存时，通知里带着取回 id（正文与标注都有）——
// 标注是压缩层附取回索引时认的那个，通知被摘要掉之后模型照样取得回。
func TestBackgroundTask_SpilledResultNoteCarriesID(t *testing.T) {
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	closeMgr(t, mgr)
	g := newGatedTool(strings.Repeat("BIG-LINE 0123456789\n", 3000))
	tk := tool.NewToolkit()
	g.register(tk, "build")
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-bg-spill")
	ag := func(m *bgScriptModel) *agent_v2.BasicAgent {
		return bgAgent("coder", m, tk, bgConfig(mgr), agent_v2.WithStore(store.NewInMemoryStore()),
			agent_v2.WithToolResultSpill(&agent_v2.ToolResultSpillConfig{Threshold: 1000}))
	}
	bgStartAndFinish(t, mgr, ag, ctx, agent_v2.NewRunSession(), g)
	m2 := &bgScriptModel{}
	if _, err := ag(m2).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("好了吗")); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, msg := range m2.input(0) {
		if !agent_v2.IsBackgroundTaskNotification(msg) {
			continue
		}
		id, _ := msg.Annotations[agent_v2.ToolResultSpillAnnotation].(string)
		if id == "" || !strings.Contains(msg.Content, id) {
			t.Fatalf("结果转存了，通知应带取回 id（标注 %q）：\n%.300s", id, msg.Content)
		}
		found = true
	}
	if !found {
		t.Fatal("fixture 没搭对：Run 2 没收到通知")
	}
}
