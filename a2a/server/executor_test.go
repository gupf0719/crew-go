package server_test

// 黑盒测试 server 侧 HITL + 流式——served agent 用**真 BasicAgent**（ask_user / 流式 mock
// model），不用闭包绕过框架路径（CLAUDE.md：HITL/Resume 必须真实组件端到端）。客户端两路：
// crew-go 自己的 RemoteAgent（双侧闭环）+ 裸 a2aclient（直接断言 wire 事件流）。

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/a2a"
	"crew-go/a2a/server"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

// serveAgent 把 agent 起成 in-process A2A server，返回 baseURL。
func serveAgent(t *testing.T, ag agent_v2.Agent) string {
	return serveAgentMW(t, ag, nil)
}

// serveAgentMW 同 serveAgent，可加 HTTP middleware（验业务经 middleware 注 ctx
// hooks/trace 的路径）。
func serveAgentMW(t *testing.T, ag agent_v2.Agent, wrap func(http.Handler) http.Handler) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	baseURL := "http://" + ln.Addr().String()
	h := server.Handler(ag, baseURL, server.WithOwnerResolver(func(context.Context) string { return "test" }))
	if wrap != nil {
		h = wrap(h)
	}
	go func() { _ = http.Serve(ln, h) }()
	t.Cleanup(func() { _ = ln.Close() })
	waitReady(t, baseURL+"/.well-known/agent-card.json")
	return baseURL
}

func waitReady(t *testing.T, url string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server 未就绪: %s", url)
}

// ---- 脚本化 model ----

// askModel：本轮无 tool_result → 对每个 question 发一个 ask_user tool_call；有 tool_result
// （人类答案）→ 终答 "got: <答案们>"。
type askModel struct {
	tools     []*schema.ToolInfo
	questions []string
}

func (m *askModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	cp := *m
	cp.tools = tools
	return &cp, nil
}

func (m *askModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	// 倒扫到最近一条 user 之间的 tool_result（只看本轮）
	var results []string
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == schema.RoleUser {
			break
		}
		if msgs[i].Role == schema.RoleTool {
			results = append([]string{msgs[i].Content}, results...)
		}
	}
	if len(results) > 0 {
		return schema.AssistantMessage("got: " + strings.Join(results, "+")), nil
	}
	tcs := make([]schema.ToolCall, 0, len(m.questions))
	for i, q := range m.questions {
		tcs = append(tcs, schema.ToolCall{
			ID:   fmt.Sprintf("call_%d", i),
			Type: "function",
			Function: schema.FunctionCall{
				Name:      agent_v2.AskUserToolName,
				Arguments: fmt.Sprintf(`{"question":%q}`, q),
			},
		})
	}
	return schema.AssistantToolCallMessage(tcs), nil
}

func (m *askModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(resp)
	schema.CloseSend(sr)
	return sr, nil
}

// streamModel：Stream 分 3 个 content chunk 发——验证 chunk → A2A artifact 桥。
type streamModel struct{ tools []*schema.ToolInfo }

func (m *streamModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	cp := *m
	cp.tools = tools
	return &cp, nil
}

func (m *streamModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage("Hello streaming world"), nil
}

func (m *streamModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	sr, send := schema.NewStreamReader[*schema.Message](4)
	send(schema.AssistantMessage("Hello"))
	send(schema.AssistantMessage(" streaming"))
	send(schema.AssistantMessage(" world"))
	schema.CloseSend(sr)
	return sr, nil
}

// gateModel：round 1 → ask_user；resume（见 tool_result）→ 进入时关 hit 信号、阻塞 gate
// 直到测试释放——用来把 resume 卡在"在途"以触发 Cancel-vs-在途-Resume 并发路径。
type gateModel struct {
	tools    []*schema.ToolInfo
	question string
	gate     chan struct{}
	hit      chan struct{}
	hitOnce  sync.Once
}

func (m *gateModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	// 返回同实例（不能 cp := *m 拷贝 sync.Once/channels——gate/hit/once 须共享让测试控制）
	m.tools = tools
	return m, nil
}

func (m *gateModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	hasResult := false
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == schema.RoleUser {
			break
		}
		if msgs[i].Role == schema.RoleTool {
			hasResult = true
			break
		}
	}
	if hasResult {
		m.hitOnce.Do(func() { close(m.hit) }) // 信号：resume 路径已进入（resend 在途）
		<-m.gate                              // 阻塞到测试释放（故意忽略 ctx，确定性拉宽在途窗口）
		return schema.AssistantMessage("got: resumed"), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "call_0", Type: "function",
		Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: fmt.Sprintf(`{"question":%q}`, m.question)},
	}}), nil
}

func (m *gateModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(resp)
	schema.CloseSend(sr)
	return sr, nil
}

func newAsker(t *testing.T, questions ...string) agent_v2.Agent {
	t.Helper()
	return agent_v2.NewBasicAgent(
		agent_v2.WithName("asker"),
		agent_v2.WithModel(&askModel{questions: questions}),
		agent_v2.WithAskUser(),
	)
}

// ---- 测试 ----

// TestServerHITL_FullLoop 双侧 HITL 闭环：served 真 BasicAgent+ask_user 抛中断 →
// A2A input-required → 客户端 RemoteAgent(WithHumanGate) 抛 InterruptError 骑 Sequential cp
// → Resume("blue") → 服务端按 taskID Resume → ask_user 拿答案 → 终答回到客户端。
func TestServerHITL_FullLoop(t *testing.T) {
	base := serveAgent(t, newAsker(t, "Which color do you prefer?"))
	ctx := context.Background()

	ra, err := a2a.NewRemoteAgent(ctx, base, a2a.WithHumanGate())
	if err != nil {
		t.Fatalf("NewRemoteAgent: %v", err)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("hitl_seq")).AddStep("remote", ra)
	hctx := compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()

	_, err = seq.Run(hctx, sess, schema.UserMessage("ask me"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("期望 InterruptError，got %v", err)
	}
	if ie.Query == nil || !strings.Contains(ie.Query.Question, "Which color do you prefer?") {
		t.Fatalf("中断 Question 应透传 server 的问题，got %+v", ie.Query)
	}

	events, err := seq.Resume(hctx, sess.RunID,
		agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("blue"),
		}))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || final.Content != "got: blue" {
		t.Fatalf("HITL 闭环终答应为 'got: blue'，got %v", final)
	}
}

// TestServerHITL_DefaultMode 客户端不开 humanGate：input-required 当普通追问返回，
// 同 session 再调（续 taskID）→ 服务端 Resume → 终答。
func TestServerHITL_DefaultMode(t *testing.T) {
	base := serveAgent(t, newAsker(t, "Which color do you prefer?"))
	ctx := context.Background()

	ra, err := a2a.NewRemoteAgent(ctx, base)
	if err != nil {
		t.Fatalf("NewRemoteAgent: %v", err)
	}
	sess := agent_v2.NewRunSession()

	ev1, err := ra.Run(ctx, sess, schema.UserMessage("ask me"))
	if err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	first := agent_v2.FinalMessage(ev1)
	if first == nil || !strings.Contains(first.Content, "input_required") || !strings.Contains(first.Content, "Which color") {
		t.Fatalf("默认模式应返追问，got %v", first)
	}

	ev2, err := ra.Run(ctx, sess, schema.UserMessage("blue"))
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	second := agent_v2.FinalMessage(ev2)
	if second == nil || second.Content != "got: blue" {
		t.Fatalf("续发后终答应为 'got: blue'，got %v", second)
	}
}

// TestServerMultiQueryJoin fanout 双 ask_user 并发中断 → 服务端把两问（各带 interruptId）合进一条 input-required
// → 一条文本答不了两问，任务继续停在 input-required、请对端按 interruptId 分别作答 → 客户端（默认模式）给按
// interruptId 的 JSON → 每问拿到自己的答案。曾经一条文本灌给全部挂起项：放在审批上，一句 approve 就批准了全部工具。
func TestServerMultiQueryJoin(t *testing.T) {
	base := serveAgent(t, newAsker(t, "Q-A?", "Q-B?"))
	ctx := context.Background()

	ra, err := a2a.NewRemoteAgent(ctx, base)
	if err != nil {
		t.Fatalf("NewRemoteAgent: %v", err)
	}
	sess := agent_v2.NewRunSession()

	ev1, err := ra.Run(ctx, sess, schema.UserMessage("ask me"))
	if err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	first := agent_v2.FinalMessage(ev1)
	if first == nil || !strings.Contains(first.Content, "Q-A?") || !strings.Contains(first.Content, "Q-B?") {
		t.Fatalf("多 query 应合并两问题，got %v", first)
	}
	ids := regexp.MustCompile(`interruptId: ([0-9a-f]+)`).FindAllStringSubmatch(first.Content, -1)
	if len(ids) != 2 {
		t.Fatalf("两问都应带上 interruptId，got %q", first.Content)
	}

	ev2, err := ra.Run(ctx, sess, schema.UserMessage("both-ok"))
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	second := agent_v2.FinalMessage(ev2)
	if second == nil || !strings.Contains(second.Content, "input_required") || !strings.Contains(second.Content, "a single reply cannot answer them all") {
		t.Fatalf("一条文本答不了两问，应请对端分别作答，got %v", second)
	}

	answer := fmt.Sprintf(`{%q: "a-ans", %q: "b-ans"}`, ids[0][1], ids[1][1])
	ev3, err := ra.Run(ctx, sess, schema.UserMessage(answer))
	if err != nil {
		t.Fatalf("Run 3: %v", err)
	}
	third := agent_v2.FinalMessage(ev3)
	if third == nil || third.Content != "got: a-ans+b-ans" {
		t.Fatalf("按 interruptId 作答之后每问应拿到自己的答案，got %v", third)
	}
}

// TestServerStreaming 裸 a2aclient 直接断言 wire：LLM chunk 桥接为 artifact 事件
// （首 chunk 建 + 后续 append），聚合文本 == 终答，终态 completed。
func TestServerStreaming(t *testing.T) {
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("streamer"), agent_v2.WithModel(&streamModel{}))
	base := serveAgent(t, ag)
	ctx := context.Background()

	client, err := a2aclient.NewFromCard(ctx, server.Card(ag, base))
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}
	var (
		artifactEvents int
		artifactText   strings.Builder
		lastState      a2atypes.TaskState
	)
	msg := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("hi"))
	for ev, e := range client.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
		if e != nil {
			t.Fatalf("stream: %v", e)
		}
		switch v := ev.(type) {
		case *a2atypes.TaskArtifactUpdateEvent:
			artifactEvents++
			if v.Artifact != nil {
				for _, p := range v.Artifact.Parts {
					if p != nil {
						artifactText.WriteString(p.Text())
					}
				}
			}
		case *a2atypes.TaskStatusUpdateEvent:
			lastState = v.Status.State
		case *a2atypes.Task:
			lastState = v.Status.State
		}
	}
	if artifactEvents < 2 {
		t.Fatalf("期望 ≥2 个 artifact 事件（chunk 实时桥接），got %d", artifactEvents)
	}
	if artifactText.String() != "Hello streaming world" {
		t.Fatalf("artifact 聚合应为终答，got %q", artifactText.String())
	}
	if lastState != a2atypes.TaskStateCompleted {
		t.Fatalf("终态应 completed，got %s", lastState)
	}
}

// recordingOrch 记录业务 OrchHooks 收到的回调（middleware 注入路径）。
type recordingOrch struct {
	agent_v2.NoOpOrchestrationHooks
	mu          sync.Mutex
	graphStarts int
	chunkEvents int
	threadIDs   map[string]struct{}
}

func (r *recordingOrch) OnGraphStart(ctx context.Context) (context.Context, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.graphStarts++
	if tid := compose_v2.GetThreadID(ctx); tid != "" {
		if r.threadIDs == nil {
			r.threadIDs = map[string]struct{}{}
		}
		r.threadIDs[tid] = struct{}{}
	}
	return ctx, nil
}

func (r *recordingOrch) OnNodeEvent(ctx context.Context, _ string, ev agent_v2.Event) context.Context {
	if ev.Type == "chunk" {
		r.mu.Lock()
		r.chunkEvents++
		r.mu.Unlock()
	}
	return ctx
}

// TestServerBusinessOrchHooksNotClobbered 锚定 audit obs-#1/#2：业务经 HTTP middleware
// push 的 WithCtxOrchHooks 必须**穿过 a2asrv detached ctx 一路 fire**（executor 与流式桥
// chain 而非顶掉），且 ThreadID=A2A ContextID 在 hook ctx 可见。
// **sanity-revert**：runStreaming 去掉 chain（只传 fw）→ graphStarts/chunkEvents 归 0 → FAIL；
// 去掉 ThreadID push → threadIDs 断言 FAIL。
func TestServerBusinessOrchHooksNotClobbered(t *testing.T) {
	rec := &recordingOrch{}
	mw := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(agent_v2.WithCtxOrchHooks(r.Context(), rec)))
		})
	}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("streamer"), agent_v2.WithModel(&streamModel{}))
	base := serveAgentMW(t, ag, mw)
	ctx := context.Background()

	client, err := a2aclient.NewFromCard(ctx, server.Card(ag, base))
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}
	var artifacts int
	var taskContextID string
	msg := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("hi"))
	for ev, e := range client.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
		if e != nil {
			t.Fatalf("stream: %v", e)
		}
		switch v := ev.(type) {
		case *a2atypes.TaskArtifactUpdateEvent:
			artifacts++
		case *a2atypes.Task:
			taskContextID = v.ContextID
		}
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.graphStarts == 0 {
		t.Fatal("obs-#1: 业务 ctx OrchHooks 的 OnGraphStart 未 fire——被 executor 的流式桥顶掉了（未 chain）")
	}
	if rec.chunkEvents == 0 {
		t.Fatal("obs-#1: 业务 ctx OrchHooks 的 OnNodeEvent(chunk) 未 fire——被顶掉")
	}
	if artifacts == 0 {
		t.Fatal("chain 后流式桥也必须仍工作（artifact 应照常投递）")
	}
	if taskContextID == "" {
		t.Fatal("未拿到 task ContextID")
	}
	if _, ok := rec.threadIDs[taskContextID]; !ok {
		t.Fatalf("obs-#2: hook ctx 应带 ThreadID=A2A ContextID(%q)，got %v", taskContextID, rec.threadIDs)
	}
}

// TestServerContinuationAfterComplete task 完成后映射已清——再续发不得成功（Failed 状态或
// 协议层错误都可，关键是不能静默"成功"）。
func TestServerContinuationAfterComplete(t *testing.T) {
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("streamer"), agent_v2.WithModel(&streamModel{}))
	base := serveAgent(t, ag)
	ctx := context.Background()

	client, err := a2aclient.NewFromCard(ctx, server.Card(ag, base))
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}
	// 第一轮：完成一个 task，记 taskID/contextID
	var taskID a2atypes.TaskID
	var contextID string
	msg := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("hi"))
	for ev, e := range client.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
		if e != nil {
			t.Fatalf("stream 1: %v", e)
		}
		if task, ok := ev.(*a2atypes.Task); ok {
			taskID, contextID = task.ID, task.ContextID
		}
	}
	if taskID == "" {
		t.Fatal("第一轮未拿到 taskID")
	}

	// 续发已完成的 task → 不得成功
	cont := a2atypes.NewMessageForTask(a2atypes.MessageRoleUser,
		a2atypes.TaskInfo{TaskID: taskID, ContextID: contextID}, a2atypes.NewTextPart("again"))
	sawFailure := false
	for ev, e := range client.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: cont}) {
		if e != nil {
			sawFailure = true // 协议层拒绝续发终态 task 也算正确
			break
		}
		if st, ok := ev.(*a2atypes.TaskStatusUpdateEvent); ok && st.Status.State == a2atypes.TaskStateFailed {
			sawFailure = true
		}
		if st, ok := ev.(*a2atypes.Task); ok && st.Status.State == a2atypes.TaskStateFailed {
			sawFailure = true
		}
	}
	if !sawFailure {
		t.Fatal("续发已完成 task 应失败（Failed 状态或协议错误），却看似成功")
	}
}

// TestServerCancelDuringResume 触发 audit MED 的并发路径：Cancel 与**在途续发 Resume** 并发
// （gate model 把 resume 卡在在途，期间 fire CancelTask）。验证：并发路径**不死锁 / 不 panic /
// 取消后续发被拒**——这是它锚定的真回归点（busy-aware Cancel 协调不当极易在此死锁）。
//
// **注意（cross-verify 实证）**：本测试**不是** Cancel-Resume 逻辑 cp-race 的 sanity-revert 锚点
// （race 是逻辑层，-race 看不见；且本测试 gate 停点在 cp 已加载之后，revert fix 仍 PASS）。
// 那个 race 的确定性锚点是白盒 TestCancelDuringResume_NoOrphanCp（executor_internal_test.go）——
// 直驱 Executor + resume 轮再问触发 SaveNS 重建，断言孤儿 pending cp；revert 即 FAIL（已实跑验证）。
func TestServerCancelDuringResume(t *testing.T) {
	gm := &gateModel{question: "你喜欢什么颜色?", gate: make(chan struct{}), hit: make(chan struct{})}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("gated"), agent_v2.WithModel(gm), agent_v2.WithAskUser())
	base := serveAgent(t, ag)
	ctx := context.Background()

	client, err := a2aclient.NewFromCard(ctx, server.Card(ag, base))
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}

	// 1. 触发 input-required，拿 taskID
	var taskID a2atypes.TaskID
	var contextID string
	var lastState a2atypes.TaskState
	for ev, e := range client.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("ask me"))}) {
		if e != nil {
			t.Fatalf("stream: %v", e)
		}
		switch v := ev.(type) {
		case *a2atypes.Task:
			taskID, contextID, lastState = v.ID, v.ContextID, v.Status.State
		case *a2atypes.TaskStatusUpdateEvent:
			lastState = v.Status.State
		}
	}
	if taskID == "" || lastState != a2atypes.TaskStateInputRequired {
		t.Fatalf("应停在 input-required，taskID=%q state=%s", taskID, lastState)
	}

	// 2. 续发（goroutine）——服务端 Resume 会卡在 gate（在途）
	cont := a2atypes.NewMessageForTask(a2atypes.MessageRoleUser, a2atypes.TaskInfo{TaskID: taskID, ContextID: contextID}, a2atypes.NewTextPart("blue"))
	resendDone := make(chan struct{})
	go func() {
		defer close(resendDone)
		for range client.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: cont}) { //nolint:revive
		}
	}()

	// 3. 等 resume 真正进入在途
	select {
	case <-gm.hit:
	case <-time.After(10 * time.Second):
		t.Fatal("resume 路径未进入（resend 未在途）")
	}

	// 4. 在途时 Cancel——**goroutine**：a2asrv handleCancelWithConcurrentRun 会等在途 run 结束
	// 才返回 CancelTask，同步调会死锁（CancelTask 等 run、run 等 gate）。
	cancelDone := make(chan struct{})
	go func() {
		defer close(cancelDone)
		_, _ = client.CancelTask(ctx, &a2atypes.CancelTaskRequest{ID: taskID})
	}()

	// 5. 给 a2asrv 一点时间分发 cancel（让我们的 Cancel 闭包 tombstone+cr() 在在途时跑），
	// 再释放 gate → 在途 Resume 返回 → conclude 见 canceled 独占 abort（全程无并发 Resume）。
	time.Sleep(300 * time.Millisecond)
	close(gm.gate)

	// 6. resend 流 + cancel 都必须收尾（核心回归断言：并发路径不死锁）
	for _, w := range []struct {
		name string
		ch   chan struct{}
	}{{"resend", resendDone}, {"cancel", cancelDone}} {
		select {
		case <-w.ch:
		case <-time.After(10 * time.Second):
			t.Fatalf("%s 未结束（疑似死锁）", w.name)
		}
	}

	// 7. 取消后再续发不得成功
	cont2 := a2atypes.NewMessageForTask(a2atypes.MessageRoleUser, a2atypes.TaskInfo{TaskID: taskID, ContextID: contextID}, a2atypes.NewTextPart("again"))
	sawFailure := false
	for ev, e := range client.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: cont2}) {
		if e != nil {
			sawFailure = true
			break
		}
		if st, ok := ev.(*a2atypes.TaskStatusUpdateEvent); ok && st.Status.State == a2atypes.TaskStateFailed {
			sawFailure = true
		}
		if st, ok := ev.(*a2atypes.Task); ok && st.Status.State == a2atypes.TaskStateFailed {
			sawFailure = true
		}
	}
	if !sawFailure {
		t.Fatal("取消后再续发应被拒，却看似成功")
	}
}

// TestServerCancel 挂起 task 被 Cancel 后映射清掉——再续发不得成功。
func TestServerCancel(t *testing.T) {
	base := serveAgent(t, newAsker(t, "Which color do you prefer?"))
	ctx := context.Background()

	ag := newAsker(t, "unused") // 只为 Card 构造（name/desc 一致即可）
	client, err := a2aclient.NewFromCard(ctx, server.Card(ag, base))
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}

	// 触发 input-required，拿 taskID
	var taskID a2atypes.TaskID
	var contextID string
	var lastState a2atypes.TaskState
	msg := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("ask me"))
	for ev, e := range client.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
		if e != nil {
			t.Fatalf("stream: %v", e)
		}
		switch v := ev.(type) {
		case *a2atypes.Task:
			taskID, contextID = v.ID, v.ContextID
			lastState = v.Status.State
		case *a2atypes.TaskStatusUpdateEvent:
			lastState = v.Status.State
		}
	}
	if taskID == "" || lastState != a2atypes.TaskStateInputRequired {
		t.Fatalf("应停在 input-required，taskID=%q state=%s", taskID, lastState)
	}

	// Cancel
	if _, err := client.CancelTask(ctx, &a2atypes.CancelTaskRequest{ID: taskID}); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}

	// 续发已取消的 task → 不得成功
	cont := a2atypes.NewMessageForTask(a2atypes.MessageRoleUser,
		a2atypes.TaskInfo{TaskID: taskID, ContextID: contextID}, a2atypes.NewTextPart("blue"))
	sawFailure := false
	for ev, e := range client.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: cont}) {
		if e != nil {
			sawFailure = true
			break
		}
		if st, ok := ev.(*a2atypes.TaskStatusUpdateEvent); ok && st.Status.State == a2atypes.TaskStateFailed {
			sawFailure = true
		}
		if st, ok := ev.(*a2atypes.Task); ok && st.Status.State == a2atypes.TaskStateFailed {
			sawFailure = true
		}
	}
	if !sawFailure {
		t.Fatal("续发已取消 task 应失败，却看似成功")
	}
}
