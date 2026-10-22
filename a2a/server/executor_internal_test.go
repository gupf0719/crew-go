package server

// 白盒直驱 Executor（绕开 a2asrv 的终态守卫）——确定性锚定 Cancel-vs-在途-Resume 逻辑 race
//（audit MED）。黑盒 TestServerCancelDuringResume 只能守"不死锁"；本测试断言**真实 corruption**：
// 孤儿 pending cp + 孤儿映射条目。
//
// **判别原理**（为何 revert 必 FAIL）：
//   - buggy（Cancel 在 busy 时也直接并发 abort）：abort 在在途 Resume 还没跑完时就 DeleteSubtree；
//     在途 run 继续（状态已在内存）→ resume 轮 model **再问一次** → 第二次中断 SaveNS **重建**
//     pending cp——但映射条目已被 Cancel 删、无人再清 → 孤儿 cp 残留（ExistsNS=true）→ FAIL。
//   - fix（tombstone + 停在途，conclude 在 Resume 返回后独占 abort）：abort 一定发生在 SaveNS
//     之后 → cp 被清干净（ExistsNS=false）→ PASS。
//
// resume 轮停点（gate）+ 再问（re-ask）让两种实现的时序都确定，无 flaky。
//
// **sanity-revert（已实跑验证）**：把 Cancel 改回"无视 busy 直接 delete+abort"后本测试 FAIL
// 并指向孤儿 cp。

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	a2apkg "crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/observability"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// reAskGateModel：round1 → ask_user("Q1")；resume 轮（见 tool_result）→ 发 hit 信号、阻塞 gate
// 直到测试放行，然后**再问** ask_user("Q2")（触发第二次中断 SaveNS——判别器的关键）。
type reAskGateModel struct {
	tools   []*schema.ToolInfo
	gate    chan struct{}
	hit     chan struct{}
	hitOnce sync.Once
}

func (m *reAskGateModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	m.tools = tools // 返回同实例：gate/hit/once 须共享让测试控制
	return m, nil
}

func (m *reAskGateModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
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
	question := "Q1"
	if hasResult {
		m.hitOnce.Do(func() { close(m.hit) }) // resume 在途信号
		<-m.gate                              // 阻塞到测试放行（故意忽略 ctx——确定性控制停点）
		question = "Q2"                       // 再问 → 第二次中断 → SaveNS 重建 pending cp
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "call_0", Type: "function",
		Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: fmt.Sprintf(`{"question":%q}`, question)},
	}}), nil
}

func (m *reAskGateModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(resp)
	schema.CloseSend(sr)
	return sr, nil
}

// drain 同步消费 Execute/Cancel 的事件迭代器，返回收到的事件。
func drain(seq func(yield func(a2atypes.Event, error) bool)) []a2atypes.Event {
	var evs []a2atypes.Event
	seq(func(ev a2atypes.Event, _ error) bool {
		evs = append(evs, ev)
		return true
	})
	return evs
}

func lastStatus(evs []a2atypes.Event) a2atypes.TaskState {
	var s a2atypes.TaskState
	for _, ev := range evs {
		switch v := ev.(type) {
		case *a2atypes.Task:
			s = v.Status.State
		case *a2atypes.TaskStatusUpdateEvent:
			s = v.Status.State
		}
	}
	return s
}

func TestCancelDuringResume_NoOrphanCp(t *testing.T) {
	gm := &reAskGateModel{gate: make(chan struct{}), hit: make(chan struct{})}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("gated"),
		agent_v2.WithModel(gm),
		agent_v2.WithAskUser(),
	)
	e := NewExecutor(ag, WithOwnerResolver(func(context.Context) string { return "" }))
	ctx := context.Background()
	tid := a2atypes.TaskID("t-race")

	// 1. 新 task → 中断 → input-required（映射建立）
	newCtx := &a2asrv.ExecutorContext{
		TaskID: tid, ContextID: "c-1",
		Message: a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("ask me")),
	}
	evs := drain(func(y func(a2atypes.Event, error) bool) { e.Execute(ctx, newCtx)(y) })
	if got := lastStatus(evs); got != a2atypes.TaskStateInputRequired {
		t.Fatalf("第一轮应停在 input-required, got %s", got)
	}
	e.mu.Lock()
	st, ok := e.tasks[taskKey{taskID: tid}]
	if !ok {
		e.mu.Unlock()
		t.Fatal("中断后映射应存在")
	}
	sid := st.sess.RunID
	e.mu.Unlock()

	// 2. 续发（goroutine）——服务端 Resume 卡在 gate（在途）
	contCtx := &a2asrv.ExecutorContext{
		TaskID: tid, ContextID: "c-1",
		StoredTask: &a2atypes.Task{ID: tid, ContextID: "c-1"},
		Message: a2atypes.NewMessageForTask(a2atypes.MessageRoleUser,
			a2atypes.TaskInfo{TaskID: tid, ContextID: "c-1"}, a2atypes.NewTextPart("blue")),
	}
	resendDone := make(chan struct{})
	go func() {
		defer close(resendDone)
		_ = drain(func(y func(a2atypes.Event, error) bool) { e.Execute(ctx, contCtx)(y) })
	}()
	select {
	case <-gm.hit:
	case <-time.After(10 * time.Second):
		t.Fatal("resume 未进入在途")
	}

	// 3. 在途时 Cancel（白盒直驱——buggy 实现会在此刻并发 abort 删 cp）
	cancelCtx := &a2asrv.ExecutorContext{TaskID: tid, ContextID: "c-1"} // Message nil = cancelation
	_ = drain(func(y func(a2atypes.Event, error) bool) { e.Cancel(ctx, cancelCtx)(y) })

	// 4. 放行 → 在途 Resume 走完（re-ask → 第二次中断 SaveNS）→ conclude 收尾
	close(gm.gate)
	select {
	case <-resendDone:
	case <-time.After(10 * time.Second):
		t.Fatal("续发未收尾（疑似死锁）")
	}

	// 5. 锚点断言：取消后不得残留孤儿 pending cp / 孤儿映射。
	// buggy：abort 先于 SaveNS → 重建的 cp 无人清 → ExistsNS=true → FAIL（精确指向原 audit MED）。
	exists, err := e.cp.ExistsNS(ctx, sid, "")
	if err != nil {
		t.Fatalf("ExistsNS: %v", err)
	}
	if exists {
		t.Fatal("Cancel 后残留孤儿 pending cp——Cancel 的 abort 与在途 Resume 失序（并发 abort 跑在 SaveNS 之前），原 audit MED 行为")
	}
	e.mu.Lock()
	n := len(e.tasks)
	e.mu.Unlock()
	if n != 0 {
		t.Fatalf("Cancel 后映射应清空，残留 %d 条目", n)
	}
}

// TestIdleCancel_CleansSession 验：input-required 后客户端 idle-Cancel（不续发，走 Cancel 的 !busy
// 分支）时 executor 接管清掉该会话的挂起记录——conclude 的 CleanupSession 只覆盖续发收尾路径，
// idle-Cancel 不经 conclude，不清则被放弃的 HITL task 的挂起记录要等过期才清。
// sanity-revert：删 !busy 分支的 CleanupSession → PendingInterrupts()!=0 → FAIL。
func TestIdleCancel_CleansSession(t *testing.T) {
	obs := observability.New(
		observability.WithTracerProvider(tracenoop.NewTracerProvider()),
		observability.WithMeterProvider(metricnoop.NewMeterProvider()),
	)

	gm := &reAskGateModel{gate: make(chan struct{}), hit: make(chan struct{})}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("gated"),
		agent_v2.WithModel(gm),
		agent_v2.WithAskUser(),
		agent_v2.WithHooks(obs.AgentHooks()), // OnAgentEnd 在中断时记挂起
	)
	e := NewExecutor(ag, WithExecutorObservability(obs), WithOwnerResolver(func(context.Context) string { return "" }))
	ctx := context.Background()
	tid := a2atypes.TaskID("t-idle-cancel")

	// 1. 新 task → ask_user 中断 → input-required（挂起记录留着待续发）
	newCtx := &a2asrv.ExecutorContext{
		TaskID: tid, ContextID: "c-idle",
		Message: a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("ask me")),
	}
	evs := drain(func(y func(a2atypes.Event, error) bool) { e.Execute(ctx, newCtx)(y) })
	if got := lastStatus(evs); got != a2atypes.TaskStateInputRequired {
		t.Fatalf("应停在 input-required, got %s", got)
	}
	if n := obs.PendingInterrupts(); n != 1 {
		t.Fatalf("input-required 应留一条挂起记录待续发，PendingInterrupts=%d 应=1", n)
	}

	// 2. 客户端放弃 idle prompt → Cancel（不续发，无在途 → 走 Cancel 的 !busy 分支）
	cancelCtx := &a2asrv.ExecutorContext{TaskID: tid, ContextID: "c-idle"} // Message nil = cancellation
	_ = drain(func(y func(a2atypes.Event, error) bool) { e.Cancel(ctx, cancelCtx)(y) })

	// 3. 锚点：idle-Cancel 应清挂起记录（!busy 分支接管 CleanupSession，不经 conclude）
	if n := obs.PendingInterrupts(); n != 0 {
		t.Fatalf("idle-Cancel 应清挂起记录（Cancel 的 !busy 分支接管 CleanupSession），残留 %d 条", n)
	}
}

// TestExecute_OwnerIsolatesTasks 钉住：一个调用方**够不着**另一个调用方挂起的 task，
// 即使它递上完全正确的 taskId。
//
// A2A 的 taskId 可以由客户端给（SDK 收到非空 taskId 就去 taskStore 查、查到就续，
// 那个 Get 不做归属过滤）。没有 owner 维时，知道 taskId 就能续别人挂起的 task：
// 读到里面等人答的提问、替对方回答。
//
// ⚠️ 断言两头都要：B 够不着（failed，且**不泄漏**「存在但不是你的」）+ A 自己续得上
// （没有正向断言的话，「两边都够不着」也能让用例绿）。
func TestExecute_OwnerIsolatesTasks(t *testing.T) {
	// resolver 从 ctx 取——模拟认证中间件放主体。
	type whoKey struct{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("gated"),
		agent_v2.WithModel(&askThenDone{}),
		agent_v2.WithAskUser(),
	)
	e := NewExecutor(ag, WithOwnerResolver(func(ctx context.Context) string {
		o, _ := ctx.Value(whoKey{}).(string)
		return o
	}))
	asUser := func(who string) context.Context {
		return context.WithValue(context.Background(), whoKey{}, who)
	}
	tid := a2atypes.TaskID("t-owned")

	// 租户 A 新 task → ask_user 中断挂起。
	newCtx := &a2asrv.ExecutorContext{
		TaskID: tid, ContextID: "c-1",
		Message: a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("ask me")),
	}
	evs := drain(func(y func(a2atypes.Event, error) bool) { e.Execute(asUser("tenant-A"), newCtx)(y) })
	if got := lastStatus(evs); got != a2atypes.TaskStateInputRequired {
		t.Fatalf("前提不成立：第一轮应停在 input-required, got %s", got)
	}

	contCtx := func() *a2asrv.ExecutorContext {
		return &a2asrv.ExecutorContext{
			TaskID: tid, ContextID: "c-1",
			StoredTask: &a2atypes.Task{ID: tid, ContextID: "c-1"},
			Message: a2atypes.NewMessageForTask(a2atypes.MessageRoleUser,
				a2atypes.TaskInfo{TaskID: tid, ContextID: "c-1"}, a2atypes.NewTextPart("回答")),
		}
	}

	// 租户 B 拿同一个 taskId 续发 → 应 failed，且错误正文跟「task 不存在」一模一样
	//（区分「不存在」和「不是你的」本身就是信息泄漏，能拿来探测别人的 taskId）。
	bEvs := drain(func(y func(a2atypes.Event, error) bool) { e.Execute(asUser("tenant-B"), contCtx())(y) })
	if got := lastStatus(bEvs); got != a2atypes.TaskStateFailed {
		t.Fatalf("租户 B 用 A 的 taskId 续到了 A 的挂起 task（status=%s，应 failed）——task 表没有 owner 维", got)
	}
	if msg := lastMessageText(bEvs); !strings.Contains(msg, "no resumable state") {
		t.Errorf("拒绝文案应与「task 不存在」一致（不泄漏归属）：%q", msg)
	}

	// 租户 A 自己续 → 应完成。
	aEvs := drain(func(y func(a2atypes.Event, error) bool) { e.Execute(asUser("tenant-A"), contCtx())(y) })
	if got := lastStatus(aEvs); got != a2atypes.TaskStateCompleted {
		t.Fatalf("租户 A 续自己的 task 失败了（status=%s）——owner 维把合法续接也挡了", got)
	}
}

// lastMessageText 取最后一个 status-update 事件里的消息文本。
func lastMessageText(evs []a2atypes.Event) string {
	out := ""
	for _, ev := range evs {
		if v, ok := ev.(*a2atypes.TaskStatusUpdateEvent); ok && v.Status.Message != nil {
			if txt, _ := a2apkg.PartsToContent(v.Status.Message.Parts); txt != "" {
				out = txt
			}
		}
	}
	return out
}

// askThenDone：第 1 次 ask_user，之后直接答。
type askThenDone struct {
	mu sync.Mutex
	n  int
}

func (m *askThenDone) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.n++
	if m.n == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc0", Type: "function",
			Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"Q1"}`},
		}}), nil
	}
	return schema.AssistantMessage("done"), nil
}

func (m *askThenDone) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(msg)
	schema.CloseSend(sr)
	return sr, nil
}

func (m *askThenDone) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// notPersistedComboAgent 是自研 agent：把中断跟 ErrCheckpointNotPersisted 绑在一条链上交回来。
type notPersistedComboAgent struct{}

func (notPersistedComboAgent) Name() string { return "combo" }
func (notPersistedComboAgent) Description() string {
	return "returns an interrupt bound with ErrCheckpointNotPersisted"
}

func (notPersistedComboAgent) Run(context.Context, *agent_v2.RunSession, *schema.Message, ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	ie := &compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "q1", Question: "Q?"}}
	return nil, fmt.Errorf("%w (%w)", ie, compose_v2.ErrCheckpointNotPersisted)
}

func (notPersistedComboAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, errors.New("not implemented")
}

// TestExecute_NotPersistedComboNotKeptPending：自研 agent 把中断跟档没写成绑在一条链上交回来时，task 不挂成
// input-required（远端答了也续不上），跟陈旧写回、身份冲突同样处置。
func TestExecute_NotPersistedComboNotKeptPending(t *testing.T) {
	e := NewExecutor(notPersistedComboAgent{}, WithOwnerResolver(func(context.Context) string { return "owner" }))
	tid := a2atypes.TaskID("t-combo")
	evs := drain(func(y func(a2atypes.Event, error) bool) {
		e.Execute(context.Background(), &a2asrv.ExecutorContext{
			TaskID: tid, ContextID: "c-combo",
			Message: a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("hi")),
		})(y)
	})
	if got := lastStatus(evs); got != a2atypes.TaskStateFailed {
		t.Fatalf("档没写成的中断不该挂成 input-required，实际 %s", got)
	}
	e.mu.Lock()
	n := len(e.tasks)
	e.mu.Unlock()
	if n != 0 {
		t.Fatalf("不该留着待续发的 task 条目，实际 %d 条", n)
	}
}

// failSaveCP 的 SaveNS 一律报错（存储连不上）。
type failSaveCP struct {
	*compose_v2.InMemoryCheckpointer
}

func (failSaveCP) SaveNS(context.Context, string, string, *compose_v2.Checkpoint) error {
	return errors.New("dial tcp 10.0.0.7:5432: connection refused")
}

// TestExecute_CheckpointNotPersistedFailsTask：中断时存档没写成，task 以 failed 收尾，不挂成
// input-required（远端答完续不上）；文案不带后端原文；挂起状态清掉，续发得「没有可续的状态」。
func TestExecute_CheckpointNotPersistedFailsTask(t *testing.T) {
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("gated"),
		agent_v2.WithModel(&askThenDone{}),
		agent_v2.WithAskUser(),
	)
	e := NewExecutor(ag, WithOwnerResolver(func(context.Context) string { return "owner" }))
	e.cp = failSaveCP{compose_v2.NewInMemoryCheckpointer()}
	tid := a2atypes.TaskID("t-np")
	evs := drain(func(y func(a2atypes.Event, error) bool) {
		e.Execute(context.Background(), &a2asrv.ExecutorContext{
			TaskID: tid, ContextID: "c-np",
			Message: a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("ask me")),
		})(y)
	})
	if got := lastStatus(evs); got != a2atypes.TaskStateFailed {
		t.Fatalf("存档没写成，task 应以 failed 收尾（不该挂成 input-required），实际 %s", got)
	}
	msg := lastMessageText(evs)
	if !strings.Contains(msg, "could not be saved") || strings.Contains(msg, "connection refused") {
		t.Errorf("文案应说明档没写成、不带后端原文：%q", msg)
	}
	cont := &a2asrv.ExecutorContext{
		TaskID: tid, ContextID: "c-np",
		StoredTask: &a2atypes.Task{ID: tid, ContextID: "c-np"},
		Message: a2atypes.NewMessageForTask(a2atypes.MessageRoleUser,
			a2atypes.TaskInfo{TaskID: tid, ContextID: "c-np"}, a2atypes.NewTextPart("回答")),
	}
	cEvs := drain(func(y func(a2atypes.Event, error) bool) { e.Execute(context.Background(), cont)(y) })
	if m := lastMessageText(cEvs); !strings.Contains(m, "no resumable state") {
		t.Errorf("挂起状态应已清掉，续发应得「没有可续的状态」，实际 %q", m)
	}
}

// errModel 每次调用都返同一个错误。
type errModel struct{ err error }

func (m *errModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, m.err
}

func (m *errModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, m.err
}

func (m *errModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestServerSpan_TimeoutAndCancelOutcomes 钉住：a2a server span 的结局跟 agent span 同一个口径——执行 ctx 结束时
// 按 ctx 判（取消不是失败）；ctx 还活着时错误链上的软取消是取消、DeadlineExceeded 是超时，context.Canceled 不算
// 取消（别处被连带取消，这次执行失败了）。普通错误仍是 a2a_agent_error。
func TestServerSpan_TimeoutAndCancelOutcomes(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	cases := []struct {
		name, outcome, errType string
		ctx                    context.Context
		err                    error
	}{
		{"执行 ctx 取消", observability.OutcomeCancelled, "", canceled, fmt.Errorf("upstream: %w", context.Canceled)},
		{"错误链上的超时", observability.OutcomeTimeout, "timeout", context.Background(), fmt.Errorf("upstream: %w", context.DeadlineExceeded)},
		{"软取消", observability.OutcomeCancelled, "", context.Background(), fmt.Errorf("stop: %w", compose_v2.ErrSoftCanceled)},
		{"错误链上的取消", observability.OutcomeError, "a2a_agent_error", context.Background(), fmt.Errorf("sibling: %w", context.Canceled)},
		{"普通错误", observability.OutcomeError, "a2a_agent_error", context.Background(), errors.New("boom")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
			obs := observability.New(observability.WithTracerProvider(tp),
				observability.WithMeterProvider(metricnoop.NewMeterProvider()))
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("failing"), agent_v2.WithModel(&errModel{err: c.err}))
			e := NewExecutor(ag, WithExecutorObservability(obs), WithOwnerResolver(func(context.Context) string { return "" }))
			execCtx := &a2asrv.ExecutorContext{TaskID: a2atypes.TaskID("t-" + c.name), ContextID: "c",
				Message: a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("hi"))}
			_ = drain(func(y func(a2atypes.Event, error) bool) { e.Execute(c.ctx, execCtx)(y) })

			var srv sdktrace.ReadOnlySpan
			for _, s := range rec.Ended() {
				if s.Name() == "a2a.server message_stream" {
					srv = s
				}
			}
			if srv == nil {
				t.Fatal("应有 a2a server span")
			}
			attrs := map[string]string{}
			for _, a := range srv.Attributes() {
				attrs[string(a.Key)] = a.Value.Emit()
			}
			if attrs["crew.outcome"] != c.outcome {
				t.Errorf("crew.outcome=%q 应=%q", attrs["crew.outcome"], c.outcome)
			}
			if attrs["error.type"] != c.errType {
				t.Errorf("error.type=%q 应=%q", attrs["error.type"], c.errType)
			}
		})
	}
}
