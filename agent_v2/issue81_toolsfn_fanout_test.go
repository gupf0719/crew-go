package agent_v2_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	skillPkg "crew-go/skill"
	"crew-go/tool"
)

// 防止 sort/atomic unused 警告 (其他测试用)
var _ = sort.Strings
var _ = atomic.Int32{}

// type alias 避免 issue81 测试名空间污染
type skillPkg_SkillDescriptor = skillPkg.SkillDescriptor
type skillPkg_SkillSource = skillPkg.SkillSource

// barrierTool 用 WaitGroup barrier 强制 N 个 tool 必须**同时**进入 Call 才能继续——
// 验证真并发(若 toolsFn 串行执行,N-1 个永远等不到 barrier release,wg.Wait 超时)。
type barrierTool struct {
	name      string
	barrier   *sync.WaitGroup
	called    atomic.Int32
	releaseCh chan struct{}
}

func (t *barrierTool) Name() string                { return t.name }
func (t *barrierTool) Description() string         { return "barrier tool" }
func (t *barrierTool) InputSchema() map[string]any { return map[string]any{} }
func (t *barrierTool) Call(ctx context.Context, args map[string]any) (*tool.Result, error) {
	t.called.Add(1)
	t.barrier.Done() // 报到
	// 等所有 N 个都报到 (release channel close)
	select {
	case <-t.releaseCh:
	case <-ctx.Done():
		return tool.Failure("ctx_done", "ctx done before barrier release"), nil
	case <-time.After(2 * time.Second):
		return tool.Failure("timeout", "barrier timeout, fanout 没生效?"), nil
	}
	return tool.Success(fmt.Sprintf("done_%s", t.name)), nil
}

var _ tool.Tool = (*barrierTool)(nil)

// TestISSUE81_Concurrent_NToolCalls_AllRun 验 N>=2 同 LLM 轮 tc 都并发跑——
// barrier 强制 N tool 必须同时进入 Call,否则 timeout fail。
//
// **bug 历史**: makeToolsFn 旧实现 for 循环串行 → tc1 调 Call 先卡 barrier
// 等 tc2 报到, 但 tc2 永远不会跑 → 测试 timeout。
//
// **fix sanity-revert**: makeToolsFn 改回 for 循环 → 测试 fail "barrier timeout"。
func TestISSUE81_Concurrent_NToolCalls_AllRun(t *testing.T) {
	const N = 4
	barrier := &sync.WaitGroup{}
	barrier.Add(N)
	releaseCh := make(chan struct{})

	tk := agent_v2_newToolkit()
	tools := make([]*barrierTool, N)
	for i := 0; i < N; i++ {
		tools[i] = &barrierTool{
			name:      fmt.Sprintf("bt_%d", i),
			barrier:   barrier,
			releaseCh: releaseCh,
		}
		tk.Register(tools[i])
	}

	// barrier waiter: 后台等所有 N 报到后 release
	go func() {
		barrier.Wait()
		close(releaseCh)
	}()

	// LLM 一轮 emit N 个 tool_call
	tcs := make([]schema.ToolCall, N)
	for i := 0; i < N; i++ {
		tcs[i] = schema.ToolCall{
			ID:   fmt.Sprintf("tc_%d", i),
			Type: "function",
			Function: schema.FunctionCall{
				Name:      fmt.Sprintf("bt_%d", i),
				Arguments: `{}`,
			},
		}
	}
	stub := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage(tcs),
		schema.AssistantMessage("done"),
	})

	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("p"),
		agent_v2.WithModel(stub),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5),
	)

	start := time.Now()
	_, err := parent.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	dur := time.Since(start)

	if err != nil {
		t.Fatalf("Run err: %v (dur=%v)", err, dur)
	}
	for i, bt := range tools {
		if bt.called.Load() != 1 {
			t.Errorf("tool[%d] %q called %d times, want 1 (barrier=%v)", i, bt.name, bt.called.Load(), barrier)
		}
	}
	// **核心断言**: 真并发 dur 应远小于 N * 2s timeout——串行执行下各 tool barrier
	// 永远等不齐 (前面的 tool 等 release, 后面的 tool 还没进 Call), 各超时 2s 共
	// N*2s=8s。fanout 化后所有 N 同时进 Call 报到, release 立即触发, 总 dur < 1s。
	if dur > 1*time.Second {
		t.Errorf("ISSUE-81 fix sanity: dur=%v 超过 1s, 疑似没真并发\n"+
			"sanity-revert: makeToolsFn 改回 for 串行 → 各 tool barrier 等不齐 timeout 2s × %d → dur ≥ %ds → 测试 fail",
			dur, N, N*2)
	}
	t.Logf("ISSUE-81 fanout PASS: %d tools 并发 release, dur=%v (串行会 ≈ %ds)", N, dur, N*2)
}

// orderedTool 按指定 sleep 时间反向完成——验证 tool_result 按 tc.ID 顺序
// （非完成时间）保 OpenAI 配对协议。
type orderedTool struct {
	name  string
	sleep time.Duration
}

func (t *orderedTool) Name() string                { return t.name }
func (t *orderedTool) Description() string         { return "ordered tool" }
func (t *orderedTool) InputSchema() map[string]any { return map[string]any{} }
func (t *orderedTool) Call(ctx context.Context, args map[string]any) (*tool.Result, error) {
	time.Sleep(t.sleep)
	return tool.Success(t.name), nil
}

var _ tool.Tool = (*orderedTool)(nil)

// TestISSUE81_OrderPreserved_ToolResultPairing 验 4 tc 不同 sleep 完成顺序
// 倒序但 tool_result 按 tc 提交顺序 (tc_a, tc_b, tc_c, tc_d) 保 OpenAI/Claude
// API 配对要求。
//
// 不直接用 stubModel 因为它返固定，需要 capture msgs 验顺序——直接 unit test
// runOne 不可（unexported）。改用集成测试 via parent.Run。
//
// 实际上 captureToolMsgsModel 太复杂——简化为直接验 stubModel 视角的 ToolResult
// 顺序: 用一个特殊 stub 在 r2 时检查 msgs 含的 tool_result 顺序。
func TestISSUE81_OrderPreserved_ToolResultPairing(t *testing.T) {
	tk := agent_v2_newToolkit()
	// 故意反向 sleep——完成顺序: fast(0ms) → mid(20ms) → slow(40ms) → slowest(60ms)
	tk.Register(&orderedTool{name: "fast", sleep: 0})
	tk.Register(&orderedTool{name: "slow", sleep: 40 * time.Millisecond})
	tk.Register(&orderedTool{name: "mid", sleep: 20 * time.Millisecond})
	tk.Register(&orderedTool{name: "slowest", sleep: 60 * time.Millisecond})

	// 用 capturer stub 收集 r2 时的 msgs 顺序
	capturedOrder := []string{}
	var capMu sync.Mutex
	captureStub := &orderCaptureStub{
		responses: []*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "tc_a", Type: "function", Function: schema.FunctionCall{Name: "fast", Arguments: `{}`}},
				{ID: "tc_b", Type: "function", Function: schema.FunctionCall{Name: "slow", Arguments: `{}`}},
				{ID: "tc_c", Type: "function", Function: schema.FunctionCall{Name: "mid", Arguments: `{}`}},
				{ID: "tc_d", Type: "function", Function: schema.FunctionCall{Name: "slowest", Arguments: `{}`}},
			}),
			schema.AssistantMessage("done"),
		},
		onR2: func(msgs []*schema.Message) {
			capMu.Lock()
			defer capMu.Unlock()
			for _, m := range msgs {
				if m != nil && m.Role == schema.RoleTool {
					capturedOrder = append(capturedOrder, m.Content)
				}
			}
		},
	}

	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("p"),
		agent_v2.WithModel(captureStub),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5),
	)
	_, err := parent.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	capMu.Lock()
	defer capMu.Unlock()
	want := []string{"fast", "slow", "mid", "slowest"}
	if len(capturedOrder) != len(want) {
		t.Fatalf("expected 4 tool_result, got %d (%v)", len(capturedOrder), capturedOrder)
	}
	for i, w := range want {
		if capturedOrder[i] != w {
			t.Errorf("ISSUE-81 order: tool_result[%d] = %q, want %q (顺序按 tc 提交顺序而非完成时间)\n"+
				"完整顺序: %v\nsanity-revert: 改 makeToolsFn 用 chan + completion 顺序 append → 顺序=完成时间序: [fast, mid, slow, slowest]",
				i, capturedOrder[i], w, capturedOrder)
		}
	}
}

// orderCaptureStub 简化的 stub model 支持 onR2 回调
type orderCaptureStub struct {
	mu        sync.Mutex
	calls     int
	responses []*schema.Message
	onR2      func(msgs []*schema.Message)
}

func (m *orderCaptureStub) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	m.calls++
	c := m.calls
	m.mu.Unlock()
	if c == 2 && m.onR2 != nil {
		m.onR2(msgs)
	}
	if c-1 < len(m.responses) {
		return m.responses[c-1], nil
	}
	return schema.AssistantMessage("end"), nil
}
func (m *orderCaptureStub) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *orderCaptureStub) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

var _ model.ChatModel = (*orderCaptureStub)(nil)

// interruptingTool 直接抛 InterruptError 让 toolsFn 收集
type interruptingTool struct {
	name     string
	question string
}

func (t *interruptingTool) Name() string                { return t.name }
func (t *interruptingTool) Description() string         { return "interrupt tool" }
func (t *interruptingTool) InputSchema() map[string]any { return map[string]any{} }
func (t *interruptingTool) Call(ctx context.Context, args map[string]any) (*tool.Result, error) {
	return nil, &compose_v2.InterruptError{
		Reason: "ask user " + t.name,
		Query: &compose_v2.AskQuery{
			// **自填**：框架不再替业务补空 ID（补出来的与分支自己算的对不上，实测会永不
			// 收敛并重放副作用）。业务提问的推荐写法是 `compose_v2.Ask`；这个 stub 只是
			// 借中断验 fanout 的聚合/优先级，用工具名当稳定 ID 足够区分。
			InterruptID: "ask_" + t.name,
			Question:    t.question,
		},
	}
}

var _ tool.Tool = (*interruptingTool)(nil)

// TestISSUE81_MultiInterrupts_MergedAndRecorded 验 N tc 同时抛 InterruptError 时
// MergeInterrupts 聚合 + graph_loop AllQueries 协议 record 全部 query 不丢。
//
// **bug 历史**: 旧 toolsFn 串行 + early return → 第 1 个 tc 抛 Interrupt 后续不跑 →
// 单 ie.Query 单 cm.recordInterrupt → 业务无法同时答 N 个 ask_user。
//
// **fix sanity-revert**: 删 MergeInterrupts 改 return interrupts[0] → cm
// PendingInterrupts 仅 1 query → 测试 fail "expected 3 pending got 1"。
func TestISSUE81_MultiInterrupts_MergedAndRecorded(t *testing.T) {
	tk := agent_v2_newToolkit()
	tk.Register(&interruptingTool{name: "ask1", question: "Q1?"})
	tk.Register(&interruptingTool{name: "ask2", question: "Q2?"})
	tk.Register(&interruptingTool{name: "ask3", question: "Q3?"})

	stub := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "tc_x", Type: "function", Function: schema.FunctionCall{Name: "ask1", Arguments: `{}`}},
			{ID: "tc_y", Type: "function", Function: schema.FunctionCall{Name: "ask2", Arguments: `{}`}},
			{ID: "tc_z", Type: "function", Function: schema.FunctionCall{Name: "ask3", Arguments: `{}`}},
		}),
		schema.AssistantMessage("done"),
	})

	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("p"),
		agent_v2.WithModel(stub),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5),
	)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := parent.Run(ctx, sess, schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("Run 应抛 Interrupt, got nil")
	}
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("应返 InterruptError, got %v (%T)", err, err)
	}

	// 核心断言: ie.AllQueries() 应含 3 个 query (不是单 query!)
	queries := ie.AllQueries()
	if len(queries) != 3 {
		t.Fatalf("ISSUE-81 fix: 3 tc 同时抛 Interrupt 应聚合到 ie.AllQueries 含 3 query;\n"+
			"实际 %d (queries=%+v)\nsanity-revert: 改 return interrupts[0] 不走 MergeInterrupts → 仅 1 query → 测试 fail",
			len(queries), queries)
	}

	// ID 全 distinct——不然业务无法逐个配答案。
	//
	// ⚠️ **这条在这个 fixture 里不承重**：三个 ID 是 `"ask_" + 工具名` 拼的，工具名本来
	// 就互不相同 ⟹ 断言恒真。留着是因为「聚合出来的这批得能逐个配答案」是本例的语义前提，
	// 但**真正守「框架算的 ID 会不会撞」的哨兵在别处**：
	// 分支实例维在 `compose_v2.TestFanout_AllInterruptsReturnedToCaller`（走 `Ask`），
	// tc 下标维在 `tool_call_idx_test.go`。
	idMap := make(map[string]bool)
	for _, q := range queries {
		idMap[q.InterruptID] = true
	}
	if len(idMap) != len(queries) {
		t.Errorf("%d 个 query 只有 %d 个不同的 InterruptID（idMap=%v）——业务无法逐个配答案",
			len(queries), len(idMap), idMap)
	}

	// 验 Question 含 3 个 (按 tc 顺序但 MergeInterrupts 不保证顺序——只验集合)
	gotQs := make([]string, 0, len(queries))
	for _, q := range queries {
		gotQs = append(gotQs, q.Question)
	}
	sort.Strings(gotQs)
	wantQs := []string{"Q1?", "Q2?", "Q3?"}
	for i, w := range wantQs {
		if i >= len(gotQs) || gotQs[i] != w {
			t.Errorf("Question[%d] = %q, want %q (sorted: got=%v want=%v)", i, gotQs[i], w, gotQs, wantQs)
		}
	}
}

// errReturningTool 返真异常 err
type errReturningTool struct{}

func (*errReturningTool) Name() string                { return "real_err" }
func (*errReturningTool) Description() string         { return "real err" }
func (*errReturningTool) InputSchema() map[string]any { return map[string]any{} }
func (*errReturningTool) Call(_ context.Context, _ map[string]any) (*tool.Result, error) {
	return nil, fmt.Errorf("非 Interrupt 业务错")
}

var _ tool.Tool = (*errReturningTool)(nil)

// TestISSUE81_InterruptPriorityOverBizErr 验多 tc 同时抛 Interrupt + 业务错时
// Interrupt 优先 propagate (HITL 控制流必须暂停)，业务错被 swallow (在 Resume
// 后 LLM 看 missing tool_result 自然重试)。
func TestISSUE81_InterruptPriorityOverBizErr(t *testing.T) {
	tk := agent_v2_newToolkit()
	tk.Register(&interruptingTool{name: "ask_int", question: "interrupt?"})
	tk.Register(&errReturningTool{})

	stub := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "tc_int", Type: "function", Function: schema.FunctionCall{Name: "ask_int", Arguments: `{}`}},
			{ID: "tc_err", Type: "function", Function: schema.FunctionCall{Name: "real_err", Arguments: `{}`}},
		}),
		schema.AssistantMessage("done"),
	})

	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("p"),
		agent_v2.WithModel(stub),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5),
	)

	_, err := parent.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil {
		t.Fatalf("Run 应抛 error (Interrupt 优先), got nil")
	}
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Errorf("ISSUE-81 fix: Interrupt 应优先 propagate over 业务错;\n"+
			"实际 err=%v (%T 非 InterruptError)", err, err)
	}
}

// TestISSUE81_NestedFanout_ParallelXBasicAgent 验证 audit C MED-2 — 三层并发场景:
// ParallelAgent (M branches goroutine) × 各 branch 内 BasicAgent (N tc fanout
// goroutine) = M*N goroutines 嵌套并发。
//
// **关键不变量**:
//   - lane events 隔离 (ISSUE-80) 让各 branch sub-agent.events 独立
//   - 各 branch sub-agent 内 N tc fanout 各自走 WithCurrentToolCallContext 独立 ctx
//   - 各 InterruptID graphNS 维度 (各 branch 独立 NS) + branchIdx 维度区分,N tc 内
//     ask_user ID 各 distinct
//   - 总 cm.PendingInterrupts 含 M*N distinct queries
//
// **bomb 防御**: sanity-revert 改 makeToolsFn 回 for 串行 → barrier timeout 各
// goroutine 各 timeout → 总 dur 大幅上升。
func TestISSUE81_NestedFanout_ParallelXBasicAgent(t *testing.T) {
	const M, N = 2, 3 // 2 branches × 3 tcs = 6 总并发

	// barrier 强制 M*N 全并发——若任一层串行,barrier 等不齐 timeout
	barrier := &sync.WaitGroup{}
	barrier.Add(M * N)
	releaseCh := make(chan struct{})

	makeBranch := func(branchName string) agent_v2.Agent {
		tk := agent_v2_newToolkit()
		for i := 0; i < N; i++ {
			tk.Register(&barrierTool{
				name:      fmt.Sprintf("%s_bt_%d", branchName, i),
				barrier:   barrier,
				releaseCh: releaseCh,
			})
		}
		tcs := make([]schema.ToolCall, N)
		for i := 0; i < N; i++ {
			tcs[i] = schema.ToolCall{
				ID:   fmt.Sprintf("%s_tc_%d", branchName, i),
				Type: "function",
				Function: schema.FunctionCall{
					Name:      fmt.Sprintf("%s_bt_%d", branchName, i),
					Arguments: `{}`,
				},
			}
		}
		stub := newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage(tcs),
			schema.AssistantMessage("done"),
		})
		return agent_v2.NewBasicAgent(
			agent_v2.WithName(branchName),
			agent_v2.WithModel(stub),
			agent_v2.WithToolkit(tk),
			agent_v2.WithMaxRounds(5),
		)
	}

	// barrier waiter: M*N 都报到后 release
	go func() {
		barrier.Wait()
		close(releaseCh)
	}()

	pa := agent_v2.NewParallelAgent(agent_v2.WithParallelName("pa"))
	for i := 0; i < M; i++ {
		pa.AddBranch(fmt.Sprintf("br_%d", i), makeBranch(fmt.Sprintf("br_%d", i)))
	}

	start := time.Now()
	_, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("nested fanout"))
	dur := time.Since(start)

	if err != nil {
		t.Fatalf("Run err: %v (dur=%v)", err, dur)
	}

	// 真三层并发 dur 应 < 1s——各 barrier 同时报到 release 立即触发
	// 串行下 (M*N 各 timeout 2s): dur >= M*N*2s = 12s
	if dur > 2*time.Second {
		t.Errorf("ISSUE-81 audit C MED-2 fix: 三层 fanout dur=%v 超 2s, 疑似某层未真并发\n"+
			"sanity-revert: makeToolsFn 回 for 串行 → barrier 等不齐 → dur >= %ds → 测试 fail",
			dur, M*N*2)
	}
	t.Logf("ISSUE-81 nested fanout PASS: M=%d × N=%d = %d goroutines 并发 release, dur=%v (串行会 ≈ %ds)",
		M, N, M*N, dur, M*N*2)
}

// TestISSUE81_InlineBody_MultipleTcs_OrderPreserved 验证 audit C LOW-1 — N tc
// 都走 P2.D inline body (activate_skill) 时 messages 顺序 = [tc0_ack, tc1_ack,
// tc2_ack, tc0_body, tc1_body, tc2_body] 不被 user role 中断 (R16 audit HIGH-1
// 协议保留)。
//
// 用 skill_fork_test.go 的 forkBox(无 fork Context, 默认 toolCall 模式) +
// InlineSkillBody=true 启 P2.D inline body 路径。
func TestISSUE81_InlineBody_MultipleTcs_OrderPreserved(t *testing.T) {
	box := &forkBox{
		skills: []skillPkg_SkillDescriptor{
			{Name: "skill_a", Description: "A"}, // 不设 Context → toolCall 模式
			{Name: "skill_b", Description: "B"},
			{Name: "skill_c", Description: "C"},
		},
		activations: map[string]string{
			"skill_a": "你是 skill_a, 做 X",
			"skill_b": "你是 skill_b, 做 Y",
			"skill_c": "你是 skill_c, 做 Z",
		},
	}

	// stub captureModel: r1 emit 3 activate_skill, r2 capture msgs 顺序
	capturedRoles := []string{}
	var capMu sync.Mutex
	captureStub := &orderCaptureStub{
		responses: []*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "tc_a", Type: "function", Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"skill_a"}`}},
				{ID: "tc_b", Type: "function", Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"skill_b"}`}},
				{ID: "tc_c", Type: "function", Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"skill_c"}`}},
			}),
			schema.AssistantMessage("done"),
		},
		onR2: func(msgs []*schema.Message) {
			capMu.Lock()
			defer capMu.Unlock()
			for _, m := range msgs {
				if m == nil {
					continue
				}
				capturedRoles = append(capturedRoles, string(m.Role))
			}
		},
	}

	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("p"),
		agent_v2.WithModel(captureStub),
		agent_v2.WithSkills(skillPkg_SkillSource{SkillBox: box}),
		agent_v2.WithInlineSkillBody(true), // P2.D inline body 模式
		agent_v2.WithMaxRounds(5),
	)
	_, err := parent.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	capMu.Lock()
	defer capMu.Unlock()
	// 期望: tool_results 连续 3 段后跟 user(body)——不被 user role 中断
	toolStart, toolEnd := -1, -1
	for i, role := range capturedRoles {
		if role == "tool" {
			if toolStart == -1 {
				toolStart = i
			}
			toolEnd = i
		}
	}
	if toolStart == -1 || toolEnd-toolStart+1 != 3 {
		t.Errorf("ISSUE-81 audit C LOW-1: 期望连续 3 个 tool messages, 实际 [%d..%d] (roles=%v)\n"+
			"R16 audit 协议: tool_results 必须连续不被 user role 中断",
			toolStart, toolEnd, capturedRoles)
		return
	}
	// tool 段后应是 user (body) 们,不应再有 tool
	for i := toolEnd + 1; i < len(capturedRoles); i++ {
		if capturedRoles[i] == "tool" {
			t.Errorf("ISSUE-81 audit C LOW-1: tool 段[%d..%d] 后位置%d 又出现 tool, 违反 [all tools, then all bodies] 协议;\nroles=%v",
				toolStart, toolEnd, i, capturedRoles)
		}
	}
}

// TestISSUE81_PartialAnswer_PartialResume_NarrowsToRemaining 验 audit #5 视角 9
// F11 fix: partial answer multi-Resume 协议——3 tc fanout (用 ask_user 内置工具
// 读 ResumeContext) → cp 含 3 query → Resume 答 2 个 → 已答 tc 拿到 answer 返
// tool result 不抛, 未答 tc re-raise 同 InterruptID → 业务可多轮 Resume 收敛。
//
// **why 用 ask_user 而非 interruptingTool**: interruptingTool 无脑每次抛 Interrupt
// 不读 ResumeContext, Resume 后还是 3 个 query (业务 anti-pattern). ask_user 走
// GetResumeAnswer ctx 拿答案 — Resume 路径正确收敛.
//
// **why 加测试**: 此前 TestISSUE81_MultiInterrupts_MergedAndRecorded 只测 Run 1
// (3 tc merge → 3 pending IDs), 未测 partial Resume 路径——未来 MergeInterrupts
// dedupe / clearInterrupts 改动可能 silent 破坏此路径 (无 fail signal).
func TestISSUE81_PartialAnswer_PartialResume_NarrowsToRemaining(t *testing.T) {
	// 用 ask_user 内置工具 (读 ResumeContext) 替 interruptingTool 让 Resume 收敛
	stub := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "tc_x", Type: "function", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"Q1?"}`}},
			{ID: "tc_y", Type: "function", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"Q2?"}`}},
			{ID: "tc_z", Type: "function", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"Q3?"}`}},
		}),
		schema.AssistantMessage("done"),
	})

	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("p_partial"),
		agent_v2.WithModel(stub),
		agent_v2.WithMaxRounds(5),
		agent_v2.WithAskUser(),
	)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// Run 1: 3 tc 全 fanout 抛 Interrupt
	_, err := parent.Run(ctx, sess, schema.UserMessage("hi"))
	ie1, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("Run 1 应抛 Interrupt, got %v", err)
	}
	queries1 := ie1.AllQueries()
	if len(queries1) != 3 {
		t.Fatalf("Run 1 应聚合 3 query, 实际 %d", len(queries1))
	}

	// 记 q1 / q2 InterruptID (答 2/3 个)
	var idQ1, idQ2, idQ3 string
	for _, q := range queries1 {
		switch q.Question {
		case "Q1?":
			idQ1 = q.InterruptID
		case "Q2?":
			idQ2 = q.InterruptID
		case "Q3?":
			idQ3 = q.InterruptID
		}
	}
	if idQ1 == "" || idQ2 == "" || idQ3 == "" {
		t.Fatalf("ID 缺失: q1=%q q2=%q q3=%q", idQ1, idQ2, idQ3)
	}

	// Resume 1: 只答 Q1 + Q2, Q3 未答
	_, err = parent.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{
			idQ1: schema.UserMessage("ans1"),
			idQ2: schema.UserMessage("ans2"),
		},
	))
	ie2, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("Resume 1 应再抛 Interrupt (Q3 未答 re-raise), got %v", err)
	}
	queries2 := ie2.AllQueries()
	// 应只剩 1 个未答 query (idQ3)
	if len(queries2) != 1 {
		t.Fatalf("partial Resume 后应只剩 1 未答 query, 实际 %d: %+v", len(queries2), queries2)
	}
	if queries2[0].Question != "Q3?" {
		t.Errorf("未答 query 应是 Q3?, 实际 %q", queries2[0].Question)
	}
	// InterruptID 跨 Resume 稳定 (visit 不漂)
	if queries2[0].InterruptID != idQ3 {
		t.Errorf("Q3 InterruptID 跨 Resume 应稳定, Run1=%q Resume1=%q", idQ3, queries2[0].InterruptID)
	}

	// Resume 2: **累积传所有 3 答案** (业务侧前端持答案累积, 见
	// TestAskUserTool_MultiPendingNoCrosstalk 同款模式) → 成功
	_, err = parent.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{
			idQ1: schema.UserMessage("ans1"),
			idQ2: schema.UserMessage("ans2"),
			idQ3: schema.UserMessage("ans3"),
		},
	))
	if err != nil {
		t.Fatalf("Resume 2 (累积答完所有) 应成功, got %v", err)
	}
}

// captureStubModel 跟 stubModel 同款但记录每次 Generate 收到的 msgs, 让测试可
// 断言 Resume 路径 message 流 (e.g., partial answer Resume 2 时 stub 应收到 N
// 个 tool_result).
type captureStubModel struct {
	responses [][]*schema.Message // captured msgs per call
	out       []*schema.Message
	idx       int
}

func newCaptureStubModel(out []*schema.Message) *captureStubModel {
	return &captureStubModel{out: out}
}

func (m *captureStubModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	// 深拷贝 msgs 防业务后续 mutate
	captured := make([]*schema.Message, len(msgs))
	copy(captured, msgs)
	m.responses = append(m.responses, captured)
	if m.idx >= len(m.out) {
		return schema.AssistantMessage("[stub exhausted]"), nil
	}
	r := m.out[m.idx]
	m.idx++
	return r, nil
}

func (m *captureStubModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *captureStubModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*captureStubModel)(nil)

// TestISSUE81_PartialAnswer_MessageFlow 验 audit #5 阶段 2 测试 MED-2:
// partial answer Resume 时 stub model 应收到 N 个 tool_result message (已答 tc
// 的 tool result + 重抛 tc 的 tool_result 不在), 验证 Resume 不 silent 丢 message
// 流.
//
// 之前 TestISSUE81_PartialAnswer_PartialResume_NarrowsToRemaining 只验证控制流
// (PendingInterrupts count) 不验 message 流——未来 Resume 路径 silent 丢已答
// tool_result 的 bug 不会被抓.
func TestISSUE81_PartialAnswer_MessageFlow(t *testing.T) {
	stub := newCaptureStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "tc_x", Type: "function", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"Q1?"}`}},
			{ID: "tc_y", Type: "function", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"Q2?"}`}},
			{ID: "tc_z", Type: "function", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"Q3?"}`}},
		}),
		schema.AssistantMessage("done"),
	})

	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("p_msgflow"),
		agent_v2.WithModel(stub),
		agent_v2.WithMaxRounds(5),
		agent_v2.WithAskUser(),
	)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// Run 1: stub 收到 1 次 (initial user msg)
	_, err := parent.Run(ctx, sess, schema.UserMessage("hi"))
	ie1, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("Run 1 应 Interrupt, got %v", err)
	}
	if len(stub.responses) != 1 {
		t.Errorf("Run 1 stub.Generate 应调 1 次 (initial), 实际 %d", len(stub.responses))
	}

	// 收集 ids
	var idQ1, idQ2, idQ3 string
	for _, q := range ie1.AllQueries() {
		switch q.Question {
		case "Q1?":
			idQ1 = q.InterruptID
		case "Q2?":
			idQ2 = q.InterruptID
		case "Q3?":
			idQ3 = q.InterruptID
		}
	}

	// Resume 1: 答 Q1+Q2, Q3 未答, 应 re-raise
	_, err = parent.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{
			idQ1: schema.UserMessage("ans1"),
			idQ2: schema.UserMessage("ans2"),
		},
	))
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("Resume 1 应再 Interrupt, got %v", err)
	}
	// Resume 1 stub.Generate 不应被调用 (toolsFn 重跑, 但 Q3 未答 → 直接抛 Interrupt
	// 整 toolsFn 失败, model 不重调). stub.responses 应仍是 1.
	if len(stub.responses) != 1 {
		t.Errorf("Resume 1 (q3 re-raise) model 不应重调, 实际 stub.responses=%d", len(stub.responses))
	}

	// Resume 2: 累积传所有 3 答案 → 全 tc 拿到答案返 tool_result → model 重调
	_, err = parent.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{
			idQ1: schema.UserMessage("ans1"),
			idQ2: schema.UserMessage("ans2"),
			idQ3: schema.UserMessage("ans3"),
		},
	))
	if err != nil {
		t.Fatalf("Resume 2 应成功, got %v", err)
	}
	// Resume 2 全 tc 成功 → toolsFn return tool_result → modelFn 调 1 次 (拿 "done")
	if len(stub.responses) != 2 {
		t.Fatalf("Resume 2 model 应调 1 次 (tool_result → modelFn), 总 stub.responses 应=2, 实际 %d", len(stub.responses))
	}

	// 核心验证: Resume 2 stub 第 2 次调用 msgs 应含 3 个 tool_result (按 idQ1/Q2/Q3 顺序)
	resume2Msgs := stub.responses[1]
	toolResultCount := 0
	gotAnswers := map[string]bool{}
	for _, m := range resume2Msgs {
		if m == nil {
			continue
		}
		if m.Role == "tool" {
			toolResultCount++
			gotAnswers[m.Content] = true
		}
	}
	if toolResultCount != 3 {
		t.Errorf("Resume 2 stub 应收 3 个 tool_result, 实际 %d (msgs: %d)", toolResultCount, len(resume2Msgs))
	}
	for _, want := range []string{"ans1", "ans2", "ans3"} {
		if !gotAnswers[want] {
			t.Errorf("Resume 2 stub 应收到 tool_result content=%q, 实际 gotAnswers=%v", want, gotAnswers)
		}
	}
}

// TestInterrupt_SeparateRunsRecordIndependently 两次独立 Run 的挂起各记各的，
// 复用同一个 ID 也互不影响。
//
// ⚠️ **这条用例的名字曾经是 `TestRecordInterrupt_DedupesByInterruptID_CrossNode`，
// 声称验「dedupe 在跨节点 / 跨超步也生效」——它从来没验到过**：Run A 用 cm、Run B 用
// cm3，两个独立 ChannelManager，而 dedupe 是 per-cm 状态，跨它俩根本走不到。实测把
// `recordInterrupt` 的整个去重循环短路，全仓测试照样绿。
//
// 改成它实际在验的东西（每个 Run 的批次原样落盘）。**去重机制本身目前零哨兵**，
// 而它在生产里唯一还活着的路径是「同超步两个分支各抛一条、ID 相同」——见
// doc/code/todo.md 那条。
func TestInterrupt_SeparateRunsRecordIndependently(t *testing.T) {
	g, err := compose_v2.NewBuilder().
		AddNode("node_a", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, &compose_v2.InterruptError{
				Reason: "node A asks",
				Query:  &compose_v2.AskQuery{Question: "A?", InterruptID: "shared_id"},
			}
		}).
		AddEdge(compose_v2.StartNode, "node_a").
		AddEdge("node_a", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// Run 1: node_a 抛 ie → cm 记 1 个 query
	err = compose_v2.Run(ctx, g, cm,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("cross_node_test"),
	)
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("Run 1 应 Interrupt, got %v", err)
	}
	pending1 := cm.PendingInterrupts()
	if len(pending1) != 1 {
		t.Fatalf("Run 1 应 1 个 pending, 实际 %d", len(pending1))
	}

	// 前置条件：Run 1 确实记了 shared_id
	if cm.PendingInterrupts()[0].InterruptID != "shared_id" {
		t.Fatalf("Run 1 应记 shared_id, 实际 %+v", cm.PendingInterrupts()[0])
	}
	// Run 2：另一个 graph、**另一个 ChannelManager**，其中一条复用 Run 1 用过的 ID。
	//
	// ⚠️ **这里不发生任何去重**——去重状态是 per-cm 的（`cm.pendingInterrupts`），
	// 两个独立 cm 之间够不着。本例验的是「各记各的」，不是「跨 Run 去重」。
	// （这四行里原先有三处在说 dedupe，全是假的：详见本函数上方 godoc。）
	gB, err := compose_v2.NewBuilder().
		AddNode("node_b", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, &compose_v2.InterruptError{
				Reason: "node B asks",
				Queries: []*compose_v2.AskQuery{
					{Question: "B-1?", InterruptID: "shared_id"}, // 跟 Run 1 同名，但互不影响
					{Question: "B-3?", InterruptID: "unique_b3"},
					// ⚠️ 这里原先还有一条批内也叫 shared_id 的（"B-2?"）——同一批内撞 ID
					// 现在是当场拒的协议违规，留着这个图跑不起来。批内撞的哨兵在
					// compose_v2.TestInterrupt_RejectsDuplicateIDsInSameBatch。
				},
			}
		}).
		AddEdge(compose_v2.StartNode, "node_b").
		AddEdge("node_b", compose_v2.EndNode).
		Compile()
	if err != nil {
		t.Fatalf("compile gB: %v", err)
	}

	cm3 := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, gB, cm3,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("cross_node_test_b"),
	)
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("Run B 应 Interrupt, got %v", err)
	}
	pendingB := cm3.PendingInterrupts()
	// node_b 抛 2 条不同 ID 的 query，两条都该原样落盘。
	//
	// ⚠️ 这里原先有第三条 B-2 跟 B-1 撞 ID、用来验 dedupe——**批内撞 ID 现在是当场拒的
	// 协议违规**，那条留着会让整个图跑不起来。而且这个用例本来也验不到 dedupe：Run A 用
	// 的是 cm、Run B 用的是 cm3，两个独立 ChannelManager，而 dedupe 是 per-cm 状态。
	if len(pendingB) != 2 {
		// 别用 %+v 打 []*AskQuery——那会印出一串指针地址，看不出丢了哪条
		got := make([]string, 0, len(pendingB))
		for _, q := range pendingB {
			got = append(got, q.InterruptID+"/"+q.Question)
		}
		t.Fatalf("node_b 的 2 条 query 都该落盘, 实际 %d: %v", len(pendingB), got)
	}
	gotIDs := make(map[string]bool)
	for _, q := range pendingB {
		gotIDs[q.InterruptID] = true
	}
	if !gotIDs["shared_id"] || !gotIDs["unique_b3"] {
		t.Errorf("应含 shared_id + unique_b3, 实际 %v", gotIDs)
	}
}
