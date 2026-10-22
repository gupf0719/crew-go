package agent_v2_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

// TestBasicAgent_StoreCtxPropagation 验证 PR 2 ctx 注入协议：
//
//	BasicAgent.Run 入口 push store.WithContext(ctx, c.Store) →
//	下游业务 tool / hook 内 store.FromContext(ctx) 真能拿到同款实例 +
//	tool 内 Put 后 outer 端 Get 真持久化。
//
// 设计：tool 模拟"业务保存用户偏好"——内部从 ctx 拿 store 调 Put 写 ns/key。
// outer Run 完后直接调 Get 验证数据真写入了 store。
func TestBasicAgent_StoreCtxPropagation(t *testing.T) {
	myStore := store.NewInMemoryStore()
	ns := store.NS("users", "alice", "preferences")

	// tool 模拟"业务在 tool 内调 store.Put"
	var toolCalled atomic.Bool
	var seenStore atomic.Pointer[store.Store]
	saveTool := tool.NewFuncTool(
		"save_pref",
		"save user preference",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"key":   map[string]any{"type": "string"},
				"value": map[string]any{"type": "string"},
			},
			"required": []string{"key", "value"},
		},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			toolCalled.Store(true)
			st := store.FromContext(ctx)
			if st == nil {
				return tool.Failure("store_not_found", "store.FromContext returned nil"), nil
			}
			seenStore.Store(&st)
			key := args["key"].(string)
			value := args["value"].(string)
			if err := st.Put(ctx, ns, key, map[string]any{"v": value}); err != nil {
				return tool.Failure("put_failed", err.Error()), nil
			}
			return tool.Success("saved"), nil
		},
	)

	// stub model：第 1 轮调 save_pref；第 2 轮返最终答复
	m := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID:       "tc1",
			Type:     "function",
			Function: schema.FunctionCall{Name: "save_pref", Arguments: `{"key":"theme","value":"dark"}`},
		}}),
		schema.AssistantMessage("preference saved"),
	})

	tk := tool.NewToolkit()
	tk.Register(saveTool)

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("test_store_ctx"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithStore(myStore),
		agent_v2.WithMaxRounds(3),
	)

	sess := agent_v2.NewRunSession()
	_, err := ag.Run(context.Background(), sess, schema.UserMessage("save my theme"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 验证 tool 真调了
	if !toolCalled.Load() {
		t.Fatal("tool not called")
	}

	// 验证 tool 内拿到的 store 是同款实例（指针等价）
	gotStorePtr := seenStore.Load()
	if gotStorePtr == nil {
		t.Fatal("tool didn't see store")
	}
	if *gotStorePtr != store.Store(myStore) {
		t.Errorf("tool saw different store instance: got %p want %p", *gotStorePtr, myStore)
	}

	// 验证 outer 端能从 store 拿到 tool 写入的数据
	item, err := myStore.Get(context.Background(), ns, "theme")
	if err != nil {
		t.Fatalf("outer Get: %v", err)
	}
	if item == nil {
		t.Fatal("outer Get returned nil — tool didn't persist data")
	}
	got := item.Value.(map[string]any)
	if got["v"] != "dark" {
		t.Errorf("Value v = %v, want dark", got["v"])
	}
}

// TestBasicAgent_NoStoreFromContextNil 验证业务不挂 WithStore 时下游
// store.FromContext(ctx) 返 nil——业务自查降级。
func TestBasicAgent_NoStoreFromContextNil(t *testing.T) {
	var seenStore atomic.Pointer[store.Store]
	var toolCalled atomic.Bool
	probeTool := tool.NewFuncTool(
		"probe",
		"probe ctx store",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			toolCalled.Store(true)
			st := store.FromContext(ctx)
			if st != nil {
				seenStore.Store(&st)
			}
			return tool.Success("ok"), nil
		},
	)

	m := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc1", Type: "function",
			Function: schema.FunctionCall{Name: "probe", Arguments: `{}`},
		}}),
		schema.AssistantMessage("done"),
	})

	tk := tool.NewToolkit()
	tk.Register(probeTool)

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("no_store"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		// **关键**：不挂 WithStore
		agent_v2.WithMaxRounds(3),
	)

	sess := agent_v2.NewRunSession()
	_, err := ag.Run(context.Background(), sess, schema.UserMessage("what theme do I like"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !toolCalled.Load() {
		t.Fatal("tool not called")
	}
	if seenStore.Load() != nil {
		t.Errorf("expected FromContext returns nil when no WithStore, got %v", *seenStore.Load())
	}
}

// TestBasicAgent_StoreCtxOverridesOuter 验证业务在 Run 外层显式
// store.WithContext push 一个 store 实例，但 BasicAgent 自己有 WithStore 另一实例时——
// **BasicAgent 自己的 WithStore 覆盖外层 ctx**（context.Value 标准语义，后 push 覆盖
// 前 push）。
//
// 这是 RFC §5 设计选择的副效果——业务想"整个嵌套树共享 store"应在 Run 入口
// store.WithContext push，BasicAgent 不挂 WithStore；想"BasicAgent 用独立 store"
// 才挂 WithStore（覆盖父 ctx）。
func TestBasicAgent_StoreCtxOverridesOuter(t *testing.T) {
	outerStore := store.NewInMemoryStore()
	innerStore := store.NewInMemoryStore()
	var seenStore atomic.Pointer[store.Store]
	probeTool := tool.NewFuncTool(
		"probe",
		"probe ctx store",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			st := store.FromContext(ctx)
			seenStore.Store(&st)
			return tool.Success("ok"), nil
		},
	)
	m := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc1", Type: "function",
			Function: schema.FunctionCall{Name: "probe", Arguments: `{}`},
		}}),
		schema.AssistantMessage("done"),
	})
	tk := tool.NewToolkit()
	tk.Register(probeTool)

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("inner_override"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithStore(innerStore), // BasicAgent 自己的 store
		agent_v2.WithMaxRounds(3),
	)

	// 外层 ctx push outerStore
	ctx := store.WithContext(context.Background(), outerStore)

	sess := agent_v2.NewRunSession()
	// query 要跟预置记忆有共同词——注入按相关性排（见 searchMemories），
	// 跟本轮问题零共同词的记忆不会被注入，那是有意的。
	_, err := ag.Run(ctx, sess, schema.UserMessage("what theme do I like"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := seenStore.Load()
	if got == nil || *got == nil {
		t.Fatal("tool didn't see any store")
	}
	// BasicAgent.WithStore 应该覆盖外层 ctx
	if *got != store.Store(innerStore) {
		t.Errorf("expected inner store override, got outer (BasicAgent WithStore didn't override ctx)")
	}
}

// TestBasicAgent_StoreCtxResumePropagation 验证 Run/Resume 协议对称——HITL
// Resume 链路也能 store.FromContext(ctx) 拿到 store。
//
// **PR 2 audit HIGH-1 回归测试**（视角 1+2 共抓）：原 PR 2 commit `990bdad`
// Run 入口 push 但 Resume 入口漏 push → HITL Resume 时下游 tool/hook FromContext
// 返 nil。Sanity-revert：删 basic_agent.go Resume 入口的 pushStoreCtx 行 →
// 此测试 fail "tool seen nil store at Resume"。
func TestBasicAgent_StoreCtxResumePropagation(t *testing.T) {
	myStore := store.NewInMemoryStore()
	var resumeStoreSeen atomic.Pointer[store.Store]
	var resumeToolCalled atomic.Bool

	probeTool := tool.NewFuncTool(
		"probe_resume",
		"probe ctx store + interrupt first time then read on resume",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			// 第 2 次（Resume 后）调到：验证 ctx 含 store
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "ask_resume"); ok {
				resumeToolCalled.Store(true)
				st := store.FromContext(ctx)
				if st != nil {
					resumeStoreSeen.Store(&st)
				}
				return tool.Success("resumed with answer: " + ans), nil
			}
			// 第 1 次：抛 Interrupt
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "ask_resume", Question: "continue?"},
			}
		},
	)

	m := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc1", Type: "function",
			Function: schema.FunctionCall{Name: "probe_resume", Arguments: `{}`},
		}}),
		schema.AssistantMessage("done"),
	})

	tk := tool.NewToolkit()
	tk.Register(probeTool)

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("resume_store_test"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithStore(myStore),
		agent_v2.WithMaxRounds(3),
	)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	// Run 1：tool 第一次调抛 Interrupt
	_, err := ag.Run(ctx, sess, schema.UserMessage("start"))
	if err == nil {
		t.Fatal("Run should return InterruptError")
	}
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("Run should return InterruptError, got %v", err)
	}

	// Resume：业务答复 → tool 第二次调读 store
	_, err = ag.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			"ask_resume": schema.UserMessage("yes"),
		}))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}

	if !resumeToolCalled.Load() {
		t.Fatal("tool not called on Resume")
	}
	gotStorePtr := resumeStoreSeen.Load()
	if gotStorePtr == nil {
		t.Fatal("PR 2 audit HIGH-1 regression: tool seen nil store at Resume (Resume entry missing store.WithContext push?)")
	}
	if *gotStorePtr != store.Store(myStore) {
		t.Errorf("Resume tool saw different store: got %p want %p", *gotStorePtr, myStore)
	}
}

// TestBasicAgent_StoreCtxNestedChildPreservesParent 验证嵌套 BasicAgent 子未挂
// WithStore 时**透传**父 ctx 的 store——不会用子的 nil 覆盖父 store。
//
// **PR 2 audit 视角 1 HIGH-1 回归测试**：原 commit `bc506d0` 删 guard 后
// pushStoreCtx unconditional push 导致子 c.Store=nil 时 push nil 覆盖父 ctx
// 的 store key value——业务直觉是"子未挂 = 透传父"，违反。修法：pushStoreCtx
// 加 isFrameworkNil short circuit 业务不挂时直接返 ctx 不覆盖。
//
// Sanity-revert：删 pushStoreCtx 内 `if isFrameworkNil(st) { return ctx }` 让
// 它退回 unconditional push → 此测试 fail "child tool saw nil store
// (parent store leaked through nested override?)"。
func TestBasicAgent_StoreCtxNestedChildPreservesParent(t *testing.T) {
	parentStore := store.NewInMemoryStore()
	var childToolSeenStore atomic.Pointer[store.Store]
	probeTool := tool.NewFuncTool(
		"probe_nested",
		"probe nested store",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			st := store.FromContext(ctx)
			childToolSeenStore.Store(&st)
			return tool.Success("ok"), nil
		},
	)
	m := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc1", Type: "function",
			Function: schema.FunctionCall{Name: "probe_nested", Arguments: `{}`},
		}}),
		schema.AssistantMessage("done"),
	})
	tk := tool.NewToolkit()
	tk.Register(probeTool)

	// **子 BasicAgent 不挂 WithStore**——按业务直觉应透传父
	childAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("child_no_store"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(3),
	)

	// 业务外层把 parentStore 注入 ctx（模拟 RFC §5 推荐"业务 Run 入口 push 整树共享"模式）
	ctx := store.WithContext(context.Background(), parentStore)

	sess := agent_v2.NewRunSession()
	_, err := childAgent.Run(ctx, sess, schema.UserMessage("probe"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := childToolSeenStore.Load()
	if got == nil || *got == nil {
		t.Fatal("PR 2 audit 视角 1 HIGH-1 regression: child tool saw nil store " +
			"(parent store leaked through nested override?)")
	}
	if *got != store.Store(parentStore) {
		t.Errorf("child tool saw different store: got %p want parent %p", *got, parentStore)
	}
}

// TestWithStoreMemory_OptionParsing 验证 WithStoreMemory option 设置 Config 字段
// 正确（PR 3.2 阶段——BasicAgent 暂未消费此 config，仅 verify option 写入）。
//
// PR 3.3 落地后 BasicAgent buildInitialMessages 将真正消费此 config 调
// store.Search 注入 messages——届时加 functional 测试。
func TestWithStoreMemory_OptionParsing(t *testing.T) {
	tests := []struct {
		name        string
		ns          store.Namespace
		cfg         *agent_v2.StoreMemoryConfig
		wantLimit   int
		wantPanic   bool
		panicSubstr string
	}{
		{
			name:      "default cfg nil",
			ns:        store.NS("users", "alice"),
			cfg:       nil,
			wantLimit: 5,
		},
		{
			name:      "custom SearchLimit",
			ns:        store.NS("users", "bob"),
			cfg:       &agent_v2.StoreMemoryConfig{SearchLimit: 10},
			wantLimit: 10,
		},
		{
			name:      "negative SearchLimit 兜底默认",
			ns:        store.NS("users", "carol"),
			cfg:       &agent_v2.StoreMemoryConfig{SearchLimit: -1},
			wantLimit: 5,
		},
		{
			name:        "invalid namespace panic",
			ns:          store.NS(),
			cfg:         nil,
			wantPanic:   true,
			panicSubstr: "invalid namespace",
		},
		{
			name:        "namespace 含 NUL 段 panic",
			ns:          store.NS("a\x00b"),
			cfg:         nil,
			wantPanic:   true,
			panicSubstr: "invalid namespace",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if tc.wantPanic {
					if r == nil {
						t.Error("expected panic, got none")
					}
					return
				}
				if r != nil {
					t.Errorf("unexpected panic: %v", r)
				}
			}()
			_ = agent_v2.NewBasicAgent(
				agent_v2.WithName("test_storemem"),
				agent_v2.WithModel(newStubModel(nil)),
				agent_v2.WithStoreMemory(tc.ns, tc.cfg),
			)
			// 由于 Config 是 unexported 字段无法直接 inspect——通过 panic / 无 panic 间接验证
			// option 走对路径。PR 3.3 加 functional 测试时通过 BasicAgent 行为间接 verify
			// SearchLimit 实际值。
		})
	}
}

// captureModelStub 业务 ChatModel——Generate 内 spy 收到的 messages。
type captureModelStub struct {
	allCalls  [][]*schema.Message
	responses []*schema.Message
	idx       int
}

func (m *captureModelStub) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	// clone msgs 防业务 mutate
	clone := make([]*schema.Message, len(msgs))
	copy(clone, msgs)
	m.allCalls = append(m.allCalls, clone)
	if m.idx >= len(m.responses) {
		return schema.AssistantMessage("[stub exhausted]"), nil
	}
	out := m.responses[m.idx]
	m.idx++
	return out, nil
}

func (m *captureModelStub) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *captureModelStub) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

// TestBasicAgent_WithStoreMemory_PreambleInjectsHistory 验证 PR 3.3 BasicAgent
// 真消费 c.StoreMemory——业务挂 WithStoreMemory + 预 Put 历史 message 后 Run，
// LLM 收到的 messages 含历史（preamble 自动 Search 注入 + 打 StoreInjectedAnnotation
// marker）。
//
// **Sanity-revert**: 删 BasicAgent.buildInitialMessages 内 StoreMemory Search 段
// → 测试 fail "LLM messages 应含历史注入"。
func TestBasicAgent_WithStoreMemory_PreambleInjectsHistory(t *testing.T) {
	myStore := store.NewInMemoryStore()
	ns := store.NS("users", "alice", "memories")
	ctx := context.Background()

	// 预 Put 一条记忆——形状同 BasicAgent commitFinal 写的（答复文本），测试场景手动预填
	if err := myStore.Put(ctx, ns, "history_1", "user previously said: I like dark theme"); err != nil {
		t.Fatalf("Put history: %v", err)
	}

	captureStub := &captureModelStub{
		responses: []*schema.Message{schema.AssistantMessage("Acknowledged dark theme")},
	}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("memtest"),
		agent_v2.WithModel(captureStub),
		agent_v2.WithStore(myStore),
		agent_v2.WithStoreMemory(ns, &agent_v2.StoreMemoryConfig{SearchLimit: 5}),
	)

	sess := agent_v2.NewRunSession()
	// query 要跟预置记忆有共同词——注入按相关性排（见 searchMemories），
	// 跟本轮问题零共同词的记忆不注入，那是有意的。
	_, err := ag.Run(ctx, sess, schema.UserMessage("what theme did I say I like?"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(captureStub.allCalls) < 1 {
		t.Fatal("LLM 应被调")
	}

	// 找历史 message（按 content 匹配）
	var injected *schema.Message
	for _, m := range captureStub.allCalls[0] {
		if m != nil && m.Content == "user previously said: I like dark theme" {
			injected = m
			break
		}
	}
	if injected == nil {
		t.Fatal("PR 3.3 regression: LLM messages 应含 StoreMemory 注入的历史" +
			"（buildInitialMessages 内 store.Search 漏调？）")
	}
	// 验 StoreInjectedAnnotation marker
	if v, ok := injected.Annotations[agent_v2.StoreInjectedAnnotation].(bool); !ok || !v {
		t.Errorf("注入的历史 message 应有 StoreInjectedAnnotation marker，实际 %v",
			injected.Annotations)
	}
}

// TestBasicAgent_WithStoreMemory_PostambleSavesAssistant 验证 PR 3.3 BasicAgent
// commitFinal 真消费 c.StoreMemory——Run 完后最终 assistant message 自动 Put 到
// store（postamble 自动写）。
//
// **Sanity-revert**: 删 BasicAgent.commitFinal 内 StoreMemory Put 段 → 测试 fail
// "store 应含 Run 后写入的 assistant message"。
func TestBasicAgent_WithStoreMemory_PostambleSavesAssistant(t *testing.T) {
	myStore := store.NewInMemoryStore()
	ns := store.NS("users", "bob", "memories")
	ctx := context.Background()

	m := newStubModel([]*schema.Message{schema.AssistantMessage("final response from agent")})
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("savetest"),
		agent_v2.WithModel(m),
		agent_v2.WithStore(myStore),
		agent_v2.WithStoreMemory(ns, nil),
	)

	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 验 store ns 下含 Run 后写入的 assistant message
	items, err := myStore.Search(ctx, ns)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(items) == 0 {
		t.Fatal("PR 3.3 regression: store 应含 Run 后写入的 assistant message" +
			"（commitFinal 内 store.Put 漏调？）")
	}
	// 验值是这次的答复文本
	var found bool
	for _, it := range items {
		// 框架存的是**答复文本**（2026-09-07 起），不是整条 *schema.Message——
		// 存消息时六个后端读回来的类型不一致（in-memory 给 Message、持久后端给 map），
		// 喂给 LLM 的东西也跟着分叉。
		if txt, ok := it.Value.(string); ok && txt == "final response from agent" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("应找到 content='final response from agent' 的 message, items=%+v", items)
	}
}

// putSpyStore 包装 store.Store 计数 Put 调用——给 ISSUE-84 sub-agent scope 协议
// 守护测试用。**为什么不直接 Search 验**：NewSharedChildSession 让 child / parent
// 共享 RunID + BasicAgent 的 commitFinal key = `RunID_assistant_{len(events)}` →
// 父子 events 都 = 1（仅 input OutputEvent）→ key 完全相同 → child Put 静默被 parent
// Put 覆盖。端到端 Search 看不出 double Put（CLAUDE.md "5 类炸弹 #2：早退兜底盖过
// fix 路径"）。spy 计数器是唯一能区分 1 次 vs 2 次 Put 的方式。
type putSpyStore struct {
	store.Store
	mu   sync.Mutex
	puts int
}

func (s *putSpyStore) Put(ctx context.Context, ns store.Namespace, key string, value any, opts ...store.PutOption) error {
	s.mu.Lock()
	s.puts++
	s.mu.Unlock()
	return s.Store.Put(ctx, ns, key, value, opts...)
}

func (s *putSpyStore) PutCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.puts
}

// TestBasicAgent_StoreMemory_SubAgentScopeNoDoublePut 验证 ISSUE-84 协议：
// 顶层 BasicAgent + AgentAsTool 包子 BasicAgent **共享同一 store 实例**且**都挂
// WithStoreMemory** 时，两边各写各的、互不覆盖。
//
// # 这条守的东西换过一次
//
// 它原先叫 `...NoDoublePut`，断言的是「**子 agent 一次都不许写**」（数 Put 调用次数），
// 守的是那个 `!IsSubAgentStoreScope(ctx)` 开关。而那个开关本身是个补丁——它防的不是
// 「子不该有记忆」，是**键里没有写入者**：键末位当年是事件序号、对父子逐字相同，
// 两边都写就后写覆盖先写。旧注释自己写明了这一点：「不能用端到端 Search 验：父子
// RunID 共享 + events 都 = 1 → **key collision 覆盖**」。
//
// 也就是说它在拿「不让第二个人写」模拟「两个人写不同的地方」，代价是业务给子 agent
// 配的 `WithStoreMemory` 静默失效（实测：同一个 agent 顶层写 1 条、当成员写 0 条）。
//
// 键里加上写入者之后，那个开关没有存在的理由，这条用例的守护对象也跟着换成真正该守的：
// **两条记忆都在、内容分得开、键不撞**。
func TestBasicAgent_StoreMemory_NestedWritersDoNotCollide(t *testing.T) {
	spy := &putSpyStore{Store: store.NewInMemoryStore()}
	ns := store.NS("users", "test", "memories")
	ctx := context.Background()

	// 子 BasicAgent: 返跟 parent 不同的 final 让 store 内容可分辨
	childModel := newStubModel([]*schema.Message{schema.AssistantMessage("child agent response")})
	childAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("child"),
		agent_v2.WithModel(childModel),
		agent_v2.WithStore(spy),
		agent_v2.WithStoreMemory(ns, nil),
	)
	childTool := agent_v2.AgentAsTool(childAgent, agent_v2.WithAgentToolDescription("delegate to child"))

	parentModel := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "tc1", Type: "function",
			Function: schema.FunctionCall{Name: "child", Arguments: `{"input":"hi"}`},
		}}),
		schema.AssistantMessage("parent agent final response"),
	})
	tk := tool.NewToolkit()
	tk.Register(childTool)
	parentAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentModel),
		agent_v2.WithToolkit(tk),
		agent_v2.WithStore(spy),
		agent_v2.WithStoreMemory(ns, nil),
		agent_v2.WithMaxRounds(3),
	)

	sess := agent_v2.NewRunSession()
	if _, err := parentAgent.Run(ctx, sess, schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 两边都写（各自的 WithStoreMemory 各自生效），且**落在不同的键上**。
	if got := spy.PutCount(); got != 2 {
		t.Errorf("父子都配了 WithStoreMemory，应各写一次共 2 次 Put，实际 %d 次"+
			"（1 = 有一边的配置被静默忽略了）", got)
	}
	items, serr := spy.Search(ctx, ns)
	if serr != nil {
		t.Fatalf("Search: %v", serr)
	}
	if len(items) != 2 {
		keys := make([]string, 0, len(items))
		for _, it := range items {
			keys = append(keys, it.Key)
		}
		t.Fatalf("两次写入应各占一个键，实际只存下 %d 条 %v（后写覆盖先写，静默丢失）",
			len(items), keys)
	}
	// 内容要分得开——这是「各写各的」的实质，不是「有两条」。
	// 框架存的是**答复文本**（2026-09-07 起），不是整条 *schema.Message。
	var seenParent, seenChild bool
	for _, it := range items {
		txt, _ := it.Value.(string)
		switch txt {
		case "parent agent final response":
			seenParent = true
		case "child agent response":
			seenChild = true
		}
	}
	if !seenParent || !seenChild {
		t.Errorf("父子的记忆应各自可辨：parent=%v child=%v", seenParent, seenChild)
	}
}

// TestBasicAgent_WithStoreMemory_RequiresStore 验证 WithStoreMemory 隐式依赖
// WithStore——业务挂 WithStoreMemory 但忘挂 WithStore 时 Run 返
// ErrStoreNotConfigured fail-fast（不让远 stack 出错）。
func TestBasicAgent_WithStoreMemory_RequiresStore(t *testing.T) {
	m := newStubModel([]*schema.Message{schema.AssistantMessage("never")})
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("nostore"),
		agent_v2.WithModel(m),
		// **关键**：挂 WithStoreMemory 但不挂 WithStore
		agent_v2.WithStoreMemory(store.NS("u"), nil),
	)
	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil {
		t.Fatal("WithStoreMemory 无 WithStore 应 fail-fast")
	}
	if !errors.Is(err, store.ErrStoreNotConfigured) {
		t.Errorf("应返 ErrStoreNotConfigured, 实际 %v", err)
	}
}

// TestBasicAgent_WithStoreTypedNilPanic 验证 typed nil 防护——业务踩坑 `var st
// *MyStore; WithStore(st)` 时 panic 而非 silent 让远 stack nil deref。
func TestBasicAgent_WithStoreTypedNilPanic(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Error("WithStore typed nil should panic")
			return
		}
		// panic 信息应该提示 typed nil
	}()
	var nilStore *store.InMemoryStore
	_ = agent_v2.NewBasicAgent(
		agent_v2.WithName("typed_nil"),
		agent_v2.WithStore(nilStore),
	)
}
