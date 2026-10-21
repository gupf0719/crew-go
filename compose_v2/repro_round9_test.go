package compose_v2_test

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestRepro_R9_D1_ReducerPanicAtomicRollback 验证 audit Round 9 Bug D1：
// 业务 reducer 在 fan-out 多分支 ApplyAll 第 K 条 reduce 时 panic，前 K-1 条应
// 那一批一条都没提交，cm.values 留空。
//
// **预期行为**：panic 转 error，cm.values 完整还原本批 ApplyAll 入口前状态。
//
// **当前 bug**（修前）：前 K-1 条已 commit 留 partial state；cp 持久化 partial
// 让 Resume 失败分支重跑时 Sum/Append 双倍计数。
func TestRepro_R9_D1_ReducerPanicAtomicRollback(t *testing.T) {
	// 业务自定义 reducer：第 3 次调用 panic
	callCount := 0
	badCh := compose_v2.Custom("bad",
		func(prev, next int) int {
			callCount++
			if callCount == 3 {
				panic("simulated reducer panic on 3rd call")
			}
			return prev + next
		},
		0,
	)

	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, badCh)

	err := cm.ApplyAll([]compose_v2.ChannelWrite{
		compose_v2.Write(badCh, 1), // call 1: 0+1=1, commit
		compose_v2.Write(badCh, 2), // call 2: 1+2=3, commit
		compose_v2.Write(badCh, 4), // call 3: panic
	})
	if err == nil {
		t.Fatal("expected panic-derived error, got nil")
	}

	// **关键不变量**：panic 应回滚——cm.values[bad] 不应留 3
	v, ok := compose_v2.Get(cm.Channels(), badCh)
	if ok && v != 0 {
		t.Fatalf("Bug D1 reproduced: reducer panic 中段 partial commit 未回滚\n"+
			"期望 cm.values[bad] = zero（未写）\n"+
			"实际 cm.values[bad] = %d（前 2 次已 commit 留 partial）\n"+
			"当年的修复方向是回滚栈；现已换成归并/提交分离——归并跑在局部 staged 上，掐断时压根没写回",
			v)
	}
}

// TestRepro_R9_D1_ReducerPanicMultiChannelRollback 验证多 channel 多 writes
// 同时被 panic 影响时一条都不提交。
func TestRepro_R9_D1_ReducerPanicMultiChannelRollback(t *testing.T) {
	chA := compose_v2.LastValue[int]("a")
	chB := compose_v2.LastValue[int]("b")
	// chC 第 1 次调用 panic
	chC := compose_v2.Custom("c",
		func(_, _ int) int {
			panic("c reducer panic")
		},
		0,
	)

	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, chA)
	compose_v2.RegisterChannel(cm, chB)
	compose_v2.RegisterChannel(cm, chC)

	// 预先给 chA / chB 写初始值（这些是本批 ApplyAll 之前的状态）
	_ = cm.ApplyAll([]compose_v2.ChannelWrite{
		compose_v2.Write(chA, 100),
		compose_v2.Write(chB, 200),
	})

	// 现在跑会 panic 的批：含 chA/chB 写 + chC panic
	err := cm.ApplyAll([]compose_v2.ChannelWrite{
		compose_v2.Write(chA, 999), // 应回滚到 100
		compose_v2.Write(chB, 888), // 应回滚到 200
		compose_v2.Write(chC, 5),   // panic
	})
	if err == nil {
		t.Fatal("expected panic-derived error")
	}

	a, _ := compose_v2.Get(cm.Channels(), chA)
	b, _ := compose_v2.Get(cm.Channels(), chB)
	if a != 100 {
		t.Fatalf("Bug D1: chA 未回滚到本批 ApplyAll 前状态: 期望 100，实际 %d", a)
	}
	if b != 200 {
		t.Fatalf("Bug D1: chB 未回滚: 期望 200，实际 %d", b)
	}
}

// TestRepro_R9_B2_SubgraphIsolatedResumeContext 验证 audit Round 9 Bug B2：
// 业务用 sentinel key（如 PlanAgent `_replan_`）父子嵌套时，需要
// WithSubgraphIsolatedResumeContext 隔离 ResumeContext 避免父子互相吃答案。
//
// **预期行为**：opt-in isolate 后子图 NodeFunc 看不到父的 UserInputs map；
// 父答案不被子吃，子需自己的 ResumeContext。
//
// **当前 bug**（未提供 isolate 选项时）：父 sentinel-key 答案 silent 被子吃
// 走业务 prod-path PlanAgent 嵌套必踩。
func TestRepro_R9_B2_SubgraphIsolatedResumeContext(t *testing.T) {
	// 子图：节点 step1 抛 sentinel-key InterruptError
	subB := compose_v2.NewBuilder()
	subB.AddNode("step1", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// 子图主动用 sentinel "_replan_"
		if ans, ok := compose_v2.GetResumeAnswerText(ctx, "_replan_"); ok {
			t.Errorf("Bug B2: 子图不该读到父的 _replan_ 答案: %q", ans)
			return nil, nil
		}
		return nil, &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{InterruptID: "_replan_", Question: "子 _replan_"},
		}
	})
	subB.AddEdge(compose_v2.StartNode, "step1")
	subB.AddEdge("step1", compose_v2.EndNode)
	sub, err := subB.Compile()
	if err != nil {
		t.Fatalf("compile sub: %v", err)
	}

	// 父图：节点 parent_a 嵌入 sub_node 子图（用 isolate option）
	parentB := compose_v2.NewBuilder()
	parentB.AddSubgraphNode("sub_node", sub,
		compose_v2.WithSubgraphIsolatedResumeContext(), // ← 关键：isolate
	)
	parentB.AddEdge(compose_v2.StartNode, "sub_node")
	parentB.AddEdge("sub_node", compose_v2.EndNode)
	parent, err := parentB.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()

	// Run1 不带答案：子图抛 _replan_ 中断、留档——真实业务里答案只出现在续跑
	// （「首跑就预埋答案」的形态已被「答案无档 fail-fast」拒绝，那是假阳性注入的
	// 温床，不再是本测试的外形）。
	runErr := compose_v2.Run(context.Background(), parent, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("b2_sid"),
	)
	if _, isInt := compose_v2.IsInterrupt(runErr); !isInt {
		t.Fatalf("Run1 子图该抛 _replan_ 中断, got %v", runErr)
	}

	// Run2 caller 在父层传 _replan_ 答案——业务意图答父层语义的 _replan_。
	// isolate 后子读不到，子重新中断（step1 里的 t.Errorf 钉着「读到即失守」）。
	resumeErr := compose_v2.Run(context.Background(), parent, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("b2_sid"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			"_replan_": schema.UserMessage("父期望的答案"), // 不应被子读到
		}),
	)
	// 子图应中断（因为 isolate 后没读到答案）
	if _, isInt := compose_v2.IsInterrupt(resumeErr); !isInt {
		t.Fatalf("Bug B2 reproduced: 子图被错误地读到父答案而 silent 通过\n"+
			"期望：子图 isolate 后看不到父答案，重新抛 InterruptError\n"+
			"实际 err: %v", resumeErr)
	}
}

// TestRepro_R9_C4_PendingInterruptsDeepCopy 验证 audit Round 9 Bug C4：
// cm.PendingInterrupts() 应返回深拷贝，业务 mutate 不污染 cm 内部 state。
//
// **预期行为**：业务修改返回的 *AskQuery 字段不影响后续 cm.PendingInterrupts() 调用。
//
// **当前 bug**（修前）：浅拷贝 slice 元素是同指针，业务 mutate 直接污染。
func TestRepro_R9_C4_PendingInterruptsDeepCopy(t *testing.T) {
	noopFn := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{
				InterruptID: "id1",
				Question:    "原始问题",
				Options:     []string{"a", "b"},
			},
		}
	}
	b := compose_v2.NewBuilder()
	b.AddNode("ask", noopFn)
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g, _ := b.Compile()

	cm := compose_v2.NewChannelManager()
	_ = compose_v2.Run(context.Background(), g, cm)

	list := cm.PendingInterrupts()
	if len(list) != 1 {
		t.Fatalf("expected 1 pending, got %d", len(list))
	}
	// 业务 mutate 拿到的副本
	list[0].Question = "业务修改后的问题"
	list[0].Options[0] = "X"

	// cm 内部应不被污染
	list2 := cm.PendingInterrupts()
	if list2[0].Question == "业务修改后的问题" {
		t.Fatalf("Bug C4 reproduced: PendingInterrupts 浅拷贝 → 业务 mutate 污染 cm 内部\n"+
			"期望：第二次调拿到原始 \"原始问题\"，实际 %q\n"+
			"修复方向：PendingInterrupts 返深拷贝（*AskQuery + Options slice）",
			list2[0].Question)
	}
	if list2[0].Options[0] == "X" {
		t.Fatalf("Bug C4: Options slice 共享 underlying，业务 mutate 污染")
	}
}

// TestRepro_R9_C3_AppendNoBackingAlias 验证 audit Round 9 Bug C3：
// Append reducer 不应复用 prev underlying array。多次 Get + Write 相互看不到
// 对方未提交的数据。
//
// **预期行为**：业务 prev Get 拿到的 slice 与 cm.values 内不共享底层数组。
//
// **当前 bug**（修前）：append(prev, next...) 在 prev cap > len 时直写老 array。
func TestRepro_R9_C3_AppendNoBackingAlias(t *testing.T) {
	histCh := compose_v2.Append[int]("hist")
	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, histCh)

	// **关键 setup**：先写 1 元素让 cap 增长到 >1（Go append spec：小 slice cap
	// 通常 doubling），再多次写让 cap 远超 len——制造 cap > len 的 prev state，
	// 触发 Append reducer 复用 underlying array 的 bug 路径。
	for i := 1; i <= 5; i++ {
		_ = cm.ApplyAll([]compose_v2.ChannelWrite{
			compose_v2.Write(histCh, []int{i}),
		})
	}

	// 业务 Get 拿一份 slice 引用——此时 cm.values[hist] cap 应 > 4
	v1, _ := compose_v2.Get(cm.Channels(), histCh)
	v1Cap := cap(v1)
	v1Len := len(v1)
	t.Logf("v1 len=%d cap=%d (need cap>len 才能 trigger bug)", v1Len, v1Cap)
	if v1Cap == v1Len {
		t.Skip("Go runtime 给 cap==len 不能触发 underlying alias bug，本测试无效")
	}

	// 再写一个：旧 bug 路径下因 cap > len，append 直写 underlying → 污染 v1
	_ = cm.ApplyAll([]compose_v2.ChannelWrite{
		compose_v2.Write(histCh, []int{99}),
	})

	// 验证 v1 内容未被污染（仍 [1,2,3,4,5]）
	for i, want := range []int{1, 2, 3, 4, 5} {
		if v1[i] != want {
			t.Fatalf("Bug C3 reproduced: Append 复用 underlying array 污染业务持有的 slice\n"+
				"v1[%d] 期望 %d，实际 %d (整 v1=%v)\n"+
				"修复方向：Append reducer make 新 backing 不复用 prev",
				i, want, v1[i], v1)
		}
	}
	// 检查 v1 后面的位置是否被串入 99（cap 处的 underlying 写）
	if v1Cap > v1Len {
		v1Extended := v1[:v1Cap]
		if v1Extended[v1Len] == 99 {
			t.Fatalf("Bug C3: v1[len:cap] 槽位被串入新 write 的 99 (cap 处 underlying 共享)")
		}
	}
	// cm 内最新值应是 [1,2,3,4,99]
	v2, _ := compose_v2.Get(cm.Channels(), histCh)
	if len(v2) != 5 || v2[4] != 99 {
		t.Fatalf("cm 状态不对: v2=%v", v2)
	}
}

// TestRepro_R9_B1_ResumeAnswerPreservesContentBlocks 验证 audit Round 9 Bug B1：
// ResumeContext.UserInputs 类型从 map[string]string 改为 map[string]*schema.Message
// 后业务多模态 / 元数据答案完整保留。
//
// **预期行为**：caller 提供带 ContentBlocks + Annotations 的 *Message 答案，节点
// GetResumeAnswer 拿到的字段完整。
//
// **当前 bug**（修前）：agent_v2 conversion 强转 string 静默丢 ContentBlocks 等。
func TestRepro_R9_B1_ResumeAnswerPreservesContentBlocks(t *testing.T) {
	var capturedAns *schema.Message
	captureFn := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if ans, ok := compose_v2.GetResumeAnswer(ctx, "id_multi"); ok {
			capturedAns = ans
			return nil, nil
		}
		return nil, &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{InterruptID: "id_multi", Question: "需要图片"},
		}
	}

	b := compose_v2.NewBuilder()
	b.AddNode("ask", captureFn)
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g, _ := b.Compile()

	cm := compose_v2.NewChannelManager()
	cper := compose_v2.NewInMemoryCheckpointer()

	// Run 1：抛中断
	_ = compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("b1_sid"),
	)

	// Run 2 (Resume)：传带 ContentBlocks + Annotations 的答案
	answerMsg := &schema.Message{
		Role:    schema.RoleUser,
		Content: "看这张图",
		ContentBlocks: []*schema.ContentBlock{
			{Type: "imageUrl"},
		},
		Annotations: map[string]any{"locale": "zh-CN", "trace_id": "abc"},
	}
	cm2 := compose_v2.NewChannelManager()
	_ = compose_v2.Run(context.Background(), g, cm2,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("b1_sid"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			"id_multi": answerMsg,
		}),
	)

	if capturedAns == nil {
		t.Fatal("Bug B1: ResumeAnswer 未被捕获")
	}
	if capturedAns.Content != "看这张图" {
		t.Fatalf("Content 字段丢失: got %q", capturedAns.Content)
	}
	if len(capturedAns.ContentBlocks) != 1 || capturedAns.ContentBlocks[0].Type != "imageUrl" {
		t.Fatalf("Bug B1 reproduced: ContentBlocks 字段丢失\n"+
			"期望 1 个 imageUrl block，实际 %+v\n"+
			"修复方向：UserInputs map[string]*schema.Message 端到端保完整字段",
			capturedAns.ContentBlocks)
	}
	if locale, _ := capturedAns.Annotations["locale"].(string); locale != "zh-CN" {
		t.Fatalf("Bug B1: Annotations.locale 丢失，实际 %v", capturedAns.Annotations)
	}
}

// TestRepro_R9_B4_UnmatchedAnswerLogsWarning 验证 audit Round 9 Bug B4：
// caller 提供 InterruptID 与 cm pending 不匹配时框架应 slog.Warn 提示业务。
//
// **预期行为**：unmatched ID 触发 slog.LevelWarn record，attrs 含 unmatched ID list。
//
// **当前 bug**（修前）：silent 接受 → 节点重抛 → caller 不知"自己提供的 ID 错了"。
func TestRepro_R9_B4_UnmatchedAnswerLogsWarning(t *testing.T) {
	// 替换 default slog handler 捕获 warning
	captureH := &captureLogHandlerR9{}
	origLogger := slog.Default()
	slog.SetDefault(slog.New(captureH))
	defer slog.SetDefault(origLogger)

	// 构造 cp 含 1 条 PendingInterrupt with InterruptID="real_id"
	cp := &compose_v2.Checkpoint{
		SchemaVer:       6,
		CurrentFrontier: []compose_v2.FrontierMember{{NodeID: "ask"}},
		PendingInterrupts: []*compose_v2.AskQuery{
			{InterruptID: "real_id", Question: "real?"},
		},
	}
	noopFn := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{InterruptID: "real_id", Question: "real?"},
		}
	}
	b := compose_v2.NewBuilder()
	b.AddNode("ask", noopFn)
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g, _ := b.Compile()

	cm := compose_v2.NewChannelManager()
	// caller 提供拼错的 ID
	_ = compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithResumeFrom(cp),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			"typo_id": schema.UserMessage("yes"),
		}),
	)

	// 验证 slog.Warn 被 emit + attrs 含 unmatched ID
	captureH.mu.Lock()
	defer captureH.mu.Unlock()
	gotWarn := false
	for _, r := range captureH.records {
		if r.Level != slog.LevelWarn {
			continue
		}
		if !strings.Contains(r.Message, "unmatched InterruptIDs") {
			continue
		}
		gotWarn = true
		// 验证 attrs 含 unmatched
		hasUnmatched := false
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "unmatched" {
				hasUnmatched = true
				return false
			}
			return true
		})
		if !hasUnmatched {
			t.Fatalf("Bug B4: slog.Warn 缺 unmatched attr")
		}
	}
	if !gotWarn {
		msgs := make([]string, len(captureH.records))
		for i, r := range captureH.records {
			msgs[i] = r.Level.String() + ":" + r.Message
		}
		t.Fatalf("Bug B4 reproduced: caller 提供 unmatched ID 时框架 silent 接受\n"+
			"修复方向：graph_loop withResumeContext 入口 diff cfg.resumeAnswers 与 cp.PendingInterrupts 的 ID 集合，unmatched 时 slog.Warn\n"+
			"实际 log records: %v", msgs)
	}
}

// captureLogHandlerR9 收集 slog 调用让测试断言（局部 helper 避免与其它包冲突）。
type captureLogHandlerR9 struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureLogHandlerR9) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *captureLogHandlerR9) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}
func (h *captureLogHandlerR9) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *captureLogHandlerR9) WithGroup(_ string) slog.Handler      { return h }
