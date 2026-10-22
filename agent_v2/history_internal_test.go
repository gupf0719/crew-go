package agent_v2

// 会话历史写入器的内部行为——都是未导出的，走白盒。
//
// 这里钉的几条都有一个共同点：**错了不会报错**。写入器吞掉后端错误是 by design
// （历史是旁路，见 historyWriter godoc），代价是一旦吞错的边界划错了，故障就彻底
// 静默——后端一直在失败、主流程一直在跑、没人知道。所以要正着验「该写的写了」，
// 也要反着验「不该让它崩的没崩」。

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// recordingHistoryStore 记下每一批 Append 的内容与顺序。
type recordingHistoryStore struct {
	mu      sync.Mutex
	batches [][]*schema.Message
	keys    []HistoryKey
	err     error // 非 nil 时每次 Append 都返它
	panicOn string
}

func (s *recordingHistoryStore) Append(_ context.Context, key HistoryKey, msgs []*schema.Message) error {
	if s.panicOn != "" {
		for _, m := range msgs {
			if m != nil && strings.Contains(m.Content, s.panicOn) {
				panic("后端炸了: " + s.panicOn)
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, msgs)
	s.keys = append(s.keys, key)
	return s.err
}

func (s *recordingHistoryStore) Load(_ context.Context, _ HistoryKey, _ HistoryQuery) (HistoryPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*schema.Message
	for _, b := range s.batches {
		out = append(out, b...)
	}
	return HistoryPage{Messages: out}, nil
}

func (s *recordingHistoryStore) flat() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, b := range s.batches {
		for _, m := range b {
			out = append(out, m.Content)
		}
	}
	return out
}

// newTestHistoryCtx 走**真实入口** startHistory 装配 ctx，返回 (ctx, 收尾)。
// 不直接 new writer——那样就绕开了 key 解析和「身份缺失不启」这两段真逻辑。
func newTestHistoryCtx(t *testing.T, st HistoryStore, convID string) (context.Context, func()) {
	t.Helper()
	hc := &historyConfig{store: st, owner: "tenant-a"}
	ctx := context.Background()
	if convID != "" {
		ctx = compose_v2.WithThreadIDCtx(ctx, convID)
	}
	return startHistory(ctx, hc, "test-agent")
}

// TestHistoryWriter_FIFOOrder：异步写必须保住产生顺序。
//
// 历史是时序回放。异步 + 多 worker 是最容易在这里翻车的组合——所以是单 worker + FIFO
// 队列。这条用足量批次验它。
func TestHistoryWriter_FIFOOrder(t *testing.T) {
	st := &recordingHistoryStore{}
	ctx, stop := newTestHistoryCtx(t, st, "conv-1")

	const n = 50
	for i := 0; i < n; i++ {
		recordHistory(ctx, "a", "a", []*schema.Message{schema.UserMessage(fmt.Sprintf("m-%02d", i))})
	}
	stop()

	got := st.flat()
	if len(got) != n {
		t.Fatalf("落盘 %d 条, want %d —— closeAndJoin 应 drain 完所有 pending 写", len(got), n)
	}
	for i, c := range got {
		if want := fmt.Sprintf("m-%02d", i); c != want {
			t.Fatalf("[%d] = %q, want %q —— 异步写乱序了，历史即失真", i, c, want)
		}
	}
}

// TestHistoryWriter_SwallowsBackendError：后端一直失败也不能打断主链路。
//
// 执行正确性挂在 cp 的 frontier 上，历史丢了不影响跑对（工具重不重跑、Resume 从哪
// 继续都跟它无关）。所以这一层吞错——但吞得干净：不 panic、不阻塞、closeAndJoin
// 正常返回。
func TestHistoryWriter_SwallowsBackendError(t *testing.T) {
	st := &recordingHistoryStore{err: errors.New("后端挂了")}
	ctx, stop := newTestHistoryCtx(t, st, "conv-1")

	for i := 0; i < 5; i++ {
		recordHistory(ctx, "a", "a", []*schema.Message{schema.UserMessage("x")})
	}
	stop() // 卡住或 panic 都会让这条测试挂

	if n := len(st.flat()); n != 5 {
		t.Errorf("后端返错时仍应逐批尝试，实际尝试 %d 批, want 5", n)
	}
}

// TestHistoryWriter_BackendPanicDoesNotCrash：后端 panic 不能崩进程。
//
// worker 跑在独立 goroutine，Run 入口的 outer recover 够不着它。业务后端一个 panic
// 就崩掉整个服务，跟「历史是旁路」这个定位完全不成比例。而且 panic 之后 worker 必须
// **继续**消费——否则后续 submit 全被静默收进队列没人处理（队列无界不会阻塞，所以连
// 卡住都不会有，只是历史莫名少一截），closeAndJoin 还会因 wg 归零立即返回。
func TestHistoryWriter_BackendPanicDoesNotCrash(t *testing.T) {
	st := &recordingHistoryStore{panicOn: "BOOM"}
	ctx, stop := newTestHistoryCtx(t, st, "conv-1")

	recordHistory(ctx, "a", "a", []*schema.Message{schema.UserMessage("before")})
	recordHistory(ctx, "a", "a", []*schema.Message{schema.UserMessage("BOOM")})
	recordHistory(ctx, "a", "a", []*schema.Message{schema.UserMessage("after")})
	stop()

	got := st.flat()
	if len(got) != 2 || got[0] != "before" || got[1] != "after" {
		t.Errorf("panic 后 worker 应继续消费，实际落盘 %v —— "+
			"worker 死掉的话后续写入静默堆积、closeAndJoin 又立即返回，历史少一截且无日志", got)
	}
}

// TestRecordHistory_NoThreadID_Skips：没声明会话身份就不写。
//
// 没有 ConvID 就没有「这是哪个会话」，写下去也无从取回——只会在后端堆一坨取不出来的
// 数据。框架跳过 + warn 一次。
func TestRecordHistory_NoThreadID_Skips(t *testing.T) {
	st := &recordingHistoryStore{}
	ctx, stop := newTestHistoryCtx(t, st, "") // 不 push ThreadID

	recordHistory(ctx, "a", "a", []*schema.Message{schema.UserMessage("x")})
	stop()

	if n := len(st.flat()); n != 0 {
		t.Errorf("没会话身份时写了 %d 条, want 0 —— 写下去也取不回来", n)
	}
}

// TestRecordHistory_StampsAgentAndDoesNotMutate：盖归属章，且不碰原消息。
//
// 归属是「拥有这个 channel 的 agent」——出了 channel 这个上下文就没了，所以离开的
// 那一刻固化到 annotation 上。
//
// 而 msgs 里的指针**就是 channel 里那批**，正在（或即将）被喂给模型。原地改它们的
// Annotations 会污染模型输入：多出一个模型看不懂的键，还可能跟压缩层的 marker 打架。
func TestRecordHistory_StampsAgentAndDoesNotMutate(t *testing.T) {
	st := &recordingHistoryStore{}
	ctx, stop := newTestHistoryCtx(t, st, "conv-1")

	orig := schema.AssistantMessage("产出")
	orig.Annotations = map[string]any{"已有键": "已有值"}

	recordHistory(ctx, "my-agent", "my-agent", []*schema.Message{orig})
	stop()

	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.batches) != 1 || len(st.batches[0]) != 1 {
		t.Fatalf("落盘批次形状不对: %v", st.batches)
	}
	stamped := st.batches[0][0]
	if got := stamped.Annotations[HistoryAgentAnnotation]; got != "my-agent" {
		t.Errorf("归属 = %v, want %q", got, "my-agent")
	}
	if got := stamped.Annotations["已有键"]; got != "已有值" {
		t.Errorf("盖章时把业务原有 annotation 弄丢了: %v", got)
	}

	if _, polluted := orig.Annotations[HistoryAgentAnnotation]; polluted {
		t.Error("盖章污染了原消息 —— 那是 channel 里正在喂给模型的同一批指针，" +
			"多出个模型看不懂的键，还可能跟压缩层 marker 打架")
	}
	if stamped == orig {
		t.Error("盖章应写在拷贝上，实际返回了同一个指针")
	}
}

// TestWithoutHistoryCtx_Strips：摘掉之后这条分支下不再写历史。
//
// 内部决策 LLM（routing 分类器 / reflect 评估器 / plan 规划器）产出的是给框架看的
// 控制信号，不是对话。落进会话历史，业务回看时会读到一堆自己从没说过、也不该看见
// 的内容。
func TestWithoutHistoryCtx_Strips(t *testing.T) {
	st := &recordingHistoryStore{}
	ctx, stop := newTestHistoryCtx(t, st, "conv-1")

	recordHistory(ctx, "a", "a", []*schema.Message{schema.UserMessage("对话")})

	inner := WithoutHistoryCtx(ctx)
	if historyWriterFrom(inner) != nil {
		t.Fatal("WithoutHistoryCtx 后仍取得到 writer")
	}
	recordHistory(inner, "classifier", "classifier", []*schema.Message{schema.UserMessage("内部决策")})

	stop()

	got := st.flat()
	if len(got) != 1 || got[0] != "对话" {
		t.Errorf("落盘 %v, want 只有 [\"对话\"] —— 内部决策的产出漏进会话历史了", got)
	}
}

// TestWithoutHistoryCtx_BlocksNestedStart：摘除是「禁止」不是「当前没有」。
//
// 分类器 / 评估器往往**自己就是个 BasicAgent**，业务很可能顺手也给它配了
// WithHistory。如果摘除只是把 writer 置空，它的 Run 一进来发现「ctx 里没有」就自己
// 开一条——摘除形同虚设，内部推理照样灌进用户的会话。
func TestWithoutHistoryCtx_BlocksNestedStart(t *testing.T) {
	st := &recordingHistoryStore{}
	hc := &historyConfig{store: st, owner: "tenant-a"}
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")

	inner := WithoutHistoryCtx(ctx)
	innerCtx, stop := startHistory(inner, hc, "classifier") // 内层自带 history 配置
	defer stop()
	if historyWriterFrom(innerCtx) != nil {
		t.Fatal("被摘除的分支下不该再开出写入器 —— 否则内部决策 agent 只要自己配了 " +
			"WithHistory 就能绕过摘除")
	}

	recordHistory(innerCtx, "classifier", "classifier", []*schema.Message{schema.UserMessage("内部决策")})
	stop()
	if n := len(st.flat()); n != 0 {
		t.Errorf("被摘除的分支写了 %d 条, want 0", n)
	}
}

// TestWithHistory_FailFast：配置错在构造期就炸。
//
// 这两个都是「跑起来才发现」代价极高的错：typed nil 会在第一次写历史时才 nil deref，
// 业务从 stack 看不出根因；owner 为空则是把所有租户的对话写进同一个桶——不会报错，
// 直到有人看见别人的对话。
func TestWithHistory_FailFast(t *testing.T) {
	t.Run("typed nil store", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("typed nil HistoryStore 应构造期 panic")
			}
			if !strings.Contains(fmt.Sprint(r), "typed nil") {
				t.Errorf("panic 信息应点明 typed nil 陷阱, got %v", r)
			}
		}()
		var st *InMemoryHistoryStore // typed nil：interface header 有 type tag
		WithHistory(st, "tenant")(&Config{})
	})

	t.Run("owner 缺失", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("owner 为空且没给 ownerFunc 时应构造期 panic —— " +
					"否则所有租户写进同一个桶，直到有人看见别人的对话才发现")
			}
			if !strings.Contains(fmt.Sprint(r), "owner") {
				t.Errorf("panic 信息应点明 owner, got %v", r)
			}
		}()
		WithHistory(NewInMemoryHistoryStore(), "")(&Config{})
	})

	t.Run("owner 由 ownerFunc 提供则放行", func(t *testing.T) {
		c := &Config{}
		WithHistory(NewInMemoryHistoryStore(), "",
			WithHistoryOwnerFunc(func(context.Context) string { return "t" }))(c)
		if c.History == nil {
			t.Fatal("给了 ownerFunc 应放行")
		}
	})
}

// TestHistoryConfig_ResolveOwner：ownerFunc 优先，返空串回落静态 owner。
func TestHistoryConfig_ResolveOwner(t *testing.T) {
	hc := &historyConfig{owner: "static"}
	if got := hc.resolveOwner(context.Background()); got != "static" {
		t.Errorf("无 ownerFunc 时 = %q, want %q", got, "static")
	}

	hc.ownerFunc = func(context.Context) string { return "dynamic" }
	if got := hc.resolveOwner(context.Background()); got != "dynamic" {
		t.Errorf("有 ownerFunc 时 = %q, want %q", got, "dynamic")
	}

	hc.ownerFunc = func(context.Context) string { return "" }
	if got := hc.resolveOwner(context.Background()); got != "static" {
		t.Errorf("ownerFunc 返空串应回落静态 owner, got %q", got)
	}
}

// TestRecordHistory_NoOwner_Skips：ownerFunc 返空且无静态 owner 时不写。
//
// 构造期只挡住了「两个都没给」，业务动态解析失败（鉴权信息没取到）走的是这条路——
// 同样不能静默写到一个空租户下，那等于把它跟别人的混在一起。
func TestRecordHistory_NoOwner_Skips(t *testing.T) {
	st := &recordingHistoryStore{}
	hc := &historyConfig{store: st, ownerFunc: func(context.Context) string { return "" }}
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
	ctx, stop := startHistory(ctx, hc, "a")
	defer stop()

	recordHistory(ctx, "a", "a", []*schema.Message{schema.UserMessage("x")})
	stop()

	if n := len(st.flat()); n != 0 {
		t.Errorf("owner 解析失败时写了 %d 条, want 0 —— 空租户等于跟别人混在一起", n)
	}
}

// TestRecordHistory_NilSafe：nil 配置 / nil 消息 / 空批都不该崩。
func TestRecordHistory_NilSafe(t *testing.T) {
	st := &recordingHistoryStore{}
	ctx, stop := newTestHistoryCtx(t, st, "conv-1")

	recordHistory(context.Background(), "a", "a", []*schema.Message{schema.UserMessage("x")}) // ctx 无 writer
	recordHistory(ctx, "a", "a", nil)                                                         // 空批
	recordHistory(ctx, "a", "a", []*schema.Message{nil, nil})                                 // 全 nil 元素
	stop()

	if n := len(st.flat()); n != 0 {
		t.Errorf("上述路径都不该写入，实际写了 %d 条", n)
	}
}

// TestDropDanglingToolCalls_MisorderedPair：错位的一对，两个方向都不能留。
//
// 结果排在它的 assistant **前面**时，两边看对方都「存在」，但那个顺序 API 照样拒。曾经
// 一边用「全流任意位置出现过这个 id」的宽判据、另一边用顺序判据，于是这一对同时满足
// 「结果是孤儿要丢」和「声明找得到配对可以留」：结果被丢、assistant 被留，**悬空
// tool_call 上 wire → 400**。
//
// 这种形状不只来自 marker 前置——业务自定义 HistoryCompactor 删掉中间某条、后端 Load 丢
// 一条、Raw 视图把子 agent 的消息插进父的配对中间，都能造出来。
//
// **fixture 里那条无关消息是必须的**：不放它的话，这个场景的正确输出恰好是空列表，于是
// 「整个函数退化成 return nil」也能满足下面两条否定断言——实测过，真会 PASS。只验「坏的
// 没留下」永远不够，得同时验「好的还在」。
func TestDropDanglingToolCalls_MisorderedPair(t *testing.T) {
	keep := schema.UserMessage("KEEP_ME_MARK") // 与配对无关，任何情况下都该原样留下
	tr := schema.ToolResultMessage("call-1", "结果")
	as := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "call-1", Type: "function",
		Function: schema.FunctionCall{Name: "f", Arguments: "{}"},
	}})
	as.Content = "" // 纯 tool_call 轮，没有文本可保

	got := dropDanglingToolCalls([]*schema.Message{keep, tr, as})

	var kept bool
	for _, m := range got {
		if m != nil && strings.Contains(m.Content, "KEEP_ME_MARK") {
			kept = true
		}
	}
	if !kept {
		t.Fatalf("与配对无关的消息被一起丢了——清洗只该动坏掉的那部分。输出 %d 条: %v",
			len(got), contentsOf(got))
	}

	for i, m := range got {
		if m.Role == schema.RoleTool {
			t.Errorf("[%d] 孤儿 tool_result 应被丢掉（它排在声明者前面）", i)
		}
		if tcs := schema.GetToolCalls(m); len(tcs) > 0 {
			t.Errorf("[%d] 悬空 tool_call 应被中和——它的结果排在它前面、不算应答，"+
				"留着必 400", i)
		}
	}
}

// TestDropDanglingToolCalls_PartialPairing：多 tool_call 只有一部分拿到结果时，
// 没被应答的那几个必须补上占位，不能裸奔上 wire。
//
// 这个形状真实可达：工具轮被中断（fanout 里一部分先返回）、业务 HistoryCompactor 删掉了
// 中间某条、后端 Load 丢了一条。曾经的处理是「有任一配对就整条保留」——于是没被应答的
// tool_call 直接送上 wire，API 400。而删掉整条又会连带丢掉已经拿到结果的那部分。
func TestDropDanglingToolCalls_PartialPairing(t *testing.T) {
	as := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "X", Type: "function", Function: schema.FunctionCall{Name: "fx", Arguments: "{}"}},
		{ID: "Y", Type: "function", Function: schema.FunctionCall{Name: "fy", Arguments: "{}"}},
	})
	okResult := schema.ToolResultMessage("X", "X 的结果")

	got := dropDanglingToolCalls([]*schema.Message{as, okResult})

	// 逐条走一遍，验配对完整：每个声明都要有结果应答
	declared := map[string]bool{}
	for _, m := range got {
		if m.Role == schema.RoleTool {
			id := schema.GetToolResultID(m)
			if !declared[id] {
				t.Errorf("孤儿 tool_result(id=%s)", id)
			}
			delete(declared, id)
			continue
		}
		for _, tc := range schema.GetToolCalls(m) {
			declared[tc.ID] = true
		}
	}
	for id := range declared {
		t.Errorf("tool_call(id=%s) 没有结果应答——它会裸奔上 wire，真实 API 直接 400。"+
			"部分配对时应给缺的那几个补占位，而不是整条放行", id)
	}

	// X 的真结果要保住（不能因为 Y 缺结果就把整条连坐删掉）
	foundX := false
	for _, m := range got {
		if m.Role == schema.RoleTool && schema.GetToolResultID(m) == "X" &&
			strings.Contains(m.Content, "X 的结果") {
			foundX = true
		}
	}
	if !foundX {
		t.Error("X 已经拿到的结果被连坐删掉了")
	}
}

// TestHistoryWriter_SubmitAfterClose_NotQueued：收摊之后到的批次必须**丢掉**，不能入队。
//
// 这条钉的是「丢弃」而不是「没落盘」——两者的区别正是这个 bug 的隐蔽之处：worker 已经
// 退出了，入不入队最终都不会落盘，从外部看一模一样。差别在内存——入了队就永远堆在那里，
// 没人消费、没有日志、没有上限。长跑进程里每个迟到批次都是一次泄漏。
//
// 迟到从哪来：`recordHistory` 只该从节点体内同步调用，但业务的流式桥之类活过 Run 的
// goroutine 很容易在 Run 收摊后才调到它。那时候丢一批 + warn 是对的，静默堆积不是。
func TestHistoryWriter_SubmitAfterClose_NotQueued(t *testing.T) {
	st := &recordingHistoryStore{}
	w := newHistoryWriter(context.Background(), st, HistoryKey{Owner: "tenant-a", ConvID: "conv-1"})
	w.closeAndJoin()

	w.submit([]*schema.Message{schema.UserMessage("迟到的批次")})

	w.mu.Lock()
	queued := len(w.queue)
	w.mu.Unlock()
	if queued != 0 {
		t.Fatalf("收摊后 submit 入队了 %d 批——worker 已退出，这批永远不会被消费，"+
			"只会静默堆在内存里；正确行为是丢弃 + warn", queued)
	}
	if got := len(st.flat()); got != 0 {
		t.Fatalf("收摊后的批次落盘了 %d 条，want 0", got)
	}
	w.closeAndJoin() // 幂等：重复收摊不能 hang 也不能 panic
}

// TestHistoryPrior_ThreeViews：跨 agent 历史的三档可见性各自成立。
//
// 一场会话是一条流，里面混着多个 agent 说的话。当前 agent 能看到别人多少，由
// `WithHandoffHistoryMode` 定三档。**归属标注是这三档唯一的判据**——分不清谁说的，
// 三档就同时垮掉：SelfOnly 会看到别人的、Raw 丢掉出处、Rewrite 不改写。
//
// 这条是变异测试补的：把归属判断改成「全当自己的」，原有整套测试无一报警——说明这三档
// 此前根本没被验过。
func TestHistoryPrior_ThreeViews(t *testing.T) {
	st := NewInMemoryHistoryStore()
	key := HistoryKey{Owner: "tenant-a", ConvID: "conv-1"}
	seed := []*schema.Message{
		stampHistoryAgent(schema.UserMessage("用户的问题"), "alice", "alice", ""),
		stampHistoryAgent(schema.AssistantMessage("alice 说的话"), "alice", "alice", ""),
		stampHistoryAgent(schema.AssistantMessage("bob 说的话"), "bob", "bob", ""),
	}
	if err := st.Append(context.Background(), key, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}
	hc := &historyConfig{store: st, owner: "tenant-a"}
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")

	// 以 bob 的视角看这条流。
	t.Run("SelfOnly——别人的一概不看", func(t *testing.T) {
		got, ok := historyPrior(ctx, hc, "bob", HandoffHistorySelfOnly, 0)
		if !ok {
			t.Fatal("historyPrior 报 false")
		}
		if len(got) != 1 || got[0].Content != "bob 说的话" {
			t.Fatalf("SelfOnly 拿到 %s——别人说的话不该出现", contentsOf(got))
		}
	})

	t.Run("Raw——全看，别人的标出处", func(t *testing.T) {
		got, ok := historyPrior(ctx, hc, "bob", HandoffHistoryRaw, 0)
		if !ok {
			t.Fatal("historyPrior 报 false")
		}
		if len(got) != 3 {
			t.Fatalf("Raw 拿到 %d 条, want 3 —— %s", len(got), contentsOf(got))
		}
		for i, m := range got {
			from, marked := m.Annotations["from_agent"].(string)
			isSelf := m.Content == "bob 说的话"
			switch {
			case isSelf && marked:
				t.Fatalf("[%d] 自己说的话被标了 from_agent=%q", i, from)
			case !isSelf && !marked:
				t.Fatalf("[%d] %q 是别人说的却没标出处——Raw 档的全部价值就是保留出处",
					i, m.Content)
			case !isSelf && from != "alice":
				t.Fatalf("[%d] from_agent=%q, want alice", i, from)
			}
			if m.Role != seed[i].Role {
				t.Fatalf("[%d] role 变了 %s→%s —— Raw 档不改写，只加标注", i, seed[i].Role, m.Role)
			}
		}
	})

	t.Run("Rewrite——别人的转述成旁白", func(t *testing.T) {
		got, ok := historyPrior(ctx, hc, "bob", HandoffHistoryRewrite, 0)
		if !ok {
			t.Fatal("historyPrior 报 false")
		}
		if len(got) != 3 {
			t.Fatalf("Rewrite 拿到 %d 条, want 3 —— %s", len(got), contentsOf(got))
		}
		var rewritten int
		for _, m := range got {
			if m.Content == "bob 说的话" {
				if m.Role != schema.RoleAssistant {
					t.Fatalf("自己说的话被改写了：role=%s content=%q", m.Role, m.Content)
				}
				continue
			}
			if strings.Contains(m.Content, "For context:") {
				rewritten++
				if m.Role != schema.RoleUser {
					t.Fatalf("改写后的旁白 role=%s, want user", m.Role)
				}
				if !strings.Contains(m.Content, "alice") {
					t.Fatalf("改写后丢了出处: %q", m.Content)
				}
			}
		}
		// user 消息原样透传（不是 assistant/tool，无需转述），只有 alice 那条 assistant 被改写。
		if rewritten != 1 {
			t.Fatalf("改写了 %d 条, want 1 —— %s", rewritten, contentsOf(got))
		}
	})
}

// suicidalHistoryStore 在 Append 里调 runtime.Goexit——**不是 panic**，recover 拦不住。
// 这正是测试替身在 Append 里调 t.Fatal 的形状，业务照 history_conformance 写自己
// backend 的测试时最容易踩。
type suicidalHistoryStore struct{}

func (suicidalHistoryStore) Append(context.Context, HistoryKey, []*schema.Message) error {
	runtime.Goexit()
	return nil
}
func (suicidalHistoryStore) Load(context.Context, HistoryKey, HistoryQuery) (HistoryPage, error) {
	return HistoryPage{}, nil
}

// TestHistoryWriter_DeathIsAnnounced：worker 非正常退出时必须留下诊断信号。
//
// 这条测的是**日志**，不是行为——worker 死了之后，写入器对外的表现跟「后端很慢」完全
// 一样：submit 不报错、Run 照跑、历史就是少一截。没有这条日志，业务面对的是一个无从
// 下手的静默故障；有了它，第一条 grep 就能定位。
//
// 用 Goexit 而不是 panic：panic 那条路已经被 recover 覆盖了，而 Goexit **不可 recover**,
// 是真正会让 worker 无声消失的那种死法。
func TestHistoryWriter_DeathIsAnnounced(t *testing.T) {
	var buf strings.Builder
	var mu sync.Mutex
	h := slog.NewTextHandler(&syncWriter{w: &buf, mu: &mu}, &slog.HandlerOptions{Level: slog.LevelError})
	orig := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(orig) })

	w := newHistoryWriter(context.Background(), suicidalHistoryStore{}, HistoryKey{Owner: "t", ConvID: "c"})
	w.submit([]*schema.Message{schema.UserMessage("触发 Goexit")})
	w.wg.Wait() // worker 已 Goexit，defer 里的 wg.Done + 告警已跑完

	mu.Lock()
	logged := buf.String()
	mu.Unlock()
	if !strings.Contains(logged, "提前退出") {
		t.Fatalf("worker 非正常退出却没留下任何 ERROR 日志——这是这类故障唯一的诊断信号。\n捕获到的日志: %q", logged)
	}
	if !strings.Contains(logged, "Goexit") {
		t.Fatalf("告警没指出 Goexit 这个成因，业务拿到日志仍然不知道该查什么。\n捕获到: %q", logged)
	}

	w.mu.Lock()
	w.closed = true // 手工收摊：worker 已经没了，closeAndJoin 的 wg.Wait 会直接返回
	w.mu.Unlock()
	w.closeAndJoin()
}

// syncWriter 给 slog handler 加锁——worker goroutine 和测试主体会同时碰这个 buffer。
type syncWriter struct {
	w  *strings.Builder
	mu *sync.Mutex
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

// suppressedHistoryStore 记下 Load 被调用过没有——摘除的分支根本不该碰后端。
type suppressedHistoryStore struct{ loaded bool }

func (s *suppressedHistoryStore) Append(context.Context, HistoryKey, []*schema.Message) error {
	return nil
}
func (s *suppressedHistoryStore) Load(context.Context, HistoryKey, HistoryQuery) (HistoryPage, error) {
	s.loaded = true
	return HistoryPage{}, nil
}

// TestLoadHistory_SuppressedFallsBackToEvents：摘除的分支读写两侧都不走历史层。
//
// 摘除挡住的是写入器。只挡写不挡读的话，业务给分类器随手配一个 WithHistory 就掉进一个
// 静默降级：写侧被摘掉、那个 store 永远空，读侧却报「读成功，只是没内容」——调用方据此
// **不回落 session events**，这个内部决策 LLM 于是什么上文都看不到。
//
// 报 false 让它跟「压根没配历史」表现一致，顺带也不该去碰后端。
func TestLoadHistory_SuppressedFallsBackToEvents(t *testing.T) {
	st := &suppressedHistoryStore{}
	hc := &historyConfig{store: st, owner: "tenant-a"}
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
	ctx = WithoutHistoryCtx(ctx)

	msgs, ok := historyPrior(ctx, hc, "a", HandoffHistorySelfOnly, 0)
	if ok {
		t.Error("摘除的分支报了「历史层接管了这次读」——调用方会因此不回落 events，" +
			"而写侧已被摘掉、这个 store 永远是空的，等于让它什么上文都看不到")
	}
	if len(msgs) != 0 {
		t.Errorf("摘除的分支读回了 %d 条", len(msgs))
	}
	if st.loaded {
		t.Error("摘除的分支还是去碰了后端——白跑一次 IO")
	}
}

// TestHistoryWriter_WaitDrained_ReadsOwnWrites：排空之后必须读得到刚提交的那批。
func TestHistoryWriter_WaitDrained_ReadsOwnWrites(t *testing.T) {
	st := &recordingHistoryStore{}
	w := newHistoryWriter(context.Background(), st, HistoryKey{Owner: "t", ConvID: "c"})
	defer w.closeAndJoin()

	for i := 0; i < 20; i++ {
		w.submit([]*schema.Message{schema.UserMessage(fmt.Sprintf("m-%02d", i))})
	}
	w.waitDrained(context.Background())

	if got := len(st.flat()); got != 20 {
		t.Fatalf("排空后落盘 %d 条, want 20 —— waitDrained 放行得太早，"+
			"读会看到一个还没写完的历史", got)
	}
}

// TestHistoryWriter_WaitDrained_NotStuckOnDeadWorker：worker 死了就不能再等。
//
// 队列里的东西在 worker 死后永远不会落盘，继续等就是把主链路挂死在一个旁路组件上。
// 这条用 Goexit 造死法——它不可 recover，是真正会让 worker 无声消失的那种。
func TestHistoryWriter_WaitDrained_NotStuckOnDeadWorker(t *testing.T) {
	w := newHistoryWriter(context.Background(), suicidalHistoryStore{}, HistoryKey{Owner: "t", ConvID: "c"})
	w.submit([]*schema.Message{schema.UserMessage("触发 Goexit")})
	w.wg.Wait() // worker 已经没了

	// 死后再提交一批：队列非空、但永远不会有人消费。
	w.submit([]*schema.Message{schema.UserMessage("没人会写的一批")})

	done := make(chan struct{})
	go func() {
		w.waitDrained(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker 已死，waitDrained 却还在等——主链路被一个旁路组件挂死了")
	}

	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.closeAndJoin()
}

// TestHistoryWriter_WaitDrained_ConcurrentReaders：多个读者同时等，不能有人漏醒。
func TestHistoryWriter_WaitDrained_ConcurrentReaders(t *testing.T) {
	st := &recordingHistoryStore{}
	w := newHistoryWriter(context.Background(), st, HistoryKey{Owner: "t", ConvID: "c"})
	defer w.closeAndJoin()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w.submit([]*schema.Message{schema.UserMessage(fmt.Sprintf("m-%02d", i))})
			w.waitDrained(context.Background())
			// 排空之后，自己那条必须已经在里面。
			var found bool
			for _, c := range st.flat() {
				if c == fmt.Sprintf("m-%02d", i) {
					found = true
				}
			}
			if !found {
				t.Errorf("goroutine %d 排空后没看到自己刚写的那条", i)
			}
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("并发读者卡住了——有人没被唤醒")
	}
}

// TestWarnUnnamedAgentInHistory：不起名却在写历史的 agent，必须留下提醒。
//
// 构造期那条检查以「自己配了 WithHistory」为前提，够不着这条路：子 agent 完全不配历史，
// 只因为挂在开了历史的父下面就参与进来——写盖自己的名字、读按自己的名字过滤。而构造它的
// 时候还不知道会被嵌套到哪，所以只能运行时说。
//
// 后果跟构造期那条一样：名字每次构造都变，写进去的历史下次一条都对不上，而且是「读成功
// 只是没内容」不回落 events 的那种失忆。
func TestWarnUnnamedAgentInHistory(t *testing.T) {
	capture := func(fn func()) string {
		var buf strings.Builder
		var mu sync.Mutex
		orig := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(
			&syncWriter{w: &buf, mu: &mu}, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(orig)
		fn()
		mu.Lock()
		defer mu.Unlock()
		return buf.String()
	}

	newCtxWithWriter := func(t *testing.T) context.Context {
		t.Helper()
		hc := &historyConfig{store: &recordingHistoryStore{}, owner: "tenant-a"}
		ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
		ctx, stop := startHistory(ctx, hc, "parent")
		t.Cleanup(stop)
		return ctx
	}

	t.Run("没起名——提醒", func(t *testing.T) {
		ctx := newCtxWithWriter(t)
		cfg := defaultConfig() // 名字是自动生成的
		got := capture(func() { warnUnnamedAgentInHistory(ctx, &cfg) })
		if !strings.Contains(got, "WithName") {
			t.Errorf("没起名却没有提醒——业务无从知道自己的历史读不回来\n捕获: %q", got)
		}
	})

	t.Run("起了名——不提醒", func(t *testing.T) {
		ctx := newCtxWithWriter(t)
		cfg := defaultConfig()
		WithName("child")(&cfg)
		if got := capture(func() { warnUnnamedAgentInHistory(ctx, &cfg) }); got != "" {
			t.Errorf("起了名还提醒——会刷屏\n捕获: %q", got)
		}
	})

	t.Run("没参与历史流——不提醒", func(t *testing.T) {
		cfg := defaultConfig()
		ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
		if got := capture(func() { warnUnnamedAgentInHistory(ctx, &cfg) }); got != "" {
			t.Errorf("压根没开历史也提醒——那时名字随便起\n捕获: %q", got)
		}
	})

	t.Run("同一条流只提醒一次", func(t *testing.T) {
		ctx := newCtxWithWriter(t)
		got := capture(func() {
			for i := 0; i < 5; i++ {
				cfg := defaultConfig()
				warnUnnamedAgentInHistory(ctx, &cfg)
			}
		})
		if n := strings.Count(got, "WithName"); n != 1 {
			t.Errorf("提醒了 %d 次, want 1——嵌套子 agent 一个 Run 里可能被调很多次，会刷爆日志", n)
		}
	})

	t.Run("被摘除的分支——不提醒", func(t *testing.T) {
		ctx := WithoutHistoryCtx(newCtxWithWriter(t))
		cfg := defaultConfig()
		if got := capture(func() { warnUnnamedAgentInHistory(ctx, &cfg) }); got != "" {
			t.Errorf("摘除的分支压根不进历史，不该提醒\n捕获: %q", got)
		}
	})
}

// blockedHistoryStore 的 Append 挂死到测试收尾才放行——模拟无视 ctx 的违约后端
// （死锁的连接池 / 没配超时的 client）。用可放行的通道而不是裸 select{}，测试结束时
// 能收干净，不留永久泄漏的 goroutine 干扰后续用例。
type blockedHistoryStore struct{ release chan struct{} }

func (s *blockedHistoryStore) Append(context.Context, HistoryKey, []*schema.Message) error {
	<-s.release
	return nil
}
func (s *blockedHistoryStore) Load(context.Context, HistoryKey, HistoryQuery) (HistoryPage, error) {
	return HistoryPage{}, nil
}

// TestHistoryWriter_WaitDrained_CtxCancelUnblocks：ctx 取消后，等待方必须放行。
//
// waitDrained 跑在图内节点里。框架对业务 NodeFunc 的契约是「尊重 ctx，deadline 才能兜底」
// ——框架自己的节点体不能例外。后端尊重 ctx 时用不到这条（worker 的 Append 会 fail-fast
// 间接放行）；后端**无视 ctx 挂死**时它是唯一出口：没有它，取消连图都收敛不了，本超步的
// cp 存不上。实测修复前：ctx 取消 2 秒后读侧仍卡死。
func TestHistoryWriter_WaitDrained_CtxCancelUnblocks(t *testing.T) {
	st := &blockedHistoryStore{release: make(chan struct{})}
	w := newHistoryWriter(context.Background(), st, HistoryKey{Owner: "t", ConvID: "c"})
	// defer 是 LIFO：先注册 closeAndJoin 让它**后**执行——必须等 close(release) 放行
	// 卡住的 Append 之后再 join，否则 join 自己会死等。只 close 不 join 的话 worker 会
	// 永久 park 在 cond.Wait()，-count=N 跑就累积 N 个泄漏 goroutine。
	defer w.closeAndJoin()
	defer close(st.release)
	w.submit([]*schema.Message{schema.UserMessage("会卡住的一批")})

	// 等 worker 取走这批进入 inFlight——不然 waitDrained 可能在它入队前就判空返回了。
	deadline := time.Now().Add(2 * time.Second)
	for {
		w.mu.Lock()
		busy := w.inFlight
		w.mu.Unlock()
		if busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker 一直没进入 inFlight")
		}
		time.Sleep(time.Millisecond)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.waitDrained(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ctx 已取消 2 秒，waitDrained 仍在等——旁路组件挂死图内节点，" +
			"deadline 兜底失效、本超步 cp 存不上")
	}
}

// gatedThenSuicidalStore：第 1 批等放行后成功，第 2 批 Goexit——用来把「收摊排空窗口里
// worker 猝死」摆成确定性时序。
type gatedThenSuicidalStore struct {
	mu      sync.Mutex
	n       int
	release chan struct{}
}

func (s *gatedThenSuicidalStore) Append(context.Context, HistoryKey, []*schema.Message) error {
	s.mu.Lock()
	s.n++
	n := s.n
	s.mu.Unlock()
	if n == 1 {
		<-s.release
		return nil
	}
	runtime.Goexit()
	return nil
}
func (s *gatedThenSuicidalStore) Load(context.Context, HistoryKey, HistoryQuery) (HistoryPage, error) {
	return HistoryPage{}, nil
}

// TestHistoryWriter_DeathDuringDrain_IsAnnounced：收摊排空的窗口里猝死，同样要有信号。
//
// 猝死告警原来只在 `alive`（closed 还是 false）时发——closeAndJoin 已标 closed 之后的
// 排空窗口里 Goexit，告警静默跳过。而 godoc 承诺这个信号防的正是「历史莫名少了一截、
// 没有任何日志」，该症状在这个窗口原样复现。
//
// **判据必须用 inFlight 不能用队列长度**：worker 取批是把整个队列搬进局部 slice，死在
// 写批中途时没写完的批次在局部变量里，`len(w.queue)` 恒为 0——按队列长度判，正好在这个
// 窗口里永远不触发。死在写批中途的可靠信号是 inFlight 还没来得及清。
func TestHistoryWriter_DeathDuringDrain_IsAnnounced(t *testing.T) {
	var buf strings.Builder
	var lmu sync.Mutex
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(
		&syncWriter{w: &buf, mu: &lmu}, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { slog.SetDefault(orig) })

	st := &gatedThenSuicidalStore{release: make(chan struct{})}
	w := newHistoryWriter(context.Background(), st, HistoryKey{Owner: "t", ConvID: "c"})

	// 第 1 批：worker 取走它、卡在 Append 里（inFlight=true）。
	w.submit([]*schema.Message{schema.UserMessage("第一批")})
	deadline := time.Now().Add(2 * time.Second)
	for {
		w.mu.Lock()
		busy := w.inFlight
		w.mu.Unlock()
		if busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker 没进入 inFlight")
		}
		time.Sleep(time.Millisecond)
	}
	// 第 2、3 批入队——放行后 worker 下一次取批会把两批一起搬进局部 slice。
	w.submit([]*schema.Message{schema.UserMessage("第二批")})
	w.submit([]*schema.Message{schema.UserMessage("第三批")})

	// 收摊（另一 goroutine——它要等 join）→ 放行 → 第二批 Goexit，第三批还在局部 slice 里。
	joined := make(chan struct{})
	go func() {
		w.closeAndJoin()
		close(joined)
	}()
	close(st.release)
	select {
	case <-joined:
	case <-time.After(2 * time.Second):
		t.Fatal("closeAndJoin 没返回")
	}

	lmu.Lock()
	logged := buf.String()
	lmu.Unlock()
	if !strings.Contains(logged, "提前退出") {
		t.Fatalf("收摊排空窗口里 worker 猝死（第三批静默蒸发）却没有任何 ERROR——"+
			"godoc 承诺的信号在这个窗口失效。\n捕获到的日志: %q", logged)
	}
}

// TestHistoryWriter_NormalShutdown_NoDeathWarning：正常收摊不能误报猝死。
//
// 猝死判据是三个信号取或（alive / diedWorking / backlog），这条钉住「三个在正常路径上
// 全为 false」——判据改宽一档（比如把 inFlight 的清理挪到告警检查之后）就会让每次正常
// 收摊都吼一嗓子，告警从信号变成噪音。
func TestHistoryWriter_NormalShutdown_NoDeathWarning(t *testing.T) {
	var buf strings.Builder
	var lmu sync.Mutex
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(
		&syncWriter{w: &buf, mu: &lmu}, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { slog.SetDefault(orig) })

	st := &recordingHistoryStore{}
	w := newHistoryWriter(context.Background(), st, HistoryKey{Owner: "t", ConvID: "c"})
	for i := 0; i < 5; i++ {
		w.submit([]*schema.Message{schema.UserMessage(fmt.Sprintf("m-%d", i))})
	}
	w.closeAndJoin()

	lmu.Lock()
	logged := buf.String()
	lmu.Unlock()
	if strings.Contains(logged, "提前退出") {
		t.Fatalf("正常收摊误报了猝死——告警从信号变成噪音。\n捕获: %q", logged)
	}
	if got := len(st.flat()); got != 5 {
		t.Fatalf("落盘 %d 条, want 5", got)
	}
}

// agent 的记录不带前文标注：它只由写前文那一处盖，带着的话这条记录会被读成调用方的前文——不按归属过滤、被挪到最前。
func TestStampHistoryAgent_StripsCallerAnnotation(t *testing.T) {
	src := schema.UserMessage("转手的前文")
	src.Annotations = map[string]any{CallerHistoryAnnotation: true, "biz": "keep"}
	got := stampHistoryAgent(src, "alice", "alice", "")
	if isCallerHistory(got) {
		t.Errorf("agent 的记录被标成了调用方的前文：%v", got.Annotations)
	}
	if got.Annotations["biz"] != "keep" {
		t.Errorf("业务标注丢了：%v", got.Annotations)
	}
	if !isCallerHistory(src) {
		t.Errorf("改到了原消息的标注")
	}
}
