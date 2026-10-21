package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
)

// memErrStore 在真 store 外面挂一层错误注入：按 namespace 决定这次 Search 返什么错，
// 以及 ListNamespaces 返什么错。
//
// ⚠️ **故意只嵌 store.Store 接口、不嵌 *InMemoryStore**：后者带 RanksByRelevance()，
// 被提升上来会让 searchMemories 走「后端自己排好了，别插手」那条早退，整个扇出路径
// 根本不执行（炸弹类型 2：早退兜底盖过 fix 路径）。
type memErrStore struct {
	store.Store
	listErr   error
	searchErr map[string]error
	counted   map[string]int    // 分片被查次数（nil = 不计）
	listExtra []store.Namespace // 额外塞给 ListNamespaces 的路径（模拟不合规后端）
}

func (m *memErrStore) ListNamespaces(ctx context.Context, opts ...store.ListOption) ([]store.Namespace, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	nss, err := m.Store.ListNamespaces(ctx, opts...)
	return append(nss, m.listExtra...), err
}

func (m *memErrStore) Search(ctx context.Context, ns store.Namespace, opts ...store.SearchOption) ([]*store.SearchItem, error) {
	if m.counted != nil {
		m.counted[ns.String()]++
	}
	if err := m.searchErr[ns.String()]; err != nil {
		return nil, err
	}
	return m.Store.Search(ctx, ns, opts...)
}

func gateInterrupt(id string) error {
	return &compose_v2.InterruptError{
		Reason: "业务在 Store 后端做人在回路 gate",
		Query:  &compose_v2.AskQuery{InterruptID: id, Question: "批准读取长期记忆吗"},
	}
}

// seedTwoWriters 铺一个**多写入者**的基准 ns。
//
// ⚠️ 少于 2 个写入者时 gatherMemoryCandidates 直接早退去查基准 ns，扇出循环一行不跑
// ——那样测的是另一条路。铺完顺手断言 fixture 真的搭对了。
func seedTwoWriters(t *testing.T, base store.Namespace) store.Store {
	t.Helper()
	st := store.NewInMemoryStore()
	ctx := context.Background()
	if err := st.Put(ctx, base, "seed", "顶层记下的：用户养了猫"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	for _, w := range []string{"w1", "w2"} {
		if err := st.Put(ctx, append(append(store.Namespace(nil), base...), w), "k", "写入者"+w+"记下的：用户养了猫"); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	nss, err := st.ListNamespaces(ctx, store.WithListPrefix(base), store.WithListMaxDepth(len(base)+1))
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	subs := 0
	for _, ns := range nss {
		if len(ns) == len(base)+1 {
			subs++
		}
	}
	if subs < 2 {
		t.Fatalf("fixture 没搭对：需要 >=2 个写入者子 ns 才进扇出路径，实际 %d", subs)
	}
	return st
}

func runWithMemory(t *testing.T, st store.Store, base store.Namespace) error {
	t.Helper()
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("memuser"),
		agent_v2.WithModel(newScopeStub(schema.AssistantMessage("答复"))),
		agent_v2.WithStoreMemory(base, &agent_v2.StoreMemoryConfig{}),
		agent_v2.WithMaxRounds(2))
	// query 非空是进打分路径的前提——空 query 时 searchMemories 直接返 nil,nil
	_, err := ag.Run(store.WithContext(context.Background(), st), agent_v2.NewRunSession(),
		schema.UserMessage("我的猫怎么样"))
	return err
}

// TestStoreMemory_ListNamespacesErrorNotSwallowed 钉住：列写入者这一步的错**不许吞**。
//
// 曾经吞得干干净净——`nss, err := st.ListNamespaces(...)` 之后是 `if err == nil {...}`，
// err != nil 就当「只有一个写入者」，静默降级去查基准 ns。后果：业务在自己的 Store 实现里
// 抛 InterruptError 做人在回路 gate（框架文档明确支持这么用）时，**中断整条消失**——
// 没人被问、Run 照常返成功。
func TestStoreMemory_ListNamespacesErrorNotSwallowed(t *testing.T) {
	base := store.NS("mem")
	st := &memErrStore{Store: seedTwoWriters(t, base), listErr: gateInterrupt("gate-list")}

	err := runWithMemory(t, st, base)
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("ListNamespaces 抛的中断被吞了：Run 返 %v（人没被问、Run 照常收尾）", err)
	}
}

// TestStoreMemory_InterruptBeatsEarlierPlainError 钉住：扇出里**中断优先**，不是先到先得。
//
// 曾经是 `if firstErr == nil { firstErr = serr }` 先到先得。于是 1 号写入者返普通业务错、
// 2 号返 InterruptError 时，返出去的是普通错——`IsInterrupt` 为 false，上层按「这轮记忆
// 查不到，warn 一下继续」处理，而业务那道 gate 永远等不到人来答。
//
// 这条跟引擎层超步部分失败的仲裁是同一件事，判据与理由见 compose_v2 的
// frontierResult.exitErr：终局失败 > 中断 > 第一个业务错。
func TestStoreMemory_InterruptBeatsEarlierPlainError(t *testing.T) {
	base := store.NS("mem")
	// 顺序承重：基准 ns 先查（放行）→ w1 返普通错（先到）→ w2 返中断（后到）
	//
	// ⚠️ key 用 Namespace.String() 现算，别写字面量——分隔符是 `.` 不是 `/`，写死过一次
	// `"mem/w1"`：map 一次没命中 ⟹ 根本没注入错误 ⟹ Run 返 nil ⟹ 测试报「缺陷还在」，
	// 而那时缺陷确实还在，**结论蒙对了、fixture 是错的**。下面顺手断言注入真的发生了。
	sub := func(w string) string {
		return append(append(store.Namespace(nil), base...), w).String()
	}
	inner := seedTwoWriters(t, base)
	st := &memErrStore{
		Store: inner,
		searchErr: map[string]error{
			sub("w1"): errors.New("db connection refused"),
			sub("w2"): gateInterrupt("gate-w2"),
		},
	}
	for k := range st.searchErr {
		if _, err := st.Search(context.Background(), store.NS(k)); err == nil {
			t.Fatalf("fixture 没搭对：注入的错在 ns %q 上没生效", k)
		}
	}

	err := runWithMemory(t, st, base)
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("先到的普通错盖掉了后到的中断：Run 返 %v", err)
	}
}

// TestStoreMemory_SameInterruptIDAcrossShardsStillSuspends 钉住：**分片扇出不许把业务的
// 一个提问变成「一批里的两条」**。
//
// 业务在自实现的 store.Store.Search 里做 HITL gate、用常量 InterruptID —— 从业务视角这是
// **一个**问题（「批准读取长期记忆吗」），它无从得知框架会把 N 次独立的 Search 调用合并
// 成一批。而一旦合并，`validateInterrupt` 的「同一批内撞 ID 必须拒」当场命中：
//
//	run aborted (terminal failure, checkpoint purged): 两条 query 用了相同的 InterruptID
//
// 后果不只是报错——cp 子树被清、Resume 返 ErrNoPendingCheckpoint，**HITL 闭环不可恢复**。
//
// 正解是让中断在这条路上**当场停**：中断的语义就是「停下来问」，一个分片说了停，还接着
// 去查别的分片本来就不对。停了就没有「一批」，业务那个常量 ID 也就不再是问题。
func TestStoreMemory_SameInterruptIDAcrossShardsStillSuspends(t *testing.T) {
	base := store.NS("mem")
	sub := func(w string) string {
		return append(append(store.Namespace(nil), base...), w).String()
	}
	// 两个分片各自抛中断，**用同一个业务自填 ID**
	st := &memErrStore{
		Store: seedTwoWriters(t, base),
		searchErr: map[string]error{
			sub("w1"): gateInterrupt("MEM_GATE"),
			sub("w2"): gateInterrupt("MEM_GATE"),
		},
	}

	err := runWithMemory(t, st, base)
	if errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("业务用常量 ID 的 HITL gate 被判成协议违规、存档被清：%v", err)
	}
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("应正常挂起等回答，实际 %v", err)
	}
}

// TestStoreMemory_InterruptStopsFanoutImmediately 钉住中断**当场停**这件事本身。
//
// 上一条只看得见「没被判成违规」，看不见「有没有继续去查别的分片」——而后者才是承重的那
// 一半（继续查 = 造出「一批」= 上一条那个坑随时会回来）。这里数实际发出的分片查询数。
func TestStoreMemory_InterruptStopsFanoutImmediately(t *testing.T) {
	base := store.NS("mem")
	sub := func(w string) string {
		return append(append(store.Namespace(nil), base...), w).String()
	}
	st := &memErrStore{
		Store:     seedTwoWriters(t, base),
		searchErr: map[string]error{sub("w1"): gateInterrupt("GATE_W1")},
		counted:   map[string]int{},
	}

	if err := runWithMemory(t, st, base); err == nil {
		t.Fatalf("fixture 没搭对：w1 的 gate 该抛中断")
	}
	// base 先查（成功）→ w1 抛中断 → **不该再有 w2**
	if n := st.counted[sub("w2")]; n != 0 {
		t.Errorf("w1 已经说了停，还去查了 w2 %d 次——那就会造出「一批」两条 query", n)
	}
	if n := st.counted[base.String()]; n != 1 {
		t.Errorf("fixture 没搭对：基准 ns 该被查 1 次，实际 %d", n)
	}
}

// memCapturingStub 记下每次 LLM 调用收到的完整消息流——「记忆到底注进去没有」只能在这儿看。
type memCapturingStub struct {
	mu   sync.Mutex
	seen [][]*schema.Message
}

func (s *memCapturingStub) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	s.mu.Lock()
	s.seen = append(s.seen, append([]*schema.Message(nil), msgs...))
	s.mu.Unlock()
	return schema.AssistantMessage("答复"), nil
}

func (s *memCapturingStub) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	m, err := s.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(m)
	schema.CloseSend(sr)
	return sr, nil
}

func (s *memCapturingStub) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return s, nil }

// injectedCount 数注进去的记忆条数：带 StoreInjectedAnnotation 的消息。
func (s *memCapturingStub) injectedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, round := range s.seen {
		for _, m := range round {
			if m != nil && m.Annotations != nil {
				if _, ok := m.Annotations[agent_v2.StoreInjectedAnnotation]; ok {
					n++
				}
			}
		}
	}
	return n
}

// TestStoreMemory_OneShardFailsOthersStillInject 钉住「一个分片挂掉不该让整轮零注入」。
//
// 这句承诺是 gatherMemoryCandidates 的 godoc 自己写的（「这个分片跳过，别的照查……不该让
// 整轮注入失败」），而它在端到端上一度为假：函数内部确实跳过了坏分片，但 searchMemories
// 拿到非 nil err 就 `return nil, err`，把**已经查成功的那几个分片的候选整批丢掉**。
// 实测「base 和 w2 都查成功了，实际注入 0 条」。
//
// 断言选在「注进去几条」而不是「Run 有没有报错」——后者在这个退化下不变（本来就不报错，
// 只 warn），是个不承重的维度。
func TestStoreMemory_OneShardFailsOthersStillInject(t *testing.T) {
	base := store.NS("mem")
	sub := func(w string) string {
		return append(append(store.Namespace(nil), base...), w).String()
	}
	st := &memErrStore{
		Store:     seedTwoWriters(t, base),
		searchErr: map[string]error{sub("w1"): errors.New("db connection refused")},
	}

	cap := &memCapturingStub{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("memuser"), agent_v2.WithModel(cap),
		agent_v2.WithStoreMemory(base, &agent_v2.StoreMemoryConfig{}),
		agent_v2.WithMaxRounds(2))
	if _, err := ag.Run(store.WithContext(context.Background(), st),
		agent_v2.NewRunSession(), schema.UserMessage("我的猫怎么样")); err != nil {
		t.Fatalf("普通后端错不该让 Run 失败：%v", err)
	}

	// base 的 seed 和 w2 的记忆都命中 query「猫」，w1 挂了
	if n := cap.injectedCount(); n < 2 {
		t.Errorf("只有 w1 挂了，base 和 w2 查成功的记忆该照常注入，实际注入 %d 条——"+
			"已经查到的候选被整批丢掉了", n)
	}
}

// TestStoreMemory_LateWritersNotStarvedByCap 钉住**候选池上限不得按分片位置饿死写入者**。
//
// 按写入者分片扇出的整个目的就是「别让某个写入者因为位置被挡在候选池外」（分片之前是靠
// 后端的默认排序取前 N 条，字典序靠后的写入者永远进不来）。而当时那个「候选池填满就 break」在**分片层**
// 把同一个病引了回来：前几个写入者数据多、填满 500，后面的写入者一次都不会被 Search，它们的记忆**永远进不了候选池**——跟谁更相关无关，只跟它排第几有关。
//
// 断言选在「靠后的写入者有没有被查过」而不是「注入结果对不对」——后者在这个退化下**看着
// 是对的**（前面写入者的记忆确实相关），承重的量是覆盖面。
func TestStoreMemory_LateWritersNotStarvedByCap(t *testing.T) {
	base := store.NS("mem")
	st := store.NewInMemoryStore()
	ctx := context.Background()
	// 8 个写入者，前 6 个各塞 100 条（cap=500，前 5 个就填满）
	writers := []string{"w1", "w2", "w3", "w4", "w5", "w6", "w7", "w8"}
	for i, w := range writers {
		ns := append(append(store.Namespace(nil), base...), w)
		n := 100
		if i >= 6 {
			n = 2 // 最后两个写入者只有少量记忆
		}
		for k := 0; k < n; k++ {
			if err := st.Put(ctx, ns, fmt.Sprintf("k%03d", k), w+" 记下的：用户养了猫"); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
	}
	probe := &memErrStore{Store: st, counted: map[string]int{}}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("memuser"), agent_v2.WithModel(&memCapturingStub{}),
		agent_v2.WithStoreMemory(base, &agent_v2.StoreMemoryConfig{}),
		agent_v2.WithMaxRounds(2))
	if _, err := ag.Run(store.WithContext(ctx, probe), agent_v2.NewRunSession(),
		schema.UserMessage("我的猫怎么样")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var never []string
	for _, w := range writers {
		ns := append(append(store.Namespace(nil), base...), w)
		if probe.counted[ns.String()] == 0 {
			never = append(never, w)
		}
	}
	if len(never) > 0 {
		t.Errorf("这些写入者一次都没被查过：%v——候选池上限按分片位置饿死了它们，"+
			"而分片扇出存在的理由正是消灭位置饥饿", never)
	}
}

// TestStoreMemory_WriterFanoutIsNotAnAuthorizationPoint 钉住**逐写入者扇出不提供数据隔离**
// 这个事实，以及它唯一真正提供的东西。
//
// ⚠️ 这条替换了一个断言**不成立的不变量**的哨兵。原来那条叫 WriterGateNotBypassedAtScale（已删）、
// 注释写「写入者数量不该改变 gate 会不会跑」，而那句话是假的：`len(subs) < 2` 早退让
// **单写入者时 gate 根本不被调用**。它挑的写入者数 5/40/120 正好把唯一的反例排除在外
// ——用例恒绿，而它声称钉住的性质并不成立。
//
// 真相（下面逐条断言）：分片列表是 `[base, w1, w2, …]`，`Search(base)` 前缀递归 ⟹ 全部
// 后代数据在第一片就进了候选池。所以：
//   - 守卫返**普通 error** ⟹ 被拒写入者的内容照样进 prompt，`err=nil`（**这是现状，不是
//     bug 报告**——写下来是因为业务会照着「拒绝一个写入者」去实现，而那样是静默泄漏）
//   - 守卫抛**控制流信号** ⟹ 内容不进，但那是整轮被作废，不是隔离
func TestStoreMemory_WriterFanoutIsNotAnAuthorizationPoint(t *testing.T) {
	run := func(writers int, deny bool) (called, leaked, interrupted bool) {
		base := store.NS("mem")
		st := store.NewInMemoryStore()
		ctx := context.Background()
		for i := 0; i < writers; i++ {
			ns := append(append(store.Namespace(nil), base...), fmt.Sprintf("w%03d", i))
			if err := st.Put(ctx, ns, "k", fmt.Sprintf("我的猫的机密 SECRET%03d", i)); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		probe := &memGateStore{Store: st, gateNS: base.String() + ".w000", hit: &called, deny: deny}
		cap := &memCapturingStub{}
		ag := agent_v2.NewBasicAgent(
			agent_v2.WithName("m"), agent_v2.WithModel(cap),
			agent_v2.WithStoreMemory(base, &agent_v2.StoreMemoryConfig{}),
			agent_v2.WithMaxRounds(2))
		_, err := ag.Run(store.WithContext(ctx, probe), agent_v2.NewRunSession(),
			schema.UserMessage("我的猫"))
		_, interrupted = compose_v2.IsInterrupt(err)
		var all strings.Builder
		cap.mu.Lock()
		for _, round := range cap.seen {
			for _, m := range round {
				if m != nil {
					all.WriteString(m.Content)
				}
			}
		}
		cap.mu.Unlock()
		leaked = strings.Contains(all.String(), "SECRET000")
		return called, leaked, interrupted
	}

	// ① 单写入者：守卫根本不被调用（这是 `len(subs) < 2` 早退的直接后果）
	if called, _, _ := run(1, false); called {
		t.Errorf("单写入者时守卫被调用了——那说明 len(subs)<2 早退没了，" +
			"godoc 里「单写入者不扇出」那段要跟着改")
	}
	// ② 多写入者：守卫被调用，但**拒绝（普通 error）拦不住内容**
	for _, n := range []int{2, 5, 40} {
		called, leaked, _ := run(n, true)
		if !called {
			t.Errorf("写入者=%d：守卫没被调用", n)
		}
		if !leaked {
			t.Errorf("写入者=%d：被拒写入者的内容没进 prompt——**这不是好消息**，"+
				"说明 Search(base) 不再前缀递归、或分片顺序变了。扇出从来不提供隔离，"+
				"这条断言在保护那个事实不被误读成「有隔离」", n)
		}
	}
	// ③ 控制流信号确实拦得住——但那是整轮作废
	for _, n := range []int{2, 40} {
		_, leaked, interrupted := run(n, false)
		if !interrupted {
			t.Errorf("写入者=%d：守卫抛的中断没冒出来", n)
		}
		if leaked {
			t.Errorf("写入者=%d：中断了却仍有内容进 prompt——整轮该被作废", n)
		}
	}
}

// memGateStore 模拟业务在某个写入者 ns 上挂守卫。deny=true 返普通 error（拒绝），
// deny=false 抛控制流信号（HITL gate）——两者的效果**完全不同**，见上面的用例。
type memGateStore struct {
	store.Store
	gateNS string
	hit    *bool
	deny   bool
}

func (g *memGateStore) Search(ctx context.Context, ns store.Namespace, opts ...store.SearchOption) ([]*store.SearchItem, error) {
	if ns.String() == g.gateNS {
		*g.hit = true
		if g.deny {
			return nil, errors.New("permission denied")
		}
		return nil, &compose_v2.InterruptError{Reason: "gate", Query: &compose_v2.AskQuery{
			InterruptID: "biz-writer-gate", Question: "允许读这个写入者的记忆吗"}}
	}
	return g.Store.Search(ctx, ns, opts...)
}

// TestStoreMemory_RecallNotDilutedByUnrelatedWriters 钉住**旁边多了无关写入者，不该让我自己
// 的记忆召不回来**。
//
// ⚠️ 这条替换了一个选在不承重维度上的断言。`_LateWritersNotStarvedByCap` 只问「每个写入者
// 被查过没有」，而「按人数摊薄每片深度」这个退化下**它恒真**——每片都查了，只是每片只查
// 很浅一层，老记忆照样捞不回来。承重的量是**召回**，不是「有没有发起查询」。
//
// fixture：w0 自己的数据逐字不变（1 条相关的老记忆 + 40 条噪声压在它上面），只改「旁边有
// 几个跟本次 query 毫不相干的写入者」。召回与否只能由那个维度解释。
func TestStoreMemory_RecallNotDilutedByUnrelatedWriters(t *testing.T) {
	recalled := func(others int) bool {
		base := store.NS("mem")
		st := store.NewInMemoryStore()
		ctx := context.Background()
		mine := append(append(store.Namespace(nil), base...), "w0")
		// 先写老记忆，再压 40 条噪声（InMemoryStore 时新优先 ⟹ 老记忆在下面）
		if err := st.Put(ctx, mine, "old", "用户的猫叫咪咪"); err != nil {
			t.Fatalf("Put: %v", err)
		}
		for k := 0; k < 40; k++ {
			_ = st.Put(ctx, mine, fmt.Sprintf("n%02d", k), "今天天气不错")
		}
		for i := 0; i < others; i++ {
			ns := append(append(store.Namespace(nil), base...), fmt.Sprintf("x%03d", i))
			_ = st.Put(ctx, ns, "k", "无关内容：季度报表")
		}
		cap := &memCapturingStub{}
		ag := agent_v2.NewBasicAgent(
			agent_v2.WithName("m"), agent_v2.WithModel(cap),
			agent_v2.WithStoreMemory(base, &agent_v2.StoreMemoryConfig{}),
			agent_v2.WithMaxRounds(2))
		if _, err := ag.Run(store.WithContext(ctx, st), agent_v2.NewRunSession(),
			schema.UserMessage("我的猫叫什么")); err != nil {
			t.Fatalf("Run: %v", err)
		}
		cap.mu.Lock()
		defer cap.mu.Unlock()
		for _, round := range cap.seen {
			for _, m := range round {
				if m != nil && strings.Contains(m.Content, "咪咪") {
					return true
				}
			}
		}
		return false
	}

	if !recalled(1) {
		t.Fatalf("fixture 没搭对：只有 1 个无关写入者时就召不回，说明跟摊薄无关")
	}
	for _, n := range []int{11, 31, 99} {
		if !recalled(n) {
			t.Errorf("旁边有 %d 个**无关**写入者时，我自己那条相关的老记忆召不回来了——"+
				"每片深度被按人数摊薄，而我的数据一个字没变", n)
		}
	}
}

// TestStoreMemory_ListErrorSurfacesOnSingleWriterPath 钉住**列写入者失败在单写入者路径上
// 也要报出去**。
//
// 那段「降级也必须报上去」的代码曾经只挂在扇出路径上，而 `ListNamespaces` 按 Go 常规失败
// 时返 `nil, err` ⟹ subs 空 ⟹ 走的是 `len(subs) < 2` 早退 ⟹ **它在自己要救的那个形态上
// 是死的**。观测 event 报 Ok=true，而覆盖面已经塌了。
func TestStoreMemory_ListErrorSurfacesOnSingleWriterPath(t *testing.T) {
	base := store.NS("mem")
	st := store.NewInMemoryStore()
	if err := st.Put(context.Background(), base, "seed", "用户养了猫"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// 按 Go 常规：失败返 (nil, err)，于是 subs 为空、走单写入者早退
	probe := &memErrStore{Store: st, listErr: errors.New("列举超时")}

	rec := &storeEventRecorder{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("m"), agent_v2.WithModel(&memCapturingStub{}),
		agent_v2.WithStoreMemory(base, &agent_v2.StoreMemoryConfig{}),
		agent_v2.WithMaxRounds(2))
	ctx := compose_v2.WithOrchHooks(store.WithContext(context.Background(), probe), rec)
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("我的猫怎么样")); err != nil {
		t.Fatalf("普通后端错不该让 Run 失败：%v", err)
	}

	searched := rec.lastSearch()
	if searched == nil {
		t.Fatalf("fixture 没搭对：没抓到 store.search event")
	}
	if searched.Ok || searched.Err == "" {
		t.Errorf("列写入者失败、覆盖面已经塌成基准 ns 一个，观测却报 Ok=%v Err=%q——"+
			"这正是本函数 godoc 自己叫的「观测在撒谎」", searched.Ok, searched.Err)
	}
}

// storeEventRecorder 抓节点内发出的 store event（EmitStoreEvent 走 EventSink，由 graph_loop
// 从 OrchHooks 注入，所以这里实现 OnNodeEvent）。
type storeEventRecorder struct {
	compose_v2.NoOpOrchestrationHooks
	mu   sync.Mutex
	seen []*agent_v2.StoreSearchAction
}

func (r *storeEventRecorder) OnNodeEvent(ctx context.Context, nodeID compose_v2.NodeID, ev compose_v2.Event) context.Context {
	if a, ok := ev.Metadata["action"].(*agent_v2.StoreAction); ok && a != nil && a.Search != nil {
		r.mu.Lock()
		r.seen = append(r.seen, a.Search)
		r.mu.Unlock()
	}
	return ctx
}

func (r *storeEventRecorder) lastSearch() *agent_v2.StoreSearchAction {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.seen) == 0 {
		return nil
	}
	return r.seen[len(r.seen)-1]
}

// TestStoreMemory_OutOfScopeItemsNeverInjected 钉住**注入 prompt 的每一条记忆都在
// StoreMemoryNS 之下**。
//
// ⚠️ 这条替换了一个测错东西的用例。原来那条钉的是「ListNamespaces 返回的越界 ns 不该被
// Search」——**那建立不了任何不变量**：注入有四条路，其中三条（后端自报按相关性排 /
// 单写入者早退 / limit 非法兜底）根本不经过 ListNamespaces。实测一个经 `Search` 返越界项
// 的后端，在单写入者和多写入者两条路上**都能把内容送进 prompt**，而那条用例全程绿。
//
// ⚠️ fixture 陷阱：越界项的内容**必须跟 query 相关**。第一次探针我给的是无关内容，
// KeywordScore==0 被打分丢掉 ⟹ 测出「没泄漏」⟹ 差点据此判定「缺陷不存在」。
func TestStoreMemory_OutOfScopeItemsNeverInjected(t *testing.T) {
	for _, writers := range []int{1, 4} {
		for _, pos := range []string{"first", "mid", "last"} {
			base := store.NS("mem")
			st := store.NewInMemoryStore()
			ctx := context.Background()
			for i := 0; i < 3; i++ {
				ns := append(append(store.Namespace(nil), base...), fmt.Sprintf("w%d", i))
				if err := st.Put(ctx, ns, "k", fmt.Sprintf("我的猫叫小白 MARK%d", i)); err != nil {
					t.Fatalf("Put: %v", err)
				}
			}
			_ = writers
			rogue := &store.SearchItem{Item: store.Item{
				Namespace: store.NS("other_tenant"), Key: "s", Value: "我的猫的机密 LEAKMARK"}}
			probe := &memRogueStore{Store: st, rogue: rogue, pos: pos}

			cap := &memCapturingStub{}
			ag := agent_v2.NewBasicAgent(
				agent_v2.WithName("m"), agent_v2.WithModel(cap),
				agent_v2.WithStoreMemory(base, &agent_v2.StoreMemoryConfig{}),
				agent_v2.WithMaxRounds(2))
			if _, err := ag.Run(store.WithContext(ctx, probe), agent_v2.NewRunSession(),
				schema.UserMessage("我的猫")); err != nil {
				t.Fatalf("Run: %v", err)
			}
			var all strings.Builder
			cap.mu.Lock()
			for _, round := range cap.seen {
				for _, m := range round {
					if m != nil {
						all.WriteString(m.Content)
					}
				}
			}
			cap.mu.Unlock()
			seen := all.String()
			if strings.Contains(seen, "LEAKMARK") {
				t.Errorf("越界项在 %s：它进了 prompt——归属过滤是后端的义务，"+
					"但框架这层没有校 SearchItem.Namespace", pos)
			}
			// **同时断言合法记忆没被原地过滤弄坏**（覆盖 / 重复 / 丢失）
			for i := 0; i < 3; i++ {
				if n := strings.Count(seen, fmt.Sprintf("MARK%d", i)); n != 1 {
					t.Errorf("越界项在 %s：合法记忆 MARK%d 出现 %d 次（应为 1）——"+
						"原地过滤把留下的元素弄坏了", pos, i, n)
				}
			}
		}
	}
}

// memRogueStore 模拟一个有 bug 的后端：Search 返回一条不在请求前缀之下的记录。
//
// pos 控制越界项插在哪儿——**这个维度是承重的**：框架侧的过滤是原地的（`got[:0]`），
// 而本仓 fixture 陷阱第 1 条说的就是「通过过滤的元素恰好都在原位 ⟹ 原地过滤写错也不显现」。
// 越界项只放最后时，留下的元素一个都不用挪；放开头/中间才逼它们全部前移。
type memRogueStore struct {
	store.Store
	rogue *store.SearchItem
	pos   string // "first" / "mid" / 其余=last
}

func (r *memRogueStore) Search(ctx context.Context, ns store.Namespace, opts ...store.SearchOption) ([]*store.SearchItem, error) {
	got, err := r.Store.Search(ctx, ns, opts...)
	switch r.pos {
	case "first":
		return append([]*store.SearchItem{r.rogue}, got...), err
	case "mid":
		if len(got) > 0 {
			out := append([]*store.SearchItem{got[0]}, r.rogue)
			return append(out, got[1:]...), err
		}
	}
	return append(got, r.rogue), err
}
