package agent_v2_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
)

// TestStoreMemory_MemberWritesToo：**业务给成员配的 WithStoreMemory 必须真的生效。**
//
// 曾经不生效：commitFinal 有一道 `!IsSubAgentStoreScope(ctx)` 开关，让所有非顶层的
// agent 一条都不写。实测同一个 agent 顶层跑写 1 条、当成员跑写 **0** 条，err 全程 nil
// ——业务配了个静默失效的配置。
func TestStoreMemory_MemberWritesToo(t *testing.T) {
	ns := store.NS("mem")
	mk := func(name, say string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName(name),
			agent_v2.WithModel(newScopeStub(schema.AssistantMessage(say))),
			agent_v2.WithStoreMemory(ns, &agent_v2.StoreMemoryConfig{}),
			agent_v2.WithMaxRounds(2))
	}
	count := func(st store.Store) int {
		items, err := st.Search(context.Background(), ns)
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		return len(items)
	}

	stTop := store.NewInMemoryStore()
	if _, err := mk("solo", "顶层答复").Run(
		store.WithContext(context.Background(), stTop), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("顶层: %v", err)
	}
	top := count(stTop)

	stMember := store.NewInMemoryStore()
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("chain")).
		AddStep("only", mk("member", "成员答复"))
	if _, err := seq.Run(
		store.WithContext(context.Background(), stMember), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("成员: %v", err)
	}
	member := count(stMember)

	if top != 1 {
		t.Fatalf("fixture 没搭对：顶层应写 1 条，实际 %d 条", top)
	}
	if member != 1 {
		t.Fatalf("成员的 WithStoreMemory 没生效：顶层写 %d 条、当成员写 %d 条"+
			"（业务配了却静默不写，err 全程 nil）", top, member)
	}
}

// TestStoreMemory_ParallelBranchesDoNotCollide：**并行分支的记忆不能互相覆盖。**
//
// 键原先是 `<RunID>_assistant_<序号>`——RunID 同一次任务里所有成员共享，序号数各自
// session 的事件条数，于是并行两分支的键**逐字相同**（实测：各写一条，只存下 1 条）。
// 这正是当年那道「不让成员写」的开关存在的理由：它在拿「不让第二个人写」模拟
// 「两个人写不同的地方」。
func TestStoreMemory_ParallelBranchesDoNotCollide(t *testing.T) {
	ns := store.NS("mem")
	st := store.NewInMemoryStore()
	ctx := store.WithContext(context.Background(), st)
	mk := func(name, say string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName(name),
			agent_v2.WithModel(newScopeStub(schema.AssistantMessage(say))),
			agent_v2.WithStoreMemory(ns, &agent_v2.StoreMemoryConfig{}),
			agent_v2.WithMaxRounds(2))
	}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("fan")).
		AddBranch("br_a", mk("a", "A 的答复")).
		AddBranch("br_b", mk("b", "B 的答复"))
	if _, err := par.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	items, err := st.Search(context.Background(), ns)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(items) != 2 {
		keys := make([]string, 0, len(items))
		for _, it := range items {
			keys = append(keys, it.Key)
		}
		t.Fatalf("两个分支各写一条应存下 2 条，实际 %d 条 %v（键撞了，后写静默覆盖）",
			len(items), keys)
	}
	// ns 里要真的带上写入者位置——不是碰巧靠别的维度分开的。
	for _, it := range items {
		last := it.Namespace[len(it.Namespace)-1]
		if last != "br_a" && last != "br_b" {
			t.Errorf("ns 末段不是写入者位置：%v", it.Namespace)
		}
	}
}

// TestStoreMemory_CanNarrowToOneWriter 钉住把写入者放进 **ns** 而不是 key 的**全部理由**：
// 事后能只读一个写入者。
//
// Search 只按 namespace 检索、不看 key——写入者若只在 key 里，`Search(ns)` 永远收窄不到
// 单个写入者，业务想做「只回忆 researcher 说过的话」得重新设计存储布局。放进 ns 之后
// 这是白拿的：搜父 ns 仍是全部（默认共享不变），搜 `ns/写入者` 就只有那一个。
func TestStoreMemory_CanNarrowToOneWriter(t *testing.T) {
	ns := store.NS("mem")
	st := store.NewInMemoryStore()
	ctx := store.WithContext(context.Background(), st)
	mk := func(name, say string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName(name),
			agent_v2.WithModel(newScopeStub(schema.AssistantMessage(say))),
			agent_v2.WithStoreMemory(ns, &agent_v2.StoreMemoryConfig{}),
			agent_v2.WithMaxRounds(2))
	}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("fan")).
		AddBranch("br_a", mk("a", "A 的答复")).
		AddBranch("br_b", mk("b", "B 的答复"))
	if _, err := par.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	all, _ := st.Search(context.Background(), ns)
	if len(all) != 2 {
		t.Fatalf("搜父 ns 应拿到两个写入者的全部 2 条（默认共享），实际 %d 条", len(all))
	}
	only, err := st.Search(context.Background(), store.NS("mem", "br_a"))
	if err != nil {
		t.Fatalf("Search 子 ns: %v", err)
	}
	if len(only) != 1 {
		t.Fatalf("收窄到 br_a 应只剩 1 条，实际 %d 条——写入者维不在 ns 上，收窄不了", len(only))
	}
	// 框架存的是答复文本（2026-09-07 起），不是整条 *schema.Message。
	if txt, ok := only[0].Value.(string); !ok || txt != "A 的答复" {
		t.Errorf("收窄到 br_a 却拿到别人的记忆：%#v", only[0].Value)
	}
	// ⚠️ **必须也验字典序靠后的那个**。只验 br_a 的话，把「收窄」实现成「搜父 ns 取
	// 第一条」（彻底错）也照样通过——br_a 恰好是全局字典序第一条。
	otherOnly, err := st.Search(context.Background(), store.NS("mem", "br_b"))
	if err != nil {
		t.Fatalf("Search br_b: %v", err)
	}
	if len(otherOnly) != 1 {
		t.Fatalf("收窄到 br_b 应只剩 1 条，实际 %d 条", len(otherOnly))
	}
	if txt, ok := otherOnly[0].Value.(string); !ok || txt != "B 的答复" {
		t.Errorf("收窄到 br_b 却拿到 %#v——收窄没按 ns 走，只是返回了字典序头部", otherOnly[0].Value)
	}
}

// TestStoreMemory_RelevanceBeatsLexicalOrder 钉住注入按**相关性**挑，不按 key 的字典序。
//
// `store.Search` 的跨 backend 契约是按 `(ns, key)` 字典序返回。把 SearchLimit 直接
// 下推给它，截出来的是「key 首字母最小的 N 条」——跟本轮问题的相关性毫无关系。
//
// ⚠️ 这**不是**「多写入者公平」问题，虽然它长得像。曾经按写入者分过配额，那治的是
// 症状：多写入者那半看着好了，**单写入者那半原样坏着**（一个 agent 写 20 条、limit=5，
// 照样是字典序前 5 条），而且从此没人会再去查。所以这条用例**两种都测**。
//
// 修法是框架侧按 [skill.KeywordScore] 打分重排（后端自报会排序时不插手，见
// RelevanceRanker）。
func TestStoreMemory_RelevanceBeatsLexicalOrder(t *testing.T) {
	mkAgent := func(st store.Store, ns store.Namespace, cap *memInjectStub) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("reader"), agent_v2.WithModel(cap),
			agent_v2.WithStoreMemory(ns, &agent_v2.StoreMemoryConfig{SearchLimit: 5}),
			agent_v2.WithMaxRounds(2))
	}
	injected := func(cap *memInjectStub, want string) bool {
		for _, m := range cap.first {
			if strings.Contains(m.Content, want) {
				return true
			}
		}
		return false
	}

	t.Run("单写入者", func(t *testing.T) {
		ns := store.NS("mem")
		st := store.NewInMemoryStore()
		ctx := store.WithContext(context.Background(), st)
		// 19 条无关的排在字典序前面，唯一相关的那条排最后。
		for i := 0; i < 19; i++ {
			_ = st.Put(context.Background(), ns, fmt.Sprintf("a_%02d", i),
				schema.AssistantMessage("跟提问无关的闲聊内容"))
		}
		_ = st.Put(context.Background(), ns, "z_last", schema.AssistantMessage("预算上限是三万"))

		cap := &memInjectStub{reply: schema.AssistantMessage("好")}
		if _, err := mkAgent(st, ns, cap).Run(ctx, agent_v2.NewRunSession(),
			schema.UserMessage("预算上限多少")); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if !injected(cap, "预算上限是三万") {
			t.Error("相关的那条排在字典序末尾就注入不进去——limit 被下推给了字典序")
		}
	})

	t.Run("多写入者", func(t *testing.T) {
		ns := store.NS("mem")
		st := store.NewInMemoryStore()
		ctx := store.WithContext(context.Background(), st)
		// 字典序靠前的两个写入者各写 3 条无关内容；相关的那条在字典序最后那个写入者手里。
		for _, w := range []string{"aaa", "bbb"} {
			for i := 1; i <= 3; i++ {
				_ = st.Put(context.Background(), store.NS("mem", w),
					fmt.Sprintf("k%d", i), schema.AssistantMessage("跟提问无关的闲聊内容"))
			}
		}
		_ = st.Put(context.Background(), store.NS("mem", "zzz"), "k1",
			schema.AssistantMessage("预算上限是三万"))

		cap := &memInjectStub{reply: schema.AssistantMessage("好")}
		if _, err := mkAgent(st, ns, cap).Run(ctx, agent_v2.NewRunSession(),
			schema.UserMessage("预算上限多少")); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if !injected(cap, "预算上限是三万") {
			t.Error("相关的那条在字典序末位的写入者手里就注入不进去")
		}
	})

	t.Run("无关的不注入", func(t *testing.T) {
		// 宁可少注也不注不相关的——分数为 0 的丢掉。
		ns := store.NS("mem")
		st := store.NewInMemoryStore()
		ctx := store.WithContext(context.Background(), st)
		_ = st.Put(context.Background(), ns, "k1", schema.AssistantMessage("完全无关的旧闲聊"))

		cap := &memInjectStub{reply: schema.AssistantMessage("好")}
		if _, err := mkAgent(st, ns, cap).Run(ctx, agent_v2.NewRunSession(),
			schema.UserMessage("预算上限多少")); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if injected(cap, "完全无关的旧闲聊") {
			t.Error("跟本轮问题零共同词的记忆被注入了——分数 0 的该丢掉")
		}
	})
}

// TestStoreMemory_IllegalWriterNameFailsLoud 钉住：写入者位置不合 namespace 字符约束时
// **报错**，不静默丢记忆。
//
// `v1.0` / `gpt-4.1` / `finance.reporter` 这类注册名，编排装配那侧是**放行**的
// （`ValidateNodeIDForNS` 只拦 `""` / `/` / `#`），而 namespace 不接受 `.`。位移之前
// 写入者在 key 里、key 没有字符约束，所以这些名字一直能用；位移之后 Put 会拒，而那个
// 拒绝本来会被 commitFinal 的 slog.Warn 吞掉 ⟹ **Run.err=nil、落盘 0 条**。
func TestStoreMemory_IllegalWriterNameFailsLoud(t *testing.T) {
	ns := store.NS("mem")
	st := store.NewInMemoryStore()
	ctx := store.WithContext(context.Background(), st)
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("worker"),
		agent_v2.WithModel(newScopeStub(schema.AssistantMessage("答"))),
		agent_v2.WithStoreMemory(ns, &agent_v2.StoreMemoryConfig{}),
		agent_v2.WithMaxRounds(2))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("chain")).
		AddStep("v1.0", ag) // 合法的编排注册名，但不是合法的 ns 段
	_, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err == nil {
		items, _ := st.Search(context.Background(), ns)
		t.Fatalf("注册名含 '.' 时记忆存不下，但 Run 返 err=nil（落盘 %d 条）——静默丢失", len(items))
	}
	// 错误正文要给得出出路，不能只说「invalid namespace」。
	for _, want := range []string{"v1.0", "WriterID"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("错误正文缺 %q，定位/出路给不全：%v", want, err)
		}
	}
}

// TestStoreMemory_IllegalWriterIDPanicsAtAssembly 钉住：业务显式给的 WriterID 不合
// namespace 字符约束时，**装配期就 panic**。
//
// 这是调用方显式给的值自身非法，它手上的信息完全够自查 ⟹ 报错合法。运行时才发现的话
// 选择只剩「静默丢记忆」或「整个 Run 失败」，两个都更差。
func TestStoreMemory_IllegalWriterIDPanicsAtAssembly(t *testing.T) {
	for _, bad := range []string{"a.b", "v1.0", "a//b", "/x", "x/", "a\x00b"} {
		t.Run(bad, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("WriterID %q 不是合法 ns 段，装配期应 panic", bad)
				}
			}()
			agent_v2.WithStoreMemory(store.NS("mem"), &agent_v2.StoreMemoryConfig{WriterID: bad})(
				&agent_v2.Config{})
		})
	}
	// 合法的不该 panic——包括带 `/` 的嵌套形式。
	for _, ok := range []string{"plain", "outer/inner", "svc-prod_1"} {
		agent_v2.WithStoreMemory(store.NS("mem"), &agent_v2.StoreMemoryConfig{WriterID: ok})(
			&agent_v2.Config{})
	}
}

// TestStoreMemory_RelevanceBackendNotSecondGuessed 钉住：后端**按相关性排序**时，
// 注入侧不再按写入者拆配额。
//
// 拆配额是给「没有相关性信号」的后端兜底的——那种后端按 (ns,key) 字典序返回，截断
// 就成了按写入者名字排序的偏袒。而向量后端已经挑好了最相关的 N 条，再拆只会把它们
// 换成「每个写入者各自的次相关」。
func TestStoreMemory_RelevanceBackendNotSecondGuessed(t *testing.T) {
	emb := store.EmbedderFunc(func(_ context.Context, texts []string) ([][]float32, error) {
		out := make([][]float32, len(texts))
		for i, tx := range texts {
			v := make([]float32, 2)
			if strings.Contains(tx, "预算") {
				v[0] = 1
			} else {
				v[1] = 1
			}
			out[i] = v
		}
		return out, nil
	})
	ns := store.NS("mem")
	st := store.NewInMemoryStore(store.WithIndex(&store.IndexConfig{Embedder: emb}))
	ctx := store.WithContext(context.Background(), st)

	// 写入者 zzz 手里握着**全部**跟"预算"相关的记忆；aaa/bbb 全是无关内容。
	// 拆配额的话 zzz 只能分到 1/3 配额，最相关的几条被无关内容挤掉。
	for i := 1; i <= 3; i++ {
		_ = st.Put(context.Background(), store.NS("mem", "zzz"),
			fmt.Sprintf("k%d", i), schema.AssistantMessage("预算相关记忆"))
		_ = st.Put(context.Background(), store.NS("mem", "aaa"),
			fmt.Sprintf("k%d", i), schema.AssistantMessage("无关内容"))
		_ = st.Put(context.Background(), store.NS("mem", "bbb"),
			fmt.Sprintf("k%d", i), schema.AssistantMessage("无关内容"))
	}

	cap := &memInjectStub{reply: schema.AssistantMessage("好")}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("r"), agent_v2.WithModel(cap),
		agent_v2.WithStoreMemory(ns, &agent_v2.StoreMemoryConfig{SearchLimit: 3}),
		agent_v2.WithMaxRounds(2))
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("预算怎么定")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	hit := 0
	for _, m := range cap.first {
		if strings.Contains(m.Content, "预算相关记忆") {
			hit++
		}
	}
	if hit != 3 {
		t.Errorf("相关性后端挑的 top-3 应原样注入，实际只有 %d 条相关的——配额被拆了", hit)
	}
}

// TestStoreMemory_WrittenValueIsEmbeddable 跨包钉住：框架写进 store 的**真实类型**
// 满足 store 语义检索的抽取契约。
//
// store 侧那条同族哨兵用的是自造的 fixture 类型——它证明「实现了那个方法就能被认出」，
// 但证明不了「我们真正写进去的东西能被认出」。这一条补的正是那半。
//
// ⚠️ **那个「真实类型」2026-09-07 起是 `string` 不是 `*schema.Message`**（`commitFinal`
// 写的是 `last.Content`）。这条用例此前断言的是 `*schema.Message` 满足
// `GetContent() string`——那句话本身没错，但它钉的**已经不是我们写进去的东西**了，
// 是一条典型的假绿哨兵：框架换了写入类型，它照样绿。
func TestStoreMemory_WrittenValueIsEmbeddable(t *testing.T) {
	// 跟 commitFinal 的 `st.Put(ctx, memNS, key, last.Content)` 同型
	var written any = schema.AssistantMessage("内容").Content

	got := store.DefaultTextExtractor(written, nil)
	if len(got) != 1 || got[0] != "内容" {
		t.Fatalf("框架写进去的值抽不出文本（got %v）——语义检索对框架自己的记忆会静默失效："+
			"抽不出文本 ⟹ 无向量 ⟹ 永远排在打了分的后面，而且不报错", got)
	}

	// 反向钉住「类型换回消息对象也别指望它还走同一条分支」：两条分支都在，但走哪条由
	// 写入侧决定，改写入侧就要回来改这条用例。
	if _, isStr := written.(string); !isStr {
		t.Errorf("写入类型变了（%T）——若是有意改的，请连同 memoryItemText 的分支归属一起更新", written)
	}
}

// TestStoreMemory_NoFilterFieldRegression 钉住 `StoreMemoryConfig` 上**没有** Filter 字段。
//
// 它曾经在，把一个 map 转发给 `store.Search(WithFilter(...))`——而那个 filter 按
// 「value 是 map」比较字段，框架写进去的恒是一条消息 ⟹ 框架自己的记忆 100% 被剔除，
// 业务自己 Put 的 map 照常命中 ⟹ 混合 ns 上看着「有结果」，实际 agent 的记忆一条不剩。
//
// ⚠️ **这条一开始写成了假绿**，值得记：原版是 `cfg := StoreMemoryConfig{}` 加一句
// 注释「字段被加回来时这个文件直接编译失败」——而**空复合字面量加字段永远不会编译
// 失败**。实测：把 Filter 加回去，这条用例绿、`go build` 绿、全仓测试全绿。
//
// 「编译期检查」这个念头本身没错，错在选的形式不承重。改成**无键字面量**：字段数
// 一变就编译失败，那才是真的编译期约束。
func TestStoreMemory_NoFilterFieldRegression(t *testing.T) {
	// 无键字面量——**加字段 / 删字段都会编译失败**，这才是真的编译期检查。
	// 加回 Filter 时这里会报 "too few values in struct literal"。
	cfg := agent_v2.StoreMemoryConfig{
		5,   // SearchLimit
		"x", // WriterID
	}
	if cfg.SearchLimit != 5 || cfg.WriterID != "x" {
		t.Fatal("字段顺序变了——上面的无键字面量要跟着调整")
	}
	// 双保险：运行时也数一遍字段名，编译期那道被谁改成有键字面量时还剩这道。
	typ := reflect.TypeOf(agent_v2.StoreMemoryConfig{})
	got := make([]string, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		got = append(got, typ.Field(i).Name)
	}
	want := []string{"SearchLimit", "WriterID"}
	if len(got) != len(want) {
		t.Fatalf("StoreMemoryConfig 字段变了：%v（期望 %v）——"+
			"若是加回 Filter，读一遍它的 godoc：那个字段会让框架自己写的记忆 100%% 被剔除", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("字段 %d = %q，期望 %q", i, got[i], want[i])
		}
	}
}

// memInjectStub 记下第一次 Generate 收到的完整 messages——注入的记忆在里面。
type memInjectStub struct {
	mu    sync.Mutex
	reply *schema.Message
	first []*schema.Message
}

func (s *memInjectStub) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	s.mu.Lock()
	if s.first == nil {
		s.first = append([]*schema.Message{}, msgs...)
	}
	s.mu.Unlock()
	m := *s.reply
	return &m, nil
}

func (s *memInjectStub) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	m, err := s.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(m)
	schema.CloseSend(sr)
	return sr, nil
}

func (s *memInjectStub) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return s, nil }

// TestStoreMemory_CandidatePoolBoundary 钉住**降级方案的已知局限**，并防它悄悄变差。
//
// 后端不支持语义检索时，框架侧打分只能在候选池里挑，而候选池按时新序取。所以一条
// **又老又相关**的记忆在噪声足够多时会被挤出去——超采只能把边界往后推，消不掉它。
//
// 这条用例二分出那个边界。它不是「必须达到某个数」的性能要求，是**让这个数字有人看着**：
// 调小超采倍数、或给候选池加了别的限制时，这里会红，而不是等业务报「记忆时灵时不灵」。
//
// 真正的解法是接支持语义检索的后端（自报 RelevanceRanker，框架完全不插手）。
func TestStoreMemory_CandidatePoolBoundary(t *testing.T) {
	probe := func(noise int) bool {
		st := store.NewInMemoryStore()
		ns := store.NS("mem")
		ctx := store.WithContext(context.Background(), st)
		// 相关的那条**最先写**（时新序里排最后，最容易被挤出候选池）
		_ = st.Put(context.Background(), ns, "relevant", schema.AssistantMessage("预算上限三万"))
		for i := 0; i < noise; i++ {
			_ = st.Put(context.Background(), ns, fmt.Sprintf("n_%03d", i),
				schema.AssistantMessage("无关闲聊"))
		}
		cap := &memInjectStub{reply: schema.AssistantMessage("好")}
		ag := agent_v2.NewBasicAgent(agent_v2.WithName("r"), agent_v2.WithModel(cap),
			agent_v2.WithStoreMemory(ns, &agent_v2.StoreMemoryConfig{SearchLimit: 5}),
			agent_v2.WithMaxRounds(2))
		if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("预算多少")); err != nil {
			t.Fatalf("Run: %v", err)
		}
		for _, m := range cap.first {
			if strings.Contains(m.Content, "预算上限三万") {
				return true
			}
		}
		return false
	}
	// 二分找边界
	lo, hi := 0, 300
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if probe(mid) {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	t.Logf("SearchLimit=5 时，最旧的相关记忆在 %d 条噪声之内捞得到", lo)
	// 超采倍数 20 ⟹ 候选池 100 ⟹ 边界应在 99 附近。给一点余量防实现细节微调时误报。
	if lo < 90 {
		t.Errorf("候选池边界退到 %d 条——超采倍数被调小了？这个数字低到业务会以为记忆功能坏了"+
			"（曾经是 14，实测业务会报「记忆时灵时不灵」）", lo)
	}
}
