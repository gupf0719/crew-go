// store_custom_backend 演示业务自定义 store.Store backend 的两件事:
//
//  1. **实现 store.Store 6 method 接口**——用内存 map 模拟一个 mem0/PG 风格后端
//     (mapStore)，并跑 store/conformance.RunStoreTests 验证语义合规 (见同目录
//     main_test.go)。业务接真实 PG/Redis/mem0 时同款先过 conformance 套。
//
//  2. **sub-agent 嵌套防双重持久化**——业务自定义 backend 在 Put 里查
//     （历史注释：曾演示 agent_v2.IsSubAgentStoreScope(ctx) 跳过 sub-agent 写入
//     唯一写入，避免父子把同一条 message 重复 Put 到父 ns (doc/v2/store.md §10.5 #4)。
//
// **为什么要自定义 backend**: 框架自带 InMemoryStore 只够 PoC；生产要把长期记忆
// 落 Redis/PG/mem0。业务实现 store.Store 接口后，agent_v2.WithStore(st) 即可无缝
// 替换——下游 hook/tool/preamble/postamble 通过 store.FromContext 拿到的都是业务
// backend。
//
// **关键 API**:
//   - 业务自实现 store.Store 接口 (Get/Put/Delete/Search/ListNamespaces/Batch)
//   - store.ParseSearchOptions(opts...) 跨包解析 SearchOption (业务 backend 读 Filter/Limit)
//   - store.ValidateFilter / store.MatchFilter 复用框架的 filter 语义 (客户端过滤后端别自己抄一份——
//     type bracketing / JSON 语义相等 / ErrFilterTypeMismatch 只报 filter 值侧, 都在那一份里)
//   - （已移除：曾在 Put 内查 IsSubAgentStoreScope 丢弃 sub-agent 写入——键加上写入者维后无必要）
//   - store/conformance.RunStoreTests(t, factory) 验证 backend 语义合规
//
// **不依赖真 LLM**——stubModel 按入参里是否含某关键词路由。
//
// 跑法:
//
//	go run ./example/store/store_custom_backend        # 跑业务闭环 demo
//	go test ./example/store/store_custom_backend        # 跑 conformance 套验证 backend 合规
//
// 全场景 PASS + 退出码 0 表示通过。
package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
)

// ── 业务自定义 backend: mapStore ──────────────────────────────────────────
//
// 用内存 map 模拟 mem0/PG 风格后端。生产实现把这层换成 Redis SET / PG INSERT 即可，
// 接口契约不变。**所有 method 必须实现完整**——conformance 套逐项验证。

// mapStore 是演示用的业务自定义 store backend。
//
// 内部用 joinedNS string 作 outer map key (Go map key 不能用 slice)，NUL 字节分隔
// namespace 段——跟 InMemoryStore 同款编码，避免 sibling namespace prefix 误命中
// (业务 PG `LIKE 'users.alice%'` 会错误匹配 `users.alice2`，conformance 专测此坑)。
type mapStore struct {
	mu   sync.RWMutex
	data map[string]map[string]*store.Item // data[joinedNS][key] = *Item
}

func newMapStore() *mapStore {
	return &mapStore{data: make(map[string]map[string]*store.Item)}
}

// joinNS 用 NUL 字节分隔 namespace 段——业务标识符不会含 NUL，避免跟段内容碰撞。
func joinNS(ns store.Namespace) string { return strings.Join(ns, "\x00") }

func splitNS(joined string) store.Namespace {
	if joined == "" {
		return store.Namespace{}
	}
	return store.Namespace(strings.Split(joined, "\x00"))
}

func (s *mapStore) Get(_ context.Context, ns store.Namespace, key string) (*store.Item, error) {
	if err := ns.Validate(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	bucket, ok := s.data[joinNS(ns)]
	if !ok {
		return nil, nil // 不存在返 (nil, nil) 非 error
	}
	it, ok := bucket[key]
	if !ok {
		return nil, nil
	}
	itCopy := *it // 浅拷贝防业务篡改 outer 字段影响内部
	return &itCopy, nil
}

func (s *mapStore) Put(ctx context.Context, ns store.Namespace, key string, value any, opts ...store.PutOption) error {
	if err := ns.Validate(); err != nil {
		return err
	}

	// ⚠️ 这里**曾经**演示「查 IsSubAgentStoreScope，是 sub-agent 就丢弃这次写入」，
	// 理由是「不丢的话父子会把同一条内容重复 Put 到同一个 ns」。
	//
	// **那个理由已经不成立**：框架给记忆键加上了写入者这一维（编排注册位置，或业务在
	// StoreMemoryConfig.WriterID 里显式给），父子各写各的键、互不覆盖。丢弃 sub-agent
	// 的写入现在只会让业务给成员配的 WithStoreMemory 静默失效。
	//
	// 业务 backend **不该**替框架做这个判断——它看不见框架的键策略，只看得见一个
	// 已经拼好的 key。要控制哪些 agent 写记忆，用它们各自的 WithStoreMemory 配置。

	if po := store.ParsePutOptions(opts...); po.TTL != 0 {
		// 本 backend 不实现 TTLAware (不提供 SupportsTTL)——业务调 WithTTL 返哨兵
		return store.ErrTTLNotSupported
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if value == nil { // value=nil 等价 Delete
		s.deleteLocked(ns, key)
		return nil
	}
	bk := joinNS(ns)
	bucket, ok := s.data[bk]
	if !ok {
		bucket = make(map[string]*store.Item)
		s.data[bk] = bucket
	}
	now := time.Now().UTC()
	if existing, ok := bucket[key]; ok {
		existing.Value = value
		existing.UpdatedAt = now
		return nil
	}
	nsCopy := make(store.Namespace, len(ns))
	copy(nsCopy, ns)
	bucket[key] = &store.Item{Namespace: nsCopy, Key: key, Value: value, CreatedAt: now, UpdatedAt: now}
	return nil
}

func (s *mapStore) Delete(_ context.Context, ns store.Namespace, key string) error {
	if err := ns.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteLocked(ns, key)
	return nil
}

// deleteLocked 内部删除——调用方必须已持 s.mu.Lock()。
func (s *mapStore) deleteLocked(ns store.Namespace, key string) {
	bk := joinNS(ns)
	bucket, ok := s.data[bk]
	if !ok {
		return // 幂等：不存在不报错
	}
	delete(bucket, key)
	if len(bucket) == 0 {
		delete(s.data, bk)
	}
}

func (s *mapStore) Search(_ context.Context, prefix store.Namespace, opts ...store.SearchOption) ([]*store.SearchItem, error) {
	if len(prefix) == 0 {
		return nil, store.ErrEmptyNamespace // 禁止空 prefix 全店扫
	}
	if err := prefix.Validate(); err != nil {
		return nil, err
	}
	cfg := store.ParseSearchOptions(opts...) // 业务 backend 用 Parse helper 读 Filter/Limit/Offset
	limit := cfg.Limit
	if limit <= 0 {
		limit = 10
	}
	if err := store.ValidateFilter(cfg.Filter); err != nil { // filter 语义与校验直接复用框架的唯一实现
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	matched := make([]*store.SearchItem, 0)
	for bk, bucket := range s.data {
		ns := splitNS(bk)
		// **sibling prefix 防误命中**: 用结构化 IsPrefixOf 而非 string LIKE——
		// prefix=[users,alice] 不会错误匹配 ns=[users,alice2]。
		if !prefix.IsPrefixOf(ns) {
			continue
		}
		for _, it := range bucket {
			ok, err := store.MatchFilter(it.Value, cfg.Filter)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			itCopy := *it
			matched = append(matched, &store.SearchItem{Item: itCopy})
		}
	}
	// **时新优先**（UpdatedAt 降序）——这是 Search 的顺序契约，见
	// `store/conformance` 的 Search_StableOrder_RecentFirst。
	//
	// 为什么不是 key 字典序：Search 恒带 limit（后端默认或调用方给），所以顺序**就是**
	// 「截断时留下谁」。按 key 排等于按首字母挑，跟这条数据对调用方有没有用零相关；
	// 一条 key 排在末尾的记录无论多相关都进不了结果集，而调用方看到的是一个正常返回
	// 的列表、没有任何信号说「还有更该给你的没返」。
	sort.Slice(matched, func(i, j int) bool {
		ti, tj := matched[i].UpdatedAt, matched[j].UpdatedAt
		if !ti.Equal(tj) {
			return ti.After(tj)
		}
		// 同一时刻写入才回落 (ns, key)——只为结果可预测，不是主序。
		if matched[i].Namespace.String() != matched[j].Namespace.String() {
			return matched[i].Namespace.String() < matched[j].Namespace.String()
		}
		return matched[i].Key < matched[j].Key
	})
	if cfg.Offset >= len(matched) {
		return []*store.SearchItem{}, nil
	}
	matched = matched[cfg.Offset:]
	if len(matched) > limit {
		matched = matched[:limit]
	}
	return matched, nil
}

func (s *mapStore) ListNamespaces(_ context.Context, opts ...store.ListOption) ([]store.Namespace, error) {
	cfg := store.ParseListOptions(opts...)
	limit := cfg.Limit
	if limit <= 0 {
		limit = 100
	}
	if len(cfg.Prefix) > 0 {
		if err := cfg.Prefix.Validate(); err != nil {
			return nil, err
		}
	}
	if len(cfg.Suffix) > 0 {
		if err := cfg.Suffix.Validate(); err != nil {
			return nil, err
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	all := make([]store.Namespace, 0, len(s.data))
	seen := make(map[string]struct{}, len(s.data))
	for bk := range s.data {
		ns := splitNS(bk)
		if len(cfg.Prefix) > 0 && !cfg.Prefix.IsPrefixOf(ns) {
			continue
		}
		if !hasSuffix(ns, cfg.Suffix) {
			continue
		}
		out := ns
		if cfg.MaxDepth > 0 && len(ns) > cfg.MaxDepth {
			out = make(store.Namespace, cfg.MaxDepth)
			copy(out, ns[:cfg.MaxDepth])
		}
		k := out.String()
		if _, dup := seen[k]; dup {
			continue // maxDepth 截断后去重
		}
		seen[k] = struct{}{}
		all = append(all, out)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].String() < all[j].String() })
	if cfg.Offset >= len(all) {
		return []store.Namespace{}, nil
	}
	all = all[cfg.Offset:]
	if len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

func (s *mapStore) Batch(ctx context.Context, ops []store.Op) ([]store.Result, error) {
	results := make([]store.Result, len(ops))
	for i, op := range ops {
		switch o := op.(type) {
		case store.GetOp:
			item, err := s.Get(ctx, o.Namespace, o.Key)
			results[i] = store.Result{Item: item, Err: err}
		case store.PutOp:
			var opts []store.PutOption
			if o.TTL != 0 {
				opts = append(opts, store.WithTTL(o.TTL))
			}
			results[i] = store.Result{Err: s.Put(ctx, o.Namespace, o.Key, o.Value, opts...)}
		case store.SearchOp:
			var opts []store.SearchOption
			if o.Filter != nil {
				opts = append(opts, store.WithFilter(o.Filter))
			}
			if o.Limit > 0 {
				opts = append(opts, store.WithLimit(o.Limit))
			}
			if o.Offset > 0 {
				opts = append(opts, store.WithOffset(o.Offset))
			}
			items, err := s.Search(ctx, o.NamespacePrefix, opts...)
			results[i] = store.Result{Items: items, Err: err}
		case store.ListNamespacesOp:
			var opts []store.ListOption
			if len(o.Prefix) > 0 {
				opts = append(opts, store.WithListPrefix(o.Prefix))
			}
			if len(o.Suffix) > 0 {
				opts = append(opts, store.WithListSuffix(o.Suffix))
			}
			if o.MaxDepth > 0 {
				opts = append(opts, store.WithListMaxDepth(o.MaxDepth))
			}
			if o.Limit > 0 {
				opts = append(opts, store.WithListLimit(o.Limit))
			}
			if o.Offset > 0 {
				opts = append(opts, store.WithListOffset(o.Offset))
			}
			nss, err := s.ListNamespaces(ctx, opts...)
			results[i] = store.Result{Namespaces: nss, Err: err}
		default:
			results[i] = store.Result{Err: fmt.Errorf("mapStore: unknown op type %T", op)}
		}
	}
	return results, nil
}

// 编译期接口实现检查——业务 backend 必加，漏 method 编译期就报错。
var _ store.Store = (*mapStore)(nil)

// ── filter 匹配 (6 比较操作符) ─────────────────────────────────────────────
//
// conformance Search 子测试逐项验证 6 操作符 + 类型不匹配哨兵——业务 backend
// 必须正确实现这套语义 (跟 InMemoryStore 对齐)。

func hasSuffix(ns, suffix store.Namespace) bool {
	if len(suffix) == 0 {
		return true
	}
	if len(suffix) > len(ns) {
		return false
	}
	off := len(ns) - len(suffix)
	for i, seg := range suffix {
		if ns[off+i] != seg {
			return false
		}
	}
	return true
}

// ── 业务闭环 demo: 自定义 backend + sub-agent 防双重持久化 ──────────────────

// stubModel 按入参是否含 "Go" 路由——模拟 LLM 看到 store 注入历史后答对。
type stubModel struct{}

func (m *stubModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for _, msg := range msgs {
		if strings.Contains(msg.Content, "Go") {
			return schema.AssistantMessage("你之前提过喜欢 Go 语言"), nil
		}
	}
	return schema.AssistantMessage("你喜欢 Go 语言"), nil
}
func (m *stubModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *stubModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\nstore_custom_backend PASS（conformance 套见 `go test ./example/store/store_custom_backend`）")
}

func run() error {
	ctx := context.Background()
	st := newMapStore() // 业务自定义 backend——替 store.NewInMemoryStore() 无缝插
	ns := store.NS("users", "alice", "memories")

	// === 场景 1: 自定义 backend 跨 Run 长期记忆 ===
	agent := agent_v2.NewBasicAgent(
		agent_v2.WithName("memory_agent"),
		agent_v2.WithModel(&stubModel{}),
		agent_v2.WithStore(st), // ← 挂业务自定义 backend
		agent_v2.WithStoreMemory(ns, nil),
	)
	sess1 := agent_v2.NewRunSession()
	if _, err := agent.Run(ctx, sess1, schema.UserMessage("我喜欢 Go")); err != nil {
		return fmt.Errorf("Run 1: %w", err)
	}
	items, err := st.Search(ctx, ns, store.WithQuery("Go"))
	if err != nil {
		return fmt.Errorf("Search after Run 1: %w", err)
	}
	if len(items) == 0 {
		return fmt.Errorf("自定义 backend 应记录 memory，got 0 条")
	}
	fmt.Printf("场景 1: 自定义 backend 记录 %d 条 memory（顶层 agent 写入）\n", len(items))

	// === 场景 2: 成员经编排嵌套时，它的记忆照样写得进去 ===
	// SequentialAgent 内嵌 BasicAgent。框架把写入者位置（步骤注册名）放进 namespace——
	// 基准 ns 下再加一段——所以成员的 WithStoreMemory 真的生效，也不会和别的写入者撞。
	// （框架仍会 push WithSubAgentStoreScope，但自己不读它；backend 别拿它整个 skip。）
	st2 := newMapStore()
	nsShared := store.NS("team", "shared", "facts")
	subAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("sub"),
		agent_v2.WithModel(&stubModel{}),
		agent_v2.WithStore(st2),
		agent_v2.WithStoreMemory(nsShared, nil),
	)
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
		AddStep("sub", subAgent)
	sess2 := agent_v2.NewRunSession()
	if _, err := seq.Run(ctx, sess2, schema.UserMessage("我喜欢 Go")); err != nil {
		return fmt.Errorf("Run 2 (sequential): %w", err)
	}
	// 成员的 WithStoreMemory 会真的生效——它写到基准 ns 下以写入者位置（这里是步骤
	// 注册名 "sub"）为末段的 namespace，跟别的写入者互不覆盖。
	subItems, err := st2.Search(ctx, nsShared)
	if err != nil {
		return fmt.Errorf("Search nsShared: %w", err)
	}
	if len(subItems) != 1 {
		return fmt.Errorf("成员配了 WithStoreMemory 就该写进去，期望 1 条实际 %d 条", len(subItems))
	}
	if ns := subItems[0].Namespace; len(ns) != len(nsShared)+1 || ns[len(ns)-1] != "sub" {
		return fmt.Errorf("记忆应写到基准 ns 下以写入者位置（步骤注册名 sub）为末段的 namespace，实际 %v", ns)
	}
	fmt.Printf("场景 2: 成员经 SequentialAgent 嵌套，WithStoreMemory 生效，namespace=%v（带写入者位置，与别人不撞）\n",
		subItems[0].Namespace)

	// === 场景 3: 对照——顶层直接 Put 正常写入 ===
	if err := st2.Put(ctx, nsShared, "manual", "顶层手动写入"); err != nil {
		return fmt.Errorf("顶层 Put: %w", err)
	}
	got, err := st2.Get(ctx, nsShared, "manual")
	if err != nil || got == nil {
		return fmt.Errorf("顶层 Put 后应能 Get 到，got %v err=%v", got, err)
	}
	fmt.Printf("场景 3: 顶层 ctx（无 sub-agent scope）Put 正常写入，Get 回 value=%q\n", got.Value)

	return nil
}
