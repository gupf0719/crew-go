package store

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// InMemoryStore 是 PoC 阶段的内存 store 实现——unit test fixture 用 + 单进程
// 业务原型期。
//
// **已知限制**：
//   - 不持久化（进程重启丢失）→ 生产用 Redis/PG 后端
//   - 无 TTL 实现（不实现 TTLAware，业务调 Put + WithTTL 返 ErrTTLNotSupported）
//   - 语义搜索**默认关**：不配 [WithIndex] 时 Search 的 query 被忽略（退回时新序）
//   - 大量 key（10K+ 同 namespace）下 Search 线性扫描慢 → 业务细化 namespace 切分
//   - 并发写同 key last-write-wins（无乐观锁，业务自加版本字段）
//
// 数据结构选择：joinedNS 字符串作 outer map key（Go map key 不能用 slice）。
type InMemoryStore struct {
	mu sync.RWMutex
	// data[joinedNS][key] = *Item
	// joinedNS = strings.Join(namespace, "\x00")  ← 用 NUL 字节分隔避免跟业务
	// namespace segment 内容冲突（namespace 段已禁止含 "."；NUL 字节是 Go string
	// 合法字符但业务 namespace 实际不会含）
	data map[string]map[string]*Item
	// vectors[joinedNS][key] = 该条记录的若干向量（一条记录可产出多段文本）。
	// 只在配了 IndexConfig 时非空。
	vectors map[string]map[string][][]float32
	// idx 是语义检索配置。nil = 不支持语义检索，Search 的 query 被忽略。
	idx *IndexConfig
}

// InMemoryOption 配置 [InMemoryStore]。
type InMemoryOption func(*InMemoryStore)

// WithIndex 开启语义检索。
//
// 不配就没有——`Search(WithQuery(...))` 的 query 被忽略，退化成 filter + 时新序。
// 详见 [IndexConfig]。
func WithIndex(cfg *IndexConfig) InMemoryOption {
	return func(s *InMemoryStore) {
		if err := cfg.Validate(); err != nil {
			panic(err.Error())
		}
		s.idx = cfg
		s.vectors = make(map[string]map[string][][]float32)
	}
}

// NewInMemoryStore 构造空 InMemoryStore——PoC / unit test fixture 用。
//
// 进程重启丢失；并发安全（sync.RWMutex）；不实现 TTLAware。
func NewInMemoryStore(opts ...InMemoryOption) *InMemoryStore {
	st := &InMemoryStore{
		data: make(map[string]map[string]*Item),
	}
	for _, o := range opts {
		o(st)
	}
	return st
}

// joinNS 把 Namespace 编码为内部 map key。
//
// 用 NUL 字节 "\x00" 作分隔符——namespace 段已被 Validate 禁止含 "."，且业务
// namespace 实际不会含 NUL（业务一般是 ascii 标识符 / UUID / 用户 ID）。NUL 是
// 最不可能跟业务内容碰撞的字符。
func joinNS(n Namespace) string {
	return strings.Join(n, "\x00")
}

// splitNS 是 joinNS 反向操作——内部 map key 拆回 Namespace（ListNamespaces 用）。
func splitNS(joined string) Namespace {
	if joined == "" {
		return Namespace{}
	}
	return Namespace(strings.Split(joined, "\x00"))
}

// ── Store 接口实现 ────────────────────────────────────────────────────────

// Get 实现 Store.Get。
func (s *InMemoryStore) Get(_ context.Context, namespace Namespace, key string) (*Item, error) {
	if err := namespace.Validate(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	bucket, ok := s.data[joinNS(namespace)]
	if !ok {
		return nil, nil
	}
	it, ok := bucket[key]
	if !ok {
		return nil, nil
	}
	// 返 copy：outer field（Namespace / Key / Value 字段本身）重新赋值不影响 store 内部
	// *Item。Namespace 额外深 copy（[]string 便宜）——防业务 `got.Namespace[i] = x` 改
	// 底层数组污染 store（N2 修复；原 shallow copy 只复制 slice header 不防元素写，注释却
	// 说"防 outer field 篡改"暗示 Namespace 受保护，实际漏了元素写这条路径）。
	//
	// **Value 不拷贝**：它是 Put 时归一化出来的那份（store 自己的，不是业务传进来的对象），每次 Get 都返回它——
	// 业务 `got.Value.(map[string]any)["k"] = "v"` 会改到 store 里存的那份。把 Get 返的 Value 视为只读，要改先
	// 自己拷一份。
	itCopy := *it
	itCopy.Namespace = append(Namespace(nil), it.Namespace...)
	return &itCopy, nil
}

// Put 实现 Store.Put。
func (s *InMemoryStore) Put(ctx context.Context, namespace Namespace, key string, value any, opts ...PutOption) error {
	if err := namespace.Validate(); err != nil {
		return err
	}
	cfg := putConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.ttl != 0 {
		// InMemoryStore 不实现 TTLAware；业务想用 TTL 用 Redis 后端（sql/PG 暂不支持，待 Phase 2）。
		return ErrTTLNotSupported
	}
	// 存的是值的 JSON 形状（见 NormalizeValue），跟持久后端读回的一样；编不成 JSON 的值在这里报错
	stored := value
	if value != nil {
		norm, err := NormalizeValue(value)
		if err != nil {
			return fmt.Errorf("store put: value is not JSON-encodable: %w", err)
		}
		stored = norm
	}

	// **embed 在加锁之前**——它是网络往返，持锁做会让整个 store 卡在一次 RPC 上。
	// 失败不让 Put 失败（见 IndexConfig godoc）：记录照常存，只是不参与语义检索。
	var vecs [][]float32
	if s.idx != nil && value != nil {
		vecs = s.embedValue(ctx, value, stored)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// value == nil 等价 Delete
	if value == nil {
		s.deleteLocked(namespace, key)
		return nil
	}
	if s.idx != nil {
		s.setVectorsLocked(namespace, key, vecs)
	}

	bucketKey := joinNS(namespace)
	bucket, ok := s.data[bucketKey]
	if !ok {
		bucket = make(map[string]*Item)
		s.data[bucketKey] = bucket
	}
	now := time.Now().UTC() // audit iter-3 视角 3 H2: 强制 UTC 防跨 dev/prod TZ 偏移
	existing, exists := bucket[key]
	if exists {
		// 覆盖：保留 CreatedAt 仅更新 Value + UpdatedAt。存的是归一化出来的那份，业务 Put 之后再改自己的对象
		// 碰不到它。
		existing.Value = stored
		existing.UpdatedAt = now
		return nil
	}
	// 新建：namespace 深 copy 防业务后续改 slice 影响 store。
	nsCopy := make(Namespace, len(namespace))
	copy(nsCopy, namespace)
	bucket[key] = &Item{
		Namespace: nsCopy,
		Key:       key,
		Value:     stored,
		CreatedAt: now,
		UpdatedAt: now,
	}
	return nil
}

// Delete 实现 Store.Delete。
func (s *InMemoryStore) Delete(_ context.Context, namespace Namespace, key string) error {
	if err := namespace.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteLocked(namespace, key)
	return nil
}

// deleteLocked 是内部 delete——**调用方必须已持 s.mu.Lock()**。
// 不持锁调用会触发 race。
func (s *InMemoryStore) deleteLocked(namespace Namespace, key string) {
	bucketKey := joinNS(namespace)
	bucket, ok := s.data[bucketKey]
	if !ok {
		return // 不存在不报错（幂等）
	}
	delete(bucket, key)
	if len(bucket) == 0 {
		delete(s.data, bucketKey)
	}
	// **向量表跟着清**——它是 data 的旁表，不同步清的话：删一条记录后它的向量还留着，
	// 下次同 key 写入若 embed 失败，新内容就会配着**上一条的向量**被检索出来。
	if vb := s.vectors[bucketKey]; vb != nil {
		delete(vb, key)
		if len(vb) == 0 {
			delete(s.vectors, bucketKey)
		}
	}
}

// Search 实现 Store.Search。
func (s *InMemoryStore) Search(ctx context.Context, namespacePrefix Namespace, opts ...SearchOption) ([]*SearchItem, error) {
	if len(namespacePrefix) == 0 {
		// 禁止空 prefix 全店扫描
		return nil, ErrEmptyNamespace
	}
	if err := namespacePrefix.Validate(); err != nil {
		return nil, err
	}
	cfg := searchConfig{limit: 10}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.limit <= 0 {
		cfg.limit = 10
	}
	// 预校验 filter 操作符——业务误传 $or / $nor 等早错
	if err := ValidateFilter(cfg.filter); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// 收集 namespacePrefix 下所有匹配 item
	matched := make([]*SearchItem, 0)
	for bucketKey, bucket := range s.data {
		ns := splitNS(bucketKey)
		if !namespacePrefix.IsPrefixOf(ns) {
			continue
		}
		for _, it := range bucket {
			ok, err := MatchFilter(it.Value, cfg.filter)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			// copy 同 Get——Namespace 深 copy 防元素写污染 store，Value 不拷贝（详见 Get 注释）
			itCopy := *it
			itCopy.Namespace = append(Namespace(nil), it.Namespace...)
			matched = append(matched, &SearchItem{Item: itCopy})
		}
	}

	// 有 query 且配了索引 → 按语义相似度排；否则按时新序（UpdatedAt 降序）。
	//
	// **filter 和语义是串联的**：上面已经把不合 filter 的滤掉了，这里只对剩下的打分。
	// 不是「配了 query 就不看 filter」。
	if s.idx != nil && cfg.query != "" {
		if err := s.rankByQuery(ctx, matched, cfg.query); err != nil {
			return nil, err
		}
	} else {
		// **时新优先**（UpdatedAt 降序）。见 conformance 的 Search_StableOrder_RecentFirst：
		// Search 恒带 limit，所以顺序就是「截断时留下谁」——按 key 字典序截断等于按首
		// 字母挑，跟这条数据对调用方有没有用零相关。时新性不完美，但它是单调有用的先验。
		sort.Slice(matched, func(i, j int) bool {
			ti, tj := matched[i].UpdatedAt, matched[j].UpdatedAt
			if !ti.Equal(tj) {
				return ti.After(tj)
			}
			// **同时间戳时回落 (ns, key)，那只是为了「同一份数据每次查同样的顺序」，
			// 不代表写入序。** 契约到此为止——见 Search 的 godoc。
			//
			// 一度在这儿放过一个进程内单调序号，为的是让碰撞桶内也保持写入序。撤掉了：
			// 它换来的那个更强性质（**跨写入运行**可复现）只有测试关心——业务是「写一次、
			// 查多次」，而同一份已写入的数据本来就稳定（实测查 30 次 1 种结果）。代价却是
			// 那个手段跨不过序列化边界 ⟹ redis/sql 上做不到 ⟹ 同一个契约在三个后端语义不同。
			if matched[i].Namespace.String() != matched[j].Namespace.String() {
				return matched[i].Namespace.String() < matched[j].Namespace.String()
			}
			return matched[i].Key < matched[j].Key
		})
	}

	// 应用 offset + limit
	if cfg.offset >= len(matched) {
		return []*SearchItem{}, nil
	}
	matched = matched[cfg.offset:]
	if len(matched) > cfg.limit {
		matched = matched[:cfg.limit]
	}
	return matched, nil
}

// ListNamespaces 实现 Store.ListNamespaces。
func (s *InMemoryStore) ListNamespaces(_ context.Context, opts ...ListOption) ([]Namespace, error) {
	cfg := listConfig{limit: 100}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.limit <= 0 {
		cfg.limit = 100
	}
	// prefix / suffix 自身需合法（非空时才校验）
	if len(cfg.prefix) > 0 {
		if err := cfg.prefix.Validate(); err != nil {
			return nil, err
		}
	}
	if len(cfg.suffix) > 0 {
		if err := cfg.suffix.Validate(); err != nil {
			return nil, err
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// 收集所有 namespace（已去重——每个 bucketKey 一条）
	all := make([]Namespace, 0, len(s.data))
	for bucketKey := range s.data {
		ns := splitNS(bucketKey)
		if !matchesPrefix(ns, cfg.prefix) {
			continue
		}
		if !matchesSuffix(ns, cfg.suffix) {
			continue
		}
		// maxDepth 截断：若 ns 长于 maxDepth 只返前 N 段
		if cfg.maxDepth > 0 && len(ns) > cfg.maxDepth {
			truncated := make(Namespace, cfg.maxDepth)
			copy(truncated, ns[:cfg.maxDepth])
			all = append(all, truncated)
			continue
		}
		all = append(all, ns)
	}

	// maxDepth 截断可能产生重复项——去重
	if cfg.maxDepth > 0 {
		all = dedupNamespaces(all)
	}

	// 稳定字典序
	sort.Slice(all, func(i, j int) bool {
		return all[i].String() < all[j].String()
	})

	// 应用 offset + limit
	if cfg.offset >= len(all) {
		return []Namespace{}, nil
	}
	all = all[cfg.offset:]
	if len(all) > cfg.limit {
		all = all[:cfg.limit]
	}
	return all, nil
}

// Batch 实现 Store.Batch。
//
// per-Op 错误隔离 best-effort——某 Op 失败 Result.Err 非 nil，其他 Op 仍执行
// （单 Op 异常不影响 batch 中其他 Op）。详见
// doc/v2/store.md §8 + §11 表 "Batch 失败语义" 行。
func (s *InMemoryStore) Batch(ctx context.Context, ops []Op) ([]Result, error) {
	results := make([]Result, len(ops))
	for i, op := range ops {
		switch o := op.(type) {
		case GetOp:
			item, err := s.Get(ctx, o.Namespace, o.Key)
			results[i] = Result{Item: item, Err: err}
		case PutOp:
			var opts []PutOption
			if o.TTL != 0 {
				opts = append(opts, WithTTL(o.TTL))
			}
			err := s.Put(ctx, o.Namespace, o.Key, o.Value, opts...)
			results[i] = Result{Err: err}
		case SearchOp:
			var opts []SearchOption
			if o.Filter != nil {
				opts = append(opts, WithFilter(o.Filter))
			}
			if o.Query != "" {
				opts = append(opts, WithQuery(o.Query))
			}
			if o.Limit > 0 {
				opts = append(opts, WithLimit(o.Limit))
			}
			if o.Offset > 0 {
				opts = append(opts, WithOffset(o.Offset))
			}
			items, err := s.Search(ctx, o.NamespacePrefix, opts...)
			results[i] = Result{Items: items, Err: err}
		case ListNamespacesOp:
			var opts []ListOption
			if len(o.Prefix) > 0 {
				opts = append(opts, WithListPrefix(o.Prefix))
			}
			if len(o.Suffix) > 0 {
				opts = append(opts, WithListSuffix(o.Suffix))
			}
			if o.MaxDepth > 0 {
				opts = append(opts, WithListMaxDepth(o.MaxDepth))
			}
			if o.Limit > 0 {
				opts = append(opts, WithListLimit(o.Limit))
			}
			if o.Offset > 0 {
				opts = append(opts, WithListOffset(o.Offset))
			}
			nss, err := s.ListNamespaces(ctx, opts...)
			results[i] = Result{Namespaces: nss, Err: err}
		default:
			// sealed interface 防外部包实现新 Op；理论上不可达
			results[i] = Result{Err: fmt.Errorf("store: unknown Op type %T", op)}
		}
	}
	return results, nil
}

// ── helper：filter / prefix / suffix ──────────────────────────────────────

// matchesPrefix 检查 ns 是否以 prefix 开头（prefix 空时永真）。
func matchesPrefix(ns, prefix Namespace) bool {
	if len(prefix) == 0 {
		return true
	}
	return prefix.IsPrefixOf(ns)
}

// matchesSuffix 检查 ns 是否以 suffix 结尾（suffix 空时永真）。
func matchesSuffix(ns, suffix Namespace) bool {
	if len(suffix) == 0 {
		return true
	}
	if len(suffix) > len(ns) {
		return false
	}
	offset := len(ns) - len(suffix)
	for i, seg := range suffix {
		if ns[offset+i] != seg {
			return false
		}
	}
	return true
}

// dedupNamespaces 按 String() 去重——maxDepth 截断后可能产生重复。
func dedupNamespaces(nss []Namespace) []Namespace {
	seen := make(map[string]struct{}, len(nss))
	out := make([]Namespace, 0, len(nss))
	for _, ns := range nss {
		key := ns.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, ns)
	}
	return out
}

// 编译期接口实现检查
var _ Store = (*InMemoryStore)(nil)

// embedValue 把一条 value 抽成文本再 embed。不持锁调用（含网络往返）。
//
// 任何一步失败都返回 nil——这条记录不参与语义检索，但 Put 照常成功。
//
// 抽文本用哪个值：业务配的抽取器（IndexConfig.Extractor）拿业务传进来的原值——它是按业务自己的类型写的；默认抽取器
// 对实现了 GetContent 的值用原值（读回的形状里没有这个方法），其余用存下的 JSON 形状——struct、`map[string]string`
// 读回是 `map[string]any`，Fields 按字段名取值跟 filter 按字段命中是同一个形状。
func (s *InMemoryStore) embedValue(ctx context.Context, value, stored any) [][]float32 {
	in := value
	if s.idx.Extractor == nil {
		if _, ok := value.(interface{ GetContent() string }); !ok {
			in = stored
		}
	}
	texts := s.idx.extractor()(in, s.idx.Fields)
	if len(texts) == 0 {
		return nil
	}
	vecs, err := s.idx.Embedder.Embed(ctx, texts)
	if err != nil {
		slog.WarnContext(ctx, "store: embed failed, item will not be semantically searchable",
			"texts", len(texts), "err", err)
		return nil
	}
	if len(vecs) != len(texts) {
		// Embedder 契约要求一一对应。数量不等说明实现有问题——不能猜哪个对应哪个。
		slog.WarnContext(ctx, "store: embedder returned wrong count, item will not be semantically searchable",
			"want", len(texts), "got", len(vecs))
		return nil
	}
	return vecs
}

// setVectorsLocked 写向量表。持锁调用。
func (s *InMemoryStore) setVectorsLocked(namespace Namespace, key string, vecs [][]float32) {
	bucketKey := joinNS(namespace)
	if len(vecs) == 0 {
		// 覆盖写但这次没能 embed：**必须清掉旧向量**，否则新内容配着旧向量被检索出来。
		if b := s.vectors[bucketKey]; b != nil {
			delete(b, key)
		}
		return
	}
	b, ok := s.vectors[bucketKey]
	if !ok {
		b = make(map[string][][]float32)
		s.vectors[bucketKey] = b
	}
	b[key] = vecs
}

// cosine 算余弦相似度。任一方为零向量时返 0（而不是 NaN）。
func cosine(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		na += x * x
		nb += y * y
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// rankByQuery 按语义相似度**原地重排** matched，并回填每条的 Score。
//
// 三条不抄就会错的细节：
//
//   - **一条记录可能有多个向量**（多段文本各自 embed）。取其中**最高**的那个分代表
//     这条记录（max pooling），不是平均——平均会让「一段命中、其余无关」的记录被
//     稀释掉，而那正是该被召回的形态。
//   - **没有向量的记录不参与打分**，但也不丢弃：它们排在所有打了分的后面（Score 保持
//     0）。原因是「配了索引」不等于「每条都 embed 成功了」——业务 seed 的数据、
//     embed 失败的、以及 value 形态不被抽取器认识的，都在这一档。丢弃它们会让
//     「加了语义检索」变成「一部分数据凭空消失」。
//   - **query 的 embedding 也要走 Embedder**，且失败时不静默退化成字典序——那会让
//     业务以为语义检索在工作。
func (s *InMemoryStore) rankByQuery(ctx context.Context, matched []*SearchItem, query string) error {
	qv, err := s.idx.Embedder.Embed(ctx, []string{query})
	if err != nil {
		return fmt.Errorf("store: embed query: %w", err)
	}
	if len(qv) != 1 || len(qv[0]) == 0 {
		return fmt.Errorf("store: embedder returned %d vectors for 1 query", len(qv))
	}

	s.mu.RLock()
	for _, it := range matched {
		b := s.vectors[joinNS(it.Namespace)]
		if b == nil {
			continue
		}
		best := 0.0
		scored := false
		for _, v := range b[it.Key] {
			if sc := cosine(qv[0], v); !scored || sc > best {
				best, scored = sc, true
			}
		}
		if scored {
			it.Score = best
		}
	}
	s.mu.RUnlock()

	// 打了分的按分降序在前；都没分的退回字典序，保持结果可预测。
	sort.SliceStable(matched, func(i, j int) bool {
		a, b := matched[i], matched[j]
		if a.Score != b.Score {
			return a.Score > b.Score
		}
		if a.Namespace.String() != b.Namespace.String() {
			return a.Namespace.String() < b.Namespace.String()
		}
		return a.Key < b.Key
	})
	return nil
}

// RanksByRelevance 实现 [RelevanceRanker]——配了 [IndexConfig] 才按相关性排。
func (s *InMemoryStore) RanksByRelevance() bool { return s.idx != nil }
