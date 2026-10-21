// Package store 实现跨 thread / 跨用户的长期 KV 持久化抽象——crew-go 的第三个
// 持久化层（前两层是 compose_v2.Checkpointer 图状态快照 + agent_v2.RunSession
// 内存会话）。
//
// 设计目标：业务场景如用户偏好持久化、长期事实记忆（mem0 接入）、多 agent 共享
// 黑板、跨 thread 幂等记录——Checkpointer 和 Memory 都装不下，因为它们各自绑死
// 某个 sessionID 范围。Store 显式跨 thread / 跨 sessionID 共享。
//
// **跟 Checkpointer ns 的命名约定差异**：本包 namespace 是业务数据分类的层级路径
// （`["users", "alice", "preferences"]`），跟 compose_v2.ns string（图运行时寻址路径
// `a/b#0`）是**不同维度概念**。两者用类型 + 包路径 + 参数名三层防混淆——业务把
// Checkpointer 的 ns string 误传给 Store 编译错误。详见 doc/v2/store.md §2。
//
// **核心接口设计**：以业务持久化 KV 抽象为目标，提供接口/Op/Item/namespace/
// TTL/IndexConfig 等核心概念，保持稳定。pre_model_hook 等 hook 体系不在本包范围。
package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ── Namespace ─────────────────────────────────────────────────────────────

// Namespace 是业务数据分类的层级路径。
//
// 是 new type 而非 type alias：
//
//	type Namespace []string   ← 我们选这个（编译期类型隔离）
//	type Namespace = []string ← 没选（alias 会让 []string 跟 Namespace 自由互转，
//	                            丢防错能力）
//
// 编译器隔离业务把字符串 ns（如 compose_v2.JoinNS 的返回值）误传成 Store namespace。
type Namespace []string

// NS 是 Namespace 构造 helper——调用方便利，**不做合法性校验**。
//
//	store.NS("users", userID, "preferences")
//	等价 store.Namespace{"users", userID, "preferences"}
//
// 校验由 Store 方法入口统一做（违反返 ErrInvalidNamespace）；业务想显式 validate
// 用 Namespace.Validate()。
func NS(parts ...string) Namespace { return Namespace(parts) }

// Validate 检查 namespace 合法性。
//
// 规则：
//   - 非空（Namespace{} 非法）
//   - 每段必须非空字符串
//   - 每段不能含 "." 字符（String() 日志输出用 "." 拼接，禁止避免日志解析歧义）
//   - 每段不能含 NUL 字节 "\x00"（InMemoryStore 内部 bucket key 编码用 NUL 作分隔
//     符；不禁止会导致 NS("a\x00b","c") 跟 NS("a","b\x00c") 共享 bucket 互相覆盖
//     —— 业务输入含外部用户串时这是真实安全 bug）
//
// 业务一般不直接调——每个 Store 方法入口自动校验。
func (n Namespace) Validate() error {
	if len(n) == 0 {
		return ErrEmptyNamespace
	}
	for i, seg := range n {
		if seg == "" {
			return errInvalidNamespacef("segment %d is empty", i)
		}
		if strings.Contains(seg, ".") {
			return errInvalidNamespacef("segment %d %q contains '.' (reserved for String() separator)", i, seg)
		}
		if strings.ContainsRune(seg, '\x00') {
			return errInvalidNamespacef("segment %d contains NUL byte (reserved for internal bucket separator)", i)
		}
	}
	return nil
}

// IsPrefixOf 判断 n 是否为 other 的前缀。
//
//	NS("users").IsPrefixOf(NS("users", "alice")) == true
//	NS("users", "alice").IsPrefixOf(NS("users")) == false
func (n Namespace) IsPrefixOf(other Namespace) bool {
	if len(n) > len(other) {
		return false
	}
	for i, seg := range n {
		if other[i] != seg {
			return false
		}
	}
	return true
}

// Equal 完全相等比较。
func (n Namespace) Equal(other Namespace) bool {
	if len(n) != len(other) {
		return false
	}
	for i, seg := range n {
		if other[i] != seg {
			return false
		}
	}
	return true
}

// String 返回点分形式作日志用："users.alice.preferences"
func (n Namespace) String() string {
	return strings.Join(n, ".")
}

// ── Item / SearchItem ─────────────────────────────────────────────────────

// Item 是 Store 的存储单元——key-value 加创建/更新时间元数据。
//
// Value 必须 JSON 可序列化（业务存到 Redis/PG 后端时框架要序列化）。InMemoryStore
// 阶段不强制校验，业务自负；接 Redis/PG 后端时违反会失败。
//
// **Value 浅持有约定**：Put 时 store 内部直接持有业务传入的 Value 指针（不 deep
// copy）；Get/Search 返回的 Item.Value 也是同一指针。业务约定：把 Value 视为不可
// 变——Put 后不再 mutate value 内 map/slice；Get 返的 Value 想改先 deep copy。违反
// 会跟 store 内部状态产生 race。
type Item struct {
	Namespace Namespace
	Key       string
	Value     any // 业务任意 JSON 兼容值（map/slice/string/number/bool/nil）
	CreatedAt time.Time
	UpdatedAt time.Time
}

// SearchItem 是 Search 返回元素——继承 Item 加可选 Score（语义搜索的相似度分数）。
//
// **Score 为 0 只表示「没有正的相关性」，不区分下面三种**：
//
//   - 这个 store 没开语义搜索（filter-only 路径，整批都是 0）
//   - 这条记录没能参与打分（抽不出文本 / embed 失败）
//   - 参与了打分，但与 query 的余弦恰好是 0（正交）
//
// **别拿 `Score == 0` 当「被判定为不相关」用**——前两种压根没被判定过。
//
// ⚠️ 排序是纯按 Score 降序：未打分的（0）与打了 0 分的**交错在一起**，谁在前取决于
// 它们的 (ns, key) 字典序，不是「未打分的一定在后面」（实测过：未打分的 `a_xxx` 排在
// 打了 0 分的 `b_xxx` 之前）。要区分这三种，业务得自己在 value 里留标记。
type SearchItem struct {
	Item
	Score float64
}

// ── Store 接口 ────────────────────────────────────────────────────────────

// Store 是跨 thread / 跨用户的长期 KV 持久化接口。
//
// 6 个核心方法 + 可选 TTLAware interface。所有方法对 namespace 不合法时返
// ErrInvalidNamespace（含 ErrEmptyNamespace）。完整错误集见 errors.go。
//
// 内置 InMemoryStore + redis/sql（PG/MySQL）后端；mem0 等业务自实现。
type Store interface {
	// Get 精确读取单条。不存在返 (nil, nil)——nil item 表示无此 key，不是 error
	// 路径，业务用 `if item == nil` 判断。
	Get(ctx context.Context, namespace Namespace, key string) (*Item, error)

	// Put 写入或覆盖。同 namespace+key 再次 Put 覆盖旧值，UpdatedAt 自动更新。
	// 支持 PutOption（含 TTL，PoC 阶段 InMemoryStore 调 WithTTL 返
	// ErrTTLNotSupported）。value 为 nil 等价 Delete。
	//
	// namespace 与 key 逐字节就是身份：逐字节不同的两个就是两份数据，只差尾随空格、大小写、
	// 重音或组合字符写法的也是（conformance 钉住；自研后端的键列要按字节比较，别用忽略尾随
	// 空格的 PAD SPACE 或不区分大小写 / 重音的排序规则）。
	//
	// **可移植的长度上限：key 255 字符、namespace 全路径 512 字符**（conformance 钉在
	// 255 / 512 上，含 4 字节字符）。这不是框架的限制，是最窄的那个后端定的——`sql` 的 MySQL 建表用
	// `data_key VARBINARY(1020)` + `ns VARBINARY(2048)`（按字节计；两列进同一个主键合计 3068
	// 字节，贴着 InnoDB 索引键 3072 字节的上限，再宽就建不出表），按最宽的 4 字节字符算
	// 正好 255 / 512 个字符。in-memory / redis / PG / SQLite 实际不受此限。
	//
	// ⚠️ **超限的失败方式取决于后端**：MySQL 非 STRICT 模式下会**静默截断**——两个只在
	// 第 1020 个字节之后不同的 key 会塌成同一行，`Get` 返回别人的数据且 err == nil。
	// 业务的 key 来自外部输入（用户 ID 拼串之类）时请自己先截或先哈希。
	Put(ctx context.Context, namespace Namespace, key string, value any, opts ...PutOption) error

	// Delete 删除单条。key 不存在不报错（幂等）。
	Delete(ctx context.Context, namespace Namespace, key string) error

	// Search 在 namespacePrefix 下查询。
	//   - 必须指定 prefix（不允许空 namespace 全店扫，空返 ErrEmptyNamespace）
	//   - 支持 filter 精确匹配 + MongoDB 风格 6 比较操作符：JSON 语义相等 + 范围 op
	//     type bracketing，**主体语义所有内置后端一致且由 conformance 逐格钉**；⚠️ 但有
	//     几处已知不一致（小数精度、对象值的 JSON 往返；字段名字符集那条已于 2026-09-07 消除），**用 filter 前
	//     先读 [WithFilter] 的「已知的跨后端不一致」那几条**
	//   - 语义搜索是**可选 capability**：配了 [IndexConfig] 的实现按相关性排序并回填
	//     [SearchItem.Score]（内置 [InMemoryStore] 用 [WithIndex] 开启），没配就忽略
	//     query。实现方可用 [RelevanceRanker] 自报
	//   - **顺序契约：时新优先**（UpdatedAt 降序）。Search 恒带 limit，所以顺序**就是**
	//     「截断时留下谁」——按 key 字典序截断等于按首字母挑，跟这条数据对调用方有没有
	//     用零相关；时新性不完美，但它是单调有用的先验。
	//   - **同 UpdatedAt 时回落 (ns, key)**，那只保证「同一份已写入的数据每次查同样的
	//     顺序」，**不代表写入先后**。时钟分辨率不足以分开一次突发写入时（实测本机
	//     10 条连续 Put 只落进 3 个时间戳桶），桶内顺序与写入序无关。要严格的写入序
	//     请自己在 value 里带序号。
	Search(ctx context.Context, namespacePrefix Namespace, opts ...SearchOption) ([]*SearchItem, error)

	// ListNamespaces 列出 store 中所有 namespace 路径。
	//   - prefix/suffix 路径过滤
	//   - maxDepth 截断返回深度（仅返回前 N 段），截断后相同的只留一个
	//   - 结果按 [Namespace.String] 的字节序排列；limit / offset 的分页窗口取在过滤、
	//     截断、去重之后的列表上——先对原始 namespace 分页再截断会少返、跨页重复
	ListNamespaces(ctx context.Context, opts ...ListOption) ([]Namespace, error)

	// Batch 批量执行 ops，按入参顺序返回 Result。
	//
	// **失败语义是 per-Op 错误隔离，且这是唯一合法语义**：某个 Op 失败时它的
	// [Result].Err 非 nil，**其余 Op 照常执行、照常持久化**。conformance 的
	// `Batch_PerOp_Error_Isolated_BestEffort` 就钉这一条。
	//
	// ⚠️ **别做成单事务原子写**。这里曾写着「后端实现（PG）可优化为单事务原子写」——
	// 那句话是错的，照它做的后端跑 conformance 必红（实测：把 [NewInMemoryStore] 包一层
	// 「任一 Op 失败则整批不落盘」，`RunStoreTests` 当场报
	// `valid PutOp value should be persisted despite sibling failing`）。
	//
	// 两种语义都放行不是「更灵活」而是更坏：业务换个后端，「兄弟 Op 保不保得住」
	// 就变成掷骰子，而两边都 err == nil。
	//
	// ⚠️ **这里没有事务逃生舱**：本接口没有任何接收事务句柄的入口。以 sql 后端为例，
	// `sql.NewStore` 收的是 `*sql.DB`、每个方法都走它去连接池取连接，而 `*sql.Tx` 独占
	// 一条连接——业务在外面 `sql.DB.BeginTx` 罩不住 Store 的调用。要原子性只能绕开本接口
	// 直接用底层存储自己的事务能力（in-memory 后端则根本没有这回事）。
	Batch(ctx context.Context, ops []Op) ([]Result, error)
}

// TTLAware 是可选 interface assertion——业务感知 store 是否支持 TTL 用：
//
//	if ta, ok := st.(store.TTLAware); ok && ta.SupportsTTL() {
//	    // 走 WithTTL 路径
//	}
//
// InMemoryStore + SQL 后端不实现 TTLAware（返 ErrTTLNotSupported，SQL 待 Phase 2 加 expires_at 列）；Redis 后端实现并返 true。
type TTLAware interface {
	SupportsTTL() bool
}

// RelevanceRanker 声明「这个 store 的 Search 会按**跟 query 的相关性**排序」，而不是
// 按默认的时新序。
//
// 可选 capability（同 [TTLAware]）——不实现视为 false。向量数据库 / mem0 / 配了
// [IndexConfig] 的 [InMemoryStore] 应返 true。
//
// **上层据此决定要不要插手结果的挑选**。举例：长期记忆注入在后端不排序时会自己给
// 候选打分重排；返 true 就是在说「别插手，我挑过了」——那时上层再排只会把最相关的
// 几条换成更差的。
//
// 是方法不是类型断言，因为同一个实现可能按配置切换（[InMemoryStore] 配了索引才有）。
type RelevanceRanker interface {
	RanksByRelevance() bool
}

// ── Op 体系 ────────────────────────────────────────────────────────────────

// Op 是 Batch 操作单元——sealed interface，4 个具体类型（GetOp / PutOp /
// SearchOp / ListNamespacesOp）实现。未导出 marker 方法 opMarker 禁止外部包实现
// 新 Op 类型。
type Op interface {
	opMarker()
}

// GetOp 批量读单条。
type GetOp struct {
	Namespace Namespace
	Key       string
}

// PutOp 批量写单条。Value 为 nil 等价 Delete。
type PutOp struct {
	Namespace Namespace
	Key       string
	Value     any
	TTL       time.Duration // 0 表示不设 TTL
}

// SearchOp 批量查询。
type SearchOp struct {
	NamespacePrefix Namespace
	Filter          map[string]any
	Query           string // 语义搜索 query（InMemoryStore 忽略）
	Limit           int    // 0 表示默认 10
	Offset          int
}

// ListNamespacesOp 批量列出 namespace。
type ListNamespacesOp struct {
	Prefix   Namespace // 空表示不过滤前缀
	Suffix   Namespace // 空表示不过滤后缀
	MaxDepth int       // 0 表示不截断
	Limit    int       // 0 表示默认 100
	Offset   int
}

func (GetOp) opMarker()            {}
func (PutOp) opMarker()            {}
func (SearchOp) opMarker()         {}
func (ListNamespacesOp) opMarker() {}

// Result 是 Batch 中单 Op 的返回——按 ops 入参顺序对应。
//
// 业务遍历 results 时按 ops[i].(type) type-switch 读对应字段：
//
//	for i, op := range ops {
//	    switch op.(type) {
//	    case store.GetOp:             item := results[i].Item
//	    case store.SearchOp:          items := results[i].Items
//	    case store.ListNamespacesOp:  nss := results[i].Namespaces
//	    case store.PutOp:             // 仅看 results[i].Err
//	    }
//	}
type Result struct {
	Item       *Item         // GetOp 返回
	Items      []*SearchItem // SearchOp 返回
	Namespaces []Namespace   // ListNamespacesOp 返回
	Err        error         // 单 Op 失败不中断 Batch（InMemoryStore 行为）
}

// ── Option 类型 ───────────────────────────────────────────────────────────

// PutOption 配置 Put 行为。
type PutOption func(*putConfig)

type putConfig struct {
	ttl time.Duration
}

// WithTTL 设置本条 Item 的生命期。后端不支持 TTL 时 Put 返 ErrTTLNotSupported。
//
// 业务想感知后端是否支持先 type-assert (store.TTLAware)：
//
//	if ta, ok := st.(store.TTLAware); ok && ta.SupportsTTL() {
//	    _ = st.Put(ctx, ns, key, value, store.WithTTL(5*time.Minute))
//	}
func WithTTL(d time.Duration) PutOption {
	return func(c *putConfig) { c.ttl = d }
}

// SearchOption 配置 Search 行为。
type SearchOption func(*searchConfig)

type searchConfig struct {
	filter map[string]any
	query  string
	limit  int
	offset int
}

// WithFilter 精确匹配 + 操作符过滤。
//
// filter map 语法（MongoDB 风格子集）：
//
//	{"key": value}              // 精确匹配（JSON 语义相等，同 $eq）
//	{"key": {"$eq": value}}     // 显式 $eq
//	{"key": {"$ne": value}}     // 字段存在且不等（JSON null 算存在，缺失不命中）
//	{"key": {"$gt": 10}}        // $gt/$gte/$lt/$lte——filter 值必须是数值
//	{"k1": v1, "k2": v2}        // 多键 AND（**不支持** $or/$nor/$in/$nin/$and）
//
// 语义（所有内置后端一致，conformance 逐格钉；权威说明见 [MatchFilter]）：
//   - 相等是 JSON 语义：10 == 10.0，true != 1，"10" != 10，数组逐元素相等（不是包含），
//     对象逐键相等
//   - 范围 op 是 type bracketing：**存的值不是数值（字符串 / bool / null / 数组 / 对象）
//     或字段缺失 ⟹ 这一行不命中、不报错**；一条异构行不会让整个 Search 失败
//   - 业务误传不支持的 op（如 $or）返 [ErrUnsupportedFilterOp]——**顶级形态**
//     `{"$or": [...]}` 同样按不支持的操作符拒，不会被当成一个叫 "$or" 的字段做
//     等值（那样零行且 err == nil，业务只看到「没数据」）。代价是**顶级 key 不能以
//     `$` 开头**，业务真有这种字段名请改名。范围 op 的 filter 值不是数值返
//     [ErrFilterTypeMismatch]（静态判定，空 namespace 也报）
//   - 仅顶级 key；嵌套 filter `{"a": {"b": ...}}` 各后端行为不一致（sql 拒绝、in-memory
//     按嵌套部分匹配），别依赖。**直值是对象时同理**——`{"f": {"x": 1}}` 会被当成嵌套
//     filter 而不是「等于这个对象」
//
// ⚠️ **filter 只认顶层值是 `map[string]any` 的记录**（客户端过滤后端）：`MatchFilter`
// 开头就断言 `value.(map[string]any)`，所以 `Put` 存 `map[string]string` /
// `struct` / 其他具体类型时，in-memory 与 example 后端**一格都匹配不上**（返 0 条、
// `err == nil`），而 redis 与三个 sql 方言因为存的是 JSON、读回来恒是
// `map[string]any`，**同一个 filter 照常命中**。实测 `map[string]string{"z":"1"}` +
// filter `{"z":"1"}`：in-memory 0 条、SQLite 1 条。**要跨后端可移植，Put 的值请用
// `map[string]any`**（或先自己过一次 JSON 往返）。根因是 `Item.Value` 的往返契约
// 没定死——修法（Put 入口统一 JSON 归一化）见 doc/code/todo.md。
//
// ⚠️ **另两处已知的跨后端不一致**（6 后端 × 74 组的差分实测，2026-09-04；修法与进展见
// `doc/code/todo.md`）：
//   - ~~**字段名字符集**~~ **已消除（2026-09-07）**：`sql` 后端曾要求顶级 key 匹配
//     `^[a-zA-Z_][a-zA-Z0-9_]*$`（它把 key 拼进 SQL 字面量、靠白名单防注入），于是
//     `user-id` / `a.b` / `用户` 这类完全合法的 JSON 字段名换后端就从「有结果」变成
//     「报错」。现在字段名一律**绑参**，注入面从结构上消失、字符集限制随之取消；
//     conformance 的 `Search_WithFilter_NonIdentifierFieldNames` 六后端逐格钉
//   - **小数精度**：**整数已统一按精确整数比**（客户端走 big.Int，三个引擎本来就精确，
//     conformance 用贴着 2^53 的阈值钉住）；**小数没有统一**，因为三个引擎自己就不一致——
//     PG 的 jsonb 是任意精度 numeric、MySQL JSON 是 DOUBLE、SQLite `json_extract` 出
//     REAL，客户端过滤走 float64。于是 `$eq 0.1` 这种在 PG 上只命中真正的 0.1，在其余
//     后端上会把 0.1 与 0.1000…001 当成同一个值。**别用 filter 做高精度小数的相等判定**。
//     ⚠️ 整数那半也有一个**在进 store 之前**就丢精度的坑：业务用 `json.Unmarshal` 解到
//     `map[string]any` 再 Put 时，大整数已经变成 float64 了（标准库默认行为），框架这层
//     再精确也救不回来。要保精度用 `json.NewDecoder(r).UseNumber().Decode(&v)`，或直接传 int64
func WithFilter(filter map[string]any) SearchOption {
	return func(c *searchConfig) { c.filter = filter }
}

// WithLimit 分页返回上限。0/不调默认 10。
func WithLimit(n int) SearchOption {
	return func(c *searchConfig) { c.limit = n }
}

// WithOffset 分页偏移。默认 0。
func WithOffset(n int) SearchOption {
	return func(c *searchConfig) { c.offset = n }
}

// WithQuery 是语义搜索查询字符串。
//
// PoC InMemoryStore 忽略；接 mem0/向量数据库后端时生效。
func WithQuery(q string) SearchOption {
	return func(c *searchConfig) { c.query = q }
}

// ListOption 配置 ListNamespaces 行为。
type ListOption func(*listConfig)

type listConfig struct {
	prefix   Namespace
	suffix   Namespace
	maxDepth int
	limit    int
	offset   int
}

// WithListPrefix 过滤 namespace 前缀。
func WithListPrefix(prefix Namespace) ListOption {
	return func(c *listConfig) { c.prefix = prefix }
}

// WithListSuffix 过滤 namespace 后缀。
func WithListSuffix(suffix Namespace) ListOption {
	return func(c *listConfig) { c.suffix = suffix }
}

// WithListMaxDepth 截断返回 namespace 深度（仅返回前 N 段）。
func WithListMaxDepth(depth int) ListOption {
	return func(c *listConfig) { c.maxDepth = depth }
}

// WithListLimit 分页返回上限。0/不调默认 100。
func WithListLimit(n int) ListOption {
	return func(c *listConfig) { c.limit = n }
}

// WithListOffset 分页偏移。默认 0。
func WithListOffset(n int) ListOption {
	return func(c *listConfig) { c.offset = n }
}

// ── Options Parse helpers (业务自定义 backend 用) ──────────────────────────
//
// 业务自实现 store.Store 接口 (PG/MySQL/Redis/mem0 后端) 时需要读 Get/Put/Search/
// ListNamespaces 的 Option 配置. Go 跨包 private 限制让业务 backend 无法直接读
// SearchOption / PutOption 内部 fn 调用结果. 这组 Parse helper 跨包导出解析结果
// 让业务 backend 完整 implement Store 接口.
//
// 业务用法:
//
//	func (s *MyPGStore) Search(ctx context.Context, ns Namespace, opts ...SearchOption) ([]*SearchItem, error) {
//	    cfg := store.ParseSearchOptions(opts...)  // 拿到 Filter / Query / Limit / Offset
//	    // 翻译 cfg.Filter 为 SQL WHERE 子句, 用 cfg.Limit / cfg.Offset 分页
//	    ...
//	}

// PutOptions 是 PutOption 解析后的可读配置. 业务自实现 Store.Put 时用 ParsePutOptions.
type PutOptions struct {
	TTL time.Duration // WithTTL 设置的 TTL; 0 表示无 TTL
}

// ParsePutOptions 解析 PutOption 列表为 PutOptions.
func ParsePutOptions(opts ...PutOption) PutOptions {
	cfg := putConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return PutOptions{TTL: cfg.ttl}
}

// SearchOptions 是 SearchOption 解析后的可读配置. 业务自实现 Store.Search 时用
// ParseSearchOptions.
type SearchOptions struct {
	Filter map[string]any // WithFilter 设置的 filter (MongoDB 风格)
	Query  string         // WithQuery 设置的语义搜索 query (业务 mem0/向量后端用)
	Limit  int            // WithLimit 设置的分页上限; <=0 业务自行兜底 (推荐 10)
	Offset int            // WithOffset 设置的分页偏移
}

// ParseSearchOptions 解析 SearchOption 列表为 SearchOptions.
func ParseSearchOptions(opts ...SearchOption) SearchOptions {
	cfg := searchConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return SearchOptions{
		Filter: cfg.filter,
		Query:  cfg.query,
		Limit:  cfg.limit,
		Offset: cfg.offset,
	}
}

// ListOptions 是 ListOption 解析后的可读配置.
type ListOptions struct {
	Prefix   Namespace
	Suffix   Namespace
	MaxDepth int
	Limit    int
	Offset   int
}

// ParseListOptions 解析 ListOption 列表为 ListOptions.
func ParseListOptions(opts ...ListOption) ListOptions {
	cfg := listConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return ListOptions{
		Prefix:   cfg.prefix,
		Suffix:   cfg.suffix,
		MaxDepth: cfg.maxDepth,
		Limit:    cfg.limit,
		Offset:   cfg.offset,
	}
}

// ── sentinel errors ───────────────────────────────────────────────────────

// ErrEmptyNamespace 表 namespace 为空（Namespace{}）—— Get/Put/Delete/Search 等
// 必须指定非空 namespace；Search/ListNamespaces 也禁止空 namespace 全店扫。
var ErrEmptyNamespace = errors.New("store: namespace is empty")

// ErrInvalidNamespace 表 namespace 段格式非法——含空段 / 含 "." / 非空但格式错。
var ErrInvalidNamespace = errors.New("store: invalid namespace")

// ErrTTLNotSupported 表 store 后端不支持 TTL——业务调 Put 传 WithTTL 但后端没实
// 现 TTLAware 时返。InMemoryStore PoC 阶段一律返此错误。
var ErrTTLNotSupported = errors.New("store: TTL not supported by this backend")

// ErrStoreNotConfigured 表 ctx 中没注入 store——helper 内部调 FromContext 拿 nil
// 时抛此错误（典型如 WithStoreMemory hook）。
//
// `agent_v2.WithStoreMemory` helper 已落地并触发此错误（basic_agent.go）。业务调
// FromContext 自查 nil 决定降级行为时也可用此 sentinel 提示用户忘配 WithStore。
var ErrStoreNotConfigured = errors.New("store: not configured in context, missing WithStore")

// ErrUnsupportedFilterOp 表 filter map 含不支持的操作符（如 $or / $nor / $in /
// $nin / $and）。
//
// crew-go 仅支持 6 比较操作符（$eq/$ne/$gt/$gte/$lt/$lte），无逻辑组合 op。
var ErrUnsupportedFilterOp = errors.New("store: unsupported filter operator")

// ErrFilterTypeMismatch 表范围操作符（$gt/$gte/$lt/$lte）的 **filter 值**不是数值
// （如 `{"age": {"$gt": "abc"}}`）——那是调用方手上就有的信息，静态判定、空 namespace
// 也报（[ValidateFilter]）。
//
// **存的值**类型对不上不是错误：那一行不命中（type bracketing，见 [MatchFilter]）——
// 调用方无法知道 namespace 里每一行存的是什么类型，报错等于让一条脏数据毒化整个查询；
// 下推型后端（sql）也做不到不读数据就报。
var ErrFilterTypeMismatch = errors.New("store: filter operator type mismatch")

// errInvalidNamespacef wrap ErrInvalidNamespace 加上下文。业务 errors.Is(err,
// ErrInvalidNamespace) 仍能匹配。
func errInvalidNamespacef(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidNamespace}, args...)...)
}
