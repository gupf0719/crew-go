# Store 设计 RFC

> 状态：**已落地（PR 1-3.3，commits `408aba7..e6505f9`）** —— 文档随实施 incremental 更新
>
> 关联：调研基于本地 langgraph v1.1.9 / langgraph-prebuilt v1.0.10 / `BaseStore` + 截图 Python 框架 `SessionStoreProtocol` + eino `CheckPointStore`；三方对照见 [`../description/three-way-comparison/store.md`](../description/three-way-comparison/store.md)
>
> ⚠️ **LangGraph 对照范围说明**：本 RFC 聚焦 **BaseStore 接口 + 数据模型核心对照**（接口/Op/Item/namespace/TTL/IndexConfig/PG schema/Runtime.merge/InjectedStore），这部分稳定保留在 langgraph 主仓不受 prebuilt 迁移影响。**hook 体系（`create_react_agent` + `pre_model_hook` + `RemoveMessage` 等）不做对照**——LangGraph prebuilt 整套已 deprecated 迁移到 `langchain.agents.create_agent` + `AgentMiddleware`（本地无 langchain 仓源码），新 API 形态等独立 RFC 调研，本 RFC 不预先猜测。

## 设计目标

crew-go 当前只有两层持久化（**注**: 下文 `agent_v2.Memory` 是历史段叙述, 接口 PR 3.3 commit `e6505f9` 已删替代为 store, 详 §6 迁移章节）：
- `compose_v2.Checkpointer`（图执行状态快照，按 sessionID 索引）
- `agent_v2.Memory`（注入 LLM 前的全量消息流，按 session 内存对象）—— **历史段叙述, 已替代为 store.Store**

业务真实需求里有第三类数据 **Checkpointer 和 Memory 都装不下**：
- 用户偏好（"喜欢深色主题 / 中文回答"）—— 跨 session 跨 thread 共享
- 长期事实记忆（mem0 风格的语义事实库）—— 跨 thread 累积
- 多 agent 共享知识库 —— 跨 agent 跨 session 黑板
- 跨 thread 幂等记录 —— 防止 action 重复执行

Store 是 crew-go 的**第三个持久化抽象**，对应 LangGraph 的 `BaseStore`、专门承接"跨 thread / 跨用户的长期 KV 数据"。

## 0. 30 秒速览

```go
import (
    "crew-go/agent_v2"
    "crew-go/store"
)

// 1. 创建 store 实例（内置 InMemoryStore / redis.NewStore / sql.NewStore(PG+MySQL)；mem0 等业务自实现）
myStore := store.NewInMemoryStore()

// 2. 挂到 Agent
ag := agent_v2.NewBasicAgent(
    agent_v2.WithName("assistant"),
    agent_v2.WithModel(m),
    agent_v2.WithStore(myStore),  // 注入到 agent ctx
)

// 3. 业务节点 / hook / tool 里从 ctx 拿 store
//    （此处变量名换成 st 避免跟外层 myStore shadow）
st := store.FromContext(ctx)
err := st.Put(ctx, store.NS("users", userID, "preferences"), "theme",
    map[string]any{"mode": "dark"})

item, err := st.Get(ctx, store.NS("users", userID, "preferences"), "theme")
if item != nil {
    println(item.Value.(map[string]any)["mode"])  // "dark"
}

// 4. 想要"框架自动注入长期记忆"用 helper
ag := agent_v2.NewBasicAgent(
    agent_v2.WithStore(myStore),
    agent_v2.WithStoreMemory(store.NS("users", userID, "memories"),
        &agent_v2.StoreMemoryConfig{SearchLimit: 5}),
)
// 替代旧 Memory 接口——preamble 自动 Search 注入 + postamble 自动 Put 保存
```

## 1. Store / HistoryStore / Checkpointer / RunSession.values / Channel state 边界

crew-go 有 **5 层 state 抽象**（`RunSession` 的 Events 与 values 生命周期不同，表里分两行
列，共 6 行）。决定数据放哪一层，按下面的顺序问：

1. "数据要不要跨 sessionID（换 thread / 换用户）看到？" — **是** → **Store**
2. "要留的是**这场会话说过的话**（跨 Run 回放 / 喂给模型当历史）？" — **是** →
   **HistoryStore**（`WithHistory`，opt-in，append-only，见 [ISSUE-98](../issue/issue-98-conversation-history.md)）
3. "数据是不是**这一次 Run 中断后续跑**要用的执行状态？" — **是** → **Checkpointer**
   （框架自动）。⚠️ 它**不是**跨 Run 累积的地方：Run 干净跑完 `cleanupOnSuccess` 就把 cp
   清了，只有中断/失败才留档供 Resume。
4. "数据是不是 sub-agent 间共享、生命周期跟 sess 实例 (不走 cp)？" — **是** → **RunSession.values**
5. 其余 = **Channel state**（框架管的当前 Run 内对话上下文）

| 抽象 | 作用域 | 谁管理 | 典型数据 | 持久化 |
|---|---|---|---|---|
| Channel state (`compose_v2.Channels`) | 单 Run 内 | 框架自动 | 当前对话的 messages / tool_call / frontier | **随 cp.Channels 存档供本次 Run 的 Resume 续跑**；Run 成功即随 cp 一起清，**不跨 Run 累积** |
| `RunSession.Events` | 单 Run 内（caller 持引用可跨 Run 复用 sess 累积——**换 `NextTask` 不清空**）| 业务 / 框架混管 (AddEvent/RestoreEvents) | 审计事件流 (OutputEvent/ChunkEvent/ExitEvent) | **不走 cp**——业务自管 `RestoreEvents` 跨 Run（`Checkpoint` 结构无 events 字段）|
| **`RunSession.values`** (`SetValue`/`GetValue`) | **单 sess 实例（half-shared child sess 跨 sub-agent 共享）** | **业务显式 SetValue/GetValue** | **sub-agent 间 sticky 状态** (e.g. 业务 `user_id` 注入承载点 / Plan 当前 step / Routing 路径标记) | **不走 cp**——生命周期跟 RunSession 实例; 业务复用 sess 跨 Run 累积（**`NextTask` 换 RunID 不动它**）, 新 sess = 新空 map (跟 eino runSession.Values 走 cp 序列化的设计不同, 见 §1.2) |
| `compose_v2.Checkpointer` | 单 sessionID 的**一次 Run**（成功即清）| 框架自动 | channel state 快照 + frontier + pending interrupts + 子图嵌套 | 业务接 InMemory/Redis/PG |
| **`agent_v2.HistoryStore`** | **一场会话（`HistoryKey{Owner, ConvID}`）跨任意多次 Run** | **框架自动写（opt-in `WithHistory`）/ 业务读** | **这场对话逐字说过的话**（用户输入 / LLM 产出 / 工具往返）| **内置 InMemory 与 SQL（`sql.NewHistoryStore`，SQLite 跑过 conformance）；Redis 等业务自研——append-only** |
| **`store.Store`** | **跨 thread / 跨用户** | **业务显式 Get/Put/Search** | **用户偏好 / 长期事实 / 共享黑板 / 幂等记录** | **业务接 InMemory/Redis/PG/mem0** |

### 1.1 RunSession.values vs Store 关键差异（混用反模式）

对应 eino 4 个 ctx helper (`GetSessionValue`/`AddSessionValue`/`AddSessionValues`/`GetSessionValues`)——**生命周期跟 RunSession 实例, 不走 cp**。业务复用同一 sess 实例跨 Run 累积仍可见; 但典型新对话 / HITL Resume 场景都是新 sess 实例 (新空 `values` map), **跟上次 Run 完全无关**。

❌ **错（常见反模式）**：用 `RunSession.SetValue` 存"用户偏好"等跨 Run 数据
```go
// 错: Run 1 set, Run 2 GetValue 拿不到 (新 sess 空 values map)
sess1.SetValue("user.theme", "dark")
// ... Run 1 结束 ...
sess2 := agent_v2.NewRunSession()  // 新 sess, values 空
theme, ok := sess2.GetValue("user.theme")  // ok=false, theme=nil
```

✅ **对**：跨 Run 持久化走 `store.Store`
```go
st.Put(ctx, store.NS("users", uid, "preferences"), "theme", "dark")
// 任意新 Run 都可 store.Get 拿到
```

✅ **对**：sub-agent 间 sticky 状态走 `RunSession.SetValue`（**仅本 Run 内**）
```go
// PlanAgent 内 sub-agent A 设状态, sub-agent B 读 — 本 Run 内 sticky
sess.SetValue("plan.currentStep", 3)
// sub-agent B
step, _ := sess.GetValue("plan.currentStep")
```

**判定决策树**：

```
数据要跨 Run 看到吗?
├── 是 → 走 store.Store (业务 Get/Put/Search)
└── 否 → 数据要跨 sub-agent 共享吗?
        ├── 是 → 走 RunSession.SetValue/GetValue (业务 KV)
        └── 否 → 走 Channel state (框架管的对话上下文,
                  业务 NodeFunc 通过 return ChannelWrite 让框架在合并点 reduce,
                  **不直接 mutate** — 见 usage-guide.md §8 反模式)
```

### 1.2 设计参考

- `RunSession.values` 对应 eino 4 个 ctx helper (`adk/runctx.go:91-128`): `GetSessionValue` / `AddSessionValue` / `AddSessionValues` (批量) / `GetSessionValues` (拿全 map). 我们只暴露 `SetValue` / `GetValue` 单 KV method, 批量场景业务自循环
- **eino vs crew-go 跨 Run 持久化策略差异** (核 `eino/adk/runctx.go:31` `// runSession CheckpointSchema: persisted via serialization.RunCtx (gob)` + `interrupt.go:199` loadCheckPoint):
  - **eino**: `runSession.Values` 走 cp gob 序列化 — 跨 Resume 同 sid 自动恢复. **无独立跨 thread Store** (跨 thread 业务自管)
  - **crew-go**: `RunSession.values` **不走 cp** (cp `Checkpoint.Channels` 不含 values 字段) — 业务复用同 sess 实例跨 Run 累积, 但跨 sid 完全独立 + 跨 thread 走独立 `store.Store`
  - 设计选择差异: eino 把 values 当"状态一部分"塞 cp; crew-go 把 values 当"业务侧 sticky KV" 跟 cp 解耦, 跨 thread 数据走专用 Store 后端 (Redis/PG)
- LangGraph 等价: `langgraph.State` (per-Run channel state) vs `BaseStore` (跨 thread 长期记忆). State 跟 Store 接口独立, 但执行时 node 可同时拿到 (`compile(checkpointer=..., store=...)` 同时挂载, node 通过 `Runtime` / `InjectedStore` 拿 store). crew-go 同款 — `WithCheckpointer` + `WithStore` 同时挂, NodeFunc 内 `store.FromContext(ctx)` 取

**Store 不是 Checkpointer 的替代**——两者并存。`compile(checkpointer=..., store=...)` 同时挂载（同 LangGraph）。事务一致性**业务自负**（Store 写不参与 cp 事务）。

## 2. 命名约定：ns vs namespace 双重防错

crew-go 已有 `ns string` 概念用在 `Checkpointer`/Graph 子系统 —— 图执行嵌套位置寻址（`a/b#0/inner` 路径风格，框架自动派生）。

**Store 引入的是另一个维度的概念** —— 业务数据分类层级（`["users", "alice", "preferences"]`，业务显式构造）。两者**不能混淆**。

防错措施靠 Go 类型系统三层：

| 层 | Checkpointer | Store |
|---|---|---|
| **参数名** | `ns` 缩写 | `namespace` 全拼 |
| **类型** | `string` | `store.Namespace`（new type `[]string`）|
| **包路径** | `compose_v2.JoinNS` / `cp.SaveNS(...)` | `store.NS(...)` / `st.Get(ctx, namespace, key)` |

业务把 ns 误传给 store 编译器拦截：

```go
ns := compose_v2.JoinNS("parent", "child")   // ns 是 string
st.Get(ctx, ns, "theme")                     // 编译错误：cannot use string as store.Namespace
```

> **设计决策**：Checkpointer 不重命名 ns→checkpointNS。LangGraph 用 `checkpoint_ns` + `namespace` 双命名主要是 Python 动态类型无法编译期隔离；Go 走另一条路——**类型 + 包路径**双层（编译期隔离误传 + 读代码时 `compose_v2.JoinNS` vs `store.NS` 包前缀做可读区分）。参数名 `ns`/`namespace` 差异是辅助。完整论证见 `../description/three-way-comparison/store.md` 决策记录表。

## 3. 核心接口

### 3.1 Namespace 类型

```go
package store

// Namespace 是业务数据分类的层级路径，区别于 compose_v2 的 ns string（图运行时寻址）。
//
// **new type 而非 type alias**——
//   `type Namespace []string`  ← 我们选这个（编译期类型隔离）
//   `type Namespace = []string` ← 没选（alias 会让 []string 跟 Namespace 自由互转，丢防错能力）
// 编译器隔离业务避免把字符串 ns 误传成数据 namespace。
type Namespace []string

// NS 是 Namespace 构造 helper —— 调用方便利，**不做合法性校验**。
//
//   store.NS("users", userID, "preferences")
//   等价于 store.Namespace{"users", userID, "preferences"}
func NS(parts ...string) Namespace { return Namespace(parts) }

// Validate 显式校验 namespace 合法性。
// 业务一般不直接调——每个 Store 方法（Get/Put/Delete/Search/ListNamespaces）入口
// 自动校验入参 namespace，违反返 ErrInvalidNamespace。
func (n Namespace) Validate() error

// IsPrefixOf 判断本 namespace 是否为 other 的前缀。
//   NS("users").IsPrefixOf(NS("users","alice")) == true
func (n Namespace) IsPrefixOf(other Namespace) bool

// Equal 完全相等比较。
func (n Namespace) Equal(other Namespace) bool

// String 返回点分形式作日志用："users.alice.preferences"
func (n Namespace) String() string
```

**校验规则**（每个 Store 方法入口统一校验；`NS()` 构造时**不**校验，让用户能用 `Namespace.Validate()` 显式检测）：
- 非空——空 `Namespace{}` 返 `ErrEmptyNamespace`（独立 sentinel）
- 每段必须非空字符串、不能含 `.` 字符（`String()` 用作分隔符）——段格式错返 `ErrInvalidNamespace`
- 每段不能含 NUL 字节 `\x00`（InMemoryStore 内部 bucket key 用 NUL 作分隔符——不禁会让 `NS("a\x00b","c")` 与 `NS("a","b\x00c")` 共享 bucket 互覆）——同返 `ErrInvalidNamespace`（`store.go:71`）

**仅 `Search`** 额外禁止空 prefix（不允许全 store 扫描，空 `namespacePrefix` 返 `ErrEmptyNamespace`）—— 见 §10 反模式 3。`ListNamespaces` 的 prefix **可选**（`WithListPrefix` 不传即列全部 namespace）。

> **跟 LangGraph 对照**：LangGraph 禁止 `namespace[0] == "langgraph"` 严格相等检查（[`base/__init__.py:1272-1275`](../../../langgraph/libs/checkpoint/langgraph/store/base/__init__.py)——只看第一段，不是字符串前缀，`("langgraph_x", ...)` 在 LangGraph 是合法的）。我们暂不做此约束（业务自己避免冲突即可）。

### 3.2 Item 与 SearchItem

```go
// Item 是 Store 的存储单元——key-value 加创建/更新时间元数据。
type Item struct {
    Namespace Namespace
    Key       string
    Value     any       // 业务任意 JSON 兼容值（map/slice/string/number/bool/nil）
    CreatedAt time.Time
    UpdatedAt time.Time
}

// SearchItem 是 Search 返回元素——继承 Item 加可选 Score（语义搜索分数，未开或未参与打分时为 0）
type SearchItem struct {
    Item
    Score float64  // 相似度。0 只表示「没有正相关性」，不区分「没开语义搜索」/「没能参与打分」/「打了分但正交」
}
```

**`Value any` 约束**：必须 JSON 可序列化（业务存到 Redis/PG 后端时框架要序列化）。InMemoryStore 阶段不强制校验，业务自负；接 PG/Redis 后端时违反会失败。

### 3.3 Store 接口

```go
type Store interface {
    // Get 精确读取单条。不存在返 (nil, nil) —— nil item 表示无此 key，
    // 不是 error 路径。业务用 `if item == nil` 判断。
    // 完整错误集见 §3.5（典型 ErrInvalidNamespace）。
    Get(ctx context.Context, namespace Namespace, key string) (*Item, error)

    // Put 写入或覆盖。同 namespace+key 再次 Put 覆盖旧值，UpdatedAt 自动更新。
    // 支持 PutOption（含 TTL，PoC 阶段 InMemoryStore 调 WithTTL 返 ErrTTLNotSupported）。
    // value 为 nil 等价 Delete（对齐 LangGraph 语义）。
    // 完整错误集见 §3.5（典型 ErrInvalidNamespace / ErrTTLNotSupported）。
    Put(ctx context.Context, namespace Namespace, key string, value any,
        opts ...PutOption) error

    // Delete 删除单条。key 不存在不报错（幂等）。
    // 完整错误集见 §3.5（典型 ErrInvalidNamespace）。
    Delete(ctx context.Context, namespace Namespace, key string) error

    // Search 在 namespacePrefix 下查询。
    //   - 必须指定 prefix（不允许空 namespace 全局扫描，空返 ErrEmptyNamespace）
    //   - 支持 filter 精确匹配 + 比较操作符
    //   - 语义搜索是可选 capability：配了 IndexConfig 的实现按相关性排；没配就忽略 query
    //   - 完整错误集见 §3.5（典型 ErrInvalidNamespace / ErrEmptyNamespace /
    //     ErrUnsupportedFilterOp / ErrFilterTypeMismatch）
    Search(ctx context.Context, namespacePrefix Namespace,
        opts ...SearchOption) ([]*SearchItem, error)

    // ListNamespaces 列出 store 中所有 namespace 路径。
    //   - prefix/suffix 路径过滤（option 不合法返 ErrInvalidNamespace）
    //   - maxDepth 截断返回深度（仅返回前 N 段）
    //   - 完整错误集见 §3.5
    ListNamespaces(ctx context.Context, opts ...ListOption) ([]Namespace, error)

    // Batch 批量执行 ops，按入参顺序返回 Result。
    //   - InMemoryStore：per-Op 错误隔离 best-effort（某 Op 失败 Result.Err 非 nil
    //     其他 Op 仍执行，区别于 LangGraph InMemoryStore "任一异常整 batch fail"）
    //   - 这是所有后端唯一合法的语义：别做成单事务原子写（conformance 钉着）；Store 不提供事务
    Batch(ctx context.Context, ops []Op) ([]Result, error)
}

// TTLAware 是可选 interface assertion——业务想感知 store 是否支持 TTL 用：
//
//   if ta, ok := st.(store.TTLAware); ok && ta.SupportsTTL() {
//       // 走 WithTTL 路径
//   }
//
// InMemoryStore + SQL 后端不实现 TTLAware；Redis 后端实现并返 true。
type TTLAware interface {
    SupportsTTL() bool
}
```

**5 + 1 设计原则**：5 个高层方法是业务 99% 调用入口（Get/Put/Delete/Search/ListNamespaces）；`Batch` 是高级 API，用于批量操作（per-Op best-effort，**不是事务**）。`TTLAware` 是后端可选能力的 introspection 入口，不计入核心 interface。

### 3.4 构造函数

```go
// NewInMemoryStore 构造内存 store——PoC / unit test fixture 用。
// 进程重启丢失；并发安全（sync.RWMutex）；不实现 TTLAware。
func NewInMemoryStore() *InMemoryStore

// crew-go/sql.NewStore (Stage 2 已落地)
//   NewStore(db *sql.DB, dialect Dialect, opts ...StoreOption) *sql.Store
//   // PG + MySQL via Dialect, 业务自管 driver + 连接池
//   // 不实现 TTLAware (PoC 不做 expires_at 列)

// crew-go/redis.NewStore (Stage 2.5 已落地)
//   NewStore(rdb *redis.Client, opts ...StoreOption) *redis.Store
//   // 纯原语 + Lua + redis.NewScript EvalSha
//   // 实现 TTLAware (Redis 原生 EXPIRE)
//   // 不支持 Redis Cluster (type 收窄 *redis.Client, 见 redis/doc.go)

// 后续 backend (业务自实现, 跑 store/conformance 验证):
//   NewMem0Store(opts ...Mem0Option) (*Mem0Store, error)
//      // 业务自实现 wrap mem0 SDK 适配 store.Store 接口 (语义搜索)
```

### 3.4.1 跨 backend capability 矩阵

业务"SDK 自选 backend"承诺 — 三 backend 都过 conformance, 但部分 capability 是
backend opt-in. 业务跨 backend 切换前**必查**此矩阵:

| Capability | InMem | sql (PG/MySQL) | redis |
|---|---|---|---|
| Get / Put / Delete 基础 KV | ✅ | ✅ | ✅ |
| Search 6 比较 op (`$eq/$ne/$gt/$gte/$lt/$lte`) | ✅ | ✅ | ✅ |
| Search WithFilter 顶级 key exact match | ✅ | ✅ | ✅ |
| Search 范围 op 打在类型不匹配字段（type bracketing：那一行不命中、不报错） | ✅ | ✅（PG `CASE`+`jsonb_typeof` / MySQL `JSON_TYPE` / SQLite `json_type` 守卫，PG 16 与 MySQL 8 真库 conformance 全绿） | ✅ |
| Search `$eq` / 直值是 JSON 语义相等（10 == 10.0、true != 1、数组逐元素**不是包含**） | ✅ | ✅（PG `->'f' = $1::jsonb` / MySQL `= CAST(? AS JSON)` / SQLite 按 JSON 类别分路） | ✅ |
| Search WithQuery 语义搜索 | ✅ 配 `WithIndex(&IndexConfig{Embedder})` 后（不配=忽略 query） | ❌ | ❌ (业务接 mem0 / 向量后端) |
| Search filter 嵌套 (`{"a": {"b": ...}}`) | ❌ | ❌ | ❌ (业务自实现 backend) |
| key / namespace 长度 | 无限 | **key 1020 / ns 2048 字节**（MySQL 建表列宽 VARBINARY，按最宽的 4 字节字符算是 255 / 512 字符，即 `Store.Put` 说的可移植上限；两列同进主键贴着 InnoDB 3072 字节上限；PG / SQLite 实际无限） | 无限 |
| namespace / key 逐字节是身份（只差尾随空格、大小写、重音或组合字符写法的两个也是两份数据） | ✅ | ✅（MySQL 身份列用 VARBINARY——`utf8mb4_bin` 忽略尾随空格、默认的 `utf8mb4_0900_ai_ci` 还不区分大小写和重音，都会把它们当成同一行；PG / SQLite 本来逐字节；真 MySQL 8.0.35 conformance 钉住） | ✅ |
| ListNamespaces 次序 = `Namespace.String()` 字节序 | ✅ | ✅（PG 身份列声明 `COLLATE "C"`——不声明时跟库的排序规则走，ICU en-US 库里 `o.a` 排在 `o.B` 前；真 PG 16.9 的 C 库与 ICU 库 conformance 钉住） | ✅ |
| ListNamespaces WithListPrefix | ✅ | ✅ | ✅ |
| ListNamespaces **WithListSuffix** | ✅ | ❌ (`ErrUnsupportedFilterOp`) | ✅ |
| ListNamespaces WithListMaxDepth | ✅ | ✅ | ✅ |
| Batch (per-Op error 隔离 best-effort) | ✅ | ✅ | ✅ |
| **TTLAware** (`WithTTL` + 自动 EXPIRE) | ❌ | ❌ (PoC 不做 expires_at) | ✅ (Redis EXPIRE) |
| filter value 含 `json.Number`（业务 `json.Decoder.UseNumber()` 路径） | ✅ | ✅（`numericArg` 先按 int64 绑参保精度、再退 float64；2026-09-04 起） | ✅ |
| 跨进程 wire (`MarshalCheckpoint` round-trip) | ✅ (单进程) | ✅ | ✅ |
| 持久化 (重启不丢) | ❌ (进程内存) | ✅ (disk fsync) | ⚠️ (业务必配 `appendonly yes`) |

**关键漂移点** (业务跨 backend 真踩坑):

- **WithListSuffix**: sql 走 `ErrUnsupportedFilterOp` fail-fast (PoC 限制), InMem/redis
  实现段级后缀匹配. 业务从 InMem 开发 → 部署 sql 时业务 ListNamespaces with Suffix
  突然 fail. Phase 2 sql 实现 `LIKE '%suffix'` 后对齐.
- **sql 业务自管 schema**: 不调 `Setup`、自己建表时, 身份列 (store 的 ns / data_key, cp
  的 sid / ns / parent_ns / graph_id) 必须按字节比较与排序——PG 声明 `COLLATE "C"`, MySQL
  用 VARBINARY. 漏了的话 MySQL 上逐字节不同的 namespace / key / 会话落进同一行 (`utf8mb4_bin`
  忽略尾随空格, 默认的 `utf8mb4_0900_ai_ci` 还不区分大小写和重音; 互相覆盖、删一个带走另一个),
  PG 上 ListNamespaces 的次序与分页窗口跟着库的排序规则变, 都 err == nil.
- **TTL**: 业务 `WithTTL` 在 InMem/sql 返 `ErrTTLNotSupported`, redis 走 EXPIRE.
  业务用 `if ta, ok := st.(store.TTLAware); ok && ta.SupportsTTL()` 显式判断
  跨 backend 兼容.
- **持久化**: Redis 默认 `appendonly no` 业务 crash 丢 1h cp. 必读 `redis/doc.go`
  "Redis 持久化配置" 章节.

### 3.5 sentinel error 速查表

业务 `errors.Is(err, store.ErrXxx)` 时用此清单——所有 Store 实现统一遵守：

| Error 常量 | 触发场景 | 业务期望响应 |
|---|---|---|
| `ErrInvalidNamespace` | `namespace` 含空段 / 含 `.` / 非空但格式错（vs LangGraph 多一条 `namespace[0] == "langgraph"` 严格相等检测保留前缀，crew-go 暂不实施）| fail-fast（业务 bug）|
| `ErrEmptyNamespace` | `Search` 入参空 `namespacePrefix` 想全局扫（`ListNamespaces` 空 prefix 合法，列全部）；或 `Namespace{}` 空校验 | fail-fast（业务 bug）|
| `ErrTTLNotSupported` | 业务调 `Put` 传 `WithTTL` 但 store 不实现 `TTLAware`（如 PoC 阶段 InMemoryStore）| 降级走非 TTL 路径 / 切到支持的后端 |
| `ErrStoreNotConfigured` | `WithStoreMemory` helper 内部 hook 调 `store.FromContext(ctx)` 拿到 nil（业务忘 `WithStore`）| fail-fast（业务忘配 WithStore）|
| `ErrUnsupportedFilterOp` | `Search` 的 `filter` 含不支持 op（如 `$or`/`$nor`）| fail-fast（业务 filter 写错）|
| `ErrFilterTypeMismatch` | `$gt`/`$gte`/`$lt`/`$lte` 的 **filter 值**不是数值（`{"age": {"$gt": "abc"}}`）——静态判定、空 namespace 也报（`store.ValidateFilter`）。**存的值**类型对不上**不是错误**：那一行不命中（type bracketing，2026-09-03 拍板，三个内置后端一致；理由与真库实测见 [three-way store.md](../description/three-way-comparison/store.md)「filter 类型不匹配语义」）| fail-fast（业务 filter 写错）|
| `ErrNegativeOffset` | `WithOffset` / `WithListOffset` 给了负数——Search 与 ListNamespaces 入口判（`store.ValidateOffset`，各后端共用这一份；曾经 in-memory 切片越界 panic、SQLite 当 0、PG / MySQL 各报各的错）| fail-fast（调用方算错了页码）|

PoC InMemoryStore 实现以上全部 sentinel；后端实现可加自己的（如 `ErrRedisConnRefused`）但**必须先返此层 sentinel 再 wrap**（业务 `errors.Is` 不被打破）。

### 3.6 Option 类型

```go
// PutOption 配置 Put 行为。
type PutOption func(*putConfig)

// WithTTL 设置本条 Item 的生命期。
// 后端不支持 TTL 时返 ErrTTLNotSupported（InMemoryStore PoC 阶段返此错误）。
func WithTTL(d time.Duration) PutOption

// SearchOption 配置 Search 行为。
type SearchOption func(*searchConfig)

// WithFilter 精确匹配 + 操作符过滤。
//
//   filter map 语法（MongoDB 风格子集）：
//     {"key": value}              // 精确匹配（JSON 语义相等，同 $eq）
//     {"key": {"$eq": value}}     // 显式 $eq
//     {"key": {"$ne": value}}     // 字段存在且不等（JSON null 算存在，缺失不命中）
//     {"key": {"$gt": 10}}        // $gt/$gte/$lt/$lte——filter 值必须是数值
//     {"k1": v1, "k2": v2}        // 多键 AND（不支持 $or）
//
//   语义（所有内置后端一致，conformance 逐格钉；权威说明见 store.MatchFilter）：
//     相等是 JSON 语义（10 == 10.0，true != 1，"10" != 10，数组逐元素相等不是包含）；
//     范围 op 是 type bracketing——存的值不是数值或字段缺失 ⟹ 这一行不命中、不报错；
//     范围 op 的 filter 值非数值 ⟹ ErrFilterTypeMismatch（静态，空 namespace 也报）；
//     不支持的 op ⟹ ErrUnsupportedFilterOp，顶级形态 {"$or": [...]} 同样拒
//     （代价：顶级 key 不能以 $ 开头）。
func WithFilter(filter map[string]any) SearchOption

// WithLimit / WithOffset 分页。Limit 默认 10，Offset 默认 0。
func WithLimit(n int) SearchOption
func WithOffset(n int) SearchOption

// WithQuery 是语义搜索查询字符串。
// PoC InMemoryStore 忽略；接 mem0/向量数据库后端时生效。
func WithQuery(q string) SearchOption

// ListOption 配置 ListNamespaces。
type ListOption func(*listConfig)

func WithListPrefix(prefix Namespace) ListOption
func WithListSuffix(suffix Namespace) ListOption
func WithListMaxDepth(depth int) ListOption  // 截断返回路径深度
func WithListLimit(n int) ListOption          // 默认 100
func WithListOffset(n int) ListOption         // 默认 0
```

### 3.7 Op 与 Result（Batch 用）

```go
// Op 是 Batch 操作单元——4 个具体类型实现此 sealed interface。
type Op interface{ opMarker() }

type GetOp struct{ Namespace Namespace; Key string }
type PutOp struct {
    Namespace Namespace
    Key       string
    Value     any   // value=nil 表示删除（对齐 LangGraph 语义）
    TTL       time.Duration
}
type SearchOp struct {
    NamespacePrefix Namespace
    Filter          map[string]any
    Query           string
    Limit, Offset   int
}
type ListNamespacesOp struct {
    Prefix, Suffix Namespace
    MaxDepth       int
    Limit, Offset  int
}

// Result 是 Op 的返回——按 ops 入参顺序对应。
type Result struct {
    Item       *Item        // GetOp 返回
    Items      []*SearchItem // SearchOp 返回
    Namespaces []Namespace   // ListNamespacesOp 返回
    Err        error         // 单 Op 失败不中断 Batch（InMemoryStore 行为）
}
```

> **跟 LangGraph 对照**：LangGraph 用 NamedTuple 严格定义 Op；我们用 struct 实现 sealed interface（Op 有未导出 marker 方法防外部实现），等价语义。

## 4. ctx 注入协议

跟 `compose_v2.WithCheckpointerCtx` 完全对称的设计（参见 [compose_v2/checkpointer.go](../../compose_v2/checkpointer.go) 的 `WithCheckpointerCtx` / `GetCheckpointer` 函数）：

```go
package store

// WithContext 注入 store 到 ctx——business-facing helper。
//
// 业务（如 agent_v2.WithStore option 实现）想让下游 Run 调用从 ctx 取 store。
// typed nil 防护：业务 `var nilSt *MyStore; WithContext(ctx, nilSt)` 时
// nilSt != nil 但 underlying nil → 后续 st.Get() 远 stack panic.
// isFrameworkNil 协议对齐 compose_v2.WithCheckpointerCtx audit¹¹ HIGH-2 fix.
func WithContext(ctx context.Context, st Store) context.Context

// FromContext 从 ctx 取 store——业务节点 / hook / tool 调用。
// 未注入时返 nil（业务自己 nil check 决定降级行为）。
func FromContext(ctx context.Context) Store
```

**调用风格统一**：

```go
// Checkpointer
ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
cp := compose_v2.GetCheckpointer(ctx)

// Store（同款）
ctx = store.WithContext(ctx, st)
st := store.FromContext(ctx)

// Session（已有同款）
sess := agent_v2.GetSession(ctx)
```

## 5. Agent option：WithStore

```go
package agent_v2

// WithStore 配置 BasicAgent 的 store 实例。
//
// 内部实现：把 store 实例存进 Config.Store；BasicAgent.Run 入口 push 进 ctx
// 让所有下游 modelFn / toolsFn 节点、业务 hook、业务 tool 通过
// store.FromContext(ctx) 取到。
//
// typed nil 防护对齐其他 With option（audit⁶ HIGH-2 fix 协议六复证）。
func WithStore(st store.Store) Option
```

**接入位置**（已按设计落地，PR 3.3 commit `e6505f9`）：

- `WithStore` 是 **BasicAgent option** 直接消费——business 直接传给 BasicAgent
- 8 编排 wrapper（Sequential / Parallel / Loop / Routing / Reflect / Handoff / Plan / Graph）**不接 `WithStore`** —— 业务给底层 BasicAgent 传 `WithStore`，wrapper 不重复声明 option
- 业务想让"整个嵌套树共享 store"的标准做法：**在 Run 入口 `store.WithContext(ctx, st)` 显式 push 进 ctx**——所有嵌套 sub-agent 自动继承（同 `OrchHooks` ctx 透传模式 [options.go:55](../../agent_v2/options.go:55)）

```go
// 推荐模式：业务在 Run 入口 push 一次，整树共享
ctx = store.WithContext(ctx, myStore)
events, err := topAgent.Run(ctx, sess, input)  // 所有嵌套 sub-agent 都能 store.FromContext(ctx)
```

## 6. WithStoreMemory helper：替代 Memory 接口

`agent_v2.Memory` 接口及其实现（`InMemoryMemory` / `MemoryConfig` / `WrapMemory` / `SpecMemoryInterface` / `specMemoryAdapter`）**已删除**（PR 3.3 commit `e6505f9`，`agent_v2/memory.go` 整文件移除）—— 项目未上线无兼容包袱。

> **历史迁移参考表**：下表保留作为旧 Memory 用户迁 StoreMemory 的语义对照——所有 `memory.go:X` link 为已删文件锚点（参考用，点击 404）。当前实现位置：`agent_v2/basic_agent.go::buildInitialMessages` 内 `if a.config.StoreMemory != nil` 分支（preamble Search）+ `agent_v2/basic_agent.go::commitFinal` 内 `if a.config.StoreMemory != nil` 分支（postamble Put，写入者维追加在 ns 后面）+ `agent_v2/autocompact.go` 内 `StoreInjectedAnnotation` 消费。

**等价 Memory 行为的 4 个差异点**（业务从 Memory 迁 StoreMemory 时注意）：

| 现有 Memory 接口能力 | WithStoreMemory 处理 |
|---|---|
| `Memory.Read` 可抛 `InterruptError` 做业务级 HITL gate（旧 `memory.go:80-82` ISSUE-74 修后）| ✅ 保留——helper 注入逻辑作为 `buildInitialMessages` 子步执行（见 §13 议题 1 方向 A 落地），InterruptError 透传到 preamble 节点，框架自动 SaveNS cp 让 Resume 时重跑 |
| `WithSubAgentMemoryScope` / `IsSubAgentMemoryScope` ctx 协议（旧 `memory.go:12-50`）—— 防双重持久化 | ⚠️ 已改名 `WithSubAgentStoreScope` / `IsSubAgentStoreScope`，4 处派发点无条件 push（grep 看当前 push 点）。**但框架自己不再读它**——防重复写这件事改由「写入者维进 ns」解决（顶层写基准 ns、成员写 `ns/位置`，本来就不重复），那道「sub-agent 直接跳过」的闸让成员的 `WithStoreMemory` 完全失效，已删。flag 仍可供业务读，别拿它整个 skip |
| `MemoryInjectedAnnotation` 跨层 marker（旧 `basic_agent.go` buildInitialMessages 内设置 + `autocompact.go` 多处消费）—— AutoCompact / ReactiveCompact / emergencyDrop 跳过保护 | ✅ 保留并改名 `StoreInjectedAnnotation`；helper preamble 注入消息时打此 marker；autocompact.go 3 处消费同步改名 |
| `MemoryConfig.MaxReadMessages` 限制返回条数 | ✅ 改名 `StoreMemoryConfig.SearchLimit`——**语义略变**：旧 `MaxReadMessages` 截断的是 message 数（旧 `InMemoryMemory.Read` 直接返 messages 数组）；新 `SearchLimit` 是 `store.Search` 返 Item 数。**1:1 等价**仅当每个 Item.Value 恰好是一条 `schema.Message`；业务 Value 含 `{"content": "...", "metadata": {...}}` 这种富结构时单 Item 可能远大于一条 message——**业务迁移时实测典型 query 返 Item 总 token 数再校准** |

> **`WithStoreMemory` 跟 `WithStore` 必须配对**：helper 内部 preamble hook 调 `store.FromContext(ctx)`——业务**必须**同 BasicAgent 已注册 `WithStore`，否则框架 fail-fast 返 `ErrStoreNotConfigured`。
>
> **`StoreMemoryConfig` 命名说明**：缩写自 "Store-backed Memory's Config"，不是 "Store [verb] Memory"——godoc 顶部会注明缩写来源避免英文读者歧义。

替代方案：`WithStoreMemory` 高层 helper，把"自动注入长期记忆"行为编译成 preamble + postamble hook 组合。业务有两种选择：

**选择 A：高层用法（等价旧 Memory 行为）**

```go
ag := agent_v2.NewBasicAgent(
    agent_v2.WithStore(st),
    agent_v2.WithStoreMemory(
        store.NS("users", userID, "memories"),
        &agent_v2.StoreMemoryConfig{
            SearchLimit: 5,
            // 默认行为：preamble 调 st.Search(query=lastUserInput) 注入到 messages 头
            //          postamble 调 st.Put 保存本轮 final assistant message
            //          （key={RunID}_assistant_{N}，**只存单条 assistant**，非 user/assistant 对）
        }),
)
```

**选择 B：低层用法（完全自定义）**

业务不用 helper，自己写 hook 调 store：

```go
ag := agent_v2.NewBasicAgent(
    agent_v2.WithStore(st),
    agent_v2.WithHooks(myHooks{}),  // hook 内自己 store.FromContext + Search/Put
)
```

**StoreMemoryConfig 字段**（PoC 范围）：

```go
type StoreMemoryConfig struct {
    // SearchLimit 是 store.Search 返回的 Item 数上限，默认 5。
    //
    // **⚠️ 业务从旧 MemoryConfig.MaxReadMessages 迁移注意**：
    //   旧字段是 prompt 注入的 message 数；新字段是 store Item 数。
    //   仅当每 Item.Value 恰好是一条 schema.Message 时 1:1 等价。
    //   业务 Value 含 `{"content": "...", "metadata": {...}}` 富结构时
    //   单 Item 可能远大于一条 message——必须实测典型 query 返
    //   Item 总 token 数再校准 SearchLimit，避免 prompt 撑爆触发压缩。
    SearchLimit int

    // Filter 是 store.Search 的可选 filter（MongoDB 风格子集）。
    Filter      map[string]any

    // 后续可加 WriteFilter / QueryExtractor 等业务自定义点
}
```

**4 层压缩第 4 层文档**：从"SessionMemory"改名"StoreMemory" —— 行为不变，底层换成 Store。

## 7. 业务用法：4 个典型场景

### 场景 1：用户偏好持久化

```go
import (
    "context"
    "fmt"
    "time"

    "crew-go/store"
)

// 注入到 system prompt 让 LLM 知道用户偏好
func loadPreferences(ctx context.Context, userID string) (string, error) {
    st := store.FromContext(ctx)
    item, err := st.Get(ctx, store.NS("users", userID, "preferences"), "language")
    if err != nil {
        return "", fmt.Errorf("load preferences: %w", err)
    }
    if item == nil {
        return "默认中文", nil  // 无偏好记录走默认值
    }
    // 业务自负 Value schema 一致性——生产代码加双返回值断言：
    //   prefs, ok := item.Value.(map[string]any); if !ok { ... }
    //   lang, ok := prefs["lang"].(string); if !ok { ... }
    prefs := item.Value.(map[string]any)
    return fmt.Sprintf("用户偏好语言：%s", prefs["lang"]), nil
}

// 检测到用户偏好后保存
func savePreference(ctx context.Context, userID string, lang string) error {
    st := store.FromContext(ctx)
    return st.Put(ctx, store.NS("users", userID, "preferences"), "language",
        map[string]any{"lang": lang, "updated": time.Now().Format(time.RFC3339)})
}
```

**何时调、从哪调**：读偏好在每次 Run 的 preamble（只跑首轮）——最省事是
`agent_v2.WithPromptTemplate(tmpl, varsFn)`：框架渲染模板时调 `varsFn(ctx, input)`，在里面调
`loadPreferences` 把偏好填进 system prompt（ctx 已由 Run 入口按 `WithStore` 注入，`store.FromContext`
拿得到）；写偏好把 `savePreference` 包成业务工具（`tool.NewFuncTool("set_language", ...)`，LLM 识别到
偏好时自己调，工具 ctx 同样拿得到 store），或在 `hook.Hooks.OnToolEnd` 里按工具结果写。非结构化的
长期记忆别走这条手写路，用 §6 的 `WithStoreMemory`。

> **业务约定 user_id**：LangGraph framework 仅识别 3 个 user-facing configurable key——`thread_id`（[`_constants.py:49`](../../../langgraph/libs/langgraph/langgraph/_internal/_constants.py)）/ `checkpoint_id`（`:53`）/ `checkpoint_ns`（`:55`）；`user_id` **是业务约定不在 framework**——业务通过 `context_schema` typed dataclass 或 `configurable["user_id"]` 自负。我们 `RunSession.SetValue("user_id", ...)` 同款业务自负，未走 framework 内置。
>
> **ns 分隔符跨工具迁移注意**：LangGraph cp ns 用 `|` 层级 + `:` task 维度（[`_constants.py:75-77`](../../../langgraph/libs/langgraph/langgraph/_internal/_constants.py) `NS_SEP = "|"` / `NS_END = ":"`）；crew-go cp ns 用 `/` 层级 + `#` fan-out（[`compose_v2/ns.go`](../../compose_v2/ns.go)）。**两套格式不兼容**——LangGraph 业务迁来时 cp ns 字符串需改写。Store namespace 用 `[]string` 走 Go 类型不受此约束。

### 场景 2：长期记忆 + 语义检索（接 mem0 后端）

```go
import (
    "context"
    "fmt"

    "crew-go/agent_v2"
    "crew-go/hook"
    "crew-go/schema"
    "crew-go/store"
)

// 业务自己写 hook 控制读写时机
type mem0Hooks struct{ hook.NoOpHooks }

// OnModelStart 签名见 hook/hooks.go: (ctx, info *AgentInfo, input *schema.ModelInput) (context.Context, error)
//
// **PR 3.3+ 推荐路径**：用 agent_v2.WithStoreMemory(ns, &StoreMemoryConfig{
// SearchLimit: 5, Filter: ...}) helper option 替代下面手写 hook——helper 把
// preamble Search 注入 + postamble Put 编译进 buildInitialMessages 子步，业务零
// 代码接入。下面的 hook 范式仅供需要**完全自定义** query 提取 / 注入位置 / 跨
// store 协同的高级场景参考。
func (h *mem0Hooks) OnModelStart(ctx context.Context,
    info *hook.AgentInfo, input *schema.ModelInput) (context.Context, error) {

    st := store.FromContext(ctx)
    sess := agent_v2.GetSession(ctx)
    // 业务自己在 Run 入口 SetValue 注入 user_id；RunSession 不内置 UserID 字段
    // GetValue 返 (any, bool) —— 此处第二个返回值用 ok 判断更安全
    userIDAny, ok := sess.GetValue("user_id")
    if !ok { return ctx, nil } // 无 user_id 跳过注入但 Run 继续
    userID, ok := userIDAny.(string)
    if !ok { return ctx, fmt.Errorf("user_id 类型非 string: %T", userIDAny) }
    lastUser := extractLastUserMsg(input.Messages)  // 业务自实现 helper：遍历 input.Messages 倒序找 Role == schema.RoleUser 的 Content

    items, _ := st.Search(ctx,
        store.NS("users", userID, "memories"),
        store.WithQuery(lastUser),     // mem0 后端把 query 拿去做 embedding 检索
        store.WithLimit(5))

    // 拼到 messages 前（业务直接改 input.Messages）
    for _, it := range items {
        input.Messages = append([]*schema.Message{
            schema.SystemMessage("相关记忆：" + it.Value.(string)),
        }, input.Messages...)
    }
    return ctx, nil
}
```

> **mem0 接 Store 后端无现成参考实现**——LangGraph 官方仓与 mem0 SDK 都不互相 wrapper。
> 业务自己写 `Mem0Store` 实现 `store.Store` 接口（`Put` 调 `mem0.add`，`Search` 调 `mem0.search`，user_id 映射由 ns 拆解）。具体 schema 约定（key/value 结构、user_id 怎么映射）待第二期 mem0 集成 RFC 定。

### 场景 3：多 agent 共享黑板

```go
import (
    "context"
    "time"

    "crew-go/store"

    "github.com/google/uuid" // 任意 uuid 库都行
)

// Agent A 写入研究发现
func (a *researcher) onFinding(ctx context.Context, finding string) error {
    st := store.FromContext(ctx)
    return st.Put(ctx, store.NS("shared", "knowledge"), uuid.New().String(),
        map[string]any{
            "content":   finding,
            "agent":     "researcher",
            "timestamp": time.Now().Unix(),
        })
}

// Agent B 读出
func (a *writer) loadKnowledge(ctx context.Context) []string {
    st := store.FromContext(ctx)
    items, _ := st.Search(ctx, store.NS("shared", "knowledge"),
        store.WithLimit(10),
        store.WithFilter(map[string]any{
            "timestamp": map[string]any{"$gt": time.Now().Add(-24*time.Hour).Unix()},
        }))
    out := make([]string, 0, len(items))
    for _, it := range items {
        // 业务自负 Value schema 一致性——生产代码加双返回值断言：
        //   m, ok := it.Value.(map[string]any); if !ok { continue }
        //   c, ok := m["content"].(string); if !ok { continue }
        out = append(out, it.Value.(map[string]any)["content"].(string))
    }
    return out
}
```

### 场景 4：跨 thread 幂等记录

```go
import (
    "context"

    "crew-go/store"
)

// 注意：本示例非并发安全——两并发调用同时 Get 都 nil 都会执行 action 都 Put（last-write-wins）。
// 见 §10 陷阱 2。生产场景按需加：sync.Mutex（进程内）/ store value 加版本字段（跨进程乐观锁）。
func executeOnce(ctx context.Context, actionID string) (any, error) {
    st := store.FromContext(ctx)
    if it, _ := st.Get(ctx, store.NS("actions", "completed"), actionID); it != nil {
        return it.Value, nil  // 已执行过返历史结果
    }
    result, err := doExpensiveAction()
    if err != nil { return nil, err }
    _ = st.Put(ctx, store.NS("actions", "completed"), actionID, result)
    return result, nil
}
```

## 7.5 业务自实现 backend 跑 conformance 套验证

业务接 Redis/PG/MySQL/mem0 后端实现 `store.Store` 接口后, **必须**跑 conformance
套验证语义合规 — 否则跨 backend 行为漂移会在生产暴露 (HITL Resume / SaaS 数据
隔离等关键路径).

```go
import "crew-go/store/conformance"

func TestMyRedisStore_Conformance(t *testing.T) {
    conformance.RunStoreTests(t, func() store.Store {
        st := mybackend.NewRedisStore("redis://localhost:6379")
        t.Cleanup(func() { _ = st.Close() })
        return st
    })
}
```

**覆盖 45+ subtest** (8 轮 audit 累计加固), 含:
- Get/Put/Delete/Search/ListNamespaces/Batch **6 interface method** 全行为（+ Namespace.Validate 独立校验组）
- **namespace 校验** (空 / 含 `.` / NUL byte / 空段拒绝)
- **跨 namespace 隔离** (sibling prefix false-positive — 业务 PG `LIKE 'users%'`
  误命中 `users_admin` 经典坑)
- **Search 6 比较 op** ($eq/$ne/$gt/$gte/$lt/$lte) + **filter 语义三条**（范围 op 的
  filter 值非数值 ⟹ `ErrFilterTypeMismatch`、空 namespace 也报 / N=2 异构 fixture 下范围 op
  只返数值行、`$lt 10` 只返 n5——钉死「CAST 成 0」「true 当 1」「null 最低」三种反命中 / `$eq`·`$ne`·直值
  是 JSON 语义相等且 `$ne` 含 JSON null 行不含缺失行）+ unsupported op 哨兵。**客户端过滤
  后端直接用 `store.ValidateFilter` + `store.MatchFilter`**（唯一实现，别自己抄一份）
- **TTL 可选 capability** — 业务 backend 不实现 TTLAware 时自动 skip 走
  `ErrTTLNotSupported` 路径
- **并发安全** — barrier 强制 N goroutine 同瞬间释放, race detector 抓 race
- **时区协议** — 强制 UTC location (业务 PG `TIMESTAMP` 无 TZ 错时区被抓)
- **Value 类型** primitive (string/bool) strict equal + numeric/composite 容忍

**conformance 不覆盖** (业务自负):
- 语义搜索 (Search WithQuery) — 业务 mem0/向量后端自测 query 准确性
- 跨进程 wire format — 业务 backend serializer round-trip 自测
- 性能 / 吞吐 — 业务自压测

## 8. InMemoryStore 实现要点

```go
type InMemoryStore struct {
    mu sync.RWMutex
    // data[joinedNS][key] = *Item
    // joinedNS = strings.Join(namespace, "\x00")  ← NUL 字节分隔
    data map[string]map[string]*Item
}
```

**关键设计**：
- namespace 内部 key 用 `strings.Join(ns, "\x00")` 编码——NUL 字节是 Go string 合法字符但业务 namespace 段实际不含（用户名/UUID/标识符）。**Validate 强制禁 NUL 段**避免 `NS("a\x00b","c")` 跟 `NS("a","b\x00c")` 共享 bucket 的安全 bug（PR 1 audit 视角 3 HIGH-1 修复）。Go map key 不能用 slice，必须 join 成 string
- `sync.RWMutex` 保护读写 —— Get/Search/ListNamespaces 走 RLock，Put/Delete 走 Lock
- **不实现 `TTLAware`**——业务调 Put + `WithTTL` 返 `ErrTTLNotSupported` 让业务显式感知；业务想感知能力前 type-assert `(store.TTLAware)`
- `Batch` 顺序执行 ops —— **per-Op 错误隔离**（失败 Op `Result.Err` 非 nil，其他 Op 仍执行）。**区别于 LangGraph InMemoryStore**（任一 Op 异常整 batch 抛异常退出）——crew-go 选择更宽容的部分失败语义，对幂等批量场景（多条 actionID 标记完成）更友好。best-effort 是所有后端唯一合法的语义；Store 契约不提供事务；需要原子性只能绕开 Store 直接操作底层数据库
- `Batch` **非原子**：Op 间隙其他 goroutine 可见中间态（每 Op 各自抢锁，区别于 LangGraph 整 batch 单锁）。业务想要 snapshot 语义得自己加锁，Store 契约不提供
- `Search` filter 解析走纯 Go 递归 map 比较（无外部依赖）
- **Get / Search 返 copy**：outer field（Namespace / Key / Value 字段替换）不影响 store。**Namespace 额外深 copy**（`[]string` 便宜）——防业务 `got.Namespace[i]=x` 元素写穿透污染 store（N2 修复；原 shallow copy 只复制 slice header 不防元素写）。**Value 不拷贝**：它是 `Put` 时归一化出来的那份（值的 JSON 形状，见 `store.NormalizeValue`），不是业务传进来的对象——业务 Put 之后再改自己的对象碰不到它；Get / Search 每次返回它，就地改返回值会改到存储里，当只读用

**已知限制**（PoC 接受）：
- 大量 key（10K+ 同 namespace）下 Search 线性扫描慢 → 业务用更细 namespace 切分
- 无持久化 —— 进程重启丢失 → 生产用 Redis/PG 后端
- 无并发版本控制 —— 并发写同 key last-write-wins → 业务自加版本字段
- Get / Search 返回的 Value 是存储里那份 —— 就地改它会改到存储里，当只读用（Put 之后改业务自己的对象不受影响）

## 9. 第一版 PoC vs 后续版本

### PoC 范围（本 RFC 落地）

- `store` 新包：`Store` interface + `TTLAware` 可选 interface + `Namespace` + `Item` / `SearchItem` + 4 个 Op 类型 + Option 类型 + 错误集
- `InMemoryStore` 实现（含 Batch per-Op 错误隔离）
- `WithContext` / `FromContext` ctx 注入协议
- `agent_v2.WithStore` option（**BasicAgent option**，8 编排 wrapper 不重复接，业务给底层 BasicAgent 传）
- `agent_v2.WithStoreMemory` helper + `StoreMemoryConfig`（替代 Memory 接口）
- 删 `agent_v2.Memory` / `InMemoryMemory` / `MemoryConfig` / `WrapMemory` / `SpecMemoryInterface`
- 4 层压缩第 4 层文档改名 "StoreMemory"
- 测试：单包 unit test + 9 编排集成测试（验 Store ctx 透传）
- example：`example/store_user_preferences/` 标准用法
- 跨 doc 同步：[`migration-history.md`](migration-history.md) 加 Store API、[`usage-guide.md`](usage-guide.md) 加 Store 用法、[`context-compaction.md`](context-compaction.md) 第 4 层改 StoreMemory

### PoC **不**包含（后续版本按需补）

- ✅ **后续已补**：语义搜索 seam（`store.Embedder` + `IndexConfig`，`InMemoryStore` 配 `WithIndex` 后真做余弦 + max pooling）。框架只做 seam 不绑供应商；接 mem0 / 向量库当后端仍是另一条独立的路
- ✅ **后续已补** TTL：`redis/` Store 实现（Redis 原生 `EXPIRE`）；`InMemoryStore` + `sql/` 不实现 `TTLAware`、`WithTTL` 返 `ErrTTLNotSupported`（SQL 待 Phase 2 加 `expires_at` 列）
- ✅ **后续已补** 生产后端：`redis/`（Store + Checkpointer）+ `sql/`（PG + MySQL，Store + Checkpointer）独立 module 已落地（conformance + 测试过）；`Mem0Store` 仍业务自 wrap（mem0 SDK 实现 `store.Store` 接口）
- ❌ async 双版本接口（Go 用 goroutine 调即可，不需要 sync/async 双套）
- ❌ Batch 原子事务（best-effort 是所有后端唯一合法的语义，2026-09-07 `d6d06312` 定；Store 契约不提供事务；需要原子性只能绕开 Store 直接操作底层数据库）
- ❌ 并发版本控制 / 乐观锁（业务自加版本字段）
- ❌ TTL 后台清理 goroutine

### 后续版本路线（参考）

| 版本 | 增量 | 触发条件 |
|---|---|---|
| v2 | `RedisStore` + TTL 实现 | 生产部署需求 |
| v3 | `Mem0Store`（语义搜索后端）+ IndexConfig | 业务接 mem0 长期记忆 |
| v4 | `PostgresStore` + pgvector | 同 store 一站式支持 KV + 语义搜索 |
| v5+ | 乐观锁 / 后台清理（Batch 原子事务已定不做，见上） | 高并发生产场景 |

## 10. 反模式与陷阱

### ❌ 反模式 1：用 Store 存当次对话的中间数据

```go
// ❌ 错：当次对话的工具中间结果不需跨 thread
st.Put(ctx, store.NS("temp"), "currentStep", "searching")

// ✅ 对：用 channel state 或 RunSession.SetValue
sess.SetValue("currentStep", "searching")
```

### ❌ 反模式 2：扁平 namespace

```go
// ❌ 错：扁平 namespace 失去层级查询能力
st.Put(ctx, store.NS("data"), "user-alice-pref-theme", value)

// ✅ 对：分层 namespace 让 Search / ListNamespaces 可按前缀过滤
st.Put(ctx, store.NS("users", "alice", "preferences"), "theme", value)
```

### ❌ 反模式 3：全局扫描

```go
// ❌ 错：空 namespace 全局扫——所有 Store 实现统一返 ErrEmptyNamespace（接口层契约）
st.Search(ctx, store.NS(), store.WithLimit(100))

// ✅ 对：始终指定具体前缀
st.Search(ctx, store.NS("users", "alice"), store.WithLimit(100))
```

### ❌ 反模式 4：把 Store 当事务参与方

```go
// ❌ 错：期望 Store 写跟 cp 写一起 commit / rollback
// Store 不参与 cp 事务——业务自负一致性
```

`Batch(ops)` **不是**原子写入（per-Op best-effort 是唯一合法语义）；Store 契约不提供事务；需要原子性只能绕开 Store 直接操作底层数据库，跨 Store 和 Checkpointer 的事务同样**不支持**。

### ⚠️ 陷阱 1：value 必须 JSON 可序列化

```go
// ❌ 错：含闭包 / channel / 业务结构体不可 JSON
st.Put(ctx, ns, "k", map[string]any{
    "callback": func() {},          // 不可序列化
    "ch":       make(chan int),     // 不可序列化
})

// ✅ 对：JSON-friendly 数据
st.Put(ctx, ns, "k", map[string]any{
    "name": "alice", "age": 30, "tags": []string{"vip"},
})
```

InMemoryStore PoC 阶段不强校验，业务自负；接 Redis/PG 后端时违反会失败。

### ⚠️ 陷阱 2：并发写同 key last-write-wins

```go
// 并发场景下后写覆盖先写——无乐观锁
// 业务想做"自增计数"必须自加版本字段或外部加锁
```

### ⚠️ 陷阱 3：Search 是线性扫描（PoC InMemory）

```go
// 大量 key（10K+ 同 namespace）下 Search 慢
// 解决：细化 namespace 层级，或等 v2/v4 上 Redis/PG 后端
```

## 10.5 易混淆参数速查（SDK 使用者必读）

业务接入时**最常踩坑的 4 组参数**，集中放此节供查阅。各自详细背景见交叉引用。

> **编号为什么跳**：本节 #1/#3/#4/#9 与 [usage-guide.md §10.5](usage-guide.md) 的 #2/#5/#7/#10 是同一套跨文档
> 统一编号（按混淆类型归到各自的权威文档），单篇内不连号是有意的，别找「丢了的」。

### #1 `store.Namespace` vs `Checkpointer` ns（**重名陷阱**）

两个 ns 完全不同概念——**同一个项目里都叫 ns，但维度正交**：

| 维度 | `store.Namespace` | `Checkpointer` ns |
|---|---|---|
| Go 类型 | `Namespace []string`（new type，多段路径）| `string`（单串，内部 `/` 分隔）|
| 谁控制 | **业务**自定义路径 | **框架**自动 push/pop（嵌套 sub-agent 入口）|
| 典型值 | `("users", "alice", "memories")` | `"plan/worker_1"` |
| 寻址 key | `(Namespace, key)` 业务数据维度 | `(sessionID, ns)` graph 嵌套维度 |
| 跨 Run 共存 | ✅ 是（长期持久化跨 thread）| ❌ 否（cp 单次 Run 临时态，成功即清；跨 Run 会话历史走 agent_v2 历史层）|
| helper API | `store.NS(parts...)` | `compose_v2.WithGraphNS(ctx, JoinNS(parent, child))` |
| 内部分隔符 | `\x00` NUL byte（PR 1 安全 fix）| `/` 斜杠 |

**编译期防误传**（Go 类型系统）：业务把 cp ns（string）传给 `st.Get` 直接编译失败：

```go
ns := compose_v2.JoinNS("parent", "child")  // ns 是 string
st.Get(ctx, ns, "theme")                    // ❌ cannot use string as store.Namespace
```

详细论证见 [§2 命名约定](#2-命名约定ns-vs-namespace-双重防错)。

### #3 `WithStore` vs `WithStoreMemory`（**隐式依赖**）

两个 option 经常一起用，但容易只挂一个：

```go
// ❌ 错：只挂 WithStoreMemory 缺 WithStore——Run/Resume 入口 fail-fast
ag := agent_v2.NewBasicAgent(
    agent_v2.WithStoreMemory(store.NS("users", uid, "memories"), nil),  // ← 隐式要求有 store
)
// → Run 返 fmt.Errorf("WithStoreMemory: %w", store.ErrStoreNotConfigured)

// ✅ 对：两个一起挂
ag := agent_v2.NewBasicAgent(
    agent_v2.WithStore(st),                                                  // ← 必须先挂 store
    agent_v2.WithStoreMemory(store.NS("users", uid, "memories"), nil),       // 才能用 helper
)
```

| Option | 用途 | 必需性 |
|---|---|---|
| `WithStore(st)` | 挂 store 实例 + Run/Resume 入口自动 push ctx | 业务用 store 必挂 |
| `WithStoreMemory(ns, cfg)` | 编译成 preamble Search + postamble Put 自动 | 可选（不用 helper 时不挂）|

**核心约定**：`WithStoreMemory` 内部调 `store.FromContext(ctx)`——ctx 无 store 时 `buildInitialMessages` 返 `fmt.Errorf("WithStoreMemory: %w", store.ErrStoreNotConfigured)`（wrap 后 propagate，业务用 `errors.Is(err, store.ErrStoreNotConfigured)` 判定）不让远 stack 出错。详见 [§5 WithStore](#5-agent-optionwithstore) + [§6 WithStoreMemory helper](#6-withstorememory-helper替代-memory-接口)。

### #4 `WithSubAgentStoreScope` vs `IsSubAgentStoreScope`（**producer / consumer 协议**）

两个名字相似但角色完全不同：

| API | 角色 | 调用方 | 调用时机 |
|---|---|---|---|
| `WithSubAgentStoreScope(ctx, true)` | **producer**——push flag 进 ctx | 框架（**7 编排 sub-agent 入口** — Sequential/Parallel/Loop/Routing/Reflect/Handoff/Plan; BasicAgent ReAct 无 sub-agent / GraphAgent 走 `RunSubAgent` helper）| 进入 sub-agent 前 |
| `IsSubAgentStoreScope(ctx)` | **consumer**——查 flag | **仅**业务自定义 Store 实现的 `Put`（框架自己不读，见下方 ⚠️） | 准备 Put 前 |

**典型业务用法**（自定义 store backend 防双重持久化）：

```go
// 业务自实现 Store backend 需 import "crew-go/agent_v2" 用此 helper
// （依赖方向单向 agent_v2 → store，业务 backend 在自己的包反向 import 是合理的）
import (
    "crew-go/agent_v2"
    "crew-go/store"
)

// store.Store 是 6 method interface（Get/Put/Delete/Search/ListNamespaces/Batch
// + 可选 TTLAware）——这里只展示 Put，业务必须实现 6 method 全（完整见 §3.3）
func (s *MyMem0Store) Put(ctx context.Context, ns store.Namespace, key string, value any, opts ...store.PutOption) error {
    // 检 flag：这次调用发生在成员位置上 → 给它不同的待遇（这里是短 TTL）
    //
    // ⚠️ **别写成 `return nil` 整个跳过**。老版本的建议是「让顶层唯一写入」，前提是
    // 「成员和顶层会写到同一个键」——写入者维挪进 ns 之后前提没了（成员写 `ns/位置`、
    // 顶层写 `ns` 本身）。照老写法实现的后端会让成员的 WithStoreMemory 一条也落不了盘，
    // 且 Run.err=nil 静默。
    if agent_v2.IsSubAgentStoreScope(ctx) {
        return s.client.AddWithTTL(ns.String(), key, value, 24*time.Hour)
    }
    // value 类型约定：用 WithStoreMemory helper 时框架传的是**答复文本 string**
    // （2026-09-07 起；此前传整条 *schema.Message，而那时 in-memory 与做 JSON 往返的
    // 后端读回来的类型不一致 ⟹ 注入给 LLM 的东西跟着分叉、两边都 err == nil）。
    // 业务直接调 st.Put(...) 时业务自管 value 类型。业务自定义 backend 想精确处理时：
    //   if txt, ok := value.(string); ok { /* 框架写的长期记忆 */ } else { /* 业务自己的 */ }
    return s.client.Add(ns.String(), key, value)
}
```

**业务无须手动 push** `WithSubAgentStoreScope`——`agent_v2` 调用别的 agent 的每一处都已自动覆盖，**push 点收敛在 4 处**（`runMember` / `runDecisionAgent` / `runNestedCall`（`RunSubAgent` + `AgentAsTool` + SkillTool fork）/ Handoff 铸造，见 `agent_v2/member_dispatch.go`、`agent_v2/run_subagent.go` 与 `agent_v2/handoff_agent.go`）。**业务只在自实现 Store backend 时调 `IsSubAgentStoreScope` 消费 flag**。

> ⚠️ **业务自管图边界**：以下场景**不在那 4 处 push 点内**，框架不自动 push scope flag：
> 1. `GraphAgent` 用户自定义 `NodeFunc` 内调子 `agent.Run`
> 2. `agent_v2.NewBasicAgentBuilder(modelFn, toolsFn).Compile()` 拿到的裸 `*Graph` 自管 NodeFunc 调子 agent.Run
> 3. 业务直接 `tool.Tool.Call(ctx, ...)` 调普通工具、工具里再调子 `agent.Run`（直接调 `AgentAsTool` 不在此列：它走 `runNestedCall`，照常 push）
>
> 想让子 agent 走 sub-scope 协议时需业务侧显式 `subCtx := agent_v2.WithSubAgentStoreScope(ctx, true)` 后再调。详见 CLAUDE.md "GraphAgent 提示"段。

详见 [§6 WithStoreMemory helper](#6-withstorememory-helper替代-memory-接口) 内 "ISSUE-84 跨 ns 协议"段（§6 表第 2 行 + 后续说明）+ [`agent_v2/store_scope_ctx.go`](../../agent_v2/store_scope_ctx.go)。

### #9 `Put(value=nil)` vs `Delete`（**LangGraph 对齐 quirk**）

两种写法**语义等价**（都删除 key），但写代码时常有歧义：

```go
// 写法 A：显式 Delete（推荐——意图清晰）
st.Delete(ctx, store.NS("users", uid, "prefs"), "theme")

// 写法 B：Put(nil) 隐式删除（LangGraph BaseStore 对齐保留）
st.Put(ctx, store.NS("users", uid, "prefs"), "theme", nil)
```

两者都是**幂等**（key 不存在不报错）。设计上保留 `Put(nil)` 是为了：

1. **LangGraph 用户迁移友好**：[`langgraph BaseStore.put`](../../../langgraph/libs/checkpoint/langgraph/store/base/__init__.py) 同款语义
2. **Batch 操作便利**：`Op` 单元只用 `PutOp{value: nil}` 表达"upsert + 删除"两种语义，不引入额外 `DeleteOp`

**InMemoryStore 实现真删除**（不留 tombstone）；业务自实现后端**必须遵守 interface 契约**——`Put(nil)` 后 `Get` 必须返 `(nil, nil)`（同 `Delete` 后行为）。这里的 nil 是无类型的 nil：typed nil 指针 / slice / map、`json.RawMessage("null")`、`MarshalJSON` 返回 null 的值编成 JSON 是 null，是一个值——存一条 Value 为 nil 的记录，覆盖写时记录还在（判删除要用业务传进来的原值、在归一化之前判，conformance `Put_Value_Encoding_To_JSON_Null_Stores_Null`）。业务后端内部可选 hard delete 或 soft delete + tombstone，但 `Get` 对外行为不变（违反契约会让 helper 假设错位）。

详见 [§3.3 Store 接口](#33-store-接口) 的 `Put` godoc。

---

## 11. 跟 LangGraph BaseStore 差异速查

LangGraph 用户迁移友好——核心抽象一致，差异点列在此：

| 差异点 | LangGraph BaseStore | crew-go store.Store | 原因 |
|---|---|---|---|
| 接口基座 | `batch(ops)` + `abatch(ops)` 抽象，其余方法默认实现委托——共 **12 公开方法**：2 abstract + 5 sync default (get/search/put/delete/list_namespaces) + 5 async default (aget/asearch/aput/adelete/alist_namespaces) | 5 高层方法 + `Batch(ops)` + 可选 `TTLAware` interface | Go 不需要 sync/async 双套；业务 99% 直接调高层方法更自然 |
| namespace 类型 | `tuple[str, ...]` | `Namespace` (new type `[]string`) | Go 没原生 tuple |
| 注入机制——node 函数 | 按参数名 `store: BaseStore` 自动注入（LangGraph runtime 识别参数名） | ctx 注入（`store.FromContext`）| Go 无 runtime 参数名识别；ctx 注入跟 Checkpointer 模式对称 |
| 注入机制——tool 函数 | `Annotated[BaseStore, InjectedStore()]` annotation 注入（`InjectedToolArg` 子类）| ctx 注入（业务 tool 内调 `store.FromContext(ctx)`）| 同上 |
| Item 字段 | `value: dict[str, Any]`（顶层**必须 dict**，[base/__init__.py:69, 127, 864](../../../langgraph/libs/checkpoint/langgraph/store/base/__init__.py)）| `Value any`（允许 map/slice/string/number/bool/nil 任意 JSON-friendly value）| Go 不限定顶层类型业务自由；**业务迁移注意**：LangGraph→crew-go 无需改（dict 子集 OK）；crew-go→LangGraph 需 wrap 非 dict 值（如 `Put(ns, key, "raw-str")` 改 `Put(ns, key, map[string]any{"value": "raw-str"})`）|
| filter 语法 | MongoDB 风格 dict `{"k": {"$gt": v}}`——**仅 6 比较操作符**（`$eq/$ne/$gt/$gte/$lt/$lte`，[base/__init__.py:255-261](../../../langgraph/libs/checkpoint/langgraph/store/base/__init__.py)）；**无 `$or/$nor/$in/$nin/$and` 逻辑组合**| 同款（`map[string]any`，6 比较操作符同集合）| 语法完全一致，业务踩 `$or` 等 LangGraph 也不支持 |
| filter 类型不匹配语义 | **无契约**：InMemory `float(value)` 抛 ValueError、Postgres `->>` 文本字典序、Sqlite `CAST AS REAL` 把非数值当 0.0——三后端三种行为（issue #7684 开着） | **type bracketing**：存值类型对不上那一行不命中、不报错；`ErrFilterTypeMismatch` 只报 filter 值侧；三内置后端 conformance 逐格一致 | 我们多一条契约；LangGraph 用户迁来后 filter 结果**更可预测**（对照文档「filter 类型不匹配语义」节） |
| TTL 配置 | `TTLConfig` 类属性（`default_ttl: float \| None` / `refresh_on_read: bool`（默认 **True** 即刷新 TTL，`_ensure_refresh` `.get("refresh_on_read", True)` 兜底，[base/__init__.py:1284](../../../langgraph/libs/checkpoint/langgraph/store/base/__init__.py)）/ `sweep_interval_minutes: int \| None`）+ `BaseStore.supports_ttl: bool` 类属性；GetOp/SearchOp `refresh_ttl: bool = True` 默认；PostgresStore `start_ttl_sweeper()` / `stop_ttl_sweeper()` / `sweep_ttl()` 3 公开方法启 Python background thread sweep | `WithTTL(d) PutOption` + `TTLAware` 可选 interface；后端实现自决定后台清理机制 | Go 用 option 函数比类属性自然 |
| TTL refresh per-call | `refresh_ttl: bool \| None` 参数业务 per-call 控制 | PoC 未支持；预留后续按业务需求补 PutOption | 业务诉求未明，先做核心 |
| 语义搜索 | `IndexConfig{dims, embed, fields: list[str]\|None}` 编译时配置；fields 默认 `["$"]` 嵌入整个对象；路径语法 `field` / `parent.child` / `array[N]` / `array[-N]` / `array[*]` / `{f1,f2}` 多字段选择（[`embed.py:333-396`](../../../langgraph/libs/checkpoint/langgraph/store/base/embed.py)）| PoC 不内置，靠 mem0 后端实现 | 不强制业务装 embedding 依赖 |
| Search 空 namespace_prefix | 允许全店扫（`store.search(())` 是合法用法）| **fail-fast** 返 `ErrEmptyNamespace`（主动设计禁止）| crew-go 偏向编译期/运行期早错；LangGraph 偏向最大灵活度 |
| Batch 失败语义 | 任一 Op 异常 → 整 batch 抛异常退出（无 try/except）；实际 InMemoryStore put 是 dict assignment 几乎不抛，异常主要来自 search/embed 路径；**`AsyncBatchedBaseStore._dedupe_ops`** 同 (ns, key) 多次 PutOp 折叠 + 相同 GetOp 合并 | **InMemoryStore 选择 per-Op 错误隔离**（失败 Op `Result.Err` 非 nil，其他仍执行）；这是所有后端唯一合法的语义（2026-09-07 定，不再邀请事务化）；PoC 不实现去重（顺序执行） | crew-go 偏好幂等批量场景宽容性；去重等 v5+ 高并发场景再做 |
| TTL 单位 | LangGraph `TTLConfig.default_ttl: float` 是**分钟**；`PutOp.ttl: float` 也是分钟 | crew-go `WithTTL(d time.Duration)` 用 Go 原生时长，业务自填 `5*time.Minute` / `300*time.Second` 等显式单位 | 避免单位歧义 |
| 命名保留 | `namespace` 不能以 `"langgraph"` 开头 | 暂无保留前缀 | 业务自避免冲突 |

> **4 层压缩第 4 层兼容性提示**：旧 Memory 接口的"第 4 层 Session Memory"行为在 RFC 落地后**整体压缩流程不变**（仍是预读注入 + 压缩跨层 marker 跳过），但单条 message 数量跟旧 `MaxReadMessages` **不一定 1:1**（见 §6 表第 4 行）。压缩边界 token 阈值业务可能需重新校准——**这是 4 层第 4 层"行为不变（整体流程）+ 数字略变（具体条数）"的精确表述**，§6/§11 两处描述不矛盾。

## 12. 实施 checklist（PoC 落地）

按 CLAUDE.md "实施前 checklist" 三步，配合 R1 audit 揭露的完整影响范围：

### Step 1：新 store 包实现

- `store/` 新包：`Store` interface + `TTLAware` 可选 interface + `Namespace` + `Item` / `SearchItem` + 4 个 Op 类型 + Option 类型 + 错误集
- `InMemoryStore` 实现（含 Batch per-Op 错误隔离）
- `WithContext` / `FromContext` ctx 注入协议
- 单包 unit test 覆盖 Get/Put/Delete/Search/ListNamespaces/Batch/Namespace.Validate/TTLAware 否定路径

### Step 2：agent_v2 集成 + 删 Memory

**新增**：
- `agent_v2.WithStore(st store.Store) Option`
- `agent_v2.WithStoreMemory(ns store.Namespace, cfg *StoreMemoryConfig) Option`
- `agent_v2.StoreMemoryConfig`（含 `SearchLimit` / `WriterID` 等字段）
- `agent_v2.WithSubAgentStoreScope(ctx, bool)` / `IsSubAgentStoreScope(ctx)` ctx helpers（**改名自 `WithSubAgentMemoryScope`** —— 协议行为不变）
- `agent_v2.StoreInjectedAnnotation` 常量（**改名自 `MemoryInjectedAnnotation`** —— marker 行为不变）

**已删除**（PR 3.3 commit `e6505f9`）：
- `agent_v2/memory.go` 整文件（Memory / InMemoryMemory / MemoryConfig / WrapMemory / SpecMemoryInterface / specMemoryAdapter / WithSubAgentMemoryScope / IsSubAgentMemoryScope / MemoryInjectedAnnotation）—— 文件不复存在，旧链接失效
- `agent_v2/config.go::WithMemory` option —— 改为 `WithStore` + `WithStoreMemory`
- BasicAgent preamble 直接调 `Memory.Read` 的逻辑 → 已切到 `agent_v2/basic_agent.go::buildInitialMessages` 内 `if a.config.StoreMemory != nil` 分支调 `store.Search`
- BasicAgent postamble `Memory.Write` 调用 → 已切到 `agent_v2/basic_agent.go::commitFinal` 内调 `store.Put`

**改名已落地**（实测 **~26 个 .go 文件 + 16 个 md 文件全仓引用**改完）：
```
WithSubAgentMemoryScope → WithSubAgentStoreScope
IsSubAgentMemoryScope   → IsSubAgentStoreScope
  ↑ 定义位置：agent_v2/store_scope_ctx.go
  ↑ **4 处 push 点**（收敛进 `runMember` / `runDecisionAgent` / `runNestedCall`
    （RunSubAgent + AgentAsTool + SkillTool fork）/ Handoff 铸造——此前是 15 处散在
    各编排里，由注释里一句人肉计数维持，而 `5afb8eb` 那次加协议就漏了 2 处）：
    见 `grep -rn "WithSubAgentStoreScope" agent_v2/`
  ↑ **框架自己不读这个 flag**——防重复写改由「写入者维进 ns」解决，见上文 #4
```

### Step 3：文档全面同步

**`doc/v2/`**：
- [`migration-history.md`](migration-history.md) §"v2 核心类型速查" —— 删 Memory 行加 Store + WithStoreMemory 行
- [`context-compaction.md`](context-compaction.md) —— "Session Memory"/"SessionMemory" 全替为"Store Memory"/"StoreMemory"（**含 `SessionMemoryCompactProvider` 类型名一并改 `StoreMemoryCompactProvider`**）；MemoryInjectedAnnotation marker 引用 3 处改名
- [`usage-guide.md`](usage-guide.md) —— 在 §3 HITL 后插入新 §"Store 业务用法"（Get/Put/Search 速记 + ctx 注入 + WithStoreMemory helper 范式）
- [`orchestration-patterns.md`](orchestration-patterns.md) —— §2.3 BasicAgent options 表 `WithMemory(cfg)` 行改 `WithStoreMemory(...)` + §4.5 "ParallelAgent + Memory 协议" 改 "ParallelAgent + StoreMemory 协议" + §11 "横切能力：Memory / Skill / ..." 改 "StoreMemory / Skill / ..." + §11.1 "Memory（跨 Run 长期记忆）"改 "StoreMemory（跨 Run 长期记忆）"
- [`hitl-resume.md`](hitl-resume.md) —— Memory.Read HITL gate 描述改 StoreMemory hook
- `observability-implementation-guide.md`（后被 [`observability.md`](observability.md) 取代）—— Memory 引用同步改名
- 新建 [`store.md`](store.md)（**本文档**）

**`doc/description/`**（R3 audit 新增项——R1 漏列）：
- 新建 [`three-way-comparison/store.md`](../description/three-way-comparison/store.md)（4 方对照 + 决策记录）
- [`three-way-comparison/README.md`](../description/three-way-comparison/README.md) 文档清单加 `store.md` 行 + ⚠️ "设计稿 PoC 待实现" status
- [`three-way-comparison/address-routing.md`](../description/three-way-comparison/address-routing.md) 末尾补一句"业务数据 namespace 见 store.md"
- [`architecture.md`](../description/architecture.md) line 107 第 4 层 SessionMemory 表 + line 168 Memory 业务实现表 → 全替 StoreMemory（**总入口架构图**，影响最大）
- [`okr.md`](../description/okr.md) line 48/55/108 KR2.1/KR2.8/KR5.6 的 `WithMemory` / `InMemoryMemory` / `MemoryConfig` / SessionMemory 引用同步
- [`okr.md`](../description/okr.md) Phase 2/5 进度——历史 OKR 记录建议保留旧 API 名 + 追加注释 "(R3 后改名 Store/StoreMemory)"

**`CLAUDE.md`**（项目根，always-on 上下文最易腐败）：
- "核心类型速查"表（约 line 321-335）—— Memory/MemoryConfig/InMemoryMemory/SpecMemoryInterface/WrapMemory 行删 → 替为 Store/Namespace/Item/WithStoreMemory 行
- **line 332"跨层 marker 常量"行** —— `MemoryInjectedAnnotation` 改 `StoreInjectedAnnotation`
- **line 333"Memory.Write 跨 ns 协议"行 ISSUE-84 描述** —— `WithSubAgentMemoryScope` / `IsSubAgentMemoryScope` 改名
- **line 315 + 344 BasicAgent 描述** —— "preamble 跑 Memory/HistoryCompactor/Skill" + "postamble 跑 Memory.Write" 改 "StoreMemory"
- "4 层上下文压缩"表（约 line 380）—— 第 4 层"SessionMemory"改名"StoreMemory"；`WithMemory(MemoryConfig{...})` 例子改 `WithStoreMemory(...)`
- "项目特定约定" —— 加一行"Store 是跨 thread 长期 KV 抽象，跟 Checkpointer 并列"
- "doc 目录说明"表 —— doc/v2 行末追加" + store.md"

**`doc/sdk-guide.md`** —— line 33 标题 `## 上下文压缩(... / SessionMemory)` + line 49 代码示例 `agent_v2.WithMemory(memCfg)` 同步改名

**`doc/issue/`**（活协议描述）：
- [`issue-84-memory-cross-ns.md`](../issue/issue-84-memory-cross-ns.md) line 57/59/77/103 InMemoryMemory godoc 警示引用同步
- [`issue-79-agent-as-tool-child-sess.md`](../issue/issue-79-agent-as-tool-child-sess.md) line 111 "跨 Run Memory" 改 "跨 Run StoreMemory"
- [`issue-76-autocompact-memory-fallback-gap.md`](../issue/issue-76-autocompact-memory-fallback-gap.md) —— Memory fallback gap 描述同步
- [`issues.md`](../issue/issues.md) line 28 ISSUE-84 整行长描述（跟 CLAUDE.md line 333 双份拷贝）同步改名；line 2007-2008 ISSUE-12 历史描述保留旧名加注释

**`doc/review/review-v6.md`** —— 历次 review Memory 引用按当时设计描述保留旧名 + 末尾追加 "(后改名 Store/StoreMemory)" 注释

**`doc/code/todo.md`** —— line 30 待办 "新加 v2_memory_demo" 改 "新加 v2_store_demo"

### Step 4：example + 测试 (**历史 R3/R4 audit 计划版, 实施版按 PR 3.3 实际产出调整**)

> ⚠️ **状态注**: 本 § Step 4 是 R3 audit 时写的 **PR 3.3 实施计划草稿**, PR 3.3 commit `e6505f9` 落地后**实际产出与计划列表有偏差** — 下列 example 文件 + 测试文件名 rename 在 PR 3.3 实际产出**未全部跟更**. 业务读时按 `ls example/` + `ls agent_v2/*_test.go` 实际产出对照, **不要按本 § 列表当 live 链接点击** (含 \`[text\](path)\` 语法已校验**多数文件不存在**). 计划→实际差异保留作 PR audit 决策记录.

**example** (计划版, 多数未实施):
- ~~新增 `example/store_user_preferences/` 标准用法 demo~~ (未建)
- ~~删 `example/` 下用 `WithMemory(...)` 的 demo~~ (R3 grep 实测 0 个 .go 文件用 Memory API, doc/code/todo.md:30 当时 `v2_memory_demo` 未建待办, 现已改 `v2_store_demo`)

**测试文件改名** (计划版, R4 grep 校准但**多数文件不存在**, PR 3.3 实施时按实际 grep 结果调整):
- ~~`agent_v2/issue84_memory_cross_ns_test.go`~~ (不存在; PR 3.3 实际测试组织按 grep 实际产出)
- ~~`agent_v2/issue75_postamble_test.go`~~ (不存在)
- ~~`agent_v2/memory_wrap_test.go` rename `store_wrap_test.go`~~ (源文件不存在; PR 3.3 已删 Memory 接口测试)
- ~~`agent_v2/preamble_hitl_test.go`~~ (不存在)
- ~~`agent_v2/repro_round5_memory_test.go` rename `repro_round5_store_test.go`~~ (源不存在; `agent_v2/repro_round5_test.go` 存在但非 memory/store 命名)
- ~~`agent_v2/repro_round6_test.go` / `r30_remaining_test.go` / `skill_listing_test.go` / `r29_low1_autocompact_internal_test.go`~~ (按 PR 3.3 实施扫)
- ~~`agent_v2/r29_actor_low1_test.go` / `issue76_autocompact_memory_internal_test.go`~~ (按 PR 3.3 实施扫)

**集成测试加**：9 编排集成测试验 Store ctx 透传（BasicAgent option 经 `store.WithContext` push 后嵌套 sub-agent / hook / tool 都能 FromContext 取到）

### Step 5：audit

按 CLAUDE.md "审计纪律" 跑 R2/R3 多 agent 单视角并发审 + sanity-revert + cross-verify。预计还有 5-15 finding（doc-writing skill 经验值——多轮收敛）。

---

## 13. 已决议（实施落地说明）

R3 audit 找出 3 个**真设计 gap**——PR 3.3（commit `e6505f9`）按推荐方向落地，本节保留作为决策记录。

### 议题 1：`WithStoreMemory` preamble hook 与 `buildInitialMessages` 的衔接顺序

**问题**：现有 [`basic_agent.go::buildInitialMessages`](../../agent_v2/basic_agent.go) 内部严格顺序（Skill 预激活 → 拼 listingMsg pendingListingMsg → StoreMemory Search 注入并打 `StoreInjectedAnnotation` → prior 取出 → AddEvent listingMsg + AddEvent input）。`WithStoreMemory` helper preamble hook 在哪一步？

**两个方向**：
- **方向 A**：helper 直接**改写 `buildInitialMessages` 内部一段**——保留同节点 + pendingListingMsg AddEvent 推迟逻辑（不破坏 ISSUE-74/75 协议）；helper hook 是"配置驱动的 buildInitialMessages 子步"非独立节点
- **方向 B**：helper 注册为**外层独立 preamble hook**——`buildInitialMessages` 调完后 hook 再 fire；这种 Skill listing AddEvent 时机错位风险大（R14 audit HIGH-1 同病复发）

**已按方向 A 落地**（PR 3.3 commit `e6505f9`）——更少破坏，跟 ISSUE-74/75 协议兼容。落地位置在 `agent_v2/basic_agent.go::buildInitialMessages` 内 `if a.config.StoreMemory != nil` 分支（grep `store.Search` 看实际行号）。

**vs LangGraph**：不做对照。LangGraph hook 体系（`create_react_agent` + `pre_model_hook` / `post_model_hook`）整套 deprecated 迁移到 `langchain.agents.create_agent` + `AgentMiddleware`（本地无 langchain 源码），新 API 形态等独立 RFC 调研。方向 A 推荐理由是**兼容现有 BasicAgent ISSUE-74/75 preamble 协议**而非基于 LangGraph 模式同源。业务想要完全自定义走 §6 选择 B（`WithStore` + 自写 hook 低层用法）。

### 议题 2：helper hook 内部"store 怎么拿"

**问题**：业务 `agent_v2.NewBasicAgent(WithStore(st1), WithStoreMemory(...))` 构造时绑 `st1`；但 Run 入口 `ctx = store.WithContext(ctx, st2); ag.Run(ctx, ...)` 又传 `st2`。helper 内部 preamble hook 用 `st1` 还是 `st2`？

**两个方向**：
- **方向 A**（闭包持有）：hook 持有 `st1` 引用——`WithStore` 绑定期固定；业务 ctx push `st2` 无效（隐式陷阱）
- **方向 B**（动态 FromContext）：hook 每次调 `store.FromContext(ctx)`——支持 Run-time 换 store；fail-fast 返 `ErrStoreNotConfigured` 如 ctx 无 store

**已按方向 B 落地**（PR 3.3 commit `e6505f9`）——跟 §5 推荐模式"业务 Run 入口 push 一次整树共享"一致。`WithStore` 仅作默认值（Run 入口 `pushStoreCtx` 注入），Run-time `store.WithContext(ctx, customStore)` 可覆盖。helper 内（`buildInitialMessages` Search + `commitFinal` Put）调 `store.FromContext(ctx)` 动态拿。

**vs LangGraph**：**跟 LangGraph 完全一致**。LangGraph node 函数声明 `store: BaseStore` 后 framework 在每次 invoke 时**动态注入**（实现两步：[`_runnable.py:293-319`](../../../langgraph/libs/langgraph/langgraph/_internal/_runnable.py) 通过 `inspect.signature` 反射构建 `func_accepts` 字典；[`_runnable.py:349-372`](../../../langgraph/libs/langgraph/langgraph/_internal/_runnable.py) `RunnableCallable.invoke` 内 `getattr(runtime, runtime_key)`，`runtime_key="store"`）；按 6 个 Protocol 变体（`_RunnableWithStore` / `_RunnableWithWriterStore` / `_RunnableWithConfigStore` 等）匹配参数名 + 类型。

**Runtime vs ToolRuntime**：node 函数注入 `Runtime` dataclass（[`runtime.py:90`](../../../langgraph/libs/langgraph/langgraph/runtime.py) `class Runtime(Generic[ContextT])` + `:163-186` 字段）含 **6 字段**——context/store/stream_writer/previous/execution_info/server_info。tool 函数注入 `ToolRuntime`（[`tool_node.py:1561`](../../../langgraph/libs/prebuilt/langgraph/prebuilt/tool_node.py) `class ToolRuntime(_DirectlyInjectedToolArg)` + `:1620-1628` 字段）含 **9 字段**——继承 Runtime + 加 state/config/tools/tool_call_id 4 个 tool 维度。

**嵌套子图继承**：通过 `Runtime.merge`（[`runtime.py:188`](../../../langgraph/libs/langgraph/langgraph/runtime.py) `def merge`；[`:195`](../../../langgraph/libs/langgraph/langgraph/runtime.py) 实际表达式 `store=other.store or self.store`；调用点 sync path [`pregel/main.py:2675`](../../../langgraph/libs/langgraph/langgraph/pregel/main.py) + async path [`pregel/main.py:3067`](../../../langgraph/libs/langgraph/langgraph/pregel/main.py)）自动继承父图 store。我们方向 B = LangGraph 标准做法。

### 议题 3：`IsSubAgentStoreScope` 协议在直接 `store.Put` 路径的失效边界

**问题**：现有 `IsSubAgentMemoryScope` 只对**业务自定义 Memory 实现**有效——`Memory.Write` 接口内业务自己查 flag。改 Store 后业务可能不用 helper 而是直接 `st.Put(ctx, ns, key, value)`——Store 接口签名无"调用者是 sub-agent 还是 top"上下文，**框架无法在 Store.Put 层面挂 scope 检查**。

**结论**（已落实到 §6 表格 ⚠️ 行）：协议**仅对 helper postamble + 业务自定义 Memory 风格 Store 后端有效**；业务直接 `st.Put` 时框架不拦——业务自负 ns 设计避免重复（典型：sub-agent 用 `ns=("sub", agentName, ...)` 子分支）。

**已落地**（PR 3.3 commit `e6505f9`）：`agent_v2/basic_agent.go::commitFinal` 内 `if a.config.StoreMemory != nil && !IsSubAgentStoreScope(ctx)` 守卫——business 通过 `WithStoreMemory` 用 helper 自动获得 scope 防护。WithStoreMemory godoc 已注明业务直 Put 路径边界。

**vs LangGraph**：**业务自负 ns 设计跟 LangGraph 完全一致**——LangGraph 也没有 sub-agent scope 协议（`is_nested` flag 在 [`pregel/_loop.py:257`](../../../langgraph/libs/langgraph/langgraph/pregel/_loop.py) 但仅用于 checkpoint，**不暴露业务**；`BaseStore.put` 接口完全无 scope/level 参数）。LangGraph 标准做法是业务 ns 设计区分（supervisor 用 `("supervisor", task_id)` ns，sub-agent 用 `("sub_agent", agent_name, ...)` ns，多 agent 示例 LANGGRAPH_MULTI_AGENT.md 里 sub-agent **根本不写 store**，记忆只走 message channel）。

**crew-go 额外特色**：我们在 helper postamble + 业务自定义 Memory 风格 Store 后端保留 `IsSubAgentStoreScope` ctx 协议——业务用 helper 时享受框架级 scope 防护（LangGraph 没有）；业务直 `st.Put` 时降级跟 LangGraph 一致（业务自负 ns）。**双轨**：helper 用户得便利，低层 Store 用户跟 LangGraph 兼容。

---

## 14. 未决议题（实施时按业务反馈补完）

R3 audit 发现的 RFC 表面未覆盖项——非阻塞但实施前应补：

- **Op sealed interface 业务扩展性**：当前 `Op` 含未导出 marker 是 Go sealed pattern，业务**只能在 store 包内**实现新 Op type。业务想加自定义 Op（如 `CompactOp` / `MergeOp`）需 fork 框架或提 RFC 扩 Op 集合
- **Batch + Result 类型分发示例**：业务调 `Batch(ops)` 后遍历 `results` 时需按 `ops[i].(type)` type-switch 读对应 Result 字段（`Item` / `Items` / `Namespaces`）——RFC 缺示例，实施时 godoc 加完整 type-switch 示例
- **`Put(ns, key, nil)` 跟 `Delete` 语义对齐**：当前承诺 `value=nil` 等价 Delete；业务想存 nil 占位用 `map[string]any{}` 或 sentinel value——godoc 明示
- **filter 语法错误的错误集**：业务误传 `{"$or": ...}` 等不支持 op 时返什么？RFC 缺 `ErrUnsupportedFilterOp` / `ErrFilterTypeMismatch`——v2 起加 fail-fast，PoC InMemoryStore 可静默返空 results
- **`store.Search` 抛 `InterruptError` 的来源**：mem0 后端 SDK 不会主动抛 Interrupt，业务想做 HITL gate 需 helper 暴露 `WithStorePreSearchHook(ctx → error)` option 或业务自实现 Store 后端代抛——v2 看业务需求决定
- **`TTLAware` 业务感知协议范式**：业务想在调 `Put` 前判断 store 是否支持 TTL，用 type-assert 协议：
  ```go
  if ta, ok := st.(store.TTLAware); ok && ta.SupportsTTL() {
      _ = st.Put(ctx, ns, key, value, store.WithTTL(5*time.Minute))
  } else {
      _ = st.Put(ctx, ns, key, value)  // 走非 TTL 路径
  }
  ```
- **业务自定义 Memory 实现迁移路径**：业务原有 `MyMemory implements Memory` 实现两条迁移路径任选——
  - **路径 A**（推荐）：实现 `store.Store` 接口作 backend，挂 `WithStore(myStore)`；业务原 Read/Write 行为编入 backend 自定义 Get/Search/Put 逻辑
  - **路径 B**：完全不实现 Store，业务自写 hook 直接拼 messages 进 LLM input（绕过 helper）
- **健康检查 / Stat API**：v2+ 路线考虑加可选 interface `HealthChecker { Healthy(ctx) error }` + `Stater { Stat(ctx, ns) (count int, size int64, err error) }` 让 SRE 接 Prometheus / liveness probe；PoC InMemoryStore 不实现
- **故障降级策略**：Redis/PG 后端挂时业务该 fail-fast 还是 fallback——业务自定，典型 4 策略：
  - fail-fast：直接 return error 中断对话（用户偏好读不出来宁可不答）
  - fallback default：用预置默认值继续（多数业务推荐）
  - circuit breaker：连续 N 次 store error 后短期跳过 store 读
  - retry with timeout：单次 error 内部 retry 1-2 次
- **PoC 工程量估算**（**已落地，保留作历史记录**）：原估 ~26 .go 文件改 + 16 md 文档同步 + 11 测试改 + 2 文件 rename ≈ 3-5 PR / 5-8 人天为粗略量级；实际落地为 PR 1-3.3 共 9 commit（含 audit 收敛 / round-fix）跨 commits `408aba7..` 至最新 audit follow-up；后端实现（Redis/PG/Mem0）各自独立 module 各 3-5 人天为类似量级，待业务实施。
- **spec / modules 同步**：`modules/common` 若有 Memory 引用（按当时 CLAUDE.md「不修改公司 spec 但 modules/ 同步」的协议，2026-10-21 已废止），落地前 grep 确认；spec 侧 `SpecMemoryInterface` 是从 spec 拷贝 / 改名后是否需要同步 spec 提交 issue 评估
