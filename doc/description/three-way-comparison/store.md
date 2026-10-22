# 主题：跨 thread 长期数据 Store

> 状态：**已落地（PR 1-3.3，commits `408aba7..e6505f9`）** —— 完整接口设计与业务用法见 [`../../v2/store.md`](../../v2/store.md)
>
> 本目录文件命名"三方对照"——本主题实际**四方对照**，多了截图 Python 框架 `SessionStoreProtocol` 作引子。
>
> ⚠️ **LangGraph 对照范围说明**：本文档基于本地 langgraph v1.1.9 源码 grep 校准，**聚焦 BaseStore 接口 + 数据模型核心对照**（接口/Op/Item/namespace/TTL/IndexConfig/PG schema/Runtime.merge/InjectedStore 等），这部分稳定保留在 langgraph 主仓不受 prebuilt 迁移影响。**hook 体系（`create_react_agent` + `pre_model_hook` + AgentMiddleware）不做对照**——LangGraph prebuilt 整套已 deprecated 迁移到 `langchain.agents`（本地无 langchain 仓源码），新 API 等独立 RFC 调研。

## 触发场景

需要查阅本文档的设计决策：
- 决定数据放 Channel state / Checkpointer / Store 哪一层
- 新增 Store 接口签名 / 实现后端
- 评估"通用 KV"vs"按用途切多 store"的抽象粒度
- 决策 namespace 类型表达（slice / string / tuple）
- 设计 TTL / 语义搜索是否第一版做
- 业务问"长期记忆该怎么接 mem0"

## 四方对照速查表

| 维度 | crew-go store | LangGraph BaseStore | eino | 截图框架 SessionStoreProtocol |
|---|---|---|---|---|
| **定位** | 跨 thread 长期 KV | 跨 thread 长期 KV | 无（仅 CheckPointStore byte blob）| 按用途切 4 个 store |
| **接口数量** | 5 高层方法 + Batch（核心 6）+ 可选 `TTLAware` interface | 2 abstract（`batch` + `abatch`）+ 5×2 default impl（sync+async 双套：get/search/put/delete/list_namespaces）| 2 方法（Get/Set byte blob）| 16+ 方法（session/kv/messages/compacted 各 4）|
| **namespace 类型** | `Namespace` (new type `[]string`) | `tuple[str, ...]` | 无 | `str = ''` 默认空 |
| **namespace 约束** | 非空 / 段非空 / 不含 `.` | 非空 / 段是 str / 不含 `.` / 不以 `"langgraph"` 开头 | 无 | 无 |
| **layer 数据模型** | 单层 KV（业务任意 JSON value） | 单层 KV（dict value）| byte blob | 4 层独立（session/kv/messages/compacted）|
| **search 能力** | filter（MongoDB 风格子集）+ 预留语义 query | filter + 语义搜索（IndexConfig）| ❌ | ❌（按用途取 messages） |
| **list_namespaces** | prefix/suffix/maxDepth | `match_conditions: tuple[MatchCondition]`（prefix/suffix `path` 内含通配符 `*`）+ `max_depth` | ❌ | ❌ |
| **TTL** | 接口预留 `WithTTL` + `SupportsTTL`（PoC InMemoryStore 返错）| `TTLConfig` + `refresh_on_read` 三态 + 后端实现 | ❌ | ❌ |
| **注入业务节点机制** | ctx 注入（`store.FromContext`）+ Agent `WithStore` option | node 函数：typed Protocol 匹配（`_runnable.py:206-244` 共 **6 个** `_RunnableWith*` Protocol 变体覆盖 store/writer/config 组合）；node 级 `Runtime` dataclass **6 字段**（[runtime.py:90,163-186](../../../langgraph/libs/langgraph/langgraph/runtime.py)：context/store/stream_writer/previous/execution_info/server_info）；tool 函数：`InjectedStore(InjectedToolArg)` 注 store + `InjectedState` 注 state + langchain_core 的 `InjectedToolCallId` 注 tool_call_id + `ToolRuntime`（[tool_node.py:1560-1628](../../../langgraph/libs/prebuilt/langgraph/prebuilt/tool_node.py) dataclass **9 字段一站式注入**=state/context/config/stream_writer/tools/tool_call_id/store/execution_info/server_info）；`compile(store=...)` 编译时挂载 | 不适用 | 业务直接持有 store 实例 |
| **跟 Checkpointer 关系** | 并存，独立持久化层 | 并存，独立持久化层 | 仅 Checkpointer | 不含 cp 概念（只关心 session 产品语义）|
| **value 序列化** | JSON 兼容 any（业务自负）| JSON 可序列化 dict | byte 流 | bytes |
| **典型后端** | InMemoryStore + Redis + SQL 已落地 + mem0 预留（业务自实现） | InMemoryStore + PostgresStore（pgvector）| InMemory + 业务自定 | 业务自定 |

## crew-go 设计（PoC）

完整接口与业务用法见 [`../../v2/store.md`](../../v2/store.md)，此处仅速记关键决策：

- **5 高层方法 + Batch**：Get / Put / Delete / Search / ListNamespaces 是业务 99% 入口；Batch 留作高级 API
- **Namespace = new type `[]string`**：编译期防误传字符串 ns（区别 Checkpointer ns string）
- **ctx 注入跟 Checkpointer 对称**：`store.WithContext(ctx, st)` + `store.FromContext(ctx)`
- **删 Memory 接口**：项目未上线无兼容包袱；`WithStoreMemory` helper 替代"框架自动注入"语义
- **第一版不做**：语义搜索 / TTL 实现 / 生产后端 / async 双套接口（2026-09-18 现状：语义检索已有接口层 `8c4e440a`、不绑供应商，redis / sql 仍无向量能力；TTL 仅 redis 支持；生产后端 `redis/` `sql/` 已内置；async 双套接口仍不做）
- **TTL 接口预留**：`WithTTL(d) PutOption` + `TTLAware` 可选 interface（独立 assertion）。InMemoryStore 不实现 `TTLAware`，业务调 Put + `WithTTL` 返 `ErrTTLNotSupported`

## LangGraph 设计（v0.6.10）

源码：`/Users/gupengfei/program/go/src/langgraph/libs/checkpoint/langgraph/store/base/__init__.py`

### 接口基座

`BaseStore` 抽象方法仅 `batch(ops)` + `abatch(ops)`（[`__init__.py:724-746`](../../../../langgraph/libs/checkpoint/langgraph/store/base/__init__.py)）。其余 `get/search/put/delete/list_namespaces` 是 default-impl 方法，内部委托 `batch([Op])[0]`——实现者写 2 方法即可。

**`AsyncBatchedBaseStore` 子类**（[`base/batch.py:58`](../../../../langgraph/libs/checkpoint/langgraph/store/base/batch.py)）：用 `asyncio.Queue` + 后台 task 累积同 tick 多调用合并成 batch 调 `abatch`；自带 `_dedupe_ops`（[`batch.py:283-323`](../../../../langgraph/libs/checkpoint/langgraph/store/base/batch.py)）—— 同 `(ns, key)` 多次 PutOp 折叠为最后一次 / 相同 GetOp 合并指向 dedupped。业务高频调用同 key 自动去重。

业务级 4 个 Op 类型（NamedTuple）：
- `GetOp(namespace, key, refresh_ttl)` —— `refresh_ttl: bool = True` 默认
- `SearchOp(namespace_prefix, filter, limit, offset, query, refresh_ttl)`
- `PutOp(namespace, key, value, index, ttl)` —— `value=None` 表示删除
- `ListNamespacesOp(match_conditions, max_depth, limit, offset)`

### namespace

`tuple[str, ...]` —— 原生层级。`_validate_namespace`（[`base/__init__.py:1255-1276`](../../../../langgraph/libs/checkpoint/langgraph/store/base/__init__.py)）约束：
- 非空
- 每段是 str
- 段不含 `.`
- 段不能为空字符串
- **第一段不能等于** `"langgraph"`（`namespace[0] == "langgraph"` 严格相等检查，**不是字符串前缀**——`("langgraph_x", ...)` 是合法的）

`NamespacePath = tuple[str | Literal["*"], ...]`（[`base/__init__.py:311`](../../../../langgraph/libs/checkpoint/langgraph/store/base/__init__.py)）—— `list_namespaces` 的 `prefix` / `suffix` 参数都是 NamespacePath 类型，**任意位置含 `*` 通配符**（不只 `MatchCondition.path`）。

### IndexConfig 语义搜索

```python
{
    "dims": int,                       # 向量维度（如 1536）
    "embed": Embeddings | EmbeddingsFunc | AEmbeddingsFunc | str,
    "fields": list[str] | None         # 要索引的 JSON 路径，默认 ["$"] 嵌入整个 JSON 对象
}
```

`fields` 路径语法支持 `metadata.title` / `chapters[*].content` / `authors[0].name` / `{title, authors[0].name}` 多字段选择。`put` 时 `index` 参数三态：`None=用 IndexConfig.fields` / `False=不索引` / `list=覆盖`。

### TTL

```python
TTLConfig = {
    "refresh_on_read": bool,
    "default_ttl": float | None,
    "sweep_interval_minutes": int | None,
}
```

`BaseStore` 类属性：`supports_ttl: bool = False` + `ttl_config: TTLConfig | None = None`（[base/__init__.py:719-720](../../../langgraph/libs/checkpoint/langgraph/store/base/__init__.py)）；后端实现通过覆盖 `supports_ttl=True` 声明支持。

**Per-call refresh 控制**：Get / Search API 含 `refresh_ttl: bool | None = None` 参数（默认 None 兜底到 `ttl_config.refresh_on_read`）—— 业务可单次覆盖 TTL 刷新行为。`_ensure_refresh(ttl_config, refresh_ttl)` 在调用层做兜底。

**TTL 后台清理 API**（PostgresStore 实现）：`start_ttl_sweeper(sweep_interval_minutes)` 启 Python background thread 周期调 `sweep_ttl()` 删除 `expires_at < NOW()` 项；`stop_ttl_sweeper(timeout)` 关闭——业务自管 thread 生命周期。

### InjectedStore 注入（仅 tool 函数）

`class InjectedStore(InjectedToolArg)`（[`tool_node.py:1707`](../../../../langgraph/libs/prebuilt/langgraph/prebuilt/tool_node.py)）是给 **tool 函数** 注入 store 的 annotation：

```python
@tool
def my_tool(query: str, store: Annotated[BaseStore, InjectedStore()]):
    items = store.search(("users", "alice", "memories"), query=query)
    return summarize(items)
```

**node 函数**注入是另一套机制——LangGraph runtime 检测 node 函数签名里有 `store: BaseStore` 参数名，**直接按名注入**（注入实现两步：[`_runnable.py:206-244`](../../../../langgraph/libs/langgraph/langgraph/_internal/_runnable.py) 6 个 typed Protocol + [`:293-319`](../../../../langgraph/libs/langgraph/langgraph/_internal/_runnable.py) `inspect.signature` 构建 `func_accepts` + [`:349-372`](../../../../langgraph/libs/langgraph/langgraph/_internal/_runnable.py) `RunnableCallable.invoke` 内 `getattr(runtime, runtime_key)`），不需要 Annotated：

```python
def my_node(state, *, store: BaseStore, config: RunnableConfig):
    user_id = config["configurable"]["user_id"]
    item = store.get(("users", user_id), "theme")
```

`compile(checkpointer=cp, store=st)` 同时挂载两层持久化。

### PostgresStore schema

源码：`/Users/gupengfei/program/go/src/langgraph/libs/checkpoint-postgres/langgraph/store/postgres/base.py:64-113`（本地实际存在）

字段速记（schema 具体 DDL 见源码 `base.py:64-89` + `base.py:104-113`）：

**主表 `store`**：`prefix text` + `key text` + `value jsonb` + `created_at`/`updated_at`/`expires_at TIMESTAMPTZ` + `ttl_minutes INT`（**LangGraph TTL 单位是分钟**，crew-go 用 Go `time.Duration` 业务自填 `5*time.Minute` 等显式单位避免歧义）+ 主键 `(prefix, key)` + btree 索引 `(prefix text_pattern_ops)`。`expires_at` 列是过期标记；**LangGraph PostgresStore 需业务显式调 `start_ttl_sweeper()` 启 Python background thread 周期 sweep 删除**（[postgres/base.py:807](../../../langgraph/libs/checkpoint-postgres/langgraph/store/postgres/base.py) `sweep_ttl()` / `:823` `start_ttl_sweeper()` / `:881` `stop_ttl_sweeper()`），**不是 PG 数据库自动**；`prefix` 列即 namespace 点分路径。

**向量表 `store_vectors`**：`prefix` + `key` + `field_name text`（JSON 路径如 `"content"` / `"items[0].text"`）+ `embedding vector(dims)`（pgvector）+ `created_at`/`updated_at` + 主键 `(prefix, key, field_name)` + 外键 `FOREIGN KEY (prefix, key) REFERENCES store(prefix, key) ON DELETE CASCADE`（主表删除级联清向量）。

ANN 索引类型 `ANNIndexConfig.kind: Literal["hnsw", "ivfflat", "flat"]`（`base.py:180`）—— `hnsw` / `ivfflat` 是 pgvector 标准 ANN 索引；`flat` **不是 ANN 索引类型，是占位符**表示"不建 ANN 索引"（`base.py:130` 创建 `store_vectors_embedding_idx` 时通过 `kind != "flat"` 判断跳过）—— LangGraph 实际工程上 flat 为默认（`base.py:1345` 注释"Today, we are not using ANN indices due to restrictions"），暂不上 ANN 索引时用。

## eino 设计

源码：`/Users/gupengfei/program/go/src/eino/internal/core/interrupt.go:27`

```go
type CheckPointStore interface {
    Get(ctx context.Context, checkPointID string) ([]byte, bool, error)
    Set(ctx context.Context, checkPointID string, checkPoint []byte) error
}
```

**仅 2 方法 byte blob**——无 Delete / List / namespace / TTL。所有结构化复杂度交给业务序列化层之上。

eino ADK 包**没有**独立的"长期记忆 Store"接口（grep `/Users/gupengfei/program/go/src/eino/adk/` 无 `Session`/`Store` interface 定义）。业务想做长期记忆只能自己写实现，不走 eino 框架抽象。

## 截图 Python 框架（SessionStoreProtocol）

按用途切 4 个独立 store：

```python
class SessionStoreProtocol(Protocol):
    # session store
    def get_session(self, session_id, namespace='') -> SessionMetadata
    def get_session_children(self, session_id, namespace='') -> Iterable[SessionMetadata]
    def create_session(self, data: SessionMetadata) -> None
    def delete_session(self, session_id, namespace='') -> None
    # kv store
    def get_value(self, session_id, key, default=None, namespace='') -> bytes
    def set_value(self, session_id, key, value: bytes, namespace='') -> None
    def delete_value(self, session_id, key, namespace='') -> None
    def clear_values(self, session_id, namespace='') -> None
    # message store
    def get_messages(self, session_id, namespace='') -> Iterable[Message]
    def set_messages(self, session_id, msgs, namespace='') -> None
    def append_messages(self, session_id, msg, namespace='') -> None
    def clear_messages(self, session_id, namespace='') -> None
    # compacted message store
    def get_compacted_messages(self, session_id, namespace='') -> Iterable[Message]
    def set_compacted_messages(self, session_id, msgs, namespace='') -> None
    def append_compacted_messages(self, session_id, msg, namespace='') -> None
    def clear_compacted_messages(self, session_id, namespace='') -> None
```

**定位差异**：该框架面向"对话产品"（每用户多 session、每 session 多消息版本），把 session metadata / messages / compacted messages 都做成一等公民的持久化抽象。

LangGraph / eino / crew-go 是"图执行"流派，把 messages 当 channel state 装进 Checkpointer，不切独立 store。

## 我们的优势

### 1. Go 强类型 + new type 编译期防误用

`Namespace` 是 new type 而非 alias —— 业务把字符串 ns 误传给 Store 编译错误。LangGraph 靠命名 `checkpoint_ns` vs `namespace` 区分但 Python 动态类型 runtime 才报错。

### 2. ctx 注入跟 Checkpointer 协议对称

`store.WithContext` / `store.FromContext` 跟现有 `compose_v2.WithCheckpointerCtx` / `compose_v2.GetCheckpointer` 完全对称的 helper —— 业务学一套 ctx 注入模式覆盖两层持久化。LangGraph 用 `Annotated[BaseStore, InjectedStore()]` 反射注入是 Python 特化我们 Go 学不来也不必学。

### 3. 单层 KV 比 4-store 切分简洁

截图框架按用途切 4 个 store（session/kv/messages/compacted）—— 业务理解负担高（"我这个数据该放哪个 store"）。我们走单层 KV + namespace 层级，业务自由组织 `("users", uid, "memories")` 这种路径即可表达"用户 alice 的记忆"。LangGraph 也走单层。

### 4. 命名跟 Checkpointer NS 编译期 + 语义双重防错

- 参数名：`ns` 缩写 vs `namespace` 全拼
- 类型：`string` vs `store.Namespace`
- 包路径：`compose_v2.JoinNS` vs `store.NS`

三层防护强于 LangGraph 单纯的命名 `checkpoint_ns` vs `namespace`。

### 5. helper 内 `IsSubAgentStoreScope` ctx 协议（LangGraph 没有）

crew-go 在 `WithStoreMemory` helper 内 postamble + 业务自定义 Store 后端保留 ctx 协议（`WithSubAgentStoreScope` / `IsSubAgentStoreScope` 15 push 点）——业务用 helper 时享受框架级"防 sub-agent 双重持久化"防护。

LangGraph 完全没有这种协议（`is_nested` flag 仅用于 checkpoint 不暴露业务，[`pregel/_loop.py:257`](../../../langgraph/libs/langgraph/langgraph/pregel/_loop.py)）；LangGraph 业务唯一防双写手段是 ns 设计区分（`("supervisor", ...)` vs `("sub_agent", agent_name, ...)`）。多 agent 示例 LANGGRAPH_MULTI_AGENT.md 里 sub-agent **根本不写 store**——记忆只走 message channel，绕开了双写问题但失去了 sub-agent 写长期记忆的能力。

**双轨设计**：业务用 helper 时享受 scope 防护（crew-go 特色）；业务直 `st.Put` 时降级跟 LangGraph 一致（业务自负 ns）。

## 我们的不足

### 1. PoC 不内置语义搜索

LangGraph 的 `IndexConfig` + `fields` JSON 路径 + 多字段索引能力是其长期记忆场景核心卖点。我们 PoC 不做 —— 业务自己写 `Mem0Store` 实现 `store.Store` 接口接 mem0 SDK。（2026-09-18：已补检索接口层 `8c4e440a`，不绑 embedding 供应商；内置 redis / sql 后端仍无向量能力，语义搜索仍靠业务后端。）

**取舍合理性**：embedding 依赖（OpenAI/Cohere 等）侵入框架太多；mem0/Zep 自家 SDK 自带语义检索能力，业务 wrap 一层适配 `store.Store` 即可。

> **现状**：LangGraph 官方仓与 mem0 之间**没有现成的 BaseStore 适配实现**（grep `mem0` 在 langgraph 0.6.10 仓 0 引用，langgraph-bb 调研文档亦无）—— 业务从 0 写。这是 PoC 不内置的代价。

### 2. 接口比 LangGraph 略多但比截图框架少

LangGraph 抽象基础是 `batch`+`abatch` 两方法，其余 default-impl。我们直接 5 高层方法 + Batch = **核心 6 方法**暴露（外加 `TTLAware` 可选 assertion）—— 实现者要写 6 个不只 1 个。

**取舍合理性**：Go 业务直接调高层方法更自然；实现 InMemoryStore 时 6 方法重复代码可控；后端实现（Redis/PG）反正每方法都要单独优化（filter 翻成 SQL / Redis Lua），统一 Batch 入口反而难。

### 3. ListNamespaces 不支持通配符

LangGraph 支持 `("a", "*", "f")` 中段通配符。我们当前仅 prefix/suffix 完整路径过滤（`store/memory.go::matchesPrefix/matchesSuffix`，无中段通配）。

**取舍合理性**：通配符是边缘需求；如有业务场景再加 `WithListPattern` option。

### 4. 无 async 双套接口

LangGraph 每方法都有 sync/async 双版本（`get` + `aget` 等）。我们仅一套（Go 用 goroutine 直接调）。

**取舍合理性**：Go 并发模型不需要 async/await 区分。

### 5. 无后台 TTL 清理 goroutine

LangGraph PostgresStore 有 `sweep_interval_minutes` 定期清过期。我们**不用后台 sweep goroutine**——TTL 走后端原生能力。

**取舍合理性**：`redis/` Store 已实现 TTL（Redis 原生 `EXPIRE` 懒过期、无需 sweep，`redis/store.go::SupportsTTL` 返 true）；`InMemoryStore` + `sql/` Store 暂不支持（不实现 `TTLAware`，`WithTTL` 返 `ErrTTLNotSupported`；SQL 待 Phase 2 加 `expires_at` 列——未做，已进中心 backlog）。无 LangGraph 式定期 sweep——Redis 原生 EXPIRE 替代。

## filter 类型不匹配语义：报错还是不命中（2026-09-03 调研并拍板 ③，09-04 落地）

> 触发：`doc/code/todo.md` 🔴「`sql` store 违反 `ErrFilterTypeMismatch` 契约」。本节合并七路单视角调研（LangGraph 三后端 / 行业 10 系统 / eino / PG 真库 / MySQL 真库 / SQLite 真引擎 / 本仓事实）。原始报告与探针输出在会话 scratchpad、不入仓，**下面每条实测都可按给出的 SQL 复现**（PG 用 `github.com/fergusstrange/embedded-postgres` 起 16.9，MySQL 用官方 `mysql-8.0.35-macos13-arm64.tar.gz` 用户态起，SQLite 的 conformance 走本模块的 `OpenSQLiteForTest`，SQL 矩阵是独立的 modernc 探针——同 driver、同 `[]byte` 绑参）。

### 问题（以下为 2026-09-03 落地前的现状；落地形态见后文「落地时定的」）

契约（`store/conformance/search.go` 的 `Search_WithFilter_TypeMismatch_Returns_Error`）：存 `{"field":"string_val"}`，filter `{"field":{"$gt":10}}` ⟹ 必须 `errors.Is(err, store.ErrFilterTypeMismatch)`；`store/conformance/conformance.go` 把它列为必实现、不可 skip。in-memory（`store/memory.go` 的 `applyOp`）、redis（`redis/store_search.go` 的 `applyOp`）、`example/store/store_custom_backend`（同款 `applyOp`）三个客户端过滤实现都做得到——它们在比较那一刻看得到存的值。`sql` 后端（`sql/store_search.go` 的 `buildFilterSQL`）把 filter 下推成 SQL，`isNumericValue` 只看 **filter 值**，存的是什么类型不读数据不知道。

### 本仓三方言真库实测（2026-09-03）

三套 conformance 各跑一遍：**store 套件三方言都只挂这一条叶子**（PG：`22P02 invalid input syntax for type numeric: "string_val"` 驱动错、非 sentinel；SQLite / MySQL：`err == nil`），checkpointer 套件三方言全绿。⚠️ MySQL 的 store 套件要先打补丁 DDL 才跑得起来——原样 `Setup` 报 `Error 1071`，见下文「顺带抓到」。

现状语义逐格（同一 namespace 里混放 `"string_val"` / `"9"` / `10` / `10.0` / `50` / `true` / `null` / 缺字段 / 对象 / 数组）：

| filter | in-memory（= 契约） | PG `(value->>'f')::numeric op $1` | MySQL `JSON_EXTRACT(value,'$.f') op ?` | SQLite `json_extract(value,'$.f') op ?` |
|---|---|---|---|---|
| `$gt: 10` | 扫到任一非数值行 ⟹ **整个 Search 报 `ErrFilterTypeMismatch`** | 扫到字符串 / 布尔 / 对象 / 数组行 ⟹ **`22P02` 整条查询失败**（逐行运行时错：`LIMIT 1` 时按物理行序可能不报）；**数字字符串 `"9"` 被静默 cast 参与比较** | **字符串 / 布尔 / 对象 / 数组行全部命中**（JSON 类型优先级 STRING > INTEGER，不报错） | **字符串 / 对象 / 数组行全部命中**（TEXT > INTEGER），不报错 |
| `$lt: 10` | 同上报错 | 同上 | **`{"f":null}` 命中**（JSON null 最低） | **`true` 命中**（bool 存成 INTEGER 1） |
| `$eq: 10`（op 形） | 只 `10` / `10.0`，**从不报错** | 同样走 cast ⟹ 字符串行 **`22P02`**；`"10"` 被 cast 后命中 | `10` / `10.0` | `10` / `10.0` |
| `$eq: 10`（直值） | 只 `10` / `10.0` | `@>` containment：`10` / `10.0` ✓ | `JSON_CONTAINS`：`10` / `10.0` ✓；⚠️ `{"f":1}` **命中 `{"f":[1,2]}`**（标量含于数组） | `json_patch` 文本归一比较：**只 `10`，不含 `10.0`** |
| `$gt: 9007199254740992` | `9007199254740993` **不命中**（`toFloat` 走 float64 丢精度） | 命中 ✓（numeric 精确） | 命中 ✓（被标成 `UNSIGNED INTEGER`） | 命中 ✓（64 位整数） |

一句话：**四个后端四种语义，没有一列跟契约相同，也没有两列相同**。旧文只记了 SQLite「不报错还返回行」、MySQL「大概率同」，没说返回的是哪些行；真库实测是**所有异类型行都被当正命中返回**，且各带一档（SQLite `$lt` 收 `true`、MySQL `$lt` 收 `null`）。

### 别人怎么做

**LangGraph（filter 语法的直接来源；本地 1.2.11 与 GitHub main 逐行核对未改）**——三后端三种行为、零跨后端契约：

- InMemoryStore `_apply_operator`：`float(value) > float(op_value)` ⟹ 字符串行**抛 `ValueError`**，一条异构行毒化整个 search（实测）；docstring 自称 "matching PostgreSQL's JSONB behavior"。
- PostgresStore `_get_filter_condition`：`value->>%s > %s` 参数 `str(value)` ⟹ **文本字典序**，连纯数值都错（`'9' > '10'`）；测试 fixture 4.5 / 3.0 / 4.0 / 3.5 恰好都是一位数所以绿。GitHub issue #7684（2026-05 开、至今 open）+ ≥9 个 `::numeric` 修复 PR 全被流程门自动关、零维护者回复。
- SqliteStore：`CAST(json_extract(...) AS REAL)` ⟹ 非数值文本变 0.0，`$gt 10` 零行、**`$lt 10` 反命中**（实测跑仓库源码）。
- 三个 tests 目录各写各的、无 store 层 conformance；`SearchOp.filter` docstring 对类型不匹配一字不提。**社区已把 memory 的抛异常判为 bug**（#7880 / #8365："should simply not match … not abort the whole search"）；#8366 在问 `$ne` 对缺字段该学 PG 还是 Mongo。

**行业 10 个 schemaless / JSON 系统，「存储侧类型不匹配」的规范行为**：

| 行为 | 系统 | 数 |
|---|---|---|
| 报错 | — | **0** |
| 不匹配（零行） | MongoDB（type bracketing："comparison operators only perform comparisons on fields where the BSON type matches"）、Cosmos DB（undefined ⟹ 排除）、DynamoDB 非键属性（"the value does not match"）、PG SQL/JSON path（三值逻辑 unknown，`"foo" > 0` 是官方例子）、Qdrant（"considered not satisfied"）、Chroma（元数据按类型分列存，`$gt` 只查数值列） | 6 |
| 按类型全序排出真假 | CouchDB Mango、MySQL JSON、SQLite、Firestore | 4 |
| 问题不存在（schema 定型） | Elasticsearch mapping、Weaviate `valueInt` / `valueText`、Typesense、DynamoDB 键属性 | 4（不计） |

「报错」只出现在两处：**filter 值自身非法**（Chroma `$gt: "abc"` 抛 ValueError、Weaviate 用错 `valueXxx`）和**列有 schema**。四个「按类型序」的里 MySQL / SQLite 是 SQL 引擎，CouchDB / Firestore 是要给 view / 索引定全序的文档库（Firestore 官方只定义了全序，range filter 跨类型是否返回未见例句）；Mongo 系 DSL 清一色不匹配。

**mem0**（本文档推荐的记忆后端）：filter 透传后端，同一个 `{"age": {"gt": 10}}` 在 pgvector 报 PG 22P02（不捕获）、Qdrant / Chroma 返空——和 LangGraph 同款「各报各的」。

**eino**：主仓与 eino-ext 都没有跨 thread KV store，也没有跨后端 filter DSL——12 个 retriever 后端七种原生 filter 载体（ES typed Query / 裸 JSON / Milvus 表达式串 / Qdrant proto / RediSearch 串 / VikingDB 自家 DSL / 火山知识库 `DocFilter` map），公共层只留类型擦除挂点 `WithDSLInfo`（注释明写 "semantics are defined by the underlying store implementation"）。它的后端全是 schema-first，「不读值不知类型」这个困难根本不出现。代价有实证：eino-ext #416 Milvus 报 `FieldData type not matched`，报告者定位不到是哪个字段。**对本决定无正面信息量**——我们已定义 DSL、已有三个内置后端要一致，回头删 DSL 等于让业务对三个后端写三种 filter。

### 四条路在真库上的表现

| | 额外查询 | 类型不匹配行 | 顺序保证 | 备注 |
|---|---|---|---|---|
| ① 预检：`Dialect` 加「取 JSON 字段类型」、查前先探 | +1 往返 | 探到就返 sentinel | — | **TOCTOU**：预检与查询之间可写入新行 ⟹ 仍会 22P02；且「多一次查询」这个前提**不成立**（见 ④） |
| ② 放宽契约为「报 sentinel 或返零行」 | 0 | 后端自选 | — | 把分歧合法化；而且我们 SQL 今天连「零行」都不是（上表是「全部命中」） |
| **③ 改契约为 type bracketing**：范围 op 只在存储类型 == filter 值类型时比较，异类型 / 缺字段**不命中、不报错**；`ErrFilterTypeMismatch` 只留给 **filter 值自身非数值**（调用方手上就有的信息） | 0 | 零行、`err == nil` | PG 用 `CASE` 有文档保证；MySQL / SQLite 比较永不报错，`AND` 即可 | PG：`(CASE WHEN jsonb_typeof(value->'f')='number' THEN (value->>'f')::numeric END) > $1`——`AND` 形靠 planner 的 cost 排序碰巧成立，实测 `ALTER FUNCTION jsonb_typeof COST 10000` 后 20/20 报 22P02，**CASE 形不受影响**；jsonpath `strict … silent=true` 亦可。MySQL：`JSON_TYPE(JSON_EXTRACT(value,'$.f')) IN ('INTEGER','UNSIGNED INTEGER','DOUBLE','DECIMAL') AND …`（**必须含 `UNSIGNED INTEGER`**，`9007199254740993` 实测被标成它）。SQLite：`json_type(value,'$.f') IN ('integer','real') AND …`。三个客户端实现的 `applyOp` 改成「存值非数值 ⟹ 该行不命中」。索引：无谓词的表达式索引在混合数据上**建不出来**（22P02）；带 `WHERE jsonb_typeof(...)='number'` 的 partial index 实测能被 `AND` 形命中，`CASE` 形未测（③ 按 `CASE` 落地后这条成了开放项，已进 `doc/code/todo.md`） |
| ④ 保留报错契约，但在**同一条查询**里报 | 0 | 报错（可映射成 sentinel） | PG 有 | PG 实测：`CASE … ELSE ('crew_type_mismatch:' \|\| jsonb_typeof(value->'f'))::numeric END`（marker 必须依赖列值，否则被常量折叠、纯数值表也炸）⟹ `22P02 "…crew_type_mismatch:string"`，Go 侧按 SQLSTATE + 前缀映射；或 `Setup` 建 plpgsql 函数 `RAISE … ERRCODE='AD001'`。**MySQL 纯 SQL 抛不了错**（`CAST` 只出 warning 3156、strict mode 也一样——实测；要 `SIGNAL` 就得建存储函数 = DDL——文档语义，未实测）；SQLite 只有 `json_extract('not json','$')` 这类 hack（文档语义，未实测）。三方言三套错误映射，错误仍是逐行运行时的（`LIMIT` / 行序相关）——买到的是一个整个生态没有第二个实现者的语义 |

### 判断：推荐 ③（2026-09-03 拍板采纳，见下「落地时定的」）

1. 行业 0/10 报错、文档型 DSL 6/6 不匹配，MongoDB 有整段规范可逐字对照；LangGraph 最接近「报错」的那个后端正被社区当 bug 报。
2. 本仓「守卫合法性」判别式（CLAUDE.md）：filter 值的类型是调用方自己给的——报错合法、保留；**存的是什么类型只有读了数据才知道**，尤其共享 namespace / 多写入者 / schema 演进（旧行 `"10"` 新行 `10`）下调用方无法满足 ⟹ 该定义默认行为、不该报错。in-memory 今天「一条异构行毒化整个 Search」正是这道守卫的代价。
3. 三方言都有零额外查询、有文档背书的实现。
4. ① 被 ④ 支配（同样报错、不多一次查询）；④ 又被 ③ 支配（③ 的语义有 10 家先例、④ 的没有）；② 最差。

**落地时定的**（2026-09-04 落地；每条都配了 N=2 fixture，`store/conformance/search.go` 三条新用例逐格钉，PG 16.9 / MySQL 8.0.35 / SQLite 真库 conformance 全绿）：

- 范围 op：存储类型 == filter 值类型才比；数值 int / float 互通；`"10"` 对 `$gt: 5` **不命中**（PG 现状会 coerce，改 CASE / jsonpath 后消失，要专属用例钉住）；`true` / `false` 不是数（SQLite 现状当 1 / 0）。
- `$eq` / `$ne` **不走 cast**（⚠️ 调研时提的是 PG containment `value @> {"f": v}`，**已被下一条的原生 JSON 相等取代**，此处只留形状不留写法）：落地为 `$eq v` ≡ 原生 JSON 相等、`$ne v` ≡ `JSONFieldExists AND NOT JSONFieldEquals`；三方言真库 conformance 全绿，`$ne` 的命中集与 in-memory 逐一相同（含 `{"f":null}` 行、不含缺失字段行）——直值与 op-map 两条路从此同一条。
- 数组 / 对象 filter 值下 `$eq` **定为逐元素相等（不是子集）**：直值与 `$eq` 都走原生 JSON 相等——PG `col->'f' = $1::jsonb`（不再用 `@>`）、MySQL `JSON_EXTRACT = CAST(? AS JSON)`（不再用 `JSON_CONTAINS`，标量含于数组的坑随之消失）、SQLite 按 JSON 类别分路（number 走 `json_type` 守卫 + `json_extract` 数值比，bool / null 走 `json_type` 文本比，string / array / object 走 `json(col) -> '$.f' = json(?)` 文本比）；客户端过滤走 `store.ValueEqual`（数值互通递归到容器、slice 类型互比、map 逐键）。`$ne` = `JSONFieldExists AND NOT JSONFieldEquals`。范围 op 的参数按 Go 数值绑定，json.Number 先转 int64 / float64（之前 sql 对 json.Number 返 sentinel、in-memory / redis 接受，也是一处漂）。嵌套 filter `{"a": {"b": ...}}` sql 明确拒绝（`ErrUnsupportedFilterOp` 带说明），in-memory 仍按嵌套部分匹配——capability 矩阵标 ❌ 不承诺。
- conformance：`Search_WithFilter_TypeMismatch_Returns_Error` 拆成三条——「filter 值非数值 ⟹ sentinel，且空 namespace 也报（静态判，别等扫到第一行）」、「异构 namespace `$gt` 只返数值行、`err == nil`」、「`$lt` 零行」（专门钉死「CAST 成 0 反命中」的形态）。
- 同步改法：客户端过滤的三份拷贝（in-memory / redis / example 各一套 `applyOp` + `valueEqual` + `toFloat`）**收成一份**——`store/filter.go` 导出 `ValidateFilter` / `MatchFilter` / `ValueEqual`，redis 与 example 直接调（之前「改一处漏两处」的形状，这次顺手消掉）；`Dialect` 接口的 `JSONContains` / `JSONExtractCompare` 换成 `JSONFieldExists` / `JSONFieldEquals(kind)` / `JSONFieldNumericCompare` 三个按语义命名的方法，`sql/store_search.go` 只拼 AND；`store/store_test.go` 的 `TestInMemoryStore_SearchFilterTypeMismatch` 拆成「filter 值侧报错」+「存值侧不报错零行」两半；`sql/store_search_test.go` 保留 filter 值侧两条，另加 json.Number 绑数值、SQL 形态、`$ne` 形态、嵌套拒绝、多键 SQL 顺序确定五条。

### 顺带抓到的（独立于契约，各自单列 backlog）

> 2026-09-04 补：落地后又做了一轮 **6 后端 × 74 组差分**（in-memory / example / redis / SQLite / PG / MySQL），
> 370 格里 52 格不一致，收敛成三类——**字段名字符集**（`sql` 的 `validateIdentifier` 拒掉 `user-id` / `a.b` / `用户`
> 这类合法 JSON 键，而 key 进 SQL 其实是字符串字面量 / JSON path，该转义不该限字符集）、**数值精度**（客户端
> float64 vs 引擎原生数值，两个方向都出现过分歧）、**嵌套与对象直值**（已在 `WithFilter` godoc 声明「别依赖」）。
> 前两类已进 `doc/code/todo.md` 并写进 `WithFilter` godoc 的「已知不一致」；`$or`、`$gt: nil`、`$gt: "abc"`、
> 数组直值这几格六后端完全一致。


- ~~🔴 **MySQL store 表建不出来**~~ **已修（2026-09-04，`data_key` 缩到 VARCHAR(255)，拍板同上）**：`sql/store_schema.go` 的 `mysqlStoreMigrations` 原写 `PRIMARY KEY (ns VARCHAR(512), data_key VARCHAR(512))`，utf8mb4 下 4096 字节 > InnoDB 索引键 3072 字节上限 ⟹ `Error 1071 (42000): Specified key was too long`（MySQL 8.0.35 实测）。现在 ns 512 + data_key 255 = 3068 字节，与 cp 表同形状；**MySQL 上 key ≤ 255 字符、ns ≤ 512 字符**是该方言的长度契约（PG / SQLite 用 TEXT 无此限）。
- ~~🟡 `$eq` / `$ne` 跨后端分叉~~ **已随 ③ 修掉**（上表第 3、4 行，另加 PG `->>` 文本比较让 `$eq "10"` 命中数字 `10`、`$ne` 的 NULL 语义丢 `{"f":null}` 行）。
- 🟡 in-memory / redis 的 `toFloat` 走 float64，> 2^53 的整数比较错（PG / MySQL / SQLite 都精确）——要一致的话该改的是 in-memory（整数路径直接比）。

### 本节引用速记

- LangGraph：`libs/checkpoint/langgraph/store/memory/__init__.py` 的 `_apply_operator`；`libs/checkpoint-postgres/langgraph/store/postgres/base.py` 与 `libs/checkpoint-sqlite/langgraph/store/sqlite/base.py` 各自的 `_get_filter_condition`（sqlite 那份在父类与子类里有逐行相同的两份）；issues #7684 / #7880 / #8365 / #8366。
- MongoDB type bracketing：`docs/manual/reference/method/db.collection.find/#type-bracketing`；PostgreSQL 4.2.14「Expression Evaluation Rules」（`CASE` 强制求值顺序）与 9.16 SQL/JSON path；MySQL「Comparison and Ordering of JSON Values」与 `JSON_TYPE()`；SQLite「Datatypes In SQLite」§4.1 与 `json_type()`。
- mem0：`mem0/vector_stores/{pgvector,qdrant,chroma}.py`；eino：`components/retriever/option.go` 的 `DSLInfo` / `WithDSLInfo`。

## 决策记录

| 决策 | 选择 | 备选 | 关联 ISSUE |
|---|---|---|---|
| 是否加 Store 抽象 | ✅ 加，作为 Checkpointer/Memory 之外第三层 | 沿用 Memory + 让业务自己包 ns | RFC v1（本文） |
| Namespace 类型 | `type Namespace []string` (new type) | `type Namespace = []string` (alias) / `string` 分隔 | RFC v1 — Q1 决策（用户拍板：靠类型隔离防混淆）|
| Checkpointer ns 是否重命名 | ❌ 不改 ns → checkpointNS | 改全为 checkpointNS 对齐 LangGraph 字面 | RFC v1 — 改动 ~380 处代码 / 14 处文档；Go 类型 + 包路径双层防护（编译期防误传 + 包前缀 `compose_v2.JoinNS` vs `store.NS` 读时区分）；LangGraph `checkpoint_ns`/`namespace` 双命名主要是 Python 弱类型妥协 |
| Memory 接口去留 | 删，`WithStoreMemory` helper 替代 | 双接口并存 / Memory 加 query 参数 | RFC v1 — Q2 决策；LangGraph 没双接口；项目未上线无兼容包袱 |
| PoC 包不包含语义搜索 | ❌ 不内置，靠 mem0 后端（2026-09-18：已有检索接口层 `8c4e440a`，仍不内置 embedding）| 内置 IndexConfig + embedding 依赖 | RFC v1 — 不强制业务装 embedding |
| TTL 实现 | 接口预留（`WithTTL` + `SupportsTTL`），InMemoryStore 返 ErrTTLNotSupported | 全套实现 / 完全不留接口 | RFC v1 — Q3 决策 |
| Batch API | ✅ 含，简化版（顺序 best-effort） | 不暴露 Batch / 完整原子事务 | RFC v1 — Q3 决策；~~事务化等后端 PG 实现再做~~ 2026-09-07 已定 best-effort 是唯一语义，不再邀请事务化（`d6d06312`）|
| 注入机制 | ctx 注入 (`FromContext`) + Agent option (`WithStore`) | 反射注入业务函数签名 / RunSession 暴露 | RFC v1 — Q2 决策；跟 Checkpointer ctx 协议对称 |
| 接口方法数 | 5 高层（Get/Put/Delete/Search/ListNamespaces）+ 1 Batch 共 6 方法核心 + 可选 TTLAware interface（独立 assertion） | 仅 Batch 抽象 + 默认实现 | RFC v1 — Go 风格直接方法调用更自然；TTL 探针拆 optional interface 让核心接口纯净 |
| Batch 失败语义 | per-Op 错误隔离（best-effort） | 原子事务（PG）/ 整 batch 异常退出（InMemory）| RFC v1 — InMemoryStore 选 best-effort 利于幂等批量场景；~~事务化等 PG 后端~~ 2026-09-07 已定 best-effort 是唯一语义（`d6d06312`）；Store 契约不提供事务；需要原子性只能绕开 Store 直接操作底层数据库 |
| iter-5-I Option A `splitNS` 读出端 0 验证 | sql backend 读出 ns string 直接 `strings.Split` + tuple，不验段空/NUL byte，不 silent skip | LangGraph `_decode_ns_bytes` 同款（base.py:1335-1340 直接 `split(".")` + tuple）/ 完整对照 `_validate_namespace` 写入端 fail-fast (base/__init__.py:1255) | 退回 iter-4 H4 段验证 + 5c35175 caller silent skip 双层过度防御 — 写入端 `store.Namespace.Validate()` (`store.go:60-76`) 已 fail-fast 防御非法 ns 入库，读出端再兜底引入 SQL `LIMIT/OFFSET` 翻页漏数据 bug + "防 panic" 空想威胁 (`strings.Split` 总返 len≥1 slice 不 panic). LangGraph 实证读出 0 验证设计成熟。业务跨 backend backup/restore 数据完整性自负（用 `ns.Validate()` 自检）|
| `Item.Value` 的往返契约 | **✅ Put 入口统一 JSON 归一化（2026-10-22）**：`store.NormalizeValue` 把值换成它的 JSON 形状（UseNumber），存、读、filter 比较都按它（filter 的比较值在 `WithFilter` 里同样归一化，直值编成 JSON 是对象时等同 `$eq`；只有无类型 nil 等于删除）；in-memory / example 在 Put 时调，sql 落盘前再编码一遍（map 键排序） | 只在比较时归一化（治不了 SQLite 按存进去的文本比，且 `MatchFilter` 逐行调）/ 保持 in-memory 原样存（条款按最强实现写，别人物理上做不到）| conformance 加 struct / 复合内容 / 编不成 JSON / 具体 Go 类型过滤四格；真 PG 全过 |
| filter 范围 op 打在类型不匹配字段：报错还是不命中 | **✅ ③ type bracketing（2026-09-03 拍板、09-04 落地）：异类型 / 缺字段不命中、不报错；`ErrFilterTypeMismatch` 只留 filter 值自身非数值；`$eq` / 直值改原生 JSON 相等；客户端过滤收成 `store.MatchFilter` 一份** | ① `Dialect` 预检（TOCTOU，且被 ④ 支配）/ ② 放宽为「sentinel 或零行」/ ④ 同一条查询里报错并映射 sentinel（PG 可、MySQL 需 DDL、SQLite 是 hack） | 2026-09-03 调研 + 三方言真库实测（四后端四种语义），见「filter 类型不匹配语义」节 |

## 三方源码本地路径速记

| 项目 | 路径 | 文件 |
|---|---|---|
| crew-go store | `/Users/gupengfei/program/go/src/crew-go/store/`（已落地，PR 1-3.3）| — |
| LangGraph BaseStore | `/Users/gupengfei/program/go/src/langgraph/libs/checkpoint/langgraph/store/base/__init__.py` | 接口 + Op + Item + IndexConfig + TTLConfig 全在此 |
| LangGraph InMemoryStore | `/Users/gupengfei/program/go/src/langgraph/libs/checkpoint/langgraph/store/memory/__init__.py` | 参考实现 |
| LangGraph 调研文档 | `/Users/gupengfei/program/go/src/langgraph-bb/LANGGRAPH_STORE.md` + `LANGGRAPH_MEMORY.md` | 已整理总结 |
| eino CheckPointStore | `/Users/gupengfei/program/go/src/eino/internal/core/interrupt.go:27` | 2 方法极简 |
| LangGraph PostgresStore | `/Users/gupengfei/program/go/src/langgraph/libs/checkpoint-postgres/langgraph/store/postgres/base.py:64-113` + `base.py:177-225` ANNIndexConfig | PG schema + 向量 + TTL 全在此（v2 RedisStore/PostgresStore 实施参考）|
| LangGraph SqliteStore | `/Users/gupengfei/program/go/src/langgraph/libs/checkpoint-sqlite/langgraph/store/sqlite/base.py:707` | Sqlite 实现（v2 SqliteStore 实施可直接参照）|
| LangGraph Runtime（node 级注入 + 嵌套继承）| `/Users/gupengfei/program/go/src/langgraph/libs/langgraph/langgraph/runtime.py:90`（`Runtime` class）+ `:188`（`def merge`）+ `:195`（`store=other.store or self.store` 表达式）| node 级 6 字段 dataclass；`merge` 嵌套子图 store 继承逻辑 |
| LangGraph NS_SEP / NS_END | `/Users/gupengfei/program/go/src/langgraph/libs/langgraph/langgraph/_internal/_constants.py:75-77` | `\|` 层级分隔 + `:` task 维度（**跟 crew-go `JoinNS` `/` 格式不兼容**，跨工具迁移需 ns 改写）|
| 截图 Python 框架 | 未知（用户提供截图，未提供仓库地址）| — |
| LangGraph store filter 三后端 | `/Users/gupengfei/program/go/src/langgraph/libs/checkpoint/langgraph/store/memory/__init__.py`（`_apply_operator`）/ `libs/checkpoint-postgres/langgraph/store/postgres/base.py`（`_get_filter_condition`）/ `libs/checkpoint-sqlite/langgraph/store/sqlite/base.py`（同名两份） | 类型不匹配三种行为，见「filter 类型不匹配语义」节 |
| eino retriever filter 挂点 | `/Users/gupengfei/program/go/src/eino/components/retriever/option.go`（`DSLInfo` / `WithDSLInfo`） | 公共层不定义 filter，各后端原生表达式 |
