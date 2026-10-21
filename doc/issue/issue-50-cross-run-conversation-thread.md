---

> ⚠️ **P1（ThreadID + ListByThread/LatestByThread）已被 [ISSUE-98](issue-98-conversation-history.md)
> 推翻并删除**（2026-07-23）。受制于 cp「成功即清」，这套 API 只能返回当前挂起的 run、名不副实。
> 会话历史改由 `agent_v2` 独立 append-only 层（`WithHistory`）承担，cp 层回归纯「单次 Run 临时态」。
> 本文档以下内容是当年 P1 的设计记录，**保留作历史语境**，其中 cp.ThreadID / ListByThread /
> LatestByThread 相关代码均已不存在。P0（GraphID 防串台）仍有效。
id: ISSUE-50
title: 跨多次 graph 执行的会话（ThreadID + GraphID 防串台）
status: ✅ 已实现（2026-05-07，方案 E + Round 5 命名修正）
priority: 中
created: 2026-04-24
reference:
  - /Users/gupengfei/program/go/src/langgraph/libs/checkpoint/langgraph/checkpoint/base/__init__.py
  - /Users/gupengfei/program/go/src/langgraph/libs/langgraph/langgraph/pregel/_algo.py
  - /Users/gupengfei/program/go/src/eino/compose/checkpoint.go
---

> ⚠️ **本文为 2026-04-24 设计提案稿**——文档中提到的"加 RunSession.ThreadID 字段"
> / `NewRunSessionForThread` helper 等均**未按提案落地**。最终采用方案 E（最小破坏面）
> + Round 5 命名修正（sess 层字段叫 `RunID` 不是 ThreadID，cp.ThreadID 字段保留）。
> 当前实施现状 + 业务用法见 `issues.md` ISSUE-50 段（P0+P1 全部 ✅ 已实现 +
> Round 3-5 cross-audit 修补记录）。

# ISSUE-50 · 跨多次 graph 执行的会话

## 背景

### 现状

crew-go 目前的"会话"概念只有 `agent.RunSession.ID`——每次 `NewRunSession()` 自动 UUID v4，**仅代表"本次 `RunGraph`/`ResumeGraph` 调用的唯一运行 ID"**，定位等同于 LangGraph 的 `run_id`。

我们已经有的三个相关能力：

| 能力 | 承载 | 定位 |
|---|---|---|
| 一次执行内的上下文 | `RunSession.values` / `Events` | run-local |
| Interrupt 存档→Resume | `Checkpointer` + `sessionID` 一对一 | 一次会话的两段执行 |
| 跨 run 长期历史 | `Memory` 接口（`Read`/`Write`） | 应用层自管 key |

**缺的是：跨多次独立 `RunGraph` 调用的"会话"一级字段。** 产品场景：
- 多轮 chat（第 1 轮跑 `RouterAgent`，第 2 轮跑 `SpecialistAgent`，共享历史）
- HITL 长流程（存档几小时后 Resume，期间可能穿插别的 graph 执行）
- 平台多 agent 协作（同一会话的消息被派发给不同 graph）

这些场景用户现在只能在应用层"自己记账"——把 `sessionID` 当 ConversationID 复用，或通过 `Memory` 手动粘合。没有框架原语。

### LangGraph 同事指出的坑

> "如果用户在使用同一个 data store（或者叫 checkpoint saver）的时候，故意用同一个 thread-id 在两个线程中 invoke 两个可能并不相干的 graph 对象"

经审 LangGraph 源码确认：

1. **LangGraph `Checkpoint` 不存 graph 身份**（[`libs/checkpoint/langgraph/checkpoint/base/__init__.py:65-97`](../../../langgraph/libs/checkpoint/langgraph/checkpoint/base/__init__.py)）：只有 `thread_id` + `checkpoint_ns`（子图路径）+ `checkpoint_id`，没有 `graph_id`
2. **Pregel 对齐节点名时静默跳过**（[`libs/langgraph/langgraph/pregel/_algo.py:575-579`](../../../langgraph/libs/langgraph/langgraph/pregel/_algo.py)）：`if name not in processes: return` —— 对不上的 node 名直接丢掉 task，不报错
3. **`assistant_id` 只在 Platform/SDK 层**（`libs/sdk-py/langgraph_sdk/schema.py`），OSS 核心不存入 Checkpoint

LangGraph 对齐 eino 的现状：两家都**没有 graph 身份守护**，都是契约层失守+静默错数据。

我们当前也一样：`Checkpointer` 按 `sessionID` 单键存储，两个不相干 `Graph` 共享同一个 sessionID 并发跑：
- Save 静默互相覆盖
- Resume 载入后，超步 frontier 的 NodeID 对不上时报 `node %q not found`（比 LangGraph 静默 skip 略强，但仍非主动校验）
- `localState` / `Events` 仍会串台污染

---

## 决策方向

**拆两层，独立价值独立推进。**

两者在代码上解耦——P0 不依赖 P1，P1 也不依赖 P0。只做 P0 就能堵住串台；只做 P1 就能提供跨 run 会话能力。

---

## P0 · GraphID 校验（正确性守护，优先做）

### 核心想法

把"checkpoint 属于哪个 graph"做成一级字段，Save 填入、Resume 严格校验。错档一出现就 fail-fast，绝不让其静默串台。

### 代码改动

#### 1. `Graph` 加身份

```go
// compose/graph.go
type Graph struct {
    name string  // 用户显式声明，或自动 fingerprint
    // ...
}

// GraphBuilder 新增
func (b *GraphBuilder) WithName(name string) *GraphBuilder {
    b.name = name
    return b
}

// Graph 暴露 ID
func (g *Graph) ID() string {
    if g.name != "" {
        return g.name
    }
    return g.fingerprint()
}
```

**Fingerprint 自动兜底**：当用户不 `WithName` 时，基于 graph 拓扑生成稳定指纹。

```go
func (g *Graph) fingerprint() string {
    nodeIDs := make([]string, 0, len(g.nodes))
    for id := range g.nodes {
        nodeIDs = append(nodeIDs, string(id))
    }
    sort.Strings(nodeIDs)
    
    h := sha256.New()
    for _, id := range nodeIDs {
        h.Write([]byte(id))
        h.Write([]byte{0})  // 分隔符
    }
    if g.stateFactory != nil {
        h.Write([]byte(reflect.TypeOf(g.stateFactory(nil)).String()))
    }
    return hex.EncodeToString(h.Sum(nil))[:16]
}
```

同构 graph 拿到相同指纹；nodeID 或 state type 任一不同就落到不同指纹。

#### 2. `Checkpoint` 加字段

```go
// agent/event.go
type Checkpoint struct {
    ID       string  // 现状：runID / sessionID
    GraphID  string  // 新增
    ThreadID string  // 新增（P0 填空即可，P1 启用）
    CreatedAt int64
    // ... 其余字段
}
```

#### 3. Save 填入 / Resume 校验

`compose/checkpoint.go` 所有 Save 路径（`saveCheckpointOnError`、`runGraphFromSuperstep` 末尾的 Save、`RunGraph` 内 InterruptError 分支）：

```go
cp := &agent.Checkpoint{
    ID:      cpID,
    GraphID: g.ID(),  // 新增
    // ...
}
_ = checkpointer.Save(ctx, cp)
```

`ResumeGraph` 开头：

```go
func ResumeGraph(ctx context.Context, g *Graph, checkpointer agent.Checkpointer, sessionID string) (*GraphState, error) {
    cp, err := checkpointer.Load(ctx, sessionID)
    if err != nil {
        return nil, fmt.Errorf("resume: load checkpoint %q: %w", sessionID, err)
    }
    
    // 新增：graph 身份守护
    if cp.GraphID != "" && cp.GraphID != g.ID() {
        return nil, fmt.Errorf("resume: checkpoint belongs to graph %q, current graph is %q (sessionID %q reused across different graphs?)", cp.GraphID, g.ID(), sessionID)
    }
    
    // ... 后续原逻辑不变
}
```

`cp.GraphID == ""` 兼容旧档（未上线其实不用考虑，但零成本就留着）。

### 防串台收益

| 场景 | 之前 | P0 之后 |
|---|---|---|
| graph A 的 Resume 加载到 B 存的档 | 超步 frontier 名对不上时报 `node not found`（运气好）；typed state / Events 串台污染（运气不好） | Load 后立刻 `checkpoint belongs to graph X, current is Y`，fail-fast |
| 同 sessionID 并发 Save | 后写覆盖先写（静默） | 仍会覆盖（Save 本身不防并发），但被覆盖方 Resume 时立刻检测到身份不匹配，不会误用 |

Save 侧的"并发覆盖"不打算解决——那是两个用户**故意**用同 sessionID 的契约违约，框架管不着。P0 只保证**错档不会静默被误用**。

### 成本

改动面小：
- `compose/graph.go`（新增 `WithName` + `ID()` + `fingerprint()`）
- `agent/event.go`（Checkpoint 加 2 字段）
- `compose/checkpoint.go`（所有 Save 填 GraphID + ResumeGraph 校验一处）
- InMemoryCheckpointer 无需改

单独出 PR，~200 行以内。

---

## P1 · ThreadID 一级字段（跨 run 会话）

### 核心想法

`RunSession` 再加一个 `ThreadID` 字段，代表"会话"；`Checkpointer` 支持按 `ThreadID` 二级索引；`Memory` 分桶 key 从 `session.ID` 切到 `session.ThreadID`。

### 代码改动

#### 1. `RunSession` 加字段

```go
// agent/session.go
type RunSession struct {
    ID       string  // 本次 run ID（= LangGraph run_id），现状保持
    ThreadID string  // 跨 run 会话 ID（= LangGraph thread_id），可选
    // ...
}

// 现有构造函数不动
func NewRunSession() *RunSession { ... }

// 新增：指定 thread 的构造函数
func NewRunSessionForThread(threadID string) *RunSession {
    s := NewRunSession()
    s.ThreadID = threadID
    return s
}
```

`ThreadID` 空字符串表示"匿名单次 run"，现有调用完全不受影响。

#### 2. `Checkpoint` 用 ThreadID

P0 已加字段，P1 启用：

```go
// Save 时
cp.ThreadID = state.Session.ThreadID
```

#### 3. `Checkpointer` 新增按 thread 查询

```go
type Checkpointer interface {
    // 现有接口全部保留
    Save(ctx, cp *Checkpoint) error
    Load(ctx, id string) (*Checkpoint, error)
    // ...
    
    // P1 新增
    ListByThread(ctx context.Context, threadID string) ([]string, error)
    LatestByThread(ctx context.Context, threadID string) (*Checkpoint, error)
}
```

`InMemoryCheckpointer` 实现：

```go
type InMemoryCheckpointer struct {
    mu         sync.RWMutex
    data       map[string]*Checkpoint      // runID -> checkpoint
    byThread   map[string][]string          // threadID -> []runID（有序，按 CreatedAt）
}

func (c *InMemoryCheckpointer) Save(_ context.Context, cp *Checkpoint) error {
    // ... 现有逻辑
    c.data[cp.ID] = &saved
    if cp.ThreadID != "" {
        c.byThread[cp.ThreadID] = append(c.byThread[cp.ThreadID], cp.ID)
        // 保持按 CreatedAt 升序
        sort.SliceStable(c.byThread[cp.ThreadID], func(i, j int) bool {
            return c.data[c.byThread[cp.ThreadID][i]].CreatedAt < c.data[c.byThread[cp.ThreadID][j]].CreatedAt
        })
    }
    return nil
}
```

#### 4. `Memory` 与 ThreadID 关联（v1 历史 — PR 3.3 后 Memory 已替代为 store.Store）

**接口不改**，纯使用约定：应用层用 `session.ThreadID` 作为 `Memory.Read/Write` 的 key，不再用 `session.ID`。文档明确这条约定即可。

> 注：PR 3.3（commit `e6505f9`）后 `Memory.Read/Write` 已替代为 `store.Search/store.Put`，ThreadID-as-key 的约定迁移到 `store.Namespace` 设计（业务用 `store.NS("thread", threadID)` 区分会话）。

### 用户使用姿态

```go
// 第 1 轮对话
threadID := "user-1001:conv-42"
session := agent.NewRunSessionForThread(threadID)
state1, _ := compose.RunGraph(ctx, graphA, &compose.GraphState{
    Session: session,
    Messages: []*schema.Message{schema.UserMessage("查我的订单")},
}, compose.GraphRunOptions{Checkpointer: cp})

// 第 2 轮（可能同 graph 也可能换 graph）
session2 := agent.NewRunSessionForThread(threadID)
// 应用层从 Memory 拉历史注入
history, _ := memory.Read(ctx, threadID)
state2, _ := compose.RunGraph(ctx, graphB, &compose.GraphState{
    Session: session2,
    Messages: append(history, schema.UserMessage("那退款怎么办")),
}, compose.GraphRunOptions{Checkpointer: cp})

// 查本 thread 所有 run
runIDs, _ := cp.ListByThread(ctx, threadID)
```

### 成本

- `agent/session.go`（RunSession 加字段 + 构造函数）
- `agent/session.go`（Checkpointer 接口加 2 方法 + InMemoryCheckpointer 实现）
- 文档：描述 ThreadID / RunID / Memory 三者分工
- 单独出 PR，~300 行

---

## 跨框架对照

| 框架 | graph 身份防串台 | 跨 run 会话（ThreadID） |
|---|---|---|
| LangGraph OSS | ❌ 静默 skip | ✅ `thread_id`，但不防串台 |
| LangGraph Cloud | ✅ `assistant_id`（Platform 层） | ✅ `thread_id` |
| eino | ❌ | ❌ 完全甩应用层 |
| crew-go（P0） | ✅ fail-fast | ❌ |
| **crew-go（P0+P1）** | ✅ | ✅ |

做完 P0+P1，我们的会话模型在**正确性**和**功能完整度**上都超过 LangGraph OSS 和 eino。

---

## 为什么现在记录不急着做

1. **主体功能优先**：P0 是正确性问题，但目前没有实际用户踩到（项目未上线），可排在主体功能稳定后
2. **P1 跟 ISSUE-49（前端适配层）有耦合**：前端多轮对话需要 ThreadID 作为 URL/WS 连接的会话键，两个 issue 可以一起推进
3. **`Memory` 接口已先行**：跨 run 历史最常见的需求已经被 `Memory` 覆盖，`ThreadID` 更多是"给会话一个稳定的身份"，不是补数据能力短板

---

## 开放问题

1. **是否要在 `RunSession.ID` 和 `Session.ThreadID` 之外保留一个"business conversation ID"**？比如产品层的"对话 42"跟框架的 ThreadID 是否 1:1 映射？倾向：1:1，让应用层自己决定 ThreadID 值，框架不解释语义。

2. **`ListByThread` 是否要支持分页 / 时间范围过滤**？InMemory 实现可以暴力返回全量，持久化实现（未来的 SQLCheckpointer 等）要考虑。暂不做，等真有存储后端时再加。

3. **`GraphID` 的 fingerprint 能否稳定？** node 增删改会导致指纹漂移——Resume 旧档会 fail。这是**对的**：拓扑变了的 graph 不应该读旧档继续跑。但要在文档提醒：长期跑的生产系统最好 `WithName("stable-name")` 而不是吃自动指纹。
