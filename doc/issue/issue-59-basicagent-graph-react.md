---
id: ISSUE-59
title: BasicAgent 内部 ReAct 循环 Graph 化（HITL 端到端闭环）
status: ✅ 已实现（Phase 0/1/2 + P0/P1，commits 148a0e4 / aea62fd / 4d3b187 / b153014 / e803069 / c8c1d5d）
priority: 高
created: 2026-04-29
closed: 2026-04-30
---

> **设计偏离记录**：原方案预想 `basicAgentState.Messages` 为 typed state 字段；
> Phase 2 实施时改为复用 `compose.GraphState.Messages` 作为单一来源（避免双写
> 同步），typed state 只保留 `Round / Initialized / PendingCall`。决策见
> `agent/basicagent_state.go` 文件头注释。

> **历史档案说明**：本文描述 v1 `agent/` 时代 BasicAgent 内部 ReAct 循环 Graph 化的设计。文中 `Memory.Read` / `Memory.Write` / `memory_read_error` / `memory_write_error` 等 Memory 接口均为 v1 历史 — PR 3.3（commit `e6505f9`）后已整体替代为 `store.Store`（`store.Search` / `store.Put` / `WithStore` + `WithStoreMemory`），错误上报路径转走 `hook.OnError` + store backend 错误码。下文保留原历史叙述不动，仅供溯源参考。


# ISSUE-59 · BasicAgent 内部 ReAct 循环 Graph 化

## 背景

ISSUE-58 Steps 1-5 落地了 HITL 协议层（AskQuery / ResumeOptions / ResumeContext /
PlanAgent.Resume / ask_user 内置 tool），但发现一个无法在协议层修复的架构漏洞：

**ask_user 在 BasicAgent 嵌入 PlanAgent / GraphAgent 时端到端不通。**

具体表现（已写测试 `compose/plan_test.go::TestPlan_Resume_AskUserTool_EndToEnd`，
`t.Skip` 状态）：worker BasicAgent 的第一轮 LLM 抛 `ask_user` tool_call → Resume
注入 user 答案 → worker 第二轮 LLM 看到的是新构造的 user 消息（`defaultExecInput`），
**不是 `tool_result(callID, answer)` 与原 `tool_call` 配对**。LLM 没有记忆，会再次
调 ask_user 形成死循环（测试里仅因 scriptedModel 第二轮硬编码了"final" 才没死循环）。

## 根因

ReAct 在 crew-go 实现为 **`agent/react.go::reactLoop` 中的单函数 for 循环**：

```go
// agent/react.go:109
for round := 0; round < cfg.MaxRounds; round++ {
    resp, newMessages, _, err := executeRound(ctx, cfg, info, messages, ...)
    messages = newMessages
    if err != nil { return nil, err }   // InterruptError 从这里冒出
    if resp != nil { finalResp = resp; break }
}
```

`messages` 是函数局部变量，存活在 Go 调用栈上：

| 位置 | 现状 | Resume 影响 |
|---|---|---|
| `messages` 变量 | 栈上局部 | 函数 return 即蒸发，无法持久化 |
| 中间 tool_call assistant 消息 | 仅在 `messages` 里 | 同上，丢失 |
| session 事件 | tool-call 中断分支不写入（`react.go::AddEvent` 仅在 `emitFinalAnswerEvents` 和 MaxRounds 耗尽两处调用） | Resume 后 `session.SelfHistory(name)` 读不到任何东西 |
| Graph state.Messages | ResumeGraph append 了 tool_result | BasicAgent 不读 state.Messages，重建时从 session 读 |

结果：Resume 时 BasicAgent.Run 重新进入 → `messages = [system] + session.SelfHistory + [input]`
→ 所有"已经发生过的 LLM 对话"全部丢失 → LLM 看到一个全新的 prompt。

## 参考实现：eino / LangGraph 都把 ReAct 编成 Graph

### eino 的 ChatModelAgent

`adk/react.go::newReact` 编出 3 节点 Graph：

```
        ┌─────────────────────────────────────┐
        ↓                                     │
  [Init] → [ChatModel] ─→ branch ─→ [ToolNode]┘
                          │
                          └→ END
```

关键机制：

1. **State 自持有 `Messages []Message`**（`adk/react.go:40-53` `type State struct`）
2. **每次 ChatModel/ToolNode 调用都把消息 append 进 `state.Messages`**
   （`adk/wrappers.go:649,719`）
3. **State 整体进 checkpoint**——gob 编码持久化（`schema.RegisterName[*State]`）
4. **Resume 时 State.Messages 自动恢复**，Graph 从中断节点边界继续跑
5. **用户答案通过 `HistoryModifier` 修改 `state.Messages`**
   （`adk/chatmodel.go::ChatModelAgentResumeData.HistoryModifier`，
   通过 `compose.WithStateModifier` 注入）—— tool_result 直接 append 进去

### LangGraph 的 create_react_agent

2 节点版本：`agent`（LLM 调用）+ `tools`（工具执行），通过条件边连成循环。
`AgentState.messages` 字段配 `add_messages` reducer 自动累积。`interrupt()`
原语：第一次抛异常，Resume 后第二次同位置直接返回 resume 值。

### 共同点

两家都把 ReAct 实现为 Graph：**循环不在代码里、在拓扑里**；**状态不在栈上、
在 State 里**。每次跨节点都是可中断/可存档的边界，HITL 是 Graph 引擎天然能力。

## 决策方向

把 BasicAgent 的内部 ReAct 循环改写为 Graph（`agent/react.go::reactLoop` 整体废除）。

最小拓扑（2 节点版，对齐 LangGraph）：

```
START → [model] ─→ branch ─→ [tools] ─┐
                    │                 │
                    └→ END             └─→ model（循环回边）
```

也可考虑 3 节点版（`init/model/tools`，对齐 eino），把"消息预处理"独立成节点。
**初步倾向 2 节点**——init 阶段在 crew-go 现状里逻辑很轻（system prompt
拼装 + memory 注入 + skill 工具注入），并到 model PreHandler 即可，不必单拎节点。

### State schema

```go
type basicAgentState struct {
    Messages    []*schema.Message  // 累积消息流，gob 可序列化
    Round       int                // 当前轮次（替代 for 循环计数）
    PendingCall *schema.ToolCall   // 中断时挂起的 tool_call（用于 Resume 恢复 tool_result 配对）
    // memory / skill / runID 等运行期辅助字段
}
```

`PendingCall` 是关键——Resume 注入 UserInput 时知道往哪个 ToolCallID 配对。

### Hooks 映射

现有 9 个 hook 时机映射到 Graph 节点：

| 当前 hook | 新位置 | 触发频次 | 备注 |
|---|---|---|---|
| `OnAgentInit` | START 之后第一个进入 model 节点前的一次性钩子（或独立 Init 节点） | 整个 BasicAgent.Run 一次 | 当前 `react.go:38` 在 reactLoop 入口调一次，先于 round 0 |
| `OnAgentStart` | model 节点 PreHandler，每次进入都触发 | 每轮 round | 与 OnAgentInit 区分关键：本 hook 每轮都调 |
| `OnModelStart` | model 节点 PreHandler（在 OnAgentStart 之后） | 每轮 round | 不变 |
| `OnModelEnd` | model 节点 PostHandler（同步路径） | 每轮 round（同步） | 不变 |
| `OnModelEndStream` | model 节点 PostHandler（流式路径） | 每轮 round（流式） | **当前无调用点**（待 StreamAgent，详见 `hook/hooks.go:52`）。Graph 化后保持"接口存在但调用点 TBD"状态，等 StreamAgent 主线落地再接 |
| `OnToolStart` | tools 节点 PreHandler，per tool_call | 每个 tool_call | 多 tool_call 时按 call 顺序触发 |
| `OnToolEnd` | tools 节点 PostHandler，per tool_call | 每个 tool_call | 同上 |
| `OnAgentEnd` | END 退出前的统一钩子（无论从 model→END 还是 branch→END） | 整个 BasicAgent.Run 一次 | 所有退出路径都要触发，包括 InterruptError 抛出时（保留 defer 配对语义） |
| `OnError` | 各节点错误分支的单点上报 | 按错误分布 | 当前 5 处调用点（memory_read / memory_write / model_error / tool_bind_error / tool_error）按节点拆 |

#### 关键约束

- **`OnAgentInit` vs `OnAgentStart` 必须严格区分**：前者整个 Run 一次，后者每轮一次。
  Graph 化后建议在 `basicAgentState` 加 `Initialized bool` 或独立 Init 节点（only-once
  路径），避免 model 节点 PreHandler 误把每轮触发当成 init
- **`OnAgentEnd` 配对保证**：当前 `react.go:215-218` 用 defer 保证所有退出路径都触发。
  Graph 化后 InterruptError 从 tool 抛出会让 Graph 直接停在 tools 节点内，
  END 钩子触发不到——必须在 Graph 引擎层加"InterruptError 也触发 OnAgentEnd"
  特殊路径（或在 BasicAgent 包装层 defer 兜底）
- **`OnError` 5 个调用点重新分布**：
  - `memory_read_error` → Init 节点的 Memory.Read 路径
  - `memory_write_error` → END 钩子前的 Memory.Write 路径
  - `model_error` / `tool_bind_error` → model 节点错误分支
  - `tool_error` → tools 节点错误分支

**hook 时机文档要重写**——本次 Graph 化后原文档里的"OnModelEnd vs OnAgentEnd
区别"等具体描述全部需要更新。CLAUDE.md 的"Hook 选择速查"section 也要重做。

### Session 与 State 的关系（重新对齐）

ISSUE-44 已经把 session 拆成两个角色：

- **LLM 输入构建**：`session.SelfHistory(name)`——本 agent 自产事件，决定 prompt
- **跨 agent 审计 / cross-agent rewrite**：`session.HistoryFor(name)` + `rewriteCrossAgentMessage`

BasicAgent Graph 化后：

- **Graph State.Messages 是真相源**：每轮 LLM/tool 输出 append 进去
- **session 仍记审计事件**：`OutputEvent` / `ExitEvent` / 跨 agent 事件——但 BasicAgent
  不再依赖 session 重建 prompt
- BasicAgent.Run 入口处理：从 session 取跨 agent 上下文（保留 ISSUE-44 行为），
  作为 State 初始 Messages 的前缀；**本 agent 历史不再走 session 重建，State 自带**

这条要小心——多次 Run 同一 BasicAgent 在同一 session 下时，第二次 Run 怎么拿到
第一次 Run 的本 agent 历史？两条路：

1. 复用上次的 State（要求外层框架持有 BasicAgent 的 State 跨 Run）
2. session 仍然记完整事件，BasicAgent.Run 入口从 session 重建 State 初值

后者保留现行多 Run 串联语义，前者更接近 eino 但需要外层主动管理 State 生命周期。
**初步倾向 2**——保持 BasicAgent.Run 接口幂等性，State 是单次 Run 的内部状态。

### 与 PlanAgent / GraphAgent 嵌套的耦合

**这是本 issue 的核心难点**，也是为什么必须挂 ISSUE-48 一起做：

PlanAgent.execNode 调 BasicAgent.Run 时，BasicAgent 内部跑了一个 Graph。如果
BasicAgent 内部 Graph 在 ToolNode 抛 InterruptError：

- **当前路径**（GraphAgent 黑盒嵌套）：外层 PlanAgent 的 Graph 视 BasicAgent 为
  黑盒节点，外层 checkpoint **看不到** BasicAgent 内部 Graph 的 State——内层
  State.Messages 又一次丢失，回到原 bug
- **需要的路径**（ISSUE-48 嵌套 checkpoint 子集）：外层 checkpoint 持久化时把
  内层 BasicAgent Graph 的 State 一并存进 Metadata；Resume 时按层级恢复

所以 ISSUE-59 落地必须配套 **ISSUE-48 的最小子集——嵌套 checkpoint 能力**。
state binding（typed state 字段共享）和事件透传（OrchHooks 看到嵌套节点）
可以暂缓，但嵌套 checkpoint 是硬依赖。

## 与 ISSUE-48 的耦合点（重启该 issue 的最小子集）

ISSUE-48 当前 🔴 暂缓，6 个未决问题（A-F）和 3 处歧义都没拍板。本 issue 触发了
它的"重启条件之一"（HITL 闭环要求嵌套 checkpoint）。

需要从 ISSUE-48 提取的最小能力：

| ISSUE-48 能力 | 本 issue 需要？ | 原因 |
|---|---|---|
| 事件透传（OrchHooks 看到 inner/stepA） | ❌ 不强求 | HITL 闭环不依赖；保留为 ISSUE-48 完整版的内容 |
| **嵌套 Checkpoint** | ✅ **必须** | 内层 State 跨 interrupt/resume 保留 |
| State 双向绑定（typed state 字段共享） | ❌ 不强求 | BasicAgent 不需要透传 typed state，只需保自己的 Messages |

ISSUE-48 审核里的 6 个问题中，与本 issue 直接相关的：

- **D. 超步 × 子图 × 中断的三层嵌套 checkpoint** —— 必须拍板，这是嵌套
  checkpoint 的核心场景
- **E. `WithSubCheckpoint(false)` 的语义陷阱** —— 嵌套 checkpoint 启用 / 禁用
  的边界要明确（默认启用即可，不需要给 BasicAgent 暴露关闭选项）

A、B、C、F 都和 state binding 相关，本 issue 不需要先拍板。

## 包结构调整（依赖反转，对齐 eino）

crew-go 当前依赖方向 `agent ← compose` 与 eino / LangGraph 完全相反——后两家都是
"图引擎在底层、agent 在上层依赖图引擎"。BasicAgent Graph 化要求 agent 依赖图引擎，
当前方向直接撞循环依赖。

**决策**：彻底反转，对齐 eino。包名保留 `compose`（与 eino 一字不差）。

### 新依赖方向

```
schema ← model/tool/hook/skill/mcp ← compose ← agent
                                       ↑          ↑
                                   纯图引擎   所有 agent 形态
```

### 包内容调整

| 当前位置 | 新位置 | 内容 |
|---|---|---|
| `compose/graph.go` `state.go` `checkpoint.go` `resume.go` `interrupt*.go` `helpers.go` `event_*.go` | **`compose/` 不变** | 纯图引擎：Graph、State、Checkpointer、ResumeContext、NodeFunc、OrchestrationHooks |
| `compose/plan.go` `llm_planner.go` `sequential.go` `parallel.go` `routing.go` `loop.go` `handoff.go` `reflect.go` `graphagent.go` 及对应测试 | **`agent/`**（同包多文件，对齐 eino adk） | 编排类全部搬到 agent，与 BasicAgent 并列 |
| `agent/`（现状不动） | **`agent/`** | BasicAgent / Session / Memory / Skill 集成 + 新增的编排 |

### 与 eino adk 的对应关系

| crew-go 新结构 | eino |
|---|---|
| `compose/`（纯图引擎） | `compose/` |
| `agent/`（所有 agent 形态） | `adk/` |

### 改动量

| 改动 | 范围 | 性质 |
|---|---|---|
| compose 包瘦身：编排文件搬到 agent | 9 个编排 .go + 它们的 _test.go | 机械搬家 |
| 编排类对 BasicAgent / Session 的依赖：现在变成同包内引用 | import 路径自然消失 | 自动解决 |
| 现 compose 包对 agent 的反向 import | 全部消失 | 循环依赖隐患根除 |
| 用户 / example 代码 | 现 `compose.PlanAgent` 等改成 `agent.PlanAgent` | 项目未上线，破坏面无所谓 |
| 真正的 ISSUE-59 工作（BasicAgent Graph 化） | 反转完成后再开始 | 与本节解耦 |

依赖反转本身机械化、低风险——主要是搬家+import 重写。真正的智力难点（嵌套
checkpoint、Graph 化 ReAct、hook 时机映射）和反转无关，反转后做。

## 实现拆解

### Phase 0：依赖反转（前置）

1. `compose/` 编排文件（plan / llm_planner / sequential / parallel / routing /
   loop / handoff / reflect / graphagent + 对应 _test.go）搬到 `agent/`
2. 搬过来的文件去掉 `package compose` 改 `package agent`，删掉 `crew-go/agent`
   的 import（同包内引用），其他 import 调整
3. example/ 代码（plan_chat_web、plan_chat、basic_chat 等）import 路径调整：
   `compose.PlanAgent` → `agent.PlanAgent`
4. compose 包内对 agent 的反向 import 全部消失（循环依赖根除）
5. 跑全量测试验证零功能变化
6. 独立 PR / commit，纯结构调整不带任何功能改动

### Phase 1：序列化类型注册 + 嵌套 Checkpoint 能力（ISSUE-48 子集）

1. **类型注册表（Q2 拍板）**
   - 新增 `compose/serialization.go`：`RegisterType[T](key)` + `typeRegistry` /
     `reverseRegistry` 双向 map
   - 序列化 schema：`{"type": <key>, "value": <json>}`，interface 字段嵌套同样格式
   - 替换 `compose/checkpoint.go` 现有 `json.Marshal` 路径——所有持久化走带类型
     标签的格式
   - ISSUE-40 `AgentAction.CustomizedAction` 的 `json:"-"` 跳过路径同步移除
2. **嵌套 checkpoint**
   - `compose/checkpoint.go` Metadata 加 `subgraph_checkpoint` 子区，存内层
     Graph 的完整 checkpoint payload（已含类型标签机制）
   - 父 Graph 的节点函数检测内层抛 InterruptError 时，把内层 checkpoint 嵌入
     父 Metadata
   - ResumeGraph 检测 Metadata 含 `subgraph_checkpoint` → 先 ResumeGraph 子图，
     子图返回后再继续父
3. 文档：`runtime.md` 补嵌套 checkpoint 协议 + 类型注册使用规约

### Phase 2：BasicAgent 内部 Graph 化

1. `agent/basicagent_graph.go`（新增）——build 内部 ReAct Graph（model + tools
   2 节点 + 循环边）
2. `agent/state.go`（新增）——`basicAgentState{Messages, Round, Initialized,
   PendingCall}` + `StateFactory` + 类型注册（Q2）
3. `agent/react.go::reactLoop` 重写为薄壳：从 session.SelfHistory 重建初值（Q3）
   → build Graph + RunGraph → 把当前 for 循环逻辑分散到节点 PreHandler/PostHandler
4. **Hook 映射**：9 个 hook 按 Q4 表挂到 Graph 节点边界（不合并 OnAgentInit /
   OnAgentStart）
5. **Session 写入修复（Q3 关键）**：model PostHandler 写 OutputEvent(assistant
   message)；tools PostHandler 每个 tool_call 写 OutputEvent(tool_result)——
   彻底解决"tool-call 中断分支不写 session"
6. `runToolCalls` / `executeToolCall` 拆成 Graph ToolNode 的实现细节
7. **ResumeContext 处理**：BasicAgent Graph 的 model 节点 PreHandler 检测
   `compose.GetResumeContext` + `state.PendingCall`，构造 tool_result 配对
   append 进 `state.Messages`，跳过当轮 LLM 调用直接进入"假装 LLM 输出已经
   在 state 里"的下一轮逻辑
8. **InterruptError 兜底 OnAgentEnd**：BasicAgent 外层包装 RunGraph 时 defer
   OnAgentEnd 触发，保证 hook 配对契约不破

### Phase 3：测试迁移与回归

1. `agent/react_coverage_test.go` 6 条 InterruptError 立即挂起回归——黑盒
   测试基本不用动，白盒（如 boomModel + 直接构造 Config 调 reactLoop）需要
   重写为通过 BasicAgent 入口
2. ISSUE-58 的 `TestPlan_Resume_AskUserTool_EndToEnd` 移除 t.Skip，断言全过
3. session 多 Run 串联场景的回归——确保 ISSUE-44 的 SelfHistory 解耦语义
   仍然成立
4. 跨 agent rewrite 的回归（ISSUE-43）——cross-agent 改写仍能正确执行

### Phase 4：ISSUE-58 doc / CLAUDE.md 同步

1. `doc/issue/issue-58-interrupt-timing-semantics.md`：
   - 修正"端到端冒烟覆盖"的描述——原仅 Planner-OnAgentStart-hook 路径
   - 标记 ask_user 端到端通过本 issue 闭环
2. CLAUDE.md：
   - "Hook 选择速查"section 重写——Graph 化后 hook 时机变化
   - 包依赖速查表更新——BasicAgent 现在是 Graph 实现
   - 已知坑（react.go executeRound 那条）整段删除或改写

## 关键决策（已拍板）

### Q1：BasicAgent 内部 Graph 是否复用 compose 包？

**已拍板**：彻底反转依赖方向，对齐 eino。详见上文"包结构调整"小节。

要点：
- compose 包名保留（与 eino 完全一致）
- compose 瘦身为纯图引擎；编排（PlanAgent / SequentialAgent / ...）全部搬到 agent
- 新方向：`compose ← agent`，循环依赖根除
- Phase 0 先做依赖反转作为前置，再启动 Phase 1+ 的功能改造

### Q2：State 的序列化方案

#### 三方实际方案对比

| 框架 | 实际实现 | 备注 |
|---|---|---|
| **eino** | **JSON + 类型标签注册**——`internal/serialization/serialization.go` 用 sonic JSON + reflect 走自定义 schema（`Type/JSONValue/StructType` 字段），通过 `RegisterName[T](key)` 把类型名注册到全局 map | 我前文误读："gob" 仅是 v0.7/v0.8 兼容代码，新版本不用 |
| **LangGraph** | **JsonPlusSerializer + msgpack**——schema 是 JSON 风格的"类型标签 + 值"，wire format 用 ormsgpack（msgpack 二进制变体）。pickle 仅作为 fallback | `langgraph/checkpoint/serde/jsonplus.py` |
| **crew-go 现状** | 纯 JSON，无类型标签——`compose/checkpoint.go` 直接 `json.Marshal` | ISSUE-40 已确认这是 gap：碰到 interface 字段（如 `AgentAction.CustomizedAction any`）丢类型，部分用 `json:"-"` 跳过 |

**两家共同结论**：都不是纯 gob，都不是纯 JSON——都是 **JSON 风格 schema + 类型标签注册**机制。

#### 拍板方案：JSON + 类型标签注册（对齐 eino RegisterName）

不引入 gob，不引入 msgpack。在 crew-go 现有 JSON 基础上补**类型注册表**：

```go
// 新增 compose/serialization.go（或 schema/serialization.go）
package compose

var typeRegistry = map[string]reflect.Type{}
var reverseRegistry = map[reflect.Type]string{}

// RegisterType 把类型名注册到全局表，支持序列化时打类型标签、反序列化时反查具体类型。
// key 推荐 "<package>_<TypeName>" 格式避免撞车（如 "crew_basicAgentState"）。
func RegisterType[T any](key string) error {
    t := reflect.TypeOf((*T)(nil)).Elem()
    for t.Kind() == reflect.Ptr { t = t.Elem() }
    if existing, ok := typeRegistry[key]; ok {
        return fmt.Errorf("key %q already registered to %s", key, existing.String())
    }
    if existing, ok := reverseRegistry[t]; ok {
        return fmt.Errorf("type %s already registered to %q", t.String(), existing)
    }
    typeRegistry[key] = t
    reverseRegistry[t] = key
    return nil
}
```

**序列化 schema 风格**（参考 eino）：

```json
{
  "type": "crew_basicAgentState",
  "value": { "Messages": [...], "Round": 2, "PendingCall": {...} }
}
```

interface 字段值同样按这套 schema 嵌套：interface 位置存 `{"type": ..., "value": ...}`，
反序列化时按 type 反查具体类型再 unmarshal。

#### 实施位置

- 类型注册表本身放 **`compose/`**（图引擎层基础设施）
- BasicAgent 的 State 类型在 `agent/` 包内 `init()` 时调 `compose.RegisterType[*basicAgentState]("crew_basic_agent_state")`
- 各编排（PlanAgent 等）需要持久化的 typed state 同样注册
- ISSUE-40 当前 `AgentAction.CustomizedAction` 的 `json:"-"` 跳过路径可以移除——补完类型标签机制后能正确反序列化

#### 时序

类型标签机制是 ISSUE-59 Phase 1（嵌套 checkpoint）的硬依赖——嵌套 checkpoint 必然
涉及 interface 字段（内层 State 是 `any` 类型）。**并入 Phase 1 实施**，单独立 commit。

### Q3：BasicAgent.Run 跨 Run 的状态语义

#### 三方实际方案对比

| 框架 | 跨 Run 语义 | 实现细节 |
|---|---|---|
| **eino** | **不感知前一次 Run**。每次 Run 都 `newBridgeStore()` 创建新 store；跨 Run 历史用户外部维护，下次 Run 时拼到 input 里 | `adk/chatmodel.go:993` |
| **LangGraph** | **同 thread_id 自动从 checkpoint 恢复**。客户端只发新一条消息，框架按 thread_id 加载 checkpoint 还原历史 messages 自动累积 | `RunnableConfig{configurable: {thread_id: ...}}` |
| **crew-go 现状** | 用 `RunSession` 作为跨 Run 边界，BasicAgent.Run 内部从 `session.SelfHistory(name)` 重建消息历史——**接近 LangGraph 模式**（session ≈ thread） | `agent/react.go:77` |

#### 拍板方案：保持 LangGraph 模式 + 修复底层 session 写入

crew-go 现状已经是 LangGraph 风格，改成 eino 风格会破坏 BasicAgent.Run 现有接口
语义和所有依赖 session 自动维护历史的代码（plan_chat_web、PlanAgent 多步等）。
**保留现有跨 Run 语义，但修复底层机制**：

##### 三条底层调整

1. **单次 Run 内 State.Messages 从 session.SelfHistory 重建初值**

   BasicAgent 内部 Graph 的 Init 阶段（model 节点首次 PreHandler，Round==0）：

   ```go
   func initState(ctx, sess, input) *basicAgentState {
       msgs := []*schema.Message{}
       if systemPrompt != "" { msgs = append(msgs, schema.SystemMessage(...)) }
       msgs = append(msgs, sess.SelfHistory(name)...)   // 跨 Run 历史
       msgs = append(msgs, input)
       return &basicAgentState{Messages: msgs, Round: 0}
   }
   ```

2. **单次 Run 结束时把累积消息回写 session**——解决 ISSUE-58 ask_user 端到端 gap 的根本

   现状：`react.go` 仅在 `emitFinalAnswerEvents`（最终答案）和 MaxRounds 耗尽两处
   写 session。**tool-call 中断分支彻底不写**——这是 ask_user 端到端不通的根因。

   Graph 化后改为 **per LLM/tool 调用都写**：
   - model 节点 PostHandler 写一条 `OutputEvent(assistant message)`——含 tool_call
     时也写
   - tools 节点 PostHandler 每个 tool_call 写一条 `OutputEvent(tool_result)`
   - 即使后续 round 抛 InterruptError 中断，前面已写的事件已落 session，
     下次 Run 重建 SelfHistory 时能看到完整 tool_call/tool_result 链

3. **Resume 走独立路径，从 checkpoint 恢复完整 State**

   `compose.ResumeGraph` 不走 session 重建——直接反序列化 checkpoint 里的 State
   （含 Messages 与 PendingCall），跳过 Init 节点。session 在 Resume 路径仅作审计
   流，不参与 State 恢复。

#### 二阶效应

ISSUE-58 端到端 gap 自动闭环：
- ask_user tool 抛 InterruptError 时，model 节点 PostHandler 已经把 `assistant(tool_call)`
  写进 session
- ResumeGraph 注入 `tool_result(callID, answer)` 到 State.Messages（已有逻辑）
- 节点续跑时 LLM 看到完整配对的 tool_call/tool_result——正常推理出最终答案

ISSUE-44 SelfHistory 解耦语义保持：跨 agent 视图仍然用 HistoryFor + rewriteCrossAgentMessage，
本 agent 视图直接读 SelfHistory；调整的只是"哪些事件被写入 session"，不改读逻辑。

### Q4：Hook 时机映射 & Memory / Skill 集成

#### 与 eino 的对照

| eino 概念 | crew-go 对应 | 备注 |
|---|---|---|
| `compose.WithStatePreHandler` / `WithStatePostHandler` | 节点 PreHandler / PostHandler | hook 直接挂这里 |
| `BeforeChatModel` / `AfterChatModel` | `OnModelStart` / `OnModelEnd` | 1:1 对应 |
| `BeforeAgent` / `AfterAgent` | `OnAgentInit` ≈ `BeforeAgent`；`OnAgentEnd` ≈ `AfterAgent` | eino 是 agent 级一次性，crew-go 的 `OnAgentStart` 每轮触发是自有设计 |
| `WrapToolCall`（tool middleware） | `OnToolStart` / `OnToolEnd` | eino 是 wrap 形式；crew-go 是前后回调，等价语义 |
| `BeforeModelRewriteState` / `AfterModelRewriteState` | 无对应 | eino 给 hook 直接改 state.Messages 的能力；crew-go 通过 `OnModelStart` 改 `input.Messages` 实现，已等价 |

#### 关键决策：保留 9 个 hook 不合并

eino 没有 `OnAgentInit` vs `OnAgentStart` 这种粒度区分，crew-go 当前每轮触发的
`OnAgentStart` 是**自有设计**——plan_chat_web 用它实现"用户主动中断"（OnAgentStart
读 pause flag 抛 InterruptError），合并就废掉这条产品功能。

**拍板**：保留 9 个 hook 不合并，在节点 PreHandler/PostHandler 层做精细分发：

| Hook | Graph 化后位置 | 实现要点 |
|---|---|---|
| `OnAgentInit` | model 节点 PreHandler，`state.Round == 0` 且 `state.Initialized == false` 时触发，触发后 set `Initialized = true` | 一次性钩子，靠 state 字段而非节点位置区分 |
| `OnAgentStart` | model 节点 PreHandler，每次进入都触发 | 每轮触发，含 Resume 后第一次进入也触发 |
| `OnModelStart` | model 节点 PreHandler，OnAgentStart 之后 | 1:1 移植 |
| `OnModelEnd` | model 节点 PostHandler（同步路径） | 1:1 移植 |
| `OnModelEndStream` | model 节点 PostHandler（流式路径） | 当前无调用点，Graph 化后保持接口存在但 TBD，等 StreamAgent 主线 |
| `OnToolStart` | tools 节点 PreHandler，per tool_call 触发 | 多 tool_call 时按 call 顺序触发 |
| `OnToolEnd` | tools 节点 PostHandler，per tool_call 触发 | 同上 |
| `OnAgentEnd` | END 退出前的统一钩子 + InterruptError 抛出时 defer 兜底 | 配对保证：包括 InterruptError 中断也要触发一次 |
| `OnError` | 各节点错误分支单点上报 | 5 个调用点按节点拆：memory_read（Init）/ memory_write（END 前）/ model_error / tool_bind_error / tool_error |

#### Memory / Skill 集成

- **Memory.Read**：Init 节点（或 model 节点 PreHandler 的 Round==0 分支），从
  Memory 读历史 prepend 到 state.Messages
- **Memory.Write**：END 退出前钩子（在 OnAgentEnd 之前），把 input + finalResp
  写入 Memory；失败上报 `OnError(memory_write_error)` 不中断主流程
- **SkillConfig**：build Graph 时检测 `cfg.SkillConfig != nil`，把 skillActivator
  作为 ToolNode 的内置工具注入；Graph 化后**不再在每次 Run 时动态创建**——
  Graph 是预编译的，工具集合编译期固定（与 eino ToolsNodeConfig 行为一致）

#### 顺序保证

节点内部 hook 顺序固定写死：

```
model 节点 PreHandler：
  1. OnAgentInit（仅 Round==0 且未初始化）
  2. Memory.Read（仅 Round==0）
  3. OnAgentStart
  4. OnModelStart

model 节点 LLM 调用 → 出 response

model 节点 PostHandler：
  1. OnModelEnd（同步）/ OnModelEndStream（流式）
  2. 写 session：OutputEvent(assistant message)
  3. 检查 tool_call：有 → tools 节点；无 → END

tools 节点 PreHandler（per tool_call）：
  1. OnToolStart

tools 节点 工具调用 → 出 result

tools 节点 PostHandler（per tool_call）：
  1. OnToolEnd
  2. 写 session：OutputEvent(tool_result)
  3. 回 model 节点

END 之前：
  1. Memory.Write
  2. OnAgentEnd
```

InterruptError 抛出时（任何节点）：
- Graph 引擎捕获后，BasicAgent 包装层 defer 触发 `OnAgentEnd`
- checkpoint 已含 state.Messages 持久化
- session 已含本轮已发生的 OutputEvent

## 不涵盖

- BasicAgent 暴露 Graph 拓扑给用户自定义（保持封装）
- Streaming 模式重新设计（保持现有 streaming 接口语义不变）
- 多 agent 协作的 Graph 拓扑变化（HandoffAgent / ParallelAgent 等仍用 compose 层 Graph）
- ISSUE-48 的 state binding（typed state 字段共享）—— 等真实需求再做
- ISSUE-48 的事件透传（OrchHooks 看到嵌套节点 ID）—— 等真实需求再做

## 实施风险

1. **依赖方向调整影响面广**：Phase 0 编排搬家会触及所有 import 路径。建议先
   写一个最小 PoC（model 节点 + tool 节点 + 一条循环边）跑通基本 ReAct，
   再做大规模迁移
2. **类型注册表全局状态**：`typeRegistry` 是包级全局 map，多次 init 注册顺序
   要保证幂等；漏注册的类型在反序列化时会报明确错误（不静默 nil）
3. **Session 写入语义变更**：Q3 修复"tool-call 中断不写 session"会让 session
   事件量变大（每轮 LLM/tool 都写）。需要评估对 ISSUE-40/44 的影响——
   ISSUE-44 的 SelfHistory / HistoryFor 路径不动，但事件总量增加可能影响
   跨 agent rewrite 的性能。Phase 2 实施时跑性能基线对比
4. **Hook 时机变更**：现有用户代码（如 plan_chat_web 的 OnAgentStart hook
   触发暂停）依赖 hook 在特定位置触发。Graph 化后 hook 时机文档要清晰，
   plan_chat_web 等示例要回归测试
5. **嵌套 checkpoint 性能**：每层 Graph 的 State 都进 Metadata，深嵌套时
   payload 可能爆炸。需要测试深度阈值，超过打 warn（参考 ISSUE-48 设计稿
   的 16 层建议）
4. **测试规模**：agent / compose 现有测试 100+ 条，Graph 化后白盒测试基本
   全要重写。建议留足时间做测试迁移，不能压缩

## 相关

- 阻塞：ISSUE-48 嵌套 checkpoint 子集（必须先做 Phase 1）
- 触发：ISSUE-58 ask_user 端到端 gap（详见
  [issue-58-interrupt-timing-semantics.md](issue-58-interrupt-timing-semantics.md)）
- eino 参考：`adk/react.go` / `adk/chatmodel.go` / `adk/wrappers.go`
- LangGraph 参考：[create_react_agent 文档](https://langchain-ai.github.io/langgraph/reference/prebuilt/#langgraph.prebuilt.chat_agent_executor.create_react_agent)
- 依赖方向：现行 `schema ← model/tool/hook/skill/mcp ← agent ← compose`
  （CLAUDE.md），本 issue 可能调整为 `runtime ← agent / compose`

## 优先级与排期

**优先级：高**——HITL 是已暴露的产品功能（plan_chat_web 已上线），ask_user
端到端不通是真实功能缺口。

**节奏建议**：

1. 先把 Q1（包结构）和 Q2（序列化）拍板，避免 Phase 2 动手后再大改
2. Phase 1（嵌套 checkpoint）单独立 PR，独立测试通过后再开始 Phase 2
3. Phase 2（BasicAgent Graph 化）作为大改，建议拉新分支独立 review，不直推 main
4. Phase 3 测试迁移与 Phase 2 同 PR 合
5. Phase 4 文档同步作为收尾 commit
