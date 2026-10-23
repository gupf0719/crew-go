# 上下文压缩（Context Compaction）

> crew-go 的上下文压缩是 4 层递进体系，**对标 Claude Code 设计**；通过分层从轻到重
> 的策略，在不丢失关键语义的前提下控制 token 增长。

## 目录

1. [设计目标与原则](#设计目标与原则)
2. [4 层架构总览](#4-层架构总览)
3. [第 1 层：MicroCompact 微压缩](#第-1-层microcompact-微压缩)
4. [第 2 层：AutoCompact 自动压缩](#第-2-层autocompact-自动压缩)
5. [第 3 层：ReactiveCompact 被动压缩](#第-3-层reactivecompact-被动压缩)
6. [第 4 层：StoreMemory（已实现 — RunSession + store.Store 接口）](#第-4-层storememory已实现--runsession--storestore-接口)
7. [实战参考：时序图 / Graph 衔接 / Marker / 排错](#实战参考时序图--graph-衔接--marker--排错)
8. [基础设施](#基础设施) (含 token 估算 / 消息分组 / 配对感知切割)
9. [配置与开关](#配置与开关)
10. [可观测性](#可观测性)
11. [与 LangGraph / eino 的对比](#与-langgraph--eino-的对比)
12. [FAQ](#faq)
13. [关键设计权衡（Round 6 审计沉淀）](#关键设计权衡round-6-审计沉淀)

---

## 设计目标与原则

**目标**：在长对话 / 多轮 tool 调用场景下，自动控制 token 增长，避免触发 LLM
provider 的 context window 限制（typical 200K），同时保留对话语义连续性。

**核心原则**：

1. **渐进式降级**：从无损（删旧 tool 输出）→ 有损但保结构（LLM 摘要）→
   抢救式（删消息组）。每层比上一层代价更高，只在必要时升级。
2. **API 不变量永不破坏**：任何切割都保持 `tool_use` ↔ `tool_result` 配对、
   `assistant` 消息内的 `thinking` 块完整性——否则下一次 LLM 调用直接 422。
3. **断路器防雪崩**：连续 N 次失败立即停止，不让上下文不可恢复时无限重试浪费 API。
4. **真实 token 优先——仅限观测上报**：`EstimateTokens` 会优先用 provider 返回的
   `Usage.InputTokens`，但那只喂给 compaction event。触发判定用纯字符估的
   `EstimateMessageTokens`（理由见 §第 2 层触发条件）。
5. **session 审计与 LLM 输入解耦**：压缩只影响下次 LLM 调用的 input，**不**改写
   `session.events` 历史（business audit trail 永久保留，跟 ISSUE-44 决策一致）。

---

## 4 层架构总览

```
┌─────────────────────────────────────────────────────────────────────┐
│ Model 节点入口 ──┬─→ [第 1 层 MicroCompact] ──→ 删旧 tool 结果      │
│                 │                                                   │
│                 └─→ [第 2 层 AutoCompact] ──→ token 超阈值时        │
│                       │                       LLM 摘要早期对话      │
│                       └─→ LLM 调用                                  │
│                              │                                      │
│                              └─→ [第 3 层 ReactiveCompact retry]    │
│                                  LLM 报 "prompt too long" 时        │
│                                  压缩 + 重试 MaxAttempts 次         │
│                                                                     │
│ [第 4 层 StoreMemory] ──→ store.Search 在 Run 入口注入到初始 msg       │
└─────────────────────────────────────────────────────────────────────┘
```

| 层 | 触发时机 | LLM 调用 | 信息损失 | 延迟 | 断路器 |
|---|---|---|---|---|---|
| 1. MicroCompact | **model 节点入口**（先于 AutoCompact） | 否 | 最小（只删旧 tool 输出） | <1ms | 无 |
| 2. AutoCompact | **model 节点入口**（MicroCompact 之后）、token 超阈值 | 是（生成摘要） | 中等（摘要替代早期对话） | 5-30s | 3 次失败停 |
| 3. ReactiveCompact | LLM 调用返回 prompt-too-long | 否（删最旧消息组重试） | 高（删最旧消息组） | ~0 | 3 次重试停（MaxAttempts，**非断路器**）|
| 4. StoreMemory | **preamble 节点入口**（ISSUE-74 修后只跑首轮）、`store.Search` 注入到初始 messages | 否 | 中等（用历史摘要） | <10ms | 无 |

**调度顺序（每轮 model 节点入口）**：
```
MicroCompact → AutoCompact → LLM 调用（含 ReactiveCompact retry wrap）→ tools → 跳回 model
```

---

## 第 1 层：MicroCompact 微压缩

**目标**：清理过期的、大块的 tool 调用结果，保留语义信息（tool_use 块仍在，只把
`tool_result` 内容替换为占位符），不调用 LLM。

### 触发时机

- **BasicAgent 内部 model 节点入口**（`agent_v2/basic_agent.go::makeModelFn` 最早段，**先于 AutoCompact**）
- 配置 `MicroCompactConfig.Enabled == true`（**v2 默认 false——需 `WithMicroCompact` 显式开启**）

> **历史**：v1 PoC 时代曾试图在 toolsFn 末段触发但实际死代码；audit Phase 5.2 Bug #2 修复时改在 modelFn 入口触发——modelFn 看到的 msgs 是累积历史，能正确清旧 tool_result。语义等价于"toolsFn 完成后清"。

### 可压缩工具白名单

```go
// agent_v2/microcompact.go
var DefaultCompactableTools = []string{
    "Read",       // 文件读取
    "Bash",       // Shell 命令输出
    "Grep",       // 搜索结果
    "Glob",       // 文件列表
    "WebSearch",  // 网页搜索结果
    "WebFetch",   // 网页内容
    "Edit",       // 文件编辑 diff
    "Write",      // 文件写入
}
```

**不在白名单**的 tool（如 Agent / Skill / MCP / 用户自定义）的结果**永远不被压缩**——
框架不知道它们的语义价值。

### 算法

```
1. 按 tool name **分组**收集所有可压缩 tool 的 tool_use ID（不是全局合并）
2. **每个 tool name 各自保留最近 N 个**（默认 KeepRecent=5）
   例：3 个 Read + 3 个 Bash + KeepRecent=5 → 全部保留（每 tool 5 个内）
3. 早于该 tool 第 N 个的 tool_result 内容替换为占位符：
   "[Old tool result content cleared by microcompact]"
4. tool_use 块完全保留（保持 API 配对完整性）
5. 入参已带 cleared / offloaded marker 的 tool_result 原样透传（BasicAgent 图路径下压缩产物不写回消息 channel，命中的只有业务直接喂进来的、与 AutoCompact 备忘套用之后 Head 里保下的清理过的结果——见「压缩结果的复用」）；offload 的跨轮 `store.Put` 去重由 ctx 内 `offloadDedup` 集合承担，同一 Run 内同一 (ns, key) 只写一次
6. 带 `SkillBodyAnnotation` marker 的 tool_result **跳过不清**
   （跨层契约保护：清了会让 skill body 被压成占位符 + AutoCompact 整组保留无意义占位符，R17 MED-2）
```

**关键不变量**：`tool_use` ↔ `tool_result` ID 配对**永不破坏**。我们只动 result 的
`Content` 字段，不删消息。

### 配置

```go
type MicroCompactConfig struct {
    Enabled          bool      // 默认 false——需 WithMicroCompact 显式开启
    KeepRecent       int       // 默认 5
    CompactableTools []string  // 默认 DefaultCompactableTools
    Placeholder      string    // 默认 "[Old tool result content cleared by microcompact]"

    // R6-M HIGH-2 补：production stable 但之前文档漏写——offload-to-store 模式
    // 让 tool_result 不直接替占位符，而是 store.Put 到外部 store + 留指针，
    // 业务后续可按指针回溯原始 tool_result。学 eino reduction.tool_result_offload 设计。
    OffloadEnabled bool         // 默认 false（纯占位符替换不变）
    OffloadNS      store.Namespace  // 空时默认 per-sid 隔离 store.NS("_micro_offload", sid)（sid 取 compose_v2.GetSessionID(ctx)）；ctx 无 sid 时才 fallback store.NS("_micro_offload")
}
```

**Offload 模式行为差异**：
- 默认 `OffloadEnabled = false`：纯占位符替换（同上 6 步算法）
- `OffloadEnabled = true` + store 已挂：tool_result 内容 `store.Put(ctx, OffloadNS, key=tcID, value=*schema.Message)` 然后 message Content 替换为 offload 指针（`[offloaded:<key>] preview: <前 200 字节，UTF-8 安全截断>`，见 `microcompact.go::cloneToolResultWithOffloadRef`）+ 打 `MicroCompactOffloadedAnnotation` + `MicroCompactOffloadKeyAnnotation`（存 key 供回溯）marker。业务后续可按 key 回溯原始 tool_result。**store 未挂时退化为纯占位符**（不报错）。
- 详见 [`agent_v2/microcompact.go::MicroCompactRun`](../../agent_v2/microcompact.go) `cloneToolResultWithOffloadRef` 路径。

**Offload 模式的两项业务责任**（启用 `OffloadEnabled` 前必读）：

1. **自管 cleanup**——offload 到 store 的数据**不**走 cp `DeleteSubtree` 自动清理（cp 清理只管 checkpoint，不碰业务 store）。长跑 / 高 QPS 场景 offload data 会在 store 持续累积，业务需自行清理（Run 结束后按 `cfg.OffloadNamespace(ctx)` 算出的 NS 用 `Search` + `Delete` 批量清；⚠️ 这个 ctx 必须带 sid（`compose_v2.WithSessionIDCtx`）——不带时它回落全局 `_micro_offload`，按前缀清会扫到所有会话的数据）。⚠️ 「给 store 后端配 TTL」这条路走不通：框架的 offload `Put` 不带 TTL、也不接收 option，内置后端都没有默认过期配置（InMemory / sql 本就不支持 TTL）——压缩产物的回收策略还没有决策，见中心 backlog（2026-09-18 核验）。默认 per-sid 隔离 NS 正是为了让业务能按 sid 批量清。
2. **LLM 重读需业务注册工具**——占位符 `[offloaded:<key>] preview: <前 200 字节>` 只给 LLM 看预览。想让 LLM 主动取回完整原始 tool_result，业务需注册一个类似 `read_offloaded_result(key)` 的工具，内部 `store.Get(ctx, cfg.OffloadNamespace(ctx), key)` 取回（ctx 同样要带 sid；key 从占位符文本或 `MicroCompactOffloadKeyAnnotation` marker 拿；**别用裸 `cfg.OffloadNS`**——默认档它为空，实际写入 NS 按 sid 派生）。注册之后把工具名填进 `MicroCompactConfig.OffloadReadToolName`，占位符会提示模型用它读回。⚠️ 两点（2026-09-18 核验）：持久后端（redis / sql）读回的是 `map[string]any`（JSON 往返）不是 `*schema.Message`，自己 decode；开了 `WithToolResultMaxBytes` 时存进去的已是截断版。框架**不内置**此工具——offload 默认假设"预览足够，极少需要回溯完整内容"。

### 不做的

- **不**调用 LLM 摘要
- **不**做缓存编辑路径（依赖 provider cache_edits API，初版仅做时间触发路径）
- **不**对自定义 tool 做白名单 fuzzy 匹配（用户必须显式加入 CompactableTools）

---

## 第 2 层：AutoCompact 自动压缩

**目标**：当历史 token 接近 model context window 时，主动触发 LLM 摘要，把早期
对话压成一段结构化总结，腾出空间继续对话。

### 触发时机

- BasicAgent 的 model 节点入口（`agent_v2/basic_agent.go::makeModelFn` 内
  MicroCompact 之后、LLM 调用之前）
- 配置 `AutoCompactConfig.Enabled == true`（默认 false——必须显式启用，因为需要
  用户配 `Model` 实例做摘要）
- 当前估算 token >= 阈值
- 断路器未触发（连续失败 < 3 次）

### 阈值计算

```go
// 有效上下文窗口 = model 上下文 - 摘要输出预留
// 内部常量（autocompact.go 未导出）：
// defaultAutoCompactSummaryMaxTokens     = 20_000  // p99.99 的摘要输出 ~17K tokens
// defaultAutoCompactSummaryBufferTokens  = 13_000  // 触发时距硬上限的安全缓冲

// 阈值通过 cfg.Threshold() 方法计算：
func (cfg *AutoCompactConfig) Threshold() int {
    smax := cfg.SummaryMaxTokens
    if smax <= 0 { smax = defaultAutoCompactSummaryMaxTokens }      // 0/负值回落默认常量
    sbuf := cfg.SummaryBufferTokens
    if sbuf <= 0 { sbuf = defaultAutoCompactSummaryBufferTokens }
    return cfg.ModelContextWindow - smax - sbuf
}
// 示例（200K 上下文, 两字段留 0 走默认）：200_000 - 20_000 - 13_000 = 167_000 tokens 触发
```

### 算法（**1 次** LLM 摘要调用）

**Call 1**：生成摘要

```
input:
  system: 原系统提示词（不动）
  user/assistant: 早期对话组 early（已剔除 tail 当前轮 + skill body + memory / offloaded 保护段；tool_use/tool_result 配对保留。
                  tail 从最后一条用户消息所在组起——框架以 user 角色追加的提示（后台任务通知、工具重复调用提醒、inline skill
                  正文，`IsFrameworkNote`）不是用户说的话，定 tail 时跳过，否则它们排在用户这一轮的提问之后，提问连同这一轮的
                  工具往返被摘进摘要，见 background-tasks.md §2.5）
  user: 抽取压缩提示词（NO_TOOLS_PREAMBLE + 9 段摘要要求）

output:
  <analysis>...</analysis>
  <summary>
  1. Primary Request and Intent: ...
  2. Key Technical Concepts: ...
  ... (9 章节)
  </summary>
```

**后处理**：

```go
// 1. 删 <analysis> 块（chain-of-thought 草稿，已无价值）
// 2. 提取 <summary> 内容
// 3. 重组 messages 为 6 段 (R7 audit HIGH-4 + HIGH-2 audit B + offload 孤儿 fix):
//    [system,
//     ...memoryInjected (含 StoreInjectedAnnotation 的旧消息),
//     ...protectedGroupsMsgs (splitProtectedGroups 按 API 轮**整组保留**：含
//       SkillBodyAnnotation 的 P1.C / P2.D inline body 组 + 含
//       MicroCompactOffloadedAnnotation 的 offload 占位符组——两者的 marker 都落在
//       tool_result 上，逐条抽会 orphan 配对的 assistant tool_call 致 OpenAI 422，
//       故整组保留保 pair 完整。offload 曾单独抽进 offloadedRefs 槽踩此坑，已并入),
//     "# Previous Conversation Summary\n\n..." (新生成 summary, UserMessage, 标 AutoCompactSummaryAnnotation),
//     "# Recently Read Files (re-injected after compaction)\n\n..." (如启用 attachment 重注入),
//     ...tail (触发位置之后保留的最近消息)]
// 顺序设计意图: memory 紧跟 system; 保护组 (skill body / offload 占位符整组) 在 memory 后
// 保激活/offload 语境连续; summary 在所有保护段后让 LLM 先看现状再看历史摘要
```

**Call 2 不在 AutoCompact 内**——AutoCompact 只调 1 次 LLM 生成摘要，重组后的 messages
返回给 BasicAgent ReAct 主循环，**主循环下一次正常 LLM 调用**才用上重组消息。所以
"两次 LLM" 是从业务整体视角说"压缩后还有正常 LLM 调用"，不是 AutoCompact 自身行为。

### 关键设计点

1. **`<analysis>` chain-of-thought**：要求 LLM 先输出思考草稿提高摘要质量，**事后
   删除**只保留 `<summary>`——不污染压缩后上下文
2. **NO_TOOLS_PREAMBLE 双放**：「禁用工具调用」前导词在提示词**前后各放一次**，
   防止 LLM 忽略尾部指令尝试调用工具浪费 maxTurns（Claude Code 实测 Sonnet 4.6+
   会忽略弱尾部指令）
3. **附件重注入**：压缩后下一轮 input 中重新注入近期读过的文件——压缩丢失了具体
   内容，重注入保证 LLM 还能引用它们。从压缩之前的原始消息流里挑，跳过 MicroCompact 这一轮会清掉的（同一个判据
   `microCompactToClear`，不把已经清掉的原文按预算注回去）与转存过的结果（正文只是说明 + 预览，见
   [tool-output-spill.md](tool-output-spill.md)）
3b. **转存的取回 id 不随摘要消失**：被摘要掉的消息里的取回 id 收成一份索引（id、工具名、大小、行数，最近 50 条）
   附在摘要后面、并记在摘要的标注上——下一次增量摘要时上一份索引接着带
4. **断路器**：连续失败 3 次后停止，不再尝试自动压缩本会话——避免上下文不可恢复
   时无限消耗 API（Claude Code 实测：1,279 个会话曾连续失败 50+ 次，最多 3,272
   次，每天浪费约 250K 次 API 调用）

### 配置

```go
type AutoCompactConfig struct {
    Enabled                bool                 // 默认 false（需显式启用 + 配 Model）
    Model                  model.ChatModel      // 摘要用 LLM（推荐用更便宜的 model）
    ModelContextWindow     int                  // 必填，model 的 context window 大小
    SummaryMaxTokens       int                  // 默认 20_000：阈值公式给摘要留的预算，也是摘要的上限（超过判无效）
    SummaryBufferTokens    int                  // 默认 13_000
    MaxConsecutiveFailures int                  // 默认 3
    // 成本守卫的两个开关（第三道是上面的 SummaryMaxTokens；见下方「成本守卫」小节）
    MinCompactableTokens   int                  // 默认 5_000：可压缩区（上一份摘要之外的）小于它就静默跳过
    MaxReductionRatio      float64              // 默认 0.8：摘要没比原文小够就判压缩无效（增量摘要时按新内容算）
    CustomInstructions     string               // 附加摘要指令（如"重点关注 TS 代码"）
    Language               AutoCompactLanguage  // 提示词语言；默认 CN
    SystemPromptOverride   string               // 摘要 Call 的 system prompt 自定义（空时回落见下方注）
    // 文件重注入（压缩后重新塞回最近读过的若干文件）
    ReinjectRecentFiles    bool                 // **默认 false——业务必须显式 true 启用**
    ReinjectMaxFiles       int                  // 默认 5
    ReinjectPerFileBudget  int                  // 默认 5_000 tokens（单文件上限）
    ReinjectTotalBudget    int                  // 默认 50_000 tokens（共预算）
    // 内部字段（业务不直接接触）：breakers *sync.Map / breakerMaxFailures int
    // ——per-session lazy 实例化避免跨 session 共享 breaker 污染
}
```

> **`SystemPromptOverride` 留空时的回落**：取**前导 system 块里第一条不带
> `StoreInjectedAnnotation` 的**（`firstNonInjectedSystem`）。不是「首个 system」——
> 不配 `WithSystemPrompt` 时排在最前的是**注入的长期记忆**，按下标取会把用户的隐私
> 当成摘要模型的 system prompt 发出去（实测：摘要 input 首条 = `system: 用户的信用卡尾号 4321…`）。
> 一条都不剩时不填，摘要调用就没有 system prompt。
>
> ⚠️ **agent 自己的 system prompt 不在这个块里**——它走 `BasicAgentPreambleOutput.System`
> 那条独立 channel，压缩层看不到（见下节）。所以这条回落实际能取到的只有「调用方自己
> 塞在最前的 system」。

### 提示词模板

提示词的英中两版完整内容随实现版本一并维护在 `agent_v2/autocompact_prompts.go`。
关键约束：

- **CRITICAL: TEXT ONLY**（禁用所有工具）
- **9 段结构化输出**：Primary Request / Key Technical Concepts / Files & Code Sections /
  Errors and fixes / Problem Solving / All user messages / Pending Tasks /
  Current Work / Optional Next Step
- **`<analysis>` + `<summary>` XML 块格式**（程序化解析依赖此格式）
- **CustomInstructions** 变量在末尾插入（用户自定义摘要要求）

### 压缩结果的复用：单次 Run 内的备忘与跨 Run 的压缩边界

**问题**：压缩只作用于喂给模型的那一份、不写回消息流（消息流里留的是完整原文，会话历史、工具账都靠它）。于是
越过阈值之后，下一轮读出来的仍是原始全量、仍超阈值，又摘要一次。实测一次 Run 里 200 轮工具调用（窗口 64k、
每轮工具结果约 4KB）：摘要被调 189 次，摘要输入累计 77.7MB 字符，随轮数平方增长。跨 Run 同理：关掉轮数截断
（`WithMaxHistoryPairs(0)`）、靠摘要控制上下文的会话，每次 Run 的 prior 是整场历史，第一轮先把它整个摘要一遍。

**单次 Run 内：备忘**（`agent_v2/compaction_memo.go`）。真摘要成功时把结果记成一份备忘，放进 BasicAgent 图的
一条 channel（`basicCompactionCh`，随存档走，续跑接着用）：「消息流的前 Upto 条由 Head 这几条代表」，Head 是那次
压缩输出里尾部之前的部分（前导 system、长期记忆注入、受保护的组、摘要、文件重注入）。每轮在 MicroCompact
**之前**先把前 Upto 条换成 Head，再照常跑 MicroCompact / AutoCompact：没超阈值就直接用；又超了就在「Head +
之后新增的」上再摘要一次，上一份摘要成了这次的输入。消息流本身一条不改。

- **只在前缀没变时套用**：前缀逐条送给模型的全部内容进指纹（`prefixAnchor`：角色、ID、正文、推理文本、拒答、
  内容块——工具调用的名字与参数、工具结果、图片等），`OnModelStart` 钩子插入、删除、换掉或改了其中任何一处（比如
  给历史里某次调用的参数脱敏）都让备忘作废，这一轮照旧在全量上压缩。标注不进指纹：它不发给模型。
- **套用之后显式告诉 AutoCompact Head 在哪**（`compactCut`）：Head 与新增部分分开分组，尾部只在新增部分里找；
  上一次的文件重注入丢掉，这次从套用之前的原始消息流重新倒扫。不这样的话，Head 里的摘要与重注入都是 user 角色，
  会被尾部规则当成「用户的最新一轮」——Head 里有受保护的组或开了重注入时，摘要一次之后就再也压不动（60 轮工具
  调用，模型输入涨到 20 万字符，`err == nil`）。
- **钩子留下的 nil 先去掉**：`OnModelStart` 钩子往消息列表里留了 nil 时，压缩分组会跳过它、备忘的 Upto 却把它算进去，
  套用之后模型输入里出现找不到声明者的 tool_result。钩子改写之后先去掉 nil，下面每一层都不必再防。
- **重注入从原始消息流挑**：套用备忘之后，文件重注入照样从套用之前的原始消息流倒扫，跳过 MicroCompact 这一轮会清
  掉的与转存过的（见上文 AutoCompact 第 3 条）。
- 同样的 200 轮任务：摘要 18 次、摘要输入 0.8MB；喂给模型的上限不变。不变量：**同一个 Run 里模型输入有界、
  不随轮数涨，跟 Head 里装了什么无关**。

**跨 Run：压缩边界**（`agent_v2/compaction_boundary.go`）。某一轮真摘要了一次、且 `OnModelStart` 钩子这一轮
没动消息列表（钩子插入或改写的内容不该被当成历史重放），模型节点成功时往会话历史追加一条边界记录：带着这一刻
的完整替代历史（受保护的组 + 摘要 + 尾部原文，去掉每次 Run 都会重新算出来的长期记忆注入与派生输入），盖本
agent 的归属与视图模式，正文是摘要，标注 `HistoryCompactionAnnotation`。读 prior 时往前读到本 agent、同视图的
最新一条边界就停：prior = 替代历史 + 边界之后的记录（别的 agent 的、别的视图的、解不出来的当它不存在）。原始
记录一条不动，历史仍是 append-only；第 0 层截断把摘要当受保护消息（它代表边界之前的全部对话）。1000 轮的
会话：每次 Run 从读 4008 条、摘要两次（每次输入 3.8MB）降到读约 190 条、不重新摘要。读历史这一侧见
[usage-guide.md](usage-guide.md) 会话历史节。

**代价与限制**（开放项见 [`doc/code/todo.md`](../code/todo.md)）：

- 边界的体量：替代历史带着尾部原文，实测 60 轮的会话里边界 JSON 约占原始记录的 15%；受保护的组（skill 正文）
  每条边界复制一遍。
- 替代历史经 JSON 往返：内存后端下一个 Run 读回时，数字型 annotation 变成 float64（持久后端本来如此）；框架
  自己的读点都不受影响。
- 指纹不含标注：钩子只改标注（它不发给模型，只影响压缩时哪些组受保护）时备忘照用。

---

## 第 3 层：ReactiveCompact 被动压缩

**目标**：当 LLM 调用返回 "prompt too long" 错误时，回退式压缩并重试——这是
AutoCompact 没拦住的兜底路径。

### 触发时机

- LLM 调用（`agent_v2/basic_agent.go::makeModelFn` 内 `callLLM` retry 循环包围 `streamOrGenerate` 单次调用）返回 prompt-too-long 类错误
- 配置 `ReactiveCompactConfig.Enabled == true`（**v2 默认 false——需 `WithReactiveCompact` 显式开启**）
- 当前重试次数 < `MaxAttempts`（默认 3）

### 错误检测

不同 provider 的 prompt-too-long 错误格式不同。我们用字符串匹配（11 条精确短语）+ 正则解析出的 token 数字兜底（`ParsePromptTooLongGap(err) > 0` 也算命中）；**不看 HTTP 状态码**：

```go
// agent_v2/reactive_compact.go::IsPromptTooLongError
// promptTooLongMarkers 实际 11 条精确字符串：
//   "prompt is too long"          // Anthropic
//   "prompt too long"
//   "input is too long"           // 部分 provider
//   "context length exceeded"     // OpenAI 官方
//   "maximum context length"      // OpenAI 旧版
//   "too many tokens in"          // Azure
//   "context_length_exceeded"     // OpenAI error.code
//   "reduce the length of the messages"  // OpenAI suggestion 字串
//   "reduce the length of your messages"
//   "range of input length should be"    // 千问/百炼
//   "request too large for"              // 部分中文 provider
//
// **注意**：v2 显式删除了 "context window" / "exceeds maximum" 模糊匹配——
// 业务错容易误命中（如"context window exceeded the user's quota"）触发误压缩
```

### 算法

```
1. LLM 调用失败 → 检测是否 prompt-too-long（IsPromptTooLongError）
2. 调用 emergencyDropOldestGroups 删最旧消息组（v2 当前实现路径）
3. 用压缩后的 input 重新调用 LLM
4. 仍失败 → 步骤 2 再压缩一次
5. 达到 MaxAttempts → 报错给 caller，不再重试
```

> v2 当前实现走「删最旧消息组」路径；一组也丢不动时，先把超过 2000 字节的 tool_result 就地挖空（`3eebc447`，从整条消息流里挑）；不复用 AutoCompact 摘要——原 PoC 设计的
> `PreferAutoCompact` 优先逻辑未实现、也没决策（已进中心 backlog；详见 §关键设计点）。

### 「删最旧消息组」抢救算法

```
1. 隔离前导 system 消息（不删）；剩余 conv 部分进入分组
   > ⚠️ **这一步保护的不是 agent 的 system prompt**。`WithSystemPrompt`（含 skill hint /
   > skill body）与 `WithPromptTemplate` 的渲染结果走 `BasicAgentPreambleOutput.System`
   > 那条独立 channel，**压缩层拿到的切片里结构上没有它们**。这里剩下的是长期记忆注入
   > 与调用方自己塞在最前的 system。
   >
   > 这一步曾经**确实**是 agent system prompt 的唯一防线，而判据是**位置**——模板一旦
   > 渲染出 few-shot，真 sysPrompt 就排在它后面、当场降级成普通可丢组，实测被直接丢掉且
   > `err == nil`。对照与决策见
   > [three-way-comparison/context-compaction.md 结论五](../description/three-way-comparison/context-compaction.md)。
2. 用 GroupMessagesByAPIRound 把 conv 按 API 轮次分组
   （按 `Annotations["run_id"]` 分组：每条 user 开新组；同 RunID 的 assistant +
    tool_result 加入当前组——保 thinking + tool 配对）
   > **run_id 两路径都 stamp**：model 节点每轮生成 run_id，Stream 聚合与 Generate 降级
   > （`agent_v2/basic_agent.go`）都盖——非流式模型（Stream 不支持→降级 Generate）多轮历史
   > 同样按轮分组。若某路径漏 stamp，该模型全轮无 run_id 会退化成单组，`len(groups)<=1`
   > 让 AutoCompact/ReactiveCompact 静默不删。
3. 识别 droppable 组：跳过含 5 marker 的保护组 (R17 audit MED-2 + R7 audit HIGH-2 + HIGH-2 audit B fix):
   - `AutoCompactSummaryAnnotation` (= `CompactionSummaryAnnotationKey`)
   - `StoreInjectedAnnotation`
   - `SkillBodyAnnotation` (P1.C / P2.D inline body)
   - `MicroCompactOffloadedAnnotation` (offload 占位符——删了 offload key 丢失无法重读)
   - `MicroCompactClearedAnnotation` (MicroCompact 清除标记——删了 cleared marker 链破坏)
   删任一 marker 组都破坏跨层契约 (4 层一致防护否则 silent bug)
4. **全保护时不删返原 messages** (R21 audit HIGH-2 fix) — 让 callLLM retry 走外层
   失败路径, 避免破坏 SkillBody 配对协议. **不要兜底丢最旧组**
5. 用 caller（`ReactiveCompactRun`）经 `ParsePromptTooLongGap(callErr)` 解析出的 gap 入参决定删几组（本函数只收 `gap int`，自己不解析）
   有 gap：从最旧 droppable 组累加 token 直到 >= gap，删那么多组
   无 gap：删 `len(droppableIdx) * EmergencyDropPercent / 100` 组（默认 20%，至少 1）
6. 永远保留至少 1 个 droppable 组（dropCount = len-1 兜底）
7. 删后首条若是 assistant，前面补 user marker（默认 "[continued after context overflow]"，
   `UserMarker` 配置）——API 要求 user-first
8. 把 system 消息拼回最前
```

### 配置

```go
type ReactiveCompactConfig struct {
    Enabled              bool   // 默认 false——需 WithReactiveCompact 显式开启
    MaxAttempts          int    // 默认 3
    EmergencyDropPercent int    // 默认 20，无 gap 信息时删最旧 20% 组
    UserMarker           string // 默认 "[continued after context overflow]"
}
```

### 关键设计点

1. **抢救式删消息组**：ReactiveCompact 走「删最旧消息组」路径（`emergencyDropOldestGroups`），一组也丢不动时先把超过 2000 字节的 tool_result 就地挖空（`3eebc447`，从整条消息流里挑，不限受保护组）——不复用 AutoCompact 摘要。原 PoC 设计的 PreferAutoCompact 优先逻辑未实现。丢掉的组里带转存取回 id 的（转存过的结果、摘要附的索引），补一条索引消息放在最前——全文还在 store 里，不留 id 模型就取不回（见 [tool-output-spill.md](tool-output-spill.md) §2.6）
2. **API 不变量保护**：删消息组时按 `Annotations["run_id"]` 分组（user 开新组，同 RunID 的 assistant + thinking + tool_use 不分离）；首条 assistant 时补 user marker
3. **断路器**：MaxAttempts 是硬上限——避免压完还是太长时无限重试

---

## 第 4 层：StoreMemory（已实现 — `RunSession` + `store.Store` 接口）

v2 通过 `agent_v2.RunSession` + `store.Store` 接口实现"跨 Run 长期记忆"——
**preamble 节点入口**（ISSUE-74 修后 buildInitialMessages 不在 Run 入口同步调，已搬到 BasicAgent graph 内 preamble 节点跑只跑首轮）调 `store.Search` 注入历史摘要到初始 messages。

```go
ag := agent_v2.NewBasicAgent(
    agent_v2.WithModel(m),
    agent_v2.WithStore(store.NewInMemoryStore()),
    agent_v2.WithStoreMemory(store.NS("memories", "u1"), &agent_v2.StoreMemoryConfig{
        SearchLimit: 20,    // 每次 Run 入口最多注入 20 条历史
    }),
)
```

**关键不变量**：
- Store 注入的消息打 `StoreInjectedAnnotation` marker——AutoCompact 跳过不重摘 +
  ReactiveCompact `isProtectedGroup` 不删
- StoreMemory 是**跨 Run** 长期记忆，与 cp（per-Run 状态快照）正交
- 业务可实现 `store.Store` 接口接 Redis / Postgres / Vector DB / mem0 适配

### 与 Claude Code 第 4 层的差异

Claude Code 的第 4 层是**后台 Session Memory 提取机制**——每轮对话后渐进更新一份
"记忆摘要"，压缩时直接用现成 SM 替代 LLM 重新摘要（<10ms 延迟、质量稳定）。

v2 当前的"StoreMemory"层用 `store.Search` 在 Run 入口注入，**不是后台渐进提取**——
业务自管 store 写入策略（commitFinal 调 `store.Put`，落的是**最终 assistant 消息原文**，key = `<RunID>_assistant_<内容指纹>`）。未来若
引入"自动摘要写入"扩展，可在 AutoCompact 之前增加 StoreMemory Provider 优化路径：

```go
type StoreMemoryCompactProvider interface {
    GetSummary(ctx context.Context, sessionID string) (*Summary, error)
    LastSummarizedMessageID(ctx context.Context, sessionID string) (string, error)
}
```

---

## 实战参考：时序图 / Graph 衔接 / Marker / 排错

> 4 个层在一次完整 BasicAgent ReAct 循环中的「精确插入位置」，以及失败回退路径
> 与排错决策树。

### 一次 BasicAgent ReAct 完整时序

```
┌──────────────────────────────────────────────────────────────────────┐
│ User input → BasicAgent.Run → 内部 ReAct Graph (channel-based)       │
│                                                                      │
│  [preamble 节点] (ISSUE-74 修, 只跑首轮)                            │
│   buildInitialMessages —— 产出**两条流**                             │
│    ① 前言 → basicSysPreambleCh（压缩层看不到）                       │
│     - PromptTemplate 渲染结果（system + 可选 few-shot）              │
│     - system prompt（含 skill hint / skill body）                    │
│    ② 会话 → basicMessagesCh（正常参与四层压缩）                      │
│     - store.Search 注入（打 StoreInjectedAnnotation）← 第 4 层       │
│     - SelfHistoryCompacted(name) ← 跨 Run 复用上次 summary           │
│     - 本次 user input 追加                                           │
│   (业务回调抛 InterruptError 时 graph 内 SaveNS, Resume 自然续跑)    │
│                                                                      │
│  [Round 1]                                                           │
│   ╭─→ model 节点（makeModelFn）                                      │
│   │    ┌─ 第 1 层 MicroCompactRun（modelFn 最早段）                  │
│   │    │   按白名单（Read/Bash/Grep/Glob/WebSearch/Edit/Write...）   │
│   │    │   把旧 tool_result 内容替换为占位符                         │
│   │    │   保 tool_use ↔ tool_result 配对结构                        │
│   │    │   占位符消息打 MicroCompactClearedAnnotation                │
│   │    │                                                             │
│   │    ├─ 第 2 层 AutoCompactRun（MicroCompact 之后）                │
│   │    │   if EstimateMessageTokens(可压缩切片) >= Threshold:        │
│   │    │       1 次 LLM 调用生成 9 段结构化摘要                      │
│   │    │       summary 打 AutoCompactSummaryAnnotation               │
│   │    │       附件重注入 + breaker 记账                             │
│   │    │   压缩后的切片只喂给本次 LLM 调用、不写回 basicMessagesCh  │
│   │    │   真摘要成功时记备忘进 basicCompactionCh（随 cp，下轮先套用）│
│   │    │                                                             │
│   │    ├─ ChatModel.WithTools(toolkit) → LLM 实际调用                │
│   │    │   ╭─ chatModel.Stream 优先（chunk 实时投递 OnNodeEvent）    │
│   │    │   ╰─ 不支持流式时改调 Generate                              │
│   │    │                                                             │
│   │    │   if err is prompt-too-long:                                │
│   │    │   ╭─ 第 3 层 ReactiveCompactRun retry wrap                  │
│   │    │   │   parsePromptTooLongGap → 删最旧消息组（保 tool 配对）  │
│   │    │   │   msgs = 截断后；retry LLM 调用                         │
│   │    │   │   循环至成功 / MaxAttempts(默认3) 用尽真错              │
│   │    │   │   **本地 retry 循环内 curMsgs = compacted；不写回 channel** │
│   │    │   │   （下轮 model 节点 Get 拿原 messages + 上轮 LLM 输出）  │
│   │    │   ╰─ 重试期 chunk 流前端会重启（chunk 重发，前端需识别）    │
│   │    │                                                             │
│   │    └─ 出口：终止时 emit ExitEvent 到 session.Events             │
│   │       （preamble 的 buildInitialMessages 末尾 emit OutputEvent 含 input）   │
│   │                                                                  │
│   │   if no tool_call → END                                          │
│   │   else → tools 节点                                              │
│   │                                                                  │
│   ╭─→ tools 节点（makeToolsFn）                                      │
│   │    ┌─ 取 last assistant 全部 tool_call（N=1 inline/N>=2 并发）   │
│   │    ├─ 任一失败/中断→整节点 err、result 全丢、Resume 重跑全部     │
│   │    ├─ 每 tool_result append 到 messages channel                  │
│   │    │   （注：MicroCompact 不在此触发——下一轮 modelFn 入口才清）  │
│   │    └─ 跳回 model 节点                                            │
│   ╰────────────────────────────────────────────────────────╮         │
│                                                            │         │
│  [Round 2]                                                 │         │
│   ╰─→ model 节点（同上，先 MicroCompact 清上轮 tool_result）─╯       │
│                                                                      │
│  循环退出 → [postamble 节点] (ISSUE-75 修):                         │
│   commitFinal: 写 ExitEvent 到 session.Events + store.Put               │
│   (业务回调抛 InterruptError 时 graph 内 SaveNS, Resume 自然续跑)    │
│                                                                      │
│  循环退出条件:                                                       │
│   ① model 输出无 tool_call → 走 postamble → END                      │
│   ② WithMaxRounds 达上限 → 抛 MaxIterationsError → Run error        │
│   ③ ReactiveCompact 真错（MaxAttempts 用尽）→ Run 整体 error         │
└──────────────────────────────────────────────────────────────────────┘
```

### Graph 节点 hook 衔接表

每层 4 层压缩在 BasicAgent ReAct Graph 的精确触发位置：

| 层 | Graph 节点 | 触发函数 | 触发时机 | 失败影响 |
|---|---|---|---|---|
| 第 1 层 MicroCompact | model | `agent_v2/microcompact.go::MicroCompactRun` | **model 节点 makeModelFn 最早段（先于 AutoCompact）** | 不阻断；记 slog.Warn 即可 |
| 第 2 层 AutoCompact | model | `agent_v2/autocompact.go::AutoCompactRun` | model 节点 makeModelFn（MicroCompact 之后，LLM 调用之前） | breaker 记账；**不阻断主链路**；业务错 `slog.WarnContext` 后继续走原 messages（不 silent），InterruptError / ctx.Err 直接 return 阻断 |
| 第 3 层 ReactiveCompact | model | `agent_v2/reactive_compact.go::ReactiveCompactRun` | LLM 调用返回 prompt-too-long 时的 retry wrap | retry 用尽 → 真错 return；上层 graph_loop 走错误路径 → cp save |
| 第 4 层 StoreMemory | 跨 Run | `RunSession + store.Store` 接口 | **preamble 节点入口**（ISSUE-74 修后 buildInitialMessages 搬到 BasicAgent graph 内 preamble 节点跑只跑首轮）调 store.Search 注入到初始 messages | 业务自己负责 |

**执行顺序（每轮 model 节点入口）**：
```
MicroCompact → AutoCompact → ChatModel.WithTools(toolkit) → LLM 调用
  └─ 含 ReactiveCompact retry wrap（prompt-too-long 时压缩 + 重试 MaxAttempts 次）
→ tools 节点（无 MicroCompact，下轮 modelFn 入口才清）→ 跳回 model
```

> **跨 Run 顺序**：preamble 节点（ISSUE-74 修后只跑首轮）`buildInitialMessages` 先调 store.Search 注入（第 4 层），再进入第一轮 model 节点的 1-3 层执行序列。

### 跨层 marker 全表

5 个**有效**跨层协作 marker（下表另保留 1 行删除线 `SupersededEventAnnotation` 仅留删除痕、不计入）（[agent_v2/session.go](../../agent_v2/session.go) +
[agent_v2/autocompact.go](../../agent_v2/autocompact.go) + [agent_v2/microcompact.go](../../agent_v2/microcompact.go) +
[agent_v2/skill_tool.go](../../agent_v2/skill_tool.go)）：

| Marker key | 类型 | Producer | Consumer | 跨 Run 行为 |
|---|---|---|---|---|
| `StoreInjectedAnnotation` | bool | `buildInitialMessages` 给 store.Search 注入消息打 marker | （a）AutoCompact 跳过不重 summarize；（b）ReactiveCompact `isProtectedGroup` 不删此组 | 随 channel 进 cp（非 session.Events） |
| `AutoCompactSummaryAnnotation` (= `CompactionSummaryAnnotationKey`) | bool | AutoCompact 给生成的 summary 消息打 marker | （a）ReactiveCompact `isProtectedGroup` 跳过保护 summary；（b）`SelfHistoryCompacted` 倒扫找 marker 跨 Run 截断早期事件（⚠️ 标准路径不命中，见协作语义图后存疑） | **不进 channel / cp**——marker 只打在本次调用合成的 summaryMsg 上 |
| `MicroCompactClearedAnnotation` | bool | MicroCompact 给已清 tool_result 占位符消息打 marker（含幂等保护：已带此 marker 的不重 clone）| AutoCompact `extractRecentFiles` 跳过避免占位符当真实文件 | **不进 channel / cp**——marker 只打在本次调用的 clone 上 |
| `MicroCompactOffloadedAnnotation` (= `"microcompact_offloaded"`) | bool | MicroCompact **offload 模式**给 placeholder 打 marker（`OffloadEnabled=true` + store 已挂，tool_result content 替为 offload 指针 + `store.Put`，见 `cloneToolResultWithOffloadRef`）| （a）AutoCompactRun summary 装配跳过、offload preview 不进 summary input；（b）`extractRecentFiles` 跳过避免 offload 占位符当真实文件 reinject；（c）MicroCompact 幂等检不重 offload（`microcompact.go` 的 `MicroCompactRun` 短路段）| **不进 channel / cp**——marker 只打在本次调用的 clone 上；但 offload 的 `store.Put` 是真副作用，业务可按 tcID 从 store 回溯原始 tool_result |
| ~~`SupersededEventAnnotation`~~ | ~~删除~~ | — | **R32+ 视角 4 audit 删** — 生产 0 caller, godoc 自称"AutoCompact 调用"实际 0 调用; 跨 Run 复用 summary 改用 `SelfHistoryCompacted` 倒扫 + `CompactionSummaryAnnotationKey` marker (FAQ §"AutoCompact summary 不写 session.Events" 已澄清此事实) | — |
| `SkillBodyAnnotation` (= `"skill_body_injected"`) | bool | `makeToolsFn` 给成功激活的 `activate_skill` 打 marker（仅 toolCall + framework actTool）：默认 full-body `tool_result` 打；**P2.D `InlineSkillBody=true` 时 terse-ack `tool_result` + body UserMessage 都打（R19——两 group 都保护，否则 tool_call group 整组压进 summary 丢激活上下文 + body 语义倒置；非 422）**| （a）MicroCompact 跳过清除带此 marker 的 tool_result（R17 MED-2，否则 marker 残留 + body 被清成占位符 → AutoCompact 整组保留无意义占位符）；（b）AutoCompact `splitProtectedGroups` 按 group 整组保留（保 tool_call/tool_result pair 完整）；（c）ReactiveCompact `isProtectedGroup` 不删此组——4 层一致防护否则前功尽弃 | 随 channel 进 cp（非 session.Events） |

**协作语义图**：

```
跨 Run 复用流程：
  Run 1: buildInitialMessages → store.Search 注入 [打 StoreInjectedAnnot]
       AutoCompact 跑 → summary [打 AutoCompactSummaryAnnot]，**只活在 modelFn 局部变量**——既不写 channel/cp，也不写 session.Events
       MicroCompact 跑 → tool 占位符 [打 MicroCompactClearedAnnot]，同上（唯一的持久副作用是 offload 的 store.Put）
  Run 2: 第 0 层 HistoryCompactor / SelfHistory 从 session.Events 重建历史（压缩产物哪都不落，per-LLM-调用）
       store.Search 重新注入 → 新一轮 AutoCompact 再扫，跳过 StoreInjectedAnnot 不重摘
       extractRecentFiles 跳过 MicroCompactClearedAnnot 占位符不当真实文件

⚠️ 跨 Run summary 复用**当前失效（已判定为设计权衡，非实现缺失）**：marker 表 `SelfHistoryCompacted`
   倒扫 session.Events 找 AutoCompactSummaryAnnotation 截断早期事件的机制依赖 summary 进 session.Events——
   但按**本文原则#5**（压缩产物只走 channel、不写 session.Events），AutoCompact summary 有意只走 channel、
   零 AddEvent，故标准 BasicAgent 路径下倒扫**永不命中、退化为全量 SelfHistory**。跨 Run 历史压缩实际由
   第 0 层 HistoryCompactor / 全量 SelfHistory 承担。补 summary 进 events 会让合成摘要污染审计流、违反
   原则#5，故未做（`agent_v2/session.go::SelfHistoryCompacted` godoc 已同步如实标注此现状）。
```

### 失败回退路径表

每层失败时下游影响：

| 触发 | 第 1 层 (MicroCompact) | 第 2 层 (AutoCompact) | 第 3 层 (ReactiveCompact) | 整体效果 |
|---|---|---|---|---|
| LLM API 报错 | N/A（不调 LLM） | breaker.RecordFailure；业务错 `slog.WarnContext` 后继续走原 messages（`compacted, ccErr := AutoCompactRun(...)`，见 `basic_agent.go::makeModelFn`）；InterruptError/ctx.Err 不吞、propagate（R23 fix） | retry MaxAttempts；用尽返 error 给 caller | 第 2 层业务错 slog.Warn 继续；第 3 层 retry 直到用尽 |
| breaker tripped | N/A | 跳过本次 AutoCompact（继续走原 messages） | N/A（ReactiveCompact 无 breaker，仅 MaxAttempts 硬上限） | 主链路不阻断 |
| ctx cancel | 立即 return（业务自己 select ctx） | 立即 return | 立即 return | 整 Run 走 ctx cancel 错误路径 |
| 配置 disabled | 直接 skip | 直接 skip | 直接 skip | 等同于不开启 4 层压缩 |
| panic | safeNodeRun recover 转 error；frontier 非空时 Run defer 仍 SaveNS Pending | 同上 | 同上 | **业务避免用 panic 表达 HITL**——Resume 重跑节点会再次 panic 死循环 |
| AutoCompact summary 解析失败 | N/A | `slog.WarnContext` 后走原 messages（不替换）| N/A | AutoCompact 不替换；如果 token 仍超阈值，下次 ReactiveCompact 兜底 |
| ReactiveCompact 删完一组后 messages 仍超 token gap | N/A | N/A | 继续 retry，直到 MaxAttempts；若 `compacted == nil \|\| len(compacted) >= len(curMsgs)` 提前 break | 用尽后返原始 prompt-too-long error 给 caller |

**关键不变量**：
- **第 1 / 2 层失败不阻断主链路** —— 业务错记账断路器 + `slog.WarnContext` 后继续走原 messages
  （**注**：InterruptError / ctx.Err() **不吞**、propagate 给 caller（R23 fix）——只有业务错才 silent 继续；
  接了 observability 时还会 emit compaction event 含 `After.Err`，业务可订阅观测压缩失败）
- **只有第 3 层用尽才真错** —— 这是给 caller 的「最后兜底」信号
- **breaker 仅 AutoCompact 用**：ReactiveCompact 只有 MaxAttempts 硬上限，无 breaker（与 §失败回退路径表 / §LangGraph 对比表一致）

### 排错决策树

> 用户看到现象 X，应该看哪 / 怎么诊断？

```
现象：LLM 调用报 "prompt too long"
  ├─ 第 3 层 ReactiveCompact 应该接住，没接住？
  │   ├─ 检查 cfg.ReactiveCompact.Enabled = true 了吗
  │   ├─ 检查 IsPromptTooLongError(err) 是否识别本 provider 的错误格式
  │   │   （OpenAI / Claude / 自定义 LLM 错误格式可能不同）
  │   └─ 订阅 OrchHooks.OnNodeEvent，看有没有 Type="reset_stream"（Content 形如
  │      「ReactiveCompact retry attempt N/M」）或 Type="compaction" 且 action.Type=="retry"
  │      ——这条路径**不打 slog**，只投事件
  └─ 如果 ReactiveCompact retry 用尽：
      ├─ 检查 ParsePromptTooLongGap 是否解析出 gap（解析失败默认删 20%，由 EmergencyDropPercent 控制）
      ├─ 检查 messages 是否含巨型 tool_result 占用主体 token
      │   → 应该让 MicroCompact 提前清；检查白名单是否覆盖该 tool
      └─ 考虑提高 SummaryMaxTokens 或换更大 context window 的 model

现象：AutoCompact 没触发（token 看着够）
  ├─ 检查 cfg.AutoCompact.Enabled = true 了吗
  ├─ 检查 cfg.Model 非 nil（默认无 model 不调 LLM）
  ├─ 检查 ModelContextWindow 是否设小了（demo 用 32K mock 时容易触发，生产 200K 阈值很高）
  ├─ 阈值 = ContextWindow - SummaryMaxTokens(默认 20K) - SummaryBufferTokens(默认 13K)
  │   demo 想用小 ContextWindow 触发，需手动调小 SummaryMaxTokens / SummaryBufferTokens
  │   （框架不自动按 ContextWindow 大小缩放）
  └─ 看 EstimateTokens(messages) 实际值（slog.Debug 可打）

现象：跨 Run 历史每次都 AutoCompact 重摘（很慢）
  ├─ 开了会话历史层（WithHistory / WithHistoryCtx）吗？开了时某一轮真摘要过，会话历史里就有一条压缩边界，
  │   下一次 Run 从本 agent、同视图的最新边界读起、不重摘（见「压缩结果的复用」）。查：
  │   ├─ 那一轮 OnModelStart 钩子动了消息列表——那一轮不写边界（钩子插入 / 改写的内容不该当历史重放）
  │   └─ 换了 agent 名或视图模式——边界按归属 + 视图认，对不上的当它不存在
  ├─ 没开会话历史层（prior 来自 session.Events）：AutoCompact summary **不 AddEvent 进 session.Events**
  │   （对齐「chunk / 摘要不写审计」原则）→ SelfHistoryCompacted 倒扫 AutoCompactSummaryAnnotation 永不命中 →
  │   跨 Run **恒退化为全量 SelfHistory** 重摘。要跨 Run 复用就开会话历史层
  └─ 单 Run 内多轮不再重复摘：真摘要成功时记备忘，下一轮先套用、只在「Head + 新增的」又超阈值时才再摘一次。
     仍然每轮都摘时查备忘有没有被作废——OnModelStart 钩子每轮插入 / 删除 / 改写前缀里的消息会让前缀指纹对不上

现象：AutoCompact 反复判无效、断路器跳闸（压缩事件的 After.Err）
  ├─ insufficient reduction：摘要没比新内容小够（增量时上限 = 上一份摘要 + 新内容 × MaxReductionRatio）
  │   → 摘要模型在原样复述；换更听指令的模型，或在 CustomInstructions 里要求压缩
  └─ exceeds SummaryMaxTokens：摘要模型写的那部分超过预算（预算写在摘要请求里）——常见于每次把上一份摘要原样保留再追加的模型
      → 在 CustomInstructions 里要求把旧摘要与新内容合并压缩，或调大 SummaryMaxTokens（触发阈值跟着降）

现象：MicroCompact 把 PASS 状态的 tool_result 也清了
  ├─ MicroCompact 默认按白名单清最旧 N 个（KeepRecent 默认 5）
  ├─ 检查白名单是否含该 tool（默认 Read/Bash/Grep/Glob/WebSearch/WebFetch/Edit/Write）
  │   非白名单 tool（MCP / Skill 等）默认不清
  └─ 想保留 PASS verdict 的 critic_tool？把它从白名单移除（业务侧定）

现象：StoreMemory 注入的消息被 AutoCompact 重摘（不该重摘）
  ├─ 检查 buildInitialMessages 注入 store.Search 结果时是否打了 StoreInjectedAnnot
  ├─ 检查 AutoCompact 摘要前是否过滤 StoreInjectedAnnot 消息
  └─ 这是默认行为；如果还出问题，可能 store 实现没走 buildInitialMessages 路径

现象：ParallelAgent 多分支 4 层压缩同时触发，触发 LLM 限流
  ├─ AutoCompact 跨分支共享 breaker，限流时连续 trip 后停摆（ReactiveCompact 不用 breaker）
  ├─ 但首次 burst 可能击穿（N 分支同时 Allow 通过，同时调 LLM）
  ├─ 业务可在 LLM provider 层加全局 rate limit 兜底
  └─ 见 §关键设计权衡 - ParallelAgent 多分支共享 breaker

现象：Streaming 前端看到 LLM token chunk 重启（重复发送）
  ├─ ReactiveCompact retry 时 LLM 重发 → chunk 流也重启
  ├─ 这是已知行为（chunk 流走的是 retry 后新一轮 LLM 调用）
  ├─ 前端可监听 reset_stream event 按 run_id 精确清空旧 chunk（详见 hitl-resume.md §9）
  └─ 见 §关键设计权衡 - Streaming + ReactiveCompact retry 时前端 chunk 重启
```

---

## 基础设施

### Token 精细估算（`compose_v2/token_estimate.go`）

```go
// EstimateMessageTokens 纯字符估算消息序列的 token 数（**不读 API Usage**——
// 读 usage 的是另一个函数 EstimateTokens，见下）。
//
// 估算规则：先累加顶层 msg.Content + msg.ReasoningContent 字段 len/4，再逐 ContentBlock：
//   - text:        len/4 chars
//   - tool_use:    len(name + json(args))/4
//   - tool_result: Content 字符串 len/4
//   - image/file/audio: 各自独立顶层 block，固定 2000
//   - 其他 ContentBlock 类型：默认 JSON marshal 后 len/4 兜底
//   - 兜底:        len(json(block))/4
// 最后 ×4/3 保守填充（API 实际 token 通常比字符估算多 ~33%）
func EstimateMessageTokens(messages []*schema.Message) int { ... }

// estimateTokensFromAPIUsage 优先返回最近一轮 LLM Usage.InputTokens。
// 调用方：先试 estimateTokensFromAPIUsage，nil 时回退 EstimateMessageTokens。
func estimateTokensFromAPIUsage(messages []*schema.Message) (int, bool) { ... }
```

**估算精度对比**：

| 来源 | 精度 | 何时拿得到 | 何时 fallback |
|---|---|---|---|
| OpenAI/Claude API 真实 Usage | 100% | LLM 调用返回后 | 调用前估算需 fallback |
| `estimateTokensFromAPIUsage`（未导出）| 99% | 倒扫最近一条带 `Usage.InputTokens>0` 的 assistant（**不比对 model**）| 没历史 Usage |
| `EstimateMessageTokens` (per block) | 70-90% | 任何时候 | 始终可用作底线 |
| 兜底 `len(json)/4 × 4/3` | 50-70% | 块类型未识别 | 仅极端情况 |

**为啥不直接用 tiktoken**：
- 各 LLM provider tokenizer 不同（OpenAI / Anthropic / 国内 model 各家算法不一致）
- 引入 tiktoken 增加 Go 依赖体积 + cgo 复杂度
- 业务场景对精度要求不极致（AutoCompact 触发阈值有 13K buffer 容错）
- 优先用 API 真实 Usage 已经覆盖 99% 场景；估算只是 fallback

**调试 token 估算**：

```go
n := compose_v2.EstimateMessageTokens(messages)
slog.Info("estimated tokens", "msg_count", len(messages), "est_tokens", n)
// 触发阈值 = ContextWindow - SummaryMaxTokens - SummaryBufferTokens
threshold := autoCfg.ModelContextWindow - autoCfg.SummaryMaxTokens - autoCfg.SummaryBufferTokens
slog.Info("compact threshold check", "threshold", threshold, "would_trigger", n > threshold)
```

> **注**：**真实触发判定**（`shouldTrigger`）先剔除 leading system + `SkillBodyAnnotation` + `StoreInjectedAnnotation` + `MicroCompactOffloadedAnnotation` 四类，再对剩下的可压缩切片调 `compose_v2.EstimateMessageTokens`，`>=` 阈值触发。
> **故意不用 `EstimateTokens`**：后者优先读 LLM 返回的 usage InputTokens（= 整个 prompt，含刚被过滤掉的 skill body / 记忆段），会把「只算可压缩切片」这件事整个架空——大 skill body 下 current 恒等于全量、反复空触发摘要直到烧穿断路器。`EstimateTokens` 现在只用于 compaction event 的 InputTokens/OutputTokens 上报。

### 消息分组（`compose_v2/message_group.go`）

```go
// GroupMessagesByAPIRound 按 message.Annotations["run_id"] 把消息分组——同一 API
// 请求的所有 assistant 消息（含流式分块、tool_use、tool_result）在同一组。
// **user 消息总是开新组**（不携带 run_id）；同 RunID 的 assistant + tool_result
// 加入当前组；新 RunID 的 assistant 开新组；system 消息单独成组。
//
// 用于：删最旧消息组（ReactiveCompact）、配对完整性检查、4 层压缩切片单位。
func GroupMessagesByAPIRound(messages []*schema.Message) [][]*schema.Message { ... }

// FlattenGroups 把分组扁平化回 []*Message 序列。
func FlattenGroups(groups [][]*schema.Message) []*schema.Message { ... }
```

**分组规则**：

```
原始 messages:
  user "问题1"               (无 RunID，开新组 Group 0)
  assistant tool_call(read)  <- RunID=R1  (Annotations[run_id]=R1，加入 Group 0)
  tool_result "..."          (无 RunID，加入当前 Group 0 配对 tool_call)
  assistant "答复1"          <- RunID=R1  (同 R1，加入 Group 0)
  user "问题2"               (无 RunID，开新组 Group 1)
  assistant "答复2"          <- RunID=R2

Group 后:
  Group 0: [user "问题1", assistant tool_call, tool_result, assistant "答复1"]
  Group 1: [user "问题2", assistant "答复2"]
```

**为啥要分组**：删消息时按组删能保 tool_use ↔ tool_result 配对完整。OpenAI API 看到
"独立的 tool_result 没对应的 tool_call" 会 422。

**分组规则精确版**：user 消息**总是开新组**（不是"归入 assistant 同组"——R2/R5 修过此误述）；
后续同 RunID（按 `Annotations["run_id"]`）的 assistant + tool_use + tool_result 加入当前组；
新 RunID 的 assistant 开新组；system 消息单独成组。这样保 tool_use ↔ tool_result 配对
天然在同组。

### 配对感知切割（`compose_v2/safe_truncate.go`）

> **注**：`AdjustIndexToPreserveAPIInvariants` 是 **index 级**切割 helper。第 1-3 层（组级删除）**不调用**它——那三层走 §分组的**组级** `GroupMessagesByAPIRound` + `FlattenGroups`（配对天然在组边界保全）。它的现役调用点是**第 0 层**：`truncatePriorPreservingMarkers` 按 `MaxHistoryPairs` 截断跨 Run 历史时用它把切点拉回配对边界（默认 20 对、默认开启）。

```go
// AdjustIndexToPreserveAPIInvariants 调整切点保 tool 配对 + thinking 完整性。
//
// 问题：天真 truncate 可能把切口落在 tool_result 上（对应 tool_use 在前面被切
// 了），下一次 LLM 调用 422。
//
// 算法：
//   1a. 收集 [start, end] 内所有 tool_result 的 tool_use_id
//   1b. 收集 [start, end] 内已有的 tool_use_id
//   1c. 找出缺失的 tool_use_id（在 start 之前）
//   1d. 向前扩展 start，直到包含所有缺失 tool_use 的 assistant 消息
//
//   2a. 收集 [start, end] 内所有 assistant 消息的 message.id
//   2b. 向前扩展 start，包含同 message.id 的更早 assistant 消息（thinking 完整性）
//
// 返回：调整后的 start 索引（>= 原 start，可能更早）
func AdjustIndexToPreserveAPIInvariants(messages []*schema.Message, start int) int { ... }
```

### 成本守卫：三种「调了 LLM 却净亏」的场景

摘要调用是**要花钱的**，而它有两条路会白花：

**一、可压缩区太小**（`MinCompactableTokens`，默认 5000）。触发阈值算的是**整个对话**，
而可压缩区（early）可能远小于它——典型是单个巨大的当前轮（一次超大 tool_result）把总量
顶过阈值，可压缩的历史其实没多少。照样调一次摘要 LLM，省不下几个 token，**且每轮重复
发生**。低于这个值就静默跳过。

⚠️ **跳过不计断路器**——「没东西可压」不是失败。残留是：阈值下一轮仍会触发，但每次都
**不调 LLM、不烧断路器**，严格优于修复前每轮一次无效调用。

⚠️ 但它**不是「零成本」**：早退点在压缩事件的 emit 之后，跳过的那些轮照样各发一对
before/after 事件 ⟹ 接了 observability 的业务会看到 `crew.compactions{crew.outcome=success}`
每轮 +1、`crew.compaction.ratio` 恒记 1.0——而 ratio 恒 1.0 正好把「压缩效果在退化」这个
信号抹平。要不要改成跳过时不发事件见 `doc/code/todo.md`。

**二、摘要没比原文小够**（`MaxReductionRatio`，默认 0.8）。LLM 可能不遵循指令原样复述、
甚至比原文更长。没有这层校验时框架照单全收：白花一次调用，还把上下文搞得**更大**，
下轮更容易再次触发，形成恶性循环。超过这个占比就判压缩无效、回退原 messages。

⚠️ **这一条计入断路器**——底层 `Generate` 成功 **≠** 业务成功（同 empty summary block）。
持续无效必须让断路器跳，否则每轮白烧一次调用。设 **> 1.0** 才等于放弃此校验（恰好 1.0 时
判据是严格大于，「摘要比原文更长」这种最该拦的仍会被拒）。

**增量摘要时这两道都只按上一份摘要之外的新内容算**。可压缩区里有上一份摘要（带 `AutoCompactSummaryAnnotation`：
套用了备忘，或 prior 从压缩边界读起）时，它已经是压缩过的——可压缩量是 early 减去它，缩减比的上限是「上一份摘要
+ 新内容 × `MaxReductionRatio`」（没有上一份摘要时就是 early × `MaxReductionRatio`，跟原来一样）。按整个 early 算的话，
上一份摘要越长、两次触发之间的新内容越少（触发看的是整段输入，上一份摘要也在里面），合法的增量摘要越难小于 0.8 倍：
被反复判成无效、每次白调一次 LLM，连续几次就让断路器跳闸——实测一个 220 轮的任务里摘要被调 135 次、64 次被判无效。
上一份摘要原样保留、新内容压到这个比例以内都算有效；把新内容原样抄进去的照样被拦下。可压缩区里有几份摘要时只有
**最新一份**算「上一份」，更早的算新内容——框架自己的路径上至多一份，业务自己写进来几份时合并它们能压小，都算
「不可再压」会让压缩一直被跳过（哨兵 `TestAutoCompact_IncrementalSummaryNotRejectedAsSummaryGrows` /
`TestAutoCompactRun_LongPreviousSummaryLittleNewContentSkips` / `TestAutoCompactRun_VerbatimIncrementalSummaryRejected` /
`TestAutoCompactRun_OnlyLatestSummaryIsPrevious`）。

**三、摘要超出预算**（`SummaryMaxTokens`，默认 20000）。触发阈值是按「摘要不超过它」留的空间
（`ModelContextWindow - SummaryMaxTokens - SummaryBufferTokens`），摘要超过它时压完照样在阈值之上、下一轮又触发。
上面的比例上限没有绝对量：把上一份摘要原样保留、只追加新内容的模型会让摘要一轮轮变长，每次按比例算都有效——审计
实测 600 轮里 515 次「有效」压缩、466 次压完仍在阈值之上，摘要涨到窗口的三倍。超过就判无效、回退原 messages、
**计入断路器**，同第二道。只比摘要模型写的那部分（标题 + `<summary>` 块），这个数写在摘要请求里（「控制在约 N token
以内」）；框架附在摘要后面的取回索引不算——它最多 50 条、约 1100 token，模型控制不了，算进去的话 `SummaryMaxTokens`
配得小时一句话的摘要也判无效、每轮判无效、断路器跳闸（审计实测）。哨兵 `TestAutoCompactRun_SummaryOverBudgetRejected` /
`TestAutoCompactRun_SpillIndexNotCountedAgainstSummaryCap` / `TestAutoCompact_PromptCarriesSummaryBudget`。

⚠️ **写微缩配置的测试和 example 都要显式调小 `MinCompactableTokens`**：window 只有几百
token 的 fixture 天然低于默认的 5000，不调小的话压缩根本不会发生——测试**照样绿**（它被
跳过了）、demo 照样打印「触发了」但一条没丢。实测已因此打死过一条既有哨兵
（`TestAutoCompact_R30C_MED1_SummarizeStripResumeContext`）和 `example/compact/compact_demo`。
⚠️ 它**不能用 0 关掉**（`<= 0` 取默认值），要关只能写 1。

### 断路器（`compose_v2/circuit_breaker.go`）

```go
// CircuitBreaker 通用断路器：连续失败 N 次后跳过；带 cooldown 自愈机制。
type CircuitBreaker struct {
    // 内部字段（不导出）：maxFailures int / cooldownDuration time.Duration /
    //                    mu sync.Mutex / failures int / tripped bool / trippedAt time.Time
}

// 默认 cooldown：trip 后 5 分钟允许第一次探测（自愈）
const defaultCircuitBreakerCooldown = 5 * time.Minute

func NewCircuitBreaker(maxFailures int) *CircuitBreaker  // maxFailures <= 0 用默认 3
func (cb *CircuitBreaker) Allow() bool                    // false 表示已 tripped 且未 cooldown
func (cb *CircuitBreaker) RecordFailure() bool            // ++; >= max 时 tripped = true，返回值是「这一次是否刚跳闸」
func (cb *CircuitBreaker) RecordSuccess()                 // 重置 failures = 0, tripped = false
```

**cooldown 自愈语义**：tripped 后等 5 分钟自动半开尝试探测——成功则重置；否则继续 trip。
**断路器一旦 trip 必须能自愈**——CLAUDE.md "公开 API 防御" 强调。

---

## 配置与开关

每层独立开关，**3 个独立 Option** 注入（不是单一 `WithCompaction` 入口）：

```go
// 通过 BasicAgent 三个独立 Option 注入（agent_v2/config.go::WithMicroCompact / WithAutoCompact / WithReactiveCompact）
agent := NewBasicAgent(
    WithModel(mainModel),
    WithMicroCompact(&MicroCompactConfig{Enabled: true, KeepRecent: 5}),
    WithAutoCompact(&AutoCompactConfig{
        Enabled:            true,
        Model:              cheapModel,
        ModelContextWindow: 200_000,
        ReinjectRecentFiles: true,
    }),
    WithReactiveCompact(&ReactiveCompactConfig{Enabled: true, MaxAttempts: 3}),
)
```

**默认配置**（3 层全部 `Enabled = false`）：

- **Micro**：默认禁用（`agent_v2/microcompact.go::MicroCompactConfig` 注释明确"需 WithMicroCompact 显式开启"）；启用后默认 `KeepRecent=5` + `DefaultCompactableTools` 白名单
- **Auto**：默认禁用；启用后必须显式配 `Model`（摘要 LLM）+ `ModelContextWindow`（**必填、无默认**——为 0 时 `shouldTrigger` 直接返 false，AutoCompact 静默永不触发；本仓库 mock demo 调小到 32K 触发演示）
- **Reactive**：默认禁用（`agent_v2/reactive_compact.go::ReactiveCompactConfig` 同款注释）；启用后默认 `MaxAttempts=3`，无需 Model（走删消息组路径）

---

## 可观测性

> **当前 v2 实现状态**：`event.Type = "compaction"` 统一事件已落地 — 3 层压缩
> (MicroCompact / AutoCompact / ReactiveCompact) 共 **9 处 emit 点**，外加
> `event.Type = "history.compact"` HistoryCompactor wrap 事件 (1 处 emit, sink-gated)。
> 业务通过 `OrchestrationHooks.OnNodeEvent` 订阅
> `Event` 后按 `event.Metadata["action"]` type-assert 为 `*CompactionAction` /
> `*HistoryCompactAction` 拿结构化数据 (token before/after / layer / trigger /
> outcome / generate.{attempt,phase,response,err} / retry.{attempt,max}).

### compaction event 完整 emit 矩阵 (9 个 + 1 history)

| 层 | emit 点 | Type | Layer | Action | 含义 |
|---|---|---|---|---|---|
| MicroCompact | `MicroCompactRun` 入口 | `compaction` | `micro` | `before` | 清白名单 tool 旧 tool_result 之前 |
| MicroCompact | `MicroCompactRun` 边界早退处 | `compaction` | `micro` | `after` | 边界/noop 路径 (空 / 全 protected / nil, 无可清) outcome 上报 |
| MicroCompact | `MicroCompactRun` 主路径出口 | `compaction` | `micro` | `after` | Step 3 清完主路径 (含 outcome) |
| AutoCompact | `AutoCompactRun` 入口 | `compaction` | `auto` | `before` | LLM 摘要调用之前 (含 trigger=threshold) |
| AutoCompact | `AutoCompactRun` 出口 | `compaction` | `auto` | `after` | 全 attempt 完成后 (含 outcome) |
| AutoCompact | `AutoCompactRun` 的摘要调用处 | `compaction` | `auto` | `generate` | LLM 摘要调用之后 (每 attempt 1 次, 含 `CompactionGenerateAction`) |
| ReactiveCompact | `ReactiveCompactRun` 入口 | `compaction` | `reactive` | `before` | 删旧 messages 之前 (含 trigger=prompt_too_long + 旧 token) |
| ReactiveCompact | `ReactiveCompactRun` 出口 | `compaction` | `reactive` | `after` | 删旧 messages 之后 (含新 token + outcome) |
| ReactiveCompact | `makeModelFn` 的 retry 循环 | `compaction` | `reactive` | `retry` | retry 循环每 attempt 之间 (含 `CompactionRetryAction`) |
| HistoryCompactor | `buildInitialMessages` 里 HistoryCompactor 调用处 | `history.compact` | (n/a) | (n/a) | HistoryCompactor 调用前后 wrap (sink-gated) |

> **额外**：ReactiveCompact retry 前还会 emit `Type="reset_stream"` 信号到 OrchHook
> （`agent_v2/basic_agent.go` 的 `makeModelFn` retry 段） 让前端按 `run_id` 精确清旧 chunk buffer — 这是
> stream UI 协议事件不是 compaction observability 事件。

### sink-gated 协议 (接了 observability 自动发)

compact / history event **无独立 EmitEvents 开关**——是否 fire 由 ctx 是否挂
`compose_v2.EventSink` 决定。BasicAgent 等 graph 执行内 framework 恒 push
nodeEventSink，event 最终去向由背后 OrchHooks 决定:业务接了 observability
(`WithCtxOrchHooks` / `WithHooks` push 了 OrchHooks) 就自动收到 event 转 metric/span;
没接则 NoOp hooks 丢弃 (payload 开销小)。**「开压缩」与「观测压缩」不再割裂**——
只需开启压缩层本身:

```go
agent := agent_v2.NewBasicAgent(
    agent_v2.WithMicroCompact(&agent_v2.MicroCompactConfig{Enabled: true}),
    agent_v2.WithAutoCompact(&agent_v2.AutoCompactConfig{
        Enabled:            true,
        Model:              summarizer,
        ModelContextWindow: 200_000,
    }),
    agent_v2.WithReactiveCompact(&agent_v2.ReactiveCompactConfig{Enabled: true}),
    // history.compact event 随 WithHistoryCompactor 自动发
)
// 接 observability 后压缩 event 自动转 metric/span:
//   obs := observability.New()   // 或 New(observability.WithTracerProvider(tp), ...)
//   events, _ := agent.Run(agent_v2.WithCtxOrchHooks(ctx, obs.OrchHooks()), sess, input)
```

> **跨层统一 sink-gated**: reactive 相关 event 由同一 sink 门统一控制——ReactiveCompact
> 层 emit (`reactive_compact.go:334/346`) **以及** basic_agent 内部 retry 路径 emit
> (`basic_agent.go:1306`) 一并 fire，无需分别开关。

### 业务 OrchHook 消费范式

```go
type compactionRecorder struct {
    compose_v2.NoOpOrchestrationHooks
}

// OnNodeEvent 真签名 3 参数（nodeID + 值类型 Event）+ 返 context.Context——
// 漏 nodeID 或用 *Event 指针会不满足 OrchestrationHooks 接口（框架只调内嵌 NoOp 版，switch 永不执行）
func (h *compactionRecorder) OnNodeEvent(ctx context.Context, nodeID compose_v2.NodeID, ev compose_v2.Event) context.Context {
    switch ev.Type {
    case agent_v2.CompactionEventType:  // "compaction"
        act, _ := ev.Metadata["action"].(*agent_v2.CompactionAction)
        if act == nil { return ctx }
        // 字段按 act.Type 分发到嵌套 action（before/after/generate/retry 各非 nil 其一，
        // 顶层只有 Layer/Type + 这 4 个嵌套指针；无 act.Trigger/InputTokens/Outcome 顶层字段）
        switch act.Type {
        case agent_v2.CompactionActionBefore:
            slog.Info("compact.before", "layer", act.Layer,
                "trigger", act.Before.Trigger,         // "threshold" / "prompt_too_long" / "manual" / ""
                "in_tokens", act.Before.InputTokens)
        case agent_v2.CompactionActionAfter:
            ok := act.After.Err == ""                  // 无 Outcome 字段——After.Err 空=成功
            slog.Info("compact.after", "layer", act.Layer, "ok", ok,
                "in_tokens", act.After.InputTokens, "out_tokens", act.After.OutputTokens,
                "dropped", act.After.Dropped, "kept", act.After.Kept, "err", act.After.Err)
        }
    case agent_v2.HistoryCompactEventType:  // "history.compact"
        act, _ := ev.Metadata["action"].(*agent_v2.HistoryCompactAction)
        if act == nil { return ctx }
        slog.Info("history.compact",
            "in_msgs", act.InputMsgs, "out_msgs", act.OutputMsgs,  // 无 KeptPairs/DiscardedPairs
            "keep_hint", act.KeepHint, "ok", act.Ok, "err", act.Err)
    }
    return ctx
}

ctx = compose_v2.WithOrchHooks(ctx, &compactionRecorder{})
agent.Run(ctx, sess, input)
```

### observability 自动桥接 (零业务代码)

业务接 `obs.OrchHooks()`（或 `obs.Install`）后（见 [observability.md](observability.md)）：
- `compaction` event 自动记 3 个指标：`crew.compactions`（按 `crew.compact.layer` 与 `crew.outcome`）、`crew.compaction.duration`、
  `crew.compaction.ratio`（按层），并在节点 span 上记 `crew.compaction` 事件（按层与阶段展开成 `crew.compact.*` 属性）。注：存档指标
  `crew.checkpoint.*` 由**独立的** `cp` event 桥接（需 opt-in `WithEmitCpEvent`），**不**是 compaction event 触发
- `history.compact` event 只记成节点 span 上的 `crew.history.compact` 事件、不转指标——业务想统计 history 压缩需自定义 OrchHook
  消费 `HistoryCompactAction`

业务**不用写**自定义 OrchHook — `observability.OrchHooks()` 已封装。
自定义 OrchHook 仅在业务想自加 label / 自定 metric / 推到非 OTel sink 时用。

---

## 与 LangGraph / eino 的对比

| 维度 | LangGraph | eino | 我们 |
|---|---|---|---|
| 内置 compact | ❌（pre_model_hook 让用户自实现） | ✅ adk middleware（产品级 LLM 摘要） | ✅ 4 层 |
| 触发方式 | 用户决定 | token 数（默认 190K） | 多层（**modelFn 入口** / token 阈值 / 错误后） |
| 配对感知 | 用户责任 | 应该有 | ✅ `GroupMessagesByAPIRound` 分组删保配对（第 1-3 层）+ `AdjustIndexToPreserveAPIInvariants` 拉回切点（第 0 层 `MaxHistoryPairs` 截断）|
| 断路器 | N/A | ❓ | ✅ **仅 AutoCompact 用**（连续 3 次失败 trip + 5 分钟 cooldown 自愈）；Reactive 仅有 MaxAttempts 硬上限非断路器 |
| 事件可观测 | N/A | ✅ BeforeSummarize/GenerateSummary/AfterSummarize | ✅ **3 层 9 emit 点 + history.compact 1 emit 点** (`event.Type="compaction"` / `"history.compact"`, sink-gated 无独立开关——接了 observability 自动发); 业务消费 OrchHook + type-assert `*CompactionAction` / `*HistoryCompactAction` (详见 §可观测性) |
| 哲学 | 框架不替你决定 | 框架内置 + 大量配置 | **3 层全部默认禁用**（需显式 `WithMicroCompact / WithAutoCompact / WithReactiveCompact`）+ 第 4 层 StoreMemory 业务自管 |

我们的设计偏向 LangGraph 哲学（默认开关克制）+ eino 的实施完整度（多层 + 配对感知 +
断路器）的折中。

---

## FAQ

**Q：MicroCompact 启用会不会丢数据？**
A：不会（**注：v2 默认禁用，业务需显式 `WithMicroCompact` 开启**）。它只清除**白名单内**
的 tool 输出 content，**保留 tool_use 和 tool_result 块结构**——LLM 仍能看到"调用了
X 工具拿了 Y 结果"，只是看不到 Y 的具体内容。
关键工具（Agent / Skill / MCP / 用户自定义）默认**不在**白名单。

**Q：AutoCompact 为什么默认不启用？**
A：需要用户显式配置摘要 Model（推荐用更便宜的 model 省钱）+ ModelContextWindow。
框架不替用户做"用哪个 model 摘要"的决定。

**Q：ReactiveCompact 不需要 Model 吗？**
A：不需要——v2 当前实现走「删最旧消息组」抢救路径（`emergencyDropOldestGroups`；一组也丢不动时就地挖空超过 2000 字节的 tool_result），不复用
AutoCompact 摘要。原 PoC 设计的 PreferAutoCompact 优先逻辑未实现。

**Q：和 ISSUE-44 的 SelfHistory 冲突吗？**
A：不冲突。压缩只改本次 LLM 调用的入参切片（modelFn 局部变量，不经 channel），
**不**改写 `session.Events` 业务历史——SelfHistory 仍能完整查询。

**Q：HITL Resume 时压缩怎么处理？**
A：Resume 时通过 cp restore + `ResumeContext` ctx 注入 user 答案——压缩从早期开
始，不影响末尾的 user 答复消息。`GroupMessagesByAPIRound` 按 `Annotations["run_id"]`
分组保 tool_use ↔ tool_result 配对完整性。

---

## 关键设计权衡（Round 6 审计沉淀）

实施过程中明确的几个跨场景设计选择——非 bug，是有意 trade-off，需用户感知：

### ParallelAgent / RoutingAgent 多分支共享 breaker

多个 fan-out 分支共享同一 `*AutoCompactConfig` 的 `breakers *sync.Map`（per-session entry 含独立 breaker + `callSem chan struct{}`，**仅同 session 内分支共享，跨 session 隔离**——ISSUE-72 修复）：

- **共享是有意**：摘要 LLM 失败通常是系统性问题（quota / model 不可用），N 个分支各自独立重试 N 次浪费 quota；共享 breaker 让一个分支失败 trip 后其它分支静默跳过 AutoCompact 走原 messages，graceful degradation
- **代价**：单分支偶发失败会影响其它分支的 AutoCompact——如果你的场景里分支独立性重要（不同模型 / 不同 quota），手动给每个分支构造独立 cfg

### AgentAsTool 内层 BasicAgent 完全独立 cfg

外层 BasicAgent 把内层 BasicAgent 包成 tool 调用（`AgentAsTool`）时：

- 内层经嵌套调用的执行器（`runNestedCall`）用 `NewSharedChildSession(parent)`：RunID 取所在任务（即父的）、values map 与父共享指针、events 独立
- 内层有自己的 `cfg.AutoCompact` / `cfg.MicroCompact` / `cfg.ReactiveCompact` 配置和 breaker
- **外层完全看不到内层的压缩活动**——内层独立 LLM 摘要调用、独立 quota 消耗
- 外层 MicroCompact 只看到 `agent_tool.Call` 返回的 wrapped string（一个 `tool_result`），按外层 tool name = 内层 agent.Name() 决定是否清——**默认不在白名单**（Agent 类工具按设计排除）

### MCP / Skill 工具默认不在 MicroCompact 白名单

`DefaultCompactableTools` 只含内置工具（Read/Bash/Grep/Glob/WebSearch/WebFetch/Edit/Write）。**不含**：

- MCP 远端工具（命名 `mcp_<client>_<source>`）——远端查询结果可能是策略关键素材
- `activate_skill` 内置工具——SKILL.md 内容是技能指导，清掉破坏后续轮次

**长 MCP session 的 token 控制**：用户应显式 opt-in 加入白名单：

```go
agent_v2.WithMicroCompact(&agent_v2.MicroCompactConfig{
    Enabled: true,
    KeepRecent: 5,
    CompactableTools: append(agent_v2.DefaultCompactableTools,
        "mcp_my_server_query_db", // canonical name = "mcp_<sanitized_client>_<sanitized_source>"
        "activate_skill",
    ),
})
```

MCP canonical name 算法见 `mcp/client.go::CanonicalName`（内部调 `sanitize`）——非字母数字下划线替换成 `_`，非 ASCII（如 CJK）转 `_%x` 保留信息。

### 编排 HITL 矩阵（哪些编排能 ask_user + Resume）

v2 单 `agent_v2.Agent` 接口 + ctx `graphNS` push 自动支持嵌入——**全 9 编排（含 BasicAgent 顶层）通过 Agent 接口 HITL Resume 闭环**，无 fallback 路径。`example/orchestration/nested_smoke` 端到端 binary smoke 跨 **7 编排** (BasicAgent + Sequential + Loop + Reflect + Handoff + Parallel + Routing) 12/12 PASS 验证；PlanAgent / GraphAgent 嵌套 HITL 走 `agent_v2/plan_nested_resume_test.go` + `r30_graph_agent_hook_order_test.go` + `r31_graph_resume_test.go` 独立单测。

| 编排 | 嵌入运行（ctx graphNS push）| Checkpointer 自动注入 | Resume API | HITL 可用 |
|---|---|---|---|---|
| BasicAgent | ✅ `agent_v2/basic_agent.go::Run` | ✅ | ✅ `Resume(ctx, sessionID, opts...)` | ✅ |
| SequentialAgent | ✅ `agent_v2/sequential_agent.go::Run` | ✅ | ✅ | ✅ |
| LoopAgent | ✅ `agent_v2/loop_agent.go::Run` | ✅ | ✅ | ✅ |
| ReflectAgent | ✅ `agent_v2/reflect_agent.go::Run` | ✅ | ✅ | ✅ |
| HandoffAgent | ✅ `agent_v2/handoff_agent.go::Run` | ✅ | ✅ | ✅ |
| ParallelAgent | ✅ `agent_v2/parallel_agent.go::Run`（per-branch delta + 仅重跑失败/中断分支） | ✅ | ✅ | ✅ |
| RoutingAgent | ✅ `agent_v2/routing_agent.go::Run`（Router 函数 / LLM Classifier 两路径） | ✅ | ✅ | ✅ |
| PlanAgent | ✅ `agent_v2/plan_agent.go::Run`（plan↔exec 循环 + sub-agent / TmpSubAgent / executor 三档） | ✅ | ✅ | ✅ |
| GraphAgent | ✅ `agent_v2/graph_agent.go::Run`（任意 `compose_v2.Graph` 包装为 Agent） | ✅ | ✅ | ✅ |

**v2 简化**：v1 时代有 `Agent` vs `NestableAgent` 双接口（ISSUE-71 接口合并才补齐 9 编排嵌入）；v2 channel-based 设计下**单接口原生支持**——任意 9 编排嵌入任意 graph 无 gap。

**嵌套 cp 物理分离**：子图 cp 通过 ctx push `graphNS` 写到父 store 的 `(parentSID, childGraphNS)` ns 路径，与父 cp 共存于同一 store 不冲突。Resume 时按 ns 子树寻址。详见 [`three-way-comparison/checkpoint-storage.md`](../description/three-way-comparison/checkpoint-storage.md)。

**⚠️ GraphAgent 用户责任**：用户自定义 NodeFunc 内调 sub-agent.Run 时框架不自动 push 父 ns，**必须走 `agent_v2.RunSubAgent(ctx, nodeID, sub, input)`**——不走的话 sub-agent 的存档落进父图自己的槽（ctx 里的 ns 就是父图的），父的写被 GraphID 守卫拒，Resume 撞 `ErrGraphIDConflict`。详见 CLAUDE.md "GraphAgent 提示"段。

> ⚠️ **别手拼 childNS**（曾教过 `WithGraphNS(ctx, JoinNS(parentNS, "<nodeID>"))`，已撤）：一次嵌套调用的身份是位置（调用方所在的节点与相位，加实例维三选一：图级 fanout / 所在 tool_call（tcID + 批内下标 + tools 节点访问序号）/ 静态重访 visit；fanout 实例节点里又起 tool_call 并发时两维一起进位置）加上「这是哪一次调用」（输入内容 + 同一份输入的第几次，或 `agent_v2.WithCallKey` 给的 key），判定住在 `RunSubAgent` 里；手拼漏实例维 ⟹ 子 cp 互覆（ISSUE-96 侧面 2），漏 visit 维 ⟹ 父循环第 2 轮的提问与第 1 轮撞同一个 InterruptID，漏调用方节点 / 相位 ⟹ 两个节点用同一份输入调同一个子 agent 撞成一次、同一个节点两次执行时钩子里的调用撞成一次，漏输入内容 ⟹ 带着新输入的第二次调用续上第一次的存档、新输入没进图；手拼也拿不到「父节点重跑时已完成的调用直接拿回结果」那份完成记录。详见 [`hitl-resume.md`](./hitl-resume.md)。

### Streaming + ReactiveCompact retry 时前端 chunk 重启

ReactiveCompact 触发 retry 时第二次 attempt 仍走 `Stream`（流式优先策略）。前端
SSE 适配层会看到：第一次失败前可能已 emit 部分 chunks → 失败 → ReactiveCompact 触发
（emit OrchHook Event `Type:"reset_stream"` 含 `Metadata.run_id`） → 第二次 Stream
重新流式 → emit 全新 chunks。

**前端体验**：可能出现"半截答案 + 全新答案"重复渲染。建议前端 SSE 适配层看到
`Type:"reset_stream"` Event 时按 `Metadata.run_id` 精确清空当前流缓冲再接新 chunks
（详见 hitl-resume.md §9.2）。

---

## 实施跟踪

| ISSUE | 范围 | 状态 |
|---|---|---|
| ISSUE-65 | 基础设施（token 估算 / 消息分组 / 配对感知切割 / 断路器） | ✅ 已实现 |
| ISSUE-66 | MicroCompact 微压缩 | ✅ 已实现 |
| ISSUE-67 | AutoCompact 自动压缩（含提示词 + 附件重注入 + 断路器 + 并发串行化） | ✅ 已实现（⚠️ 当年清单里的「跨 Run 持久化」2026-10 才落地、形态也不同：压缩产物仍不写回消息 channel；单次 Run 内是备忘（`basicCompactionCh`，随 cp 走），跨 Run 是会话历史里的压缩边界（要开会话历史层），见「压缩结果的复用」）|
| ISSUE-68 | ReactiveCompact 被动压缩 | ✅ 已实现 |
