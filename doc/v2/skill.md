# Skill 系统（动态能力激活）

> **本文档定位**：crew-go skill 子系统的**用法 + 机制权威源**。skill 是 agent 编排的横切能力（与 Store / Hooks / Retry 并列），覆盖 skill 定义、加载、3 激活模式、body 注入、fork、压缩协作。
>
> **与其他文档边界**（避免重复 → 防 stale）：
> - **三方对照决策**（eino / Claude Code / LangGraph 怎么做 + 为什么这么选 + L0 源头分层未来设计）→ [`doc/description/three-way-comparison/skill-system.md`](../description/three-way-comparison/skill-system.md)，本文不复述
> - **压缩保护细节**（`SkillBodyAnnotation` 的 Producer/Consumer/4 层协作）→ [`context-compaction.md` §跨层 marker 全表](context-compaction.md)，本文只讲"为什么需要保护"+ 一句话结论
> - **演化时间线 + API 速查单行**（P1/P2 各阶段 commit）→ [`migration-history.md`](migration-history.md)

---

## 1. 概念

skill 是**可被 LLM 按需激活的能力包**——一个目录 + `SKILL.md`（frontmatter 元数据 + body playbook）。与 tool 的区别：

| | tool | skill |
|---|---|---|
| 形态 | 函数（Name/Description/InputSchema/Call）| 目录 + SKILL.md（prompt + 可选资源/脚本）|
| 加载 | 注册即全程在 toolkit | metadata 常驻、body **按需激活**才进 ctx（progressive disclosure）|
| 用途 | 执行确定动作 | 注入领域知识/playbook 引导 LLM（也可 fork 成 sub-agent 执行）|

核心思想 **progressive disclosure**：skill 的 metadata（name/description）轻量常驻供 LLM 判断"要不要用"，body（完整 playbook，可能很长）只在激活后才注入 ctx —— 省 token。

---

## 2. 快速上手

```go
import (
    "crew-go/agent_v2"
    "crew-go/skill"
)

// 1. 从目录加载 skill（扫 rootDir/*/SKILL.md，构造时一次性全量加载）
sb, err := skill.NewDefaultSkillBox("./skills")
if err != nil { /* ... */ }

// 2. 接入 BasicAgent
ag := agent_v2.NewBasicAgent(
    agent_v2.WithModel(m),
    agent_v2.WithSkills(skill.SkillSource{SkillBox: sb}),
)
```

默认（toolCall 模式）框架自动做 3 件事：(a) sysPrompt 拼 hint 提示"有专项技能可用"；(b) 每轮给 toolkit 注入内部 `activate_skill` 工具让 LLM 看到；(c) LLM 调 `activate_skill({skill})` 后框架取 body 注入对话流。

`SKILL.md` 示例（`./skills/translate/SKILL.md`）：
```markdown
---
name: translate
description: 把文本翻译成目标语言
whenToUse: 用户要求翻译 / 跨语言转换时
---
你是专业翻译。保持原意、语气自然，术语用行业标准译法……
```

---

## 3. 集成机制（框架内部）

### SkillBox 接口（`skill/skillbox.go`，10 方法）

业务可自实现 `SkillBox`（如从 DB / 远程加载），框架只依赖接口：

| 方法 | 作用 |
|---|---|
| `Name` / `RootDir` | box 标识 |
| `ListSkills() []SkillDescriptor` | 列加载成功的 skill |
| `MatchSkills(ctx, query) []SkillMatchResult` | 自然语言匹配（分数降序）|
| `ActivateSkill(ctx, name) (*ActiveSkill, error)` | 激活 → 返展开视图（含 body）|
| `ReadSkillPrompt(name)` | 读 SKILL.md body 原文 |
| `ListSkillResources(name)` / `ReadSkillResource(name, path)` | skill 内资源文件清单 / 读取 |
| `ListSkillActions(name)` | 列 skill 的 action 清单（**已实现**；`WithSkillActionFunc` 程序化注册的 go-func action）|
| `RunSkillAction` | go-func action 执行（**已实现**：进程内 Go 函数，见 §11；自定义 SkillBox 不支持可返 `ErrActionNotImplemented`）|

> `DefaultSkillBox` 构造时一次性全量加载（`NewDefaultSkillBox`），运行时只读 → 天然并发安全。业务自实现 backend 须自负并发安全。

### BasicAgent 集成点

BasicAgent 是 preamble + model + tools + postamble 四节点 channel-based Graph（见 [orchestration-patterns.md](orchestration-patterns.md) / [channel-engine.md](channel-engine.md)），skill 接在：
- **preamble 节点**（`buildInitialMessages`，仅首轮 ISSUE-74）：adaptive/pre_match 模式的预激活（body 进 sysPrompt）；**toolCall 模式**的 skill 清单走 `activate_skill` 工具 description 全量列（走 API tools 参数、不进历史流）
- **model 节点**（`makeModelFn`）：toolCall 模式每轮注入 `activate_skill` 工具到 toolkit
- **tools 节点**（`makeToolsFn`）：toolCall 模式实际执行 `activate_skill.Call`

### 加载 / 发现（`skill/discover.go` + `skillbox.go`）

`DefaultSkillBox.scan()` 扫 rootDir **一级子目录**（不递归），每个目录拼 `SKILL.md` 读取解析。容错：读不到 / `Name==""` 会先 `slog.Warn` 再跳过（`skillbox.go`），只有「文件不存在」和不在 allowList 是静默跳过。

frontmatter 用 **`gopkg.in/yaml.v3` 真解析器**（`ParseSkillMD` → `parseFrontmatter`）解到 `map[string]any`，再经一个映射 switch 落进 `SkillDescriptor`。曾经是手写 YAML 子集解析器，一轮审计在它身上抓出 **7 条同源静默缺陷**（嵌套 `metadata:` 被拍平 / 多行 `|` 正文全丢 / 行尾注释吞值 / 引号里的逗号被切碎 / 非列表键收到列表的闸只挂在两条产列表分支之一 / 别名表两份必须同步的拷贝 / BOM 让整份 frontmatter 失踪），已全部换掉，哨兵见 `skill/parser_defects_internal_test.go`。**BOM 那条换解析器不解决**——切分在解析之前、是我们自己做的，单独剥。bool 走 `parseBoolOptional` 三态（`true/yes/y/1/on` → true，`false/no/n/0/off` → false，**其余 typo/中文/空 → 保留字段默认**，R12/R13）。key 别名走**通用归一化**（小写 + 去掉 `-` `_`，见 `normalizeKey`）而不是逐键列清单——`whenToUse` / `when_to_use` / `when-to-use` / `WHEN-TO-USE` 全部落到同一个 case，加新字段不需要同步任何第二份清单。**非列表键收到列表值报错**（两种写法一视同仁：YAML 里 `[a]` 和 `- a` 是同一种节点）；列表键收到裸标量则留空不报错（`allowed_tools: a, b` 这种逗号裸串不是 YAML 列表，不猜）。**裸控制符在解析期被拒**（yaml.v3 的行为），转义形式仍走 `sanitizeDescriptor` 清洗。

### 关键词匹配（`skill/match.go`）

`keywordScore(query, text)`：分词后逐 token `Contains` 计命中，归一化 `min(1.0, matched / max(tokenCount, 2))`（分母 `max(n,2)` 防单词查询虚高）。`tokenize` 按非字母数字字符切分（字母/数字累积成 token，≥2 字符保留——`gpt4` 是单 token）、**CJK 逐字成 token**（`isCJK` 覆盖汉字/假名/韩文 → 中文匹配友好）。匹配文本 = `Name + Description + WhenToUse + Tags + Examples`。顶层 `matchSkills` 对每 skill 算分、**仅 score > 0 入候选**、按 score 降序返回——故 `DefaultSkillBox.MatchSkills` 天然不含零分 skill（这是 §5.2 `MinScore=0` 对 DefaultSkillBox 仍能筛掉无关 skill 的前提；业务自定义 box 若返 score==0 候选，`MinScore=0` 会放行）。

---

## 4. frontmatter 字段全集（`skill.SkillDescriptor`）

| 字段 | frontmatter key | 默认 | 说明 |
|---|---|---|---|
| `Name` | `name` | — | skill 标识（空则跳过加载）|
| `Description` | `description` | — | LLM 判断用途 + 匹配文本 |
| `WhenToUse` | `whenToUse` / `when_to_use` | — | 使用时机（进匹配文本）|
| `Tags` / `Examples` | `tags` / `examples` | — | 列表，进匹配文本 |
| `Context` | `context` | `inline` | 执行模式：`inline`（body 进当前对话流）/ `fork`（spawn sub-agent）（`fork_with_context` **已删（2026-08-26）**，写它现在在解析期报错，见 §11）。**⚠️ `fork` 仅 toolCall 模式生效**——adaptive/pre_match 下 `Context` 被忽略、body 一律进 sysPrompt（连带 `model:` 失效；见 §5.2 警告）|
| `Agent` | `agent` | — | fork 模式 sub-agent 名（经 `WithSkillSubAgentResolver` 解析；空则默认建 BasicAgent）|
| `Model` | `model` | — | **仅 fork 模式有意义**的 per-skill model 名（经 `WithSkillModelResolver` 解析；inline 模式整 Run 共享一个 model 无切换边界）|
| `DisableModelInvocation` | `disable-model-invocation` | `false` | 禁止 LLM 激活此 skill（见 §5.3）|
| `UserInvocable` | `user-invocable` | `true` | 允许用户端调用——**框架不强制**，预留供业务 UI/CLI 过滤（v2 无 slash command）|
| `AllowedTools` | `allowed-tools` | — | 解析 + 解析期 `slog.Warn`，**框架不强制**且这不是「还没做」——三条执行路径下都没有良定义语义（`fork` 默认路径子 agent 一个业务工具都不给、配了 `WithSkillSubAgentResolver` 时那是业务自己的 agent、`inline` 没有作用域边界可挂）。按轮裁工具用 `agent_v2.WithToolFilter`。判据见其 godoc |

> **字段表到此为止——`SkillDescriptor` 只有这 11 个导出字段，每个都在自己的 godoc 里带一个
> 处置标记**（`[框架消费]` 框架读它并据此行动 / `[业务自负]` 框架解析但不据此行动、理由写在
> 该字段 godoc 里），机械对账见 `TestSkillDescriptor_EveryFieldDeclaresDisposition`。
>
> `MultiTurn` / `MaxTurns` / `InputSchema` / `OutputSchema` **2026-08-27 已删**：前两个来自
> agentdock-spec，语义是「skill 在**主 AgentLoop** 中允许占用的最大轮次」，预设「skill 激活后
> 跨轮持续活着」这个我们**没有的状态**（同 `AllowedTools` 那条判据）；后两个解析器里根本没有
> 对应分支、恒零值。四个都是零 consumer 的 spec 占位——**不解析的字段，不可能变成
> 「解析了但没兑现」**。写了它们现在是未知键，静默忽略。
>
> `UserInvocable` 的 JSON tag **故意无 `omitempty`**：默认 true、可显式 false，跨系统消费方（DB/Web UI）须能区分"显式 false" vs "未声明"——omitempty 会 strip false 导致语义漂移（R12）。

---

## 5. 激活模式（`skill.SkillActivationMode`）

模式在 `WithSkills(SkillSource{Options: &skill.SkillRuntimeOptions{ActivationMode: ...}})` 设（`Options` 是 `*SkillRuntimeOptions` **指针**，注意 `&`）——**不是** `SkillSelectorConfig`（后者无 Mode 字段：`MinScore` 两模式都消费；`Model`（多候选 LLM 仲裁）/ `MaxCandidates` **仅 adaptive 消费**——pre_match 多候选强制 top1、提前 return 不读这两个字段）。

> **默认是 `tool_call`**（`skillActivationMode` 函数空值兜底 toolCall，V1 默认实现）。注意：`SkillActivationModeAdaptive` 常量 godoc 标"默认推荐"是**推荐语义**，代码兜底值是 toolCall——业务想要 adaptive 需显式设 `ActivationMode: skill.SkillActivationModeAdaptive`。

### 5.1 toolCall（默认）

LLM 通过 `activate_skill` 工具主动激活：model 节点每轮注入工具（toolkit clone 后 Register），LLM 调 `activate_skill({"skill":"X"})` → tools 节点执行 `skillTool.Call` → 取 body 注入（见 §6）。sysPrompt 拼默认 hint（可 `WithSkillSystemPrompt` 自定义）。

### 5.2 adaptive / pre_match（`WithSkillSelector`）

preamble 节点预匹配（**仅首轮**），不依赖 LLM 调工具：

- **adaptive**（推荐）：`MatchSkills` 拿候选 → 无候选不激活 / 单候选直接激活 / 多候选时把候选 metadata 注入 + 让 LLM 结构化选择（需配 router model）
- **pre_match**：同 adaptive 但**多候选强制 top1**，不调 LLM 仲裁

`selectSkillAdaptive`（`agent_v2/adaptive_skill.go`）**双阶段过滤**（先 DisableModelInvocation + Name=="" / 后 MinScore），分两阶让 `SkillSelection.Reason` 区分两种"未激活"（"all N disabled" vs "top1 score X < min Y"）。`SkillSelectorConfig.MinScore`：0 = 不门槛、负数 = 等同 0；`MaxCandidates`（仅 adaptive 多候选 LLM 仲裁、需 `Model != nil` 时生效）：给 LLM 看的最大候选数，0/负数 = 默认 8，超出按 score 降序截断。

> 这两个模式下 body 拼进 **sysPrompt**（system 消息），4 层压缩天然全跳过 system，无需 marker 保护。body 同样按 `SkillBodyMaxChars` 截断（因进 system 无压缩层兜底，`DefaultSkillBodyMaxChars=20000` ≈ 5K token 对齐 Claude Code 单 skill 上限）。

> **⚠️ 重要：adaptive/pre_match 模式忽略 `Context` 字段**——`fork` 分流只在 toolCall 模式的 `activate_skill.Call` 内（按 `active.Skill.Context` 字段 switch 分流到 `runForkMode`）。声明 `context: fork` 的 skill 若经 adaptive/pre_match 激活，会被**静默当 inline**（body 进 sysPrompt、不 spawn sub-agent、per-skill `model:` 也失效），**无 warn**。**要用 fork 必须 toolCall 模式。**（`Context`/`ActivationMode` 是正交两维，但 fork 实现只挂在 toolCall 路径。）

业务可 `WithSkillSelectionHook` 订阅选择结果（**纯观测**——签名无 error 返回、panic 被 recover 吞，不能做 HITL gate；HITL 见 §10）。

### 5.3 二维可见性控制（P2.A）

两个正交 frontmatter 字段控制 skill 对"LLM 端"和"用户端"的可见性：

- **`disable-model-invocation: true`** → 禁止 LLM 激活，**3 处独立强制**：(1) `activate_skill` 工具 listing 过滤（LLM 看不到）；(2) `Call` 防御——即使 LLM 猜名硬调返 `SkillDisabledErrCode="skill_disabled"` 业务级失败；(3) adaptive/pre_match 候选过滤。
- **`user-invocable: false`** → 框架不强制（v2 无 slash command），业务自行用此字段过滤 user-facing 列表。

> `SkillDisabledErrCode` 是 **tool error code**（填 `tool.Result.Error.Code`）非 message marker，不在 `tool.IsFrameworkError` 列表 → 反馈 LLM 让其换 skill 重试，不触发压缩/abort。

---

## 6. body 注入策略

skill 激活后 body 进 ctx 有 **4 条路径**，由模式 + `Context` + `WithInlineSkillBody` 决定：

| 路径 | 触发 | body 落点 | marker |
|---|---|---|---|
| **A. tool_result full-body**（默认）| toolCall + inline + `InlineSkillBody=false` | `tool_result` content = 完整 body | 打 `SkillBodyAnnotation` |
| **B. inline UserMessage**（P2.D）| toolCall + inline + `InlineSkillBody=true` | tool_result 退化 terse ack + body 作 `[system_reminder]` UserMessage | **terse-ack + body UserMessage 都打**（R19）|
| **C. fork SystemPrompt** | `context: fork` | body 作独立 sub-agent 的 SystemPrompt | 不进父 ctx（隔离）|
| **D. adaptive sysPrompt** | adaptive / pre_match | body 拼进父 agent system 消息（**忽略 `Context`——即使声明 `fork` 也走这条**，见 §5.2 警告）| 不打（system 天然免压缩）|

### 6.1 inline 默认（tool_result）

最简：`skillTool.Call` 返 `tool.Success(body)`，tools 节点塞进 `tool_result` + 打 `SkillBodyAnnotation`。业务可直接读 `tool_result.Output` 解析 body（hook / 前端 SSE 友好）。

### 6.2 inline body P2.D（`WithInlineSkillBody(true)`）

把 body 从 tool_result 挪到 `[system_reminder]` UserMessage（对齐 Claude Code 风格），tool_result 退化 terse ack（"Skill X activated; see next system reminder for content."）。好处：跨压缩友好 + tool_result terse 省 token。

**关键（R19）**：terse-ack tool_result **和** body UserMessage **都打** `SkillBodyAnnotation` —— 因 `GroupMessagesByAPIRound` 把 terse-ack（RoleTool）与 assistant tool_call 分到 group N、body UserMessage（RoleUser）开 group N+1；若只 body 打 marker，group N 会被整组压进 summary → 激活上下文丢失 + body 语义倒置（**非 422 配对问题**，整组压缩无 orphan）。压缩协作细节见 [context-compaction.md §跨层 marker 全表](context-compaction.md)。

body 经 `SkillBodyMaxChars` 截断；parallel tool_call 时所有 body UserMessage 在全部 tool_result 之后统一 append（R16，防 `[tool,user,tool,user]` 交错被 API 拒）。

### 6.3 fork 模式 + per-skill model（P2.C + P2.B）

`context: fork` → skill body 作**独立 sub-agent** 的 SystemPrompt，LLM 通过 `activate_skill({"skill","query"})` 传子任务 `query`，sub-agent 隔离跑完返 final answer 作 tool_result。配套 `model:` 字段做 per-skill model 切换（如 SQL skill 走 GPT-4 / 翻译走轻量）。

```go
ag := agent_v2.NewBasicAgent(
    agent_v2.WithModel(parentModel),
    agent_v2.WithSkills(skill.SkillSource{SkillBox: sb}),
    agent_v2.WithSkillSubAgentResolver(func(name string) (agent_v2.Agent, error) {
        if name == "research_specialist" { return researchAgent, nil }
        return nil, fmt.Errorf("unknown agent %q", name)
    }),  // P2.C：业务注册 name→预配置 Agent
    agent_v2.WithSkillModelResolver(func(name string) (model.ChatModel, error) {
        // P2.B：name→ChatModel（仅默认 BasicAgent 路径用）
    }),
)
```

`runForkMode` 流程：验 `query` 非空 → 解析 sub-agent（resolver；**仅 resolver 空时才建默认 BasicAgent + 解析 per-skill model + 截断 body**）→ **push child ns + 递归 fork 防护（见 §8 #1/#5）+ `WithSubAgentStoreScope`（防 sub-agent StoreMemory 双重持久化到父 ns，ISSUE-84，对齐 AgentAsTool）** → **半共享 child sess（`NewSharedChildSession`：values map 与父**共享指针**、events 独立，对齐 AgentAsTool；父 ctx 无 sess 时 fallback 全新 sess，ISSUE-79）** 跑 → 返 final。**注意**：sub-agent 与父**共享 session values**（fork sub-agent 间共享 KV），故业务自管 KV（`SetValue`/`GetValue`）须加 tcID/query hash 前缀——同轮多个同 skill fork 共享 `parent.values` 否则互覆。body/ctx 仍隔离（body 在 sub SystemPrompt 不进父对话流）。

**fork vs inline trade-off**：

| 维度 | inline | fork |
|---|---|---|
| body 影响 | 进父 ctx 影响后续推理 | 隔离子 agent，不污染父 ctx |
| token | 长 skill 累积父 ctx | 父只见 final answer |
| 模型 | 整 Run 一个 model | 每 fork skill 可不同 model |
| 失败 | 影响父 ctx | 返 `SkillForkErrCode` 业务级，父 LLM 换路径 |

---

## 7.（已删除）增量 skill listing

增量 skill listing 已删除（2026-07）——toolCall 模式统一走 `activate_skill` 工具 description 全量列 skill（走 API tools 参数、被 prompt caching 覆盖、不进对话流）。

**为什么删**：把一份「必须一直在模型眼前」的状态放进会被压缩 / 截断 / 换源的对话流，就得在每条动对话流的路径上单独保护它——删除前散布 5 文件 14 处，漏一处即静默失效，[ISSUE-99](../issue/issue-99-listing-invariant-broken-by-kv.md) 是同一根因的第 6 个 bug。放回工具 description 后防护代码归零（工具定义每轮重新生成，不存在"被压缩掉"）。六家实测与全部设计取舍见 [skill-system.md §2.1](../description/three-way-comparison/skill-system.md)。

## 8. fork 路径关键不变量（10 条，业务用 fork 必读）

fork 是 skill 最复杂路径（spawn sub-agent + ns 分离 + HITL + 安全），经 R14-R22 多轮 audit 加固。权威在源码 `agent_v2/skill_tool.go::runForkMode`：

| # | 不变量 | 为什么 | 源码 |
|---|---|---|---|
| 1 | **ns 物理分离** | child sub-agent push `JoinFanoutNS(parentNS, forkNSPrefix+name, hash(身份))` child ns（身份只看位置：调用方所在的节点与相位 + tcID + 批内下标 + tools 节点访问序号——一个 activate_skill tool_call 就是一次 fork；与 `AgentAsTool` / `RunSubAgent` 共用 `run_subagent.go` 的执行器；`forkNSPrefix="__skillfork_"` sentinel），否则 cper 启用时 sub cp 写父 ns 冲突 `ErrGraphIDConflict`；sentinel 前缀既防与父 graph 节点名（model/tools）冲突，又让业务 step/branch 名不撞使深度计数精确；同轮多 activate_skill 靠 tool_call 身份防 cp 互覆（ISSUE-78），模型跨轮或批内复用同一个 tool_call ID 时靠访问序号与批内下标分开 | `runForkMode` |
| 2 | **InterruptError / ctx.Err propagate** | sub-agent 抛中断或 ctx 取消必须 propagate 给父（不 wrap 成业务错），否则 HITL 链断 | `skill_tool.go` R14 |
| 2b | **已完成的 fork 不重跑** | `activate_skill` 是框架元工具、不进 tools 节点的工具记账；同批另一个工具挂起、tools 节点重跑时，已经跑完的 fork 由嵌套调用的完成记录拿回（`compose_v2.SaveStepResult`，按 `Rerunnable` 记），不再执行 sub-agent；没跑完的续自己的子存档 | `runNestedCall`，哨兵 `TestNestedCall_CompletedSkillForkNotRerun` |
| 3 | **P2.D 不误套 fork 答案** | `runForkMode` 在 `tool.Result.Metadata` 标 `SkillForkExecutedMetaKey`，`makeToolsFn` 检到跳过 P2.D inline body 包装 + 不打 marker（fork final answer 不是 body）| R15 |
| 4 | **body 截断** | fork 默认 BasicAgent 同样按 `SkillBodyMaxChars` 截断 SystemPrompt（与 inline/adaptive 对齐）| R14 |
| 5 | **递归 fork 防护**（抽成 `checkForkRecursion` 纯函数，两个独立机制）| (a) **自指检测**：`SplitNS` segment 精确比对 `seg.NodeID == forkNSPrefix+name`（**segment 而非前缀**比对，防 `r` 误判 `research`，R21）；(b) **总深度限制**：数带 `forkNSPrefix`（`"__skillfork_"` sentinel）前缀的段 ≥ `maxSkillForkDepth=5` fail-fast。**fork 段用 `__skillfork_` 双下划线 sentinel 前缀**（非可读的 `skill_`）——业务 SequentialAgent step / ParallelAgent branch 名直接成 ns 段，sentinel 让深度计数只数真 fork 段，业务用 `skill_setup` 等自然命名不再被误算（根因修复——曾用 `skill_` 前缀计数会误杀） | R19/R21 |
| 6 | **SkillBox Name="" 过滤**（listing/adaptive 路径，非 fork 专属）| 业务自定义 SkillBox 返空 Name 的 skill 在 **listing/选择路径**被过滤（`filterModelInvocableSkills` / `selectSkillAdaptive`，防脏数据）——`runForkMode` 本身不做此过滤（skill 已激活、Name 非空才到 fork）| R19 |
| 7 | **⚠️ prompt injection（SECURITY，范围 fork + P2.D inline）** | **若 SKILL.md body 来自不可信源（多租户 / plugin 市场 / 远端配置中心），body 可含伪造 `</system>` / system_reminder 对话边界注入诱导 LLM 误识权威指令**——`active.Prompt`（body）直接作 fork sub-agent SystemPrompt **或 P2.D inline body UserMessage**。**业务必须保证 SKILL.md body 来源可信**，或自定义 SkillBox 实现层做 body 内容审查——**框架审元数据（name/desc 等：parse 边界清洗 + listing 框定，见 §14），但不审 body 内容**（审了破坏合法 skill）。此外 `query` 也是 LLM 生成输入，sub-agent 敏感操作（DB 写 / shell）须在工具层校验 | `skill_tool.go` R22 |
| 8 | **fork 隔离 hook 不对称** | fork 默认 sub-agent **不继承** parent Hooks/Toolkit/Memory（隔离），但**继承 OrchestrationHooks**（observability 连续）——业务须知默认 fallback 无业务 hook | R14/R22 |
| 9 | **HITL 限制** | 默认 fallback BasicAgent **不注 ask_user** → sub-agent 内 LLM 不能主动抛 InterruptError，HITL 在默认路径不可达。业务要 fork+HITL 必须 `WithSkillSubAgentResolver` 自构造 sub-agent + 显式 `WithAskUser()` | R16 |
| 10 | **并发 SubAgentResolver 安全** | resolver 返单例 sub-agent 时业务须保证 stateful 字段并发安全（ParallelAgent 多 branch 同时 fork 同实例）；推荐 closure 内每次 new 独立实例 | R16 |

---

## 9. 与 4 层压缩协作（引用，不复述）

skill body 注入对话流后须**跨压缩保鲜**（否则被 summarize 丢失）。marker：
- `SkillBodyAnnotation`（`"skill_body_injected"`）：保护 body（toolCall full-body / P2.D 两 group）

4 层压缩对带 marker 的组一致防护（MicroCompact 跳过清除 / AutoCompact 整组保留不进 summary / ReactiveCompact `isProtectedGroup` 不删）。

> **完整 Producer/Consumer/R19 跨层协作细节 → [context-compaction.md §跨层 marker 全表](context-compaction.md)**（压缩文档是 marker 权威源，审了 5 轮）。本文不复制——避免同病 stale。

---

## 10. HITL + skill

业务自定义 `SkillBox.ActivateSkill` 可抛 `compose_v2.InterruptError` 做"敏感 skill 激活前用户审批"，按模式两个触发位置：
- **adaptive / pre_match**：preamble 节点内（`buildInitialMessages`），框架自动 SaveNS cp + Resume 重跑
- **toolCall**（默认）：tools 节点内（LLM 调 `activate_skill` 触发），按 tools 节点协议 SaveNS cp + Resume 重跑——抛中断的那次 `activate_skill` 自己会再调一次（它没成功），同批已成功的兄弟 tool_call 由节点 memo 重放不重执行（见 [hitl-resume.md §6.2](hitl-resume.md)）

**idempotent 契约**：Resume 时 `ActivateSkill` 被再次调用，业务实现须支持多次调用返同结果（除非自读 `ResumeContext` 主动改写）。

`WithSkillSelectionHook` **不能**做 HITL（纯观测，panic 被吞）。fork 模式 HITL 限制见 §8 #9。

---

## 11. skill action 执行（确定性能力：Go-func 进程内）

skill 除了「注入 body / fork sub-agent」（教 LLM 怎么做），还能暴露 **action**——激活后**确定性执行代码**（不经 LLM），LLM 经 `run_skill_action` 工具触发（区别于 `activate_skill` 注入上下文）。

### 11.1 runtime

只有一类 runtime——`go`（`RuntimeGo`，进程内 Go 函数，无子进程）：业务经 `WithSkillActionFunc(skill, desc, fn)` 注册，`fn func(ctx, args) (*SkillExecutionResult, error)`，框架直接调用。

> **脚本路径已删**（2026-06-30）：`python`/`bash`/`node` 声明式脚本 action + `ScriptExecutor` 沙箱 seam + SKILL.md frontmatter `actions:` 解析当初是对齐 spec `skill.tsp` 占位（`RunSkillScript`）建的，零生产 consumer；三方调研实锤主流框架（eino `Shell`/`execute` tool、Claude Code Bash）都把脚本执行收敛到 **tool 层 open-bash**，无一在 skill 层做声明式脚本执行。故删 script 路径、保留 go-func（对 `skill.tsp` `RunSkillScript` 的主动偏离）。脚本执行交 tool 层 sandbox tool（见 [skill-system.md](../description/three-way-comparison/skill-system.md) 删除依据）。

### 11.2 用法（Go-func）

```go
box, _ := skill.NewDefaultSkillBox(rootDir,
    skill.WithSkillActionFunc("inventory",
        skill.SkillActionDescriptor{Name: "get_stock", Description: "查 SKU 库存",
            ArgumentsSchema: map[string]any{"type": "object",
                "properties": map[string]any{"sku": map[string]any{"type": "string"}}, "required": []any{"sku"}}},
        func(ctx context.Context, args map[string]any) (*skill.SkillExecutionResult, error) {
            return &skill.SkillExecutionResult{Success: true, Output: lookupStock(args["sku"].(string))}, nil
        }))
ag := agent_v2.NewBasicAgent(agent_v2.WithSkills(skill.SkillSource{SkillBox: box}), agent_v2.WithModel(m))
// LLM 看到 run_skill_action 工具（列 inventory/get_stock + args schema）→ 调它 → go-func 跑 → 结果回灌
```

### 11.3 LLM 触发：run_skill_action 工具

有 ≥1 个**可调** action 的 skill 时框架自动挂 `run_skill_action({skill, action, args})` 工具（单一 meta-tool）。**条件注册**（无 action 不挂，独立于 activation mode）；与 `activate_skill` **职责正交不冲突**（一个管 body、一个管 action）。`DisableModelInvocation` 的 skill 的 action 不列出、不可调。

### 11.4 结果 + 错误

- `SkillExecutionResult{Success, ExitCode, Stdout, Stderr, Output, Error, ErrorCode}` 结构化（优于扁平字符串）；失败 action（`Success=false`）降级为 `[failed] ...` 文本回灌 LLM 继续 ReAct（对齐 MAF）。
- 业务 ActionFunc 可抛 `compose_v2.InterruptError` 做 HITL（敏感 action 执行前真人审批）——框架 propagate 存 cp。
- 找不到（或 go action 未注册实现）→ `ErrSkillActionNotFound`（`errors.Is` 友好）。

### 11.5 边界

- **action 须挂在已加载 skill 上**：`WithSkillActionFunc(skillName, ...)` 的 skillName 必须对应有 SKILL.md 的 skill；注册到未加载 skill 静默不生效。
- **go-func action 须程序化注册**：经 `WithSkillActionFunc` 注册（SKILL.md frontmatter 声明不了 go 函数）。
- **可重入契约**：框架可能并发调用同一 `ActionFunc`（同轮多 action / 多 branch），业务实现须可重入 / 线程安全。

> 设计综合：单 meta-tool 触发 + 结构化 `SkillExecutionResult`（ExitCode/Stdout/Stderr，优于扁平字符串）；脚本执行不在 skill 层做（已删，收敛到 tool 层 open-bash，见删除依据）。真 qwen 端到端 smoke → [`smoke-test-recipes.md` §6](../description/smoke-test-recipes.md)；三方对照 → [`skill-system.md`](../description/three-way-comparison/skill-system.md)。

---

## 12. 已实现 vs 未实现

**已实现**：3 激活模式（toolCall/adaptive/pre_match）/ P2.A 二维控制 / P2.B per-skill model / P2.C fork / P2.D inline body / CJK 匹配 / 路径穿越防护 / 资源类型化 / 元数据注入清洗 + listing 信任框定（§14）/ **skill action 执行**（§11：Go-func 进程内 + run_skill_action 工具）。

**未实现 / 限制**（细节见 [skill-system.md §不足](../description/three-way-comparison/skill-system.md) + [issue-88](../issue/issue-88-skill-schema-dead-code.md)）：
- **skill action 仅 go-func**：经 `WithSkillActionFunc` 程序化注册（SKILL.md frontmatter 声明不了 go 函数）。声明式脚本 action 路径（python/bash/node + `ScriptExecutor` 沙箱 seam + frontmatter `actions:` 解析）已删（2026-06-30，spec 占位无 consumer），脚本执行交 tool 层 open-bash（见 §11.1 删除依据）
- **嵌套 skill 发现**（`a/b/SKILL.md` 子目录 skill；发现仅扫 rootDir 一级——但注意 skill **内部**资源文件扫描是递归的 `WalkDir`，reference 子目录文件能读到）/ 热重载（构造时一次性加载）
- ~~`fork_with_context`~~ 已删除（2026-08-26）：不在 agentskills.io 标准里，兼容对象只有 eino 一家；写它现在在解析期报错
- ~~frontmatter `inputSchema` / `outputSchema`~~ 已删（`bd1ed59e`，零 consumer）；`allowed-tools` 解析但**决定不强制**（解析期对非空值出声，理由见 `AllowedTools` 字段 godoc；按轮裁工具用 `WithToolFilter`）
- **skill 多激活（同时激活 N skill）：暂缓（2026-06 决策）**——spec 残留字段 `SkillPreMatchOptions`（TopK / 阈值 / `MaxActiveSkills` / UseEmbedding）**已删除**（commit `c0f187d` 从 spec sync 进来后 runtime 零消费）。理由：**默认 toolCall 模式** LLM 可跨轮反复调 `activate_skill`，**早已支持 N 激活**（无 cap）；adaptive/pre_match 维持单 skill（`SkillSelection.Skill` 单指针）。行业 7 框架 skill 层**无一**做框架 TopK 预选（全"全量列出 + LLM 自选"），故不在 adaptive 预选路径加复杂度。未来有业务驱动 → toolCall 加可选 cap+dedup，不改 adaptive 单指针

---

## 13. 未来方向：L0 源头分层

当前 toolCall 模式 body 全量进 tool_result + marker 压缩保护是**治标**——多 skill 长会话累积仍占 ctx。**L0 源头分层**（progressive disclosure 进一步：core body 触发加载 + reference 文件按需 read）是治本方向，业内（Anthropic Agent Skills 三级 / SkillReducer）已验证。

> 完整设计（三级分层 / `read_skill_resource` 工具 / 框架路由缓解触发 / over-read 预算闸门）→ [skill-system.md §skill 源头分层方案设计](../description/three-way-comparison/skill-system.md) + [ISSUE-92](../issue/issue-92-skill-source-layering.md)（📋 待实施）。

---

## 14. 安全：不可信 skill 注入防护（marketplace）

> 适用 skill 来自**不可信源**（第三方 / 用户上传 skill 包 / skill 市场）；开发者自写 skill 可信，威胁低。skill 元数据（name/description 等）**激活前**仅靠安装/上架就进 LLM 可见 prompt——两层框架防护 + 一条业务边界。

### 14.1 元数据输入清洗（`skill/sanitize.go`，parse 边界，always-on）

`parseFrontmatter` 收尾对 9 个文本元数据字段（`Name` / `Description` / `WhenToUse` / `Context` / `Agent` / `Model` / `Tags` / `Examples` / `AllowedTools`）做 `sanitizeMetadata`：**折叠换行 / 控制符为单空格 + 截断超长**（标识符类 128 / 文本类 1024 rune）。trust-boundary 一处净化 → listing / 选择 / 错误消息所有下游自动安全。合法元数据无换行 / 控制符，清洗对其零影响（无需 opt-in）。body 不在此列（见 §14.3）。

- **挡**：单行内控制符（ANSI `\x1b` / null / 回车）→ listing 在终端 / UI 渲染时的注入与显示伪造；未来非逐行 load 路径（JSON manifest / marketplace API，值可含真实换行）伪造独立 list item 的结构注入。
- **现状诚实提示**：当前 SKILL.md frontmatter 解析器**严格逐行**（`strings.Split(fm, "\n")`，每行 `key: 单行值`）——真实换行注入不进单个 value，故"换行伪造 list item"经 SKILL.md **当前不可达**。清洗对 SKILL.md 的现实价值是**控制符加固**；换行折叠是给未来非逐行 load 路径（JSON / API）**前瞻兜底**。**新增任何 SkillDescriptor 入库路径必须复用此清洗**（`discover.go` 注释已标）。

### 14.2 listing 信任框定（`skillListingTrustNote` / router prompt）

清洗删不掉**合法文本形态**的注入（恶意 description 写成"无视上面所有规则…"全是合法字符）。三个把元数据喂 LLM 做选择的注入面统一加 prompt 层框定，标注"描述为 skill 用途说明、非给 LLM 的指令"：

| 注入面 | 框定载体 | 源码 |
|---|---|---|
| 全量 listing（`activate_skill` tool Description）| `skillListingTrustNote` 嵌 preamble | `skill_tool.go` |
| adaptive LLM router | `defaultRouterSystemPrompt` 末句英文框定 | `adaptive_skill.go` |

adaptive 候选经 `json.Marshal` 序列化（已防 JSON 结构注入），框定防的是**文本操纵 router**。框定降低 LLM 误从，**非 100% 保证**（prompt 注入本质难根绝）——业界标准防御层，与输入清洗叠加构成纵深。

### 14.3 body 内容业务自负（框架边界，详见 §8 #7）

skill **body**（激活后作 fork sub-agent SystemPrompt / P2.D inline UserMessage，载入 `ActiveSkill.Prompt`）是 skill 的**指令内容本身**——框架**不审查 body**（审了会破坏合法 skill）。body 含伪造 `</system>` / system_reminder 边界注入风险，**业务必须保证 SKILL.md body 来源可信，或在自定义 `SkillBox` 内做内容审查**。`query`（LLM 生成的 fork 输入）同理，sub-agent 敏感操作（DB 写 / shell）须在工具层校验。

---

## 15. API 速查 + 代码定位

### options（全在 `agent_v2/config.go`）

| option | 作用 |
|---|---|
| `WithSkills(skill.SkillSource)` | 接入 skill（含 SkillBox + Options.ActivationMode）|
| `WithSkillSystemPrompt(string)` | 自定义 toolCall hint（仅 toolCall）|
| `WithSkillSelector(SkillSelectorConfig)` | adaptive/pre_match 配置（MinScore / Model / MaxCandidates）|
| `WithSkillSelectionHook(SkillSelectionHook)` | 订阅选择结果（纯观测）|
| `WithSkillBodyMaxChars(int)` | body 截断上限（默认 20000）|
| `WithInlineSkillBody(bool)` | P2.D：body 走 system_reminder UserMessage |
| `WithSkillListingBudget(total, perEntry int)` | listing 预算（默认 4000/200）|
| `WithSkillModelResolver(SkillModelResolver)` | P2.B：per-skill model（仅 fork）|
| `WithSkillSubAgentResolver(SkillSubAgentResolver)` | P2.C：fork sub-agent 解析 |

> 单行 API 速查 + 演化时间线见 [migration-history.md](migration-history.md)（不在此重复维护）。

### 代码定位

| 模块 | 文件 |
|---|---|
| skill 类型 / SkillBox / 加载 / 匹配 | `skill/skill.go` `skill/skillbox.go` `skill/discover.go` `skill/match.go` |
| 元数据注入清洗（§14.1）+ listing 信任框定（§14.2）| `skill/sanitize.go` / `agent_v2/skill_tool.go`（`skillListingTrustNote`）/ `agent_v2/adaptive_skill.go`（`defaultRouterSystemPrompt`）|
| **skill action 执行（§11）** | `skill/action.go`（`ActionFunc` / `RuntimeGo`）/ `skill/skillbox.go`（`RunSkillAction` / `WithSkillActionFunc`）/ `agent_v2/skill_action_tool.go`（`run_skill_action` 工具）。go-func 进程内（脚本路径已删，执行隔离交 tool 层 open-bash，见 §11.1 删除依据）|
| toolCall 激活 + body 注入 + fork | `agent_v2/skill_tool.go`（`skillTool.Call` / `runForkMode`）|
| adaptive/pre_match 选择 | `agent_v2/adaptive_skill.go`（`selectSkillAdaptive`）|
| BasicAgent 集成（preamble/model/tools 节点）| `agent_v2/basic_agent.go`（`buildInitialMessages` / `makeModelFn` / `makeToolsFn`）|
| 压缩保护 | `agent_v2/autocompact.go`（`splitProtectedGroups`）/ `reactive_compact.go`（`isProtectedGroup`）/ `microcompact.go` |
| options | `agent_v2/config.go` |
