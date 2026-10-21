# Skill 系统设计三方对照

## 主题

「Skill」（技能）= 一份按需加载的**专项指导手册**：开发者用 SKILL.md（YAML frontmatter + Markdown body）声明一个能力包，agent 在合适场景下加载它的 prompt body 进入上下文，从而获得专项执行能力（翻译 / code review / PDF 处理 / web research 等）。

与「tool」的区别：
- **tool** = 程序化能力，LLM 调用 → 框架执行外部代码 → 返结构化结果
- **skill** = 知识/工作流能力，LLM 加载 → 框架返一段 prompt body → LLM 用此 prompt 自我引导后续动作

skill 的核心价值在 **progressive disclosure**（渐进式披露）：上下文里只挂"skill 名 + 一行描述"占很少 token，真正的指南 body（可能几百行）只在需要时按名称按需加载。但**Claude Code 源码审计揭示，"渐进式披露"只是 skill 系统的一个维度**——完整 skill 系统至少含 5 个正交维度，详见下文 §"Claude Code Skills 子系统（5 维度概览）"。

---

## 三方设计速览

| 项 | crew-go V1 | eino skill middleware | Claude Code Skills | LangGraph |
|---|---|---|---|---|
| 概念抽象 | `skill.SkillBox` 接口 + `agent_v2.WithSkills` option | `skill.Backend` 接口 + `skill.NewMiddleware` 中间件 | `Command (type='prompt')` 统一抽象 + `SkillTool` 内置工具 | **无原生 skill 概念** |
| 文件格式 | SKILL.md（YAML frontmatter + body）| SKILL.md（YAML frontmatter + body）| SKILL.md（YAML frontmatter + body）| — |
| 触发协议 | LLM 调内置 `activate_skill` tool | LLM 调内置 `skill` tool | LLM 调内置 `Skill` tool（首字母大写）| 用 tool / subgraph 模拟 |
| 激活模式 | toolCall（V1）/ adaptive / pre_match（V2 P1.B 实现）| inline / fork / fork_with_context | inline / fork / remote-canonical / 文件路径条件激活 / DiscoverSkills（实验）| — |
| body 注入位置 | 作为 tool_result 返给 LLM | 作为 tool_result 返给 LLM；fork 模式下作为子 agent UserMessage | inline 走 `newMessages` 注入对话流（轻 tool_result `"Launching skill: xxx"`）；fork 走子 agent | — |
| 子 agent 隔离 | ❌ 仅 inline，body 进当前 agent 上下文 | ✅ fork / fork_with_context | ✅ Forked 模式 + 独立 agentId + 独立 token budget | 用 subgraph 模拟 |
| 关键词预匹配 | ✅ `MatchSkills(query)` CJK + 英文分词打分 | ❌ 由 LLM 直接读 Backend.List 的描述选 | ❌ 默认全列；DiscoverSkills 实验功能用 LLM-driven retrieval | — |
| 资源（Resource）| ✅ scan SKILL 目录非 SKILL.md 文件 → `SkillResource[]` | ✅ 通过 `BaseDirectory` 暴露目录路径让 LLM 自取 | ✅ 通用 Read/Bash/Glob 按需读 + bundled skill 可声明 `files: Record<string, string>` lazy extract 到 disk | — |
| Action（确定性执行）| ✅ go-func 进程内（`run_skill_action` 工具，[`skill.md` §11](../../v2/skill.md)）；声明式脚本 action 路径已删（2026-06-30），脚本执行收敛到 tool 层 open-bash（同 Claude/eino）| ❌ 不暴露独立 action 类型 | ✅ 任意脚本可被 Bash 工具直接执行 + markdown 内嵌 ``!`...` `` shell 执行 | — |
| per-skill model 切换 | ❌ | ✅ frontmatter `model: xxx` + `ModelHub` 解析 | ✅ frontmatter `model:` + `contextModifier` per-call 切换 + `[1m]` 后缀延续 |
| 跨 compaction body 保鲜 | ✅ V2 P1.C 实现（toolCall 模式）—— `SkillBodyAnnotation` marker + AutoCompact 按 group 保留 + ReactiveCompact `isProtectedGroup` 跨层一致防护 | ❌ | ✅ `invokedSkills` 全局态 + per-agentId 隔离 + 5K token/skill 预算 | — |
| 文件热重载 | ❌ | ❌ | ✅ chokidar + 300ms debounce + 2s polling + ConfigChange hook block | — |
| 学习反馈 | ❌ | ❌ | ✅ 5 turn 后台 LLM 评估 → SkillUpdate → survey 用户确认 → 改写 SKILL.md | — |
| 排序推荐 | ❌（业务用 MatchSkills 自定）| ❌ | ✅ `usageCount × 0.5^(daysSinceUse/7)` 7 天半衰期 + 0.1 floor | — |

---

## 2026 H1 行业收敛：SKILL.md 已成事实标准（+MAF / OpenAI / CrewAI）

> 2026 Q2 实地代码调查（主纲 [`industry-survey-2026h1.md`](industry-survey-2026h1.md) §二 收敛①）：上文「三方对照」（crew-go / eino / Claude Code）之外，**又有 3 个独立框架引入 SKILL.md + 渐进披露**——MAF、OpenAI Agents SDK、CrewAI。连同前三方，**6 个框架都有 SKILL.md**（含标准制定方 Claude Code；其中 **5 个采纳方** = 主纲 §收敛① 的「×5」）——收敛到 Anthropic Agent Skills（[agentskills.io](https://agentskills.io)）开放标准；Claude Code 官方原文佐证见 [`harness-survey.md`](harness-survey.md) §三。

| 框架 | SKILL.md + 渐进披露 | file:line（已核实） | 本篇是否已详述 |
|---|---|---|---|
| crew-go | ✅ | （本文全篇） | ✅ |
| eino | ✅ | `adk/middlewares/skill/`（§eino 设计核心） | ✅ |
| Claude Code | ✅（标准制定方） | `code.claude.com/docs/en/skills` | ✅ |
| **MAF** | ✅ 自述遵循 agentskills.io | `python/.../core/agent_framework/_skills.py` | ⬇️ 下方补 |
| **OpenAI Agents SDK** | ✅（仅 sandbox 作用域） | `src/agents/sandbox/capabilities/skills.py` | ⬇️ 下方补 |
| **CrewAI** | ✅ 3 级 DisclosureLevel | `lib/crewai/src/crewai/skills/models.py:25` | ⬇️ 下方补 |
| LangGraph（对照） | ❌ 核心无 | — | ✅ |

**3 个新框架的独特点（对应主纲 §三 action items）**：

| 框架 | 独特机制 | crew-go 现状 |
|---|---|---|
| **MAF** | ① 可执行脚本（`@ClassSkill.script` inline 函数 / CLI 命令）+ 附加资源（`@ClassSkill.resource`）② frontmatter 注入前 **XML 转义**（防 prompt 注入） | ① **go-func 已实现等效**（进程内 Go 函数，[`skill.md` §11](../../v2/skill.md)；MAF inline-func ≈ 我们 go-func）；MAF CLI/subprocess 类脚本执行我们收敛到 **tool 层 open-bash**（声明式脚本 action 路径已删，2026-06-30）② **已实现等效**（crew-go 用 markdown 非 XML，对应 parse 边界元数据清洗 + listing 信任框定，见 [`skill.md` §14](../../v2/skill.md)）|
| **CrewAI** | **3 级 DisclosureLevel 自动晋升**（METADATA→INSTRUCTIONS→RESOURCES，`with_disclosure_level()`） | 你**已有**「listing → `activate_skill`(body) → `ReadSkillResource`」三级结构，但靠 **LLM 显式调工具**逐级；CrewAI **自动晋升**——与你 L0 源头分层（[ISSUE-92](../../issue/issue-92-skill-source-layering.md)）方向一致，可借鉴自动化路由 |
| **OpenAI Agents SDK** | SKILL.md loader（`LazySkillSource` / `LocalDirLazySkillSource`），但**仅沙箱作用域** | 你 skill 是全局一等机制，比 OpenAI 完整 |
| **eino**（已详述，补一点） | **压缩时保护 skill**（`PreserveSkillsConfig{MaxTokensPerSkill, SkillsTokenBudget}`，#967） | 你 `SkillBodyAnnotation` 跨层保护——eino per-skill + 总预算双层裁剪更细（主纲 P1） |

> ⚠️ **标准的 6 个字段里 `allowed-tools` 单独标着「Experimental. Support for this field
> may vary between agent implementations」**（2026-08-27 调研读 agentskills.io 原文）。
> 这条豁免是承重的：它让「解析了但不强制」对这一个字段成为**合规**处置，而对 `paths`
> （标准里根本没有）/ `context`（厂商扩展）不成立。我们的处置据此分两档——`allowed-tools`
> 出声放行、写错的 `context` 值当场拒。
>
> **收敛结论**：SKILL.md + frontmatter + 渐进披露已是跨框架事实标准；**唯一可溯源血缘是 agentskills.io 公开标准**。⚠️ **原文写「各框架源码自引」，2026-08-26 逐仓 grep 后更正**：真正在源码里自引的只有 MAF 一家（`_skills.py` 5 处），CrewAI 只在一句测试 docstring 里，agentscope 引的是 Anthropic 博客不是标准，**eino 零命中**——它是纯粹借了格式、一句合规声明都没有。<br>**而且「采纳格式」≠「兑现语义」**：MAF / CrewAI 都解析了标准的 6/6 字段，却**只消费 name + description**；`allowed-tools` 六家里**只有 Claude Code 真的按它裁工具**。静默丢失发生在**消费层而非解析层**——比「结构体少个字段」更隐蔽，因为字段在 API 上可见、可读、看起来兑现了。**收敛点**（多框架共有）：name/description / 渐进披露 / `allowed_tools` 白名单（crew-go V2 + Claude Code + CrewAI）/ per-skill model（crew-go V2 + eino + Claude Code）。**crew-go 差异化**：执行模式（inline/fork/fork_with_context 三模式，与 eino 并列、超 MAF/OpenAI/CrewAI；Claude Code 4 模式最全）+ **crew-go 独有** CJK 关键词预匹配 + 资源类型化（eino/Claude Code 均无，见下文 §crew-go 当前优势）。**可补**：MAF 可执行脚本（注入防护已实现，见上 ②）、CrewAI 自动晋升。

> *本节（2026 H1 增量）audit：外部事实经独立 agent 读源码核实（MAF `_skills.py:55/1192`、OpenAI `skills.py:138`、CrewAI `models.py:25`）；自审 + 独立审共修 3 处框架/精度（并列 vs 独有、listing vs disclosure、per-skill model 标 V2）。并入最终跨文档批量 audit 再过。*

---

## crew-go 设计核心

`SkillBox` **10 方法**接口（vs eino Backend 2 方法 vs Claude Command 抽象——对照见 §三方对照表）：

```go
type SkillBox interface {
    Name() string; RootDir() string
    ListSkills() []SkillDescriptor
    MatchSkills(ctx, query string) []SkillMatchResult
    ListSkillResources(name string) []SkillResource
    ReadSkillResource(name, path string) string
    ReadSkillPrompt(name string) string
    ListSkillActions(name string) []SkillActionDescriptor
    ActivateSkill(ctx, name string) (*ActiveSkill, error)
    RunSkillAction(ctx, skillName, actionName string, args map[string]any) (*SkillExecutionResult, error)
}
```
业务注入 `SkillSource{SkillBox, Options: *SkillRuntimeOptions}`。**集成位置 / activate_skill 行为 / 4 节点
拓扑 / 行号软引用见 [skill.md §3](../../v2/skill.md)（用法权威），本节不重复。**

**加载 + 匹配机制要点**：
- 加载（`skillbox.go`/`discover.go`）：扫一级子目录读 SKILL.md → 自研 frontmatter parser（不引 yaml）；单文件
  失败 / 无 SKILL.md / rootDir 不存在均静默跳过。**未解析**：`inputSchema`/`outputSchema`/`actions`（spec
  预留，parser 无 case）。资源按扩展名归 `SkillResource{Path, ResourceType}`（.md→reference / .py→script /
  .j2→template / 其他→asset）。
- 匹配（`match.go`）：CJK 逐字切 + 英文 ≥2 过滤；归一化 `min(matched/max(len,2), 1.0)`；score>0 降序。
  `adaptive`/`pre_match` 走 `selectSkillAdaptive` 预匹配，`toolCall` 不预匹配（LLM 按 name 激活）。

---

## eino 设计核心

### 数据结构（`adk/middlewares/skill/skill.go`）

```go
// frontmatter 字段（用 yaml.v3 解析）
type FrontMatter struct {
    Name        string      `yaml:"name"`
    Description string      `yaml:"description"`
    Context     ContextMode `yaml:"context"`  // ""/"fork"/"fork_with_context"
    Agent       string      `yaml:"agent"`    // 指定走哪个子 agent（通过 AgentHub 解析）
    Model       string      `yaml:"model"`    // 指定 LLM 模型（通过 ModelHub 解析）
}

type Skill struct {
    FrontMatter
    Content       string  // SKILL.md body
    BaseDirectory string  // skill 目录绝对路径
}

// 简洁的 Backend 契约（只 2 方法）
type Backend interface {
    List(ctx context.Context) ([]FrontMatter, error)
    Get(ctx context.Context, name string) (Skill, error)
}
```

### 集成机制

- **以 middleware 形式存在**：`NewMiddleware(ctx, &Config{Backend, AgentHub, ModelHub, ...})` 返 `adk.ChatModelAgentMiddleware`
- `BeforeAgent` 钩子：把 skill system prompt 追加到 `runCtx.Instruction`、把 `skillTool` 追加到 `runCtx.Tools`
- `WrapModel` 钩子：检测 ctx 里 `__skill_active_model__` local value → 通过 `ModelHub.Get(name)` 替换 chatModel（per-skill 模型切换）

### 三种 context 模式（`skill.go:419-441` 的 `InvokableRun` switch；`401-411` 是 `Info()` 渲染描述）

```go
switch skill.Context {
case ContextModeForkWithContext:  // 派生子 agent + 携带父 history
    return s.runAgentMode(ctx, skill, true, argumentsInJSON)
case ContextModeFork:             // 派生子 agent + 不带 history
    return s.runAgentMode(ctx, skill, false, argumentsInJSON)
default:                          // inline：body 作 tool_result 返
    if skill.Model != "" {
        s.setActiveModel(ctx, skill.Model)
    }
    return s.buildSkillResult(ctx, skill, argumentsInJSON)
}
```

- inline 模式：与 crew-go 当前实现等价（注：`ContextMode` 仅定义 fork / fork_with_context 两个常量，inline 是 `default:` 分支无 enum 值）
- fork 模式：`AgentHub.Get(ctx, skill.Agent, &AgentHubOptions{Model: m})` 拿到独立 agent，启个新 Run（构造 `&adk.AgentInput{Messages: [UserMessage(skillContent)]}`），结果聚合后作为 tool_result 返父 agent
- fork_with_context 模式：同 fork 但 `messages = append(parentHistory, ToolMessage(skillContent, toolCallID))`——子 agent 看到父对话历史；`toolCallID` 由 `compose.GetToolCallID(ctx)` 从父 ctx 取（`skill.go:516`），缺了 ToolMessage 形式不合法

### 与 crew-go 的根本不同

1. **接口最小化**：Backend 只 2 方法（List/Get），其他都通过 `Skill` 结构体的 `BaseDirectory` 字段暴露给 LLM 自查
2. **fork 模式提供子 agent 隔离**：子 agent 的 LLM 调用不污染父 agent 的 message history（关键 use case：长任务分解避免上下文爆炸）
3. **per-skill model/agent 切换**：frontmatter 直接声明用哪个 model / 用哪个预定义 agent 跑这个 skill
4. **Backend 而非 SkillBox**：解耦了「加载来源」（filesystem / s3 / OCI / git）和「执行入口」——crew-go 把所有职责都放 SkillBox 接口里（10 个方法）

---

## Claude Code Skills 子系统（5 维度概览）

> Claude Code 是业内最完整的 skill 实现。**完整 6 来源 / 5 正交维度 / 6 层渐进披露 / 4 执行模式 / 28 SAFE
> 字段 / 源码行号索引见 [external-references/claude-code-skill-system.md](../external-references/claude-code-skill-system.md)**（707 行反混淆权威 + §4 工程窍门常量决策）。本节只留 5 维度 + crew-go 抄了哪些（三方对照所需最小信息）：

| 维度 | Claude Code 做法 | crew-go 抄了吗 |
|---|---|---|
| 1 渐进式披露（6 层：声明→索引→预算→展开 body→template→conditionally-active）| listing 三档预算 + body 按需展开 | ✅ 抄 listing 预算降级 + body 注入；conditionally-active（`paths` 条件激活）❌ 未实现 |
| 2 执行模式（inline / fork / remote-canonical / coordinator-summary）| 4 种 | ✅ 抄 inline + fork 两种 |
| 3 跨边界持久化（`invokedSkills` 全局态 + per-agentId 隔离 + POST_COMPACT 保鲜）| 自我修复协议 | ⚠️ crew-go 用 4 层压缩 marker 保护替代（治标，见 §不足 #10）|
| 4 学习反馈（`skillImprovement.ts` 后台评估 self-improvement loop）| 有 | ❌ 未抄（P3 慎做）|
| 5 文件系统响应（`skillChangeDetector.ts` chokidar 热重载）| 有 | ❌ 未抄（启动扫描即可）|

## LangGraph（无原生 skill 概念）

LangGraph 没有「skill」这个抽象——它的能力扩展机制是：
- **Tool**：标准 langchain tool（用 `@tool` 装饰器或 `BaseTool` 子类）
- **Subgraph**：用 `StateGraph` 套 `StateGraph` 做嵌套，子图作为 ToolNode 调用

业务想做 skill-style 能力包通常自己套：
1. 把 SKILL.md body 包成一个 `lazy_tool` 返 prompt 字符串
2. 或者把它包成 subgraph，主 graph 通过 conditional edge 路由进去

→ LangGraph 的取舍：宁可少一层抽象、用通用机制（tool/subgraph）灵活组合；缺点是没规范，业务各自实现 SKILL.md 加载、约定复用难。

→ crew-go 选择走 SKILL.md + activate_skill 协议，是因为 agent 框架要面向**业务直接可用**而不是 framework primitives，规约统一价值大于灵活度损失。

---

## 三方对照表（详细）

### 1. SKILL.md frontmatter 字段对比

| 字段 | crew-go | eino | Claude Code | 说明 |
|---|---|---|---|---|
| `name` | ✅ | ✅ | ✅ | 唯一标识 |
| `description` | ✅ | ✅ | ✅ | 一行描述（注入 system prompt） |
| `whenToUse` | ✅ | ❌ | ✅ | 触发场景描述（matchSkills + system prompt 用） |
| `tags[]` | ✅ | ❌ | ✅ | 关键词集合 |
| `examples[]` | ✅ | ❌ | ✅ | 触发示例 |
| `multiTurn` / `maxTurns` | **已删（2026-08-27）**：spec 语义是「skill 在主 AgentLoop 中允许占用的最大轮次」，预设「激活后跨轮持续活着」这个我们没有的状态；零 consumer 的 spec 占位 | ❌ | ❌ | 多轮 skill 上限 |
| `allowedTools[]` | ⚠️ spec / V2 仅解析入库未消费（`discover.go` 写 `desc.AllowedTools`，无运行时工具白名单强制，dead metadata）| ❌ | ✅ skill 内可调工具白名单 + contextModifier 临时提权 |
| `inputSchema` / `outputSchema` | **已删（2026-08-27）**：结构体有字段而 parser 无对应分支 → 恒零值，零 consumer | ❌ | ❌ | skill 输入输出 JSON Schema |
| `context` (fork mode) | ✅ V2 P2.C 实现（`SkillDescriptor.Context`，**inline / fork 两种**；`fork_with_context` **已删（2026-08-26）**，写它现在在解析期报错）| ✅ | ✅ frontmatter `context: fork` |
| `agent` (sub-agent name) | ✅ V2 P2.C 实现（`SkillDescriptor.Agent` + `WithSkillSubAgentResolver`）| ✅ | ✅ |
| `model` (per-skill model) | ✅ V2 P2.B 实现（`SkillDescriptor.Model` + `WithSkillModelResolver`；仅默认 BasicAgent fork 路径用，业务 sub-agent 自带 model）| ✅ | ✅ + `[1m]` 后缀延续 |
| `effort` (推理深度) | ❌ | ❌ | ✅ contextModifier 改 effortValue |
| `paths[]` (条件激活) | ❌ | ❌ | ✅ ignore 库 gitignore 风格 + brace expansion |
| `disableModelInvocation` | ✅ V2 P2.A 实现（`SkillDescriptor.DisableModelInvocation`）| ❌ | ✅ 用户专属 skill / LLM 不能调 |
| `userInvocable` | ✅ V2 P2.A 实现（`SkillDescriptor.UserInvocable`，默认 true，框架不强制；业务上层 UI / CLI 自行用此字段过滤 user-facing 列表）| ❌ | ✅ 与 disableModelInvocation 组合实现"用户/模型可调"二维 |
| `hooks` (skill 内嵌 hook) | ❌ | ❌ | ✅ frontmatter 声明 + `once: true` 自卸 |
| `argumentHint` / `argumentNames` | ❌ | ❌ | ✅ 用户调 /skill 时参数提示 |
| `version` | ❌（spec 预留，struct/parser 均未落地）| ❌ | ✅ |
| `aliases` | ❌ | ❌ | ✅ skill 别名 |

### 2. 加载 + 资源访问对比

| 维度 | crew-go | eino | Claude Code |
|---|---|---|---|
| 来源抽象 | `SkillBox`（10 方法）| `Backend`（2 方法）| `Command (type='prompt')` 统一抽象 + 6 类来源（bundled/disk/plugin/builtinPlugin/mcp/legacy commands）|
| 默认实现 | `DefaultSkillBox` 扫 rootDir | `filesystemBackend` 扫 baseDir | filesystem 自动 + bundled 编译进 binary + plugin 注入 |
| 子目录递归 | ❌ 仅一级子目录 | ❌ 仅一级 | ✅ `discoverSkillDirsForPaths` 文件操作时按需上行查找 `.claude/skills/` |
| YAML 解析 | 自研 parser（避免依赖）| `gopkg.in/yaml.v3` | 内部 frontmatterParser |
| 资源类型推断 | ✅ 按扩展名（reference/script/template/asset）| ❌ 不分类 | ❌ 不分类（用通用工具） |
| 资源访问方式 | `box.ReadSkillResource(name, path)` 框架受控 | LLM 用 `BaseDirectory` 自己拼路径 + 通用工具读 | LLM 用通用 Read / Bash / Glob 按需读 + bundled `files` lazy extract 到 disk |
| 路径穿越防护 | ✅ `strings.HasPrefix(full, skillDir+sep)` 校验 | ❌ 由通用工具方负责 | ✅ bundled extract 用 `O_NOFOLLOW|O_EXCL + 0o700 + per-process nonce` 防 symlink 攻击；运行时由通用 FileReadTool 的权限模型保护 |
| 热重载 | ❌ 启动一次性扫 | ❌ | ✅ chokidar + 三段式 debounce/polling/hook-block |

### 3. 激活协议对比

| 维度 | crew-go | eino | Claude Code |
|---|---|---|---|
| 工具名 | `activate_skill`（不可改）| `skill`（通过 `SkillToolName *string` 指针字段覆盖）| `Skill`（首字母大写）|
| 工具描述 | 动态列所有 skill：`name: description` | 模板化 + i18n（中英）| `getPrompt` 描述工具用法 + 单独的 `skill_listing` attachment 走 system_reminder 增量下发 |
| 工具参数 | `{skill: string}` | `{skill: string}`，`CustomToolParams` 可扩展 | `{skill: string, args?: string}` |
| 多激活模式 | spec 3 种全部实现（toolCall V1 + adaptive/pre_match V2 P1.B）| 3 种 context（inline/fork/fork_with_context）| 4 种执行模式 + paths 条件激活 + DiscoverSkills 实验功能（共 6 种激活路径）|
| 失败行为 | unknown skill → 返 `tool.Failure(ErrCodeNotFound,...)` Result (业务级失败, 反馈 LLM 让其换 skill 重试 — 注: `tool.IsFrameworkError` 是 categorization helper, framework 当前**不消费** error code, 所有 tool 错统一序列化进 tool_result 反馈给 LLM); **disable-model-invocation skill** → 返 `tool.Failure(SkillDisabledErrCode="skill_disabled",...)` Result (同款业务级失败 V2 P2.A) | unknown skill → `fmt.Errorf` 抛 Go error 返调用方 | `validateInput` 在 callable 前拦截：unknown / disableModelInvocation / non-prompt 均返 ValidationResult error |
| 权限模型 | ❌ | ❌ | ✅ `checkPermissions`：deny rules / allow rules / SAFE_SKILL_PROPERTIES allowlist 自动放行 / 否则 ask user |

### 4. 子 agent / 上下文隔离对比

| 维度 | crew-go | eino | Claude Code |
|---|---|---|---|
| inline 模式 | ✅ **默认模式**（`context` 未声明 / `inline`）| ✅ `default:` 分支（无 enum 值）| ✅ 默认 |
| fork 模式 | ✅ **V2 P2.C 实现**（`context: fork`，业务通过 `WithSkillSubAgentResolver` 注 sub-agent，或框架默认 BasicAgent + skill body 作 sysPrompt + parent model 兜底）| ✅ AgentHub.Get + 新 AgentInput | ✅ executeForkedSkill + 独立 agentId + 独立 token budget |
| fork_with_context | ❌ **已删除（2026-08-26）**——`context` 键不在 agentskills.io 标准里，Claude Code 只认 `fork` 一个值，这第三档是 eino 发明的且只有它实现。我们「预留兼容」的对象只有 eino 一家，而抄进来还不兑现语义。现在写它在**解析期**报错要解决"通过 ctx 传父 session"协议 | ✅ history 复制 + ToolMessage 包 skillContent | ⚠️ 单独 forked，无 with-context 变体（设计哲学：fork 就完全隔离）|
| 子 agent 选择 | ✅ `SkillDescriptor.Agent` + `WithSkillSubAgentResolver(name→Agent)`（V2 P2.C）| `frontmatter.agent` → AgentHub.Get | frontmatter `agent:` 字段 |
| skill body 进 token 位置 | 默认 tool_result；P2.D `WithInlineSkillBody(true)` 启用后改走 `[system_reminder]` UserMessage 注入对话流（terse-ack tool_result + body UserMessage **都打 marker**——R19，与 Claude Code 等价） | inline: tool_result; fork: UserMessage; fork_with_context: ToolMessage | inline: **`newMessages` 注入对话流（非 tool_result）**；fork: 独立子 agent UserMessage |
| 跨 sub-agent 状态隔离 | ❌（无概念）| 子 agent 独立 | ✅ per-agentId 复合 key 防串台 + 完成时 clearInvokedSkillsForAgent 释放 |

---

## crew-go 设计哲学差异 (2026-05 audit follow-up #8, 三方对照后保留)

crew-go 跟 Claude / eino 在几个关键设计上故意不一致, 是经过权衡的合理选择, 不是 V1 包袱:

### 1. fork body 走 SystemPrompt 而非 UserMessage (vs Claude)

**Claude 协议** (`cc-local/src/utils/forkedAgent.ts:224`): skill body 作 **UserMessage** 注入 forked agent (`promptMessages = [createUserMessage({content: skillContent})]`), 跨 compact 走 `invokedSkills` Map + `POST_COMPACT_MAX_TOKENS_PER_SKILL=5_000` 显式保鲜协议.

**crew-go 协议** (`agent_v2/skill_tool.go::runForkMode`): skill body 作 **SystemPrompt** 注入 sub-agent (`NewBasicAgent(WithName("skill_"+name), WithSystemPrompt(body), WithModel(subModel))`), 跨 compact 走 **system slot 不压**的天然保护 + `bodyMaxChars` 截断防 ctx 爆 (`DefaultSkillBodyMaxChars`).

**为何不抄 Claude**:
- crew-go SystemPrompt 设计**零业务负担**——业务不需要接 `invokedSkills` 协议, 4 层压缩协议 (AutoCompact) 自动跳 system token 估算
- Claude UserMessage 设计**需要业务接 invokedSkills**——crew-go 改 UserMessage 等于失去 system 不压保护 + 引入业务侧 4 层压缩对 `SkillBodyAnnotation` marker 防护负担
- 两种**等效设计**, crew-go SystemPrompt 更适合 Go-native 业务 (零额外协议)

### 2. WithInlineSkillBody 保留可选 toggle (vs Claude 唯一行为)

**Claude 协议**: skill body 走 `[system_reminder]` UserMessage 注入对话流——**唯一行为, 无 toggle**.

> **增量 listing 已删除（2026-07）**，`WithIncrementalSkillListing` 不再存在——listing 统一
> 走 `activate_skill` 工具 description 全量。这个结论绕了三圈才站住（初次删除依据有误 →
> revert → 一度决定保留 opt-in → audit 查出同一根因第 6 个 bug 后重删），全过程与六家实测
> 见下节。**下节的对照仍然有效且值得读**：它记录了为什么框架层不该把 listing 放对话流。

**crew-go 协议**: 两条路径业务可选:
- `WithInlineSkillBody(false)` (默认): tool_result content 全文 — 业务 hook 解析 body / 前端 SSE 渲染 skill body **直接读 tool_result** 友好
- `WithInlineSkillBody(true)`: 走 Claude 风格 `[system_reminder]` UserMessage — 跨 compact 友好 + LLM API token 节省 (tool_result terse)

**业务何时选哪个**:
- **Go-native 业务 hook 解析 body** (e.g., 业务 OnToolEnd hook 解析 tool_result content 自定义渲染) → 走 `false` (默认)
- **跨 LLM API token 优化 / 长会话 + AutoCompact 频繁** → 走 `(true)`
- **跨 LLM 生态拷 Claude SKILL.md 跑** → 走 `(true)` 行为对齐 Claude

**为何不删 option 强制对齐 Claude**:
- 业务**真有人用** false 路径 (example/graph/v2_skill_demo + 18 测试覆盖两条路径)
- "项目未上线不留兼容路径"规约目的是**不为旧版本保留 deprecated path**, option 不是 deprecated 是当前真正运行时选择
- 删 option 等于剥夺业务选择权——crew-go 多出来的灵活性是 Go-native 优势, 不该砍

### 2.1 增量 skill listing：三方机制与「真相源」之争

#### 先看分野：做不做增量，按「框架 vs 产品」分得干干净净

2026-07 实测六家，无一例外：

| 实现 | 定位 | listing 载体 | 增量机制 | 证据 |
|---|---|---|---|---|
| **eino** | 框架 | 工具 Description | **无** | `adk/middlewares/skill/prompt.go` `toolDescriptionTemplate` + `{{range .Matters}}` 全量；`announced/incremental/dedup` 关键词 grep 零命中 |
| **MAF** | 框架 | Instructions（system prompt） | **无** | `Microsoft.Agents.AI/Skills/AgentSkillsProvider.cs:234` `Instructions = BuildSkillsInstructions(skills)`；其 `Deduplicating*` 是**同名多源去重**不是已宣布名单 |
| **OpenAI Agents SDK** | 框架 | Instructions | **无** | `agents/sandbox/capabilities/skills.py:109` "rendered into instructions"；lazy 的是 **body 路径**，`:71` 明说 index 本身 "present up front" |
| **LangGraph** | 框架 | 无 skill 概念 | — | skill 相关源文件 0 个 |
| **Claude Code** | 产品 CLI | 对话流 reminder | **有** | `sentSkillNames`（`attachments.ts:2607`） |
| **Grok** | 产品 CLI | 对话流 reminder | **有** | `announced_names`（`mod.rs:107`） |
| **crew-go** | 框架 | **工具 Description** | **无**（曾有 opt-in 开关，2026-07 删，见决策记录） | — |

**三家框架零家做增量，两家产品 CLI 全做。** 第一性解释：框架不知道业务的会话长度、
skill 规模、provider 缓存策略，只能给**简单可预测**的默认——全量放稳定前缀（工具 desc /
system prompt）被 prompt caching 覆盖，成本可控、行为确定；产品 CLI 面对的是自己的真实
场景（超长会话、上百 skill、自控 provider），既值得精细优化，**也有能力长期维护那套状态
同步**。

⟹ **crew-go 站框架这边：listing 只走工具 description 全量，不提供对话流增量的开关。**

曾经保留过一个 opt-in 开关（想两头都占），最终删掉——不是因为产品级需求不真实，而是因为
**这个位置本身会持续产生 bug**：把一份「必须一直在模型眼前」的状态放进会被压缩 / 截断 /
换源 / 重建的对话流，就得在每一条动对话流的路径上单独记得保护它。删除前 `SkillListingAnnotation`
散布在 **5 个文件 14 处**，漏一处即静默失效——[ISSUE-99](../../issue/issue-99-listing-invariant-broken-by-kv.md)
是同一根因的第 6 次表现。放回工具 description 之后，这些防护代码归零，因为工具定义每轮
重新生成、压根不存在"被压缩掉"。

产品级场景真需要时：git history 有完整实现，本节有六家实测与全部设计取舍。

#### 再看细节：做增量的两家，分歧在「名单存在哪」

**结论先行**：Claude Code 与 Grok **都**把 listing 放对话流并按「已宣布名单」做增量，
**都会累积多条**（首次全量一条 + 后续增量各一条），谁都没做到「恒一条」。三家真正的
分歧不在增量与否，而在**「已宣布过什么」这个名单存在哪**。

| 维度 | Claude Code | Grok | crew-go |
|---|---|---|---|
| listing 载体 | 对话流 `system_reminder` | 对话流 `<system-reminder>` | **工具 description 全量**，不进对话流 |
| 名单载体 | `sentSkillNames` Map（`attachments.ts:2607`） | `announced_names` HashSet（`mod.rs:107`） | **挂在 listing 消息的 annotation 上**（`skillListingNamesAnnotation`） |
| 名单真相源 | 运行时**独立状态**，resume 时扫消息补 | 运行时**独立状态**，单独持久化后恢复 | **始终从对话历史推导**（无独立状态） |
| 跨 session 防重发 | `restoreSkillStateFromMessages` 扫到既有 `skill_listing` attachment 就 `suppressNextSkillListing()` 闩，下次**整个跳过一次**（`conversationRecovery.ts:382` / `attachments.ts:2633`） | `restore_announced_names()` 恢复**完整名单** + seed 见名单非空则不排 pending（`mod.rs:283/334`）——粒度更细，恢复后若有**新增** skill 仍能只宣布新增那部分 | 每次 buildListing 都从 prior 消息回收名单（`basic_agent.go:665-690`），无需"恢复"动作 |
| **跨 session 新增的 skill** | ❌ **漏宣布**（`attachments.ts:2629-2631` 自认 trade-off：skills added between sessions won't be announced until the next non-resume session） | ✅ 正常宣布（恢复完整名单，差集自然算出新增） | ✅ 正常宣布（回收的也是完整名单） |
| **compact 后** | **不重置名单**——重发完整 listing 是 `pure cache_creation`，太贵（`postCompactCleanup.ts:65-69`） | **清空名单**（`on_compaction`，`mod.rs:525`）+ 全量 listing 写进摘要（`format_compaction_skill_listing`，`listing.rs:643`） | 不需要处理（见下） |
| 重建对话防双份 | 无此场景 | `retain` 剥掉既有 reminder 再注入（`session_setup.rs:80-89`），仅 `initialize()` / 切模型触发 | 无此场景 |
| 运行中动态发现 | 启动时确定 | `SkillUpdateKind::Discovery`——模型进入含 SKILL.md 的目录即发现并宣布 | 启动时确定 |
| listing 预算 | 未查证 | `SKILL_BUDGET_CONTEXT_PERCENT`，超预算降级 names-only 并可丢尾部 | `SkillListingBudgetChars` + per-entry 截断 |

**compact 处那一行是两家的正面分歧**：Claude Code 赌「不重发」（省 cache_creation），
Grok 两手准备（信息进摘要 + 允许重新宣布）。粒度上 Grok 更细——恢复名单后若**新增**了
skill 仍只宣布新增那部分，而 `suppressNextSkillListing` 是整个跳过一次。

**crew-go 为什么两边的机制都不需要**：名单不是独立状态，而是**跟着 listing 消息走**
（`skillListingNamesAnnotation` 挂在消息 annotation 上）。于是「名单」与「对话」天然同步：

- listing 还在历史里 → 名单回收得到 → 不重发
- listing 被压掉了 → 名单回收不到 → 自动重发（自愈）

**更一般的观察**：名单与对话的对账是**不可避免**的——差别只在于「在哪些时点对账」。
两家都是独立状态，于是必须挑时点补做：Claude Code 在 resume 时扫消息设闩
（`restoreSkillStateFromMessages`），Grok 在 resume 时恢复名单、又在 compact 时清名单
（`on_compaction`）。**每多一个会改动对话的操作，就多一个必须记得挂钩子的时点**——漏挂
即失配。crew-go 把名单挂在消息上之后，「读」本身就是对账，不存在"漏挂时点"这个失效
模式。

同理 Grok 的 `retain` 剥旧对我们无用——它服务于「切模型时重建整个 conversation，
`rewrite_zero_turn_prefix` 保留了继承来的 reminder，再注一条就双份」这个 Grok 特有场景，
crew-go 没有「重建对话」这个动作。

**为什么 crew-go 不做这件事**（写死在此避免重复调研）：listing 放在**工具 description**
里，内容每轮一致、落在请求的稳定前缀，被 provider 的 prompt caching 命中，**重发的实际
成本≈0**——增量优化的收益因此接近于零。曾经那份「省 ~25K token」的估算隐含了「工具定义
每轮重发且不被缓存」这一前提，主流 provider 缓存工具定义时该数字不适用。

**唯一会让这笔账翻转的情形**：实测某主力 provider **不缓存工具定义**，或 skill 集大到工具
定义本身撑爆首轮预算。真遇到时请先读 [ISSUE-99](../../issue/issue-99-listing-invariant-broken-by-kv.md)
——它记录了上一次把 listing 放进对话流的代价（5 文件 14 处防护、同一根因 6 个 bug），
再决定是重新引入还是换个不进对话流的位置（例如每轮重新生成的 system 段）。

### 3. `paths` 字段 silent 忽略 (未实现条件激活)

**Claude 协议** (`cc-local/src/skills/loadSkillsDir.ts:159-178`): `paths` 字段配 gitignore 风格 (`paths: "**/*.tsx, **/*.ts"`) → 业务用 `Read/Write/Edit` tool 操作匹配文件时才暴露此 skill (conditionalSkills/dynamicSkills 二阶段).

**crew-go 协议**: parser **silent 忽略未知字段** (skill/discover.go:115-163 switch 无 `paths` case), 业务从 Claude 拷的 SKILL.md 含 `paths` 字段时:
- `name / description / context / agent / model` 等已识别字段正常生效
- `paths` 字段 **silent 丢失** — LLM 始终看到此 skill 无条件激活

**业务跨生态拷 SKILL.md 注意**:
- 字段语义不变 (skill 仍可激活, 内容不变)
- 行为差异: crew-go 总是暴露 skill, Claude 只在打开匹配文件时暴露
- 业务真需要条件激活走 P3 (IDE 集成时一并加 `SkillDescriptor.Paths` + 文件 hook runtime 消费), 不做"占位 parse"(加字段不消费让业务误以为有功能)

---

## crew-go 当前优势

1. **CJK 友好的 keyword matching**——eino / Claude Code 都没把 skill 选择前置，全推给 LLM；crew-go 的 `MatchSkills` 提供"调 LLM 之前先按关键词初筛"基础能力，未来 adaptive 模式直接用
2. **spec / runtime 解耦**（2026-10-21 起不再适用：crew-go 不再依赖公司 spec）——公司 spec 的 `lib/crew-go/modules/skill/skill.tsp` 完整定义 9 model + 1 enum + 3 interface，多语言 runtime 实现可对齐；eino 的 skill 中间件没对应跨语言 spec
3. **资源类型化**——`SkillResource.ResourceType`（reference/script/template/asset）让业务在 UI 区分展示 skill 包含的不同类型文件（eino / Claude Code 把所有文件等同处理）
4. **路径穿越防护**——`ReadSkillResource` 强制校验 `strings.HasPrefix(full, skillDir+filepath.Separator)`（`skill/skillbox.go:202`）避免 `../../../etc/passwd` 类攻击；拼 `Separator` 防 `/foo` 与 `/foobar` 假阳性匹配；eino 因为暴露 BaseDirectory 让通用工具读，由通用工具方负责（攻击面分散）
5. **go-func action 执行已落地**——spec 的 `SkillActionDescriptor + RunSkillAction` 以 go-func 进程内实现（`WithSkillActionFunc` 注册 + `run_skill_action` 工具，详见 [`skill.md` §11](../../v2/skill.md)）；eino 完全没 action 概念（仅 fork-LLM）。声明式脚本 action 路径（含 `RunSkillScript`）已删（2026-06-30，spec 占位无 consumer），脚本执行收敛到 tool 层 open-bash（同 eino `Shell`/Claude Bash）
6. **未识别 frontmatter bool 值保留默认（V2 P2.A R12/R13 audit 收敛）**——业务写 `disable-model-invocation: maybe`（typo）/ `user-invocable: 是的`（中文）/ 任何不在识别表的值，框架返 `(false, recognized=false)` → caller 跳过赋值 → 保留字段默认；vs Claude Code `parseBooleanFrontmatter`（`utils/frontmatterParser.ts`）单纯返 false 会**静默翻转默认 true 字段**（user-invocable 默认 true 写 `maybe` → false）。同语义 bool 字段（multi_turn / disable-model-invocation / user-invocable）统一走 `parseBoolOptional` 3 态识别，避免一个 helper 一个语义

---

## crew-go 当前不足

> 前 5 项是 R1 自审已知；后 6 项是 R1-R4 cross-audit 揭示的 Claude Code 维度缺口

### 已知不足（R1）

1. ~~**只实现 toolCall 模式**~~（**V2 P1.B 已实现 adaptive / pre_match**）—— `agent_v2/adaptive_skill.go` + `WithSkillSelector(SkillSelectorConfig{Model, MinScore, MaxCandidates, SystemPrompt})`；`SkillSource.Options.ActivationMode` 现在 runtime 真消费。**spec `SkillRuntimeOptions.PreMatch`（`SkillPreMatchOptions`：TopK / 阈值 / MaxActiveSkills / UseEmbedding）已删除**（2026-06）—— 从 spec sync 进来后 runtime 零消费，按"无人调=未完成""不留兼容路径"删；crew-go 用更简 `SkillSelectorConfig` 替代。**skill 多激活暂缓决策**：默认 toolCall 已支持 N 激活（LLM 跨轮反复 activate_skill），行业 7 框架 skill 层无一做框架 TopK 预选，故不在 adaptive 加 MaxActiveSkills 复杂度（详见 skill.md §12）
2. ~~**无 fork / fork_with_context 模式**~~（**V2 P2.C 实现 fork**；`fork_with_context` **已删（2026-08-26）**——不在 agentskills.io 标准里，写它现在在解析期报错）—— `SkillDescriptor.Context = "fork"` 触发 sub-agent spawn；`WithSkillSubAgentResolver(name→Agent)` 业务注入预配置 sub-agent，或框架默认 BasicAgent（SystemPrompt = skill body + parent model 兜底）。`activate_skill({skill, query})` LLM 必传 query。隔离 RunSession 跑 sub-agent，final answer 作 tool_result 返父 agent
3. ~~**无 per-skill model 切换**~~（**V2 P2.B 实现**）—— `SkillDescriptor.Model` frontmatter 字段 + `WithSkillModelResolver(name→ChatModel)` 业务注入解析器；仅 fork 默认 BasicAgent 路径用（业务自定义 sub-agent 自带 model）。SQL skill 走 GPT-4 / 翻译 skill 走轻量 model 等场景能做了
4. **无子目录递归**——`scan` 只读一级子目录，组织层级深的 skill 仓库（按业务线分子目录）需要扁平化
5. ~~**system prompt 提示是固定中文 hardcode**~~（**V2 P1 Task 2 修复**）——`WithSkillSystemPrompt(string)` 允许业务自定义 hint 文案，仅 toolCall 模式生效（adaptive / pre_match 用 skill body 直接拼）
6. ~~**无 RunSkillAction 实现**~~（**go-func 已实现**：`DefaultSkillBox.RunSkillAction` 进程内调 go 函数；`WithSkillActionFunc` 程序化注册 + `ListSkillActions` 返注册的 action + LLM 经 `run_skill_action` 工具触发；详见 [`skill.md` §11](../../v2/skill.md)）——**声明式脚本 action 路径已删**（2026-06-30）：`RunSkillScript` / `WithSkillScriptAction` / python·bash·node runtime / `ScriptExecutor` 沙箱 seam / SKILL.md frontmatter `actions:` 解析（`parseActions`）当初是对齐 `skill.tsp` 占位建的，零生产 consumer；三方调研实锤无框架在 skill 层做声明式脚本执行（eino `Shell`/`execute` tool、Claude Code Bash 都走 tool 层 open-bash），故删 script 路径、脚本执行收敛到 tool 层
7. ~~**`MatchSkills` 实现但未被 `activate_skill` 流程调用**~~（**V2 P1.B 修复**）—— adaptive / pre_match 模式用 `MatchSkills` 做预筛（`selectSkillAdaptive`）。`toolCall` 模式仍不走预筛（LLM 直接按 name 激活）

### Claude Code 维度对比揭示的新缺口（R2-R4）

8. ~~**无预算裁剪**~~（**V2 P1.A 修复**）—— `agent_v2/skill_tool.go::formatSkillListing` 三档降级 + UTF-8 安全截断；`WithSkillListingBudget(totalChars, perEntryChars int)` 业务可调整阈值。仅 toolCall 模式生效
9. **无增量 listing 下发** —— 曾实现（P2.E）后**已删除**：listing 塞进会话历史流会跨 session 累积且被压缩层保护删不掉。现统一走 `activate_skill` 工具 description 全量列 skill——走 API tools 参数、被 prompt caching 缓存、不进历史流。理由见 [conversation-history.md §六](conversation-history.md)
10. ~~**无跨 compaction body 保鲜**~~（**V2 P1.C 已实现 toolCall 模式**）—— `SkillBodyAnnotation` marker + AutoCompact `splitProtectedGroups` 按 group 保留（保 tool_call/tool_result pair 完整避免 OpenAI 422）+ ReactiveCompact `isProtectedGroup` 加 SkillBody 检查跨层一致。**已知限制**：仅 toolCall 模式（adaptive / pre_match 已通过 sysPrompt 注入 skill body 不经 tool_result，AutoCompact 本身不动 system，天然保鲜）；不像 Claude `POST_COMPACT_MAX_TOKENS_PER_SKILL=5_000` 做单 skill 预算裁剪——crew-go 整组保留更简单，trade-off 是 token 上多花一些。**📋 但此"压缩时保护"是治标：L0 源头分层方案（见 §skill 源头分层方案设计）落地后移除此专门保护——reference 改按需读取走通用压缩、body 层不再常驻（但 reference 多轮重复读仍需 per-Run 去重兜底，见该节风险 #4）**
11. **无文件热重载**——业务改 SKILL.md 必须重启 agent 才能生效，DX 差。Claude 的 chokidar + 300ms debounce + 2s polling + ConfigChange-hook-block 三段式可直接抄
12. **无权限模型 + safe properties allowlist**——Claude 的"白名单优于黑名单"安全设计（28 字段 SAFE_SKILL_PROPERTIES，新加未知字段默认 ask user）值得借鉴，未来 V2 加权限时直接抄
13. ~~**无 disableModelInvocation / userInvocable 二维**~~（**V2 P2.A 修复**）—— `SkillDescriptor.DisableModelInvocation` + `SkillDescriptor.UserInvocable` 两字段 frontmatter 解析 + 3 处独立防御（`activate_skill.Description()` listing 过滤 + `Call()` 防御 + `selectSkillAdaptive` 候选过滤）。`UserInvocable` 默认 true 框架不强制（v2 无 slash command），仅作为 metadata 供业务上层 UI / CLI 自行过滤 user-facing 列表。**关键设计**：未识别 frontmatter 值（typo / "maybe"）保留默认而非静默翻转——bool 字段统一走 `parseBoolOptional` 3 态识别（R12 audit MED-1 + R13 audit LOW-1 收敛）

---

## crew-go 待审 bug：H3 已审 + 无隐患（2026-05）

> R4 audit H3：Claude Code 在 sub-agent 触发 compact 时区分 main vs subagent 防误清父 cache。曾担心
> crew-go 4 层压缩嵌入运行有同样隐患。**审计结果：无隐患**——crew-go 全局可变态最小化、**无任何
> "模块级 process cache"**（Claude 有 `getUserContext.cache` 等才需 main/sub 区分）。

AutoCompact breaker 双层物理隔离：① **per-BasicAgent**（`config.go::WithAutoCompact` 每次 `copied.breakers
= &sync.Map{}`，主要机制——sub-agent 嵌入各自 NewBasicAgent 不同 cfg）+ ② **per-sessionID**
（`autocompact.go::getOrCreateBreakerEntry` 补充，同 BasicAgent 跨 Run 不同 RunID）。注意 sub-agent 嵌入
**多数编排共享 parent session**（Sequential/Parallel/... 传父 session），故隔离主靠机制①。全局态梳理
（typeRegistry / breakers / 各 regex 常量；PlanAgent 的临时子 agent 配置 2026-10-19 起随存档走、不在实例上）全部已隔离或不可变。加固：
`agent_v2/compaction_test.go` 2 sanity test（`TestAutoCompact_BreakerIsolatedAcrossSessions` +
`_AcrossCfgInstances`）守护两隔离层。**业务警示**：业务 `store.Store` 实现若引入"父子共享 cache"语义，
子 agent invalidate 影响父——业务责任、框架不防御，建议按 RunSession 维度隔离 cache。

---

## skill 源头分层方案设计（L0）→ 完整设计见 ISSUE-92

> toolCall 模式 SKILL.md body 跨 compaction 累积（§不足 #10）的**治本方案**：源头三级分层
> （L1 metadata always / L2 core body 触发 / L3 reference 按需 + 框架 deterministic 路由 + over-read
> 预算闸门），把累积主体从 body 层迁到 reference 层。业内 3-0 验证（Anthropic Agent Skills 三级官方 /
> SkillReducer arXiv 2603.29919 / AGENTS.md spec #135）。**完整设计（三级分层 / `read_skill_resource`
> 工具 / L3a-d 触发缓解 / over-read 预算闸门伪代码 / L2 简化边界 / 5 条风险 / 实施顺序）见
> [ISSUE-92](../../issue/issue-92-skill-source-layering.md)**。📋 待实施 2026-06。

## 决策记录

| 时间 | 决策 | 关联 ISSUE / commit | 理由 |
|---|---|---|---|
| v1 时代 | 选 SKILL.md + activate_skill 工具 protocol（不学 LangGraph 的 tool 化）| ISSUE-19 | Claude Code 的 SKILL.md 已是事实标准，规约统一让生态可复用 skill 包 |
| v1 时代 | spec 设计 3 种激活模式但 V1 仅实现 toolCall | ISSUE-19 | toolCall 是最简实现，让业务先用上；adaptive / preMatch 等业务有真实需求再做 |
| v1 时代 | 自研 frontmatter parser 不引 yaml 库 | ISSUE-19 | 减少依赖；frontmatter 字段简单（string/bool/int/string-list），不值得引第三方 |
| v2 重构 | skill 集成保持原 v1 API（`WithSkills(skill.SkillSource)`）| v2 channel-based 重构 | skill 子系统稳定，重构 channel-based 时 1:1 平移即可 |
| v2 重构 | `activate_skill` 工具改名内部不导出 → 后续 Sub-5f audit 改 export `ActivateSkillToolName` (observability 端识别 skill tool 调用) | v2 重构 + Sub-5f audit（`agent_v2/skill_tool.go::ActivateSkillToolName`）| 业务无须 import 内部 tool name 常量但 observability hook 需识别——折中 export const |
| 2026-05 audit R1-R4 | 决定不抄 Claude self-improvement loop 维度 | ISSUE-73 | 学习闭环涉及 LLM 评估 + survey UI + 改写 SKILL.md 三层耦合，业务复杂度大 ROI 低；agent_v2 走"业务 hook 自定义"扩展路径而不是框架内置 |
| 2026-05 audit R1-R4 | 决定不抄 Claude DiscoverSkillsTool（LLM-driven retrieval）| ISSUE-73 | 是 ant-only 实验功能 + 依赖 embedding 基础设施 + crew-go V1 用 MatchSkills 关键词预筛已经覆盖大部分场景 |
| 2026-05 audit R1-R4 | 决定**借鉴** Claude 跨 compaction 持久化 + 文件热重载 + main/subagent 区分 | ISSUE-73 | 这三个是产品级要点，对 crew-go 长会话 / DX / 嵌入运行 bug 预防有真实 ROI |
| 2026-05 V2 P1 第 1-3 批 | 实现 WithSkillSystemPrompt + WithSkillListingBudget（三档降级 + UTF-8 安全）+ adaptive/pre_match 模式（WithSkillSelector）| ISSUE-73 | 总 ~1500 行实现 + 38 测试 + 4 轮 audit（每轮 100% 命中率，共修 14 真 finding）。综合 audit H1 揭示了"修了一半"的典型 bug：P1.B audit 修了 `callSkillRouterLLM` 返 err 但 `selectSkillAdaptive` 转 strategy 字段返出去 → buildInitialMessages 不接 → control flow 信号丢失。综合 audit H2 揭示 adaptive 注大 SKILL.md body 进 system 时 AutoCompact 反复死循环——已 fix 改 `shouldTrigger` 跳 system token 估算 |
| 2026-06 | 决定做 L0 源头分层（三级 progressive disclosure + 框架 deterministic 路由 + over-read 预算闸门），**大幅缓解**（body 层）+ reference 层 per-Run 去重 替代 #10 的压缩保护 | ISSUE-92 | 业内 3-0 证据（Anthropic 三级官方 / SkillReducer less-is-more / AGENTS.md spec #135 分层≠压缩互补）+ crew-go `SkillResource` 数据模型雏形好（路由/解析/Description 需新建，非零成本）+ deterministic 路由（新增 export 的 `MatchReferences` 复用 keywordScore 算法）解 Vercel evals 实测 56%（对 Anthropic Agent Skills）触发漏读 |
| 2026-07 | **删除** `WithIncrementalSkillListing`，listing 统一走 `activate_skill` 工具 description 全量 | `54eb5e2` 删 → `dd37070` revert（依据被推翻）→ 复查后重删；详见 §2.1 + [ISSUE-99](../../issue/issue-99-listing-invariant-broken-by-kv.md) | **三次判断，最后一次才站住**：(1) 初次删除依据「业内一致走工具 description 全量」——在框架范围内成立（eino / MAF / OpenAI Agents SDK 零家做增量），但错在当成了全部，产品级 CLI 两家全做，故 revert；(2) 复查后一度决定保留作 opt-in 逃生舱；(3) 随后 audit 查出**同一根因的第 6 个 bug**（ISSUE-99，静默永久失效），才看清这不是孤立缺陷——**把「必须一直在模型眼前的状态」放进会被压缩的对话流**，导致删除前需 5 文件 14 处单独保护、漏一处即静默失效，且每新增一条动对话流的路径就要记得再加一处。而两个候选修法都不理想：「判定只认 prior」无业内先例（自创），「契约声明」依赖业务遵守。⟹ 与其用自创方案补一个框架层本不该有的问题，不如回到框架同侪的位置：把 listing 放在每轮重新生成、不会被压缩的地方，防护代码归零。**产品级需求真实存在但属产品层**，业务真需要时 git history 有完整实现、本节有六家实测与全部取舍 |

---

## 待办（potential V2 work，按 ROI 排）

> 开放状态以 [doc/code/todo.md](../../code/todo.md) 中心表为准（2026-09-07 归置：下方「P3 慎做」与「战略选择」
> 两表各对应中心表一行，改状态改那边）；本节只保留对照理由与工作量估算。

### 实施原则（写代码前必读）

V2 实施时三类 reference 来源的角色定位 + 抄/不抄边界：

| reference 来源 | 角色 | 怎么用 |
|---|---|---|
| **Claude Code 反混淆源码**（cc-local）| 设计哲学 + 工程窍门来源 | 学**设计取舍**（为什么这么做、解决什么问题）+ 抄**经验数值**（300ms debounce / 1% budget / POST_COMPACT_MAX_TOKENS_PER_SKILL=5_000 这类常量决策有 commit 教训）|
| **公司 spec go-runtime**（早期参考，2026-10-21 起不再依赖）| 接口契约 + 算法雏形 | 学**思路**（两阶段 retrieval + LLM 选择）；保**接口兼容**（SkillBox 10 方法 + frontmatter 格式）；**不抄代码**（spec 的 skill 层脚本 exec 占位 crew-go 已不采纳，见 R1 不足 #6）|
| **eino skill middleware** | 同领域不同实现的对照 | 看 fork 模式怎么和子 agent 协议对齐；不抄代码 |

**核心原则**：

1. **学思路自己写，不照抄代码**（包括 spec runtime）。理由：
   - spec / Claude / eino 的实现都带有各自架构取舍（spec 跟 runtime 包深耦合 + panic 校验 / Claude 跟 Bun event loop 适配 / eino 中间件模式），抄过来要清理
   - 自己写过一遍才能演进——业务来新需求时知道哪里能动
   - spec 的常量 / 阈值 / fallback 顺序都是 spec 团队的取舍，crew-go 业务可能要重新决定

2. **保持接口契约 + SKILL.md 格式跟 spec 兼容**。这是 spec 的真正价值——**让 skill 包能跨 runtime 复用**。

3. **每个常量决策要写注释说明 why**（参考 Claude Code 习惯：`RELOAD_DEBOUNCE_MS = 300 // 30 events 不 debounce 会 deadlock Bun event loop`）。

4. **安全模型设计先行**。涉及脚本执行 / shell 命令 / 跨进程的项必须先确定 P3 安全模型再实现。

---

### P1 必修（Claude 揭示的产品级缺口）

| 项 | 工作量 | 来源 | 备注 |
|---|---|---|---|
| ~~审 + 修 sub-agent 嵌入运行的 compact cleanup 隔离~~ | ✅ **已审无隐患**（双重隔离 + 2 sanity test 加固，详见 §"crew-go 待审 bug")| R4 H3 | **已关闭** |
| ~~跨 compaction skill body 保鲜~~ | ✅ **V2 P1.C 已实现** | R3 H1 | `SkillBodyAnnotation` + 4 层压缩跨层一致防护（AutoCompact splitProtectedGroups + ReactiveCompact isProtectedGroup）|
| ~~预算裁剪 listing（三档降级）~~ | ✅ **V2 P1.A 已实现** | R3 Layer 3 | `formatSkillListing` 三档 + UTF-8 安全 + `WithSkillListingBudget` |
| ~~`CustomSystemPrompt` 支持业务定制 hint 文案~~ | ✅ **V2 P1 Task 2 已实现** | R1 不足 #5 | `WithSkillSystemPrompt(string)` |
| ~~实现 `adaptive` 激活模式~~ | ✅ **V2 P1.B 已实现**（pre_match 一并实现）| R1 不足 #1 | `selectSkillAdaptive` + `WithSkillSelector(SkillSelectorConfig)`；学 spec 思路自己写、不抄代码（用 `model.ChatModel.Generate` 不用 HTTP 直调）。**综合 audit 修了 control flow 不 propagate + AutoCompact 算 system token 死循环 + toolkit 同名工具静默覆盖 3 真 HIGH** |

### P2 应做（业务诉求驱动）

| 项 | 工作量 | 来源 | 备注 |
|---|---|---|---|
| ~~`disableModelInvocation` / `userInvocable` 二维~~ | ✅ **V2 P2.A 已实现** | R1-R4 不足 #13 | `SkillDescriptor.DisableModelInvocation` + `SkillDescriptor.UserInvocable`；`activate_skill.Description()` listing 过滤 + `Call()` 防御 + `selectSkillAdaptive` 候选过滤 3 处独立防御；UserInvocable 默认 true 框架不强制（v2 无 slash command）；R12 audit MED-1 + R13 audit LOW-1 收敛 5 类炸弹防御 + bool 字段统一 `parseBoolOptional` 3 态识别 |
| ~~实现 `fork` / `fork_with_context` 模式~~ | ✅ **V2 P2.C 已实现 fork**；`fork_with_context` **已删（2026-08-26）** | R1 不足 #2 | `SkillDescriptor.Context = "fork"` + `WithSkillSubAgentResolver` 业务注入；隔离 RunSession 跑 sub-agent；返 SkillForkErrCode=skill_fork_failed 业务级失败让 LLM 重试 |
| ~~per-skill model 切换~~ | ✅ **V2 P2.B 已实现** | R1 不足 #3 | `SkillDescriptor.Model` frontmatter 字段 + `WithSkillModelResolver(name→ChatModel)`；仅 fork 默认 BasicAgent 路径用 |
| ~~inline body 走 newMessages 而非 tool_result~~ | ✅ **V2 P2.D 已实现** | R3 Layer 4 | `WithInlineSkillBody(true)` 启用后 tool_result 退化 terse ack，skill body 作为 `[system_reminder]` UserMessage 注入对话流；`SkillBodyAnnotation` marker **terse-ack tool_result + body UserMessage 都打（R19——两 group 都保护；AutoCompact 按 group 分组）**。仅 framework 注入的 `activate_skill` 成功调用时生效——业务工具不影响（防双层防护遮蔽） |
| 增量 listing 下发（`sentSkillNames` 等价）| ❌ **曾实现后删除** | R3 Layer 2 | listing 进历史流会跨 session 累积且压缩层删不掉；改为 tool description 全量（API tools 参数 + prompt caching）|

### P3 慎做（基础设施依赖大）

| 项 | 工作量 | 来源 | 备注 |
|---|---|---|---|
| 文件热重载（chokidar 等价 + 三段式 debounce）| 中（fsnotify + 50 行 Go）| R4 H2 | DX 提升明显，但要业务有"频繁改 SKILL.md"诉求 |
| paths 条件激活（gitignore 风格 + 文件 tool hook）| 大（要在所有文件操作 tool 埋激活点）| R3 Layer 6 | 仅当 crew-go 走 IDE/编辑器集成路线时做 |
| safe properties allowlist 权限模型 | 大（要先有完整权限模型基础设施）| R4 安全设计 | V2 加权限时直接抄 |
| 子目录递归扫描 | 小（`filepath.WalkDir` 替 `os.ReadDir`）| R1 不足 #4 | skill 数量大时再做 |
| ~~skill 层脚本 action 执行~~（已删，2026-06-30）| — | R1 不足 #6 | go-func action（`RunSkillAction`）已实现（进程内，无安全模型负担）；声明式脚本 action 路径（`RunSkillScript` + sandbox seam）已删——三方调研实锤脚本执行收敛到 **tool 层 open-bash**（eino `Shell`/Claude Bash），skill 层不再做（删除依据见 R1 不足 #6）|
| **Skill 命名强校验**（snake_case 目录名 + 目录名 == frontmatter.name + SKILL.md 一级标题 == name）| 小（30 行 discover.go）| spec runtime 提示 | 避免大规模 skill 包命名混乱；做成 opt-in option `WithStrictNaming(true)` 不破坏现有 skill |
| Markdown 内嵌 ``!`...` `` shell 执行 | 大（安全模型不清前不能做）| R3 模板替换 | 恶意 skill 可执行任意 shell |

### 战略选择

| 项 | 工作量 | 备注 |
|---|---|---|
| 支持 MCP 协议作为 skill 来源（消费 MCP server 的 prompts/tools）| 中（crew-go 已有 mcp 包，加 SkillBox 适配器）| 接入整个 MCP 生态（Cursor / Zed / Claude Desktop 都在推），跨生态价值巨大 |
| Skill self-improvement loop（5 turn LLM 评估 + 改写 SKILL.md）| 大（涉及后台 LLM 评估 + 改写文件 + UI）| 业内独家差异化但 ROI 不确定，**建议不做**——业务通过 hook 自定义 |

---

## 跨包依赖

```
schema ← skill ← agent_v2.WithSkills (config option)
                 ↓
                 agent_v2.skillTool (internal, not exported)
                 ↓
                 agent_v2.BasicAgentBuilder (toolkit injection)
```

skill 包**不依赖** compose_v2 / agent_v2 / hook，只依赖 schema 和标准库——保持 skill 子系统独立可被任何 runtime 复用（包括未来 v3 / 第三方 runtime）。

---

## 相关代码定位

### crew-go 自身

| 主题 | 文件 | 关键行 |
|---|---|---|
> **行号软引用** (audit follow-up #8): basic_agent.go 重构成 preamble+model+tools+postamble 4 节点后行号全 stale, 改函数名引用. 业务定位走 `grep -n <funcName>` 即可.

| 主题 | 文件 | 函数名 / 协议入口 |
|---|---|---|
| SkillBox 接口 | `skill/skillbox.go` | `SkillBox`, `SkillSource`, `DefaultSkillBox` |
| DefaultSkillBox 实现 | `skill/skillbox.go` | `NewDefaultSkillBox`, `*DefaultSkillBox` 方法 |
| SKILL.md frontmatter parser | `skill/discover.go` | `parseFrontmatter` switch 段 |
| 关键词匹配 | `skill/match.go` | `MatchSkills`, `keywordScore` |
| WithSkills option | `agent_v2/config.go` | `WithSkills` |
| sysPrompt skill hint 注入 | `agent_v2/basic_agent.go` | `buildInitialMessages` (preamble) |
| modelFn toolkit 注入 | `agent_v2/basic_agent.go` | `makeModelFn` (clone toolkit + skillTool) |
| toolsFn dispatch 注入 | `agent_v2/basic_agent.go` | `makeToolsFn` (per-BasicAgent 单次 clone) |
| 内置 activate_skill 工具 | `agent_v2/skill_tool.go` | `ActivateSkillToolName` const + `skillTool` struct |
| AutoCompact（sub-agent 隔离）| `agent_v2/autocompact.go` | (全文, breakers per-BasicAgent + per-sessionID) |
| ReactiveCompact 断路器 | `agent_v2/reactive_compact.go` | (全文) |

### eino 对照源码

| 主题 | 文件 | 关键行 |
|---|---|---|
| eino 主入口 | `eino/adk/middlewares/skill/skill.go` | 199-411 |
| eino filesystem backend | `eino/adk/middlewares/skill/filesystem_backend.go` | 30-150 |
| eino system prompt 模板 | `eino/adk/middlewares/skill/prompt.go` | 1-154 |

### Claude Code（cc-local 反混淆源码）

完整源码行号索引（6 来源 / 28 SAFE 字段 / 各机制 file:line）见
[external-references/claude-code-skill-system.md](../external-references/claude-code-skill-system.md) §9。

## audit 报告（追溯本文档结论的来源）

本文档"Claude Code Skills 子系统（5 维度概览）"和"crew-go 当前不足 #8-#13"基于 R1-R4 audit 累计 ~3000+ 行 cc-local 源码审计。审计报告未沉淀为单独文档（结论已合并到本文），过程要点：

| 轮 | 找到 | 命中率 |
|---|---|---|
| R1（自审）| 6 层渐进式披露 + 11 决策（已述部分准确，但范围窄）| — |
| R2（独立 audit）| 5 个漏掉的子系统（forked / remote canonical / self-improvement / ranking / hot reload）| 100% |
| R3（独立 audit）| 4 个漏掉的跨边界持久化机制（invokedSkills / postCompactCleanup 黑名单 / resume 协议 / SKILL.md hooks）| 100% |
| R4（ROI 定向 audit）| 4 个工程窍门（fire-and-forget 加载 / debounce 常量 / **main-subagent 区分** / agentMessages 释放）| 100% |

**关键洞察**：R3 揭示了渐进式披露的"自我修复"机制（compaction/resume 后 skill body 不丢）；R4 H3 揭示 crew-go 在嵌入运行时可能有同样隐患（已在 §"待审 bug" 节列出）。

**未审范围**（按 CLAUDE.md "不承诺扫干净"原则）：
- MCP skill builder 路径（`mcpSkillBuilders.ts` Anthropic 内部 MCP 集成细节）
- Skill permission 完整路径（`SkillTool.ts:432-578 checkPermissions`，crew-go 没基础设施抄了无处接）
- Skill self-improvement 5-turn 触发判定细节（已决定不抄）
- coordinator multi-worker skill 隔离（crew-go 没多 worker 概念）
- UI / telemetry 类（crew-go 不需要）

---

# frontmatter 解析：手写 vs 真解析器（2026-08-27 实挖）

> **状态：已落地（2026-08-27）**——`skill/discover.go` 已换成 `gopkg.in/yaml.v3`，7 条全部
> 消失（哨兵 `skill/parser_defects_internal_test.go`），BOM 那条按下面说的单独剥。
>
> 起因：一轮审计在 `skill/discover.go` 手写的 YAML 子集解析器上抓出 **7 条同源缺陷**
> （嵌套 `metadata:` 被拍平 / 多行 `|` 正文全丢 / 行尾注释吞值 / 引号里的逗号被切碎 /
> BOM 让整份不识别 / 非列表键收到列表的闸只挂在两条分支之一 / 别名表两份必须同步的拷贝）。
>
> **LangGraph 与 eino 没有 skill frontmatter 这个面**（eino 无 skill 概念），
> 所以这一节只有 Codex 可比。

## Codex 的做法（`codex-rs/skills/src/parser.rs`，2026-08-21）

**用 `serde_yaml` 真解析器**，不手写。工作区 `Cargo.toml` 里 `serde_yaml = "0.9"`。

### 值得学的四条细节

1. **schema 极小**：`SkillFrontmatter` 只有 **3 个字段**——`name` / `description` /
   `metadata.short-description`。对比我们的 `SkillDescriptor` **15 个**。
   **你不解析的字段，不可能变成「解析了但没兑现」。**
2. **嵌套就是嵌套**：`metadata` 是一个 typed 子结构（`SkillFrontmatterMetadata`），
   不是拍平。我们那条「`metadata:` 底下的 `name` 顶掉 skill 真实身份」在这个形状下不存在。
3. **连字符走 `rename` 写一次**：`#[serde(rename = "short-description")]`。
   我们的别名表是 `isListKey` 与映射 switch **两份必须同步的拷贝**，今天刚因此栽一次。
4. **缺字段是错误不是默认**：`description` 为空 ⟹ `SkillParseError::MissingField`。
   错误是 typed 的（`MissingFrontmatter` / `InvalidYaml` / `MissingField`）。

### 一条克制的例外，值得连它的克制一起学

`repair_frontmatter_scalar_fields`：第三方 skill 常写 `description: Build for AWS: ECS`
（未加引号的冒号），这是非法 YAML。Codex 对**这一个已知形态**做**行导向**的修复后重试，
注释写明理由是「**保持行导向，让不相关的非法 YAML 仍然浮出来**」。

**不是「宽容一点」，是「对一个已知良性形态定点宽容，其余照旧报错」。**

## ⚠️ 换解析器**不**解决的那条：BOM

Codex 的 `extract_frontmatter` 同样手写，判据是 `line.trim() == "---"`。
**Rust 的 `str::trim` 走 White_Space 属性，而 U+FEFF 是 Cf 类不是 White_Space**
（实测确认），全仓对 BOM **零处理零测试**。

**所以 Codex 有跟我们一样的 BOM bug。** frontmatter 的**切分**两边都是手写的，
真解析器只接管切分之后那一段。

> 这条是「挖三方要挖细节不只挖形状」的直接实证：形状（用 serde_yaml）抄对了，
> 细节（BOM）自己补，就是下一个 bug。

## 未知键：Codex 同一个仓里两套政策

| 结构 | 政策 |
|---|---|
| skill frontmatter（`SkillFrontmatter`） | **无 `deny_unknown_fields`** ⟹ serde 默认**静默忽略** |
| tool call 参数（`core/src/tools/handlers/*`） | **`deny_unknown_fields`** |
| MCP agent plugin config、`config/agent_roles.rs` | **`deny_unknown_fields`** |

**分叉不是「人写的 vs 机器写的」**（`agent_roles.rs` 是人写的配置却严格），
**是「schema 归谁所有」**：第一方配置（我们定 schema）严格；第三方 skill 文件
（schema 在生态里演进）宽松。

⚠️ **这条跟我们 `three-class-fix-plan.md` 里那条提案相反**——那份写的是「是行为开关
⟹ 报错」。按 Codex 的判据，skill frontmatter 的未知键**不该报错**；正确的做法是
**把 schema 收到只剩你真的兑现的字段**，那样根本没什么可未知的。
