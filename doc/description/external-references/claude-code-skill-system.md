# Claude Code Skill 系统完整设计

> **来源**：基于 `cc-local/src/` 部分泄漏反混淆 TS 源码（前后 4 轮 audit 验证，每轮 100% 命中率）。
>
> **审计范围**：~3000+ 行 TS 源码，覆盖 11 个子系统 + 4 个工程窍门。**未审范围**（按 CLAUDE.md "不承诺扫干净"原则）：MCP skill builder 内部细节 / 完整 permission 路径 / coordinator multi-worker / UI / telemetry 子系统——按 ROI 不审。
>
> **边界**：本文档是 **Claude Code 内部实现的逆向记录、非 crew-go 设计**——作三方对照 [skill-system.md](../three-way-comparison/skill-system.md) 的 Claude Code 一方源码细节附录。crew-go 对应 skill 设计见 [doc/v2/skill.md](../../v2/skill.md)。

---

## 0. 一图看全

```
                              ┌──────────────────────────────────────────┐
                              │  Skill = 按需加载的专项指导手册              │
                              │  统一建模为 Command (type='prompt')        │
                              └──────────────────────────────────────────┘
                                              │
       ┌──────────────────────────────────────┼──────────────────────────────────────┐
       │                                      │                                      │
   维度 1                              维度 2-3                                  维度 4-5
渐进式披露 6 层                       执行 + 跨边界                              学习 + 响应

启动声明机制 ─┐                   ┌── inline (newMessages 注入)              ┌── 5 turn 评估
            │                   │                                         │
增量listing ─┤── 6 层 ──→ ⚙️  ──  ┼── fork (子 agent 隔离)                   ├── SkillUpdate
            │                   │                                         │
预算裁剪三档 ─┤                   ├── remote canonical (AKI/GCS)            ├── survey 用户确认
            │                   │                                         │
调用展开body─┤                   └── coordinator-summary (worker 推迟)      └── applySkillImprovement
            │                                                                  改写 SKILL.md
资源按需读  ──┤                   ┌── invokedSkills (body 跨 compact)
            │                   │     + per-agent 隔离 + 5K 预算
paths条件激活┘                   │
                                ├── sentSkillNames (listing 跨边界)
                                │     + suppressNext for resume
                                │
                                └── chokidar 文件热重载
                                      + 300ms debounce + 2s polling
```

**核心数据流**：6 个来源 → `Command` 抽象 → `SkillTool` 调用入口 → 4 种执行模式 → 跨 4 类边界持久化 → 后台学习反馈

---

## 1. 核心抽象：Command + 6 类来源

### 1.1 skill 不是独立 runtime object

Claude Code 没发明专门的 SkillRuntime——skill 统一建模为 `Command (type='prompt')`，与用户的 slash command（`/help` `/compact` 等）共享同一个抽象。

**唯一构造点**：`skills/loadSkillsDir.ts:270-401` `createSkillCommand`

```typescript
return {
  type: 'prompt',
  name: skillName,
  description, hasUserSpecifiedDescription,
  allowedTools, argumentHint, argNames,
  whenToUse, version, model,
  disableModelInvocation, userInvocable,
  context: executionContext,        // 'inline' | 'fork' | undefined
  agent, effort, paths,
  contentLength: markdownContent.length,
  isHidden: !userInvocable,
  source, loadedFrom, hooks,
  skillRoot: baseDir,
  async getPromptForCommand(args, toolUseContext) {
    // body 加载 + 模板替换 + shell 执行 都在这里
  },
}
```

→ 任何 skill 最终都是「能 `getPromptForCommand` 出文本块的 Command」。SkillTool 不关心来源。

### 1.2 6 类来源 + 统一聚合

`getAllCommands` (`SkillTool.ts:81`) 聚合：

| 来源 | `loadedFrom` | 加载时机 | 特殊性 |
|---|---|---|---|
| **bundled** | `'bundled'` | 编译进 binary，启动注册 | 预算裁剪不截断 / telemetry 真名 / `files` 可 lazy extract |
| **disk** | `'skills'` | 启动扫 `~/.claude/skills/` + `.claude/skills/` | 业务自定义 SKILL.md |
| **plugin** | `'plugin'` | 启动扫 plugin 目录 | 第三方扩展 |
| **builtinPlugin** | `'builtinPlugin'` | Claude 内建插件 | 类似 bundled 但走 plugin 协议 |
| **mcp** | `'mcp'` | 运行时 MCP server 通过 prompts/list 发现 | 跨进程 |
| **commands_DEPRECATED** | `'commands_DEPRECATED'` | 兼容旧 `~/.claude/commands/` 目录 | 过渡兼容 |

`uniqBy([...local, ...mcp], 'name')`：local 优先，重名 MCP 被丢。

---

## 2. 五个正交维度

### 维度 1：激活与上下文管理（渐进式披露 6 层）

```
Layer 1：机制声明（启动时进 system prompt，常驻 ~50 token）
─────────────────────────────────────────────
constants/prompts.ts:382-384 hasSkills 块：
"/<skill-name> is shorthand for users to invoke a user-invocable skill.
 Use the Skill tool to execute them. IMPORTANT: Only use Skill for skills
 listed in its user-invocable skills section - do not guess."

       ↓ 每个对话回合

Layer 2：skill 索引（增量下发，走 system_reminder）
─────────────────────────────────────────────
utils/messages.ts:3728-3737：
  case 'skill_listing':
    return wrapMessagesInSystemReminder([createUserMessage({
      content: `The following skills are available...`,
      isMeta: true,
    })])

utils/attachments.ts:2607,2700-2730 增量去重核心：
  const sentSkillNames = new Map<agentKey, Set<name>>()
  // agentKey = toolUseContext.agentId ?? '' （主线程空字符串、子 agent 各自隔离）
  const newSkills = allCommands.filter(cmd => !sent.has(cmd.name))
  for (cmd of newSkills) sent.add(cmd.name)

附加机制：
- :2633 suppressNextSkillListing()  // resume 时既有 listing 已在 transcript
- :2641 FILTERED_LISTING_MAX = 30   // bundled+mcp 过 30 时降级 bundled-only
- :2664-2666 NODE_ENV === 'test' 早返
- :2669-2673 没 SkillTool 的 agent 不发 listing（子 agent 关键）

       ↓ 索引超 1% context window 时

Layer 3：预算裁剪三档（tools/SkillTool/prompt.ts）
─────────────────────────────────────────────
:21-29 常量：
  SKILL_BUDGET_CONTEXT_PERCENT = 0.01     // 1% context
  CHARS_PER_TOKEN = 4
  DEFAULT_CHAR_BUDGET = 8_000             // fallback (1% × 200K × 4)
  MAX_LISTING_DESC_CHARS = 250            // 单条上限

:88-170 三档：
  档 1: 全 fits → 全描述发
  档 2: 超 budget → bundled 全描述、其他截断到 maxDescLen
  档 3: maxDescLen < MIN_DESC_LENGTH(20) → bundled 全描述、其他降级 "- name"

bundled 永不截断的工程理由（注释）：
  "Per-entry hard cap... since the cap is generous enough to
   preserve the core use case"

       ↓ LLM 决定 "这个 skill 看起来匹配"

Layer 4：调用时展开 body（按需）
─────────────────────────────────────────────
SkillTool.ts:843-862 mapToolResultToToolResultBlockParam：
  if (status === 'forked')
    content = `Skill "X" completed (forked execution).\n\nResult:\n${result}`
  else  // inline
    content = `Launching skill: ${commandName}`   // 极轻 ~10 token

:766-840 真正 body 通过 newMessages 注入对话流：
  return {
    data: { success, commandName, allowedTools, model },
    newMessages: tagMessagesWithToolUseID(processedCommand.messages, toolUseID),
    contextModifier(ctx) {
      // 临时改 allowedTools / model / effort
      // 不污染父 ctx——只在这次 tool call 内生效
    }
  }

       ↓ skill body 提到 "用 templates/foo.j2"

Layer 5：资源按需读（通用工具）
─────────────────────────────────────────────
LLM 用通用 Read / Bash / Glob 直接读 skill 目录文件
loadSkillsDir.ts:344-369 三个模板变量：
  ${CLAUDE_SKILL_DIR}    → skill 目录绝对路径
  ${CLAUDE_SESSION_ID}   → 当前 session
  $ARGUMENTS             → 用户调 /skill 时传的参数

bundled skill 可声明 files: Record<string, string> 让框架 lazy extract 到
secure dir（O_NOFOLLOW|O_EXCL + 0o700 + per-process nonce 防 symlink 攻击）

       ↓ 用户读/写/编辑文件时

Layer 6：条件激活（环境触发）
─────────────────────────────────────────────
SKILL.md 里 paths: ["**/*.tsx"] 的 skill 默认存 conditionalSkills map 不暴露
loadSkillsDir.ts:1012-1041 ignore 库 gitignore 风格：
  const skillIgnore = ignore().add(skill.paths)
  if (skillIgnore.ignores(relativePath)) {
    dynamicSkills.set(name, skill)         // 移到激活集
    conditionalSkills.delete(name)
    activatedConditionalSkillNames.add(name)
  }

3 处文件 tool hook 触发：
  FileReadTool.ts:579-590
  FileWriteTool.ts:234,245
  FileEditTool.ts:408,422

paths 字段写法灵活（utils/frontmatterParser.ts:189 splitPathInFrontmatter）：
  - 逗号分隔字符串 / YAML 数组
  - brace expansion: {a,b}/{c,d}
  - glob: src/*.{ts,tsx}
  - 自动剥离 /** 后缀
  - 全 ** 视为 undefined（不应用条件激活）
```

---

### 维度 2：执行模式（4 种）

| 模式 | 触发 | 入口 | 行为 | tool_result 内容 |
|---|---|---|---|---|
| **inline**（默认）| 任意 skill | `processPromptSlashCommand` | `getPromptForCommand` 拿 body → `tagMessagesWithToolUseID` 包成 newMessages → 加 contextModifier | `"Launching skill: xxx"` 极轻 |
| **fork** | frontmatter `context: fork` | `executeForkedSkill` (`SkillTool.ts:122-289`) | `prepareForkedCommandContext` → `runAgent({ agentDefinition, promptMessages })` 派生独立子 agent → 跑完 `clearInvokedSkillsForAgent` 释放 | `"Skill X completed (forked execution).\n\nResult: ..."` 含完整结果 |
| **remote canonical**（ant-only 实验）| `_canonical_<slug>` 前缀 | `executeRemoteSkill` (`SkillTool.ts:969-1108`) | `loadRemoteSkill(slug, meta.url)` 从 AKI/GCS 拉 SKILL.md → 注入 user message（不走 slash-command 展开）| 无（直接注入对话）|
| **coordinator-summary** | 主线程 coordinator 模式 + 非 sub-agent | `processSlashCommand.tsx:830-870` | 不加载 body，发"这个 skill 给 worker 用"摘要 | summary 文本，body 推迟到 worker 真用时 |

#### inline 模式核心：newMessages + contextModifier

`SkillTool.ts:766-840`：
```typescript
return {
  data: { success, commandName, allowedTools, model },
  newMessages,                        // ← skill body 在这里！
  contextModifier(ctx) {
    let modifiedContext = ctx
    if (allowedTools.length > 0) {
      // 临时给当前 tool call 加权限
      modifiedContext = { ...ctx, getAppState() {
        return { ...ctx.getAppState(),
          toolPermissionContext: {
            ...,
            alwaysAllowRules: { ...,
              command: [...allowed, ...allowedTools] }
          }
        }
      }}
    }
    if (model) {
      // 临时切 model（保留 [1m] 后缀避免降级）
      modifiedContext = { ..., options: { ..., mainLoopModel: resolveSkillModelOverride(...) }}
    }
    if (effort !== undefined) {
      // 临时切推理深度
    }
    return modifiedContext
  },
}
```

**设计哲学**：tool_result 只是回执（`Launching skill: xxx`），body 走对话流（newMessages）符合 LLM 对"新指令进入对话"的处理直觉。

---

### 维度 3：跨边界持久化（自我修复协议）

| 边界 | invokedSkills（body）| sentSkillNames（listing 去重）|
|---|---|---|
| **跨 compact** | ✅ `bootstrap/state.ts:1510 addInvokedSkill` 存全文 → `compact.ts:1494 createSkillAttachmentIfNeeded` 重新打包，按 `invokedAt desc` + `POST_COMPACT_MAX_TOKENS_PER_SKILL=5_000` 单 skill 上限 + 总预算裁剪 | ✅ `postCompactCleanup.ts:65-69` 显式注释「Intentionally NOT calling resetSentSkillNames(): re-injecting the full skill_listing (~4K tokens) post-compact is pure cache_creation」|
| **跨 resume** | ✅ `conversationRecovery.ts:382 restoreSkillStateFromMessages` 扫 `invoked_skills` attachment 重新 addInvokedSkill | ✅ 同函数扫到既有 `skill_listing` attachment 就 `suppressNextSkillListing()` 闩，下次跳过返回 |
| **跨 sub-agent** | ✅ `Map<"${agentId}:${skillName}", {content, invokedAt}>` per-agentId 复合 key 防串台；sub-agent 完成时 `clearInvokedSkillsForAgent` 释放 | ✅ `agentKey = toolUseContext.agentId ?? ''` 主线程空字符串、子 agent 各自隔离 |
| **跨 process** | ❌ 进程内 only | ❌ 进程内 only（resume 协议补偿）|

#### 关键的工程哲学

`postCompactCleanup.ts:31-51` 区分 main vs sub-agent：

```typescript
const isMainThreadCompact =
  querySource === undefined ||
  querySource.startsWith('repl_main_thread') ||
  querySource === 'sdk'

if (isMainThreadCompact) {
  // 仅主线程清模块级 cache（contextCollapse / memory-files / userContext）
  getUserContext.cache.clear?.()
  resetGetMemoryFilesCache('compact')
}
```

注释明示：「Subagents (agent:*) run in the same process and share module-level state... resetting those when a SUBAGENT compacts would corrupt the MAIN thread's state」

---

### 维度 4：学习反馈（`utils/hooks/skillImprovement.ts` 267 行）

```
[每个 user 消息]
       ↓
计数 userCount
       ↓
if (userCount - lastAnalyzedCount >= TURN_BATCH_SIZE=5)
       ↓
触发后台 LLM 评估（不阻塞用户对话）
       ↓
LLM 输出 SkillUpdate[]：
  type SkillUpdate = {
    skillName: string
    section: string       // 改 SKILL.md 哪一段
    change: string        // 怎么改
    reason: string        // 为什么改
  }
       ↓
推 React survey UI（hooks/useSkillImprovementSurvey.ts）
       ↓
用户确认 → applySkillImprovement → 改写 skill 文件本身
       ↓
（chokidar 检测到文件变化 → reset listing → 下个 turn LLM 看到更新版 skill）
```

→ 这是业内罕见的「**框架反向改进 skill 定义**」维度。

---

### 维度 5：文件系统响应性（`utils/skills/skillChangeDetector.ts` 311 行）

```
chokidar fs event
       ↓
scheduleReload(changedPath)：
  pendingChangedPaths.add(changedPath)
  if (reloadTimer) clearTimeout(reloadTimer)
  reloadTimer = setTimeout(async () => {
    const paths = [...pendingChangedPaths]
    pendingChangedPaths.clear()
    
    // 一次 batch 一次 hook 调用（不是 per-file）
    const results = await executeConfigChangeHooks('skills', paths[0])
    if (hasBlockingResult(results)) return
    
    clearSkillCaches()
    clearCommandsCache()
    resetSentSkillNames()        // ← 这里会让下个 turn 重发完整 listing
    skillsChanged.emit()
  }, RELOAD_DEBOUNCE_MS)
```

**工程窍门常量**：
- `RELOAD_DEBOUNCE_MS = 300` —— 注释明示「30 events 不 debounce 会 deadlock Bun event loop」
- `POLLING_INTERVAL_MS = 2000` —— 注释「skill 改动稀疏，2s 比 default 100ms 少 20× stat 调用」
- `FILE_STABILITY_THRESHOLD_MS = 1000` + `FILE_STABILITY_POLL_INTERVAL_MS = 500` —— 防止 IDE 写入半截被误读

**Bun 兼容**：注释里提到「Bun's native fs.watch() has a PathWatcherManager deadlock」→ 用 stat() polling 绕开。

---

## 3. SKILL.md frontmatter 完整字段表

| 字段 | 类型 | 作用 | 出处 |
|---|---|---|---|
| `name` | string | 唯一标识 | `parseSkillFrontmatterFields:239` |
| `description` | string | 一行描述（注入 listing）| `:212` 含 fallback 提取 |
| `whenToUse` | string | 触发场景说明 | `:252` |
| `tags` | string[] | 关键词集合 | — |
| `examples` | string[] | 触发示例 | — |
| `argumentHint` | string | `/skill <hint>` UI 提示 | `:246` |
| `arguments` | string[] | 参数名（用于 `$ARGUMENTS` 替换）| `:249 parseArgumentNames` |
| `version` | string | skill 版本 | `:253` |
| `model` | string | per-skill 模型（可设 `'inherit'` 不覆盖）| `:222 parseUserSpecifiedModel` |
| `effort` | low/medium/high/integer | 推理深度 | `:228 parseEffortValue` |
| `context` | `'fork'` 或 undefined | 执行模式（`'inline'` 是 default 无 enum）| `:260 executionContext` |
| `agent` | string | 指定哪个 agent 跑（fork 模式用）| `:261` |
| `paths` | string\|string[] | 条件激活 glob 模式 | `:159 parseSkillPaths` |
| `disable-model-invocation` | bool | LLM 不能调（用户专属）| `:255 parseBooleanFrontmatter` |
| `user-invocable` | bool（默认 true）| 用户能否 `/skill` 触发 | `:217` |
| `allowed-tools` | string[] | skill 内可调工具白名单 | `:242 parseSlashCommandToolsFromFrontmatter` |
| `hooks` | object | skill 内嵌 hook 注册 | `:259 parseHooksFromFrontmatter` |
| `shell` | string | shell 命令配置 | `:263 parseShellFrontmatter` |
| `aliases` | string[] | skill 别名 | bundledSkills only |

---

## 4. 关键工程决策表（带常量 + 出处）

| 决策 | 常量/机制 | 出处 |
|---|---|---|
| 1% context budget 给 listing | `SKILL_BUDGET_CONTEXT_PERCENT = 0.01` | `prompt.ts:21` |
| 单条 listing 描述硬上限 | `MAX_LISTING_DESC_CHARS = 250` | `prompt.ts:29` |
| 极端降级阈值 | `MIN_DESC_LENGTH = 20`（< 20 时 non-bundled 降为仅名字）| `prompt.ts:68` |
| skill-search 启用时入口过滤 | `FILTERED_LISTING_MAX = 30` | `attachments.ts:2641` |
| 学习评估批处理 | `TURN_BATCH_SIZE = 5`（每 5 user msg 评估一次）| `skillImprovement.ts:31` |
| 单 skill compact 后 token 上限 | `POST_COMPACT_MAX_TOKENS_PER_SKILL = 5_000` | `compact.ts:129` |
| 文件热重载 debounce | `RELOAD_DEBOUNCE_MS = 300` | `skillChangeDetector.ts:42` |
| chokidar 轮询间隔 | `POLLING_INTERVAL_MS = 2000` | `skillChangeDetector.ts:49` |
| ranking 半衰期 | 7 天，floor 0.1：`usageCount × Math.pow(0.5, daysSinceUse / 7)` | `skillUsageTracking.ts:51` |
| ranking 写入 debounce | 60s | `skillUsageTracking.ts:13 recordSkillUsage` |
| 工具名 | `SKILL_TOOL_NAME = 'Skill'` | `tools/SkillTool/constants.ts` |
| 安全字段 allowlist | 28 字段 SAFE_SKILL_PROPERTIES | `SkillTool.ts:875-908` |
| bundled extract 文件权限 | `0o700` (dir) + `0o600` (file) + `O_NOFOLLOW|O_EXCL` | `bundledSkills.ts safeWriteFile` |

---

## 5. 完整数据流（用户问题 → skill 激活 → 后续）

```
[启动]
  ├─ 6 来源加载 → bundled + disk + plugin + builtinPlugin + mcp + legacy
  ├─ createSkillCommand 统一构造 Command (type='prompt')
  ├─ 分流：paths 字段的进 conditionalSkills；其他进 unconditionalSkills
  ├─ chokidar 启动监听 .claude/skills/ + ~/.claude/skills/
  └─ system prompt 加 hasSkills 块（Layer 1）

[用户提问]
  │
  ├─ getSkillListingAttachments 算增量：
  │   - 取 sentSkillNames Map（agentKey 维护）
  │   - 过滤 newSkills = allCommands - sent
  │   - 过预算时三档降级
  │   - emit skill_listing attachment
  │   - mark sent
  │
  ├─ messages.ts wrap 成 system_reminder + user message
  └─ → LLM 推理

[LLM 决定激活 skill X]
  │
  ├─ 调 Skill({ skill: 'X', args: '...' })
  │
  ├─ validateInput：
  │   - skill 存在？disableModelInvocation? type === 'prompt'?
  │   - remote canonical 走 _canonical_ fast-path
  │
  ├─ checkPermissions：
  │   - deny rules 拦截
  │   - allow rules 放行
  │   - SAFE_SKILL_PROPERTIES allowlist 自动放行（无未知字段）
  │   - 否则 ask user
  │
  ├─ recordSkillUsage(commandName)  // ranking 计数（60s debounce）
  │
  ├─ 按 context 分流执行：
  │   ├─ context === 'fork' → executeForkedSkill 派生子 agent
  │   ├─ _canonical_ → executeRemoteSkill 拉 AKI/GCS
  │   └─ default → processPromptSlashCommand inline 展开
  │
  ├─ inline 展开（getMessagesForPromptSlashCommand）：
  │   ├─ getPromptForCommand 拿 body
  │   │   ├─ baseDir 注入 "Base directory for this skill: <dir>"
  │   │   ├─ ${CLAUDE_SKILL_DIR} / ${CLAUDE_SESSION_ID} / $ARGUMENTS 替换
  │   │   ├─ markdown 内嵌 !`...` shell 执行（loadedFrom !== 'mcp'）
  │   ├─ registerSkillHooks 注册 SKILL.md 内嵌 hooks（once 自卸）
  │   ├─ addInvokedSkill 入账（per-agentId 复合 key + 全文）
  │   ├─ getAttachmentMessages 抽 @-mention / MCP resource
  │   └─ 加 command_permissions attachment
  │
  └─ 返回：
      ├─ tool_result = "Launching skill: xxx" (10 token)
      ├─ newMessages = [user(metadata), user(skill body, isMeta=true), ...attachments]
      └─ contextModifier 临时改 allowedTools/model/effort

[LLM 看到 newMessages 中的 body 继续推理]
  │
  ├─ skill body 提到 "用 Read 读 templates/foo.j2"
  ├─ LLM 调通用 Read 工具（用 ${CLAUDE_SKILL_DIR} 拼绝对路径）
  ├─ 文件 tool 触发 hook：
  │   ├─ discoverSkillDirsForPaths 上行查找 .claude/skills/
  │   ├─ addSkillDirectories(.catch fire-and-forget)
  │   └─ activateConditionalSkillsForPaths（paths 匹配的 skill 进 dynamicSkills）
  │
  └─ 下个 turn 的 listing attachment 会包含新激活的 conditional skills

[每 5 user msg 触发学习评估]
  │
  ├─ 后台 LLM 评估当前 skill 用得好不好
  ├─ 产 SkillUpdate[] 推 survey UI
  ├─ 用户确认 → applySkillImprovement 改写 SKILL.md
  └─ chokidar 检测变化 → debounce 300ms → resetSentSkillNames → 下个 turn 重发更新版

[长会话触发 compact]
  │
  ├─ runPostCompactCleanup(querySource):
  │   ├─ isMainThreadCompact = querySource.startsWith('repl_main_thread')
  │   ├─ if isMainThreadCompact: 清模块级 cache
  │   └─ 故意 NOT 清 sentSkillNames（4K cache_creation 浪费）
  │
  ├─ createSkillAttachmentIfNeeded(agentId)：
  │   ├─ 取 invokedSkills.filter(byAgent)
  │   ├─ sort by invokedAt desc
  │   ├─ per-skill 截断到 5K token
  │   ├─ 总预算裁剪（丢最不相关的）
  │   └─ emit invoked_skills attachment 进新会话
  │
  └─ → 新会话 LLM 看到的：
      - skill listing 不重发（sentSkillNames 保留）
      - 已激活 skill 的 body 通过 invoked_skills 重新进入
      - 渐进式披露在长会话中"自我修复"

[claude -p 重启 / resume]
  │
  ├─ restoreSkillStateFromMessages 扫 transcript：
  │   ├─ 遇到 invoked_skills attachment → 重新 addInvokedSkill 入 STATE
  │   └─ 遇到 skill_listing attachment → suppressNextSkillListing()
  │
  └─ 下个 turn 不重发 ~600 token listing
```

---

## 6. 边界条件与安全模型

### 6.1 安全字段 allowlist（白名单优于黑名单）

`SkillTool.ts:875-908` 列了 28 个 "safe" 字段。`skillHasOnlySafeProperties` 检查：

```typescript
function skillHasOnlySafeProperties(command): boolean {
  for (const key of Object.keys(command)) {
    if (SAFE_SKILL_PROPERTIES.has(key)) continue
    const value = command[key]
    if (value === undefined || value === null) continue
    if (Array.isArray(value) && value.length === 0) continue
    if (typeof value === 'object' && Object.keys(value).length === 0) continue
    return false  // 非 safe 字段有有效值 → 要权限
  }
  return true
}
```

**设计哲学**：未来加新字段默认要权限确认，避免"加新字段忘了考虑安全"。

### 6.2 路径穿越防护（bundled extract）

`bundledSkills.ts:resolveSkillFilePath`：
```typescript
const normalized = normalize(relPath)
if (
  isAbsolute(normalized) ||
  normalized.split(pathSep).includes('..') ||
  normalized.split('/').includes('..')
) {
  throw new Error(`bundled skill file path escapes skill dir`)
}
```

`safeWriteFile`：
```typescript
const SAFE_WRITE_FLAGS = O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW
//                                              ^^^^^   ^^^^^^^^^^
//                                              防覆盖   防 symlink
const fh = await open(p, SAFE_WRITE_FLAGS, 0o600)  // owner-only
```
+ per-process nonce 嵌入 extract 路径
+ **故意不** unlink+retry on EEXIST（unlink 也跟 symlink）

### 6.3 gitignored skill dir 跳过

`loadSkillsDir.ts:892-897` `discoverSkillDirsForPaths`：
```typescript
if (await isPathGitignored(currentDir, resolvedCwd)) {
  // 阻止 node_modules/pkg/.claude/skills 静默加载
  continue
}
```

### 6.4 MCP skill 不执行 shell

`loadSkillsDir.ts:374`：
```typescript
// Security: MCP skills are remote and untrusted — never execute inline
// shell commands (!`…` / ```! … ```) from their markdown body.
if (loadedFrom !== 'mcp') {
  finalContent = await executeShellCommandsInPrompt(...)
}
```

### 6.5 disableModelInvocation 二维权限

```yaml
# /deploy 这种用户专属 skill
disable-model-invocation: true   # validateInput 阶段 LLM 调被拒
user-invocable: true             # 用户 /deploy 可触发
```

`SkillTool.ts:411-418`：
```typescript
if (foundCommand.disableModelInvocation) {
  return {
    result: false,
    message: `Skill ${name} cannot be used with Skill tool due to disable-model-invocation`,
    errorCode: 4,
  }
}
```

### 6.6 子 agent 不污染父状态

- `clearInvokedSkillsForAgent(syncAgentId)` 在 sub-agent 完成时立即释放（`AgentTool/agentToolUtils.ts:683`）
- `agentMessages.length = 0` 显式释放数组（GC 在长 closure 引用下不回收）
- compact cleanup main vs subagent 区分（前述 §维度 3）

---

## 7. UI / 用户交互层（简述）

| 入口 | 文件 | 行为 |
|---|---|---|
| `/<skill-name>` slash command | 命令解析层 | 识别 skill 名 → 等价于 SkillTool({skill: name}) |
| `commands/skills/skills.tsx` | UI | skill 列表浏览界面 |
| `SkillImprovementSurvey.tsx` | UI | 学习评估弹 survey |
| `useSkillsChange.ts` | React hook | 订阅 skillsChanged signal 触发 UI 重渲染 |

---

## 8. 设计哲学总结

### 为什么这样设计

1. **统一抽象**：skill 不是单独 runtime，与 slash command 共享 `Command` 抽象 → 减少代码重复 + 用户/LLM 触发路径统一

2. **渐进式披露解 token 经济**：100 个 skill 也只占 ~1500 token listing；body 按需加载；资源再按需 → context 占用与"当前任务"成正比，不与"理论可用能力"成正比

3. **跨边界自我修复**：`invokedSkills` + `sentSkillNames` 跨 compact/resume/sub-agent 持久化 → 长会话中 progressive disclosure 不退化为"知道 skill 名但忘了怎么用"

4. **白名单优于黑名单**：SAFE_SKILL_PROPERTIES + path traversal 防护 + MCP 不执行 shell + safe extract → 未来加字段/特性默认安全

5. **fire-and-forget + 异步 batch**：文件操作触发的 skill 发现 / 激活 用 `.catch(() => {})` 不阻塞用户操作 → 用户体感无延迟

6. **常量决策来自工程教训**：`RELOAD_DEBOUNCE_MS = 300` 注释明示"不 debounce 会 deadlock Bun event loop"——每个常量都有对应的 commit 历史教训，不是凭感觉

7. **学习闭环让 skill 越用越准**：5 turn 评估 → SkillUpdate → 用户确认 → 改写 skill → 文件监听 → 重发 listing → 下个 turn 用更新版 → 评估 → ... 形成正反馈

8. **多代理架构兼容**：coordinator-summary 模式让主线程不加载 skill body（worker 真用时才加载）；agentKey 隔离 sentSkillNames + invokedSkills；compact main/sub-agent 区分 → 多 agent 体系下 skill 也是渐进式的

---

## 9. 完整源码定位索引

### 核心入口
| 主题 | 文件 | 行 |
|---|---|---|
| 工具名常量 | `tools/SkillTool/constants.ts` | 1 |
| SkillTool buildTool | `tools/SkillTool/SkillTool.ts` | 331-869 |
| validateInput | 同上 | 354-430 |
| checkPermissions | 同上 | 432-578 |
| call (主入口) | 同上 | 580-841 |
| executeForkedSkill | 同上 | 122-289 |
| executeRemoteSkill | 同上 | 969-1108 |
| mapToolResultToToolResultBlockParam | 同上 | 843-862 |
| SAFE_SKILL_PROPERTIES | 同上 | 875-908 |
| getPrompt 工具描述 | `tools/SkillTool/prompt.ts` | 173-196 |
| 预算裁剪 formatCommandsWithinBudget | 同上 | 70-171 |
| 预算常量 | 同上 | 21-29 |

### Skill 加载
| 主题 | 文件 | 行 |
|---|---|---|
| createSkillCommand | `skills/loadSkillsDir.ts` | 270-401 |
| parseSkillFrontmatterFields | 同上 | 185-265 |
| parseSkillPaths | 同上 | 155-178 |
| getSkillDirCommands | 同上 | 638-804 |
| activateConditionalSkillsForPaths | 同上 | 997-1058 |
| discoverSkillDirsForPaths | 同上 | 861-915 |
| addSkillDirectories | 同上 | 923+ |
| 模板变量替换 | 同上 | 344-369 |
| bundled 注册 | `skills/bundledSkills.ts` | registerBundledSkill |
| bundled extract 安全 | `skills/bundledSkills.ts` | safeWriteFile |
| frontmatter splitPathInFrontmatter | `utils/frontmatterParser.ts` | 189 |

### 增量下发 + 持久化
| 主题 | 文件 | 行 |
|---|---|---|
| sentSkillNames Map | `utils/attachments.ts` | 2607 |
| getSkillListingAttachments | 同上 | 2661-2751 |
| suppressNextSkillListing | 同上 | 2633 |
| FILTERED_LISTING_MAX | 同上 | 2641 |
| filterToBundledAndMcp | 同上 | 2651 |
| skill_listing 走 system_reminder | `utils/messages.ts` | 3728-3737 |
| addInvokedSkill | `bootstrap/state.ts` | 1510-1563 |
| createSkillAttachmentIfNeeded | `services/compact/compact.ts` | 1494-1534 |
| POST_COMPACT_MAX_TOKENS_PER_SKILL | 同上 | 129 |
| postCompactCleanup main vs sub | `services/compact/postCompactCleanup.ts` | 31-51,65-69 |
| restoreSkillStateFromMessages | `utils/conversationRecovery.ts` | 382-403 |

### 学习 + 响应
| 主题 | 文件 | 行 |
|---|---|---|
| Skill self-improvement | `utils/hooks/skillImprovement.ts` | 全文 267 行 |
| TURN_BATCH_SIZE = 5 | 同上 | 31 |
| applySkillImprovement | 同上 | 188 |
| 文件热重载 | `utils/skills/skillChangeDetector.ts` | 全文 311 行 |
| RELOAD_DEBOUNCE_MS | 同上 | 42 |
| POLLING_INTERVAL_MS | 同上 | 49 |
| scheduleReload 三段式 | 同上 | 248-279 |
| Skill ranking | `utils/suggestions/skillUsageTracking.ts` | 全文 55 行 |
| getSkillUsageScore (7 天半衰期) | 同上 | 44-55 |
| recordSkillUsage (60s debounce) | 同上 | 13 |

### Hook + 集成
| 主题 | 文件 | 行 |
|---|---|---|
| SKILL.md hooks 注册 | `utils/hooks/registerSkillHooks.ts` | 20-64 |
| once 自卸 | 同上 | 35-41 |
| 文件 tool hook 触发 | `tools/FileReadTool/FileReadTool.ts` | 575-590 |
|  | `tools/FileWriteTool/FileWriteTool.ts` | 234,245 |
|  | `tools/FileEditTool/FileEditTool.ts` | 408,422 |

### System Prompt
| 主题 | 文件 | 行 |
|---|---|---|
| hasSkills 块 | `constants/prompts.ts` | 382-384 |
| getDiscoverSkillsGuidance | 同上 | 336-340 |
| getSessionSpecificGuidanceSection | 同上 | 360-410 |

---

## 10. 一句话总结

> **Claude Code Skills = 5 个正交维度（激活 / 执行 / 持久化 / 学习 / 响应）+ 11 个独立子系统 + 4 个工程窍门 + 28 个 SAFE allowlist 字段 + ~3000 行 TS 代码** 的复合工程系统。

**真正的不是"渐进式披露"——而是"渐进式披露在所有边界（compact/resume/sub-agent/process）下都能自我修复 + 还能反向改进 skill 定义本身"**。
