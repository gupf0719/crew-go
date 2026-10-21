# ISSUE-88 · skill / schema spec 占位 dead code（脚本 action 路径已删,余两项保留）

**优先级**：LOW（dead code 不影响业务,但占 API surface + 让新读者疑惑）
**状态**：
- **skill 脚本 action 路径**：✅ **已删除**（2026-06-30）—— `RunSkillScript` /
  `WithSkillScriptAction` / `ScriptExecutor` 沙箱 seam / `RuntimePython|Bash|Node` /
  `SkillActionDescriptor.Entrypoint` / frontmatter `actions:` 解析（`parseActions`）全删;
  **go-func action 路径保留**（`RunSkillAction` go-func 分派 / `WithSkillActionFunc` /
  `RuntimeGo` / `ActionFunc` / `run_skill_action` 工具——已实装且有 consumer
  `example/harness/skill_action_smoke`）. 依据见下「决策依据」.
- **`SkillBoxes` / `SkillBoxBuilder`** + **`ToolResultMessage` 第二参数 `name`**：
  ⏸ 仍保留观察（2026-05-17 audit²⁸ 决策不变）.
**触发**：本 session global code review 阶段发现 3 处 dead API;2026-06-30 三方调研 +
远程沙箱 harness 接入触发对脚本 action 路径的重审（详见下）.

## 3 处 dead code

| 位置 | dead 表现 | 三方对照 (audit²⁸) | 业务影响 |
|---|---|---|---|
| `skill.SkillBoxes` / `SkillBoxBuilder` interface + `defaultSkillBoxes/Builder` impl | 业务 0 调用 (全用 `NewDefaultSkillBox(rootDir, opts...)` 函数式工厂). 仅 `agent_v2/config.go` godoc 1 注释引用 | LangGraph 用 StateGraph builder pattern (但是 builder object); eino 用 `NewGraph + Option` 函数式; Claude Code 直接 `registerBundledSkill(definition)` 注册 (无 builder) | ❌ 无影响—— skill 业务路径 `ListSkills` / `MatchSkills` / `ActivateSkill` 不经此 interface |
| `skill.SkillBox.RunSkillAction` go-func（保留）/ ~~`RunSkillScript` + 脚本沙箱 seam~~（**2026-06-30 删**）| **当时**（2026-05-17）：两者皆永抛 `not implemented` dead 桩. **此后变化**：go-func 路径实装（`RunSkillAction` 进程内分派 + `WithSkillActionFunc` + `run_skill_action` 工具,有 consumer `example/harness/skill_action_smoke`）; 脚本路径（`RunSkillScript` / `ScriptExecutor` seam / python·bash·node runtime / frontmatter `actions:` 解析）始终零 consumer → 2026-06-30 删 | LangGraph 无 skill 概念; eino skill 只 body+fork,脚本执行在 **tool 层** `Shell`/`execute` 走 open-bash; **Claude Code 无 skill 层 "action runtime" 抽象**,脚本走 Bash tool; 远程沙箱走 tool 层 sandbox_bash —— 无框架在 skill 层做声明式脚本执行 | ❌ 无影响——go-func action 覆盖进程内确定性执行; 脚本/shell 执行业务走 tool 层 open-bash（自写 FuncTool 调 `os/exec` 或接 sandbox tool）|
| `schema.ToolResultMessage(callID, name, content)` 第二参数 `name` | 函数 `_` 丢弃; `ToolresultContentBlock` 字段只 `{Type, ToolCallId, Content}` 无 Name 字段; 26+ caller 全传 `tc.Function.Name` 但函数不用 | LangGraph `ToolMessage(content, tool_call_id)` 无 name; **Claude Code `ToolResultBlockParam` 来自官方 Anthropic SDK 无 name**; eino `Message.ToolName` 有 + `WithToolName` 选项 (但作内部追踪用,LLM API spec 不要求) | ❌ 无影响——crew-go 业务下游路由 tool_result 用 `tc.Function.Name` (tool_call 端取),不依赖 tool_result 内 name |

## 脚本 action 路径删除决策 (2026-06-30)

**背景**：issue-88 写时（2026-05-17）`RunSkillAction` / `RunSkillScript` 都是死桩
（永抛 `not implemented`）. 此后 **go-func 路径实装**（`RunSkillAction` 进程内分派 +
`WithSkillActionFunc` + `RuntimeGo` + `ActionFunc` + `run_skill_action` 工具,有 consumer
`example/harness/skill_action_smoke`）—— 只剩**脚本路径**（`RunSkillScript` / `ScriptExecutor`
沙箱 seam / `RuntimePython|Bash|Node` / `SkillActionDescriptor.Entrypoint` / frontmatter
`actions:` 解析）始终零生产 consumer.

**删除依据**:
- 脚本 action 当初是**对齐 spec `skill.tsp` 占位**（`RunSkillScript`）建的,不是为具体
  场景;`ScriptExecutor` 零生产 consumer.
- 三方调研实锤（2026-06-30）:**eino** skill 只 body+fork,执行 seam 在 tool 层
  `Shell`/`execute` tool 走 open-bash;**langgraph** 无 skill、无沙箱 seam,交 tool 层;
  **Claude Code / 远程沙箱** open-bash（Bash / sandbox_bash tool）—— **没有框架在 skill 层
  做声明式脚本执行,主流是 tool 层 open-bash**.
- 叠加远程沙箱 harness 接入（走 tool 层 sandbox_bash FuncTool）—— 正是本 issue
  预留的「重审触发」（见旧「何时重审」第 3 条）.
- go-func 路径已实装且有 consumer,只剩 script 路径死 → 删 script、留 go-func.

**对 spec 的偏离**:这是对 `skill.tsp` `RunSkillScript` 的**主动偏离**,CLAUDE.md 允许
（「modules/ 有更优 Go 原生设计直接优化 + 注释说明原因」/「未上线不留兼容路径」）.
spec 同步成本（旧「为什么保留」第 1 条担心的）让位于「不留零 consumer 死路径」—— 脚本
执行能力收敛到 tool 层 open-bash（业务自写 FuncTool 调 `os/exec` 或接 sandbox tool）.

---

## 决策依据 (audit²⁸, 2026-05-17) —— 余两项保留

> 下文针对**仍保留**的 `SkillBoxes` / `SkillBoxBuilder` + `ToolResultMessage` 第二参数;
> `RunSkillAction` go-func 已实装、`RunSkillScript` 脚本路径已删（见上）.

**为什么三方都不用这些抽象但 spec 还预留**:
- spec (`agentdock-spec`) 是跨语言 type contract 设计,**为未来扩展预留接口槽位**——
  Java/Python runtime 后续若实现 SkillBoxBuilder DI 注入等场景,go-runtime 已有占位
  避免再加 method
- crew-go Go runtime 是 spec 的**实现**,按 CLAUDE.md "modules/ 目录有差异以我们的
  设计为准并注释说明原因" — 但 spec 接口保留对齐让未来 spec 同步成本低

**为什么保留而非删**:
1. 删 `ToolResultMessage` 第二参数改变 schema 包公开 API → 26+ caller 同步改 + 业务
   若仍想存 tool_name 元数据 (类 eino 内部追踪用) 失去口子
2. 删 `SkillBoxBuilder` 影响最小但和上面一项不分开删意义不大 (一并保留)
3. 项目未上线无业务负担,**dead code 占 API surface 但不阻塞功能**——成本/收益不
   对称 (维护成本低,删需谨慎评估 spec 同步)

> 注:旧「为什么保留」首条「删 `SkillBox.RunSkillAction/Script` 改 interface 协议 →
> spec 同步成本」已**不再适用**—— `RunSkillAction` go-func 实装,`RunSkillScript` 脚本
> 路径 2026-06-30 删（见上「脚本 action 路径删除决策」,spec 同步成本让位于「不留零
> consumer 死路径」）.

**何时重审**（仍保留的两项）:
- 项目正式上线前最后一轮 API freeze audit
- spec v2 升级时 (重新对照 spec 实际接口要求)

> 旧「何时重审」第 3 条「业务有 skill 内置脚本 runtime 诉求触发 RunSkillAction 真实现」
> **已触发并结案**:go-func action 已实装满足进程内确定性执行;脚本/shell 执行的诉求
> （远程沙箱 harness 接入）收敛到 **tool 层 open-bash**,不在 skill 层做（见上删除决策）.

## 相关已有文档 (避免跨文档同病重复描述)

- **权威源**: [`doc/description/three-way-comparison/skill-system.md`](../description/three-way-comparison/skill-system.md)
  - §"crew-go 当前不足" R1 #6:"无 RunSkillAction 实现"——已翻转（go-func 已实现 +
    声明式脚本 action 路径 2026-06-30 删,脚本执行收敛 tool 层 open-bash）
  - §"P3 慎做":skill 层脚本 action 执行已删（同上）
  - §"crew-go 当前优势" 第 5 条:go-func action 已落地
- skill action 用法权威源: [`doc/v2/skill.md` §11](../v2/skill.md)（go-func 用法 +
  `run_skill_action` 工具 + 删除依据）
- **本 ISSUE 是决策记录**,三方对照分析不重复 skill-system.md 已写部分

## 不影响业务的证据 (grep)

```bash
# skill 真业务路径
grep -rn "\.ActivateSkill\|\.MatchSkills\|\.ListSkills" --include="*.go" agent_v2 | grep -v _test.go
# → 10+ 处 agent_v2 业务调用 (skill_tool.go / adaptive_skill.go / basic_agent.go)

# RunSkillScript 脚本路径已删（2026-06-30）—— 应 0 处
grep -rn "RunSkillScript\|ScriptExecutor\|WithSkillScriptAction" --include="*.go"
# → 0 处（已删）

# RunSkillAction go-func 路径有 consumer（已实装,非 dead）
grep -rn "\.RunSkillAction\|WithSkillActionFunc" --include="*.go" example agent_v2 | grep -v _test.go
# → example/harness/skill_action_smoke + agent_v2/skill_action_tool.go 等

# 仍 dead 的 SkillBoxes builder 工厂
grep -rn "NewSkillBoxes\b" --include="*.go" | grep -v _test.go | grep -v skillbox.go
# → 仅 agent_v2/config.go godoc 1 注释引用,0 处真业务调用（仍保留观察）

# ToolResultMessage 第二参数: caller 全传但函数 `_` 丢
grep -rn "ToolResultMessage(" --include="*.go" | grep -v "schema/message.go" | wc -l
# → 26 处 caller (全在 agent_v2 / example / 测试) + 函数定义 1 处 `_` 丢弃
```
