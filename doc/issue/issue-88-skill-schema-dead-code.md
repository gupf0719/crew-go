# ISSUE-88 · skill / schema spec 占位 dead code（✅ 已解决,占位全部删除）

**优先级**：LOW（dead code 不影响业务,但占 API surface + 让新读者疑惑）
**状态**：✅ **已解决（2026-10-21）**——spec 占位全部删除,分两步:
- **skill 脚本 action 路径**：✅ **已删除**（2026-06-30）—— `RunSkillScript` /
  `WithSkillScriptAction` / `ScriptExecutor` 沙箱 seam / `RuntimePython|Bash|Node` /
  `SkillActionDescriptor.Entrypoint` / frontmatter `actions:` 解析（`parseActions`）全删;
  **go-func action 路径保留**（`RunSkillAction` go-func 分派 / `WithSkillActionFunc` /
  `RuntimeGo` / `ActionFunc` / `run_skill_action` 工具——已实装且有 consumer
  `example/harness/skill_action_smoke`）. 依据见下「决策依据」.
- **`SkillBoxes` / `SkillBoxBuilder`** + **`ToolResultMessage` 第二参数 `name`** +
  **`ErrActionNotImplemented`**：✅ **已删除**（2026-10-21）. 此前 ⏸ 保留观察（2026-05-17
  audit²⁸ 决策）,唯一站得住的保留理由是「spec 为跨语言 runtime 预留接口槽位」——2026-10-21
  起 crew-go 不再依赖、不再同步公司 spec,这条理由不再成立. 见下「余项删除决策」.
**触发**：本 session global code review 阶段发现 3 处 dead API;2026-06-30 三方调研 +
远程沙箱 harness 接入触发对脚本 action 路径的重审;2026-10-21 用户确认 crew-go 不再依赖
公司 spec,触发对余项的重审（详见下）.

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

## 余项删除决策 (2026-10-21)

**背景**：2026-10-21 用户确认 crew-go 不再依赖公司 spec（CLAUDE.md「项目特定约定」:`modules/`
起步于公司 spec 生成类型,2026-10-21 起不再依赖、不再同步）. 下文「决策依据 (audit²⁸)」的保留
理由逐条复核:

| 保留理由（2026-05-17）| 复核（2026-10-21）|
|---|---|
| spec 为跨语言 runtime 预留接口槽位,保留对齐让未来 spec 同步成本低 | 不再成立——不再同步 spec,没有同步成本可省 |
| 删 `ToolResultMessage` 第二参数要改 schema 包公开 API + 26+ caller 同步改 | 项目未上线、不留兼容路径（CLAUDE.md）;实际 84 处调用,全在本仓,已同步改完 |
| 业务若想存 tool_name 元数据会失去口子 | 参数一直被 `_` 丢弃,这个口子从来不存在;真要存工具名得给 `ToolresultContentBlock` 加字段,跟留一个被丢弃的参数无关. 框架自己要工具名的地方（return-direct 收尾）已在产出时记进 `ReturnDirectToolAnnotation` |
| 删 `SkillBoxBuilder` 影响最小,但和上一项不分开删意义不大 | 两项一起删 |

**删了什么**:
- `skill.SkillBoxes` / `skill.SkillBoxBuilder` 两个 interface + `skill.NewSkillBoxes` + 内部实现
  `defaultSkillBoxes` / `defaultSkillBoxBuilder`——生产、测试、example 均零调用. builder 能做的,
  `skill.NewDefaultSkillBox(rootDir, skill.WithSkillBoxName(name), skill.WithAllowList(names))` 都能做.
- `skill.ErrActionNotImplemented`——issues.md 把它和两个死桩列在同一项;go-func 实装后只剩 6 个
  测试桩在返它,生产代码没有任何 `errors.Is` 判它（CLAUDE.md:哨兵只在调用方要 `errors.Is` 时才
  定义）. 框架只在至少一个可被 LLM 调的 skill 列出了 action 时才挂 `run_skill_action`
  （`boxHasActions`）,不会去调不提供 action 的 box 的 `RunSkillAction`. 测试桩改返
  `skill.ErrSkillActionNotFound`,`SkillBox.RunSkillAction` 的 godoc 同步改为指向它.
- `schema.ToolResultMessage(callID, _ /* name unused */, content)` → `ToolResultMessage(callID, content)`,
  84 处调用同步（agent_v2 生产 7 处 + `agent_v2/history_conformance` 2 处 + example 3 处 + 测试 72 处）;
  `agent_v2.failedToolResult` 的 `toolName` 参数只为喂这个位置,一并删（5 处调用）.
- 注释:`skill/skillbox.go` 三处「对齐 spec … interface」;`agent_v2/basic_agent.go` 两处引用
  `_ /* name unused */` 的说明.

**保留**:`SkillBox.RunSkillAction`（go-func 已实装,consumer 是 `agent_v2/skill_action_tool.go` 与
`example/harness/skill_action_smoke`）与 `ErrSkillActionNotFound`——它们不是占位.

**文档同步**:[`doc/v2/skill.md`](../v2/skill.md) §3 `RunSkillAction` 行 /
[`doc/v2/structured-output.md`](../v2/structured-output.md) §4.5 构造器签名 /
[`doc/code/todo.md`](../code/todo.md) 注释失实清单那行.

---

## 决策依据 (audit²⁸, 2026-05-17) —— 余两项保留（已被上节 2026-10-21 删除决策取代,保留作决策史）

> 下文针对当时**仍保留**的 `SkillBoxes` / `SkillBoxBuilder` + `ToolResultMessage` 第二参数;
> `RunSkillAction` go-func 已实装、`RunSkillScript` 脚本路径已删（见上）.

**为什么三方都不用这些抽象但 spec 还预留**:
- spec（公司 spec）是跨语言 type contract 设计,**为未来扩展预留接口槽位**——
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

**何时重审**（当时仍保留的两项）:
- 项目正式上线前最后一轮 API freeze audit
- spec v2 升级时 (重新对照 spec 实际接口要求)

> 2026-10-21 结案:不是上面哪一条触发的,而是保留的前提「对齐 spec」本身没了——crew-go 不再
> 依赖公司 spec,也就不会有「spec v2 升级」可等. 见上「余项删除决策」.

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

## 证据 (grep)

```bash
# skill 真业务路径
grep -rn "\.ActivateSkill\|\.MatchSkills\|\.ListSkills" --include="*.go" agent_v2 | grep -v _test.go
# → 10+ 处 agent_v2 业务调用 (skill_tool.go / adaptive_skill.go / basic_agent.go)

# 已删的占位符号（脚本路径 2026-06-30;builder / 哨兵 2026-10-21）—— 应 0 处
grep -rnE "RunSkillScript|ScriptExecutor|WithSkillScriptAction|SkillBoxes|SkillBoxBuilder|defaultSkillBox(es|Builder)|ErrActionNotImplemented" --include="*.go" .
# → 0 处（已删）

# RunSkillAction go-func 路径有 consumer（已实装,非 dead）
grep -rn "\.RunSkillAction\|WithSkillActionFunc" --include="*.go" example agent_v2 | grep -v _test.go
# → example/harness/skill_action_smoke + agent_v2/skill_action_tool.go 等

# ToolResultMessage 第二参数已删（2026-10-21）:三参数调用编译不过,
# go build / go vet 通过即说明调用点全部改完
grep -n "^func ToolResultMessage" schema/message.go
# → func ToolResultMessage(callID, content string) *Message
```
