---
id: ISSUE-47
title: PlanStep 显式声明原文透传（InputRefs），默认不注入 executed
status: ✅ 已完成
priority: 高
created: 2026-04-22
depends: ISSUE-45
---

# ISSUE-47 · Planner 全权决定每步数据流，框架不猜

## 背景

ISSUE-45 确立了 PlanAgent 的"replan-每步"哲学——Planner 每跑完一步都重新规划。
但该 issue **只解决了"规划层的动态性"，没解决"每步输入注入层的问题"**。

`defaultExecInput`（[compose/plan.go:484](../../compose/plan.go)）当前行为：

```go
// 原始目标：<original>
// 你的任务：<current.Description>
// 上一步（<prev.Step>）的产出：<prev.Result>
```

**问题**：框架无条件注入"上一步产出"。Plan 非严格线性依赖时这是错的——
step C 和 A/B 无关时，A/B 的结果进 C 的 input 是纯污染 + token 浪费。

实测 plan_chat 轮 2：

```
1. [researcher] 收集原理                ← step 0
2. [writer]     整理优点                ← step 1，依赖 0
3. [critic]     分析不足                ← step 2，依赖 0+1
4. [researcher] 编写代码示例            ← step 3，只依赖 0，不依赖 critic 审稿
5. [writer]     汇总                    ← step 4，依赖 0+1+2+3
```

step 3 被塞了 step 2 的 critic 审稿产出，但代码示例和审稿意见语义无关，纯干扰。

更隐蔽的问题是——**"塞上一步"在线性链下看起来经常是对的**（step i 恰好依赖 step i-1）。
但那是框架瞎猜碰运气，Planner 没有知情权也没法纠正。一旦数据流不是线性链
（跨步依赖、非相邻依赖、独立任务穿插），默认就默默塞错。

## 核心原则：Supervisor 范式

新方案的哲学是把 PlanAgent 定位为 **Supervisor / Manager**：

- **Planner = 大脑**——唯一持有跨步上下文，根据 agent 描述和任务判断每步数据依赖
- **Sub-agent = 执行器**——只看到 Planner 派发的一次性任务指令 + 按需附带的前置原文
- **框架 = 哑管道**——不猜、不提纯、不改写，只按 Planner 的显式声明把原文搬过去

每一步 Planner 对每个 sub-agent 做一个二元判断：**这个 agent 的任务依赖前面某些步骤的结果吗？**

- **不依赖**（独立任务）：只给"原始目标 + 任务 description"，不注入任何 executed
- **依赖**：声明要哪几步的原文，框架附上

**关键原则：框架和 Planner 都不做"提纯"**。要传就原文，一个字不改。
需要精简时（例如 a 输出 50KB、b 只需摘要），那是 **plan 编排层面的问题**——
Planner 应在规划时插入一个 `summarizer` step（`a → summarizer → b`），
而不是让框架或 Planner 自己当场压缩原文。这样"压缩"变成**可编排、可观测、可调试**
的 step，而不是框架隐藏动作。

## API 设计

### 1. PlanStep 加 `InputRefs`

```go
type PlanStep struct {
    Description  string
    SubAgentName string
    TmpSubAgent  *TmpSubAgentConfig

    // InputRefs 声明这一步需要哪些已完成步骤的**原文**附到 user prompt 里。
    // - nil / 空 slice：独立任务，不附任何 executed 原文
    // - [i, j, ...]：按声明顺序附 executed[i], executed[j], ... 的 Result 原文
    //
    // 语义是"原文透传"——框架不改写、不提纯、不合并，Planner 要精简就在规划时
    // 插 summarizer step。
    InputRefs []int
}
```

### 2. 改 `defaultExecInput`

```go
func defaultExecInput(original string, executed []ExecutedStep, current PlanStep) *schema.Message {
    var sb strings.Builder
    fmt.Fprintf(&sb, "原始目标：%s\n\n你的任务：%s", original, current.Description)
    for _, idx := range current.InputRefs {
        if idx < 0 || idx >= len(executed) {
            continue // 越界保护——记 warn 日志
        }
        e := executed[idx]
        fmt.Fprintf(&sb, "\n\n步骤 %d（%s）的产出：\n%s", idx+1, e.Step, e.Result)
    }
    return schema.UserMessage(sb.String())
}
```

**关键变化**：`InputRefs` 为空时**完全不附**任何 executed——与现状"硬塞上一步"
直接相反。这是有意的破坏性调整：保留旧默认 = 把错误固化。

### 3. `LLMPlanner` 的 `plan` signal tool 加 `depends_on`

```go
// plan tool schema 新增字段
"depends_on": map[string]any{
    "type": "array",
    "items": map[string]any{
        "type":    "integer",
        "minimum": 0,
        // maximum 每次 replan 动态生成 = len(executed) - 1
    },
    "description": "这一步需要哪些已完成步骤的**原文**。独立任务留空数组。" +
        "引用的是 executed 数组的 index（从 0 开始）。",
},
```

`tool_choice=required` + schema 里 `minimum/maximum` 约束，让 provider 先校验 index，
比事后跳过健壮。

### 4. Planner prompt 指导

默认 `LLMPlanner` system prompt 补判断规则（**不要求提纯**）：

```
对每一步判断它所分配的 sub-agent 所执行的任务，是否依赖前面已完成步骤的产出：
- 依赖：在 depends_on 里列出对应 index（从 0 开始）。原文会原样附给 sub-agent
- 独立：留空数组 []

判断起点是"这个 agent 的任务本身"，而不是"我的 description 里要不要提到前面"。
需要前面步骤的原文才能完成任务 → 列依赖；任务独立 → 留空。

不要在 description 里复制粘贴前面步骤的结果原文——设了 depends_on 框架会自动附上。
```

## ExecInputBuilder 的定位

`ExecInputBuilder` 保留，但**降级为用户层应急出口**：

- **主路径** = `InputRefs`（Planner 每步声明）
- **兜底** = `ExecInputBuilder`（用户 Go 代码自定义全局 input 构建逻辑）

两者是**互斥覆盖**关系（不是叠加）——一旦用户设了 `ExecInputBuilder`，`InputRefs`
就失效。文档需明确这点，避免用户同时用两者被绕晕。

**实施决定**：`scenario_arbiter` 彻底迁移到 `InputRefs`，删除 `ExecInputOverride`（详见落地总结）。
保留 override 会在 demo 里同时存在两套语义（refs 在 prompt 里说一套、override 在 Go 代码里做另一套），
违背"演示新机制"的目的。Override 仍作为框架级 escape hatch 保留给用户场景。

## 落地新增：demo 路径的 string-ref 解析

实施时发现：让 LLM 直接填 0-based int 容易出错（多轮场景下数 index 累），
所以 **demo 路径**（`example/orchestration/plan_chat/main.go` 的 `newJSONPlanner`）扩展了 refs 语义：

- refs 元素可以是 **int**（精确 index）或 **string**（agent 名）
- string → 在 `executed[].Agent` 里**倒序**找首个匹配，取其 index
- 找不到 → **立即返回 error**，不静默跳过（拼写错或捏造必须暴露）
- 多版本场景（想看历史所有 coder）→ string 只取最近一个，必须改用 int 列表

**为什么没让生产路径（LLMPlanner）也支持 string**：
- LLMPlanner 用 `tool_choice=required` + JSON Schema（`type: integer`），provider 层先校验，几乎不会出错
- 加 string 解析增加歧义和失败模式，得不偿失
- 两条路径职责分清：demo 宽进（教学），生产严进（鲁棒）

**ExecutedStep 加 `Agent` 字段**：string ref 需要按 agent 名定位历史步骤，
但原 `ExecutedStep` 只有 `Step`（description）和 `Result`，没有 agent 名。
新增 `Agent string` 字段，由 `execNode` 从 `SubAgentName` / `TmpSubAgent.Name` /
自动生成的 `tmp_step_<N>` 三级派生填入。默认 executor 路径下 `Agent` 为空字符串。

## 迁移影响（破坏性）

- **现有依赖"框架默认塞上一步"的用例会立刻坏**——sub-agent 看不到前置原文 → 失败
- `example/orchestration/plan_chat/` 除 arbiter 外的 5 个 scenario 的 Planner prompt 都要重写：
  - 加 `depends_on` 字段的判断规则
  - 要求 Planner 每步显式声明依赖
- 用 `ExecInputBuilder` 覆盖默认的场景不受影响（arbiter 等）
- 内置 `LLMPlanner` 默认 prompt 同步更新；用户自定义 prompt 需自己同步
- 测试影响：`compose/plan_test.go` 里依赖默认注入的断言都要改

## 实现拆解

1. **compose/plan.go**
   - `PlanStep` 加 `InputRefs []int`
   - `defaultExecInput` 按 `InputRefs` 注入 executed 原文，空则完全不附
   - 越界 index 跳过 + warn 日志
2. **compose/llm_planner.go**
   - `plan` tool schema 补 `depends_on`，动态 `maximum = len(executed) - 1`
   - 默认 Planner system prompt 补"判断依赖 + 不提纯"规则
   - 解析 tool_call 时把 `depends_on` 映射到 `PlanStep.InputRefs`
3. **example/orchestration/plan_chat**
   - 各 scenario 的 Planner prompt 补 `depends_on` 判断规则
   - arbiter 可选保留 `ExecInputOverride` 或改用 `InputRefs: [0,1,2,...]`
4. **doc/description/patterns.md**
   - PlanAgent 章节重写"每步输入构成"部分
   - 写清 `InputRefs` 与 `ExecInputBuilder` 的互斥覆盖关系
   - 强调"框架不提纯"设计原则——需要精简时插 summarizer step
5. **CLAUDE.md**
   - `PlanStep` 条目加 `InputRefs` 说明
   - PlanAgent 描述补"Planner 声明原文透传"

## 已知局限

本 issue 解决**执行层**的数据流问题，但**不解决** Planner 侧的上下文累积：

- Replan-每步下 Planner 每次 call 都需要看**全部 executed 原文**才能判断下一步依赖
- 任务越长，Planner 单次 call 的 input token 越大——长任务下 Planner 侧会成为
  context window 瓶颈

这是 **Supervisor 范式 + replan-每步 的结构性代价**——Planner 必须掌握全局。
本 issue 不触碰这一层。长任务下需要 Planner 侧历史压缩的场景，正解是**分层规划**
（PlanAgent 嵌套 PlanAgent，外层 Planner 拆成子任务交给内层 Planner），
这是独立 issue 范围。

新方案的**显式优化**在**执行层**：sub-agent 的 input 从 "硬塞上一步" 变成
"Planner 声明要哪些就给哪些（可以是 0 个）"——绝大多数独立任务 step 的
sub-agent prompt 不再膨胀。

## 收尾补丁（2026-04-23）

复盘"开放问题"后撤掉多数伪问题（详见下方），真正落地一条防御：

- **TmpSubAgent.Name 撞已注册 SubAgents 时立即 error**：避免 `ExecutedStep.Agent`
  出现同名 entry 导致 demo string-ref 倒序匹配静默错位（[compose/plan.go](../../compose/plan.go)
  execNode 构造 TmpSubAgent 分支）

> ExecInputBuilder + InputRefs 互斥曾考虑加 runtime warn，但项目处于测试阶段无
> 存量用户、所有 scenario 已迁完，不存在"用户改 prompt 加 refs 但不知道被
> builder 吞掉"的真实场景，纯属脑补。文档明确互斥关系即可。

复盘撤回的伪问题（供后来者少走弯路）：

- **0-based 反直觉**：prompt 已显式 + user 消息用 `[N]` label 标注，LLM 是字面抄 label
  不是数 index，不存在"数错"
- **sibling 没合法出口**：replan-每步契约下只执行 remaining[0] 就重调 Planner，
  同批 sibling 永远不需要互相 ref——等 B 真要执行时 A 已在 executed 里
- **首次 plan depends_on 越界**：首批第一个 step 的 executed 必为空，depends_on
  只能合法地填 `[]`；LLM 若硬填非空就是 hallucinate，跳过 + warn 就是唯一合理处理
- **生产路径扩 string ref**：label 可见时 int 没有"数错"问题，string 取最近一个
  反而限制精确选择——demo 宽进/生产严进的双路径职责本来就对

## 已完成项

- `compose/plan.go`：`PlanStep.InputRefs []int` + `ExecutedStep.Agent string` 字段；
  `defaultExecInput` 改为按 `InputRefs` 注入原文（空则完全不附）；execNode 从
  `SubAgentName` / `TmpSubAgent.Name` / 自动生成 `tmp_step_<N>` 三级派生 `Agent`
- `compose/llm_planner.go`：`plan` tool schema 加 `depends_on`，动态 `maximum =
  len(executed) - 1`；默认 system prompt 补"判断依赖 + 不提纯 + 0-based"规则；
  解析时映射到 `PlanStep.InputRefs`
- `compose/plan_test.go` / `compose/llm_planner_test.go`：断言更新到新语义
- `example/orchestration/plan_chat/main.go`：扩展 demo 的 `Refs []json.RawMessage`，新增
  `resolveRefs` 支持 int/string 混填；`buildReplanPrompt` 展示改成
  `[%d] (agent_name) desc` 0-based 对齐
- `example/orchestration/plan_chat/main_test.go`：覆盖 resolveRefs 的 7 个用例（int / string 倒序匹配 /
  混填 / 找不到报错 / 非法类型报错 / executed 空报错）
- 6 个 scenario 全部迁移到 refs 声明（`default` / `code_review` / `feasibility` /
  `fault` / `budget` / `drill`），fan-in / 多版本场景给出 int 显式提示
- `scenario_arbiter` 删除 `ExecInputOverride`，全量迁到 `refs:["searcher_a","searcher_b","searcher_c"]`
- `doc/description/patterns.md`：PlanAgent 章节重写"每步输入构成"+ summarizer
  step 编排建议 + ExecInputBuilder 互斥覆盖
- `CLAUDE.md`：`PlanStep` 条目加 `InputRefs` 与 `ExecutedStep.Agent` 说明

## 已知约束（按设计保留）

- **生产路径（LLMPlanner）只支持 int refs**：JSON Schema `type: integer` + `maximum`
  做 provider 层校验更鲁棒。string ref 的歧义和失败模式得不偿失，仅 demo 路径放开
- **"倒数第 N 个 X"语义不直接支持**：string ref 只取最近一个匹配；多版本场景
  （想拿历史所有 coder / driller / arbiter 的产出）必须改用 int 列表
- **默认 executor 路径下 `Agent` 为空字符串**：未设 `SubAgentName` 也未设
  `TmpSubAgent` 的步骤没有 agent 名，只能被 int ref 引用，不能被 string ref 命中
- **`InputRefs` 与 `ExecInputBuilder` 互斥覆盖**：一旦用户设了 `ExecInputBuilder`，
  `InputRefs` 就失效——文档已强调，避免叠加误用

## 开放问题

- **InputRefs 引用越界**（LLM 乱填）：实现选择"跳过 + warn"。生产路径有 JSON
  schema `maximum` 兜底，真到跳过分支说明 schema 也被绕过，warn 足够，不值得
  error 重新 plan
- **重复引用**（`[0, 0]`）：允许，按声明顺序注入（重复注入同一步原文）
- **自引用/未来引用**（`[current_idx]`）：越界处理（executed 里还没这步），跳过
- **string ref 命中策略**：当前固定"倒序取最近一个 + 找不到立即 error"。
  重名 agent 不主动 panic（`SubAgents` map 注册阶段已天然去重，runtime 不会出现
  重名实例）；若用户在 demo 里手填 `Refs:[]json.RawMessage{...}` 写错 agent 名，
  立即 error 暴露而非静默跳过
- **生产路径是否扩 string ref**：暂不做。若后续 LLM 数 index 出错率高再评估
  （需新增越界/拼写错的 retry 协议）
- **Planner 侧上下文累积**：长任务下 Planner 单次 input token 膨胀——本 issue
  不解决，正解是分层规划（PlanAgent 嵌套），独立 issue 范围

## 副收益：为动态并行铺路（future work）

显式 `InputRefs` 天然支持"静态可达性分析"：

- 若 Planner 一次返回 `[A, B, C]` 且三者 `InputRefs` 都只指向 executed
  （不互相指向 sibling）→ 三者之间无数据依赖，理论上可并发
- 若 Planner 返回 `[A, B]` 且 `B.InputRefs` 指向 `A`（同批 sibling）→ 必须串行

但**这需要扩展 `InputRefs` 的语义**（让 index 能同时指 executed 和 remaining sibling），
当前 issue 保持 InputRefs 只指 executed。动态并行是独立 issue，不在本 issue 范围。

## 参考

- ISSUE-45：PlanAgent replan-每步机制（确立了规划层动态性，本 issue 补数据层）
