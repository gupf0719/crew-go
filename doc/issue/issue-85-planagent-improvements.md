# ISSUE-85 · PlanAgent MED/LOW 改进集合

**优先级**：MED（架构对称性 + 业务体验改进，3 HIGH 已修剩 4 MED + 2 LOW）
**状态**：✅ 已修（2026-05-16）— HIGH-1/2/3 commit 23d75cb + MED/LOW 全修
**触发**：R32+ 9 agent 并发审 视角 1 (PlanAgent NS) + 视角 2 (SkillSubAgentResolver
返编排型 sub-agent NS push) 收集所有 PlanAgent 相关 finding

## 已修 (commit 23d75cb)
- HIGH-1: `len(refs)` 当 step 计数 BUG → 用 globalIdx (扩 PlanWorkerFn 签名)
- HIGH-2: TmpSubAgent + Resume 死循环 → LoadAndDelete → Load
- HIGH-3: pendingTmpSteps 内存泄漏 → cleanupPendingTmpSteps defer

## 待修 finding

### MED-1: TestPlan_R28_HIGH1_NestedNSPushResume 测试假绿（视角 1）

**位置**: `agent_v2/plan_nested_resume_test.go:106, 127`

`expectChildNS = "step_plan/plan/sub_step_0"` 与代码实际 `"plan_step/sub_step_0"`
不符 (PlanAgent 不 push 自己 name + step 名是 "plan_step" 不是 "step_plan")。
测试用 `t.Logf` 不 `t.Errorf` → 假绿。

**修法**: 修正字面量 + Logf 升级 Errorf 真断言。

### MED-2: PlanAgent 不 push 自己 name + 缺 PlanBuilder.WithName（视角 1）

PlanAgent 自身 NS 不 push name → GraphID 走 fingerprint 兜底 (其他 6 编排都有
`compose_v2.NewXxx().WithName(p.Name())`). fingerprint 在 sub-agents 集合变更时
变化, Resume 前 user `AddSubAgent("newOne", ...)` → ErrGraphIDConflict。

**修法**: 给 `compose_v2.PlanBuilder` 加 `WithName` 方法, PlanAgent.buildPlanGraph
调用。

### MED-3: ExecInputBuilder.executed 是 InputRefs 子集而非全部历史（视角 1）

`Planner.executed` godoc 明确说"已完成的所有步骤", ExecInputBuilder 的 `executed`
命名相同但实为子集 (refs 映射的 highExecuted)。`defaultExecInputBuilder` 输出
"步骤 1（finder）的产出" — `idx+1` 是子集本地索引+1, 非真实步骤序号。

**修法**: 把全 executed 也传进 wrapper (改 PlanWorkerFn 签名加 `executed []ExecutedStep`
参数), 或 worker 内部从 closure 引用累计 list。

### MED-4: AddSubAgent 无 NS-safe 字符校验 + 无重复注册检测（视角 1 + 视角 2）

与其他编排不一致:
- HandoffAgent.AddAgent panic on ValidateNodeIDForNS 失败
- ParallelAgent.AddBranch panic on duplicate name
- **PlanAgent.AddSubAgent 静默接受任何 string + 重复注册 silent overwrite**

**修法**: panic-fast 校验 + 重复检测 (4 处统一: PlanAgent.AddSubAgent /
SequentialAgent.AddStep / ParallelAgent.AddBranch / RoutingAgent.AddBranch)。

### LOW-1: makeTmpSubAgent UTF-8 字符串切片可能产生非法 UTF-8（视角 1）

**位置**: `agent_v2/plan_agent.go:603`

`name = "tmp_step_" + stepDesc[:min(len(stepDesc), 8)]` — 多字节 UTF-8 (中文
每字 3 字节) 时第 8 字节可能切在中间 → name 含非法 UTF-8。下游事件 emit /
log slog 可能报错或乱码。

**修法**: 用 `[]rune(stepDesc)[:min(...)]` 按字符切。

### LOW-2: resolveRefs 静默跳过失败 ref（视角 1）

**位置**: `agent_v2/plan_agent.go:633-651`

`IndexRef(99)` 在 executed 长度只有 5 时静默跳过, `AgentRef("nonexistent")` 静默
跳过。caller 拿到的 resolvedRefs 比 InputRefs 短, `defaultExecInputBuilder`
silent 漏数据。

**修法**: 至少 `slog.WarnContext(ctx, "ref unresolved", ...)`, 或 strict 模式返 error。

### MED (视角 2): PlanAgent.runPlanGraph 不 forward `compose_v2.GetSessionID(ctx)`

与其他 8 编排不对称 — 当前 fork 链路因 NewSharedChildSession 共享 RunID 不踩
bug, 但架构不对称易让业务自定义 SkillSubAgentResolver 返 PlanAgent 时踩。

**修法**: 照搬 BasicAgent.runGraph L304-309 模板:
```go
sid := compose_v2.GetSessionID(ctx)
if sid == "" { sid = session.RunID }
runOpts = append(runOpts, ..., compose_v2.WithSessionID(sid))
```

### LOW (视角 2): GraphAgent 业务 NodeFunc 漏 push graphNS 时 cp 冲突无 framework defense

业务通过 SkillSubAgentResolver 返 GraphAgent 时, 内部用户自定义 NodeFunc 直接调
`subBA.Run(ctx, ...)` 漏 push 子 ns → cp 与 GraphAgent 自身 cp 冲突。CLAUDE.md
已文档化但无 framework warn。

**修法**: GraphAgent godoc 加更显眼警告 + 可选 `WithGraphAgentAutoPushChildNS(true)`
option 让框架自动 push, 或 `compose_v2.Run` 内为根节点检查 cp.GraphID 冲突时
提供更友好 error 指向"业务自定义 NodeFunc 嵌套"可能。

## audit follow-up (2026-05-16)

R32+ 视角 1+2 修完后再 audit, 揭露 2 真 HIGH:

### audit HIGH-1: TmpSubAgent dispatcher allExecuted 漏修 (commit 谎报修补)

commit 0aa15bf 描述说 "PlanAgent 3 worker 都改用 globalIdx + allExecuted", 实际**TmpSubAgent dispatcher 漏修** (MED-3 完整闭环失败 — 视角 B audit 抓自我谎报)。

**修法**: `agent_v2/plan_agent.go::PlanAgent.runPlanGraph` TmpSubAgent dispatcher 单独 Edit 改用 globalIdx + allExecuted, 与 SubAgentName / executor 两 worker 一致。

### audit HIGH-2: (Step, Agent) 反向映射用正序 → 多 agent 同名场景错位

`resolveRefs` 通过 `AgentRef("name")` 反查 executed[i] 时用正序 (`for j := 0; j < len; j++`), 多 agent 同名场景 (LoopAgent body 多轮调同 sub-agent) 永远拿到第一个 — 用户预期"最近一次该 agent 输出"语义反位。

**修法**: 改倒序 `for j := len(allExecuted) - 1; j >= 0; j--` — 多名同 agent 场景拿"最近"语义对齐用户直觉。

### audit follow-up 测试:

- `agent_v2/issue85_planagent_improvements_test.go` 加 sub-test:
  - `TestISSUE85_MED3_ExecInputBuilderSeesFullExecuted_TmpSubAgent` — 验 TmpSubAgent dispatcher 也走 globalIdx + allExecuted (audit HIGH-1 fix)
  - `TestISSUE85_HIGH2_ResolvedRefsReverseLookup` — 验 (Step, Agent) 反向映射用倒序 (audit HIGH-2 fix)

### audit MED 改进:

- **WithSubAgents fail-fast** — `agent_v2/plan_agent.go::WithSubAgents` 加 NS validate + nil check, 避免静默接受非法 NS 字符或 nil sub-agent

## audit follow-up² (commit 49d3065 audit, 2026-05-16)

commit 49d3065 落地 (audit follow-up 修 IndexRef regression + WithSubAgents 合并) 后
再 audit, 揭露:

### audit² HIGH-A1: IndexRef regression 测试只覆盖 1/3 worker

视角 A 实测 — commit 49d3065 测试 `TestISSUE85_AuditFollowup_HIGH1_IndexRefPreservesExplicit`
只走 SubAgentName worker (用 `pa.AddSubAgent("finalizer", finalizer)`). 单独 revert
TmpSubAgent (plan_agent.go:566) 或 executor (plan_agent.go:609) IndexRef fix 测试**仍 PASS** —
**5 类炸弹 #4 双层防护遮蔽变体** (3 worker 任意 1 个修对就 PASS).

同 commit 0aa15bf "3 处都改" 谎报模式 — 修了多 path 测试只覆盖 1 path, audit 才抓到.

**修法**: 加 2 子测试:
- `TestISSUE85_AuditFollowup2_HIGH1_IndexRefPreservesExplicit_TmpSubAgent` — TmpSubAgent dispatcher 路径
- `TestISSUE85_AuditFollowup2_HIGH1_IndexRefPreservesExplicit_Executor` — executor worker 路径

### audit² HIGH-A2: AddSubAgent / WithSubAgents typed nil interface 陷阱

视角 A 探针实测揭露 — `if a == nil` 检漏 Go interface "typed nil":
`var b *BasicAgent; pa.AddSubAgent("x", b)` 时 b 是 typed nil pointer 但 Agent
interface header 持非 nil type tag → `a == nil` 是 false → 不 panic → 业务后续
`ag.Run(...)` nil deref panic 在 worker 远 stack 难定位.

**修法**: 加 `isAgentNil(a Agent) bool` helper 用 `reflect.Value.IsNil()` 检 underlying
value 真 nil. AddSubAgent + WithSubAgents 改用此 helper. 加 2 测试 (`*_TypedNilTrap`).

### audit² MED-A1: WithSubAgents 合并语义正向测试缺口

视角 A + D 跨视角共识 — commit 49d3065 测试 `TestISSUE85_AuditFollowup_HIGH2_WithSubAgentsMergesNotOverwrite`
名 "MergesNotOverwrite" 但实质只测 dup-panic 路径. 真"merge 保留 prior"语义 0 正向断言.
若未来 silent revert 合并改回 `p.subAgents = agents`, dup-check 能保护 dup name 但
**不同 name 的 silent loss** (AddSubAgent('a1') + WithSubAgents({'a2': ...}) → a1 丢失) 无测试覆盖.

**修法**: 加 `TestISSUE85_AuditFollowup2_MED1_WithSubAgentsMergePreservesPrior` —
端到端验"a1 注册 + WithSubAgents 加 a2 后 planner 调 a1 应成功".

## 关联

- R32+ 视角 1 audit: PlanAgent NS 完整 finding
- R32+ 视角 2 audit: SkillSubAgentResolver 返编排型 sub-agent NS push 链
- R32+ audit follow-up: TmpSubAgent dispatcher commit 谎报修补 + (Step, Agent) 倒序映射
- R32+ audit follow-up² (4 agent commit-level audit): IndexRef 3 worker 测试覆盖 + typed nil interface 陷阱 + 合并语义正向测试
