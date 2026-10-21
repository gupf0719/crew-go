# ISSUE-75 · BasicAgent commitFinal postamble 阶段 InterruptError 后 cp 已 cleanup / Resume 不可达

**优先级**：高（HITL 设计完整性）
**状态**：✅ 已修（2026-05-15）— 方案 A postamble 节点化落地（与 ISSUE-74 完全对称）
**发现轮次**：R27a audit HIGH-1（2026-05-14）
**对称 ISSUE**：[ISSUE-74](issue-74-preamble-hitl-cp-gap.md)（preamble 阶段同模式 bug 已修）

> **API 变更说明**（PR 3.3 commit e6505f9）：本 issue 描述的 `Memory.Write` 接口已由 store 体系替代——
> `Memory.Write` → `store.Put` / `WithMemory` → `WithStore` + `WithStoreMemory`。postamble 节点化的修复在
> store 体系下等价保留（postamble 节点跑 commitFinal + `store.Put`）。下文叙述保留历史 API 名作档案。

## 问题

`BasicAgent.Run` 流程：

```
runGraph（compose_v2.Run 成功 return）
    ↓ graph_loop defer 触发 cleanupOnSuccess → DeleteSubtree(sid, "") 清整 sid
commitFinal（line 167 / 238）
    ↓ Memory.Write 业务可抛 InterruptError（R24 audit MED-2 修后 propagate）
caller 拿 InterruptError
    ↓ caller 调 BasicAgent.Resume(sid, WithResumeUserInputs)
Resume → compose_v2.Run auto-load
    ↓ cper.LoadNS(sid, "") 返 nil（已被 DeleteSubtree 删）
fresh Run 路径从头跑全 graph
    ↓ 又跑到 commitFinal → Memory.Write 又抛 InterruptError
**HITL 链断 / 业务依赖 ResumeContext 续跑路径丢失 / 严重时无限循环**
```

## 与 ISSUE-74 对照

ISSUE-74（preamble，已修）和本 ISSUE（postamble）是 BasicAgent.Run 拓扑两端的对称缺口：

| 阶段 | ISSUE | 业务回调位置 | bug 现象 |
|---|---|---|---|
| **preamble**（graph 启动**前**） | ISSUE-74（已修）| `Memory.Read` / `HistoryCompactor` / `SkillBox.ActivateSkill` | cp 不存（graph 没启动）→ Resume 找不到 cp |
| **postamble**（graph 完成**后**） | ISSUE-75（本）| `Memory.Write` | cp 已被 cleanup → Resume 找不到 cp |

ISSUE-74 修法（方案 A）已落地：把 preamble 业务回调下沉到 graph 首段独立节点。本 ISSUE
对称解：把 commitFinal 上升到 graph 末尾独立节点。

## 修法（方案 A：postamble 节点化）

把 commitFinal 的 Memory.Write + AddEvent 重构成 graph 末尾 `postamble` 节点：

```
现拓扑：
  StartNode → preamble → model → tools → model（循环）
                              ↓ 无 tool_call
                            EndNode
  ↑ commitFinal 在 BasicAgent.Run 里跑（graph 之外）

改后：
  StartNode → preamble → model → tools → model（循环）
                              ↓ 无 tool_call
                            postamble → EndNode
  ↑ commitFinal 移成 graph 内 postamble 节点
```

抛 InterruptError 走 graph 内 SaveNS 协议自然支持 Resume：
1. postamble 节点抛 Interrupt → graph_loop 自动 SaveNS cp + recordInterrupt
2. cleanupOnSuccess 不触发（runErr != nil）
3. caller Resume → cm.Restore → 重跑 postamble → Memory.Write 通过 GetResumeAnswerText 拿答案续跑

## 实施工作量参考

类似 ISSUE-74 的 6 step 实施链（compose_v2 引擎层 + agent_v2 BasicAgent + 5 回归测试 +
godoc + smoke + CLAUDE.md）。复用 ISSUE-74 已有的 BasicAgentBuilder.WithPreamble 模式
增加 `BasicAgentBuilder.WithPostamble` builder + 拓扑分支。

## 当前缓解

`agent_v2/basic_agent.go::commitFinal`（line 606-621）已加 slog.WarnContext：

```
"agent_v2: commitFinal Memory.Write 抛 InterruptError——cp 已被 cleanup（v2 已知限制 ISSUE-75）；caller 需走应用层 HITL 路径"
```

err 仍 propagate（`%w` wrap，`IsInterrupt` 透 chain），caller 至少能感知 HITL 触发。
业务可走应用层 HITL 路径（业务自己持久化用户答案后重新调 Run）。

## 关联

- [ISSUE-74](issue-74-preamble-hitl-cp-gap.md)：preamble 阶段同模式 bug 已修——方案 A 模板可复用
- R24 audit MED-2：`commitFinal` propagate Interrupt（commit `245557a`）——本 ISSUE 是 R24 fix 的对称缺口
- R27a audit HIGH-1（2026-05-14）：发现本 ISSUE

## 决策

R27a audit 暂不修，单独 ISSUE 跟踪。优先级 **高** 但非阻塞——业务实际触发场景较窄
（Memory.Write 抛 Interrupt 是 R24 设计认可 pattern 但实际业务侧用 commitFinal 阶段
HITL gate 概率较低，业务通常走 prepare 阶段做 HITL；当前 caller 至少能感知 Interrupt
走应用层 HITL）。

后续修复参考 ISSUE-74 6 step 实施链。
