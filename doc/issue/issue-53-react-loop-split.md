---
id: ISSUE-53
title: executeRound 拆分（ISSUE-51 #2 拆出独立追踪）
status: ✅ 已实现
priority: 中
created: 2026-04-27
resolved: 2026-04-27
source: ISSUE-51 #2 原列为"executeRound 拆分"延后项；2026-04-27 单独拆出 issue 跟踪并立即落地
---

# ISSUE-53 · executeRound 拆分

## 当初为什么要拆

ISSUE-51 #2 的原始记录：在单测覆盖扩展过程（`agent` 76.5% → 90.3%）中发现 `executeRound`
**743 行测试代码只为覆盖这一个函数**，是明确的复杂度信号。

具体痛点：
- 一函承载 8 件不相关职责：model 调用 / tool 循环 / 8 种 hook 触发 / memory 注入 /
  history compact / session stamping / runID stamp / skill 路由
- **新增 hook 类型必须改这处巨型函数**，改坏概率高
- **局部替换不可能原子化**：想让 tool 循环支持并发执行，几乎不可能不动 model 调用——它们
  的局部变量（response / chatModel / outMessages）是缠绕的
- CLAUDE.md 已列出"OnModelEnd vs OnAgentEnd"这种坑，正是**函数内不同分支对 outMessages
  累积时机不一致**导致的，典型职责混杂副作用
- ISSUE-51 #1 的 `tc.ID` 笔误漏洞，根因之一就是函数太长 → 同函数内不同分支引用同名变量时
  作者凭记性记"hook 后该读哪个" → 漏一处就是 bug

修 ISSUE-51 时只在 #1 范围内做了局部抽取（把工具调用循环体抽成 `executeToolCall` helper），
executeRound 主体仍是单函数。本 issue 接续把整个 executeRound 拆完。

## 落地方案

executeRound 抽 3 个未导出 helper，主体降到 ~25 行：

```
executeRound（主体只做调度）
 ├── defer OnAgentEnd
 ├── callModelStep           段 2+3+4 → (response, error)
 ├── if 无 tool_call:
 │     emitFinalAnswerEvents 段 6
 │     return response
 └── runToolCalls            段 7+8 → ([]*Message, abortErr)
```

| Helper | 入参 | 职责 |
|---|---|---|
| `callModelStep` | ctx, cfg, info, messages, chatModel, toolInfos, runID | OnModelStart → 比对/重绑 → Generate → stampRunID → OnModelEnd → 再 stampRunID。返回 response。**不碰 outMessages** |
| `runToolCalls` | ctx, cfg, info, toolCalls, skillActivator, runID | 遍历 toolCalls → 委托 `executeToolCall`，聚合所有 tool_result 消息 + 首个中止错误 |
| `emitFinalAnswerEvents` | ctx, agentName, input, response, runID | 无 tool_call 轮往 session 写 inputEv + exitEv（共享 runID）。session==nil 时 no-op |

executeRound 主体重写后：

```go
func executeRound(...) (finalResp, outMessages, llmResp, outErr) {
    defer func() { ... OnAgentEnd 回写 ... }()
    outMessages = messages

    // 段 1：调 LLM
    response, err := callModelStep(ctx, cfg, info, messages, chatModel, toolInfos, runID)
    if err != nil { return nil, outMessages, nil, err }

    // 段 2：分支 —— 无 tool_call 即最终答案
    toolCalls := schema.GetToolCalls(response)
    if len(toolCalls) == 0 {
        emitFinalAnswerEvents(ctx, cfg.Name, input, response, runID)
        return response, outMessages, response, nil
    }

    // 段 3：有 tool_call —— 附 assistant 消息后跑 tool 循环
    outMessages = append(outMessages, response)
    toolMsgs, abortErr := runToolCalls(ctx, cfg, info, toolCalls, skillActivator, runID)
    outMessages = append(outMessages, toolMsgs...)
    if abortErr != nil { return nil, outMessages, response, abortErr }
    return nil, outMessages, response, nil
}
```

## 设计取舍

**为什么不打包成 `roundDeps` struct 减参数？**

最自然写法是 `type roundDeps struct { cfg, info, runID, skill }`，helper 接 `*roundDeps`，
参数从 6-7 个降到 2-3 个。但被 CLAUDE.md "ctx 只传不存" 边界扫到：若 `roundDeps` 不持 ctx，
helper 还是要单独传 ctx；若持 ctx 违反约定。最后省下来字段就 4 个，收益不抵新增类型成本，
**保持显式参数列表**。

**为什么 `callModelStep` 不返回更新后的 chatModel？**

OnModelStart 的 Tools 改写**只在本轮生效**——重绑后的 chatModel 用完即丢。把它返出去会
暗示"重绑跨轮可见"，那是另一个语义（应该走 cfg 一级），**现在不打开这扇门**。

**为什么不顺便拆 reactLoop？**

reactLoop 的"准备阶段"（systemPrompt + Memory.Read + SelfHistory + compact + 绑工具，注：v1 历史，PR 3.3 后 Memory.Read 已替代为 store.Search）虽然
45 行但顺序线性、每步独立，读起来不费劲——拆分边际收益小，且会引入 8+ 入参回传 4 元组的
难看签名。**当前不动 reactLoop**，等真正出现"准备阶段被复用"诉求时再单独评估。

## 不变量（落地时已遵守）

- `defer OnAgentEnd` 留在 executeRound 顶层 —— 与 reactLoop 里的 OnAgentStart 严格配对，
  下沉到 helper 会让错误路径漏触发
- `outMessages` 命名返回值 + defer 改写的语义不动 —— 三个 helper 都不直接 mutate outMessages，
  由 executeRound 主体显式 append
- `runToolCalls` 内部对 `ctx.Err()` 的检查放在每次 tool 循环开头 —— 不要升到入口一次性查，
  那会改变并发取消语义
- `runID stamp` 的两次贴（OnModelEnd 前/后）搬进 callModelStep 后保留两次 —— 这是 hook 改写
  Message 后的契约要求，不是冗余
- `emitFinalAnswerEvents` 在 `GetSession(ctx) == nil` 时 no-op —— 与原行为一致，让脱离 session
  的调用也能跑通

## 行数对比

| | 函数 | 行数（含签名注释） |
|---|---|---|
| 现状 | executeRound | 110 |
| 重构后 | executeRound 主体 | 43（实际逻辑 ~20 行） |
|  | callModelStep | 56 |
|  | runToolCalls | 28 |
|  | emitFinalAnswerEvents | 18 |

总行数持平，**单函数最大复杂度从 110 → 56**。三个新 helper 各自职责单一、命名即文档。

## 测试影响

零改动通过：`agent` / `compose` / `hook` / `mcp` / `skill` 全部单测在重构后立即跑过。原
`react_coverage_test.go`（743 行）端到端覆盖所有 hook / 错误 / tool 分支，行为不变它们照过。

新 helper 默认走旧用例覆盖，不需要补单独的单元测试 —— 它们的所有分支都通过 executeRound
间接命中。

## 交付物

- [x] **2026-04-27** [agent/react.go](../../../crew-go/agent/react.go)：抽 3 个 helper，
      executeRound 主体 110 → 43 行（实际逻辑 ~20 行）
- [x] **2026-04-27** 全量单测通过，行为零变化

## 参考

- ISSUE-51 · 同一轮设计债清单（#1 抽 `executeToolCall` helper，本 issue 接 #2 拆主函数）
- [agent/react.go](../../../crew-go/agent/react.go) — 重构后状态
- [agent/react_coverage_test.go](../../../crew-go/agent/react_coverage_test.go) — 743 行端到端覆盖，零改动通过
- CLAUDE.md "Hook 选择速查" / "ctx 只传不存"
