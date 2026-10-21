# ISSUE-94：非-interrupt 致命错留悬空 Pending cp（通用框架行为，LOW backlog）

**状态**：⚪ **已解决**（2026-07-27，`2b5c9bc`）——见文末「最终解法」
**发现**：结构化输出补审 round —— 三个独立 audit agent（compact 叠加深审 / 结构化动态交互 / 结构化 A4 cross-verify）各自 probe 收敛到同一现象。

## 现象

caller 挂 `Checkpointer` 且 `compose_v2.Run` 以**非-interrupt 致命 error** 结束（model 错 / tool 错 / `StructuredOutputError` / 业务节点返 Go error）时：失败节点让 `cm.Snapshot().CurrentFrontier` 非空 → `graph_loop.go:347` 的 SaveNS 早退守卫 `len(CurrentFrontier)==0 && len(PendingInterrupts)==0` 不触发 → 存一份 `Status=Pending, CurrentFrontier=[failed-node], PendingInterrupts=0` 的 cp。而 `cleanupOnSuccess` 只在 `runErr==nil` 跑 → 这份 cp **永不清理**。

结构化输出的 `StructuredOutputError`（A4 预算耗尽 / commitFinal `sm==nil`）是一个**确定性、易触发**的入口，但现象本身**非结构化专属**——任何 node 非-interrupt 致命错都这样。

## 为什么判 LOW / 良性（3 agent 共识 + 实测）

- re-Resume 这个 sid：`commonResumePrep` 的 GA-M2 fail-fast 用 `ExistsNS` 返 true（cp 存在）→ 不 fail-fast → LoadNS 重建同一历史 → 确定性**再抛同一 error**（A4 在 LLM 之前，不额外调 LLM，不返 nil err，不出垃圾）。
- 无数据损坏 / 无控制流错乱 / 无死循环。PROBE3 实测：普通业务工具返 Go error 同样留 1 个 `Status=Pending` cp（frontier=[tools]），与 StructuredOutputError 留 [postamble]/[model] cp 同源。

## 真实代价

1. 长跑高 QPS 下确定性失败的 sid 会累积悬空 Pending cp，占存储（对齐 CLAUDE.md `active.sessions` OOM 关注模式）。
2. 违反 GA-M2 设计假设的精神（`resume_helpers.go` 注释：「cp 存在 ≈ 真正等答案的 Pending」）——这份悬空 cp 存在但 `PendingInterrupts=0`，不等任何输入。standard "cp exists → Resume" 的 caller 会永远撞同错。

## 修法方向（未实施）

两条候选：
- **A**：`graph_loop` SaveNS 早退守卫加条件——`CurrentFrontier` 非空但 `PendingInterrupts` 空 ⇒ 非 HITL 终态错，不存 Pending cp。
- **B**：Run wrapper 对非-interrupt 终态 error 走 `DeleteSubtree`。

**2026-07-27 补充：框架已有"终局失败主动清理"的现成机制**——`ErrRunAborted` 那条路
（`graph_loop.go` Run 出口 `errors.Is(runErr, ErrRunAborted)` → `DeleteSubtree` 清子树），
当前只有 cond 失败的 `CondFailAbort` 档走它。所以候选 B 不需要新造机制，只需回答
**「哪些错该算终局」**——而这正是 backlog 的原因：框架分不清确定性错（该丢）和瞬时错
（该留着重试）。真要动，方向应是**让业务声明**（类比 `CondFailPolicy` 把「重跑还是放弃」
交给业务，因为只有业务知道上游是否确定性），而不是框架猜。

**说清楚：那是绕开这个技术难点，不是解决它。** 框架信息不足——它看到的只是一个 error，
判不了下次会不会好；业务知道自己的上游是什么。代价是多一个 option 和一份心智负担，
收益是不用去解一个框架本就信息不足的问题。

**权衡（为何 backlog 不修）**：当前"非-interrupt 错也存 cp"保留了**瞬时错（网络抖动）的 crash-recovery Resume 重试能力**；改成"不存"会丢这个。框架难区分"确定性错（该丢）vs 瞬时错（该留）"。为一个 LOW/良性行为改 `graph_loop` 核心（高风险区，需 race test + 多轮 audit），ROI 低。

### 2026-07-27 复查：上面这段权衡，一句缩水、一句依然成立

| 原文 | 复查结论 |
|---|---|
| 「保留 cp 是为了瞬时错的 crash-recovery」 | **大幅缩水**——瞬时错这一档在业内普遍由**节点级重试**覆盖，而我们同样有 `RunWithRetry` + `SimpleRetry` / `ExponentialBackoff` 且 7 个编排全接了。绝大多数瞬时错在那一层就消化了，轮不到 cp 兜 |
| 「框架难区分确定性错 vs 瞬时错」 | **依然成立，没有解决** |

**第二条为什么还成立**（实测框架现有分类能力只有两档）：

```go
func IsRetryable(err error) bool {
	if IsControlFlowSignal(err) { return false }
	return !errors.Is(err, ErrRunAborted)   // 其余一律当可重试
}
```

只认「控制流信号」和「终局失败」，剩下全归一类。model 层的 `fallback` 稍细一点（按 HTTP
408/429 之类分），但只覆盖 model 调用，够不着 tool 错和业务错。

**一处自我更正**：复查过程中一度写过「兜不住的（重试耗尽）恰恰是确定性错」——**那句是错的**。
重试耗尽 ≠ 确定性错：网络断 30 秒、重试 3 次全落在这 30 秒里，耗尽了但它仍是瞬时错，过一会儿
Resume 能成，这种情况留着 cp 确实有价值。真正剩下的是「**重试窗口盖不住的长故障**」这一档
——触发面比原记录小，但不是零。

**仍未解决 / 当前无人在做**：让框架自己区分确定性错与瞬时错。可能的抓手是给 error 分类做一个
可扩展谓词层（业务注册自己的分类器），但那比「让业务在 option 上声明」重得多，且分类器写错的
后果跟现在一样静默。

⟹ **净结论：backlog 的理由变弱了但没被推翻。真要动，得先回答「谁来区分」——推荐让业务声明。**

**caller 侧缓解**（无需改框架）：caller 可靠 `len(cp.PendingInterrupts)==0 && len(CurrentFrontier)>0` 识别"确定性失败悬空 cp" vs "等 ask_user 的 cp"（后者 PendingInterrupts 非空），自行 `DeleteSubtree` 清理。

修时注意：不能误删真 HITL Pending cp（靠 PendingInterrupts 非空区分），不能破坏 a2a Cancel 依赖的 Abort 幂等。


## 2026-07-27 二次深挖：「谁来区分」有解，而且抓手我们已经有了

上一节说「框架难区分确定性错 vs 瞬时错」依然成立。再挖一层后**这个结论要修正**——难的不是
区分本身，是**此前没人把分类结果接到 cp 决策上**。

### 业内确实有成熟的分类器

主流实现里有一个按**异常/错误类型**分类的谓词（不是按"重试了几次"）：网络类 → 瞬时；
HTTP 5xx → 瞬时、4xx → 不是；编程错（类型错 / 参数错 / 查找失败之类）→ 确定性；
未知 → 默认当瞬时（宁可多重试）。它是可覆盖的（业务能换掉默认谓词）。

**但那个分类结果只喂给重试决策，不参与 cp 去留**——耗尽或不可重试都是直接抛。它们靠别的
结构免疫「失败 cp 反复重现同一个错」：append-only 历史 + 非自动 resume（残骸被新 head 推过去）、
或干脆不自动加载。我们三条都不占（见 checkpoint-storage.md 对照）。

### 而我们的分类抓手比想象中齐全

| 层 | 抓手 | 位置 | 粒度 |
|---|---|---|---|
| model | `model.APIError{StatusCode, Body}` + `fallback.DefaultFailoverPolicy` | `fallback/policy.go:60-92` | **比业内那个还细**：0（网络不可达）/ 408·429（超时限流）/ 4xx（请求本身有问题，换了白搭）/ 5xx（provider 故障），还单独识别 quota 耗尽（部分 provider 用非 429 表达，按 body 关键词判） |
| tool | `tool.ErrCode*` 枚举 | `tool/tool.go:140-144` | `timeout` / `retry_limit_exceeded` 偏瞬时；`invalid_arguments` / `tool_not_found` 偏确定性 |
| 引擎 | `IsControlFlowSignal` / `ErrRunAborted` | `compose_v2/retry.go:145` | 只有两档，是当前 `IsRetryable` 的全部依据 |

**关键前提已实测通过**：错误冒泡到 Run 出口时**类型信息完好**——构造 429 和 400 两种
`APIError` 跑完整 Run，出口处 `errors.As(err, &apiErr)` 都拿得到原状态码。也就是说
"出口按错误性质决定 cp 去留"技术上可行，不存在"错误被 wrap 掉分类不了"的障碍。

实测同时确认了现状：**429 和 400 都留 cp，没有区别对待**。

### 结论修正

原记录「框架难区分」→ 应改为「**框架有分类能力，只是没接到 cp 决策上**」。model 层那个
分类器（`DefaultFailoverPolicy`）的语义是「换个 provider 有没有用」，跟「过一会儿重试有没有
用」高度重合但不完全等价——真要复用得先想清楚语义对齐（例如 quota 耗尽：换账号有用，
过一天也有用，但立刻重跑没用）。

**这让方案 B（出口按错误性质决定 cp 去留）从"卡在难点上"变成"卡在语义设计上"**，是可推进的。

---

## 最终解法（2026-07-27，`2b5c9bc`）

**根因不是「留了 cp」，是存档在说谎。** `SnapshotForGraph` 无条件把 Status 标成 Pending，
于是失败退出留下的存档跟 HITL 挂起长得一模一样——auto-load 赖以成立的不变量
（cp 存在 ⇒ 真有人在等答案）被破坏了。

**解法**：加一档 `CPStatusFailed`，出口按 `IsControlFlowSignal` 判：

- 中断 / 软取消 / 陈旧写回 → **挂起**，照常 Pending
- 其余带错退出 → `CPStatusFailed`

**完全不看错误是瞬时还是确定性**——那正是原 backlog 卡住的难点，这一刀绕开了它：框架
判不了一个 error 下次会不会好，也不必判，标出「这是失败现场」就够了，要不要再试是业务的事。

**能力不丢**：`ListPending` 不返失败态（不再被自动捡起重撞），但**存档保留**，新增
`Status.Resumable()` 谓词把「能不能恢复」和「要不要自动恢复」分开——业务判断这次是瞬时
故障，显式 `WithResumeFrom` 仍能从失败点续跑。durability 中途写的不受影响（恒 Pending，
它是崩溃恢复点必须能自动捡）。

设计依据与五家对照见 [checkpoint-storage.md](../description/three-way-comparison/checkpoint-storage.md)
——本质是把「cp 存在 ⇒ 有人在等」这条硬不变量在我们的约束下重建（别家靠「失败不写」，
我们不能不写（durability 档要求），所以靠「写但标对」）。
