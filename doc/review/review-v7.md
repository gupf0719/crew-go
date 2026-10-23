# Code Review #7 — test 分支增量审计 (2026-10-22)

> **范围**：`test` 分支 2026-10-21 22:38 → 2026-10-22 16:01 的 20 个 commit
> （`008d63e^..806c3a3`，239 文件 +14500/−15329）。按子系统分：mcp 按 server 降级（7 个
> commit）/ observability 改用标准 OpenTelemetry（2）/ `model/anthropic` 原生适配器（3）/
> `ModelNamer` 协议（2）/ 软降落轮（1）/ `sql` HistoryStore（1）/ 输入摘要深嵌套（1）/
> 清理 spec 依赖（1）/ 文档与测试（2）。
>
> **结论**：代码质量高，问题集中在**哨兵覆盖**而非行为错误。全部机械验证与两个强制冒烟通过。
> 计 **1 条无条件行为 bug**（H-01）+ **1 条条件行为缺陷**（H-02，provider 不守 `tool_choice=none`
> 时触发）+ **6 处哨兵缺口**（实现正确但无防护，下次重构会静默退化）。
>
> ⚠️ **所有 `file:line` 以审计时的 `806c3a3` 为准**。本轮修复已经改动了其中几处（H-01 的
> `mcp/server.go:234`、H-04 的 `observability/content.go:245`、M-02 的 `fallback/fallback.go:491`
> 等），行号会漂——定位请按函数名与符号名找，别照行号跳。
>
> **修复状态**（2026-10-23 收口）：H-01 / H-03 / H-04 / H-05 / H-06 / M-01 / M-02 / M-03 与 H-02 的
> 可观测性那半**已全部修完**，各配 sanity-revert 验证过的回归哨兵。本轮登记进 backlog 的 5 组预存缺陷
> 随后逐组修完（一组一个 commit）：`cec060b` fallback `startIndex` 候选为空返 -1 不给越界下标 /
> `ce8cb61` history conformance 长 key 用例 + `HistoryKey` 可移植上限声明 / `f6e0c89` observability
> 三条承重语义哨兵（扇出实例维 / A2A server span 嵌套 / tool.category 白名单）/ `a3d7218` 宽度失控
> 劈出专属的 `MaxWidthError` + BasicAgent 出口重铸 `Iterations` / `f1afa64` 上一条带出来的
> `error.type` 分类没跟上（修它时发现 `errorTypeOf` 5 条既有分支零用例，补了全分支表）。
>
> **仍开着的只有两条**，都在 `doc/code/todo.md` 里带理由登记：① H-02 的**行为改动**（留一格无工具轮 /
> 出口边收尾）——它的曝光面取决于「`tool_choice=none` 在真 provider 上强制不强制」，而本环境没有 API
> key，没实测就改默认行为是拿设计换猜测；② H-04 的最后一条哨兵（「装两层 Observability + 扇出」组合下
> `crew.node.instance` 的 span 个数）——需要再搭一层 Observability 才测得出，单独那半已有哨兵。

---

## 审计方法

5 个单视角审计 agent 并发（CLAUDE.md「多 agent 单视角并发优于单 agent 多视角」），
每条 finding 再由主审在独立 git worktree 里重做变异 / 写探针复现。下文每条标注证据等级：

- **[复现]** — 主审亲自重做变异或写探针，看到了报告描述的那个结果
- **[实测+环节核实]** — agent 的对照实测，主审核实了其中的关键代码环节，未独立重跑完整探针
- **[机械对账]** — 静态核对（数用例、grep 调用点、读 DDL），不涉及变异或探针
- 没有标签的（M-02 / M-03）：M-02 是 agent 探针实测、主审另做了变异验证（见该节「修复」）；M-03 是
  主审自己 grep 出来的

### 全绿项

| 验证项 | 结果 |
|---|---|
| `go build` / `go vet ./...` | PASS |
| `go test ./...`（根 module） | PASS |
| `go test` sql / redis / a2a/zk（3 个独立 module） | PASS |
| `go test -race -count=3` observability / agent_v2 / model / fallback | PASS，无竞争 |
| `go test -race ./mcp/ -count=2` | PASS |
| `example/orchestration/nested_smoke` | **12/12** |
| `example/orchestration/plan_chat_web` 冒烟 | PASS（中断 → Resume → cleanup 全链） |
| `example/a2a/hitl_smoke` / `example/orchestration/channel_smoke` | 全 PASS / 5/5 |
| `commentaudit` 第 1 层（183 个改动文件） | 失效符号 **0**；7 条路径 ERROR 读语境后**全是假阳**（eino / a2a-go 的 `internal/…`，三方对照文档引用对方源码本就不在本仓） |
| `TestGodoc_NoBlankLineBetweenCommentAndDecl` / `TestGodoc_NoAbsorbedDocBlock` | PASS（扫全仓含独立 module，带扫描面自检） |
| `TestHookCtx_AdoptedHooksOnlyThroughCallHook` | PASS — observability 新增的 `wrap.go` 包装业务钩子**没有绕过** `compose_v2.CallHook` 这个唯一采纳入口 |
| `TestMemberDispatch_AllAgentRunsGoThroughHelper` / `TestHandoffGrant_EveryReadIsSelfChecked` | PASS |
| 被删的旧 observability 导出 API（`GenTraceID` / `InjectToHeaders` / `TraceIDFor` / `Counter` / `Config` …）按 `observability.X` 限定形式查全仓残留 | **0**（.go 与 .md 都查了）。⚠️ 裸写的类型名查不到——M-03 报的 `fault-analysis-system.md:680` 里就有一个裸 `Config` |

### 范围声明（重要）

审计环境**无任何 API key**（无 `.env`、环境变量里也没有），所以**真 provider 路径本轮一律没有
复跑**，`example/model/anthropic_smoke` 在 `-tags integration` 下 4 例全部 SKIP。这不等于那些路径
没被验过——作者自述的情况如下，本审计**未能复核**：

- **anthropic 已真机跑过**：`9ac2de4` 自述「真 API 跑通 example/model/anthropic_smoke 四项（全部
  `prefix_mismatch_behavior="error"`）」、`521c11a` 自述「Haiku 5.5 下四项在 error 档全过：G1 回放
  thinking 3 次、G2 第二轮 12528 个输入 token 里 12464 个读 cache」；`b3583bd` 另自述在真 API 上验过
  「工具照旧 + `tool_choice=none` 成功，摘工具或改一个工具描述都 400」。
  ⚠️ 本篇初稿曾引 `8fb0749` 的「真 API 冒烟尚未真机跑」并由此断言「真 provider 路径全部未验证」
  ——那是**选择性引用了同范围内被后两个 commit 推翻的自述**，已更正（文档审计 E1）。
- **qwen / deepseek 的 `tool_choice=none` 仍无实测**（`b3583bd` 自述「只核了官方文档」），而 qwen 是
  本仓默认测试模型 ——**H-02 的曝光面大小取决于它**，建议最先验这一件。
- `sql` HistoryStore 的 PG / MySQL 建表未连真库（`9df7d65` 已如实披露并登记 todo）。
- `model/anthropic` 包本身 3496 行（生产 1803 + 测试 1693）。⚠️ 初稿写的「4271 行」是 `8fb0749` 跨
  28 个文件的总插入数（含 CLAUDE.md、go.sum、`modules/common` 等），不是这个包的行数，已更正（E4）。

---

## H-01 [MED-HIGH] mcp：业务 Client 的 `CallTool` panic 冲出 `RemoteTool.Call`

**位置**：[`mcp/server.go:234`](../../mcp/server.go)（`res, err := c.CallTool(callCtx, source, args)`）

**问题**
`mcp.Client` 的 `CallTool` 是业务可自定义的实现。它 panic 时没有任何兜底，panic 穿过
`server.call` → `RemoteTool.Call` → `tool.Toolkit.Call`（tool 层全包无 recover）→ 被
agent_v2 / compose_v2 的外层兜底转成 error → **BasicAgent 整轮 Run 失败**。

这违反本子系统自己声明的契约（`mcp` 包 godoc + CLAUDE.md 速查行）：

> `RemoteTool.Call` **只在调用方 ctx 结束时返回 error**，其余失败（离线、连接出错、超时、
> JSON-RPC 错误、HTTP 错误）都是 `tool.Failure`。

后果正是「Hub 按 server 降级」整个设计要避免的那件事：一个 server 拖垮整个 Run。

**[复现]** 探针（`memClient.onCall` 里 panic，`WithClient` 固定实例）与对照组同跑：

| 路径 | panic 结果 |
|---|---|
| `CallTool` panic | ❌ **冲出 `RemoteTool.Call`** |
| 工厂 / `Initialize` panic（对照） | ✅ 兜住，转成连接失败 + `panicerr.Log` 带栈 |

**为什么不是过度防御**：判据不是「看到没有显式防御」，而是这一层**已经决定要兜业务 Client
的 panic**——`connect`、`closeQuietly`、`Hub.Close` 三处都兜了，`connect` 的注释明写「不让
goroutine panic 带走整个进程」。漏的恰好是唯一一条声明「任何失败都不返 error」的路径。

**修法方向**（未实施）：照 `connect` 的形状给 `c.CallTool` 包 `defer recover` →
`tool.Failure(tool.ErrCodeInternal, …)` + `panicerr.Log`。要不要顺带 `markOffline` 请作者定
——panic 不代表连接坏，倾向不标。

**修复**：`server.callClient` 兜住 `c.CallTool` 的 panic → `tool.Failure(tool.ErrCodeInternal, …)` +
`panicerr.Log`（照 `connect` 的形状），**不标掉线**（panic 是调用方代码的 bug，连接可能好着，标掉线会把
一个工具的 bug 升级成整个 server 的降级）。哨兵 `TestHub_ClientCallToolPanic_IsFailureNotPanic` 另断言
「panic 后同 server 的另一件工具照常调得通、状态仍是 online」。sanity-revert（去掉 recover）红，失败
信息 `panic 冲出了 RemoteTool.Call：业务 Client 炸了（契约要求转成 tool.Failure）`。

---

## H-02 [MED] 软降落轮：provider 不守 `none` 时一个收尾答复都不交付，且错误正文丢掉语境

**位置**：[`agent_v2/basic_agent.go:2801`](../../agent_v2/basic_agent.go)（none 分支）、
[`:3224`](../../agent_v2/basic_agent.go)（执行闸）、
[`agent_v2/basic_builder.go:1230`](../../agent_v2/basic_builder.go)（`WithMaxVisit(maxRounds + 1)`）、
[`agent_v2/basic_agent.go:327`](../../agent_v2/basic_agent.go) 与 `:481`（友好正文）

**问题**
commit `b3583bd` 把软降落从「摘空工具集」改成「工具照旧 + `tool_choice=none`」。provider 或
业务自写 `ChatModel` 接受但**不强制执行** none 时（代码注释自己点名「业务自己的 ChatModel
也未必认它」，而 commit message 承认 qwen / deepseek 未用真 key 验证，qwen 正是本仓默认测试
模型）：工具照旧绑着 ⟹ 模型照调 ⟹ 整批被 `:3224` 拦成失败结果 ⟹ 条件边判「末条有
tool_call」回 model 节点 ⟹ 顶层撞引擎 `maxVisit`、续跑撞新闸 ⟹ **图失败**。

图失败 ⟹ postamble 不跑 ⟹ 没有 ExitEvent、没有 `commitFinal`、没有第 4 层 StoreMemory 的
`store.Put`，`FinalMessage(events) == nil`。**软降落的全部承诺（「模型得到一次收尾机会」）
现在完全押在 provider 守不守 none 上，没有任何兜底。**

**[实测+环节核实]** agent 用「请求里有工具就调、没工具才产文本」的探针模型（这才是「不守
none」的真实形态——provider 调不出请求里没有的工具）对照两棵代码树：

| | 每轮绑的工具 | 工具实际执行 | `FinalMessage` | err 正文 |
|---|---|---|---|---|
| 旧行为（摘工具） | `[[noop][noop][]]` | 2 | `"收尾答复"` | `basic agent "probe2": 轮次预算耗尽…（答复在 events 里，用 FinalMessage 取）` |
| HEAD | `[[noop][noop][noop]]` | 2 | **`nil`** | `node "model" visited 4 times (max 3)` |

两边副作用次数相同（执行闸不是差异来源），差异纯在「有没有收尾答复 + 正文说不说人话」。

主审核实的关键环节：`basic_agent.go:327` 那段友好正文的前置确实是 **`err == nil`**，所以这
条路上（err ≠ nil）它**永不生效**，运维看到的是裸引擎消息、一个字没提软降落，排查方向指不
出去。`maxVisit = maxRounds + 1` 也确实没给「整批被拦之后再来一次真正无工具的轮次」留格子。

**同根的第二个表现**：整批被拦的那一批里，业务 `OnToolStart` 里的 HITL 确认照常问用户（注释
已声明「会被问到这次调用」），用户答完只换来 `MaxIterationsError` + 无答复——CLAUDE.md 自己
标的「人会被问一个注定答不成的问题」反模式。既有用例
`TestSoftLanding_HardToolCallNotExecutedAfterResume` 正是这个形态，但它把它当 fixture。

**修法方向**（未实施，由作者定）：整批被拦（**没有任何工具真执行**）时走出口边收尾，或留
一格无工具轮；至少把「这一批被拦了」的事实 wrap 进错误正文。

**修复（只修了可观测性那半）**：Run 与 Resume 两个出口的判据从 `err == nil` 放宽到「也认引擎抛的
`MaxIterationsError`」，这条路上的正文改成点出**软降落轮 + 模型仍硬调了工具 + `round_budget_exhausted`
+ FinalMessage 可能为 nil**（文案与成功那条**有意不同**：那条说「答复在 events 里」，这条不能照抄）。
两个出口一起改——只改 Run 就是本篇「贯穿的模式」批评的那个形状。哨兵
`TestSoftLanding_HardToolCallErrorMentionsSoftLanding`，sanity-revert 红且直接打出那条裸引擎消息
`node "model" visited 4 times (max 3)`。

**行为改动未实施**（留一格无工具轮 / 出口边收尾）：需要设计决策，且**曝光面取决于 qwen / deepseek 到底
强制不强制执行 none**（至今只核过官方文档）。已登记 [backlog](../code/todo.md)，建议先实测再定方向。

---

## H-03 [MED] 软降落轮：「只放行框架那件 `final_output`」零哨兵

**位置**：[`agent_v2/basic_agent.go:3224`](../../agent_v2/basic_agent.go)

**问题**
执行闸用 [`isFrameworkFinalOutput`](../../agent_v2/structured_output.go)（按实例认）放行框架
那件 `final_output`。这条决策承重，但**没有任何哨兵**。

**[复现]** 把它换成同文件下一行就在用的同形状 helper
[`isFrameworkMintedIn`](../../agent_v2/framework_tool.go)（放行任何框架工具）
→ `go test ./agent_v2/` **全绿**（3.19s 全量）。

这不是臆想的误改：闸上下几段注释反复讲「判的是实例不是名字」，而 `isFrameworkMintedIn` 正是
这个文件里同形状的常用 helper。

**后果**（agent 实测）：变异下 `WithAskUser()` + `maxRounds=1` + 不认 none 的模型硬调
`ask_user` ⟹ `ask_user` **执行** ⟹ Run 停在提问；用户答完 Resume ⟹
`轮次预算 1 轮已用完、软降落轮也已用过…` + `FinalMessage = nil`。**又一个注定答不成的问题。**

**既有覆盖为什么钉不住**：`TestSoftLanding_BusinessFinalOutputWithoutSchemaNotExempt` 钉的是
「业务同名 `final_output` 不豁免」——业务工具本来就不是 minted，这条变异下它照样被拦，用例照
绿。全仓 `round_budget_exhausted` 只出现在**一处**断言
（[`agent_v2/soft_landing_test.go:148`](../../agent_v2/soft_landing_test.go)，业务 noop 工具）。
注释给 `transfer_to_agent` / `route_to` 写了整段「为什么不放行」的论证，**一条用例都没有**。

**缺的哨兵**：软降落轮硬调 `ask_user` / `transfer_to_agent` / `route_to` / `tool_search` 时，
断言「工具体执行 0 次 **且** 不中断」——按 CLAUDE.md「验泄漏要断言工具有没有真执行，不是有
没有中断」，这里两条都要，中断那条专抓 `ask_user`。

**修复**：哨兵 `TestSoftLanding_HardCalledFrameworkToolNotExecuted`——`WithAskUser()` + `maxRounds=1` +
不认 none 的模型硬调 `ask_user`，断言「Run 没停在提问上」（`ask_user` 真执行的表现就是抛中断，所以
「不中断」恰好是「没执行」的可观测量）+ 仍以 `MaxIterationsError` 收尾。sanity-revert（换成
`isFrameworkMintedIn`）红，失败信息 `软降落轮硬调的 ask_user 被执行了——Run 停在提问 … 上`。

---

## H-04 [MED] observability 重写：几条承重语义失去哨兵

**位置**：`observability/`（`c0262fe` + `3166ad1`）

**问题**
生产代码 5232 → 2356 行、测试 **7536 → 1709 行（降 77%）**。删掉的大部分**删对了**——自建
exporter / OTLP 批量导出 / 采样 / Resource 校验 / 自建 trace-ID 生成 / 自建 W3C 解析 / 自建
metric registry 都交给了 OTel SDK 与 `sdkmetric.NewView`，`stale_span_test.go` 钉的「span 泄漏
在 per-session map 里」那一类结构上已不存在（span 只走 ctx）。

但有几条**框架自己的语义**仍然承重，哨兵没了。下列变异后 `go test ./observability/` 全绿
（标 **[复现]** 的是主审独立重做过的；其余 4 条为审计 agent 实测，主审只核实了语义仍在代码里
承重、未独立重跑变异）：

| 变异 | 位置 | 失败场景 |
|---|---|---|
| **[复现]** 删 `lastAssistantMessage` 里的 `!isUnresolvedCallsRecord(m)` | [`observability/content.go:245`](../../observability/content.go) | agent span 的 `gen_ai.output.messages` 上报的「最终答复」是一句框架中文记账（`describeUnresolvedCalls` 合成的那条是「assistant + 零 tool_call」，长得正像最终答复）。出事的恰好是模型没产出就退出的错误路径——正是有人看 trace 排障的时候。被删的 `last_assistant_internal_test.go` 注释原话：「把这里的跳过 revert 掉，全仓 `go test ./...` 照样全绿」。CLAUDE.md 点名 observability 是仍需 [`agent_v2.IsUnresolvedCallsRecord`](../../agent_v2/basic_agent.go) 这个谓词的 2 处之一 |
| **[复现]** `text()` 改成先截断再脱敏 | [`observability/content.go:36`](../../observability/content.go) | 手机号 / API key 跨在 `WithMaxContentChars` 截断点上 → 正则匹配不到 → **前半段明文进 `gen_ai.input.messages`**。姊妹路径 `errText` **有**哨兵（`TestErrorMessage_RedactedBeforeTruncated`），内容路径没有 |
| **[复现]** 放开压缩事件的 `a.Type != CompactionActionAfter` 闸门 | [`observability/orch_hooks.go:264`](../../observability/orch_hooks.go) | `crew.compactions` 计数静默翻倍。老 suite 对事件→指标映射有 6 条专钉，新 suite 对 `MetricCompactions` / `MetricCheckpointOps` / `MetricPlanReplans` / `MetricRouteDecisions` / `MetricFailovers` 各 0 处断言 |
| **[复现]** 去掉 `ErrCheckpointNotPersisted` 排除 | [`observability/handle.go:195`](../../observability/handle.go) | 出口写档失败的中断被记成 `blocked` + span 不标 Error ⟹ 运维看到的跟正常等人答复一模一样；连带多计一次 `crew.interrupts` 并留下永不被取走的挂起记录（`PendingInterrupts()` 永久 +1） |
| 按字节切的 `truncate` | [`observability/content.go:21`](../../observability/content.go) | 中文内容超限 → span 属性含非法 UTF-8 → OTLP 后端丢属性 / 整条 span。现有 `TestContent_Truncated` 只数「长」的出现次数，字节切照过。老 suite 两条专钉（`TestP26Audit_RenderContentUTF8Boundary` 等）全删 |
| `inst := ""`（扇出实例维） | `observability/orch_hooks.go` | 同节点多实例扇出时 trace 上分不出哪条是哪个；嵌套同名节点时第二个实例的 span 被判「已开过」整个不建 |
| `StartA2AServerSpan` 丢远端父 + 改 `SpanKind` | [`observability/a2a.go`](../../observability/a2a.go) | 跨进程 trace 断成两棵树。inject / extract 那半**有**保护（`TestPropagation_RoundTrip`），消费 extract 结果的那半没有——两端只钉了一端 |
| `crew.tool.category` 白名单放开 | `observability/agent_hooks.go` | 业务 `tool.Categorizer` 返任意字符串 → 指标 / 属性基数无上限 |

**修复**：补了 4 条哨兵，覆盖上表前 5 条语义（每条都做过 sanity-revert）：
`TestLastAssistant_SkipsUnresolvedCallsRecord` / `TestContent_RedactedBeforeTruncated`（同时钉「先脱敏再
截断」与「截断不切坏字符」两条）/ `TestCompactionMetric_OnlyAfterCounts` /
`TestInterrupt_CheckpointNotPersisted_IsErrorNotBlocked`（连带断言「不留下永不被取走的挂起记录」，
sanity-revert 下 `PendingInterrupts()=1`）。

⚠️ **「截断不切坏字符」那条断言的形式是实测定下来的**，两种看起来更自然的写法都**不承重**：
`utf8.ValidString(in)`（内容经 json 编码进属性，编码结果永远合法）与
`strings.ContainsRune(in, utf8.RuneError)`（json 把非法字节转义成**字面六个字符** `\ufffd`，按码点找不到）
——都是写完做变异才发现的，正是 CLAUDE.md「断言选在不承重的维度上等于没测」。

**未修**：`crew.node.instance` / `StartA2AServerSpan` / `crew.tool.category` 三条已登记
[backlog](../code/todo.md)。

**已核实有保护的**（变异即红，不必补）：「取消只按调用方 ctx 判」（立论最硬的那条）、
`crew.agent.active` 的 +1/−1 配平（4 条独立用例断言 `sum == 0`）、JSON 先还原 `\uXXXX` 再脱敏、
五档 `crew.outcome` + span status 只在 error / timeout 标 Error、装两层只记一份、句柄按实例区分、
身份属性四项、内容采集默认关、`CleanupSession` / `PendingInterrupts`。

---

## H-05 [MED] `sql` HistoryStore：PG / MySQL 身份列 DDL 零机械防护

**位置**：[`sql/history.go:151`](../../sql/history.go)（PG `owner` / `conv_id` 的
`TEXT COLLATE "C"`）、[`:161`](../../sql/history.go)（MySQL `VARBINARY(1020)`）、
[`sql/schema_test.go:124`](../../sql/schema_test.go)（`TestSetup_IdentityColumnsCompareBytes`）

**问题**
`TestSetup_IdentityColumnsCompareBytes` 是表驱动 sqlmock DDL 哨兵，正则逐字钉
`TEXT COLLATE "C"` / `VARBINARY(n)`。它自己的注释写明了为什么要钉 DDL 本身：

> conformance 的身份逐字节用例只在真 MySQL 与非 C 排序规则的真 PG 上会红，SQLite 本来逐字节、
> sqlmock 不执行 SQL，所以这里钉 DDL 本身。

**[机械对账]** 该表驱动恰好 **4 个 case**（`store/PG`、`store/MySQL`、
`checkpointer/PG`、`checkpointer/MySQL`），**没有 history 的两个**；而
`grep -rn "NewHistoryStore" sql/` 的测试调用点只有
[`sql/history_test.go`](../../sql/history_test.go) 两处、都传 `SQLite{}`
⟹ `historyMigrations` 的 `case Postgres:` / `case MySQL:` 两个分支**在任何测试里都没执行过
一次，连 sqlmock 都没有**。

**失败场景**：有人把 `owner TEXT COLLATE "C"` 改成 `owner TEXT`，或把 `VARBINARY(1020)` 改成
`VARCHAR(255)` → SQLite conformance 全绿（SQLite 默认 BINARY 本来逐字节）、sqlmock 没有这条
用例、PG / MySQL 没跑 → 真上 MySQL 后 `owner="acme "` 与 `"acme"` 落进同一个会话、`"Acme"`
与 `"acme"` 串行（默认 `utf8mb4_0900_ai_ci` 不区分大小写与重音），`err == nil`。

**修法方向**（未实施）：给那个表驱动加两个 case。注意 history/MySQL 只有**一条** migration
（索引内联进 `CREATE TABLE`），所以 `mock.ExpectExec("CREATE INDEX")` 要按 case 可选。

**修复**：表驱动加了 `history/PG` 与 `history/MySQL` 两个 case（现 6 个全过），结构体加 `inlineIndex`
字段让 `CREATE INDEX` 期望按 case 可选——history/MySQL 把索引内联进 `CREATE TABLE`，只有一条 migration。
两条 sanity-revert 都红：PG 去掉 `COLLATE "C"`、MySQL 把 `VARBINARY(1020)` 换成 `VARCHAR(255)`。

**顺带**：`sql/history_test.go` 是全仓**唯一**未过 `gofmt` 的文件（由 `9df7d65` 带入），已 `gofmt -w`。

**附带**：conformance 没有长 key 用例（`agent_v2/history_conformance` 10 条里最长的 key 是
`"acme/eu"`），而 store conformance 有 `Long_Keys_At_Portable_Limit_Do_Not_Collide`。ConvID
常直接来自 wire、业务控制不住它长什么样：超 1020 字节时 MySQL 严格模式整批 Append 失败、PG
到约 2704 字节撞 btree 索引行上限、SQLite 照样成功。历史层的写是旁路（失败吞 + warn），所以
症状是「同一个长 ConvID 在 SQLite 上历史正常、搬到 MySQL 后模型开始忘上一轮」。属
conformance 套 + `HistoryKey` godoc 的缺口（没声明可移植上限），不是这段 DDL 写错了。

---

## H-06 [MED] `62d2547` 的 fix 接线在本仓工具链上零覆盖

**位置**：[`agent_v2/input_digest.go:241`](../../agent_v2/input_digest.go)
（`digestValue` 里 `encodeTree(&buf, tree)` 那段）

**问题**
**[复现]** 把接线换回 `json.Marshal(tree)`（旧行为）→ `go test ./agent_v2/` 全量 **全绿**
（3.1s），两条新哨兵（`TestEncodeTree_MatchesJSONMarshal` /
`TestEncodeTree_DeeperThanJSONLimit`）单独跑也绿。

原因：两条新哨兵都**直接调 `encodeTree`**，变异的是 `digestValue` 的接线、碰不到它们。唯一
会红的分支依赖 go1.27 才有的编码深度限制，而本仓 `go.mod` 是 `go 1.26.0` +
`toolchain go1.26.4`。

**失败场景**：将来有人把 `encodeTree` 换回 / inline 回 `json.Marshal`（或删掉它做「简化」）
→ 本仓任何一次 `go test` 全绿 → 下游**用 go1.27 构建 crew-go 的应用**重新吃到「只在别的
标注上不同的两份输入算出同一个身份」= 工单-1 的批准兑付给工单-2，全程 `err == nil`。

**两点为作者说话**：commit message 已如实披露「go1.26.4 照绿」，不是谎报；`toolchain` 固定了
本模块的构建工具链，所以 fix 是正确的**前瞻性**修复而非空改——受害者是下游 1.27 构建方。

**修法方向**（未实施）：版本无关的接线哨兵做不出来（`walk` 已去环去共享 ⟹ 这棵树上
`json.Marshal` 在 1.26 永不报错，构造不出「两版都失败」的输入）。只剩两条：埋 seam（编码器
提成包级变量或 `digestValueWith(enc)` 供内部测试注入一个深度受限的编码器），或在 `encodeTree`
godoc 明写一句「本函数与 `digestValue` 的接线只在 go1.27+ 才有测试覆盖」。

**修复**（取了 godoc 那条）：`encodeTree` 的 godoc 加了一段，写明「本函数与 `digestValue` 的接线只在
go1.27+ 才有测试覆盖」、为什么做不出版本无关的哨兵、以及**改那一行之前要先想清楚下游用 1.27 构建的应用
会不会重新吃到「只在别的标注上不同的两份输入算出同一个身份」**，并留了埋 seam 的两种做法。

没埋 seam 是权衡：为一条「只在更高版本上才会红」的哨兵给生产代码开一个测试专用注入点，代价比它买到的
防护更大；而这段话守的是**唯一会改到它的那个动作**（把那一行换回 `json.Marshal`）。

---

## M-01 [LOW-MED] `model/anthropic` 的空模型名守卫裸奔

**位置**：[`model/anthropic/request.go:37`](../../model/anthropic/request.go) /
[`model/openai/client.go:274`](../../model/openai/client.go)

**问题**
两处有同款守卫 `if o.Model != nil && *o.Model != ""`（把 `WithModel("")` 当没覆盖，与
`model.RequestConfig` 的取法一致）。**[复现]** 退回 `if o.Model != nil {`：

| 变异 | 结果 |
|---|---|
| anthropic | `ok` —— **全绿，无哨兵** |
| openai（对照） | **FAIL** |

**溯源**：anthropic 的守卫是 `8fb0749` 先加的，`806c3a3` 立规矩时只给 openai 配了哨兵
（`model/openai/model_name_test.go` 的 `TestClient_EmptyModelOverrideIgnored`），**没有回头
钉已存在的 anthropic 入口**。

现状**没有行为 bug**（守卫在，且 Generate / Stream 共用 `buildRequest` 这一处），缺的是防
回归。失败场景：以后有人改 `buildRequest` 的模型名解析，`model.WithModel("")`（业务
`WithRunModelOptions` 里条件拼选项、条件不成立时留空串是常见写法）会发出 `"model": ""` →
Anthropic 400，而 `ModelInput.Config` 报的仍是构造期模型名，观测上看不出来。

**修法方向**（未实施）：`model/anthropic/request_test.go` 的 `TestCallOptions` 加一例，抄
openai 那个用例的双断言形状。

**修复**：`model/anthropic/model_name_test.go` 加 `TestClient_EmptyModelOverrideIgnored`（与 openai 同名
哨兵对称）：空覆盖发构造期模型名、非空覆盖发新的、`model.RequestConfig` 报的与实际发的一致。
⚠️ 必须**自建带模型名的 client**——helpers 里的 `newClient` 构造期模型名是空串，拿它验不出空覆盖与非空
覆盖的差别。sanity-revert 红：`空覆盖应发构造期的 base-model，got ""`。

---

## M-02 [LOW] `fallback.ModelName` 在「所有候选的门都关着」时报的不是实际起手的那个

**位置**：[`fallback/fallback.go:491`](../../fallback/fallback.go)

**问题**
agent 探针实测（首选 `Retry-After=30m`、备选 `2m`，两个门都关上）：`ModelName()` 返首选，
而下一次 `Generate` 走 `tryMembers` → `nextMember` 的「都要等时，等最早能再请求的那个」
⟹ 起手请求的是**备选**。`ModelName` 的 for 循环没有复刻那一档，直接取 `f.members[0]`。

**判为已知取舍、不计为 bug**：该函数 godoc 明写「都不满足时是第一个候选」+「是尽力而为的
预估」，且 `ModelInput.Config` 是 CLAUDE.md 定义的 **[只读]** 观测档。

**但值得重审规则本身**：`806c3a3` 已经是这条规则的第二个反例（它修的是「只看断路器是否熔断」
导致门关着 / 半开探测期两种都报错），这是**第三个**。按 CLAUDE.md「同一处规则被审出第二个
反例时停下来重审规则本身，别补第三个分支」——正解是让 `ModelName` 复用
`nextMember(states, now)`，而不是维护第二份选法（否则这是「必须与 X 保持一致 = 设计债自白」
的新拷贝：`tryMembers` 的挑法再改一次，这里又会漂）。

**修复**（按「重审规则本身」那条走的）：`ModelName` 不再自己写选法，改为 `startIndex(now)` **复用
`nextMember`**——它挑、门与断路器按 `tryMembers` 的同一套分档筛、筛掉的置 `done` / `deferred` 再挑下一个；
`nextMember` 返回 `wait > 0` 时（都要等）那个就是起手的，这正是旧版漏掉的一档。唯一差别是不判调用方期限
（需要 ctx，而 `model.ModelNamer` 不带），已写进 godoc。

哨兵 `TestFallback_ModelName_AllGatesClosed`（首选 Retry-After=30m、备选 2m，用短 deadline 让它不真睡）。
**sanity-revert 的结果最有说服力**：把 `startIndex` 换回「按顺序找第一个门开着且断路器放行的、都不满足
取 members[0]」，**只有这条新哨兵红**（`应报 qwen-plus，got "qwen-max"`），另外 3 条 ModelName 用例全过
——精确印证了「旧哨兵钉不住这个分叉」。

---

## M-03 [LOW] 两处活文档教已删除的 `observability.New` 旧签名

**问题**
新签名是 `func New(opts ...Option) *Observability`（**单返回值，无 error**，参数是变长
`Option` 不是 `Config`；`Config` 类型已删）。两处活文档还教旧签名，照抄编译不过：

| 位置 | 原文 |
|---|---|
| [`doc/v2/context-compaction.md:902`](../v2/context-compaction.md) | `//   obs := observability.New(cfg)` |
| [`doc/v2/fault-analysis-system.md:472`](../v2/fault-analysis-system.md) | `obs, err := observability.New(cfg)   // 注意：返 (*Observability, error)` |
| [`doc/v2/fault-analysis-system.md:680`](../v2/fault-analysis-system.md) | `observability.New(cfg Config) (*Observability, error)   // 返两值` |

`context-compaction.md` **这次改过却漏了这一行**，典型的「改了一处忘了另一处」。
`fault-analysis-system.md` 一行没改（该文自称「未实现的设计提案」，但 `:680` 是 API 签名速查
表，正是会被抄的地方）。

两处机械层按设计抓不到：`commentaudit` 只扫本次改动文件（`fault-analysis-system.md` 不在扫描面内），
且 `cfg` 不是反引号包裹的符号。权威文档 [`doc/v2/observability.md`](../v2/observability.md) 是对的
（`c0262fe` 新建，同时删掉了旧的 `observability-implementation-guide.md`）。

**修复**：三处都改成新签名（`New()` / `New(opts ...Option) *Observability`）。上表引的是**修前原文**，
作为审计记录保留。

---

## 贯穿的模式：规矩立在 A 入口、B 入口没跟上

**H-03 / H-05 / H-06 / M-01 是同一形状**，正是 CLAUDE.md「立一条新规矩，同一次就要问『哪几
条路能到这儿』」：

| finding | 规矩立在 | 漏掉的入口 |
|---|---|---|
| M-01 | openai 的空模型名哨兵 | anthropic（同款守卫先存在） |
| H-05 | store / checkpointer 的身份列 DDL 哨兵 | history（新表） |
| H-06 | `encodeTree` 本体的两条哨兵 | 它与 `digestValue` 的接线 |
| H-03 | 注释里给 `transfer_to_agent` / `route_to` 写的整段论证 | 一条用例都没有 |

本仓已有根治范式（`TestMemberDispatch_AllAgentRunsGoThroughHelper` /
`TestHandoffGrant_EveryReadIsSelfChecked` 那类机械对账哨兵）。**建议把这几处收成同款机械
对账，而不是逐个补用例**——逐个补用例下一轮还会漏第五个。

**流程观察**：commit message 里**没有验证段**的有 4 个——`c6e3058`（ModelNamer，21 文件）、
`c0262fe`（observability 重写，239 文件、删 7536 行测试）、`008d63e`（清理 spec，63 文件）与 merge
`bc485b5`；`3166ad1` 只有一行。其中 `c6e3058` 之后 **15 分 44 秒**（15:46:11 → 16:01:55）就有
`806c3a3` 修它：一个它引入的 bug（`fallback.ModelName` 判据取错）+ 一处它造出来的不一致（新加的
`model.RequestConfig` 与 openai 客户端对 `WithModel("")` 的取法不同；那个 `if o.Model != nil` 本身
在 `c6e3058^` 就已如此）。

按 CLAUDE.md 的 commit-level audit 触发规则，「破坏性 API / 默认行为变」属于「commit 后立即 audit」
那一档——`c0262fe` 带 `!` 标记、删掉 77% 的测试，正是最该被这条规则点到的那个，而 H-04 的 8 处缺口
也确实全在它里面。

⚠️ 初稿把 `c6e3058` 写成「唯一没有验证段的」且时间差写成「26 分钟」，两处都错，已更正
（文档审计 E2 / E3）。

---

## 已核实没问题（逐条带变异 / 探针证据，不再展开）

- **mcp 的并发与生命周期**（除 H-01）：gen（连接代数）两个方向、锁序无环（`h.mu → s.mu →
  q.mu`）、goroutine 生命周期（`bg.Add` happens-before `Wait`、`goOnline` 恒在 `bg.Done` 之前）、
  `cloneSpecs` / `cloneAnnotations` / `cloneValue` 真切断共享、串行语义（排队与执行各一份时限）、
  HTTP/2 不谎称「没有发出」、固定实例不标掉线、工具集 Start 后不变、状态回调里可调 `Close`
  ——逐条变异后既有哨兵都红且失败信息精确。另有 2 条「已实现、载重、但无哨兵」：`goOnline` 的
  `s.closed` 早退（缺它会泄漏一个永不关闭的新连接）、`markOffline` 的 `bg.Add(1)` 在 `s.mu` 内
  （挪出去 `-race` 直接报竞争）。
- **软降落的其余 9 个方向**：工具只有一个派发点（`grep -rn "\.Call(ctx\|\.Call(callCtx" agent_v2/ | grep -v _test`
  仅 1 命中，在 `basic_agent.go`，无旁路）、return-direct 不被绕过、`withFinalRound` 每轮都推（含 false，
  两条变异都让既有哨兵变红）、审批次序无危害面、memo 与续跑一致、`tool_choice` 覆盖次序
  （none 排最后必赢，`TestSoftLanding_ForcedToolChoiceOverriddenToNone` 钉住
  `[required nil none]`）、名字 vs 实例不分叉、「至多一个软降落轮」各档判据正确、
  `model/openai/client.go` 那 4 行是纯注释改动。
- **anthropic 契约**：流式完整性（没收到 `message_stop` / refusal / `model_context_window_exceeded`
  / ctx 取消 各自变异后哨兵都红）、`APIError` + `Retry-After` 回填、thinking 签名回放（无快照
  标注时丢 `ReasoningContent` 是**对的**——无签名的 thinking 块 API 会拒）、`WithAllowedTools`
  裁 `wireTools`、`WithExtraBody` 走 RFC 7386 merge patch。
- **`ModelNamer` 可选接口各路径**：不实现它的成员 → `ModelName()` 返空串；空串**不会**被当
  成真模型名发出去（`observability/agent_hooks.go` 四处都守了非空）；`WithTools` 派生物仍满足
  该接口且与原件口径一致；`ModelInput.Config` 只有一个生产构造点。
- **输入摘要（`62d2547`）fix 本身正确**：20000 棵随机树上 `encodeTree` 与 `json.Marshal` 逐
  字节相同；**没有深度截断** ⟹「截断点之后才不同的两份输入撞摘要」不成立（depth 9999 /
  10001 / 20000 / 100000 四档实跑，只在最深叶子不同的两份全部算出不同摘要）；性能不变量还在
  （共享 DAG 深 24 即 2^24 条路径耗时 69µs，靠 `walk` 的 `visit` 把重复容器换成 `ref:N` 坏
  叶子 ⟹ `encodeTree` 拿到的是真树不是 DAG）。
- **`sql` HistoryStore 的 10 条 conformance 在 SQLite 上逐条实跑 PASS**；对着两份 DDL 逐条
  过，append-only（无 UNIQUE / 无 UPSERT）、顺序（`ORDER BY id` 配 `BIGSERIAL` /
  `AUTO_INCREMENT` + 批内逐行 INSERT）、两维 key 不拼串、整批一个事务、`Append` 不改入参、
  `Load` 返快照 在 PG / MySQL 上**也都成立**——真缺口是 H-05 的 DDL 哨兵，不是语义。
  ⚠️ 但 `Concurrent` 在 SQLite 上通过**不构成任何并发保证**（`sql/sqlite_testing.go` 的
  `SetMaxOpenConns(1)` 把它整条串行化了）。

---

## 未覆盖 / 下一轮

1. **真 provider 实测**（最高优先）：`tool_choice=none` 在 qwen / deepseek 上强制不强制
   ——H-02 的曝光面取决于它；`example/model/anthropic_smoke` 的真机跑。
2. `a2a` / `agui` 两侧对 `round_budget_exhausted` 失败结果与裸引擎 `maxVisit` 错误的呈现。
3. 流式（`Stream`）路径下软降落的端到端（本轮探针走的是 `Generate`）。
4. observability 的 span 拓扑 / 父子关系在嵌套编排下的完整对照（本轮只验了几条点）。

**不承诺「已扫干净」**——本轮 5 视角并发 + 交叉验证后仍找到 1 个行为 bug 与 6 处哨兵缺口；
按 CLAUDE.md 的经验值，命中数几何递减但非 0。
