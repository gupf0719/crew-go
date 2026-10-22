# 超大工具输出转存：WithToolResultSpill

> **一句话**：业务工具的结果超过阈值（默认 50000 字节）时，这一批调用收尾时（`OnToolEnd` 之后）就把全文存进
> `store.Store`，消息流里只留一段说明 + 头尾预览 + 取回 id；模型需要时用 `read_tool_output` 按行分段读或按正则搜。不依赖
> 压缩触发；被压缩掉之后取回 id 仍留在摘要里。

---

## 0. 实现状态（2026-10 落地）

| 件 | 位置 |
|---|---|
| 配置、转存、取回工具 | [`agent_v2/tool_spill.go`](../../agent_v2/tool_spill.go)（`ToolResultSpillConfig` / `spillToolResult` / `read_tool_output` / `conversationScope`） |
| option | `agent_v2.WithToolResultSpill(cfg)`；`Config.ToolResultSpill`（[`agent_v2/config.go`](../../agent_v2/config.go)） |
| 集成 | 结果渲染 [`agent_v2/tool_render.go`](../../agent_v2/tool_render.go)：`renderToolContent` 渲染、标「待处理」，tools 节点在 `OnToolEnd` 与批内裁决之后用 `finishPendingToolResult` 按大小处理（[`agent_v2/basic_builder.go`](../../agent_v2/basic_builder.go)）；后台任务结束时当场处理（同一个 `sizeToolContent`）；`withFrameworkTools` 注入 `read_tool_output`（[`agent_v2/basic_agent.go`](../../agent_v2/basic_agent.go)）；MicroCompact 清理已转存结果时保留取回 id（[`agent_v2/microcompact.go`](../../agent_v2/microcompact.go)）；AutoCompact 摘要与 ReactiveCompact 紧急丢组时留下取回 id 的索引（[`agent_v2/autocompact.go`](../../agent_v2/autocompact.go) / [`agent_v2/reactive_compact.go`](../../agent_v2/reactive_compact.go)） |
| 测试 | [`tool_spill_test.go`](../../agent_v2/tool_spill_test.go) / [`tool_spill_regress_test.go`](../../agent_v2/tool_spill_regress_test.go) / [`tool_spill_compact_test.go`](../../agent_v2/tool_spill_compact_test.go) / [`tool_spill_internal_test.go`](../../agent_v2/tool_spill_internal_test.go) / [`tool_spill_read_internal_test.go`](../../agent_v2/tool_spill_read_internal_test.go)；真 qwen 冒烟 [`example/harness/spill_smoke`](../../example/harness/spill_smoke/main.go) |

仅对 **BasicAgent** 生效。

---

## 1. 背景：一条大输出要付很多轮的钱

一条几百 KB 的工具结果进了消息流，就跟着进每一轮的 prompt、每份存档、会话历史，直到某一层压缩把它清掉。原有两处手段
都不解决：

- **源头截断**（`WithToolResultMaxBytes`）把超出部分丢掉，取不回；
- **MicroCompact 的卸载**只在它清旧结果时才发生——那之前的每一轮都在为这条结果付费，而它清掉的时候往往已经撑爆过一次
  上下文；取回工具还得业务自己注册。

产生时转存是追加式的：消息流里从一开始就只有预览，之后每一轮、每份存档、会话历史都只带这个预览，已经发生过的前缀也不会
变（不破坏 prompt 缓存）。（「产生时」指这一批调用收尾、结果写进消息流之前，见 §2.1。）

---

## 2. 设计

### 2.1 什么时候转存、留下什么

- **何时**：业务工具的结果渲染出来的正文超过 `Threshold` 字节——成功的 `res.Output`，失败（`tool.Failure`）的 message 部分
  （`[code]` 前缀照留，模型照样看得出是失败；构建、测试日志正是大输出的常见来源，它们往往以失败返回）。框架铸的工具
  （`read_tool_output` 自己、`final_output`、`activate_skill`……）与 return-direct 结果豁免——前者的输出是结构化的或本身
  有上限，后者产出即答复、不进消息流。
- **在 `OnToolEnd` 之后**：runOne 只渲染、给这条 tool_result 标「待处理」；tools 节点在这一批的 `OnToolEnd` 与批内裁决
  （同一批里给出不同决定的被换成失败结果）之后，按钩子之后的成败与错误码统一按大小处理。所以钩子看到、改写的是全文，
  **存进 store 的是钩子改写之后的那份**——结果脱敏放在 `OnToolEnd` 里，模型用 `read_tool_output` 读回的也是脱敏之后的；
  钩子把结果改大了也照样转存。消息上的失败标注跟着钩子之后的结论走（改成成功就去掉、改了说明就换成改写之后的），
  它们随消息进存档与会话历史，留着钩子之前那份的话脱敏对它们不生效。后台任务结束时没有钩子这一步，当场处理（脱敏
  放在工具中间件里，见 [background-tasks.md](background-tasks.md) §2.5）。
- **留下什么**：一段说明（工具名、多少字节、多少行、怎么取回）+ 头尾预览（头 4 : 尾 1，共 `PreviewBytes` 字节）。行数跟
  取回工具同一口径（超长的行折成多行算，§2.3）。消息上盖 `ToolResultSpillAnnotation`，值是取回 id。
- **取回 id 按内容定**：`sp_` + 内容 SHA-256 的前 8 字节（16 位十六进制）。同一份内容在同一个会话里只存一份——续跑重做
  这一批时内容没变就是同一条记录，不留孤儿；不同会话的命名空间不同，同一个 id 读不到别人的。
- **跟 `ToolResultMaxBytes` 同时配**：先判转存，预览再按 `ToolResultMaxBytes` 截（预览通常远小于它）；转存不了时退路是
  截断到转存阈值，原始大小照样记在 `TruncatedOriginalBytesAnnotation` 上给后面的压缩层看。
- **代价**：同一批里别的调用中断时，这一批没有收尾，这次调用的全文随工具账进存档一次（续跑时命中工具账、照常转存）。
  这一批处理完之后，工具账里的结果换成处理之后的那份、定下（见 §2.6）：工具账在一次 Run 里不回收，留着全文的话每一轮
  的全文都留在内存里——实测 20 轮 × 2MB 输出多占 80MB，定下之后 0.1MB。

### 2.2 存在哪：按调用方身份与会话分命名空间

存进 `ToolResultSpillConfig.Store`，没给时用 ctx 里注入的 store（`WithStore` / `store.WithContext`）。命名空间是
`("_tool_spill", "o"+hash(调用方身份), "c"+hash(会话 ID))`，没有会话 ID 时第三段是 `"t"+hash(任务 RunID)`。两段都按哈希
入：会话 ID、调用方身份来自外部，可能含命名空间不允许的字符，也不该原样出现在存储键里。调用方身份取
`compose_v2.GetOwner(ctx)`；ctx 里没有调用主体时取这次会话历史的归属（`WithHistory` 的 owner 或 `WithHistoryOwnerFunc`
按请求解析出来的那个）——两个租户用了同一个会话 ID 时也不落进同一个命名空间。

`read_tool_output` 用同一个函数（`conversationScope`，后台任务也用它）算命名空间，所以**只能读到当前会话（任务）转存的
内容**——别的会话拿着 id 读到的是「没找到」。id 会出现在模型可见的文本与会话历史里，不能靠它保密。

### 2.3 取回：read_tool_output

框架铸的工具（`ReadToolOutputToolName = "read_tool_output"`），开了转存时由框架每次 Run 注入、不写进业务的 toolkit；
业务 toolkit 里有同名工具时框架那件生效、构造期告警。

| 参数 | 说明 |
|---|---|
| `id` | 必填，转存说明里给出的那个 |
| `offset` | 从第几行开始（从 1 数），默认 1；超出总行数返回 `out_of_range` |
| `limit` | 最多读多少行，默认 200，上限 2000 |
| `pattern` | 只返回匹配这个正则（RE2）的行；配合 `offset` 从某行之后接着搜。非法正则返回 `invalid_arguments` |

- 按换行切行；超过 2000 字节的行在字符边界上折成多行、各自编号（单行的大 JSON 也能分段读到）。
- 每行带行号（`1500| …`，折行之后的编号，跟说明里的行数同一口径）；每次最多返回 16000 字节（抬头与结尾也算在内），读不完时说明「后面还有 N 行，接着读用
  offset=…」；按 pattern 搜时结尾给出扫到了哪一行，搜到末尾就说「后面没有更多匹配的行」，不让模型接着搜。
- `offset` / `limit` 写成字符串（`"200"`）也认。
- 它自己的输出不再被转存。

### 2.4 写失败 / 没有 store

退化成截断到阈值（头尾保留），说明里写明原始大小与「这次未能转存，无法取回全文」，**不留取回 id**——留一个取不回来的 id
只会让模型白调一次。没有 store 时只告警一次。

### 2.5 生命周期

配了 `TTL` 且后端支持 TTL（`store.TTLAware`）就按 TTL 过期；否则不过期，业务按命名空间前缀 `_tool_spill` 自己清。会话
结束时**不删**：续跑、回看还在引用它。过期之后会话历史里的预览还在，取回会报「没找到」。

### 2.6 跟别的机制的关系

- **工具账（续跑不重放）**：工具账在每个调用成功那一刻记下的是转存之前、带「待处理」标注的那份；这一批没处理完就作废
  （同批别的调用中断或出错）时，续跑命中工具账、不重跑工具，照常走 `OnToolEnd` 与按大小处理——内容不变，取回 id 相同
  （按内容定），store 里还是同一条。这一批处理完（`OnToolEnd`、批内裁决、按大小处理）之后结果就定下了：这次执行之后
  在提交之前作废（节点成功之后的钩子 Goexit、提交出错）时，续跑不再处理这几个调用——`OnToolStart` / `OnToolEnd` 都不
  触发，交出定下的那份与钩子当时做出的转交 / 路由决定。再交给钩子的话它看到的是转存说明不是原文，按全文判断的钩子会
  把成功翻成失败（审计实测）。规矩跟配没配转存无关，见 [hitl-resume.md](hitl-resume.md) §6.5。
- **会话历史**：记的是预览，不是全文——长会话的历史不再被一条大输出撑大，下一次 Run 读回来的也只是预览。
- **MicroCompact**：清理产生时就转存过的结果时只清掉预览、留下取回方式（`[较早的工具输出已清理；全文仍可用
  read_tool_output 取回，id="…"]`），不把转存说明再卸载一遍（那样模型要先读回说明才拿得到 id）。
- **AutoCompact / ReactiveCompact**：被摘要、被紧急丢掉的消息里的取回 id 收成一份索引（每条是 id、工具名、大小、行数，
  去重后留最近 50 条）——AutoCompact 把它附在摘要后面、并记在摘要的标注上（下一次增量摘要接着带）；ReactiveCompact
  紧急丢组时补一条索引消息。不留的话全文还在 store 里，模型再也取不回。AutoCompact 摘要后的文件重注入不把转存过的
  结果当文件内容（它的正文只是说明 + 预览）。
- **后台任务**（[background-tasks.md](background-tasks.md)）：任务结束时用同一份 `sizeToolContent`，超过阈值的结果同样
  转存，通知里带取回 id。

---

## 3. 公开 API

| 符号 | 说明 |
|---|---|
| `WithToolResultSpill(cfg *ToolResultSpillConfig) Option` | 开启；cfg 为 nil 时全用默认值 |
| `ToolResultSpillConfig{Threshold, PreviewBytes, Store, TTL}` | `Threshold` 默认 50000；`PreviewBytes` 默认 2000（大于 `Threshold` 时按 `Threshold`）；`Store` 为 nil 时用 ctx 里的；`TTL` 为 0 不过期 |
| `ReadToolOutputToolName` | `"read_tool_output"` |
| `ToolResultSpillAnnotation` | 被转存的那条 tool_result 上的标注，值是取回 id |

## 4. 用法

```go
ag := agent_v2.NewBasicAgent(
    agent_v2.WithName("ops"),
    agent_v2.WithModel(m),
    agent_v2.WithToolkit(tk),
    agent_v2.WithStore(st),            // 转存到这里
    agent_v2.WithToolResultSpill(nil), // nil = 全用默认值
)
```

真 qwen 跑通的例子见 `example/harness/spill_smoke`：工具吐出 344010 字节的订单清单，模型看到的工具结果最大 2345 字节，
读懂说明后调 `read_tool_output` 取回、答对了其中一单的备注。

## 5. 不变量与哨兵

| 不变量 | 哨兵 |
|---|---|
| 超阈值的结果在消息流里有界，且原文任意一段读得回；失败结果同样 | `TestToolResultSpill_LargeOutputSpilledAndReadable` / `TestToolResultSpill_PatternSearch` / `TestToolResultSpill_LargeFailureSpilled`（失败的不处理） |
| 存进去的是 `OnToolEnd` 改写之后的那份；钩子把结果改大、改成大失败也照样处理；消息上的失败标注跟着钩子之后的结论走 | `TestToolResultSpill_RedactHookAppliesToRetrievableText`（钩子之前就转存 / 不处理待处理结果）/ `TestToolResultSpill_RedactHookNoSpillBaseline` / `TestToolResultSpill_HookEnlargesResult_Spilled` / `TestToolResultSpill_HookTurnsIntoBigFailure_Spilled` / `TestToolResultSpill_HookTurnsBigFailureIntoSuccess_ClearsFailureMarker` / `TestToolResultSpill_HookRedactsBigFailure_FailureMarkerRedacted`（失败标注留着钩子之前那份） |
| 被批内裁决换掉的大结果照样处理，错误码是替换之后的 | `TestToolResultSpill_SupersededBigResult_Spilled` |
| 一批处理完之后工具账里不留全文；续跑不再处理定下的那份，结论不翻转 | `TestToolResultSpill_MemoDoesNotRetainFullOutput`（变异：不定下）/ `TestToolMemo_FixedAfterProcessing_HookVerdictDoesNotFlip`（变异：定下的照样交给钩子） |
| 只读得到当前会话（任务）转存的；ctx 里没有调用主体时按会话历史的归属分租户 | `TestToolResultSpill_OtherConversationCannotRead` / `TestToolResultSpill_OwnerFromHistoryWhenCtxHasNone`（不取历史归属） |
| 取不回时不给 id | `TestToolResultSpill_NoStoreFallsBackToTruncation` |
| 小结果不动；return-direct 不转存 | `TestToolResultSpill_SmallOutputUntouched` / `TestToolResultSpill_ReturnDirectExempt` |
| 会话历史里只有预览；下一个任务经历史里的 id 读得回 | `TestToolResultSpill_HistoryKeepsPreviewOnly` / `TestToolResultSpill_NextTaskReadsViaHistory` |
| 续跑不重跑、不重新转存；同一份内容只存一份 | `TestToolResultSpill_ResumeReplaysSameNotice` / `TestToolResultSpill_SameContentStoredOnce`（id 不按内容定） |
| 读取有界（抬头结尾算在内）、长行可读、越界报错、搜到末尾不让接着搜、字符串数字参数 | `TestReadToolOutput_ByteCap` / `TestReadToolOutput_CapStrict`（上限不扣抬头结尾）/ `TestReadToolOutput_CapStrictWithEscapedPattern`（抬头按固定余量估）/ `TestReadToolOutput_LongSingleLineFolded` / `TestReadToolOutput_FoldMultibyteValid` / `TestReadToolOutput_OutOfRange` / `TestReadToolOutput_PatternFooterAfterFullScan`（搜完不记扫描位置）/ `TestReadToolOutput_ArgEdges` / `TestReadToolOutput_StringNumberArgs`（不认字符串数字） |
| 说明里的行数与取回工具同一口径 | `TestReadToolOutput_NoticeCountsFoldedLines`（行数按换行） |
| 压缩层清理、摘要、紧急丢组之后取回 id 还在 | `TestMicroCompact_SpilledResultKeepsLocator` / `TestToolResultSpill_IDSurvivesAutoCompact`（摘要不附索引）/ `TestToolResultSpill_IDSurvivesRepeatedAutoCompact`（增量不接着带）/ `TestToolResultSpill_IDSurvivesReactiveCompact`（紧急丢组不补） |
| 摘要后的重注入不把转存预览当文件内容 | `TestAutoCompact_ReinjectSkipsSpilledResults` |

括号里是让它变红的变异。

## 6. 决策记录

| 决策 | 理由 |
|---|---|
| 在结果产生时转存，不等压缩 | 等压缩清理时，这条结果已经在之前每一轮的 prompt、每份存档、会话历史里付过费；产生时转存是追加式的，不改已经发生过的前缀 |
| 在 `OnToolEnd` 之后转存 | 钩子之前就转存的话，钩子收到的是预览、store 里存的是钩子之前的全文：结果脱敏只作用在预览上，模型用取回工具照样读回原文（审计实测）。代价是中断的那一批里全文随工具账进存档一次 |
| 失败结果也转存 | 构建 / 测试日志是大输出的常见来源，往往以失败返回；只截断的话没配 `ToolResultMaxBytes` 时它原样进每一轮的 prompt。`[code]` 前缀照留，模型照样看得出失败（曾经只转存成功输出，审计实测失败的大日志原样进了消息流） |
| 取回 id 按内容定 | 续跑重做这一批时同一份内容不再存第二份；命名空间按会话分，同一个 id 跨会话读不到 |
| 压缩时把取回 id 收成索引留下 | 摘要、紧急丢组都会让带 id 的那条消失；全文还在 store 里，不留 id 模型就再也取不回 |
| 命名空间按调用方身份 + 会话分、按哈希入 | id 会出现在模型可见的文本与历史里，不能当凭据；按会话分之后，别的会话拿着 id 也读不到 |
| 取回工具按行编号、长行折行、每次有上限 | 模型要能「接着读」与「只看匹配的行」；单行的大 JSON 不折行就一次读不完也分不了段；有上限才不会把转存省下的又一次拿回来 |
| 写失败时不给 id | 取不回来的 id 只会让模型白调一次 |
| 会话结束不删 | 续跑与回看还在引用；过期交给 TTL |
