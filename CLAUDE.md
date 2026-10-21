# crew-go 开发规范

## 注释语言

所有代码注释、文档注释（godoc）一律使用中文。

## 代码风格

- 错误处理优先 early return，不嵌套 `if err != nil`
- 不用 `_` 忽略 error，除非有注释说明原因
- 接口定义在使用方，不在实现方

## 命名约定

- 缩写词全大写：`ID` 不写 `Id`，`URL` 不写 `Url`
- context 参数永远第一个，命名 `ctx`
- 构造函数用 `New` 前缀，选项函数用 `With` 前缀

## 包设计

- 包名是名词单数（`tool` 不是 `tools`），不创建 `util`/`common`/`misc` 万能包
- 循环依赖是设计问题，重新分层而不是用 interface 绕过
- 依赖方向：`schema` ← `model/tool/hook/skill/mcp/prompt` ← `compose_v2` ← `agent_v2`；`internal/panicerr` 是只依赖标准库的叶子，各层兜住 panic 时共用
- 包内测试文件用 `package xxx_test`（黑盒测试），只测公开 API

## 错误处理

- 错误信息小写、不加标点：`"failed to open file"` 而非 `"Failed to open file."`
- wrap 时用 `%w` 保留调用链：`fmt.Errorf("load config: %w", err)`
- 哨兵错误（`var ErrXxx = errors.New(...)`）只在调用方需要 `errors.Is` 判断时才定义

## context 使用

- ctx 只传不存，不把 ctx 存进 struct 字段
- `context.WithValue` 的 key 用私有类型，避免碰撞

## 并发

- 不暴露 `sync.Mutex`，持有锁的结构体不被拷贝
- goroutine 必须有明确的生命周期，谁启动谁负责退出

## 测试

- 用标准库 `testing`，不引入 mock 框架；用手写 interface stub 代替
- 表驱动只用于纯输入→输出 / 边界值枚举；需要差异化 stub 的错误路径拆独立 `TestXxx`，别硬塞表里
- 公开 API 行为走黑盒（`package xxx_test`）；未导出函数 / 内部分支走白盒（`package xxx`），文件加 `_internal_test.go` 后缀；不要为"能测"把内部函数 export
- 依赖真实 LLM / 外部服务的用例统一打 `//go:build integration`，放 `example/`；默认 `make test` / `go test ./...` 不编译
- 详细规约见 [doc/description/testing-conventions.md](doc/description/testing-conventions.md)

## 性能

- slice 预分配：`make([]T, 0, n)`
- 热路径避免 `fmt.Sprintf`，改用 `strings.Builder`
- 频繁分配的小对象考虑 `sync.Pool`

## 可观测性

- 关键操作通过 ctx 透传 trace，不用全局 logger
- 结构化日志用 `slog`，字段 key 用常量不用魔法字符串
- **业务接 observability** (Phase 2 Sub-5 完成):
  - **优先用 `ctx, opts := obs.Install(ctx, agentOpts...)`** — 一次接上两条链（业务级 8
    method + 编排级 5 method），**拿不到 opts 就拿不到 ctx**，没有只装一半的用法。
    业务自己也配了 `WithHooks` 时不会被顶掉（走 `WithHooksChained`，obs 链在最前）。
  - 两半各自的入口 `obs.AgentHooks()` / `obs.OrchHooks()` 仍在，给 **Install 覆盖不了的那一档**：
    agent 建一次、ctx 每请求 push（典型 a2a/server），结构上就是两个安装点。
    ⚠️ 那一档**漏装不报错**——Compact/Cp/Fanout/Node/Skill 8 个 metric 全不 fire、node 与
    graph 的 span 也不建。框架会在首次 `OnAgentEnd` 发现「业务级接了、编排级没跑过」时
    **warn 一次**（判据是「obs 自己那半跑过没有」，不是「ctx 里有没有」——编排 hook 走
    RunOption 也合法，只查 ctx 会误报）
  - 业务必 `compose_v2.WithSessionIDCtx(ctx, sess.RunID)` push sid + `defer obs.CleanupSession(sid)`
    防 sessions map leak (长跑高 QPS 不调会 OOM)
  - **trace span 身份 attr** (对齐 AI_TRACE_SPEC / CS-OSS Monitor): `gen_ai.request.id`
    (=sid, 自动) + `gen_ai.session.id` (跨 run 会话聚类, 业务 opt-in
    `compose_v2.WithThreadIDCtx(ctx, convID)`, 不 push 不 emit, 类比 LangGraph thread_id) +
    `gen_ai.system` (provider, model client `openai.WithSystem` 透传 → `Message.Metadata.System`,
    deepseek/qwen 默认已注入) + `gen_ai.response.model`. **`agent_v2.WithSession` 自动把
    sid=RunID 提前 push** (RunID 非空就推、覆盖 ctx 上已有的——任务的身份就是 RunID；嵌套位置上 session 的
    RunID 必须是所在任务的、跟父相同, 沿用父 sid) 让顶层 agent span (OnAgentStart 早于 runGraph) 也带 request.id +
    不落孤儿 session。`agent_v2.DetachTask` 开的独立任务有自己的 sid ⟹ 观测层是另一个会话，要自己 `CleanupSession`（会话 ID 也被剥掉，`gen_ai.session.id` 要自己推）
- **metric 协议** (31 个 metric name 常量 in `observability/metric.go` — P0+P1 后 +3 新常量 resumes/plan_replans/route_decisions + interrupts/interrupt_resume_duration 升级 auto-fire + failover / member.tripped sink-gated auto-fire):
  - **25 framework auto-fire** (业务接 hook 后自动): agent.run.total/duration/rounds /
    runs.inprogress / llm.call.duration / llm.tokens.total / tool.call.duration /
    tool.error.total / node.execution.total / skill.activate.total / active.sessions /
    compact.{total,duration,ratio} / cp.{save.total,size.bytes} / fanout.branches /
    **interrupts** (P0 新) / **interrupt_resume_duration** (P0 新) / **resumes** (P0 新, label{action}) /
    **plan_replans** (P1 新, label{phase}) / **route_decisions** (P1 新, label{target}) /
    **failover** (fallback 候选切换, sink-gated 同 compact — 业务用 fallback + 接 observability 自动采集, label{reason,from}) /
    **stale_writes** (陈旧写回检测命中, `WithStaleWriteGuard` 开启时才可能 fire, label{crew_stale_op}) /
    **member.tripped** (fallback 候选断路器熔断, sink-gated 同 failover — 区别 failover 计成员间切换, tripped 计单成员 closed→open 熔断边缘含"全员挂光"failover 计不到, label{member,reason})
  - **6 业务自填** (framework 拿不到 / 跨 Run 控制流): LLMCostUSD / LLMTTFT / LLMTPOT /
    LLMError / StreamChunks / ToolCallsPerRound (业务 metric — 之前 InterruptTotal /
    InterruptResumeDuration 已 P0 升级 auto-fire 走 agent_v2.WithEndError / WithResumePhase ctx 协议)
- **metric label 命名** (Round 5 audit Q1 fix): 对齐 OTel GenAI semconv —
  `gen_ai.agent.name` / `gen_ai.agent.kind` / `gen_ai.tool.name` / `gen_ai.request.model` /
  `gen_ai.system` (provider, 业务 cost metric 用) / `gen_ai.usage.type`. crew-go 自定义 label 用 `crew.*` prefix (e.g.,
  `crew.compact.layer` / `crew.outcome` / `crew.cp.op` / `crew.node.id` /
  `crew.resume.action` / `crew.route.target` / `crew.plan.phase` /
  `crew.failover.reason` / `crew.failover.from` / `crew.tripped.member` / `crew.tripped.reason`)
- **cardinality cap** (`Config.MaxMetricSeries` default 10000, per-type 独立): 业务高基数
  label (e.g., user_id / request_id) 触发 cap 后 Counter/Histogram/Gauge() 返 nil
  + `obs.MetricCardinalityDropped()` 业务可监控
- **per-category 关闭** (`Config.DisabledMetrics []string`): metric name 黑名单 (wire 名如
  `gen_ai_compacts` / `gen_ai_failovers`)——命中的 metric 在 Counter/Histogram/Gauge 入口返
  nil (复用 cap 同款 nil-safe silent skip), 业务按类关观测降本 (省 series/cardinality/上报);
  event emit 仍发 (下层 fallback/compact 零改动只是不转 metric)。空=全采 (默认不变), 对齐
  OTel View DropAggregation 的 name-based drop
- **component span kind** (Phase 2): trace 标组件类型 (retrieval/embedding/guardrail，对齐
  CS-OSS `gen_ai.retrieval.*` + OTel operation.name) 两路径 — (1) **BasicAgent ReAct tool
  路径**外部能力是 tool, 业务 tool 实现 `tool.Categorizer` 返 `CategoryRetrieval` 等 → tool
  span 标 kind; (2) **GraphAgent 显式编排路径** node 直调组件 (不经 LLM tool_call),
  `compose_v2.WithNodeComponent("retrieval")` 声明 → node span operation.name 标 kind. 机制:
  只改 span operation.name + spanName 展示, **registry 配对锚点恒 node kind 不变** (跟 tool
  span callID 路径零冲突); graph_loop 进 node 时 `WithComponentKind` 总是 set (含空) consume
  清除继承, 防 component 跨 graph/agent 边界泄漏 (component 只作用声明它的那层). 非法 kind 经
  `isToolKind` 校验 fallback 默认 kind. **agent span 不标 component** (OnAgentStart 不读, 恒
  operation.name=agent 对齐 OTel invoke_agent) — 原 ReflectAgent evaluator/reflector 标
  evaluator 已退化删除 (CS-OSS/OTel operation.name 无 evaluator, sub-agent 即 invoke_agent)
- 三方对照决策见 `doc/description/three-way-comparison/observability-callback.md`
  (callback 协议) + `observability-trace-spec.md` (trace/metric 字段 vs AI_TRACE_SPEC + 3 gap 已补)
- **改 observability span attr / metric 前必对照 monitor-agent spec** (CS-OSS Monitor 上报规范, 本地
  `/Users/gupengfei/program/go/src/monitor-agent/docs/` — `AI_TRACE_SPEC.md` trace attr + `AI_METRICS_SPEC.md`
  metric)。三铁律 (2026-07 一连串偏离/误删的血泪教训):
  1. **命名两套别混** (AI_METRICS_SPEC line 143/147): trace span attr **点号** (`gen_ai.model` / `gen_ai.system` /
     `gen_ai.status` / `gen_ai.agent.kind`) vs metric label **下划线** (`gen_ai_model` / `gen_ai_system`)。同一语义
     trace/metric wire 名不同, `metrics_mapping` value 存 metric 下划线名。把 metric 下划线误用到 span attr =
     前端按 spec 点号读不到 (曾偏 4 个 attr, commit `5296699` 修)
  2. **体量控制走 spec 机制、别砍 span attr**: 大内容进 `gen_ai_content` event (spec 定 1MB 单独限) +
     `RecordMessageContent` 开关 + `MaxAttributeValueChars`。**别加 fail-closed 白名单裁 span attr** — 新增 attr
     默认被砍反复漏 (metrics_mapping 差点砍掉、补后又漏 tool.call_id/usage) + 违反 spec (砍掉 spec 定义 attr 致
     前端缺字段) + 省不了体量 (span attr 全小 value, 大头 content 在 event、filter 不碰)。曾加 TraceMinimalAttrs 已删
  3. **审计 attr 覆盖穷举所有 helper × 调用点**: `strKV` / `intAttr` / `floatAttr` 三 helper, **别只 grep 字面量**
     — 常量键 (如 `MetricsMappingAttrKey` / `attrA2A*`) 是盲区, 自审当场漏掉、靠独立 agent 才揪出

## 交付纪律

- **`go build` 通过 ≠ 能用**。build 只检查语法/类型，运行时逻辑 bug 完全看不到。改完可运行的代码（尤其 example/demo）必须本地跑一次冒烟，至少让程序完整跑完一条路径再交
- **交互式程序冒烟用 `printf '行1\n行2\n' | go run ...`**，不要用 `echo`——`echo` 会把 `\n` 当字面量，stdin 收不到换行，程序卡住假装"跑通了"
- **改 cp / 嵌套 / Resume / ns 路径 → commit 前必跑两个 binary 冒烟**：
  1. `go run ./example/orchestration/nested_smoke`——7 编排嵌套 HITL Resume 闭环 (BasicAgent + Sequential
     + Loop + Reflect + Handoff + Parallel + Routing; Plan/Graph 嵌套走 agent_v2 单测
     `plan_nested_resume_test.go` / `r30_graph_agent_hook_order_test.go` /
     `r31_graph_resume_test.go`) + cleanup + GraphID 防串台（12 场景，30 秒）
  2. `go build -o /tmp/web ./example/orchestration/plan_chat_web && /tmp/web -mock -addr :19999 &`，
     curl 触发 `q=任务` + `?session_id=xxx` interrupt + Resume——验证 SSE 流 +
     cleanup。完整脚本见 `doc/description/smoke-test-recipes.md`
  历史教训：v1 时代 ISSUE-70 Phase 1-2 多个核心架构 commit 只跑 `go test ./...` 没
  binary 冒烟，靠最后累积验证撑过——bisect 不友好。后续每个相关 commit 必跑此两个
- **改 a2a/（A2A 协议接入）→ commit 前必跑两个 a2a 冒烟**：
  1. `go run ./example/a2a/hitl_smoke`——自包含（自动拉起 rich server），4 场景：Task
     状态机 / input-required 默认 + taskID 续跑 / 流式 artifact 聚合 / WithHumanGate
     HITL 闭环（~10 秒）
  2. `go run ./example/a2a/llm_smoke`——真 qwen（.env QWEN_API_KEY），3 部：全互通环
     （server 适配+注册+发现+LLM tool-calling）/ 真 LLM 流式 chunk→artifact 过 wire /
     server 侧 HITL（qwen ask_user over A2A）（~1-2 分钟）。完整说明见
     `doc/description/smoke-test-recipes.md` §3
- **证据与假设冲突时优先相信证据**。日志里两次输出一字不差 + 带 `[mock xxx]` 前缀 → 先怀疑"消息打错位置"，不要先归因"LLM 偷懒复读"。具体字面量比抽象行为假设更可靠
- **改 hook 前先读 `agent_v2/basic_agent.go` 的 `preambleFn` / `modelFn` / `toolsFn` / `postambleFn`**（ISSUE-74/75 修后 BasicAgent 是 preamble + model + tools + postamble **四节点** channel-based Graph + 循环回边；ISSUE-81 后 tools 节点内 N>=2 tool_call goroutine 并发 fanout），搞清楚每个 hook 触发时 messages 的累积状态。`OnAgentStart` 整个调用 1 次, 在 agent_v2 wrapper Run/Resume top-level fire（图执行之前, 跟 OnAgentEnd 单一所有权对称, 跟 PlanAgent / GraphAgent / eino ADK 对齐）。`OnModelStart` / `OnModelEnd` 每轮在 modelNode 触发；preamble / postamble 节点只触发引擎层 `OnNodeStart/End`。`OnAgentEnd` ISSUE-82 fix 后 Run defer 1 次 + 每次 Resume defer 1 次（HITL 闭环算 2 次），不每轮触发；要观测每轮请用 `OnModelStart` / `OnModelEnd`
- **Graph 超步执行用 `compose_v2.Run` + `WithCheckpointer` 时必须设 ctx deadline**。超步内分支失败策略是 best-effort 等兄弟跑完——业务错不 cancel 兄弟，若某分支 `NodeFunc` 不返回则兄弟也会一直等，deadline 是唯一外部兜底。用户自定义 `NodeFunc` 不保证有内部超时时尤其重要

## 多层系统 bug 审计方法论

多层 / 多 hook / Resume / 并发分支的真 bug 在**交互层**，不在算法里。单测 + 集成测同时绿仍可能漏掉一打 bug。**先审计再写测试**——审计找 bug，测试加固已审计路径。

### 测试可信度

- 单测全绿是必要不充分
- **mock 必须读 input 做断言**（"下游应收到 X"）；忽略 input 返回固定响应的"集成测试"等于单测——抓不到"上游吞消息"类 bug
- **sanity-revert 才算测试通过**：临时还原 fix（用 `git stash push -- <files>` 或 Python script 改关键代码块）跑回归，**必须看到 fail 信息精确指向 bug**。还过的测试 = 假绿；测试设计要保证 old/new 真有差异（否则 tautological）。并发场景需用 WaitGroup 屏障 + 短超时强制真正并发，否则 Go scheduler 会自然串行化掉 thunder-herd 测试
- **mock 闭包绕开真实路径不算端到端覆盖**：`TestPlan_Resume_Replan_WithUserText` 用闭包 mock planner 自己读 ResumeContext，绕开 LLMPlanner 默认路径，直到用户实测才暴露 ps.Input 没注入（ISSUE-72 Round 3）。HITL / Resume / 4 层压缩等跨层 feature 必须用真实 `NewLLMPlanner` / 真实 `BasicAgent` 端到端测，HITL Resume 测试矩阵见 [doc/description/testing-conventions.md](doc/description/testing-conventions.md) §7
- **断言选在「不承重的维度」上，等于没测**（2026-08 实证）：`nested_smoke` 只断言最终
  文本，而一次真实破坏（中间层多 push 了一次信号）的表现是**叶子被多调了一次**——
  最终文本仍然「看着对」。改成断言「叶子 model 被调次数 == 中断次数 + 1」才抓得到。
  写断言前问一句：**这个 bug 发生时，我断言的这个量会变吗？** 不会变就换一个量。
  同类：验「有没有泄漏」时断言「有没有中断」不够，要断言**工具有没有真执行**
  （中断可能来自别的原因，而工具静默执行才是泄漏的实质）。
- **sanity-revert 必须真跑，不能逻辑推理**：ISSUE-73 R8/R9 4 颗假绿炸弹全是"靠推理判断 sanity OK"漏的——R8 实测 1 颗，R9 双 audit 实测 3 颗（含跨仓 1 颗）。流程：(1) sed 精确匹配 revert fix 单行/单块；(2) 跑测试看是否 fail；(3) fail 信息要精确指向 bug 不能"别的原因 fail"；(4) 立即恢复
- **7 类炸弹 fixture 设计陷阱**（R8/R9 拆弹归纳 + 2026-08 补第 6、7 类，写测试时主动避免）：
  1. **过滤后元素恰好原位**：in-place filter `s[:0]` 测试用"通过过滤的元素 = 原始位置元素"——污染不显现假绿（R9 T1 案例）。修法：让通过过滤元素 ≠ 原位元素
  2. **早退兜底盖过 fix 路径**：`if len(groups) <= 1 { return }` 早退让 fix 后续逻辑根本不执行——stub.called=0 跟 fix 无关（R9 T2 案例）。修法：构造数据让早退条件失效
  3. **error 字符串不匹配 regex**：`prompt is too long: ... > ... maximum` 不匹配 OpenAI/Claude 真实格式 → ParsePromptTooLongGap=0 → emergencyDrop 兜底——fix 是否生效跟测试无关（R9 T2 案例）。修法：用业务真实 error 字符串
  4. **双层防护遮蔽**：fix 路径有冗余防护（如 root cp + sub cp 都 wrap 类似错误）→ 断言用通用关键词被冗余路径误命中（R9 T3 案例）。修法：断言用 fix 路径专属字符串而非通用关键词
  5. **mock 不读 input 返固定响应**：等价单测，抓不到"上游吞消息 / 序列化漏字段"类 bug（CLAUDE.md 早写过，R9 audit 发现 6 个 stub model 全踩）。修法：mock 至少 1 个测试断言 `containsString(captured.Input, "expected_field")`
  6. **fixture 没搭对场景 ⟹ 假阴**（2026-08-04 新增，前 5 类都是假绿，这类反过来）：验证一个
     **已知缺陷是否存在**时，fixture 漏掉了触发该缺陷的必要条件，于是测出「缺陷不存在」。
     实例：验 `approve_always` 跨任务泄漏时漏传 `WithResumeSession`——Resume 默认新建空
     session（`approval.go` 注释写明），规则写进临时对象随即消失 ⟹ 任务 B 照常要审批 ⟹
     测试报「已修复」。**就此收手会把一个真实的安全缺陷判成不存在，还留下一条假绿哨兵。**
     修法：写「验证缺陷存在」的测试时，**先让它红**（即先确认能复现）再谈别的；测不出来
     时的第一反应应是「我的 fixture 是不是没搭对」而不是「缺陷不存在」——尤其当结论与
     既有的代码级分析相反时（那时两边必有一边错，而 fixture 出错的先验概率高得多）。

     **⚠️ 本仓最高频的那个子形态（2026-08 一个 session 内栽 7 次）：让任务「成功收尾」了。**
     `cleanupOnSuccess` 会在成功路径删掉 cp ⟹ **没有残留可泄漏 / 可误载入** ⟹ 想验的
     那条路径根本不存在，用例恒真。所有涉及「上一个任务留下的东西污染下一个任务」的
     测试，任务 A **必须停在中断且不 Resume**（被放弃），不能跑完。
     判断方法很快：跑完之后 `ListPending(sid)` 数一下残留，是 0 就说明 fixture 没搭对。
  7. **跨「每次新铸的身份维」比对派生值 ⟹ 必然不等 ⟹ 假阳**（2026-08-13 新增，**一次
     session 内我和两个独立 agent 各自栽了一遍**——三方独立踩中说明它不是粗心，是这个
     fixture 形态看起来太合理）。
     实例：验「某改动有没有让 `InterruptID` 偏移 / 让计数器多 tick」，写法是「跑两次
     `Run`，比两次的 ID」。**必然不等**——`InterruptSite.TaskID`（任务代号维）每次新开
     任务都新铸，差异全来自那一维，跟被验的东西无关。三份报告分别得出「计数器被多消耗
     了」「身份算法变了」两个**都不存在**的结论。
     修法：**要么同一次 Run 内做差分，要么锁住那个会新铸的维**（同 `sessionID` +
     checkpointer 让 `TaskID` 从存档读回）。
     **推广**：断言「派生值 X 变了/没变」之前，先列出 X 的**全部输入维**，逐维问「这一维
     在我的对照组之间是否恒定」。有任何一维不恒定，这个对照就不承重。
  8. **fixture 替被测代码补上了它本该自己推导的东西 ⟹ 两种判据分不出来 ⟹ 假绿**（2026-09-29 新增，
     第 7 类的反面：该**不同**的那一维在对照组之间被别的机制抹平了）。实例：验「框架补的 session
     取所在任务的 sid、不取 ctx 上的 sid」，fixture 在**图外**推请求 session——图自己的 sid 进节点时
     盖掉了它，ctx 上的 sid 恰好等于所在任务，两种取法算出同一个值，变异照绿。改成在**节点里**推才
     分得开。同类：既有用例都显式替新会话推 sid，把「两把尺」的分叉藏住了。
     判别式：写完问一句「**生产调用方会不会比我的 fixture 少做一步**」——会的话，少做的那一步
     正是被测代码该自己完成的，fixture 替它做了就测不到。

### 审计前画两张图

1. **时序图**：Round N vs Round N+1，每层何时跑、改什么 state、emit 什么 event
2. **state 所有权图**：每个变量的 producer / consumer / lifetime（per-call / per-Run / per-Session / cp 持久化）。lifetime 混淆是 bug 富矿

### 必查 9 类 bug

| 类别 | 怎么找 | 典型陷阱 |
|---|---|---|
| **跨层数据流** | A 改 X，B 下一轮读 X 怎样 | A 写占位符 B 当真实内容用 |
| **触发顺序** | A 在 B 之前 / 之后 / 之间结果不同 | 顺序敏感未文档化 |
| **跨 Run 持久化** | Run 1 改写在 Run 2 是否需要 | per-Run 优化跨 Run 失效，每 Run 重做浪费 |
| **消息分组语义** | "按组操作"必问"system / leading meta 在哪一组" | system 被分到 oldest 组，删 oldest 把 system 删了 |
| **错误路径** | mock 返回 `(nil, nil)` / 空 string / 空 slice / nil 元素 | nil resp 没判直接 deref panic |
| **钩子状态所有权** | 钩子修改是 ephemeral 还是 persisted | ephemeral 写回 state 跨轮累积 |
| **Resume / Checkpoint 一致性** | cp 保存什么、Resume 重建什么、审计视图 vs LLM 输入视图 | 两个视图差异未对齐 |
| **并发共享态 + thundering herd** | 分支共享 cfg 字段（breaker / counter）的 mutex 够吗、并发是否击穿限流 | N 分支同时通过 Allow，同时调 LLM，计数瞬间从 0 跳到 N |
| **退出条件 / 边界 / 主流场景** | MaxAttempts 耗尽、空 input、only-system、单组、最常见场景的边界 | "找最后一条 user"在"单 user + 多 agent 轮"主流场景失效 |
| **注释承诺 vs 代码实现** | 注释里说"会做 X" → grep 实现是否真做 X | 重构动了实现没改注释；feature 部分实现注释承诺全部（ISSUE-72 Round 3：plan.go 注释说"让 Planner 看到 UserInput"，planNode 完全没读 ResumeContext） |

### 公开 API 防御

- 调用方对返回值 nil / empty 必须显式守卫：`if x == nil` 或 `if len(x) > 0`——一行省后续一小时排查
- "成功/失败"语义点必问"什么算成功"：底层 RPC 成功 ≠ 业务成功（空响应、不遵循 prompt、违反格式都是失败，算 RecordFailure 否则断路器不跳）
- 断路器一旦 trip 必须能自愈（cooldown 半开），否则一次 quota burst 永久禁用整个能力
- **Go interface "typed nil" 陷阱**：业务传 `var m *MyModel; WithModel(m)` 时 m 是 typed nil pointer 但 interface header 持非 nil type tag → `m == nil` 是 false 漏 fail-fast → 业务后续 `m.Generate()` 远 stack panic 难定位。**框架已防** (R32+ audit²-²⁷ 71 真 HIGH 修): typed-nil 守卫跨 **9 包 8 个名字**——`agent_v2`/`compose_v2`/`store` 叫 `isFrameworkNil`，另有 `a2a.isNilIface` / `a2a/server.mustAgent`(panic 不返 bool) / `fallback.isNilModel` / `mcp.isClientNil` / `tool.isToolNil` / `hook.isHooksNil` / `agent_v2.isAgentNil`。**按名字 grep 找不全**（这条本身被两次按名字 grep 的更正各漏一个，最后靠形状找齐）。覆盖 30+ 业务入口 (With* options + builder + 工厂 + 注册). 业务自定义 Agent/Hooks/Tool 实现时同款用 `reflect.IsNil` 检. 详见 [`doc/v2/usage-guide.md` §8 反模式](doc/v2/usage-guide.md)

### 派调研之前先问一句「我是不是已经默认了某个前提」

**问错的问题会得到一个证据充分的错答案。** 调研忠实回答你问的那个，不会替你质疑前提。

**实例（2026-08-05）**：加了一道守卫，然后问「A（按种子推断）还是 B（同给就报错）」。
调研 + 实测都干净利落地证明了 A 在我们这儿不安全，于是落了 B。**但没人被问「要不要有
这道守卫」**——两个 commit 后挖出来的答案是「不该有，它拒掉的全是合法调用」。

**线索当时就在手上**：同一批调研已经写着「那个策略在库层 grep 0 命中，只在平台层」「库层
的常态就是没有这种守卫」。**拿着「库层没有」的证据，却把问题框成了「该用哪种形态」。**

**判别式**：如果你的问题是「X 该怎么做」，先补一句「以及 X 该不该做」。选择题形式的
prompt（A 还是 B）尤其危险——它把「都不选」这个选项从答案空间里删掉了。

配套：**给调研的 prompt 里明写「如果证据指向别的形状 / 指向不该改，直说」**——本仓实测
这条真的会被用上（一次直接否掉了提案本身）。

### 否定式断言的集合，不构成「它真的干活了」

「没报错 + 没返哨兵 + 没重放副作用」三条**在「什么都没发生」这个状态上同时为真**。
2026-08-05 实测：一个「续跑静默 no-op」的退化下，只有否定断言的那条用例照单全绿，而它
的姊妹用例（有一条正向断言）红了。

**写完一组断言问一句：有没有至少一条是「它必须产出 X」？** 没有的话，这组断言在空转
状态下恒真。

### 守卫合法，当且仅当调用方能用它手上已有的信息满足它

加一道 fail-fast 之前先问一句：**要让这个守卫不报错，调用方得先知道什么？**

答案里只要出现**只有框架知道**的状态（存档在不在、什么 `Status`、跑到第几步、frontier
在哪），这道守卫就不是守卫——它是**把框架的决策外包给了调用方**，而调用方唯一能做的是
去复现框架的私有判据，**那就是又一份会漂的判据拷贝**（见上一节）。

**反例（本仓 2026-08-05 加了又撤的那个）**：「顶层 + 传了种子 + 存档命中 ⟹ 报错」。它给的
出路是「要么声明新开、要么去掉种子」，可**调用方无法知道该走哪条**——没存档时摘掉种子图就
没输入了。它能用的探针是 `ExistsNS`，而引擎的判据是 `Status == Pending`：**两个不同的谓词
读同一份状态**，在 `Failed` 存档上分叉。实测调用方照提示自查后摘掉种子 → 引擎不 auto-load →
节点拿到**空输入**且 `err == nil`。守卫非但没消灭那个坑，还把它从框架内部搬到了公开 API 上。

**正例**：「同时有多个挂起时，回答必须指名 InterruptID」。那个 ID 是**框架抛给调用方的**，
它手上就有，照做零成本。同理「`WithResumeFrom` 传了不存在的存档 ⟹ 报错」——调用方给的东西
自己就能检查。

**判别式**：守卫的入参里如果**只有调用方的显式意图**（两个互斥的意图同时给了、给的东西自身
非法），报错合法；如果是**「你的意图」对上「我发现的状态」**，那不是矛盾，是**你该定义清楚
默认行为**。

⚠️ 顺带一条：`Status` 三档（ISSUE-94）是**引擎的私有知识**——业务能观测的只有「有没有存档」。
加它有充分理由（别删），但代价是**从此绝不能要求调用方复现 auto-load 的判据**。

### 同一条判据抬高一层：契约条款合法，当且仅当**每个实现**都能用手上已有的信息满足它

上面那条管「框架 vs 调用方」。**接口有多类实现时，同一个形状再来一遍**——只是主语从
「调用方」换成「实现」：

> 问：**要让这条契约为真，实现得先知道什么？** 答案里出现只在**某一类实现的运行时**才
> 存在的东西（Go 指针 / Go 类型 / 事务 / 内存布局），这条就是非法条款。

**方向决定对错，不是「从几个实现归纳」决定对错**（2026-09-07 实证，三条同款）：

- **从最弱的实现归纳 = 对**。`Long_Keys_At_Portable_Limit_Do_Not_Collide` 是照 MySQL
  一家的列宽（`data_key VARBINARY(1020)`，按 4 字节字符算 255 个）定的，完全正当——最窄的那个
  定义了可移植边界。
- **从最强的实现归纳 = 错**。in-memory 能力最强（比较那一刻手里握着 Go 值），照它写的
  条款别人物理上做不到。本仓踩过三次，**每次都是 in-memory 的实现细节被提升成了所有
  实现的义务**：
  1. `ErrFilterTypeMismatch` 旧形态（存值类型对不上就报错）——下推型后端不读数据不知道
     存的类型。已于 `4ca9a78` 劈成两半修掉：filter 值非数值走静态哨兵（调用方手上的
     信息，六个实现都做得到），存值类型对不上走 type bracketing 不命中不报错。
  2. `Item.Value` 的「Get 返回同一指针（浅持有）」——那是「不序列化」的实现细节，对
     6 个后端里的 4 个本来就是假的。
  3. `Batch` 的 per-Op 错误隔离——`BestEffort` 这个词只在「没有事务这个概念」的
     in-memory 语境下讲得通（见 `doc/code/todo.md`）。⚠️ 结局别读反：2026-09-07 仍定 best-effort
     为**唯一**语义——逐 Op 顺序执行每个后端都做得到；删掉的是 godoc 里「可优化为单事务原子写」
     那句邀请（两种语义并存才是问题），见 `store.Store.Batch` godoc。

**⚠️ 别加 capability 分档来「解决」这个**——业内试过并放弃了：面向多云的 `gocloud.dev`
在设计文档里列了三条路，其中 enforced feature code **八年没落地、全仓 0 命中**，它的
指导原则是「不兼容必须尽早可见：编译期 > 启动期 > 运行时 error > panic」，**这个排序里
没有『静默给出不同答案』这一档**；Gateway API 把可分档的条件写死成「支持该特性的实现
**行为与语义完全相同**」——要么没有，要么一致，**「都有但各答各的」没有档位**。正解是
让弱实现去满足强契约（归一化 / 收窄形状），或者把条款改成所有实现都做得到的形状。

### 「必须与 X 保持一致」是设计债的自白，不是可接受的维护约定

注释里出现「这个判据在两处出现，必须同步」「与 XXX 的判据保持一致，否则……」时，
**那不是提醒，那是在声明一处已知会漂的拷贝**。本仓实证：同一句这样的注释挂在
`skipInitialWrites` 上，而它守的那对判据**照样漂移了四次**——每次都是「改了一处忘了
另一处」，每次的症状都是静默走错分支（`err == nil`）。注释拦不住，因为改代码的人
往往看的是另一处。

**正确反应是消灭那个拷贝**，而消灭的办法通常不是「抽成一个共用函数」（那只是把两份
变成一份 + N 个调用点，仍要求每个调用点记得调），而是**让第二处根本不需要做那个判断**：

- **最常见的成因：一个选项/字段被两种语义共用**。`WithInitialWrites` 同时承担了
  「调用方的初始输入」（只该在新开时给）和「不管哪条路都摆上这几个值」（子图同步父图
  import 值要的）——两种语义对「续跑时要不要 apply」的答案**正好相反**，于是它只能选
  一个，剩下那半的调用方**被迫自己判断这次算不算续跑**。而那个判断需要 `LoadNS`，
  可引擎进去本来就要 `LoadNS` ⟹ **同一个决策被抄到了另一个包**，抄本和正本失配的
  症状就是「两头不着的 fresh run」（那边从 StartNode 起跑、这边不注入输入，
  节点拿到空输入；业务节点直接解引用就 panic，channel 是值类型则静默读到零值）。
  **这个坑本仓踩过四次。**
  修法是把两种语义拆开：公开的 `WithInitialWrites` **只在新开路径 apply**，「两条都 apply」
  那个语义收进框架内部不对外（`withImportOverlay`，子图独家；2026-09-02 起它摆的是不经 reducer 的 baseline，不再是覆盖写——见 `AddSubgraphNode` 速查行）。调用方无条件
  传即可，**判断收回引擎唯一一处**，两处自查连同各自那次多余的 `LoadNS` 一起删掉
  （`39890db` 收敛掉两处自查，`b75d16f` 把公开面收成一个）。
- **识别这个成因的问法**：一个选项/字段，**有没有两个调用方对它「什么时候生效」的
  期望是相反的**？有 ⟹ 它注定要逼其中一方自己判断，那个判断就是下一个拷贝。
- 判断「该消灭还是该抽函数」的问法：**这两处的判断，信息是不是只有其中一处有？**
  是 ⟹ 让有信息的那处决定完往下传/往下做，另一处不该有这个判断。

⚠️ 消灭之后**立刻 grep 那个符号的所有引用**——被删判据的注释常常还在教后来者写回来
（实测：`cross_task_isolation_test.go` 里那句「必须同步」在收敛之后仍挂着）。

#### 同一个成因的另一个后果：不逼任何人判断，直接静默失效

上面说的是「共用 ⟹ 逼调用方自己判断 ⟹ 判据拷贝会漂」。**同一个成因还有第二种出口**：
借来的那个量在常见输入上碰巧够用，于是谁都不用判断，直到出现第二个才错——而那时
`err == nil`。2026-08 一次盘点里，**五条独立的开放问题归到了同一个形状**：

| 借的是什么 | 拿去干什么 | 要求怎么分叉 |
|---|---|---|
| `store` 层「按 map 字段过滤」 | 过滤框架写的长期记忆 | 它要 `map[string]any`，而这条路的 value 曾是 `*schema.Message`。**后果按后端相反**：in-memory 上 value 原样是 Message（不是 map）⟹ 配了 Filter 注入 0 条；持久后端存 JSON、读回是 map ⟹ 同一个 Filter 命中，但类型断言失败、走 fallback 变成一条内容是 JSON 文本的 SystemMessage。两个轴同时相反且都 `err == nil`。**✅ 2026-09-07 修**：写入侧改存**答复文本**（`string` 过 JSON 往返还是 `string`）⟹ 六后端逐字相同；`StoreMemoryConfig.Filter` 本身早已删除。哨兵 `sql/store_memory_crossbackend_test.go` 用真 SQLite 与 in-memory 对照 |
| 历史层归属 `HistorySlot` | 记忆键的唯一性 | 归属要**跨次稳定**（`member_dispatch.go` 明写），键要**逐次唯一**——正好相反 |
| 同上 | 内部决策 LLM 的记忆身份 | 它们按设计**不进历史** ⟹「没有归属」顺带变成「没有身份」 |
| wire 来的 `ContextID` | 判定谁能续这个会话 | 续接标识**可以从外部给**，授权凭据**必须验证来源** |
| tool_call 批内声明序 | 跨批区分是谁 | 批内有定义，跨批下标空间**重叠**（两批都有 0 号） |

**共同签名：N=1 正确，N≥2 静默错。** 一个写入者不撞键、一个决策 LLM 不撞、一批
tool_call 下标不重叠、没有攻击者时 wire ID 就是合法的——所以它们全都能通过测试、
能上线。

**判别式（写代码时用）**：写下「用 X 来做 Y」时，如果 **X 的名字里没有 Y 的意思**就
停一下——`HistorySlot` 里没有「唯一」，`ContextID` 里没有「授权」，`Index` 里没有
「身份」，`Filter` 里没说 value 类型。

停下来之后做的**不是加注释**，是给新用途起一个属于它自己的名字（哪怕只是个 wrapper），
让下一个改 X 的人看得见有第二个消费者。测试上配一条硬要求：**每个借用点必须有一个
N=2 的用例**——上面五条里只要当初有任何一条写了「两个写入者」「两批 tool_call」的
用例，就活不到今天。

**修法方向由这个根因决定**：正解一律是「让第二种语义有自己的载体」，不是「让借来的
那个更聪明」。有一档是**删掉那次借用**——`Filter` 那条借的能力在这条路上根本不成立，
而它想解决的分域问题已被「写入者维进 ns」覆盖。

### 立一条新规矩，同一次就要问「哪几条路能到这儿」

加一道守卫 / 一条不变量时，最容易漏的不是判据本身，而是**产生同一种东西的其他入口**。
这一条本仓在**同一个 session 内连栽两次**，症状一模一样：守卫写在 A 路径上，业务从 B
路径进来照样把要拒的形态送进存档，而且**不报错**。

实例（2026-08-07）：立了「中断请求必须带 ID」，校验长在「节点返回 error」那条路上。

- **`OnNodeStart` 抛的中断整条绕过**——早退在校验上面几十行。实测两个分支各抛一次 ⟹
  两条挂起 ID 都是空、彼此不可区分，`GetResumeAnswer(ctx, "")` 拿到第一条。
- **同一批里两条 query 撞 ID 没人拦**——`recordInterrupt` 撞 ID 只 warn 然后 skip。
  实测调用方看到 2 条、存档里只有 1 条：用户按「批准转账」那条答，而读同一个 ID 的
  代码可能执行「删除历史」。

**判别式**：写完守卫，别问「判据对不对」，问 **「这个东西还能从哪儿冒出来？」** 然后
逐条列、逐条实测——`grep` 那个类型的所有构造点 + 所有 `return nil, <该类型>` 的位置，
比读一遍主路径可靠得多。列不全时把它当成设计信号：**能从五个地方冒出来的东西，守卫就
不该长在其中一个地方**，抽成一个函数让所有入口共用（然后 grep 确认入口数 == 调用数）。

⚠️ 顺带：**扩大了「必须自己给」的人群，就等于扩大了「给错」的暴露面**。上一轮把自填 ID
的要求从少数扩到全部 HITL 业务，却没给「给重了」加守卫——那个坑是自己挖的。

### 拒绝一个非法请求时，「返什么错」和「留什么现场」是两个独立决定

`fmt.Errorf("...")` 拒掉一个业务违规，看着完事了，实际漏了两件事（2026-08-07 实测）：

1. **它默认可重试**。`IsRetryable` 对普通 error 返 true ⟹ 配了 `WithXxxRetryConfig` 的
   编排会把这个**确定性的代码 bug** 重试 `MaxRetries+1` 次，副作用跟着重放——实测扣了
   4 次款、烧了 4 次 LLM。
2. **它留下的存档不但不是死的，还会被自动捡起**。`Status=failed` 只是报告位——auto-load 闸门与
   `agent_v2.Resume` 读的都是 `HasResumableWork() && BelongsToTask()`，**不读 Status**。实测：
   a→b、b 报业务错，同 sid 再 Run ⟹ a 一次不跑、b 从失败点重跑。**这正是要 `ErrRunAborted`
   的理由**——确定性 bug 留下的档会被一次又一次静默续跑，而它每次都失败在同一个地方。
   （2026-08-13 之前确实是「auto-load 不捡、Resume 直接返 `ErrNoPendingCheckpoint`」，那道
   gate 已撤——按 Status 挡的立论站不住，见 `Checkpoint.HasResumableWork` godoc。）

两件都由 `%w` 上 `ErrRunAborted` 一并解决（`IsRetryable` 已排除它；Run 出口据它主动
`purgeExitSlot`）。**判别式**：这个错是「重跑一次可能就好了」还是「重跑必然一样」？
后者一律 `ErrRunAborted`——业务违反协议、参数非法、类型不对，都属后者。

**新增任何 fail-fast 时逐条过这三问**（2026-08-13 又栽一次：给 `Ask` 加「拒绝自填
InterruptID」时只想了「返什么错」，实测 `IsRetryable=true` ⟹ 配了 retry 的编排把一个
确定性协议违规跑了 4 次；留下的 `failed` 档还**顶掉了同 sid 上那条合法的 Pending 档**，
用户已经答过的问题从存档里消失）：
1. 这个错**重跑一次可能就好，还是重跑必然一样**？后者一律 `%w ErrRunAborted`。
2. 它会**留下什么档**？`failed` 档占着 (sid,ns) 单槽，而且**会被 auto-load 捡起**（闸门按内容判、
   不读 Status）——
   而它可能覆盖掉本来在那儿的挂起档。
3. 错误正文**定位信息给全没有**？至少 ns + nodeID + 惹事的那个值本身。

⚠️ 措辞另有一档纪律：**错误信息里别写变更日志**（「框架**不再**替你补」——读到的人绝大
多数没见过旧行为，只会去搜「什么时候改的」），也别写设计辩护（那属于 godoc）。正文一两句
「问题 + 出路」，理由指过去。定位信息要给全：**只给 nodeID 不给 ns，嵌套子图里定位不了**。

### 改规则之前先写不变量——「修来修去」「修 A 出 B」从哪来（2026-10-13 盘点）

一次对 117 个提交的盘点里两类都有，而且都是**审计一轮抓一个、修一个再出一个**：

- **修来修去**（同一条规则反复改，有的改回原样）：钩子返回 ctx 里的会话 ID，取框架的 → 改成钩子可配 → 改回取框架的
  （`b6d33bab` → `e34df9cd` → `789c89c8`）；OnNodeEvent 里做出的决定，算数 → 扩到三处 → 整个撤掉、只作观测
  （`b6d33bab` → `6ba23ac7` → `2cef2292`）；业务强制 tool_choice=required 的额度规则一天多改 4 版
  （`3b8bdab4` → `df350bc0` → `ab49ed43` → `a7bdb630`），每版修的是上一版被审计找出的反例。
- **修 A 出 B**（自己的修复带出新缺陷）：钩子 ctx 采纳前后约 10 个提交，`b6d33bab` 带出「钩子装的软取消停不下来」、
  修它的 `e34df9cd` 又带出「一个 Run 里两个会话 ID」……收进 `compose_v2.CallHook` 一处之后才停；上行信号终局
  `81dc1118` 把标记只挂在外层包装上、`errors.As` 取出结构体又变回可重试（`da4655e4` 修）；渐进披露的揭示集加任务维
  （`4352b056`）带出会话 KV 随任务数增长，挪出会话 KV（`fcb08464`）又带出检测键原文进存档（`081860e3` 修）。

四个根因，各配一条做法：

1. **按反例打补丁，没先写不变量**。改一条判据之前先用一句话写出它要守的不变量（「多强制的轮数至多一轮，跟工具库
   多大无关」「换了任务看不到上一个任务的 Y」），测试按不变量命名，再列对抗性输入逐条过：规模（远大于预算）、
   不守规矩的插件（检索器返回候选以外的名字）、生命周期（中断 / 放弃 / 续跑带不带原 session / `NextTask` / 复用
   RunID / 没配 checkpointer 的重试）、并发与嵌套。额度规则如果第一版就写下那句不变量，「只豁免一批」就是第一版。
2. **同一份状态、同一个判断存在两处**。缓存 + 权威副本、每个调用点各写一份逻辑——修一处漏一处，每次修键只修了
   其中一份。做法是**消灭第二份**而不是同步它（见上面「必须与 X 保持一致」一节）：揭示集删掉会话缓存
   （`fcb08464`）、采纳收进 `CallHook`（`0ebf0318`）之后，这两条链才停。
3. **照搬外部做法的形状，没核对它成立的前提**。抄之前把对方的前提写出来逐条对照：`df350bc0` 照搬「tool_search
   不计」，可对方的检索在服务端、和随后的调用在同一次响应里完成，我们的检索每次占一整轮——这正是下一版的反例。
4. **修复只拿报出来的那个反例验证**。新测试先在旧代码上跑红，再做变异；行为变化拿改动前后两棵树跑同一个探针
   （`git archive <父提交>` 导出到草稿目录、`go -C <目录> test`）对照——说「行为不变」要有对照输出，不能靠读代码
   （本仓实测：读代码推断「Reflect 的 actor 每轮共用一个位置」，探针显示是 `round_N/actor`，推断错了）。

**停止条件**：同一处规则被审出**第二个**反例时停下来重审规则本身，别补第三个分支。审计 agent 的价值是找你漏掉的，
不是替你设计——收敛靠动手前写下的不变量，不靠审计轮数。按这套做的 `fcb08464`：两个审计 agent 0 回归，它自己带出
的只有一处小问题。

### 实施前 checklist（写代码前过一遍，不是事后清场）

事后审计兜不住"加抽象时根本没把所有调用点接全"类 bug。开工前 30 秒强制五步：

1. **加新字段 / ctx-value / 类型约定 → 先 grep helper 现有调用点**。`grep -n "WithXxx\|GetXxx" --include="*.go" | grep -v _test`，对账 producer ↔ consumer 数量。**helper 写好但生产无人调 = 设计未完成**（典型坑：v1 时代 `WithCurrentNS` / `EnterChildNS` 写好只在测试里调，业务嵌套 push 子 ns 实际用 `WithGraphNS+JoinNS` 两步范式不走 helper，整套 currentNS 维度协议生产 0 caller 最终在 audit follow-up #6 commit `08bd6a7` 删干净）

   **协议变更四复证教训 (audit / audit² / audit³ / audit⁴ 同模式四轮重复)**:
   - commit `0aa15bf` PlanWorkerFn 签名扩 — "3 worker 都改" 实只改 1 worker (谎报)
   - commit `49d3065` IndexRef regression fix — 测试只覆盖 1/3 worker
   - commit `a8b42b9` isAgentNil helper — 只 PlanAgent 用其他 6 编排漏防
   - commit `dc142c3` isAgentNil 7 编排 — 实际漏 SetExecutor + WithLLMPlannerSubAgents 2 处

   **强约束 grep checklist**: 加新 helper / 协议时**必 grep**:
   ```bash
   # 找所有"接受 Agent 参数"的入口函数 (or 同类签名)
   grep -nE "func .*\(.*ag Agent\)|func .*\(.*a Agent\)|func With.*Agent.*Option" agent_v2/*.go | grep -v _test
   # producer 数 vs consumer 数对账
   grep -c "isAgentNil\|协议名" --include="*.go" agent_v2/*.go
   ```
   覆盖所有调用点后才算"协议落地完成". 漏 1 处就是 4 复证下一轮.
2. **删 / 重命名 API → 同步 grep 所有引用**，包含注释里的过时说明。一个调用漏改 = 行为悄悄变。`grep -rn "OldName" --include="*.go"`，包括 `.md` 文档。**删的是一条没有符号名的判据**（「ctx 上的父 sid 优先、否则 RunID」这种）时按符号 grep 找不到——要 grep 它的**中文说法**（「父 sid」「优先用 ctx」）和被删代码的**形状**；2026-09 一次删判据后，7 处注释与一条测试的失败信息还在教人写回来
3. **加新行为分支（如 IsResumeTarget 加 sibling 模式）→ 立刻问"这个分支会让谁意外命中"**。修一个测试 fail 后**必须重新审新分支引入的副作用**，不要"测试绿就结案"——加 pattern N+1 90% 会让某些原本 false 的场景变 true，主动找出"谁现在会意外 true"
4. **改了导出符号的语义 / 在既有 godoc 旁边插了新的导出符号 → 当场 `go doc <包> <符号>` 看一眼**（2026-08-13 栽过）。插 `NextAnswerSlot` 时忘了跟上面那段旧 godoc 之间留空行，Go 的 doc 注释是「紧邻声明的连续注释块」⟹ 整块被吞给了新函数：`go doc NextAnswerSlot` 渲染出的第一句是**另一个函数的文档**，而且被吞进来的还包括一句**正被这次改动论证为假**的话。`go build` / `go vet` / 全量测试**全都不报**，只有 `go doc` 看得见。**2026-09-02 起有机械哨兵**：`compose_v2/godoc_attachment_test.go` 的 `TestGodoc_NoBlankLineBetweenCommentAndDecl`，第四次复发（且是在修第三次的那个 commit 里造出来的）后加的，扫全仓含 `sql`/`redis`/`a2a/zk` 独立 module——它只抓「注释块与声明之间隔了空行 ⟹ Doc 为 nil」这一种脱钩（第一版检查「Doc 与声明的距离」是假绿：一旦有空行 Doc 就已是 nil，sanity-revert 证伪后重写）。**2026-09-21 第五次复发**是另一种形状——两个注释块**中间没有空行**被当成一整块、整块归给下面那个声明，上面那个符号的 godoc **凭空蒸发**，而上面那条哨兵照过；于是加了姊妹检法 `TestGodoc_NoAbsorbedDocBlock`（判据：某导出符号 Doc 为 nil，**且**它的名字是别人 doc 块的**首词**）。⚠️ 方向极易写反——粘连时合并块归**后面**那个声明、首行是**前面**那个符号的名字，查中间行会把正常行文卷进来（第一版就反了，当场 10 条假阳）。同一次还要 grep 该符号的**教学面**——类型 godoc 里的示例、设计文档、CLAUDE.md 速查表：改语义时它们是最大的 drift 面，而机械层只抓得到「符号不存在」，抓不到「示例教的是刚被禁掉的写法」。
5. **涉及 cp / 子图 / interrupt / state / 寻址 / 会话历史 子系统的设计改动 → 先读对应三方对照文档**。`doc/description/three-way-comparison/` 下按主题分文件（checkpoint-storage / subgraph-isolation / interrupt-resume / address-routing / conversation-history），每篇含三方对照表 + 我们的优势 / 不足 + 决策记录。**目的**：避免 spawn agent 重复研究三方源码、避免被 "建议照抄 LangGraph" 类报告误导忽略我们已有优势、保留历次决策上下文不断章取义。改完顺手把新发现的差异 incremental 写回对应文档，文档活的不会 stale

写之前就过这五步，比写完再 audit 早一拍——审计能找到的 bug，枚举能直接避免。

### 交叉审计流程

被问"还有 bug 吗"时按这套走一遍再答，不要凭"刚改完看着没问题"应付：

1. **自审**：按上面 9 类逐项过一遍，画时序图 + 所有权图
2. **独立 agent 复审**：spawn 一个 general-purpose agent 读相同代码、明确告诉它已审过哪些类别、要它专查没系统看过的场景。两边发现的 bug 取并集——agent 视角正交于自审，互相补漏。派单写法见下方「派审计别让它重做已做过的事」
3. **针对性 sanity revert**：每个 fix 配一个回归测试，**测试必须能在 fix 被 revert 时 fail 并指向 bug**。若 fail 信息含糊或测试仍过，重设计测试（常见原因：测试 setup 没让 old/new 行为分叉）。全文件 `git stash` revert 在测试引用了新 API 时会编译失败，用 Python script 只 revert 关键代码块更精确
4. **对照三方文档检查决策是否偏照抄**：拿 `doc/description/three-way-comparison/` 对应主题文档过一遍——独立 agent 报告"建议抄 LangGraph X"时核对我们已有优势是否被忽略；新决策没沉淀到决策记录表的话顺手补一行
5. **报告全部 bug + 自评估每个 fix 是否引入新 bug**——主动对自己的修复也做同样审计，避免修一个引入两个
6. **audit agent 报告必交叉验证**：找到 N 个 bug 不要直接修——逐条核实真假：行为类的写探针复现（在改动上跑红才算真，红的原因要正是报告说的那个），文档 / 注释类的按报告给的 file:line 对照代码核；复现不出来时先怀疑自己的 fixture 没搭对（第 6 类炸弹），再判误报。探针复现比再派一个 agent 读代码判断更硬，也省掉一整轮。Round 4-5 命中率 33-43%（67% 误判，多为"看到代码无显式防御就报缺陷"过度防御）。范围越宽（系统审多 ISSUE）误判越多；审 specific 问题（单 commit 范围）命中率高
7. **spawn 调研 / audit agent 时必须在 prompt 里禁止它再派生子 agent**（2026-07-31 实测教训）：
不加这条约束时 agent 会自己 spawn 子 agent 分头挖、再用 Bash 循环等待它们。父 agent 做完
自己那部分就把报告返回了（结果可用），但**它的下游会挂着继续烧 token**——实测一轮 9 个调研
agent 留下 18 个后台任务跑了 3 小时以上、单个 200k+ token，而且**父 agent 的 agentId 停不掉
它们**（子任务属于子 agent 自己的会话，`TaskStop` 用父 id 或名字都报 no task found），只能在
后台任务面板里手动逐个停。prompt 里加一句「禁止 spawn 任何子 agent，自己读完」即可。
⚠️ 另注：`TaskList` 查的是 todo 列表**不是后台 agent 列表**，别拿它核实「agent 停了没」。

**派审计别让它重做已做过的事**（2026-10-14：两个审计各跑了 20 多分钟还没交——清单里约三分之一早有哨兵，
两边又各自从零读同一批代码、从零搭测试脚手架）。审计要全面，但重复验证不是全面。派单时：
- **附已证清单**：不变量 → 哨兵测试名 → 哪个变异会让它红；明说这些不用再验，除非怀疑测试本身是假绿。
- **只列这次改动新带来的风险**，按高低排；低风险项一句依据带过，不写探针。
- **附代码地图**（关键函数的 file:line）和能直接复用的测试 helper，省掉找代码、搭脚手架的轮次。
- **时间盒**：约 15 分钟先交，报告分列已查 / 未查；要续就给同一个 agent 发消息（上下文还在），不重新派。
- **视角数按改动定**：每个 agent 仍只审一个视角（单 agent 多视角的漏审见下），但只派这次改动真正涉及的
  视角——只动一个子系统的单一风险派一个，改了全局约定才派两三个。

目标是一轮审计 10 到 15 分钟。

**单 agent + 多视角同时审 ≠ 多 agent + 单视角并发审**（R28→R29 实证）：R28 单 agent 同时审 6 视角自报"0 finding"→ R29 4 agent 单视角并发复审揭露 6 真 finding（含 3 真 HIGH + 自己引入的 ISSUE-74 regression）。**单 agent 多视角同时审命中率显著低**——视角间互相干扰、深度不够。审计纪律应优先"多 agent 单视角并发"，"单 agent 多视角"仅作粗筛。

经验上 N 轮审计找到的 bug 数量按几何递减但 ≠ 0：自审 0 个 → 用户追问 3 个 → 跨层 3 个 → agent 协作 6 个 → 专审未看场景 4 个 → R28 单 agent 多视角 0 个 → R29 多 agent 单视角 6 个（**多 agent 单视角能反向揭露单 agent 多视角的"假绿"漏审**）。**不要承诺"扫干净了"**——多层系统的 bug 总在你以为干净时冒出来；说"已知未发现，可以继续审 X / Y / Z 角度"更诚实。

**自己改的代码也要再 audit**：R29 视角 3 HIGH-1 是我修 ISSUE-74 commit `b07c140` 时引入的 input dup regression（`AddEvent(input)` 在 `runGraph` 之前但 buildInitialMessages 已被移到 preamble 节点跑——prior 取出含刚加的 input 又 append 一次）。**修复完成 ≠ 修对**——任何改动后必须按 9 类系统过一遍、必须独立 agent 复审、必须 sanity-revert 验证。**自己写的最容易盲**——R26 audit 实施 ISSUE-74 报"18 角度审 1 LOW 命中率 5%"——其实漏掉了我自己引入的 HIGH（R26 是我自己 spawn agent 审的，agent 没视角看到 input dup）。

### 强制执行机制（Stop hook + audit marker）

**项目本地配 `.claude/settings.json` Stop hook** 让 audit + cross-verify 不再依赖
LLM 自觉遵守——高风险 commit 后未做 audit 时 hook 返 `decision="block"` 让 Claude
**无法收尾**。

触发条件（`.claude/scripts/check-audit-stop.sh`）：
- 工作树 clean (已 commit)
- 最近 commit 改动文件**先过滤 `\.go$`**（`.md` 即使文件名含 channel/interrupt 也不触发），再 grep 命中: `session\.go|checkpointer|graph_loop|_agent\.go|sync|channel|interrupt`
- ⚠️ **`_test.go` 照样触发，这是故意的**：削弱一条哨兵的断言跟改生产代码同样危险（本仓实测：一次改动让 6 条既有断言从「钉契约」变成「钉新行为」，若无人复核就是静默失去防护）。**但纯测试新增**（只加哨兵、零生产代码改动）按下面「不 audit」那档处理——此时该做的是**如实说明**而不是标一个没发生的审计（marker 不带理由，假标记比没标记更坏：它让下一个人以为审过了）

强制流程：
1. 高风险 commit 后必须 spawn 单视角审计 agent 并发审（每个只审一个视角，个数按这次改动真正涉及的视角定，见「派审计别让它重做已做过的事」）→ `bash .claude/scripts/mark-audit.sh audit`
2. audit 找的 finding 必须 cross-verify (CLAUDE.md "审计纪律" 步骤 6) →
   `bash .claude/scripts/mark-audit.sh crossverify`
3. 两 marker 都标后才能 Stop

helper 命令：
- `bash .claude/scripts/mark-audit.sh status`：查最近 commit 状态
- `bash .claude/scripts/mark-audit.sh all`：一次标 audit + crossverify (audit + cross-verify 都做了)

marker 文件 `.claude/audit/<sha>.{audit,crossverify,commentaudit}.done` 在 .gitignore
不入仓库（本地工作流标记不污染 commit）。

**第二个 Stop hook：注释 drift gate**（`.claude/scripts/check-comment-audit-stop.sh`，
与 audit hook 并列配在 `settings.json hooks.Stop[]`）：最近 commit 改动的 .go 注释 + .md
**活文档**（排历史归档 issue/review/code——那里"已删/整文件移除"语境引用早删文件是正确
记录非 drift）经 `commentaudit` 第 1 层机械对账增量校验，有失效符号引用 / 失效路径行号 /
.md 里失效 file:line 代码引用 / 失效 markdown 链接（ERROR）时 block。出口同款 marker——
逐个**读语境**判真假后（历史/对照/eino 外部语境=假阳，裸引用不存在符号 / 失效 file:line /
失效链接=真 drift，真的改注释或文档引用）→
`bash .claude/scripts/mark-audit.sh commentaudit`。**第 1 层 ERROR 是候选不是判决**——
绝不见 ERROR 就改，必读语境（详见 comment-audit skill）。

**为什么 hook 而非 user memory**：feedback_audit_trigger_rule.md 是软约束 LLM 经常
忘（R31 ISSUE-79 / R32 ISSUE-80 都是事后补 audit 才发现自己引入的 race）。Stop
hook 是 harness 强约束**无法跳过**。

### commit-level audit vs round-level audit（R32 实证补充）

**两者互补不替代**：

- **commit-level audit**：commit 后立即 spawn audit。抓回归 + bisect 友好——bug 当场抓不潜伏跨多 commit。**但**视角窄看不到跨 commit 累积问题。
- **round-level audit**：一组 commit 完成后审。抓跨 commit 设计一致性 + 累积影响。**但** bug 跨多 commit 潜伏（如 ISSUE-74 regression 跨 R26→R29 才被发现）。

**触发规则**（项目实证）：

| commit 类型 | audit 策略 |
|---|---|
| 共享状态（sess / cp / channel / sync / 全局变量）| **commit 后立即 audit + race test** |
| 破坏性 API（签名变 / 默认行为变 / `feat!:`）| **commit 后立即 audit** |
| 并发原语（goroutine / WaitGroup / atomic / Mutex）| **commit 后立即 audit + race test** |
| 文档 / 拼写 / 删 dead code / 单点 bug fix（已配测试）| 不 audit |
| 一组相关 commit 完成（feature / ISSUE 落地）| **round-level audit** 抓设计一致性 |

**R32 实证案例**：commit `7c9c7db`（ISSUE-79 fix）我自己引入 race condition——
NewSharedChildSession 让父子 child sess 共享 values map 但 mu 是 per-sess 独立 →
race detector 100% 触发。如果 commit 后立即 audit + race test 当场就抓到，等到
R32 round-level audit 才发现 → 多 1 个 commit + 多次用户追问的成本。

**race 测试必须用真实 -race flag 跑**——不是逻辑推理"map 应该 race-free"。改 sess /
channel / map / sync 相关代码时 commit message 必须含 "go test -race PASS" 才算修完。

**用户监督价值**：人在 loop 里抓 bug 比 LLM 自审快得多。R31 ISSUE-79 我加 opt-in
违反 CLAUDE.md "项目未上线不留兼容路径" 规约——用户一句"这是干嘛的"5 秒抓到 50 行
违规。**遇到设计选择优先问用户而非脑补**——比如默认行为是否变化、是否加 opt-in、
是否加防御性 nil check。这些 LLM 写时容易"按训练数据 pattern 套"——明确询问用户更可靠。

## 文档撰写纪律

**写文档前必调 `doc-writing` skill**——内容已迁到项目 skill `.claude/skills/doc-writing/SKILL.md`
（写 vs 审两种认知模式 / 6 类系统性偏差 / 5 条 grep 必修 / 多轮 audit 必需 / 跨文档同病
防治 / 标准工作流）。从 CLAUDE.md 抽出原因：always-on token 占 ~107 行但仅写文档时
需要——progressive disclosure 让条件性知识按需加载。

**触发场景**（应主动调 skill）：
- 写新设计文档（`doc/v2/*.md` / `doc/description/*.md` / README）
- 改架构 / 设计 / 注释类长文档
- 写超过 30 行 godoc 块
- 用户说"写文档/补文档/更新架构/写注释"

**核心**：LLM 写文档系统性偏差（v2 文档体系审 5 轮累计 100+ HIGH，context-compaction.md
单篇 42 HIGH）—— 自觉准确率 ~0%，必须按 skill 流程。

**未完成项进中心表，别埋设计文档正文**（2026-07-03 doc 治理教训）：设计文档正文的「待定 /
待办 / 第一版不做 / 未核实 / 倾向不加」等 marker 是 stale 重灾区——决策做了不回改 marker，
读者读正文误判特性还在设计中（fallback §11「待定」8 条里 5 条早已决/已实现却没同步；
observability「🔥 实施中」其实早完工）。规约：
- **真开放项**进中心表（`doc/code/todo.md` backlog / `doc/issue/issues.md`）带 doc 反向链；
  设计文档正文只留一句「见中心 backlog」指针，不在正文重复描述开放状态。
- **决策一旦做出**同步清 marker（「待定」→「已决 + 代码位置」/「非目标」），别留对话、别埋正文。
- **定期扫**（doc 治理 / 合大 feature 前）：`grep -rniE "待定|待办|第一版不做|未核实|尚未|倾向不(加|做)|待实施|TODO|未实现" doc/ --include="*.md"`
  逐条核「已决(改)/ 非目标(标清)/ 真开着(进 backlog)」——上次"整理未完成工作"只翻 todo/issues、漏了设计文档正文埋点。

## 注释/godoc 校验纪律

**校验存量注释/godoc 是否与代码一致前必调 `comment-audit` skill**
（`.claude/skills/comment-audit/SKILL.md`）——注释是文档错误的**上游**（v2 文档体系
100+ HIGH 很多从注释 drift 起，错的 godoc 被抄进 README/architecture）。与 doc-writing
互补：doc-writing 管"写时防偏差"（生成模式），comment-audit 管"事后校验存量"（判别模式）。

**触发场景**（应主动调 skill）：
- 重命名/删除导出 API、大规模重构后（注释最易 stale）
- commit 改动含大量注释/godoc
- 用户说"校验注释/审注释/查注释 drift/godoc 准不准/注释和代码对不对"

**三层方法**：第 1 层 `commentaudit` 脚本（`go/ast` 机械对账：失效符号引用 / 数字声明
冲突 / 失效路径，可挂 Stop hook gate）→ 第 2 层契约 vs 实现（LLM 读语境）→ 第 3 层
语义偏差（引用 doc-writing）。**引用外部包符号必须写 qualified 形式**（`pgx.QueryExecModeExec` / `pgtype.planEncode`，不是裸的 `QueryExecModeExec`）——工具的符号表只覆盖本仓，裸标识符会被当成本仓符号查而报假阳；它对「首段像包名」的 qualified 引用有专门的跳过分支。仓内既有写法是把整个调用链放进一对反引号（如 `json.NewDecoder(r).UseNumber().Decode(&v)`）。**同一 session 内因为这条踩过三次**，而且 qualified 形式对读者也更有用（知道去哪个包找）。

**核心铁律：第 1 层机械层只产"候选"，判真假必须读语义
语境，绝不单独判错**（否则就是"看代码无显式防御就报缺陷"的过度防御误判，同 audit
67% 误判病）。**工具准确率必须换没调过参的项目盲测**——只在本仓调出"0 假阳"是过拟合
（eino/net/http 盲测各 0 假阳才可信）。

## 业务自实现 backend 必跑 conformance 套

业务接 PG/MySQL/Redis 等自定义 backend 实现 `store.Store` / `compose_v2.Checkpointer`
接口时**必跑 conformance 套**验证语义合规:

```go
// Store backend conformance
import "crew-go/store/conformance"
func TestMyRedisStore(t *testing.T) {
    conformance.RunStoreTests(t, func() store.Store {
        st := mybackend.NewRedisStore("redis://localhost:6379")
        t.Cleanup(func() { _ = st.Close() })
        return st
    })
}

// Checkpointer backend conformance
import "crew-go/compose_v2/checkpointer_conformance"
func TestMyPGCheckpointer(t *testing.T) {
    checkpointer_conformance.RunCheckpointerTests(t, func() compose_v2.Checkpointer {
        cp := mybackend.NewPGCheckpointer("postgres://...")
        t.Cleanup(func() { _ = cp.Close() })
        return cp
    })
}

// A2A 服务目录 backend conformance（Nacos/etcd/Consul 等自实现 registry.Directory）
import dirconf "crew-go/a2a/registry/conformance" // alias 防与 store/conformance 同名碰撞
func TestMyNacosDirectory(t *testing.T) {
    dirconf.RunDirectoryTests(t, func() registry.Directory {
        d := mybackend.NewNacosDirectory("nacos://...")
        t.Cleanup(func() { _ = d.Close() })
        return d
    })
}

// 会话历史 backend conformance（业务自实现 Redis/PG 等 agent_v2.HistoryStore）
import "crew-go/agent_v2/history_conformance"
func TestMyRedisHistory(t *testing.T) {
    history_conformance.RunHistoryStoreTests(t, func() agent_v2.HistoryStore {
        hs := mybackend.NewRedisHistory("redis://localhost:6379")
        t.Cleanup(func() { _ = hs.Close() })
        return hs
    })
}
```

**conformance 覆盖什么** (8 轮 audit 累计抓 ~130 真 finding 加固):
- Store 6 method 全字段 round-trip / namespace 隔离 / 跨 ns 防泄露 / 并发安全 /
  6 比较 op / **filter 语义三条**（范围 op 的 filter 值非数值 ⟹ `ErrFilterTypeMismatch` 且空 ns 也报 /
  N=2 异构 fixture 下**类型对不上的行不命中、不报错**（type bracketing，`$lt 10` 只返 n5——钉死「CAST 成 0」
  「true 当 1」「null 最低」三种反命中）/ `$eq`·`$ne`·直值是 JSON 语义相等——10 == 10.0、true != 1、
  数组逐元素**不是包含**、`$ne` 含 JSON null 行不含缺失行）/ TTL 可选 capability / sibling prefix
  false-positive (业务 PG `LIKE 'x%'` 经典坑) / **namespace 与 key 逐字节是身份**（只差尾随空格、
  大小写、重音或组合字符写法的两个也是两份数据——列用 MySQL `utf8mb4_bin` 的 PAD SPACE 或默认的
  `utf8mb4_0900_ai_ci` 会串行）/ **ListNamespaces 按
  `String()` 字节序**、带 maxDepth 时分页取在截断去重之后（PG 列不声明 `COLLATE "C"` 会跟库的排序
  规则走）。**客户端过滤后端直接用 `store.ValidateFilter` +
  `store.MatchFilter`**（唯一实现，别抄）；下推型后端照 `sql` 三个 dialect 的 `JSONField*` 形态
  （PG `CASE`+`jsonb_typeof` 不能用 AND、MySQL `JSON_TYPE` 列表必含 `UNSIGNED INTEGER`）
- Checkpointer 8 method 全字段 round-trip (含 PendingInterrupts.InterruptID HITL
  稳定 ID / NodeVisits zero-value / **CompletedFrontier 顺序敏感 + DeferredSends
  全字段含 Owner + PendingSendWrites**（R33 PA-H1 + ISSUE-95#1 + F-B —— 三者漏序列化
  **都不报错**，只是分别退化成「成功分支后继静默丢」/「Target 双发」/「SendFrom target
  读到零值」，是最隐蔽的一档）/ Channels round-trip) / **GraphID conflict 以存档里
  最新那份为准**（放行的写入连 GraphID 一起整份替换、换成空也照换，被拒的不动存档；判定与写入
  原子——两个不同 GraphID 并发写同一 (sid, ns) 恰好一个成功，新键与已有的行分开测（已有的行上全靠先锁住那一行再判）。GraphID 另存一列的后端，那一列只能是
  存档的副本、每次一起写：sql 曾让列在第一次写入后冻结，写入侧按列拒的正是载入侧按存档放行的图）/
  跨 sid 隔离 / SaveBatch partial failure / Cleanup keepLastN 排序 /
  **DeleteSubtree 范围**（自身 + 经 `/` 往下的后代；`#`/`@` 兄弟执行必须存活——照旧语义
  「连同实例一起删」实现的后端在 `DeleteSubtree_Sibling_Executions_Survive` 红，那样的后端
  会让一次执行的成功清理删掉兄弟执行还挂着的存档，`err == nil`）/ **(sid, ns) 逐字节是身份**
  （`KeyIdentity`：只差尾随空格、大小写、重音或组合字符写法的两个会话 / ns 是两份档，键列用非二进制
  排序规则会串档、删一个带走另一个）
- HistoryStore 10 子测试: Append/Load round-trip / **append-only**（逐字重复的两条都得留，
  不能去重或覆盖）/ 顺序（历史是时序回放，乱序即失真）/ **Owner + ConvID 两个维度都要区分**
  （只按 ConvID 分 = 把别人的对话拉给当前用户）/ ToolCalls·Annotations 字段完整（配对断了
  模型会重复调用、归属丢了不知谁说的）/ 空会话返 (nil,nil) 不是错误 / 空批 no-op /
  Load 返快照非内部引用 / 并发读写
- Directory 15 子测试: 注册/发现 round-trip (Weight *int 按值比 + agentName) / 缺省值
  (agentName 缺省=serviceName) / ns+service 隔离 / 同 service 多 agentName 聚合发现 / 同
  instanceId 不同 agentName 共存 (§4.3 4 元组判定) / Deregister 幂等 / UpdateStatus + 非法
  status 拒 / 非法 payload 拒 / 默认 ns 归一 / Metadata 快照隔离 (改返回值不污染存储) /
  重复注册返 ErrInstanceExists (§4.3)

**conformance 不覆盖** (业务自负):
- 语义搜索 (Search WithQuery) — 业务 mem0/向量后端自测准确性
- 跨进程 wire format (PG JSONB / Redis bytes) — 业务 backend serializer 自测
- 性能 / 吞吐 — 业务自压测
- Directory binding 特有语义 (malformed 节点跳过 / path↔payload 一致性 / ephemeral
  断连消失 / Base64url 路径编码) — 需 binding 自测能注入裸节点, 见
  `a2a/registry/conformance` 包 godoc (协议规范 [doc/description/a2a-registry-discovery-spec.md](doc/description/a2a-registry-discovery-spec.md))

## 项目特定约定

- **项目未上线，不考虑向后兼容、旧使用者、迁移路径**。设计/重构时直接按最优解走，破坏性改动随便提；不要为"防存量用户踩坑"加 warn / deprecated / fallback 路径——那些都是脑补的不存在场景。需要重命名/删 API 直接动手，所有调用点同步改完即可
- 不修改 `agentdock-spec` 里的任何文件
- `modules/` 目录存放从 `agentdock-spec/go-runtime` 拷贝过来的 spec 生成类型，不直接依赖外部模块
  - 以 spec 设计为参考，但不完全照搬：若有更优的 Go 原生设计，直接在 `modules/` 里优化
  - spec 更新时手动同步，有差异的地方以我们的设计为准并注释说明原因
  - **同步 spec 时只看 `.tsp` 文件和 `go-runtime/` 目录，忽略 `java-runtime/` 和 `python-runtime/`**
- `schema` 包的类型是 `modules/common` 的 type alias，不另起炉灶
- 编排模式统一编译为 Graph，不自己维护循环
- **Hook 消费语义**（按字段分档，**非"所有 hook 一律可改"**——权威表见 `hook/hooks.go` Hooks 接口 godoc）：hook 入参虽是指针，但"改了是否生效"取决于框架是否在该 hook 后消费该字段。两档：(1) **[消费]** 改写生效——`OnAgentStart`/`OnModelStart` 的 `input.Messages`、`OnModelEnd` 的 `output.Message`、`OnModelEndStream` 的返回 reader（不得为 nil：返回 nil 或报错都让这次模型调用失败）、`OnToolStart` 的 `input.Name`/`input.ArgumentsJSON`（只在这一批调用第一次交给工具执行时生效：续跑沿用那一次定下的内容、改写不生效——用户答过的、审批批过的、已执行过的都是那一份，见 `agent_v2.fixToolCallContent`；定下的内容随中断档落盘，凭证 / 短时令牌 / trace_id 这类每次都要新取的值放钩子返回的 ctx 或用工具中间件注入，别写进参数；改写让调用出错时修好钩子也救不回这一批，只能放弃任务重开）、`OnToolEnd` 的 `output.Result.*`（框架读改写值喂下游工具调用 / 写回 LLM 历史；`Success` 还决定这次调用做出的转交 / 路由算不算、框架控制工具短不短路——整体替换 Result 时 `Success` 零值为 false，按失败处理、模型看到失败格式）；(2) **[只读]** 改写不生效——`OnModelStart` 的 `Tools`（恒 nil 未填充）、`OnModelEnd` 的 `Usage`、`OnToolStart`/`OnToolEnd` 的 `CallId`（tool_call↔tool_result 配对锚点，改它破坏 OpenAI 422）、`OnAgentEnd`、`OnError`（观测/统计/锚点字段，by design 无下游；tool/model 层对齐 eino observation-only）。ctx 返回值链式交给下一个 hook；整条链返回之后，框架用开始一侧（`OnAgentStart` / `OnModelStart` / `OnModelEndStream` / `OnToolStart`）的返回值，**只用于它包住的那次调用**——这次 Run、这次模型调用、这次流式读取、第 i 个 tool_call 的执行（工具体、中间件、它调起的嵌套 agent），同一次的结束钩子收的也是它；结束一侧（`OnModelEnd` / `OnToolEnd` / `OnAgentEnd`）与 `OnError` 的返回值不往后流（曾经 OnToolStart 的返回值赋回整批共用的 ctx：b 的钩子收到 a 放的值、a 的工具体带着 b 的值在跑，实测；为此 `BasicAgentToolFn` 多了一个 `callCtxs []context.Context` 参数，每个调用一个 ctx）。采纳经 `compose_v2.CallHook`（放标记、调、采纳收在这一处）：框架的身份与位置一律取框架的；返回的 ctx 从入参派生时，业务值、调用方可配的（环境、软取消）与取消都听钩子的，外来的（存下来的、`Background`）只借业务值（框架分不出 span 与凭证，跨请求存 ctx 会带进别的请求的凭证；每个采纳点首次采纳外来 ctx 时告警一次、带采纳点位置）；可配项从它之后才读的地方生效，本层钩子之前已定下的不变。用户实现 hook 可纯观测也可改写，但改写仅在 [消费] 档字段生效。

## doc 目录说明

| 目录 | 用途 |
|---|---|
| `doc/description/` | 项目架构与功能模块说明（[architecture.md](doc/description/architecture.md) 总入口 / [smoke-test-recipes.md](doc/description/smoke-test-recipes.md) 必跑冒烟 / [testing-conventions.md](doc/description/testing-conventions.md) 测试规约 / [three-way-comparison/](doc/description/three-way-comparison/) 三方对照决策记录）|
| `doc/v2/` | redesign/channel-based 演化历史 + v2 速查（[migration-history.md](doc/v2/migration-history.md) **新 session 优先读**）+ 设计原理（channel-based-design.md）+ 业务侧用法（usage-guide.md）+ PoC 决策溯源（poc-report.md）|
| `doc/code/` | 代码改动记录与待办（[todo.md](doc/code/todo.md)）|
| `doc/review/` | 历次 Code Review 结果与问题清单 |
| `doc/issue/` | 已发现问题的跟踪列表（含 v1 时代历史 ISSUE-40~72）|

---

# 架构摘要（跨 session 快速上下文）

> 总入口 + 详细导航见 [doc/description/architecture.md](doc/description/architecture.md)；
> v2 完整公开 API 速查见 [doc/v2/migration-history.md](doc/v2/migration-history.md)；
> 已知 bug / 历史决策 ISSUE 见 [doc/issue/issues.md](doc/issue/issues.md)。

## 项目定位

Go 原生 LLM Agent 编排框架。参考 LangGraph Pregel + eino adk + agentdock-spec 设计，不做跨语言 codegen，面向 Go 生态保持简洁。所有编排（BasicAgent ReAct + Sequential / Parallel / Loop / Plan / Reflect / Routing / Handoff / GraphAgent）编译为同一个 channel-based BSP 图执行引擎。

测试用 LLM：**默认 qwen `NewTestClient`**（`model/qwen`）。**模型名用 `qwen.TestModelCandidates`**（带日期的固定快照在前、滚动别名兜底；`qwen3.6-flash` 免费额度已实测耗尽，新用例别照抄）——额度耗尽想自动切下一个就用 `fallback.New` 把候选串起来（`fallback` 认得 dashscope 403 `AllocationQuota.FreeTierOnly`，范式见 `example/compose/identity_guard_smoke`）。baseURL = `https://dashscope.aliyuncs.com/compatible-mode/v1`（阿里云 dashscope，兼容 OpenAI API）。API key 走项目根 `.env` 的 `QWEN_API_KEY`（`.gitignore` 已排除，`example/envloader` 自动加载）。**DeepSeek（`model/deepseek`，`deepseek-v4-flash`，`DEEPSEEK_API_KEY`）仍可用作备选**（`observability_demo` 等 `-provider deepseek|qwen` 两路保留）。qwen `NewClient`（显式传 key）+ `model/openai`（无 default URL）不用作测试。

## 包依赖方向

```
schema ← model/tool/hook/skill/mcp/prompt ← compose_v2 ← agent_v2 ← a2a / a2a/server / agui / decision
a2a/registry（纯 stdlib 叶子）← a2a / a2a/server / a2a/zk（独立 go module，隔离 go-zookeeper + testcontainers）
agui（AG-UI 前端协议适配，叶子集成层）← agent_v2 + hook + compose_v2
internal/panicerr（只依赖标准库：兜住 panic 时取栈、挂 PanicError、记日志）← compose_v2 / agent_v2 / a2a / agui / mcp / model/openai / observability / schedule
```

## 核心类型速查

> 完整 v2 公开 API + 业务用法见 [doc/v2/migration-history.md](doc/v2/migration-history.md) §v2 核心类型速查 / [doc/v2/usage-guide.md](doc/v2/usage-guide.md)。本表只列**最常用**的高频 API。

| 包 | 关键类型 / API | 说明 |
|---|---|---|
| `schema` | `Message`, `StreamReader[T]`, `MessageRole` | `Message` / `MessageRole` 是 spec 生成类型（`modules/common`）的 type alias；`StreamReader[T]` 是 `schema/stream.go` 自己定义的泛型流（spec 里没有对应类型） |
| `model` | `ChatModel` 接口, `APIError`, `ErrStreamUnsupported`, `ErrContentFiltered`, `ErrContextWindowExceeded` | `Generate` + `Stream` + `WithTools`，openai/qwen 实现；**流式只有收到 finish_reason 才算输出完整**：openai 客户端把读出错、调用方 ctx 取消、错误帧（data 里带 error 键或 `event: error`）、data 不是 JSON、结束原因 `content_filter`（`ErrContentFiltered`，不可重试）、`model_context_window_exceeded`（`ErrContextWindowExceeded`，不可重试，文本含 context length exceeded、第 3 层压缩认得出）与 DeepSeek 的 `insufficient_system_resource` / `aborted` 等、流结束时没收到结束原因，都作为流的错误交出，不当正常结束（只闭流的话消费方读到 io.EOF，会把半截当完整答复）；Generate 按同一套结束原因判断；SSE 按规范解码，只有流末尾没空行的最后一帧照常处理（规范要求丢弃，丢了会把放在那一帧的 finish_reason 当截断）；不发 finish_reason 的 provider 用 `Preset.OptionalFinishReason` 声明；不支持流式的实现让 Stream 返回 `model.ErrStreamUnsupported`（或 nil reader；不接受 stream=true 的端点用 `openai.WithStreamUnsupported` / `Preset.StreamUnsupported` 声明），BasicAgent 只在这时改调 Generate，其余建流错误原样交给重试与 fallback；BasicAgent 消费流时只有生产方交出 io.EOF 才算结束，流被别人半路关掉（`schema.ErrStreamCanceled`，如钩子起的超时保护）按错误处理（钩子要截断并把已有内容当完整输出，就在自己的 Recv 里返回 io.EOF）（曾经一律改调 Generate：429 不看 Retry-After 立刻再打，配了 fallback 时整条候选链再走一遍）；`APIError`（Provider/StatusCode/Body/Err/**RetryAfter**）是 provider HTTP 调用结构化错误（fallback 分类 / ReactiveCompact 检测复用，openai client 回填）；`RetryAfter` 由 client 解析 HTTP `Retry-After` 头回填（RFC 9110 两形式：秒数 + HTTP-date；过期/不可解析填 0），经 `compose_v2.RetryAfterProvider` 被重试层读取；`WithAllowedTools(names...)` 工具白名单（model.Option，openai client 裁 `req.Tools`，provider-agnostic，配 ToolChoice 可强制；见 tool-visibility.md）；**多模型适配**（Tier-A 增删=改数据）：`openai.Preset`+`NewCompatible` 统一 OpenAI 兼容 provider 装配 / `model.WithExtraBody`(per-request provider 扩展逃生舱)+`deepseek.WithThinkingOption`·`qwen.WithEnableThinkingOption`(按次 thinking) / `Message.ReasoningContent` 一等字段(thinking 多轮回传，ISSUE-89) / agent 层 `agent_v2.WithRunModelOptions`·`WithResumeModelOptions`(per-Run 透传)；详见 [model-adaptation.md](doc/v2/model-adaptation.md)|
| `fallback` | `FallbackChatModel`, `New`, `FailoverPolicy`, `DefaultFailoverPolicy`, `ErrAllMembersUnavailable` | model.ChatModel 故障切换装饰器（有序候选 + per-member 断路器 + `ShouldFailover` 按 StatusCode 分类 + per-member 重试 + 仅建流阶段切 Stream）；**「在这个候选上重试」与「换下一个候选」是两个判断**：重试只认会自己恢复的错误（5xx / 429 / 408 / 网络 / 不认识的错误；按时间窗口的配额是限流、照样重试），计费类额度用完、其余 4xx、声明了 `compose_v2.NonRetryable` 的不重试（额度用完照样切换；policy 返回 false 只表示不换、不表示不重试）；**要等很久的候选先推迟**：Retry-After 超过 60 秒、且切换判据允许换时，记下它最早能再请求的时刻去试别的候选，**每次挑此刻能请求的里排序最靠前的、都要等时等最早能请求的那个**——推迟不是放弃，别的都没成时回来把重试用完；要等的时间等完已过调用方期限的不等（错误交给切换判据）；推迟改了「先等再换」的次序，四条不变量保证它不比原地等更差：服务端说「T 之前别来」对所有调用一视同仁（长 Retry-After 给候选设共享的门，`WithTools` 派生的共用，T 之前谁都不请求它：断路器闭合时当推迟，熔断中或等不到 T 时跳过，没请求过它的调用报「门还要关多久」、请求过的以自己的错为准；改去请求别的候选照常发切换事件；门不是失败、不记断路器）；真失败当场记、一次调用里一个候选至多记一次；回到推迟的候选不再经过断路器（同原地重试，回来前先看门；只被门挡过、没请求过的照常经过断路器）；候选 i 报「换候选没用」时排在它后面的不再试、前面推迟着的照样回去，最终失败返回排得最靠前的那个（2026-10-20 第四版推翻：推迟当场记失败让并发调用把候选熔断，之后到达的直接跳过它，20 个并发只成功 3 个；曾经共用 `ShouldFailover` 一个判据：额度耗尽先在同一个候选上重试白等，10 分钟的 Retry-After 干等到调用方超时；后来换走就不回来，后面的候选失败时立刻失败，都是实测）；`fallback.New([]model.ChatModel{...})` 注入 `WithModel` 对 9 编排透明。Stream 遇到不支持流式的候选（`model.ErrStreamUnsupported` 或 nil reader）就地用它的 Generate、包成单项流（顺序不倒退、不记断路器失败；交回调用方改调 Generate 的话会从第一个候选重走，刚失败的候选立刻又被打）；BasicAgent 对只有一个片段的流整条采用、不做增量拼接，拿到的跟直接调 Generate 一样（拼接会丢消息 ID、结束原因等，还把同一 ID 的两个工具调用并成一个；OnModelEndStream 钩子改了片段数时照常拼接）。候选的 Generate 返回 nil 消息按候选故障处理，两条路一样。默认策略对 `model.ErrContextWindowExceeded` / `model.ErrContentFiltered` 不切换、不重试：跟 HTTP 400 报的同一类错误同一个答案（它们包在 StatusCode 为 0 的 APIError 里，按状态码会被当成网络错误逐个重试）。**控制流信号（interrupt/ctx）由框架层 guard 在业务 policy 之前无条件豁免**（`New` 构造期 `guardedPolicy` 包装 `WithFailoverPolicy` 注入的 policy，复用 `compose_v2.IsRetryFutile`——按调用方 ctx 判，不按错误链上有没有 ctx 错）——业务自定义 policy 漏写豁免也不吞中断（不切换/不重试/不误计断路器）。独立成包（import model+compose_v2 避免分层成环）。详见 [doc/v2/model-fallback.md](doc/v2/model-fallback.md) |
| `tool` | `Tool`, `Toolkit`, `FuncTool`, `StreamableTool` | 工具接口与注册表 |
| `hook` | `Hooks`, `NoOpHooks`, `ChainHooks` | 8 个生命周期回调；NoOp 嵌入 / Chain 串联 |
| `skill` | `SkillBox`, `DefaultSkillBox`, `NewInMemorySkillBox`, `SkillSource`, `SkillContent`, `ParseSkillMD`, `SkillDescriptor{DisableModelInvocation, UserInvocable, Context, Agent, Model, ...}` | skill 容器；扫描 rootDir/*/SKILL.md，关键词匹配；frontmatter 二维（P2.A）**强制程度不对称**：`disable-model-invocation` 默认 false，**框架强制**（下述三处 + skill action 三处）；`user-invocable` 默认 true，**框架不强制**——它管的是 user-facing 列表，而框架不拥有那个面（无 slash command / CLI），业务自己过滤（`SkillDescriptor` godoc 有说明）。同理 `allowed-tools` 解析但不强制（三条执行路径下都没有良定义语义，见其 godoc；按轮裁工具用 `WithToolFilter`）；执行模式（P2.C/B）：`context: inline\|fork` + `agent: <name>` + `model: <name>`；**skill action**（确定性执行，`RunSkillAction`，仅 go-func 进程内 `WithSkillActionFunc(skill, desc, fn)`）：LLM 经 `run_skill_action` 工具触发（`agent_v2.RunSkillActionToolName`，`boxHasActions` 条件注册、与 `activate_skill` 职责正交不冲突、DisableModelInvocation gate）；`SkillExecutionResult{ExitCode,Stdout,Stderr,...}` 结构化。**脚本型 action（python/bash/node + `ScriptExecutor` 沙箱 seam + frontmatter `actions:` 解析）2026-06-30 已删**——spec 占位零 consumer，脚本执行收敛 tool 层 open-bash（见 [doc/v2/skill.md §11](doc/v2/skill.md)）。**内存装配**（远程 skill 源复用）：`NewInMemorySkillBox(name, []SkillContent{Descriptor,Body,Dir})` 从预解析内容装配、复用 DefaultSkillBox 匹配/激活/fork/净化（不扫本地 FS，ListSkillResources 返空）；`ParseSkillMD` 导出供远程源解析 SKILL.md 原文（含净化）。详见 [doc/v2/skill.md §11](doc/v2/skill.md) |
| `mcp` | `Client`, `Hub`, `RemoteTool` | MCP Server 集成（StdioClient / StreamableHTTPClient + Hub 聚合） |
| `prompt` | `Template`, `FromMessages`, `System`/`User`/`Assistant`/`Placeholder`, `Format` | 轻量提示词模板（叶子包，仅依赖 schema）：`text/template` 变量绑定 + 消息列表占位（`Placeholder`）→ `[]*schema.Message`；BasicAgent 经 `WithPromptTemplate` 集成（preamble 渲染 prepend）。见 prompt 包 godoc / tool-visibility.md |
| `sandbox` | `Sandbox` 接口(`Exec(ctx,cmd)→*ExecResult`), `NewBashTool`, `BashToolName` | 远程命令执行沙箱**可插拔 seam**（tool 层，接口在使用方）：一个 `Exec` 撑全部（read/write/ls 用 shell 拼），后端收敛权限/隔离/超时/认证；`NewBashTool(sb)` 包成 LLM 的 `sandbox_bash` 工具（单一沙箱入口，非零 exit 标注回灌）。**框架不带内置后端**，业务实现 `Sandbox`；远程 skill 源用 `skill.ParseSkillMD` + `skill.NewInMemorySkillBox` 装配。**sandbox_bash 输出不可信数据框定**（`<sandbox_output_XXXX>` 随机 nonce 标签 + 描述声明，防 file-content 注入，与 skill-metadata `sanitize` 正交，软层；硬隔离靠后端）。权威文档 [doc/v2/sandbox.md](doc/v2/sandbox.md) |
| `decision`（子包 `decision/jev`、`decision/bailian`） | `Client` / `(*Client).Decide`，`Protocol`（`ProtocolSystemOne` / `ProtocolOpenAIDecisions`），`Answer.Refused`，选项 `WithBaseURL` / `WithModel` / `WithHTTPClient` / `WithAttemptTimeout` / `WithMaxRetries` / `WithBackoff` / `WithRetryBudget`，`Choice` / `Score` / `Noul`，`NewRouteDecider(client, opts...)`（`WithInstructions` / `WithStateFunc`），`NewClient(apiKey, Preset, opts...)` + `Preset{Provider, BaseURL, DefaultModel, RequestIDHeader}`；`jev.NewClient(apiKey, opts...)`；`bailian.NewClient(apiKey, region, workspaceID, opts...)`（`bailian.Beijing` / `bailian.Singapore`）| **决策模型接入**（不生成文本，对选择题 / 打分题 / 是非题直接给判定与概率）。**用哪家由调用方选**：TypeSafe 的 Jev（`jev.NewClient`）或阿里云百炼的 `decision-model-preview`（`bailian.NewClient`，请求发到 `https://{业务空间ID}.{地域}.maas.aliyuncs.com/compatible-mode/v1/systemone`，key 用百炼的 API key）。`decision.Client` 还支持 OpenAI Decisions API 的线上格式（`Preset.Protocol` 设 `decision.ProtocolOpenAIDecisions`：questions 是带 name 的数组、是非题叫 predicate、能单题拒答；Jev 与百炼直连只有 System One）；`Request` / `Response` / 路由决策方只有一套，客户端收发时换格式，拒答记进 `Answer.Refused`、路由决策方据此弃权。State 与说明都是文本、是非题不带说明时两种格式实测答案逐项相同，对象形输入下两种格式交给模型的文本不同（System One 下由服务端转，百炼把对象转成「键: 值」；OpenAI Decisions 格式下客户端转成 JSON），结果有出入（见 route-decider.md §5.2）。两家是同一个协议（TypeSafe System One），构造出的都是 `*decision.Client`，校验、重试、路由决策方完全一样；差别（地址、模型名、key、请求 ID 所在的响应头：Jev 是 `x-typesafe-request-id`、百炼是 `x-request-id`）收在各子包的 `Preset` 里，服务方标识与请求 ID 头不随 `WithBaseURL` 变，结构同 `model/openai` + `model/qwen`。曾经整包叫 `jev`、百炼客户端也是 `*jev.Client`，读起来像「百炼的模型就是 Jev」，2026-10-14 拆成这样。`Client` 覆盖 System One 与 OpenAI Decisions 两种线上格式，重试照官方 Python SDK 的语义自写（408 / 429 / 5xx / 网络层重试 2 次、遵守 retry-after-ms / retry-after、单次 10 秒超时、**一次调用总预算 30 秒**）——不借框架的通用重试，那边照 Retry-After 等且不设上限，一个 retry-after: 3600 就能把路由挂一小时。下一次重试前的等待（服务端要求的或指数退避）不短于 ctx 剩余时间时不等、直接返回上一次的错误（等完之后的请求仍可能撞上 deadline，路由里要保证升级就用派生 ctx 包住决策方）；预算只管还要不要再发，最坏耗时约为预算加一次单次超时，路由里要硬上限就在决策方外面用派生 ctx 包一层。错误用 `model.APIError`；请求与响应都按服务端 OpenAPI schema 与官方 SDK 的规则校验（按编成 JSON 后的形状判，不限定 Go 类型；`Answer.Legend` 的值必须是文本、对象或数组，别的形状整份响应报错；服务端新增的答案类型照官方 SDK 跳过、不作废整个响应；每个答案只解它那一类的字段；概率为 null 报错不当 0），本地校验失败与成功状态但响应体坏了或缺必填字段的错误都不可重试。`NewRouteDecider` 把它包成 `agent_v2.RouteDecider`：候选分支是选项、分支描述是选项说明，输入没有文本时弃权、不发请求。**置信度很接近 (p_max − 1/n)/(1 − 1/n)**，n 是候选数，两家都是（p_max 小于 1 的样本上，Jev 平均差 0.005、最多 0.019；百炼 26 次平均差 0.004、最多 0.010）：主要由最高概率决定，前两名差距影响很小，候选越少同一 p_max 对应的置信度越低。默认问题描述带防注入句（实测不带时冒充系统通知的注入以 0.86 置信度成功，门槛拦不住）。中文 458 条实测与评测程序见 `example/decision/routing_eval`（两家都接了）：**百炼 79.5%、Jev 99.3%**，百炼置信度 ≥ 0.85 的 273 条错 16 条（Jev 440 条全对），判错多数选了「其他」；百炼升级链（门槛 0.85 交给 qwen）95.6%、四成请求要升级；**防注入句对百炼基本无效**（7 条注入样本照办 4 条、最高置信度 0.98，Jev 0 条）。可运行演示 `example/decision/routing_demo`（REPL，`-provider jev|bailian`、`-shadow`）。**改了请求校验、响应解析或某一家的接入就跑 `example/decision/client_smoke`**（对真服务端一次问三种题型 + 请求不存在的模型应报 4xx 且不重试 + RoutingAgent 挂路由决策方；`-provider jev` 需 `TYPESAFE_API_KEY`，`-provider bailian -workspace llm-xxx` 用 `DASHSCOPE_API_KEY` 或 `QWEN_API_KEY`）——单测全是 httptest 桩，桩照我们对 schema 的理解写，理解与真服务端不一致时单测照样全绿。权威文档 [doc/v2/route-decider.md](doc/v2/route-decider.md) |
| `compose_v2` | `GraphBuilder`, `NewBuilder`, `Graph`, `NodeFunc`, `Run`, `Compile` | 图引擎核心：`NodeFunc func(ctx, Channels) ([]ChannelWrite, error)`；`Run(ctx, g, cm, opts...)` 单一执行入口（含 Resume）|
| `compose_v2` | `NodeOption`, `WithNodeComponent`, `WithComponentKind`, `GetComponentKind` | `AddNode(id, fn, WithNodeComponent("retrieval"))` 声明 node 组件类型（Phase 2 GraphAgent 显式编排 trace span kind）；`WithComponentKind`/`GetComponentKind` 是 ctx 透传协议（graph_loop 进 node consume set，observability OnNodeStart 读；agent span 不标 component）|
| `compose_v2` | `CondOption`, `WithCondFailPolicy`, `CondFailPolicy`(`CondFailAbort` 默认 / `CondFailRerunFrom`) | `AddConditionalEdge(from, cond, WithCondFailPolicy(CondFailRerunFrom))` 声明 **cond 报错**（业务错/ctx.Err/panic）时的 Resume 语义。**cond 是纯判断**故其报错只可能是「上游数据缺失/非法」——该重试还是放弃取决于上游是否确定性，只有业务知道：`Abort`(默认，终局失败——wrap `ErrRunAborted` → Run 出口**主动 DeleteSubtree 清 cp 子树**，适合纯计算上游) / `RerunFrom`(存 Pending cp → Resume 重跑 from 让它重新产出，适合 **LLM 上游**如 planner/classifier。⚠️ **对 Append/Sum 不再双计**——出边算在提交之前 ⟹ 出边算不出来的成员根本不算成功、writes 从没提交过，那条「业务须保 from 的 writes 幂等」的旧义务**已解除**；节点内已发生的**外部副作用**另说，用 `SaveNodeState` 记进度重跑时跳过；节点里调模型 / 子 agent 的**产出**则按 `Rerunnable` 记——本档**否决**时作废、重跑时重做；`RunSubAgent` 的完成记录默认是普通记录，产出那次调用要用 `agent_v2.WithRerunnable()` 点名（同一个节点里的扣款、问过人的把关不该跟着重做——实测一律作废就扣两次款）；cond 被外面停下（软取消 / 陈旧写回 / 调用方 ctx 已经死了——错误链上有没有 ctx 错都一样，判官用 `%v` 包一层就断链）不算否决；否决之后重做时在同一个位置提的问题换新 ID。**入口条件边**：嵌套图的入口否决时「上游」是外层节点里产出这张图输入的那个分域（经 `RunSubAgent` / AgentAsTool 调起的 agent 的主图算调用方的）；`AddSubgraphNode` 的子图、编排成员的主图推不动真正的上游（它已提交、不会重跑），确定性判官永不收敛——校验放在产出那个节点自己的出边上；`RunWithRetry` 包住嵌套运行时否决不在重试之间推进（判官按 `Rerunnable` 记下的判断依据原样拿回）；顶层图入口没有上游、本档不存档（直接重跑那次 Run）)。cond 抛 InterruptError 是协议违规，**恒按 Abort 拒绝**不受本 policy 影响 |
| `compose_v2` | `WithRunNS`, `WithFreshRun` | RunOption：嵌套子图 ns push / 强制忽略 cp 从头跑 |
| `compose_v2` | `WithInputDigest(d)`, `WithInputIdentity(d)` | RunOption：调用方交来的输入摘要，新开时记进存档（`Checkpoint.InputDigest`）、续跑沿用，auto-load 时比对——前者只核对（对不上告警、照样续；身份另有来源，如 call key），后者的摘要**就是**调用的身份（对不上 = 两次输入不同的调用撞进同一个槽 ⟹ 拒绝：不可重试、**不清档**——槽里那份属于另一次调用，而错抛在图开始执行之前）。`agent_v2.RunSubAgent` 自动给 |
| `compose_v2` | `NewSignalKind`, `EmitSignal` / `EmitSignals`, `CaptureSignals`, `JoinSignals`, `JoinSignalsFirstWins` + `SignalSuperseded`, `Signals{Take, Len}`, `Signal`, `SignalRecord`, `SignalConflictError`, `SignalRecipient`, `CallHook` / `EnterHook`, `ExecutionStateKey` / `EnvironmentKey`（`Checkpoint.Signals` / `CompletedSignals` / `RecipientKey`） | **上行信号**：执行里做出、交给上方消费方的控制决定（转交给谁、走哪条分支）。**一个收件人上的一类信号是一个单值格子**（同单写入者 `LastValue`）：**先后做出的后者为准**（Sequential 第二步、Loop 下一轮修正前面的决定；后面没再做决定不抹掉前面的），**同一超步只能有一个成员做决定**——两个成员都对同一个收件人做了决定（**值相同也算**）⟹ 图的设计错误，`SignalConflictError`（类型本身带着 `ErrRunAborted`：不重试、清档；在记录提问与提交之前返回，跟 `LastValue` 被两个成员写同一个位置），出路是只让一个成员做决定、其余成员的意见当普通产出由下游一个节点汇总后再定。`ParallelAgent` 的分支因此**拿不到**转交 / 路由能力（想「并行出意见再定转交」用 `Sequential[Parallel, 决定者]`），能撞上的只有业务显式转授出来的并发决定方（几个并发节点各自 `RegrantFromNode`）；节点里自己起 goroutine 并发调几个被转授的 agent 框架看不见，要用 `CaptureSignals` 分路收、`JoinSignals` 合并。⚠️ 曾经「值不同才报、不清档、留给续跑」：续跑把整个超步的成员连同副作用重做一遍（旁观的兄弟也重做），决定从记录里拿回时次次再撞、任务过不去（实测）。业务经 `JoinSignals` 自己合并的几路：值一致算一次（业务显式选的合并语义）、不一致同样报 `SignalConflictError`——往上交就终局（`errors.As` 取出结构体再返回也一样），要自己裁决就接住后自己处置或改用 `JoinSignalsFirstWins` 按次序取（曾经不带终局标记，冲突往上交之后续跑从完成记录原样再撞；后来标记只挂在外层包装上，取出来的结构体又变回可重试——两次都是审计实测）。**同一批 tool_call 排在前面的生效**（`JoinSignalsFirstWins`：次序来自那条已提交的回复，发起者是收得到反馈的模型），后面给出不同值的那个调用的结果换成失败结果（`agent_v2.SupersededDecisionErrCode`，带着它原本的结果——业务工具也能经嵌套调用做决定，丢了产出模型会再调一次；transfer_to_agent / route_to 这类框架控制工具的结果只是回执，不带）；在这批调用的 `OnToolEnd` 全部跑完之后定——钩子改成失败的那次决定作废（框架那件控制工具不再短路；业务 return-direct 工具照常以失败内容收尾），改成成功的照算；「算不算失败」与「模型看到的内容」由同一份改写后的 Result 推出；被盖掉的那个调用整个不算（它别的决定也不生效）；`OnToolEnd` 钩子里做出的决定排在这批调用的决定之后（看过结果才做的，后者为准）。曾经也报冲突：续跑从工具账原样重放、必然再撞，任务永远过不去（审计实测）。⚠️ 曾经「同一次调用里不一致一律报冲突」把先后修正当矛盾（整个 Run 失败），程序化转交又「取最后一个」把并发矛盾静默挑掉、部分失败续跑后赢家翻转。**跟着写入者的提交走**：写进当前这段执行（节点执行，或 `CaptureSignals` 包住的一段）——节点成功且本超步提交才并入图的已提交状态（随存档落盘，schema v17）；部分失败超步里成功成员的先记在一边（`CompletedSignals`），整超步完成时跟续跑成功的合并，结果不随停没停过变；图**成功**跑完时交给调用它的那段执行（往哪儿交在 `Run` 入口、钩子换 ctx 之前就取下）；失败 / 中断 / 被否决的作废。⚠️ 「作废」只管没做成的部分：重试下一次尝试拿回已完成的嵌套调用、续上被调方已提交的步骤时，那些决定随记录一起再交出。cond 与节点级钩子（OnNodeStart / OnNodeEnd）里做出的跟着成员走，入口条件边的算这张图的；OnGraphStart / OnGraphEnd 的属于调用方那段执行（End 的排在本图之后）；**OnNodeEvent 只作观测**：在里面做出的不生效（它的 ctx 上没有收信号处，`EmitSignal` 返回 false、`HandoffTo` 报错），决定要在发事件的那段代码里直接做——事件不带它是从哪一段执行发出来的（曾经靠在各段入口按具体类型重绑 sink 让经事件做出的跟着那一段走，业务包一层转发的 sink、钩子里再发一次事件就又漏了，实测）；`RunWithRetry` 每次尝试单独收；`DetachExecution` 剥掉。跳过执行的地方（工具账、嵌套调用的完成记录）把当时做出的连同产出记下、拿回时 `EmitSignals`；业务自己跳过嵌套图也要这么做。**收件人由引擎铸**（`SignalRecipient`：这个任务的密钥 + ns + 位置（含访问序号）+ 分域的 HMAC，随存档走、续跑同值）——`Signals` 的 JSON 能手写、`EmitSignals` 收任何一组，挡住伪造的是收件人经正常入口算不出来（曾经是 `"handoff:"+ns`，业务工具 `GetGraphNS` 一拼就转走了对话，没授予的终点候选 / 被当工具用的孙辈 / 分类器三处边界全绕过，实测；分域那一维挡候选自己的钩子把 ns 推回上一层再铸）。收件人放进 ctx 时包遮罩（`sealedRecipient`），打印 ctx 读不出。防护范围到正常入口为止：故意从存档读出 `RecipientKey` 自己算的不在其内（同进程业务代码不是信任边界，issue-99 §5.4）。密钥**一个任务一把**（顶层新开时铸、经 ctx 沿用、每份档都带）：中间层的档没写成时续跑从上层继承回同一把，顶层的档没写成时任务代号也换了、子层旧档不复活（曾经每张图各铸各的，消费方的档没写成时子层已落盘的决定静默丢失，三个耐久档都中）。种类放在不导出变量里只挡正常入口；`NewSignalKind` 同名只能铸一次（panic 报出先铸者位置）。采纳业务钩子返回的 ctx 只经 `compose_v2.CallHook`（放这次调用的标记、调、采纳三步收在这一处，引擎与 agent_v2、a2a 的采纳点都走它；工具中间件那种以「调 next 时传进来的 ctx」交回的用 `compose_v2.EnterHook`；底层两步不导出，哨兵 `TestHookCtx_AdoptedHooksOnlyThroughCallHook` 扫全仓，查有没有绕过入口直接调钩子、取钩子的方法值、交给钩子的不是入口放了标记的那个 ctx；业务组合钩子里再套 CallHook 也认得出）：框架的身份与位置——收信号的那段执行、收件人、授予在内——一律取框架自己的，跟 `DetachExecution` 剥的是同一份清单；调用方可配的（环境、软取消）按钩子返回的 ctx 是哪来的分——从入参派生的听钩子的，外来的（存下来的、`Background`）取框架这次的，取消与截止时间同理（`context.Cause` 跟 `Err` 取同一边）；会话 ID 是对话的身份、不论哪种都取框架的（钩子推了只会让一个 Run 里出现两个会话 ID，实测；钩子在派生的 ctx 上推会话 ID / sid 不生效，每个采纳点告警一次）（上层包的 key 实现 `compose_v2.ExecutionStateKey` 或 `EnvironmentKey`，剥离与采纳都按它认；曾经环境与取消一律跟钩子返回的 ctx，外来 ctx 上别的请求的调用主体、编排钩子、store、checkpointer 照收——流式片段送到别人的连接、两个租户共用一个转交状态，实测；曾经各个采纳点各铺各的几样，模型钩子那处一样都不铺：钩子返回别处缓存的 ctx 时，同一个候选第二次上场的转交没人取、模型答复记不进会话历史，实测；后来 agent_v2 另有一份清单与一个采纳函数，引擎的 `OnGraphStart` / `OnNodeStart` 不认得它的 key，钩子在那两处返回缓存的 ctx 时会话历史与转交照样丢，实测）。见 `compose_v2/signal.go` + `agent_v2/side_signal.go` |
| `compose_v2` | `SaveNodeState` / `GetNodeState[T]`, `SaveStepResult(ctx, key, v, opts...)` / `GetStepResult[T](ctx, key)`, `Rerunnable()`, `RerunGeneration(ctx)`, `EnterPhase(ctx, scope)`, `HandOver(ctx, scope)`, `CallHook(ctx, call)` / `EnterHook(ctx)`, `WithNodeStateSealed(ctx, scope)`, `SubCallScope(ctx)` | 节点私有中断态：本节点这一次执行已经做成了什么，立即落 cm、中断 / 失败时随存档导出、重跑时读回跳过——**写在副作用提交之后**。被跳过的部分里问过人（`Ask`）时，那一问放进它自己的相位里问（`EnterPhase`）——在节点体里问的话提问按次序编号，跳过它会让后面的提问挪到它的 ID 上，调用方累积传答案时拿到它的答案（几件事各问一次确认时就会中，`err == nil`）。前者整体一格，后者按 key 分条（业务 key 先哈希再入档，任何字符都安全；`RunSubAgent` 用它记已完成的嵌套调用）。**分域**：工具函数里按 tool_call（`WithCurrentToolCallContext` 派生；BasicAgent 另按工具名再分一层——`OnToolStart` 能改写工具名）、图的钩子相位与条件边按相位、交出去的一段执行（`HandOver`：嵌套调用被调方在自己图外的那一段——它的 agent 级钩子 / 编排成员 / 决策 LLM / 转交候选；`EnterPhase`：BasicAgent 的 `OnToolStart` / `OnToolEnd` 钩子，每次调用一个）各有自己的一格与步骤记录，**还有自己的提问与嵌套调用计数、自己的相位名**（进提问身份），碰不到节点体那一格（没分域时工具或工具钩子里一次 `SaveNodeState` 覆盖 tools 节点的账、已执行的工具续跑时重放——实测；共用计数时被调方在图外问一次就占掉调用方一个序号，调用被拿回之后调用方后面那一问换 ID 重问——实测）；采纳业务钩子返回的 ctx 时经 `CallHook`：节点体那一套（私有态一格、相位名、计数、在不在 tool_call 里）连同其余执行状态一律取框架自己的。`WithNodeStateSealed` 只按某个维度再分账、不开相位（工具体按工具名）。**`Rerunnable()`** 标「这是产出」：被**否决**时作废、重跑时重做。重做代际**按分域记**：节点自己的条件出边按 `CondFailRerunFrom` 否决 ⟹ 整次执行换代；它里面跑的嵌套图的入口条件边否决 ⟹ 只让**产出那份输入的分域**（节点体 = 整次执行、某个 tool_call、某个钩子相位；经 `HandOver` 交给被调方的主图——`RunSubAgent` / AgentAsTool 调起的 agent——输入算**调用方**那个分域产的，推进被调方那一格的话调用方的产出原样拿回、续跑永不收敛，实测）换代，而且**等成员以这个错失败时才推进**（错被兜底吞掉 / 被重试消化 / 转成工具失败结果就不推进——当场推进整次执行时兄弟 tool_call 的提问换 ID 重问、挂起的可重做调用换槽重来，实测）。否决标记沿错误链往上走；成员以**非控制流**的错失败、而标记没传上来时（业务 `%v` 包断了链、扇出只报一个错），这次执行里记过的否决照样推进（中断 / 被停下时只认标记：否决被吞掉转而问人的执行要原样重放到那一问）；有效代际 = 执行级 + 分域链上各层之和，交出去的那一段（`HandOver` 封的那一格与它下面的分域）只算交接那一层往下的——调用方那一层重做时这是不是同一次调用由调用的身份决定，身份没变的调用里问过的问题不换 ID、按 `Rerunnable` 记在那一格的不作废（曾经算上调用方那一层，挂着的调用在图外问过的问题被再问一遍，实测）。条件边被外面停下（软取消 / 陈旧写回 / 调用方 ctx 已经死了）不算否决，中断 / 失败之后的续跑照常拿回。`RerunGeneration` 返当前分域的有效代际，给要把它拼进别的身份的调用方（`agent_v2.WithRerunnable` 的调用身份、提问身份已自带）。`SubCallScope` 返当前相位名（节点体为空；只在一张图之内区分，跨图要连同 ns）。节点外、顶层图的 `OnGraphStart` / `OnGraphEnd` 与入口条件边里是 no-op（嵌套图的这几处记在外层节点执行名下）。随节点「没成」的出口存档落盘，durability 在超步边界写的存档不带（进程在节点执行中途崩溃时丢，配 sync / async 也一样）。不设清除点、只落本层 |
| `compose_v2` | `AddSubgraphNode(id, sub, WithImport(ch), WithExport(ch))` | 子图作父图节点。**import 摆的是子 cm 的 baseline**（一层不经 reducer 的底层值，每次进入都从父图重摆、不进 Snapshot）；子图读到 reduce(baseline, 自己的写)；**export 只送子图自己写的那部分**经父 reducer 合并，没碰过的不回送。同一 channel 同时 import+export 是正当用法（父 `Sum` 5、子 +3 → 父 8）。修之前「复制值」是「经 reducer 写」，对 `Sum`/`Append` 是相加：子图没碰的被原样回送父 5→10 / [a]→[a a]，带子 cp 续跑再灌一次 → 18，全程 `err==nil`（2026-09-02 修，`subgraph_baseline_test.go` 7 条哨兵）。⚠️ 子图内 reducer 的 `prev` 只含子图自己的累积，不含 baseline |
| `compose_v2` | `Durability`(exit/sync/async), `WithDurability`, `WithDurabilityCtx`, `GetDurability` | cp 写入时机 RunOption（移植 LangGraph durability）：exit(默认=历史行为,仅出口 defer 写,硬崩丢进度)/sync(每超步同步阻塞写,扛硬崩)/async(单 worker 后台 goroutine 写,FIFO 有序 cp_N 先于 cp_N+1 + join-before-cleanup + per-task panic recover)。ctx 透传嵌套子图继承父档；优先级 RunOption>ctx 继承>默认 Exit。崩溃恢复价值需配持久 Checkpointer。见 `compose_v2/durability.go` |
| `compose_v2` | `WithStaleWriteGuard()`, `WithStaleWriteGuardCtx(ctx)`, `ErrCheckpointStale`, `Checkpoint.Epoch` | **opt-in 陈旧写回检测**（ISSUE-97）：同一 sid 被并发 Run/Resume 时，双方各自载入同一份 cp、各自执行、各自写回 → 后写者静默覆盖先写者且**两边都 err=nil**。开启后写/删 cp 前比对世代，被推进过则返 `ErrCheckpointStale`。**不阻止副作用双跑**（检测在写回时，那时两边都执行完了）、**非原子**（read-then-write 有窗口）、**每层各判各的**（子层基线=刚读到那代，只覆盖本层读-写窗口）、**只管 cp**（store.Store 的写在图内、早于判定）——只把最坏的「静默」变成「可见」。**默认关闭**；`agent_v2` 9 编排下只能用 ctx 那个（编排内部自己拼 RunOption 不透传业务的），嵌套子图也靠 ctx 继承。⚠️ **带「新开」信号的 Run 不设世代基线**，守卫要等本次首写之后才生效（首写本身不校验）。agent_v2 顶层 Run 恒新开，于是默认 exit 档下，新开的 Run 全程不受保护——成功清理时还会把槽里别人的挂起档一并删掉；sync / async 档从本次第二次写入起受保护（每次写成功都会把基线推到自己写的那一代）。同 sid 两个任务并发本身违反 RunID 一任务一枚的契约。真解在调用方串行化（会话亲和路由/分布式锁/队列）|
| `compose_v2` | `SoftCancelSignal`, `NewSoftCancelSignal`, `WithSoftCancel`/`WithSoftCancelCtx`/`GetSoftCancel`, `ErrSoftCanceled` | 外部异步软取消 RunOption：业务在另一 goroutine `Trigger(reason)`，Run 在**超步边界**优雅停（当前超步先跑完）+ frontierAtExit/SaveNS 存档可 Resume（返 `errors.Is(err, ErrSoftCanceled)`；与 `ctx.Done()` 硬停的区别是**当前超步善终不丢**——硬停掐正在跑的超步、进度回滚到上个超步边界，但出口**同样存 Pending cp 可 Resume**（`shouldStayPending` 含 ctx 错），不是「不存档」——审计实测新 ctx 下 Resume 闭环；出口写档不跟着调用方取消，持久后端上同样存得下，曾经只在内存后端上成立）。一次性（每 Run 建新信号）+ 末超步 Trigger 是 no-op（Run 正常成功 err=nil）+ ctx 透传嵌套子图继承。见 `compose_v2/soft_cancel.go` |
| `compose_v2` | `Channel[T]`, `LastValue`, `Append`, `Sum`, `OnceFlag`, `Get`, `Write` | 4 种 reducer + 读写 API（**唯一** state 模型——v1 GraphState/ProcessState 已删）。**`LastValue` 是单写入者 channel**（2026-09-02）：同一超步里两个成员写它、或两条 `SendFrom` 指令的 payload 写它 ⟹ 引擎当场拒（`%w ErrRunAborted`，正文点出 channel + 两个写入者），不再静默丢一个——它的 reducer 是 `return next`，并发下赢家由 frontier 索引序（声明序）决定、跟意图无关，「N=1 对、N≥2 静默错」。逃生舱不加新 API：真要「谁最后写谁赢」用 `Custom(name, func(_, next T) T { return next }, zero)` 显式声明。⚠️ `SendDirective.Payload` 写进的是**共享** channel 状态、不是目标实例的私有入参——同 target 扇出 N 次时 N 条 payload 塌成一份，而实例**拿不到自己是第几个**（`InstanceFanoutIndex` 是进程级计数器不是 0..N-1）⟹ **「同 target 扇出 + 每实例不同输入」今天没有正确写法**（`send.go` godoc 曾教过两版都不成立的写法，已改成如实声明）。不同输入用不同 target（ParallelAgent 的 `worker_<name>`）。payload 改私有入参 vs 暴露扇出序号的取舍在 `doc/code/todo.md` backlog 待定 |
| `compose_v2` | `Checkpointer` 接口 + `InMemoryCheckpointer` | 8 方法（NS-aware）：`SaveNS/LoadNS/ExistsNS/DeleteSubtree/ListPending/LoadAllInSession/SaveBatch/Cleanup`；按 (sid, ns) 二级 key 物理分离；cp 是单次 Run 临时态成功即清，跨 Run 会话历史走 agent_v2 历史层（ISSUE-98）。**持久 backend 多数场景不必自研**——框架已内置 `redis.NewCheckpointer` / `sql.NewCheckpointer`（PG+MySQL 双方言，各自独立 module）；真要自研跑 `compose_v2/checkpointer_conformance.RunCheckpointerTests(t, factory)` |
| `compose_v2` | `Checkpoint{...}` 结构 | **cp 权威篇 → [doc/v2/checkpoint.md](doc/v2/checkpoint.md)**（全字段 / 三个身份字段的区别 / 生命周期 / 寻址 / 节点私有中断态 / 并发保护 / Resume 协议）。速查表只留四条最容易踩的：(1) **`Status` 三档** `Unset`/`Pending`(等答案的挂起)/`Failed`(失败现场) —— ⚠️ **2026-08-13 起它只是报告位，不参与控制流**：「捡不捡这份档」三处闸门（Run 入口 auto-load / 子图载入 / `agent_v2.Resume`）统一读 **`Checkpoint.HasResumableWork()`**（按**内容**判：frontier 或挂起清单非空），**失败档也续得上**（Resume 不带答案 = 重试失败分支、成功兄弟不重放；带答案 = 答问题，两者可叠加）。出口写档位的判据仍是 `shouldStayPending`（**宽于** `IsControlFlowSignal`，另含 `CondFailRerunFrom` 与坏存档两类），但它只决定**标什么位**、不决定**能不能续**——后者是另一个谓词 `LeavesResumableCheckpoint`，两者从此不是同一件事；(2) **`ConsumedAnswers`** = 已兑付的答案位（「一个答案只能满足一个提问」的账本）——漏序列化**不报错**，只是那道兜底静默失效；(3) **`NodeStates` 两条不变量别改**——不设清除点（visit 只在成功路径递增 ⟹ 节点成功后旧 state 自然不可达）+ 不向上冒泡（只落本层 cp）；(4) **业务自研 backend 漏任何字段都不报错**，只是各自静默退化（逐字段清单见权威篇 §2.4） |
| `compose_v2` | `OrchestrationHooks`, `NoOpOrchestrationHooks`, `RecordingHooks`, `ChainOrchHooks`, `NewChainOrchHooks` | 5 回调（OnGraphStart/End, OnNodeStart/End, OnNodeEvent）；`RecordingHooks` 测试 helper；`ChainOrchHooks` 多 hook 链（nil/typed-nil 过滤；**无** per-hook panic 隔离——audit² 删，hook panic 由 framework outer recover 兜底转 err）|
| `compose_v2` | `PanicError`（`internal/panicerr.Error` 的类型别名）`{Value, Stack}` | 框架兜住的 panic：原值 + panic 发生时的调用栈。**每个把 panic 转成错误的兜底点都把它并列挂在返回的错误上**（`errors.As` 取），同时记一条带栈的 ERROR 日志（持锁的 `ApplyAll` / `ApplyPendingSendsAndClear` / `Restore` 由 Run 出口补记；业务直接调这几个方法时日志由调用方记；日志 handler 在 `panicerr.Log` 自己起、自己等的 goroutine 里跑——handler 是业务代码，它 panic 或调 `runtime.Goexit` 时不能掐断还没记下结论的兜底路径）。约定：**错误文本不含栈**、与原来逐字相同（错误文本会交给 agui 前端、a2a 对端、AgentAsTool 转给模型的工具失败、span 的 error.message）；**不实现 Unwrap**——panic 值是 error 时 `errors.Is` 认不认得出它照各兜底点原来的写法（多数 `%w`，OnGraphStart / OnNodeStart 钩子、async 写档、AutoCompact、流桥接等只取文本），露出来会把「panic 了一个中断 / ctx 错」当成真中断、真取消；挂上之后错误用多分支 `Unwrap() []error`，单层 `errors.Unwrap` 返 nil。栈不另设上限（运行时只打最内 50 + 最外 50 帧），不进存档、不进 span。分工见 `compose_v2/panic_recovery.go`「栈与日志」|
| `compose_v2` | `Event{Type, Content, Metadata}` | OnNodeEvent payload 简化版（不带 *AgentEvent）|
| `compose_v2` | `EventSink`, `GetEventSink(ctx)` | 节点内主动发 event 给前端流 |
| `compose_v2` | `Graph.ID()` | graph 身份（防 cp 串台）——`cp.GraphID = g.ID()` 直读 + SaveNS 冲突守卫 + Resume fail-fast；不走 ctx（WithGraphID/GetGraphID ctx pair 废弃已删） |
| `compose_v2` | `WithOrchHooks(ctx, hooks)`, `GetOrchHooks(ctx)` | hook ctx 透传协议（嵌套 sub-agent 自动继承父 hooks）|
| `compose_v2` | `RetryConfig`（**`MaxRetries>0` 时 `ShouldRetry` 谓词必填**——重试重放的是整个成员调用连同副作用，而框架在这层只看得到 `error`、分不出「网络抖了一下」和「参数非法重跑也一样」。⚠️ **成员内部已成功的部分不重放**（`RunWithRetry` 第 2 次尝试起清「新开」信号，成员按自己的存档 auto-load；没配 checkpointer / 成员是远程或自研的壳时安全降级回从头跑。节点里用它包 `agent_v2.RunSubAgent` 同理：每次尝试从同一个嵌套调用编号数起，输入不变的调用算出同一个身份——第一次尝试里已完成的拿回记下的结果、失败的续上它的存档；fn 里直接 `Ask` 的问题同理（提问序号一起退回、答案账本按尝试分层，答过的不重问；编排重试成员时成员在图外问的也是）；**手写的循环每次都算新调用、从头跑**。⚠️ 所以「拿到产出、校验不合格就重来」**别用它**（已完成的调用每次原样拿回、重试预算白花）——在循环里调，或把校验放到条件边上声明 `CondFailRerunFrom` + 给产出那次调用声明 `agent_v2.WithRerunnable()`）；装配期 `ValidateRetryConfig` 拦，想用框架保守默认就传 `SimpleRetry`/`ExponentialBackoff`，它们自带 `IsRetryable`）, `SimpleRetry`, `ExponentialBackoff`, `RunWithRetry[T]`, `IsRetryable`, `IsControlFlowSignal`, `RetryAfterProvider`, `RetryAfterFromError`, `HTTPStatusProvider`, `IsRateLimited` | 重试机制（泛型 helper + 7 编排接入）；**`RetryConfig.RateLimitMaxRetries`** 给限流（429）一个**独立计数器**——它的等待是秒到分钟级（还常照 `Retry-After` 等），跟 5xx 毫秒级重试共用一个 `MaxRetries` 会让一次限流把整个 Run 拖很久；两个上限同时生效谁先到谁停。⚠️ **必须独立计数、不能「共享 attempt 比一个更低的上限」**——后者算的是别人消耗的次数，`503,503,429` 下那次 429 一次都没重试，而「过载被重试打压后转限流」正是常见退化路径。⚠️ 它属于 `RetryConfig` 不属于 `BackoffPolicy`（「试几次」不是「等多久」），放错会让 `fallback.PerModelRetry.Backoff` 长出一个静默不生效的字段。**`RetryAfterProvider`**（接口定义在使用方，`model.APIError` 实现）让重试层免 import model 即可读服务端 `Retry-After`：`RunWithRetry` + fallback per-member 重试**优先按它退避**，**有意不受 `MaxDelay` 约束**（那个字段管指数退避别失控增长，不用来推翻服务端指令），外层边界交给 ctx deadline。`IsControlFlowSignal` 是"什么是控制流信号"（InterruptError / ctx.Canceled / DeadlineExceeded / **ErrSoftCanceled**（业务主动说停，ctx 取消的优雅版）/ **ErrCheckpointStale**（重跑不会让账记上、只重造副作用））的**定义处**，不各处散写 errors.As/Is。**决定「要不要再做一次」用的是 `IsRetryFutile(ctx, err)`**：三个框架自铸哨兵照旧按 error 判，ctx 那档改问 `ctx.Err()`——错误链上有 ctx 错 ≠ 调用方的 ctx 死了（pgx、`http.Client{Timeout}` 都会产 `DeadlineExceeded`）；框架重试守卫与 fallback 已改用它。⚠️ **默认谓词 `IsRetryable` 还没跟上**：仍走 `IsControlFlowSignal`，后端内部超时一律不重试，而 `SimpleRetry` / `ExponentialBackoff` / 7 个 `WithXxxMaxRetries` 默认都是它（见 `doc/code/todo.md`）。`IsRetryable` **不只是它的否定**：它另排 **确定性的 provider HTTP 失败**（400/401/403/404/422，经 `HTTPStatusProvider` 接口按状态码判；0 / 408 / 409 / 429 / 5xx / 未知码照旧重试。⚠️ 这不叫「框架替业务猜」——那几个码是 HTTP 定死的语义，而**不透明的业务错框架仍然不猜**）、业务自己实现 `NonRetryable` 声明的、以及 `ErrRunAborted`（终局失败且 cp 子树已删，重跑=fresh run 从头再来、副作用全部重放）——那是"失败的结局"不是"控制流信号"，故不塞进前者（否则 fallback 层跟着承担它不需要的语义） |
| `compose_v2` | `ErrNoSubgraphCheckpoint`, `ErrGraphIDConflict`, `ErrNoPendingCheckpoint`, `ErrRunAborted`, `ErrCheckpointNotPersisted` | sentinel error（errors.Is 友好）；**`ErrCheckpointNotPersisted`** = Run 出口要写的档没写成（写档或写档前的快照失败；挂起类退出与业务报错退出都算）：**档没写成就不能报告成可以续**——中断 / 软取消 / ctx 错不在错误链上（`IsInterrupt` 为假），业务错与写档原因在；同时带 `ErrRunAborted`（不重试、各吞错点不吞、出口尽力清这一层的子树、上层跟着清），**槽属于别人时例外**（写档撞上 `ErrCheckpointStale` / `ErrGraphIDConflict`：不带 ErrRunAborted、不删，照样不重试）；扇出仲裁排在中断之前。曾经把写档错误跟中断一起 `%w`：前端照常请用户回答，答完续跑找不到档、或续进回答之前那份档重问已答的问题（实测）。**出口写档不跟着调用方取消**：每次出口存储调用用 `context.WithoutCancel`、调用方结束之后再给 10 秒宽限，写档只发一次；陈旧写回检查与写档各是一次调用，检查拿不到结论（读报错、没做成）时写、删都放行，写档结果不明时清档前的检查认这次写的那一代作自己的；宽限从每次调用自己的起点算，调用方放弃之后读 / 写 / 删每一类调用失败所占的墙钟时间（重叠只算一次、放弃之前的不算）各累计不超过一个宽限、用完之后 ctx 已结束的同类调用不再发（总等待有上界、跟嵌套层数无关；ctx 还活着的调用不拦）；先定结论、再记日志、发 cp 事件（cp 事件钩子与出口日志 handler 的 panic 当场兜住、只记日志、不进错误）；后端出口存储调用 panic 按那次调用失败处理；`RunWithRetry` 框架守卫不重试它——曾经用已取消的 ctx，持久后端上「调用方取消之后可以续跑」从没成立过（SQLite 实测写不进去），三条不变量见 `compose_v2/exit_checkpoint.go`。agui 给 `checkpoint_save_failed`、a2a 以 failed 收尾，两边都清自己的挂起状态、文案脱敏；见 `doc/v2/checkpoint.md` §3.5；ErrNoPendingCheckpoint（R33 GA-M2）= 9 编排 Resume 对不存在 cp 统一 fail-fast（sid 拼错/cleanupOnSuccess 已清/从未跑过——修复前静默 fresh-run 出垃圾 final err=nil；Abort 豁免保 a2a Cancel 幂等清理）；**ErrRunAborted** = 本次 Run **终局失败不可 Resume**，Run 出口据此**主动 DeleteSubtree 清 cp 子树**（业务 `errors.Is` 判「已终局、别再 Resume」）——**必须主动删而非"跳过写"**：cp 是 (sid,ns) **单槽覆盖式**，跳过写只是不覆盖、先前的 cp（HITL 档 / durability 中途 cp）会幸存且 Status=Pending → auto-load 捡起 → 重跑已成功节点 / **重问已答过的问题** / 子图 **livelock**（子跳过写但父照存 → 反复 Resume 反复问，永不收敛）。**子图终局性自动向上传播**（子 abort 错经 subgraph `%w` 冒泡 → 父出口同样命中 → 父子树一并清，无需逐层协调）。**来源**：cond 的 `CondFailAbort` 档（含 cond interrupt 协议违规拒绝）+ `validateInterrupt`（typed-nil `*InterruptError` / `Query` 与 `Queries` 同时填 / 一条 query 都没有 / `InterruptID` 为空 / 同一批内撞 ID / 落在已兑付的答案位）+ `recordInterruptBatch` 跨分支撞 ID + `Ask` 自身三条（自填 ID / 空 Question / 非节点作用域）+ **单写入者冲突三形态**（成员间 / 两条 payload 间 / 成员写 vs payload 写）+ **上行信号的单写入者冲突**（同一超步两个成员都对同一个收件人做了决定、`JoinSignals` 合并的几路给出不同决定，`SignalConflictError`）。⚠️ 前面几类是「业务违反协议、重跑必然一样」，**单写入者那几类不是**——它的理由是「这个矛盾会随存档反复复现，留 Pending 档只会让 auto-load 一遍遍撞」（条件写换批数据就可能不触发，见 `state-model.md` §六-10）。**判别式**：这个错重跑一次可能就好，还是重跑必然一样？后者才配这个哨兵——它同时买到「不重试」（`IsRetryable` 已排除）和「清死档」两件事，而普通 `fmt.Errorf` 两件都不给（实测代价：配了 retry 的编排下一次确定性 bug 扣了 4 次款；留下的 `Status=failed` 档会被续跑闸门捡起——闸门按内容判、不读 Status——每续一次都撞同一个确定性错误）。**入口默认档是「恢复否则新开」**：该 (sid,ns) 有挂起存档就续、没有就拿 `WithInitialWrites` 的种子从头跑——种子收窄成「只在新开路径 apply」之后它不再声称「这是新任务」，所以「有种子 + 存档命中」是**正常形态**（崩溃恢复就长这样）。想开新任务传 `WithFreshRun()`。⚠️ 这里加过一道「有种子 + 存档命中就报错」的守卫，**已撤**（2026-08-05）——见「守卫合法性」规约|
| `compose_v2` | `MaxIterationsError`, `MaxRoundsError` | `MaxIterationsError` 在 BasicAgent（maxRounds+1）/ LoopAgent / GraphAgent maxVisit 触顶时 graph_loop.go 实际抛 (errors.As 解析 `Iterations` 字段); `MaxRoundsError` typed 但生产路径不抛 — 预留供业务自定义 |
| `compose_v2` | `RegisterValueType[T]`, `LookupType`, `LookupKey`, `MarshalTagged`, `UnmarshalTagged` | type registry + tagged JSON 序列化（业务跨 cp 边界存自定义类型） |
| `compose_v2` | `InterruptError{Reason, Query, Queries, Data}` + `IsInterrupt(err)` + `AllQueries()` + `MergeInterrupts(ies)` | error-as-control-flow，触发存档挂起；Queries 多 query 容器（ISSUE-81 toolsFn fanout 多 tc 同时抛 Interrupt 用，与 Query 互斥 Queries 优先）；MergeInterrupts 聚合多 ie 到单 ie.Queries 让 graph_loop record 全部 query |
| `compose_v2` | **`Ask(ctx, q)`** + `AskQuery{InterruptID, Question, Options}` | **`Ask` 是业务提问的唯一推荐入口**（算 ID → 查答案 → 有就返、没有就抛中断）：`ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准转账吗"})`。InterruptID 由框架按 (graphNS, nodeID, 实例身份\|visit, tc ID, tc 下标, idx, **任务代号**, 重做代际, 外层代际链) 算稳定 ID（重做代际：条件边按 `CondFailRerunFrom` 否决之后重做的那次执行问的是新问题——否则累积传答案的调用方会把对 v1 的批准兑付给 v2；按分域算，兄弟 tool_call 被否决不影响它；外层代际链：节点里跑的嵌套图有自己的 cm，外层节点被否决重做时它从头跑、在同一个位置问，要靠承载它的每一层外层执行的代际区分；两者为 0 时都不计入）。<br>⚠️ **为什么必须走这一个入口**：ID 的一维是**本分支自己的计数器**，它的当前值**只有正在跑的那段代码知道** ⟹ 「算 ID」和「用 ID」必须是同一次调用，第二处想重建都是猜。试过让扇出方在交出中断之后替分支盖章（按「第 j 个 query 配第 j 个计数值」猜），实测**比不补还糟**：开了工具审批时审批层自己先 tick 过一次 ⟹ 全错位 ⟹ 工具体**永不收敛**；「一个工具问两次」时第二问被盖上第一问已答过的 ID ⟹ **副作用无界重放**（实测 16 次仍在涨，而不补时只 1 次）。那条路已删。<br>⚠️ **引擎不再有任何兜底**：`validateInterrupt` 对空 ID **当场拒**（`%w ErrRunAborted`，不重试 + 清死档），对**同一批内撞 ID** 同样拒（放过的话第二条被 `recordInterrupt` 静默丢掉——调用方看到 2 条、存档里只有 1 条，用户答「批准转账」那条可能触发「删除历史」）。这两道校验**所有产生中断的入口共用一份**（节点返回 + `OnNodeStart` hook——后者原先整条绕过，实测空 ID 挂起照样落档）。**`Ask` 不接受自填 `InterruptID`**（填了当场拒）——身份的选择权在业务手里时，两个不同的提问能落在同一个答案位上（实测跨超步 / 跨 visit / 同节点两问三种，全部 `err == nil`）。想给前端一个自己认得的标签走另一条路：直接构造 `&InterruptError{Query: ...}`，那条路上的 ID 只作标签、`Ask` 不认它。框架自带三处 HITL（`ask_user` / 工具审批 / a2a）本来就是「同一次调用里算完就用」，`Ask` 是把那个模式提成公开 API |
| `compose_v2` | `WithCurrentToolCallContext(ctx, tcID, toolCallIdx)` / `GetCurrentToolCallID(ctx)` | toolsFn ctx 协议**唯一写侧**（单维的 `WithCurrentToolCallID` 已随 `309cddf` 删除，串行 N=1 也走这个）：一站式注六样（多一样是「在 tool_call 里」的章 `compose_v2.InToolCall`——有的 provider 不回 tool_call ID，按 ID 非空判会让同批两个嵌套调用落进同一个槽）：tc.ID + tc 下标（批内第几个，不是图级分支下标）+ 独立的提问计数器（atomic.Int64，ISSUE-81 toolsFn fanout）+ 独立的嵌套调用次序计数器（`NextSubCallIndex`，同一个 tc 里用同一份输入对同一个子 agent 调两次时各落一个存档槽）+ 节点私有态的 tool_call 分域（工具函数里的 `SaveNodeState` / `SaveStepResult` 记在这个 tc 名下）——业务自定义 fanout toolsFn 用新 helper 后，业务工具**提问一律走 `compose_v2.Ask`**（它算 ID、查答案、没答案才抛；自填 ID 当场被拒）。交出一段执行时（`compose_v2.HandOver` / `EnterPhase`）「在 tool_call 里」的章被摘掉（位置维照留）：嵌套调用的被调方在自己的图之外跑的 agent 级钩子、业务工具钩子都不是那次 tool_call 本身 |
| `compose_v2` | `JoinNS`, `ParentNS`, `IsNSInSubtree`, `WithGraphNS`, `GetGraphNS`, `ValidateNodeIDForNS`, `JoinFanoutNS` | ns 物理分离 helpers (JoinFanoutNS 防 fan-out 多实例 cp 互覆 ISSUE-78)。**`IsNSInSubtree(root, ns)` 定义 `DeleteSubtree` 的删除范围**：root 自身 + 经 `/` 往下的后代；`#`（fan-out 实例）/ `@`（重访）段是同一个节点的**兄弟执行**，不删（2026-09-23 改：之前算作后代，一次执行成功清理时把兄弟执行还挂着的存档一起删了——同一个节点里对同一个名字调两次时第二次的挂起档、子图节点同超步 SendFrom 实例的挂起档）。`ParentNS` 仍把 `a#0` 归到 `a` 下，只用于观测按节点归组，不表示所有权 |
| `compose_v2` | `SendFrom`, `SendDirective` | 超步 fan-out 注入分支 |
| `compose_v2` | `AdjustIndexToPreserveAPIInvariants` | 切点配对感知（保 tool_use ↔ tool_result + 同 RunID 块连续，避免 OpenAI 422）|
| `compose_v2` | `EstimateTokens`, `EstimateMessageTokens`, `GroupMessagesByAPIRound` | Token 估算 + 消息分组（4 层压缩用） |
| `agent_v2` | 7 个低阶编排 builder | `BasicAgentBuilder` / `SequentialAgentBuilder` / `ParallelAgentBuilder` / `LoopAgentBuilder` / `RoutingAgentBuilder` / `ReflectAgentBuilder` / `HandoffAgentBuilder`（构造 `NewBasicAgentBuilder` / `NewSequentialAgentBuilder` / …；每个对应同包高阶 wrapper）；plan **无低层 builder**（已 inline 进 `PlanAgent`，commit `856bb6e`）|
| `agent_v2` | `Agent` 单接口 | `Run(ctx, sess, input, opts...) ([]*AgentEvent, error)` + `Resume(ctx, sid, opts...)` + `Name() / Description()`；9 编排都满足 |
| `agent_v2` | `BasicAgent`（ReAct）| 内部 preamble + model + tools + postamble **四节点** channel-based Graph + 循环回边（preamble 只跑首轮 ISSUE-74 / postamble 跑 commitFinal + StoreMemory store.Put ISSUE-75）；tools 节点内 N>=2 tool_call **goroutine 并发** fanout（ISSUE-81，N=1 仍 inline 零开销）；跨 InterruptError 走 cp round-trip 不丢；preamble 节点支持 StoreMemory/HistoryCompactor/Skill HITL Resume |
| `agent_v2` | `SequentialAgent`, `ParallelAgent`, `LoopAgent`, `RoutingAgent`, `ReflectAgent`, `HandoffAgent`, `PlanAgent`, `GraphAgent` | 8 编排（链式 builder + sub-agent 嵌入 HITL Resume Continue 闭环；**Replan 仅 PlanAgent + BasicAgent + GraphAgent 接受（GraphAgent R21 audit MED-1 透传），其余 6 编排 fail-fast `not supported`**）|
| `agent_v2` | `Config`, `Option`, `WithName`, `WithDescription`, `WithSystemPrompt`, `WithModel`, `WithToolkit`, `WithHooks`, `WithMaxRounds` | 标配 BasicAgent options |
| `agent_v2` | `WithMicroCompact / WithAutoCompact / WithReactiveCompact / WithStore / WithStoreMemory / WithSkills / WithSkillSystemPrompt / WithSkillListingBudget / WithSkillSelector / WithSkillSelectionHook / WithSkillBodyMaxChars / WithInlineSkillBody / WithSkillModelResolver / WithSkillSubAgentResolver / WithMaxHistoryPairs / WithHistoryCompactor` | 4 层压缩 + StoreMemory + Skill（toolCall/adaptive/pre_match 三模式 + 预算裁剪 + 自定义 hint + selection hook 业务订阅 + body 长度上限防 ctx 爆 + DisableModelInvocation 三处独立强制：`activate_skill.Description()` listing 过滤 + `Call()` 防御返 `SkillDisabledErrCode="skill_disabled"` 业务级失败 + `selectSkillAdaptive` 双阶段候选过滤（disabled 先 / MinScore 后，分阶 Reason 让 None 路径区分两种）+ P2.D inline body：`WithInlineSkillBody(true)` 启用后 tool_result 退化 terse ack + body 作为 `[system_reminder]` UserMessage 注入对话流（marker 打 terse-ack tool_result + body UserMessage 两处——R19 两 group 都保护）+ P2.C+B fork mode：`context: fork` frontmatter + `WithSkillSubAgentResolver(name→Agent)` / `WithSkillModelResolver(name→ChatModel)` per-skill model 切换；`activate_skill({skill,query})` LLM 必传 query；fork 失败返 `SkillForkErrCode="skill_fork_failed"` 业务级；**不可信源注入防护**（元数据 parse 边界清洗 `skill/sanitize.go` + listing 信任框定 `skillListingTrustNote`/router prompt，防第三方/marketplace skill 包经描述文本注入；body 仍业务自负）+ **skill 完整权威文档见 [`doc/v2/skill.md`](doc/v2/skill.md)**（定义/激活/body/fork/listing/压缩协作 5 轮 audit；§14 注入防护；§11 skill action 执行新增）)+ 第 0 层 history 兜底 |
| `agent_v2` | `WithHistory(hs, owner, opts...)`, `HistoryStore`, `HistoryKey{Owner, ConvID}`, `NewInMemoryHistoryStore`, `WithHistoryOwnerFunc`, `WithoutHistoryCtx`, `HistoryAgentAnnotation` | **会话历史层**（ISSUE-98，opt-in 零开销，`agent_v2/history.go`）：把消息流按**会话**（`compose_v2.WithThreadIDCtx` 声明的 ConvID）append-only 留存跨 Run 活着——补 cp「单次 Run 临时态成功即清」留不住历史的洞。**四类写入点**：BasicAgent 的三个节点各自写（preamble 记 UserInput / model 记产出 / tools 记结果，均节点成功后写）+ **编排边界补记** `settleHistorySlot`（11 个生产调用点：Sequential / Parallel / Routing / Loop / Handoff 各自的子调用处 + Plan 三条 worker + Reflect actor + `RunSubAgent`）。**边界补记是给「不是 BasicAgent 的执行体」兜底的**——业务塞进编排的远程 agent / 自研 agent / 适配别的框架的壳跑完什么也不留，没有它整条链会断。**输入的持久点在跨进边界那一刻不等成功**：11 个边界位置在「可 Resume 的退出」时也结算一次输入（`settleHistoryOnPause`，判据 `LeavesResumableCheckpoint` 按处置意图不按错类型；Handoff 位置按**历史视图模式**选守卫——共享视图（Rewrite/Raw）any-stream 只记一份、HandoffHistorySelfOnly per-slot 各流完整（成功与 pause 两路同一套，成功那半曾 per-slot 落两份是 pre-existing 一并修）；Parallel 扇出恒 per-slot。同一个「限不限归属」开关，答案由**这条流共享不共享**决定。另 Plan worker 输入**由独立 build 节点（plan_build_input）拼一次写 planBuiltInputCh 提交**、worker 只读——重跑从 cp 恢复同一条、身份天然稳定（防同任务描述随 HITL 轮次线性重复；首版哈希身份已退役，同款形状 = Reflect 的 feedback 节点）；顺带 ExecInputBuilder 审批中断 Resume 只重跑 build 节点、planner 不重跑）。判据两把尺：**输入**看「这次调用范围内有没有产生记录」（ctx 回执，沿父链上溯）+ `Role == user` + **按流**原子认领（兄弟分支各记各的、同一条流上不重记）；**产出**看「这次调用范围内记过这条消息没有」（范围 + 全局登记表按指针，透传的跳过、加工过的照记）。**`AgentAsTool` 的产出会被 BasicAgent 的 tools 节点写成 tool_result 时不补记**（tools 节点直接调起、转发、组合工具，自写 toolsFn 也算；组合工具里的那次调用要留痕改用 `RunSubAgent`），**别处（GraphAgent 节点体、各种钩子里）补记**（那里没有 tool_result 兜底，非 BasicAgent 的被调方自己不记，不补就一条不剩——实测 0/0）；判据是 tools 节点在调工具前盖的章（记下自己这次执行的位置，AgentAsTool 比位置——流进嵌套图、流到被调方图外那一段时位置对不上），不看「在不在 tool_call 里」：自写 toolsFn 不盖 tool_call 章时那个为假、产出照样成 tool_result（实测记两份），GraphAgent 节点体自盖 tool_call 章做扇出时那个为真、却没人记（实测一条不剩）。**嵌套调用暂停时结算过的输入按调用身份记进父节点的步骤记录**，续跑完成、再次暂停都不再记——调用方每次重跑都现拼一条输入（AgentAsTool 从参数拼的也是）时按消息 ID 认不出，实测每多一次中断多一份；GraphAgent 作为子时不再自己在暂停出口结算（交给边界）。**写入器由编排层拥有**（`startHistoryForOrchestration`，7 编排 × Run/Resume 共 14 个入口；配置从子 agent 借，**多个子声明了不同 store/owner 时直接返错**不静默挑一个）。**GraphAgent 两个入口已接**（`aa9226b`）但配置**只能走 `WithHistoryCtx`**——它没有静态子列表借不到配置，把 `WithHistory` 挂在图里某个 BasicAgent 上时那个写入器活在它自己的子 ctx 里、GraphAgent 这层看不到（实测该配法下图自己那层一条不记）。顶层时只开范围回执**不推 slot**，故图名不进后代归属（推了的话改个图名 → 下面所有子的历史全变孤儿 + 两种配法归属互不相认，均已实测）。+ 单 worker FIFO 异步（旁路：写失败/panic 吞+warn 不打断主链路，执行正确性挂 cp frontier 与历史无关）+ `prior` 换源（开了从历史读逐字完整流含工具往返，没开回落 events 残缺=有意降级）。`Owner` 必填无默认（ConvID 来自 wire 是续接标识非授权令牌，多租户用 `WithHistoryOwnerFunc`）；**`WithName` 也必填**（构造期 fail-fast——名字是历史里的**显示名**，跨归属改写会进模型 prompt，自动名每次构造都不同、模型和人都读到 `agent_v2_xxxxxxxx` 无意义标识符）。**归属盖 `HistoryAgentAnnotation`，值是该 agent 在编排树里的位置**（`AddBranch`/`AddStep`/`AddAgent` 的注册名，框架强制唯一；嵌套拼成 `outer/inner`；顶层回落 agent 名）——不用 agent 名是因为框架不校验它唯一，两个分支挂同名 agent 归属就撞、历史合并成一条流；位置**只放注册名不放轮次/跳数/下标**（那些跨 Run 会重置，带上就读不回历史）。另有 `HistoryAgentDisplayAnnotation` 存 agent 名供展示。⚠️ 代价：**归属是编排结构的函数**，改分支名/加 wrapper/挪层次都会让旧历史对不上，失效时是**彻底空**（SelfOnly 整条丢弃且不回落 events）。详见 `agent_v2/history_slot.go`；内部决策 LLM（routing 分类器 / reflect 评估器与反馈教练 / plan 规划器，共 4 处；`WithRouteDecider` 的决策方也在调用处套上）经 `WithoutHistoryCtx` 摘除不进历史（是「禁止」非「当前没有」防嵌套 agent 自配 WithHistory 绕过；读写两侧都摘，只挡写不挡读会让配了历史的分支连 events 都回落不到）。**编排合成的派生 input 按成分分档**（`DerivedInputAnnotation`，编排在源头打）：过滤**收在 `recordHistory` 一处**对所有调用方生效——**打 marker ≠ 有人读它**，原先 `isDerivedInput` 只有三个消费点（编排边界 input/final、preamble 的 user input），**tools 节点那条路一个都没有**、整批无过滤入库；框架合成的消息（如重复调用提醒）会以 `role=user` 留在 append-only 历史里、**下个回合原样重放进 prompt**、此后每轮都喂给模型。判据：判据是「拼进去的东西是不是已经在历史里」——Sequential 的「原始目标 + 上一步产出」全可重算故不记，Reflect 的评审反馈 / Plan 的任务描述来自被摘除的 LLM、派生输入是它们唯一的落脚点故照记。**不复用 store.Store**（那层知识可改写删，历史 append-only 语义冲突）。业务自研 backend 跑 `agent_v2/history_conformance.RunHistoryStoreTests(t, factory)`（10 条：含 append-only / 顺序 / 双维度 key 隔离 + **分隔符歧义**（`{acme/eu, c-1}` 与 `{acme, eu/c-1}` 拼串主键会撞）/ 字段完整 / 快照隔离 / **Append 不得原地改写入参** / 并发）。**嵌套时子 agent 的 `WithHistory` 不生效**（读写都沿用父的写入器——一场会话一条流）。**共享视图（转交模式）下同一条原始输入只记一份**，判据是「读历史 + 写入器原子认领」两段：前者管跨 Run（Resume 重跑到同一步时前一个 Run 的记录已落盘），后者管同一个 Run 内的并发（扇出分支同时读历史时谁都还没写完，只读历史会各记一份——实测连跑八次记了 1/2/2/3/1/2/2/3 份）；认领在写入器的锁里做，那是这条流上唯一天然串行的点。**SelfOnly（无转交）下每分支各记一份是有意的**——那样每条分支流各自完整（有问有答），重复只出现在「把并发分支拼成一条流看」这个视图上，而拼接本身就没有良定义的顺序。⚠️ 边界补记那侧的输入认领**按流分**正是为它：用「这条消息全局记过没有」判会让先跑到的分支剥夺其余分支的归属，实测份数随调度在 1~4 之间翻。详见 [`doc/v2/usage-guide.md` 会话历史节](doc/v2/usage-guide.md) + [ISSUE-98](doc/issue/issue-98-conversation-history.md) |
| `agent_v2` | `WithToolSearch`, `ToolSearchConfig{Enabled, Deferred, Retriever, MaxResults, MinScore}` | 工具级渐进披露（仅 BasicAgent）：deferred 工具初始**不进 LLM bind list**，LLM 调 `tool_search(query)` 检索解锁后才进；**揭示集只存图状态**（BasicAgent 图的 `basicRevealedCh`，随存档走，bind list 与执行闸都只读它；一批里检索解锁的名字先记在 tools 节点这次执行上、节点成功后写进去）——一个任务（一次 Run 连同续跑，带不带原 session 都一样）内累积，换了任务（`NextTask` 或复用 RunID 再跑顶层 Run）是新开的图、从零开始，并行分支与嵌套的被调方各有各的，会话 KV 上不留东西。执行闸只认这一批开始前已提交的解锁集：同一批里直调刚解锁的工具一律被拒，从下一批起可调（曾经看批内排列，`[lookup, tool_search]` 实测 50 次全放行）。没配 checkpointer 的重试尝试是新图、从空开始。曾经会话 KV 上另有一份缓存，键先后漏过编排位置（同名并行分支一个检索解锁、另一个白拿，fail-closed 被击穿——对照组实测隔壁检索一次、从没检索过的分支工具体执行 1 次）与任务（上一个任务解锁的工具下一个任务没检索就能调），两份拷贝还会分叉、键随任务数增长，2026-10-13 删掉缓存 + 可插拔检索（默认 keyword 复用 `skill.KeywordScore`，业务可插语义）+ **fail-closed** 拒未揭示工具执行（越界硬调返 `[tool_not_revealed]` 不执行，真 qwen smoke 实测触发）。常用 `Deferred=mcp.IsRemoteTool`。详见 [`doc/v2/toolsearch.md`](doc/v2/toolsearch.md) |
| `agent_v2` | `WithRepeatToolReminder(cfg)`, `RepeatToolConfig{Enabled,Thresholds,PreviewMaxChars}`, `RepeatToolReminderAnnotation` | **工具重复调用检测**（仅 BasicAgent，默认关、开关零开销）：模型连续用**同样参数**调同一个工具到阈值（默认 `[3,5,8]`）时，框架往对话流里塞一条提醒（排在全部 tool_result 之后，同 inline skill body 的位置）。三条判据：**① 在工具执行之后计数**——因为**被审批拒掉的调用也走这条路**，而「死命捶一个被拒的工具」恰恰最该打断（对上 `ApprovalAsk` + Deny）；**② 框架工具对链透明**（既不计数也不重置，否则模型插一个 `activate_skill` 就能洗白）；**③ 预览截断但检测键按完整串算**（键还按 JSON 深度 key-sort 归一，换字段顺序骗不过；存的是摘要——它随存档走，存原文时大参数每份存档多一份副本、解析失败的原始串里的非法 UTF-8 经 JSON 往返后对不上）。链是「一个任务里的一条 ReAct 调用流」的状态，**存在 BasicAgent 图的 channel**（`basicRepeatChainCh`，随存档走）：这一批用到时从已提交的那份拷一份、tools 节点成功后写回，一批中断时随那次执行作废、续跑重跑这一批时只算一次；续跑带不带原 session 都接着算；新任务、并行分支、嵌套的被调方各跑各的图。曾经挂在 `RunSession` KV 上按键区分，键先后漏过编排位置（values map 在并发分支之间共享，两个**同名**并行分支撞进同一条链：计数跨分支合并、各调 2 次凑成 4 次误报，链上的写裸奔在锁外、实测必现 DATA RACE）与任务（上一个任务的次数累计到下一个任务），计数还在交出中断之前就记了、续跑重算，2026-10-13 移进图状态。提醒**不进会话历史**（打 `DerivedInputAnnotation`，判据同既有派生输入分档：它可由历史里的重复调用完全重算）。⚠️ **不是 `maxRounds` 的替代**，是那道悬崖之前的一级台阶 |
| `agent_v2` | `WithToolFilter(fn)`, `WithPromptTemplate(tmpl, varsFn)` | 工具裁剪 + 提示词模板（仅 BasicAgent）：`WithToolFilter` 每轮 model 调用前裁业务工具子集（框架工具恒豁免、与审批一致（按实例上的封闭标记判、不按名字，见 `agent_v2/framework_tool.go`——业务工具恰好叫框架工具的名字照常受裁）；nil=不裁）；`WithPromptTemplate` preamble 渲染 `prompt.Template` prepend（保留 `WithSystemPrompt` 字符串）。见 [`tool-visibility.md`](doc/description/three-way-comparison/tool-visibility.md) |
| `agent_v2` | `WithApprovalMode(ApprovalAuto/ApprovalAsk)`, `WithApprovalHeuristic`, `ApprovalDecision`, `ApprovalOptionApprove/ApproveAlways/Deny` | **工具审批/权限层**（harness P1，仅 BasicAgent，`approval.go`）：Auto 默认零开销 / Ask 每**业务**工具执行前抛 `InterruptError` 等 caller 审批（审批问题在自己的相位里取提问序号 `NextAnswerSlot`，骑 cp 挂起；**一次调用只准入一次**：结论 Allow / 用户批准 / approve_always 规则放行任何一条路准入之后，续跑不再问、不再被推翻——改答 deny 不推翻，不带原 session、规则丢了也不再问；启发式拿参数的拷贝，就地改了不影响执行；工具中间件在审批之后每次执行都跑，它改的参数不经审批）+ `ApprovalInterruptReason`（审批中断的 Reason）与 `NormalizeApprovalAnswer`（审批答复的词表，**只有这一份**：审批层认不出按拒绝，agui / a2a 适配层认不出请对方重答；`Yes.`、`好的` 这类自然语言认不出）+ 启发式 seam（**不内置** shell allowlist，规则内容业务自负；**每次工具调用只判一次，判的就是执行的**——一批调用的内容（`OnToolStart` 改写之后的工具名与参数）在第一次交给工具执行时定下（`fixToolCallContent`，不论审批开没开、只另存被改写过的），审批结论在第一次形成时记在 `toolCallMemo.Decided`（审批生效时一律记，含无启发式的 ApprovalAsk），续跑按定下的内容执行、重放结论、不看当时的配置（加上或去掉启发式、开关审批都不推翻已形成的结论），审批问题在自己的相位里占提问序号；不这样时各有反例：每次重判会推翻用户答复、让工具自己的提问与审批问题串位，只按调用重放会让被改写成 1000 的参数沿用对 100 的放行，按内容重判会在钩子每次注入新值时推翻用户的拒绝，只在审批生效时固定会让工具账对不上、已成功的退款再执行一遍，均实测）+ `approve_always` don't-ask-again（存 RunSession KV、**按任务（RunID）分**：`NextTask` 换了 RunID 就是新的一套，复用同一个 RunID 开新任务时由编排入口清掉；粒度是同工具 + 同参数，整个工具用 `ApproveAlwaysTool`；**只对答应它的那个 agent 实例生效**——键带构造时铸的身份，编排其他成员、嵌套调用的被调方或调用方同名的工具是另一个工具、照样要问（曾经沙箱子 agent 里答的「一直允许」放行父 agent 的同名生产工具，实测），agent 被重建就再问一次（agui 每个请求重建 agent）；**跨 Resume 持久需 `WithResumeSession`** 复用 session）；框架工具（ask_user/activate_skill/…）豁免；拦截在 `makeToolsFn` runOne 内（规则 RMW 用 `approvalMu` 守护，fanout 并发安全），fanout 多工具 `MergeInterrupts` 一次问完；Resume 按答案 approve / approve_always / deny（拒绝=工具级 error 不 abort）。ReadOnly/Bypass 未做 |
| `agent_v2` | `WithOutputSchema(name, schema, opts...)`, `WithMaxValidationRetries`, `FinalOutputToolName`, `StructuredOutputError{SchemaName,Attempts,LastError}` | **结构化输出**（仅 BasicAgent，聚焦 P1，`structured_output.go`）：业务声明一次 output schema → 框架注册 `final_output` return-direct 自校验工具 → 校验（required + 顶层类型）失败经**既有 ReAct 循环回炉**（`tools→model` 边白送，非另造重试引擎）→ 成功后最终答复即 typed JSON（`FinalMessage().Content` 直接 `json.Unmarshal`）。**机制固定 ModeTool**；能力允许时 tool_choice=required 逼经工具收尾；A1 最终消息从**成功** final_output tool_result 的校验后 Content 合成（非模型 raw args）；A4 预算按**本次调用**里框架那件 final_output 实际被调的次数计（tools 节点按派发层的章记进 channel、跨 Resume 累积，不扫含 prior 的消息列表；改名的工具两个方向都对得上）超 maxRetries 抛 `StructuredOutputError`；「算不算一次失败」与 `LastError` 同一判据——这次调用在 `OnToolEnd` 之后是不是失败的：这次没产出通过 schema 校验的答复就算一次失败：参数不是合法 JSON、schema 校验失败、校验通过却被钩子否决（业务追加校验）、被钩子从失败改成成功（答复有效与否归框架，那次不成为答复，报钩子之前的失败原因）都算，回炉、同批业务 return-direct 工具让路（曾被挑成答复短路收尾，最终答复是失败文本不是 JSON；参数非法曾经哪处都不认）；框架那件 `final_output` 带「框架铸的」标记、豁免审批 / ToolFilter / tool_search 渐进披露 bind+dispatch 两侧（按实例判：没配 schema 时业务同名工具照常受这几道闸），由框架每次 Run 注入、不写进业务的 toolkit；与 Handoff/Routing 组合 construct 期 fail-fast panic。model 层（P0）：`WithJSONSchema`/`WithJSONObject` + `NormalizeJSONSchema(s, profile)`（2 档 `SchemaProfile`）+ `StructuredCapability`+`StructuredCapable`（provider 自报，openai strict / qwen·deepseek `NonStrictToolCapability`）。真 deepseek 冒烟 `example/harness/structured_smoke`。详见 [structured-output.md](doc/v2/structured-output.md)「实现状态」|
| `agent_v2` | `RunSession{RunID}` + **`NextTask`** + `History / SelfHistory / SelfHistoryCompacted / HistoryFor / HistoryForRaw / RestoreEvents / AddEvent / SetValue / GetValue / Events / ForkLane / CommitLaneEvents / RootSession / ParentSession` | 会话 + 富 API (ISSUE-79 半共享 child sess + ISSUE-80 lane events + ISSUE-84 HistoryForRaw owner-aware; audit follow-up 删 stale `AddEventInGraph`/`SupersedeEventsByMarker`). **SetValue/GetValue 是 per-sess 业务 KV (sub-agent 间共享, 不走 cp)** — 跨 Run 数据走 `store.Store`. **`RunID` 的契约是「一枚只服务一个任务」**（一次 Run → 零到多次 Resume → 终结）——它是 cp 主索引，复用同一枚开新任务 ⟹ 后一个任务成功收尾时按 sid 清整棵子树，**把前一个还挂着的挂起存档一并删掉且 `err=nil`**。开新任务调 **`sess.NextTask()`**（换 RunID、**保留 values / events**——跨任务要攒的东西挂在**对象**上不挂在 ID 上）。**任务的身份就是 RunID**：`WithSession` 把 ctx 上的 sid 推成它（RunID 非空时，覆盖已有的），approve_always 规则按它分（换 RunID 就是新的一套）。**在一个正在执行的任务里**（节点体、钩子、嵌套调用的被调方……）session 的 RunID 必须属于这个任务，对不上入口直接拒（嵌套位置上 `%w ErrRunAborted`）；RunID 为空的字面量 session 也拒（框架替它补就得改业务的对象）；Resume 只在顶层调、`sessionID` 与 `WithResumeSession` 的 RunID 必须一致。**开独立任务是显式动作**：`agent_v2.DetachTask(ctx)`（剥掉当前执行留在 ctx 上的状态、留下调用方配的环境，底下是 `compose_v2.DetachExecution`；会话 ID 与软取消信号也剥——独立任务不属于当前会话、不继承上一个 Run 的停止，要进哪个会话自己推）——框架不从「RunID 不同」去猜「业务想开独立任务」，当新任务读会让 `inner.Run(ctx, NewRunSession(), in)` 每次父节点重跑都新开一个、内层问两个问题就永远答不完（实测活锁）。脱离之后 session / Resume 的 RunID 不能是这一路脱离出来之前所在的任何一个**仍在执行**的任务（拒；`compose_v2.DetachedFrom` 记着这一串——经 DetachTask 还是直接用 `compose_v2.DetachExecution`、脱离几层都算，曾经只有 DetachTask 记、只记一层，实测绕过之后兄弟分支挂着的存档被删；「仍在执行」按任务判——任务挂起之后用当时在节点里存下的 ctx 脱离再续跑它是合法的，任务又续跑起来之后再拿那个 ctx 脱离照样拒（agent 级钩子里存下的经 DetachTask 脱离判不出存活、一律拒，续跑它用 `DetachExecution`）：不在节点执行范围里的 Run 铸一个存活标记、出口置否，配了 sid 的还在进程里登记这个任务在跑，进入任务自己的执行时把链里它那几次已经结束的执行去掉（还在跑的留着：节点里脱离之后直接 `compose_v2.Run` 这个任务的 sid，里面的 agent 入口照样拒）；曾经只看那一次执行，续跑期间用上一次存下的 ctx 脱离、拿这个任务的 session 跑 agent 被放过，那个 agent 收尾时删了这个任务挂着的存档，实测）；独立任务挂起时它的中断**不能**从节点返回（引擎终局拒收——答案送不进去，续跑时它从头再问、永不收敛，实测；原样返回、用 `IsInterrupt` / `errors.As` 取出来返回、再包一层、合并进别的中断都拒——记号打在交出去的那份拷贝上：曾经只挂在外面那层包装上，取出来的写法照样活锁；后来改成写在业务的中断对象上，包级复用的中断对象从此在普通任务里也被拒，均审计实测）。入口拒绝恒不可重试（在任务里、含脱离出来的，再 `%w ErrRunAborted`）。自研 `Agent` 在 Run 入口调 `agent_v2.EnterRun` 走同一个入口（判 session、盖任务章、遮住调用方的节点授予，`a2a.RemoteAgent` 已接；曾经只判 session，节点里被裸调的 RemoteAgent 的 OnAgentStart 拿到了调用方的路由能力，实测）。框架不替你判「这次算不算新任务」（只有入口知道），也不能偷偷帮你换（换了 Resume 就找不到存档）。想表达「这几个任务属于同一场对话」用 `compose_v2.WithThreadIDCtx` 的 ConvID（归历史层/观测层，**cp 这层不认识会话**）。哨兵 `agent_v2/session_task_contract_test.go`。 **6 层** state 边界见 [`doc/v2/store.md` §1](doc/v2/store.md)（Channel state / RunSession.Events / RunSession.values / Checkpointer / HistoryStore / store.Store——曾写作「4 层」，是 HistoryStore 那层加进来之前的旧数） |
| `agent_v2` | `AgentEvent`, `NewOutputEvent`, `NewExitEvent`, `NewChunkEvent`, `FinalMessage(events)` | 事件构造 + final 提取 |
| `store` | `Store` interface, `NewInMemoryStore`, `Namespace`, `NS()`, `Item`, `SearchItem`, `WithContext`, `FromContext`, `WithTTL`, `WithFilter`, `WithLimit`, `WithOffset`, `WithQuery`, `WithList*`, `TTLAware`, 6 sentinel `Err*`, **`ValidateFilter` / `MatchFilter` / `ValueEqual`** | 跨 thread 长期 KV 持久化层（用户偏好/长期事实/共享黑板/幂等记录）。InMemoryStore + 内置 redis / sql（PG·MySQL）+ 业务自实现 mem0 等后端。**业务自实现 backend 跑 `store/conformance.RunStoreTests(t, factory)` 验证语义合规**。**filter 语义（2026-09-03 拍板 type bracketing）**：`$eq`/直值是 JSON 语义相等（10 == 10.0、true != 1、数组逐元素不是包含）；`$gt/$gte/$lt/$lte` 的 filter 值必须数值（否则 `ErrFilterTypeMismatch`，静态、空 ns 也报），**存的值类型对不上是那一行不命中、不报错**——一条异构行不毒化整个 Search，下推型后端不读数据也实现得了；客户端过滤后端直接用 `store.MatchFilter`（in-memory / redis / example 三份拷贝已收成一份）。sql 后端的身份列按字节比较与排序：MySQL 用 VARBINARY，ns 2048 + key 1020 字节（InnoDB 3072 字节上限，按 4 字节字符算 512 / 255 个；`utf8mb4_bin` 是 PAD SPACE，只差尾随空格的 ns / key 会落进同一行），PG 声明 `COLLATE "C"`（不声明时 ORDER BY 跟库的排序规则走）；业务自管 schema 要照做。详见 `doc/v2/store.md` + `doc/description/three-way-comparison/store.md`「filter 类型不匹配语义」 |
| `agent_v2` | `WithStore(st store.Store)`, `WithStoreMemory(ns store.Namespace, *StoreMemoryConfig)`, `StoreMemoryConfig{SearchLimit, WriterID}` | Store 集成 BasicAgent option——Run/Resume 入口自动 pushStoreCtx 让下游节点/hook/tool FromContext 拿 store；WithStoreMemory helper 编译成 preamble Search 注入 + postamble Put 保存（Memory 接口已删，PR 3.3 commit `e6505f9`）|
| `agent_v2` | `Reflector`, `EvalFunc`, `ReflectOpinion{GoalMatched,Opinion,Critique}`, `WithEvaluator`, `WithReflectorAgent` | ReflectAgent 评估三档——R33 起**互斥 fail-fast**（多档同配 / ReflectorAgent 无 Evaluator 单配 → panic）；**R33 拆图** actor→evaluate→cond→[End\|feedback]：Reflector 签名扩 goal、verdict/critique 进 channel（feedback 中断 Resume 不重评 evaluator）、budget 判定在 feedback 前（耗尽轮不白跑）、evaluator/reflectorAgent 收 user-role 合成输入（含 goal+critique）、final.Annotations 标 reflect_goal_matched/reflect_rounds |
| `agent_v2` | `Aggregator`, `WithAggregator`, `AggregatorToMerger` | ParallelAgent 简化聚合（不暴露 BranchResult 元信息）|
| `agent_v2` | `RoutingChoice`, `RouteTool` const, `WithRouteTool`, `RegrantRouteTo`, `RegrantFromNode`, `RouteNoCoordinatorErrCode`, `WithRouter`, `WithRoutingClassifier`, `WithRoutingFallback`, `WithRoutingDescriptions` | RoutingAgent 的决策来源二选一：Router 函数（业务规则自己说了算，返空=显式 End）或升级链（决策方 → classifier → 兜底），两者互斥、同时配构造期 panic；classifier 装备走 `WithRouteTool()`（R33 RT-M3 框架内置 return-direct route_to——裸 ToolInfo / 普通 FuncTool 形态做不出路由决定）。**路由能力是签名不是环境**（`agent_v2/route_grant.go`，同转交那套）：RoutingAgent 经 RunOption 只授予分类器这个对象，`WithRouteTool` 只是 opt-in，**两把闸都开**才有 route_to（工具由框架每次 Run 注入，不进 toolkit）；编排分类器经 `runMember` 自动转授给成员（HandoffAgent 转给候选；`ParallelAgent` 的分支并发、不转授），业务壳显式 `RegrantRouteTo(opts, self, inner)`，GraphAgent 节点 / BasicAgent 工具与图里的钩子（agent 级 OnAgentStart/End 跑在图外、拿不到）显式 `RegrantFromNode(ctx, sub)`（取最近放进来的那一份：BasicAgent / GraphAgent 在 Run 入口放自己收到的，框架调起另一个 agent 的四个入口——编排成员、决策 LLM、RunSubAgent / AgentAsTool / skill fork、转交候选——给被调方放空的；所以子图节点、节点里直接 `compose_v2.Run` 的嵌套图都算本层，孙辈拿到空的。曾按「ns 相同」判，子图节点被当孙辈、转授静默落空，实测；它也转授转交授予）。曾经资格是环境：分类器在同一段执行里请教的 agent 定的路由就是分类器的路由（壳请教 helper 定 math、自己定 chat ⟹ 走 math，`err == nil`）。没被授予时模型硬发 route_to 拿到 `RouteNoCoordinatorErrCode` 软失败；被授予了却没有在等的 RoutingAgent（如经 `DetachTask` 开的独立任务）也是这个软失败（曾回「已路由到 X」并短路，模型以为定了路由）；`NewRouteTool` 已不导出|
| `agent_v2` | `RouteDecider`, `RouteRequest`, `RouteDecision`, `RouteEscalation`, `WithRouteDecider(d, minConfidence, opts...)`, `WithBranchMinConfidence`, `WithShadow`, `RouteUndecidedError`, `RouteDecisionOf`, `RouteReason*` / `RouteStage*` | **带置信度的路由决策**：决策方报分支与置信度，够门槛就采纳，低置信 / 弃权 / 非法结果 / 普通报错都升级到下一档（下一个决策方 → classifier → 兜底），原因记进路由决策事件的 `Escalations`。**往上抛的判据是 `compose_v2.MustPropagate`**，不看错误链上有没有 ctx 错——决策服务自己的请求超时要升级；classifier 那一档同一判据：有兜底时它失败降级的原因记在事件的 `ClassifierFailure`；**没有兜底时返回 `RouteUndecidedError`**（原因在它的同名字段；曾静默路由到 End 且 `err == nil`，已改）；同一批里多次调 `route_to` 且目标不一致时排在前面的生效（后面那次回一条失败结果）；分类器里并发几路都定了路由是终局失败（`SignalConflictError`），有无兜底都原样往上抛。门槛按档、可按分支覆盖，只和同一档比，不同决策方的分数不需要可比。`WithShadow()` 把一档设成影子档：照常调用、如实记录（`Shadow` 为 true，够门槛记 `RouteReasonShadow`），永不采纳，**对路由不可见**——后面各档的请求里没有它、它报错（含 panic，当场兜住）不挪重判起点也不进 `LastErr`、跨档约束只看非影子档、`RouteUndecidedError` 的错误文本里没有它；**不占真实档的下标**（身份 `route_shadow_<j>`、续跑记录另存），在途任务挂上或摘掉影子档续跑不错位（接管=加一个真实档，排在后面的真实档下标加一，那是按位置对齐的老问题，见 backlog）；用来在线上并跑攒样本，定好门槛后去掉这个选项让它接管。决策方在 classify 节点里跑，能调 `compose_v2.Ask` 问用户；**每一档调用前注入独立的调用身份**（`WithCurrentToolCallContext`），否则各档共用提问计数器，后一档的问题会拿到前一档的答案；框架调它时已套 `WithoutHistoryCtx`。每一档都没决定又无处升级时（含 classifier 失败又没有兜底）返回 `*RouteUndecidedError`：**任务层面的失败、不是终局**（曾包过 `ErrRunAborted`，已撤：它穿过所有把失败交还给模型的吞错点，包成工具时决策方弃权反而终结父 Run），存档留着、默认判据下外层不自动重试。以它收尾后 Resume 时按失败出在哪里重判，重新调用的档之前的档都原样重放（那些档里用户答过的问题不再问）：失败出在 classifier（没兜底时报错 / 没给出候选分支）时决策方重放、只重判 classifier，有决策方报错时改从第一个报错的决策方起；失败出在决策方链（没配 classifier）时有档报错从第一个报错的档起，没有就从头判。⚠️ classifier 默认不重试，一次瞬时 503 就以本错误收尾，要重试给它的模型套 `fallback.New` + `WithPerModelRetry`。**以失败收尾后重新调用的档换新身份**（`route_decider_<i>_retry<n>`，失败重试代数记在节点私有状态，三个失败出口都经 `saveRetryProgress` 加一、加一只在那一处）：问新问题、新 ID，它的子 agent 换命名空间从头跑——身份不变时，失败前从没兑付的旧答案会被套到内容已经变了的新问题上（实测把确认 refunds 的「是」用在「转到 delete_account 吗」上，送去注销账户，`err == nil`）。中断或必须上抛的错误退出后 Resume 时不加代数、报错的档照样重放，否则它这次若拿定主意会绕过用户刚答的问题。最后一个报错的档（决策方或 classifier，`ClassifierFailure` 以 `error: ` 开头即 classifier）的原始错误在 `LastErr`、**故意不上错误链**（决策服务或 classifier 模型自己的超时会被当成调用方 ctx 到期）。决策方收到的输入拷到 `Annotations` / `Metadata` / 内容块这一层——并行编排里兄弟分支共享同一个输入指针，只拷结构体时决策方写 map 会与兄弟的读并发、进程崩溃。`RouteDecisionAction` 带 `Stage` / `Confidence` / `Scores` / `Escalations`，用 `RouteDecisionOf` 取；指标仍只读 AgentName 与 Target。权威文档 [doc/v2/route-decider.md](doc/v2/route-decider.md) |
| `agent_v2` | `WithHandoffTransfer`, `HandoffTransferToolName`, `HandoffTo`, `WithHandoffHistoryMode`(Rewrite默认/Raw/SelfOnly), `WithHandoffInitial`, `EndConversation`, `HandoffPromptPrefix`, `HandoffSignalKey`, `(*HandoffAgent).State(sess)` | Handoff **模型 B 对话控制权转移**（transfer 工具 LLM 自主转交 return-direct + 持久 active store.Store 跨 Run + 历史可见性后接看前接默认 Rewrite §9）；详见 doc/v2/handoff.md |
| `agent_v2` | `PlanStep{Description, SubAgentName, TmpSubAgent, InputRefs}`, `RefSpec{Index *int, AgentName}`, `IndexRef`, `AgentRef`, `ExecutedStep`, `Planner`, `ExecInputBuilder`, `TmpSubAgentConfig` | PlanAgent 类型集 |
| `agent_v2` | `(*PlanAgent).AddSubAgent / SetExecutor`, `WithSubAgents`, `WithPlanModel`, `WithPlanToolkit`, `WithExecInputBuilder`, `WithPlanMaxSteps` | PlanAgent 链式 + options |
| `agent_v2` | `NewLLMPlanner(m, opts...) Planner`, `WithLLMPlannerSubAgents` | 内置 LLM 驱动 Planner（无导出的 `LLMPlanner` 类型，构造函数返 `Planner` 接口）（自动从 sub-agent.Description() 抽职责注入 prompt）|
| `agent_v2` | `AgentAsTool`, `RunnableAsTool` | Agent 包成 tool（plan_chat_web 用）|
| `agent_v2` | `AskUserToolName` const, `WithAskUser()` | HITL ask_user 内置工具集成（跟结构化输出的 `final_output` 一样由框架每次 Run 注入、**不写进业务传进来的 toolkit**，跟 `WithToolkit` 先后无关——曾经写进去，几个 agent 共用一个 toolkit 时没配的 agent 也拿到了，没配结构化输出的那个拿别人的 schema JSON 当最终答复收尾，`err == nil`）|
| `agent_v2` | `WithCtxOrchHooks(ctx, h)`, `GetCtxOrchHooks(ctx)` | 高层 hook ctx 透传（嵌套子 agent 自动继承父 hooks）|
| `agent_v2` | `StoreInjectedAnnotation`, `AutoCompactSummaryAnnotation`, `MicroCompactClearedAnnotation`, `MicroCompactOffloadedAnnotation`, `CompactionSummaryAnnotationKey`, `SkillBodyAnnotation`, `UnresolvedCallsRecordAnnotation` + `IsUnresolvedCallsRecord(m)` | 跨层 marker 常量（message annotation 类型——4 层压缩对带 marker 的 message 一致防护——AutoCompact 抽出不进 summary、ReactiveCompact emergencyDrop 跳过; audit follow-up 删 stale `SupersededEventAnnotation`; `MicroCompactOffloadedAnnotation` offload 占位符 + 配套 `MicroCompactOffloadKeyAnnotation` 存 key）。**`UnresolvedCallsRecordAnnotation`** 标「这条 assistant 是框架合成的记账文本」（`dropDanglingToolCalls` 给零结果的工具轮补的那条，内容形如「此前发起过以下工具调用，但历史中没有它们的结果：xxx」）——它是「assistant + 零 tool_call」，**正好长得像一条最终答复**，所以凡是框架**挑一条消息代表某件事**的地方都必须经 `IsUnresolvedCallsRecord` 跳过它。仍在扫、仍需谓词的有 2 处：`observability.lastAssistantMessage`(agent_output event 内容) / `ToolFilter` 的 lastInput。（`lastAssistant` / `finalAssistantOf` 曾登记在内，现已随「最终产出改由决策点写专用 channel」删除——记账文本在 preamble 侧合成、进不了那条 channel，那两处的防护从「靠谓词」变成「结构上不可能」。）**业务自己扫 messages 挑答复时同样要调**。<br>⚠️ **agent 的前言不靠 marker**——`WithPromptTemplate` 渲染结果与 `WithSystemPrompt`（含 skill hint/body）走 **`schema.ModelInput.Preamble`** 这个**独立字段**，跟 `Messages` 并列，压缩层结构上碰不到。这里加过一个 `AgentPreambleAnnotation`（拼进 `Messages` 过 hook、再按 marker 劈回来），**已删**：实测两处「N=1 对、N≥2 静默错」——业务插在前言之前的消息劈完跑到了它之后，业务造新 system 想顶掉前言时两条都发出去。合并再拆开这件事本身就是那个坑的来源。⚠️ 「共用谓词」不产生保证——每处都得有自己的断言（实测：任一处 revert 掉跳过，若无专属用例则全套仍绿）|
| `agent_v2` | `WithSubAgentStoreScope(ctx, true)` + `IsSubAgentStoreScope(ctx)` | StoreMemory 跨 ns 协议 ctx helpers。⚠️ **框架自己已不读这个 flag**——它曾守在 `commitFinal` 的 store.Put 前面，而那道闸让成员配的 `WithStoreMemory` **一条也写不进去**；防重复写改由「写入者维进 ns」解决（顶层写基准 ns、成员写 `基准ns/<位置>`，本来就不重复）。业务后端可以继续读它做自己的判断，但**别用它整个 skip**（那会把框架的修复抵消掉）。**push 点收敛在 4 处**（`runMember` / `runDecisionAgent` / `runNestedCall`（RunSubAgent + AgentAsTool + SkillTool fork）/ Handoff 铸造）——曾是 15 处、由注释里一句人肉计数维持，而 `5afb8eb` 那次加协议就漏了 2 处 |
| `agent_v2` | **`runMember` / `runDecisionAgent`**（包内，`member_dispatch.go`）| **编排调用别的 agent 的唯二入口**。7 编排此前各自手写同一段派发代码（9 个成员 + 3 个内部决策 LLM 共 12 处），每处承载 8 条协议（子 ns / 历史归属 / store scope / 重试 / 转交转授 / 中断落输入 / 产出提取 / 边界补记）——**漏任何一条都不报错**，只静默退化。现在：`runMember(ctx, grants, memberSpec{...})` 跑编排的**成员**（进历史、可继承编排收到的转交 / 路由能力，`grants` 是**位置参数**——空值 `memberGrants{}` 是合法值「这个成员不该有任何能力」，放进结构体会让「忘了想」和「想过了不给」变成同一个字面量）；`runDecisionAgent(ctx, nsSuffix, ag, sess, input, route)` 跑**内部决策 LLM**（分类器 / 评估器 / 反馈教练——不进历史、**永不**拿转交能力；`route` 只是 RoutingAgent 给自己的分类器新铸的那一份，参数类型是 `*routeGrant`，转交授予传不进来）。**两个函数就是两种语义**，下一个写编排的人面对的是一道有名字的选择题而不是 8 条要背的协议。机械对账见 `TestMemberDispatch_AllAgentRunsGoThroughHelper`（第 5 处直调 `Agent.Run` 就红）+ `TestHandoffGrant_EveryReadIsSelfChecked`（裸读 handoffGrant / routeGrant 不自查就红）|
| `agent_v2` | `SkillDisabledErrCode` (= `"skill_disabled"`) | **tool error code 非 message marker**——P2.A `activate_skill` 拒绝 disabled skill 时填 `tool.Result.Error.Code`；不在 `tool.IsFrameworkError` 列表，反馈 LLM 让其换 skill 重试（不会触发 4 层压缩或 abort 整 Run）|
| `agent_v2` | `MicroCompactConfig` / `AutoCompactConfig` / `ReactiveCompactConfig` + 对应 `Run` | 4 层压缩第 1/2/3 层（第 4 层是 Store + Session）|
| `agent_v2` | `IsPromptTooLongError`, `ParsePromptTooLongGap` | ReactiveCompact retry 触发判定（regex 解析 OpenAI/Claude prompt-too-long 错误）|
| `a2a` | `RemoteAgent`, `NewRemoteAgent(ctx, cardURL, opts...)`, `NewRemoteAgentFromCard`, `NewRemoteAgentTool` | 消费远程 A2A agent（官方 a2a-go SDK）：agent 形态进编排 / tool 形态（=AgentAsTool）进 BasicAgent toolkit；Task 状态机 + taskID 续跑（session KV，Resume 需 `WithResumeSession`）+ 流式 chunk（EventSink）|
| `a2a` | `WithName / WithDescription / WithClientOptions / WithHumanGate / WithHooks / WithDiscoveryAgentName` | RemoteOption：改名/描述；auth + otelhttp trace 注入（经 a2aclient FactoryOption）；input-required 默认返追问让 host LLM 自动补、`WithHumanGate` 抛 InterruptError 骑 host cp 真人 HITL（远端挂多问时每问一个宿主提问、答完按 interruptId 回；远端只挂一条审批时宿主那一问也标成审批；默认模式下远端挂多问时接受 interruptId → 答复的 JSON；等人答复的那一问按调用位置记进 host 节点私有态、随存档走：没拿到答复不联系远端、原样再问；远端任务结束之后再跑发原始请求、新任务的追问换新 ID；节点里几次调用 / 同批几个 tool_call / 并行分支各续各的；续跑不依赖原 session；可多轮；引擎钩子 / 条件边里没有提问位置，远端要人时报错；不在任何节点执行里时沿用 session 句柄）；`WithHooks` fire OnAgentStart/End 对（agent 形态观测）；`WithDiscoveryAgentName` 限发现指定 agentName（同 service 多 agent 精确路由，仅 FromService 生效）|
| `a2a` | `NewRemoteAgentFromService(ctx, dir, ns, svc, sel, opts...)` + `NewRemoteAgentToolFromService` + `ErrNoEligibleInstance` | 服务发现 glue：Discover（聚合该 svc 下所有 agentName 实例）→ 可选 `WithDiscoveryAgentName` 过滤 → `registry.Eligible` → Selector（去重按 `InstanceIdentity`）→ 拉 card 建 RemoteAgent（card 失败 §8 failover 换实例）；无可路由实例返哨兵（errors.Is 区分"等注册"vs"全失败重试"）|
| `a2a/registry` | `Directory` / `Registration` 接口, `ServiceInstance`(含 `AgentName`), `NewInMemory`, `WeightPtr`, `InstanceIdentity`, `ParseDiscoveredJSON`, `ErrInstanceExists` | A2A 多语言注册发现协议服务目录：§3 模型（Weight 是 `*int`——nil=未给→默认 100，`&0`=显式不路由；`AgentName` 区分同 serviceName 下多个 Agent Card，缺省=serviceName）+ §3.3 双侧校验 + InMemory 参考实现（重复注册按 4 元组 ns+service+agentName+instanceId 判定返 `ErrInstanceExists` §4.3）；binding 从 wire 解析节点统一走 `ParseDiscoveredJSON`（malformed 跳过防一坏炸全）；selector/failover 去重身份用 `InstanceIdentity`(agentName+instanceId) 非裸 instanceId（同端口多 agent 不互屏蔽）|
| `a2a/registry` | `Eligible`, `Selector` 接口, `NewRoundRobinSelector / NewRandomSelector / NewWeightedSelector` | §6 选择：SERVING+weight>0 过滤 + 轮询/随机/加权三策略（同次 failover 不重选已试过的）|
| `a2a/server` | `NewExecutor(agent, WithOwnerResolver(fn))`, `Handler(agent, baseURL, WithOwnerResolver(fn))`, `WithTaskStore`, `Card`, `Instance` | 把 `agent_v2.Agent` 暴露成 A2A server（a2asrv 适配，RemoteAgent 的逆）：server 侧 HITL（taskID↔RunSession/cp Resume 映射 + 多轮 + Cancel 协调；挂多问时每一问带 interruptId、状态消息附挂起清单 DataPart，对端按 interruptId 分别作答——一条文本不再灌给全部挂起项，可以只答其中几问；工具审批的答复只认固定词与 `{"approved": bool}`，同一问两种写法不一致、答复形状不对、重发答过的，都不调 Resume、继续 input-required 请重答，约定见 `a2a/pending.go`；**服务端按任务累积已接受的答复、每次续发整份交给 Resume**——没进工具账的提问（ask_user、节点里直接 Ask 的）续跑时会重问，只交这一条消息的话一个节点先后问两问就来回重问、永远答不完（实测，审批分支之前就是这样））+ 流式（chunk→artifact 桥）+ ThreadID=A2A ContextID（观测会话聚类 + HandoffAgent 跨 task 持久转交）；业务 OrchHooks 经 HTTP middleware push 自动与流式桥 chain。⚠️ **`WithOwnerResolver` 必配**（构造期 panic）——contextId / taskId 都可以由客户端给，单靠它们谁知道谁就能续别人挂起的 task / 读它的历史（实测过 tasks/get 与 tasks/resubscribe 两条路）。单租户返固定值即可，多租户从认证中间件放进 ctx 的主体取；换持久 task store 走 `WithTaskStore`（框架自动包归属检查，别自拼 a2asrv 栈）|
| `a2a/zk`（子模块） | `zk.New(servers, timeout, opts...)`, `WithRootPath`, `DefaultRootPath`, `DumpSubtree` | ZooKeeper Binding v1（§4）：**4 层路径 `{ns}/{serviceName}/{agentName}/{encodedInstanceId}`**（Discover 遍历 agentName 中间层聚合该 service 下所有 agentName 实例）+ ephemeral 实例叶子 + Base64url-no-padding 编码 + path↔payload 三段一致性校验（serviceName/agentName/instanceId）+ malformed skip 带 slog；`DumpSubtree` raw dump 任意层级排查跨语言对端结构——跑 `registry/conformance` 验收（integration tag，Docker testcontainers 或 `ZK_ADDR=host:2181`）|
| `agui` | `Handler(AgentFactory, WithOwnerResolver(fn), WithCheckpointer)`, `AgentFactory`, `NewSSEWriter`, `Event` + `New*`（RunStarted/Finished/Interrupted/Error/Step*/TextMessage*/ToolCall*/StateSnapshot/Custom） | **AG-UI（Agent-User Interaction Protocol）适配包**——agent 事件流→AG-UI 标准事件经 SSE 推前端（业务直接接 AG-UI 生态现成前端组件，不必自写前端）。`Handler(AgentFactory)` **per-connection** 构造挂 adapter（一个 struct 同时实现 `hook.Hooks` + `agent_v2.OrchestrationHooks`）——因 RunOption 无 per-Run hook.Hooks 注入，故每请求经工厂构造挂独立 adapter；**非 BasicAgent 编排**工厂把 adapter 挂到每个 sub-agent（tool/model 走 hook.Hooks 构造期注入），chunk 走 ctx OrchHooks 嵌套继承（handler `WithCtxOrchHooks(ctx, adapter)` push，sub-agent 沿 ctx 逐段真流式）。映射：chunk 按 `run_id` 分流→`TextMessage*`、非流式（Generate 无 chunk）OnModelEnd 补发完整 `TextMessage`、`OnToolStart/End`→`ToolCall*`（`CallId` 作 toolCallId 配对锚点）、run 边界→`RunStarted/Finished`。**工具审批**：中断按 AG-UI 核心值 reason=`tool_call` 发（带 toolCallId 与 responseSchema `{approved: boolean}`（不收别的字段），四个选项放 metadata；没有工具调用 ID 时用 `confirmation`），续跑前校验审批答复（true / false、只有 approved 一个字段的对象、选项字符串；带 `editedArgs` 的拒绝——只取 approved 会按原参数执行），认不出回 `invalid_resume_payload`、这些问题保持挂起，一批里有一条认不出整批不采纳——曾经 payload 压成文本交给审批层、认不出按拒绝，`{"approved":true}` 被当成拒绝。**resume 列表恰好覆盖挂起的中断**（AG-UI 1.0 不支持只答一部分）：未知 ID、重复 ID、漏答都回 `invalid_resume`；**采纳过的答复按任务累积、每次续跑整份交给 Resume**（同 a2a/server，理由相同）。**HITL 闭环**：`WithCheckpointer` + in-process session store（(owner,threadId)→sess **两维结构体键不拼串**；busy 并发保护同 threadId 拒并发 Run/Resume + WithMaxSessions 上限防 OOM + serveRun 覆盖挂起先清旧 cp + ServeHTTP panic recover 兜底 + nil agent 守卫），前端带 `resume:[{interruptId,status,payload}]`（对齐官方 ResumeEntry：status=resolved/cancelled，全 cancelled→ResumeActionAbort 终止发 **`RunFinished(outcome.type=cancelled)`**——⚠️ 这里曾发「无 outcome」的 RunFinished 并注明「官方判别联合无 cancelled 分支不能塞」，**那在当时是对的、2026-09-17 的 1.0 定稿后变成违规**：1.0 加了 cancelled 分支，同时规定缺省 outcome **意味着成功**，于是原方案把取消报成了成功完成的 run。另：**续跑的 wire `runId` 取 `in.RunID` 不是 `sess.RunID`**（后者是我们的「一个任务」身份、是 cp 主索引；AG-UI 的 runId 是「一次 run」身份且 MUST NOT 复用），`RUN_ERROR` **不带 runId**（1.0 schema 是 closed 的，多发会被 strip），工具结果的 `messageId` 用 `<runID>/<callID>` 保 thread 内唯一）续发→`agent.Resume`（`InterruptError`→`RunFinished(interrupt)`；同 a2a/server 限制：重启丢/跨进程不支持）。**StateSnapshot 业务 opt-in**（节点内 `EventSink.Send(Type:"state_snapshot")`——无统一 state schema + Go 擦除故不自动发，见 state-model.md §六-9）。内联精简协议层零重依赖（字段/json tag 照 ag-ui wire camelCase + Unix ms timestamp，不引官方 Go SDK 的 logrus）。真 qwen 冒烟 `example/agui/agui_smoke` + HITL/SequentialAgent 单测。⚠️ **`WithOwnerResolver` 必配**（构造期 panic）——threadId 由**客户端**生成，不带调用方身份时谁猜中谁就能续别人的挂起会话（读到里面的提问、替对方回答）。owner 进会话键 + push 给下游（HandoffAgent 持久转交、会话历史层都按它分区）。**淘汰也按 owner 分摊**：份额最大的那个出账、**到达者不赢平局**（平局由发起请求的 owner 自己出，出不起就谁也不淘汰）——只按份额不管平局时，「跑完即走」的请求份额恒 1、跟受害者持平就回落全局最旧，实测一个请求就清掉别人的挂起会话连同 cp。代价是**上限变软**（≈ `max(maxSessions, 有挂起会话的租户数) + 在途数`，增量挂在**身份数**上不挂在请求数上）。决策见 [doc/issue/issue-49-frontend-stream-adapter.md](doc/issue/issue-49-frontend-stream-adapter.md) |

## 编排模式一览

9 编排都实现 `agent_v2.Agent` 单接口，可无限嵌套，自动支持 HITL Resume。

| 模式 | 包 | 核心逻辑 |
|---|---|---|
| `BasicAgent` | `agent_v2` | ReAct 循环（preamble + model + tools + postamble 四节点 channel-based Graph + 循环回边；preamble 跑 StoreMemory/HistoryCompactor/Skill 预激活只跑首轮 ISSUE-74；postamble 跑 commitFinal + StoreMemory store.Put ISSUE-75；tools 节点内 N>=2 tc goroutine 并发 fanout ISSUE-81）|
| `SequentialAgent` | `agent_v2` | 线性 step 链；`AddStep(name, ag)` 链式 |
| `ParallelAgent` | `agent_v2` | 并发 fan-out + Merger / Aggregator 聚合；`AddBranch(name, ag)` 链式 |
| `LoopAgent` | `agent_v2` | 循环 body + ExitCondition（`(ctx, lastOut, round) bool`）+ maxIterations |
| `RoutingAgent` | `agent_v2` | Router 函数，或「决策方 → LLM Classifier → 兜底」升级链，二选一（同时配构造期 panic）|
| `HandoffAgent` | `agent_v2` | **模型 B 对话控制权转移**：LLM 调 transfer_to_agent 工具(return-direct)转交，控制权**持久**转给新 agent(store.Store 跨 Run，非 cp 因 cleanupOnSuccess 删 cp)；后接默认看前接历史(WithHandoffHistoryMode 默认 Rewrite)；maxHops 防环；转交决定是上行信号（transfer 工具 / 成员经 `HandoffTo` 做出的是同一类：先后做出的后者为准、同一超步两个成员都做了决定按设计错误终局、同一批 tool_call 排在前面的生效；Parallel 候选的分支拿不到转交能力），跟着写入者的提交走；Reflect 候选里被评审否掉那一轮的决定随那一轮的产出作废 |
| `PlanAgent` | `agent_v2` | replan-每步：`plan ↔ exec` 循环；sub-agent / TmpSubAgent / executor 三种执行器（按优先级）|
| `ReflectAgent` | `agent_v2` | actor + Reflector / Evaluator+ReflectorAgent / EvalFunc 三档评估 |
| `GraphAgent` | `agent_v2` | 包任意 `compose_v2.Graph` 为 Agent（caller 提供 InputWriter / OutputReader）|

## 信号优先级（路由 / 转交 / 解析）

1. 框架入口做出的上行信号（首选）—— Routing：被授予路由能力的分类器（及它显式转授的组成部分）用 `route_to` 工具（`WithRouteTool` opt-in，`RouteTool="route_to"`）做出的路由；Handoff 模型 B：`transfer_to_agent` 工具（`WithHandoffTransfer`）或成员经 `HandoffTo` 做出的转交（两条做出的是同一类决定）。决定是**上行信号**（`compose_v2.EmitSignal`，收件人由引擎铸、别的代码算不出来），**跟着写入者那段执行的提交走**——中断续跑不丢、失败 / 被否决 / 结果是「没成」的工具调用（`OnToolEnd` 跑完之后仍是失败的：超时、`tool.Failure`、中间件或钩子改成失败）里做出的作废；先后做出的后者为准，同一批 tool_call 排在前面的生效（后面那个回一条失败结果），同一超步两个成员都做了决定的在合并点按设计错误终局（`SignalConflictError` 并带 `ErrRunAborted`，分类器失败的降级不吞它）；消费方（HandoffAgent 的 step 节点 / RoutingAgent 的 classify 节点）在候选 / 分类器那次调用成功之后按收件人取（见 `agent_v2/side_signal.go`）
2. Routing 的兜底：从分类器最终消息解析——先看消息里的 `route_to` tool_call 参数，再看 `msg.Annotations["route_target"]`（编程注入；注解键是字面量 `route_target`，不是导出常量）。**Handoff 没有 annotation 通道**：`HandoffSignalKey="handoff_target"` 是 stepFn 裁决之后写给条件边的内部字段，入站产出上的会被清掉（那条通道绕过授予与候选校验，已删）
3. 文本内容去掉首尾空白之后整段等于某个分支名（Routing 兜底解析的最后一种，不做关键词匹配）；都取不到时有兜底分支走兜底，没有就返 `RouteUndecidedError`

## 关键不变量

- **HITL 中断寻址**：`AskQuery.InterruptID` 由框架按 `(graphNS, nodeID, 实例身份, visit, tc ID, tc 下标, idx)` 七个位置维 + 在不在 tool_call 里 + 相位（钩子、交接出去的那一段各有名字；节点体为空）+ **任务代号** + 重做代际（被否决后重做时，按分域）+ 外层代际链（嵌套图里、外层执行被否决重做时；交接过来的主图里承载它的那一项是交接那一格的代际、不算调用方那一层——调用的身份决定是不是同一次调用；调用方所在图之外的各层照带——那几层重做时调用方的图从头跑、完成记录跟着没了，被调方重新产出，提问必须换新，曾经整条清空时对 plan-v1 的批准兑付给了 plan-v2，实测）算稳定 ID（后四样取默认值时不进哈希）。tc ID 可能为空（有的 provider 不回），「在不在 tool_call 里」另有一维——只靠 ID 与下标时，钩子里的提问与 tool_call #0 里的提问撞成同一个 ID、整个 Run 终局失败（实测）。**业务不能自填**——`Ask` 对非空值当场拒（自填=两个不同提问共用一个答案位，实测三种泄漏）；自填只在直接构造 `&InterruptError{Query: ...}` 那条路上合法，语义是**给前端的标签**、不参与取答案；多 pending 存 `cp.PendingInterrupts []*AskQuery`；`agent_v2.WithResumeUserInputs(map[InterruptID]*Message)` 配对答案
- **InterruptError.AllQueries() 严格非空**：v2 fail-fast——单 query 用 `Query` 字段，多 query 用 `Queries` 字段（ISSUE-81 fanout，与 Query 互斥 Queries 非空时优先）；业务"用户暂停按钮"等场景填虚拟 Query Question 即可（`plan_chat_web/hooks.go::interruptHook` 范式）。framework graph_loop 在 **node 路径** fail-fast 不允许 `len(ie.AllQueries())==0`；**cond fn 禁止 interrupt**（cond 是**纯判断**：读 channel → 返目标，不产 writes/不改 state。中断的本质是**要新输入**；cond 需要新输入才能判断 = 判断所需信息没备齐 = **上游 node 没尽责**，该由 node 去问、答案写 channel、cond 只读现成的判方向。且 cond 是纯函数，Resume 后要重新执行，答案还得从 channel 读——让 cond 中断绕一圈还是回到「让上游备好」。graph_loop 一律转**非-interrupt** error 拒绝（**不** `%w` 原 err，否则 IsInterrupt 仍 true 但已不 record → 无 ID 可配 → Resume 死锁）并引导改用 node。RoutingAgent 的 Router/classifier 已在 classify node 内跑，原生支持 HITL）；**cond 报错的 Resume 语义业务声明**（`AddConditionalEdge(from, cond, WithCondFailPolicy(...))`）：纯判断报错只可能是「判断前提不成立」= 上游数据缺失/非法，该重试还是放弃取决于**上游是否确定性**（纯计算重跑必拿同样坏数据；LLM 上游重跑很可能就好），只有业务知道 → `CondFailAbort`（**默认**，终局失败——wrap `ErrRunAborted` → Run 出口**主动 DeleteSubtree 清 cp 子树**；「跳过写」不够，单槽覆盖式下先前的 cp 会幸存被 auto-load 捡起）/ `CondFailRerunFrom`（存 **Pending** cp 让业务 Resume 重跑 from——注意档位判据是**框架对这个错的处置意图**而不是错的类型：它返的是业务错，只按「是不是控制流信号」判会被标成失败现场（2026-08-13 起续跑闸门按内容判、不读 Status，续跑本身不受影响，差别在 `ListPending` 列不列得出它）；见 `shouldStayPending`。⚠️ **对 Append/Sum 不再双计**——出边算在提交之前，算不出来的成员不算成功、writes 从没进过 channel，旧的「须保 writes 幂等」义务已随之解除（`graph.go` 的 `CondFailRerunFrom` godoc 为准）；外部副作用用 `SaveNodeState` 记进度；节点里调模型的产出按 `Rerunnable` 记、经 `RunSubAgent` 调的产出那次声明 `agent_v2.WithRerunnable()`，本档否决时作废、重跑时重做）；**IsInterrupt 穿透 errors.Join `[]error` 多错误树**（asInterrupt 认单链 `Unwrap() error` + 多链 `Unwrap() []error`，对齐 errors.As；graph_loop 部分中断走 `errors.Join(FirstErr, applyErr)` 依赖它，漏 `[]error` 分支则 HITL Resume 静默失效）
- **多 tool_call 部分中断**：toolsNode 整节点失败（channel writes 作废），但每个 tool_call 成功那一刻就记进节点私有 memo（`toolCallMemo`，载体 `compose_v2.SaveNodeState`、随 cp 走）——Resume 重跑 toolsNode 时命中的**不调函数体**、重放产出（含那次调用做出的转交 / 路由决定——上行信号随 memo 记下、命中时交出），只有中断/失败/没跑到的执行；`OnToolStart/End` 仍全部 fire。业务幂等只剩 memo 覆盖不到的场景（进程在 tools 节点执行中途崩溃——memo 只随节点「没成」的出口存档落盘，durability 在超步边界写档、带不上它，配 sync / async 也一样 / `SaveNodeState` 失败 / 没做出上行信号的框架元工具刻意不进 memo）；ask_user 二阶段语义自动幂等（GetResumeAnswer 命中即返答案不抛）。「rerun-all + 业务自负幂等」是 2026-07-30 之前的旧说法。详见 `doc/v2/hitl-resume.md` §6
- **fan-out 超步统一**：所有 fan-out 编译为图原生超步 frontier，复用 superstep_snapshot 的 per-branch delta；Resume 默认仅重跑失败/中断分支，成功分支不重跑
- **超步部分失败时返给调用方的那一个 error**（`frontierResult.exitErr`）：优先级 **终局失败（`AbortErr`）> 坏存档（`NotResumableErr`）> 档没写成（`NotPersistedErr`：嵌套执行出口写档失败、槽属于别人所以不带 `ErrRunAborted` 的那种）> 中断 > 第一个业务错**——前三档之后，有任何分支中断就返中断（1 个时返该分支的**原始 error** 不返抠出来的裸 ie，链上并列挂的 `ErrCheckpointStale` 等第二 sentinel 必须保住；多个时 `MergeInterrupts` 合成一个带 N 条 query 的），没有中断才返第一个业务错。**不聚合成多错误树**：`errors.Join` 会让 `errors.Is/As` 的语义从「这个错是不是 X」静默变成「有没有哪个分支是 X」（实测 2 个可重试业务错 + 1 个 wrap 了自己下游 `DeadlineExceeded` 的分支 → `IsControlFlowSignal` 返 true → 那 2 个再也不重试）。要看每个失败分支各自的原因走 `OnNodeEnd`（每个失败分支各 fire 一次，带分支身份）
- **嵌套 cp 物理分离**：每个嵌入运行 sub-agent 通过 ctx push graphNS 让子 cp 物理写到 `(parentSID, parentNS/childName)`——多层嵌套 cp 不互覆
- **cleanupOnSuccess**：Run 1 完成后自动清 cp；Run 2 同 sessionID 不被旧 Pending cp 污染
- **chunk 不写 session.Events**：streaming chunk（`AgentAction.IsChunk=true`）通过 OrchHooks.OnNodeEvent 实时投递给前端 SSE，不写审计；最终聚合 assistant 走 OutputEvent / ExitEvent 写审计
- **GraphID 防 cp 串台**：两个不同 Graph 共享同 sid 时 SaveNS / Run 入口 fail-fast 返 `errors.Is(err, ErrGraphIDConflict)` 哨兵

## 4 层上下文压缩

> 完整设计文档：[`doc/v2/context-compaction.md`](doc/v2/context-compaction.md) — 算法/阈值公式/9 段/NO_TOOLS_PREAMBLE/附件重注入/断路器/跨层 marker/时序图/排错决策树

| 层 | 何时跑 | 何处跑 | 调用 LLM | 用法 |
|---|---|---|---|---|
| **第 1 层 MicroCompact** | model 节点入口（先于 AutoCompact）| `agent_v2/microcompact.go::MicroCompactRun` 由 `modelFn` 最早段触发 | ❌ | `WithMicroCompact(&MicroCompactConfig{Enabled: true})` |
| **第 2 层 AutoCompact** | model 节点入口（MicroCompact 之后）| `agent_v2/autocompact.go::AutoCompactRun` 由 `modelFn` 早段触发 | ✅ 摘要调用 | `WithAutoCompact(&AutoCompactConfig{Enabled: true, Model: m, ModelContextWindow: 200_000})`<br>⚠️ 两道**成本守卫**：`MinCompactableTokens`（默认 5000）可压缩区太小就静默跳过、**不计断路器**（「没东西可压」不是失败）；`MaxReductionRatio`（默认 0.8）摘要没比原文小够就判压缩无效、回退原 messages、**计入断路器**（底层 Generate 成功 ≠ 业务成功，否则每轮白烧一次调用）。微缩测试配置要显式调小前者 |
| **第 3 层 ReactiveCompact** | LLM 报 prompt-too-long 时 | `agent_v2/reactive_compact.go::ReactiveCompactRun` 由 modelFn retry 包围 | ❌ | `WithReactiveCompact(&ReactiveCompactConfig{Enabled: true})` |
| **第 4 层 StoreMemory** | 跨 Run | `RunSession` + `store.Store` 接口（preamble 节点调 `store.Search` 注入；ISSUE-74 修后业务可在 Store 实现内抛 InterruptError 做 HITL gate）| ❌ | `WithStore(st)` + `WithStoreMemory(ns, &StoreMemoryConfig{SearchLimit})` |

执行顺序（每轮 model 节点入口）：MicroCompact → AutoCompact → LLM 调用（含 ReactiveCompact retry）→ tools。

**关键不变量**：
- **agent 的前言不在被压缩的消息流里**（`WithPromptTemplate` 渲染结果 + `WithSystemPrompt` 含 skill hint/body）——它走 `BasicAgentPreambleOutput.System` 那条独立 channel，作为 `schema.ModelInput.Preamble`（与 `Messages` 并列的[消费]档字段）交给 `OnModelStart`，再由 model 节点在 `callLLM` 内、四层压缩全跑完之后 prepend。判据从「前导位置」变成**没有判据**：曾经三处（`autocompact` 的 `convStart`/`sysEnd`、`reactive_compact` 的 `sysEnd`）拿位置当身份，配了含 few-shot 的模板就认错——实测紧急压缩直接丢掉真 sysPrompt、AutoCompact 把它连同 skill 正文摘掉、`shouldTrigger` 把 120k 字符的它算进阈值，**全程 `err == nil`**。那三处现在剩下的是长期记忆注入与调用方自己塞的 system。对照与决策见 [context-compaction.md 结论五](doc/description/three-way-comparison/context-compaction.md)
- 删消息组用 `compose_v2.GroupMessagesByAPIRound`，每条 user 消息开启新一组，后续 assistant + tool_result 加入当前组——保 tool_use ↔ tool_result 配对完整，避免 OpenAI 422。**system 消息单独成组**（既开新组也关新组）：压缩层按组决定丢不丢、含 system 的组受保护，混组会把整轮 assistant + tool_result 一起变成永久不可压缩——实测 `[user][system, 30KB assistant][user]` 下摘要跑完**入 30016 出 30059 字节反而变大**、还白烧一次 LLM，而阈值下一轮照样触发。⚠️ 那句「单独成组」在 2026-09-08 之前**只写在注释里没做到**（只让它开新组、没让它关）
- `compose_v2.AdjustIndexToPreserveAPIInvariants` 切点拉回保 RunID 块连续——thinking + tool_call 不分离
- 删后首条若是 assistant，前面补 user marker（API 要求 user 开头）
- AutoCompact 失败不阻断主链路——记账到断路器，emit session 事件后继续走原 messages
- ReactiveCompact 在 modelFn 内 retry 循环局部变量 `curMsgs` 上覆盖压缩——retry 成功后只 Write LLM 输出到 channel，**不写回压缩 messages**；下一轮 model 节点重新 Get 拿到的还是原 messages + 上轮输出（业务依赖此特性需注意）

## 编排 HITL 矩阵

9 编排（含 BasicAgent）都通过 `Agent.Run` 自动支持嵌入运行（框架按 graphNS push 注入父位置），HITL Resume 全闭环：

```go
ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
events, err := seq.Run(ctx, sess, input)
if ie, ok := compose_v2.IsInterrupt(err); ok {
    // 用户答完 ask_user
    events, err = seq.Resume(ctx, sess.RunID,
        agent_v2.WithResumeUserInputs(map[string]*schema.Message{
            ie.Query.InterruptID: schema.UserMessage("answer"),
        }))
}
```

> **Resume 无 cp fail-fast（R33 GA-M2）**：`Resume(ctx, sid)` 对不存在的 cp（sid 拼错 / cleanupOnSuccess 已清 / 从未跑过）统一返 `errors.Is(err, compose_v2.ErrNoPendingCheckpoint)`——业务据此区分"等用户答案"vs"会话已结束 / 上次是带着错退出的"（**两种情况返这个哨兵**：没有存档（sid 拼错 / 从未跑过 / 上次正常跑完已清）、存档里没有可续的内容（frontier 与挂起都空）。失败现场的档自 2026-08-13 起放行——`Resume` 不带答案 = 重试失败分支、成功兄弟不重放）；`ResumeActionAbort` 豁免（幂等清理恒返 nil，a2a Cancel 协调依赖）。

> **GraphAgent 提示**：用户自定义 NodeFunc 内部调子 agent.Run 时，框架不自动 push 父 ns——**走 `agent_v2.RunSubAgent(ctx, nodeID, sub, input)`**（一站式 push graphNS + child session + storeScope）。⚠️ **别手拼 childNS**（曾教过 `WithGraphNS(ctx, JoinNS(parentNS, "<nodeID>"))`，已撤）：一次嵌套调用的**身份** = **位置**（调用方所在的节点与相位——节点体 / 某个图钩子 / 某条条件边，见 `compose_v2.SubCallScope`——加实例维三选一：图级 fanout 实例 / 所在 tool_call（tcID + 批内下标 + tools 节点访问序号）——模型给的 ID 跨轮、批内都可能重复 / 静态重访 visit；fanout 实例节点里又起 tool_call 并发时两维一起进位置、不叠 visit）+ **这是哪一次调用**（不带 key 时是**输入内容 + 同一份输入在本次执行里的第几次**，带 `agent_v2.WithCallKey(k)` 时是 key；全历史输入（且没有自定义 schema）的 `AgentAsTool` 与 skill fork **只看位置**——输入由框架现拼、续跑时可能不同；`AgentAsTool` 另可用 `agent_v2.WithAgentToolCallKey(fn)` 从 args 取 key——中间件注入的每次都变的值落进了交给被调方的输入时（自定义 schema 的整个 args，或改写了 `input` 字段本身；默认 schema 下往别的字段注入不影响）必须给，不然每次续跑都是新调用、永不收敛（`OnToolStart` 的注入续跑不变：一批调用的内容在第一次执行时定下）；同一次调用的 key 必须每次都一样；声明了 `agent_v2.WithRerunnable()` 的再带上调用所在分域的重做代际）；存档槽 = `#身份哈希的低 63 位`（`JoinFanoutNS` 下标 int64，与平台字长无关）。`AgentAsTool` **不分是谁调起的**（tools 节点直接调、组合工具体里调、同名装饰器 / 中间件转发、钩子里调——调用栈上的名字可以被改写、转发、复用，框架分不可靠）：一律记完成记录（只记最终那条消息）——tools 节点的工具账只在整个 tool_call 完成时记，AgentAsTool 返回之后同一个 tool_call 还可能挂起，那时只有这份记录挡得住子 agent 重跑（实测：用户批准草稿 v1、执行的是 v2）；补不补记历史只看在不在 tool_call 里（见会话历史那一行）。中间件在 next 之后校验产出、不合格时返 error 拿不到新的一份（完成记录原样拿回），要换一份返 `tool.Failure`、让模型重发 tool_call。那段判定住在 `RunSubAgent` 里，手拼等于复制一份会漂的拷贝——漏实例维 ⟹ 子 cp 互覆（ISSUE-96 侧面 2），漏 visit 维 ⟹ 父循环第 2 轮的提问与第 1 轮撞同一个 InterruptID（答案账本跨子图边界后是响亮的 `ErrRunAborted`），漏调用方节点 / 相位 ⟹ 两个节点、或一个节点的两次执行在钩子里用同一份输入调同一个子 agent 撞成一次，漏输入内容 ⟹ 带着新输入的第二次调用捡起第一次的失败存档接着跑、新输入没进图。**已完成的嵌套调用被记住**：整组事件（AgentAsTool 只记最终那条消息）记在父节点这次执行的私有态（`compose_v2.SaveStepResult`；默认普通记录，`WithRerunnable()` 点名的按 `Rerunnable` 记），父节点重跑（中断续跑 / 失败续跑 / `RunWithRetry` 重试）时直接拿回、不再执行——副作用不重放、问过的不重问，它做出的转交与路由决定（发给上层 HandoffAgent / RoutingAgent 的上行信号）随记录一起记、拿回时交出（不在 events 里，漏了就静默丢转交）；钩子相位里的调用同样记住（节点级钩子与条件边记在那个节点这次执行名下，嵌套图的 `OnGraphStart` / `OnGraphEnd` 与入口条件边记在外层节点执行名下，只有顶层图的图级钩子与入口条件边没有可挂的地方；同一次外层执行里对同一个 ns 跑几遍嵌套图，按第几遍分开记——每一遍都照跑、别按进度跳过，跳过的话后一遍拿回前一遍的记录；嵌套图亲口声明 `WithFreshRun` 时图级相位什么也不续）。拿回的是存档往返后的形态，读标注按 JSON 类型读。输入不同的调用之间身份跟次序无关，按进度跳过已完成的调用、条件分支插进一次调用、累积传答案都不会让一个答案落到别的调用上，链式调用（起草 → 审批问人）一次收敛；⚠️ **输入完全相同的几次**只能按次序区分，别自己按进度跳过（全部照调一遍即可），要跳过就给 key；⚠️ **有转交 / 路由决定的调用别跳过**——那些决定只在「再调一次、被拿回」时交出，跳过就丢（`err == nil`）。**输入变了就是另一次调用**：新开、从头跑、自己问；⚠️ 输入里带着每次都变的值（请求号、时间戳、节点体里直接调模型的产出）时每次重跑都是新调用、永不收敛——用 `WithCallKey(item.ID)` 声明身份（输入变了照样续，打告警），或用 `SaveStepResult`（产出标 `Rerunnable`）把那个值记住。交给被调方的是输入的**独立拷贝**（结构逐层拷；`any` 值**按原类型递归深拷**——切片、数组、map 不管元素是什么一路拷下去，值结构体逐个拷导出字段（连同嵌入的结构体、含未导出的嵌入类型里的），标量原样，拷完类型不变、nil 与空不混；**指针、函数、channel 原样共享**，被调方钩子别经指针原地改调用方交来的东西，那样改会写回、续跑时换身份重问。曾只拷到「元素是标量」那一层：`[]map[string]any` 共享，钩子一改就写回调用方、续跑换身份重问、扣款重放，并发扇出时框架读摘要与兄弟钩子写同一个 map、进程 fatal，实测；更早按存档往返一遍，首跑就丢精度、丢类型）。输入摘要整体往返不回来时逐叶子走，坏叶子按「在哪儿 + 怎么坏的」单独计入，别的叶子照样区分（一个 NaN 曾让整张标注表塌成一个标记，工单-1 的批准兑付给工单-2）；结构体按字段展开、键不是字符串的 map 展开、复数记值，只有自定义编码出错的值整体算一个叶子；共享的子容器只走一遍（记引用），整个遍历线性（曾经深 14 的共享结构 20 秒算不完、十万层嵌套 10GB 内存，实测）。不带 key 的调用在被调方存档记输入摘要，续跑时对不上 = 撞槽 ⟹ 拒绝、不清档（`compose_v2.WithInputIdentity`）。同一次执行（节点执行 / 一个 tool_call / 一次图钩子或工具钩子调用 / 被调方在自己图外的那一段——agent 级 `OnAgentStart` 与 `OnAgentEnd` 同在这一段）里、同一个位置上重复用一个 key ⟹ `ErrRunAborted`。**重试同一次调用**用 `compose_v2.RunWithRetry` 包住 `RunSubAgent`；**「产出不合格要重来」**别用它、也别让节点返错再 Resume（两者都原样拿回已完成的调用）——在节点里循环调（每一轮是新调用），或把校验放到条件边上声明 `CondFailRerunFrom` 并给产出那次调用声明 `agent_v2.WithRerunnable()`（条件边否决时重做的是**整次调用**——身份里带重做代际，被调方在自己图之外记下的东西、它问的问题都换新；没点名的照常拿回；只有父节点直接发起的那一层点名才算数）。「这次 Run 是不是新任务」由编排 Run 入口在 `OnAgentStart` **之前**判（`enterRunTask`：ctx 上有没有一个正在执行的任务——节点执行范围记下的图 sid 优先，其次编排入口与嵌套调用执行器盖的任务章），钩子里发起的调用看得到新任务的宣告（不会拿上个任务的 approve_always）。**任务的身份就是 session 的 RunID**：ctx 上的 sid 只是它的副本（两把尺时长寿 ctx 上钉着的旧 sid 会让 `NextTask` 之后的新任务接着跑被放弃任务的存档，实测）。**在一个正在执行的任务里，session 的 RunID 必须属于它，对不上入口直接拒**（`%w ErrRunAborted`，什么都不执行）；Resume 在那样的位置上一律拒。**嵌套调用的被调方属于调用方的任务**：`RunSubAgent` / `AgentAsTool` / skill fork 给被调方的 session 的 RunID 就是所在任务（直接跑 `compose_v2` 图、ctx 里另推了一个 session 时取图的 sid），并替它盖章——调用方是业务自研 agent、框架外的循环、`compose_v2` 图的图级钩子都续得上。**开独立任务**（节点里给另一个会话开任务、续跑另一个挂起的任务）先 `agent_v2.DetachTask(ctx)`：框架不从「RunID 不同」去猜，当新任务读会让 `inner.Run(ctx, NewRunSession(), in)` 每次父节点重跑都新开一个、内层问两个问题就永远答不完（实测活锁）；当同一个任务读又会让拿别的任务的 ctx 给另一个会话开任务静默并进去。业务自己的循环里开新任务换 RunID（`NextTask`）；approve_always 规则按 RunID 分，换了 RunID 就是新的一套。⚠️ 节点里**直接** `compose_v2.Run` 一张会问人的嵌套图、跑完之后节点又失败：续跑时它从头再跑、在同一个位置问，累积传的旧答案会被兑付给新内容（实测，既有限制，见 todo）——记下它跑完了续跑时跳过，或经 `RunSubAgent` 调。

## Hook 选择速查（观测 BasicAgent ReAct 用）

BasicAgent 内部 preamble + model + tools + postamble 四节点 channel-based Graph（ISSUE-74/75 修后），hook 时机：

业务级 model/tool hook 按 model / tools 节点为锚点触发，`OnAgentStart` / `OnAgentEnd` 在 wrapper top-level（图执行外）触发——preamble / postamble 只触发引擎层 `OnNodeStart/End`，不触发任何业务级 hook。tools 节点内 N>=2 tool_call 时**业务 tool 函数本身 goroutine 并发执行**（ISSUE-81 agent_v2.makeToolsFn 内 WaitGroup fanout），但 **`OnToolStart/End` hook 调用本身保持串行**（agent_v2/basic_builder.go for 循环在 toolsFn 前/后各串行触发一次）—— 业务 hook 内共享状态写入**无需 thread-safe**；业务 tool 函数内共享状态**需 thread-safe**。

| Hook | 触发频次 | 位置 |
|---|---|---|
| `OnAgentStart` | Run 同步 1 次 + 每次 Resume 同步 1 次（**次数**跟 OnAgentEnd 对称——但 OnAgentStart 同步先于图执行、OnAgentEnd 是 defer；业务 OTel ctx-push pattern Run / Resume 两段各 work）| agent_v2.{编排}.Run/Resume top-level（图执行之前，preamble / modelNode 之前），跟 PlanAgent / GraphAgent / eino ADK 对齐 |
| `OnModelStart` / `OnModelEnd` | 每轮 round | 紧贴 LLM 调用（Stream + chunk 实时投递；模型不支持流式时改调 Generate）前后；OnModelEnd 拿到的是聚合后完整消息 |
| `OnToolStart` / `OnToolEnd` | per tool_call | tools 节点逐个工具触发 |
| `OnAgentEnd` | Run defer 1 次 + 每次 Resume defer 1 次（ISSUE-82 fix 后单一所有权）| Run/Resume 入口 defer，含 InterruptError / panic / ctx.Err 所有退出路径；HITL Resume 闭环算 2 次（Run 抛 Interrupt + Resume 完成）|

| 想看 | 用哪个 hook | 取什么字段 |
|---|---|---|
| 子 agent 收到的最初输入 | `OnAgentStart` | `in.Messages[-1]` |
| 每轮 LLM 调用前的消息流 | `OnModelStart` | `in.Messages` |
| 每轮 LLM 输出 | `OnModelEnd` | `output.Message`——`len(GetToolCalls)==0` 即最终答复 |
| 子 agent Run 完成后所有累积消息 | `OnAgentEnd` | `out.Messages`（本 Run 累积消息流的快照） |
