# 故障分析系统（Fault Analysis / RCA）设计方案

> 状态：**[设计]** 提案，未实现。本文是动手前的完整蓝图，定稿后按 §12 分阶段实现。
> **做不做、何时做以 [doc/code/todo.md](../code/todo.md) 中心表为准**（2026-09-07 归置，本篇正文不再单独记状态）。
>
> 定位：一个**生产对齐的根因分析（RCA）系统**，搭在 crew-go 之上。它的第二身份是
> **整个 SDK 的端到端活体集成测试**——但这是**自然副产物，不是设计目标**（见 §1 优先级）。
>
> 设计依据：2026-06 业内调研（§2）。所有编排接口签名已对照 `agent_v2` / `compose_v2` /
> `tool` / `observability` 真实代码（本文写作时逐一 grep 验证）。

---

## 1. 目标与定位

### 1.1 双目标与优先级（硬约束）

| 优先级 | 目标 | 含义 |
|---|---|---|
| **P0（最高）** | **故障分析做到最好** | 架构、编排选型、提示词、证据链一切以"RCA 质量"为准绳 |
| P1（副产物） | 自然覆盖 SDK 能力 | 只用**天然契合 RCA 的**编排/能力；覆盖是结果不是目的 |

**铁律**：**不能为了测试 SDK 而故意增加复杂度**。任何"为了练到某编排"而硬塞的结构一律砍掉。
判断标准永远是"这对故障分析是不是最优"，不是"这能不能多覆盖一个 API"。

### 1.2 非目标

- **不做树搜索 / MCTS / LATS**（§14 附否决证据）——公开材料未见生产采用；成本显著更高（LATS-RCA 实测约 3.8× token / 3.9× API 调用，arXiv:2605.03505 Table 1）；DeepSeek-R1 在其训练中将 MCTS 列为失败尝试。
- **不默认全量 fan-out** 查所有数据源——按假设**选择性**取证（§5.2）。
- **不做固定规则提纯**——源头提纯交给**目标感知的 LLM**（§5.2）。
- 不追 benchmark 跑分形态，追**生产正确形态**。

### 1.3 一句话架构

> **一个假设驱动的编排器（PlanAgent + LLMPlanner + RCA 规划 prompt / 或自定义 Planner），按"形成假设 → 选数据源
> 取证 → 验证/剪枝 → 收敛根因"的回路推进；每个数据源是一个会按当前假设提纯的 distiller
> 子 agent（BasicAgent + mock 工具）；证据链显式累积；根因由单个推理体收口产出；修复动作经
> HITL 人审放行。**

---

## 2. 设计依据（业内对照，2026-06 调研结论）

完整两路调研（开源生态 + 生产采用）结论一致，三条承重结论：

**结论 A：赢的不是"多 agent"，是"结构化假设-证据-验证回路"。**
同一篇 LATS-RCA 论文 apples-to-apples：单 ReAct 39.8% → 多 agent ReAct 57.4% → 树搜索 91.3%。
但 91.3% 是 **benchmark SOTA，不是 production SOTA**（见结论 C）。真正稳定的增益来自**控制流
是不是结构化的假设回路**，不是 agent 数量。Tran & Kiela（arXiv 2604.02460, 2026-04）：同等
token 预算下单 agent 持平/胜过多 agent，多 agent 增益多是"算力/上下文 artifact"。

**结论 B：生产正解 = hypothesis-loop + 选择性派发 + 写收口单 agent + 拓扑感知。**
所有在跑的 AI-SRE 产品（Datadog Bits AI、incident.io、Azure SRE Agent、AWS DevOps Agent、
PagerDuty）都是**假设回路 / 并行多 agent 取证**，**没有一个点名用 value-model MCTS**。它们用的是**带证据剪枝的假设分支探索**（Datadog 自述
"branches explored or pruned based on evidence"），而非 value-model 引导的搜索算法。三方铁律一致：**读可并行，写/synthesis 必须收口到单
agent**（Anthropic + LangChain + Cognition）。

**结论 C：公开材料中未见生产采用 value-model 树搜索。**
DeepSeek-R1 论文 §4.2 把 MCTS 列为 "Unsuccessful Attempts"（token 搜索空间指数爆炸、value
model 训不动）——这是**一家实验室在 R1 训练中的取舍**，不等于全行业结论；incident.io 等在跑产品
据二手整理（ZenML LLMOps DB）用假设驱动回路而非树搜索算法；LATS-RCA 是 arXiv 预印本（探索性
case study 非生产部署）。**benchmark 刷分技术 ≠ 该上生产的技术；且"未见公开案例"≠"证明无人用"——
不排除内部未公开使用。**

**对本系统的映射**：按结论 B 搭——这恰好就是 crew-go 现有编排能直接表达的形态，**我们不落后、
是对齐生产现实**。

> 关键引用：[LATS-RCA arXiv:2605.03505](https://arxiv.org/abs/2605.03505) ·
> [DeepSeek-R1 §4.2 arXiv:2501.12948](https://arxiv.org/html/2501.12948v1) ·
> [Datadog Bits AI SRE](https://www.datadoghq.com/blog/building-bits-ai-sre/) ·
> [incident.io（ZenML LLMOps DB）](https://www.zenml.io/llmops-database/ai-powered-incident-response-system-with-multi-agent-investigation) ·
> [Azure SRE Agent RCA](https://learn.microsoft.com/en-us/azure/sre-agent/root-cause-analysis) ·
> [Anthropic Building Effective Agents](https://www.anthropic.com/research/building-effective-agents) ·
> [Tran & Kiela arXiv:2604.02460](https://arxiv.org/abs/2604.02460) · [树搜索算法/奖励设计 survey arXiv:2510.09988](https://arxiv.org/abs/2510.09988)

---

## 3. 架构总览

```
                         用户/告警: "支付服务 P99 延迟从 80ms 飙到 2s，从 14:30 开始"
                                              │
                                              ▼
                    ┌─────────────────────────────────────────────┐
                    │  Orchestrator（PlanAgent + Planner）          │
                    │  维护假设回路：形成假设→选数据源取证→验证→剪枝  │
                    │  finalAnswer = 收口的 RCA 报告（单推理体产出）  │
                    └───────┬───────────────────────────┬─────────┘
                            │ 选择性派发（按当前假设）       │ HITL：修复动作前人审
              ┌─────────────┼─────────────┬──────────────┐ │
              ▼             ▼             ▼              ▼ ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐  ┌──────────┐
        │log_       │ │metric_   │ │trace_    │ │changes_  │  │verifier  │
        │distiller  │ │distiller │ │distiller │ │distiller │  │(P1 证据  │
        │(BasicAgent│ │          │ │          │ │          │  │ 链审计)  │
        │ +query_   │ │+query_   │ │+query_   │ │+recent_  │  │          │
        │ logs mock)│ │ metrics) │ │ traces)  │ │ changes) │  │          │
        └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘  └────┬─────┘
             │ 读原始(大)   │            │            │             │
             │ 只返结构化   ▼            ▼            ▼             ▼
             │ 证据块   ┌────────────────────────────────────────────┐
             └────────▶│  共享证据黑板（store.Store）+ 服务依赖拓扑     │
                       │  raw 可 offload 存 key，fetch_detail 按需取    │
                       └────────────────────────────────────────────┘
             横切：observability（trace + metric 覆盖整棵编排树）
```

> 图注：图为 P0+P1 全貌。**P0 只含 log/metric/trace/changes 4 个 distiller**；topology distiller +
> verifier 是 **P1**（图中"服务依赖拓扑"指 topology distiller 经 `query_topology` 取证产出的**证据**，
> 非黑板预置数据）。Orchestrator 实为 `PlanAgent + Planner + RCA 规划 prompt`（§4.4）。

**核心数据流不变量**：原始日志/trace（可能极大）只活在 **distiller 子 agent 的隔离上下文**里；
跨回编排器的**只有提纯后的结构化证据块**。这是上下文不爆炸的根本机制（§7）。

---

## 4. 控制流：hypothesis → evidence → test

### 4.1 为什么是 PlanAgent 而非单 BasicAgent

故障分析的本质是**带证据链的迭代推理**：每一步都基于"已有证据"决定"下一步查什么"，且要能
**显式记录"假设 → 取了什么证 → 验证结果"**。这正是 `PlanAgent` 的 replan-每步循环：

| RCA 概念 | PlanAgent 机制 |
|---|---|
| 形成/修正假设 | Planner 每步基于 `executed []ExecutedStep` 重新规划 `remaining []PlanStep` |
| 选数据源取证 | `PlanStep.SubAgentName` 指向某 distiller 子 agent |
| 验证/剪枝 | 下一次 Planner 调用看到 `ExecutedStep.Result`（证据），决定继续/换向/收敛 |
| 证据链 | `[]ExecutedStep`（Step/Agent/Result 三字段）是"步骤→agent→证据"有序轨迹；"假设"维度嵌在 Step 描述 + Result 证据块的 `hypothesis_ref`（非结构字段，追溯需解析 Result JSON）|
| 收敛根因 | Planner 返 `finalAnswer`（空 `remaining`）= 单推理体收口的 RCA |

单 `BasicAgent`（ReAct）也能跑，但假设轨迹隐式埋在消息流里、不结构化，证据链可追溯性差——
**故障分析要的就是显式证据链，所以 PlanAgent 更合适**（这点在前期讨论已确认）。

### 4.2 一次完整回路（时序）

> 下方证据块为**示意简写**（突出关键发现）；真实 distiller 输出严格遵循 §5.2 JSON schema。step
> 描述前缀 `[H1]` 即 §4.4 要求的假设 id 透传（distiller 据此回填 `hypothesis_ref`）。

```
Run(orchestrator, sess, "支付服务 P99 14:30 起飙到 2s")
  └─ Planner 调用#1（executed=∅）：
       形成假设 H1="变更引入" → 规划 step1{changes_distiller, "[H1] 查 14:30 前后是否有支付服务变更"}
  └─ exec step1 → changes_distiller.Run → 证据块: {found_deploy: true, time:14:28, commit:abc}
  └─ Planner 调用#2（executed=[step1]）：
       假设"变更引入" → step2{trace_distiller, "查 14:30 后支付链路错误 span，定位到 commit abc 的代码路径"}
  └─ exec step2 → trace_distiller.Run → 证据块: {error_span: "PaymentGW.charge", err:"conn timeout to db"}
  └─ Planner 调用#3：
       假设细化"abc 引入 DB 连接问题" → step3{metric_distiller, "查支付服务 DB 连接池指标 14:25-14:40"}
  └─ exec step3 → metric_distiller.Run → 证据块: {db_pool_used: 100%, since:14:29}
  └─ Planner 调用#4：
       证据闭环 → step4{verifier, "审证据链是否自洽、有无遗漏"}   ← P1 引入
  └─ exec step4 → verifier.Run → {chain_sound: true, gaps: []}
  └─ Planner 调用#5：
       验证通过 → 返 finalAnswer = RCA 报告（根因=commit abc 把连接池 size 从 50 改成 5）
```

### 4.3 停止条件（缺一不可，防失控/防早收敛）

1. **收敛或优雅放弃**：Planner 判定证据链自洽 + verifier 通过（`chain_sound:true`）→ 返 `finalAnswer`（根因）；或所有
   假设被反驳、无新假设可形成 → respond 输出"未能确定根因 + 已排除清单"（见 §4.4 prompt 第 7 条）。
   两者都是正常 respond 出口（区别于下面预算硬兜底的 error 路径）。
2. **预算（两层，别混）**：**软预算**——RCA 规划 prompt 要求 Planner 步数逼近预算时主动
   `respond` 收口产出"最可能根因 + 未决项"（**正常出口**，见 §4.4 prompt 第 6 条）。**硬兜底**——
   `WithPlanMaxSteps(n)`（如 12）触顶时框架抛 `MaxIterationsError`（`graph_loop.go`，**不产
   finalAnswer**），是**异常路径**：业务需 `errors.As(err, &MaxIterationsError{})` 捕获后从
   `sess.Events()` 已写入的证据自行降级出报告。**优雅收口靠软预算让 Planner 自己 respond，
   不要依赖硬兜底**（硬兜底只给 error）。
3. **超时**：编排器 Run 必须设 `ctx` deadline（CLAUDE.md 交付纪律——超步 best-effort 需外部兜底）。
4. **HITL**：需要人类才能提供的信息（如"这次变更是否预期内"）→ `ask_user` 中断等答案（§8）。

### 4.4 Planner 两档实现（P0 内置，P1+ 自定义）

- **P0：内置 `NewLLMPlanner` + 注入 RCA 规划 prompt**。框架保证**单步推进**（每轮只执行
  `remaining[0]` → 跑完回 plan 节点重规划 → 每次重规划都看到全部已执行步骤的真实证据；
  `agent_v2/plan_agent.go` 取 remaining[0]+回边 + `llm_planner.go` 注入 executed）——假设回路的
  **控制流闭合性由框架兜底，与 LLMPlanner 一次返回几个 step 无关**（多余 step 被丢弃重规划）。
  **但**内置 LLMPlanner 默认 prompt 是**通用"计划拆解器"，零 RCA/假设/取证/剪枝语义**——**必须**
  用 `WithLLMPlannerSystemPrompt` 注入假设驱动的 RCA 规划 prompt（否则 §4.2 那种"假设→证伪→
  细化"推理链不会自发产生）。骨架：

  ```
  你是根因分析规划器。基于【目标】和【已取证据】推进调查：
  1. 形成 1-3 个候选根因假设，**每个分配稳定短 id（H1/H2/H3）**（如 H1="近期变更引入"/H2="上游依赖故障"）。
  2. 每步选**最能证实或证伪当前最高优先级假设**的 distiller（plan 工具填 sub_agent），并**在 step
     description 开头写明本步验证哪个假设 id**（如"[H1] 查 14:30 前后是否有支付服务变更"）——distiller
     据此在证据块 `hypothesis_ref` 回填 id。不要一次把所有数据源都查——选择性取证。
  3. 收到证据后：支持→细化假设；反对→剪掉该假设转下一个；不足→补取。
  4. 已查过的源不要重复（看"已完成步骤"列表）。
  5. 证据链闭合、根因被证据支撑 → respond 工具产出 RCA（根因 + 支撑证据引用 + 置信度）。
  6. 步数逼近预算仍未收敛 → 基于现有证据 respond 出"最可能根因 + 未决项"（软收口，见 §4.3）。
  7. 若所有候选假设都被证据反驳、且无新假设可形成 → respond 输出"未能确定根因 + 已排除假设清单 +
     建议人工排查方向"，**严禁为收口编造无证据支撑的根因**（RCA 系统最危险的失败模式）。
  ```
- **P1+：自定义 `Planner` 函数**。`Planner` 签名给全控制权——可在 `sess` KV 里维护**显式**假设
  列表（`SetValue("hypotheses", ...)`）、`json.Unmarshal` 校验证据块、实现"验证后才收敛"+"已用
  distiller 去重"的硬逻辑。生产级假设回路用这档。
  > **P0→P1 是质变不是平滑加配置**：内置 LLMPlanner **不读 `sess`**（`llm_planner.go` 里
  > `_ = sess`），P0 的"假设"只活在 LLM 每轮重读 executed 的**隐式推理**里；P1 自定义 Planner
  > 才有**代码显式假设状态机** + 结构化证据校验。两档共享 `PlanAgent` 执行机制，但假设管理质不同。

---

## 5. 组件设计

### 5.1 编排器 Orchestrator（PlanAgent + Planner）

```go
// sub-agents 同一个 map 同时喂给 planner（用于"知道有哪些能力"）和 PlanAgent（用于执行解析）；
// 传同一 map 后不要再 mutate——两边 key set 必须始终一致
subAgents := map[string]agent_v2.Agent{
    "changes_distiller": changesDistiller,
    "log_distiller":     logDistiller,
    "metric_distiller":  metricDistiller,
    "trace_distiller":   traceDistiller,
    // 以下 P0 之后引入（P0 = 上面 4 个 distiller）：
    // "topology":      topologyDistiller, // P1（拓扑感知，配 query_topology + S3 数据集）
    // "verifier":      verifier,          // P1（证据链审计）
    // "parallel_probe": parallelProbe,    // 可选：相关性取证（§5.5）
}

planner := agent_v2.NewLLMPlanner(m,
    agent_v2.WithLLMPlannerSubAgents(subAgents),
    // 关键：内置默认 prompt 是通用"计划拆解器"，无 RCA/假设语义。必须注入假设驱动的
    // RCA 规划 prompt（骨架见 §4.4），否则 LLM 只会当普通"分几步查日志"任务
    agent_v2.WithLLMPlannerSystemPrompt(rcaPlanningPrompt),
)

orch := agent_v2.NewPlanAgent(planner,
    agent_v2.WithSubAgents(subAgents),   // 注意：必须再注册一次给执行器
    agent_v2.WithPlanMaxSteps(12),
)

// 共享证据黑板：PlanAgent 无 WithStore option（那是 BasicAgent 专属）。让所有 distiller
// 共享 store 的机制是在编排器 Run 入口把 store 注入 ctx——store 经 worker ctx 自动透传到每个
// distiller 子 agent，其工具用 store.FromContext(ctx) 读写。完整 setup 见 §7 / §9：
//   ctx = store.WithContext(ctx, sharedStore)
```

**收口**：`PlanAgent` 的 `finalAnswer` 由单个 Planner LLM 调用产出 = 天然满足"synthesis 收口单
agent"。如需更规整的报告格式，可选在末尾加一个 `report_writer` 步骤（见 §5.4），但非必需。

### 5.2 数据源 distiller（BasicAgent + mock 工具 + 目标感知提纯）

**每个 distiller 是一个 BasicAgent**，配自己的 mock 工具 + 一段"按当前假设提纯"的 system prompt。
关键：distiller 读原始数据后**只输出结构化证据块**，原始数据不外泄（§3 不变量）。

```go
logToolkit := tool.NewToolkit()
logToolkit.Register(queryLogsTool)          // P0：查询工具（mock 见 §10）
// logToolkit.Register(offloadDetailTool)   // P1：offload 写工具（生成 raw_ref key）
// logToolkit.Register(fetchDetailTool)     // P1：offload 读工具（与写成对，见 §10.1）

logDistiller := agent_v2.NewBasicAgent(
    agent_v2.WithName("log_distiller"),
    agent_v2.WithDescription("按当前假设查询并提纯日志：返回结构化证据块（命中模式/异常摘要/原始片段 key），不返原始日志"),
    agent_v2.WithModel(m),
    agent_v2.WithToolkit(logToolkit),
    agent_v2.WithSystemPrompt(logDistillerPrompt), // 见下
    agent_v2.WithMaxRounds(4),
)
```

**目标感知提纯（而非固定规则）**——这是 distiller 的灵魂，写在 system prompt 里：

```
你是日志取证专家。编排器会在【取证目标】里带一个假设 id（如 `[H1]`）。
1. 用 query_logs 拉原始日志（可能上万行）。
2. 只围绕该假设判断哪些日志是支持/反对证据，忽略无关噪声。
3. 输出 JSON 证据块（不要回贴原始日志）：把 `[H1]` 原样填进 `hypothesis_ref`、为本块生成唯一
   `evidence_id`（如 logs-1）；需保留全量供后续回取时（P1）先调 `offload_detail(content=...)`
   拿 key 填进 `raw_ref`，否则留空。
   {
     "evidence_id": "<本块唯一 id，如 logs-1，供 verdict 的 supporting_evidence 引用>",
     "source": "logs",
     "hypothesis_ref": "<§4.4 step description 里带的假设 id，如 H1>",
     "finding": "<一句话结论>",
     "supports": true|false,        // 支持还是反驳该假设
     "confidence": 0.0-1.0,
     "key_evidence": ["<最多 3 条关键日志行摘要>"],
     "raw_ref": "<P1：offload_detail 返回的 key（见 §10.1）；P0 留空>"
   }
```

> **为什么是 LLM 提纯不是规则**：故障形态无穷，"哪些日志相关"取决于**当前假设**——固定规则
> （如"只抓 ERROR 级"）会漏掉"假设 X 下 INFO 行才是关键证据"的情况。LLM 按目标动态判断更准
> （前期讨论已确认）。

**选择性派发（不全查）**：编排器**只**对当前假设相关的 distiller 规划步骤。"延迟飙升"先查
changes + trace，确认 DB 嫌疑后才查 metric；不会一上来把 4 个数据源全跑一遍。这是结论 B 的
"选择性派发"，也直接回应"不是所有故障都要全查、包一起浪费"。

> **distiller 失败处理**（框架行为 + 业务责任，别漏）：单个 distiller `Run` 返 error 时，
> **PlanAgent 默认不重试、整条 RCA 立即中止**（`NewPlanAgent` 不设 retryCfg → 零值单次执行 →
> worker err propagate 到 `PlanAgent.Run`）。两层应对：(1) 瞬时故障——给编排器加
> `WithPlanRetryConfig(compose_v2.ExponentialBackoff(...))` 或 `WithPlanMaxRetries(n)` 兜底；
> (2) "某源彻底挂了仍想基于其他证据收敛"——**框架无内置"跳过失败源继续"能力**，需 **P1 自定义
> Planner**：在 worker 内 catch err 转成"该源不可用"证据块（而非 propagate），让 Planner 当软信号
> 继续推进。P0 用内置档时，distiller 失败 = 整 Run 失败。

### 5.3 Judge / Verifier（证据链审计，P1）

**主方案：in-loop verifier 子 agent**（便宜、对齐生产）。编排器在收敛前规划一个 verify 步骤，
派给一个独立的 `verifier` BasicAgent，对**证据链本身**做对抗性审计：

```go
verifier := agent_v2.NewBasicAgent(
    agent_v2.WithName("verifier"),
    agent_v2.WithDescription("审计证据链：根因是否被证据真正支撑？有无逻辑断点/时序矛盾/症状放大偏差？"),
    agent_v2.WithModel(m),
    agent_v2.WithSystemPrompt(verifierPrompt), // 检查：时序一致性 / 因果方向 / 是否错怪下游受害者 / 证据是否足够
    agent_v2.WithMaxRounds(2),
)
```

verifier 输出 schema（verifierPrompt 要求按此结构产出，§4.2 时序的 `chain_sound`/`gaps` 即此）：

```
{ "chain_sound": true|false, "gaps": ["<缺失/断裂证据描述>"] }   // 通过 = chain_sound:true
```

verifier 返"证据不足/有断点"（`chain_sound=false`）时，其结论作为 `ExecutedStep.Result` 喂回
Planner，Planner 据此**只补缺失的那部分证据**（不重跑全程）——比 ReflectAgent 外层包裹便宜得多。

> 注意："只补不重跑"是 **Planner 的决策**（默认 prompt 含"不重复已完成步骤"软约束 + RCA prompt
> 引导），**框架不强制**。要硬保证"已查源不再派"，需 P1 自定义 Planner 在 `executed` 里对已用
> distiller 去重。

> **为什么不默认用 ReflectAgent 包裹编排器**：`ReflectAgent`（actor→evaluate→反馈→actor）在
> 反馈轮会**重跑整个 actor**。若 actor=PlanAgent，等于每轮反馈重跑整条调查链（重新调 mock/真实
> 数据源），成本高且可能触发限流。**in-loop verifier 在一次 Run 内完成验证-补证，更优**。
> ReflectAgent 外层包裹仅作为"要一个完全独立的最终质量门"时的**可选增强**（P2，需接受重跑成本 +
> `maxRounds` 上限）。

### 5.4 Synthesis 收口（默认即 Planner，可选 report_writer）

默认：Planner 的 `finalAnswer` 就是收口的 RCA。若要标准化报告格式（根因/证据链/影响面/建议修复/
置信度），可选用 `SequentialAgent` 串一个末尾格式化步骤——但这是锦上添花，**不是架构必需**：

```go
// 可选：仅当需要严格报告模板时
report := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("rca_pipeline"))
report.AddStep("investigate", orch).AddStep("format", reportWriter)
```

### 5.5 可选：parallel_probe（相关性并行取证）

当编排器有一个**需要多源同窗相关性**的假设（如"14:30 同一时间窗 log+metric+trace 是否同时异常"），
**读可以并行**。此时（且仅此时）派一个 `ParallelAgent` 包住 log+metric+trace distiller 并发取证，
Planner 再做相关性收口：

```go
parallelProbe := agent_v2.NewParallelAgent(agent_v2.WithParallelName("parallel_probe"))
parallelProbe.AddBranch("log", logDistiller).
    AddBranch("metric", metricDistiller).
    AddBranch("trace", traceDistiller)
```

> 注意边界：这**不是默认全量 fan-out**（那会浪费，已否决 §14）。`parallel_probe` 只在 Planner
> 显式判断"本假设需要跨源同窗相关性"时才派——默认仍是单 distiller 选择性派发。符合"读可并行、
> 但按需"。

---

## 6. 证据链与结构化输出

### 6.1 证据块 schema（P0 用现有 API，prompt 约束 + 轻校验）

P0 阶段 distiller 通过 **system prompt 约束**输出 §5.2 的 JSON 证据块。**注意**：内置 LLMPlanner
**不**对证据块做 `json.Unmarshal`/校验——它只解析自己的 plan/respond tool_call，distiller 的
`Result` 原样进 `executed`。所以"证据块校验 + 解析失败重试"需 **P1 自定义 Planner**（在 Planner
函数内 unmarshal `executed[i].Result` 判合格、不合格则规划同 distiller 重试 step）。**P0 用内置档
时**，证据块合规只能靠 distiller prompt 自觉 + verdict 阶段 LLM 容错。

> 最低防线：畸形/非 JSON 的 `Result` 会原样作为文本证据喂回 Planner（**不会 panic 或中断回路**），
> 由 Planner LLM 容错降权——但不保证识别"这条证据无效"（硬拒需 P1 校验，正文已述）。

### 6.2 与 structured-output 的关系（未来硬化）

`doc/v2/structured-output.md` **已落地**（`agent_v2.WithOutputSchema`，见其「实现状态」节），证据块和
最终 verdict 可直接改用 `WithOutputSchema` + 强制工具调用获得**~100% schema 合规 + 校验回炉**，消除
prompt-约束-JSON 偶发不合规。**本系统不阻塞于它**：P0 也可先用 prompt+解析，再无缝替换为强约束。

### 6.3 可追溯性（证据链不断点）

- 每个证据块带 `hypothesis_ref`（属于哪个假设）+ `source` + `confidence` + `supports`。
- `[]ExecutedStep`（Step/Agent/Result 三字段）是"步骤→agent→证据"有序轨迹；假设语义嵌在 Step
  描述文本 + Result 证据块的 `hypothesis_ref`（非 ExecutedStep 结构字段）。
- 最终 RCA 报告**强制引用**支撑它的证据块（prompt 要求 verdict 列 `supporting_evidence` 引用），
  杜绝"结论凭空蹦出来"。
- **多根因**：`finalAnswer` 是自由文本，可在一次收口里列多个 contributing factors（各带独立
  `supporting_evidence`），不强制收敛到单一根因——§10.2 S4 测的是"红鲱鱼单因果"，真·多独立根因
  靠 prompt 引导 Planner 并列输出，框架不阻塞。

**verdict 统一字段**（即 `finalAnswer` = RCA 报告 = verdict，**三者同一产物的不同叫法**；P0 prompt
约束、P3 structured-output 硬化）——§4.4 prompt 第 5/6/7 条、§5.4、§4.3 各处措辞都对齐这张表：

```
{
  "root_causes": [ {"description": "...", "supporting_evidence": ["<evidence_id>", ...], "confidence": 0.0-1.0} ],
  "ruled_out":   ["<已排除假设 id + 一句话依据>"],   // §4.4 prompt 第 7 条
  "undetermined": false,                            // true = 未能确定根因
  "recommended_actions": ["<建议修复，HITL 人审后执行，见 §8>"]
}
```

---

## 7. 上下文管理（核心：不让 log/trace 撑爆编排器）

| 机制 | 作用 | 落点 |
|---|---|---|
| **源头提纯（主）** | 原始数据只在 distiller 子上下文消费，跨回编排器的只有结构化证据块 | §5.2，distiller system prompt |
| **offload + fetch_detail（按需）** | distiller 把全量原始片段存进 `store.Store`（key 化），证据块只带 key；编排器/其他 distiller 需要细节时用 `fetch_detail` 工具按 key 取 | §10 工具 + `store.WithContext(ctx, st)`（Run 入口注入，透传子 agent） |
| **嵌套 cp 物理分离** | 每个 distiller 子 agent 的 cp 写到独立 ns，互不干扰；编排器 cp 只存假设/证据轨迹 | 框架自动（graphNS push） |
| **4 层压缩（distiller 层兜底）** | 压缩**只作用于 distiller（BasicAgent）**自身 ReAct 历史——读大日志时防 distiller 上下文爆炸。**编排器（PlanAgent）无压缩 option**，其轨迹靠源头提纯保持精炼 + `WithPlanMaxSteps` 限步 | `WithMicroCompact`/`WithAutoCompact`（**配在 distiller 上，非编排器**）|

> 关键认知：**上下文不爆炸靠"源头提纯"，不靠"事后压缩"。** distiller 把"上万行日志"变成"3 行
> 关键证据 + 1 个 key"，编排器永远只看精炼证据。压缩是兜底，不是主力。

> store 透传：`store.WithContext(ctx, st)` 在编排器 Run 入口注入后，由 Go `context.WithValue`
> 叠加语义保证透传到 distiller 工具——PlanAgent worker 只在 ctx 上**叠加** graphNS /
> SubAgentStoreScope（不重建、无 `context.Background()` 截断，已代码核实）。**P1 offload 落地时仍
> 建议加集成测试断言 distiller 工具内 `store.FromContext(ctx) != nil`**——作回归防护（机制本身成立）。

---

## 8. HITL（修复动作人审 + 信息补全）

故障分析系统**绝不能自动改生产**。两类中断点：

1. **信息补全中断**：编排器/distiller 需要只有人能给的信息（"这次变更是否预期内？"）→ 内置
   `ask_user` 工具（`WithAskUser()`）→ 抛 `InterruptError` → 人答 → `Resume` 续跑。
2. **修复动作 gate（关键）**：RCA 出根因后，**任何**"建议执行回滚/重启/扩容"动作，必须先经
   `ask_user` 拿到人类明确批准才能（在真实接入时）执行。P0/P1 修复动作只**建议不执行**，但
   gate 结构先搭好。

```go
ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
events, err := orch.Run(ctx, sess, input)
if ie, ok := agent_v2.IsInterrupt(err); ok {
    // ⚠️ 走 AllQueries()：同一轮多个 tool_call 都要人答时框架合成一条中断
    //（Queries 容器），那时 ie.Query 为 nil，写 ie.Query.X 会 panic。
    answers := make(map[string]*schema.Message)
    for _, q := range ie.AllQueries() {
        // 展示 q.Question 给运维，拿到批准/补充信息
        answers[q.InterruptID] = schema.UserMessage("已确认，批准回滚")
    }
    events, err = orch.Resume(ctx, sess.RunID,
        agent_v2.WithResumeUserInputs(answers))
}
```

> 嵌套场景（distiller 内部 `ask_user` 中断）由框架按 graphNS 自动 cp round-trip + Resume 闭环
> 支持（CLAUDE.md 编排 HITL 矩阵）。**重跑边界**：Resume 时 toolsNode 整节点重跑，但同批**已成功的
> tool_call 由节点 memo 重放、不重执行**（hitl-resume.md §6.2）；`ask_user` 自身二阶段幂等。仍会重复调用的
> 只有中断那一刻没跑完的工具，以及进程在 tools 节点执行中途崩溃的情形（memo 只随节点「没成」的出口
> 存档落盘，sync / async durability 在超步边界写档、带不上它，配了也一样）——§12 P2/P3 接真实数据源 /
> 修复动作时按 hitl-resume.md §6.3 那张表处理。

---

## 9. 可观测性（整棵编排树 trace + metric）

接 observability 后，**整棵树**（PlanAgent → 每个 distiller → 每次 model/tool 调用 → 每次中断/
压缩）都 emit trace span + metric。**这条 trace 本身就是 SDK 的集成测试产物**——能在一条 trace
里看到完整调查链，就证明嵌套编排/cp/HITL/hook 全链路通。

> 下方是在 §5.1 的 `orch` 构造上**追加** `WithPlanHooks` 接观测——**同一个 PlanAgent，非新建**。

```go
obs, err := observability.New(cfg)   // 注意：返 (*Observability, error)
if err != nil { /* 处理 */ }

ctx = compose_v2.WithSessionIDCtx(ctx, sess.RunID)
ctx = agent_v2.WithCtxOrchHooks(ctx, obs.OrchHooks())   // 编排级 5 hook（漏接则 Node/Fanout 等 metric 全漏）
ctx = store.WithContext(ctx, sharedStore)               // 证据黑板：透传到所有 distiller（§7）
defer obs.CleanupSession(sess.RunID)                    // 防 sessions map leak

orch := agent_v2.NewPlanAgent(planner,
    agent_v2.WithSubAgents(subAgents),
    agent_v2.WithPlanMaxSteps(12),
    agent_v2.WithPlanHooks(obs.AgentHooks()),  // 编排器自身的业务级 hook（注意是 WithPlanHooks 不是 WithHooks）
)
// 每个 distiller BasicAgent 各自 agent_v2.WithHooks(obs.AgentHooks()) 让其也被观测
```

> 必同时接 `AgentHooks()`（业务级——编排器用 `WithPlanHooks(obs.AgentHooks())`、distiller
> BasicAgent 用 `WithHooks(obs.AgentHooks())`）+ `OrchHooks()`（编排级，ctx 透传）——CLAUDE.md
> 可观测性规约：漏接 OrchHooks 则 Compact/Cp/Fanout/Node/Skill 8 个 metric 全漏。

---

## 10. mock 数据与故障场景

### 10.1 mock 工具（FuncTool）

每个数据源一个 `tool.NewFuncTool`，返回"故意做大"的原始数据（验证提纯真的发生）：

```go
queryLogsTool := tool.NewFuncTool(
    "query_logs",
    "按 service + 时间窗 + 关键词查询原始日志行（可能上万行）",
    map[string]any{
        "type": "object",
        "properties": map[string]any{
            "service": map[string]any{"type": "string"},
            "since":   map[string]any{"type": "string", "description": "RFC3339"},
            "keyword": map[string]any{"type": "string"},
        },
        "required": []string{"service"},
    },
    func(ctx context.Context, args map[string]any) (*tool.Result, error) {
        raw := mockLogStore.Query(args) // 从内置故障场景数据集捞
        return &tool.Result{Success: true, Output: raw}, nil
    },
)

// 证据黑板 namespace（业务自定义；offload 写 + fetch 读用同一个）
var evidenceNS = store.NS("rca", "evidence")

// offload 写 + 读两个工具属 **P1**（offload 整套 P1，见 §12）；P0 不注册。
// distiller 把"故意做大"的原始片段先调 offload_detail 拿 key，再把 key 填进证据块 raw_ref。
offloadDetailTool := tool.NewFuncTool(
    "offload_detail",
    "把全量原始片段存进证据黑板，返回 key（填进证据块 raw_ref）",
    map[string]any{ /* content: string */ },
    func(ctx context.Context, args map[string]any) (*tool.Result, error) {
        st := store.FromContext(ctx)
        if st == nil {
            return &tool.Result{Success: false, Error: &tool.Error{Code: "no_store", Message: "store not in ctx"}}, nil
        }
        content, ok := args["content"].(string)    // comma-ok 防 panic（库惯例）
        if !ok {
            return &tool.Result{Success: false, Error: &tool.Error{Code: tool.ErrCodeInvalidArguments, Message: "content required"}}, nil
        }
        key := genEvidenceKey()                     // 业务生成唯一 key
        if err := st.Put(ctx, evidenceNS, key, content); err != nil {
            return &tool.Result{Success: false, Error: &tool.Error{Code: "put_failed", Message: err.Error()}}, nil
        }
        return &tool.Result{Success: true, Output: key}, nil
    },
)

fetchDetailTool := tool.NewFuncTool(
    "fetch_detail",
    "按 offload key 取回此前 distiller 暂存的全量原始片段",
    map[string]any{ /* key: string */ },
    func(ctx context.Context, args map[string]any) (*tool.Result, error) {
        st := store.FromContext(ctx)                // 单返回值；store.WithContext 未注入返 nil
        if st == nil {
            return &tool.Result{Success: false, Error: &tool.Error{Code: "no_store", Message: "store not in ctx"}}, nil
        }
        key, ok := args["key"].(string)             // comma-ok 防 panic（库惯例，非裸 assert）
        if !ok {
            return &tool.Result{Success: false, Error: &tool.Error{Code: tool.ErrCodeInvalidArguments, Message: "key required"}}, nil
        }
        item, err := st.Get(ctx, evidenceNS, key)
        if err != nil || item == nil {              // Get 对不存在的 key 返 (nil, nil)，必须显式判 item==nil
            return &tool.Result{Success: false, Error: &tool.Error{Code: "not_found", Message: "evidence key not found"}}, nil
        }
        return &tool.Result{Success: true, Output: item.Value}, nil
    },
)
```

工具集：`query_logs` / `query_metrics` / `query_traces` / `recent_changes`（P0）；`query_topology`
（P1 拓扑）；`offload_detail` + `fetch_detail`（P1，offload 写 + 读，契约成对）。

### 10.2 故障场景数据集（分阶段加，覆盖不同推理路径）

| # | 场景 | 考验什么 | 阶段 |
|---|---|---|---|
| S1 | **坏变更引发延迟/错误**：14:28 部署 commit abc → 14:30 起 P99 飙升 + DB 连接超时 | happy path：changes→trace→metric 收敛 | P0 |
| S2 | **DB 连接池耗尽，无变更**：池 100%、请求阻塞、无近期部署 | 无变更线索，必须靠 metric+trace 深挖 | P1 |
| S3 | **上游依赖故障（症状放大陷阱）**：A 服务报错，但拓扑显示 A 依赖 B，真因在 B | 拓扑感知：别错怪下游受害者 | P1 |
| S4 | **多异常红鲱鱼**：多个并发异常，仅一个因果 | 假设剪枝 / 证据权衡 / verifier 价值 | P2 |

---

## 11. SDK 覆盖映射（自然副产物，非硬凑）

**天然用到（因为 RCA 真需要）**：

| SDK 能力 | 在本系统的角色 |
|---|---|
| `PlanAgent` + `LLMPlanner` + 自定义 `Planner` | 假设回路编排器（核心） |
| `BasicAgent` ReAct + `Toolkit` + `FuncTool` | distiller + mock 数据源工具 |
| 嵌套编排 + cp 物理分离 + graphNS | 编排器→distiller 多层，cp 隔离 |
| HITL：`InterruptError` + `Resume` + `ask_user` | 信息补全 + 修复 gate |
| `store.Store`（`store.WithContext(ctx, st)` 注入） | 证据黑板 + offload/fetch_detail（跨 Run incident 记忆为可选延伸，本文未展开）|
| 4 层压缩（Micro/Auto） | **distiller（BasicAgent）**自身 ReAct 历史兜底压缩（编排器无压缩 option，轨迹靠源头提纯）|
| observability（trace + metric） | 整棵树可观测 = 集成测试产物 |
| `ReflectAgent`（P2 可选） | 独立最终质量门 |
| `SequentialAgent`（可选） | 报告格式化收口 |
| `ParallelAgent`（按需） | parallel_probe 相关性并行取证 |
| `fallback.FallbackChatModel`（后期可选） | 给编排器/distiller 注入故障切换，在 RCA 负载下验证 |

**自然没用到（且 OK，不硬塞）**：`RoutingAgent`（Planner 的选择已含路由语义）、`HandoffAgent`
（无控制权转移需求）、`LoopAgent`（plan 循环已含）、`GraphAgent`（PlanAgent 够用；仅当未来要自定义
树搜索才需要——已否决 §14）。**Skill** 可后期给 distiller 装领域知识包（如"K8s 故障排查 skill"），
但非 P0 必需。

---

## 12. 分阶段实施

| 阶段 | 范围 | 验收 |
|---|---|---|
| **P0** | PlanAgent + LLMPlanner + **RCA 规划 prompt（`WithLLMPlannerSystemPrompt`，否则无假设推理）** + 4 个 distiller（log/metric/trace/changes，BasicAgent + mock）+ S1 场景 + ctx deadline | 真 qwen 跑通 S1 一条"假设→取证→收敛根因"闭环，根因正确指向 commit abc |
| **P1** | 自定义 Planner（显式假设状态）+ topology distiller + in-loop verifier + S2/S3 + offload/fetch_detail（Store）+ observability 接入 | S2/S3 正确（含拓扑感知不错怪下游）；trace 完整呈现调查链 |
| **P2** | HITL 修复 gate（ask_user + Resume）+ S4 红鲱鱼 + （可选）ReflectAgent 外层质量门 | 修复动作必经人审；S4 正确剪枝；中断/续跑闭环 |
| **P3** | structured-output 硬化证据块/verdict（待该 feature 落地）+ fallback 注入 + Skill 领域包 + 更多场景 | schema 强约束；模型故障切换下仍出正确 RCA |

> 落点：实现代码放 `example/fault_analysis/`（snake_case 约定，对齐 `nested_smoke`/`plan_chat_web`）。

---

## 13. 测试策略

- **mock 冒烟**：mock 数据源固定 S1-S4，断言最终 RCA 的根因字段命中预期 commit/服务/指标。
  mock 工具**必须读 input 做断言**（如 query_traces 收到的 time window 是否是编排器按假设算的），
  否则等于忽略上游（CLAUDE.md 测试可信度规约）。
- **真 qwen 集成**：`//go:build integration`，`example/fault_analysis/` 下真跑（默认 `make test`
  不编译），验证真 LLM 下假设回路能收敛。
- **"正确 RCA"的判定**：不只看 RPC 成功——根因指向错误/证据链有断点/把下游当根因 都算失败
  （对齐 CLAUDE.md "什么算成功"）。
- **观测即测试**：P1 起，一条完整 trace 覆盖 编排器→distiller→model→tool→（中断）→Resume，
  即证明全链路通。

---

## 14. 已否决方案（附理由）

| 方案 | 否决理由 | 证据 |
|---|---|---|
| **树搜索 / MCTS / LATS / GraphAgent 树搜索** | 公开材料未见生产采用 value-model MCTS（不排除内部未公开）；成本显著更高（LATS-RCA 实测 ~3.8× token / 3.9× API 调用）；在跑产品用带证据剪枝的假设分支而非 value-model 树搜索 | DeepSeek-R1 §4.2 将 MCTS 列失败尝试；Datadog 自述 branch-and-prune 未点名 MCTS；LATS-RCA 是学术预印本（§2 结论 C） |
| **默认全量 fan-out（一上来 ParallelAgent 包所有 distiller 全查）** | 不是所有故障都需所有数据源，浪费 token + 制造噪声证据 | 结论 B "选择性派发"；用户明确指出"包一起浪费"。改为：选择性单派 + 按需 parallel_probe（§5.5） |
| **固定规则提纯数据** | "哪些数据相关"取决于当前假设，规则无法适配；漏关键证据 | 目标感知 LLM 提纯更准（§5.2） |
| **多 agent "supervisor + 一堆通用专家" 为多而多** | 增益多是算力/上下文 artifact 非架构；通用多 agent 复杂化审计 | Tran & Kiela 2026；SREGym（通用 Claude Code e2e 成功率高于专门 agent Stratus 60.7% vs 54.8%，但 Stratus mitigation/token 效率更优——说明"盲目堆专门多 agent 不必然更好"，非"通用必胜"）。我们的多"agent"是**有明确数据源职责的 distiller**，不是同质专家堆叠 |
| **ReflectAgent 外层包裹编排器作默认质量门** | 反馈轮重跑整条调查链，成本高 + 可能限流 | 改为 in-loop verifier（§5.3）；ReflectAgent 仅作 P2 可选独立门 |

---

## 附：关键接口签名（写作时对照真实代码验证）

```
agent_v2.NewLLMPlanner(m model.ChatModel, opts ...LLMPlannerOption) Planner
agent_v2.WithLLMPlannerSubAgents(agents map[string]Agent) LLMPlannerOption
agent_v2.WithLLMPlannerSystemPrompt(s string) LLMPlannerOption  // 注入 RCA 规划 prompt（默认是通用计划拆解器，无假设语义）
agent_v2.NewPlanAgent(planner Planner, opts ...PlanOption) *PlanAgent
agent_v2.WithSubAgents(agents map[string]Agent) PlanOption        // 与上面同一个 map
agent_v2.WithPlanMaxSteps(n int) PlanOption
agent_v2.Planner = func(ctx, sess *RunSession, input string, executed []ExecutedStep) (remaining []PlanStep, finalAnswer string, err error)
agent_v2.PlanStep{Description, SubAgentName string, TmpSubAgent *TmpSubAgentConfig, InputRefs []RefSpec}
agent_v2.ExecutedStep{Step, Agent, Result string}

agent_v2.NewBasicAgent(opts ...Option) *BasicAgent
agent_v2.WithName/WithDescription/WithSystemPrompt/WithModel/WithToolkit/WithMaxRounds/WithHooks/WithStore/WithStoreMemory/WithAskUser
agent_v2.NewReflectAgent(opts ...ReflectOption) *ReflectAgent  // WithReflector/WithEvaluator/WithReflectorAgent
agent_v2.NewSequentialAgent(...).AddStep(name, ag)
agent_v2.NewParallelAgent(...).AddBranch(name, ag)

agent_v2.Agent: Run(ctx, sess *RunSession, input *schema.Message, opts ...RunOption) ([]*AgentEvent, error)
                Resume(ctx, sessionID string, opts ...ResumeOption) ([]*AgentEvent, error); Name(); Description()
agent_v2.NewRunSession() *RunSession   // .RunID / .SetValue / .GetValue / .History()
agent_v2.FinalMessage(events) *schema.Message
agent_v2.IsInterrupt(err) (*compose_v2.InterruptError, bool)
agent_v2.WithResumeUserInputs(map[string]*schema.Message) ResumeOption
agent_v2.AskUserToolName = "ask_user";  agent_v2.WithAskUser() Option

tool.NewToolkit().Register(t) / .RegisterAll(...)
tool.NewFuncTool(name, desc string, inputSchema map[string]any, fn func(ctx, args map[string]any)(*tool.Result, error)) *FuncTool
tool.Result{Success bool, Output any, Error *tool.Error, Metadata map[string]any}
tool.ReturnDirectMetaKey  // 工具短路 ReAct 循环

compose_v2.WithCheckpointerCtx(ctx, cp) / compose_v2.WithSessionIDCtx(ctx, sid) / compose_v2.InterruptError
schema.SystemMessage/UserMessage/AssistantMessage(content) *Message

observability.New(cfg Config) (*Observability, error)   // 返两值
obs.AgentHooks() hook.Hooks;  obs.OrchHooks() agent_v2.OrchestrationHooks;  obs.CleanupSession(sid)
agent_v2.WithCtxOrchHooks(ctx, h) context.Context
store.FromContext(ctx) store.Store            // 单返回值；未注入返 nil
store.WithContext(ctx, st) context.Context    // Run 入口注入共享 store，透传到所有子 agent
```
