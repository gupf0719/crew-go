# 三方对照文档（crew-go / LangGraph / eino）

## 用途

crew-go 借鉴 LangGraph + eino 设计但不完全照抄——做架构决策时需要快速对照三方在同一问题上的处理方式，找出对方不足、吸收对方优势、形成我们自己的取舍。

本目录文档**不**是"LangGraph / eino 是什么"的全景描述（已有官方文档），而是**针对具体决策点**的对照速查：每个文档围绕一个主题（如"checkpoint 存储"、"子图隔离"），给出三方设计的核心数据结构 + 关键源码定位 + 优劣对照表 + 我们的决策记录。

## 什么时候查这里

按 CLAUDE.md "实施前 checklist" 第 4 条：涉及以下子系统的设计改动**必须先读对应主题文档**：

| 改动类型 | 必读文档 |
|---|---|
| Checkpoint 存储语义 / 接口签名 / **挂起存档与新任务的碰撞** | `checkpoint-storage.md` |
| 会话历史层（写入器 / 清洗 / 截断 / 归属）| `conversation-history.md` |
| 子图嵌套 / 隔离 / 注册方式 | `subgraph-isolation.md` |
| Interrupt / Resume 语义 | `interrupt-resume.md` |
| 上下文压缩 / 内容卸载到外部存储 | `context-compaction.md`（⚠️ 只覆盖「卸载去重」一个切口，其余层仍零对照）|
| NS / Address 寻址模型 | `address-routing.md` |
| Skill 加载 / 激活 / 子 agent 隔离 | `skill-system.md` |
| 可观测性 trace + metrics 集成 (OpenTelemetry) | `observability-trace-spec.md`（对照 OTel GenAI 语义约定）+ `observability-callback.md`（钩子协议决策）+ `observability-langgraph.md` / `observability-eino.md` (⚠️ 风格特例: 拆 2 文档单方面深度调研, 不是单文档三方对照; crew-go 决策综合见 `observability-eino.md` §8) |

读文档时重点看：
1. **三方对照表**：当前问题三方各怎么做，哪个有 corner case 是我们没考虑的
2. **我们的优势**：避免被 agent 报告"建议照抄 LangGraph"误导，确认我们已有的更优部分要保留
3. **我们的不足**：明确列出"对方做对了我们还没做"的项，决策时是否要补
4. **决策记录**：历次 ISSUE 我们怎么取舍的，避免重复辩论

## 维护原则

- **incremental 写**：每次推进 ISSUE / Phase 时把新发现的三方差异 incremental 写回对应文档；不专门开 review session 写
- **小步迭代**：控制单篇信息密度；超长主题（如 skill-system / error-handling）可拆子文件或外置细节到 external-references
- **决策记录可追加**：每次大设计选择（合 commit / 选方案 A vs B）在文档底部"决策记录"加一行，关联 ISSUE 编号
- **三方源码版本快照**：源码引用带 file:line 时附说明（如"LangGraph 0.6.10"），后续版本变化时 incremental 更新

## 强制查阅时机

CLAUDE.md 已加：

1. **"实施前 checklist" 第 4 条**：cp / 子图 / interrupt / state 设计改动前必读
2. **"交叉审核流程"加一步**：自审 + 独立 agent 复审之外，对照三方文档检查决策是否偏照抄

## 当前文档清单

| 文档 | 主题 | 状态 | 关联 ISSUE |
|---|---|---|---|
| `checkpoint-storage.md` | Checkpoint 存储模型 | ✅ 已写 | ISSUE-70 Phase 1/2 |
| `conversation-history.md` | 会话历史层（跨 Run 对话留存）| ✅ 已写 | ISSUE-98 |
| `subgraph-isolation.md` | 子图嵌套与隔离 | ✅ 已写 | ISSUE-64, ISSUE-70 Phase 2 |
| `interrupt-resume.md` | Interrupt / Resume 语义 | ✅ 已写 | ISSUE-58/59/70 |
| `context-compaction.md` | 上下文压缩 / 内容卸载 | ⚠️ 部分（仅卸载去重切口）| — |
| `address-routing.md` | NS / Address 寻址模型 | ✅ 已写 | ISSUE-70 Phase 3 |
| `agent-interface.md` | Agent 接口 / 嵌入协议 | ✅ 已写 | ISSUE-71（接口合并）|
| `skill-system.md` | Skill 系统设计（SKILL.md / 激活模式 / 子 agent 隔离）| ✅ 已写 | ISSUE-19, ISSUE-73 |
| `observability-callback.md` | observability callback / hook 协议层决策（含 metric 分类 / event-driven metric）| ✅ 已写 | Phase 2 Sub-5 |
| `observability-trace-spec.md` | trace / 指标 vs OpenTelemetry GenAI 语义约定：对得上的、偏离的、没做的与决策 | ✅ 已写（2026-10 按 OTel 重写） | Phase 2 Sub-5 |
| `error-handling.md` | 错误处理与失败恢复三方对照 | ✅ 已写 | 错误处理对照 |
| `state-model.md` | 状态 / 数据流模型（channel-based vs 三方）| ✅ 已写 | ISSUE-70 |
| `store.md` | Store 跨 thread 持久层三方对照 | ✅ 已写 | store PR |
| `observability-eino.md` | eino observability 调研（Handler + span stack 自管）| ✅ 已写 | Phase 2 Sub-5 |
| `observability-langgraph.md` | LangGraph observability 调研（callback + LangSmith）| ✅ 已写 | Phase 2 Sub-5 |
| `tool-visibility.md` | 工具可见性 / 裁剪三方对照（ToolFilter / AllowedTools / 框架工具豁免）| ✅ 已写 | OKR 8.4 |
| `routing-decision.md` | 带置信度的路由决策：门槛放在哪、报错与低置信是否同路、弃权与回落形态（通用框架 + Rasa / Dialogflow / Semantic Router / NeMo / RouteLLM / TypeSafe）| ✅ 已写 | RouteDecider |
| `runtime-model-params.md` | 运行时模型参数控制（thinking/reasoning 按次开关）**五方**对照（eino/OpenAI-Agents/MAF/LangChain·LangGraph/CrewAI）| ✅ 已写 | ISSUE-89 |
| `industry-survey-2026q3.md` | **2026-09-21 增量调查**（10 仓全量 pull 后）：「运行中插话」与「回合结束闸门」五家做法对照 + **AG-UI 1.0 定稿后我们两处真不合规** + OTel semconv 三个月零影响核对 + 16 条可迁移发现 | ✅ 已写 | 插话/闸门拍板 |

## 三方源码本地路径

| 项目 | 路径 | 版本 |
|---|---|---|
| crew-go | `/Users/gupengfei/program/go/src/crew-go` | `redesign/channel-based` (v1 已删，看 `agent_v2/` + `compose_v2/`) |
| LangGraph | `/Users/gupengfei/program/go/src/langgraph` | 主分支克隆（2026-09-20 拉；另有 `langgraph-bb` 已停滞在 2026-04-29）|
| eino | `/Users/gupengfei/program/go/src/eino` | 主分支克隆（2026-09-20 拉）|
| Codex (OpenAI，Rust) | `/Users/gupengfei/program/go/src/codex` | 主分支克隆（**2026-09-21 拉**）；`codex-rs/` 100+ crates |
| DeepSeek Harness (`dsh`) | `/Users/gupengfei/program/go/src/deepseek-harness` | **`0.1.6-alpha.2`**（2026-09-17 拉；developer preview，明示会破坏兼容）|
| Grok harness (xAI，Rust) | `/Users/gupengfei/program/go/src/grok-build` | 主分支克隆（2026-09-19 拉）|
| MAF (Microsoft Agent Framework) | `/Users/gupengfei/program/go/src/agent-framework` | 主分支克隆（2026-09-21 拉）|
| OpenAI Agents SDK | `/Users/gupengfei/program/go/src/openai-agents-python` | 主分支克隆（2026-09-21 拉）|
| CrewAI | `/Users/gupengfei/program/go/src/crewAI` | 主分支克隆（2026-09-19 拉）|
| AG-UI | `/Users/gupengfei/program/go/src/ag-ui` | **1.0.0 已定稿**（2026-09-17 拉）|
| OTel GenAI semconv | `/Users/gupengfei/program/go/src/otel-semconv-genai` | 主分支克隆（2026-09-16 拉；**整体仍 `development`，零 stable**）|
| TypeSafe（Jev 决策模型）| `/Users/gupengfei/program/go/src/typesafe-ai` | 官方 Python / JS SDK、LLM 模拟适配器、agent skill 四个仓库（2026-09-22 拉）|

> **v1→v2 等价 cheatsheet** 见 [`../v2/migration-history.md`](../../v2/migration-history.md)。本目录文件中 `compose/*.go:line` 引用都是 v1 时代位置，等价位置自查 v2 包 godoc。
