# 测试规约

> 单测必过、集成可选；新增 API 配套测试；黑盒/白盒按测试对象决定，不教条。
>
> 来源：ISSUE-52 测试与 CI 流程缺口梳理。

## 1. 单元测试 vs 集成测试

| 维度 | 单元测试 | 集成测试 |
|---|---|---|
| 位置 | 各包内 `*_test.go`（不带 build tag） | `example/adv_*_test.go` |
| 依赖 | 仅本地、纯内存 stub / 手写假实现 | 真实 qwen API (`QWEN_API_KEY`, dashscope 兼容 OpenAI); DeepSeek (`DEEPSEEK_API_KEY`) 仍可备选 |
| 触发 | 默认 `make test` / `go test ./...` | `make test-integration`（带 `-tags=integration`） |
| 时间预算 | 单包毫秒~秒级 | 单条 60~300s，整体 5min+ |
| CI 角色 | 必过门 | 可选 / nightly |

集成测试统一打 `//go:build integration` 标签。**默认 build 不会编译这些文件**——CI 单测路径不会因为没 key 或网络抖动 red。需要跑时显式 `make test-integration`。

新增依赖外部服务（任何 API 调用、子进程、文件系统外部目录等）的测试，必须落到 `example/` 或同样带 integration tag 的位置，**不可混进默认单测**。

### 模型名：**别写死，用 `qwen.TestModelCandidates`**

真 LLM 测试/example **不要硬编码模型名**，用候选列表（带日期的固定快照在前、滚动别名兜底）：

```go
// 单个：留空即取候选首位
m := qwen.NewTestClient("")

// 想让某个候选失效时自动切下一个（推荐给整套测试用）
models := make([]model.ChatModel, 0, len(qwen.TestModelCandidates))
for _, name := range qwen.TestModelCandidates {
    models = append(models, qwen.NewTestClient(name))
}
m := fallback.New(models)
```

**为什么**（都是踩过的）：

- **免费额度按模型耗尽**。`qwen3.6-flash` 耗尽那次，整套 15 个 integration 测试**全部 403**——而默认 `go test` 不带 tag 跑不到，一直没暴露。写死模型名 = 把整套测试绑在一个会过期的资源上。
- **候选入列前必须逐个探活**，不能只看"这个模型有额度"。两类失效：额度耗尽（403 `AllocationQuota`）/ **与 preset 参数不兼容**（如强制要求 `enable_thinking=true`，而 `presetDashscope` 写死 `false` → 恒 400）。
- **不兼容的比耗尽的更毒**：`fallback.DefaultFailoverPolicy` 只对额度类 4xx 切换，普通 400 直接抛——链走到这种候选会**硬停**，后面的健康候选一个都够不着。
- 守卫：`go test -tags integration ./model/qwen/ -run TestTestModelCandidates_AllReachable` 逐个探活，某个候选失效会指名报出来。

带日期的固定快照优先，是因为同名永远是同一个模型、测试行为可复现；滚动别名（`qwen3.7-max`）阿里侧会指向新版本，用例可能因换代而行为漂移。

## 2. 新增 / 修改公开 API → 必须带单测

**适用范围**（只要满足任一条就必须配套单测）：

- 新增 exported 函数 / 方法 / 类型；
- 新增 / 修改 `agent` `compose` `hook` `tool` `model` 等核心包的关键内部函数（即使未导出）；
- 修复了一个具体 bug（必须有用例锁住回归路径）。

**判断"关键内部函数"的标准**：
- 被多个 exported API 共用的辅助；
- 含错误分支 / 并发 / 状态变更逻辑；
- 出过 bug 或未来容易再出 bug 的位置。

不强制纯样板代码（getter/setter、纯结构体定义）走单测。

历史遗留盲区（基线覆盖 0%）按机会成本逐步补，不一次性堵上（注：下列 `agent/`、`compose/` 是 **v1 路径已删**，v2 等价见 `agent_v2/skill_tool.go` / `agent_v2/options.go::applyRunOptions` / `agent_v2/session.go::rewriteCrossAgentMessage`）：
- `agent/skill_tool.go`
- `agent/options.go applyRunOptions`
- `compose/rewrite.go rewriteMemoryMessages` / `rewriteCrossAgentMessage`

## 3. 黑盒 vs 白盒：按测试对象决定

CLAUDE.md 默认倾向黑盒（`package xxx_test`），但**不教条**。

| 测试对象 | 文件命名 | package 声明 |
|---|---|---|
| 公开 API 行为 | `xxx_test.go` | `package xxx_test`（黑盒） |
| 未导出函数 / 内部分支 | `xxx_internal_test.go` | `package xxx`（白盒） |

**关键原则**：不要为了"能测"而把内部函数 export，违背最小 API 表面。

实例参考：
- (v1 已删) `compose/resume_internal_test.go` 测 `IsResumeTarget` / `addNSAndAncestors` 等未导出 helper → v1 时代白盒范式. v2 同款范式见 `compose_v2/*_internal_test.go` (e.g., `parallel_sort_internal_test.go` 测 ParallelAgentBuilder 内部 sort)。
- `agent/architecture_audit_internal_test.go` 锁住 `package agent` 内部命名 / 依赖结构 → 白盒合理。
- `agent/autocompact_internal_test.go` 触发 `AutoCompactRun` 内部分支 + 断路器并发 → 白盒合理。

强行黑盒会逼出"仅为测试而 export"的反模式。

新增白盒测试文件统一加 `_internal_test.go` 后缀，便于 grep 区分。

## 4. 表驱动只用于纯输入→输出

CLAUDE.md "优先表驱动" 细化为：

**用表驱动**：
- 输入 / 输出都是简单值，断言一致（`{name, input, want}`）；
- 边界值枚举（empty / nil / 超长 / 特殊字符）。

**不用表驱动**（拆成独立 `TestXxx`）：
- 每个 case 需要不同的 stub / mock（如 `errToolModel`、`nilEventsAgent`）；
- 错误路径需要差异化 setup；
- 用例间逻辑差异 > 数据差异，硬塞表里反而拖累可读性。

参考：`agent/options_coverage_test.go` 表驱动合理；`agent/memory_errors_test.go` 拆函数合理。

## 5. mock 框架 ban 名单

CLAUDE.md 已明：**不引入 mock 框架**（gomock / mockery 等都不用）。

替代方案：
- 接口定义在调用方 → 测试时手写 stub 实现；
- 复杂场景写命名清晰的 stub struct（如 `errToolModel`、`stubChatModel`）；
- 流式 / 并发 stub 走标准库 `chan`、`sync` 即可。

## 6. 覆盖率使用建议

- 本地巡检：`go test -coverprofile=/tmp/x.cover ./<pkg>/... && go tool cover -func=/tmp/x.cover`；
- 不追单包 100%——盯**新增代码**和**已知盲区**，老代码维持基线即可；
- 覆盖率快照已纳入 CI（`.gitlab-ci.yml` 的 coverage job，`7672ceea`）；本规约不强制单包目标。

## 7. HITL Resume 测试矩阵

**背景**：HITL（ask_user 中断 + Resume）跨多层（cp 持久化 + ns 寻址 + UserInputs
注入 + 中断信号源）"语义层 feature × 注入路径"组合多，单测难穷举。强制约定：
每改 HITL 相关代码必查矩阵覆盖。

### 矩阵维度

| 维度 | 档 | 备注 |
|---|---|---|
| **ResumeAction** | Continue / Replan / Abort | Replan 接受 = PlanAgent + BasicAgent + GraphAgent (R21 audit MED-1 透传) 共 3 编排，其它 6 编排 (Sequential / Parallel / Loop / Routing / Reflect / Handoff) fail-fast `ResumeActionReplan not supported` |
| **UserInputs 注入路径** | 无 / 单 entry / 多 entry | v2 严格契约：所有 InterruptError 必须含 Query.InterruptID（hook-pause 业务也要填虚拟 Question）|
| **编排** | BasicAgent / Sequential / Loop / Reflect / Handoff / Parallel / Routing / Plan / GraphAgent | 9 编排都通过 `Agent.Resume(ctx, sid, opts...)` 同模式支持 |
| **中断节点位置** | ask_user tool / hook-pause / 业务节点 InterruptError | 工具与业务节点走 `compose_v2.Ask`：ID 由框架按执行位置派生，自填会被拒；hook 相位不在任何节点执行里、`Ask` 会被拒——直接返 `&InterruptError{Query: ...}` 并自填 InterruptID 作标签（空 ID 当场 `ErrRunAborted`）|

### 当前覆盖

主要分两层（按 [smoke-test-recipes.md](./smoke-test-recipes.md)）：

**单元测试**（`agent_v2/*_test.go` + `compose_v2/*_test.go`）：
- `agent_v2/sequential_agent_test.go::TestSequentialAgent_HITLResume` — Continue + 单 entry + Sequential + ask_user
- `agent_v2/repro_round*_test.go` — 多个 reproducer（涵盖 InterruptID 5 维冲突 / streaming + ie 相互作用 / fan-out per-branch delta + Resume / 嵌套子图 cp 物理分离）
- `agent_v2/retry_integration_test.go` — Retry 不吃 InterruptError 守卫
- `compose_v2/audit_fixes_test.go` — fan-out / Resume / GraphID / ThreadID 多 audit 回归

**Binary smoke**（`example/`）：
- `example/orchestration/nested_smoke/main.go` — 13 scenario 端到端 binary（5 基础 + 4 高级 + Parallel/Routing 嵌入 + GraphID/ThreadID）
- `example/orchestration/channel_smoke/main.go` — 5 场景 compose_v2 底层（含 BasicAgent ReAct + Plan + 嵌套子图独立 cp + parallel per-branch delta）
- `example/orchestration/plan_chat_web/` mock 模式 — interrupt + Resume continue + cleanupOnSuccess

### 强制约定

**改 HITL 相关代码（含 cp / 5 维 InterruptID / ns push / hook / Resume）的 PR**：
1. 查矩阵——本次改动是否引入新组合或改变现有组合行为
2. 新组合无测试 → **必须补**到对应 `agent_v2/*_test.go` / `compose_v2/*_test.go`
3. 改注释承诺的功能 → grep 实现是否真做（防 stale 注释）
4. 跑两个 binary smoke（nested_smoke + plan_chat_web mock）做端到端兜底

## 参考

- CLAUDE.md "测试" / "交付纪律" 段落（本文档为其细化版）
- ISSUE-51 / ISSUE-52（推动本规约成文的两个 issue）
- Makefile `test-unit` / `test-integration` 两个 target
