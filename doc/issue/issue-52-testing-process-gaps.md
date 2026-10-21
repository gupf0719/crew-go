---
id: ISSUE-52
title: 测试与 CI 流程缺口（集成测试与单测未隔离 / 子系统无基线覆盖 / 风格规约未贯彻）
status: 🟡 部分落地（#1/#3/#5 已处理，#2/#4 待跟进）
priority: 中
created: 2026-04-24
updated: 2026-04-27
source: 单元测试覆盖扩展过程（ISSUE-51 同一轮工作）中回溯整理
---

# ISSUE-52 · 测试与 CI 流程缺口

## 背景

在补齐 hook/compose/agent 单测覆盖的过程中发现，项目对"测什么、怎么测、什么时候跑"缺一套成文规约，导致：
- CI 本地跑不稳（依赖真实 LLM 的用例混在默认 `go test ./...`）；
- 某些子系统（如 `agent/skill_tool.go`）基线覆盖 0%，说明合入时没配套测试；
- 测试风格（黑盒 vs 白盒）与 CLAUDE.md 建议脱钩。

## 问题清单

### 1. 集成测试与单元测试未隔离

`TestAdv_07_PlanAgent` 等用例走真实千问 API，带 5 分钟 deadline。默认 `go test ./...`：
- 无网络/无 key 环境直接 red；
- 本地跑一次 5 分钟起步，没人愿意执行完再提交；
- 刚跑完的后台任务里，`./...` 能过只是因为当前环境恰好有凭证，不代表 CI 绿。

**建议**：
- 引入 build tag `//go:build integration`，所有依赖真实 LLM 的用例加 tag；
- 默认 `go test ./...` 只跑单测；
- 新增 Makefile target `test-integration` 显式运行 integration tag；
- CI 矩阵拆两条：`test-unit`（必过）、`test-integration`（可选/nightly）。

### 2. 关键子系统无基线覆盖

覆盖率巡检发现以下文件合入时就没配套单测：

| 文件 | 当前覆盖 | 含义 |
|---|---|---|
| `agent/skill_tool.go` | 0% | skill 激活内部 tool 从未被单测触达 |
| `agent/options.go` `applyRunOptions` | 16.7% | RunOption 路径几乎没被端到端用 |
| `agent/react.go` 中 `rewriteMemoryMessages` | 只在黑盒端到端里偶尔命中 | 独立函数无直接单测 |

**根因**：合入时没有"新增公共 API/内部关键函数 → 必须带单测"的 PR 模板约束。

**建议**：
- 新增 `doc/description/testing-conventions.md`：列出"新增 exported 函数/关键内部函数必须带同包测试"的规约；
- PR 模板加 checkbox："本次新增/修改的 exported API 均有单测覆盖"；
- CI 加阈值报警：整体覆盖率下降超过 2% 阻止合并（纯统计信号，不是硬门）。

### 3. 黑盒 / 白盒测试风格未统一

CLAUDE.md 倾向 `package xxx_test`（黑盒）。实际巡检：

| 包 | 黑盒 | 白盒 |
|---|---|---|
| `compose` | 少数 | 多数（需访问 `parsePlanToolArgs` 等未导出辅助） |
| `agent` | 多数 | 少数（`react_coverage_test.go` 是白盒） |

**现状合理性**：纯黑盒测不到内部分支（例如 `parsePlanToolArgs("bad")` 的 unmarshal 路径）。强行黑盒会逼着把内部函数 export，违背"最小 API 表面"。

**建议**：
- CLAUDE.md 把"倾向黑盒"改为"按测试对象定：公开 API 走黑盒，内部分支走白盒"；
- 白盒测试文件命名加 `_internal_test.go` 后缀以示区分（Go 社区惯例）；
- 或保留现状但在 CLAUDE.md 明确"白盒测试用于覆盖未导出逻辑，不应暴露仅为了测试的 export"。

### 4. 覆盖率快照未纳入 CI/文档

当前覆盖率只能靠本地 `go tool cover -func=/tmp/compose.cover` 手动查。历次提交前后涨落没有记录，ISSUE-51 能识别出的"覆盖 0% 子系统"也完全靠人工巡检。

**建议**：
- CI 跑完单测后产出 `coverage.out`，上传为 artifact；
- 在 `doc/description/` 加一页 `coverage-snapshot.md`，每次重大覆盖变动手动追加（半自动，不求实时精准）；
- 或接 codecov（小项目非必需，但门槛低）。

### 5. 表驱动用例粒度不统一

CLAUDE.md "优先表驱动"。实际：
- `options_coverage_test.go` 里少数用表驱动；
- `react_coverage_test.go`、`memory_errors_test.go` 全是逐个函数 `TestXxx`。

**原因**：错误路径通常需要不同的 stub（`errToolModel`、`nilEventsAgent` 等），参数化反而拖累可读性。**不是所有场景都适合表驱动**。

**建议**：把 CLAUDE.md 的"优先表驱动"细化为：
- 纯输入→输出用表驱动（已是共识）；
- 需要差异化 stub/mock 的错误路径用独立 `TestXxx`（避免强塞进 table）。

## 交付物

- [x] #1 集成测试 build tag 隔离 + Makefile 配套；CI 矩阵已落 `.gitlab-ci.yml`（build / test / race / vet / lint / coverage 六 job，`8f5e626`）
- [x] #2 新增 `doc/description/testing-conventions.md`；MR 模板已落 `.gitlab/merge_request_templates/Default.md`（2026-09-07——远端是 GitLab 不是 GitHub）
- [x] #3 CLAUDE.md 测试风格段落细化（指向 testing-conventions.md）
- [x] #4 覆盖率快照：coverage job 产出 `coverage.out` artifact（留 30 天）+ GitLab coverage 正则抓 total 进 MR 概览 / 徽章（2026-09-07）
- [x] #5 CLAUDE.md 表驱动规约细化（合并到测试段落 + testing-conventions.md）

## 2026-04-27 落地说明

- 所有 `example/adv_*_test.go` 加 `//go:build integration` 标签；默认 `go test ./...` 不再编译这些文件，CI 单测路径与真实 LLM 完全解耦。
- 新增 `Makefile`：`make test` = `go test ./...`（单测必过门）；`make test-integration` 显式跑集成（带 `HYPERMNESIA_API_KEY` + 600s timeout）。
- `doc/description/testing-conventions.md` 一处汇总：单测 vs 集成、新增 API → 必带单测、黑/白盒按对象决定（白盒文件加 `_internal_test.go` 后缀）、表驱动只用于纯输入→输出、ban mock 框架。
- CLAUDE.md "测试" 段落改写为四条精简规则 + 指向 conventions 文档；`doc 目录说明` 表格补 `testing-conventions.md`。
- ~~未做：#2 的 PR 模板部分（无 `.github/`）；#1 的 CI 矩阵（无 CI 配置）；#4 全部。这些都依赖 CI 平台决策，单独立项再说。~~ **2026-09-07 全部收尾**（CI 平台是 GitLab，见上方交付物）。

## 优先级

| 子项 | 建议顺位 | 理由 |
|---|---|---|
| #1 集成测试隔离 | **高** | 直接影响 CI 可用性，改动局限 |
| #3 黑/白盒规约 | 中 | 主要是文档澄清 |
| #5 表驱动细化 | 中 | 文档澄清 |
| #2 基线覆盖规约 | 中 | 需要流程变更（PR 模板） |
| #4 覆盖率快照 | 低 | 边际收益，等项目更大再做 |

## 参考

- CLAUDE.md "测试" / "交付纪律" 段落
- [agent/skill_tool.go](../../../crew-go/agent/skill_tool.go) — 0% 基线
- ISSUE-51 · 同一轮测试工作中暴露的设计债
