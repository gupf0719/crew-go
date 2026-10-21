# ISSUE-92 · skill 源头分层（L0 progressive disclosure）+ 触发缓解 + over-read 预算闸门

**优先级**：中（缓解 toolCall 模式 skill body 跨 compaction 累积，但要改 SKILL.md 格式 + 新工具，影响面大）
**状态**：📋 设计完成待实施（2026-06）
**触发**：讨论"压缩保护 skill body"时对照 Claude 发现机制根本不同（crew-go marker 就地保护 vs Claude 异地存活重打包）→ 深挖业内（deep-research，105 agent / 多源 3-0 验证）→ 源头分层是治本方向（reference 层多轮重复读仍需 per-Run 去重，非完全免控制）

## 完整设计（L0 / progressive disclosure）

> **目标**：从源头解决 toolCall 模式 skill body 跨 compaction 累积（见 §当前不足"Claude
> Code 维度对比揭示的新缺口" #10）——用"分层加载"**大幅缓解**"压缩时保护"（把累积主体从
> body 层迁到 reference 层）。业内调研（2026-06 deep-research，多源 3-0 验证）证实源头分层是
> 治本方向，**但 reference 层多轮重复读仍需轻量去重**（见风险 #4），非完全免控制。本节是**设计方案**，
> 现有机制加 `路径` 引用、**【提议】**标注待实施项。

### 背景：为什么从源头分层而非压缩保护

当前 toolCall 模式把 SKILL.md body 全量注入 tool_result + 打 `SkillBodyAnnotation`
（`agent_v2/skill_tool.go:44`）marker，4 层压缩跨层保护它不被压（§不足 #10 P1.C）。
**问题**：多 skill 长会话累积无界，且压缩专门保护它反而加剧占比。

业内证据（deep-research 2026-06，全部 3-0 对抗验证）：

- **Anthropic Agent Skills 官方就是三级 progressive disclosure**（见 §维度 1 渐进式披露）：
  L1 metadata(~100 tok) always 注入 / L2 SKILL.md body(<5k) 触发时读 / L3 引用文件按需读
  （未用零 token）。官方在 L1/L2/L3 **加载阶段不做内容裁剪/压缩**（区别于 context 压力时的压缩裁剪——二者不同层、互补，见下方 spec #135）。
- **SkillReducer (arXiv 2603.29919)**：source-level 重构为 core 模块(always) + reference
  模块(read_file 按需)，五类 taxonomy（Core Rule 38.5% / Background 40.7% / Example 12.9% /
  Template 7.6% / Redundant 0.3%，仅 core always-load）；实测 less-is-more（质量 +2.8%）。
- 反面：**Codex** AGENTS.md 全量拼接 + 32KiB 字节截断；**Copilot** 自动注入全量 merge（模型零 agency）。
- **关键判断**（AGENTS.md spec #135）：源头分层(load-time) 与压缩裁剪(context-pressure-time)
  **不同层、互补不替代**，但 spec 规定"不应同时保留同一内容摘要版+全量版"——即走 L0 后，
  §不足 #10 的 `SkillBodyAnnotation` 全量保护应**移除**。

### crew-go 落地基础（L3 数据模型雏形好；路由 / 解析 / Description 需新建）

`skill.SkillResource`（`skill/skill.go:84`）天生 metadata-only：

```go
type SkillResource struct {
    Path         string  // 文件路径
    ResourceType string  // reference/script/template/asset
    Description  string  // 描述
}  // 无 Content 字段——扫描时只记元信息，不加载内容
```

`ActiveSkill.Resources`（`skill/skill.go:105`）也只带 metadata。**这就是 L3"reference 按需"
的天然雏形**——reference 内容默认不在 context，只有 `ActiveSkill.Prompt`（SKILL.md body）全量注入。

**但"基础好"仅限 L3 数据模型层**：路由（`MatchReferences` 缺）/ per-reference frontmatter
解析（现有 parser 不读 reference frontmatter）/ `Description` 填充（`scanResources` 现状恒空）
三条链路都需新建（见风险 #3），不是全局低成本。

### 三级分层设计（对齐 Anthropic §维度 1）

> **一句话主线**：core 留 SKILL.md body 常驻 + 详情下沉 reference 按需读 + 框架 deterministic
> 路由替 LLM 兜住"该读没读"。三级 progressive disclosure 对齐 Anthropic，比纯 LLM 方案多一层
> 框架路由兜底。下方 L3a–d / 预算闸门 / dedup 都是为"按需读得准、读得有界"服务的细节。

| 级别 | crew-go 落地 | 现状 |
|---|---|---|
| L1 metadata | `SkillDescriptor` name/desc → `activate_skill` 工具 description 全量列（`agent_v2/skill_tool.go`）| ✅ 已有 |
| L2 core body | `ActiveSkill.Prompt` 注入但**约定只放 core**（参 SkillReducer：只留 Core Rule）| ⚠️ 当前全量，缺写作约定 |
| L3 reference 按需 | `SkillResource`（已 metadata-only）+ **【提议】`read_skill_resource` 工具** | ❌ 缺读取工具 |

### 【提议】read_skill_resource 工具（L3 入口）

`read_skill_resource(skill, path string) → 内容`：LLM 看到 reference metadata（path / type，
**`Description` 待 `scanResources` 填充后才可见**——现状恒空，见风险 #3）后可主动读（框架侧可薄封装
现有 `skill.SkillBox.ReadSkillResource`，`skill/skillbox.go:219`，无需从零实现读取逻辑）；读入作 tool_result 注入（用完即弃，走通用压缩——**不打 `SkillBodyAnnotation`**）。

### 触发可靠性缓解（L0 最大风险：Vercel evals 实测 56%（对 Anthropic Agent Skills） 未触发）

reference 按需读纯靠 LLM 自主决策时，Vercel evals 实测 **56% 该读没读**。crew-go 用
**框架 deterministic 路由**（reference 级匹配复用 `skill/match.go::keywordScore` 词频算法 [0,1]，但需 skill 包新增 export 的 `MatchReferences`——见风险 #3）大幅降低漏读（下表 L3a–d 是 L3 reference 层内部的四种机制，非与 L1/L2/L3 平行的层级）：

| 层 | 机制 | 靠谁 |
|---|---|---|
| **L3a 框架路由**（主力）| **【提议】**`SkillResource` 加 `When`+`Topics`（需新建 per-reference frontmatter 解析，见风险 #3）→ skill 包**新增 export 的 `MatchReferences`**（复用 `keywordScore` 词频算法；`keywordScore` 本身 unexported，agent_v2 跨包调不到）做 query↔reference 匹配 → score≥MinScore 框架**自动读入** | 框架（deterministic）|
| **L3b pre-load** | **【提议】**frontmatter 标 `always_load` 的 reference 激活即读 | 强制 |
| **L3c LLM 兜底** | 框架没路由到的，LLM 主动 `read_skill_resource` | LLM（对齐 Anthropic 原生）|
| **L3d 可观测** | 复用 `SkillSelectionHook`（`agent_v2/adaptive_skill.go:59`）模式 + "skill 激活 0 reference 读" warn | — |

**本质**：把现有 adaptive skill 选择（query→skill，`agent_v2/adaptive_skill.go::selectSkillAdaptive`）
**下沉一层**到 reference 级（query→reference）——框架 deterministic 匹配为主、LLM 自主读降级兜底。
这是相对 Anthropic 纯 LLM 方案的结构性优势——词频算法（`keywordScore`）+ `MinScore` 门槛已在；但 reference 级路由需 skill 包**新增 export 的 `MatchReferences`**（现有 exported `MatchSkills` 是 query→skill 级，matchText 不含 reference），非零成本。

### 【提议】over-read 预算闸门（控 deterministic 路由词频假阳）

这是**读取时入口闸门**，区别于压缩的历史闸门：

```
budget = min((ModelContextWindow − 当前已用token) × ReadBudgetRatio, ReferenceReadBudgetCap)
// 当前已用：compose_v2.EstimateTokens(messages)
//   注：EstimateTokens 估 []*schema.Message；reference content 是 string，需包成 Message 估或加 string helper
```

读取决策（reference 路由后）：
1. 候选 `MatchReferences(query, refs)`【提议：skill 包新增 export，内部复用 `keywordScore` 算法】score 降序
2. 滤 score < `MinScore`（复用 `SkillSelectorConfig.MinScore`，`agent_v2/adaptive_skill.go:44`）
3. `always_load` 优先入选
4. 贪心累加：超 budget 或单 ref 超 `PerReferenceMaxTokens` → **skip**
5. emit 可观测 event

**边界裁决**（勿漏）：`always_load`（L3b 必读）**豁免 budget**（强制读）但仍受 `PerReferenceMaxTokens` 截断；`budget ≤ 0`（长会话末期 used≥window）时**全退化 L3c LLM 兜底**（不主动读）；skip 的 ref 本 Run **不再被框架路由重读**（仅 L3c LLM 可主动读，避免跨轮重复累积，呼应风险 #4）。

**⭐ skip 不丢失**：超预算 skip 的 reference 不主动读，但 metadata 仍在 listing → LLM 可主动
read（L3c 兜底）。**预算闸门是"框架替你读 vs 你自己读"的分界阀**，`ReadBudgetRatio` 是业务
在"可靠性 vs token 成本"间的旋钮，把 L3a↔L3c 连成连续谱（不是两个割裂机制）。

三闸门正交：`MinScore`（质量/相关性）+ budget（数量/总 token）+ `PerReferenceMaxTokens`（单条）。

### L2 简化（落地 L0 后）

reference 变成按需读取的普通 tool_result（用完即弃），移除 **toolCall reference 路径** 的
`SkillBodyAnnotation` 专门保护（§不足 #10）：`agent_v2/autocompact.go::splitSkillBodyGroups`
+ `reactive_compact.go::isProtectedGroup` SkillBody 分支 + `microcompact.go` SkillBody 检查。

**⚠️ 边界（勿按字面全删）**：`SkillBodyAnnotation` 是**共享 marker**——inline 模式
（`WithInlineSkillBody`，inline 模式 terse-ack tool_result + body UserMessage **都打它**（R19），
`skill_inline_body_test.go` 守护）是 L0 **未触及**的独立 feature。移除保护**仅限 toolCall
reference 路径**，inline 模式的 `SkillBodyAnnotation` 保护必须**保留**（除非 inline 一并随 L0 重构）。
（原 `SkillListingAnnotation` 已随增量 listing 一并删除——L1 metadata 现在走 `activate_skill` 工具 description，属 API tools 参数不进对话流，无需跨压缩保鲜。）

### 风险与代价（诚实）

1. **触发可靠性**（最大）：即使 deterministic 路由，`Topics` 关键词质量决定命中率。缓解：
   L3a 路由 + L3b pre-load 兜底必读 + 框架辅助从 reference 文件名/首行抽 `Topics`。
2. **taxonomy 依赖**：SkillReducer 实证 55,315 skill 中 26.4% 缺路由 description——分层质量
   靠 skill 作者配合或框架辅助。
3. **新增成本（勿低估，第 2 轮 audit 揭示）**：① `read_skill_resource` 工具（可薄封装现有 `SkillBox.ReadSkillResource`，`skill/skillbox.go:219`）② **新建 per-reference frontmatter 解析链路** —— 现有 `skill/discover.go::parseFrontmatter` 只解析 SKILL.md 顶部 `SkillDescriptor`、**不读 reference 文件自身的 frontmatter**；且 `SkillResource.Description` **现状恒空**（`scanResources` 只填 Path+ResourceType，从不填 Description）→ 要让 LLM "看到 reference description" 必须先让 `scanResources` 填充 ③ skill 包新增 export 的 `MatchReferences`（见触发缓解）④ SKILL.md 写作约定。**这不是"扩个字段"的轻量改动**。
4. **同一 reference 多轮重复读入累积**："用完即弃" 仅单次成立；多轮中 LLM 兜底 / 框架路由可能反复读入同一 reference（当前无 dedup）——L0 要根治的"累积无界"在 reference 层可能复发。缓解：per-Run 读取去重（框架路由 + L3c LLM 兜底**两条读入路径都查本 Run 已读集**，命中即不重读）+ skip 的 ref 本 Run 不被框架重新路由。**dedup ≠ #10 的 `SkillBodyAnnotation` 保护**（不是把"控制累积"请回来）：dedup 是**读取入口幂等**——每 reference 本 Run 至多读 1 次、读入后照常受通用压缩裁剪，**上界有限**；#10 是**历史保护**——已注入 body 跨轮常驻 + 对压缩免疫、**无界**（行 #10 病根）。两者不同档，故 dedup 仍属治本范畴。
5. **破坏性**：改 skill body 注入语义（全量→core），现有 skill 包要重组（项目未上线可接受）。

### 实施顺序

0. **前置（依赖底座）**：让 `scanResources` 填充 `SkillResource.Description` + 新建 per-reference frontmatter 解析链路（风险 #3②）—— 否则步骤 1 注入的 listing Description 恒空、对 LLM 零信息量
1. `read_skill_resource` 工具（薄封装 `SkillBox.ReadSkillResource`）+ SkillResource metadata 注入 listing（**依赖步骤 0** 的 Description）
2a. skill 包新增 export 的 `MatchReferences`（复用 keywordScore 词频算法）
2b. 框架 deterministic 路由（L3a，**依赖 2a**）+ over-read 预算闸门
3. pre-load（L3b）+ 可观测（L3d）+ **reference 读取 per-Run 去重**（风险 #4 缓解）
4. 移除 **toolCall reference 路径** 的 `SkillBodyAnnotation` 专门保护（L2 简化；inline 模式保留）
5. SKILL.md 写作指南（core 留 body / 详细放 reference）

业内对照见 [skill-system.md §三方对照表 + §维度概览](../description/three-way-comparison/skill-system.md)。

---


## 关联

- [skill-system.md §三方对照表 + §维度概览](../description/three-way-comparison/skill-system.md) — 业内对照 + crew-go 抄了哪些 + §不足 #10（本方案替代的现状：压缩保护，治标）
- 业内：Anthropic Agent Skills（三级官方）/ SkillReducer arXiv 2603.29919 / AGENTS.md spec #135 / Cursor .mdc
