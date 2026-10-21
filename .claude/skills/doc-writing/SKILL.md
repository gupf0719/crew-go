---
name: doc-writing
description: crew-go 项目写技术文档时的反偏差工作流——LLM 写文档存在系统性偏差（v2 文档体系审了 5 轮累计 100+ HIGH，单篇 context-compaction.md 42 HIGH）。写新设计文档（.md / 长 godoc）/ 改 doc/v2 / doc/description 下任何文件 / 改 README / 改 architecture.md / 写超过 30 行的 godoc 块 / 用户说"写文档/补文档/更新文档/写架构文档/改注释" 时**必须**调用此 skill。包含：写 vs 审两种认知模式分析 / 6 类系统性偏差（占比统计）/ 5 条 grep 必修动作 / 多轮 audit 必需性 / 跨文档同病防治 / 标准工作流。**不要跳过**——LLM 自觉写文档准确率 ~0%，必须按本 skill 流程。
---

# crew-go 文档撰写纪律

**核心问题**：LLM 写设计文档时即使"对照代码写"也会大量出错——v2 文档体系审了 5 轮累计 100+ HIGH（context-compaction.md 单篇 42 HIGH）。**不是"看代码不仔细"——是 LLM 写文档存在系统性偏差**。

## 写 vs 审是两种认知模式（必读：理解为什么写时难提前发现）

**同一个 LLM，"写"和"审"是完全不同的认知模式**——这不是"努力点就能一次写对"，是 LLM 的固有行为模式：

| 维度 | 写文档（生成模式）| 审文档（判别模式）|
|---|---|---|
| 目标 | 产出流畅完整的内容 | 找具体错误 |
| 注意力 | 整篇 1700 行分散 | 单个 claim 聚焦 |
| 真相源 | 训练数据 + 文档自己 | 代码（外部锚点）|
| 触发 grep 的 impulse | 弱（生成压力）| 强（prompt 明确要求）|
| 认知偏差 | 流畅性 / anchoring / "看起来对" | 怀疑性 / "我要找 bug" |
| 验证步骤 | 隐式跳过 | 显式必做 |

**4 个根本原因**：

1. **流式生成的本质矛盾**：写 token 时要在每个 API 名暂停 grep —— 跟"流畅生成"目标天然冲突。类比人类讲演时随口说"那本书的作者是 X"——讲的瞬间不会停下来翻书核对
2. **Anchoring 在生成时已发生**：一旦写出 `cm.RestoreFromCP`（错），下一段基于"它存在"继续推理，错误像滚雪球
3. **训练数据 vs 代码的拉扯**：见过几百个 LangGraph / LangChain 类似框架——`IsResumeTarget` / `BeforeSummarize` 都是常见 pattern，写时**自动激活**比 grep crew-go 实际代码"更省力"
4. **没有"内部审"信号**：写完一段没有"等等这里要核对"的内置触发器——必须外部 agent / 用户提示"审一下"才激活验证模式

**关键认知**：**写时的 LLM 和审时的 LLM 几乎是两个"人格"**——同一参数同一模型，**prompt 决定认知模式**。这就是为什么"先 grep 再写"难做到——它要求生成模式临时切换到判别模式，LLM 还没"暂停 + 切换"的内置机制。

**直接推论**：**禁止相信自己写文档时的输出**——必须切换到 audit 模式（独立 agent 复审）才有接近 100% 准确率。这不是"建议"是"流程必需"，类似传统软件工程的"必须有测试"。

## 6 类系统性偏差（写时必防）

| 偏差 | 具体表现 | 占比 |
|---|---|---|
| **训练数据 anchoring** | 见过 LangGraph `IsResumeTarget` → 假设 v2 也有 → 写下虚构 API | ~30% |
| **PoC 设计稿当事实** | PoC 期 "我们会做 X" → 落地简化/删除 → 文档仍引用 | ~20% |
| **v1 stale 没改透** | 改路径但没改概念 / 改表头但没改示例代码 | ~25% |
| **跨文档同病** | 同概念多文档独立写，错的时候全错（如 Replan 矩阵 / panic cp 跨 4 篇错）| ~10% |
| **functional claim 没 grep 实际逻辑** | 抓到 API 签名以为懂行为（如"panic 不存 cp"实际看 frontier）| ~10% |
| **算法/表格不完整** | 抓主干漏边角（如算法漏 step 1/最后一步收尾 / 表漏字段）| ~5% |

## 写文档前的 5 条 grep 必修

```
1. **写 API 名/字段/默认值/常量** → 必 grep 实际代码
   ❌ 凭印象写 NewParallel().Worker
   ✅ grep "^func New|^type|^const|^var" 实际包后写

2. **写 functional claim**（"框架自动 X" / "panic 时 Y" / "压缩后 Z"）
   → 必 grep 实际逻辑代码（不只是签名）
   ❌ "panic 不存 cp"（凭设计意图）
   ✅ grep "SaveNS" 实际触发条件 + 完整读 defer 块

3. **写算法/时序图** → 逐步对照源码完整数步骤
   ❌ 抓主干 for 循环写
   ✅ 数源码完整步骤数（含 step 1 隔离 system / 最后 sys 拼回 等收尾）

4. **修复一处** → 立即 grep 全文 + 跨文档
   ❌ 修 §6 一处
   ✅ grep "X|相似词" 在所有 v2 文档 + CLAUDE.md（防跨文档同病）

5. **表格列写 ✅/❌** → 每格必有具体证据
   ❌ "事件可观测 ✅"（自我美化）
   ✅ "✅ reset_stream Event"（具体什么 event）
```

## 多轮 audit 是必需品不是可选品

单视角 audit 只能抓 30% 的错。**4-5 轮多视角 audit 才接近收敛**（v2 文档体系实证）：

| 轮 | 视角 | 命中类型 |
|---|---|---|
| R1 | API 签名 / 字段 / 路径 | 凭印象写的虚构 API |
| R2 | functional claim / 跨文档 / 修复后引入新 bug | "框架自动 X" 失实 |
| R3 | 业务实跑示例 / 同文档自相矛盾 / v2 化名 vs 实际 var | 表格化名错 / 输出格式偏差 |
| R4 | 代码实际抛/不抛 / 跨包功能耦合 / 绝对化语句反例 | typed error 死代码 / OrchHooks 引擎层失效 |
| R5 | 算法步骤完整性 / 表格逻辑 / 跨段自相矛盾 | 算法缺 step 1 / 字符串字面量错 |

**不要以为单轮 audit 干净**——单轮命中率 100%（不是 33-43% 误判率）说明确实有问题，必须再审。

## 跨文档同病防治（最难发现）

同一概念多篇文档独立写——错的时候**多篇一起错**。例：
- "ResumeAction.Replan 所有 agent 接受" 错在 hitl-resume + orchestration + agent.go 注释 3 处
- "ReactiveCompact 同步回 messages" 错在 context-compaction + CLAUDE.md
- "panic 不存 cp" 错在 channel-engine + hitl-resume

**防治方法**：
- 写新文档前先 grep 已有文档对该概念的描述——保证一致
- 修一处时同步 grep `concept|alias|related_term` 在所有文档
- 关键概念应有"权威源" + 其他文档引用此源（避免重复描述）

## 写文档的标准工作流

```
1. 写之前：grep 实际 API 签名 + 字段 + 默认值 + 函数实现逻辑
2. 写完：spawn audit agent 复审（用上面 5 类视角的 1 类）
3. audit 报告：grep 验证每条 finding 真假（67% 误判率经验值仅适用宽范围审；单文档专审命中率接近 100%）
4. 修后：grep 全文 + 跨文档 是否同病
5. 重复 2-4 至少 3-5 轮（命中数几何递减但非 0)
6. **不承诺"已扫干净"**——说"R5 后接近收敛，剩余可能仍有 < 5 HIGH"

经验值：
- channel-engine.md 5 轮 audit 累计修 28 HIGH
- hitl-resume.md 4 轮累计修 14 HIGH
- context-compaction.md 5 轮累计修 42 HIGH（v1 stale 文档恢复时最多）
- orchestration-patterns.md 4 轮累计修 20 HIGH
```
