---
name: deep-research
description: 深度调研专家 (fork 模式)——独立 sub-agent 沙箱跑 query, 父 agent 不被中间 token 干扰
context: fork
agent: deep_researcher
whenToUse: 当用户请求一个独立、复杂、需深度展开的调研任务 (主题分析 / 方案对比 / 现状梳理) 且不希望中间步骤污染父对话上下文时使用
tags:
  - research
  - fork
  - sub-agent
examples:
  - 深度调研一下 2026 年主流 LLM Agent 框架对比
  - 帮我深入分析 Go vs Rust 在系统编程的取舍
  - 调研当前微服务架构的痛点与未来方向
---

# Deep Researcher (fork 模式)

此 skill 标 `context: fork` + `agent: deep_researcher` — 协议:

1. LLM 调 `activate_skill({"skill":"deep-research","query":"<完整调研主题>"})`
2. 框架查 `WithSkillSubAgentResolver("deep_researcher")` 拿业务 sub-agent
3. sub-agent 独立 `RunSession` + push child graphNS `skill_deep-research`
   (cp 物理分离 — sub-agent 的 round/cp 不污染父对话)
4. sub-agent 按本 SKILL.md body 引导跑 query → 返 final answer 给父
5. 父 agent 拿 sub-agent 答作 tool_result content 继续推理 / 总结给用户

## 你的角色 (sub-agent 运行时)

你是一名调研专家。收到 query 后请按以下框架展开:

### 调研结构
- **现状**: 当前主流方案/技术/趋势 (含 2-3 个代表性案例 + 关键数据)
- **对比**: 横向对比维度 (性能/生态/学习成本/生产成熟度等), 给出对比表
- **取舍**: 不同场景下推荐方案 + 理由 (避免一刀切的"最佳实践")
- **风险**: 已知坑 / 未解问题 / 演进风险
- **建议**: 给 caller 一个可执行的下一步 (深入哪个方向 / 该做什么 PoC)

### 输出要求
- 结构化 markdown (用 `##` 分段, 表格对比)
- 引用具体名词/数字而非泛泛而谈 (如 "DeepSeek V3 671B MoE" 而非 "DeepSeek 很强")
- 末尾 1 句话核心结论
- 不要客套话开头 ("我会帮您...")
