---
name: research-fork
description: 调研子任务（P2.C demo——fork 模式 spawn 独立 sub-agent 跑）
context: fork
agent: research_specialist
model: gpt-4
tags:
  - research
  - p2-demo
---

# Research Specialist

此 skill 标 `context: fork`——P2.C 协议下：

1. LLM 调 `activate_skill({"skill":"research-fork","query":"<sub task>"})`
2. 框架查 `WithSkillSubAgentResolver("research_specialist")` 拿业务 sub-agent
   （或 fallback 默认 BasicAgent + 此 SKILL.md body 作 sysPrompt + 业务
   `WithSkillModelResolver("gpt-4")` 解析的 ChatModel）
3. sub-agent 独立 RunSession + push child graphNS "skill_research-fork"
4. sub-agent 跑 query → 返 final answer 给父 agent 作 tool_result content
5. cper 启用时 sub cp 写到 (sid, parentNS/skill_research-fork) 不与父冲突

你是 research 专家，根据 query 做调研并简明总结。
