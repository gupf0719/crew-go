---
name: legacy-disabled
description: 已下线的旧 skill（仅作 P2.A demo——业务可临时下线 skill 但保留文件）
disable-model-invocation: true
user-invocable: true
tags:
  - legacy
  - p2-demo
---

# Legacy Skill

此 skill 标 `disable-model-invocation: true`——P2.A 协议下：

- LLM 看不到此 skill（`activate_skill` listing 过滤掉）
- LLM 即使硬猜名字硬调，框架返 `SkillDisabledErrCode="skill_disabled"` 业务级失败
- adaptive / pre_match 模式预筛也过滤

业务用途：临时下线 skill 但保留 SKILL.md 文件 + 元数据。
