package agent_v2

// 旧 compact.go 的所有函数都已被新设计替代：
//
//   - 旧 MicroCompact(messages, maxPairs) → 删除（ISSUE-66）
//     语义错位（实际是按对裸截断），相同截断兜底由 react.go::truncateHistory 承担。
//     新 MicroCompact 见 agent/microcompact.go::MicroCompactRun（白名单内的旧
//     tool_result 内容清除）。
//
//   - 旧 AutoCompact(ctx, m, messages, maxPairs) + NewAutoCompactor → 删除（ISSUE-67）
//     简易摘要实现已被新 AutoCompactRun（agent/autocompact.go）取代——
//     新版按 token 估算触发、9 段结构化摘要、NO_TOOLS_PREAMBLE 双放、
//     断路器、附件重注入。
//
// HistoryCompactor 接口（options.go）+ truncateHistory（react.go）保留作 P0 兜底
// 截断扩展点——AutoCompact 失败 / 用户没启用任何 token-based 压缩时仍可用。
