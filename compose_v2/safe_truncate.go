package compose_v2

import "crew-go/schema"

// 配对感知的切片调整（ISSUE-65）
//
// 上下文压缩 / 历史 truncate 都需要切消息列表。但裸 slice 索引会切到 tool_call
// 与 tool_result 之间，导致 OpenAI 422（"tool_use ids ... did not have ...
// tool_result blocks"）或 Anthropic 类似错误。
//
// 本 helper 把 caller 期望的切点 start 向前扩展到一个"安全"位置——保证
// [start, end] 范围内 tool_use ↔ tool_result 配对完整、同 RunID 的 assistant
// 块连续。
//
// **设计参考 Claude Code adjustIndexToPreserveAPIInvariants**（按 message.id +
// tool_use_id），适配 schema.Message 用 RunID 替代 message.id。

// AdjustIndexToPreserveAPIInvariants 调整 start 切点保 API 不变量。
//
// 入参：
//   - messages：完整消息列表
//   - start：caller 想保留的起点（保留 [start:len]）
//
// 出参：调整后的 start 索引（≤ 原 start，可能向前扩展以维持不变量）。
//
// **算法**：
//
//	步骤 1：修复 tool_use / tool_result 配对
//	  1a. 收集 [start, end] 内所有 tool_result 的 tool_use_id
//	  1b. 收集 [start, end] 内 assistant 已携带的 tool_use id
//	  1c. 找出 missing = result 用到但 use 不在范围内
//	  1d. 向前扫描 [0, start)，找含 missing tool_use 的 assistant 消息，
//	      把 start 拉回到最早一条
//
//	步骤 2：修复 RunID 块连续性
//	  2a. 收集 [start, end] 内所有 assistant 消息的 RunID
//	  2b. 向前扫描 [0, start)，把 start 拉回到同 RunID 的最早一条
//
// **边界情况**：
//   - start <= 0：返 0（保留全部）
//   - start >= len(messages)：返原值（空切片）
//   - 没有不变量需修复：返原 start
//
// 与 v1 compose.AdjustIndexToPreserveAPIInvariants 行为完全等价。
func AdjustIndexToPreserveAPIInvariants(messages []*schema.Message, start int) int {
	if start <= 0 {
		return 0
	}
	if start >= len(messages) {
		return start
	}

	// 步骤 1：tool_use / tool_result 配对
	resultIDs := map[string]bool{}
	useIDs := map[string]bool{}
	for _, m := range messages[start:] {
		if m == nil {
			continue
		}
		switch m.Role {
		case schema.RoleAssistant:
			for _, tc := range schema.GetToolCalls(m) {
				if tc.ID != "" {
					useIDs[tc.ID] = true
				}
			}
		case schema.RoleTool:
			if id := schema.GetToolResultID(m); id != "" {
				resultIDs[id] = true
			}
		}
	}

	missing := map[string]bool{}
	for id := range resultIDs {
		if !useIDs[id] {
			missing[id] = true
		}
	}

	// 1d：向前扫描找最早含 missing 的 assistant 消息
	if len(missing) > 0 {
		for i := start - 1; i >= 0; i-- {
			m := messages[i]
			if m == nil || m.Role != schema.RoleAssistant {
				continue
			}
			hit := false
			for _, tc := range schema.GetToolCalls(m) {
				if missing[tc.ID] {
					hit = true
					delete(missing, tc.ID)
				}
			}
			if hit {
				start = i
			}
			if len(missing) == 0 {
				break
			}
		}
	}

	// 步骤 2：RunID 块连续性
	runIDs := map[string]bool{}
	for _, m := range messages[start:] {
		if m == nil || m.Role != schema.RoleAssistant {
			continue
		}
		if id := getRunID(m); id != "" {
			runIDs[id] = true
		}
	}

	// 向前扫描把 start 拉回到同 RunID 的最早一条
	if len(runIDs) > 0 {
		for i := start - 1; i >= 0; i-- {
			m := messages[i]
			if m == nil || m.Role != schema.RoleAssistant {
				continue
			}
			id := getRunID(m)
			if id != "" && runIDs[id] {
				start = i
			}
		}
	}

	return start
}
