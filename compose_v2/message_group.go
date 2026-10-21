package compose_v2

import "crew-go/schema"

// 消息分组（ISSUE-65）
//
// 上下文压缩 4 层都需要把消息按"API 轮次"分组——同一 LLM 调用产生的所有消息
// （含流式分块、tool_use、tool_result）应在同一组。删消息时按组删保 thinking +
// tool_call/tool_result 配对完整性，不会切到中间触发 OpenAI 422。
//
// 设计参考 Claude Code 的 groupMessagesByApiRound（按 message.id 分组），但适配
// 我们 schema.Message 没有原生 message.id 的情况——用 Annotations["run_id"]
// （ISSUE-49 / ISSUE-63 已用 RunID 标识 LLM 调用）作分组 key。
//
// 分组规则：
//  1. 含 RunID 的 assistant 消息开始新一组
//  2. 同 RunID 的后续 assistant 块（streaming chunk / tool_use / thinking）在同组
//  3. assistant 后跟随的 tool 角色消息（tool_result）在同组——它们是该轮 LLM
//     输出的 tool_call 的响应
//  4. 下一个 user 消息开始新一组
//  5. RunID 缺失的消息按 role 边界分组（user 边界开始新组）

// RunIDAnnotationKey 是 [schema.Message] 的 `Annotations` 里 RunID 的固定 key。
//
// **导出是因为它有跨包消费者**：agent_v2 盖这个戳（Stream 聚合 / Generate 两条路）、
// agui 按它分流前端的 TextMessage、记忆指纹按它排除（那条尤其要紧——`run_id` 每次 Run
// 都不同，混进指纹就等于随机键，Resume 重跑必然多写一条，而且**完全静默**）。
//
// ⚠️ **它只管 annotation key，不管事件的 wire 字段名。** `compose_v2.Event.Metadata["run_id"]`
// 和 agui 发给前端的那个 JSON 字段跟它**恰好同名，但是另一个契约**——后者跟前端的读取点
// 配对，改了要一起改。用这个常量去表达那个，等于把两个能独立演进的东西焊死。
//
// 这个区分是**实测出来的**：导出常量那次我按字面量批量替换，把 4 处 `Event.Metadata` 一起
// 换了，逐处读宿主才发现。机械对账哨兵 `TestRunIDKey_LiteralCopiesAreAccounted` 里记着
// 怎么分。
//
// ⚠️ 它一度是未导出 const，注释写着「与 agent_v2 的 … 保持一致」——按本仓规约那是**设计债
// 的自白**：一处判据在多处出现、靠人记着同步。annotation 侧现已全部收敛到这一份。
//
// ⚠️ **「收敛完了」这句话我说错过一次**：当时把 agui 的读取点整批归成 wire，实际其中一处
// 读的是 `Message.Annotations`、跟 agent_v2 的写入点配对。变异测试（把常量值改掉、字面量
// 不动）当场让 agui 4 条测试红——前端文本重复一遍、流式那条 message 永不收 End。
// 教训：**分类靠逐处读宿主，不靠「这个包大概是干什么的」。**
const RunIDAnnotationKey = "run_id"

// runIDKey 是包内别名，保留是为了不动本文件既有的读取点。
const runIDKey = RunIDAnnotationKey

// GroupMessagesByAPIRound 把消息按 API 轮次分组——同一 LLM 调用产生的所有
// 消息（含 streaming chunk、tool_use、后续 tool_result）在同一组。
//
// 用于：删最旧消息组（ReactiveCompact）、配对完整性检查（safe_truncate 辅助）、
// 估算单轮 tokens 等场景。
//
// 算法：
//  1. 遍历 messages
//  2. 当出现新的 user 消息时，开始新一组
//  3. 当出现 assistant 消息：
//     - 若有 RunID 且与当前组的 RunID 不同 → 开始新组
//     - 否则加入当前组
//  4. tool 消息加入当前组（它们是当前轮 LLM 输出的 tool_call 的响应）
//
// 边界情况：
//   - 空输入返回空切片
//   - 单条消息单独成组
func GroupMessagesByAPIRound(messages []*schema.Message) [][]*schema.Message {
	if len(messages) == 0 {
		return nil
	}

	var groups [][]*schema.Message
	var current []*schema.Message
	var currentRunID string // 当前组的 RunID（assistant 消息携带，tool/user 不带）

	for _, msg := range messages {
		if msg == nil {
			continue
		}

		startNewGroup := false
		switch msg.Role {
		case schema.RoleUser:
			// 新 user 消息总是开启新一组——它代表用户新一轮交互
			if len(current) > 0 {
				startNewGroup = true
			}
		case schema.RoleAssistant:
			// 含 RunID 且与当前组不同 → 新组
			runID := getRunID(msg)
			if runID != "" && currentRunID != "" && runID != currentRunID {
				startNewGroup = true
			}
		case schema.RoleSystem:
			// system 只**开**新组——它还得**关**上，见下面 prevWasSystem 那段。
			if len(current) > 0 {
				startNewGroup = true
			}
		case schema.RoleTool:
			// tool_result 跟随当前组（响应当前组 assistant 的 tool_call）
			// 不开新组
		}

		// **上一条是 system ⟹ 这一条无论什么 role 都开新组** —— 让 system 真的「单独成组」。
		//
		// ⚠️ 这一段是补上的：上面那个 case 只让 system **开**新组、没让它**关**，而
		// `RoleAssistant` 只在 RunID 变化时才开新组 ⟹ 「system 紧跟一条无 RunID 的 assistant」
		// 会混成同一组。原注释写的是「system 消息总是单独成组」——**那句一直是假的**。
		//
		// 为什么要它真成立：压缩层按「组」决定丢不丢，而含 system 的组受保护。混组意味着
		// **一条 system 会把整轮 assistant + 全部 tool_result 一起变成永久不可压缩**。实测代价：
		// `[user][system, 30KB assistant][user]` 下 `shouldTrigger` 照样 true（那 30KB 算进阈值）
		// 但整组压不掉 ⟹ 摘要跑完**入 30016 字节、出 30059 字节，反而变大**，白烧一次 LLM，
		// 下一轮状态没变再触发——正是 `shouldTrigger` godoc 列的「反复无效触发 + 断路器跳」。
		//
		// **配对校验本身不看 system**：那套不变量只看 `RoleAssistant` 的 tool_calls 和
		// `RoleTool` 的 result id（见 `safe_truncate.go`），`RoleSystem` 一个字都不参与。
		// ⚠️ **但这不代表「在 system 旁边切组」是安全的**：`[user][assistant tc][system][tool]`
		// 会被切成**三组** `[user assistant]` + `[system]` + `[tool]`（system 既开新组也关新组，
		// 所以 tool_result 自己单独一组），丢掉第一组就留下孤儿 tool_result。
		// 第 3 层紧急压缩丢完会再数一遍配对（`dropOrphanToolResults`），AutoCompact 那条路
		// 还没有——见 `doc/code/todo.md` 同名条目。框架自己不会在工具轮中间插 system，
		// 业务在 hook 里插才会遇到。
		if len(current) > 0 && current[len(current)-1].Role == schema.RoleSystem {
			startNewGroup = true
		}

		if startNewGroup {
			groups = append(groups, current)
			current = nil
			currentRunID = ""
		}

		current = append(current, msg)
		// 更新当前组 RunID（仅 assistant 携带）
		if msg.Role == schema.RoleAssistant {
			if id := getRunID(msg); id != "" {
				currentRunID = id
			}
		}
	}

	if len(current) > 0 {
		groups = append(groups, current)
	}
	return groups
}

// getRunID 从消息 Annotations 提取 run_id；不存在返回空串。
func getRunID(msg *schema.Message) string {
	if msg == nil || msg.Annotations == nil {
		return ""
	}
	v, ok := msg.Annotations[runIDKey].(string)
	if !ok {
		return ""
	}
	return v
}

// FlattenGroups 把分组后的消息列表展平回单一切片（保持组内顺序 + 组间顺序）。
// ReactiveCompact 删完最旧组后用此函数重组。
func FlattenGroups(groups [][]*schema.Message) []*schema.Message {
	if len(groups) == 0 {
		return nil
	}
	total := 0
	for _, g := range groups {
		total += len(g)
	}
	out := make([]*schema.Message, 0, total)
	for _, g := range groups {
		out = append(out, g...)
	}
	return out
}
