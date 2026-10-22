package agent_v2

import "crew-go/schema"

// IsFrameworkNote 报 m 是不是框架以 user 角色追加进对话流的提示——它不是用户说的话。三类：后台任务的结束通知
// （[IsBackgroundTaskNotification]）、工具重复调用提醒（[RepeatToolReminderAnnotation]）、inline skill 正文
// （[WithInlineSkillBody] 下盖着 [SkillBodyAnnotation] 的那条 user 消息）。
//
// 它们都排在用户这一轮的输入之后（通知在调模型之前追加，另两类跟一批工具结果一起），按角色倒着找「用户这一轮的输入」
// 会挑到它们。框架里这样挑的两处都跳过它：AutoCompact 定尾部（挑到了会把用户刚问的话连同这一轮的工具往返摘进摘要）、
// [WithToolFilter] 收到的「本轮最后一条真实消息」（挑到了业务按框架的提示裁工具）。业务自己扫消息挑用户的输入时同样
// 要跳过；框架合成的 assistant 记账文本另见 [IsUnresolvedCallsRecord]。
func IsFrameworkNote(m *schema.Message) bool {
	if m == nil || m.Role != schema.RoleUser {
		return false
	}
	if IsBackgroundTaskNotification(m) {
		return true
	}
	if reminder, _ := m.Annotations[RepeatToolReminderAnnotation].(bool); reminder {
		return true
	}
	body, _ := m.Annotations[SkillBodyAnnotation].(bool)
	return body
}
