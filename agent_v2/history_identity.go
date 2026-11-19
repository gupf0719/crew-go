package agent_v2

import "crew-go/schema"

// HistoryInputMessageIDAnnotation 记录宿主当前用户输入的线上消息 ID。
// HistoryStore 仍用消息 ID 和任务章区分每次出现。
const HistoryInputMessageIDAnnotation = "crew_history_input_message_id"

const hostedHistoryTaskAnnotation = "crew_history_hosted_task"

// HistoryToolReference 关联历史中的原始工具调用 ID 和框架分配的调用实例。
type HistoryToolReference struct {
	CallID     string `json:"callId"`
	InstanceID string `json:"instanceId,omitempty"`
}

// HistoryToolReferences 返回历史消息的工具调用引用。助手消息按声明顺序返回，
// 工具结果返回对应的一个引用。旧记录可能没有调用实例。
func HistoryToolReferences(msg *schema.Message) []HistoryToolReference {
	if msg == nil {
		return nil
	}
	if msg.Role == schema.RoleTool {
		id := schema.GetToolResultID(msg)
		if id == "" {
			return nil
		}
		instance, _ := msg.Annotations[ToolCallInstanceIDExtraKey].(string)
		return []HistoryToolReference{{CallID: id, InstanceID: instance}}
	}
	if msg.Role != schema.RoleAssistant {
		return nil
	}
	calls := schema.GetToolCalls(msg)
	if len(calls) == 0 {
		return nil
	}
	batchID, _ := msg.Annotations[toolCallBatchIDAnnotation].(string)
	batch := toolBatch{batchID: batchID}
	out := make([]HistoryToolReference, len(calls))
	for i, call := range calls {
		out[i] = HistoryToolReference{CallID: call.ID, InstanceID: batch.callInstanceID(i)}
	}
	return out
}

// HistoryTurnAt 根据历史中的一个消息位置找到该轮最早的记录和用户输入。
// 调用方可复制最早位置之前的记录来另开分支。返回的输入属于传入的快照。
// 旧记录按任务章找最早的用户输入，带宿主消息 ID 时优先使用宿主输入。
func HistoryTurnAt(messages []*schema.Message, at int) (start int, input *schema.Message, ok bool) {
	if at < 0 || at >= len(messages) || messages[at] == nil {
		return 0, nil, false
	}
	target := messages[at]
	task, _ := target.Annotations[HistoryTaskAnnotation].(string)
	if task == "" {
		if target.Role != schema.RoleUser {
			return 0, nil, false
		}
		for i, m := range messages[:at] {
			if m != nil && m.Role == schema.RoleUser && m.Annotations[HistoryTaskAnnotation] == nil && sameTurnInput(m, target) {
				return i, m, true
			}
		}
		return at, target, true
	}

	start, hosted, legacy := at, -1, -1
	var hostedTask, legacyAmbiguous bool
	for i, msg := range messages {
		if msg == nil || !historyRecordOfTask(msg, task) {
			continue
		}
		start = min(start, i)
		if hosted, _ := msg.Annotations[hostedHistoryTaskAnnotation].(bool); hosted {
			hostedTask = true
		}
		if ownerTask, _ := msg.Annotations[HistoryTaskAnnotation].(string); ownerTask != task || msg.Role != schema.RoleUser {
			continue
		}
		if id, _ := msg.Annotations[HistoryInputMessageIDAnnotation].(string); id != "" {
			if hosted < 0 {
				hosted = i
			} else if !sameTurnInput(msg, messages[hosted]) {
				return 0, nil, false
			}
		} else if legacy < 0 {
			legacy = i
		} else if !sameTurnInput(msg, messages[legacy]) {
			legacyAmbiguous = true
		}
	}
	user := hosted
	if user < 0 && !hostedTask && !legacyAmbiguous && legacy <= at {
		user = legacy
	}
	if user < 0 || (target.Role == schema.RoleUser && user != at && !sameTurnInput(target, messages[user])) {
		return 0, nil, false
	}
	return start, messages[user], true
}

// 同一任务可能从多个归属记下同一个共享用户输入。
func sameTurnInput(a, b *schema.Message) bool {
	return a.ID != "" && a.ID == b.ID && inputContentKey(a) == inputContentKey(b)
}
