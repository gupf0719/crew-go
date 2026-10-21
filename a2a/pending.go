package a2a

import (
	"encoding/json"
	"fmt"

	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
)

// 远端挂起多个提问时按 interruptId 分别作答的约定（crew-go 自己的服务端与客户端共用）。
//
// 服务端停在 input-required 时，状态消息里除了给人看的文本，另带一个 DataPart：
//
//	{"crew.pending": [{"interruptId": "...", "question": "...", "options": [...], "approval": true, "toolCallId": "..."}]}
//
// 对端续发时每一问一个 DataPart：
//
//	{"interruptId": "...", "approved": true}   // 工具审批
//	{"interruptId": "...", "answer": "..."}    // 其余提问；审批也可以给选项字符串
//
// 只挂着一问时，照旧接受一条文本（工具审批时文本只认固定的几个词，见 agent_v2.NormalizeApprovalAnswer），也接受
// 一个不带 interruptId 的答复 DataPart（{"answer": "..."} 或 {"approved": bool}）。挂着多问时只接受按 interruptId
// 的答复：一条文本当成全部提问的答案的话，一句 approve 就批准了全部工具。
//
// 一问的答复同时用了两种写法时两者必须是同一个答复，否则请重答（审批不能在两个说法里挑一个执行）：DataPart 里
// approved 与 answer 都给时 answer 必须是同一个选项；只挂一问时文本与答复 DataPart（带不带 interruptId 都算）一起给，
// 审批的文本是固定词之一时必须是同一个选项、不是固定词的文本当附注，其余提问的文本必须与 answer 相同。挂多问时文本
// 归不到哪一问，不看。形状不对的答复 DataPart（answer 不是字符串之类）同样请重答。
//
// 挂着多问时可以只答其中几问：没答的继续挂着，下一次 input-required 只列还在等的；服务端记着已经答过的，对端
// 不必重发（重发已经答过的会被请重答）。

// PendingQuestionsKey 是 input-required 状态消息里挂起清单那个 DataPart 的键。
const PendingQuestionsKey = "crew.pending"

// PendingQuestion 是远端挂起的一问。
type PendingQuestion struct {
	InterruptID string   `json:"interruptId"`
	Question    string   `json:"question"`
	Options     []string `json:"options,omitempty"`
	// Approval 为 true 表示这一问是工具审批：答复给 {"approved": bool}，或四个选项之一。
	Approval   bool   `json:"approval,omitempty"`
	ToolCallID string `json:"toolCallId,omitempty"`
}

// 答复 DataPart 的键。
const (
	answerInterruptIDKey = "interruptId"
	answerApprovedKey    = "approved"
	answerTextKey        = "answer"
)

// PendingQuestionsPart 把挂起清单做成一个 DataPart。
func PendingQuestionsPart(qs []PendingQuestion) *a2atypes.Part {
	items := make([]any, 0, len(qs))
	for _, q := range qs {
		m := map[string]any{"interruptId": q.InterruptID, "question": q.Question}
		if len(q.Options) > 0 {
			opts := make([]any, 0, len(q.Options))
			for _, o := range q.Options {
				opts = append(opts, o)
			}
			m["options"] = opts
		}
		if q.Approval {
			m["approval"] = true
		}
		if q.ToolCallID != "" {
			m["toolCallId"] = q.ToolCallID
		}
		items = append(items, m)
	}
	p := a2atypes.NewDataPart(map[string]any{PendingQuestionsKey: items})
	p.MediaType = "application/json"
	return p
}

// PendingQuestionsFromParts 从一条消息的 parts 里取出挂起清单；没有这个 DataPart 时返回 nil。
func PendingQuestionsFromParts(parts a2atypes.ContentParts) []PendingQuestion {
	for _, p := range parts {
		if p == nil {
			continue
		}
		m, ok := p.Data().(map[string]any)
		if !ok {
			continue
		}
		raw, ok := m[PendingQuestionsKey]
		if !ok {
			continue
		}
		// 经 JSON 往返取出：Data 可能是解码出的 map / []any，也可能是本进程里直接放进去的值
		b, err := json.Marshal(raw)
		if err != nil {
			return nil
		}
		var qs []PendingQuestion
		if err := json.Unmarshal(b, &qs); err != nil {
			return nil
		}
		return qs
	}
	return nil
}

// AnswerPart 构造一条按 interruptId 作答的 DataPart：approved 非 nil 时是审批答复，否则是文本答复。
func AnswerPart(interruptID string, approved *bool, text string) *a2atypes.Part {
	m := map[string]any{answerInterruptIDKey: interruptID}
	if approved != nil {
		m[answerApprovedKey] = *approved
	} else {
		m[answerTextKey] = text
	}
	p := a2atypes.NewDataPart(m)
	p.MediaType = "application/json"
	return p
}

// Answer 是对端按 interruptId 给出的一条答复（从续发消息里解析出来）。
type Answer struct {
	InterruptID string
	// Approved 非 nil 表示给的是 {"approved": bool}。
	Approved *bool
	// Text 是 "answer" 字段的文本（Approved 为 nil 时用）。
	Text string
	// HasText 表示给了 "answer" 字段且是字符串。
	HasText bool
}

// AnswersFromMessage 取出续发消息里的答复 DataPart：带 interruptId 的进 byID，不带的进 bare（只挂一问时用）。
// interruptId / answer / approved 三个键一个都没有的 DataPart 不是答复，忽略；带了其中一个却形状不对（interruptId
// 不是非空字符串、answer 不是字符串、approved 不是布尔、answer 与 approved 都没有）时 problem 说明哪里不对。对端
// 是想作答的，忽略掉的话那一问拿到的是空答案（曾经 {"answer": 42} 让提问收到 ""，任务照常做完）。
func AnswersFromMessage(msg *schema.Message) (byID []Answer, bare []Answer, problem string) {
	if msg == nil {
		return nil, nil, ""
	}
	for _, cb := range msg.ContentBlocks {
		if cb == nil || cb.Data == nil {
			continue
		}
		m, ok := cb.Data.Data.(map[string]any)
		if !ok {
			continue
		}
		rawID, hasID := m[answerInterruptIDKey]
		rawApproved, hasApproved := m[answerApprovedKey]
		rawText, hasText := m[answerTextKey]
		if !hasID && !hasApproved && !hasText {
			continue
		}
		var a Answer
		if hasID {
			id, ok := rawID.(string)
			if !ok || id == "" {
				return nil, nil, `"interruptId" must be a non-empty string`
			}
			a.InterruptID = id
		}
		if hasApproved {
			v, ok := rawApproved.(bool)
			if !ok {
				return nil, nil, fmt.Sprintf(`"approved" must be true or false%s`, forInterrupt(a.InterruptID))
			}
			a.Approved = &v
		}
		if hasText {
			v, ok := rawText.(string)
			if !ok {
				return nil, nil, fmt.Sprintf(`"answer" must be a string%s`, forInterrupt(a.InterruptID))
			}
			a.Text, a.HasText = v, true
		}
		if a.Approved == nil && !a.HasText {
			return nil, nil, fmt.Sprintf(`an answer needs "answer" or "approved"%s`, forInterrupt(a.InterruptID))
		}
		if a.InterruptID != "" {
			byID = append(byID, a)
		} else {
			bare = append(bare, a)
		}
	}
	return byID, bare, ""
}

// forInterrupt 给答复格式错误的说明补上是哪一问（ID 是对端给的，过长时截短）。
func forInterrupt(id string) string {
	if id == "" {
		return ""
	}
	n := 0
	for i := range id {
		if n == 80 {
			id = id[:i] + "…"
			break
		}
		n++
	}
	return fmt.Sprintf(" (interruptId %q)", id)
}
