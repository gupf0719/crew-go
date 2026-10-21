package server

import (
	"fmt"
	"strings"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
)

// 续发答复的校验：工具审批的答复认不出时不交给审批层（那里认不出就按拒绝处理，`Yes.`、只有 DataPart 的
// 结构化答复曾都被当成拒绝），任务继续停在 input-required、请对端重答；挂着多问时只接受按 interruptId 的
// 答复（一条文本灌给全部提问的话，一句 approve 就批准了全部工具）；同一问的两种写法不一致、答复形状不对，
// 也请重答。约定见 a2a 包的 pending.go。

// approvalAnswerHint 是工具审批答复能接受的写法，写进给对端的消息里。
const approvalAnswerHint = `reply "approve", "approve_always", "approve_always_tool" or "deny", or send a DataPart {"approved": true|false}`

// isApproval 判断这一问是不是工具审批。
func isApproval(q *compose_v2.AskQuery) bool {
	return q != nil && q.Reason == agent_v2.ApprovalInterruptReason
}

// pendingQuestions 把挂起提问转成发给对端的清单。
func pendingQuestions(qs []*compose_v2.AskQuery) []a2a.PendingQuestion {
	out := make([]a2a.PendingQuestion, 0, len(qs))
	for _, q := range qs {
		if q == nil {
			continue
		}
		out = append(out, a2a.PendingQuestion{
			InterruptID: q.InterruptID,
			Question:    q.Question,
			Options:     q.Options,
			Approval:    isApproval(q),
			ToolCallID:  q.ToolCallID,
		})
	}
	return out
}

// pendingMessage 构造 input-required 的状态消息：给人看的文本 + 挂起清单 DataPart。note 非空时放在文本最前面
// （再问一次时说明为什么）。
func pendingMessage(qs []*compose_v2.AskQuery, note string) *a2atypes.Message {
	text := joinQuestions(qs)
	if note != "" {
		text = note + "\n" + text
	}
	return a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart(text),
		a2a.PendingQuestionsPart(pendingQuestions(qs)))
}

// joinQuestions 把挂起提问合并为一条 input-required 文本。只挂一问时是问题本身（工具审批另附能接受的答复）；
// 挂多问时编号列出，每一问带上它的 interruptId，末尾说明怎么按 interruptId 作答。
func joinQuestions(qs []*compose_v2.AskQuery) string {
	if len(qs) == 1 {
		if isApproval(qs[0]) {
			return qs[0].Question + "\n(" + approvalAnswerHint + ")"
		}
		return qs[0].Question
	}
	var sb strings.Builder
	for i, q := range qs {
		fmt.Fprintf(&sb, "%d. %s (interruptId: %s)\n", i+1, q.Question, q.InterruptID)
	}
	sb.WriteString(`Answer each question with its own DataPart {"interruptId": "<id>", "answer": "<text>"}; ` +
		`tool approvals take {"interruptId": "<id>", "approved": true|false} or one of "approve", "approve_always", "approve_always_tool", "deny" as the answer.`)
	return sb.String()
}

// resumeInputs 按挂起提问解析续发消息里的答复，返回这条消息答了哪几问（interruptId → 答复）；executor 把它并进
// 这个任务里已经答过的那些，整份交给 Resume。answered 是这个任务里已经答过的提问，只用来把「重发了答过的」跟
// 「认不出的 ID」分开说。答复不可用时返回 nil 和请对端重答的原因（不调 Resume）。
func resumeInputs(qs []*compose_v2.AskQuery, answered map[string]*schema.Message, msg *schema.Message) (map[string]*schema.Message, string) {
	byID, bare, problem := a2a.AnswersFromMessage(msg)
	if problem != "" {
		return nil, "The answer was not understood: " + problem + "."
	}
	text := ""
	if msg != nil {
		text = strings.TrimSpace(msg.Content)
	}
	pending := make(map[string]*compose_v2.AskQuery, len(qs))
	for _, q := range qs {
		if q != nil {
			pending[q.InterruptID] = q
		}
	}
	inputs := make(map[string]*schema.Message, len(qs))
	if len(byID) > 0 {
		if len(bare) > 0 {
			return nil, "Some answers carry an interruptId and some do not; give every answer its interruptId."
		}
		for _, a := range byID {
			q, ok := pending[a.InterruptID]
			if !ok {
				if _, done := answered[a.InterruptID]; done {
					return nil, fmt.Sprintf("interruptId %q was already answered; answer only the questions still waiting.", a.InterruptID)
				}
				return nil, fmt.Sprintf("interruptId %q is not one of the questions waiting for an answer.", clip(a.InterruptID))
			}
			if _, dup := inputs[a.InterruptID]; dup {
				return nil, fmt.Sprintf("interruptId %q was answered more than once.", a.InterruptID)
			}
			ans, reason := answerFor(q, a)
			if ans == nil {
				return nil, reason
			}
			inputs[a.InterruptID] = ans
		}
		// 只挂一问时文本归这一问：跟 DataPart 一起给时得是同一个答复（同下面不带 interruptId 的那条路）。挂多问时
		// 文本归不到哪一问，不看。
		if len(qs) == 1 && text != "" {
			if ans, ok := inputs[qs[0].InterruptID]; ok && !sameAnswer(qs[0], text, ans) {
				return nil, fmt.Sprintf("The text %q and the DataPart give different answers; send one answer.", clip(text))
			}
		}
		return inputs, ""
	}
	if len(qs) > 1 {
		return nil, "Several questions are waiting; a single reply cannot answer them all. Answer each one with its own DataPart."
	}
	if len(qs) == 0 {
		return inputs, ""
	}
	q := qs[0]
	if len(bare) > 1 {
		return nil, "Several answers were given for one question; send one."
	}
	if len(bare) == 1 {
		// 一个不带 interruptId 的答复 DataPart 答这唯一的一问；同时还有文本时，文本得是同一个答复。
		ans, reason := answerFor(q, bare[0])
		if ans == nil {
			return nil, reason
		}
		if text != "" && !sameAnswer(q, text, ans) {
			return nil, fmt.Sprintf("The text %q and the DataPart give different answers; send one answer.", clip(text))
		}
		inputs[q.InterruptID] = ans
		return inputs, ""
	}
	if !isApproval(q) {
		inputs[q.InterruptID] = msg
		return inputs, ""
	}
	option, ok := agent_v2.NormalizeApprovalAnswer(text)
	if !ok {
		return nil, fmt.Sprintf("The answer %q was not recognized for a tool approval.", clip(text))
	}
	inputs[q.InterruptID] = schema.UserMessage(option)
	return inputs, ""
}

// answerFor 把一个答复 DataPart 转成交给 Resume 的消息；不可用时返回 nil 和原因。审批的 approved 与 answer 都给了
// 时两者得是同一个选项。
func answerFor(q *compose_v2.AskQuery, a a2a.Answer) (*schema.Message, string) {
	if !isApproval(q) {
		if a.Approved != nil || !a.HasText {
			return nil, fmt.Sprintf("Question %q takes a text answer: {\"interruptId\": %q, \"answer\": \"<text>\"}.", q.InterruptID, q.InterruptID)
		}
		return schema.UserMessage(a.Text), ""
	}
	option, ok := "", false
	if a.HasText {
		if option, ok = agent_v2.NormalizeApprovalAnswer(a.Text); !ok {
			return nil, fmt.Sprintf("The answer %q to %q was not recognized for a tool approval.", clip(a.Text), q.InterruptID)
		}
	}
	if a.Approved != nil {
		fromBool := approvalOptionFromBool(*a.Approved)
		if ok && option != fromBool {
			return nil, fmt.Sprintf("The answer to %q says both %q and approved=%t; send one answer.", q.InterruptID, clip(a.Text), *a.Approved)
		}
		option = fromBool
	}
	return schema.UserMessage(option), ""
}

// sameAnswer 判断跟 DataPart 一起给的文本跟 DataPart 解析出的答复是不是同一个。审批：文本是固定词之一时按选项比；
// 不是固定词的文本不是答复（给人看的附注，A2A 客户端常这么带），不算冲突。其余提问：任何文本都可能是答复，按原文比。
func sameAnswer(q *compose_v2.AskQuery, text string, ans *schema.Message) bool {
	if !isApproval(q) {
		return text == strings.TrimSpace(ans.Content)
	}
	option, ok := agent_v2.NormalizeApprovalAnswer(text)
	return !ok || option == ans.Content
}

// clip 截短写进请重答消息里的对端原文：答复可以任意长，原样回显的话整段又进了状态消息（task store 里存着它）。
func clip(s string) string {
	const max = 80
	n := 0
	for i := range s {
		if n == max {
			return s[:i] + "…"
		}
		n++
	}
	return s
}

func approvalOptionFromBool(approved bool) string {
	if approved {
		return agent_v2.ApprovalOptionApprove
	}
	return agent_v2.ApprovalOptionDeny
}
