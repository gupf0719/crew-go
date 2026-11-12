package a2a

import (
	"context"
	"crypto/rand"
	"fmt"
	"maps"
	"strings"
	"sync"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

const (
	continuationAnnotation       = "a2a.continuation"
	answerContinuationAnnotation = "a2a.answer_continuation"
	inputRequiredPrefix          = "[input_required] "
)

// Continuation 返回默认模式追问的续接句柄。没有待答问题时返回空串。
func Continuation(output *schema.Message) string {
	if output == nil {
		return ""
	}
	token, _ := output.Annotations[continuationAnnotation].(string)
	return token
}

// WithContinuation 复制输入并标记它在回答哪次远端追问。用同一个 RemoteAgent 和 RunSession
// 调 Run，或在当前 run 中经 RunSubAgent 调用。句柄来自 [Continuation]，只能用于原 run。
func WithContinuation(input *schema.Message, token string) *schema.Message {
	if input == nil {
		input = schema.UserMessage("")
	}
	answer := *input
	answer.Annotations = maps.Clone(input.Annotations)
	if answer.Annotations == nil {
		answer.Annotations = make(map[string]any)
	}
	answer.Annotations[answerContinuationAnnotation] = token
	return &answer
}

// AgentToolInputSchema 为默认的 AgentAsTool 包装声明任务输入和可选的续接参数。
func (a *RemoteAgent) AgentToolInputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"input": map[string]any{
				"type":        "string",
				"description": "The task to delegate or the answer to the remote agent's question.",
			},
			"continuation": map[string]any{
				"type":        "string",
				"minLength":   1,
				"description": "Copy the continuation from the tool result for the question you are answering. Omit it for a new task.",
			},
		},
		"required": []string{"input"},
	}
}

// AgentToolInput 把工具参数转成 RemoteAgent 的输入。续接句柄作为本地标注传递。
func (a *RemoteAgent) AgentToolInput(args map[string]any) (*schema.Message, error) {
	input, _ := args["input"].(string)
	msg := schema.UserMessage(input)
	if v, ok := args["continuation"]; ok {
		token, ok := v.(string)
		if !ok || token == "" {
			return nil, fmt.Errorf("a2a: continuation must be a non-empty string")
		}
		msg = WithContinuation(msg, token)
	}
	return msg, nil
}

// AgentToolOutput 把续接句柄附在工具结果里，供宿主模型在下一次调用时回传。
func (a *RemoteAgent) AgentToolOutput(output *schema.Message) string {
	token := Continuation(output)
	if token == "" {
		return output.GetContent()
	}
	return fmt.Sprintf("%scontinuation=%s\n%s\n(Call this tool with the answer in input and copy the continuation unchanged.)",
		inputRequiredPrefix, token, strings.TrimPrefix(output.Content, inputRequiredPrefix))
}

// RestoreNestedOutput 恢复宿主重放的远端追问所引用的会话句柄。
func (a *RemoteAgent) RestoreNestedOutput(ctx context.Context, session *agent_v2.RunSession, input, output *schema.Message) {
	token := Continuation(output)
	if token == "" || !compose_v2.WithinNodeScope(ctx) {
		return
	}
	var answerToken string
	if input != nil {
		answerToken, _ = input.Annotations[answerContinuationAnnotation].(string)
	}
	ctx = compose_v2.EnterPhase(ctx, a.callPhase(ctx, input, answerToken))
	pos := compose_v2.CurrentInterruptSite(ctx).InterruptID()
	question, _ := compose_v2.GetStepResult[gatePending](ctx, gateStepKey(pos))
	if question.TaskID != "" && Continuation(question.FollowUp) == token {
		a.rememberContinuation(session, token, question)
	}
}

// 每个 run 的挂起问题存于 session KV。子 session 共享 KV，因而后续工具调用能找到句柄。
type continuationSet struct {
	mu           sync.Mutex
	pending      map[string]*remoteContinuation
	topLevelBusy map[string]struct{}
}

type remoteContinuation struct {
	client   *a2aclient.Client
	question gatePending
	inUse    bool
}

var continuationSetInitMu sync.Mutex

// 顶层真人问题通过会话 KV 接续，与共享该 KV 的 lane 共用同一个执行许可。
func (a *RemoteAgent) claimTopLevelCall(session *agent_v2.RunSession, continuation string) (func(), error) {
	set := getContinuationSet(session, true)
	set.mu.Lock()
	defer set.mu.Unlock()
	if _, busy := set.topLevelBusy[a.name]; busy {
		return nil, fmt.Errorf("a2a: a top-level call is already in progress for this remote agent")
	}
	if pending, _ := a.loadIDs(session); continuation != "" && pending != "" {
		return nil, fmt.Errorf("a2a: answer the pending human question before continuing another remote task")
	}
	set.topLevelBusy[a.name] = struct{}{}
	return func() {
		set.mu.Lock()
		defer set.mu.Unlock()
		delete(set.topLevelBusy, a.name)
	}, nil
}

func getContinuationSet(session *agent_v2.RunSession, create bool) *continuationSet {
	key := "a2a:continuations:" + session.RunID
	v, _ := session.GetValue(key)
	set, _ := v.(*continuationSet)
	if set != nil || !create {
		return set
	}
	continuationSetInitMu.Lock()
	defer continuationSetInitMu.Unlock()
	v, _ = session.GetValue(key)
	if set, _ = v.(*continuationSet); set != nil {
		return set
	}
	set = &continuationSet{
		pending:      make(map[string]*remoteContinuation),
		topLevelBusy: make(map[string]struct{}),
	}
	session.SetValue(key, set)
	return set
}

// questionChanged 判断 GetTask 是否看到了新的追问。即使远端没返回消息历史，已换题的
// 答复也先走快照，授权或审批则交给宿主的真人路径。
func questionChanged(question gatePending, task *a2atypes.Task) bool {
	if task == nil || !waitsForInput(task.Status.State) {
		return false
	}
	if task.Status.State == a2atypes.TaskStateAuthRequired {
		return true
	}
	msg := task.Status.Message
	if msg == nil {
		return false
	}
	text := a2aPartsText(msg.Parts)
	return (question.QuestionMsgID != "" && msg.ID != "" && msg.ID != question.QuestionMsgID) ||
		(strings.TrimSpace(text) != "" && text != question.Question) ||
		anyApproval(PendingQuestionsFromParts(msg.Parts))
}

func (a *RemoteAgent) askForContinuation(session *agent_v2.RunSession, question gatePending) *schema.Message {
	token := rand.Text()
	a.rememberContinuation(session, token, question)

	text := fmt.Sprintf("%sRemote agent %q asks: %s", inputRequiredPrefix, a.card.Name, question.Question)
	if len(question.Items) > 1 {
		text += "\n(" + jsonAnswerHint + ")"
	}
	out := schema.AssistantMessage(text)
	out.Annotations = map[string]any{continuationAnnotation: token}
	return out
}

func (a *RemoteAgent) rememberContinuation(session *agent_v2.RunSession, token string, question gatePending) {
	if question.Binding == "" || question.Binding != a.binding {
		return
	}
	set := getContinuationSet(session, true)
	set.mu.Lock()
	defer set.mu.Unlock()
	if _, known := set.pending[token]; !known {
		question.FollowUp = nil
		set.pending[token] = &remoteContinuation{client: a.client, question: question}
	}
}

// beginContinuation 独占一问，避免同一答复并发发送。网络失败时释放占用以便再次对账。
func (a *RemoteAgent) beginContinuation(session *agent_v2.RunSession, token string) (gatePending, func(consumed, attempted bool), error) {
	set := getContinuationSet(session, false)
	if set == nil {
		return gatePending{}, nil, fmt.Errorf("a2a: continuation is not pending for this remote agent in the current run")
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	p := set.pending[token]
	if p == nil || p.client != a.client {
		return gatePending{}, nil, fmt.Errorf("a2a: continuation is not pending for this remote agent in the current run")
	}
	if p.inUse {
		return gatePending{}, nil, fmt.Errorf("a2a: continuation already has an answer in progress")
	}
	p.inUse = true
	return p.question, func(consumed, attempted bool) {
		set.mu.Lock()
		defer set.mu.Unlock()
		if consumed {
			set.pending[token] = nil // 同一 session 的宿主存档重放不得复活已答的问题。
		} else {
			p.question.AnswerUnsent = p.question.AnswerUnsent && !attempted
			p.inUse = false
		}
	}, nil
}
