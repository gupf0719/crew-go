package a2a_test

import (
	"context"
	"errors"
	"iter"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// 不变量：
//   - 远端停在 AUTH_REQUIRED 上时交给真人，凭证走带外通道：人没有拒绝就不发消息、重新挂上那个任务取后续（先轮询到
//     它离开 AUTH_REQUIRED；已经结束的取终态，没结束的订阅或接着轮询）；人拒绝时取消远端任务、报错。
//   - RemoteAgent 在节点执行里被调起时，提问不占调用它的那段代码的提问序号：调用方按进度跳过一次已完成的调用，后面
//     的提问不挪位。框架交过来的调用（RunSubAgent / AgentAsTool / 编排成员）按交接相位里的次序找回挂着的那一问，
//     跟输入内容在续跑之间变没变无关。
//   - 记在调用位置上的「等真人答」记录只在远端真的还在等真人时留着：远端转成交给宿主模型的普通追问时清掉，人的
//     批准不会被再发给远端、当成那个追问的答复。

// authRemote 是停在 AUTH_REQUIRED 上、等带外凭证的假远端：新任务先要授权，granted 关上（凭证到了）之后产出一段
// 内容再完成；续任务（收到消息）时记下并完成。done 在任务完成时关上。
type authRemote struct {
	mu       sync.Mutex
	got      []string
	granted  chan struct{}
	done     chan struct{}
	doneOnce sync.Once
	cancels  atomic.Int32
}

func newAuthRemote() *authRemote {
	return &authRemote{granted: make(chan struct{}), done: make(chan struct{})}
}

func (r *authRemote) record(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, s)
}

func (r *authRemote) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.got...)
}

func (r *authRemote) Execute(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2atypes.Event, error] {
	return func(yield func(a2atypes.Event, error) bool) {
		txt := inputText(execCtx)
		if execCtx.StoredTask != nil {
			r.record("cont:" + txt)
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted,
				a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("续:"+txt))), nil)
			return
		}
		r.record("new:" + txt)
		if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
			return
		}
		if !yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateAuthRequired,
			a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("请到 https://auth.example/x 授权"))), nil) {
			return
		}
		select {
		case <-r.granted:
		case <-ctx.Done():
			return
		}
		if !yield(a2atypes.NewArtifactEvent(execCtx, a2atypes.NewTextPart("报表内容")), nil) {
			return
		}
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted, nil), nil)
		r.doneOnce.Do(func() { close(r.done) })
	}
}

func (r *authRemote) Cancel(_ context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2atypes.Event, error] {
	r.cancels.Add(1)
	return func(yield func(a2atypes.Event, error) bool) {
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCanceled, nil), nil)
	}
}

// authSeq 把默认模式的 RemoteAgent 放进 SequentialAgent，跑到它停在授权请求上为止。
func authSeq(t *testing.T, r *authRemote) (agent_v2.Agent, context.Context, *agent_v2.RunSession, *compose_v2.AskQuery) {
	t.Helper()
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, r))
	if err != nil {
		t.Fatal(err)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("auth_seq")).AddStep("remote", ra)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err = seq.Run(ctx, sess, schema.UserMessage("出报表"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || len(ie.AllQueries()) != 1 {
		t.Fatalf("前置：远端要授权时应交给真人一问，got err=%v", err)
	}
	return seq, ctx, sess, ie.AllQueries()[0]
}

func finalText(evs []*agent_v2.AgentEvent) string {
	if m := agent_v2.FinalMessage(evs); m != nil {
		return m.Content
	}
	return ""
}

// 人授权得慢：续跑时远端已经拿到凭证跑完了。重新挂上取它的终态，不发消息——发消息的话，对已经结束的任务服务端
// 报 invalid params（实测），跑完的结果拿不回来。答复用自然语言也一样（认不出的答复不当拒绝）。
func TestRemoteAgent_AuthRequired_ResumeAfterRemoteFinished_TakesResult(t *testing.T) {
	r := newAuthRemote()
	seq, ctx, sess, q := authSeq(t, r)
	if q.Reason != agent_v2.ApprovalInterruptReason || !slices.Equal(q.Options, []string{agent_v2.ApprovalOptionApprove, agent_v2.ApprovalOptionDeny}) {
		t.Errorf("授权请求应按审批交给真人（批准 / 拒绝），got reason=%q options=%q", q.Reason, q.Options)
	}
	close(r.granted)
	select {
	case <-r.done:
	case <-time.After(5 * time.Second):
		t.Fatal("前置：远端拿到凭证后应跑完")
	}
	evs, err := seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{q.InterruptID: schema.UserMessage("已授权")}))
	if err != nil || !strings.Contains(finalText(evs), "报表内容") {
		t.Fatalf("续跑应拿回远端跑完的结果，got final=%q err=%v", finalText(evs), err)
	}
	if want := []string{"new:出报表"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q——续跑不该给远端任务发消息", r.snapshot(), want)
	}
}

// 人答得快：续跑时远端还停在 AUTH_REQUIRED 上。订阅的第一个事件是任务当时的快照，越过它接着等，凭证到了之后的产出
// 照常拿到。曾经把人的答复作为消息发过去，执行器还在跑的服务端回「任务还在执行」（实测），什么也拿不到。
func TestRemoteAgent_AuthRequired_ResumeWhileRemoteWaiting_WaitsForIt(t *testing.T) {
	r := newAuthRemote()
	seq, ctx, sess, q := authSeq(t, r)
	time.AfterFunc(300*time.Millisecond, func() { close(r.granted) })
	evs, err := seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{q.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApprove)}))
	if err != nil || !strings.Contains(finalText(evs), "报表内容") {
		t.Fatalf("续跑应等到远端拿到凭证、拿回它的产出，got final=%q err=%v", finalText(evs), err)
	}
	if want := []string{"new:出报表"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q——续跑不该给远端任务发消息", r.snapshot(), want)
	}
}

// 人拒绝授权：取消远端任务、这次调用报错；不把拒绝作为消息发过去（执行器还在跑的服务端不收）。
func TestRemoteAgent_AuthRequired_Deny_CancelsRemoteTask(t *testing.T) {
	r := newAuthRemote()
	seq, ctx, sess, q := authSeq(t, r)
	_, err := seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{q.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionDeny)}))
	if err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("拒绝授权应让这次调用报错，got %v", err)
	}
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Errorf("拒绝之后不该再问一次，got %v", err)
	}
	if n := r.cancels.Load(); n != 1 {
		t.Errorf("远端任务应被取消一次，实际 %d 次", n)
	}
	if want := []string{"new:出报表"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q", r.snapshot(), want)
	}
}

// 不在任何节点执行里（顶层直接调）：句柄记在 session 上，拿同一个 session 带上人的答复再 Run 一次就重新挂上原来的
// 任务，答复不作为请求发出去。
func TestRemoteAgent_AuthRequired_TopLevel_SameSessionReattaches(t *testing.T) {
	r := newAuthRemote()
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, r))
	if err != nil {
		t.Fatal(err)
	}
	sess := agent_v2.NewRunSession()
	_, err = ra.Run(context.Background(), sess, schema.UserMessage("出报表"))
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("前置：远端要授权时应交给真人，got %v", err)
	}
	close(r.granted)
	evs, err := ra.Run(context.Background(), sess, schema.UserMessage("好了"))
	if err != nil || !strings.Contains(finalText(evs), "报表内容") {
		t.Fatalf("同一个 session 再 Run 应重新挂上原来的任务，got final=%q err=%v", finalText(evs), err)
	}
	if want := []string{"new:出报表"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q", r.snapshot(), want)
	}
}

// doneRemote 收到请求就完成。
type doneRemote struct{ calls atomic.Int32 }

func (r *doneRemote) Execute(_ context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2atypes.Event, error] {
	r.calls.Add(1)
	return func(yield func(a2atypes.Event, error) bool) {
		if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
			return
		}
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted,
			a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("查到了:"+inputText(execCtx)))), nil)
	}
}

func (r *doneRemote) Cancel(_ context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2atypes.Event, error] {
	return func(yield func(a2atypes.Event, error) bool) {
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCanceled, nil), nil)
	}
}

// driveByQuestion 跑宿主到收敛：按问题文本作答，答案累积传，续跑带原 session。
func driveByQuestion(t *testing.T, host agent_v2.Agent, input string, answerFor func(q string) string) (asked []string, final string, err error) {
	t.Helper()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	answers := map[string]*schema.Message{}
	evs, err := host.Run(ctx, sess, schema.UserMessage(input))
	for i := 0; i < 6; i++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			break
		}
		for _, q := range ie.AllQueries() {
			asked = append(asked, q.Question)
			answers[q.InterruptID] = schema.UserMessage(answerFor(q.Question))
		}
		evs, err = host.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	}
	return asked, finalText(evs), err
}

// 节点里直接调 RemoteAgent、远端直接完成：节点按框架推荐的写法记下进度（SaveStepResult），续跑时跳过这次调用，之后
// 依次问两问。曾经默认模式下每次调用都占调用方一个提问序号：跳过它之后第一问挪到它的位置上又问了一遍，第二问拿到
// 第一问的答案、从没问过（实测，`err == nil`）。
func TestRemoteAgent_InNodeDirectCall_SkippingItDoesNotShiftLaterAsks(t *testing.T) {
	r := &doneRemote{}
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, r))
	if err != nil {
		t.Fatal(err)
	}
	host := rsHost(t, func(ctx context.Context, _ string) (string, error) {
		if _, ok := compose_v2.GetStepResult[string](ctx, "remote_done"); !ok {
			evs, err := ra.Run(ctx, agent_v2.GetSession(ctx), schema.UserMessage("查询"))
			if err != nil {
				return "", err
			}
			if err := compose_v2.SaveStepResult(ctx, "remote_done", finalText(evs)); err != nil {
				return "", err
			}
		}
		a, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "QA:转多少钱?"})
		if err != nil {
			return "", err
		}
		b, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "QB:确认执行吗?"})
		if err != nil {
			return "", err
		}
		return "A=" + a.Content + " B=" + b.Content, nil
	})
	asked, final, err := driveByQuestion(t, host, "go", func(q string) string {
		if strings.HasPrefix(q, "QA") {
			return "100"
		}
		return "yes"
	})
	if err != nil || final != "A=100 B=yes" {
		t.Fatalf("got final=%q err=%v（问过 %q）", final, err, asked)
	}
	if want := []string{"QA:转多少钱?", "QB:确认执行吗?"}; !slices.Equal(asked, want) {
		t.Errorf("问过 %q，want %q", asked, want)
	}
	if n := r.calls.Load(); n != 1 {
		t.Errorf("远端应只被调一次，实际 %d 次", n)
	}
}

// 经 RunSubAgent 调起、带 call key 声明身份、输入在续跑之间变了：远端那一问按交接相位里的次序找回，续的是同一个
// 远端任务。RemoteAgent 要是对框架交过来的调用也按内容再分一层相位，变了的输入就找不回挂着的那一问——原始请求
// 作为新任务重发、再问一次审批。
func TestRemoteAgent_HandedOverWithCallKey_InputChangesAcrossReruns_ContinuesSameTask(t *testing.T) {
	r := &waitingRemote{stop: a2atypes.TaskStateInputRequired, approval: true}
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, r))
	if err != nil {
		t.Fatal(err)
	}
	var runs atomic.Int32
	host := rsHost(t, func(ctx context.Context, _ string) (string, error) {
		n := runs.Add(1)
		in := schema.UserMessage("出报表 #" + string(rune('0'+n)))
		evs, err := agent_v2.RunSubAgent(ctx, "remote", ra, in, agent_v2.WithCallKey("report"))
		if err != nil {
			return "", err
		}
		return finalText(evs), nil
	})
	asked, final, err := driveByQuestion(t, host, "go", func(string) string { return agent_v2.ApprovalOptionApprove })
	if err != nil || !strings.Contains(final, "已执行:approve") {
		t.Fatalf("got final=%q err=%v（问过 %q）", final, err, asked)
	}
	if want := []string{"new:出报表 #1", "cont:approve"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q——续跑应续原来的任务", r.snapshot(), want)
	}
	if len(asked) != 1 {
		t.Errorf("审批应只问一次，问过 %q", asked)
	}
}

// followUpRemote：新任务要审批，批准之后改问一个普通追问，答了追问之后完成。按任务记下收到的每条消息。
type followUpRemote struct {
	mu   sync.Mutex
	msgs map[string][]string
	seq  []string
}

func (r *followUpRemote) Execute(_ context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2atypes.Event, error] {
	return func(yield func(a2atypes.Event, error) bool) {
		txt := inputText(execCtx)
		r.mu.Lock()
		if r.msgs == nil {
			r.msgs = map[string][]string{}
		}
		task := string(execCtx.TaskID)
		r.msgs[task] = append(r.msgs[task], txt)
		n := len(r.msgs[task])
		r.seq = append(r.seq, txt)
		r.mu.Unlock()
		switch n {
		case 1:
			if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
				return
			}
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateInputRequired, a2atypes.NewMessage(a2atypes.MessageRoleAgent,
				a2atypes.NewTextPart("要删除 /data/x 吗"), a2a.PendingQuestionsPart([]a2a.PendingQuestion{{
					InterruptID: "q1", Question: "要删除 /data/x 吗", Approval: true,
					Options: []string{"approve", "approve_always", "deny"},
				}}))), nil)
		case 2:
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateInputRequired,
				a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("请补充收件人"))), nil)
		default:
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted,
				a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("已发给收件人:"+txt))), nil)
		}
	}
}

func (r *followUpRemote) Cancel(_ context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2atypes.Event, error] {
	return func(yield func(a2atypes.Event, error) bool) {
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCanceled, nil), nil)
	}
}

// 默认模式、节点里直接调：人批准之后远端改问一个普通追问（交给宿主模型），节点随后问了别的、续跑时整个节点重跑。
// 人的批准只能发给远端一次。曾经那条「等真人答」的记录在转成普通追问时没清，节点再跑时把批准又发给远端、被当成
// 那个追问的答复（实测，`err == nil`）。
func TestRemoteAgent_DefaultMode_ApprovalNotResentAsFollowUpAnswer(t *testing.T) {
	r := &followUpRemote{}
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, r))
	if err != nil {
		t.Fatal(err)
	}
	host := rsHost(t, func(ctx context.Context, _ string) (string, error) {
		evs, err := ra.Run(ctx, agent_v2.GetSession(ctx), schema.UserMessage("原始请求"))
		if err != nil {
			return "", err
		}
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续吗?"})
		if err != nil {
			return "", err
		}
		return finalText(evs) + " | " + ans.Content, nil
	})
	asked, _, err := driveByQuestion(t, host, "go", func(q string) string {
		if strings.Contains(q, "删除") {
			return agent_v2.ApprovalOptionApprove
		}
		return "ok"
	})
	if err != nil {
		t.Fatalf("宿主：%v（问过 %q）", err, asked)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.msgs) != 1 {
		t.Fatalf("应只有一个远端任务，got %v", r.msgs)
	}
	for task, msgs := range r.msgs {
		approvals := 0
		for _, m := range msgs {
			if m == agent_v2.ApprovalOptionApprove {
				approvals++
			}
		}
		if approvals != 1 {
			t.Errorf("任务 %s 收到 %q：人的批准应只发一次", task, msgs)
		}
	}
}

// 引擎钩子里没有提问位置：遇到授权请求同审批，终局错误 + 取消远端任务（errors.Is 判得出）。
func TestRemoteAgent_AuthRequiredInEngineHook_AbortsAndCancels(t *testing.T) {
	r := newAuthRemote()
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, r))
	if err != nil {
		t.Fatal(err)
	}
	var hookErr error
	hooks := gateStartHooks{fn: func(ctx context.Context, nodeID string) error {
		if nodeID != "n" {
			return nil
		}
		_, hookErr = ra.Run(ctx, agent_v2.GetSession(ctx), schema.UserMessage("req-hook"))
		return hookErr
	}}
	host := rsHost(t, func(context.Context, string) (string, error) { return "body-ok", nil })
	ctx := agent_v2.WithCtxOrchHooks(compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer()), hooks)
	_, err = host.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
	if !errors.Is(hookErr, compose_v2.ErrRunAborted) {
		t.Fatalf("钩子里远端要授权时应返回终局错误，got %v", hookErr)
	}
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("宿主应以终局错误结束，got %v", err)
	}
	if n := r.cancels.Load(); n != 1 {
		t.Errorf("远端任务应被取消一次，实际 %d 次", n)
	}
}
