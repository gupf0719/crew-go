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

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// 不变量：远端的审批请求（挂起清单里标了审批的问题、任务状态 AUTH_REQUIRED）不管开没开 WithHumanGate 都交给真人，
// 宿主模型不能替人批准远端的操作；WithHumanGate 只决定普通追问交给谁。接不住中断的位置（引擎钩子、条件边）遇到
// 审批请求时返回终局错误，并取消远端那个没人会去续的任务。
//
// 曾经默认模式（没开 WithHumanGate）把审批问句当普通追问交给宿主模型、提示它「再调一次补信息」，宿主模型回一个
// yes，远端的审批层就按批准处理、工具执行了（端到端探针实测）；AUTH_REQUIRED 被当成「流意外结束」，已收到的内容
// 当成功答复返回，远端任务一直挂着。

// waitingRemote 是进程内假远端：新任务停在 stop 状态上要人（approval 为真时挂起清单里标成审批），续任务时记下收到
// 的答复并完成。cancels 数远端收到的取消请求。
type waitingRemote struct {
	stop     a2atypes.TaskState
	approval bool
	mu       sync.Mutex
	got      []string
	cancels  atomic.Int32
}

func (r *waitingRemote) record(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, s)
}

func (r *waitingRemote) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.got...)
}

func (r *waitingRemote) Execute(_ context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2atypes.Event, error] {
	return func(yield func(a2atypes.Event, error) bool) {
		txt := inputText(execCtx)
		if execCtx.StoredTask != nil {
			r.record("cont:" + txt)
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted,
				a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("已执行:"+txt))), nil)
			return
		}
		r.record("new:" + txt)
		if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
			return
		}
		parts := []*a2atypes.Part{a2atypes.NewTextPart("要删除 /data/x 吗")}
		if r.approval {
			parts = append(parts, a2a.PendingQuestionsPart([]a2a.PendingQuestion{{
				InterruptID: "q1", Question: "要删除 /data/x 吗", Approval: true,
				Options: []string{"approve", "approve_always", "deny"},
			}}))
		}
		yield(a2atypes.NewStatusUpdateEvent(execCtx, r.stop, a2atypes.NewMessage(a2atypes.MessageRoleAgent, parts...)), nil)
	}
}

func (r *waitingRemote) Cancel(_ context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2atypes.Event, error] {
	r.cancels.Add(1)
	return func(yield func(a2atypes.Event, error) bool) {
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCanceled, nil), nil)
	}
}

// runDefaultMode 把没开 WithHumanGate 的 RemoteAgent 放进 SequentialAgent 跑一次；停在提问上就答 answer 续跑一次。
func runDefaultMode(t *testing.T, r *waitingRemote, answer string) (asked []string, final string, err error) {
	t.Helper()
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, r))
	if err != nil {
		t.Fatal(err)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("approval_seq")).AddStep("remote", ra)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	evs, err := seq.Run(ctx, sess, schema.UserMessage("原始请求"))
	if ie, ok := compose_v2.IsInterrupt(err); ok {
		answers := map[string]*schema.Message{}
		for _, q := range ie.AllQueries() {
			asked = append(asked, q.Question)
			answers[q.InterruptID] = schema.UserMessage(answer)
		}
		evs, err = seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	}
	if m := agent_v2.FinalMessage(evs); m != nil {
		final = m.Content
	}
	return asked, final, err
}

// 默认模式下远端的工具审批交给真人：抛宿主中断，人答了才续远端任务。
func TestRemoteAgent_DefaultMode_ApprovalAskedOfHuman(t *testing.T) {
	r := &waitingRemote{stop: a2atypes.TaskStateInputRequired, approval: true}
	asked, final, err := runDefaultMode(t, r, "approve")
	if err != nil {
		t.Fatalf("应收敛：%v（远端收到 %q）", err, r.snapshot())
	}
	if len(asked) != 1 || !strings.Contains(asked[0], "删除") {
		t.Fatalf("远端的审批请求应作为中断交给真人，实际问过 %q（final=%q）——审批被交给了宿主模型", asked, final)
	}
	if want := []string{"new:原始请求", "cont:approve"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q", r.snapshot(), want)
	}
	if final != "已执行:approve" {
		t.Errorf("final = %q，want 已执行:approve", final)
	}
}

// 默认模式下普通追问照旧交给宿主模型（当输出返回），不升级成中断：WithHumanGate 只管它。
func TestRemoteAgent_DefaultMode_OrdinaryQuestionStillToModel(t *testing.T) {
	r := &waitingRemote{stop: a2atypes.TaskStateInputRequired}
	asked, final, err := runDefaultMode(t, r, "unused")
	if err != nil {
		t.Fatalf("普通追问不该报错：%v", err)
	}
	if len(asked) != 0 {
		t.Fatalf("普通追问在默认模式下不该交给真人，实际问过 %q", asked)
	}
	if !strings.Contains(final, "[input_required]") {
		t.Errorf("普通追问应作为输出交给宿主模型，final = %q", final)
	}
}

// AUTH_REQUIRED 交给真人，不当成功答复。曾经当成「流意外结束」，返回已收到的内容当成功答复，远端任务一直挂着。
//
// 宿主收到挂起态就停止读流、不等服务端关流：a2a-go 的 a2asrv 到了 AUTH_REQUIRED 不关流、等着带外的凭证，等它关流会
// 一直等到客户端超时。续跑那半（重新挂上、拒绝时取消）见 remote_gate_fixes_test.go。
func TestRemoteAgent_AuthRequired_AskedOfHuman(t *testing.T) {
	r := &waitingRemote{stop: a2atypes.TaskStateAuthRequired}
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, r))
	if err != nil {
		t.Fatal(err)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("auth_seq")).AddStep("remote", ra)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	evs, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原始请求"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		final := ""
		if m := agent_v2.FinalMessage(evs); m != nil {
			final = m.Content
		}
		t.Fatalf("AUTH_REQUIRED 应作为中断交给真人，got err=%v final=%q", err, final)
	}
	if qs := ie.AllQueries(); len(qs) != 1 || !strings.Contains(qs[0].Question, "删除") {
		t.Errorf("交给真人的应是远端的那句话，got %+v", qs)
	}
	if want := []string{"new:原始请求"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q", r.snapshot(), want)
	}
}

// 引擎钩子里（没有提问位置）遇到远端的审批请求：返回终局错误，取消远端那个没人会去续的任务；不交给宿主模型。
func TestRemoteAgent_ApprovalInEngineHook_AbortsAndCancelsRemote(t *testing.T) {
	r := &waitingRemote{stop: a2atypes.TaskStateInputRequired, approval: true}
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
	if _, isInt := compose_v2.IsInterrupt(hookErr); hookErr == nil || isInt || !errors.Is(hookErr, compose_v2.ErrRunAborted) {
		t.Fatalf("钩子里远端要审批时应返回终局错误（不是交给模型、也不是接不住的中断），got %v", hookErr)
	}
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("宿主应以终局错误结束，got %v", err)
	}
	if n := r.cancels.Load(); n != 1 {
		t.Errorf("远端那个在等人的任务应被取消一次，实际 %d 次", n)
	}
}

// 默认模式下节点里的普通追问：补信息再调一次，续的是同一个远端任务。节点里的提问位置一律取之后，默认模式照旧靠
// session 上的句柄续跑——换成 gate 模式那样不用句柄的话，补的信息会被当成新任务的请求发出去、原始请求丢了。
func TestRemoteAgent_DefaultMode_InNodeFollowUpContinuesSameTask(t *testing.T) {
	r := &waitingRemote{stop: a2atypes.TaskStateInputRequired}
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, r))
	if err != nil {
		t.Fatal(err)
	}
	host := rsHost(t, func(ctx context.Context, _ string) (string, error) {
		sess := agent_v2.GetSession(ctx)
		first, err := ra.Run(ctx, sess, schema.UserMessage("原始请求"))
		if err != nil {
			return "", err
		}
		if m := agent_v2.FinalMessage(first); m == nil || !strings.Contains(m.Content, "[input_required]") {
			return "", errors.New("前置：第一次调用应返回远端的追问")
		}
		second, err := ra.Run(ctx, sess, schema.UserMessage("补充信息"))
		if err != nil {
			return "", err
		}
		return agent_v2.FinalMessage(second).Content, nil
	})
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	if _, err := host.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("宿主：%v", err)
	}
	if want := []string{"new:原始请求", "cont:补充信息"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q——补的信息没续上原来的任务", r.snapshot(), want)
	}
}
