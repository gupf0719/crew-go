package a2a_test

import (
	"context"
	"errors"
	"iter"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
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

// 第二轮审计的几条：
//   - 在别人给的相位里（框架交接给包着 RemoteAgent 的业务壳）不按请求内容分：交过来的输入在续跑之间变了照样续上。
//   - 顶层（句柄在 session 上）重新挂上的那次失败了（超时、网络错误），授权等待的标记还在，下一次照样重新挂上。
//   - 远端不支持流式时轮询任务状态，不订阅。
//   - 节点里直接调用时请求每次都变的话，把拼好的请求记下来就收敛（godoc 教的做法）。

// wrapAgent 是业务自研的壳：原样转调 RemoteAgent（日志、计量、鉴权之类的装饰器）。
type wrapAgent struct{ inner agent_v2.Agent }

func (w *wrapAgent) Name() string        { return "wrap" }
func (w *wrapAgent) Description() string { return "wraps a remote agent" }
func (w *wrapAgent) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	ctx, sess, err := agent_v2.EnterRun(ctx, sess, w.Name())
	if err != nil {
		return nil, err
	}
	return w.inner.Run(ctx, sess, in)
}
func (w *wrapAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, errors.New("wrapAgent: standalone Resume not supported")
}

// 框架交接的是包着 RemoteAgent 的业务壳（RunSubAgent 带 call key），交过来的输入每次重跑都变：远端那一问按交接相位
// 里的次序找回，续的是同一个远端任务、审批只问一次。曾经 RemoteAgent 只认「交给的是不是我」，交给壳时又按内容分了
// 一层相位，变了的输入找不回挂着的那一问——原始请求作为新任务一遍遍重发（审计实测 6 次）。
func TestRemoteAgent_WrapperHandedOverWithCallKey_InputChanges_ContinuesSameTask(t *testing.T) {
	r := &waitingRemote{stop: a2atypes.TaskStateInputRequired, approval: true}
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, r))
	if err != nil {
		t.Fatal(err)
	}
	w := &wrapAgent{inner: ra}
	var runs atomic.Int32
	host := rsHost(t, func(ctx context.Context, _ string) (string, error) {
		in := schema.UserMessage("出报表 #" + strconv.Itoa(int(runs.Add(1))))
		evs, err := agent_v2.RunSubAgent(ctx, "wrap", w, in, agent_v2.WithCallKey("report"))
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

// 顶层直接调、远端停在授权上：人答了之后那次重新挂上被调用方的超时打断（远端还没拿到凭证），授权等待的标记还在——
// 凭证到了之后同一个 session 再 Run 一次照样重新挂上、拿到结果，不把人的答复当成消息发过去。曾经在重新挂上之前就清
// 了标记，第三次 Run 把「好了」发给已经跑完的任务、服务端回 invalid params（审计实测），之后每一次都这样。
func TestRemoteAgent_AuthRequired_TopLevel_ReattachInterrupted_ThenContinues(t *testing.T) {
	r := newAuthRemote()
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, r))
	if err != nil {
		t.Fatal(err)
	}
	sess := agent_v2.NewRunSession()
	if _, err := ra.Run(context.Background(), sess, schema.UserMessage("出报表")); err == nil {
		t.Fatal("前置：远端要授权时应交给真人")
	} else if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("前置：远端要授权时应交给真人，got %v", err)
	}
	tctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	_, err = ra.Run(tctx, sess, schema.UserMessage("好了"))
	cancel()
	if err == nil {
		t.Fatal("前置：远端还没拿到凭证、调用方 300ms 超时，这一次应报错")
	}
	close(r.granted)
	select {
	case <-r.done:
	case <-time.After(5 * time.Second):
		t.Fatal("前置：远端拿到凭证后应跑完")
	}
	evs, err := ra.Run(context.Background(), sess, schema.UserMessage("好了"))
	if err != nil || !strings.Contains(finalText(evs), "报表内容") {
		t.Fatalf("凭证到了之后同一个 session 再 Run 应重新挂上、拿到结果，got final=%q err=%v", finalText(evs), err)
	}
	if want := []string{"new:出报表"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q——人的答复不该作为消息发过去", r.snapshot(), want)
	}
}

// bgAuthHandler 是不支持流式的假远端（形状同 Python SDK 的服务端：AUTH_REQUIRED 时立刻把任务返回、授权在后台完成）：
// SendMessage 返回停在 AUTH_REQUIRED 上的任务，advance 把它推进到别的状态。只实现用得到的方法，订阅被调到时记一笔
// 并报不支持。
type bgAuthHandler struct {
	a2asrv.RequestHandler // 没实现的方法不该被调到
	mu                    sync.Mutex
	task                  *a2atypes.Task
	sends, subscribes     atomic.Int32
}

func (h *bgAuthHandler) SendMessage(_ context.Context, _ *a2atypes.SendMessageRequest) (a2atypes.SendMessageResult, error) {
	h.sends.Add(1)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.task = &a2atypes.Task{
		ID:        a2atypes.NewTaskID(),
		ContextID: a2atypes.NewContextID(),
		Status: a2atypes.TaskStatus{State: a2atypes.TaskStateAuthRequired,
			Message: a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("请到 https://auth.example/x 授权"))},
	}
	snapshot := *h.task
	return &snapshot, nil
}

func (h *bgAuthHandler) GetTask(_ context.Context, req *a2atypes.GetTaskRequest) (*a2atypes.Task, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.task == nil || h.task.ID != req.ID {
		return nil, a2atypes.ErrTaskNotFound
	}
	snapshot := *h.task
	return &snapshot, nil
}

func (h *bgAuthHandler) SubscribeToTask(context.Context, *a2atypes.SubscribeToTaskRequest) iter.Seq2[a2atypes.Event, error] {
	h.subscribes.Add(1)
	return func(yield func(a2atypes.Event, error) bool) {
		yield(nil, a2atypes.ErrUnsupportedOperation)
	}
}

// advance 把任务推到 st；content 非空时带上一段产出。
func (h *bgAuthHandler) advance(st a2atypes.TaskState, content string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	next := *h.task
	next.Status = a2atypes.TaskStatus{State: st}
	if content != "" {
		next.Artifacts = []*a2atypes.Artifact{{ID: a2atypes.NewArtifactID(), Parts: a2atypes.ContentParts{a2atypes.NewTextPart(content)}}}
	}
	h.task = &next
}

func startNonStreamingServer(t *testing.T, h a2asrv.RequestHandler) string {
	t.Helper()
	mux := http.NewServeMux()
	srv := httptest.NewUnstartedServer(mux)
	baseURL := "http://" + srv.Listener.Addr().String()
	card := &a2atypes.AgentCard{
		Name:        "Test Agent",
		Description: "non-streaming test fixture",
		SupportedInterfaces: []*a2atypes.AgentInterface{
			a2atypes.NewAgentInterface(baseURL+"/invoke", a2atypes.TransportProtocolJSONRPC),
		},
		DefaultInputModes:  []string{"text"},
		DefaultOutputModes: []string{"text"},
		Capabilities:       a2atypes.AgentCapabilities{Streaming: false},
	}
	mux.Handle("/invoke", a2asrv.NewJSONRPCHandler(h))
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	srv.Start()
	t.Cleanup(srv.Close)
	return baseURL
}

// 远端不支持流式：重新挂上时轮询任务状态，离开 AUTH_REQUIRED 之后接着轮询到它结束，不订阅（订阅不了）。
func TestRemoteAgent_AuthRequired_NonStreamingRemote_Polls(t *testing.T) {
	h := &bgAuthHandler{}
	ra, err := a2a.NewRemoteAgent(context.Background(), startNonStreamingServer(t, h))
	if err != nil {
		t.Fatal(err)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("auth_seq")).AddStep("remote", ra)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err = seq.Run(ctx, sess, schema.UserMessage("出报表"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || len(ie.AllQueries()) != 1 {
		t.Fatalf("前置：远端要授权时应交给真人一问，got %v", err)
	}
	time.AfterFunc(200*time.Millisecond, func() { h.advance(a2atypes.TaskStateWorking, "") })
	time.AfterFunc(700*time.Millisecond, func() { h.advance(a2atypes.TaskStateCompleted, "报表内容") })
	evs, err := seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("已授权")}))
	if err != nil || !strings.Contains(finalText(evs), "报表内容") {
		t.Fatalf("续跑应轮询到远端跑完、拿回它的产出，got final=%q err=%v", finalText(evs), err)
	}
	if n := h.subscribes.Load(); n != 0 {
		t.Errorf("远端不支持流式时不该订阅，订阅了 %d 次", n)
	}
	if n := h.sends.Load(); n != 1 {
		t.Errorf("远端应只收到一条消息（原始请求），实际 %d 条", n)
	}
}

// 节点里直接调用、请求带每次都变的值：把拼好的请求记下来（SaveStepResult），续跑时请求不变，远端那一问找得回、续的是
// 同一个任务（godoc 里教的做法）。
func TestRemoteAgent_InNodeDirectCall_RememberedRequest_Converges(t *testing.T) {
	r := &waitingRemote{stop: a2atypes.TaskStateInputRequired, approval: true}
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, r))
	if err != nil {
		t.Fatal(err)
	}
	var builds atomic.Int32
	host := rsHost(t, func(ctx context.Context, _ string) (string, error) {
		req, ok := compose_v2.GetStepResult[string](ctx, "request")
		if !ok {
			req = "出报表 #" + strconv.Itoa(int(builds.Add(1))) // 每次重新拼都不同（请求号、时间戳、现调模型拼的）
			if err := compose_v2.SaveStepResult(ctx, "request", req); err != nil {
				return "", err
			}
		}
		evs, err := ra.Run(ctx, agent_v2.GetSession(ctx), schema.UserMessage(req))
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
		t.Errorf("远端收到 %q，want %q", r.snapshot(), want)
	}
}
