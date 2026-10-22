package a2a

import (
	"context"
	"iter"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// graceAuthExec 停在 AUTH_REQUIRED 上等 granted 关上，然后产出一段内容、完成；续任务（收到消息）时记下。
type graceAuthExec struct {
	granted chan struct{}
	mu      sync.Mutex
	conts   []string
}

func (e *graceAuthExec) Execute(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2atypes.Event, error] {
	return func(yield func(a2atypes.Event, error) bool) {
		if execCtx.StoredTask != nil {
			e.mu.Lock()
			e.conts = append(e.conts, execCtx.Message.Parts[0].Text())
			e.mu.Unlock()
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted, nil), nil)
			return
		}
		if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
			return
		}
		if !yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateAuthRequired,
			a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("请到 https://auth.example/x 授权"))), nil) {
			return
		}
		select {
		case <-e.granted:
		case <-ctx.Done():
			return
		}
		if !yield(a2atypes.NewArtifactEvent(execCtx, a2atypes.NewTextPart("报表内容")), nil) {
			return
		}
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted, nil), nil)
	}
}

func (e *graceAuthExec) Cancel(_ context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2atypes.Event, error] {
	return func(yield func(a2atypes.Event, error) bool) {
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCanceled, nil), nil)
	}
}

// 人说已经授权、远端却一直停在 AUTH_REQUIRED 上：等过 authReattachGrace 就交出任务、再问一次人（新的一问），不一直挂到
// 调用方超时；凭证到了之后再答一次照常拿到结果。全程不给远端任务发消息。
func TestReattach_StillAuthRequiredAfterGrace_AsksAgain(t *testing.T) {
	oldGrace, oldPoll := authReattachGrace, authPollInterval
	authReattachGrace, authPollInterval = 300*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { authReattachGrace, authPollInterval = oldGrace, oldPoll })

	exec := &graceAuthExec{granted: make(chan struct{})}
	mux := http.NewServeMux()
	srv := httptest.NewUnstartedServer(mux)
	baseURL := "http://" + srv.Listener.Addr().String()
	card := &a2atypes.AgentCard{
		Name: "Test Agent", Description: "grace fixture",
		SupportedInterfaces: []*a2atypes.AgentInterface{a2atypes.NewAgentInterface(baseURL+"/invoke", a2atypes.TransportProtocolJSONRPC)},
		DefaultInputModes:   []string{"text"}, DefaultOutputModes: []string{"text"},
		Capabilities: a2atypes.AgentCapabilities{Streaming: true},
	}
	mux.Handle("/invoke", a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(exec)))
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	srv.Start()
	t.Cleanup(srv.Close)

	ra, err := NewRemoteAgent(context.Background(), baseURL)
	if err != nil {
		t.Fatal(err)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("grace_seq")).AddStep("remote", ra)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err = seq.Run(ctx, sess, schema.UserMessage("出报表"))
	first, ok := compose_v2.IsInterrupt(err)
	if !ok || len(first.AllQueries()) != 1 {
		t.Fatalf("前置：远端要授权时应交给真人一问，got %v", err)
	}
	answers := map[string]*schema.Message{first.AllQueries()[0].InterruptID: schema.UserMessage("已授权")}

	start := time.Now()
	_, err = seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	second, ok := compose_v2.IsInterrupt(err)
	if !ok || len(second.AllQueries()) != 1 {
		t.Fatalf("远端过了宽限仍停在 AUTH_REQUIRED 上时应再问一次人，got %v", err)
	}
	if waited := time.Since(start); waited < authReattachGrace {
		t.Errorf("应等过宽限再问，只等了 %v", waited)
	}
	q2 := second.AllQueries()[0]
	if q2.InterruptID == first.AllQueries()[0].InterruptID || !strings.Contains(q2.Question, "授权") {
		t.Errorf("再问的应是新的一问（新 ID）、问的仍是授权，got id=%q question=%q", q2.InterruptID, q2.Question)
	}

	close(exec.granted)
	answers[q2.InterruptID] = schema.UserMessage("已授权")
	evs, err := seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	if err != nil {
		t.Fatalf("凭证到了之后再答一次应拿到结果，got %v", err)
	}
	if m := agent_v2.FinalMessage(evs); m == nil || !strings.Contains(m.Content, "报表内容") {
		t.Errorf("final = %v，want 含「报表内容」", m)
	}
	exec.mu.Lock()
	defer exec.mu.Unlock()
	if len(exec.conts) != 0 {
		t.Errorf("远端任务不该收到消息，收到 %q", exec.conts)
	}
}
