package a2a_test

// 续远端任务的请求远端收下了、办完了，只是响应在路上丢了（客户端收到 503）。不变量：
//   - 远端任务已经结束：再跑不再去续它（远端会拒），取回它的结果；
//   - 远端收下答复、又问了新的一问：问人新的那一问，不把旧答复再发一次（那会被当成新那一问的答复）。
// 曾经句柄与在等的那一问按设计留着、每次重试 / 续跑都去续那个已结束的任务，远端报 task in a terminal state，卡住（审计实测）。

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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

// startLostResponseServer 同 startFlakyContinuationServer，但第一次续任务的请求照常交给远端处理完，再给客户端回 503：
// 远端收下了、办了，响应在路上丢了。lost 报那一次有没有发生。
func startLostResponseServer(t *testing.T, exec a2asrv.AgentExecutor) (url string, lost *atomic.Bool) {
	t.Helper()
	return startLostResponseServerWith(t, exec, nil)
}

// startLostResponseServerWith 同上；received 非 nil 时那一次续任务的请求在后台交给远端、等 received 报远端收下了就回 503
// （远端还在办，响应丢了）。
func startLostResponseServerWith(t *testing.T, exec a2asrv.AgentExecutor, received func() bool) (url string, lost *atomic.Bool) {
	t.Helper()
	mux := http.NewServeMux()
	srv := httptest.NewUnstartedServer(nil)
	url = "http://" + srv.Listener.Addr().String()
	card := &a2atypes.AgentCard{
		Name:        "Lossy Agent",
		Description: "in-process test fixture",
		SupportedInterfaces: []*a2atypes.AgentInterface{
			a2atypes.NewAgentInterface(url+"/invoke", a2atypes.TransportProtocolJSONRPC),
		},
		DefaultInputModes:  []string{"text"},
		DefaultOutputModes: []string{"text"},
		Capabilities:       a2atypes.AgentCapabilities{Streaming: true},
	}
	mux.Handle("/invoke", a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(exec)))
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	lost = &atomic.Bool{}
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body) // 读失败时 body 为空，照常交给 mux
		req.Body = io.NopCloser(bytes.NewReader(body))
		if req.URL.Path == "/invoke" && bytes.Contains(body, []byte(`"taskId"`)) && lost.CompareAndSwap(false, true) {
			if received == nil {
				mux.ServeHTTP(httptest.NewRecorder(), req) // 远端照常处理完
			} else {
				go mux.ServeHTTP(httptest.NewRecorder(), req.Clone(context.Background())) // 远端在后台接着办
				for !received() {
					time.Sleep(5 * time.Millisecond)
				}
			}
			http.Error(w, "upstream unavailable", http.StatusServiceUnavailable)
			return
		}
		mux.ServeHTTP(w, req)
	})
	srv.Start()
	t.Cleanup(srv.Close)
	return url, lost
}

// 远端收下答复、办完了，响应丢了：重试 / 续跑取回那个已完成任务的结果，只问一次人、远端只收到一次答复。
func TestHumanGate_LostResponse_CompletedTaskResultRecovered(t *testing.T) {
	for _, withRetry := range []bool{true, false} {
		r := &gateRemote{}
		url, lost := startLostResponseServer(t, fnExec(r.exec))
		asked, _, final, err := gateRun(t, url, withRetry)
		if !lost.Load() {
			t.Fatal("fixture 没搭对：续任务的响应一次都没丢过")
		}
		if err != nil {
			t.Fatalf("withRetry=%v 没收敛：%v（远端收到 %q）", withRetry, err, r.snapshot())
		}
		if want := []string{"new:原始请求", "cont:答#1"}; !slices.Equal(r.snapshot(), want) {
			t.Errorf("withRetry=%v 远端收到 %q，want %q", withRetry, r.snapshot(), want)
		}
		if len(asked) != 1 || final != "续任务办了:答#1" {
			t.Errorf("withRetry=%v 应只问一次、取回已完成任务的结果：问过 %q，final=%q", withRetry, asked, final)
		}
	}
}

// 远端收下答复之后又问了新的一问，响应丢了：问人新的那一问（新 ID），答复续同一个任务；旧答复不再发一次。
func TestHumanGate_LostResponse_NewQuestionAskedNotOldAnswerResent(t *testing.T) {
	r := &gateRemote{onCont: func(n int32) a2atypes.TaskState {
		if n == 1 {
			return a2atypes.TaskStateInputRequired
		}
		return a2atypes.TaskStateCompleted
	}}
	url, lost := startLostResponseServer(t, fnExec(r.exec))
	asked, ids, final, err := gateRun(t, url, true)
	if !lost.Load() {
		t.Fatal("fixture 没搭对：续任务的响应一次都没丢过")
	}
	if err != nil {
		t.Fatalf("没收敛：%v（远端收到 %q）", err, r.snapshot())
	}
	if want := []string{"new:原始请求", "cont:答#1", "cont:答#2"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q——旧答复又发了一次、被当成新那一问的答复", r.snapshot(), want)
	}
	if len(asked) != 2 || ids[0] == ids[1] || asked[1] != "还要再确认一次吗" {
		t.Errorf("应接着问人新的那一问（新 ID）：问过 %q，ID %q", asked, ids)
	}
	if final != "续任务办了:答#2" {
		t.Errorf("final = %q，want 续任务办了:答#2", final)
	}
}

// 默认模式（追问交给宿主）：顶层第二次 Run 补充信息时响应丢了，第三次 Run 取回那个已完成任务的结果，不再去续它。
func TestRemoteAgent_LostResponse_SessionHandleRecoversResult(t *testing.T) {
	r := &gateRemote{}
	url, lost := startLostResponseServer(t, fnExec(r.exec))
	ra, err := a2a.NewRemoteAgent(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	sess := agent_v2.NewRunSession()
	if _, err := ra.Run(ctx, sess, schema.UserMessage("原始请求")); err != nil {
		t.Fatalf("第一次 Run 应返回追问：%v", err)
	}
	if _, err := ra.Run(ctx, sess, schema.UserMessage("补充")); err == nil || !lost.Load() {
		t.Fatalf("fixture 没搭对：第二次 Run 的响应应丢失并报错，实际 err=%v lost=%v", err, lost.Load())
	}
	evs, err := ra.Run(ctx, sess, schema.UserMessage("补充"))
	if err != nil {
		t.Fatalf("第三次 Run 应取回已完成任务的结果，实际 %v（远端收到 %q）", err, r.snapshot())
	}
	if got := agent_v2.FinalMessage(evs); got == nil || !strings.Contains(got.Content, "续任务办了:补充") {
		t.Fatalf("应取回已完成任务的结果，实际 %v", got)
	}
	if want := []string{"new:原始请求", "cont:补充"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q", r.snapshot(), want)
	}
}

// 续任务之前查状态时远端说没有这个任务了：清掉在等的那一问，之后再跑发原始请求（新任务）、人被重新问一次（新 ID），
// 答复续上新任务——同远端任务失败那条路。曾经每次都去续那个不存在的任务。
func TestHumanGate_TaskVanished_StartsNewTask(t *testing.T) {
	r := &gateRemote{}
	mux := http.NewServeMux()
	srv := httptest.NewUnstartedServer(nil)
	url := "http://" + srv.Listener.Addr().String()
	card := &a2atypes.AgentCard{
		Name: "Forgetful Agent", Description: "in-process test fixture",
		SupportedInterfaces: []*a2atypes.AgentInterface{a2atypes.NewAgentInterface(url+"/invoke", a2atypes.TransportProtocolJSONRPC)},
		DefaultInputModes:   []string{"text"}, DefaultOutputModes: []string{"text"},
		Capabilities: a2atypes.AgentCapabilities{Streaming: true},
	}
	mux.Handle("/invoke", a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(fnExec(r.exec))))
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	// 远端「丢了」第一个任务：问人那一刻查它的那一次照常放行（记下是哪个任务），之后再查它都报不存在
	var forgot atomic.Bool
	var mu sync.Mutex
	lostTask := ""
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body) // 读失败时 body 为空，照常交给 mux
		req.Body = io.NopCloser(bytes.NewReader(body))
		var rpc struct {
			ID     any    `json:"id"`
			Method string `json:"method"`
			Params struct {
				ID string `json:"id"`
			} `json:"params"`
		}
		lost := false
		if json.Unmarshal(body, &rpc) == nil && rpc.Method == "GetTask" {
			mu.Lock()
			if lostTask == "" {
				lostTask = rpc.Params.ID
			} else if rpc.Params.ID == lostTask {
				lost = true
			}
			mu.Unlock()
		}
		if lost {
			forgot.Store(true)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": rpc.ID,
				"error": map[string]any{"code": -32001, "message": "task not found"}})
			return
		}
		mux.ServeHTTP(w, req)
	})
	srv.Start()
	t.Cleanup(srv.Close)

	asked, ids, final, err := gateRun(t, url, true)
	if !forgot.Load() {
		t.Fatal("fixture 没搭对：续任务之前没有查过任务状态")
	}
	if err != nil {
		t.Fatalf("没收敛：%v（远端收到 %q）", err, r.snapshot())
	}
	if want := []string{"new:原始请求", "new:原始请求", "cont:答#2"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q", r.snapshot(), want)
	}
	if len(asked) != 2 || ids[0] == ids[1] || final != "续任务办了:答#2" {
		t.Errorf("应对新任务重新问一次（新 ID）、答复续上新任务：问过 %q，ID %q，final=%q", asked, ids, final)
	}
}

// 远端收下了答复、还在办（执行器发出结局之前任务仍停在原来那一问上），响应丢了：重试时不重发、也不把原来那一问当成新的一问
// 再问人，等远端回应后取回结果。
func TestHumanGate_LostResponse_AnswerInFlightNotReasked(t *testing.T) {
	r := &gateRemote{contDelay: 300 * time.Millisecond}
	url, lost := startLostResponseServerWith(t, fnExec(r.exec), func() bool { return len(r.snapshot()) >= 2 })
	asked, _, final, err := gateRun(t, url, true)
	if !lost.Load() {
		t.Fatal("fixture 没搭对：续任务的响应一次都没丢过")
	}
	if err != nil {
		t.Fatalf("没收敛：%v（远端收到 %q）", err, r.snapshot())
	}
	if want := []string{"new:原始请求", "cont:答#1"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q——答复重发了", r.snapshot(), want)
	}
	if len(asked) != 1 || final != "续任务办了:答#1" {
		t.Errorf("应只问一次、等远端回应后取回结果：问过 %q，final=%q", asked, final)
	}
}

// 远端收下了答复、已经切到 working 还在办，响应丢了：重试时不重发，重新挂上任务读到结果。
func TestHumanGate_LostResponse_WorkingTaskResubscribed(t *testing.T) {
	r := &gateRemote{contDelay: 300 * time.Millisecond, contWorking: true}
	url, lost := startLostResponseServerWith(t, fnExec(r.exec), r.working.Load)
	asked, _, final, err := gateRun(t, url, true)
	if !lost.Load() {
		t.Fatal("fixture 没搭对：续任务的响应一次都没丢过")
	}
	if err != nil {
		t.Fatalf("没收敛：%v（远端收到 %q）", err, r.snapshot())
	}
	if want := []string{"new:原始请求", "cont:答#1"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q——答复重发了", r.snapshot(), want)
	}
	if len(asked) != 1 || final != "续任务办了:答#1" {
		t.Errorf("应只问一次、重新挂上取回结果：问过 %q，final=%q", asked, final)
	}
}

// 答复在途、等远端回应的时候调用方超时：交出的错误认得出是调用方的 ctx（errors.Is DeadlineExceeded），不是「远端还没回应」。
func TestHumanGate_LostResponse_InFlightWaitHonorsCallerDeadline(t *testing.T) {
	r := &gateRemote{contDelay: 3 * time.Second}
	url, lost := startLostResponseServerWith(t, fnExec(r.exec), func() bool { return len(r.snapshot()) >= 2 })
	ra, err := a2a.NewRemoteAgent(context.Background(), url, a2a.WithHumanGate())
	if err != nil {
		t.Fatal(err)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("gate_seq"),
		agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(2))).AddStep("remote", ra)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err = seq.Run(ctx, sess, schema.UserMessage("原始请求"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应停在提问上：%v", err)
	}
	short, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
	defer cancel()
	_, err = seq.Resume(short, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("答#1")}))
	if !lost.Load() {
		t.Fatal("fixture 没搭对：续任务的响应一次都没丢过")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("等远端回应时调用方超时，错误应认得出 DeadlineExceeded，实际 %v", err)
	}
	if got := r.snapshot(); len(got) != 2 {
		t.Errorf("答复不该重发：远端收到 %q", got)
	}
}
