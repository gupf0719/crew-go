package a2a_test

// 续远端任务前对账（reconcileTask）的四条补漏，均为审计实测：
//   - session 上的句柄不是在远端等人时留下的（上一次的流停在 working 上就断了）：下一次调用是新的请求，不能按答复对账、
//     把上一个请求的结果当成这一次的交出去；
//   - 远端收下答复后原样再挂一次（状态消息复用同一条）：不能当成「还没回应」一直等，要问人那一问；
//   - 按快照收尾时，问人之前流出过的 artifact 不算这一轮的产出；
//   - 答人那一问时查不到远端状态（GetTask 瞬时失败，含解析错误）：不盲发旧答复，下一次接着对账。

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// recRemote 记下远端收到的消息（new: 新任务首条 / cont: 续任务），exec 决定怎么回。
type recRemote struct {
	mu    sync.Mutex
	got   []string
	conts atomic.Int32
}

func (r *recRemote) record(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, s)
}

func (r *recRemote) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.got...)
}

// interceptServer 起一个远端，intercept 返回 true 表示这个请求它已经处理了（不再交给远端）。
func interceptServer(t *testing.T, exec a2asrv.AgentExecutor, intercept func(w http.ResponseWriter, req *http.Request, body []byte, mux http.Handler) bool) string {
	t.Helper()
	mux := http.NewServeMux()
	srv := httptest.NewUnstartedServer(nil)
	url := "http://" + srv.Listener.Addr().String()
	card := &a2atypes.AgentCard{
		Name: "Probe Agent", Description: "in-process test fixture",
		SupportedInterfaces: []*a2atypes.AgentInterface{a2atypes.NewAgentInterface(url+"/invoke", a2atypes.TransportProtocolJSONRPC)},
		DefaultInputModes:   []string{"text"}, DefaultOutputModes: []string{"text"},
		Capabilities: a2atypes.AgentCapabilities{Streaming: true},
	}
	mux.Handle("/invoke", a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(exec)))
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body) // 读失败时 body 为空，照常交给远端
		req.Body = io.NopCloser(bytes.NewReader(body))
		if req.URL.Path == "/invoke" && intercept(w, req, body, mux) {
			return
		}
		mux.ServeHTTP(w, req)
	})
	srv.Start()
	t.Cleanup(srv.Close)
	return url
}

// loseFirstContinuation 让第一次续任务的请求远端照常处理完、再给客户端回 503。
func loseFirstContinuation(lost *atomic.Bool) func(http.ResponseWriter, *http.Request, []byte, http.Handler) bool {
	return func(w http.ResponseWriter, req *http.Request, body []byte, mux http.Handler) bool {
		if bytes.Contains(body, []byte(`"taskId"`)) && lost.CompareAndSwap(false, true) {
			mux.ServeHTTP(httptest.NewRecorder(), req)
			http.Error(w, "upstream unavailable", http.StatusServiceUnavailable)
			return true
		}
		return false
	}
}

// driveGate 把开了 human gate 的 RemoteAgent 放进带重试的 SequentialAgent，跑到收敛：停在提问上就答（答#1、答#2……），
// 普通错停下时不带新答案续跑一次。
func driveGate(t *testing.T, url string) (asked []string, final string, err error) {
	t.Helper()
	ra, err := a2a.NewRemoteAgent(context.Background(), url, a2a.WithHumanGate())
	if err != nil {
		t.Fatal(err)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("gate_seq"),
		agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(2))).AddStep("remote", ra)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	answers := map[string]*schema.Message{}
	evs, err := seq.Run(ctx, sess, schema.UserMessage("原始请求"))
	for round := 0; round < 6 && err != nil; round++ {
		if ie, ok := compose_v2.IsInterrupt(err); ok {
			for _, q := range ie.AllQueries() {
				asked = append(asked, q.Question)
				answers[q.InterruptID] = schema.UserMessage(fmt.Sprintf("答#%d", len(asked)))
			}
		} else if errors.Is(err, compose_v2.ErrRunAborted) {
			break
		}
		evs, err = seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	}
	if m := agent_v2.FinalMessage(evs); m != nil {
		final = m.Content
	}
	return asked, final, err
}

// 默认模式：第 1 次 Run("A") 的流在远端发出 working 之后就断了（句柄留着），远端随后办完。第 2 次 Run("B") 是新的请求：
// 当新任务发出去、拿到 B 的结果，不是 A 的。
func TestRemoteAgent_StaleHandleAfterTruncatedStream_SendsNewRequest(t *testing.T) {
	r := &recRemote{}
	exec := fnExec(func(execCtx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
		txt := inputText(execCtx)
		if execCtx.StoredTask != nil {
			r.record("cont:" + txt)
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted,
				a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("cont-result:"+txt))), nil)
			return
		}
		r.record("new:" + txt)
		if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
			return
		}
		if !yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateWorking, nil), nil) {
			return
		}
		time.Sleep(100 * time.Millisecond)
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted,
			a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("result:"+txt))), nil)
	})
	var cut atomic.Bool
	url := interceptServer(t, exec, func(w http.ResponseWriter, req *http.Request, body []byte, mux http.Handler) bool {
		if !bytes.Contains(body, []byte(`SendStreamingMessage`)) || bytes.Contains(body, []byte(`"taskId"`)) || !cut.CompareAndSwap(false, true) {
			return false
		}
		// 第一次新任务的流：把 working 那一帧交给客户端之后就干净地结束流（远端在后台接着办）
		pr, pw := io.Pipe()
		go func() {
			mux.ServeHTTP(&pipeWriter{h: http.Header{}, pw: pw}, req.Clone(context.Background()))
			_ = pw.Close()
		}()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		br := bufio.NewReader(pr)
		working := false
		for {
			line, err := br.ReadString('\n')
			if line != "" {
				_, _ = io.WriteString(w, line)
				w.(http.Flusher).Flush()
			}
			working = working || strings.Contains(line, "TASK_STATE_WORKING")
			if (working && strings.TrimSpace(line) == "") || err != nil {
				go func() { _, _ = io.Copy(io.Discard, pr) }() // 远端接着办，输出丢弃
				return true
			}
		}
	})
	ra, err := a2a.NewRemoteAgent(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	sess := agent_v2.NewRunSession()
	if _, err := ra.Run(ctx, sess, schema.UserMessage("A")); err != nil || !cut.Load() {
		t.Fatalf("fixture 没搭对：第 1 次 Run 的流应在 working 之后断开，实际 err=%v cut=%v", err, cut.Load())
	}
	time.Sleep(300 * time.Millisecond) // 远端把 A 办完
	evs, err := ra.Run(ctx, sess, schema.UserMessage("B"))
	got := ""
	if m := agent_v2.FinalMessage(evs); m != nil {
		got = m.Content
	}
	if err != nil || got != "result:B" {
		t.Fatalf("B 应当作新任务发出去、拿到 B 的结果，实际 err=%v final=%q（远端收到 %q）", err, got, r.snapshot())
	}
	if want := []string{"new:A", "new:B"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q", r.snapshot(), want)
	}
}

type pipeWriter struct {
	h  http.Header
	pw *io.PipeWriter
}

func (p *pipeWriter) Header() http.Header         { return p.h }
func (p *pipeWriter) Write(b []byte) (int, error) { return p.pw.Write(b) }
func (p *pipeWriter) WriteHeader(int)             {}
func (p *pipeWriter) Flush()                      {}

// 远端收下答复后原样再挂一次（状态消息还是那一条），响应丢了：问人那一问（新 ID），答复续上任务，不当成「还没回应」一直等。
func TestHumanGate_LostResponse_ReusedStatusMessageAskedAgain(t *testing.T) {
	r := &recRemote{}
	exec := fnExec(func(execCtx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
		txt := inputText(execCtx)
		if execCtx.StoredTask == nil {
			r.record("new:" + txt)
			if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
				return
			}
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateInputRequired,
				a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("批准吗"))), nil)
			return
		}
		r.record("cont:" + txt)
		if r.conts.Add(1) == 1 {
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateInputRequired, execCtx.StoredTask.Status.Message), nil)
			return
		}
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted,
			a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("done:"+txt))), nil)
	})
	var lost atomic.Bool
	url := interceptServer(t, exec, loseFirstContinuation(&lost))
	type outcome struct {
		asked []string
		final string
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		asked, final, err := driveGate(t, url)
		done <- outcome{asked, final, err}
	}()
	var o outcome
	select {
	case o = <-done:
	case <-time.After(10 * time.Second):
		// 当成「还没回应」时每次都等满在途宽限（30 秒）、永远不问人
		t.Fatalf("10 秒内没收敛：对账把远端原样再挂的那一问当成了还没回应（远端收到 %q）", r.snapshot())
	}
	if !lost.Load() {
		t.Fatal("fixture 没搭对：续任务的响应一次都没丢过")
	}
	if o.err != nil || o.final != "done:答#2" || len(o.asked) != 2 {
		t.Fatalf("应再问一次人、答复续上任务：err=%v final=%q 问过 %q（远端收到 %q）", o.err, o.final, o.asked, r.snapshot())
	}
}

// 远端问人之前先流出一段 artifact；答复之后流出这一轮的 artifact 收尾，响应丢了：取回的答复只含这一轮的，跟正常收尾一样。
func TestHumanGate_LostResponse_SnapshotSkipsEarlierArtifacts(t *testing.T) {
	r := &recRemote{}
	exec := fnExec(func(execCtx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
		txt := inputText(execCtx)
		if execCtx.StoredTask == nil {
			r.record("new:" + txt)
			if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
				return
			}
			if !yield(a2atypes.NewArtifactEvent(execCtx, a2atypes.NewTextPart("PREAMBLE.")), nil) {
				return
			}
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateInputRequired,
				a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("批准吗"))), nil)
			return
		}
		r.record("cont:" + txt)
		if !yield(a2atypes.NewArtifactEvent(execCtx, a2atypes.NewTextPart("got:"+txt)), nil) {
			return
		}
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted, nil), nil)
	})
	normalURL := interceptServer(t, exec, func(http.ResponseWriter, *http.Request, []byte, http.Handler) bool { return false })
	_, normal, err := driveGate(t, normalURL)
	if err != nil {
		t.Fatalf("对照组（不丢响应）没收敛：%v", err)
	}
	var lost atomic.Bool
	lossyURL := interceptServer(t, exec, loseFirstContinuation(&lost))
	_, recovered, err := driveGate(t, lossyURL)
	if !lost.Load() || err != nil {
		t.Fatalf("fixture 没搭对或没收敛：lost=%v err=%v", lost.Load(), err)
	}
	if recovered != normal || strings.Contains(recovered, "PREAMBLE") {
		t.Fatalf("取回的答复应跟正常收尾一样（%q），实际 %q", normal, recovered)
	}
}

// 续任务的响应丢了（远端收下答复、改问了「删除历史吗」），紧接着那一次 GetTask 也失败一次：不盲发旧答复，下一次对账后
// 问人新的那一问。
func TestHumanGate_LostResponse_GetTaskFailureDoesNotResend(t *testing.T) {
	lostThenGetTaskFailsOnce(t, func(w http.ResponseWriter, _ []byte) {
		http.Error(w, "upstream unavailable", http.StatusServiceUnavailable)
	})
}

// 同上，那一次 GetTask 报的是解析错误（JSON-RPC -32700；REST 传输下客户端解不开错误响应体也报它）：这一次没解析成，不是
// 每次都一样，同样不盲发（曾按确定性错误照常发答复，远端第二次收到同一个答复）。
func TestHumanGate_LostResponse_GetTaskParseErrorDoesNotResend(t *testing.T) {
	lostThenGetTaskFailsOnce(t, func(w http.ResponseWriter, body []byte) {
		var rpc struct {
			ID any `json:"id"`
		}
		if err := json.Unmarshal(body, &rpc); err != nil {
			t.Errorf("解析 GetTask 请求：%v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": rpc.ID,
			"error": map[string]any{"code": -32700, "message": "parse error"}}); err != nil {
			t.Errorf("写错误响应：%v", err)
		}
	})
}

// lostThenGetTaskFailsOnce：第一次续任务的响应丢掉，之后第一次 GetTask 交给 fail 应答。
func lostThenGetTaskFailsOnce(t *testing.T, fail func(w http.ResponseWriter, body []byte)) {
	t.Helper()
	r := &recRemote{}
	exec := fnExec(func(execCtx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
		txt := inputText(execCtx)
		if execCtx.StoredTask == nil {
			r.record("new:" + txt)
			if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
				return
			}
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateInputRequired,
				a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("批准转账吗"))), nil)
			return
		}
		r.record("cont:" + txt)
		if r.conts.Add(1) == 1 {
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateInputRequired,
				a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("删除历史吗"))), nil)
			return
		}
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted,
			a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("done:"+txt))), nil)
	})
	var lost, getFailed atomic.Bool
	lose := loseFirstContinuation(&lost)
	url := interceptServer(t, exec, func(w http.ResponseWriter, req *http.Request, body []byte, mux http.Handler) bool {
		if lose(w, req, body, mux) {
			return true
		}
		if lost.Load() && bytes.Contains(body, []byte(`"GetTask"`)) && getFailed.CompareAndSwap(false, true) {
			fail(w, body)
			return true
		}
		return false
	})
	asked, final, err := driveGate(t, url)
	if !getFailed.Load() {
		t.Fatal("fixture 没搭对：对账的 GetTask 一次都没失败过")
	}
	if err != nil || final != "done:答#2" || len(asked) != 2 || asked[1] != "删除历史吗" {
		t.Fatalf("应问人「删除历史吗」、答复续上任务：err=%v final=%q 问过 %q", err, final, asked)
	}
	if want := []string{"new:原始请求", "cont:答#1", "cont:答#2"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q——旧答复又发了一次", r.snapshot(), want)
	}
}
