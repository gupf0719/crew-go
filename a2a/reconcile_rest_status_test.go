package a2a_test

// REST 传输下答人那一问之前查任务状态，GetTask 被只拦读任务的鉴权层拒掉（允许发消息、不允许读任务），响应体不是 A2A 的
// 错误形状（Spring Boot / FastAPI 的默认体、纯文本）：按 HTTP 状态码认出这是鉴权拒绝（确定性错误），照常发答复、续上任务。
// 曾经客户端只看响应体，这几种分别成了解析错误 / 内部错误 / 服务端错误，都按瞬时处理，人的答复每次都卡在对账、报可重试错误。

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// interceptRESTServer 起一个只提供 HTTP+JSON 传输的远端（挂在 /rest 下），intercept 返回 true 表示这个请求它已经处理了。
func interceptRESTServer(t *testing.T, exec a2asrv.AgentExecutor, intercept func(w http.ResponseWriter, req *http.Request) bool) string {
	t.Helper()
	mux := http.NewServeMux()
	srv := httptest.NewUnstartedServer(nil)
	url := "http://" + srv.Listener.Addr().String()
	card := &a2atypes.AgentCard{
		Name: "Probe REST Agent", Description: "in-process test fixture",
		SupportedInterfaces: []*a2atypes.AgentInterface{a2atypes.NewAgentInterface(url+"/rest", a2atypes.TransportProtocolHTTPJSON)},
		DefaultInputModes:   []string{"text"}, DefaultOutputModes: []string{"text"},
		Capabilities: a2atypes.AgentCapabilities{Streaming: true},
	}
	mux.Handle("/rest/", http.StripPrefix("/rest", a2asrv.NewRESTHandler(a2asrv.NewHandler(exec))))
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body) // 读失败时 body 为空，照常交给远端
		req.Body = io.NopCloser(bytes.NewReader(body))
		if strings.HasPrefix(req.URL.Path, "/rest/") && intercept(w, req) {
			return
		}
		mux.ServeHTTP(w, req)
	})
	srv.Start()
	t.Cleanup(srv.Close)
	return url
}

func TestHumanGate_REST_GetTaskAuthRejectionByStatusStillSends(t *testing.T) {
	for _, tc := range []struct {
		name        string
		status      int
		contentType string
		body        string
	}{
		{"Spring Boot 默认 403", http.StatusForbidden, "application/json", `{"status":403,"error":"Forbidden","path":"/rest/tasks/x"}`},
		{"FastAPI 默认 403", http.StatusForbidden, "application/json", `{"detail":"Not authenticated"}`},
		{"网关纯文本 403", http.StatusForbidden, "text/plain", "Forbidden"},
		{"网关纯文本 401", http.StatusUnauthorized, "text/plain", "Unauthorized"},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
				yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted,
					a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("done:"+txt))), nil)
			})
			var denied atomic.Int32
			url := interceptRESTServer(t, exec, func(w http.ResponseWriter, req *http.Request) bool {
				// 只拒读任务（GET /tasks/{id}），发消息、订阅照常
				if req.Method != http.MethodGet || !strings.HasPrefix(req.URL.Path, "/rest/tasks/") || strings.Contains(req.URL.Path, ":") {
					return false
				}
				denied.Add(1)
				w.Header().Set("Content-Type", tc.contentType)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
				return true
			})
			asked, final, err := driveGate(t, url)
			if denied.Load() == 0 {
				t.Fatal("fixture 没搭对：GetTask 一次都没被拒过")
			}
			if err != nil || final != "done:答#1" || len(asked) != 1 {
				t.Fatalf("GetTask 被鉴权层拒（确定性）时应照常发答复、续上任务：err=%v final=%q 问过 %q 远端收到 %q",
					err, final, asked, r.snapshot())
			}
		})
	}
}
