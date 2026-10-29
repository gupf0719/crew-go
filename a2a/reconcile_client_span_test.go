package a2a_test

// 续远端任务之前的对账（GetTask）失败、提前返回时，这次远端调用照样有 client span（以失败收尾）。

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// startGetTaskFlaky 起一个远端：第 2 次 GetTask（续任务前的对账）回 503，其余照常。
func startGetTaskFlaky(t *testing.T, exec a2asrv.AgentExecutor) (string, *atomic.Int32) {
	t.Helper()
	mux := http.NewServeMux()
	srv := httptest.NewUnstartedServer(nil)
	url := "http://" + srv.Listener.Addr().String()
	card := &a2atypes.AgentCard{
		Name:        "Flaky GetTask Agent",
		Description: "probe",
		SupportedInterfaces: []*a2atypes.AgentInterface{
			a2atypes.NewAgentInterface(url+"/invoke", a2atypes.TransportProtocolJSONRPC),
		},
		DefaultInputModes:  []string{"text"},
		DefaultOutputModes: []string{"text"},
		Capabilities:       a2atypes.AgentCapabilities{Streaming: true},
	}
	mux.Handle("/invoke", a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(exec)))
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	getTasks := &atomic.Int32{}
	failed := &atomic.Bool{}
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		req.Body = io.NopCloser(bytes.NewReader(body))
		if req.URL.Path == "/invoke" && bytes.Contains(body, []byte(`"GetTask"`)) {
			if getTasks.Add(1) == 2 && failed.CompareAndSwap(false, true) {
				http.Error(w, "upstream unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		mux.ServeHTTP(w, req)
	})
	srv.Start()
	t.Cleanup(srv.Close)
	return url, getTasks
}

// TestReconcileFailure_HasClientSpan：human gate 模式下远端问人，用户答复后续跑时先对账（GetTask），对账那次 503 ⟹ 提前返回
// 错误。这次远端调用要有自己的 client span、以失败收尾，传输方式按名片记（名片只声明了 JSON-RPC）。
func TestReconcileFailure_HasClientSpan(t *testing.T) {
	obs, rec := newRecordingObs(t)
	r := &gateRemote{}
	url, getTasks := startGetTaskFlaky(t, fnExec(r.exec))
	ra, err := a2a.NewRemoteAgent(context.Background(), url, a2a.WithHumanGate(), a2a.WithObservability(obs))
	if err != nil {
		t.Fatal(err)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("gate_seq")).AddStep("remote", ra)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	clientSpans := func() []sdktrace.ReadOnlySpan {
		var out []sdktrace.ReadOnlySpan
		for _, s := range rec.Ended() {
			if s.SpanKind() == trace.SpanKindClient {
				out = append(out, s)
			}
		}
		return out
	}

	_, err = seq.Run(ctx, sess, schema.UserMessage("原始请求"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture：远端应先问人，got %v", err)
	}
	if n := len(clientSpans()); n != 1 {
		t.Fatalf("第一次调用应有 1 个 client span，got %d", n)
	}
	answers := map[string]*schema.Message{}
	for _, q := range ie.AllQueries() {
		answers[q.InterruptID] = schema.UserMessage("答复")
	}
	_, err = seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	if err == nil || getTasks.Load() < 2 {
		t.Fatalf("fixture：续跑时对账那次 GetTask 应 503 并提前返回，got err=%v GetTask=%d", err, getTasks.Load())
	}
	spans := clientSpans()
	if len(spans) != 2 {
		t.Fatalf("对账失败的那次远端调用也应有 client span，ended client span=%d（应为 2）", len(spans))
	}
	last := spans[1]
	if last.Status().Code != codes.Error {
		t.Errorf("对账失败的 client span 应以失败收尾，got status=%v", last.Status())
	}
	for _, kv := range last.Attributes() {
		if string(kv.Key) == "crew.a2a.transport" && kv.Value.AsString() != "jsonrpc" {
			t.Errorf("crew.a2a.transport=%q，名片只声明了 JSON-RPC，应为 jsonrpc", kv.Value.AsString())
		}
	}
}
