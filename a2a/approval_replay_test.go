package a2a_test

import (
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
	"sync/atomic"
	"testing"
	"time"

	"crew-go/a2a"
	"crew-go/a2a/server"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

type approvalRoundTrip func(*http.Request) (*http.Response, error)

func (f approvalRoundTrip) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestRemoteAgent_TopLevelLostApprovalResponse(t *testing.T) {
	checkLostApproval(t, false, false, true)
}

func TestRemoteAgent_HostedLostApprovalResponseWithoutHistory(t *testing.T) {
	checkLostApproval(t, true, false, true)
}

func TestRemoteAgent_StaleSnapshotDoesNotRetargetApproval(t *testing.T) {
	checkLostApproval(t, true, true, true)
}

func TestRemoteAgent_UnknownSnapshotDoesNotResendApproval(t *testing.T) {
	checkLostApproval(t, true, true, true, `{"state":"TASK_STATE_PAUSED"}`)
}

func TestRemoteAgent_ReattachStaleSnapshotPreservesContinuation(t *testing.T) {
	remote := &gateRemote{onCont: func(n int32) a2atypes.TaskState {
		if n == 1 {
			return a2atypes.TaskStateInputRequired
		}
		return a2atypes.TaskStateCompleted
	}}
	var lost, staleRead, subscribed atomic.Bool
	var firstStatus atomic.Value // json.RawMessage
	lose := loseFirstContinuation(&lost)
	url := interceptServer(t, fnExec(remote.exec), func(w http.ResponseWriter, req *http.Request, body []byte, mux http.Handler) bool {
		if lose(w, req, body, mux) {
			return true
		}
		var call struct{ Method string }
		if json.Unmarshal(body, &call) != nil {
			return false
		}
		if call.Method == "SubscribeToTask" && staleRead.Load() {
			subscribed.Store(true)
			http.Error(w, "subscription unavailable", http.StatusServiceUnavailable)
			return true
		}
		if call.Method != "GetTask" {
			return false
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		var response, result map[string]json.RawMessage
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Errorf("解析 GetTask 响应：%v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return true
		}
		if err := json.Unmarshal(response["result"], &result); err != nil {
			t.Errorf("解析任务快照：%v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return true
		}
		if !lost.Load() {
			firstStatus.Store(result["status"])
		}
		if staleRead.Load() {
			delete(result, "history")
			result["status"] = json.RawMessage(`{"state":"TASK_STATE_WORKING"}`)
			if subscribed.Load() {
				result["status"], _ = firstStatus.Load().(json.RawMessage)
			}
		}
		var err error
		response["result"], err = json.Marshal(result)
		if err != nil {
			t.Errorf("编码任务快照：%v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return true
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Errorf("写入 GetTask 响应：%v", err)
		}
		return true
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ra, err := a2a.NewRemoteAgent(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	sess := agent_v2.NewRunSession()
	first, err := ra.Run(ctx, sess, schema.UserMessage("原始请求"))
	token := a2a.Continuation(agent_v2.FinalMessage(first))
	if err != nil || token == "" {
		t.Fatalf("前置：应收到首轮续接句柄，实际 %v", err)
	}
	answer := a2a.WithContinuation(schema.UserMessage("答一"), token)
	if _, err := ra.Run(ctx, sess, answer); err == nil || !lost.Load() {
		t.Fatalf("前置：答复已处理但响应丢失，实际 %v", err)
	}
	staleRead.Store(true)
	if events, err := ra.Run(ctx, sess, answer); err == nil || !subscribed.Load() || a2a.Continuation(agent_v2.FinalMessage(events)) != "" {
		t.Fatalf("重挂读到旧问题时应保留原续接句柄，实际 events=%v err=%v", events, err)
	}
	staleRead.Store(false)
	events, err := ra.Run(ctx, sess, answer)
	second := agent_v2.FinalMessage(events)
	nextToken := a2a.Continuation(second)
	if err != nil || second == nil || !strings.Contains(second.Content, "还要再确认一次吗") || nextToken == "" || nextToken == token {
		t.Fatalf("远端快照恢复后应取回新的问题，实际 final=%+v err=%v", second, err)
	}
	events, err = ra.Run(ctx, sess, a2a.WithContinuation(schema.UserMessage("答二"), nextToken))
	if final := agent_v2.FinalMessage(events); err != nil || final == nil || final.Content != "续任务办了:答二" {
		t.Fatalf("新问题的答复应完成原任务，实际 final=%+v err=%v", final, err)
	}
	if got := remote.snapshot(); !slices.Equal(got, []string{"new:原始请求", "cont:答一", "cont:答二"}) {
		t.Fatalf("两个问题应各收到一次答复，实际 %v", got)
	}
}

func TestRemoteAgent_FailedReattachRetainsDeliveryEvidence(t *testing.T) {
	remote := &gateRemote{}
	var stage, subscribes atomic.Int32
	url := interceptServer(t, fnExec(remote.exec), func(w http.ResponseWriter, _ *http.Request, body []byte, _ http.Handler) bool {
		current := stage.Load()
		if current == 0 {
			return false
		}
		var rpc struct {
			ID     json.RawMessage
			Method string
			Params struct{ ID string }
		}
		if json.Unmarshal(body, &rpc) != nil {
			return false
		}
		switch rpc.Method {
		case "SubscribeToTask":
			subscribes.Add(1)
			http.Error(w, "subscription unavailable", http.StatusServiceUnavailable)
		case "GetTask":
			status := `{"state":"TASK_STATE_WORKING"}`
			if current == 2 {
				status = `{"state":"TASK_STATE_INPUT_REQUIRED"}`
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"id":%q,"status":%s}}`, rpc.ID, rpc.Params.ID, status)
		default:
			return false
		}
		return true
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ra, err := a2a.NewRemoteAgent(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	sess := agent_v2.NewRunSession()
	events, err := ra.Run(ctx, sess, schema.UserMessage("原始请求"))
	token := a2a.Continuation(agent_v2.FinalMessage(events))
	if err != nil || token == "" {
		t.Fatalf("前置：应收到续接句柄，实际 %v", err)
	}
	answer := a2a.WithContinuation(schema.UserMessage("答复"), token)
	stage.Store(1)
	if _, err := ra.Run(ctx, sess, answer); err == nil || subscribes.Load() != 1 {
		t.Fatalf("前置：远端报告 working 后应尝试重挂一次，实际订阅 %d 次，err=%v", subscribes.Load(), err)
	}
	stage.Store(2)
	if _, err := ra.Run(ctx, sess, answer); err == nil || len(remote.snapshot()) != 1 {
		t.Fatalf("重挂失败后仍须对账，缺历史的待答快照不能触发发送，实际 %v，err=%v", remote.snapshot(), err)
	}
	stage.Store(0)
	events, err = ra.Run(ctx, sess, answer)
	if final := agent_v2.FinalMessage(events); err != nil || final == nil || final.Content != "续任务办了:答复" {
		t.Fatalf("读到完整历史后应能补发，实际 final=%+v err=%v", final, err)
	}
	if got := remote.snapshot(); !slices.Equal(got, []string{"new:原始请求", "cont:答复"}) {
		t.Fatalf("远端应只收到一次答复，实际 %v", got)
	}
}

func TestRemoteAgent_LostOrdinaryReplyDoesNotApprove(t *testing.T) {
	t.Run("顶层普通追问", func(t *testing.T) { checkLostApproval(t, false, false, false) })
	t.Run("嵌套追问读到旧快照", func(t *testing.T) { checkLostApproval(t, true, true, false) })
}

func TestRemoteAgent_DistinctApprovalAfterOrdinaryQuestion(t *testing.T) {
	ag := rsHost(t, func(ctx context.Context, _ string) (string, error) {
		for _, q := range []compose_v2.AskQuery{
			{Question: "approve refund?", Reason: agent_v2.ApprovalInterruptReason},
			{Question: "reference number?"},
			{Question: "approve deletion?", Reason: agent_v2.ApprovalInterruptReason},
		} {
			if _, err := compose_v2.Ask(ctx, &q); err != nil {
				return "", err
			}
		}
		return "done", nil
	})
	const base = "http://a2a.review.invalid"
	h := server.Handler(ag, base, server.WithOwnerResolver(func(context.Context) string { return "review" }))
	c := &http.Client{Transport: approvalRoundTrip(func(req *http.Request) (*http.Response, error) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req.Clone(context.Background()))
		return w.Result(), nil
	})}
	ctx := context.Background()
	ra, err := a2a.NewRemoteAgentFromCard(ctx, server.Card(ag, base), a2a.WithClientOptions(a2aclient.WithJSONRPCTransport(c)))
	if err != nil {
		t.Fatal(err)
	}
	sess := agent_v2.NewRunSession()
	_, err = ra.Run(ctx, sess, schema.UserMessage("request"))
	if ie, ok := compose_v2.IsInterrupt(err); !ok || !strings.Contains(ie.Query.Question, "approve refund?") {
		t.Fatalf("应先询问第一项审批，实际 %v", err)
	}
	events, err := ra.Run(ctx, sess, schema.UserMessage("approve"))
	if msg := agent_v2.FinalMessage(events); err != nil || msg == nil || !strings.Contains(msg.Content, "reference number?") {
		t.Fatalf("应收到普通追问，实际 final=%+v err=%v", msg, err)
	}
	token := a2a.Continuation(agent_v2.FinalMessage(events))
	if token == "" {
		t.Fatal("普通追问缺续接句柄")
	}
	_, err = ra.Run(ctx, sess, a2a.WithContinuation(schema.UserMessage("42"), token))
	if ie, ok := compose_v2.IsInterrupt(err); !ok || !strings.Contains(ie.Query.Question, "approve deletion?") {
		t.Fatalf("应收到第二项审批，实际 %v", err)
	}
	events, err = ra.Run(ctx, sess, schema.UserMessage("approve"))
	if msg := agent_v2.FinalMessage(events); err != nil || msg == nil || msg.Content != "done" {
		t.Fatalf("第二项审批应一次续跑成功，实际 final=%+v err=%v", msg, err)
	}
}

// 真 server.Handler 收下第一条答复并推进到下一问，传输层丢掉响应。后续对账可能缺历史或仍看到旧快照。
func checkLostApproval(t *testing.T, hosted, stale, firstApproval bool, statusOverride ...string) {
	t.Helper()
	const attachment = "https://example.invalid/refund.pdf"
	var q2Done atomic.Int32
	ag := rsHost(t, func(ctx context.Context, _ string) (string, error) {
		for i, q := range []string{"approve refund?", "approve deletion?"} {
			query := &compose_v2.AskQuery{Question: q}
			if i == 1 || firstApproval {
				query.Reason = agent_v2.ApprovalInterruptReason
			}
			ans, err := compose_v2.Ask(ctx, query)
			if err != nil {
				return "", err
			}
			if i == 0 && !firstApproval {
				if len(ans.ContentBlocks) != 1 || ans.ContentBlocks[0] == nil || ans.ContentBlocks[0].File == nil ||
					ans.ContentBlocks[0].File.File == nil || ans.ContentBlocks[0].File.File.URL != attachment {
					t.Errorf("普通追问的附件未到达 Ask：%+v", ans.ContentBlocks)
				}
			}
			if i == 1 && ans.Content == "approve" {
				q2Done.Add(1)
			}
		}
		return "done", nil
	})
	ctx := context.Background()
	const base = "http://a2a.review.invalid"
	h := server.Handler(ag, base, server.WithOwnerResolver(func(context.Context) string { return "review" }))
	var lost, staleRead atomic.Bool
	var continuations atomic.Int32
	var firstQuestion json.RawMessage
	c := &http.Client{Transport: approvalRoundTrip(func(req *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req.Clone(context.Background()))
		if bytes.Contains(body, []byte(`"method":"SendStreamingMessage"`)) && bytes.Contains(body, []byte(`"taskId"`)) {
			continuations.Add(1)
			if lost.CompareAndSwap(false, true) {
				if !strings.Contains(w.Body.String(), "approve deletion?") {
					t.Errorf("fixture: first answer must advance to deletion question, response %s", w.Body.String())
				}
				w = httptest.NewRecorder()
				http.Error(w, "lost response", http.StatusServiceUnavailable)
			}
		}
		if hosted && bytes.Contains(body, []byte(`"method":"GetTask"`)) {
			var response map[string]json.RawMessage
			var result map[string]json.RawMessage
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				return nil, err
			}
			if err := json.Unmarshal(response["result"], &result); err != nil {
				return nil, err
			}
			delete(result, "history")
			response["result"], err = json.Marshal(result)
			if err != nil {
				return nil, err
			}
			if stale {
				if continuations.Load() == 0 {
					firstQuestion = response["result"]
				} else if staleRead.Load() {
					response["result"] = firstQuestion
					if len(statusOverride) > 0 {
						var snap map[string]json.RawMessage
						if err := json.Unmarshal(firstQuestion, &snap); err != nil {
							return nil, err
						}
						snap["status"] = json.RawMessage(statusOverride[0])
						response["result"], err = json.Marshal(snap)
						if err != nil {
							return nil, err
						}
					}
				}
			}
			w.Body.Reset()
			if err := json.NewEncoder(w).Encode(response); err != nil {
				return nil, err
			}
		}
		return w.Result(), nil
	})}
	opts := []a2a.RemoteOption{a2a.WithClientOptions(a2aclient.WithJSONRPCTransport(c))}
	if hosted {
		opts = append(opts, a2a.WithHumanGate())
	}
	ra, err := a2a.NewRemoteAgentFromCard(ctx, server.Card(ag, base), opts...)
	if err != nil {
		t.Fatal(err)
	}
	sess := agent_v2.NewRunSession()
	var agUnderTest agent_v2.Agent = ra
	if hosted {
		agUnderTest = agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("review_host")).AddStep("remote", ra)
		ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	}
	events, err := agUnderTest.Run(ctx, sess, schema.UserMessage("request"))
	ie, ok := compose_v2.IsInterrupt(err)
	var token string
	if !hosted && !firstApproval {
		if msg := agent_v2.FinalMessage(events); err != nil || msg == nil || !strings.Contains(msg.Content, "approve refund?") {
			t.Fatalf("fixture ordinary question: final=%+v err=%v", msg, err)
		}
		token = a2a.Continuation(agent_v2.FinalMessage(events))
		if token == "" {
			t.Fatal("普通追问缺续接句柄")
		}
	} else if !ok || !strings.Contains(ie.Query.Question, "approve refund?") {
		t.Fatalf("fixture first question: %v", err)
	}
	answer := func(first bool) ([]*agent_v2.AgentEvent, error) {
		msg := schema.UserMessage("approve")
		if first && !firstApproval {
			msg.ContentBlocks = []*schema.ContentBlock{{Type: string(schema.ContentBlockTypeFile), MediaType: "application/pdf",
				File: &schema.FileContentBlock{Type: schema.ContentBlockTypeFile, File: &schema.FileData{URL: attachment}}}}
		}
		if hosted {
			return agUnderTest.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: msg}))
		}
		if first && token != "" {
			msg = a2a.WithContinuation(msg, token)
		}
		return agUnderTest.Run(ctx, sess, msg)
	}
	_, err = answer(true)
	if err == nil || !lost.Load() || q2Done.Load() != 0 {
		t.Fatalf("fixture lost=%v err=%v q2Done=%d", lost.Load(), err, q2Done.Load())
	}
	if stale {
		staleRead.Store(true)
		_, err = answer(true)
		if err == nil || continuations.Load() != 1 || q2Done.Load() != 0 {
			t.Fatalf("远端状态不明时应保留原审批，实际发送 %d 次、执行 %d 次，err=%v", continuations.Load(), q2Done.Load(), err)
		}
		if _, ok := compose_v2.IsInterrupt(err); ok || errors.Is(err, compose_v2.ErrRunAborted) {
			t.Fatalf("状态不明时应返回可再次对账的错误，实际 %v", err)
		}
		staleRead.Store(false)
	}
	events, err = answer(true)
	if q2Done.Load() != 0 {
		t.Fatalf("deletion executed without its own human approval: q2Done=%d final=%+v err=%v", q2Done.Load(), agent_v2.FinalMessage(events), err)
	}
	ie, ok = compose_v2.IsInterrupt(err)
	if !ok || !strings.Contains(ie.Query.Question, "approve deletion?") {
		t.Fatalf("expected new approval question: %v", err)
	}
	if got := continuations.Load(); got != 1 {
		t.Fatalf("取回新问题前只应发送原答复一次，实际 %d", got)
	}
	events, err = answer(false)
	if err != nil || q2Done.Load() != 1 {
		t.Fatalf("explicit second approval did not complete: done=%d err=%v", q2Done.Load(), err)
	}
	if got := agent_v2.FinalMessage(events); got == nil || got.Content != "done" {
		t.Fatalf("final=%+v, want done", got)
	}
}
