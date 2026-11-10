package a2a_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/a2a"
	"crew-go/a2a/server"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"

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
	_, err = ra.Run(ctx, sess, schema.UserMessage("42"))
	if ie, ok := compose_v2.IsInterrupt(err); !ok || !strings.Contains(ie.Query.Question, "approve deletion?") {
		t.Fatalf("应收到第二项审批，实际 %v", err)
	}
	events, err = ra.Run(ctx, sess, schema.UserMessage("approve"))
	if msg := agent_v2.FinalMessage(events); err != nil || msg == nil || msg.Content != "done" {
		t.Fatalf("第二项审批应一次续跑成功，实际 final=%+v err=%v", msg, err)
	}
}

// 真 server.Handler 收下第一条答复并推进到下一问，传输层丢掉响应。后续对账可能缺历史或仍看到旧快照。
func checkLostApproval(t *testing.T, hosted, stale, firstApproval bool) {
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
	var lost atomic.Bool
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
				} else if continuations.Load() == 1 {
					response["result"] = firstQuestion
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
	if !hosted && !firstApproval {
		if msg := agent_v2.FinalMessage(events); err != nil || msg == nil || !strings.Contains(msg.Content, "approve refund?") {
			t.Fatalf("fixture ordinary question: final=%+v err=%v", msg, err)
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
		return agUnderTest.Run(ctx, sess, msg)
	}
	_, err = answer(true)
	if err == nil || !lost.Load() || q2Done.Load() != 0 {
		t.Fatalf("fixture lost=%v err=%v q2Done=%d", lost.Load(), err, q2Done.Load())
	}
	events, err = answer(true)
	if q2Done.Load() != 0 {
		t.Fatalf("deletion executed without its own human approval: q2Done=%d final=%+v err=%v", q2Done.Load(), agent_v2.FinalMessage(events), err)
	}
	ie, ok = compose_v2.IsInterrupt(err)
	if !ok || !strings.Contains(ie.Query.Question, "approve deletion?") {
		t.Fatalf("expected new approval question: %v", err)
	}
	wantContinuations := int32(1)
	if stale {
		wantContinuations = 2 // 旧快照允许发送，由服务端按审批 ID 拒绝。
	}
	if got := continuations.Load(); got != wantContinuations {
		t.Fatalf("continuations=%d, want %d", got, wantContinuations)
	}
	events, err = answer(false)
	if err != nil || q2Done.Load() != 1 {
		t.Fatalf("explicit second approval did not complete: done=%d err=%v", q2Done.Load(), err)
	}
	if got := agent_v2.FinalMessage(events); got == nil || got.Content != "done" {
		t.Fatalf("final=%+v, want done", got)
	}
}
