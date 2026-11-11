package a2a_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/a2a"
	"crew-go/a2a/server"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"

	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

func serveContinuationGraph(t *testing.T, ctx context.Context, body func(context.Context, string) (string, error)) *a2a.RemoteAgent {
	t.Helper()
	ag := rsHost(t, body)
	const base = "http://a2a.review.invalid"
	h := server.Handler(ag, base, server.WithOwnerResolver(func(context.Context) string { return "review" }))
	c := &http.Client{Transport: approvalRoundTrip(func(req *http.Request) (*http.Response, error) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req.Clone(context.Background()))
		return w.Result(), nil
	})}
	ra, err := a2a.NewRemoteAgentFromCard(ctx, server.Card(ag, base), a2a.WithClientOptions(a2aclient.WithJSONRPCTransport(c)))
	if err != nil {
		t.Fatal(err)
	}
	return ra
}

func TestRemoteAgent_FollowUpRestoredWithoutOriginalSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	ra := serveContinuationGraph(t, ctx, func(ctx context.Context, _ string) (string, error) {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "approve remote operation?", Reason: agent_v2.ApprovalInterruptReason}); err != nil {
			return "", err
		}
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "reference number?"})
		if err != nil {
			return "", err
		}
		return "received " + ans.Content, nil
	})
	host := rsHost(t, func(ctx context.Context, _ string) (string, error) {
		ev, err := agent_v2.RunSubAgent(ctx, "delegate", ra, schema.UserMessage("delegate task"))
		if err != nil {
			return "", err
		}
		token := a2a.Continuation(agent_v2.FinalMessage(ev))
		if token == "" {
			t.Error("missing followup token")
		}
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "host reads reference number?"})
		if err != nil {
			return "", err
		}
		ev, err = agent_v2.RunSubAgent(ctx, "answer", ra, a2a.WithContinuation(ans, token))
		return finalText(ev), err
	})
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, schema.UserMessage("go"))
	approval, ok := compose_v2.IsInterrupt(err)
	if !ok || !strings.Contains(approval.Query.Question, "approve remote operation?") {
		t.Fatalf("missing approval: %v", err)
	}
	_, err = host.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		approval.Query.InterruptID: schema.UserMessage("approve"),
	}))
	followUp, ok := compose_v2.IsInterrupt(err)
	if !ok || !strings.Contains(followUp.Query.Question, "host reads reference number?") {
		t.Fatalf("missing host followup: %v", err)
	}
	ev, err := host.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		followUp.Query.InterruptID: schema.UserMessage("42"),
	}))
	if err != nil || finalText(ev) != "received 42" {
		t.Fatalf("resume must deliver the same remote question's answer: got %q, err %v", finalText(ev), err)
	}
}

func TestRemoteAgent_TopLevelContinuationsDoNotOverwriteApproval(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	answerStarted, releaseAnswer := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var approved []string
	ra := serveContinuationGraph(t, ctx, func(ctx context.Context, input string) (string, error) {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "reference number for " + input + "?"}); err != nil {
			return "", err
		}
		if paused, _ := compose_v2.GetStepResult[bool](ctx, "answer seen"); input == "A" && !paused {
			close(answerStarted)
			select {
			case <-releaseAnswer:
			case <-ctx.Done():
				return "", ctx.Err()
			}
			if err := compose_v2.SaveStepResult(ctx, "answer seen", true); err != nil {
				return "", err
			}
		}
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "approve " + input + "?", Reason: agent_v2.ApprovalInterruptReason}); err != nil {
			return "", err
		}
		mu.Lock()
		approved = append(approved, input)
		mu.Unlock()
		return "done " + input, nil
	})
	sess := agent_v2.NewRunSession()
	var tokens []string
	for _, in := range []string{"A", "B"} {
		ev, err := ra.Run(ctx, sess, schema.UserMessage(in))
		token := a2a.Continuation(agent_v2.FinalMessage(ev))
		if err != nil || token == "" {
			t.Fatalf("start %q: token %q, err %v", in, token, err)
		}
		tokens = append(tokens, token)
	}
	first := make(chan error, 1)
	go func() {
		_, err := ra.Run(ctx, sess.ForkLane(), a2a.WithContinuation(schema.UserMessage("42"), tokens[0]))
		first <- err
	}()
	select {
	case <-answerStarted:
	case <-ctx.Done():
		t.Fatal("A 未到达远端答复屏障")
	}
	_, busyErr := ra.Run(ctx, sess.ForkLane(), a2a.WithContinuation(schema.UserMessage("42"), tokens[1]))
	close(releaseAnswer)
	if busyErr == nil || !strings.Contains(busyErr.Error(), "already in progress") {
		t.Fatalf("并发 lane 应共享执行许可，实际 %v", busyErr)
	}
	if err := <-first; err == nil {
		t.Fatal("第一问应进入真人审批")
	} else if ie, ok := compose_v2.IsInterrupt(err); !ok || !strings.Contains(ie.Query.Question, "approve A?") {
		t.Fatalf("第一问应针对 A，实际 %v", err)
	}
	if _, err := ra.Run(ctx, sess, a2a.WithContinuation(schema.UserMessage("42"), tokens[1])); err == nil ||
		!strings.Contains(err.Error(), "pending human question") {
		t.Fatalf("A 等真人时应拒绝续 B，实际 %v", err)
	}
	ev, err := ra.Run(ctx, sess, schema.UserMessage("approve"))
	if err != nil || finalText(ev) != "done A" {
		t.Fatalf("对 A 的审批应执行 A，实际 %q，%v", finalText(ev), err)
	}
	if _, err := ra.Run(ctx, sess, a2a.WithContinuation(schema.UserMessage("42"), tokens[1])); err == nil {
		t.Fatal("B 应在解除占用后进入自己的审批")
	} else if ie, ok := compose_v2.IsInterrupt(err); !ok || !strings.Contains(ie.Query.Question, "approve B?") {
		t.Fatalf("第二问应针对 B，实际 %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(approved) != 1 || approved[0] != "A" {
		t.Fatalf("B 在真人答复之前不应执行，实际 %q", approved)
	}
}

func TestRemoteAgent_SameTextDifferentContinuationsKeepTheirPhase(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	ra := serveContinuationGraph(t, ctx, func(ctx context.Context, input string) (string, error) {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "reference number for " + input + "?"}); err != nil {
			return "", err
		}
		if input == "B" {
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "approve B?", Reason: agent_v2.ApprovalInterruptReason}); err != nil {
				return "", err
			}
		}
		return "done " + input, nil
	})
	host := rsHost(t, func(ctx context.Context, _ string) (string, error) {
		sess := agent_v2.GetSession(ctx)
		tokens, ready := compose_v2.GetStepResult[[]string](ctx, "questions")
		if !ready {
			for _, in := range []string{"A", "B"} {
				ev, err := ra.Run(ctx, sess, schema.UserMessage(in))
				if err != nil {
					return "", err
				}
				tokens = append(tokens, a2a.Continuation(agent_v2.FinalMessage(ev)))
			}
			if err := compose_v2.SaveStepResult(ctx, "questions", tokens); err != nil {
				return "", err
			}
		}
		if _, ok := compose_v2.GetStepResult[string](ctx, "answered A"); !ok {
			ev, err := ra.Run(ctx, sess, a2a.WithContinuation(schema.UserMessage("42"), tokens[0]))
			if err != nil {
				return "", err
			}
			if err := compose_v2.SaveStepResult(ctx, "answered A", finalText(ev)); err != nil {
				return "", err
			}
		}
		ev, err := ra.Run(ctx, sess, a2a.WithContinuation(schema.UserMessage("42"), tokens[1]))
		return finalText(ev), err
	})
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, schema.UserMessage("go"))
	approval, ok := compose_v2.IsInterrupt(err)
	if !ok || !strings.Contains(approval.Query.Question, "approve B?") {
		t.Fatalf("missing approval: %v", err)
	}
	ev, err := host.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		approval.Query.InterruptID: schema.UserMessage("approve"),
	}))
	if err != nil || finalText(ev) != "done B" {
		t.Fatalf("resume after checkpointing A must deliver the approval to B: got %q, err %v", finalText(ev), err)
	}
}
