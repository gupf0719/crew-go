package agui_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
)

type failAbortCleanupCP struct {
	*compose_v2.InMemoryCheckpointer
	fail bool
	sid  string
}

func (c *failAbortCleanupCP) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	c.sid = sid
	return c.InMemoryCheckpointer.SaveNS(ctx, sid, ns, cp)
}

func (c *failAbortCleanupCP) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	if c.fail {
		return 0, errors.New("temporary delete outage")
	}
	return c.InMemoryCheckpointer.DeleteSubtree(ctx, sid, ns)
}

func TestHandler_AbortCleanupRetry(t *testing.T) {
	for _, retryByRun := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "new_run"}[retryByRun], func(t *testing.T) {
			cp := &failAbortCleanupCP{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
			llm := &askThenAnswerModel{}
			h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
				return agent_v2.NewBasicAgent(agent_v2.WithName("abort-retry"), agent_v2.WithModel(llm), agent_v2.WithAskUser(), agent_v2.WithHooks(hooks))
			}, agui.WithCheckpointer(cp))
			start := `{"threadId":"abort-retry","runId":"start","messages":[{"role":"user","content":"hi"}]}`
			iid := findInterruptID(parseSSE(postAGUI(h, start).Body.String()))
			if iid == "" || cp.sid == "" {
				t.Fatal("fixture did not persist an interrupt")
			}
			sid := cp.sid
			cancelBody := `{"threadId":"abort-retry","runId":"cancel","resume":[{"interruptId":"` + iid + `","status":"cancelled"}]}`
			next := `{"threadId":"abort-retry","runId":"next","messages":[{"role":"user","content":"new task"}]}`

			cp.fail = true
			first := postAGUI(h, cancelBody).Body.String()
			if !strings.Contains(first, `"code":"abort_failed"`) || strings.Contains(first, `"type":"cancelled"`) ||
				strings.Contains(first, "temporary delete outage") {
				t.Fatalf("failed cleanup should give a sanitized error, got %s", first)
			}
			answer := `{"threadId":"abort-retry","runId":"answer","resume":[{"interruptId":"` + iid + `","status":"resolved","payload":"blue"}]}`
			for _, request := range []string{answer, next} {
				resp := postAGUI(h, request)
				if resp.Code != http.StatusConflict || !strings.Contains(resp.Body.String(), `"code":"cleanup_pending"`) {
					t.Fatalf("cleanup entry should protect the cancelled task, got %d %s", resp.Code, resp.Body)
				}
			}
			if llm.calls != 1 {
				t.Fatalf("cancelled task or replacement executed during cleanup outage: model calls=%d", llm.calls)
			}
			if saved, err := cp.LoadNS(t.Context(), sid, ""); err != nil || saved == nil {
				t.Fatalf("expected old checkpoint before retry, got %v err=%v", saved, err)
			}

			cp.fail = false
			if retryByRun {
				if body := postAGUI(h, next).Body.String(); llm.calls != 2 || !strings.Contains(body, `"type":"success"`) {
					t.Fatalf("new run should start after old checkpoint is cleaned, calls=%d, body=%s", llm.calls, body)
				}
			} else if body := postAGUI(h, cancelBody).Body.String(); !strings.Contains(body, `"type":"cancelled"`) {
				t.Fatalf("retry should finish cancellation, got %s", body)
			}
			if saved, err := cp.LoadNS(t.Context(), sid, ""); err != nil || saved != nil {
				t.Fatalf("checkpoint remained after successful retry, got %v err=%v", saved, err)
			}
		})
	}
}
