package agui_test

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/tool"
)

type evictionAbortCP struct {
	*failAbortCleanupCP
	wantSID string
	badCtx  bool
}

func (c *evictionAbortCP) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	if sid == c.wantSID {
		deadline, bounded := ctx.Deadline()
		if ctx.Err() != nil || compose_v2.GetOwner(ctx) != "test-owner" || compose_v2.GetThreadID(ctx) != "old" || !bounded || time.Until(deadline) <= 0 || time.Until(deadline) > 11*time.Second {
			c.badCtx = true
		}
	}
	return c.failAbortCleanupCP.DeleteSubtree(ctx, sid, ns)
}

func TestHandler_EvictionCompletesCancelledTaskHistory(t *testing.T) {
	for _, tc := range []struct {
		name               string
		byTTL, customOwner bool
	}{{name: "capacity"}, {name: "TTL_other_owner", byTTL: true}, {name: "capacity_custom_history_owner", customOwner: true}, {name: "TTL_custom_history_owner", byTTL: true, customOwner: true}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				hs := agent_v2.NewInMemoryHistoryStore()
				cp := &evictionAbortCP{failAbortCleanupCP: &failAbortCleanupCP{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}}
				runs := 0
				tk := tool.NewToolkit()
				tk.Register(tool.NewFuncTool("transfer", "transfer", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) { runs++; return tool.Success("done"), nil }))
				opts := []agui.Option{agui.WithCheckpointer(cp)}
				if tc.byTTL {
					opts = append(opts, agui.WithSessionTTL(time.Second))
				} else {
					opts = append(opts, agui.WithMaxSessions(1))
				}
				historyOwner := "test-owner"
				historyOpt := agent_v2.WithHistory(hs, "tenant")
				if tc.customOwner {
					historyOwner = "history-test-owner"
					historyOpt = agent_v2.WithHistory(hs, "", agent_v2.WithHistoryOwnerFunc(func(ctx context.Context) string {
						owner, _ := ctx.Value(testOwnerKey{}).(string)
						return "history-" + owner
					}))
				}
				h := ownerAwareHandler(func(hooks hook.Hooks) agent_v2.Agent {
					return agent_v2.NewBasicAgent(agent_v2.WithName("evict"), agent_v2.WithModel(transferModel{}), agent_v2.WithToolkit(tk), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), historyOpt, agent_v2.WithHooks(hooks))
				}, opts...)
				iid := findInterruptID(parseSSE(postAs(h, "test-owner", `{"threadId":"old","messages":[{"role":"user","content":"转账"}]}`).Body.String()))
				if iid == "" {
					t.Fatal("no interrupt")
				}
				sid := cp.sid
				cp.wantSID = sid
				cp.fail = true
				body := postAs(h, "test-owner", `{"threadId":"old","resume":[{"interruptId":"`+iid+`","status":"cancelled"}]}`).Body.String()
				if !strings.Contains(body, `"code":"abort_failed"`) {
					t.Fatalf("cleanup did not fail: %s", body)
				}
				cp.fail = false
				newOwner := "test-owner"
				if tc.byTTL {
					time.Sleep(time.Second + time.Nanosecond)
					newOwner = "other-owner"
				}
				postAs(h, newOwner, `{"threadId":"new","messages":[{"role":"user","content":"hello"}]}`)
				exists, err := cp.ExistsNS(t.Context(), sid, "")
				if err != nil || exists {
					t.Fatalf("eviction did not remove old checkpoint: exists=%v err=%v", exists, err)
				}
				history, err := agent_v2.LoadAllHistory(t.Context(), hs, agent_v2.HistoryKey{Owner: historyOwner, ConvID: "old"})
				if err != nil {
					t.Fatal(err)
				}
				if !hasToolCall(history, "c1") || runs != 0 {
					t.Fatalf("fixture invalid: old history=%v tool runs=%d", history, runs)
				}
				t.Logf("checkpoint removed; tool_call remains=%v tool_result_present=%v", hasToolCall(history, "c1"), hasToolResult(history, "c1"))
				if !hasToolResult(history, "c1") {
					t.Fatal("eviction did not finish already-cancelled task history")
				}
				if cp.badCtx {
					t.Fatal("cleanup lost victim owner/thread identity or independent deadline")
				}
			})
		})
	}
}
