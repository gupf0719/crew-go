package agui_test

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

const outcomeInternalDetail = "fixture backend details"

type failedOutcomeCheckpointer struct {
	*compose_v2.InMemoryCheckpointer
}

func (*failedOutcomeCheckpointer) SaveNS(context.Context, string, string, *compose_v2.Checkpoint) error {
	return errors.New(outcomeInternalDetail)
}

func TestRunOutcome_UsesExecutionContextAndSafeErrors(t *testing.T) {
	for _, tc := range []struct {
		name      string
		cause     error
		cancel    bool
		saveFails bool
		needle    string
	}{
		{name: "business failure", needle: `"code":"agent_error"`},
		{name: "inner context canceled", cause: context.Canceled, needle: `"code":"agent_error"`},
		{name: "caller canceled during business failure", cancel: true, needle: `"outcome":{"type":"cancelled"}`},
		{name: "soft canceled", cause: compose_v2.ErrSoftCanceled, needle: `"outcome":{"type":"cancelled"}`},
		{name: "terminal failure", cause: compose_v2.ErrRunAborted, needle: `"code":"run_aborted"`},
		{name: "checkpoint write failure", saveFails: true, needle: `"code":"checkpoint_save_failed"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var sid string
			builder := compose_v2.NewBuilder()
			builder.AddNode("invoke", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				sid = compose_v2.GetSessionID(ctx)
				if tc.cancel {
					cancel()
				}
				if tc.saveFails {
					_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "continue?"})
					return nil, err
				}
				if tc.cause != nil {
					return nil, fmt.Errorf("%s: %w", outcomeInternalDetail, tc.cause)
				}
				return nil, errors.New(outcomeInternalDetail)
			})
			builder.AddEdge(compose_v2.StartNode, "invoke").AddEdge("invoke", compose_v2.EndNode)
			graph, err := builder.Compile()
			if err != nil {
				t.Fatal(err)
			}
			var cp compose_v2.Checkpointer = compose_v2.NewInMemoryCheckpointer()
			if tc.saveFails {
				cp = &failedOutcomeCheckpointer{compose_v2.NewInMemoryCheckpointer()}
			}
			h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
				return agent_v2.NewGraphAgent(graph, agent_v2.WithGraphAgentName("error-outcome"),
					agent_v2.WithGraphAgentHooks(hooks),
					agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
					agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return nil }))
			}, agui.WithCheckpointer(cp))
			w := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(ctx, "POST", "/", strings.NewReader(
				`{"threadId":"outcome","runId":"r","messages":[{"role":"user","content":"start"}]}`))
			h.ServeHTTP(w, req)
			body := w.Body.String()
			if !strings.Contains(body, tc.needle) || strings.Contains(body, outcomeInternalDetail) {
				t.Fatalf("期望终态 %s 且不输出内部错误，实际 %s", tc.needle, body)
			}
			if sid == "" {
				t.Fatal("业务节点没有执行")
			}
			if saved, err := cp.LoadNS(t.Context(), sid, ""); err != nil || saved != nil {
				t.Fatalf("终态应清理本次执行的存档，实际 %+v 错误 %v", saved, err)
			}
		})
	}
}

func TestRunOutcome_PanicRejectsWithoutInternalDetail(t *testing.T) {
	const internalDetail = "fixture factory details"
	h := ownedHandler(func(hook.Hooks) agent_v2.Agent { panic(internalDetail) })
	body := postAGUI(h, `{"threadId":"outcome","runId":"r","messages":[{"role":"user","content":"start"}]}`).Body.String()
	if !strings.Contains(body, `"code":"panic"`) || strings.Contains(body, internalDetail) {
		t.Fatalf("工厂 panic 应有固定错误内容，实际 %s", body)
	}
}
