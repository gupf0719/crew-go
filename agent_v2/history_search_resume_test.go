package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

type conversationConsentKey struct{}

type previousRequestStartHook struct {
	hook.NoOpHooks
	previous context.Context
}

func (h previousRequestStartHook) OnAgentStart(context.Context, *hook.AgentInfo, *hook.AgentInput) (context.Context, error) {
	return h.previous, nil
}

type searchReadCounter struct {
	agent_v2.HistoryStore
	reads atomic.Int32
}

func (s *searchReadCounter) Load(ctx context.Context, key agent_v2.HistoryKey, q agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	if q.Agent != "" && q.Limit == 128 {
		s.reads.Add(1)
	}
	return s.HistoryStore.Load(ctx, key, q)
}

func recallCall(callID, name, args string) schema.ToolCall {
	return schema.ToolCall{ID: callID, Function: schema.FunctionCall{Name: name, Arguments: args}}
}

func recallResult(messages []*schema.Message, id string) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if schema.GetToolResultID(messages[i]) == id {
			return messages[i].Content
		}
	}
	return ""
}

func TestHistorySearchResumeRechecksCurrentConsent(t *testing.T) {
	for _, stage := range []string{"sibling pause", "stale middleware", "outer hook"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
			ctx = compose_v2.WithThreadIDCtx(compose_v2.WithOwner(ctx, "review-team"), "issues")
			granted := context.WithValue(ctx, conversationConsentKey{}, true)
			hs := &searchReadCounter{HistoryStore: agent_v2.NewInMemoryHistoryStore()}
			old := schema.UserMessage("original reference AGENT-202")
			member := "reviewer"
			if stage == "outer hook" {
				member = "review"
			}
			old.Annotations = map[string]any{
				agent_v2.HistoryAgentAnnotation: member, agent_v2.HistoryTaskAnnotation: "earlier-run",
			}
			if err := hs.Append(ctx, agent_v2.HistoryKey{Owner: "review-team", ConvID: "issues"}, []*schema.Message{old}); err != nil {
				t.Fatal(err)
			}
			wait := func(ctx context.Context) error {
				_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "可以继续？"})
				return err
			}
			calls := []schema.ToolCall{recallCall("lookup", "search_history", `{"query":"AGENT-202"}`)}
			opts := []agent_v2.Option{
				agent_v2.WithName("reviewer"), agent_v2.WithHistory(hs, "static"),
				agent_v2.WithHistorySearch(func(ctx context.Context, key agent_v2.HistoryKey) bool {
					ok, _ := ctx.Value(conversationConsentKey{}).(bool)
					return ok && key.Owner == compose_v2.GetOwner(ctx)
				}),
				agent_v2.WithModel(&fnModel{fn: func(_ context.Context, messages []*schema.Message) (*schema.Message, error) {
					if got := recallResult(messages, "lookup"); got != "" {
						return schema.AssistantMessage(got), nil
					}
					return schema.AssistantToolCallMessage(calls), nil
				}}),
			}
			if stage != "stale middleware" {
				kit := tool.NewToolkit()
				kit.Register(tool.NewFuncTool("pause_for_user", "等用户确认", map[string]any{"type": "object"},
					func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
						if err := wait(ctx); err != nil {
							return nil, err
						}
						return tool.Success("done"), nil
					}))
				calls = append(calls, recallCall("pause", "pause_for_user", `{}`))
				opts = append(opts, agent_v2.WithToolkit(kit))
			} else {
				opts = append(opts, agent_v2.WithToolMiddleware(func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
					return func(ctx context.Context, name string, args map[string]any) (*tool.Result, error) {
						// 中间件重用同一用户的旧 ctx。授权仍应取这次的请求值。
						res, err := next(granted, name, args)
						if err != nil || res == nil || !res.Success {
							return res, err
						}
						if err := wait(ctx); err != nil {
							return nil, err
						}
						return res, nil
					}
				}))
			}
			var a agent_v2.Agent = agent_v2.NewBasicAgent(opts...)
			if stage == "outer hook" {
				a = agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("review-flow"),
					agent_v2.WithSequentialHooks(previousRequestStartHook{previous: granted})).AddStep("review", a)
			}
			session := agent_v2.NewRunSession()
			_, err := a.Run(granted, session, schema.UserMessage("查一下旧编号"))
			interrupt, ok := compose_v2.IsInterrupt(err)
			if !ok || len(interrupt.AllQueries()) != 1 || hs.reads.Load() != 1 {
				t.Fatalf("首次查到历史后应挂起: reads=%d err=%v", hs.reads.Load(), err)
			}
			answer := agent_v2.WithResumeUserInputs(map[string]*schema.Message{
				interrupt.AllQueries()[0].InterruptID: schema.UserMessage("继续"),
			})
			if stage == "sibling pause" {
				if _, err := a.Resume(compose_v2.WithOwner(ctx, "other-team"), session.RunID, answer); !errors.Is(err, compose_v2.ErrGraphIDConflict) {
					t.Fatalf("其他主体应无法继续这个任务: %v", err)
				}
			}
			events, err := a.Resume(ctx, session.RunID, answer)
			if err != nil {
				t.Fatal(err)
			}
			if final := agent_v2.FinalMessage(events); final == nil || !strings.Contains(final.Content, "history_search_denied") ||
				strings.Contains(final.Content, "AGENT-202") || hs.reads.Load() != 1 {
				t.Fatalf("撤回权限后泄漏了查询结果: final=%v reads=%d", final, hs.reads.Load())
			}
		})
	}
}
