package a2a_test

import (
	"context"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

func checkGateReplacementWire(t *testing.T, r *gateRemote) {
	t.Helper()
	requests := r.requests()
	if len(requests) != 4 {
		t.Fatalf("预期发出首发、续答、重发、续答共四条消息，实际 %+v", requests)
	}
	initial, answer, replacement, nextAnswer := requests[0], requests[1], requests[2], requests[3]
	if initial.task == "" || initial.context == "" || initial.target != "" || initial.requestContext != "" || len(initial.references) != 0 {
		t.Errorf("首发应开独立任务，实际 %+v", initial)
	}
	if replacement.task == "" || replacement.task == initial.task || replacement.messageID == initial.messageID ||
		replacement.target != "" || replacement.requestContext != initial.context ||
		!slices.Equal(replacement.references, []a2atypes.TaskID{initial.task}) {
		t.Errorf("重发应开新任务、保留原线程并引用失败任务，实际 %+v，旧任务 %+v", replacement, initial)
	}
	for _, pair := range [][2]gateWireMessage{{initial, answer}, {replacement, nextAnswer}} {
		start, reply := pair[0], pair[1]
		if reply.target != start.task || reply.task != start.task || reply.requestContext != start.context || len(reply.references) != 0 {
			t.Errorf("答复应只续其当前待答任务，实际 %+v，待答 %+v", reply, start)
		}
	}
}

// 失败结果从 GetTask 对账取得，宿主的重试也只能在下一条新请求上加引用。
func TestHumanGate_LostTerminalResponse_ReferencesFailedTask(t *testing.T) {
	for _, terminal := range []a2atypes.TaskState{a2atypes.TaskStateFailed, a2atypes.TaskStateRejected, a2atypes.TaskStateCanceled} {
		t.Run(string(terminal), func(t *testing.T) {
			r := &gateRemote{onCont: func(n int32) a2atypes.TaskState {
				if n == 1 {
					return terminal
				}
				return a2atypes.TaskStateCompleted
			}}
			url, lost := startLostResponseServer(t, fnExec(r.exec))
			asked, ids, final, err := gateRun(t, url, true)
			if !lost.Load() {
				t.Fatal("测试没有丢掉第一次答复的终局响应")
			}
			checkAskedAgainForNewTask(t, r, asked, ids, final, err)
		})
	}
}

func terminalReferenceHost(t *testing.T, url string, opts ...a2a.RemoteOption) *agent_v2.SequentialAgent {
	t.Helper()
	ra, err := a2a.NewRemoteAgent(t.Context(), url, append([]a2a.RemoteOption{a2a.WithHumanGate()}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("gate_ref")).AddStep("remote", ra)
}

func failFirstGate(t *testing.T, ctx context.Context, host *agent_v2.SequentialAgent, s *agent_v2.RunSession) string {
	t.Helper()
	_, err := host.Run(ctx, s, schema.UserMessage("原始请求"))
	pause, ok := compose_v2.IsInterrupt(err)
	if !ok || len(pause.AllQueries()) != 1 {
		t.Fatalf("预期远端首发挂起，实际 %v", err)
	}
	id := pause.AllQueries()[0].InterruptID
	_, err = host.Resume(ctx, s.RunID, agent_v2.WithResumeSession(s),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{id: schema.UserMessage("答#1")}))
	if err == nil || !strings.Contains(err.Error(), "task ended "+string(a2atypes.TaskStateFailed)) {
		t.Fatalf("预期旧任务已经终局失败，实际 %v", err)
	}
	return id
}

func failThenComplete(n int32) a2atypes.TaskState {
	if n == 1 {
		return a2atypes.TaskStateFailed
	}
	return a2atypes.TaskStateCompleted
}

func TestHumanGate_TerminalReferenceSurvivesCheckpointRoundTrip(t *testing.T) {
	r := &gateRemote{onCont: failThenComplete}
	url := startServer(t, fnExec(r.exec))
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithDurabilityCtx(compose_v2.WithCheckpointerCtx(t.Context(), cp), compose_v2.DurabilitySync)
	s := agent_v2.NewRunSession()
	oldQuestion := failFirstGate(t, ctx, terminalReferenceHost(t, url), s)
	saved, err := cp.LoadNS(ctx, s.RunID, "")
	if err != nil || saved == nil {
		t.Fatalf("应保留宿主失败存档，实际 %v", err)
	}
	wire, err := compose_v2.MarshalCheckpoint(saved)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := compose_v2.UnmarshalCheckpoint(wire)
	if err != nil {
		t.Fatal(err)
	}
	reopened := compose_v2.NewInMemoryCheckpointer()
	if err := reopened.SaveNS(ctx, s.RunID, "", restored); err != nil {
		t.Fatal(err)
	}
	ctx = compose_v2.WithCheckpointerCtx(t.Context(), reopened)
	host := terminalReferenceHost(t, url)
	_, err = host.Resume(ctx, s.RunID)
	pause, ok := compose_v2.IsInterrupt(err)
	if !ok || len(pause.AllQueries()) != 1 || pause.AllQueries()[0].InterruptID == oldQuestion {
		t.Fatalf("还原存档后应由新任务重新提问，实际 %v", err)
	}
	newQuestion := pause.AllQueries()[0].InterruptID
	events, err := host.Resume(ctx, s.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{newQuestion: schema.UserMessage("答#2")}))
	checkAskedAgainForNewTask(t, r, []string{"批准吗", pause.AllQueries()[0].Question}, []string{oldQuestion, newQuestion}, finalText(events), err)
}

type changeFailedGatePayload struct{ hook.NoOpHooks }

func (changeFailedGatePayload) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	in.Messages = []*schema.Message{schema.UserMessage("另一件事")}
	return ctx, nil
}

type rewriteReferenceRequest struct {
	tenant, text      string
	template          *a2atypes.Message
	dropAnswerContext bool
}

func (r rewriteReferenceRequest) Before(ctx context.Context, req *a2aclient.Request) (context.Context, any, error) {
	switch payload := req.Payload.(type) {
	case *a2atypes.SendMessageRequest:
		if r.dropAnswerContext && payload.Message.TaskID != "" {
			return context.Background(), nil, nil
		}
		payload.Tenant = r.tenant
		if r.template != nil && payload.Message.TaskID == "" {
			payload.Message = r.template
		}
		if r.text != "" && payload.Message.TaskID == "" {
			payload.Message.Parts = a2atypes.ContentParts{a2atypes.NewTextPart(r.text)}
		}
	case *a2atypes.GetTaskRequest:
		payload.Tenant = r.tenant
	}
	return ctx, nil, nil
}

func (rewriteReferenceRequest) After(context.Context, *a2aclient.Response) error { return nil }

func TestHumanGate_ReferenceNeedsScopeOfTerminalOperation(t *testing.T) {
	r := &gateRemote{onCont: failThenComplete}
	url := startServer(t, fnExec(r.exec))
	ctx := compose_v2.WithCheckpointerCtx(t.Context(), compose_v2.NewInMemoryCheckpointer())
	s := agent_v2.NewRunSession()
	host := terminalReferenceHost(t, url,
		a2a.WithClientOptions(a2aclient.WithCallInterceptors(rewriteReferenceRequest{dropAnswerContext: true})))
	failFirstGate(t, ctx, host, s)
	_, err := host.Resume(ctx, s.RunID, agent_v2.WithResumeSession(s))
	if _, waiting := compose_v2.IsInterrupt(err); !waiting {
		t.Fatalf("重发后应产生新任务的提问，实际 %v", err)
	}
	requests := r.requests()
	if len(requests) != 3 || requests[1].target != requests[0].task || requests[2].target != "" ||
		len(requests[2].references) != 0 {
		t.Fatalf("不能将查询的归属用来认证后续失去上下文的续答，实际 %+v", requests)
	}
}

func TestHumanGate_TerminalReferenceLeavesSDKMessageTemplateUnchanged(t *testing.T) {
	r := &gateRemote{onCont: failThenComplete}
	url := startServer(t, fnExec(r.exec))
	ctx := compose_v2.WithCheckpointerCtx(t.Context(), compose_v2.NewInMemoryCheckpointer())
	s := agent_v2.NewRunSession()
	failFirstGate(t, ctx, terminalReferenceHost(t, url), s)
	template := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("原始请求"))
	host := terminalReferenceHost(t, url,
		a2a.WithClientOptions(a2aclient.WithCallInterceptors(rewriteReferenceRequest{template: template})))
	_, err := host.Resume(ctx, s.RunID, agent_v2.WithResumeSession(s))
	if _, waiting := compose_v2.IsInterrupt(err); !waiting {
		t.Fatalf("重发后应在新任务中提问，实际 %v", err)
	}
	requests := r.requests()
	if len(requests) != 3 || !slices.Equal(requests[2].references, []a2atypes.TaskID{requests[0].task}) {
		t.Fatalf("线上重发应带旧任务引用，实际 %+v", requests)
	}
	if template.TaskID != "" || template.ContextID != "" || len(template.ReferenceTasks) != 0 {
		t.Fatalf("线上重发改写了业务消息模板：%+v", template)
	}
}

func TestHumanGate_TerminalReferenceStaysWithOriginalRequest(t *testing.T) {
	for _, change := range []string{"payload", "owner", "tenant", "thread", "wire-payload", "wire-tenant"} {
		t.Run(change, func(t *testing.T) {
			r := &gateRemote{onCont: failThenComplete}
			url := startServer(t, fnExec(r.exec))
			ctx := compose_v2.WithOwner(t.Context(), "owner-a")
			ctx = compose_v2.WithThreadIDCtx(ctx, "thread-a")
			ctx = a2atypes.AttachTenant(ctx, "tenant-a")
			ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
			s := agent_v2.NewRunSession()
			initial := []a2a.RemoteOption{}
			if change == "wire-tenant" {
				initial = append(initial, a2a.WithClientOptions(a2aclient.WithCallInterceptors(rewriteReferenceRequest{tenant: "wire-a"})))
			}
			host := terminalReferenceHost(t, url, initial...)
			failFirstGate(t, ctx, host, s)

			switch change {
			case "payload":
				host = terminalReferenceHost(t, url, a2a.WithHooks(changeFailedGatePayload{}))
			case "owner":
				ctx = compose_v2.WithOwner(ctx, "owner-b")
			case "tenant":
				ctx = a2atypes.AttachTenant(ctx, "tenant-b")
			case "thread":
				ctx = compose_v2.WithThreadIDCtx(ctx, "thread-b")
			case "wire-payload":
				host = terminalReferenceHost(t, url,
					a2a.WithClientOptions(a2aclient.WithCallInterceptors(rewriteReferenceRequest{text: "另一件事"})))
			case "wire-tenant":
				host = terminalReferenceHost(t, url,
					a2a.WithClientOptions(a2aclient.WithCallInterceptors(rewriteReferenceRequest{tenant: "wire-b"})))
			}
			// 保留原 session，验证旧线程在身份切换时也不会从 session 缓存带过去。
			events, err := host.Resume(ctx, s.RunID, agent_v2.WithResumeSession(s))
			payloadChanged := change == "payload" || change == "wire-payload"
			if payloadChanged {
				if err != nil || finalText(events) != "新任务办了:另一件事" {
					t.Fatalf("新请求应正常执行，实际 %q, %v", finalText(events), err)
				}
			} else if _, pending := compose_v2.IsInterrupt(err); !pending {
				t.Fatalf("新作用域中的原始请求应开启新任务，实际 %v", err)
			}
			requests := r.requests()
			if len(requests) != 3 || requests[2].target != "" || requests[2].task == requests[0].task || len(requests[2].references) != 0 {
				t.Fatalf("已切换的请求不应带旧引用，实际 %+v", requests)
			}
			if !payloadChanged && requests[2].requestContext != "" {
				t.Fatalf("切换作用域后带上了旧线程：%+v", requests[2])
			}
		})
	}
}

func TestHumanGate_TopLevelTerminalReferenceUsesOriginalRun(t *testing.T) {
	for _, newTask := range []bool{false, true} {
		name := "retry"
		if newTask {
			name = "next-task"
		}
		t.Run(name, func(t *testing.T) {
			r := &gateRemote{onCont: failThenComplete}
			ra, err := a2a.NewRemoteAgent(t.Context(), startServer(t, fnExec(r.exec)), a2a.WithHumanGate())
			if err != nil {
				t.Fatal(err)
			}
			s := agent_v2.NewRunSession()
			_, err = ra.Run(t.Context(), s, schema.UserMessage("原始请求"))
			if _, waiting := compose_v2.IsInterrupt(err); !waiting {
				t.Fatalf("首发应产生待答问题，实际 %v", err)
			}
			_, err = ra.Run(t.Context(), s, schema.UserMessage("答#1"))
			if err == nil || !strings.Contains(err.Error(), "task ended "+string(a2atypes.TaskStateFailed)) {
				t.Fatalf("续答后应看到终局，实际 %v", err)
			}
			if newTask {
				s.NextTask()
			}
			_, err = ra.Run(t.Context(), s, schema.UserMessage("原始请求"))
			if _, waiting := compose_v2.IsInterrupt(err); !waiting {
				t.Fatalf("应挂起新任务，实际 %v", err)
			}
			if newTask {
				requests := r.requests()
				if len(requests) != 3 || requests[2].target != "" || len(requests[2].references) != 0 ||
					requests[2].requestContext != requests[0].context || requests[2].task == requests[0].task {
					t.Fatalf("NextTask 保留线程并发新委派，实际 %+v", requests)
				}
				return
			}
			events, err := ra.Run(t.Context(), s, schema.UserMessage("答#2"))
			if err != nil || finalText(events) != "续任务办了:答#2" {
				t.Fatalf("新问题应接收新答案，实际 %q, %v", finalText(events), err)
			}
			checkGateReplacementWire(t, r)
		})
	}
}

func TestRemoteAgent_TerminalReferenceUsesSelectedInterface(t *testing.T) {
	first, other := &gateRemote{}, &gateRemote{}
	primary := startServer(t, fnExec(func(e *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
		first.record(e, inputText(e))
		if yield(a2atypes.NewSubmittedTask(e, e.Message), nil) {
			yield(a2atypes.NewStatusUpdateEvent(e, a2atypes.TaskStateFailed, nil), nil)
		}
	}))
	rest := httptest.NewServer(a2asrv.NewRESTHandler(a2asrv.NewHandler(fnExec(other.exec))))
	t.Cleanup(rest.Close)
	card := &a2atypes.AgentCard{
		Name: "same_card", Capabilities: a2atypes.AgentCapabilities{Streaming: true},
		SupportedInterfaces: []*a2atypes.AgentInterface{
			a2atypes.NewAgentInterface(primary+"/invoke", a2atypes.TransportProtocolJSONRPC),
			a2atypes.NewAgentInterface(rest.URL, a2atypes.TransportProtocolHTTPJSON),
		},
	}
	host := func(transport a2atypes.TransportProtocol) *agent_v2.SequentialAgent {
		ra, err := a2a.NewRemoteAgentFromCard(t.Context(), card, a2a.WithHumanGate(),
			a2a.WithClientOptions(a2aclient.WithConfig(a2aclient.Config{PreferredTransports: []a2atypes.TransportProtocol{transport}})))
		if err != nil {
			t.Fatal(err)
		}
		return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("same_graph")).AddStep("remote", ra)
	}
	ctx := compose_v2.WithCheckpointerCtx(t.Context(), compose_v2.NewInMemoryCheckpointer())
	s := agent_v2.NewRunSession()
	if _, err := host(a2atypes.TransportProtocolJSONRPC).Run(ctx, s, schema.UserMessage("检查任务")); err == nil {
		t.Fatal("第一个接口应返回远端终局失败")
	}
	events, err := host(a2atypes.TransportProtocolHTTPJSON).Resume(ctx, s.RunID, agent_v2.WithResumeSession(s))
	if err != nil || finalText(events) != "新任务办了:检查任务" {
		t.Fatalf("同一张名片选中的新接口应处理全新任务，实际 %q, %v", finalText(events), err)
	}
	requests := other.requests()
	if len(first.requests()) != 1 || len(requests) != 1 || requests[0].target != "" ||
		requests[0].requestContext != "" || len(requests[0].references) != 0 {
		t.Fatalf("名片相同也不得把旧引用送到另一个接口，实际 %+v", requests)
	}
}

func TestRemoteAgent_InitialFailureReferencesTaskOnRetry(t *testing.T) {
	r := &gateRemote{}
	url := startServer(t, fnExec(func(e *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
		if len(r.requests()) == 0 {
			r.record(e, inputText(e))
			if yield(a2atypes.NewSubmittedTask(e, e.Message), nil) {
				yield(a2atypes.NewStatusUpdateEvent(e, a2atypes.TaskStateFailed, nil), nil)
			}
			return
		}
		r.exec(e, yield)
	}))
	ra, err := a2a.NewRemoteAgent(t.Context(), url, a2a.WithHumanGate())
	if err != nil {
		t.Fatal(err)
	}
	host := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("retry"),
		agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(1))).AddStep("remote", ra)
	events, err := host.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("检查任务"))
	if err != nil || finalText(events) != "新任务办了:检查任务" {
		t.Fatalf("初次失败后应重发原请求，实际 %q, %v", finalText(events), err)
	}
	requests := r.requests()
	if len(requests) != 2 || requests[0].target != "" || len(requests[0].references) != 0 ||
		requests[1].target != "" || !slices.Equal(requests[1].references, []a2atypes.TaskID{requests[0].task}) ||
		requests[1].requestContext != requests[0].context {
		t.Fatalf("首次即终局的任务也应在重发时被引用：%+v", requests)
	}
}
