package a2a_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// 网关仅阻断 GetTask，并丢掉第一次补答的响应。远端已经处理该答复并切到下一问。
func startReadDeniedAfterLostReply(t *testing.T) (string, *recRemote, *atomic.Bool) {
	t.Helper()
	r := &recRemote{}
	allowRead := &atomic.Bool{}
	var lost atomic.Bool
	exec := fnExec(func(ctx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
		input := inputText(ctx)
		if ctx.StoredTask == nil {
			r.record("new:" + input)
			if yield(a2atypes.NewSubmittedTask(ctx, ctx.Message), nil) {
				yield(a2atypes.NewStatusUpdateEvent(ctx, a2atypes.TaskStateInputRequired,
					a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("第一问"))), nil)
			}
			return
		}
		r.record("cont:" + input)
		if r.conts.Add(1) == 1 {
			yield(a2atypes.NewStatusUpdateEvent(ctx, a2atypes.TaskStateInputRequired,
				a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("第二问"))), nil)
			return
		}
		yield(a2atypes.NewStatusUpdateEvent(ctx, a2atypes.TaskStateCompleted,
			a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("done:"+input))), nil)
	})
	lose := loseFirstContinuation(&lost)
	url := interceptServer(t, exec, func(w http.ResponseWriter, req *http.Request, body []byte, mux http.Handler) bool {
		var call struct{ Method string }
		if json.Unmarshal(body, &call) == nil && call.Method == "GetTask" && !allowRead.Load() {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return true
		}
		return lose(w, req, body, mux)
	})
	return url, r, allowRead
}

func TestJSONRPCGetTaskDeniedAfterLostReplyPreservesContinuation(t *testing.T) {
	url, remote, allowRead := startReadDeniedAfterLostReply(t)
	agent, err := a2a.NewRemoteAgent(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	session := agent_v2.NewRunSession()
	events, err := agent.Run(t.Context(), session, schema.UserMessage("start"))
	token := a2a.Continuation(agent_v2.FinalMessage(events))
	if err != nil || token == "" {
		t.Fatalf("第一问未返回续接句柄: %v", err)
	}
	reply := a2a.WithContinuation(schema.UserMessage("reply-one"), token)
	if _, err := agent.Run(t.Context(), session, reply); err == nil {
		t.Fatal("第一次答复的响应应丢失")
	}
	if _, err := agent.Run(t.Context(), session, reply); !errors.Is(err, a2atypes.ErrUnauthorized) || remote.conts.Load() != 1 {
		t.Fatalf("无法对账时旧答案被重发: err=%v, messages=%v", err, remote.snapshot())
	}
	allowRead.Store(true)
	events, err = agent.Run(t.Context(), session, reply)
	next := agent_v2.FinalMessage(events)
	if err != nil || next == nil || !strings.HasSuffix(next.Content, "第二问") || a2a.Continuation(next) == "" || a2a.Continuation(next) == token || remote.conts.Load() != 1 {
		t.Fatalf("应取回远端第二问: output=%v err=%v messages=%v", next, err, remote.snapshot())
	}
	events, err = agent.Run(t.Context(), session, a2a.WithContinuation(schema.UserMessage("reply-two"), a2a.Continuation(next)))
	if final := agent_v2.FinalMessage(events); err != nil || final == nil || final.Content != "done:reply-two" {
		t.Fatalf("第二问续接失败: %v, %v", final, err)
	}
}

func TestJSONRPCGetTaskDeniedAfterLostReplySurvivesHostResume(t *testing.T) {
	url, remote, allowRead := startReadDeniedAfterLostReply(t)
	agent, err := a2a.NewRemoteAgent(t.Context(), url, a2a.WithHumanGate())
	if err != nil {
		t.Fatal(err)
	}
	host := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("host")).AddStep("remote", agent)
	ctx := compose_v2.WithCheckpointerCtx(t.Context(), compose_v2.NewInMemoryCheckpointer())
	session := agent_v2.NewRunSession()
	_, err = host.Run(ctx, session, schema.UserMessage("start"))
	first, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("没有收到第一问: %v", err)
	}
	answer := agent_v2.WithResumeUserInputs(map[string]*schema.Message{first.Query.InterruptID: schema.UserMessage("reply-one")})
	if _, err := host.Resume(ctx, session.RunID, answer); err == nil {
		t.Fatal("第一次答复的响应应丢失")
	}
	if _, err := host.Resume(ctx, session.RunID, answer); !errors.Is(err, a2atypes.ErrUnauthorized) || remote.conts.Load() != 1 {
		t.Fatalf("宿主重建 session 后旧答复被重发: err=%v, messages=%v", err, remote.snapshot())
	}
	allowRead.Store(true)
	_, err = host.Resume(ctx, session.RunID, answer)
	second, ok := compose_v2.IsInterrupt(err)
	if !ok || second.Query.Question != "第二问" || first.Query.InterruptID == second.Query.InterruptID || remote.conts.Load() != 1 {
		t.Fatalf("应取回身份独立的第二问: err=%v, messages=%v", err, remote.snapshot())
	}
}

func TestJSONRPCGetTaskDeniedForLegacyCheckpointNeedsReconciliation(t *testing.T) {
	url, remote, allowRead := startReadDeniedAfterLostReply(t)
	agent, err := a2a.NewRemoteAgent(t.Context(), url, a2a.WithHumanGate())
	if err != nil {
		t.Fatal(err)
	}
	host := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("host")).AddStep("remote", agent)
	checkpointer := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(t.Context(), checkpointer)
	session := agent_v2.NewRunSession()
	_, err = host.Run(ctx, session, schema.UserMessage("start"))
	first, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("没有收到第一问: %v", err)
	}
	cp, err := checkpointer.LoadNS(ctx, session.RunID, "")
	if err != nil || cp == nil {
		t.Fatalf("读取宿主存档: %v", err)
	}
	body, err := compose_v2.MarshalCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}
	// 旧存档没有投递状态，可能已经向远端发送过旧答复。
	const marker = `,"answer_unsent":true`
	if !strings.Contains(string(body), marker) {
		t.Fatal("没有在存档中找到首次投递标记")
	}
	legacy, err := compose_v2.UnmarshalCheckpoint([]byte(strings.Replace(string(body), marker, "", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkpointer.SaveNS(ctx, session.RunID, "", legacy); err != nil {
		t.Fatal(err)
	}
	answer := agent_v2.WithResumeUserInputs(map[string]*schema.Message{first.Query.InterruptID: schema.UserMessage("reply-one")})
	if _, err := host.Resume(ctx, session.RunID, answer); !errors.Is(err, a2atypes.ErrUnauthorized) || remote.conts.Load() != 0 {
		t.Fatalf("旧档投递状态未知时应先对账: err=%v, messages=%v", err, remote.snapshot())
	}
	allowRead.Store(true)
	if _, err := host.Resume(ctx, session.RunID, answer); err == nil || remote.conts.Load() != 1 {
		t.Fatalf("恢复读取权限后应投递这次答复: err=%v, messages=%v", err, remote.snapshot())
	}
}

func TestHumanGate_JSONRPC_GetTaskAuthRejectionByStatusStillSends(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			r := &recRemote{}
			exec := fnExec(func(ctx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
				input := inputText(ctx)
				if ctx.StoredTask == nil {
					r.record("new:" + input)
					if yield(a2atypes.NewSubmittedTask(ctx, ctx.Message), nil) {
						yield(a2atypes.NewStatusUpdateEvent(ctx, a2atypes.TaskStateInputRequired,
							a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("请补充信息"))), nil)
					}
					return
				}
				r.record("cont:" + input)
				yield(a2atypes.NewStatusUpdateEvent(ctx, a2atypes.TaskStateCompleted,
					a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("done:"+input))), nil)
			})
			var rejected atomic.Int32
			url := interceptServer(t, exec, func(w http.ResponseWriter, _ *http.Request, body []byte, _ http.Handler) bool {
				var req struct{ Method string }
				if json.Unmarshal(body, &req) != nil || req.Method != "GetTask" {
					return false
				}
				rejected.Add(1)
				http.Error(w, http.StatusText(status), status)
				return true
			})
			asked, final, err := driveGate(t, url)
			if rejected.Load() == 0 {
				t.Fatal("未经过拒绝 GetTask 的网关")
			}
			if got, want := r.snapshot(), []string{"new:原始请求", "cont:答#1"}; err != nil || final != "done:答#1" || len(asked) != 1 || !slices.Equal(got, want) {
				t.Fatalf("续接结果 final=%q err=%v asked=%v，远端收到 %q，期望 %q", final, err, asked, got, want)
			}
		})
	}
}
