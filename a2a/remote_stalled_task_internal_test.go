package a2a

// 远端任务停在中间状态、却没有执行在跑（持久 task store + 远端重启），重新挂上时订阅只交回快照就正常关流（a2a-go 自
// v2.7.0、a2a-js 的服务端都这样）：不能当成办完了。曾经 consume 把它落到「流停在中间状态上」那一档，已聚合的空内容
// 当成功答复交出去、err == nil（审计实测）。另：流里给出 SDK 认不出的任务状态时也不能当成已完成、清掉句柄。

import (
	"context"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
)

// stalledStore：stalled 置位后 Get 把任务报成 working（模拟远端重启：任务留在 working、没有执行在跑）。
type stalledStore struct {
	*taskstore.InMemory
	stalled *atomic.Bool
}

func (s stalledStore) Get(ctx context.Context, id a2atypes.TaskID) (*taskstore.StoredTask, error) {
	st, err := s.InMemory.Get(ctx, id)
	if err != nil || st == nil || st.Task == nil || !s.stalled.Load() {
		return st, err
	}
	cp := *st
	t := *st.Task
	t.Status = a2atypes.TaskStatus{State: a2atypes.TaskStateWorking}
	cp.Task = &t
	return &cp, nil
}

// askThenDoneExec：新任务要人（input-required），续任务时完成并回 done:<答复>。
type askThenDoneExec struct{}

func (askThenDoneExec) Execute(_ context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2atypes.Event, error] {
	return func(yield func(a2atypes.Event, error) bool) {
		if execCtx.StoredTask != nil {
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted,
				a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("done:"+execCtx.Message.Parts[0].Text()))), nil)
			return
		}
		if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
			return
		}
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateInputRequired,
			a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("need more"))), nil)
	}
}

func (askThenDoneExec) Cancel(_ context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2atypes.Event, error] {
	return func(yield func(a2atypes.Event, error) bool) {
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCanceled, nil), nil)
	}
}

func TestReattach_StalledTaskIsAnErrorNotAnEmptyAnswer(t *testing.T) {
	oldGrace, oldPoll := stalledSubscribeGrace, authPollInterval
	stalledSubscribeGrace, authPollInterval = 300*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { stalledSubscribeGrace, authPollInterval = oldGrace, oldPoll })

	var stalled atomic.Bool
	store := stalledStore{InMemory: taskstore.NewInMemory(nil), stalled: &stalled}
	mux := http.NewServeMux()
	srv := httptest.NewUnstartedServer(mux)
	url := "http://" + srv.Listener.Addr().String()
	card := &a2atypes.AgentCard{Name: "Stalled", Description: "stalled remote",
		SupportedInterfaces: []*a2atypes.AgentInterface{a2atypes.NewAgentInterface(url+"/invoke", a2atypes.TransportProtocolJSONRPC)},
		DefaultInputModes:   []string{"text/plain"}, DefaultOutputModes: []string{"text/plain"},
		Capabilities: a2atypes.AgentCapabilities{Streaming: true}}
	mux.Handle("/invoke", a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(askThenDoneExec{}, a2asrv.WithTaskStore(store))))
	srv.Start()
	t.Cleanup(srv.Close)

	ra, err := NewRemoteAgentFromCard(context.Background(), card)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	sess := agent_v2.NewRunSession()
	if _, err := ra.Run(ctx, sess, schema.UserMessage("hi")); err != nil {
		t.Fatalf("前置：远端要人时默认模式交回追问，实际 %v", err)
	}
	taskID, _ := ra.loadIDs(sess)
	if taskID == "" || !ra.loadHandleWaiting(sess) {
		t.Fatalf("fixture 没搭对：句柄应留着、在等人，实际 task=%q", taskID)
	}

	stalled.Store(true) // 远端重启：任务留在 working、没有执行
	evs, err := ra.Run(ctx, sess, schema.UserMessage("more"))
	if err == nil {
		final := ""
		if m := agent_v2.FinalMessage(evs); m != nil {
			final = m.Content
		}
		t.Fatalf("远端没在办这个任务，不能当成办完了（曾经交回空答复、err == nil），实际 final=%q", final)
	}
	if !strings.Contains(err.Error(), "not working on it") {
		t.Fatalf("错误应说明远端没在办这个任务，实际 %v", err)
	}
	if got, _ := ra.loadIDs(sess); got != taskID {
		t.Fatalf("句柄应留着（task=%q），实际 %q", taskID, got)
	}

	// 远端恢复：同一个句柄续得上，答复照常送到
	stalled.Store(false)
	evs, err = ra.Run(ctx, sess, schema.UserMessage("more"))
	if err != nil {
		t.Fatalf("远端恢复之后应续上原来的任务：%v", err)
	}
	if m := agent_v2.FinalMessage(evs); m == nil || m.Content != "done:more" {
		t.Fatalf("应拿到原任务续上之后的答复 done:more，实际 %+v", m)
	}
}

// 远端流里给出一个本版本 SDK 认不出的任务状态（映射成 Unspecified）然后关流：按「流停在中间状态上」处理，句柄留着，
// 不跟「远端只回了一条消息、没有任务」混成已完成（曾经清掉句柄、把状态消息当最终答复，审计实测）。
func TestConsume_UnrecognizedTaskStateIsNotCompleted(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewUnstartedServer(mux)
	url := "http://" + srv.Listener.Addr().String()
	card := &a2atypes.AgentCard{Name: "p", Description: "p",
		SupportedInterfaces: []*a2atypes.AgentInterface{a2atypes.NewAgentInterface(url+"/invoke", a2atypes.TransportProtocolJSONRPC)},
		DefaultInputModes:   []string{"text/plain"}, DefaultOutputModes: []string{"text/plain"},
		Capabilities: a2atypes.AgentCapabilities{Streaming: true}}
	mux.HandleFunc("/invoke", func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		id := "1"
		if i := strings.Index(string(body), `"id":`); i >= 0 {
			rest := string(body)[i+5:]
			if j := strings.IndexAny(rest, ",}"); j > 0 {
				id = rest[:j]
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "data: {\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":{\"task\":{\"id\":\"t1\",\"contextId\":\"c1\",\"status\":{\"state\":\"TASK_STATE_SUBMITTED\"}}}}\n\n", id)
		fmt.Fprintf(w, "data: {\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":{\"statusUpdate\":{\"taskId\":\"t1\",\"contextId\":\"c1\",\"status\":{\"state\":\"TASK_STATE_PAUSED\",\"message\":{\"messageId\":\"m\",\"role\":\"ROLE_AGENT\",\"parts\":[{\"text\":\"paused\"}]}}}}}\n\n", id)
	})
	srv.Start()
	t.Cleanup(srv.Close)

	ra, err := NewRemoteAgentFromCard(context.Background(), card)
	if err != nil {
		t.Fatal(err)
	}
	sess := agent_v2.NewRunSession()
	evs, err := ra.Run(context.Background(), sess, schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run：%v", err)
	}
	if tid, _ := ra.loadIDs(sess); tid != "t1" {
		t.Fatalf("认不出的状态不算结束，句柄应留着（t1），实际 %q", tid)
	}
	if m := agent_v2.FinalMessage(evs); m != nil && m.Content == "paused" {
		t.Fatal("认不出的状态不该被当成已完成、把状态消息当最终答复")
	}
}
