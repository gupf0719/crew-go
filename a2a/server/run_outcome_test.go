package server_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/a2a/server"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

func TestServer_TerminalFailureCodeSurvivesSubscriptionAndTaskRead(t *testing.T) {
	const internalDetail = "internal failure details from graph"
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	defer release()
	b := compose_v2.NewBuilder().WithName("terminal_failure")
	b.AddNode("work", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		select {
		case <-gate:
			return nil, fmt.Errorf("%w: %s", compose_v2.ErrRunAborted, internalDetail)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	b.AddEdge(compose_v2.StartNode, "work")
	b.AddEdge("work", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ag := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("terminal_failure"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return nil }))
	base := serveAgent(t, ag)
	client, err := a2aclient.NewFromCard(ctx, server.Card(ag, base))
	if err != nil {
		t.Fatal(err)
	}

	taskID := make(chan a2atypes.TaskID, 1)
	type streamResult struct {
		status a2atypes.TaskStatus
		err    error
	}
	sent := make(chan streamResult, 1)
	go func() {
		revealedID := false
		var result streamResult
		req := &a2atypes.SendMessageRequest{
			Message: a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("run")),
		}
		for ev, err := range client.SendStreamingMessage(ctx, req) {
			if err != nil {
				result.err = err
				break
			}
			switch event := ev.(type) {
			case *a2atypes.Task:
				if !revealedID {
					taskID <- event.ID
					revealedID = true
				}
				result.status = event.Status
			case *a2atypes.TaskStatusUpdateEvent:
				result.status = event.Status
			}
		}
		sent <- result
	}()

	var id a2atypes.TaskID
	select {
	case id = <-taskID:
	case result := <-sent:
		t.Fatalf("执行流在任务建立前结束，状态 %v，错误 %v", result.status, result.err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	// 订阅拿到在途任务快照后才放行图节点。确保断言的是订阅收到的终态。
	released := false
	var subscribed a2atypes.TaskStatus
	for ev, err := range client.SubscribeToTask(ctx, &a2atypes.SubscribeToTaskRequest{ID: id}) {
		if err != nil {
			t.Fatalf("订阅失败：%v", err)
		}
		switch event := ev.(type) {
		case *a2atypes.Task:
			subscribed = event.Status
			if !released {
				if event.ID != id || event.Status.State == a2atypes.TaskStateFailed {
					t.Fatalf("订阅没有取得在途任务快照：%+v", event)
				}
				release()
				released = true
			}
		case *a2atypes.TaskStatusUpdateEvent:
			subscribed = event.Status
		}
	}

	assertFailure := func(source string, st a2atypes.TaskStatus) {
		t.Helper()
		if st.State != a2atypes.TaskStateFailed || st.Message == nil || st.Message.Metadata["crew.errorCode"] != "run_aborted" {
			t.Fatalf("%s 应投影 terminal failure 及错误码，实际 %+v", source, st)
		}
		if text, _ := partsText(st.Message.Parts); text == "" || strings.Contains(text, internalDetail) {
			t.Fatalf("%s 应保留安全说明，实际 %q", source, text)
		}
	}
	result := <-sent
	if result.err != nil {
		t.Fatalf("执行流失败：%v", result.err)
	}
	assertFailure("执行流", result.status)
	assertFailure("订阅流", subscribed)

	task, err := client.GetTask(ctx, &a2atypes.GetTaskRequest{ID: id})
	if err != nil {
		t.Fatal(err)
	}
	assertFailure("任务查询", task.Status)
}
