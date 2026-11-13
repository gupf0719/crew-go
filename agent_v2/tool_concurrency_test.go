package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

func TestMaxConcurrentTools_CancelQueuedAndReplayCompleted(t *testing.T) {
	ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	entered := make(chan int, 4)
	release := make(chan struct{})
	var calls [4]atomic.Int32
	var firstRun atomic.Bool
	firstRun.Store(true)
	tk := tool.NewToolkit()
	names := make([]string, len(calls))
	for i := range calls {
		names[i] = fmt.Sprintf("call_%d", i)
		tk.Register(tool.NewFuncTool(names[i], names[i], map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) {
				calls[i].Add(1)
				if firstRun.Load() {
					entered <- i
					<-release
				}
				return tool.Success(names[i]), nil
			}))
	}
	var outputs []string
	llm := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		reply(toolCallsMessage(names...)),
		func(in []*schema.Message) (*schema.Message, error) {
			for _, msg := range in {
				if msg.Role == schema.RoleTool {
					outputs = append(outputs, msg.Content)
				}
			}
			return schema.AssistantMessage("done"), nil
		},
	}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("bounded-tools"), agent_v2.WithToolkit(tk),
		agent_v2.WithModel(llm), agent_v2.WithMaxConcurrentTools(2))
	sess := agent_v2.NewRunSession()
	done := make(chan error, 1)
	go func() {
		_, err := ag.Run(runCtx, sess, schema.UserMessage("run"))
		done <- err
	}()

	var admitted []int
	for range 2 {
		select {
		case i := <-entered:
			admitted = append(admitted, i)
		case <-ctx.Done():
			close(release)
			t.Fatal("did not reach tool concurrency budget")
		}
	}
	cancel()
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
	case <-ctx.Done():
		t.Fatal("tool scheduler did not return after cancellation")
	}
	slices.Sort(admitted)
	if !slices.Equal(admitted, []int{0, 1}) || calls[2].Load() != 0 || calls[3].Load() != 0 {
		t.Fatalf("unexpected dispatch: admitted=%v c=%d d=%d", admitted, calls[2].Load(), calls[3].Load())
	}
	firstRun.Store(false)
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess)); err != nil {
		t.Fatal(err)
	}
	for i := range calls {
		if got := calls[i].Load(); got != 1 {
			t.Fatalf("tool %d executed %d times", i, got)
		}
	}
	if !slices.Equal(outputs, names) {
		t.Fatalf("tool results out of declaration order: %v", outputs)
	}
}
