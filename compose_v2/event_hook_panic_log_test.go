package compose_v2_test

// OnNodeEvent 钩子的确定性 bug 会在每个流式片段上都 panic：同一次节点执行里只在第 1、10、100、1000… 次各记一条带栈的日志，
// 不逐条记（曾经 2000 个片段记 2000 条，审计实测）。片段可能从几个 goroutine 并发发出（tools 节点并发跑几个 tool_call）。

import (
	"context"
	"sync"
	"testing"

	"crew-go/compose_v2"
)

type everyEventPanics struct {
	compose_v2.NoOpOrchestrationHooks
}

//go:noinline
func evEveryEventExplodes() { panic("event boom") }

func (everyEventPanics) OnNodeEvent(context.Context, compose_v2.NodeID, compose_v2.Event) context.Context {
	evEveryEventExplodes()
	return nil
}

func TestEventHookPanic_LoggedAtPowersOfTen(t *testing.T) {
	logs := psCaptureLogs(t)
	b := compose_v2.NewBuilder().WithName("event_panic_log")
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sink := compose_v2.GetEventSink(ctx)
		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 500; i++ {
					sink.Send(compose_v2.Event{Type: "chunk", Content: "x"})
				}
			}()
		}
		wg.Wait()
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g := rsCompile(t, b)
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), compose_v2.WithHooks(everyEventPanics{})); err != nil {
		t.Fatalf("OnNodeEvent 只作观测，节点照常跑完：%v", err)
	}
	if n := logs.countStack("evEveryEventExplodes"); n != 4 {
		t.Fatalf("2000 次 panic 应只在第 1、10、100、1000 次各记一条（4 条），实际 %d 条", n)
	}
}
