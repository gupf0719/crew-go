package agent_v2_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

type idleStreamExit struct {
	at    time.Time
	cause error
}

// idleStreamModel 首次交付一个片段后等取消，第二次持续交付三个片段。
type idleStreamModel struct {
	calls atomic.Int32
	exit  chan idleStreamExit
}

func (*idleStreamModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, errors.New("expected streaming request")
}

func (m *idleStreamModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *idleStreamModel) Stream(ctx context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	attempt := m.calls.Add(1)
	reader, send := schema.NewStreamReader[*schema.Message](4)
	go func() {
		defer schema.CloseSend(reader)
		if attempt == 1 {
			if err := compose_v2.SleepWithContext(ctx, 2*time.Second); err != nil {
				schema.SendError(reader, err)
				return
			}
			send(schema.AssistantMessage("未完成"))
			<-ctx.Done()
			m.exit <- idleStreamExit{at: time.Now(), cause: context.Cause(ctx)}
			schema.SendError(reader, ctx.Err())
			return
		}
		for _, chunk := range []string{"重试", "完成", "。"} {
			if err := compose_v2.SleepWithContext(ctx, 2*time.Second); err != nil {
				schema.SendError(reader, err)
				return
			}
			send(schema.AssistantMessage(chunk))
		}
		send(&schema.Message{Role: schema.RoleAssistant, Metadata: &schema.MessageMetadata{FinishReason: "stop"}})
	}()
	return reader, nil
}

func TestGraphAgent_NodeIdleTimeoutRetriesStalledBasicAgentStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := &idleStreamModel{exit: make(chan idleStreamExit, 1)}
		sub := agent_v2.NewBasicAgent(agent_v2.WithName("idle_stream_worker"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(2))
		out := compose_v2.LastValue[*schema.Message]("result")
		b := compose_v2.WithChannel(compose_v2.NewBuilder().WithName("idle_stream_host"), out)
		retry := compose_v2.SimpleRetry(1)
		retry.BackoffPolicy.InitialDelay = 7 * time.Second
		b.AddNode("generate", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			events, err := agent_v2.RunSubAgent(ctx, "stream_worker", sub, schema.UserMessage("generate"))
			if err != nil {
				return nil, err
			}
			return []compose_v2.ChannelWrite{compose_v2.Write(out, agent_v2.FinalMessage(events))}, nil
		}, compose_v2.WithNodeIdleTimeout(3*time.Second), compose_v2.WithNodeTimeout(12*time.Second), compose_v2.WithNodeRetry(retry))
		b.AddEdge(compose_v2.StartNode, "generate")
		b.AddEdge("generate", compose_v2.EndNode)
		graph, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		host := agent_v2.NewGraphAgent(graph, agent_v2.WithGraphAgentName("idle_stream_host"),
			agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
			agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
				msg, _ := compose_v2.Get(chs, out)
				return msg
			}))
		log := &eventLog{}
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
		ctx = agent_v2.WithCtxOrchHooks(ctx, log)
		start := time.Now()
		events, err := host.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
		if result := agent_v2.FinalMessage(events).GetContent(); err != nil || m.calls.Load() != 2 || result != "重试完成。" {
			t.Fatalf("第二次流式生成应独立续期并交付完整结果，实际 %q，调用 %d 次，err=%v", result, m.calls.Load(), err)
		}
		synctest.Wait()
		select {
		case exit := <-m.exit:
			var nt *compose_v2.NodeTimeoutError
			if elapsed := exit.at.Sub(start); elapsed != 5*time.Second || !errors.As(exit.cause, &nt) || !nt.Idle {
				t.Fatalf("第一个片段应续期到第 5 秒，实际 %v，cause=%v", elapsed, exit.cause)
			}
		default:
			t.Fatal("第一个流未收到取消")
		}
		var firstID string
		reset := false
		for _, ev := range log.events {
			id, _ := ev.Metadata["run_id"].(string)
			if ev.Type == "chunk" && ev.Content == "未完成" {
				firstID = id
			}
			if ev.Type == "reset_stream" && firstID != "" && id == firstID {
				reset = true
			}
		}
		if firstID == "" || !reset {
			t.Fatalf("失败的流应按原 run_id 撤回，实际 %+v", log.events)
		}
	})
}
