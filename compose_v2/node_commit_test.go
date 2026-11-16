package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

func TestAfterNodeCommit_RollbackThenResume(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cm := compose_v2.NewChannelManager()
	var attempts int
	var values []int
	value := compose_v2.Custom("value", func(_ int, next int) int {
		if next == 1 {
			panic("test reducer rejects first value")
		}
		return next
	}, 0)
	b := compose_v2.NewBuilder()
	compose_v2.WithChannel(b, value)
	b.AddNode("write", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		attempts++
		compose_v2.AfterNodeCommit(ctx, func() {
			got, _ := compose_v2.Get(cm.Channels(), value)
			values = append(values, got)
		})
		return []compose_v2.ChannelWrite{compose_v2.Write(value, attempts)}, nil
	})
	b.AddNode("read", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if !slices.Equal(values, []int{2}) {
			return nil, fmt.Errorf("notification was not delivered before the next node: %v", values)
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "write")
	b.AddEdge("write", "read")
	b.AddEdge("read", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("node-commit")}
	if err := compose_v2.Run(ctx, g, cm, opts...); err == nil || len(values) != 0 {
		t.Fatalf("rolled-back write must not notify, err=%v values=%v", err, values)
	}
	if err := compose_v2.Run(ctx, g, cm, opts...); err != nil || attempts != 2 || !slices.Equal(values, []int{2}) {
		t.Fatalf("resumed write must notify once with the committed value, err=%v attempts=%d values=%v", err, attempts, values)
	}
}

func TestAfterNodeCommit_RetryAndPartialCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var notifications []string
	var attempts int
	b := compose_v2.NewBuilder()
	b.AddNode("retry", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		attempts++
		attempt := attempts
		compose_v2.AfterNodeCommit(ctx, func() { notifications = append(notifications, fmt.Sprint(attempt)) })
		if attempt == 1 {
			return nil, errors.New("reject first attempt")
		}
		return nil, nil
	}, compose_v2.WithNodeRetry(compose_v2.SimpleRetry(1)))
	b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.AfterNodeCommit(ctx, func() { notifications = append(notifications, "answer") })
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"})
		return nil, err
	})
	for _, id := range []compose_v2.NodeID{"retry", "ask"} {
		b.AddEdge(compose_v2.StartNode, id)
		b.AddEdge(id, compose_v2.EndNode)
	}
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cp := compose_v2.NewInMemoryCheckpointer()
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("partial-commit")}
	interrupted, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...))
	if !ok || len(interrupted.AllQueries()) != 1 || !slices.Equal(notifications, []string{"2"}) {
		t.Fatalf("partial commit must notify only the successful retry, interrupted=%v notifications=%v", interrupted, notifications)
	}
	answers := map[string]*schema.Message{interrupted.AllQueries()[0].InterruptID: schema.UserMessage("yes")}
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), append(opts, compose_v2.WithResumeAnswers(answers))...); err != nil ||
		attempts != 2 || !slices.Equal(notifications, []string{"2", "answer"}) {
		t.Fatalf("resume must notify only the remaining member, err=%v attempts=%d notifications=%v", err, attempts, notifications)
	}
}
