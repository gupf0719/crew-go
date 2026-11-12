package compose_v2_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

func TestSendInputIsPrivateAndSurvivesWireResume(t *testing.T) {
	input := compose_v2.LastValue[string]("worker_input")
	output := compose_v2.Append[string]("worker_output")
	builder := compose_v2.NewBuilder()
	compose_v2.WithChannel(builder, input)
	compose_v2.WithChannel(builder, output)
	var mu sync.Mutex
	calls := make(map[string]int)
	builder.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		for _, task := range []string{"A", "B"} {
			compose_v2.SendFrom(ctx, compose_v2.SendDirective{Target: "worker", Input: []compose_v2.ChannelWrite{compose_v2.Write(input, task)}})
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(input, "shared")}, nil
	})
	builder.AddNode("worker", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		task := compose_v2.MustGet(chs, input)
		mu.Lock()
		calls[task]++
		mu.Unlock()
		if task == "B" {
			answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "输入 " + task})
			if err != nil {
				return nil, err
			}
			if answer.Content != "B answer" {
				return nil, fmt.Errorf("worker %q got %q", task, answer.Content)
			}
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(output, []string{task})}, nil
	})
	builder.AddEdge(compose_v2.StartNode, "split").AddEdge("split", "worker").AddEdge("worker", compose_v2.EndNode)
	graph, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	first := compose_v2.NewChannelManager()
	const sid = "private-send-input"
	err = compose_v2.Run(ctx, graph, first, compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid))
	interrupt, ok := compose_v2.IsInterrupt(err)
	if !ok || len(interrupt.AllQueries()) != 1 || interrupt.AllQueries()[0].Question != "输入 B" {
		t.Fatalf("应仅挂起 B 实例，err=%v, calls=%v", err, calls)
	}
	saved, err := cp.LoadNS(ctx, sid, "")
	if err != nil || saved == nil || len(saved.CurrentFrontier) != 1 || len(saved.FrontierInputs) != 1 {
		t.Fatalf("失败子集应只保留 B 的私有输入，checkpoint=%+v err=%v", saved, err)
	}
	data, err := compose_v2.MarshalCheckpoint(saved)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := compose_v2.UnmarshalCheckpoint(data)
	if err != nil {
		t.Fatal(err)
	}
	second := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, graph, second,
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid), compose_v2.WithResumeFrom(restored),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{interrupt.AllQueries()[0].InterruptID: schema.UserMessage("B answer")}))
	if err != nil {
		t.Fatal(err)
	}
	if calls["A"] != 1 || calls["B"] != 2 || calls["shared"] != 1 || len(calls) != 3 {
		t.Fatalf("实例收到错误输入或重放成功兄弟: %v", calls)
	}
	if got := compose_v2.MustGet(second.Channels(), input); got != "shared" {
		t.Fatalf("私有输入写进共享状态: %q", got)
	}
	got := compose_v2.MustGet(second.Channels(), output)
	slices.Sort(got)
	if !slices.Equal(got, []string{"A", "B", "shared"}) {
		t.Fatalf("worker 产出: %q", got)
	}
}

func TestSendInputSurvivesDeferredSendAndReachesCondition(t *testing.T) {
	input := compose_v2.LastValue[string]("deferred_input")
	b := compose_v2.NewBuilder()
	compose_v2.WithChannel(b, input)
	b.AddNode("sender", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{Target: "target", Input: []compose_v2.ChannelWrite{compose_v2.Write(input, "private")}})
		return nil, nil
	})
	b.AddNode("asker", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续吗"})
		return nil, err
	})
	b.AddNode("target", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if got := compose_v2.MustGet(chs, input); got != "private" {
			return nil, fmt.Errorf("target saw %q", got)
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "sender").AddEdge(compose_v2.StartNode, "asker")
	b.AddConditionalEdge("target", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		if got := compose_v2.MustGet(chs, input); got != "private" {
			return "", fmt.Errorf("condition saw %q", got)
		}
		return compose_v2.EndNode, nil
	})
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	const sid = "deferred-send-input"
	err = compose_v2.Run(ctx, graph, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid))
	interrupt, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应挂起 asker: %v", err)
	}
	saved, err := cp.LoadNS(ctx, sid, "")
	if err != nil || saved == nil || len(saved.DeferredSends) != 1 || len(saved.DeferredSends[0].Input) != 1 {
		t.Fatalf("deferred send 私有输入未存档: %+v, %v", saved, err)
	}
	raw, err := compose_v2.MarshalCheckpoint(saved)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := compose_v2.UnmarshalCheckpoint(raw)
	if err != nil {
		t.Fatal(err)
	}
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(ctx, graph, cm, compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid), compose_v2.WithResumeFrom(restored),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{interrupt.AllQueries()[0].InterruptID: schema.UserMessage("继续")})); err != nil {
		t.Fatal(err)
	}
	if compose_v2.Has(cm.Channels(), input) {
		t.Fatal("deferred send 的私有输入写进了共享状态")
	}
}

func TestSendInputSubgraphWithImport(t *testing.T) {
	input := compose_v2.LastValue[string]("input")
	output := compose_v2.Append[string]("output")
	grand := compose_v2.NewBuilder()
	compose_v2.WithChannel(grand, input)
	compose_v2.WithChannel(grand, output)
	grand.AddNode("take", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		v := compose_v2.MustGet(chs, input)
		if v == "B" {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "private=" + v})
			if err != nil {
				return nil, err
			}
			v = ans.Content
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(output, []string{v})}, nil
	})
	grand.AddEdge(compose_v2.StartNode, "take").AddEdge("take", compose_v2.EndNode)
	gg, err := grand.Compile()
	if err != nil {
		t.Fatal(err)
	}
	child := compose_v2.NewBuilder()
	compose_v2.WithChannel(child, input)
	compose_v2.WithChannel(child, output)
	child.AddSubgraphNode("grand", gg, compose_v2.WithImport(input), compose_v2.WithExport(output))
	child.AddEdge(compose_v2.StartNode, "grand").AddEdge("grand", compose_v2.EndNode)
	cg, err := child.Compile()
	if err != nil {
		t.Fatal(err)
	}
	top := compose_v2.NewBuilder()
	compose_v2.WithChannel(top, input)
	compose_v2.WithChannel(top, output)
	top.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "child", Input: []compose_v2.ChannelWrite{compose_v2.Write(input, "A")}},
			compose_v2.SendDirective{Target: "child", Input: []compose_v2.ChannelWrite{compose_v2.Write(input, "B")}},
		)
		return []compose_v2.ChannelWrite{compose_v2.Write(input, "shared")}, nil
	})
	top.AddSubgraphNode("child", cg, compose_v2.WithImport(input), compose_v2.WithExport(output))
	top.AddEdge(compose_v2.StartNode, "split").AddEdge("child", compose_v2.EndNode)
	tg, err := top.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	first := compose_v2.NewChannelManager()
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("nested-private-send")}
	err = compose_v2.Run(ctx, tg, first, opts...)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || len(ie.AllQueries()) != 1 || ie.AllQueries()[0].Question != "private=B" {
		got, _ := compose_v2.Get(first.Channels(), output)
		t.Fatalf("want private=B interrupt; err=%v output=%q", err, got)
	}
	second := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, tg, second, append(opts, compose_v2.WithResumeAnswers(map[string]*schema.Message{
		ie.AllQueries()[0].InterruptID: schema.UserMessage("B answered"),
	}))...)
	if err != nil {
		t.Fatal(err)
	}
	got := slices.Clone(compose_v2.MustGet(second.Channels(), output))
	slices.Sort(got)
	if !slices.Equal(got, []string{"A", "B answered"}) {
		t.Fatal(got)
	}
	if compose_v2.MustGet(second.Channels(), input) != "shared" {
		t.Fatal("shared input changed")
	}
}

type embeddedChannelWrite struct{ compose_v2.ChannelWrite }

func TestSendInputInvalidWriteDoesNotHang(t *testing.T) {
	b := compose_v2.NewBuilder()
	b.AddNode("bad", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{Target: "target", Input: []compose_v2.ChannelWrite{embeddedChannelWrite{}}})
		return nil, nil
	})
	noop := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil }
	b.AddNode("peer", noop)
	b.AddNode("target", noop)
	b.AddEdge(compose_v2.StartNode, "bad").AddEdge(compose_v2.StartNode, "peer").AddEdge("peer", compose_v2.EndNode).AddEdge("target", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- compose_v2.Run(t.Context(), g, compose_v2.NewChannelManager()) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("wanted an error from invalid embedded write")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run deadlocked after invalid input write")
	}
}
