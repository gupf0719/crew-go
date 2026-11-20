package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

func noScheduleWrites(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
	return nil, nil
}

func TestJoinEdgeAcrossDepthRetainsOrdinaryAnyOf(t *testing.T) {
	parts := compose_v2.Append[string]("parts")
	anySeen := compose_v2.Append[string]("any_seen")
	allSeen := compose_v2.Append[string]("all_seen")
	b := compose_v2.NewBuilder()
	compose_v2.WithChannel(b, parts)
	compose_v2.WithChannel(b, anySeen)
	compose_v2.WithChannel(b, allSeen)
	b.AddNode("left", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(parts, []string{"L"})}, nil
	})
	b.AddNode("relay", noScheduleWrites)
	b.AddNode("right", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(parts, []string{"R"})}, nil
	})
	summarize := func(seen *compose_v2.Channel[[]string]) compose_v2.NodeFunc {
		return func(_ context.Context, ch compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			view := strings.Join(compose_v2.MustGet(ch, parts), "")
			return []compose_v2.ChannelWrite{compose_v2.Write(seen, []string{view})}, nil
		}
	}
	b.AddNode("any", summarize(anySeen)).AddNode("all", summarize(allSeen))
	b.AddEdge(compose_v2.StartNode, "left").AddEdge(compose_v2.StartNode, "relay")
	b.AddEdge("relay", "right").AddEdge("left", "any").AddEdge("right", "any")
	b.AddJoinEdge([]compose_v2.NodeID{"right", "left"}, "all")
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(t.Context(), g, cm); err != nil {
		t.Fatal(err)
	}
	if got := compose_v2.MustGet(cm.Channels(), anySeen); !slices.Equal(got, []string{"L", "LR"}) {
		t.Fatalf("普通边应照常在两个超步触发：%v", got)
	}
	if got := compose_v2.MustGet(cm.Channels(), allSeen); !slices.Equal(got, []string{"LR"}) {
		t.Fatalf("汇合只能在两条写入都提交后执行一次：%v", got)
	}
}

func TestJoinEdgeReusesOneArrivalPerPredecessorEachRound(t *testing.T) {
	left := compose_v2.Sum("left")
	right := compose_v2.Sum("right")
	views := compose_v2.Append[string]("views")
	b := compose_v2.NewBuilder()
	compose_v2.WithChannel(b, left)
	compose_v2.WithChannel(b, right)
	compose_v2.WithChannel(b, views)
	b.AddNode("fork", noScheduleWrites).AddNode("delay1", noScheduleWrites).AddNode("delay2", noScheduleWrites)
	b.AddNode("left", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(left, 1)}, nil
	})
	b.AddNode("right", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(right, 1)}, nil
	})
	b.AddNode("join", func(_ context.Context, ch compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		view := fmt.Sprintf("%d/%d", compose_v2.MustGet(ch, left), compose_v2.MustGet(ch, right))
		return []compose_v2.ChannelWrite{compose_v2.Write(views, []string{view})}, nil
	})
	b.AddEdge(compose_v2.StartNode, "fork").AddEdge("fork", "left").AddEdge("fork", "delay1").
		AddEdge("delay1", "delay2").AddEdge("delay2", "right")
	b.AddConditionalEdge("left", func(_ context.Context, ch compose_v2.Channels) (compose_v2.NodeID, error) {
		if compose_v2.MustGet(ch, left)%2 == 1 {
			return "left", nil
		}
		return compose_v2.EndNode, nil
	})
	b.AddJoinEdge([]compose_v2.NodeID{"left", "right"}, "join")
	b.AddConditionalEdge("join", func(_ context.Context, ch compose_v2.Channels) (compose_v2.NodeID, error) {
		if len(compose_v2.MustGet(ch, views)) < 2 {
			return "fork", nil
		}
		return compose_v2.EndNode, nil
	})
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(t.Context(), g, cm); err != nil {
		t.Fatal(err)
	}
	if got := compose_v2.MustGet(cm.Channels(), views); !slices.Equal(got, []string{"2/1", "4/2"}) {
		t.Fatalf("每轮应等待新的右前驱，且两次左到达只触发一次：%v", got)
	}
}

func TestJoinEdgeUsesCommittedSiblingOnResume(t *testing.T) {
	total := compose_v2.Sum("total")
	joined := compose_v2.LastValue[int]("result")
	b := compose_v2.NewBuilder()
	compose_v2.WithChannel(b, total)
	compose_v2.WithChannel(b, joined)
	var leftCalls, joinCalls atomic.Int32
	b.AddNode("left", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		leftCalls.Add(1)
		return []compose_v2.ChannelWrite{compose_v2.Write(total, 2)}, nil
	})
	b.AddNode("right", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "right?"})
		if err != nil {
			return nil, err
		}
		if answer.Content != "approved" {
			return nil, fmt.Errorf("unexpected answer %q", answer.Content)
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(total, 3)}, nil
	})
	b.AddNode("join", func(_ context.Context, ch compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		joinCalls.Add(1)
		return []compose_v2.ChannelWrite{compose_v2.Write(joined, compose_v2.MustGet(ch, total))}, nil
	})
	b.AddEdge(compose_v2.StartNode, "left").AddEdge(compose_v2.StartNode, "right").
		AddJoinEdge([]compose_v2.NodeID{"left", "right"}, "join")
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx, stop := context.WithTimeout(compose_v2.WithCheckpointerCtx(t.Context(), cp), 5*time.Second)
	defer stop()
	ctx = compose_v2.WithSessionIDCtx(ctx, "join-partial")
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager())
	pause, ok := compose_v2.IsInterrupt(err)
	if !ok || len(pause.AllQueries()) != 1 || leftCalls.Load() != 1 || joinCalls.Load() != 0 {
		t.Fatalf("只有左侧应提交：left=%d join=%d err=%v", leftCalls.Load(), joinCalls.Load(), err)
	}
	saved, err := cp.LoadNS(ctx, "join-partial", "")
	if err != nil || saved == nil || !slices.Equal(saved.CompletedFrontier, []compose_v2.FrontierMember{{NodeID: "left"}}) {
		t.Fatalf("缺少部分提交记录：cp=%+v err=%v", saved, err)
	}
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(ctx, g, cm, compose_v2.WithResumeAnswers(map[string]*schema.Message{
		pause.AllQueries()[0].InterruptID: schema.UserMessage("approved"),
	})); err != nil {
		t.Fatal(err)
	}
	if result := compose_v2.MustGet(cm.Channels(), joined); result != 5 || leftCalls.Load() != 1 || joinCalls.Load() != 1 {
		t.Fatalf("恢复必须计入左侧且不重跑：result=%d left=%d join=%d", result, leftCalls.Load(), joinCalls.Load())
	}
}

func TestDeferredQueueEnforcesBatchWidthBeforeRelease(t *testing.T) {
	var produced, finalized atomic.Int32
	b := compose_v2.NewBuilder().WithMaxFrontierWidth(2).WithDeferredNodes("finalize")
	b.AddNode("produce", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		produced.Add(1)
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{Target: "finalize"}, compose_v2.SendDirective{Target: "finalize"})
		return nil, nil
	})
	b.AddNode("finalize", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		finalized.Add(1)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "produce").AddEdge("produce", "produce")
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	err = compose_v2.Run(t.Context(), g, compose_v2.NewChannelManager())
	var width *compose_v2.MaxWidthError
	if !errors.As(err, &width) || !errors.Is(err, compose_v2.ErrRunAborted) ||
		width.Width != 4 || produced.Load() != 2 || finalized.Load() != 0 {
		t.Fatalf("超宽收尾应在等待中终止：produce=%d finalize=%d err=%v", produced.Load(), finalized.Load(), err)
	}
}

func TestScheduleCheckpointRejectsCorruptProgressBeforeGraphStart(t *testing.T) {
	var calls atomic.Int32
	b := compose_v2.NewBuilder().WithName("validate-schedule").WithDeferredNodes("finalize")
	b.AddNode("left", noScheduleWrites).AddNode("right", noScheduleWrites).AddNode("finalize", noScheduleWrites)
	b.AddNode("work", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		calls.Add(1)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "work").AddJoinEdge([]compose_v2.NodeID{"left", "right"}, "finalize")
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name    string
		corrupt func(*compose_v2.Checkpoint)
	}{
		{name: "no_active_frontier", corrupt: func(cp *compose_v2.Checkpoint) { cp.CurrentFrontier = nil }},
		{name: "unknown_join", corrupt: func(cp *compose_v2.Checkpoint) {
			cp.JoinProgress = map[int][]compose_v2.NodeID{1: {"left"}}
		}},
		{name: "duplicate_deferred_instance", corrupt: func(cp *compose_v2.Checkpoint) {
			cp.CurrentFrontier[0].InstanceID = "i1"
			cp.DeferredFrontier[0].InstanceID = "i1"
		}},
		{name: "invalid_deferred_token", corrupt: func(cp *compose_v2.Checkpoint) {
			cp.DeferredFrontier[0].InstanceID = "broken"
		}},
		{name: "mixed_active_frontier", corrupt: func(cp *compose_v2.Checkpoint) {
			cp.JoinProgress, cp.DeferredFrontier = nil, nil
			cp.CurrentFrontier = append(cp.CurrentFrontier, compose_v2.FrontierMember{NodeID: "finalize"})
		}},
		{name: "duplicate_released_instance", corrupt: func(cp *compose_v2.Checkpoint) {
			cp.JoinProgress, cp.DeferredFrontier = nil, nil
			member := compose_v2.FrontierMember{NodeID: "finalize", InstanceID: "i1"}
			cp.CurrentFrontier = []compose_v2.FrontierMember{member}
			cp.CompletedFrontier = []compose_v2.FrontierMember{member}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cper := compose_v2.NewInMemoryCheckpointer()
			ctx, cancel := context.WithTimeout(compose_v2.WithCheckpointerCtx(t.Context(), cper), 5*time.Second)
			defer cancel()
			ctx = compose_v2.WithSessionIDCtx(ctx, tt.name)
			saved := &compose_v2.Checkpoint{
				GraphID: g.CheckpointIdentity(), Epoch: "old", Status: compose_v2.CPStatusPending,
				CurrentFrontier:  []compose_v2.FrontierMember{{NodeID: "work"}},
				JoinProgress:     map[int][]compose_v2.NodeID{0: {"left"}},
				DeferredFrontier: []compose_v2.FrontierMember{{NodeID: "finalize"}},
			}
			tt.corrupt(saved)
			if err := cper.SaveNS(ctx, tt.name, "", saved); err != nil {
				t.Fatal(err)
			}
			hooks := &compose_v2.RecordingHooks{}
			err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithHooks(hooks))
			if starts, _ := hooks.SnapshotGraphLifecycle(); !errors.Is(err, compose_v2.ErrCheckpointNotResumable) || starts != 0 || calls.Load() != 0 {
				t.Fatalf("破损进度应在启动前拒绝：starts=%d work=%d err=%v", starts, calls.Load(), err)
			}
			after, err := cper.LoadNS(ctx, tt.name, "")
			if err != nil || after == nil || after.Epoch != "old" || after.Status != compose_v2.CPStatusPending {
				t.Fatalf("应保留原档供排查：cp=%+v err=%v", after, err)
			}
		})
	}
}

func TestGraphScheduleDeclarationsEnterCheckpointIdentity(t *testing.T) {
	b := compose_v2.NewBuilder().WithName("schedule-identity").
		AddNode("a", noScheduleWrites).AddNode("b", noScheduleWrites).AddNode("finish", noScheduleWrites).
		AddEdge(compose_v2.StartNode, "a").AddEdge(compose_v2.StartNode, "b")
	ordinary, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	sources := []compose_v2.NodeID{"b", "a"}
	b.AddJoinEdge(sources, "finish")
	sources[0] = "unknown"
	joined, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	barrier, err := b.WithDeferredNodes("finish").Compile()
	if err != nil {
		t.Fatal(err)
	}
	if ordinary.ID() != barrier.ID() || ordinary.CheckpointIdentity() == joined.CheckpointIdentity() ||
		joined.CheckpointIdentity() == barrier.CheckpointIdentity() {
		t.Fatal("两种调度策略均应改变存档身份，同时保留图的公开名称")
	}
	if _, err := b.AddJoinEdge([]compose_v2.NodeID{"a", "a"}, "finish").Compile(); err == nil {
		t.Fatal("不应接受重复前驱")
	}
}
