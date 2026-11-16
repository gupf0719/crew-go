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

func TestFanoutJoinedInterruptsKeepOrderAndCauses(t *testing.T) {
	cause := errors.New("underlying branch error")
	first := fmt.Errorf("tool: %w", errors.Join(ie("a"), errors.Join(cause, ie("b"))))
	for _, tt := range []struct {
		name string
		err  error
		want []string
	}{
		{"one branch", compose_v2.PickFanoutError([]error{first}), []string{"a", "b"}},
		{"multiple branches", compose_v2.PickFanoutError([]error{first, ie("c")}), []string{"a", "b", "c"}},
		{"nested aggregation", compose_v2.PickFanoutError([]error{
			fmt.Errorf("inner: %w", compose_v2.PickFanoutError([]error{ie("a"), ie("b")})),
			errors.Join(ie("c"), cause),
		}), []string{"a", "b", "c"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, isInterrupt := compose_v2.IsInterrupt(tt.err)
			var byType *compose_v2.InterruptError
			if !isInterrupt || !errors.As(tt.err, &byType) || byType != got || !errors.Is(tt.err, cause) {
				t.Fatalf("中断投影与原始原因应同时保留，实际 %v", tt.err)
			}
			if ids := fanoutQuestions(got.AllQueries()); !slices.Equal(ids, tt.want) {
				t.Fatalf("公开待答问题 = %v，期望 %v", ids, tt.want)
			}
		})
	}
}

func TestFanoutJoinedInterruptRejectsHiddenInvalidLeaf(t *testing.T) {
	var nilInterrupt *compose_v2.InterruptError
	for _, tt := range []struct {
		name    string
		invalid error
	}{
		{"no query", &compose_v2.InterruptError{Reason: "缺问题"}},
		{"typed nil", nilInterrupt},
		{"nil query", &compose_v2.InterruptError{Queries: []*compose_v2.AskQuery{nil}}},
		{"mutually exclusive", &compose_v2.InterruptError{
			Query:   &compose_v2.AskQuery{InterruptID: "b"},
			Queries: []*compose_v2.AskQuery{{InterruptID: "d"}},
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := compose_v2.PickFanoutError([]error{errors.Join(ie("a"), tt.invalid), ie("c")})
			if _, pending := compose_v2.IsInterrupt(got); pending || !errors.Is(got, compose_v2.ErrRunAborted) {
				t.Fatalf("非法叶子应终止本批，实际 %v", got)
			}
		})
	}
}

func TestGraphJoinedInterruptsCheckpointAndResume(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	for _, tt := range []struct {
		name    string
		invalid error
	}{
		{"resume all", nil},
		{"abort malformed leaf", &compose_v2.InterruptError{Reason: "缺问题"}},
		{"abort duplicate ID", ie("same")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			const sid = "joined-ask"
			cp := compose_v2.NewInMemoryCheckpointer()
			var replaceWithInvalid bool
			var answersSeen []string
			g, err := compose_v2.NewBuilder().WithName(sid).
				AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					if replaceWithInvalid {
						return nil, errors.Join(ie("same"), tt.invalid)
					}
					a, errA := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "first"})
					b, errB := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "second"})
					if err := errors.Join(errA, errB); err != nil {
						return nil, fmt.Errorf("node questions: %w", err)
					}
					answersSeen = []string{a.Content, b.Content}
					return nil, nil
				}).
				AddEdge(compose_v2.StartNode, "ask").
				AddEdge("ask", compose_v2.EndNode).Compile()
			if err != nil {
				t.Fatal(err)
			}
			opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)}
			err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...)
			interrupt, pending := compose_v2.IsInterrupt(err)
			if !pending || interrupt == nil {
				t.Fatalf("首次运行应暂停，实际 %v", err)
			}
			saved, err := cp.LoadNS(ctx, sid, "")
			if err != nil || saved == nil {
				t.Fatalf("应保存待答现场，实际 checkpoint=%v err=%v", saved, err)
			}
			publicIDs := fanoutQuestions(interrupt.AllQueries())
			if tt.invalid == nil && (len(publicIDs) != 2 || !slices.Equal(publicIDs, fanoutQuestions(saved.PendingInterrupts))) {
				t.Fatalf("应公开并写档两个问题，实际 public=%v checkpoint=%v", publicIDs, fanoutQuestions(saved.PendingInterrupts))
			}
			answers := make(map[string]*schema.Message, len(publicIDs))
			for _, q := range interrupt.AllQueries() {
				answers[q.InterruptID] = schema.UserMessage(q.Question + " answer")
			}
			replaceWithInvalid = tt.invalid != nil
			err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), append(opts, compose_v2.WithResumeAnswers(answers))...)
			if tt.invalid == nil {
				if err != nil || !slices.Equal(answersSeen, []string{"first answer", "second answer"}) {
					t.Fatalf("续跑应兑付两个答案，实际 %v err=%v", answersSeen, err)
				}
			} else if _, pending := compose_v2.IsInterrupt(err); pending || !errors.Is(err, compose_v2.ErrRunAborted) {
				t.Fatalf("非法中断应终止，实际 %v", err)
			}
			if saved, err := cp.LoadNS(ctx, sid, ""); err != nil || saved != nil {
				t.Fatalf("退出后应清档，实际 checkpoint=%v err=%v", saved, err)
			}
		})
	}
}

func fanoutQuestions(queries []*compose_v2.AskQuery) []string {
	ids := make([]string, 0, len(queries))
	for _, q := range queries {
		ids = append(ids, q.InterruptID)
	}
	return ids
}

func TestFanoutJoinedInterruptDoesNotMutateSourceQuery(t *testing.T) {
	source := &compose_v2.InterruptError{Reason: "source reason", Query: &compose_v2.AskQuery{InterruptID: "a"}}
	got := compose_v2.PickFanoutError([]error{source, ie("b")})
	interrupt, pending := compose_v2.IsInterrupt(got)
	if !pending || interrupt.AllQueries()[0].Reason != source.Reason {
		t.Fatalf("合并后的问题缺少原因，实际 %v", got)
	}
	if source.Query.Reason != "" {
		t.Fatalf("源问题不应被改写，实际 %q", source.Query.Reason)
	}
}

func TestGraphJoinedInterruptPreservesCheckpointFailure(t *testing.T) {
	for _, failure := range []error{compose_v2.ErrCheckpointNotResumable, compose_v2.ErrCheckpointNotPersisted} {
		t.Run(failure.Error(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			const sid = "joined-checkpoint-error"
			cp := compose_v2.NewInMemoryCheckpointer()
			var resume bool
			g, err := compose_v2.NewBuilder().WithName(sid).
				AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					if resume {
						return nil, errors.Join(ie("a"), &compose_v2.InterruptError{Reason: "缺问题"}, failure)
					}
					_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "first"})
					return nil, err
				}).AddEdge(compose_v2.StartNode, "ask").AddEdge("ask", compose_v2.EndNode).Compile()
			if err != nil {
				t.Fatal(err)
			}
			opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)}
			err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...)
			if _, pending := compose_v2.IsInterrupt(err); !pending {
				t.Fatalf("初次运行应暂停，实际 %v", err)
			}
			resume = true
			err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...)
			if !errors.Is(err, failure) || errors.Is(err, compose_v2.ErrRunAborted) {
				t.Fatalf("存档错误须保留原优先级，实际 %v", err)
			}
			saved, err := cp.LoadNS(ctx, sid, "")
			if err != nil || saved == nil {
				t.Fatalf("存档失败后应保留现场，实际 checkpoint=%v err=%v", saved, err)
			}
			if slices.Contains(fanoutQuestions(saved.PendingInterrupts), "a") {
				t.Fatal("存档失败分支的新问题不应进入待答现场")
			}
		})
	}
}

func TestGraphFanoutDuplicateInterruptDespiteReducerError(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	applyFailure := errors.New("reducer failed")
	channel := compose_v2.Custom("failed", func(_, _ int) int { panic(applyFailure) }, 0)
	graph := compose_v2.NewBuilder().WithName("duplicate-join")
	for _, id := range []compose_v2.NodeID{"a", "b"} {
		graph.AddNode(id, func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, ie("duplicate")
		}).AddEdge(compose_v2.StartNode, id).AddEdge(id, compose_v2.EndNode)
	}
	graph.AddNode("writer", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(channel, 1)}, nil
	}).AddEdge(compose_v2.StartNode, "writer").AddEdge("writer", compose_v2.EndNode)
	g, err := graph.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cp := compose_v2.NewInMemoryCheckpointer()
	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, channel)
	const sid = "duplicate-reducer"
	err = compose_v2.Run(ctx, g, cm, compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid))
	if _, pending := compose_v2.IsInterrupt(err); pending || !errors.Is(err, compose_v2.ErrRunAborted) || !errors.Is(err, applyFailure) {
		t.Fatalf("重复 ID 应终止且保留 reducer 错误，实际 %v", err)
	}
	if saved, err := cp.LoadNS(ctx, sid, ""); err != nil || saved != nil {
		t.Fatalf("终局退出后应清档，实际 checkpoint=%v err=%v", saved, err)
	}
}
