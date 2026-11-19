package compose_v2_test

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"crew-go/compose_v2"
)

func TestNodeIdleTimeout_RenewsOnProgress(t *testing.T) {
	for _, tt := range []struct {
		name string
		step func(context.Context) error
	}{
		{"heartbeat", func(ctx context.Context) error {
			compose_v2.Heartbeat(ctx)
			return nil
		}},
		{"event", func(ctx context.Context) error {
			compose_v2.GetEventSink(ctx).Send(compose_v2.Event{Type: "progress"})
			return nil
		}},
		{"save_step", func(ctx context.Context) error {
			return compose_v2.SaveStepResult(ctx, "done", "committed")
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const idle = 3 * time.Second
				out := compose_v2.LastValue[string]("progress_output")
				g := singleNodeGraph(t, "progress_"+tt.name, "work", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					for range 3 {
						if err := compose_v2.SleepWithContext(ctx, 2*time.Second); err != nil {
							return nil, err
						}
						if err := tt.step(ctx); err != nil {
							return nil, err
						}
					}
					return []compose_v2.ChannelWrite{compose_v2.Write(out, "finished")}, nil
				}, []*compose_v2.Channel[string]{out}, compose_v2.WithNodeIdleTimeout(idle))
				cm := compose_v2.NewChannelManager()
				start := time.Now()
				err := compose_v2.Run(t.Context(), g, cm)
				got, _ := compose_v2.Get(cm.Channels(), out)
				if err != nil || got != "finished" || time.Since(start) != 6*time.Second {
					t.Fatalf("持续进度应允许节点跨过初始空闲期限，执行 %v，产出 %q，err=%v", time.Since(start), got, err)
				}
			})
		})
	}
}

func TestNodeIdleTimeout_FixedAndCallerDeadlines(t *testing.T) {
	for _, tt := range []struct {
		name      string
		wall      time.Duration
		caller    time.Duration
		heartbeat bool
		wantAfter time.Duration
		wantIdle  bool
	}{
		{name: "idle_first", wall: 9 * time.Second, wantAfter: 3 * time.Second, wantIdle: true},
		{name: "fixed_first", wall: 2 * time.Second, wantAfter: 2 * time.Second},
		{name: "equal", wall: 3 * time.Second, wantAfter: 3 * time.Second},
		{name: "fixed_after_renewals", wall: 7 * time.Second, heartbeat: true, wantAfter: 7 * time.Second},
		{name: "caller_first", wall: 9 * time.Second, caller: time.Second, wantAfter: time.Second},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				if tt.caller > 0 {
					req, cancel := context.WithTimeout(ctx, tt.caller)
					defer cancel()
					ctx = req
				}
				var cause error
				var advertised time.Time
				g := singleNodeGraph(t, "deadline_"+tt.name, "wait", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					advertised, _ = ctx.Deadline()
					if tt.heartbeat {
						for ctx.Err() == nil {
							if compose_v2.SleepWithContext(ctx, 2*time.Second) == nil {
								compose_v2.Heartbeat(ctx)
							}
						}
					} else {
						<-ctx.Done()
					}
					cause = context.Cause(ctx)
					return nil, ctx.Err()
				}, nil, compose_v2.WithNodeTimeout(tt.wall), compose_v2.WithNodeIdleTimeout(3*time.Second))
				start := time.Now()
				err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager())
				hard := tt.wall
				if tt.caller > 0 {
					hard = tt.caller
				}
				if elapsed := time.Since(start); elapsed != tt.wantAfter || !advertised.Equal(start.Add(hard)) {
					t.Fatalf("实际执行 %v，传给节点的固定期限 %v，want %v/%v", elapsed, advertised.Sub(start), tt.wantAfter, hard)
				}
				var timeout, inner *compose_v2.NodeTimeoutError
				if tt.caller > 0 {
					if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(cause, context.DeadlineExceeded) || errors.As(err, &timeout) {
						t.Fatalf("调用方期限应保留原取消归属，err=%v，cause=%v", err, cause)
					}
					return
				}
				wantLimit := tt.wall
				if tt.wantIdle {
					wantLimit = 3 * time.Second
				}
				if !errors.As(err, &timeout) || !errors.As(cause, &inner) || timeout.Idle != tt.wantIdle ||
					inner.Idle != tt.wantIdle || timeout.Timeout != wantLimit || inner.Timeout != wantLimit ||
					errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					t.Fatalf("应标记本次尝试的时限类型和原因，err=%+v，cause=%+v", err, cause)
				}
			})
		})
	}
}

func TestNodeIdleTimeout_SubgraphCommitsRenewParent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var order []compose_v2.NodeID
		b := compose_v2.NewBuilder().WithName("idle_child")
		prev := compose_v2.StartNode
		for _, id := range []compose_v2.NodeID{"read", "process", "save"} {
			b.AddNode(id, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				if err := compose_v2.SleepWithContext(ctx, 2*time.Second); err != nil {
					return nil, err
				}
				order = append(order, id)
				return nil, nil
			})
			b.AddEdge(prev, id)
			prev = id
		}
		b.AddEdge(prev, compose_v2.EndNode)
		child, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		g := idleParent(t, "commit_parent", child, compose_v2.WithNodeIdleTimeout(3*time.Second))
		if err := compose_v2.Run(t.Context(), g, compose_v2.NewChannelManager()); err != nil || len(order) != 3 || order[2] != "save" {
			t.Fatalf("子图每次提交应给父节点续期，已完成 %v，err=%v", order, err)
		}
	})
}

func TestNodeIdleTimeout_StaleAndDetachedProgressDoNotRenewParent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var oldCtx context.Context
		var oldSink compose_v2.EventSink
		calls := 0
		child := singleNodeGraph(t, "idle_child_retry", "work", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			calls++
			if calls == 1 {
				oldCtx, oldSink = ctx, compose_v2.GetEventSink(ctx)
				return nil, errors.New("try again")
			}
			if err := compose_v2.SleepWithContext(ctx, 2*time.Second); err != nil {
				return nil, err
			}
			compose_v2.Heartbeat(oldCtx)
			oldSink.Send(compose_v2.Event{Type: "late"})
			compose_v2.Heartbeat(compose_v2.DetachExecution(ctx))
			compose_v2.GetEventSink(ctx).Send(compose_v2.Event{Type: "reset_stream"})
			<-ctx.Done()
			return nil, ctx.Err()
		}, nil, compose_v2.WithNodeRetry(compose_v2.SimpleRetry(1)))
		g := idleParent(t, "stale_parent", child, compose_v2.WithNodeIdleTimeout(3*time.Second))
		start := time.Now()
		err := compose_v2.Run(t.Context(), g, compose_v2.NewChannelManager())
		var nt *compose_v2.NodeTimeoutError
		if !errors.As(err, &nt) || !nt.Idle || time.Since(start) != 3*time.Second || calls != 2 {
			t.Fatalf("失效尝试和撤回事件不能延长父节点预算，执行 %v，调用 %d 次，err=%v", time.Since(start), calls, err)
		}
	})
}

func idleParent(t *testing.T, name string, child *compose_v2.Graph, options ...compose_v2.NodeOption) *compose_v2.Graph {
	t.Helper()
	b := compose_v2.NewBuilder().WithName(name)
	b.AddSubgraphNode("sub", child, compose_v2.WithSubgraphNodeOptions(options...))
	b.AddEdge(compose_v2.StartNode, "sub")
	b.AddEdge("sub", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestNodeIdleTimeout_LateSuccessCannotCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithCancel(t.Context())
		defer cancel()
		out := compose_v2.LastValue[string]("late_output")
		g := singleNodeGraph(t, "late_idle", "write", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			<-ctx.Done()
			cancel()
			return []compose_v2.ChannelWrite{compose_v2.Write(out, "late")}, nil
		}, []*compose_v2.Channel[string]{out}, compose_v2.WithNodeIdleTimeout(time.Second))
		cm := compose_v2.NewChannelManager()
		err := compose_v2.Run(parent, g, cm)
		var nt *compose_v2.NodeTimeoutError
		if !errors.As(err, &nt) || !nt.Idle || nt.Err != nil {
			t.Fatalf("迟来的成功应保留空闲超时结论，实际 %v", err)
		}
		if got, _ := compose_v2.Get(cm.Channels(), out); got != "" {
			t.Fatalf("超时后结果进入了 channel，实际 %q", got)
		}
	})
}

func TestNodeIdleTimeout_CallerCancelDuringBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		timer := time.AfterFunc(2*time.Second, cancel)
		defer timer.Stop()
		calls := 0
		retry := compose_v2.SimpleRetry(1)
		retry.BackoffPolicy.InitialDelay = 5 * time.Second
		g := singleNodeGraph(t, "idle_backoff", "wait", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			calls++
			<-ctx.Done()
			return nil, ctx.Err()
		}, nil, compose_v2.WithNodeIdleTimeout(time.Second), compose_v2.WithNodeRetry(retry))
		err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager())
		if !errors.Is(err, context.Canceled) || calls != 1 {
			t.Fatalf("调用方取消应结束退避，实际执行 %d 次，err=%v", calls, err)
		}
	})
}

func TestNodeTimeout_DeadlineCheckedBeforeAcceptingResult(t *testing.T) {
	old := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(old)
	g := singleNodeGraph(t, "fixed_return_time", "work", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		deadline, _ := ctx.Deadline()
		// 占用当前 P 跨过截止时间，节点返回时 timer 回调仍可能待调度。
		for time.Now().Before(deadline) {
		}
		return nil, nil
	}, nil, compose_v2.WithNodeTimeout(100*time.Microsecond))
	for range 10 {
		err := compose_v2.Run(t.Context(), g, compose_v2.NewChannelManager())
		var nt *compose_v2.NodeTimeoutError
		if !errors.As(err, &nt) || nt.Idle {
			t.Fatalf("越过固定截止时间的结果应按固定超时处理，实际 %v", err)
		}
	}
}

func TestNodeTimeout_InheritedLateChildKeepsCommittedPrefix(t *testing.T) {
	for _, tt := range []struct {
		name string
		opt  compose_v2.NodeOption
	}{
		{"idle", compose_v2.WithNodeIdleTimeout(time.Second)},
		{"fixed", compose_v2.WithNodeTimeout(time.Second)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var prepared, finished int
				b := compose_v2.NewBuilder().WithName("late_subgraph")
				b.AddNode("prepare", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					prepared++
					return nil, nil
				})
				b.AddNode("finish", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					finished++
					if finished == 1 {
						<-ctx.Done()
					}
					return nil, nil
				})
				b.AddEdge(compose_v2.StartNode, "prepare")
				b.AddEdge("prepare", "finish")
				b.AddEdge("finish", compose_v2.EndNode)
				child, err := b.Compile()
				if err != nil {
					t.Fatal(err)
				}
				g := idleParent(t, "late_parent", child, tt.opt, compose_v2.WithNodeRetry(compose_v2.SimpleRetry(1)))
				cp := compose_v2.NewInMemoryCheckpointer()
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
					compose_v2.WithSessionID("late-"+tt.name), compose_v2.WithCheckpointer(cp))
				if err != nil || prepared != 1 || finished != 2 {
					t.Fatalf("父节点重试应保留子图已提交的前序，prepare=%d finish=%d err=%v", prepared, finished, err)
				}
			})
		})
	}
}

type childTimeoutHook struct {
	compose_v2.NoOpOrchestrationHooks
	phase string
	ran   bool
}

func (h *childTimeoutHook) OnNodeStart(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	if id == "work" && h.phase != "end" {
		bodyCtx := ctx
		if h.phase == "start_derived_cancel" {
			derived, cancel := context.WithCancel(ctx)
			cancel()
			bodyCtx = derived
		}
		<-ctx.Done()
		h.ran = true
		return bodyCtx, nil
	}
	return ctx, nil
}

func (h *childTimeoutHook) OnNodeEnd(ctx context.Context, id compose_v2.NodeID, _ compose_v2.Channels, _ error) context.Context {
	if id == "work" && h.phase == "end" {
		<-ctx.Done()
		h.ran = true
	}
	return ctx
}

func TestNodeIdleTimeout_InheritedDeadlineStopsChildAfterHook(t *testing.T) {
	for _, phase := range []string{"start", "start_derived_cancel", "end"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := &childTimeoutHook{phase: phase}
				var bodies, conditions int
				b := compose_v2.NewBuilder().WithName("hook_child")
				b.AddNode("work", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					bodies++
					return nil, nil
				})
				b.AddEdge(compose_v2.StartNode, "work")
				b.AddConditionalEdge("work", func(context.Context, compose_v2.Channels) (compose_v2.NodeID, error) {
					conditions++
					return compose_v2.EndNode, nil
				})
				child, err := b.Compile()
				if err != nil {
					t.Fatal(err)
				}
				g := idleParent(t, "hook_parent", child, compose_v2.WithNodeIdleTimeout(time.Second))
				err = compose_v2.Run(compose_v2.WithOrchHooks(t.Context(), h), g, compose_v2.NewChannelManager())
				wantBodies := 0
				if phase == "end" {
					wantBodies = 1
				}
				var timedOut *compose_v2.NodeTimeoutError
				if !h.ran || !errors.As(err, &timedOut) || !timedOut.Idle || bodies != wantBodies || conditions != 0 {
					t.Fatalf("外层空闲超时后不能继续执行子图业务，hook=%v，节点体 %d 次，条件 %d 次，err=%v", h.ran, bodies, conditions, err)
				}
			})
		})
	}
}

func TestNodeIdleTimeout_PreservesInterruptAndSoftCancel(t *testing.T) {
	for _, kind := range []string{"interrupt", "soft_cancel"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				g := singleNodeGraph(t, "idle_"+kind, "work", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					calls++
					<-ctx.Done()
					if kind == "soft_cancel" {
						return nil, compose_v2.ErrSoftCanceled
					}
					_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "continue?"})
					return nil, err
				}, nil, compose_v2.WithNodeIdleTimeout(time.Second), compose_v2.WithNodeRetry(compose_v2.RetryConfig{
					MaxRetries: 2, ShouldRetry: func(error) bool { return true },
				}))
				err := compose_v2.Run(t.Context(), g, compose_v2.NewChannelManager())
				_, interrupted := compose_v2.IsInterrupt(err)
				if calls != 1 || (kind == "interrupt" && !interrupted) ||
					(kind == "soft_cancel" && !errors.Is(err, compose_v2.ErrSoftCanceled)) {
					t.Fatalf("超时收尾应保留控制流错误，调用 %d 次，err=%v", calls, err)
				}
			})
		})
	}
}

func TestNodeIdleTimeout_CheckpointKeepsSuccessfulSibling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var quick, stalled atomic.Int32
		b := compose_v2.NewBuilder().WithName("idle_siblings")
		b.AddNode("progressing", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			quick.Add(1)
			for range 3 {
				time.Sleep(time.Second)
				compose_v2.Heartbeat(ctx)
			}
			return nil, nil
		}, compose_v2.WithNodeIdleTimeout(2*time.Second))
		b.AddNode("stalled", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if stalled.Add(1) == 1 {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return nil, nil
		}, compose_v2.WithNodeIdleTimeout(2*time.Second))
		for _, id := range []compose_v2.NodeID{"progressing", "stalled"} {
			b.AddEdge(compose_v2.StartNode, id)
			b.AddEdge(id, compose_v2.EndNode)
		}
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		cp := compose_v2.NewInMemoryCheckpointer()
		opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("idle-siblings")}
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		var nt *compose_v2.NodeTimeoutError
		if err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...); !errors.As(err, &nt) || !nt.Idle {
			t.Fatalf("兄弟节点的进度不能替卡住的节点续期，err=%v", err)
		}
		saved, err := cp.LoadNS(ctx, "idle-siblings", "")
		if err != nil || saved == nil || saved.Status != compose_v2.CPStatusFailed || len(saved.CurrentFrontier) != 1 ||
			saved.CurrentFrontier[0].NodeID != "stalled" {
			t.Fatalf("失败档应只待重跑卡住的节点，实际 %+v，err=%v", saved, err)
		}
		if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...); err != nil || quick.Load() != 1 || stalled.Load() != 2 {
			t.Fatalf("续跑应只重做失败节点，实际次数 %d/%d，err=%v", quick.Load(), stalled.Load(), err)
		}
	})
}

type idleProgressObserver struct {
	compose_v2.NoOpOrchestrationHooks
	sink      compose_v2.EventSink
	forwarded int
}

type observedSink struct{ compose_v2.EventSink }

func (s observedSink) Send(ev compose_v2.Event) { s.EventSink.Send(ev) }

func (h *idleProgressObserver) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	if compose_v2.GetGraphNS(ctx) != "" {
		compose_v2.GetEventSink(ctx).Send(compose_v2.Event{Type: "observe_graph_start"})
	}
	return ctx, nil
}

func (h *idleProgressObserver) OnNodeEvent(ctx context.Context, _ compose_v2.NodeID, event compose_v2.Event) context.Context {
	if event.Type == "observe_graph_start" {
		h.sink = compose_v2.GetEventSink(ctx)
	} else if event.Type == "relayed_observation" {
		h.forwarded++
	}
	return ctx
}

func TestNodeIdleTimeout_ObserverRelayDoesNotRenew(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &idleProgressObserver{}
		child := singleNodeGraph(t, "child", "work", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}, nil)
		g := singleNodeGraph(t, "parent", "work", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			subCtx, _ := compose_v2.BeginStreamScope(ctx)
			subCtx = compose_v2.WithEventSink(subCtx, observedSink{compose_v2.GetEventSink(subCtx)})
			if err := compose_v2.Run(compose_v2.WithGraphNS(subCtx, "child"), child, compose_v2.NewChannelManager()); err != nil {
				return nil, err
			}
			time.Sleep(2 * time.Second)
			if h.sink != nil {
				h.sink.Send(compose_v2.Event{Type: "relayed_observation"})
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}, nil, compose_v2.WithNodeIdleTimeout(3*time.Second))
		start := time.Now()
		err := compose_v2.Run(compose_v2.WithOrchHooks(t.Context(), h), g, compose_v2.NewChannelManager())
		var timedOut *compose_v2.NodeTimeoutError
		if h.forwarded != 1 {
			t.Fatalf("观测转发应照常交付，实际 %d 次", h.forwarded)
		}
		if !errors.As(err, &timedOut) || !timedOut.Idle || time.Since(start) != 3*time.Second {
			t.Fatalf("观测转发不能延长父节点预算，实际 %s，err=%v", time.Since(start), err)
		}
	})
}
