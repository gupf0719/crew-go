package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

func init() {
	compose_v2.RegisterValueType[string]("string")
}

// singleNodeGraph 是 START → name → END 的一节点图，节点带上 opts。
func singleNodeGraph(t *testing.T, graphName string, name compose_v2.NodeID, fn compose_v2.NodeFunc,
	chs []*compose_v2.Channel[string], opts ...compose_v2.NodeOption) *compose_v2.Graph {
	t.Helper()
	b := compose_v2.NewBuilder().WithName(graphName)
	for _, ch := range chs {
		b = compose_v2.WithChannel(b, ch)
	}
	b.AddNode(name, fn, opts...)
	b.AddEdge(compose_v2.StartNode, name)
	b.AddEdge(name, compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// 节点失败后按 WithNodeRetry 原地重试，成功那次的写入算数、失败那几次的作废。
func TestNodeRetry_RetriesUntilSuccess(t *testing.T) {
	out := compose_v2.LastValue[string]("__nr_out__")
	var calls atomic.Int32
	g := singleNodeGraph(t, "node_retry_basic", "flaky", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		n := calls.Add(1)
		if n < 3 {
			return []compose_v2.ChannelWrite{compose_v2.Write(out, fmt.Sprintf("bad-%d", n))}, errors.New("transient")
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(out, "ok")}, nil
	}, []*compose_v2.Channel[string]{out}, compose_v2.WithNodeRetry(compose_v2.SimpleRetry(2)))

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm); err != nil {
		t.Fatalf("重试两次之后该成功，实际 %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("节点该跑 3 次（1 次 + 2 次重试），实际 %d", got)
	}
	if v, _ := compose_v2.Get(cm.Channels(), out); v != "ok" {
		t.Errorf("只有成功那次尝试的写入算数，实际 channel 值 %q", v)
	}
}

// 重试用完、谓词否决都直接以最后一次的错失败。
func TestNodeRetry_ExhaustedAndVetoed(t *testing.T) {
	transient := errors.New("transient")
	for _, tc := range []struct {
		name      string
		cfg       compose_v2.RetryConfig
		wantCalls int32
	}{
		{"exhausted", compose_v2.SimpleRetry(2), 3},
		{"vetoed", compose_v2.RetryConfig{MaxRetries: 5, ShouldRetry: func(error) bool { return false }}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			g := singleNodeGraph(t, "node_retry_"+tc.name, "n", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				calls.Add(1)
				return nil, transient
			}, nil, compose_v2.WithNodeRetry(tc.cfg))
			err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager())
			if !errors.Is(err, transient) {
				t.Fatalf("该以节点的错失败，实际 %v", err)
			}
			if got := calls.Load(); got != tc.wantCalls {
				t.Errorf("节点跑了 %d 次，want %d", got, tc.wantCalls)
			}
		})
	}
}

// 失败那次尝试里的 SendFrom 投递必须作废——每次尝试一个收集器。
//
// sanity-revert：runNodeBody 改成各次尝试共用一个 sendSink ⟹ 第一次尝试投的两份也留下，
// w 跑 3 次而不是 1 次。
func TestNodeRetry_FailedAttemptSendsDiscarded(t *testing.T) {
	var calls, workerRuns atomic.Int32
	b := compose_v2.NewBuilder().WithName("node_retry_sends")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if calls.Add(1) == 1 {
			compose_v2.SendFrom(ctx, compose_v2.SendDirective{Target: "w"}, compose_v2.SendDirective{Target: "w"})
			return nil, errors.New("transient after send")
		}
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{Target: "w"})
		return nil, nil
	}, compose_v2.WithNodeRetry(compose_v2.SimpleRetry(1)))
	b.AddNode("w", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		workerRuns.Add(1)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("w", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := workerRuns.Load(); got != 1 {
		t.Errorf("只有成功那次尝试投递的 1 份该跑，实际 w 跑了 %d 次——失败尝试的投递漏进来了", got)
	}
}

// 节点私有态跨尝试保留：第一次尝试里已经做成、记下的步骤，第二次读回来跳过。
func TestNodeRetry_StepResultSurvivesAttempts(t *testing.T) {
	var calls, charges atomic.Int32
	g := singleNodeGraph(t, "node_retry_step", "pay", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, done := compose_v2.GetStepResult[string](ctx, "charge"); !done {
			charges.Add(1)
			if err := compose_v2.SaveStepResult(ctx, "charge", "receipt-1"); err != nil {
				return nil, err
			}
		}
		if calls.Add(1) == 1 {
			return nil, errors.New("transient after charge")
		}
		return nil, nil
	}, nil, compose_v2.WithNodeRetry(compose_v2.SimpleRetry(1)))
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("节点该跑 2 次，实际 %d", calls.Load())
	}
	if got := charges.Load(); got != 1 {
		t.Errorf("扣款该只做 1 次（第二次尝试读回记录跳过），实际 %d 次", got)
	}
}

// 答过的问题在重试里不再问：第二次尝试的提问从同一个序号数起、拿到同一个答案。
//
// 这是一个业内实现里真实存在过的坑：重试循环不重置提问计数器，第二次尝试的第一问算成第二问，
// 已答的答案对不上，同一个问题又问一遍、失败前的副作用也重放一次。
func TestNodeRetry_AnsweredQuestionNotReasked(t *testing.T) {
	out := compose_v2.LastValue[string]("__nr_ans_out__")
	var asks, fails atomic.Int32
	g := singleNodeGraph(t, "node_retry_ask", "approve", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗？"})
		if err != nil {
			asks.Add(1)
			return nil, err
		}
		if fails.Add(1) == 1 {
			return nil, errors.New("transient after answer")
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(out, ans.Content)}, nil
	}, []*compose_v2.Channel[string]{out}, compose_v2.WithNodeRetry(compose_v2.SimpleRetry(1)))

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "nr_ask_sid"
	err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑该停在提问上，实际 %v", err)
	}
	cm := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, g, cm, compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("yes")}))
	if err != nil {
		t.Fatalf("续跑后答案在手、第一次尝试失败、第二次尝试该成功，实际 %v", err)
	}
	if got := asks.Load(); got != 1 {
		t.Errorf("问题只该问 1 次，实际问了 %d 次——重试的第二次尝试把答过的问题又问了一遍", got)
	}
	if v, _ := compose_v2.Get(cm.Channels(), out); v != "yes" {
		t.Errorf("第二次尝试该拿到同一个答案，实际 %q", v)
	}
}

// 中断不重试：它不是失败，是在等人。
func TestNodeRetry_InterruptNotRetried(t *testing.T) {
	var calls atomic.Int32
	g := singleNodeGraph(t, "node_retry_interrupt", "ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		calls.Add(1)
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "?"})
		return nil, err
	}, nil, compose_v2.WithNodeRetry(compose_v2.RetryConfig{MaxRetries: 3, ShouldRetry: func(error) bool { return true }}))
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager())
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("该原样交出中断，实际 %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("中断不该被重试（谓词照单全收也一样），实际节点跑了 %d 次", got)
	}
}

// 要重试就得说清楚重试什么：MaxRetries > 0 而不给谓词，装配期当场 panic。
func TestWithNodeRetry_RequiresPredicate(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("WithNodeRetry(MaxRetries=1, 无 ShouldRetry) 该 panic")
		}
	}()
	compose_v2.WithNodeRetry(compose_v2.RetryConfig{MaxRetries: 1})
}

// 响应 ctx 的节点到点被解开，以 *NodeTimeoutError 失败；错误链上不露出 DeadlineExceeded，
// 节点里能从 context.Cause 分出是自己超时。
func TestNodeTimeout_CooperativeNodeTimesOut(t *testing.T) {
	var cause atomic.Value
	g := singleNodeGraph(t, "node_timeout_coop", "slow", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		<-ctx.Done()
		cause.Store(context.Cause(ctx))
		return nil, ctx.Err()
	}, nil, compose_v2.WithNodeTimeout(30*time.Millisecond))

	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager())
	var nte *compose_v2.NodeTimeoutError
	if !errors.As(err, &nte) {
		t.Fatalf("该以 *NodeTimeoutError 失败，实际 %v", err)
	}
	if nte.Timeout != 30*time.Millisecond {
		t.Errorf("Timeout = %v, want 30ms", nte.Timeout)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Error("节点自己超时不该在错误链上露出 DeadlineExceeded——那会被当成调用方的 ctx 到期")
	}
	var causeNTE *compose_v2.NodeTimeoutError
	if c, _ := cause.Load().(error); !errors.As(c, &causeNTE) {
		t.Errorf("节点里 context.Cause(ctx) 该是 *NodeTimeoutError，实际 %v", c)
	}
}

// 超时的尝试按默认谓词重试——超时错误不能「碰巧」落进不重试的那一类。
//
// sanity-revert：给 NodeTimeoutError 加 `Unwrap() error { return e.Err }` ⟹ 链上露出
// DeadlineExceeded ⟹ IsRetryable 判控制流不重试 ⟹ 本用例 Run 失败、节点只跑 1 次。
func TestNodeTimeout_RetriedByDefaultPredicate(t *testing.T) {
	var calls atomic.Int32
	g := singleNodeGraph(t, "node_timeout_retry", "flaky", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if calls.Add(1) == 1 {
			<-ctx.Done()
			return nil, fmt.Errorf("upstream: %w", ctx.Err())
		}
		return nil, nil
	}, nil, compose_v2.WithNodeTimeout(30*time.Millisecond), compose_v2.WithNodeRetry(compose_v2.SimpleRetry(1)))

	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager()); err != nil {
		t.Fatalf("第一次尝试超时、第二次该成功，实际 %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("节点该跑 2 次，实际 %d", got)
	}
}

// 不看 ctx 的节点超时之后才返回成功：这次结果按超时丢弃（超过时限的尝试就是失败的尝试）。
func TestNodeTimeout_LateSuccessDiscarded(t *testing.T) {
	out := compose_v2.LastValue[string]("__nt_late_out__")
	g := singleNodeGraph(t, "node_timeout_late", "deaf", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		time.Sleep(80 * time.Millisecond) // 不看 ctx
		return []compose_v2.ChannelWrite{compose_v2.Write(out, "late")}, nil
	}, []*compose_v2.Channel[string]{out}, compose_v2.WithNodeTimeout(20*time.Millisecond))

	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g, cm)
	var nte *compose_v2.NodeTimeoutError
	if !errors.As(err, &nte) {
		t.Fatalf("超时后才返回的成功该按超时处理，实际 %v", err)
	}
	if nte.Err != nil {
		t.Errorf("节点返回的是成功，NodeTimeoutError.Err 该为 nil，实际 %v", nte.Err)
	}
	if v, _ := compose_v2.Get(cm.Channels(), out); v != "" {
		t.Errorf("被丢弃的那次结果不该写进 channel，实际 %q", v)
	}
}

// 调用方的 ctx 先死了：那是调用方喊停，不改写成节点超时。
func TestNodeTimeout_CallerCancelNotRelabeled(t *testing.T) {
	g := singleNodeGraph(t, "node_timeout_caller", "wait", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, nil, compose_v2.WithNodeTimeout(5*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("该是调用方取消，实际 %v", err)
	}
	var nte *compose_v2.NodeTimeoutError
	if errors.As(err, &nte) {
		t.Error("调用方取消不该被改写成节点超时")
	}
}

// 节点超时之后 panic：panic 是代码问题，原样交出、不改写成超时。
func TestNodeTimeout_PanicKept(t *testing.T) {
	g := singleNodeGraph(t, "node_timeout_panic", "boom", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		<-ctx.Done()
		panic("cleanup exploded")
	}, nil, compose_v2.WithNodeTimeout(20*time.Millisecond))

	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager())
	var pe *compose_v2.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("该带着 panic 失败，实际 %v", err)
	}
	var nte *compose_v2.NodeTimeoutError
	if errors.As(err, &nte) {
		t.Error("panic 不该被改写成超时")
	}
}

// 节点超时是一次普通的节点失败：同超步的兄弟照常跑完、提交，存档是失败现场（不是等答案的挂起），
// 续跑只重跑超时的那个。
//
// sanity-revert：NodeTimeoutError 在链上露出 DeadlineExceeded ⟹ 出口按「调用方 ctx 到期」标成
// Pending ⟹ 本用例的档位断言红。
func TestNodeTimeout_SiblingKeptAndCheckpointFailed(t *testing.T) {
	var slowShouldBlock atomic.Bool
	slowShouldBlock.Store(true)
	var fastRuns atomic.Int32
	b := compose_v2.NewBuilder().WithName("node_timeout_sibling")
	b.AddNode("slow", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if slowShouldBlock.Load() {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return nil, nil
	}, compose_v2.WithNodeTimeout(30*time.Millisecond))
	b.AddNode("fast", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		fastRuns.Add(1)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "slow")
	b.AddEdge(compose_v2.StartNode, "fast")
	b.AddEdge("slow", compose_v2.EndNode)
	b.AddEdge("fast", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "nt_sibling_sid"
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	var nte *compose_v2.NodeTimeoutError
	if !errors.As(err, &nte) {
		t.Fatalf("该以节点超时失败，实际 %v", err)
	}
	cp, err := cper.LoadNS(ctx, sid, "")
	if err != nil || cp == nil {
		t.Fatalf("该留存档，err=%v cp=%v", err, cp)
	}
	if cp.Status != compose_v2.CPStatusFailed {
		t.Errorf("存档档位 %v，want failed——节点超时是这次执行失败了，不是在等答案", cp.Status)
	}

	slowShouldBlock.Store(false)
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid)); err != nil {
		t.Fatalf("续跑该成功，实际 %v", err)
	}
	if got := fastRuns.Load(); got != 1 {
		t.Errorf("成功的兄弟不该重跑，实际 fast 跑了 %d 次", got)
	}
}

// 子图节点经 WithSubgraphNodeOptions 配重试 / 超时：下一次尝试从子图自己的存档续跑，子图里已经跑完的节点不重跑。
func TestSubgraphNodePolicy_RetryResumesChild(t *testing.T) {
	for _, tc := range []struct {
		name string
		// failFirst 是子图第二个节点第一次被调时怎么失败
		failFirst func(ctx context.Context) error
		opts      []compose_v2.NodeOption
	}{
		{
			name:      "retry",
			failFirst: func(context.Context) error { return errors.New("transient") },
			opts:      []compose_v2.NodeOption{compose_v2.WithNodeRetry(compose_v2.SimpleRetry(1))},
		},
		{
			name:      "timeout",
			failFirst: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
			opts: []compose_v2.NodeOption{compose_v2.WithNodeTimeout(50 * time.Millisecond),
				compose_v2.WithNodeRetry(compose_v2.SimpleRetry(1))},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var aRuns, bRuns atomic.Int32
			cb := compose_v2.NewBuilder().WithName("sub_policy_child_" + tc.name)
			cb.AddNode("a", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				aRuns.Add(1)
				return nil, nil
			})
			cb.AddNode("b", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				if bRuns.Add(1) == 1 {
					return nil, tc.failFirst(ctx)
				}
				return nil, nil
			})
			cb.AddEdge(compose_v2.StartNode, "a")
			cb.AddEdge("a", "b")
			cb.AddEdge("b", compose_v2.EndNode)
			child, err := cb.Compile()
			if err != nil {
				t.Fatalf("compile child: %v", err)
			}
			pb := compose_v2.NewBuilder().WithName("sub_policy_parent_" + tc.name)
			pb.AddSubgraphNode("sub", child, compose_v2.WithSubgraphNodeOptions(tc.opts...))
			pb.AddEdge(compose_v2.StartNode, "sub")
			pb.AddEdge("sub", compose_v2.EndNode)
			parent, err := pb.Compile()
			if err != nil {
				t.Fatalf("compile parent: %v", err)
			}
			cper := compose_v2.NewInMemoryCheckpointer()
			if err := compose_v2.Run(context.Background(), parent, compose_v2.NewChannelManager(),
				compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("sub_policy_"+tc.name)); err != nil {
				t.Fatalf("重试的第二次尝试该成功，实际 %v", err)
			}
			if got := aRuns.Load(); got != 1 {
				t.Errorf("子图里已经跑完的 a 不该重跑，实际跑了 %d 次", got)
			}
			if got := bRuns.Load(); got != 2 {
				t.Errorf("b 该跑 2 次（失败一次 + 续跑一次），实际 %d", got)
			}
		})
	}
}

// 时限先到、调用方随后取消、节点再返回成功：这次成功照样按超时丢弃，写入不提交（审计实测：曾经按返回那一刻
// 调用方死没死判，迟到的成功被接受、写入提交了）。
func TestNodeTimeout_LateSuccessAfterCallerCancelDiscarded(t *testing.T) {
	out := compose_v2.LastValue[string]("__nt_late_cancel_out__")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := singleNodeGraph(t, "node_timeout_late_cancel", "deaf", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		time.Sleep(80 * time.Millisecond) // 不看 ctx：时限（30ms）早就到了
		cancel()                          // 调用方在节点返回之前取消
		time.Sleep(10 * time.Millisecond)
		return []compose_v2.ChannelWrite{compose_v2.Write(out, "late")}, nil
	}, []*compose_v2.Channel[string]{out}, compose_v2.WithNodeTimeout(30*time.Millisecond))
	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(ctx, g, cm)
	if err == nil {
		t.Fatal("超过时限的尝试不该算成功")
	}
	if v, _ := compose_v2.Get(cm.Channels(), out); v != "" {
		t.Errorf("被丢弃的那次结果不该写进 channel，实际 %q", v)
	}
}
