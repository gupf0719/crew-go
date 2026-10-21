package compose_v2_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// 上行信号跟着写入者的提交走（见 signal.go）：节点执行成功、本超步提交成功才算数；失败、被
// 中断、被否决的那次执行做出的作废；图成功跑完时把已提交的（含续跑时从存档恢复的）交给调用方。

var (
	sigKindA = compose_v2.NewSignalKind("compose_v2_test.a")
	sigKindB = compose_v2.NewSignalKind("compose_v2_test.b")
)

// sigValues 取 (k, dest) 上的那条（一个收件人上至多一条），以切片返回好断言「没有」与「是哪个」。
func sigValues(s compose_v2.Signals, k *compose_v2.SignalKind, dest string) []string {
	got, ok, _ := s.Take(k, dest)
	if !ok {
		return nil
	}
	return []string{got.Value}
}

func sigEmit(ctx context.Context, v string) {
	compose_v2.EmitSignal(ctx, sigKindA, compose_v2.Signal{Dest: "d", Value: v})
}

// 节点做出的信号在图成功跑完时交给调用方。
func TestSignal_CommittedNodeSignalReachesCaller(t *testing.T) {
	b := compose_v2.NewBuilder().WithName("sig_ok")
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sigEmit(ctx, "v1")
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g := rsCompile(t, b)
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager())
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := sigValues(sigs, sigKindA, "d"); len(got) != 1 || got[0] != "v1" {
		t.Errorf("调用方应收到节点做出的信号，实际 %v", got)
	}
}

// sigSwallowHost 返回一张外层图：它唯一的节点跑 inner，**吞掉 inner 的错**、自己照常成功。
//
// 用它判「作废」：inner 里没成的那次执行做出的信号若越过了 inner 的提交纪律、直接落进外层节点，
// 外层节点成功提交时就会把它带出去。只看 inner 自己的 Run 报不报错是测不出这一点的——Run 报错时
// 调用方那层 CaptureSignals 本来就整个丢弃。
func sigSwallowHost(t *testing.T, name string, inner *compose_v2.Graph) *compose_v2.Graph {
	t.Helper()
	ob := compose_v2.NewBuilder().WithName(name)
	ob.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_ = compose_v2.Run(ctx, inner, compose_v2.NewChannelManager(), compose_v2.WithRunNS(name+"_inner")) // 有意吞掉
		return nil, nil
	})
	ob.AddEdge(compose_v2.StartNode, "host")
	ob.AddEdge("host", compose_v2.EndNode)
	return rsCompile(t, ob)
}

// 节点做出信号之后失败：信号随这次执行作废——哪怕调用方吞掉了这张图的错、自己照常成功。
func TestSignal_FailedNodeSignalDiscarded(t *testing.T) {
	ib := compose_v2.NewBuilder().WithName("sig_fail_inner")
	ib.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sigEmit(ctx, "v1")
		return nil, errors.New("boom")
	})
	ib.AddEdge(compose_v2.StartNode, "n")
	ib.AddEdge("n", compose_v2.EndNode)
	g := sigSwallowHost(t, "sig_fail", rsCompile(t, ib))
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager())
	})
	if err != nil {
		t.Fatalf("外层吞掉了 inner 的错，应成功：%v", err)
	}
	if got := sigValues(sigs, sigKindA, "d"); len(got) != 0 {
		t.Errorf("失败那次执行做出的信号不该交出去，实际 %v", got)
	}
}

// 第一个节点做出信号并提交，第二个节点问人。续跑时第一个节点不再执行，它的信号从存档里恢复、
// 图跑完时交出去——恰好一份。
func TestSignal_CommittedBeforePause_DeliveredOnceAfterResume(t *testing.T) {
	var firstRuns atomic.Int32
	b := compose_v2.NewBuilder().WithName("sig_pause")
	b.AddNode("first", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		firstRuns.Add(1)
		sigEmit(ctx, "from_first")
		return nil, nil
	})
	b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续吗"})
		return nil, err
	})
	b.AddEdge(compose_v2.StartNode, "first")
	b.AddEdge("first", "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g := rsCompile(t, b)
	opts := rsOpts()
	first, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...)
	})
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应停在提问上，got %v", err)
	}
	if first.Len() != 0 {
		t.Errorf("停下时不该交出信号（图没跑完），实际 %d 条", first.Len())
	}
	answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("是")}
	second, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), append(opts, compose_v2.WithResumeAnswers(answers))...)
	})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if got := sigValues(second, sigKindA, "d"); len(got) != 1 || got[0] != "from_first" {
		t.Errorf("续跑跑完时应恰好交出一份停下之前已提交的信号，实际 %v", got)
	}
	if n := firstRuns.Load(); n != 1 {
		t.Errorf("first 已提交，续跑不该再执行，实际 %d 次", n)
	}
}

// 条件边按 CondFailRerunFrom 否决了第一次执行：那次执行做出的信号作废，只剩重做那次的。
func TestSignal_RejectedExecutionSignalDiscarded(t *testing.T) {
	var runs atomic.Int32
	b := compose_v2.NewBuilder().WithName("sig_reject")
	b.AddNode("a", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sigEmit(ctx, fmt.Sprintf("v%d", runs.Add(1)))
		return nil, nil
	})
	var evals atomic.Int32
	b.AddConditionalEdge("a", func(context.Context, compose_v2.Channels) (compose_v2.NodeID, error) {
		if evals.Add(1) == 1 {
			return "", errors.New("产出不合格")
		}
		return compose_v2.EndNode, nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	b.AddEdge(compose_v2.StartNode, "a")
	g := rsCompile(t, b)
	opts := rsOpts()
	if _, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...)
	}); err == nil {
		t.Fatal("第一次应被否决")
	}
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...)
	})
	if err != nil {
		t.Fatalf("续跑: %v", err)
	}
	if got := sigValues(sigs, sigKindA, "d"); len(got) != 1 || got[0] != "v2" {
		t.Errorf("只该交出重做那次的信号，实际 %v", got)
	}
}

// cond 里做出的信号跟着成员走：cond 通过才算数；cond 失败时作废——哪怕调用方吞掉了这张图的错。
func TestSignal_CondPhaseSignalFollowsMember(t *testing.T) {
	for _, pass := range []bool{true, false} {
		t.Run(fmt.Sprintf("pass=%v", pass), func(t *testing.T) {
			ib := compose_v2.NewBuilder().WithName(fmt.Sprintf("sig_cond_inner_%v", pass))
			ib.AddNode("a", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
			ib.AddConditionalEdge("a", func(ctx context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
				sigEmit(ctx, "from_cond")
				if !pass {
					return "", errors.New("判不了")
				}
				return compose_v2.EndNode, nil
			})
			ib.AddEdge(compose_v2.StartNode, "a")
			g := sigSwallowHost(t, fmt.Sprintf("sig_cond_%v", pass), rsCompile(t, ib))
			sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
				return compose_v2.Run(ctx, g, compose_v2.NewChannelManager())
			})
			if err != nil {
				t.Fatalf("外层吞掉了 inner 的错，应成功：%v", err)
			}
			got := sigValues(sigs, sigKindA, "d")
			if pass && (len(got) != 1 || got[0] != "from_cond") {
				t.Errorf("cond 通过时它做出的信号应随成员交出，实际 %v", got)
			}
			if !pass && len(got) != 0 {
				t.Errorf("cond 失败时它做出的信号应作废，实际 %v", got)
			}
		})
	}
}

// 节点里跑的嵌套图成功时把信号交给外层节点；外层节点随后失败的话，一起作废。
func TestSignal_NestedGraphBubblesThroughOuterCommit(t *testing.T) {
	ib := compose_v2.NewBuilder().WithName("sig_inner")
	ib.AddNode("x", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sigEmit(ctx, "from_inner")
		return nil, nil
	})
	ib.AddEdge(compose_v2.StartNode, "x")
	ib.AddEdge("x", compose_v2.EndNode)
	inner := rsCompile(t, ib)
	for _, outerFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("outerFails=%v", outerFails), func(t *testing.T) {
			ob := compose_v2.NewBuilder().WithName(fmt.Sprintf("sig_outer_%v", outerFails))
			ob.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				if err := compose_v2.Run(ctx, inner, compose_v2.NewChannelManager(), compose_v2.WithRunNS("sig_inner")); err != nil {
					return nil, err
				}
				if outerFails {
					return nil, errors.New("outer boom")
				}
				return nil, nil
			})
			ob.AddEdge(compose_v2.StartNode, "host")
			ob.AddEdge("host", compose_v2.EndNode)
			g := rsCompile(t, ob)
			var got []string
			_, _ = compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
				s, err := compose_v2.CaptureSignals(ctx, func(ctx context.Context) error {
					return compose_v2.Run(ctx, g, compose_v2.NewChannelManager())
				})
				got = sigValues(s, sigKindA, "d")
				return err
			})
			if !outerFails && (len(got) != 1 || got[0] != "from_inner") {
				t.Errorf("嵌套图的信号应随外层节点交出，实际 %v", got)
			}
			if outerFails && len(got) != 0 {
				t.Errorf("外层节点失败时嵌套图交上来的信号应一起作废，实际 %v", got)
			}
		})
	}
}

// RunWithRetry：失败那次尝试做出的信号作废，只交出成功那次的。
//
// ⚠️ fixture 让**成功那次尝试不做决定**：同一个收件人上后交的本来就会盖掉先交的，两次都做决定的话
// 「失败那次漏进来了」也被成功那次盖住、测不出来（实测：去掉按尝试收的那一层，旧写法照样全绿）。
func TestSignal_RunWithRetry_FailedAttemptDiscarded(t *testing.T) {
	attempt := 0
	cfg := compose_v2.RetryConfig{MaxRetries: 2, ShouldRetry: func(error) bool { return true }}
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		_, err := compose_v2.RunWithRetry(ctx, cfg, func(ctx context.Context) (int, error) {
			attempt++
			if attempt == 1 {
				sigEmit(ctx, "attempt1")
				return 0, errors.New("transient")
			}
			compose_v2.EmitSignal(ctx, sigKindA, compose_v2.Signal{Dest: "d2", Value: "attempt2"})
			return 1, nil
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := sigValues(sigs, sigKindA, "d"); len(got) != 0 {
		t.Errorf("失败那次尝试做出的信号应作废，实际交出了 %v", got)
	}
	if got := sigValues(sigs, sigKindA, "d2"); len(got) != 1 || got[0] != "attempt2" {
		t.Errorf("成功那次尝试做出的信号应交出，实际 %v", got)
	}
}

// 先后两个超步各做一次决定：后做的为准；后面的超步没再做决定不会抹掉前面的。
func TestSignal_SequentialSuperstepsLaterWins(t *testing.T) {
	for _, bDecides := range []bool{true, false} {
		t.Run(fmt.Sprintf("b_decides=%v", bDecides), func(t *testing.T) {
			b := compose_v2.NewBuilder().WithName(fmt.Sprintf("sig_seq_%v", bDecides))
			b.AddNode("a", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				sigEmit(ctx, "from_a")
				return nil, nil
			})
			b.AddNode("b", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				if bDecides {
					sigEmit(ctx, "from_b")
				}
				return nil, nil
			})
			b.AddEdge(compose_v2.StartNode, "a")
			b.AddEdge("a", "b")
			b.AddEdge("b", compose_v2.EndNode)
			g := rsCompile(t, b)
			sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
				return compose_v2.Run(ctx, g, compose_v2.NewChannelManager())
			})
			if err != nil {
				t.Fatal(err)
			}
			want := "from_a"
			if bDecides {
				want = "from_b"
			}
			if got := sigValues(sigs, sigKindA, "d"); len(got) != 1 || got[0] != want {
				t.Errorf("先后两个超步：应以 %q 为准，实际 %v", want, got)
			}
		})
	}
}

// sigFanoutGraph：a、b 同一超步并发，各做一次决定（值为空 = 这个成员不做决定；a 可选先问人）。
func sigFanoutGraph(t *testing.T, name, va, vb string, aAsks bool) *compose_v2.Graph {
	t.Helper()
	b := compose_v2.NewBuilder().WithName(name)
	b.AddNode("a", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		time.Sleep(10 * time.Millisecond) // a 后跑完：并入次序跟完成次序无关
		if aAsks {
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续吗"}); err != nil {
				return nil, err
			}
		}
		if va != "" {
			sigEmit(ctx, va)
		}
		return nil, nil
	})
	b.AddNode("b", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if vb != "" {
			sigEmit(ctx, vb)
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddEdge(compose_v2.StartNode, "b")
	b.AddEdge("a", compose_v2.EndNode)
	b.AddEdge("b", compose_v2.EndNode)
	return rsCompile(t, b)
}

// runSigMaybeResume 在 cp 上跑一次（sid "s"）；停在提问上时答「是」续跑一次。返回调用方收到的信号与
// 最终的错。
func runSigMaybeResume(t *testing.T, g *compose_v2.Graph, cp compose_v2.Checkpointer) (compose_v2.Signals, error) {
	t.Helper()
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("s")}
	var sigs compose_v2.Signals
	var err error
	sigs, err = compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...)
	})
	if ie, ok := compose_v2.IsInterrupt(err); ok {
		answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("是")}
		sigs, err = compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
			return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), append(opts, compose_v2.WithResumeAnswers(answers))...)
		})
	}
	return sigs, err
}

// 同一超步的两个成员都对同一个收件人做了决定：一个收件人在同一超步里只能有一个决定方（单写入者，
// 跟 LastValue 同一条规则）——报 SignalConflictError（类型本身带着 ErrRunAborted：不重试、Run 出口清档），
// **值相同也报**，调用方什么也收不到，存档里什么也不剩；**停没停过结果一样**（a 先停在提问上、b 的
// 决定先记在一边，续跑时 a 再做决定，照样报）。
//
// ⚠️ 曾经按 frontier 顺序拼起来、由消费方取最后一个：赢家由声明顺序决定，而 a 停过一次之后，b 的先
// 经部分提交并入、a 的续跑后才并入，赢家翻转（审计实测 SALES ⟶ BILLING）。后来改成「值不同才报、
// 不清档、留给续跑」：续跑把整个超步的成员连同副作用重做一遍，决定从记录里拿回时次次再撞（实测）。
func TestSignal_FanoutBothDecide_TerminalRegardlessOfPause(t *testing.T) {
	for _, tc := range []struct{ name, va, vb string }{
		{"differ", "billing", "sales"},
		{"agree", "billing", "billing"},
	} {
		for _, aAsks := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s_a_asks=%v", tc.name, aAsks), func(t *testing.T) {
				cp := compose_v2.NewInMemoryCheckpointer()
				g := sigFanoutGraph(t, fmt.Sprintf("sig_both_%s_%v", tc.name, aAsks), tc.va, tc.vb, aAsks)
				sigs, err := runSigMaybeResume(t, g, cp)
				var conflict *compose_v2.SignalConflictError
				if !errors.As(err, &conflict) || !errors.Is(err, compose_v2.ErrRunAborted) {
					t.Fatalf("两个成员都做了决定应报 SignalConflictError 并带 ErrRunAborted，实际 err=%v", err)
				}
				if compose_v2.IsRetryable(err) {
					t.Error("冲突是设计错误，不该可重试")
				}
				if sigs.Len() != 0 {
					t.Errorf("冲突时调用方不该收到任何信号，实际 %d 条", sigs.Len())
				}
				f, s := conflict.First.Value, conflict.Second.Value
				if !(f == tc.va && s == tc.vb) && !(f == tc.vb && s == tc.va) {
					t.Errorf("冲突应报出两个成员各自的决定 %q 与 %q，实际 %q 与 %q", tc.va, tc.vb, f, s)
				}
				pend, perr := cp.ListPending(context.Background(), "s")
				if perr != nil {
					t.Fatal(perr)
				}
				if len(pend) != 0 {
					t.Errorf("冲突是终局：存档该清掉，实际还剩 %d 份", len(pend))
				}
			})
		}
	}
}

// 同一超步只有一个成员做决定、另一个不做：正常交给调用方，停没停过结果一样。规则管的是「几个成员
// 做了决定」，不是「几个成员拿得到决定权」。
func TestSignal_FanoutOneDecider_Delivered(t *testing.T) {
	for _, tc := range []struct{ name, va, vb, want string }{
		{"a_decides", "billing", "", "billing"},
		{"b_decides", "", "sales", "sales"},
	} {
		for _, aAsks := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s_a_asks=%v", tc.name, aAsks), func(t *testing.T) {
				g := sigFanoutGraph(t, fmt.Sprintf("sig_one_%s_%v", tc.name, aAsks), tc.va, tc.vb, aAsks)
				sigs, err := runSigMaybeResume(t, g, compose_v2.NewInMemoryCheckpointer())
				if err != nil {
					t.Fatal(err)
				}
				if got := sigValues(sigs, sigKindA, "d"); len(got) != 1 || got[0] != tc.want {
					t.Errorf("只有一个成员做决定时应交出它的决定 %q，实际 %v", tc.want, got)
				}
			})
		}
	}
}

// Take 只取种类与收件人都对上的，其余原样留下；Signals 的 JSON 往返保持内容。同一个收件人上
// 先后交的，后交的为准。
func TestSignals_TakeAndJSONRoundTrip(t *testing.T) {
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		compose_v2.EmitSignal(ctx, sigKindA, compose_v2.Signal{Dest: "d1", Value: "old", Origin: "o0"})
		compose_v2.EmitSignal(ctx, sigKindA, compose_v2.Signal{Dest: "d1", Value: "x", Origin: "o1"})
		compose_v2.EmitSignal(ctx, sigKindB, compose_v2.Signal{Dest: "d1", Value: "y"})
		compose_v2.EmitSignal(ctx, sigKindA, compose_v2.Signal{Dest: "d2", Value: "z"})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if sigs.Len() != 3 {
		t.Fatalf("同一个 (种类, 收件人) 只该有一条，实际 %d 条", sigs.Len())
	}
	b, err := json.Marshal(sigs)
	if err != nil {
		t.Fatal(err)
	}
	var back compose_v2.Signals
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	got, ok, rest := back.Take(sigKindA, "d1")
	if !ok || got.Value != "x" || got.Origin != "o1" {
		t.Errorf("Take(A, d1) = %+v ok=%v，应是后交的那条", got, ok)
	}
	if rest.Len() != 2 {
		t.Errorf("其余两条应原样留下，实际 %d 条", rest.Len())
	}
	if v := sigValues(rest, sigKindB, "d1"); len(v) != 1 || v[0] != "y" {
		t.Errorf("rest 里的 B 信号丢了：%v", v)
	}
}

// 同名只能铸一次；panic 正文说出第一次是在哪儿铸的（两个包抢同一个名字时好定位）。
func TestNewSignalKind_DuplicatePanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("同名第二次铸造应 panic")
		}
		if msg := fmt.Sprint(r); !strings.Contains(msg, "signal_test.go") {
			t.Errorf("panic 正文应带上先铸者的位置，实际 %q", msg)
		}
	}()
	compose_v2.NewSignalKind("compose_v2_test.a")
}

// 没有任何执行在收时（脱离出来的独立运行、不在节点里），信号交不出去、也不会落到别人头上。
func TestSignal_DetachedExecutionDoesNotLeakIntoCaller(t *testing.T) {
	var emitted bool
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		emitted = compose_v2.EmitSignal(compose_v2.DetachExecution(ctx), sigKindA, compose_v2.Signal{Dest: "d", Value: "x"})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if emitted || sigs.Len() != 0 {
		t.Errorf("脱离出来的执行做出的信号不该落进当前执行：emitted=%v len=%d", emitted, sigs.Len())
	}
}

// 同一超步一个成员做出信号并成功、另一个问人：部分提交保留成功那个的信号；续跑时它不再执行，
// 图跑完时它的信号恰好交出一份。
func TestSignal_PartialSuperstepCommitKeepsSucceededMemberSignal(t *testing.T) {
	var aRuns atomic.Int32
	b := compose_v2.NewBuilder().WithName("sig_partial")
	b.AddNode("a", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		aRuns.Add(1)
		sigEmit(ctx, "from_a")
		return nil, nil
	})
	b.AddNode("b", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续吗"})
		return nil, err
	})
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddEdge(compose_v2.StartNode, "b")
	b.AddEdge("a", compose_v2.EndNode)
	b.AddEdge("b", compose_v2.EndNode)
	g := rsCompile(t, b)
	opts := rsOpts()
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("b 应停在提问上，got %v", err)
	}
	answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("是")}
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), append(opts, compose_v2.WithResumeAnswers(answers))...)
	})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if got := sigValues(sigs, sigKindA, "d"); len(got) != 1 || got[0] != "from_a" {
		t.Errorf("部分提交里成功成员的信号应保留、跑完时恰好交出一份，实际 %v", got)
	}
	if n := aRuns.Load(); n != 1 {
		t.Errorf("a 已在部分提交里成功，续跑不该再执行，实际 %d 次", n)
	}
}
