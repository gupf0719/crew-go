package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// 上行信号在「执行的边界」上的几处：钩子换了 ctx、图级钩子、复用 cm、Restore、条件边里的事件钩子、
// 引擎铸的收件人。每一处都是「信号该交给谁 / 该不该交」这一个问题在不同入口上的答案。

// sigEmitHooks 是在图级 / 节点事件钩子里做决定、或替换 ctx 的编排钩子。
type sigEmitHooks struct {
	compose_v2.NoOpOrchestrationHooks
	startForeign bool   // OnGraphStart 返回一个不是从入参派生的 ctx（违约，但实测有人沿用缓存的 ctx）
	endValue     string // OnGraphEnd 做出的决定（空 = 不做）
	endViaEvent  bool   // OnGraphEnd 经 EventSink 发一个事件，由 OnNodeEvent 做决定
	eventValue   string // OnNodeEvent 做出的决定（空 = 不做）
}

func (h *sigEmitHooks) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	if h.startForeign {
		return context.Background(), nil
	}
	return ctx, nil
}

func (h *sigEmitHooks) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, _ error) context.Context {
	if h.endViaEvent {
		if s := compose_v2.GetEventSink(ctx); s != nil {
			s.Send(compose_v2.Event{Type: "sig_probe"})
		}
	}
	if h.endValue != "" {
		sigEmit(ctx, h.endValue)
	}
	return ctx
}

func (h *sigEmitHooks) OnNodeEvent(ctx context.Context, _ compose_v2.NodeID, ev compose_v2.Event) context.Context {
	if h.eventValue != "" && ev.Type == "sig_probe" {
		sigEmit(ctx, h.eventValue)
	}
	return ctx
}

func sigOneNodeGraph(t *testing.T, name string, fn compose_v2.NodeFunc) *compose_v2.Graph {
	t.Helper()
	b := compose_v2.NewBuilder().WithName(name)
	b.AddNode("n", fn)
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	return rsCompile(t, b)
}

// OnGraphStart 返回一个不是从入参派生的 ctx：图成功了，它已提交的信号照样交给调用方。
//
// 曾经出口那道 defer 读的是被钩子替换掉的 ctx——上面没有调用方在收，信号静默丢失、`err == nil`
// （审计实测：不挂钩子收到 1 条，挂上收到 0 条）。
func TestSignal_OnGraphStartForeignCtxStillBubbles(t *testing.T) {
	g := sigOneNodeGraph(t, "sig_foreign_start", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sigEmit(ctx, "from_node")
		return nil, nil
	})
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithHooks(&sigEmitHooks{startForeign: true}))
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := sigValues(sigs, sigKindA, "d"); len(got) != 1 || got[0] != "from_node" {
		t.Errorf("OnGraphStart 换了 ctx 之后图的信号仍应交给调用方，实际 %v", got)
	}
}

// OnGraphEnd 在图的节点之后才跑，它做出的决定是后做的：同一个收件人上应以它为准。
//
// 曾经它直接交给调用方、而本图的信号随后才在出口冒上去，于是先做的盖掉了后做的（审计实测
// 「OnGraphEnd(后做),node(先做)」的顺序）。
func TestSignal_OnGraphEndOrderedAfterGraphSignals(t *testing.T) {
	g := sigOneNodeGraph(t, "sig_end_order", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sigEmit(ctx, "from_node")
		return nil, nil
	})
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithHooks(&sigEmitHooks{endValue: "from_end"}))
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := sigValues(sigs, sigKindA, "d"); len(got) != 1 || got[0] != "from_end" {
		t.Errorf("OnGraphEnd 的决定是后做的、应为准，实际 %v", got)
	}
}

// OnGraphEnd 里经 EventSink 触发的 OnNodeEvent 做出的决定不生效（OnNodeEvent 只作观测）：调用方收到的是
// 节点里做出的那个。要在 OnGraphEnd 里改决定就直接做（TestSignal_OnGraphEndOrderedAfterGraphSignals）。
func TestSignal_OnGraphEndViaEventSinkHasNoEffect(t *testing.T) {
	g := sigOneNodeGraph(t, "sig_end_event_order", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sigEmit(ctx, "from_node")
		return nil, nil
	})
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithHooks(&sigEmitHooks{endViaEvent: true, eventValue: "from_end_event"}))
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := sigValues(sigs, sigKindA, "d"); len(got) != 1 || got[0] != "from_node" {
		t.Errorf("OnGraphEnd 经事件钩子做出的决定不该生效，调用方应收到节点做出的 from_node，实际 %v", got)
	}
}

// sigThreeMemberGraph：a、b、c 三个成员同一超步跑、都直接连 End。
func sigThreeMemberGraph(t *testing.T, name string, a, b, c compose_v2.NodeFunc) *compose_v2.Graph {
	t.Helper()
	bd := compose_v2.NewBuilder().WithName(name)
	bd.AddNode("a", a)
	bd.AddNode("b", b)
	bd.AddNode("c", c)
	for _, n := range []compose_v2.NodeID{"a", "b", "c"} {
		bd.AddEdge(compose_v2.StartNode, n)
		bd.AddEdge(n, compose_v2.EndNode)
	}
	return rsCompile(t, bd)
}

// 同一超步 a 问人、b / c 都做了决定：冲突是终局（SignalConflictError 本身带着 ErrRunAborted），a 的提问不记——这个 Run 已经
// 结束，问题答了也用不上；存档清掉，同一个 sid 再跑是新开，不会捡起这份档再撞一次。
//
// 曾经冲突不清档、提问照记进存档：调用方带着答案续跑，b / c 的决定要是从记录里拿回的就原样再撞，
// 那一问白答（实测）。
func TestSignal_ConflictIsTerminal_SiblingQuestionNotKept(t *testing.T) {
	var aRuns atomic.Int32
	g := sigThreeMemberGraph(t, "sig_conflict_with_ask",
		func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			aRuns.Add(1)
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗"}); err != nil {
				return nil, err
			}
			return nil, nil
		},
		func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			sigEmit(ctx, "billing")
			return nil, nil
		},
		func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			sigEmit(ctx, "sales")
			return nil, nil
		})
	const sid = "sig_conflict_ask"
	cper := compose_v2.NewInMemoryCheckpointer()
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid)}
	ctx := context.Background()

	err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...)
	var conflict *compose_v2.SignalConflictError
	if !errors.As(err, &conflict) || !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("应报冲突并带 ErrRunAborted：%v", err)
	}
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Errorf("终局失败压过中断，不该报成中断：%v", err)
	}
	if cp, lerr := cper.LoadNS(ctx, sid, ""); lerr == nil && cp != nil {
		t.Fatalf("冲突是终局，存档应清掉，实际还在（挂起 %d 条）", len(cp.PendingInterrupts))
	}
	if pend, _ := cper.ListPending(ctx, sid); len(pend) != 0 {
		t.Errorf("没有人该在等回答，ListPending 应为空，实际 %d 份", len(pend))
	}
	if aRuns.Load() != 1 {
		t.Fatalf("前提：a 应跑过 1 次，实际 %d", aRuns.Load())
	}
}

// 同一超步一个成员终局失败、另两个冲突：两者都是终局，返回的错带 ErrRunAborted、清档。
//
// 曾经冲突不带 ErrRunAborted、又抢先返回：不清档，留下一份同 sid 每跑一次就再撞一次冲突的档
// （审计实测）。
func TestSignal_ConflictWithSiblingAbort_Terminal(t *testing.T) {
	g := sigThreeMemberGraph(t, "sig_conflict_abort",
		func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, fmt.Errorf("协议违规: %w", compose_v2.ErrRunAborted)
		},
		func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			sigEmit(ctx, "billing")
			return nil, nil
		},
		func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			sigEmit(ctx, "sales")
			return nil, nil
		})
	const sid = "sig_conflict_abort"
	cper := compose_v2.NewInMemoryCheckpointer()
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("同超步有成员终局失败，应报终局失败：%v", err)
	}
	if cp, lerr := cper.LoadNS(context.Background(), sid, ""); lerr == nil && cp != nil {
		t.Errorf("终局失败应清档，存档却还在（status=%v）", cp.Status)
	}
}

// 同一超步「成员写 vs SendFrom payload 写」同一个单写入者 channel，同时两个成员都做了决定：两处都是
// 单写入者冲突、都终局，返回的错带 ErrRunAborted、清档。曾经信号冲突不终局、又排在 payload 那道检测
// 之前返回：那道检测根本没机会跑、档也没清（审计实测）。
func TestSignal_ConflictWithPayloadSingleWriter_Terminal(t *testing.T) {
	lv := compose_v2.LastValue[string]("sig_sw_lv")
	bd := compose_v2.NewBuilder().WithName("sig_conflict_sw")
	compose_v2.WithChannel(bd, lv)
	bd.AddNode("b", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sigEmit(ctx, "billing")
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{Target: "t",
			Payload: []compose_v2.ChannelWrite{compose_v2.Write(lv, "from_payload")}})
		return nil, nil
	})
	bd.AddNode("c", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sigEmit(ctx, "sales")
		return []compose_v2.ChannelWrite{compose_v2.Write(lv, "from_member")}, nil
	})
	bd.AddNode("t", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	for _, n := range []compose_v2.NodeID{"b", "c"} {
		bd.AddEdge(compose_v2.StartNode, n)
		bd.AddEdge(n, compose_v2.EndNode)
	}
	bd.AddEdge("t", compose_v2.EndNode)
	g := rsCompile(t, bd)
	const sid = "sig_conflict_sw"
	cper := compose_v2.NewInMemoryCheckpointer()
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("payload 撞了单写入者，应报终局失败：%v", err)
	}
	if cp, lerr := cper.LoadNS(context.Background(), sid, ""); lerr == nil && cp != nil {
		t.Errorf("终局失败应清档，存档却还在（status=%v）", cp.Status)
	}
}

// 同一个 cm 跑两次新开：第二次成功时不该把第一次已提交的再交一次。
func TestSignal_ReusedCMFreshRunDoesNotRedeliver(t *testing.T) {
	run := 0
	g := sigOneNodeGraph(t, "sig_reuse_cm", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		run++
		// 两次交给不同的收件人：同一个收件人上后交的本来就会盖掉前一次，测不出「旧的又交了一次」
		dest := "d_run1"
		if run == 2 {
			dest = "d_run2"
		}
		compose_v2.EmitSignal(ctx, sigKindA, compose_v2.Signal{Dest: dest, Value: "v"})
		return nil, nil
	})
	cm := compose_v2.NewChannelManager()
	for i := 1; i <= 2; i++ {
		sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
			return compose_v2.Run(ctx, g, cm, compose_v2.WithFreshRun())
		})
		if err != nil {
			t.Fatal(err)
		}
		if i == 2 && sigs.Len() != 1 {
			t.Errorf("第二次新开只该交出自己做出的 1 条，实际 %d 条（上一次的又交了一次）", sigs.Len())
		}
	}
}

// Restore 恢复的是存档那一刻的状态：存档里没有信号时，cm 上残留的旧信号要清掉，不能在这次成功时
// 交出去。
func TestSignal_RestoreReplacesStaleSignals(t *testing.T) {
	// cm 上先留一条旧信号：在它上面跑完一张会做决定的图
	// 节点名跟下面那张图不同：复用 cm 时同名节点的访问序号会串（那是复用 cm 的既有限制，见
	// NewChannelManager），这条只测信号
	sb := compose_v2.NewBuilder().WithName("sig_stale_src")
	sb.AddNode("stale_n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.EmitSignal(ctx, sigKindA, compose_v2.Signal{Dest: "d_stale", Value: "stale"})
		return nil, nil
	})
	sb.AddEdge(compose_v2.StartNode, "stale_n")
	sb.AddEdge("stale_n", compose_v2.EndNode)
	stale := rsCompile(t, sb)
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), stale, cm); err != nil {
		t.Fatal(err)
	}
	// 另一张图停在提问上、不做任何决定，存档里没有信号
	asked := false
	g := sigOneNodeGraph(t, "sig_restore_dst", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		asked = true
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续吗"})
		return nil, err
	})
	opts := rsOpts()
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || !asked {
		t.Fatalf("应停在提问上，got %v", err)
	}
	answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("是")}
	// 续跑用那个带着旧信号的 cm
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, cm, append(opts, compose_v2.WithResumeAnswers(answers))...)
	})
	if err != nil {
		t.Fatal(err)
	}
	if sigs.Len() != 0 {
		t.Errorf("存档里没有信号，续跑成功时不该交出 cm 上残留的旧信号，实际 %d 条", sigs.Len())
	}
}

// CallHook：采纳一个不是从入参派生的 ctx 时，之后做出的决定照样交给原来那段执行。
func TestCallHook_RestoresSignalScope(t *testing.T) {
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		adopted, _ := compose_v2.CallHook(ctx, func(context.Context) (context.Context, error) {
			return context.Background(), nil // 钩子违约返回的 ctx：上面没有任何执行在收
		})
		if !compose_v2.EmitSignal(adopted, sigKindA, compose_v2.Signal{Dest: "d", Value: "kept"}) {
			t.Error("采纳之后应有执行在收")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := sigValues(sigs, sigKindA, "d"); len(got) != 1 || got[0] != "kept" {
		t.Errorf("采纳之后做出的决定应交给原来那段执行，实际 %v", got)
	}
}

type adoptBizKey struct{}

// hookReturning 返回一个直接交回 got 的钩子。
func hookReturning(got context.Context) func(context.Context) (context.Context, error) {
	return func(context.Context) (context.Context, error) { return got, nil }
}

// TestCallHook_ByOrigin：采纳按钩子返回的 ctx 是哪来的分。
//   - 两种都一样：框架的身份与位置（这里取 sid、ns 与会话 ID）取交给钩子之前的那个——钩子推的也不算；
//   - 派生的：业务值、调用方可配的（这里取软取消与 checkpointer）、取消都听钩子的；
//   - 外来的（别处缓存的 ctx）：业务值取钩子的，可配的与取消取框架这次的——那上面是别的请求的；
//     context.Cause 跟 Err 是同一个请求的（曾经 Err 是这次的、Cause 是那个请求的）；
//   - 钩子返回 nil：原样用交给它的那个。
func TestCallHook_ByOrigin(t *testing.T) {
	cpNow, cpOther := compose_v2.NewInMemoryCheckpointer(), compose_v2.NewInMemoryCheckpointer()
	scNow, scOther := compose_v2.NewSoftCancelSignal(), compose_v2.NewSoftCancelSignal()
	base, cancelBase := context.WithCancelCause(compose_v2.WithSoftCancelCtx(compose_v2.WithCheckpointerCtx(compose_v2.WithThreadIDCtx(
		compose_v2.WithSessionIDCtx(compose_v2.WithGraphNS(context.Background(), "outer/x"), "sid-now"), "conv-now"), cpNow), scNow))
	defer cancelBase(nil)
	check := func(t *testing.T, got context.Context, wantSC *compose_v2.SoftCancelSignal, wantCp compose_v2.Checkpointer) {
		t.Helper()
		if sid, ns, conv := compose_v2.GetSessionID(got), compose_v2.GetGraphNS(got), compose_v2.GetThreadID(got); sid != "sid-now" || ns != "outer/x" || conv != "conv-now" {
			t.Errorf("身份与位置应取交给钩子之前的那个：sid=%q ns=%q 会话 ID=%q", sid, ns, conv)
		}
		if v, _ := got.Value(adoptBizKey{}).(string); v != "span" {
			t.Errorf("业务在钩子里推的值应保留，实际 %q", v)
		}
		if compose_v2.GetSoftCancel(got) != wantSC {
			t.Error("软取消取错了一边")
		}
		if compose_v2.GetCheckpointer(got) != wantCp {
			t.Error("checkpointer 取错了一边")
		}
	}

	t.Run("派生的：可配的与取消听钩子的", func(t *testing.T) {
		var cancel context.CancelCauseFunc
		got, _ := compose_v2.CallHook(base, func(c context.Context) (context.Context, error) {
			var hookCtx context.Context
			hookCtx, cancel = context.WithCancelCause(context.WithValue(compose_v2.WithSoftCancelCtx(compose_v2.WithCheckpointerCtx(
				compose_v2.WithThreadIDCtx(compose_v2.WithSessionIDCtx(c, "sid-pushed"), "conv-hook"), cpOther), scOther), adoptBizKey{}, "span"))
			return hookCtx, nil
		})
		check(t, got, scOther, cpOther)
		cancel(errors.New("hook-timeout"))
		if got.Err() == nil || context.Cause(got).Error() != "hook-timeout" {
			t.Errorf("钩子在派生的 ctx 上加的取消应生效、原因是它给的：Err=%v Cause=%v", got.Err(), context.Cause(got))
		}
	})

	var foreign context.Context
	t.Run("外来的：可配的与取消取框架这次的", func(t *testing.T) {
		// 别处缓存的 ctx：另一次钩子调用交给钩子的那个（带着那一次的标记）
		var otherCall context.Context
		_, _ = compose_v2.CallHook(context.Background(), func(c context.Context) (context.Context, error) {
			otherCall = c
			return c, nil
		})
		stale, cancelStale := context.WithCancelCause(context.WithValue(compose_v2.WithSoftCancelCtx(compose_v2.WithCheckpointerCtx(
			compose_v2.WithThreadIDCtx(compose_v2.WithSessionIDCtx(compose_v2.WithGraphNS(otherCall, "stale/y"), "sid-stale"),
				"conv-stale"), cpOther), scOther), adoptBizKey{}, "span"))
		foreign, _ = compose_v2.CallHook(base, hookReturning(stale))
		check(t, foreign, scNow, cpNow)
		cancelStale(errors.New("req-A: client gone"))
		if foreign.Err() != nil {
			t.Error("外来 ctx 的取消（上一个请求结束了）不该停下这一次")
		}
		if bg, _ := compose_v2.CallHook(base, hookReturning(context.Background())); compose_v2.GetCheckpointer(bg) != cpNow || compose_v2.GetSoftCancel(bg) != scNow {
			t.Error("钩子返回 Background 时可配的应取框架这次的，不是丢掉")
		}
	})
	cancelBase(errors.New("req-B"))
	if foreign != nil {
		if foreign.Err() == nil {
			t.Error("调用方取消了这次请求：采纳外来 ctx 之后也该停下")
		}
		if c := context.Cause(foreign); c == nil || c.Error() != "req-B" {
			t.Errorf("Cause 应跟 Err 是同一个请求的（req-B），实际 %v", c)
		}
		if child, stop := context.WithCancel(foreign); child.Err() == nil || context.Cause(child).Error() != "req-B" {
			t.Errorf("在采纳后的 ctx 上派生的子 ctx 也应是 req-B：Err=%v Cause=%v", child.Err(), context.Cause(child))
			stop()
		} else {
			stop()
		}
	}

	var in context.Context
	if got, _ := compose_v2.CallHook(base, func(c context.Context) (context.Context, error) {
		in = c
		return nil, nil
	}); got != in {
		t.Error("钩子返回 nil 时应原样用交给它的那个")
	}
}

// SignalRecipient：节点外为空；同一个节点执行里按 label 区分；续跑时同一个位置算出同一个值；
// 新开一次运行（新密钥）就不同；同一个节点的下一次执行（访问序号变了）也不同。
func TestSignalRecipient_StableAcrossResumeNewPerRun(t *testing.T) {
	if r := compose_v2.SignalRecipient(context.Background(), "x"); r != "" {
		t.Errorf("节点外不该铸出收件人，实际 %q", r)
	}

	var seen []string
	asked := 0
	b := compose_v2.NewBuilder().WithName("sig_recipient")
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		x, y := compose_v2.SignalRecipient(ctx, "x"), compose_v2.SignalRecipient(ctx, "y")
		if x == "" || x == y {
			t.Errorf("同一个节点执行里不同 label 应铸出不同的非空收件人，实际 %q / %q", x, y)
		}
		seen = append(seen, x)
		if asked == 0 {
			asked++
			_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续吗"})
			return nil, err
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g := rsCompile(t, b)

	opts := rsOpts()
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应停在提问上，got %v", err)
	}
	answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("是")}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), append(opts, compose_v2.WithResumeAnswers(answers))...); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || seen[0] != seen[1] {
		t.Fatalf("续跑时同一个位置应算出同一个收件人（密钥随存档恢复），实际 %v", seen)
	}

	// 新开一次（新的密钥）：同一个位置的收件人不同——上一次运行留下的决定认不出是发给这次的
	asked = 1 // 这次不问
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager()); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 3 || seen[2] == seen[0] {
		t.Errorf("新开的运行应铸出不同的收件人，实际 %v", seen)
	}
}

// 同一个节点的下一次执行（访问序号变了）铸出的收件人不同——每一跳一个收件人。
func TestSignalRecipient_DiffersPerVisit(t *testing.T) {
	var seen []string
	b := compose_v2.NewBuilder().WithName("sig_recipient_visit")
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		seen = append(seen, compose_v2.SignalRecipient(ctx, "x"))
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddConditionalEdge("n", func(context.Context, compose_v2.Channels) (compose_v2.NodeID, error) {
		if len(seen) < 2 {
			return "n", nil
		}
		return compose_v2.EndNode, nil
	})
	g := rsCompile(t, b)
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager()); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || seen[0] == seen[1] {
		t.Errorf("同一个节点的两次执行应铸出不同的收件人，实际 %v", seen)
	}
}

// JoinSignals：并发的几路对同一个收件人给出不同的值 ⟹ SignalConflictError 并带 ErrRunAborted（不重试、
// 往上交就终局）；一致的算一次；不同收件人互不相干。
func TestJoinSignals_ConflictAndAgree(t *testing.T) {
	capture := func(fn func(ctx context.Context)) compose_v2.Signals {
		s, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error { fn(ctx); return nil })
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	a := capture(func(ctx context.Context) {
		compose_v2.EmitSignal(ctx, sigKindA, compose_v2.Signal{Dest: "d", Value: "x", Origin: "a"})
		compose_v2.EmitSignal(ctx, sigKindA, compose_v2.Signal{Dest: "d_other", Value: "p"})
	})
	b := capture(func(ctx context.Context) {
		compose_v2.EmitSignal(ctx, sigKindA, compose_v2.Signal{Dest: "d", Value: "x", Origin: "b"})
		compose_v2.EmitSignal(ctx, sigKindB, compose_v2.Signal{Dest: "d", Value: "q"})
	})
	joined, err := compose_v2.JoinSignals(a, b)
	if err != nil {
		t.Fatalf("值一致的并发决定不该冲突：%v", err)
	}
	if joined.Len() != 3 {
		t.Errorf("应合并成 3 条（d 上的一致决定算一次），实际 %d 条", joined.Len())
	}
	c := capture(func(ctx context.Context) {
		compose_v2.EmitSignal(ctx, sigKindA, compose_v2.Signal{Dest: "d", Value: "y", Origin: "c"})
	})
	_, err = compose_v2.JoinSignals(a, c)
	var conflict *compose_v2.SignalConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("并发的不同值应报 SignalConflictError，实际 %v", err)
	}
	if compose_v2.IsRetryable(err) || !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("冲突应不重试、带 ErrRunAborted（往上交就终局），实际 retryable=%v aborted=%v",
			compose_v2.IsRetryable(err), errors.Is(err, compose_v2.ErrRunAborted))
	}
	// 终局标记挂在类型本身上：业务用 errors.As 取出结构体再往上交，同样终局、不可重试、必传。曾经标记只在
	// 外面那层包装上，取出来的结构体变回可重试、在 RoutingAgent 里被降级成兜底（审计实测）。
	if _, ok := err.(*compose_v2.SignalConflictError); !ok {
		t.Errorf("JoinSignals 应直接返回 *SignalConflictError（类型断言的调用方才认得出），实际 %T", err)
	}
	if !errors.Is(conflict, compose_v2.ErrRunAborted) || compose_v2.IsRetryable(conflict) ||
		!compose_v2.MustPropagate(context.Background(), conflict) {
		t.Errorf("取出来的结构体也应终局：aborted=%v retryable=%v mustPropagate=%v",
			errors.Is(conflict, compose_v2.ErrRunAborted), compose_v2.IsRetryable(conflict),
			compose_v2.MustPropagate(context.Background(), conflict))
	}
	if conflict.Kind != sigKindA.Name() || conflict.First.Value != "x" || conflict.Second.Value != "y" ||
		conflict.First.Origin != "a" || conflict.Second.Origin != "c" {
		t.Errorf("冲突应按路的顺序报出种类、两个值与各自的来源，实际 %+v", conflict)
	}
}

// TestJoinSignalsFirstWins_BranchIsAtomic：排在前面的那一路生效；后面一路只要有一条跟已生效的不一致，
// 这一路整个不算（它没冲突的那条也不算），冲突的那条逐条报出来；值一致的算一次。
//
// 按条丢弃时，被盖掉的那一路没冲突的决定照样生效——而调用方把这一路当成「没成」（tools 节点给它换成
// 失败结果），两边结论相反（审计实测）。
func TestJoinSignalsFirstWins_BranchIsAtomic(t *testing.T) {
	capture := func(fn func(ctx context.Context)) compose_v2.Signals {
		s, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error { fn(ctx); return nil })
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	b0 := capture(func(ctx context.Context) {
		compose_v2.EmitSignal(ctx, sigKindA, compose_v2.Signal{Dest: "d", Value: "x"})
	})
	b1 := capture(func(ctx context.Context) { // 跟 b0 冲突 ⟹ 整路不算，b 上的 z 也不算
		compose_v2.EmitSignal(ctx, sigKindA, compose_v2.Signal{Dest: "d", Value: "y"})
		compose_v2.EmitSignal(ctx, sigKindB, compose_v2.Signal{Dest: "e", Value: "z"})
	})
	b2 := capture(func(ctx context.Context) { // 跟 b0 一致 ⟹ 整路算数，e 上是 w
		compose_v2.EmitSignal(ctx, sigKindA, compose_v2.Signal{Dest: "d", Value: "x"})
		compose_v2.EmitSignal(ctx, sigKindB, compose_v2.Signal{Dest: "e", Value: "w"})
	})
	merged, sup := compose_v2.JoinSignalsFirstWins(b0, b1, b2)
	if got := sigValues(merged, sigKindA, "d"); len(got) != 1 || got[0] != "x" {
		t.Errorf("d 上应是排在前面的 x，实际 %v", got)
	}
	if got := sigValues(merged, sigKindB, "e"); len(got) != 1 || got[0] != "w" {
		t.Errorf("被盖掉的那一路整个不算：e 上应是第 2 路的 w、不是第 1 路的 z，实际 %v", got)
	}
	if merged.Len() != 2 {
		t.Errorf("应合并成 2 条，实际 %d 条", merged.Len())
	}
	if len(sup) != 1 || sup[0].Branch != 1 || sup[0].Kept.Value != "x" || sup[0].Dropped.Value != "y" {
		t.Errorf("应只报第 1 路那条冲突（x 生效、y 被盖），实际 %+v", sup)
	}
}

// TestCondAbort_TerminalErrorNotWrappedTwice：条件边（默认 CondFailAbort）返回一个本来就终局的错——
// 这里是判官里 JoinSignals 合并出的冲突——时只补位置、不再叠一层 ErrRunAborted：正文里终局文本只
// 出现一次且带着条件边的位置，errors.Is / errors.As 照样命中，存档清掉。曾经无条件再包一层，正文里
// 出现两遍（审计实测）。
func TestCondAbort_TerminalErrorNotWrappedTwice(t *testing.T) {
	b := compose_v2.NewBuilder().WithName("cond_terminal_once")
	b.AddNode("n", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddConditionalEdge("n", func(ctx context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		x, err := compose_v2.CaptureSignals(ctx, func(c context.Context) error { sigEmit(c, "billing"); return nil })
		if err != nil {
			return "", err
		}
		y, err := compose_v2.CaptureSignals(ctx, func(c context.Context) error { sigEmit(c, "sales"); return nil })
		if err != nil {
			return "", err
		}
		_, err = compose_v2.JoinSignals(x, y)
		return "", err
	})
	g := rsCompile(t, b)
	cp := compose_v2.NewInMemoryCheckpointer()
	_, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("cond_terminal_once"))
	})
	var conflict *compose_v2.SignalConflictError
	if !errors.Is(err, compose_v2.ErrRunAborted) || !errors.As(err, &conflict) {
		t.Fatalf("条件边返回的冲突应原样终局、取得出结构体，实际 %v", err)
	}
	if n := strings.Count(err.Error(), compose_v2.ErrRunAborted.Error()); n != 1 {
		t.Errorf("正文里终局文本应只出现一次，实际 %d 次：%s", n, err)
	}
	if !strings.Contains(err.Error(), `condition from "n"`) {
		t.Errorf("正文应带着条件边的位置，实际 %s", err)
	}
	all, lerr := cp.LoadAllInSession(context.Background(), "cond_terminal_once")
	if lerr != nil {
		t.Fatal(lerr)
	}
	if len(all) != 0 {
		t.Errorf("终局失败应清掉存档，实际还剩 %d 份", len(all))
	}
}
