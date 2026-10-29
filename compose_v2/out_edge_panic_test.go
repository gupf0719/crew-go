package compose_v2_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// unwrapPanicErr 是一个 `Unwrap()` 会 panic 的业务 error——`wrapCondFailure` 里的
// `IsInterrupt(err)` 会沿 Unwrap 链走，于是 panic 发生在 `safeCondRun` 的 recover **之外**。
type unwrapPanicErr struct{}

func (*unwrapPanicErr) Error() string { return "biz cond error" }
func (*unwrapPanicErr) Unwrap() error { panic("unwrap boom") }

// TestOutEdge_PanicOutsideCondBody_DoesNotCrashProcess 钉住「出边计算里的 panic 不打死进程」。
//
// # 它守的那条不对称
//
// 出边计算挪进成员的 goroutine 之后，`evalOutEdges` 就跑在**跨 goroutine** 的位置上了。
// `runNodeWithHooks` 自带的 recover 随它 return 就结束；`safeCondRun` 只裹 cond 的**函数体**。
// 而 `wrapCondFailure` 对业务 error 做的链式 unwrap（`IsInterrupt`）在两者之外——业务给一个
// `Unwrap()` 会 panic 的 error 就能打死整个进程。
//
// 判据是**同一个 cond 在两条路径上的不对称**：N=1 走 fast path（同 goroutine）时 panic 还
// 有 Run 出口的 defer 链接得住；N≥2 走 fan-out goroutine 时**直接 crash**——跨 goroutine
// 的 panic 谁都接不到。本用例用 2 个 `SendFrom` 实例走后一条。
//
// ⚠️ 别把「fast path 那半」读成没问题：它当时不崩进程，但会让 cleanup 把存档当成功清掉
// ——那是下面 TestOutEdge_PanicOnFastPath_DoesNotDestroyArchive 守的另一条。两条各守一半。
//
// 「跨 goroutine 不能依赖 Run 入口 outer recover」这条本仓早写死在 `runNodeWithHooks` 的
// 注释里，出边挪进 goroutine 那次漏了照做。
//
// sanity-revert：去掉 `executeFrontier` fan-out goroutine 顶上那个 `defer recover()`
// → 本用例让**整个测试进程**崩掉（不是 FAIL，是 panic 打死 go test）。
func TestOutEdge_PanicOutsideCondBody_DoesNotCrashProcess(t *testing.T) {
	b := compose_v2.NewBuilder().WithName("outedge_panic")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// 两个实例 ⟹ 走 fan-out goroutine 那条路（N=1 的 fast path 测不出这条）
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "X"},
			compose_v2.SendDirective{Target: "X"},
		)
		return nil, nil
	})
	b.AddNode("X", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddNode("Y", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("Y", compose_v2.EndNode)
	b.AddConditionalEdge("X", func(context.Context, compose_v2.Channels) (compose_v2.NodeID, error) {
		return "", &unwrapPanicErr{} // 函数体本身不 panic——panic 在框架对它做 unwrap 时
	})

	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}

	// **承重**：跑到这一行说明进程还活着（崩了的话 go test 直接死，连 FAIL 都看不到）
	runErr := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager())
	if runErr == nil {
		t.Fatal("该返回错误")
	}
	if !strings.Contains(runErr.Error(), "panic recovered") {
		t.Errorf("panic 该被转成 error 并说清是 panic，实际 err=%v", runErr)
	}
	t.Logf("err = %v", runErr)
}

// TestOutEdge_PanicOnFastPath_DoesNotDestroyArchive 钉住「主体 panic 不会被当成成功、
// 更不会把用户正等着答的存档删掉」。
//
// # 它守的那个后果
//
// `Run` 的 defer 链是 LIFO：入口那个 recover **注册最早 ⟹ 最后跑**（那是故意的，它专收
// defer 链自己抛的 panic）。于是 cleanup 与 `OnGraphEnd` 跑的时候 `runErr` **还是 nil**：
//
//   - cleanup 走 `runErr == nil` 分支 ⟹ `purgeExitSlot(reason="success")`
//   - `OnGraphEnd` 被告知 `err == nil` ⟹ 观测层记成功
//   - 而 `Run` 最终返回的是 error
//
// 三者互相矛盾，最不可逆的是第一条：**用户正等着答的挂起档被删了**，此后 Resume 恒返
// `ErrNoPendingCheckpoint`——会话没了。
//
// 单成员（N=1）的出边 panic 走的正是这条路（fan-out 那条有自己的 recover）。本用例用
// 「先中断、再在续跑时 panic」把最刺眼的形态摆出来。
//
// sanity-revert：删掉 cleanup defer **之后**注册的那个 recover → 本用例红，
// 且红成「cp 没了 + OnGraphEnd 收到 nil」。
func TestOutEdge_PanicOnFastPath_DoesNotDestroyArchive(t *testing.T) {
	var boom bool
	var graphEndErr error
	var sawGraphEnd bool

	mk := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("fastpath_destroy")
		b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗"}); err != nil {
				return nil, err
			}
			return nil, nil
		})
		b.AddNode("tail", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		})
		b.AddEdge(compose_v2.StartNode, "n")
		b.AddEdge("tail", compose_v2.EndNode)
		// 单成员 ⟹ 走 fast path（没有 fan-out goroutine 的那道 recover）
		b.AddConditionalEdge("n", func(context.Context, compose_v2.Channels) (compose_v2.NodeID, error) {
			if boom {
				return "", &unwrapPanicErr{} // panic 发生在框架对它做 unwrap 时
			}
			return "tail", nil
		})
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		return g
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "fastpath_destroy_sid"
	hooks := &endCapture{err: &graphEndErr, saw: &sawGraphEnd}

	// Run1：用户被问了一个问题，存档留着
	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, mk(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid)))
	if !ok {
		t.Fatal("Run1 该中断")
	}
	if exists, _ := cper.ExistsNS(ctx, sid, ""); !exists {
		t.Fatal("fixture 没搭对：Run1 该留下挂起档")
	}

	// Run2：用户答了，而这次出边计算 panic
	boom = true
	err2 := compose_v2.Run(ctx, mk(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithHooks(hooks),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			ie.AllQueries()[0].InterruptID: schema.UserMessage("批准"),
		}))
	t.Logf("Run2 err = %v", err2)
	t.Logf("OnGraphEnd 收到 err = %v", graphEndErr)

	if err2 == nil {
		t.Fatal("Run2 该返回错误")
	}
	// **承重 1**：观测层不能被告知「成功」——它跟返回值必须说同一件事
	if sawGraphEnd && graphEndErr == nil {
		t.Errorf("OnGraphEnd 收到 err=nil，而 Run 返回的是 %v\n"+
			"  = 主体 panic 还没转成 runErr，cleanup / OnGraphEnd 看到的是「成功」", err2)
	}
	// **承重 2（最不可逆的那条）**：用户的挂起档不能被当成功清掉
	exists, _ := cper.ExistsNS(ctx, sid, "")
	if !exists {
		t.Error("挂起档被删了——框架一边返 error、一边按 reason=success 清了子树。\n" +
			"  用户正等着答的问题就这么没了，此后 Resume 恒返 ErrNoPendingCheckpoint")
	}
}

// endCapture 抓 OnGraphEnd 收到的 err
type endCapture struct {
	compose_v2.NoOpOrchestrationHooks
	err *error
	saw *bool
}

func (h *endCapture) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, err error) context.Context {
	*h.err = err
	*h.saw = true
	return ctx
}

// panicAtGraphEnd 让业务 OnGraphEnd hook 抛 panic——它跑在图结束之后，panic 就地兜住、只记日志，
// 图成没成都不改变结局。
type panicAtGraphEnd struct {
	compose_v2.NoOpOrchestrationHooks
}

func (panicAtGraphEnd) OnGraphEnd(context.Context, compose_v2.Channels, error) context.Context {
	panic("OnGraphEnd boom")
}

// TestPanicRecovery_TwoPrefixesStayDistinct 钉住「两道 recover 的前缀不能被抹平」。
//
// # 它们为什么必须不同
//
// 业务据这行前缀区分两件**处置相反**的事：
//
//   - `graph body panic recovered` —— 图主体炸了，**存档已正确存成 failed**，可 Resume
//   - `Run panic recovered` —— 出口 defer 链里框架自己的代码炸了（业务钩子与出口存储调用另有兜底，落不到这一道），
//     存档状态以错误链上的结论为准
//
// 前者该续跑，后者该先确认存档到底在不在。前缀一样时业务只能靠猜。
//
// # 为什么需要这条哨兵
//
// 这两个前缀刚被抹平过一次：给出口那道改名时用了个不限次数的字符串替换，把**入口那道**
// 一起改了。而当时**全套测试仍然全绿**——没有任何用例守这一维，它是被人工比对代码时
// 才发现的。这正是「断言选在不承重的维度上等于没测」的反面：没测的维度连假绿都算不上。
//
// 入口那道业务代码触发不到之后，「两个前缀互不相同」改由 panic_prefix_internal_test.go 比两个常量；本用例守主体那道的
// 前缀确实出现、图失败时业务 OnGraphEnd 的 panic 不改变结局。
func TestPanicRecovery_TwoPrefixesStayDistinct(t *testing.T) {
	mk := func(bodyPanic bool) *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("prefix_distinct")
		b.AddNode("n", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		})
		b.AddNode("tail", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		})
		b.AddEdge(compose_v2.StartNode, "n")
		b.AddEdge("tail", compose_v2.EndNode)
		b.AddConditionalEdge("n", func(context.Context, compose_v2.Channels) (compose_v2.NodeID, error) {
			if bodyPanic {
				return "", &unwrapPanicErr{} // 图主体（出边计算）里 panic
			}
			return "tail", nil
		})
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		return g
	}

	// ① 图主体 panic —— 归出口那道
	bodyErr := compose_v2.Run(context.Background(), mk(true), compose_v2.NewChannelManager())
	if bodyErr == nil || !strings.Contains(bodyErr.Error(), "graph body panic recovered") {
		t.Errorf("图主体 panic 应带 `graph body panic recovered` 前缀，实际 %v", bodyErr)
	}

	// ② 图失败时的业务 OnGraphEnd hook panic：就地兜住、只记日志，错误照旧是节点那个（入口那道只兜框架自己的出口代码，
	// 业务代码触发不到；两个前缀不同由 panic_prefix_internal_test.go 守）
	fb := compose_v2.NewBuilder().WithName("prefix_distinct_fail")
	fb.AddNode("n", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, errors.New("业务错")
	})
	fb.AddEdge(compose_v2.StartNode, "n")
	fb.AddEdge("n", compose_v2.EndNode)
	failing, err := fb.Compile()
	if err != nil {
		t.Fatal(err)
	}
	hookErr := compose_v2.Run(context.Background(), failing, compose_v2.NewChannelManager(),
		compose_v2.WithHooks(panicAtGraphEnd{}))
	if hookErr == nil || hookErr.Error() != `node "n": 业务错` {
		t.Errorf("图失败时 OnGraphEnd 的 panic 不改变结局，应照旧返回节点的错误，实际 %v", hookErr)
	}
	t.Logf("主体 panic: %v", bodyErr)
	t.Logf("hook panic: %v", hookErr)
}
