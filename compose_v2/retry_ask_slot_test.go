package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// 不变量：重试同一次调用 ⟹ 同一套提问 ID，已答的不再问；失败那次尝试的兑付不挡重试。
//
// [compose_v2.RunWithRetry] 的每次尝试是同一次调用再做一遍：fn 里按同样次序提的问题拿到同样的
// 序号（同一个 ID），第一次尝试里已经兑付的答案在后面的尝试里同一位置照样兑付——不换 ID 重问，
// 也不被答案账本判成「落在已兑付的位上」。

// retryAskDrive 跑一张图直到收敛：每次停下，把问到的每个问题答成「答:<问题>」并累积着传。返回依次
// 问过的问题与最后的错。
func retryAskDrive(t *testing.T, g *compose_v2.Graph) (asked []string, err error) {
	t.Helper()
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()), compose_v2.WithSessionID("s")}
	answers := map[string]*schema.Message{}
	err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...)
	for round := 0; round < 6; round++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			break
		}
		for _, q := range ie.AllQueries() {
			asked = append(asked, q.Question)
			answers[q.InterruptID] = schema.UserMessage("答:" + q.Question)
		}
		err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
			append(opts, compose_v2.WithResumeAnswers(answers))...)
	}
	return asked, err
}

// retryAskJoin 合并并发分支的错：有不是中断的错就返回它，中断合成一个，都没有返回 nil。
func retryAskJoin(errs []error) error {
	var ies []*compose_v2.InterruptError
	for _, e := range errs {
		if e == nil {
			continue
		}
		ie, ok := compose_v2.IsInterrupt(e)
		if !ok {
			return e
		}
		ies = append(ies, ie)
	}
	if m := compose_v2.MergeInterrupts(ies); m != nil {
		return m
	}
	return nil
}

// retryAskGraph 造一张单节点图，节点体是 body。
func retryAskGraph(t *testing.T, body compose_v2.NodeFunc) *compose_v2.Graph {
	t.Helper()
	b := compose_v2.NewBuilder()
	b.AddNode("n", body)
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// TestRunWithRetry_AskInFn_AnsweredQuestionNotReasked（节点体 Ask + RunWithRetry）：fn 先问一次人，拿到答案
// 之后瞬时失败两次。重试的第二、三次尝试在同一位置拿到同一个答案、不重问；fn 后面那一问拿到的是它自己
// 的答案。
//
// 修之前提问序号不随重试退回：后面的尝试里那一问换了 ID、用户被重问；而累积传答案时，重问拿到的回答
// 会在下一次续跑里落到 fn 后面那一问的位置上——后一问被前一问的答案静默满足（实测）。只退回序号、不按
// 尝试给答案账本分层时，第二次尝试取同一个 ID 被判成已兑付，整个任务终局失败。
func TestRunWithRetry_AskInFn_AnsweredQuestionNotReasked(t *testing.T) {
	var flakes atomic.Int32
	var got []string
	g := retryAskGraph(t, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (struct{}, error) {
			a1, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "Q1 批准转账？"})
			if err != nil {
				return struct{}{}, err
			}
			if flakes.Add(1) <= 2 {
				return struct{}{}, errors.New("下游抖了一下")
			}
			a2, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "Q2 批准删除？"})
			if err != nil {
				return struct{}{}, err
			}
			got = []string{a1.Content, a2.Content}
			return struct{}{}, nil
		})
		return nil, err
	})
	asked, err := retryAskDrive(t, g)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %q）", err, asked)
	}
	// Q1 答过之后 fn 跑 4 次：第一次续跑里失败两次、第三次走到 Q2 停下；第二次续跑一次走完
	if n := flakes.Load(); n != 4 {
		t.Fatalf("fixture 没搭对：Q1 答过之后 fn 应跑 4 次，实际 %d", n)
	}
	if want := []string{"Q1 批准转账？", "Q2 批准删除？"}; !slices.Equal(asked, want) {
		t.Errorf("问过 %q，want %q——重试换了 ID 重问", asked, want)
	}
	if want := []string{"答:Q1 批准转账？", "答:Q2 批准删除？"}; !slices.Equal(got, want) {
		t.Errorf("fn 拿到的答案 = %q，want %q——有答案落到了别的问题上", got, want)
	}
}

// TestRunWithRetry_SelfChosenID_RetryReconsumesSameAnswer：fn 走自填 ID 那条路（GetResumeAnswer + 自己构造
// 中断），答案到了之后瞬时失败一次。第二次尝试拿同一个 ID 照样取到答案。
//
// 修之前第一次尝试的兑付直接记进节点的账本：第二次尝试取同一个 ID 被当成「已兑付」、随后抛的中断落在
// 已兑付的位上，整个任务终局失败（实测）。只退回提问序号、不处理账本时，走 Ask 的提问也是这个下场。
func TestRunWithRetry_SelfChosenID_RetryReconsumesSameAnswer(t *testing.T) {
	var flaked atomic.Bool
	var got string
	g := retryAskGraph(t, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		v, err := compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (string, error) {
			ans, ok := compose_v2.GetResumeAnswer(ctx, "approve-transfer")
			if !ok || ans == nil {
				return "", &compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "approve-transfer", Question: "批准转账？"}}
			}
			if flaked.CompareAndSwap(false, true) {
				return "", errors.New("下游抖了一下")
			}
			return ans.Content, nil
		})
		got = v
		return nil, err
	})
	asked, err := retryAskDrive(t, g)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %q）", err, asked)
	}
	if !flaked.Load() {
		t.Fatal("fixture 没搭对：fn 一次都没失败过")
	}
	if len(asked) != 1 || got != "答:批准转账？" {
		t.Errorf("应问一次、拿到自己的答案；问过 %q，拿到 %q", asked, got)
	}
}

// TestRunWithRetry_ConcurrentToolCalls_EachRetriesOwnQuestions（并行分支）：同一个节点里两个 tool_call 并发，
// 各自用 RunWithRetry 包一段「问人 → 瞬时失败一次」。两边共用节点的答案账本，各有各的提问序号；一边的重试
// 不影响另一边，两问各问一次、各拿各的答案。
func TestRunWithRetry_ConcurrentToolCalls_EachRetriesOwnQuestions(t *testing.T) {
	var flakedA, flakedB atomic.Bool
	var mu sync.Mutex
	got := map[string]string{}
	g := retryAskGraph(t, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		calls := []struct {
			id, q  string
			flaked *atomic.Bool
		}{{"call_a", "A 批准？", &flakedA}, {"call_b", "B 批准？", &flakedB}}
		errs := make([]error, len(calls))
		var wg sync.WaitGroup
		// 屏障：两个 tool_call 的第一次尝试都拿到答案之后才一起失败，两边的重试窗口重叠。带超时，
		// fixture 搭错（有一边没走到这儿）时不至于挂死
		arrived := make(chan struct{}, len(calls))
		release := make(chan struct{})
		go func() {
			defer close(release)
			for range calls {
				select {
				case <-arrived:
				case <-time.After(2 * time.Second):
					return
				}
			}
		}()
		for i, c := range calls {
			wg.Add(1)
			go func(i int, id, q string, flaked *atomic.Bool) {
				defer wg.Done()
				tcCtx := compose_v2.WithCurrentToolCallContext(ctx, id, i)
				_, errs[i] = compose_v2.RunWithRetry(tcCtx, compose_v2.SimpleRetry(2), func(ctx context.Context) (struct{}, error) {
					ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: q})
					if err != nil {
						return struct{}{}, err
					}
					if flaked.CompareAndSwap(false, true) {
						arrived <- struct{}{}
						<-release
						return struct{}{}, errors.New("下游抖了一下")
					}
					mu.Lock()
					got[q] = ans.Content
					mu.Unlock()
					return struct{}{}, nil
				})
			}(i, c.id, c.q, c.flaked)
		}
		wg.Wait()
		return nil, retryAskJoin(errs)
	})
	asked, err := retryAskDrive(t, g)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %q）", err, asked)
	}
	slices.Sort(asked)
	if want := []string{"A 批准？", "B 批准？"}; !slices.Equal(asked, want) {
		t.Errorf("问过 %q，want %q", asked, want)
	}
	if got["A 批准？"] != "答:A 批准？" || got["B 批准？"] != "答:B 批准？" {
		t.Errorf("各自拿到的答案 = %v", got)
	}
}

// TestRunWithRetry_NestedRetry_EachQuestionAskedOnce（重试里套重试 + 重试中途中断再续跑）：外层 fn 问 Q1、
// 跑内层重试（内层问 Q2、答过之后失败一次）、内层成功之后外层失败一次、再问 Q3。每一问只问一次，各拿各的
// 答案。
func TestRunWithRetry_NestedRetry_EachQuestionAskedOnce(t *testing.T) {
	var innerFlaked, outerFlaked atomic.Bool
	var got []string
	g := retryAskGraph(t, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (struct{}, error) {
			a1, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "Q1"})
			if err != nil {
				return struct{}{}, err
			}
			a2, err := compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (*schema.Message, error) {
				a, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "Q2"})
				if err != nil {
					return nil, err
				}
				if innerFlaked.CompareAndSwap(false, true) {
					return nil, errors.New("内层抖了一下")
				}
				return a, nil
			})
			if err != nil {
				return struct{}{}, err
			}
			if outerFlaked.CompareAndSwap(false, true) {
				return struct{}{}, errors.New("外层抖了一下")
			}
			a3, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "Q3"})
			if err != nil {
				return struct{}{}, err
			}
			got = []string{a1.Content, a2.Content, a3.Content}
			return struct{}{}, nil
		})
		return nil, err
	})
	asked, err := retryAskDrive(t, g)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %q）", err, asked)
	}
	if !innerFlaked.Load() || !outerFlaked.Load() {
		t.Fatal("fixture 没搭对：内外层各应失败一次")
	}
	if want := []string{"Q1", "Q2", "Q3"}; !slices.Equal(asked, want) {
		t.Errorf("问过 %q，want %q", asked, want)
	}
	if want := []string{"答:Q1", "答:Q2", "答:Q3"}; !slices.Equal(got, want) {
		t.Errorf("拿到的答案 = %q，want %q", got, want)
	}
}

// retryAskSelfChosen 走自填 ID 那条路取 key 上的答案，没有就抛以它为 ID 的中断。
func retryAskSelfChosen(ctx context.Context, key string) (*schema.Message, error) {
	ans, ok := compose_v2.GetResumeAnswer(ctx, key)
	if !ok || ans == nil {
		return nil, &compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: key, Question: "问 " + key}}
	}
	return ans, nil
}

// TestRunWithRetry_RetiredAttemptConsumption_CountsAfterSuccess：重试期间后一次尝试看不见前面尝试的兑付，但
// RunWithRetry 返回之后，每次尝试兑付过的答案都算兑付——同一个答案不能再满足别的提问。
//
// fixture 故意让两次尝试用不同的自填 ID（K1、K2）：只有这样「失败那次尝试的兑付有没有并回外层」才看得出来
// （走 Ask 的确定性 fn 每次尝试问的是同一批问题，最后那次尝试本来就会再兑付一遍）。现实里对应的是：前一次
// 尝试里完成的嵌套调用，后一次尝试拿回记录、不再执行——它兑付过的答案不会被再兑付一遍。
func TestRunWithRetry_RetiredAttemptConsumption_CountsAfterSuccess(t *testing.T) {
	var reuse []bool
	g := retryAskGraph(t, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		attempt := 0
		_, err := compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(1), func(ctx context.Context) (struct{}, error) {
			attempt++
			if _, err := retryAskSelfChosen(ctx, fmt.Sprintf("K%d", attempt)); err != nil {
				return struct{}{}, err
			}
			if attempt == 1 {
				return struct{}{}, errors.New("下游抖了一下")
			}
			return struct{}{}, nil
		})
		if err != nil {
			return nil, err
		}
		_, ok1 := compose_v2.GetResumeAnswer(ctx, "K1")
		_, ok2 := compose_v2.GetResumeAnswer(ctx, "K2")
		reuse = []bool{ok1, ok2}
		return nil, nil
	})
	asked, err := retryAskDrive(t, g)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %q）", err, asked)
	}
	if !slices.Equal(asked, []string{"问 K1", "问 K2"}) {
		t.Fatalf("fixture 没搭对：应先后问 K1、K2，实际 %q", asked)
	}
	if !slices.Equal(reuse, []bool{false, false}) {
		t.Errorf("重试返回之后再取 K1、K2 = %v，want [false false]——有尝试兑付过的答案没并回外层账本，"+
			"能被别的提问再兑付一次", reuse)
	}
}

// TestRunWithRetry_RetiredAttemptConsumption_CountsAfterExhausted：同上，出口换成重试用尽——节点接住这个错接着
// 往下走时，各次尝试兑付过的答案同样算兑付。
func TestRunWithRetry_RetiredAttemptConsumption_CountsAfterExhausted(t *testing.T) {
	var reuse []bool
	var lastErr error
	g := retryAskGraph(t, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		attempt := 0
		_, err := compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(1), func(ctx context.Context) (struct{}, error) {
			attempt++
			if _, err := retryAskSelfChosen(ctx, fmt.Sprintf("K%d", attempt)); err != nil {
				return struct{}{}, err
			}
			return struct{}{}, errors.New("下游一直不好")
		})
		if _, isInt := compose_v2.IsInterrupt(err); isInt {
			return nil, err
		}
		lastErr = err
		_, ok1 := compose_v2.GetResumeAnswer(ctx, "K1")
		_, ok2 := compose_v2.GetResumeAnswer(ctx, "K2")
		reuse = []bool{ok1, ok2}
		return nil, nil
	})
	asked, err := retryAskDrive(t, g)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %q）", err, asked)
	}
	if lastErr == nil || !slices.Equal(asked, []string{"问 K1", "问 K2"}) {
		t.Fatalf("fixture 没搭对：两次尝试应都兑付后失败，问过 %q，重试的错 %v", asked, lastErr)
	}
	if !slices.Equal(reuse, []bool{false, false}) {
		t.Errorf("重试用尽之后再取 K1、K2 = %v，want [false false]", reuse)
	}
}

// TestRunWithRetry_SameAttemptCollision_StillTerminal：一次尝试里两个不同的问题撞了同一个自填 ID——用户只答了
// 第一问，第二问不能被同一个答案满足。这次尝试以中断退出（中断不重试），节点那一层的校验要看得见这次尝试的
// 兑付、当场终局失败；看不见的话第二问以同一个 ID 挂起，每次续跑都这样、永远答不完。
func TestRunWithRetry_SameAttemptCollision_StillTerminal(t *testing.T) {
	var secondDone atomic.Bool
	g := retryAskGraph(t, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(1), func(ctx context.Context) (struct{}, error) {
			if _, err := retryAskSelfChosen(ctx, "K"); err != nil {
				return struct{}{}, err
			}
			ans, ok := compose_v2.GetResumeAnswer(ctx, "K")
			if !ok || ans == nil {
				return struct{}{}, &compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "K", Question: "确认删除全部历史？"}}
			}
			secondDone.Store(true)
			return struct{}{}, nil
		})
		return nil, err
	})
	asked, err := retryAskDrive(t, g)
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("撞 ID 应当场终局失败，got %v（问过 %q）", err, asked)
	}
	if secondDone.Load() {
		t.Error("第二问被第一问的答案满足了")
	}
}

// TestRunWithRetry_ConsumedBeforeRetry_StillVisibleInAttempts：进入重试之前节点已经兑付过的答案，重试的每次尝试
// 照样看得见——尝试里另一个问题撞了同一个自填 ID 时不能拿到它，当场终局失败。分层只对「前面几次尝试」
// 不可见，不对尝试之外。
func TestRunWithRetry_ConsumedBeforeRetry_StillVisibleInAttempts(t *testing.T) {
	var reused atomic.Bool
	g := retryAskGraph(t, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, err := retryAskSelfChosen(ctx, "K"); err != nil {
			return nil, err
		}
		_, err := compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(1), func(ctx context.Context) (struct{}, error) {
			ans, ok := compose_v2.GetResumeAnswer(ctx, "K")
			if !ok || ans == nil {
				return struct{}{}, &compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "K", Question: "撞了同一个 ID 的另一问"}}
			}
			reused.Store(true)
			return struct{}{}, nil
		})
		return nil, err
	})
	asked, err := retryAskDrive(t, g)
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("撞 ID 应当场终局失败，got %v（问过 %q）", err, asked)
	}
	if reused.Load() {
		t.Error("尝试里的另一问拿到了进入重试之前已经兑付过的答案")
	}
}

// retryAskDraftGraph 造一张会问人的嵌套图：draft 每跑一次产出新的一版（v1、v2……，模拟调模型重新生成），
// confirm 问「批准 vN 吗」、把批准结果写进 out。drafts 记 draft 跑了几次。
func retryAskDraftGraph(t *testing.T, drafts *atomic.Int32) (*compose_v2.Graph, *compose_v2.Channel[string]) {
	t.Helper()
	draftCh := compose_v2.LastValue[string]("draft")
	outCh := compose_v2.LastValue[string]("out")
	b := compose_v2.NewBuilder().WithName("retry_ask_draft")
	compose_v2.WithChannel(b, draftCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("draft", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(draftCh, fmt.Sprintf("v%d", drafts.Add(1)))}, nil
	})
	b.AddNode("confirm", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		d, _ := compose_v2.Get(chs, draftCh)
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准 " + d + " 吗"})
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, d+" 已批准("+ans.Content+")")}, nil
	})
	b.AddEdge(compose_v2.StartNode, "draft")
	b.AddEdge("draft", "confirm")
	b.AddEdge("confirm", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g, outCh
}

// TestRunWithRetry_NestedGraphRegenerated_OldApprovalNotAppliedToNewContent：fn 里直接跑一张会问人的嵌套图，
// 它答完、成功（存档随之清掉），fn 后面一步瞬时失败。重试的第二次尝试从头再跑那张图，draft 产出 v2，confirm
// 在同一个位置、同一个 ID 问「批准 v2 吗」。用户只批准过 v1——这个批准不能兑付给 v2：当场终局失败。
//
// 嵌套图冒上来的兑付曾经也按尝试分层，后一次尝试看不见它，结果是 `v2 已批准(答:批准 v1 吗)`、`err == nil`
// （审计实测）。
func TestRunWithRetry_NestedGraphRegenerated_OldApprovalNotAppliedToNewContent(t *testing.T) {
	var drafts atomic.Int32
	inner, outCh := retryAskDraftGraph(t, &drafts)
	var flaked atomic.Bool
	var final string
	g := retryAskGraph(t, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		out, err := compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (string, error) {
			cm := compose_v2.NewChannelManager()
			if err := compose_v2.Run(ctx, inner, cm, compose_v2.WithRunNS("inner")); err != nil {
				return "", err
			}
			o, _ := compose_v2.Get(cm.Channels(), outCh)
			if flaked.CompareAndSwap(false, true) {
				return "", errors.New("下游抖了一下")
			}
			return o, nil
		})
		final = out
		return nil, err
	})
	asked, err := retryAskDrive(t, g)
	if n := drafts.Load(); n != 2 {
		t.Fatalf("fixture 没搭对：重试应从头再跑嵌套图（draft 跑 2 次），实际 %d 次", n)
	}
	if final != "" {
		t.Fatalf("对 v1 的批准被兑付给了重新生成的内容：final=%q（问过 %q）", final, asked)
	}
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("同一个位置再问应当场终局失败，got %v（问过 %q）", err, asked)
	}
}

// TestRunWithRetry_NestedGraphRecorded_SkippedOnRetry：RunWithRetry godoc 给的出路——嵌套图跑完就用
// SaveStepResult 记下，后面的尝试跳过它。重试不再重跑那张图，批准只问一次、用在它问的那一版上。
func TestRunWithRetry_NestedGraphRecorded_SkippedOnRetry(t *testing.T) {
	var drafts atomic.Int32
	inner, outCh := retryAskDraftGraph(t, &drafts)
	var flaked atomic.Bool
	var final string
	g := retryAskGraph(t, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		out, err := compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (string, error) {
			o, done := compose_v2.GetStepResult[string](ctx, "inner")
			if !done {
				cm := compose_v2.NewChannelManager()
				if err := compose_v2.Run(ctx, inner, cm, compose_v2.WithRunNS("inner")); err != nil {
					return "", err
				}
				o, _ = compose_v2.Get(cm.Channels(), outCh)
				if err := compose_v2.SaveStepResult(ctx, "inner", o); err != nil {
					return "", err
				}
			}
			if flaked.CompareAndSwap(false, true) {
				return "", errors.New("下游抖了一下")
			}
			return o, nil
		})
		final = out
		return nil, err
	})
	asked, err := retryAskDrive(t, g)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %q）", err, asked)
	}
	if !flaked.Load() || drafts.Load() != 1 {
		t.Fatalf("fixture 没搭对：应失败过一次、draft 只跑 1 次，实际失败过 %v、draft %d 次", flaked.Load(), drafts.Load())
	}
	if !slices.Equal(asked, []string{"批准 v1 吗"}) || final != "v1 已批准(答:批准 v1 吗)" {
		t.Errorf("应只问一次 v1、批准用在 v1 上；问过 %q，final=%q", asked, final)
	}
}
