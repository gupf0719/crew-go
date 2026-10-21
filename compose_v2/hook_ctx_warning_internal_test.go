package compose_v2

// 采纳钩子 ctx 的两类告警按「采纳点 × 类别」去重、带上采纳点的位置：
//   - 钩子返回外来 ctx：曾经全进程只打一次、不带位置——第一个违约者之后的都不出声，也看不出是哪个
//     钩子（审计实测：33 次外来采纳只打出 2 条）；
//   - 钩子在派生 ctx 上推会话 ID / sid：推了不生效（身份取框架的），曾经不告警。

import (
	"context"
	"log"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

type warnCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *warnCapture) Enabled(context.Context, slog.Level) bool { return true }
func (h *warnCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}
func (h *warnCapture) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *warnCapture) WithGroup(string) slog.Handler      { return h }

// captureHookCtxWarnings 接管默认 logger 并清空告警去重表，测试结束时还原。
//
// log 包的输出与 flags 要一起还原：slog.SetDefault 换成自定义 handler 时会顺带把 log 包的输出改指到
// 它，之后 SetDefault 回原来的默认 logger 并不把 log 的输出改回来——同一个测试二进制里此后所有走默认
// logger 的日志都流进这个已经弃用的 handler，谁也看不到。
func captureHookCtxWarnings(t *testing.T) *warnCapture {
	t.Helper()
	h := &warnCapture{}
	orig, origOut, origFlags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(h))
	resetHookCtxWarned()
	t.Cleanup(func() {
		slog.SetDefault(orig)
		log.SetOutput(origOut)
		log.SetFlags(origFlags)
		resetHookCtxWarned()
	})
	return h
}

// resetHookCtxWarned 清空告警去重表。
func resetHookCtxWarned() {
	hookCtxWarnedMu.Lock()
	defer hookCtxWarnedMu.Unlock()
	clear(hookCtxWarned)
	clear(hookCtxWarnedSites)
}

// matching 返回消息含 substr 的告警：各自的消息与 site 属性。
func (h *warnCapture) matching(substr string) (msgs, sites []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if !strings.Contains(r.Message, substr) {
			continue
		}
		site := ""
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "site" {
				site = a.Value.String()
			}
			return true
		})
		msgs = append(msgs, r.Message)
		sites = append(sites, site)
	}
	return msgs, sites
}

// returning 返回一个直接交回 got 的钩子（不管收到什么）。
func returning(got context.Context) func(context.Context) (context.Context, error) {
	return func(context.Context) (context.Context, error) { return got, nil }
}

// TestCallHook_ForeignWarningPerSiteWithLocation：同一个采纳点连续采纳外来 ctx 只告警一次，另一个采纳点再告警
// 一次；两条都带着各自调 CallHook 的位置。统一入口之下采纳机制自己的那几帧对所有采纳点都一样——去重键只取
// 一帧的话两个采纳点被认成同一个，第二个不出声。
func TestCallHook_ForeignWarningPerSiteWithLocation(t *testing.T) {
	h := captureHookCtxWarnings(t)
	for i := 0; i < 3; i++ {
		_, _ = CallHook(context.Background(), returning(context.Background())) // 采纳点 A
	}
	_, _ = CallHook(context.Background(), returning(context.Background())) // 采纳点 B

	_, sites := h.matching("不是从入参派生")
	if len(sites) != 2 || sites[0] == sites[1] {
		t.Fatalf("应每个采纳点告警一次、各带自己的位置，实际 %q", sites)
	}
	for _, s := range sites {
		if !strings.Contains(s, "TestCallHook_ForeignWarningPerSiteWithLocation") || !strings.Contains(s, "_test.go:") {
			t.Errorf("site 应指向调 CallHook 的那一处，实际 %q", s)
		}
	}
}

type warnBizKey struct{}

// TestCallHook_WarnsWhenHookPushesIdentity：钩子在派生 ctx 上推了跟框架不同的会话 ID / sid——推了不生效，各告警
// 一次；没推、推的值与框架相同时不告警。
func TestCallHook_WarnsWhenHookPushesIdentity(t *testing.T) {
	h := captureHookCtxWarnings(t)
	base := WithSessionIDCtx(WithThreadIDCtx(context.Background(), "conv-now"), "sid-now")

	_, _ = CallHook(base, func(c context.Context) (context.Context, error) {
		return context.WithValue(c, warnBizKey{}, "span"), nil
	})
	_, _ = CallHook(base, func(c context.Context) (context.Context, error) {
		return WithSessionIDCtx(WithThreadIDCtx(c, "conv-now"), "sid-now"), nil
	})
	if msgs, _ := h.matching("不生效"); len(msgs) != 0 {
		t.Fatalf("没推身份、或推的值与框架相同时不该告警，实际 %q", msgs)
	}

	got, _ := CallHook(base, func(c context.Context) (context.Context, error) {
		return WithSessionIDCtx(WithThreadIDCtx(c, "conv-hook"), "sid-hook"), nil
	})
	if GetThreadID(got) != "conv-now" || GetSessionID(got) != "sid-now" {
		t.Fatalf("钩子推的身份不该生效：会话 ID=%q sid=%q", GetThreadID(got), GetSessionID(got))
	}
	msgs, sites := h.matching("不生效")
	if len(msgs) != 2 || !strings.Contains(strings.Join(msgs, "|"), "会话 ID") || !strings.Contains(strings.Join(msgs, "|"), "sid") {
		t.Fatalf("推了不同的会话 ID 与 sid 应各告警一次，实际 %q", msgs)
	}
	for _, s := range sites {
		if !strings.Contains(s, "TestCallHook_WarnsWhenHookPushesIdentity") {
			t.Errorf("site 应指向调 CallHook 的那一处，实际 %q", s)
		}
	}
}

// identityLookupCounter 数 ctx 上会话 ID / sid 这两样身份被查了几次。
type identityLookupCounter struct {
	context.Context
	n *int
}

func (c identityLookupCounter) Value(key any) any {
	switch key.(type) {
	case threadIDKey, sessionIDKey:
		*c.n++
	}
	return c.Context.Value(key)
}

// TestCallHook_NoIdentityLookupWithoutPush：钩子这次调用里没经公开的推法推过身份时，采纳不去查会话 ID / sid——查要
// 沿 ctx 链走，每次采纳都查的话，一批 tool_call 里采纳层层嵌套、开销按批大小的平方涨（审计实测 N=32 时整次 Run
// 慢三成）。推过之后才比对。
func TestCallHook_NoIdentityLookupWithoutPush(t *testing.T) {
	captureHookCtxWarnings(t)
	base := WithSessionIDCtx(WithThreadIDCtx(context.Background(), "conv-now"), "sid-now")

	n := 0
	_, _ = CallHook(base, func(c context.Context) (context.Context, error) {
		return identityLookupCounter{Context: context.WithValue(c, warnBizKey{}, "span"), n: &n}, nil
	})
	if n != 0 {
		t.Fatalf("钩子没推身份，采纳却查了 %d 次身份", n)
	}

	_, _ = CallHook(base, func(c context.Context) (context.Context, error) {
		pushed := WithThreadIDCtx(c, "conv-hook") // 经公开的推法推：在这次钩子调用上记一笔
		return identityLookupCounter{Context: pushed, n: &n}, nil
	})
	if n == 0 {
		t.Fatal("钩子推过身份，采纳该比对（否则推了不生效也不告警）")
	}
}

// TestCallHook_ForeignRepeatDoesNotResolveStack：同一个采纳点反复采纳外来 ctx，告警只在第一次打；之后每次只付去重
// 的代价，不再解析调用栈（审计实测曾是每次 1.2µs、8 次分配）——分配次数跟采纳派生 ctx 一样多。
func TestCallHook_ForeignRepeatDoesNotResolveStack(t *testing.T) {
	captureHookCtxWarnings(t)
	stale := context.WithValue(context.Background(), warnBizKey{}, "stale")
	foreignHook := returning(stale)
	derivedHook := func(c context.Context) (context.Context, error) { return c, nil }
	_, _ = CallHook(context.Background(), foreignHook) // 第一次：告警、解析调用栈
	derived := testing.AllocsPerRun(50, func() { _, _ = CallHook(context.Background(), derivedHook) })
	foreign := testing.AllocsPerRun(50, func() { _, _ = CallHook(context.Background(), foreignHook) })
	if foreign > derived {
		t.Errorf("同一个采纳点反复采纳外来 ctx，每次分配 %.0f 次，多于采纳派生 ctx 的 %.0f 次（去重之后不该再有开销）", foreign, derived)
	}
}

// callVia 是一个会被内联的小包装：内联到两处时，「调 CallHook 的那一处」在机器码里有两份副本、两组 PC，源码位置
// 却是同一个。
func callVia(got context.Context) { _, _ = CallHook(context.Background(), returning(got)) }

// TestCallHook_InlinedWrapperIsOneSite：调 CallHook 的函数被内联到多处时，同一个源码位置仍只告警一次——去重按 PC
// 走快路径，但第一次碰到一组 PC 时还要按解析出的位置再查一次。只按 PC 去重的时候，可内联的包装从两处调用打两条
// 一模一样的告警（审计实测，泛型的两种实例化、直接调用的闭包同理）。
func TestCallHook_InlinedWrapperIsOneSite(t *testing.T) {
	h := captureHookCtxWarnings(t)
	callVia(context.Background())
	callVia(context.Background())
	if _, sites := h.matching("不是从入参派生"); len(sites) != 1 {
		t.Fatalf("同一个源码位置应只告警一次，实际 %d 条：%q", len(sites), sites)
	}
}

// adoptViaEnterHook 是工具中间件那种采纳点：EnterHook 小到会被内联进这里，它交出的采纳函数随之改名。它自己不许
// 被内联——生产里的采纳点（tools 节点里的大函数）也不会被内联；它被内联进测试函数时闭包的名字又是另一个样子，
// 测不到那个形状（实测：不加这一行时退回按函数名的名单照样绿）。
//
//go:noinline
func adoptViaEnterHook(got context.Context) context.Context {
	_, adopt := EnterHook(context.Background())
	return adopt(got) // 采纳点：告警要指向这一行
}

// TestEnterHook_ForeignWarningSiteIsCaller：经 EnterHook 交出的采纳函数采纳外来 ctx，告警的位置指向调采纳函数的
// 那一处。EnterHook 被内联进调用方之后，它交出的闭包叫「调用方.EnterHook.func1」——曾经按函数名列的跳过名单认不出
// 它，位置报成了 phase.go 里那个闭包（审计实测）。
//
// ⚠️ 眼下 EnterHook 超了内联预算（enterHookCall 要查外层的标记），闭包不改名，按函数名的名单也认得出：这条用例只在
// EnterHook 重新可内联时才分得出两种判法。2026-10-13 用变异实测过：把 enterHookCall 改回不查外层标记（EnterHook
// 随之可内联）时，退回旧名单这条红在「phase.go:168」，按文件判照绿。
func TestEnterHook_ForeignWarningSiteIsCaller(t *testing.T) {
	h := captureHookCtxWarnings(t)
	adoptViaEnterHook(context.Background())
	_, sites := h.matching("不是从入参派生")
	if len(sites) != 1 {
		t.Fatalf("应告警一次，实际 %q", sites)
	}
	if !strings.Contains(sites[0], "hook_ctx_warning_internal_test.go:") || strings.Contains(sites[0], "phase.go") {
		t.Errorf("site 应指向调采纳函数的那一行，实际 %q", sites[0])
	}
}

// TestCallHook_NestedCallHookIsDerived：业务的组合钩子用 CallHook 调内层钩子、交回内层采纳的结果——外层那次采纳认得
// 出它是从交给组合钩子的 ctx 派生的：内层钩子加的截止时间、推的业务值都在，不告警。曾经外层只比一个标记，把它当成
// 外来的：截止时间静默丢掉、还打一条误报的告警（审计实测）。
func TestCallHook_NestedCallHookIsDerived(t *testing.T) {
	h := captureHookCtxWarnings(t)
	inner := func(c context.Context) (context.Context, error) {
		cc, cancel := context.WithTimeout(c, time.Hour)
		t.Cleanup(cancel)
		return context.WithValue(cc, warnBizKey{}, "inner"), nil
	}
	got, err := CallHook(context.Background(), func(c context.Context) (context.Context, error) {
		return CallHook(c, inner)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.Deadline(); !ok {
		t.Error("内层钩子加的截止时间应生效")
	}
	if got.Value(warnBizKey{}) != "inner" {
		t.Errorf("内层钩子推的业务值应在，实际 %v", got.Value(warnBizKey{}))
	}
	if msgs, _ := h.matching("不是从入参派生"); len(msgs) != 0 {
		t.Errorf("嵌套采纳的结果是派生的，不该告警，实际 %q", msgs)
	}
}

// TestCallHook_NestedForeignWarnsAtInnerSite：组合钩子里的内层钩子返回外来 ctx——内层那次采纳告警一次（位置是组合
// 钩子里调 CallHook 的那一行），外层不再告警；可配的取框架的。
func TestCallHook_NestedForeignWarnsAtInnerSite(t *testing.T) {
	h := captureHookCtxWarnings(t)
	stale, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	got, _ := CallHook(context.Background(), func(c context.Context) (context.Context, error) {
		return CallHook(c, returning(stale)) // 内层采纳点
	})
	if _, ok := got.Deadline(); ok {
		t.Error("外来 ctx 上的截止时间不该生效")
	}
	_, sites := h.matching("不是从入参派生")
	if len(sites) != 1 || !strings.Contains(sites[0], "TestCallHook_NestedForeignWarnsAtInnerSite.func1") {
		t.Fatalf("应只在内层那次采纳告警一次，实际 %q", sites)
	}
}

// TestCallHook_NestedIdentityPushWarnsAtOuter：组合钩子在内层采纳之后推了会话 ID——ctx 答的是内层的标记，「推过身份」
// 要连外层那次调用一起记，外层采纳才去比对、告警（推了不生效）。
func TestCallHook_NestedIdentityPushWarnsAtOuter(t *testing.T) {
	h := captureHookCtxWarnings(t)
	base := WithThreadIDCtx(context.Background(), "conv-now")
	got, _ := CallHook(base, func(c context.Context) (context.Context, error) {
		adopted, err := CallHook(c, func(cc context.Context) (context.Context, error) { return cc, nil })
		return WithThreadIDCtx(adopted, "conv-composite"), err
	})
	if GetThreadID(got) != "conv-now" {
		t.Fatalf("组合钩子推的会话 ID 不该生效，实际 %q", GetThreadID(got))
	}
	if msgs, _ := h.matching("不生效"); len(msgs) != 1 {
		t.Fatalf("推了不生效应在外层采纳时告警一次，实际 %q", msgs)
	}
}
