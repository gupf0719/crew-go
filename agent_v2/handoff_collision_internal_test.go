package agent_v2

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"crew-go/compose_v2"
	"crew-go/store"
)

// sigCapture 在一段新的执行里跑 fn，返回它做出的信号（白盒测试用）。
func sigCapture(t *testing.T, fn func(ctx context.Context)) compose_v2.Signals {
	t.Helper()
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		fn(ctx)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return sigs
}

func emitTransfer(ctx context.Context, dest, origin, target string) {
	compose_v2.EmitSignal(ctx, handoffSignal, compose_v2.Signal{Dest: dest, Value: target, Origin: origin})
}

// TestTakeHandoffTarget_SequentialLaterWins：同一段执行里先后做出的转交，后做的为准；transfer 工具
// 与 HandoffTo 做出的是同一类，谁后谁赢——不再有「工具路径优先」。
//
// ⚠️ 曾经的语义是「同一次调用里目标不一致一律报错」：Sequential 候选的第二步修正第一步的转交、
// Loop 下一轮改主意，全部被当成冲突、整个 Run 失败（审计实测，改动前的实现是后者为准）。曾经还有
// 「工具路径优先」：业务代码在内层 LLM 转交之后再 HandoffTo 修正，赢的却是先做出的那个。
func TestTakeHandoffTarget_SequentialLaterWins(t *testing.T) {
	if got, _ := takeHandoffTarget(compose_v2.Signals{}, "d"); got != "" {
		t.Errorf("没有决定应返空，实际 %q", got)
	}

	sigs := sigCapture(t, func(ctx context.Context) {
		emitTransfer(ctx, "d", "c0#0", "first")
		emitTransfer(ctx, "d", "c1#1", "second")
		emitTransfer(ctx, "d", "c2#2", "third")
	})
	if got, _ := takeHandoffTarget(sigs, "d"); got != "third" {
		t.Errorf("先后做出的转交应以最后一次为准，实际 %q", got)
	}

	toolThenCode := sigCapture(t, func(ctx context.Context) {
		emitTransfer(ctx, "d", "call_x#0", "from_tool")
		compose_v2.EmitSignal(ctx, handoffSignal, compose_v2.Signal{Dest: "d", Value: "from_code"})
	})
	if got, _ := takeHandoffTarget(toolThenCode, "d"); got != "from_code" {
		t.Errorf("工具转交之后业务代码再 HandoffTo，应以后者为准；实际 %q", got)
	}
}

// TestTakeHandoffTarget_OnlyOwnDest：只取发给自己的；发给别的收件人的、别的种类的原样留下，
// 交给更上方。
func TestTakeHandoffTarget_OnlyOwnDest(t *testing.T) {
	sigs := sigCapture(t, func(ctx context.Context) {
		emitTransfer(ctx, "inner", "c0#0", "billing")
		emitTransfer(ctx, "outer", "c1#0", "sales")
		compose_v2.EmitSignal(ctx, routeSignal, compose_v2.Signal{Dest: "rt", Value: "math"})
	})
	got, rest := takeHandoffTarget(sigs, "inner")
	if got != "billing" {
		t.Fatalf("应只取发给 inner 的那条，实际 %q", got)
	}
	if rest.Len() != 2 {
		t.Fatalf("发给别处的两条应原样留下，实际 %d 条", rest.Len())
	}
	if up, _ := takeHandoffTarget(rest, "outer"); up != "sales" {
		t.Errorf("留下的那条应仍能被 outer 取到，实际 %q", up)
	}
}

func containsStr(s, sub string) bool { return strings.Contains(s, sub) }

// TestSideSignal_ConcurrentJoinIsDeterministic 用 WaitGroup 屏障强制 N 个写入方**真并发**
// （不让调度器把它们自然串行化），跑够多轮确认合并结果恒定：每一路各自 CaptureSignals、汇合时
// 合并。两种合并都验——JoinSignals（并发必须一致：业务在节点里自己扇出时用）恒定报同一个冲突，
// JoinSignalsFirstWins（tools 节点的 fanout，ISSUE-81）恒定是下标最小的那一路生效，跟谁先跑完无关。
//
// 几路直接往同一段执行里交的话，它们被当成先后做出的、谁后到谁为准，随调度变——所以并发合并必须
// 显式做。
func TestSideSignal_ConcurrentJoinIsDeterministic(t *testing.T) {
	targets := []string{"alpha", "beta", "gamma", "delta"}
	for iter := 0; iter < 300; iter++ {
		branches := make([]compose_v2.Signals, len(targets))
		var ready, done sync.WaitGroup
		ready.Add(len(targets))
		done.Add(len(targets))
		// 乱序启动，让「先起跑的」不是下标 0
		for _, i := range []int{3, 1, 0, 2} {
			go func(i int) {
				defer done.Done()
				ready.Done()
				ready.Wait() // 屏障：四路都到齐才一起写
				branches[i] = sigCapture(t, func(ctx context.Context) {
					emitTransfer(ctx, "d", fmt.Sprintf("call_%d#%d", i, i), targets[i])
				})
			}(i)
		}
		done.Wait()
		// 四个不同目标 ⟹ 恒定报冲突。**「恒定」才是这条用例要验的**。
		_, err := compose_v2.JoinSignals(branches...)
		var conflict *compose_v2.SignalConflictError
		if !errors.As(err, &conflict) || compose_v2.IsRetryable(err) {
			t.Fatalf("iter %d 四个不同目标应恒定报冲突（不可重试），实际 err=%v", iter, err)
		}
		if conflict.First.Value != "alpha" || conflict.Second.Value != "beta" {
			t.Fatalf("iter %d 冲突应按路的顺序报（alpha 与 beta），跟谁先跑完无关；实际 %q 与 %q",
				iter, conflict.First.Value, conflict.Second.Value)
		}
		won, superseded := compose_v2.JoinSignalsFirstWins(branches...)
		if s, ok, _ := won.Take(handoffSignal, "d"); !ok || s.Value != "alpha" || len(superseded) != 3 {
			t.Fatalf("iter %d 按下标第一个生效：应是 alpha、另外三路被盖掉，实际 %+v（被盖掉 %d 路）",
				iter, s, len(superseded))
		}
		for j, sp := range superseded {
			if sp.Branch != j+1 || sp.Kept.Value != "alpha" || sp.Dropped.Value != targets[j+1] {
				t.Fatalf("iter %d 被盖掉的应按下标报：第 %d 条 %+v", iter, j, sp)
			}
		}
	}
	// 目标一致的几路算一次。
	same := make([]compose_v2.Signals, 3)
	for i := range same {
		same[i] = sigCapture(t, func(ctx context.Context) { emitTransfer(ctx, "d", fmt.Sprintf("c%d", i), "same") })
	}
	joined, err := compose_v2.JoinSignals(same...)
	if err != nil || joined.Len() != 1 {
		t.Fatalf("目标一致的几路应合并成一条，实际 len=%d err=%v", joined.Len(), err)
	}
}

// TestHandoffDest_CtxStripIsolatesToolBoundary 白盒验证「agent 被当工具用时摘掉收件人」这条协议
// 本身：摘过之后读不到，而内层自己 push 的照常可见（嵌套 HandoffAgent 不受影响）。
func TestHandoffDest_CtxStripIsolatesToolBoundary(t *testing.T) {
	ctx := withHandoffDest(context.Background(), "outer")
	if d, ok := getHandoffDest(ctx); !ok || d != "outer" {
		t.Fatal("前提不成立：push 之后读不到自己的收件人")
	}
	stripped := withoutHandoffDest(ctx)
	if d, ok := getHandoffDest(stripped); ok {
		t.Errorf("工具边界没摘干净：仍读得到收件人 %q", d)
	}
	// 原 ctx 不受影响（摘是派生一个新 ctx，不是就地改）
	if d, ok := getHandoffDest(ctx); !ok || d != "outer" {
		t.Error("摘收件人污染了上游 ctx")
	}
	// 摘过之后内层自己 push 的照常可见——嵌套 HandoffAgent 进去会 push 自己的
	if d, ok := getHandoffDest(withHandoffDest(stripped, "inner")); !ok || d != "inner" {
		t.Error("摘过之后内层再 push 自己的收件人读不到——嵌套 HandoffAgent 会失效")
	}
	// 没有时摘是 no-op（省一次 alloc）
	bare := context.Background()
	if withoutHandoffDest(bare) != bare {
		t.Error("没有收件人时摘不该派生新 ctx")
	}
}

// TestHandoffActiveNS_InstanceDimension_PreventsCrossPollution 白盒回归（audit LOW-2，同
// MED-1 根因）：两个不同 HandoffAgent 共享同一 ThreadID + 同一 store 时持久 active 不互污染。
//
// **sanity-revert**：去 handoffActiveNS 的 name 维度则两实例同 slot → hB 的 salesB 覆盖 →
// hA.readPersistedActive 读到 salesB（不在 hA.agents）→ 自愈清 + fallback initial，
// gotA="frontA"≠"billingA"，本测试 FAIL 精确指向跨实例污染。
func TestHandoffActiveNS_InstanceDimension_PreventsCrossPollution(t *testing.T) {
	st := store.NewInMemoryStore()
	ctx := context.Background()
	const tid = "conv-shared"

	// 两个不同 HandoffAgent（白盒直构造，readPersistedActive 只读 name/initial/agents）。
	hA := &HandoffAgent{name: "support", initial: "frontA", agents: map[string]Agent{"frontA": nil, "billingA": nil}}
	hB := &HandoffAgent{name: "sales", initial: "frontB", agents: map[string]Agent{"frontB": nil, "salesB": nil}}

	// 两实例各持久自己的 active 到同一 store + 同一 ThreadID。
	if err := st.Put(ctx, handoffActiveNS(hA.name, "", tid), handoffActiveKey, "billingA"); err != nil {
		t.Fatal(err)
	}
	if err := st.Put(ctx, handoffActiveNS(hB.name, "", tid), handoffActiveKey, "salesB"); err != nil {
		t.Fatal(err)
	}

	// 各自读到自己的 active（name 维度隔离，不互污染、不被对方自愈误清）。
	if got := hA.readPersistedActive(ctx, st, tid); got != "billingA" {
		t.Errorf("HandoffA active 被跨实例污染：期望 billingA，实际 %q", got)
	}
	if got := hB.readPersistedActive(ctx, st, tid); got != "salesB" {
		t.Errorf("HandoffB active 被跨实例污染：期望 salesB，实际 %q", got)
	}
}
