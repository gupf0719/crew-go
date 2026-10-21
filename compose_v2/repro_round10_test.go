package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// panickyJSONValue.UnmarshalJSON 故意 panic——模拟业务自定义 channel value type
// 含 schema 校验 / enum 断言 panic 的生产场景。
type panickyJSONValue struct {
	X int
}

func (p *panickyJSONValue) UnmarshalJSON([]byte) error {
	panic("biz UnmarshalJSON panic")
}

// 注册 panicky type
func init() {
	compose_v2.RegisterValueType[*panickyJSONValue]("compose_v2_test.panickyJSONValue")
}

// TestRepro_R10_L1_RestoreAtomicRollback 验证 audit R10 角度 C L1：
// 业务自定义 UnmarshalJSON panic 时 Restore 不应留 partial-restore 半烂状态。
//
// **预期行为**：panic 后 cm.values / visitCount / pendingInterrupts 应回滚到
// Restore 入口前状态——atomic 语义同 R9 D1 ApplyAll 修复。
//
// **当前 bug**（修前）：partial-restore 让 cm 进 mixed state，caller 用 stale +
// new 混合数据。
func TestRepro_R10_L1_RestoreAtomicRollback(t *testing.T) {
	chGood := compose_v2.LastValue[int]("good")
	chBad := compose_v2.LastValue[*panickyJSONValue]("bad")

	// cm1 准备 good=10 作"Restore 入口前状态"
	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, chGood)
	compose_v2.RegisterChannel(cm, chBad)
	_ = cm.ApplyAll([]compose_v2.ChannelWrite{
		compose_v2.Write(chGood, 10),
	})

	// cm2 构造一个含 panicky bad 的 cp（marshal 时不 panic，仅 unmarshal panic）
	cm2 := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm2, chGood)
	compose_v2.RegisterChannel(cm2, chBad)
	_ = cm2.ApplyAll([]compose_v2.ChannelWrite{
		compose_v2.Write(chGood, 99),
		compose_v2.Write(chBad, &panickyJSONValue{X: 1}),
	})
	snap, err := cm2.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// 跑多次（Go map 迭代随机顺序，需 N 次提高概率覆盖 good 在 bad 之前的迭代）
	corruptedCount := 0
	for i := 0; i < 50; i++ {
		// 重置 cm 到 good=10 状态
		cm = compose_v2.NewChannelManager()
		compose_v2.RegisterChannel(cm, chGood)
		compose_v2.RegisterChannel(cm, chBad)
		_ = cm.ApplyAll([]compose_v2.ChannelWrite{compose_v2.Write(chGood, 10)})

		restoreErr := cm.Restore(snap)
		// 修复后：restoreErr 应是 panic 转的 error（含 "rolled back"）
		if restoreErr == nil {
			t.Fatalf("iter %d: 期望 Restore 返 error 表明 panic 被 recover", i)
		}
		if !strings.Contains(restoreErr.Error(), "panic") {
			t.Fatalf("iter %d: 期望 error 含 'panic' 标识，实际 %v", i, restoreErr)
		}

		// 关键验证：cm.values[good] 应回滚到 10（Restore 入口前状态），不是 99
		v, _ := compose_v2.Get(cm.Channels(), chGood)
		if v != 10 {
			corruptedCount++
		}
	}
	if corruptedCount > 0 {
		t.Fatalf("Bug L1 reproduced: Restore panic 中段未 atomic 回滚\n"+
			"50 次中 %d 次 cm.values[good] != 10（partial-restore 留 mixed state）\n"+
			"期望全 50 次回滚到 Restore 入口前状态\n"+
			"修复方向：Restore 入口 snapshot values/visitCount/pendingInterrupts，"+
			"defer recover 时 LIFO 还原（同 R9 D1 ApplyAll atomic 模式）",
			corruptedCount)
	}
}

// TestRepro_R10_D_N1_FanoutInterruptPlusReducerPanic 验证 audit R10 D-N1：
// fan-out 部分中断 + 兄弟分支 reducer panic 时 interrupt query 永久丢失（D1
// 修复后的新增 gap）。
//
// **预期行为**：即使 ApplyAll panic，interrupt query 应已 record 到 cm。
// 修复方向：先 record AllInterrupts 再 ApplyAll。
func TestRepro_R10_D_N1_FanoutInterruptPlusReducerPanic(t *testing.T) {
	// 业务自定义 reducer 在第 K 条 panic
	badCh := compose_v2.Custom("bad", func(_, _ int) int {
		panic("biz reducer panic")
	}, 0)

	// 节点 split：SendFrom 分发到 worker_a (抛中断) + worker_b (写 badCh 触发 panic)
	splitFn := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "worker_a"},
			compose_v2.SendDirective{Target: "worker_b"},
		)
		return nil, nil
	}
	workerA := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{InterruptID: "ask_A", Question: "?"},
		}
	}
	workerB := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(badCh, 1)}, nil
	}

	b := compose_v2.NewBuilder()
	b.AddNode("split", splitFn)
	b.AddNode("worker_a", workerA)
	b.AddNode("worker_b", workerB)
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("worker_a", compose_v2.EndNode)
	b.AddEdge("worker_b", compose_v2.EndNode)
	g, _ := b.Compile()

	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, badCh)
	_ = compose_v2.Run(context.Background(), g, cm)

	// 验证：cm.PendingInterrupts() 应至少含 ask_A（修复前因 ApplyAll panic return
	// 跳过 recordInterrupt → 空）
	pending := cm.PendingInterrupts()
	gotAskA := false
	for _, q := range pending {
		if q != nil && q.InterruptID == "ask_A" {
			gotAskA = true
			break
		}
	}
	if !gotAskA {
		t.Fatalf("Bug D-N1 reproduced: fan-out 部分中断 + 兄弟 reducer panic\n"+
			"interrupt query 永久丢失，cm.PendingInterrupts() = %d 条无 ask_A\n"+
			"修复方向：graph_loop 先 record AllInterrupts 再 ApplyAll", len(pending))
	}
}

// TestRepro_R10_D_N2_NilQueryFailFast 验证 audit R10 D-N2：
// 业务抛 InterruptError{Query: nil} 时框架应 fail-fast 转普通 error，避免
// HITL 永久 deadlock（caller 看 InterruptError 但 cm.PendingInterrupts() 空）。
func TestRepro_R10_D_N2_NilQueryFailFast(t *testing.T) {
	throwFn := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, &compose_v2.InterruptError{
			Reason: "biz forgot Query field",
			// Query 故意 nil — 业务笔误
		}
	}
	b := compose_v2.NewBuilder()
	b.AddNode("ask", throwFn)
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g, _ := b.Compile()

	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g, cm)
	if err == nil {
		t.Fatal("expected error from nil-Query InterruptError")
	}

	// 验证：err 不应是 InterruptError（fail-fast 转普通 error 让业务 build/test 期发现）
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Fatalf("Bug D-N2 reproduced: InterruptError{Query: nil} 仍被识别为 InterruptError\n" +
			"caller 看中断标志但 cm.PendingInterrupts() 空 → WithResumeAnswers 无 ID 可填 → 永久 deadlock\n" +
			"修复方向：runNodeWithHooks 检测 ie.Query == nil 时转普通 error，提示业务漏填 Query")
	}
	// 承重①：**错误要能定位到出事的节点和业务自己给的 reason**——只说「漏填了」的话，
	// 一张图里十几个节点，业务不知道是哪个。
	//
	// ⚠️ 这里**不钉具体措辞**：措辞是会改的（原先钉的字面量 "Query is nil" 就在一次
	// 重写中失配了），而「哪个节点 + 业务的 reason」是这条错真正承载的信息。
	for _, want := range []string{`node "ask"`, "biz forgot Query field"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error 里没有 %q，业务无从定位：%v", want, err)
		}
	}
	// 承重②：**不可重试 + 存档不留**——这是确定性的代码 bug（业务漏填字段），
	// 重跑必然同样漏填，重试只会把副作用再放一遍。
	if compose_v2.IsRetryable(err) {
		t.Error("漏填 Query 是代码 bug，不该被判为可重试")
	}
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("应 wrap ErrRunAborted（业务据此判「已终局、别再 Resume」）：%v", err)
	}
}

// TestRepro_R10_D_N3_PanicErrorChain 验证 audit R10 D-N3：
// reducer panic(error) 类型时 ApplyAll wrap 应保 errors.Is/As chain。
type bizErrR10 struct{ msg string }

func (e *bizErrR10) Error() string { return e.msg }

var errBizR10 = &bizErrR10{msg: "biz error"}

func TestRepro_R10_D_N3_PanicErrorChain(t *testing.T) {
	badCh := compose_v2.Custom("bad", func(_, _ int) int {
		panic(errBizR10) // panic with error type
	}, 0)
	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, badCh)
	err := cm.ApplyAll([]compose_v2.ChannelWrite{compose_v2.Write(badCh, 1)})
	if err == nil {
		t.Fatal("expected error from panic")
	}
	var got *bizErrR10
	if !errors.As(err, &got) {
		t.Fatalf("Bug D-N3 reproduced: panic(*bizErr) wrap 用 %%v 丢失 error chain\n"+
			"业务 errors.As 无法识别原 error 类型做差异化处理\n"+
			"修复方向：panic value 实现 error 时用 %%w 保 chain\n"+
			"实际 err: %v", err)
	}
}

// TestRepro_R11_1_FanoutInterruptPlusReducerPanicPreservesType 验证 R11 audit
// 发现的 R10-D 引入的 P0 边缘：fan-out 部分中断 + 兄弟分支 reducer panic →
// ApplyAll 整批不提交把成功分支 writes 株连 + 类型链销毁让 caller errors.As
// 失败 + cp 持久化 frontier=[A] 缺 v_B → Resume 后 v_B 永久丢失。
//
// **预期行为**（修复后）：
//  1. errors.As(err, &InterruptError) 应返 true（保 res.FirstErr 类型链）
//  2. updateFrontier 应让 Resume 重跑成功分支（恢复 v_B）
func TestRepro_R11_1_FanoutSuccessWritesPreservedOnApplyAllPanic(t *testing.T) {
	// chBad reducer 第 1 次 reduce 就 panic
	chBad := compose_v2.Custom("bad", func(_, _ int) int {
		panic("biz reducer panic")
	}, 0)
	// chOK 业务正常累加
	chOK := compose_v2.LastValue[int]("ok")

	// fan-out: split → A (抛中断) + B (写 chOK=42) + C (写 chBad=1 触发 panic)
	splitFn := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "A"},
			compose_v2.SendDirective{Target: "B"},
			compose_v2.SendDirective{Target: "C"},
		)
		return nil, nil
	}
	nodeA := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{InterruptID: "ask_A", Question: "?"},
		}
	}
	nodeB := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(chOK, 42)}, nil
	}
	nodeC := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(chBad, 1)}, nil
	}

	b := compose_v2.NewBuilder()
	b.AddNode("split", splitFn)
	b.AddNode("A", nodeA)
	b.AddNode("B", nodeB)
	b.AddNode("C", nodeC)
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("A", compose_v2.EndNode)
	b.AddEdge("B", compose_v2.EndNode)
	b.AddEdge("C", compose_v2.EndNode)
	g, _ := b.Compile()

	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, chBad)
	compose_v2.RegisterChannel(cm, chOK)
	err := compose_v2.Run(context.Background(), g, cm)
	if err == nil {
		t.Fatal("expected error from fan-out interrupt + panic")
	}

	// 验证 1：err 应保 InterruptError 类型链（修前 ApplyAll panic err wrap 覆盖）
	var ie *compose_v2.InterruptError
	if !errors.As(err, &ie) {
		t.Fatalf("R11-1 reproduced: errors.As 无法识别 InterruptError 类型\n"+
			"R10-D D-N1 顺序调整修了 deadlock 但 ApplyAll panic err wrap 覆盖 res.FirstErr 类型链\n"+
			"修复方向：ApplyAll 失败时若 res.FirstErr != nil，wrap res.FirstErr 作主 error\n"+
			"实际 err: %v", err)
	}
	if ie == nil || ie.Query == nil || ie.Query.InterruptID != "ask_A" {
		t.Fatalf("InterruptError 类型保留但内容错: %+v", ie)
	}
	// 验证 2：cm.PendingInterrupts() 应含 ask_A
	pending := cm.PendingInterrupts()
	if len(pending) != 1 || pending[0].InterruptID != "ask_A" {
		t.Fatalf("ask_A 未 record: %v", pending)
	}
}

// TestResumeAnswersWithoutArchive_FailFast：cper+sid+answers 给齐但 (sid, ns) 没有
// 可续的存档——必须 fail-fast 返 ErrNoPendingCheckpoint，且**图体一步都不执行**。
//
// 前身是 R11-4 的 WARN 版（只告警、照跑 fresh run）——已升级：调用方攥着框架发的
// InterruptID 在答题而没有档在等这道题，fresh 跑下去答案静默蒸发 + 同样的问题换个
// 新 ID 再问一遍（「为什么答完又问」死循环），业务节点还会经 GetResumeContext
// 假阳性误读旧答案。
func TestResumeAnswersWithoutArchive_FailFast(t *testing.T) {
	nodeRuns := 0
	noopFn := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		nodeRuns++
		return nil, nil
	}
	b := compose_v2.NewBuilder()
	b.AddNode("noop", noopFn)
	b.AddEdge(compose_v2.StartNode, "noop")
	b.AddEdge("noop", compose_v2.EndNode)
	g, _ := b.Compile()

	cper := compose_v2.NewInMemoryCheckpointer() // 空 cper, sid 不存在 cp
	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("nonexistent_sid"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			"x": schema.UserMessage("y"),
		}),
	)
	if !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
		t.Fatalf("答案无档该 fail-fast 返 ErrNoPendingCheckpoint, got %v", err)
	}
	if nodeRuns != 0 {
		t.Fatalf("fail-fast 该在图体执行之前, 节点却跑了 %d 次", nodeRuns)
	}
	// 定位信息要给全：sid + 答案 ID
	for _, want := range []string{"nonexistent_sid", "x"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("错误正文该含定位信息 %q, got %v", want, err)
		}
	}
}

// bizReducerErr 业务 reducer panic 抛 typed error——业务 caller 想用 errors.As
// 拿原类型做差异化处理。
type bizReducerErr struct{ Code int }

func (e *bizReducerErr) Error() string { return fmt.Sprintf("biz code=%d", e.Code) }

// TestRepro_R12_2_DoubleErrorPreservesBothChains 验证 R12-2：fan-out 同时有
// InterruptError + reducer panic typed error 时，主链 InterruptError + 副链
// reducer typed err 都应能 errors.As 穿透（用 errors.Join 替代 fmt.Errorf %v）。
func TestRepro_R12_2_DoubleErrorPreservesBothChains(t *testing.T) {
	chBad := compose_v2.Custom("bad", func(_, _ int) int {
		panic(&bizReducerErr{Code: 42}) // typed error
	}, 0)
	chOK := compose_v2.LastValue[int]("ok")

	splitFn := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "A"},
			compose_v2.SendDirective{Target: "B"},
		)
		return nil, nil
	}
	nodeA := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{InterruptID: "ask_A", Question: "?"},
		}
	}
	nodeB := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// B 写 chBad 和 chOK——chBad 的 reducer panic
		return []compose_v2.ChannelWrite{
			compose_v2.Write(chOK, 42),
			compose_v2.Write(chBad, 1),
		}, nil
	}

	b := compose_v2.NewBuilder()
	b.AddNode("split", splitFn)
	b.AddNode("A", nodeA)
	b.AddNode("B", nodeB)
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("A", compose_v2.EndNode)
	b.AddEdge("B", compose_v2.EndNode)
	g, _ := b.Compile()

	cm := compose_v2.NewChannelManager()
	compose_v2.RegisterChannel(cm, chBad)
	compose_v2.RegisterChannel(cm, chOK)
	err := compose_v2.Run(context.Background(), g, cm)
	if err == nil {
		t.Fatal("expected error from double InterruptError + reducer panic")
	}

	// 验证 1：errors.As 应能拿到 InterruptError（R11-1 已修）
	var ie *compose_v2.InterruptError
	if !errors.As(err, &ie) {
		t.Fatalf("R12-2 regression: errors.As 无法拿到 InterruptError\n实际 err: %v", err)
	}
	if ie.Query == nil || ie.Query.InterruptID != "ask_A" {
		t.Fatalf("InterruptError 内容错: %+v", ie)
	}

	// 验证 2：errors.As 应能拿到 bizReducerErr（R12-2 修复点）
	var be *bizReducerErr
	if !errors.As(err, &be) {
		t.Fatalf("R12-2 reproduced: secondary chain 已断，业务无法 unwrap typed reducer err\n"+
			"R11-1 用 fmt.Errorf %%v 嵌入 secondary 让 errors.As(&BizErr) 失败\n"+
			"修复方向：errors.Join(primary, secondary) 双链都保 unwrap\n"+
			"实际 err: %v", err)
	}
	if be.Code != 42 {
		t.Fatalf("bizReducerErr 内容错: code=%d", be.Code)
	}
}

// TestResumeAnswersWithoutArchive_EmptyMapIsNotAnAnswer：空但非 nil 的答案 map
// **等于没给**（全仓既有约定：9 编排都按 len>0 判），不该被守卫拒。
//
// 曾按 `!= nil` 判 ⟹ `answers := map[...]{}; for ... {}` 然后无条件传的写法被拒，
// 而错误正文自己说「提供了 0 条答案」——前提不成立。
func TestResumeAnswersWithoutArchive_EmptyMapIsNotAnAnswer(t *testing.T) {
	nodeRuns := 0
	b := compose_v2.NewBuilder()
	b.AddNode("noop", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		nodeRuns++
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "noop")
	b.AddEdge("noop", compose_v2.EndNode)
	g, _ := b.Compile()

	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()),
		compose_v2.WithSessionID("empty_answers_sid"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{}), // 空但非 nil
	)
	if err != nil {
		t.Fatalf("空答案 map 等于没给，不该被拒: %v", err)
	}
	if nodeRuns != 1 {
		t.Fatalf("图该正常跑, 节点跑了 %d 次", nodeRuns)
	}
}

// TestResumeAnswersWithoutArchive_ReasonMatchesSituation：错误正文必须说**这一次**
// 的真实处境，不能一句话套三种。
//
// 尤其 fresh 那档：声明新开时 auto-load 整块跳过，**档很可能还在**——正文断言
// 「没有存档」是事实错误，会把人支到「去查 sid」的错误排查方向。
func TestResumeAnswersWithoutArchive_ReasonMatchesSituation(t *testing.T) {
	mkGraph := func(runs *int) *compose_v2.Graph {
		b := compose_v2.NewBuilder()
		b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			*runs++
			_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准?"})
			return nil, err
		})
		b.AddEdge(compose_v2.StartNode, "ask")
		g, _ := b.Compile()
		return g
	}

	t.Run("档不在", func(t *testing.T) {
		runs := 0
		err := compose_v2.Run(context.Background(), mkGraph(&runs), compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()),
			compose_v2.WithSessionID("no_cp_sid"),
			compose_v2.WithResumeAnswers(map[string]*schema.Message{"x": schema.UserMessage("y")}))
		if !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
			t.Fatalf("该返哨兵: %v", err)
		}
		if !strings.Contains(err.Error(), "没有存档") {
			t.Errorf("正文该说「没有存档」: %v", err)
		}
	})

	t.Run("声明了新开时档还在", func(t *testing.T) {
		cper := compose_v2.NewInMemoryCheckpointer()
		sid := "fresh_with_answers_sid"
		runs := 0
		// 先跑一轮留下挂起档
		ie := compose_v2.Run(context.Background(), mkGraph(&runs), compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
		if _, ok := compose_v2.IsInterrupt(ie); !ok {
			t.Fatalf("Run1 该中断: %v", ie)
		}
		// 档确实在
		if cp, _ := cper.LoadNS(context.Background(), sid, ""); cp == nil {
			t.Fatalf("fixture 没搭对：档该在")
		}
		// 声明新开 + 同时给答案（两个意图矛盾）
		err := compose_v2.Run(context.Background(), mkGraph(&runs), compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
			compose_v2.WithFreshRun(),
			compose_v2.WithResumeAnswers(map[string]*schema.Message{"x": schema.UserMessage("y")}))
		if !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
			t.Fatalf("该返哨兵: %v", err)
		}
		if strings.Contains(err.Error(), "没有存档") {
			t.Errorf("档明明还在，正文却说「没有存档」——会把人支去查 sid: %v", err)
		}
		if !strings.Contains(err.Error(), "声明了新开") {
			t.Errorf("正文该说清是本次声明了新开: %v", err)
		}
	})
}
