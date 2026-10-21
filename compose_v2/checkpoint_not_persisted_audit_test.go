package compose_v2_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// 出口写档失败（ErrCheckpointNotPersisted）的第二批：审计找出的几个入口与窗口。

// 业务写了宽松的重试谓词（恒 true）：嵌套执行出口写档撞上身份冲突（槽属于别人，不带 ErrRunAborted）也不
// 重试。曾经框架那道 guard 漏了它，只靠类型上的 NonRetryable（只在默认谓词里生效），嵌套执行连同副作用跑了
// 4 次（审计实测）。
func TestNotPersisted_RetryGuardHoldsWithPermissivePredicate(t *testing.T) {
	cp := newNotPersistedCP()
	cp.conflictNS = "child"
	var work, attempts atomic.Int32
	cb := compose_v2.NewBuilder().WithName("np_retry_child")
	cb.AddNode("work", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		work.Add(1)
		return nil, nil
	})
	cb.AddNode("ask", npAsk("子图的提问"))
	cb.AddEdge(compose_v2.StartNode, "work")
	cb.AddEdge("work", "ask")
	cb.AddEdge("ask", compose_v2.EndNode)
	child := npCompile(t, cb)

	b := compose_v2.NewBuilder().WithName("np_retry")
	b.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.RunWithRetry(ctx, compose_v2.RetryConfig{MaxRetries: 3, ShouldRetry: func(error) bool { return true }},
			func(c context.Context) (struct{}, error) {
				attempts.Add(1)
				return struct{}{}, compose_v2.Run(c, child, compose_v2.NewChannelManager(), compose_v2.WithRunNS("child"))
			})
		return nil, err
	})
	b.AddEdge(compose_v2.StartNode, "host")
	b.AddEdge("host", compose_v2.EndNode)
	err := compose_v2.Run(context.Background(), npCompile(t, b), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("np_retry"))
	if !errors.Is(err, compose_v2.ErrCheckpointNotPersisted) {
		t.Fatalf("应返 ErrCheckpointNotPersisted：%v", err)
	}
	if a, w := attempts.Load(), work.Load(); a != 1 || w != 1 {
		t.Fatalf("档没写成不该重试：尝试 %d 次、work 跑了 %d 次（应各 1 次）", a, w)
	}
}

// delayedSaveCP 的 SaveNS 先等 delay 再写。lateCommit 为假时等的途中 ctx 结束就报 ctx 错、不写（同持久后端）；
// 为真时照样写进去、写完再看 ctx（写进去了，回包时调用方已经走了）。
type delayedSaveCP struct {
	*compose_v2.InMemoryCheckpointer
	delay      time.Duration
	lateCommit bool
	saves      atomic.Int32
}

func (c *delayedSaveCP) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	c.saves.Add(1)
	if c.lateCommit {
		time.Sleep(c.delay)
		if err := c.InMemoryCheckpointer.SaveNS(context.Background(), sid, ns, cp); err != nil {
			return err
		}
		return ctx.Err()
	}
	select {
	case <-time.After(c.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return c.InMemoryCheckpointer.SaveNS(ctx, sid, ns, cp)
}

// 调用方的截止时间在出口写档途中到点：写档不被截断，挂起照常落盘、照常报中断（调用方超时导致的退出也要
// 存下档），而且只写一次。曾经写档用调用方的 ctx，到点就按档没写成处理；后来改成到点后用宽限再写一次。
func TestExitCheckpoint_DeadlineDuringSaveNotCut(t *testing.T) {
	cp := &delayedSaveCP{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer(), delay: 150 * time.Millisecond}
	b := compose_v2.NewBuilder().WithName("np_deadline_not_cut")
	b.AddNode("ask", npAsk("批准吗"))
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	const sid = "np_deadline_not_cut"
	err := compose_v2.Run(ctx, npCompile(t, b), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid))
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("写档不该被调用方的截止时间截断，应照常报中断：%v", err)
	}
	if ok, _ := cp.ExistsNS(context.Background(), sid, ""); !ok {
		t.Fatal("挂起档应已写成")
	}
	if n := cp.saves.Load(); n != 1 {
		t.Fatalf("出口写档应只发一次，实际 %d 次", n)
	}
}

// 开了陈旧写回守卫、写档途中调用方到点、后端其实已经写进去了：照常报中断，不报 ErrCheckpointStale。曾经到点
// 之后用宽限再写一次，第二次的陈旧写回检查看见刚写进去的那份，把自己写的档当成别人写的（审计实测）。
func TestExitCheckpoint_DeadlineDuringSaveWritesOnce(t *testing.T) {
	cp := &delayedSaveCP{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer(), delay: 150 * time.Millisecond, lateCommit: true}
	b := compose_v2.NewBuilder().WithName("np_deadline_once")
	b.AddNode("ask", npAsk("批准吗"))
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	const sid = "np_deadline_once"
	err := compose_v2.Run(ctx, npCompile(t, b), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid), compose_v2.WithStaleWriteGuard())
	if errors.Is(err, compose_v2.ErrCheckpointStale) {
		t.Fatalf("自己写的档不该被当成别人写的：%v", err)
	}
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("应照常报中断：%v", err)
	}
	if n := cp.saves.Load(); n != 1 {
		t.Fatalf("出口写档应只发一次，实际 %d 次", n)
	}
}

// cpEventPanicHooks 在 op 那一类 cp 事件上 panic。
type cpEventPanicHooks struct {
	compose_v2.NoOpOrchestrationHooks
	op compose_v2.CpOperationType
}

func (h cpEventPanicHooks) OnNodeEvent(ctx context.Context, _ compose_v2.NodeID, ev compose_v2.Event) context.Context {
	if ev.Type == compose_v2.CpEventType && ev.Content == string(h.op) {
		panic("cp event hook bug")
	}
	return ctx
}

// 出口写档成功之后，cp.save 事件的钩子 panic：档写成了就是写成了——照常报中断（钩子的 panic 当场兜住、不进
// 错误），档留着、续得上。曾经钩子在兜存储 panic 的那段里调，它的 panic 被当成写档失败，刚写好的档被清掉（审计
// 实测）。
func TestExitCheckpoint_HookPanicAfterSaveKeepsCheckpoint(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	b := compose_v2.NewBuilder().WithName("np_hook_panic_save")
	b.AddNode("ask1", npAsk("批准吗"))
	b.AddNode("ask2", npAsk("再确认一次"))
	b.AddEdge(compose_v2.StartNode, "ask1")
	b.AddEdge("ask1", "ask2")
	b.AddEdge("ask2", compose_v2.EndNode)
	g := npCompile(t, b)
	const sid = "np_hook_panic_save"
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid),
		compose_v2.WithEmitCpEvent(true), compose_v2.WithHooks(cpEventPanicHooks{op: compose_v2.CpOperationSave}))
	if errors.Is(err, compose_v2.ErrCheckpointNotPersisted) {
		t.Fatalf("档写成了，钩子 panic 不该改成档没写成：%v", err)
	}
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("应照常报中断：%v", err)
	}
	var pe *compose_v2.PanicError
	if errors.As(err, &pe) {
		t.Errorf("cp 事件钩子的 panic 不该进错误：%v", err)
	}
	err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("批准")}))
	ie2, isInt := compose_v2.IsInterrupt(err)
	if !isInt || ie2.AllQueries()[0].Question != "再确认一次" {
		t.Fatalf("档应续得上、走到第二问：%v", err)
	}
}

// 档没写成、出口清旧档之后 cp.cleanup 事件的钩子 panic：结论照旧是档没写成（不报中断），旧档已删，钩子的
// panic 当场兜住、不进错误。结论要在调钩子之前定下——钩子 panic 时 runErr 还是原来的中断，前端就会请用户回答
// 一个续不上的问题。
func TestNotPersisted_HookPanicDuringPurgeKeepsVerdict(t *testing.T) {
	cp := newNotPersistedCP()
	b := compose_v2.NewBuilder().WithName("np_hook_panic_purge")
	b.AddNode("ask1", npAsk("批准吗"))
	b.AddNode("ask2", npAsk("再确认一次"))
	b.AddEdge(compose_v2.StartNode, "ask1")
	b.AddEdge("ask1", "ask2")
	b.AddEdge("ask2", compose_v2.EndNode)
	g := npCompile(t, b)
	const sid = "np_hook_panic_purge"
	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)))
	if !ok {
		t.Fatal("前置：第一段应停在 ask1")
	}
	cp.failSave.Store(true)
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid),
		compose_v2.WithEmitCpEvent(true), compose_v2.WithHooks(cpEventPanicHooks{op: compose_v2.CpOperationCleanup}),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("批准")}))
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Fatalf("档没写成，钩子 panic 也不该让它变回中断：%v", err)
	}
	if !errors.Is(err, compose_v2.ErrCheckpointNotPersisted) || !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("应返 ErrCheckpointNotPersisted + ErrRunAborted：%v", err)
	}
	var pe *compose_v2.PanicError
	if errors.As(err, &pe) {
		t.Errorf("cp 事件钩子的 panic 不该进错误：%v", err)
	}
	if ok, _ := cp.ExistsNS(context.Background(), sid, ""); ok {
		t.Fatal("回答之前那份档应已清掉")
	}
}

// 成功收尾、清档之后 cp.cleanup 事件的钩子 panic：Run 照常成功。档已经清掉，让 Run 因此报错只会引来从头重跑、
// 副作用重放（曾经追加到错误上）。
func TestExitCheckpoint_HookPanicAfterSuccessPurgeKeepsSuccess(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	b := compose_v2.NewBuilder().WithName("np_hook_panic_success")
	b.AddNode("ask1", npAsk("批准吗"))
	b.AddEdge(compose_v2.StartNode, "ask1")
	b.AddEdge("ask1", compose_v2.EndNode)
	g := npCompile(t, b)
	const sid = "np_hook_panic_success"
	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)))
	if !ok {
		t.Fatal("前置：第一段应停在 ask1")
	}
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid),
		compose_v2.WithEmitCpEvent(true), compose_v2.WithHooks(cpEventPanicHooks{op: compose_v2.CpOperationCleanup}),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("批准")}))
	if err != nil {
		t.Fatalf("成功收尾时 cp 事件钩子 panic 不该让 Run 报错：%v", err)
	}
	if ok, _ := cp.ExistsNS(context.Background(), sid, ""); ok {
		t.Fatal("档应已清掉")
	}
}

// panicDeleteCP 的 DeleteSubtree 在 panicDelete 置位时 panic。
type panicDeleteCP struct {
	*notPersistedCP
	panicDelete atomic.Bool
}

func (c *panicDeleteCP) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	if c.panicDelete.Load() {
		panic("backend delete bug")
	}
	return c.notPersistedCP.DeleteSubtree(ctx, sid, ns)
}

// 终局失败出口清档时后端 panic：清档失败写进错误，兜住的 panic 照样 errors.As 取得到。曾经清档失败只以文本
// 进错误（%v），panic 的记录（带栈）丢了。
func TestAbortPurge_BackendPanicAttached(t *testing.T) {
	cp := &panicDeleteCP{notPersistedCP: newNotPersistedCP()}
	cp.panicDelete.Store(true)
	b := compose_v2.NewBuilder().WithName("np_abort_purge_panic")
	b.AddNode("bad", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{InterruptID: "self-filled", Question: "?"})
		return nil, err
	})
	b.AddEdge(compose_v2.StartNode, "bad")
	b.AddEdge("bad", compose_v2.EndNode)
	err := compose_v2.Run(context.Background(), npCompile(t, b), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("np_abort_purge_panic"))
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("前置：自填 InterruptID 应终局失败：%v", err)
	}
	var pe *compose_v2.PanicError
	if !errors.As(err, &pe) {
		t.Errorf("清档时后端的 panic 应挂在错误上：%v", err)
	}
}

// 档没写成、清旧档时后端 panic：同上，panic 的记录挂在错误上。
func TestNotPersisted_PurgeBackendPanicAttached(t *testing.T) {
	cp := &panicDeleteCP{notPersistedCP: newNotPersistedCP()}
	b := compose_v2.NewBuilder().WithName("np_purge_panic")
	b.AddNode("ask1", npAsk("批准吗"))
	b.AddNode("ask2", npAsk("再确认一次"))
	b.AddEdge(compose_v2.StartNode, "ask1")
	b.AddEdge("ask1", "ask2")
	b.AddEdge("ask2", compose_v2.EndNode)
	g := npCompile(t, b)
	const sid = "np_purge_panic"
	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)))
	if !ok {
		t.Fatal("前置：第一段应停在 ask1")
	}
	cp.failSave.Store(true)
	cp.panicDelete.Store(true)
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("批准")}))
	if !errors.Is(err, compose_v2.ErrCheckpointNotPersisted) {
		t.Fatalf("应返 ErrCheckpointNotPersisted：%v", err)
	}
	var pe *compose_v2.PanicError
	if !errors.As(err, &pe) {
		t.Errorf("清档时后端的 panic 应挂在错误上：%v", err)
	}
}

// panicSaveCP 的 SaveNS 在 panicSave 置位时 panic。
type panicSaveCP struct {
	*compose_v2.InMemoryCheckpointer
	panicSave atomic.Bool
}

func (c *panicSaveCP) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	if c.panicSave.Load() {
		panic("backend driver bug")
	}
	return c.InMemoryCheckpointer.SaveNS(ctx, sid, ns, cp)
}

// 自研后端在出口写档时 panic：跟写档失败同样处置——不报告成中断，回答之前那份旧档清掉。曾经 panic 冒到 Run
// 最外层的 recover、跟中断并列，中断还在错误链上，前端照常请用户回答，答完又问一遍已经答过的问题（审计实测）。
func TestNotPersisted_SavePanicIsNotPersisted(t *testing.T) {
	cp := &panicSaveCP{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
	b := compose_v2.NewBuilder().WithName("np_panic")
	b.AddNode("ask1", npAsk("批准吗"))
	b.AddNode("ask2", npAsk("再确认一次"))
	b.AddEdge(compose_v2.StartNode, "ask1")
	b.AddEdge("ask1", "ask2")
	b.AddEdge("ask2", compose_v2.EndNode)
	g := npCompile(t, b)
	const sid = "np_panic"
	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)))
	if !ok {
		t.Fatal("前置：第一段应停在 ask1")
	}
	cp.panicSave.Store(true)
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("批准")}))
	cp.panicSave.Store(false)
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Fatalf("写档 panic、档没写成，不该报告成中断：%v", err)
	}
	if !errors.Is(err, compose_v2.ErrCheckpointNotPersisted) || !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("应返 ErrCheckpointNotPersisted + ErrRunAborted：%v", err)
	}
	var pe *compose_v2.PanicError
	if !errors.As(err, &pe) {
		t.Errorf("错误上应挂着 PanicError（带栈）：%v", err)
	}
	if ok, _ := cp.ExistsNS(context.Background(), sid, ""); ok {
		t.Fatal("回答之前那份档应已清掉")
	}
}

// 成功收尾时陈旧写回检查拦住了清档、cp.cleanup 事件的钩子 panic：结论照旧是 ErrCheckpointStale（钩子的 panic
// 当场兜住、不进错误），别人的档还在。结论要在调钩子之前定下——钩子 panic 时 runErr 还是 nil 的话，调用方只看到一个
// panic，不知道这次执行建立在过时的状态上。
func TestStaleWriteGuard_HookPanicKeepsStaleVerdict(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	var hijacked atomic.Bool
	const sid = "stale_hook_panic"
	build := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("stale_hook_panic")
		b.AddNode("ask", npAsk("批准吗"))
		b.AddNode("work", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if hijacked.CompareAndSwap(false, true) {
				hijackSlot(t, cper, sid, "")
			}
			return nil, nil
		})
		b.AddEdge(compose_v2.StartNode, "ask")
		b.AddEdge("ask", "work")
		b.AddEdge("work", compose_v2.EndNode)
		return npCompile(t, b)
	}
	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(context.Background(), build(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid), compose_v2.WithStaleWriteGuard()))
	if !ok {
		t.Fatal("前置：第一段应中断")
	}
	err := compose_v2.Run(context.Background(), build(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid), compose_v2.WithStaleWriteGuard(),
		compose_v2.WithEmitCpEvent(true), compose_v2.WithHooks(cpEventPanicHooks{op: compose_v2.CpOperationCleanup}),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("批准")}))
	if !errors.Is(err, compose_v2.ErrCheckpointStale) {
		t.Fatalf("钩子 panic 不该盖掉陈旧写回的结论：%v", err)
	}
	var pe *compose_v2.PanicError
	if errors.As(err, &pe) {
		t.Errorf("cp 事件钩子的 panic 不该进错误：%v", err)
	}
	if ok, _ := cper.ExistsNS(context.Background(), sid, ""); !ok {
		t.Fatal("别人的档不该被删")
	}
}

// cp 事件只作观测：中途写档（sync / async）、入口载入、节点里经 EmitCpEvent 发的，钩子 panic 都不让 Run 失败。曾经
// sync 中途写档时走成图主体 panic、async 下被当成写档失败、入口 cp.load 让续跑直接失败、节点里发的让节点失败（审计
// 实测）。
func TestCpEvent_HookPanicNeverFailsRun(t *testing.T) {
	twoSteps := func(name string) *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName(name)
		b.AddNode("a", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
		b.AddNode("b", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
		b.AddEdge(compose_v2.StartNode, "a")
		b.AddEdge("a", "b")
		b.AddEdge("b", compose_v2.EndNode)
		return npCompile(t, b)
	}
	for _, d := range []compose_v2.Durability{compose_v2.DurabilitySync, compose_v2.DurabilityAsync} {
		t.Run("save_"+string(d), func(t *testing.T) {
			sid := "cp_hook_panic_" + string(d)
			err := compose_v2.Run(context.Background(), twoSteps(sid), compose_v2.NewChannelManager(),
				compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()), compose_v2.WithSessionID(sid),
				compose_v2.WithDurability(d), compose_v2.WithEmitCpEvent(true),
				compose_v2.WithHooks(cpEventPanicHooks{op: compose_v2.CpOperationSave}))
			if err != nil {
				t.Fatalf("中途写档的 cp.save 钩子 panic 不该让 Run 失败：%v", err)
			}
		})
	}
	t.Run("load", func(t *testing.T) {
		cp := compose_v2.NewInMemoryCheckpointer()
		b := compose_v2.NewBuilder().WithName("cp_hook_panic_load")
		b.AddNode("ask1", npAsk("批准吗"))
		b.AddEdge(compose_v2.StartNode, "ask1")
		b.AddEdge("ask1", compose_v2.EndNode)
		g := npCompile(t, b)
		const sid = "cp_hook_panic_load"
		ie, ok := compose_v2.IsInterrupt(compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)))
		if !ok {
			t.Fatal("前置：第一段应停在 ask1")
		}
		err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid),
			compose_v2.WithEmitCpEvent(true), compose_v2.WithHooks(cpEventPanicHooks{op: compose_v2.CpOperationLoad}),
			compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("批准")}))
		if err != nil {
			t.Fatalf("入口 cp.load 钩子 panic 不该让续跑失败：%v", err)
		}
	})
	t.Run("emit_in_node", func(t *testing.T) {
		b := compose_v2.NewBuilder().WithName("cp_hook_panic_emit")
		b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			compose_v2.EmitCpEvent(compose_v2.WithEmitCpEventCtx(ctx, true),
				&compose_v2.CpAction{Type: compose_v2.CpOperationSave, Save: &compose_v2.CpSaveAction{Ok: true}})
			return nil, nil
		})
		b.AddEdge(compose_v2.StartNode, "n")
		b.AddEdge("n", compose_v2.EndNode)
		err := compose_v2.Run(context.Background(), npCompile(t, b), compose_v2.NewChannelManager(),
			compose_v2.WithHooks(cpEventPanicHooks{op: compose_v2.CpOperationSave}))
		if err != nil {
			t.Fatalf("节点里经 EmitCpEvent 发的 cp 事件钩子 panic 不该让节点失败：%v", err)
		}
	})
}
