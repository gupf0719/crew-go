package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// 出口要写的档没写成时（ErrCheckpointNotPersisted）：不能再报告成可以续，槽里不留能被续进去的旧档。
// 见 compose_v2.ErrCheckpointNotPersisted。

var errBackendDown = errors.New("backend down")

// notPersistedCP 包一层内存存档。failSave 置位时 SaveNS 报 errBackendDown（failNS 非空时只对那个 ns）；
// conflictNS 上的写一律报 ErrGraphIDConflict；honorCtx 为真时 ctx 已结束就直接报 ctx 错，跟持久后端一样。
type notPersistedCP struct {
	*compose_v2.InMemoryCheckpointer
	failSave   atomic.Bool
	failNS     string
	conflictNS string
	honorCtx   bool
}

func newNotPersistedCP() *notPersistedCP {
	return &notPersistedCP{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
}

func (c *notPersistedCP) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	if c.honorCtx && ctx.Err() != nil {
		return ctx.Err()
	}
	if c.conflictNS != "" && ns == c.conflictNS {
		return fmt.Errorf("%w: sid=%q ns=%q", compose_v2.ErrGraphIDConflict, sid, ns)
	}
	if c.failSave.Load() && (c.failNS == "" || c.failNS == ns) {
		return errBackendDown
	}
	return c.InMemoryCheckpointer.SaveNS(ctx, sid, ns, cp)
}

func (c *notPersistedCP) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	if c.honorCtx && ctx.Err() != nil {
		return 0, ctx.Err()
	}
	return c.InMemoryCheckpointer.DeleteSubtree(ctx, sid, ns)
}

func npCompile(t *testing.T, b *compose_v2.GraphBuilder) *compose_v2.Graph {
	t.Helper()
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

func npAsk(question string) compose_v2.NodeFunc {
	return func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: question})
		return nil, err
	}
}

// 续跑后第二次挂起时写档失败：调用方不再看到中断（请用户回答一个续不上的问题），槽里回答之前那份档
// 也清掉——不清的话再续一次，续进的是回答之前那份，已经答过的问题重问、答完副作用重做。
func TestNotPersisted_InterruptExitNotReportedAsInterrupt(t *testing.T) {
	cp := newNotPersistedCP()
	var work atomic.Int32
	b := compose_v2.NewBuilder().WithName("np_interrupt")
	b.AddNode("ask1", npAsk("批准吗"))
	b.AddNode("work", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		work.Add(1)
		return nil, nil
	})
	b.AddNode("ask2", npAsk("再确认一次"))
	b.AddEdge(compose_v2.StartNode, "ask1")
	b.AddEdge("ask1", "work")
	b.AddEdge("work", "ask2")
	b.AddEdge("ask2", compose_v2.EndNode)
	g := npCompile(t, b)
	const sid = "np_interrupt"
	run := func(opts ...compose_v2.RunOption) error {
		all := append([]compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)}, opts...)
		return compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), all...)
	}

	ie, ok := compose_v2.IsInterrupt(run())
	if !ok {
		t.Fatal("前置：第一段应停在 ask1")
	}
	answers := compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("批准")})

	cp.failSave.Store(true)
	err := run(answers)
	cp.failSave.Store(false)
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Fatalf("档没写成，不该报告成中断：%v", err)
	}
	if !errors.Is(err, compose_v2.ErrCheckpointNotPersisted) || !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("应返 ErrCheckpointNotPersisted + ErrRunAborted：%v", err)
	}
	if !errors.Is(err, errBackendDown) {
		t.Errorf("写档失败的原因应留在错误链上：%v", err)
	}
	if compose_v2.IsRetryable(err) {
		t.Errorf("不该自动重试（重跑会把这次做过的副作用再做一遍）：%v", err)
	}
	if !compose_v2.MustPropagate(context.Background(), err) {
		t.Errorf("各吞错点不该吞掉它：%v", err)
	}
	if ok, _ := cp.ExistsNS(context.Background(), sid, ""); ok {
		t.Fatal("槽里回答之前那份档应已清掉")
	}
	if work.Load() != 1 {
		t.Fatalf("前置：work 应跑过 1 次，实际 %d", work.Load())
	}

	// 拿着原来的答案再续一次：没有档可续，明确报 ErrNoPendingCheckpoint，work 不重做。
	if err := run(answers); !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
		t.Fatalf("没有档可续，带着答案续跑应报 ErrNoPendingCheckpoint：%v", err)
	}
	if n := work.Load(); n != 1 {
		t.Errorf("续进了回答之前那份档，work 被重做了（共 %d 次）", n)
	}
}

var errBizFailure = errors.New("business failure")

// 业务报错退出、写「失败现场」档也失败：同样终局，业务错留在错误链上，回答之前那份旧档清掉。
func TestNotPersisted_FailedExitKeepsBusinessError(t *testing.T) {
	cp := newNotPersistedCP()
	var effects atomic.Int32
	b := compose_v2.NewBuilder().WithName("np_failed")
	b.AddNode("ask", npAsk("批准吗"))
	b.AddNode("charge", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if effects.Add(1) == 1 {
			return nil, errBizFailure
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", "charge")
	b.AddEdge("charge", compose_v2.EndNode)
	g := npCompile(t, b)
	const sid = "np_failed"
	run := func(opts ...compose_v2.RunOption) error {
		all := append([]compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)}, opts...)
		return compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), all...)
	}
	ie, ok := compose_v2.IsInterrupt(run())
	if !ok {
		t.Fatal("前置：第一段应停在提问")
	}
	answers := compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("批准")})

	cp.failSave.Store(true)
	err := run(answers)
	cp.failSave.Store(false)
	if !errors.Is(err, compose_v2.ErrCheckpointNotPersisted) || !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("应返 ErrCheckpointNotPersisted + ErrRunAborted：%v", err)
	}
	if !errors.Is(err, errBizFailure) {
		t.Errorf("业务错应留在错误链上：%v", err)
	}
	if ok, _ := cp.ExistsNS(context.Background(), sid, ""); ok {
		t.Fatal("槽里回答之前那份档应已清掉")
	}
	// 按「失败档可以续跑重试」再续一次：没有档可续，明确报 ErrNoPendingCheckpoint，charge 不重做。
	if err := run(answers); !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
		t.Fatalf("没有档可续，带着答案续跑应报 ErrNoPendingCheckpoint：%v", err)
	}
	if n := effects.Load(); n != 1 {
		t.Errorf("续进了回答之前那份档，副作用重做了（共 %d 次）", n)
	}
}

// 软取消退出、写档失败：调用方不再看到 ErrSoftCanceled（它在说「之后可以续」）。
func TestNotPersisted_SoftCancelExit(t *testing.T) {
	cp := newNotPersistedCP()
	cp.failSave.Store(true)
	sig := compose_v2.NewSoftCancelSignal()
	b := compose_v2.NewBuilder().WithName("np_soft")
	b.AddNode("n1", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sig.Trigger("用户点了停")
		return nil, nil
	})
	b.AddNode("n2", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	b.AddEdge(compose_v2.StartNode, "n1")
	b.AddEdge("n1", "n2")
	b.AddEdge("n2", compose_v2.EndNode)
	err := compose_v2.Run(context.Background(), npCompile(t, b), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("np_soft"), compose_v2.WithSoftCancel(sig))
	if errors.Is(err, compose_v2.ErrSoftCanceled) {
		t.Fatalf("档没写成，不该再报告成软取消（调用方会以为之后能续）：%v", err)
	}
	if !errors.Is(err, compose_v2.ErrCheckpointNotPersisted) {
		t.Fatalf("应返 ErrCheckpointNotPersisted：%v", err)
	}
}

// 调用方 ctx 取消导致的退出：出口写档不跟着取消，持久后端（ctx 已结束就报错的那种）上也存得下，
// 之后用新 ctx 续得上。曾经用的是已经取消的 ctx，持久后端一律写不进去（SQLite 实测）。
func TestExitCheckpoint_CallerCancelStillPersisted(t *testing.T) {
	cp := newNotPersistedCP()
	cp.honorCtx = true
	var block atomic.Bool
	block.Store(true)
	started := make(chan struct{}, 1)
	var runs atomic.Int32
	b := compose_v2.NewBuilder().WithName("np_cancel")
	b.AddNode("a", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		runs.Add(1)
		if !block.Load() {
			return nil, nil
		}
		started <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddEdge("a", compose_v2.EndNode)
	g := npCompile(t, b)
	const sid = "np_cancel"
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()
	err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid))
	if !errors.Is(err, context.Canceled) || errors.Is(err, compose_v2.ErrCheckpointNotPersisted) {
		t.Fatalf("调用方取消导致的退出应照常报 ctx 错、档已写成：%v", err)
	}
	if ok, _ := cp.ExistsNS(context.Background(), sid, ""); !ok {
		t.Fatal("调用方取消之后档应已写成（出口写档不该跟着取消）")
	}
	block.Store(false)
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)); err != nil {
		t.Fatalf("新 ctx 续跑：%v", err)
	}
	if n := runs.Load(); n != 2 {
		t.Errorf("续跑应把被取消的 a 再跑一次（共 2 次），实际 %d", n)
	}
	if ok, _ := cp.ExistsNS(context.Background(), sid, ""); ok {
		t.Error("续跑成功之后档应已清掉")
	}
}

// 同一超步：一个分支里的嵌套执行出口写档撞上身份冲突（槽属于别人，不带 ErrRunAborted），另一个分支
// 正常提问。仲裁要报档没写成，不能报兄弟的中断——用户答完续跑，那个分支也续不上。
func TestNotPersisted_DominatesSiblingInterrupt(t *testing.T) {
	cp := newNotPersistedCP()
	cp.conflictNS = "child"
	cb := compose_v2.NewBuilder().WithName("np_child")
	cb.AddNode("ask", npAsk("子图的提问"))
	cb.AddEdge(compose_v2.StartNode, "ask")
	cb.AddEdge("ask", compose_v2.EndNode)
	child := npCompile(t, cb)

	b := compose_v2.NewBuilder().WithName("np_fanout")
	b.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, compose_v2.Run(ctx, child, compose_v2.NewChannelManager(), compose_v2.WithRunNS("child"))
	})
	b.AddNode("sib", npAsk("兄弟的提问"))
	b.AddEdge(compose_v2.StartNode, "host")
	b.AddEdge(compose_v2.StartNode, "sib")
	b.AddEdge("host", compose_v2.EndNode)
	b.AddEdge("sib", compose_v2.EndNode)
	err := compose_v2.Run(context.Background(), npCompile(t, b), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("np_fanout"))
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Fatalf("有分支的档没写成，不该报兄弟的中断（用户答完续跑那个分支也续不上）：%v", err)
	}
	if !errors.Is(err, compose_v2.ErrCheckpointNotPersisted) || !errors.Is(err, compose_v2.ErrGraphIDConflict) {
		t.Fatalf("应报那个分支的 ErrCheckpointNotPersisted（带着身份冲突）：%v", err)
	}
}
