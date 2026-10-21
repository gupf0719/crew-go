package compose_v2_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// 收件人密钥一个任务一把（见 signal.go「收件人密钥」一节）。场景都是同一个形状：写入者跑在嵌套图里，
// 把发给消费方的决定落进自己的存档；消费方那张图自己的存档没写成（进程崩溃 / 出口那次写档失败）。
// 曾经密钥每张图各铸各的、随本图下一次存档才落盘：续跑时消费方重铸、收件人对不上，子层已落盘的决定
// 静默丢失——三个耐久档都中，`err == nil`（审计实测）。

var keyDurKind = compose_v2.NewSignalKind("compose_v2_test.key_durability")

type keyDurDestKey struct{}

// keyDurCP 包一层 in-memory checkpointer：failNS 上的第一次写失败（后端抖一下）；frozen 之后一切写 /
// 删都当没发生（进程死了，盘上停在那一刻）。
type keyDurCP struct {
	*compose_v2.InMemoryCheckpointer
	failNS string
	failed atomic.Bool
	frozen atomic.Bool
	mu     sync.Mutex
	saved  []string
}

func newKeyDurCP(failNS string) *keyDurCP {
	return &keyDurCP{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer(), failNS: failNS}
}

func (c *keyDurCP) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	if c.frozen.Load() {
		return nil
	}
	if c.failNS != "" && ns == c.failNS && c.failed.CompareAndSwap(false, true) {
		return errors.New("后端瞬时错误")
	}
	c.mu.Lock()
	c.saved = append(c.saved, ns)
	c.mu.Unlock()
	return c.InMemoryCheckpointer.SaveNS(ctx, sid, ns, cp)
}

func (c *keyDurCP) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	if c.frozen.Load() {
		return 0, nil
	}
	return c.InMemoryCheckpointer.DeleteSubtree(ctx, sid, ns)
}

func (c *keyDurCP) savedNS(ns string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.saved {
		if s == ns {
			return true
		}
	}
	return false
}

// keyDurFixture 搭三层：顶层 [n0 → host]，host 跑消费方图 cg（ns "cg"，第一个超步就是 step），step 铸收件人、
// 跑写入者图（ns "cg/inner"）：w 把决定发给收件人，然后 tail 节点按 tail 函数收尾（问人 / 模拟崩溃）。
type keyDurFixture struct {
	g     *compose_v2.Graph
	got   []string
	wRuns atomic.Int32
	mu    sync.Mutex
}

func newKeyDurFixture(t *testing.T, name string, tail compose_v2.NodeFunc) *keyDurFixture {
	t.Helper()
	f := &keyDurFixture{}
	wb := compose_v2.NewBuilder().WithName(name + "_inner")
	wb.AddNode("w", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		f.wRuns.Add(1)
		dest, _ := ctx.Value(keyDurDestKey{}).(string)
		compose_v2.EmitSignal(ctx, keyDurKind, compose_v2.Signal{Dest: dest, Value: "billing"})
		return nil, nil
	})
	wb.AddNode("tail", tail)
	wb.AddEdge(compose_v2.StartNode, "w")
	wb.AddEdge("w", "tail")
	wb.AddEdge("tail", compose_v2.EndNode)
	inner := rsCompile(t, wb)

	cb := compose_v2.NewBuilder().WithName(name + "_consumer")
	cb.AddNode("step", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		dest := compose_v2.SignalRecipient(ctx, "x")
		sigs, err := compose_v2.CaptureSignals(ctx, func(c context.Context) error {
			return compose_v2.Run(context.WithValue(c, keyDurDestKey{}, dest), inner, compose_v2.NewChannelManager(),
				compose_v2.WithRunNS(compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), "inner")))
		})
		if err != nil {
			return nil, err
		}
		s, ok, _ := sigs.Take(keyDurKind, dest)
		f.mu.Lock()
		if ok {
			f.got = append(f.got, "took:"+s.Value)
		} else {
			f.got = append(f.got, "miss")
		}
		f.mu.Unlock()
		return nil, nil
	})
	cb.AddEdge(compose_v2.StartNode, "step")
	cb.AddEdge("step", compose_v2.EndNode)
	consumer := rsCompile(t, cb)

	b := compose_v2.NewBuilder().WithName(name)
	b.AddNode("n0", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	b.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, compose_v2.Run(ctx, consumer, compose_v2.NewChannelManager(), compose_v2.WithRunNS("cg"))
	})
	b.AddEdge(compose_v2.StartNode, "n0")
	b.AddEdge("n0", "host")
	b.AddEdge("host", compose_v2.EndNode)
	f.g = rsCompile(t, b)
	return f
}

func (f *keyDurFixture) last() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.got) == 0 {
		return ""
	}
	return f.got[len(f.got)-1]
}

func askTail(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
	_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认？"})
	return nil, err
}

// TestSignalKey_ConsumerSaveFailsOnce：exit 档，写入者那张图问人挂起、存档写成了；消费方那张图（cg）的
// 出口存档撞上一次瞬时写失败。档没写成就不能报告成可以续：这个任务终局失败、整个会话的存档清掉（不然
// 续跑时消费方重铸密钥、收件人对不上，子层已落盘的决定静默丢失）。同 sid 新开之后写入者从头再跑、
// 再问一次，答完决定照常送到。
func TestSignalKey_ConsumerSaveFailsOnce(t *testing.T) {
	f := newKeyDurFixture(t, "keydur_savefail", askTail)
	cp := newKeyDurCP("cg")
	const sid = "keydur_savefail"
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)}
	err := compose_v2.Run(context.Background(), f.g, compose_v2.NewChannelManager(), opts...)
	if !cp.failed.Load() || !cp.savedNS("cg/inner") {
		t.Fatalf("前置：消费方的档写失败一次、写入者的档写成了（failed=%v saved=%v）", cp.failed.Load(), cp.saved)
	}
	if _, isInt := compose_v2.IsInterrupt(err); isInt || !errors.Is(err, compose_v2.ErrCheckpointNotPersisted) ||
		!errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("消费方的档没写成：应返 ErrCheckpointNotPersisted + ErrRunAborted、不报告成中断，实际 %v", err)
	}
	for _, ns := range []string{"", "cg", "cg/inner"} {
		if ok, _ := cp.ExistsNS(context.Background(), sid, ns); ok {
			t.Errorf("任务终局失败，ns %q 的存档应已清掉", ns)
		}
	}
	err = compose_v2.Run(context.Background(), f.g, compose_v2.NewChannelManager(), opts...)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("同 sid 新开后写入者应从头再跑、再问一次：%v", err)
	}
	answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("是")}
	if err := compose_v2.Run(context.Background(), f.g, compose_v2.NewChannelManager(),
		append(opts, compose_v2.WithResumeAnswers(answers))...); err != nil {
		t.Fatalf("答完续跑：%v", err)
	}
	if got := f.last(); got != "took:billing" {
		t.Errorf("新开后重新做出的决定没送到：%v", f.got)
	}
	if n := f.wRuns.Load(); n != 2 {
		t.Errorf("写入者应在新开的任务里从头再跑一次（共 2 次），实际 %d 次", n)
	}
}

// TestSignalKey_CrashAfterWriterSaved：sync / async 档，写入者把决定落进自己的存档之后、消费方那张图
// 一次存档都还没写，进程崩溃。重启同 sid 续跑——决定必须送到。
func TestSignalKey_CrashAfterWriterSaved(t *testing.T) {
	for _, d := range []compose_v2.Durability{compose_v2.DurabilitySync, compose_v2.DurabilityAsync} {
		t.Run(string(d), func(t *testing.T) {
			cp := newKeyDurCP("")
			var crash atomic.Bool
			crash.Store(true)
			crashTail := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				if !crash.Load() {
					return nil, nil
				}
				// 等写入者那一超步的存档真的落盘（async 档是后台写）再死
				for i := 0; i < 400 && !cp.savedNS(compose_v2.GetGraphNS(ctx)); i++ {
					time.Sleep(5 * time.Millisecond)
				}
				cp.frozen.Store(true)
				return nil, errors.New("进程在这里死了")
			}
			f := newKeyDurFixture(t, "keydur_crash_"+string(d), crashTail)
			opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("keydur_crash"),
				compose_v2.WithDurability(d)}
			if err := compose_v2.Run(context.Background(), f.g, compose_v2.NewChannelManager(), opts...); err == nil {
				t.Fatal("第一次应死在 tail 上")
			}
			if !cp.savedNS("cg/inner") || cp.savedNS("cg") {
				t.Fatalf("前置：写入者的档落了盘、消费方的一份都没有，实际 %v", cp.saved)
			}
			crash.Store(false)
			cp.frozen.Store(false)
			if err := compose_v2.Run(context.Background(), f.g, compose_v2.NewChannelManager(), opts...); err != nil {
				t.Fatalf("重启续跑：%v", err)
			}
			if got := f.last(); got != "took:billing" {
				t.Errorf("崩溃前写入者已落盘的决定续跑后没送到：%v", f.got)
			}
			if n := f.wRuns.Load(); n != 1 {
				t.Errorf("写入者那一步已落盘，续跑不该重做，实际跑了 %d 次", n)
			}
		})
	}
}

// TestSignalKey_TopLevelRestart_StaleChildNotResurrected：顶层自己的档没写成（exit 档出口写失败），出口
// 尽力清档也失败了一次——子层那些旧存档留在盘上。只能新开时任务代号换了，它们过不了续跑闸门：写入者
// 从头再跑、决定重新做出，而不是被复活后发给一个已经不存在的收件人。
func TestSignalKey_TopLevelRestart_StaleChildNotResurrected(t *testing.T) {
	f := newKeyDurFixture(t, "keydur_toprestart", askTail)
	cp := newKeyDurCP("")
	top := &topFailOnceCP{keyDurCP: cp} // 只让顶层（ns ""）那一次写、那一次清档失败
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(top), compose_v2.WithSessionID("keydur_toprestart")}
	err := compose_v2.Run(context.Background(), f.g, compose_v2.NewChannelManager(), opts...)
	if _, isInt := compose_v2.IsInterrupt(err); isInt || !errors.Is(err, compose_v2.ErrCheckpointNotPersisted) {
		t.Fatalf("顶层的档没写成：应返 ErrCheckpointNotPersisted、不报告成中断，实际 %v", err)
	}
	if !top.failedTop.Load() || !top.failedDeleteTop.Load() || !cp.savedNS("cg/inner") {
		t.Fatalf("前置：顶层的档写失败、清档也失败，子层的档写成了（saved=%v）", cp.saved)
	}
	// 顶层没有档：同 sid 再跑就是新开（新任务代号）
	err = compose_v2.Run(context.Background(), f.g, compose_v2.NewChannelManager(), opts...)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("新开后写入者应从头再跑、再问一次：%v", err)
	}
	answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("是")}
	if err := compose_v2.Run(context.Background(), f.g, compose_v2.NewChannelManager(),
		append(opts, compose_v2.WithResumeAnswers(answers))...); err != nil {
		t.Fatalf("答完续跑：%v", err)
	}
	if got := f.last(); got != "took:billing" {
		t.Errorf("新开后重新做出的决定没送到：%v", f.got)
	}
	if n := f.wRuns.Load(); n != 2 {
		t.Errorf("顶层新开时子层旧档不该被复活，写入者应从头再跑一次（共 2 次），实际 %d 次", n)
	}
}

// topFailOnceCP 让顶层（ns ""）的第一次写、第一次清档失败。
type topFailOnceCP struct {
	*keyDurCP
	failedTop       atomic.Bool
	failedDeleteTop atomic.Bool
}

func (c *topFailOnceCP) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	if ns == "" && c.failedTop.CompareAndSwap(false, true) {
		return errors.New("后端瞬时错误")
	}
	return c.keyDurCP.SaveNS(ctx, sid, ns, cp)
}

func (c *topFailOnceCP) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	if ns == "" && c.failedDeleteTop.CompareAndSwap(false, true) {
		return 0, errors.New("后端瞬时错误")
	}
	return c.keyDurCP.DeleteSubtree(ctx, sid, ns)
}
