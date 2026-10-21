package compose_v2_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestSendDirectives_GoexitLeavesSameStateAsPanic 守「SendDirective payload 的提交点
// 被掐断时，存档与 panic 路径逐字段相同」。
//
// # 它防的是什么
//
// `ApplyAll(dirWrites)` 的两句补偿（frontier + payload）原本挂在 `err != nil` 分支上，
// 而 `runtime.Goexit` 既不返回也不产生 err ⟹ 补偿跑不到，而 `completedSiblings`
// 早已在上面被清（它的内容正是转移到这两样里的）⟹ **三样一起消失**。
//
// 实测（同一 fixture 只换掐断方式）：
//
//	panic  → cp frontier=[worker,worker] pendingSend=2 → Resume 后 acc=[from_good]
//	Goexit → cp frontier=[bad]           pendingSend=0 → Resume 后 acc=[]   ← 永久丢
//
// # 修法为什么不是「把 clear 也推迟」
//
// 试过，**实测双放**：`completedSiblings` 与 `pendingSendWrites` 承载**同一批 payload**
// （deferred sends 先并进 SendDirectives、再摊成 dirWrites），两者同时留在存档 ⟹
// Resume 时一条从 pendingSendWrites 补 apply、一条从 deferredSends 重新并流 ⟹
// `acc=[from_good from_good]`。所以 clear 留在原位（转移在那一刻完成），
// pre-arm 只摆「转移的下家」。
//
// # 断言选在哪
//
// **两条掐断路径的存档必须逐字段相同**——比断言某个具体值更贴近不变量：panic 路径
// 一直是对的，Goexit 只要跟它一致就对。
// cpDigest 是两条掐断路径要逐字段比的那份摘要。
type cpDigest struct {
	frontier   string
	completed  string
	deferred   int
	pendingSnd int
	accAfter   string
	workerRuns int
	goodRuns   int
}

func TestSendDirectives_GoexitLeavesSameStateAsPanic(t *testing.T) {
	// ⚠️ 两条子用例都用真掐断方式各跑一遍，**再逐字段比**——这条用例曾经是假绿：
	// 开关是包级变量且全仓从没被赋过 true，于是它一直只跑 panic 分支，而 doc 里
	// 承诺的「逐字段相同」没有任何断言在比。把 fix 整个 revert 掉它照样绿。
	panicDigest := runSendDirectivesScenario(t, false)
	goexitDigest := runSendDirectivesScenario(t, true)

	// **承重（本用例的全部意义）**：Goexit 路径的存档必须与 panic 路径逐字段相同。
	// panic 那条一直是对的，所以「跟它一致」是最贴近不变量的断言。
	if goexitDigest != panicDigest {
		t.Errorf("两条掐断路径的存档不一致——Goexit 下 caller 侧记账没留在安全态\n"+
			"  panic : %+v\n  goexit: %+v", panicDigest, goexitDigest)
	}
}

func runSendDirectivesScenario(t *testing.T, useGoexit bool) cpDigest {
	t.Helper()
	acc := compose_v2.Append[string]("zz_acc")
	var boom atomicBool
	// 这个 channel 的 reducer 在 dirWrites 那批里返错——用类型不匹配触发
	// （reduceAny 对类型错 panic ⟹ ApplyAll 转 error）
	payload := compose_v2.Custom("zz_payload", func(prev, next string) string {
		if boom.get() {
			if useGoexit {
				runtime.Goexit()
			}
			panic(errors.New("ZZ_REDUCER_BOOM"))
		}
		return next
	}, "")

	// fixture 自身的状态被 fan-out 的并发成员 goroutine 同时碰，得自己加锁
	// （-race 会抓；这不是生产代码的问题）
	var mu sync.Mutex
	var interrupted bool
	var wRuns, gRuns int

	b := compose_v2.NewBuilder().WithName("zz_clear")
	b = compose_v2.WithChannel(b, acc)
	b = compose_v2.WithChannel(b, payload)
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "good"},
			compose_v2.SendDirective{Target: "bad"},
		)
		return nil, nil
	})
	// good：成功 + 发一个 SendFrom（它的 send 会进 deferredSends）
	b.AddNode("good", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		mu.Lock()
		gRuns++
		mu.Unlock()
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{
			Target:  "worker",
			Payload: []compose_v2.ChannelWrite{compose_v2.Write(acc, []string{"from_good"})},
		})
		return nil, nil
	})
	// bad：首跑中断（制造部分失败超步 ⟹ good 的记录进 completedSiblings）
	b.AddNode("bad", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		mu.Lock()
		first := !interrupted
		interrupted = true
		mu.Unlock()
		if first {
			return nil, &compose_v2.InterruptError{Reason: "zz",
				Query: &compose_v2.AskQuery{InterruptID: "zzq", Question: "?"}}
		}
		// Resume 后成功，并再发一个 send —— 这一批 dirWrites 里含 good 的 deferred
		compose_v2.SendFrom(ctx, compose_v2.SendDirective{
			Target:  "worker",
			Payload: []compose_v2.ChannelWrite{compose_v2.Write(payload, "trigger")},
		})
		return nil, nil
	})
	b.AddNode("worker", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		mu.Lock()
		wRuns++
		mu.Unlock()
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("worker", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	sid := "zz_clear_sid_panic"
	if useGoexit {
		sid = "zz_clear_sid_goexit"
	}

	e1 := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	cp1, _ := cper.LoadNS(ctx, sid, "")
	t.Logf("Run1 err=%v", e1)
	if cp1 != nil {
		t.Logf("  cp1: frontier=%v completed=%v deferred=%d pendingSend=%d",
			cp1.CurrentFrontier, cp1.CompletedFrontier, len(cp1.DeferredSends), len(cp1.PendingSendWrites))
	}

	// Resume：bad 成功、发 send ⟹ dirWrites 含 good 的 deferred payload + bad 的
	// payload；payload reducer 引爆 ⟹ ApplyAll 走普通 error 路径
	boom.set(true)
	var e2 error
	fin := make(chan struct{})
	go func() {
		defer close(fin)
		e2 = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
			compose_v2.WithResumeAnswers(map[string]*schema.Message{"zzq": schema.UserMessage("go")}))
	}()
	<-fin
	cp2, _ := cper.LoadNS(ctx, sid, "")
	t.Logf("Run2 err=%v", e2)
	if cp2 != nil {
		t.Logf("  cp2: frontier=%v completed=%v deferred=%d pendingSend=%d",
			cp2.CurrentFrontier, cp2.CompletedFrontier, len(cp2.DeferredSends), len(cp2.PendingSendWrites))
	}

	// Run3：修好 reducer 再续 —— 看 worker 跑几次、acc 里有几份 from_good
	boom.set(false)
	cm3 := compose_v2.NewChannelManager()
	e3 := compose_v2.Run(ctx, g, cm3,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	got, _ := compose_v2.Get(cm3.Channels(), acc)
	mu.Lock()
	wR, gR := wRuns, gRuns
	mu.Unlock()
	t.Logf("Run3 err=%v / worker 跑了 %d 次 / good 跑了 %d 次 / acc=%v", e3, wR, gR, got)

	// 三条各自的绝对判据仍留着——比对相等只能保证「两条一样」，一起错也会通过。
	if len(got) != 1 || got[0] != "from_good" {
		t.Errorf("[useGoexit=%v] payload 该恰好一份，实际 acc=%v\n"+
			"  []   ⟹ 掐断时 frontier/payload/completed 三样一起丢\n"+
			"  两份 ⟹ 源头与下家同时留在存档，Resume 双放", useGoexit, got)
	}
	if wR != 2 {
		t.Errorf("[useGoexit=%v] worker 该恰好跑 2 次，实际 %d 次", useGoexit, wR)
	}
	if gR != 1 {
		t.Errorf("[useGoexit=%v] good 已成功不该重跑，实际 %d 次", useGoexit, gR)
	}

	d := cpDigest{deferred: -1, pendingSnd: -1}
	if cp2 != nil {
		// ⚠️ **只比 NodeID，不比 InstanceID**：实例号来自**进程级发号器**，第二次跑
		// 必然递增（i3/i4 → i7/i8）。把它纳入比对就是「跨每次新铸的身份维比派生值」
		// ⟹ 必然不等 ⟹ 假阳。承重的是「哪些节点在 frontier 里」，不是它们的号。
		d.frontier = frontierNodeIDs(cp2.CurrentFrontier)
		d.completed = frontierNodeIDs(cp2.CompletedFrontier)
		d.deferred = len(cp2.DeferredSends)
		d.pendingSnd = len(cp2.PendingSendWrites)
	}
	d.accAfter = fmt.Sprint(got)
	d.workerRuns, d.goodRuns = wR, gR
	return d
}

// atomicBool 是 fixture 用的小开关（reducer 在成员 goroutine 里读它）。
type atomicBool struct {
	mu sync.Mutex
	v  bool
}

func (b *atomicBool) set(v bool) { b.mu.Lock(); b.v = v; b.mu.Unlock() }
func (b *atomicBool) get() bool  { b.mu.Lock(); defer b.mu.Unlock(); return b.v }

// frontierNodeIDs 只取 NodeID 拼串——实例号是进程级发号器产物，不参与跨次比对。
func frontierNodeIDs(ms []compose_v2.FrontierMember) string {
	ids := make([]string, 0, len(ms))
	for _, m := range ms {
		ids = append(ids, string(m.NodeID))
	}
	return strings.Join(ids, ",")
}
