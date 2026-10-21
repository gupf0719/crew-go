package compose_v2_test

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"crew-go/compose_v2"
)

// goexitOnSaveCper 在 SaveNS 里 Goexit——业务 checkpointer 实现里一句 logger.Fatal
// （zap.WithFatalHook 配置下）就是这形态。
type goexitOnSaveCper struct {
	compose_v2.Checkpointer
	calls int
}

func (c *goexitOnSaveCper) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	c.calls++
	runtime.Goexit()
	return nil
}

// TestAsyncWriter_GoexitSurfacesError 守「async cp worker 猝死必须浮现，不许静默」。
//
// # 它防的是什么
//
// `writeOne` 的 per-task recover 兜得住业务 checkpointer 的 panic，**兜不住
// `runtime.Goexit`**——deferred 照跑、goroutine 消失，而 `firstErr` 永不置位。之后：
// 队列没满则全部中途 cp **静默蒸发**而 Run 报完全成功（async 档买的就是「硬崩可恢复」，
// 那个保证无声消失）；队列满则 submit 阻塞在死 receiver 上。
//
// # 承重的是一条**注册次序**，而它极易被无意挪动
//
// `defer w.wg.Done()` 必须注册在兜底 defer **之前** ⟹ LIFO 下兜底先跑、`wg.Done()`
// 后跑 ⟹ `closeAndJoin` 的 `wg.Wait()` 返回时 `firstErr` 已置好。
//
// 把 `defer w.wg.Done()` 往下挪一行（代码等价、只换注册次序，比如为了「让兜底最先
// 注册」）⟹ `wg.Wait()` 先返回、读到 nil ⟹ **修复静默失效，而全套测试全绿**。
// 独立审计实测过这个变异，连续多次 FAIL 不是偶发。本用例就是钉那一维的。
//
// sanity-revert 两种，都该红：
//  1. 去掉 defer 里 `!drained` 那支 → `closeAndJoin` 返 nil
//  2. 把 `defer w.wg.Done()` 挪到兜底 defer 之后注册 → 同上
func TestAsyncWriter_GoexitSurfacesError(t *testing.T) {
	cper := &goexitOnSaveCper{Checkpointer: compose_v2.NewInMemoryCheckpointer()}

	b := compose_v2.NewBuilder().WithName("async_goexit")
	for _, id := range []compose_v2.NodeID{"n1", "n2"} {
		b.AddNode(id, func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		})
	}
	b.AddEdge(compose_v2.StartNode, "n1")
	b.AddEdge("n1", "n2")
	b.AddEdge("n2", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}

	// Run 跑在自己 goroutine 里：worker 被掐死不影响这里，但 Run 本身会正常返回
	// （worker 是后台的），所以能直接拿到 runErr。
	done := make(chan error, 1)
	go func() {
		done <- compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("async_goexit_sid"),
			compose_v2.WithDurability(compose_v2.DurabilityAsync))
	}()
	runErr := <-done
	t.Logf("SaveNS 被调 %d 次 / runErr=%v", cper.calls, runErr)

	// **承重 1（fixture 自检）**：worker 真的被掐死过——否则下面断的不是这个场景
	if cper.calls == 0 {
		t.Fatal("SaveNS 一次都没被调（fixture 没搭对：async 档该在超步边界写 cp）")
	}

	// **承重 2**：必须浮现。返 nil ⟹ 中途 cp 全部静默蒸发而 Run 报成功，
	// 而 async 档的全部价值就是那些 cp。
	if runErr == nil {
		t.Fatal("async cp worker 被 Goexit 掐死，Run 却报完全成功——" +
			"中途 checkpoint 静默蒸发，硬崩恢复的保证无声消失")
	}
	// **承重 3**：错误要说得清是 worker 异常终止（而不是笼统的 cp 写失败）
	if !strings.Contains(runErr.Error(), "worker") {
		t.Errorf("错误该点明是后台 worker 异常终止，实际：%v", runErr)
	}
}
