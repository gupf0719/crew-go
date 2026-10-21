package compose_v2_test

import (
	"context"
	"sync"
	"testing"

	"crew-go/compose_v2"
)

// TestR33_GraphID_ConcurrentAccess_NoRace 复现四编排 review GA-H2：
//
// 业务 graph 未 WithName 时 ID() 走惰性 fingerprint 路径写 g.cachedID 无锁——
// GraphAgent 是唯一长持单个 *Graph 跨 Run 共享的编排，两并发 Run + interrupt
// 同时走 SaveNS defer 的 g.ID() 并发读写 cachedID（-race 实证 DATA RACE）。
//
// fix：Compile() 末尾（单线程）预热 cachedID，运行期 ID() 只读。
// sanity-revert：删 Compile 预热 → 本测试在 `go test -race` 下 fail（DATA RACE）。
// 注意本测试必须配 -race 跑才有检出力（无 -race 时仅验证 ID 稳定性）。
func TestR33_GraphID_ConcurrentAccess_NoRace(t *testing.T) {
	ch := compose_v2.LastValue[string]("__r33_idrace_ch__")
	bld := compose_v2.NewBuilder() // 故意不 WithName——逼 fingerprint 路径
	compose_v2.WithChannel(bld, ch)
	bld.AddNode("n", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	bld.AddEdge(compose_v2.StartNode, "n")
	bld.AddEdge("n", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	const n = 8
	ids := make([]string, n)
	var wg sync.WaitGroup
	start := make(chan struct{}) // WaitGroup 屏障强制真并发（CLAUDE.md 并发测试纪律）
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ids[i] = g.ID()
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 1; i < n; i++ {
		if ids[i] != ids[0] {
			t.Fatalf("ID 不稳定: ids[%d]=%q != ids[0]=%q", i, ids[i], ids[0])
		}
	}
	if ids[0] == "" {
		t.Fatal("ID 为空")
	}
}
