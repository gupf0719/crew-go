package compose_v2_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
)

// TestFanout_AllInterruptsReturnedToCaller：同一超步多个分支同时中断时，返回给 caller 的
// error 必须带**全部** query，不能只带第一个。
//
// 框架在 cp 侧早就记全了（`recordInterrupt` 遍历 `AllInterrupts`），返回值那条路却一直是
// `res.FirstErr`——只带第一个分支的。业务照文档范式用 `AllQueries()` 建答案 map 就会
// **系统性少答**：cp 里挂着 N 个中断，业务只看得见 1 个，答完 Resume 又撞上没答的那些，
// 要多绕几轮才收敛（不死锁，但每轮都是一次多余的往返）。
//
// cp 侧的数字在这里当**对照组**——它代表「框架其实知道有几个」，两边不等就是漏返。
//
// **sanity-revert**：把 graph_loop.go 失败返回处改回 `return res.FirstErr`，
// 本测试 FAIL 并报 caller 只拿到 1 个、cp 里有 3 个。
func TestFanout_AllInterruptsReturnedToCaller(t *testing.T) {
	const branches = 3
	var seq atomic.Int64

	b := compose_v2.NewBuilder().WithName("fanout_multi_int")
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ds := make([]compose_v2.SendDirective, 0, branches)
		for i := 0; i < branches; i++ {
			ds = append(ds, compose_v2.SendDirective{Target: "w"})
		}
		compose_v2.SendFrom(ctx, ds...)
		return nil, nil
	})
	// 每个实例都抛中断，各带自己的 question——问题文本必须可区分，
	// 否则「返回了几个」在内容上看不出来（同款陷阱：fixture 不可区分导致假绿）。
	//
	// ⚠️ **ID 必须让框架算（走 `Ask`），不能 fixture 自己发**。这里曾经填
	// `fmt.Sprintf("branch_%d", n)`——那样「三个 ID 互不相同」是 fixture 的原子计数器
	// 保证的，跟框架一点关系没有：实测把 `interruptSiteFromCtx` 里的
	// `InstanceID: GetCurrentInstanceID(ctx)` 改成 `""`，compose_v2 全包照样绿。
	//
	// 而这三个实例是**同一个 NodeID** 的 fan-out——六维坐标里只有 `InstanceID` 能区分
	// 它们。所以走 `Ask` 之后，下面那条 ID-distinctness 断言才真的在守「实例身份从 ctx
	// 读回来了」这件事（同一变异下它会红）。
	b.AddNode("w", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		n := seq.Add(1)
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{
			Question: fmt.Sprintf("分支 %d 的问题", n),
		})
		return nil, err
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("w", compose_v2.EndNode)

	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	runErr := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("fmi_sid"))

	ie, isInt := compose_v2.IsInterrupt(runErr)
	if !isInt {
		t.Fatalf("应当返回 InterruptError，实际 %v", runErr)
	}
	got := ie.AllQueries()

	cp, cpErr := cper.LoadNS(ctx, "fmi_sid", "")
	if cpErr != nil {
		t.Fatalf("LoadNS: %v", cpErr)
	}
	inCp := 0
	if cp != nil {
		inCp = len(cp.PendingInterrupts)
	}

	t.Logf("caller 拿到 %d 个 query；cp 里记了 %d 个", len(got), inCp)
	if len(got) != inCp {
		t.Errorf("caller 拿到 %d 个 query，cp 里却记了 %d 个——业务照 AllQueries() 建答案 map "+
			"会系统性少答，答完 Resume 又撞上没答的那些", len(got), inCp)
	}
	if len(got) != branches {
		t.Errorf("caller 拿到 %d 个 query, want %d（每个分支各一个）", len(got), branches)
	}
	// 每个 query 都得有独立的 InterruptID，不然业务配答案时无从区分
	ids := map[string]bool{}
	for _, q := range got {
		if q == nil {
			t.Fatal("AllQueries 里有 nil")
		}
		ids[q.InterruptID] = true
	}
	if len(ids) != len(got) {
		t.Errorf("%d 个 query 只有 %d 个不同的 InterruptID——业务无法逐个配答案", len(got), len(ids))
	}
}
