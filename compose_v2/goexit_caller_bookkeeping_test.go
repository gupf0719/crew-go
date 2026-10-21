package compose_v2_test

import (
	"context"
	"runtime"
	"testing"

	"crew-go/compose_v2"
)

// TestGoexit_SiblingWritesSurviveFrontierNarrowing 守「提交点被掐断时，caller 侧的记账
// 也得留在安全态」。
//
// # 它防的是什么（这是一条**修 A 带出 B** 的回归）
//
// 部分失败超步里，frontier 会被窄化成失败分支（Resume 只跑失败的、不重跑成功兄弟）。
// 那次窄化原本发生在 `ApplyAll` **之前**，靠 error 分支再把整超步塞回来补偿——而
// `runtime.Goexit` 既不返回也不产生 err，补偿天生跑不到。于是：
//
//   - 成功兄弟的 writes 被 ApplyAll 的 Goexit 回滚（那是对的，原子性）
//   - 但 frontier 里没有它（窄化已经生效）
//   - ⟹ Resume 不重跑它 ⟹ **那些写入永久消失**
//
// ⚠️ 这个洞是**补上 ApplyAll 的 Goexit 回滚才暴露的**：在那之前，半应用的写入侥幸
// 留在存档里、而它的生产者又不重跑，两个错正好抵消。修一个就露出另一个。
//
// 修法是 pre-arm / post-disarm：窄化意图先记下，`ApplyAll` **真的成了**才落地。
//
// sanity-revert：把窄化挪回 ApplyAll 之前（即 `narrowFrontierTo` 那套换回就地
// `updateFrontier(res.FailedBranches)`）→ 本用例红在「good 的写入永久消失」。
func TestGoexit_SiblingWritesSurviveFrontierNarrowing(t *testing.T) {
	log := compose_v2.Append[string]("sib_log")
	var armed bool
	bomb := compose_v2.Custom("sib_bomb", func(prev, next string) string {
		if armed {
			runtime.Goexit() // 提交点被掐断
		}
		return next
	}, "")

	var badRuns int
	b := compose_v2.NewBuilder().WithName("sib_frontier")
	b = compose_v2.WithChannel(b, log)
	b = compose_v2.WithChannel(b, bomb)
	b.AddNode("split", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	// good：成功并写 log —— 它的写入必须活下来（要么留在存档，要么重跑重产）
	b.AddNode("good", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		armed = true
		return []compose_v2.ChannelWrite{
			compose_v2.Write(log, []string{"good"}),
			compose_v2.Write(bomb, "trigger"),
		}, nil
	})
	// bad：首跑失败（制造「部分失败超步」这个前提），Resume 时成功并写 log
	b.AddNode("bad", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		badRuns++
		if badRuns == 1 {
			return nil, context.DeadlineExceeded // 控制流信号：留 Pending 档
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(log, []string{"bad"})}, nil
	})
	b.AddEdge(compose_v2.StartNode, "split")
	b.AddEdge("split", "good")
	b.AddEdge("split", "bad")
	b.AddEdge("good", compose_v2.EndNode)
	b.AddEdge("bad", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "sib_frontier_sid"

	fin := make(chan struct{})
	go func() {
		defer close(fin)
		_ = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	}()
	<-fin

	saved, _ := cper.LoadNS(ctx, sid, "")
	if saved == nil {
		t.Fatal("该留下存档")
	}
	var inFrontier []string
	for _, fm := range saved.CurrentFrontier {
		inFrontier = append(inFrontier, string(fm.NodeID))
	}
	_, hasLog := saved.Channels["sib_log"]
	t.Logf("存档 frontier=%v / sib_log present=%v", inFrontier, hasLog)

	// **承重**：good 的写入不能凭空消失。两种合法结局二选一——
	//   (a) 写入留在存档里（没被回滚）；(b) 被回滚了，但 good 在 frontier 里等重跑。
	// 两者皆无 = 永久丢失，那正是修复前 Goexit 路径的症状。
	var goodWillRerun bool
	for _, id := range inFrontier {
		if id == "good" {
			goodWillRerun = true
		}
	}
	if !hasLog && !goodWillRerun {
		t.Fatalf("成功兄弟的写入永久消失：既被回滚、又不在 frontier 里等重跑\n"+
			"  frontier=%v sib_log存在=%v\n"+
			"  （窄化跑在 ApplyAll 之前、而 Goexit 下补偿跑不到 ⟹ 两头落空）",
			inFrontier, hasLog)
	}
}
