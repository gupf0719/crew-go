package compose_v2_test

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"crew-go/compose_v2"
)

// TestApplyAll_GoexitRollsBackToo 守「提交点被掐断时不留半应用状态」。
//
// # 它防的是什么
//
// ApplyAll 的原子回滚原本整段写在 `if r := recover()` 里——`runtime.Goexit`（业务
// reducer 里一句 logger.Fatal）也展开这条栈，但 recover 返 nil ⟹ **回滚整个跳过** ⟹
// 同一批里先提交的写入留在 cm 上、被持久化进 Failed 档 ⟹ Resume 重跑整超步 ⟹
// Append/Sum **双计**（实测修复前 `[x]` → Resume → `[x x]` 且 err=nil）。
//
// 这正是 R9 D1 当初给 panic 路径消灭掉的形态，Goexit 路径漏了。
//
// sanity-revert：把 defer 里 `if !applied { rollback() }` 那支去掉 → 本用例红在
// 「存档里留了半应用状态」。
func TestApplyAll_GoexitRollsBackToo(t *testing.T) {
	appendCh := compose_v2.Append[string]("half")
	// 第二个 channel 的 reducer 在首次 reduce 时 Goexit——同一批 ApplyAll 里
	// appendCh 先提交、它后炸
	var boom bool
	bombCh := compose_v2.Custom("bomb", func(prev string, next string) string {
		if boom {
			runtime.Goexit()
		}
		return next
	}, "")

	b := compose_v2.NewBuilder().WithName("applyall_goexit")
	b = compose_v2.WithChannel(b, appendCh)
	b = compose_v2.WithChannel(b, bombCh)
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		boom = true
		return []compose_v2.ChannelWrite{
			compose_v2.Write(appendCh, []string{"x"}),
			compose_v2.Write(bombCh, "trigger"),
		}, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "applyall_goexit_sid"

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	}()
	<-done

	saved, _ := cper.LoadNS(ctx, sid, "")
	if saved == nil {
		t.Fatal("该留下 Failed 档")
	}
	// **承重**：存档里不能有半应用状态。修复前 half 里是 ["x"]——Resume 重跑会变 ["x","x"]。
	half, has := saved.Channels["half"]
	t.Logf("存档 Channels[half] present=%v value=%v", has, half)
	if has {
		t.Errorf("提交点被掐断，同批先提交的写入该回滚，实际存档里留了 half=%v\n"+
			"  Resume 重跑整超步 ⟹ 双计（Append/Sum 上就是多算一份）", half)
	}
}

// TestApplyAll_RollbackScopeIsThisBatchOnly 守回滚的**粒度**：只回本批，不碰前几个
// 超步已提交的写入。
//
// 跟 TestApplyAll_GoexitRollsBackToo 不是同一维——那条守「回滚跑没跑」，这条守「回滚
// 回多了没有」。两个方向都会出错数据：不跑 ⟹ 半应用状态进存档（Resume 双计）；回多了
// ⟹ 把上几个超步的好数据一起还原（静默丢进度）。
//
// `rollbackStack` 只记「每个 channel 本批第一次被改动前的 prev」，所以语义上就该只回
// 本批——本用例把它钉住，Goexit 路径上也成立。
func TestApplyAll_RollbackScopeIsThisBatchOnly(t *testing.T) {
	acc := compose_v2.Append[string]("acc")
	var boom bool
	bomb := compose_v2.Custom("bomb2", func(prev, next string) string {
		if boom {
			runtime.Goexit()
		}
		return next
	}, "")

	b := compose_v2.NewBuilder().WithName("zz_rb")
	b = compose_v2.WithChannel(b, acc)
	b = compose_v2.WithChannel(b, bomb)
	// 第一个节点先正常写入一份「好数据」（上一超步提交，不该被回滚）
	b.AddNode("good", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(acc, []string{"keep"})}, nil
	})
	// 第二个超步里 Goexit —— 回滚只该还原本批，不该动上一批的 "keep"
	b.AddNode("bad", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		boom = true
		return []compose_v2.ChannelWrite{
			compose_v2.Write(acc, []string{"dirty"}),
			compose_v2.Write(bomb, "x"),
		}, nil
	})
	b.AddEdge(compose_v2.StartNode, "good")
	b.AddEdge("good", "bad")
	b.AddEdge("bad", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "zz_rb_sid"
	fin := make(chan struct{})
	go func() {
		defer close(fin)
		_ = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	}()
	<-fin

	saved, _ := cper.LoadNS(ctx, sid, "")
	if saved == nil {
		t.Fatal("该留 Failed 档")
	}
	raw, has := saved.Channels["acc"]
	t.Logf("存档 acc present=%v raw=%s", has, string(raw.Data))
	// **承重**：上一超步的 "keep" 必须还在（回滚只回本批），"dirty" 不能在
	if !has {
		t.Fatal("上一超步已提交的写入被连带回滚了——回滚粒度错（应只回本批）")
	}
	s := string(raw.Data)
	if !strings.Contains(s, "keep") {
		t.Errorf("上一超步的 keep 丢了（双重回滚）：%s", s)
	}
	if strings.Contains(s, "dirty") {
		t.Errorf("本批半应用的 dirty 留下来了（回滚没跑）：%s", s)
	}
}
