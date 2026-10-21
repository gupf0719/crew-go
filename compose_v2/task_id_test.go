package compose_v2_test

import (
	"context"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// 中断寻址掺进「任务代号」这一维之后的两条不变量。
//
// # 修的是什么
//
// 中断寻址 ID 原先六维**全是位置**（graphNS / nodeID / instanceID / visit / toolCallIdx /
// idx），而位置在新任务里全部重置 ⟹ 同一 sid 下两个任务走到同一个地方，ID **确定性相等**
// （实测两个任务六维逐字段相同，不是概率碰撞）。
//
// 后果不是「状态错」，是**给任务 A 的答案被任务 B 的问题消费掉**——实测过：用户看着
// 「批准给张三转 5000 吗」点了批准，系统执行的是「已删除」，`err` 全程 nil。校验拦不住，
// 因为陈旧答案不是「对不上」，是**正好对上了**。
//
// # 两个要求必须由不同的东西承担
//
//   - **跨 Resume 稳定**（人在思考期间调用方攥着这个 ID，回来还得对得上）← 位置维度负责
//   - **跨任务唯一** ← 任务代号负责
//
// 纯位置方案不可能同时满足：位置不变正是它稳定的原因，也正是它跨任务必撞的原因。

func buildTaskIDGraph(t *testing.T, seen *[]string) (*compose_v2.Graph, *compose_v2.Channel[string]) {
	t.Helper()
	inCh := compose_v2.LastValue[string]("tid_in")
	b := compose_v2.NewBuilder()
	b = compose_v2.WithChannel(b, inCh)
	b.AddNode("ask", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		id := compose_v2.CurrentInterruptSite(ctx).InterruptID()
		if a, ok := compose_v2.GetResumeAnswer(ctx, id); ok && a != nil {
			*seen = append(*seen, "答:"+a.Content)
			return nil, nil
		}
		v, _ := compose_v2.Get(chs, inCh)
		*seen = append(*seen, "问@"+v+"#"+id)
		return nil, &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{InterruptID: id, Question: "批准「" + v + "」吗"},
		}
	})
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g, inCh
}

// TestTaskID_CrossTaskIDsDiffer 主证：同 sid 两个任务的中断 ID 必须不同。
func TestTaskID_CrossTaskIDsDiffer(t *testing.T) {
	var seen []string
	g, inCh := buildTaskIDGraph(t, &seen)
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	run := func(input string) *compose_v2.AskQuery {
		err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("same_sid"),
			compose_v2.WithFreshRun(), // 明确：这是新任务
			compose_v2.WithInitialWrites(compose_v2.Write(inCh, input)))
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			t.Fatalf("应中断，got %v", err)
		}
		return ie.Query
	}

	// ⚠️ 两个任务都**停在中断且不 Resume**——跑完的话 cleanupOnSuccess 会清掉存档，
	// 后一个任务读不到前一个的代号、也就无从谈碰撞，这条用例会恒真。
	qA := run("给张三转 5000")
	qB := run("清空我的历史")
	t.Logf("A.id=%s  B.id=%s", qA.InterruptID, qB.InterruptID)

	if qA.InterruptID == qB.InterruptID {
		t.Fatalf("两个任务的中断 ID 相同（%s）——给 A 的答案会被 B 的问题消费掉。\n"+
			"轨迹=%v\n（六维全是位置时这是确定性相等，不是概率碰撞）", qA.InterruptID, seen)
	}
}

// TestTaskID_StableAcrossResume 反向承重：**同一个任务跨 Resume，ID 必须一字不变**。
//
// 这是掺维度时最容易弄坏的那一半——人在思考期间调用方攥着这个 ID，回来对不上就再也答
// 不进去了。用「每次写存档都换」的值（比如 `Epoch`）算 ID 就会踩这个坑：`Durability=Sync`
// 每超步写一次，同一次 Run 内跨超步 ID 就漂了。
func TestTaskID_StableAcrossResume(t *testing.T) {
	var seen []string
	g, inCh := buildTaskIDGraph(t, &seen)
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("stable"),
		compose_v2.WithDurability(compose_v2.DurabilitySync), // 逼每超步都写存档
		compose_v2.WithInitialWrites(compose_v2.Write(inCh, "转 5000")))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应中断，got %v", err)
	}
	first := ie.Query.InterruptID

	// 拿到 ID 之后隔一轮再答（模拟人去思考了）——中间没有别的调用推进这个 sid
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("stable"),
		compose_v2.WithDurability(compose_v2.DurabilitySync),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			first: schema.UserMessage("批准")})); err != nil {
		t.Fatalf("续跑应成功，got %v 轨迹=%v", err, seen)
	}

	// 承重：答案真被消费了 —— 只断「没报错」分辨不出「ID 漂了所以节点重新问了一遍」，
	// 那种情况下 Run 会再抛中断而不是成功，但若上游哪天改成静默跳过就漏了。
	if len(seen) == 0 || seen[len(seen)-1] != "答:批准" {
		t.Fatalf("答案没被消费——ID 跨 Resume 漂了。轨迹=%v\n"+
			"（首跑 ID=%s；掺的那一维若每次写存档都换就会这样）", seen, first)
	}
}

// TestTaskID_NestedMintsOwn 嵌套子图各铸各的代号，且各自跨 Resume 稳定。
//
// ⚠️ 这条**必须实测不能推理**：子图有自己的 (sid, ns) 存档，代号是各存各的还是沿 ctx
// 继承，光读码容易看错——而继承会让子图在全新一跑里以为自己在续跑。
func TestTaskID_NestedMintsOwn(t *testing.T) {
	var seen []string
	g, inCh := buildTaskIDGraph(t, &seen)
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	runNS := func(ns, input string) *compose_v2.AskQuery {
		err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("nested"),
			compose_v2.WithRunNS(ns),
			compose_v2.WithInitialWrites(compose_v2.Write(inCh, input)))
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			t.Fatalf("ns=%s 应中断，got %v", ns, err)
		}
		return ie.Query
	}

	qX := runNS("parent/childX", "任务X")
	qY := runNS("parent/childY", "任务Y")
	t.Logf("childX.id=%s  childY.id=%s", qX.InterruptID, qY.InterruptID)
	if qX.InterruptID == qY.InterruptID {
		t.Fatalf("两个子图的中断 ID 相同——ns 维度也没救回来：%s", qX.InterruptID)
	}

	// 同一个子图续跑：代号必须从它自己的存档读回，ID 一字不变
	before := len(seen)
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("nested"),
		compose_v2.WithRunNS("parent/childX"),
		compose_v2.WithInitialWrites(compose_v2.Write(inCh, "任务X")),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			qX.InterruptID: schema.UserMessage("批准")})); err != nil {
		t.Fatalf("子图续跑应成功，got %v", err)
	}
	if len(seen) == before || seen[len(seen)-1] != "答:批准" {
		t.Fatalf("子图续跑时答案没被消费——它的代号没从自己的存档读回。轨迹=%v", seen[before:])
	}
}
