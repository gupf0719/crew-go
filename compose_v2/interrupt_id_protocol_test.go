package compose_v2_test

import (
	"context"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// 中断寻址 ID 的三条协议不变量。
//
// # 背景：加「任务代号」这一维时漏了两个参与点
//
// 这一维要真正生效，得三个环节都接上：**铸**（新开时生成）/ **带**（随存档持久化）/
// **用**（算 ID 时读进去）。第一版只接了「铸」和一半的「带」，于是：
//
//   - 框架给「业务没自填 ID 的中断」补 ID 时，用的是**另写一遍字段列表**的字面量——
//     那份列表漏了 `TaskID`（也早就漏了 `ToolCallIdx`）⟹ 官方文档写作「默认（推荐）」
//     的那条路上，跨任务的 ID 照撞，要修的 bug 原样存活
//   - `Snapshot()` 不填代号 ⟹ 跨进程 wire-push 那条路（文档推荐用法）对端载入到空代号、
//     铸新的 ⟹ 中断 ID 变了 ⟹ 调用方拿着旧 ID 永远答不进去（实测连跑五轮不收敛）
//
// 两处都是**「同一份东西写了两遍」**：字段列表写了两遍、代号的填充点分散在两个函数。
// 修法都是消灭那份平行拷贝，不是给它补字段——那个字面量已经证明了自己会漂。

// buildAskGraph 造一个用 `compose_v2.Ask` 提问的图（= 现在的推荐写法）。
//
// ⚠️ 它原先是「不自填 ID、让框架事后补」——那条路已被拒（框架看不见节点内部的分支结构，
// 补出来的 ID 与分支自己算的对不上，实测永不收敛并把副作用重放到无界）。改走 `Ask` 之后
// 「算 ID」和「用 ID」是同一次调用，下面几条钉的**任务代号语义**一字未变。
func buildAskGraph(t *testing.T, log *[]string) (*compose_v2.Graph, *compose_v2.Channel[string]) {
	t.Helper()
	inCh := compose_v2.LastValue[string]("af_in")
	b := compose_v2.NewBuilder()
	b = compose_v2.WithChannel(b, inCh)
	b.AddNode("act", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		v, _ := compose_v2.Get(chs, inCh)
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{
			Question: "批准「" + v + "」吗",
		}); err != nil {
			return nil, err
		}
		*log = append(*log, "执行了:"+v)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "act")
	b.AddEdge("act", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g, inCh
}

// ⚠️ 这里原先还有一条 AutoFillEqualsCurrentSite（已删），钉的是「框架事后补的 ID == 节点自己算的
// ID」。走 `Ask` 之后那两者**是同一次调用**，这条不变量在构造上无法违反，用例随之删除。
// 它当年抓到的那个确定性死锁（问用一个 ID、答查另一个）现在由「只有一个入口」结构性排除。

// TestInterruptIDProtocol_AskCrossTaskDiffer 用 Ask 提问时，跨任务的自动 ID 不得相同。
//
// 这是「用户批的是这件事、系统执行的是另一件」那个缺陷的原生形态——业务按文档推荐的
// 默认写法（不自填 ID）时它照样成立。
func TestInterruptIDProtocol_AskCrossTaskDiffer(t *testing.T) {
	var log []string
	g, inCh := buildAskGraph(t, &log)
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	ask := func(input string) string {
		err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("conv"),
			compose_v2.WithFreshRun(), // 明确：这是新任务
			compose_v2.WithInitialWrites(compose_v2.Write(inCh, input)))
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			t.Fatalf("应中断，got %v", err)
		}
		return ie.Query.InterruptID
	}

	// ⚠️ 两个任务都停在中断且不 Resume——跑完的话存档被清，后一个任务读不到前一个的
	// 代号，这条用例恒真。
	idA := ask("给张三转 5000")
	idB := ask("清空我的历史")
	t.Logf("A.id=%s  B.id=%s", idA, idB)
	if idA == idB {
		t.Fatalf("两个任务的自动 ID 相同（%s）——用户批准 A 那件事，系统会执行 B。\n"+
			"（框架补 ID 那处若漏掉任务代号这一维就会这样）", idA)
	}

	// 承重②：拿**任务 A 的** ID 去答，绝不能促成任何执行
	log = nil
	_ = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("conv"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			idA: schema.UserMessage("批准")}))
	if len(log) > 0 {
		t.Fatalf("拿任务 A 的 ID 回答，却促成了执行：%v", log)
	}
}

// TestInterruptIDProtocol_WirePushRoundTrip 跨进程 wire-push 闭环必须收敛。
//
// 这条路是 `SnapshotForGraph → Marshal → wire → Unmarshal → WithResumeFrom`（文档推荐的
// 跨进程用法），**全程不经过框架写存档的那个入口**——所以「代号只在写入口盖」时它拿不到，
// 对端会铸一枚新的、ID 随之改变，调用方拿着旧 ID 永远答不进去。
func TestInterruptIDProtocol_WirePushRoundTrip(t *testing.T) {
	var log []string
	g, inCh := buildAskGraph(t, &log)
	ctx := context.Background()

	// 「进程 1」：不挂 checkpointer，全靠手动快照 + wire
	cm1 := compose_v2.NewChannelManager()
	err := compose_v2.Run(ctx, g, cm1,
		compose_v2.WithInitialWrites(compose_v2.Write(inCh, "转 5000")))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应中断，got %v", err)
	}
	held := ie.Query.InterruptID // 调用方攥着这个 ID 去问人

	snap, err := cm1.SnapshotForGraph(g)
	if err != nil {
		t.Fatalf("SnapshotForGraph: %v", err)
	}
	wire, err := compose_v2.MarshalCheckpoint(snap)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// 「进程 2」：从 wire 恢复，拿着人给的答案续跑
	cp2, err := compose_v2.UnmarshalCheckpoint(wire)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithResumeFrom(cp2),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			held: schema.UserMessage("批准")})); err != nil {
		t.Fatalf("wire-push 闭环应收敛，got %v\n"+
			"（快照若不带任务代号，对端会铸一枚新的 ⟹ ID 变了 ⟹ 手里那个 ID 永远答不进去）", err)
	}
	if len(log) == 0 {
		t.Fatalf("答案没被消费——快照没把任务代号带过 wire")
	}
}

// TestInterruptIDProtocol_MissingTaskIDDegradesDeterministically 退化方向必须是**确定性**的。
//
// 存档里没有代号（升级期旧存档 / 业务自研 backend 漏序列化 / 业务自己拼的 `&Checkpoint{}`）
// 时，绝不能「铸一枚新的」——那样每次续跑都换一枚 ⟹ 中断 ID 每次都变 ⟹ **永不收敛**。
// 正确的退化是沿用空值 = 回到纯位置派生 = 引入这一维之前的行为。
func TestInterruptIDProtocol_MissingTaskIDDegradesDeterministically(t *testing.T) {
	var log []string
	g, inCh := buildAskGraph(t, &log)
	cper := dropTaskIDCheckpointer{compose_v2.NewInMemoryCheckpointer()}
	ctx := context.Background()

	run := func(opts ...compose_v2.RunOption) error {
		base := []compose_v2.RunOption{
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("degrade"),
			compose_v2.WithInitialWrites(compose_v2.Write(inCh, "转 5000")),
		}
		return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), append(base, opts...)...)
	}

	ie1, _ := compose_v2.IsInterrupt(run())
	// 第一轮答案必然失配（首跑有代号、存档里被抹了），调用方按协议从新中断里重读 ID
	err2 := run(compose_v2.WithResumeAnswers(map[string]*schema.Message{
		ie1.Query.InterruptID: schema.UserMessage("批准")}))
	ie2, again := compose_v2.IsInterrupt(err2)
	if !again {
		// 直接收敛也可接受（退化更轻），**但这条分支也必须断言**——否则将来退化真的
		// 变轻时，这个用例会悄悄变成什么都不验的空转。
		if err2 != nil {
			t.Fatalf("没再中断却也没成功：%v", err2)
		}
		if len(log) == 0 {
			t.Fatal("没再中断、也没报错，但答案根本没被消费——静默什么都没做")
		}
		return
	}
	// 承重：**第二轮起必须稳定**——再答一次就该收敛，不能无限换 ID
	if err := run(compose_v2.WithResumeAnswers(map[string]*schema.Message{
		ie2.Query.InterruptID: schema.UserMessage("批准")})); err != nil {
		t.Fatalf("代号缺失时应退化成「稳定的纯位置派生」而不是「每次换一枚」。\n"+
			"第二轮 ID=%s 再答仍失败：%v\n（若每次 Resume 都铸新代号，这里永不收敛）",
			ie2.Query.InterruptID, err)
	}
	if len(log) == 0 {
		t.Fatalf("答案始终没被消费——代号缺失时的退化不是确定性的")
	}
}

// dropTaskIDCheckpointer 存的时候抹掉任务代号，模拟「旧存档 / backend 漏序列化」。
type dropTaskIDCheckpointer struct{ compose_v2.Checkpointer }

func (d dropTaskIDCheckpointer) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	c := *cp
	c.TaskID = ""
	return d.Checkpointer.SaveNS(ctx, sid, ns, &c)
}
