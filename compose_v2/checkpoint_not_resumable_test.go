package compose_v2_test

// 守「续跑一份自相矛盾的存档必须报错，而且不能毁掉现场」。
//
// **矛盾**特指：存档说有人在等回答（`PendingInterrupts` 非空），却没有任何地方能跑到
// 那个提问（`CurrentFrontier` 空）。frontier 空**且**没有挂起是「跑完了」，是合法的，
// 由 `TestResume_FromAlreadyCompletedSnapshot` 钉着——两条一起才划出边界。

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// dropFrontierCper 模拟**业务自研 backend 漏序列化 `CurrentFrontier`**——这是这个 bug
// 最隐蔽也最真实的产地：存进去是完整的，读回来少一个字段，而且**读写两侧都不报错**。
//
// 用它而不是手搓一份空档：手搓档测的是「合成的坏输入」，而这个测的是「一次真 HITL 中断
// 经过一个有缺陷的 backend 之后长什么样」——后者才是业务会撞上的形态。
type dropFrontierCper struct {
	compose_v2.Checkpointer
}

func (d *dropFrontierCper) LoadNS(ctx context.Context, sid, ns string) (*compose_v2.Checkpoint, error) {
	cp, err := d.Checkpointer.LoadNS(ctx, sid, ns)
	if err != nil || cp == nil {
		return cp, err
	}
	clone := *cp
	clone.CurrentFrontier = nil // ← 漏掉的就是这一个字段
	return &clone, nil
}

func TestResume_ContradictoryCheckpointRejected(t *testing.T) {
	askCalls := 0
	b := compose_v2.NewBuilder().WithName("g_contradict")
	b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		askCalls++
		if _, ok := compose_v2.GetResumeAnswer(ctx, "q"); !ok {
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "q", Question: "批准转账吗"}}
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	inner := compose_v2.NewInMemoryCheckpointer()
	cper := &dropFrontierCper{Checkpointer: inner}
	ctx := context.Background()

	// Run 1：真中断，存档写进去时是完整的
	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("sid"))); !ok {
		t.Fatal("fixture 没搭对：Run 1 该中断")
	}
	stored, _ := inner.LoadNS(ctx, "sid", "")
	if stored == nil || len(stored.CurrentFrontier) == 0 || len(stored.PendingInterrupts) == 0 {
		t.Fatalf("fixture 没搭对：存进去的档该是完整的，实际 %+v", stored)
	}
	beforeCalls := askCalls

	// Run 2：用户答完回来续跑——backend 把 frontier 弄丢了
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("sid"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{"q": schema.UserMessage("批准")}))

	// 承重①：必须报错。**这里最容易写成否定式断言**——「没报错」在「什么都没发生」这个
	// 状态上恰好为真，所以下面还要一条正向的（节点有没有真跑）。
	if !errors.Is(err, compose_v2.ErrCheckpointNotResumable) {
		t.Fatalf("存档说有人在等回答却没有起跑地址，该报 ErrCheckpointNotResumable。\n"+
			"实际 err=%v，而这一跑 ask 节点执行了 %d 次（用户的答案根本没送达）", err, askCalls-beforeCalls)
	}
	// 承重②：不可重试——重跑必然拿到同一份档
	if compose_v2.IsRetryable(err) {
		t.Error("这个错重跑必然一样，配了 retry 的编排会把它重试到次数用满")
	}
	// 承重③：**现场原样保留**。存档不自洽时最不该做的就是毁掉唯一的排查依据。
	after, _ := inner.LoadNS(ctx, "sid", "")
	if after == nil {
		t.Fatal("原始存档被删了——排查依据没了")
	}
	if after.Status != stored.Status || len(after.PendingInterrupts) != len(stored.PendingInterrupts) ||
		len(after.CurrentFrontier) != len(stored.CurrentFrontier) {
		t.Errorf("原始存档被回写覆盖了：\n  之前 status=%s frontier=%d pending=%d\n  之后 status=%s frontier=%d pending=%d",
			stored.Status, len(stored.CurrentFrontier), len(stored.PendingInterrupts),
			after.Status, len(after.CurrentFrontier), len(after.PendingInterrupts))
	}
}

// childOnlyDropCper 只对**子 ns** 丢 `CurrentFrontier`——嵌套形态下的同一类 backend 缺陷。
type childOnlyDropCper struct {
	compose_v2.Checkpointer
}

func (d *childOnlyDropCper) LoadNS(ctx context.Context, sid, ns string) (*compose_v2.Checkpoint, error) {
	cp, err := d.Checkpointer.LoadNS(ctx, sid, ns)
	if err != nil || cp == nil || ns == "" {
		return cp, err
	}
	clone := *cp
	clone.CurrentFrontier = nil
	return &clone, nil
}

// TestContradictoryCheckpoint_AncestorStaysResumable 守「拒绝之后**祖先层**的存档不能被
// 盖成失败现场」。
//
// **为什么这条比它看起来重要**：报错的那一层守住了自己的现场（守卫在 Restore 之前、
// 出口早退不写），但**每个祖先层还要写自己的档**。按错误类型判的话它们会一路被盖成
// [compose_v2.CPStatusFailed]，于是业务修好 backend 之后这些档在 `ListPending` 里一条都列不出来
// （⚠️ 本条写于 2026-08-13 撤 Status 闸门之前，当时后果更重：auto-load 不捡、`agent_v2.Resume` 直接拒，
// 会话彻底救不回来；今天闸门按内容判，续跑本身不受影响）。
// 而嵌套是 9 编排的常态。
//
// 判据挂在 `shouldStayPending`（不是 `IsControlFlowSignal`——那会让 fallback 层跟着
// 承担它不需要的语义），跟已经在那儿的 `errCondRerunFrom` 同类：返的是错，但**框架的
// 处置意图**是「存着等修好再续」。
//
// sanity-revert：`retry.go::shouldStayPending` 去掉 `ErrCheckpointNotResumable` 那一项
// → 本测试 fail，父档 status 从 pending 变 failed。
func TestContradictoryCheckpoint_AncestorStaysResumable(t *testing.T) {
	sub := compose_v2.NewBuilder().WithName("sub_ask")
	sub.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, ok := compose_v2.GetResumeAnswer(ctx, "q"); !ok {
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "q", Question: "批准吗"}}
		}
		return nil, nil
	})
	sub.AddEdge(compose_v2.StartNode, "ask")
	sub.AddEdge("ask", compose_v2.EndNode)
	sg, err := sub.Compile()
	if err != nil {
		t.Fatalf("compile sub: %v", err)
	}

	b := compose_v2.NewBuilder().WithName("parent_of_ask")
	b.AddSubgraphNode("SUB", sg)
	b.AddEdge(compose_v2.StartNode, "SUB")
	b.AddEdge("SUB", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile parent: %v", err)
	}

	inner := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// Run 1 用好 backend：父子两份档都完整
	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(inner), compose_v2.WithSessionID("nest"))); !ok {
		t.Fatal("fixture 没搭对：Run 1 该中断")
	}
	before, _ := inner.LoadNS(ctx, "nest", "")
	if before == nil || before.Status != compose_v2.CPStatusPending {
		t.Fatalf("fixture 没搭对：父档该是 pending，实际 %+v", before)
	}

	// Run 2 用有缺陷的 backend（只坏子 ns）→ 子层守卫开火
	runErr := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(&childOnlyDropCper{Checkpointer: inner}),
		compose_v2.WithSessionID("nest"))
	if !errors.Is(runErr, compose_v2.ErrCheckpointNotResumable) {
		t.Fatalf("子层该报 ErrCheckpointNotResumable，实际 %v", runErr)
	}

	after, _ := inner.LoadNS(ctx, "nest", "")
	if after == nil {
		t.Fatal("父档被删了")
	}
	if after.Status != compose_v2.CPStatusPending {
		t.Fatalf("父档被盖成 %s——它就从「谁在等答案」的清单里消失了。\n"+
			"坏的是子层那份存档，不是这次执行；祖先层不该跟着被判死刑", after.Status)
	}

	// **子档也要查**：只断言父档的 Status 是不够的——祖先层的 Status 对了，但如果子档
	// 本身被这一轮改写（frontier 被抹平），修好 backend 照样救不回来。
	// 这条断言是那个缺口的**唯一防线**：变异实测「把守卫从 `cm.Restore` 之前挪到之后」
	// 时，只有它红（同用例的父档 Status 断言不报）。
	child, _ := inner.LoadNS(ctx, "nest", "SUB")
	if child == nil {
		t.Fatal("子档被删了——那是唯一的排查依据")
	}
	if len(child.CurrentFrontier) == 0 || len(child.PendingInterrupts) == 0 {
		t.Fatalf("子档被改写了：frontier=%d pending=%d（期望都非 0）——"+
			"坏的是 backend 的读路径，存储里那份本来是好的",
			len(child.CurrentFrontier), len(child.PendingInterrupts))
	}

	// **最终承重**：换回好 backend 之后整条链要能救回来。
	// 前面那些都是「没被弄坏」的否定式断言，只有这一条是正向的——**它必须产出**。
	askedAgain := false
	if ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(inner), compose_v2.WithSessionID("nest"))); ok {
		for _, q := range ie.AllQueries() {
			if q.InterruptID == "q" {
				askedAgain = true
			}
		}
	}
	if !askedAgain {
		t.Fatal("修好 backend 之后这条会话没能续上——数据自始至终是好的，不该救不回来")
	}
}

// TestPendingList_RebuiltNotMerged 钉住「挂起清单按本轮重建、不做跨轮合并」这个选择，
// 连同它的代价一起。
//
// # 为什么要有这条用例
//
// 这里曾经加过一个「写回时把上一轮没跑到的提问合并回来」的补救，动机是：节点在重抛之前
// 失败时，清单会被清空后写回去，用户正等着答的问题从存档里消失。补救本身修掉了那个症状，
// 补救本身修掉了那个症状，但它连续四轮在「僵尸」和「少报」之间摇摆——修一头另一头就冒出来。
//
// # 摇摆的根因不是「合并」，是我们只记了半本账
//
// 这里一度归因成「合并判不了缺席」，还从中总结出一条「挂起集必须导出自早于本轮决定的时点」
// 的原理。**那条原理是错的**：纯累加的做法照样能自动消解，只要「答过」也是一条持久正记录，
// 挂起集读时用 `问过 − 答过` 算出来即可。
//
// 真正的判据是**挂起集必须有一个总写入者**，二选一：
//   - **每次全量覆盖**（本轮重建整张表，也就是我们现在这条路）；
//   - **加和减都是显式正记录**（问一次记一条、答一次记一条，读时相减）。
//
// 被禁止的是第三种：**加是正记录、减靠推断「这轮没再出现」**。那正是被撤掉的 merge 的形状。
//
// 而我们写不出 `问过 − 答过`，是因为**只持久化了半本账**：`AskQuery` 进了
// `cp.PendingInterrupts`，答案却只活在单次 Resume 的运行期配置上、从不落档。缺了减数那一半，
// 任何合并都只能去猜缺席的含义——摇摆是缺账的后果，不是合并的性质。想换第二条路，
// 前提是先把「答过」也持久化。
//
// # 代价（本用例第二段在量它）
//
// 节点在重抛之前失败的那一轮，存档的清单是空的——**存档对自己的描述不准**。
//
// ⚠️ **别把「问题会回来」读成「续上了」**。那一轮是普通业务错 ⟹ 存档标成失败现场；
// **写这条时**（2026-08-13 撤 Status 闸门之前）auto-load 只捡挂起档 ⟹ 下一轮是 fresh run，
// 问题重新被问出来靠整条链从头跑一遍、**上游副作用跟着重放**（第二段用一个上游计数节点钉住）。
//
// 这个代价**跟合不合并无关**——实测在带合并的版本上同样重放，决定它的是当时「业务错 ⟹ Failed
// ⟹ auto-load 不捡」这条正交性质（2026-08-13 起续跑闸门按内容判（`HasResumableWork`）、不读 Status，那条性质已不成立，
// 但「清单不承权威、frontier 承权威」这个结论没变）。合并唯一改变的是那份自我描述，却为它付了四个 bug。
//
// 那份描述**不是没人读**——准确的说法只有一句：**没有任何东西靠它路由答案**（`agui` /
// `a2a/server` 的待答清单都从 `ie.AllQueries()` 建，答案按 ID 直接投递）。读点则有好几处
// 且还在增加：诊断（`warnUnmatchedAnswerIDs`，清单空则整段静默早退）、事件计数
// （`CpSaveAction.PendingInterruptsCount`）、以及**本文件上面那道自洽守卫自己**——它的
// 判据就是「`PendingInterrupts` 非空而 frontier 空」。
//
// ⚠️ 别再写「零消费者」（这里一度这么写）。要清单就 grep `PendingInterrupts`，别在注释里
// 数——这份数目已经涨过两次。
func TestPendingList_RebuiltNotMerged(t *testing.T) {
	failBeforeAsk := false
	// **上游计数节点**：没有它，第二段的「问题回来了」在任何能把图跑起来的实现下都成立
	// （实测：把 frontier 整个抹掉，问题照样回来）——那是一条恒真断言。有了它，「从
	// frontier 续」和「从头再跑」才分得开，被文档化的那个代价也才有哨兵。
	upstream := 0
	b := compose_v2.NewBuilder().WithName("rebuild")
	b.AddNode("pre", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		upstream++
		return nil, nil
	})
	b.AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if failBeforeAsk {
			return nil, errors.New("问之前先查了个下游，503 了")
		}
		if _, ok := compose_v2.GetResumeAnswer(ctx, "q"); !ok {
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "q", Question: "批准转账吗"}}
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "pre")
	b.AddEdge("pre", "ask")
	b.AddEdge("ask", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("rb"))); !ok {
		t.Fatal("fixture 没搭对：第一跑该中断")
	}

	// ① 代价：这一轮没跑到提问点 ⟹ 清单是空的（**不合并**）
	failBeforeAsk = true
	_ = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("rb"))
	mid, _ := cper.LoadNS(ctx, "rb", "")
	if mid == nil {
		t.Fatal("失败现场该留着")
	}
	if len(mid.PendingInterrupts) != 0 {
		t.Fatalf("这是**有意的**：清单按本轮重建，没跑到就是空的。现在是 %d 条——"+
			"若是重新加了合并，请先读本用例上面那段说明", len(mid.PendingInterrupts))
	}
	// 这里钉的是**档位标对**（业务错 ⟹ 失败现场），不是「下一轮会怎么走」——2026-08-13 起续跑闸门按内容判（`HasResumableWork`）、不读 Status，
	// 下一轮怎么走由 frontier 与挂起清单决定。（写这条时读反了，措辞是「真正决定下一轮
	// 怎么走的是 Status」；更早还断言过 frontier 非空并写着「问题能自己回来全靠它」，
	// 那句同样是假的：把 frontier 抹掉，问题照样回来。）
	if mid.Status != compose_v2.CPStatusFailed {
		t.Fatalf("这一轮是普通业务错，档该是失败现场，实际 %s——"+
			"若改成了 Pending，下面那段量到的代价就该跟着改", mid.Status)
	}

	// ② **承重**：下一轮问题必须重新被问出来（这才是「丢」可以接受的全部理由），
	//    **并且要量清楚它是怎么回来的**。
	//
	// 2026-08-13 之前这里断言「上游重放 1 次（fresh run 从头跑）」——那时失败档被
	// auto-load 的档位 gate 挡住。gate 撤掉（捡不捡按内容判，`HasResumableWork`）后
	// 失败档被捡起、**从失败节点续跑**：上游不再重放，问题由 frontier 上的提问节点
	// 重新问出来。「不合并」的代价从「整条链重跑」降为「零」——清单丢的那一轮
	// 自我描述，靠续跑天然补回。
	failBeforeAsk = false
	replayedFrom := upstream
	ie, isInt := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("rb")))
	if !isInt || len(ie.AllQueries()) != 1 || ie.AllQueries()[0].InterruptID != "q" {
		t.Fatalf("问题没有重新被问出来——那样「不合并」就真成了丢失。isInterrupt=%v", isInt)
	}
	if replayed := upstream - replayedFrom; replayed != 0 {
		t.Fatalf("上游重放了 %d 次，期望 0 次（失败档被捡起、从失败节点续跑，上游的"+
			"副作用不该再来一遍）。变回 1 说明失败档又被某道档位 gate 挡住了——"+
			"那正是撤掉的那道（嵌套 Resume 时子图捡不起失败子档 ⟹ 副作用重放）", replayed)
	}
	if back, _ := cper.LoadNS(ctx, "rb", ""); back == nil || len(back.PendingInterrupts) != 1 {
		t.Errorf("存档里的清单也该跟着回来")
	}

	// ③ 闭环收尾
	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("rb"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{"q": schema.UserMessage("批准")})); err != nil {
		t.Fatalf("答完该收尾: %v", err)
	}
}
