package compose_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// 续跑负载的两条通道必须互不串。
//
// # 修的是什么
//
// 曾经只有一条通道（`UserInputs map[InterruptID]*Message`），而它扛了两种语义：
//
//   - **寻址型**：这条答案是给某个具体提问的（「批准转账吗」→「批准」）
//   - **非寻址型**：用户这轮说了什么（「换个方向」），不指向任何提问
//
// 两者对「要不要限定给我」的答案**正好相反**，混在一条通道里的代价实测过两条：
//
//  1. 非寻址型没有真 ID 可用，只能拿**假 key** 挤进去（业务约定 `"_replan_"` 之类）。
//     那个假 ID 跟真挂起并存时，`ask_user` 取不到自己的答案、反复重抛中断 —— **死循环**。
//  2. 为了让非寻址型读得到，出现过一个「取 map 里**任意一条**」的读法。它同时也能取到
//     **别人的**答案：实测同一会话里任务 A 挂起被放弃、任务 B 挂起，用户答 A 的 ID，
//     用那个读法的节点执行了 **B** 那件事，`err == nil`。
//
// **给它加过滤治不了本**——A 的 ID 合法地在挂起集里，怎么裁都留着。所以删掉那个读法
// 本身：现在节点只有 `GetResumeAnswer(ctx, myID)` 一条路，**拿到别人的答案在构造上不可能**。

// TestResumeChannelSplit_AddressedNotVisibleAsAmbient 寻址型的答案**不得**从非寻址通道漏出来。
//
// 这条是防退化的关键：将来若有人图方便把 `UserInputs` 的某一条也塞进 `UserText`
// （"反正单挂起时就一条"），那个「取任意一条」的漏就原样回来了。
func TestResumeChannelSplit_AddressedNotVisibleAsAmbient(t *testing.T) {
	var sawText string
	var sawTextOK bool
	b := compose_v2.NewBuilder()
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sawText, sawTextOK = compose_v2.GetResumeUserText(ctx)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// 只给寻址型答案，不给非寻址型
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			"some_interrupt_id": schema.UserMessage("批准转账 5000"),
		})); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if sawTextOK {
		t.Fatalf("寻址型答案从非寻址通道漏出来了：%q\n"+
			"后果：任何读非寻址通道的组件（如 planner）都会拿到不属于它的答案——"+
			"实测过用户批准 A 那件事、系统执行了 B", sawText)
	}
}

// TestResumeChannelSplit_AmbientNotVisibleAsAddressed 反向：非寻址型的文本**不得**被
// 当成某个提问的答案。
//
// 防的是「把 UserText 也塞一份进 UserInputs 兜底」这种写法——那会让任意一个正在等答案的
// 节点吃到它，等于用户随口说的一句话变成了对某个审批的「同意」。
func TestResumeChannelSplit_AmbientNotVisibleAsAddressed(t *testing.T) {
	var got *schema.Message
	var gotOK bool
	b := compose_v2.NewBuilder()
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// 节点用自己的 ID 取——这是现在**唯一**的取答案方式
		id := compose_v2.CurrentInterruptSite(ctx).InterruptID()
		got, gotOK = compose_v2.GetResumeAnswer(ctx, id)
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// 只给非寻址型文本
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithResumeUserText("换个方向重新规划")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if gotOK {
		t.Fatalf("非寻址型文本被当成了某个提问的答案：%v\n"+
			"后果：用户随口一句话会变成对某个审批的「同意」", got)
	}
}

// TestResumeChannelSplit_BothCanCoexist 两条通道可以同时给，各走各的。
//
// 这是 `plan_chat_web` 的真实形态：同一句话既是给 `ask_user` 的答案（寻址），也是
// 「用户这轮说了什么」（非寻址，planner 要据此调整剩余步骤）。**调用方显式给两次**——
// 拆分之前这半是靠「取任意一条」偶然得到的。
func TestResumeChannelSplit_BothCanCoexist(t *testing.T) {
	var addressed *schema.Message
	var ambient string
	asked := false
	b := compose_v2.NewBuilder()
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		id := compose_v2.CurrentInterruptSite(ctx).InterruptID()
		if a, ok := compose_v2.GetResumeAnswer(ctx, id); ok {
			addressed = a
			ambient, _ = compose_v2.GetResumeUserText(ctx)
			return nil, nil
		}
		asked = true
		return nil, &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{InterruptID: id, Question: "批准吗"}}
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// ⚠️ ID 必须**从同一次跑里**拿——每次新开会铸新的任务代号，另起一跑算出的 ID 不同
	// （那正是跨任务不串台的机制）。
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("both")))
	if !ok || !asked {
		t.Fatal("首跑应中断（fixture 没搭对）")
	}

	if err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("both"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("批准"),
		}),
		compose_v2.WithResumeUserText("顺便把预算调到 3000")); err != nil {
		t.Fatalf("续跑: %v", err)
	}
	// 承重：两条各自到位，且**没有互相顶替**
	if addressed == nil || addressed.Content != "批准" {
		t.Fatalf("寻址型没到位：%v", addressed)
	}
	if ambient != "顺便把预算调到 3000" {
		t.Fatalf("非寻址型没到位：%q", ambient)
	}
}

// TestAsk_DoesNotWriteBackToCaller `Ask` **不写回调用方的对象**。
//
// # 这条防的是什么
//
// 写回时，「调用方声明的身份」和「上一次 Ask 自己写回去的残留」变成同一件事——而制造后者
// 的正是 `Ask` 自己。于是复用同一个 `AskQuery`（存成工具结构体字段 / 闭包外变量，很自然的
// 写法）时第二问不再 tick、直接拿第一问的 ID ⟹ **命中第一问已经答过的答案**。
//
// 实测那个形态：轨迹「问第一问 → 答=是 → 问第二问 → 答=是」，但第二问**从未被抛出去**、
// 图成功收尾 `err == nil`。审批语境下 = **静默自动批准**。
//
// **没有任何错误信号**，而且连「同一批里撞 ID」那道硬拒也够不着——那道校验查的是**被抛出去
// 的**那批 query，而这里第二问压根没抛，落档的自始至终只有一条。
func TestAsk_DoesNotWriteBackToCaller(t *testing.T) {
	var asked []string
	shared := &compose_v2.AskQuery{Question: "第一问"}
	step := 0
	b := compose_v2.NewBuilder()
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		step = 0
		for {
			step++
			shared.Question = "第" + string(rune('0'+step)) + "问"
			ans, err := compose_v2.Ask(ctx, shared)
			if err != nil {
				if ie, ok := compose_v2.IsInterrupt(err); ok && ie.Query != nil {
					asked = append(asked, ie.Query.Question+"#"+ie.Query.InterruptID)
				}
				return nil, err
			}
			_ = ans
			if step >= 2 {
				return nil, nil
			}
		}
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	e1 := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("wb"))
	ie1, ok := compose_v2.IsInterrupt(e1)
	if !ok {
		t.Fatalf("首跑应中断，got %v", e1)
	}
	// **承重①：调用方的对象没被写脏**
	if shared.InterruptID != "" {
		t.Fatalf("Ask 写回了调用方的对象（InterruptID=%q）——复用该对象时第二问会带着"+
			"第一问的身份、命中已答过的答案、没问就做了", shared.InterruptID)
	}
	asked = nil
	e2 := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("wb"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			ie1.Query.InterruptID: schema.UserMessage("是")}))
	// **承重②（正向）：第二问必须真的被问出去**——只断「没串台」在「压根没问」时恒真
	if _, again := compose_v2.IsInterrupt(e2); !again {
		t.Fatalf("第二问没有被问出去（续跑直接收尾 err=%v）——它吃掉了第一问的答案。\n"+
			"本轮抛出的提问：%v", e2, asked)
	}
	if len(asked) == 0 || !strings.HasPrefix(asked[0], "第2问") {
		t.Fatalf("续跑抛出的应是第二问，实际 %v", asked)
	}
}

// TestAsk_FailsFastOutsideNode 图外调 `Ask` 必须报错，而不是给一个永远没人能答的 ID。
//
// 图外各维度取零值 ⟹ 算出一个**常量** ID（实测两处不相干的提问都拿到
// `a5f04c5e3163b86f`，会互相吃答案）。更要紧的是：那种中断**存不进 cp**——实测
// `ListPending` 为 0、Resume 直接 `ErrNoPendingCheckpoint`。
//
// 所以这道守卫**拒掉的不是合法调用**，它把「发一个永远没人能答的 ID」换成「当场说清为什么」。
// 判据也合法（按本仓规约）：入参只有「调用方把代码放在哪」这件它自己知道的事，
// 不是框架发现的状态；逃生门天然存在——自填 ID 就不触发（身份不再由位置派生）。
func TestAsk_FailsFastOutsideNode(t *testing.T) {
	_, err := compose_v2.Ask(context.Background(), &compose_v2.AskQuery{Question: "图外提问"})
	if err == nil {
		t.Fatal("图外调 Ask 应报错")
	}
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Fatalf("图外的错误不该是中断（没人能答它）：%v", err)
	}
	// **自填 ID 不再是 Ask 的逃生门**（2026-08-12 改）：那条路正是三种答案泄漏的入口
	// ——业务能选 key ⟹ 两个不同的提问可以共用一个答案位（跨超步 / 跨 visit / 同节点
	// 两问，见 NextAnswerSlot godoc）。Ask 现在只认位置派生的身份。
	_, err2 := compose_v2.Ask(context.Background(),
		&compose_v2.AskQuery{InterruptID: "biz::order-42", Question: "图外但自填"})
	if _, isInt := compose_v2.IsInterrupt(err2); isInt {
		t.Fatal("Ask 不该接受自填 ID——那是「两个提问共用一个答案位」的入口")
	}
	if err2 == nil || !strings.Contains(err2.Error(), "不接受自填") {
		t.Fatalf("拒绝时要说清为什么 + 给出路，实际 %v", err2)
	}
	// **逃生门仍在，只是换了地方**：想给前端一个自己认得的标签，直接构造 InterruptError。
	// 那条路上的 ID 只作标签——因为取答案的入口（Ask）不认它，它路由不了任何东西。
	label := &compose_v2.InterruptError{Query: &compose_v2.AskQuery{
		InterruptID: "biz::order-42", Question: "图外但自填"}}
	if _, isInt := compose_v2.IsInterrupt(label); !isInt {
		t.Fatal("直接构造 InterruptError 这条路该照常可用")
	}
}
