package compose_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestAnswerConsumption_NilValueIsNotConsumption 钉住「**key 在、value 是 nil** ⟹
// 当没答案、照常再问」——不是终局失败。
//
// # 为什么是这条语义
//
// `map{"id": nil}` 是**明写支持的形态**（见 NextAnswerSlot godoc）：前端反序列化失败、
// DB 字段 NULL 都会长成这样。框架三处 HITL（ask_user / 工具审批 / a2a human-gate）
// 全都按 `if ok && ans != nil` 双重校验，然后 fall through 去再问一次。
//
// # 它曾经踩到的坑
//
// 兑付记账最初按 **`found`**（key 在不在）记，于是那条路变成：记账落下 → 调用方 fall
// through 抛中断（同一个 id）→ 判它落在已兑付位上 → `ErrRunAborted` + **清整棵存档
// 子树**。三处框架 HITL 实测全中：会话被删掉，而不是再问一次。
//
// 判据必须是「**真的交出了一个能用的答案**」，不是「key 在不在 map 里」。
//
// sanity-revert：把 interrupt.go 里 `if found && a != nil` 改回 `if found`
// （`GetResumeAnswer` 里同款）→ 本用例红，且红成「err 是 ErrRunAborted、存档被清」。
func TestAnswerConsumption_NilValueIsNotConsumption(t *testing.T) {
	asked := 0
	mk := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("nilvalue")
		b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			asked++
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗"}); err != nil {
				return nil, err
			}
			return nil, nil
		})
		b.AddEdge(compose_v2.StartNode, "n")
		b.AddEdge("n", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		return g
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "nilvalue_sid"

	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, mk(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid)))
	if !ok {
		t.Fatal("Run1 该中断")
	}
	id := ie.AllQueries()[0].InterruptID

	// 调用方传了一个 **nil value**（前端反序列化失败 / DB 字段 NULL）
	err2 := compose_v2.Run(ctx, mk(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{id: nil}))

	// **承重 1**：这该是「再问一次」，不是终局失败
	if errors.Is(err2, compose_v2.ErrRunAborted) {
		t.Errorf("nil value 该当作「没答案、再问一次」，实际终局失败: %v\n"+
			"  = 记账按「key 在不在」记，而不是按「真的交出了能用的答案」记", err2)
	}
	ie2, isInt := compose_v2.IsInterrupt(err2)
	if !isInt {
		t.Fatalf("该重新抛出中断（再问一次），实际 err=%v", err2)
	}
	// **承重 2**：问的还是同一位（身份没漂），调用方补上真答案就能续
	if got := ie2.AllQueries()[0].InterruptID; got != id {
		t.Errorf("再问该落在同一位上（身份由位置派生），got %s want %s", got, id)
	}
	// **承重 3**：存档还在——终局失败会把它连根清掉，那是最不可逆的后果
	if left, _ := cper.LoadAllInSession(ctx, sid); len(left) == 0 {
		t.Error("存档被清掉了——调用方拿一个 null value 回来，整个会话就没了")
	}

	// **承重 4（正向）**：补上真答案能跑完
	if err := compose_v2.Run(ctx, mk(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{id: schema.UserMessage("批准")})); err != nil {
		t.Fatalf("补上真答案该跑通: %v", err)
	}
	if asked != 3 {
		t.Errorf("节点该跑 3 次（首问 / nil 再问 / 真答案跑通），实际 %d", asked)
	}
}

// TestAnswerConsumption_NonNilButEmptyIsStillConsumption 守判据的**另一侧**：
// 消息非 nil 但内容为空时，那**算**一次兑付。
//
// # 为什么这一侧也要钉
//
// 兑付判据（`found && a != nil`）必须跟**调用方判「有没有答案」的判据**一致，否则会分叉：
// 一边认为兑付了、另一边 fall through 再问 ⟹ 第二问落在已兑付位上 ⟹ 终局失败 + 清档
// （那正是 nil-value 那条回归的形状，只是换了个触发值）。
//
// 实测框架三处 HITL 的写法都是 `if ok && ansMsg != nil`，且拿到非 nil 消息后**都不会
// 再问一次**：ask_user 直接返回其内容、审批层对空内容走 fail-closed 拒绝、a2a 拿它当
// 发送内容。所以「非 nil 但空」在四处的语义一致——记账、不再问。
//
// 这条钉的是那个**一致性**：判据散在四个地方（引擎记账 + 三处 HITL），谁改都可能漂。
func TestAnswerConsumption_NonNilButEmptyIsStillConsumption(t *testing.T) {
	asked := 0
	mk := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder().WithName("emptyval")
		b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			asked++
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗"})
			if err != nil {
				return nil, err
			}
			// 拿到非 nil 消息就当有答案往下走——三处框架 HITL 都是这个形状
			_ = ans
			return nil, nil
		})
		b.AddEdge(compose_v2.StartNode, "n")
		b.AddEdge("n", compose_v2.EndNode)
		g, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		return g
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	const sid = "emptyval_sid"

	ie, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, mk(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid)))
	if !ok {
		t.Fatal("Run1 该中断")
	}
	id := ie.AllQueries()[0].InterruptID

	// **非 nil 但内容为空**——它是一个有效答案（用户就是没填字），不是「没答案」
	if err := compose_v2.Run(ctx, mk(), compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			id: {Role: schema.RoleUser, Content: ""},
		})); err != nil {
		t.Fatalf("非 nil 的空消息该当作有效答案、正常跑完，实际 err=%v\n"+
			"  终局失败 = 记账判据与调用方判据分叉了", err)
	}
	if asked != 2 {
		t.Errorf("节点该跑 2 次（首问 + 拿到答案跑通），实际 %d——"+
			"更多说明它又被问了一次（判据分叉）", asked)
	}
}
