package compose_v2_test

// 答案兑付账本跨子图边界哨兵（answer_consumption.go 的 parent 链 +
// graph_loop.go 成功出口的 bubbleConsumedAnswersTo）。
//
// 钉的缺陷形态：账本挂在每层图自己的 cm/cp 上——子图 A 兑付、成功、记录随子层
// 一起消失；后续子图 B 撞同一个自填 ID 时**谁都不记得那一位已兑付** ⟹ B 的工具体
// 拿着 A 的授权答案执行（「批准转账」的票放行了别的动作），err==nil。
//
// 修后语义（两半，本测试同时钉住——单独 revert 任一半都红）：
//   - 链（读）：B 层的判定沿父链看见祖先已提交的兑付 ⟹ GetResumeAnswer 当没答案；
//   - 上冒（写）：A 子图成功出口把兑付集冒进父层节点账本，随父超步提交进父 cm。
//
// B 随之抛出的同 ID 中断落在已兑付位上 ⟹ validateInterrupt 按既有条款响亮终局
// （ErrRunAborted）——身份撞车从「静默错授权」变成「当场可见」。

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// buildSubWithConstID 造一个「节点用业务自填常量 ID 走 GetResumeAnswer」的子图——
// 自填 ID 是账本 godoc 点名「最容易撞」的形态。
func buildSubWithConstID(t *testing.T, constID string, got *[]string, found *[]bool) *compose_v2.Graph {
	t.Helper()
	bld := compose_v2.NewBuilder()
	bld.AddNode("work", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		ans, ok := compose_v2.GetResumeAnswer(ctx, constID)
		*found = append(*found, ok)
		if !ok {
			return nil, &compose_v2.InterruptError{
				Reason: "need approval",
				Query:  &compose_v2.AskQuery{InterruptID: constID, Question: "批准吗?"},
			}
		}
		*got = append(*got, ans.Content)
		return nil, nil
	})
	bld.AddEdge(compose_v2.StartNode, "work")
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("sub compile: %v", err)
	}
	return g
}

// TestAnswerLedger_CrossSubgraphBoundary：subA 兑付并成功收尾后，subB 撞同一个
// 自填 ID——B 必须**拿不到** A 的答案，且 B 的同 ID 新提问按「落在已兑付位」响亮
// 终局（ErrRunAborted），而不是静默复用。
func TestAnswerLedger_CrossSubgraphBoundary(t *testing.T) {
	const constID = "approve-transfer"
	var gotA, gotB []string
	var foundA, foundB []bool

	subA := buildSubWithConstID(t, constID, &gotA, &foundA)
	subB := buildSubWithConstID(t, constID, &gotB, &foundB)

	bld := compose_v2.NewBuilder()
	bld.AddSubgraphNode("subA", subA)
	bld.AddSubgraphNode("subB", subB)
	bld.AddEdge(compose_v2.StartNode, "subA")
	bld.AddEdge("subA", "subB")
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("parent compile: %v", err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	sid := "ledger-cross-boundary-sid"
	base := context.Background()

	e1 := compose_v2.Run(base, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	if _, ok := compose_v2.IsInterrupt(e1); !ok {
		t.Fatalf("Run1 subA 该中断, got %v", e1)
	}

	e2 := compose_v2.Run(base, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			constID: schema.UserMessage("批准A"),
		}))

	// A 恰好兑付一次
	if len(gotA) != 1 || gotA[0] != "批准A" {
		t.Fatalf("subA 该恰好兑付一次, got %v", gotA)
	}
	// **B 不该拿到 A 的答案**——静默错授权的实质就在这一步
	if len(gotB) != 0 {
		t.Fatalf("subB 不该拿到 A 的答案（跨子图边界账本失守）, got %v", gotB)
	}
	if len(foundB) == 0 || foundB[0] {
		t.Fatalf("subB 的 GetResumeAnswer 该返回未找到, foundB=%v", foundB)
	}
	// B 的同 ID 新提问落在已兑付位 ⟹ 响亮终局，不是再挂一次
	if e2 == nil {
		t.Fatalf("Run2 该以终局失败结束（身份撞车必须可见）, got nil")
	}
	if !errors.Is(e2, compose_v2.ErrRunAborted) {
		t.Fatalf("撞已兑付位该按 ErrRunAborted 终局, got %v", e2)
	}
}
