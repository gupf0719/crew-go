// node_state_demo 演示 compose_v2.SaveNodeState / GetNodeState——节点私有中断态（mock，不需要 API key）。
//
// 场景：一个节点里连发 3 张订单（每张都是真副作用），第 2 张是大额、要先问人
// （compose_v2.Ask）。节点是原子的：中断时整节点 writes 作废、Resume 从头重跑——
// 已经发出去的第 1 张会不会再发一遍，取决于节点有没有记下「我做到哪了」。
//
//	[A] 用 memo：做完一件记一件（SaveNodeState），Resume 时读回（GetNodeState）跳过 → 3 张各发 1 次
//	[B] 不用 memo：Resume 时第 1 张重发 → 4 次副作用，而且 err == nil，谁都不会报错
//
// 唯一的硬规约：**memo 必须在副作用提交之后写**（记「已经做成了什么」，不是「打算做什么」）。
// 见 compose_v2.SaveNodeState godoc。
//
// 跑：go run ./example/harness/node_state_demo
package main

import (
	"context"
	"fmt"
	"os"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// shipMemo 是节点自己攒的进度：订单号 → 回执。随 cp 持久化，只有这个节点读得回。
type shipMemo struct {
	Done map[string]string `json:"done"`
}

func init() {
	// memo 与 channel value 走同一套 tagged JSON 协议——类型必须先注册
	compose_v2.RegisterValueType[*shipMemo]("node_state_demo.shipMemo")
}

var orders = []string{"O-1", "O-2", "O-3"}

// buildGraph 造单节点图。useMemo=false 是反例：同一段逻辑去掉 Save/Get 两行。
func buildGraph(useMemo bool, effects *[]string) *compose_v2.Graph {
	out := compose_v2.LastValue[string]("shipped")
	b := compose_v2.NewBuilder().WithName("ship_graph")
	b = compose_v2.WithChannel(b, out)
	b.AddNode("ship", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		memo := &shipMemo{Done: map[string]string{}}
		if useMemo {
			// ok==true 不保证非 nil（指针类型 round-trip 回来可能是 typed nil）——守卫别省
			if prev, ok := compose_v2.GetNodeState[*shipMemo](ctx); ok && prev != nil {
				memo = prev // 重跑：读回上次做完的
			}
		}
		for _, order := range orders {
			if _, already := memo.Done[order]; already {
				continue // 上次已发，跳过——不重复副作用
			}
			if order == "O-2" {
				// 没答案就抛中断（节点整体失败、cp 存档）；Resume 后同一位置拿到答案继续。
				// 这一问在每张订单自己的相位里问：做完的订单续跑时跳过，跳过它不会让后面订单的提问挪位
				// （见 compose_v2.Ask「按同样的次序问」）
				if _, err := compose_v2.Ask(compose_v2.EnterPhase(ctx, "confirm:"+order), &compose_v2.AskQuery{Question: order + " 是大额订单，确认发货？"}); err != nil {
					return nil, err
				}
			}
			*effects = append(*effects, order) // ← 真副作用（发货）
			memo.Done[order] = "receipt-" + order
			if useMemo {
				// 副作用已提交，再记——顺序反过来会让重跑跳过一件根本没做成的事
				if err := compose_v2.SaveNodeState(ctx, memo); err != nil {
					return nil, err
				}
			}
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(out, fmt.Sprintf("%d 张已发", len(memo.Done)))}, nil
	})
	b.AddEdge(compose_v2.StartNode, "ship")
	b.AddEdge("ship", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		fmt.Println("Compile:", err)
		os.Exit(1)
	}
	return g
}

// runScenario 跑 Run → 中断 → Resume 闭环，返回副作用序列。
func runScenario(ctx context.Context, useMemo bool, sid string) []string {
	var effects []string
	g := buildGraph(useMemo, &effects)
	cper := compose_v2.NewInMemoryCheckpointer()

	err := compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		fmt.Println("Run 应中断，实际:", err)
		os.Exit(1)
	}
	q := ie.AllQueries()[0]
	fmt.Printf("  中断: %q  中断前副作用: %v\n", q.Question, effects)

	cp, err := cper.LoadNS(ctx, sid, "")
	if err != nil || cp == nil {
		fmt.Println("LoadNS:", err, cp)
		os.Exit(1)
	}
	// 走框架的 Resume 路径：新 cm + WithResumeFrom(cp) + 答案按 InterruptID 投递
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeFrom(cp),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{q.InterruptID: schema.UserMessage("确认")}))
	if err != nil {
		fmt.Println("Resume:", err)
		os.Exit(1)
	}
	fmt.Printf("  Resume 完成，副作用序列: %v\n", effects)
	return effects
}

func main() {
	ctx := context.Background()
	pass := true
	check := func(cond bool, name string) {
		if cond {
			fmt.Printf("  ✓ %s\n", name)
		} else {
			fmt.Printf("  ✗ %s\n", name)
			pass = false
		}
	}

	fmt.Println("=== [A] 用 memo：做完一件记一件 ===")
	a := runScenario(ctx, true, "batch-with-memo")
	check(len(a) == 3, "3 张订单各发 1 次（第 1 张没有在 Resume 时重发）")

	fmt.Println("\n=== [B] 不用 memo：同一段逻辑去掉 Save/Get ===")
	b := runScenario(ctx, false, "batch-without-memo")
	check(len(b) == 4 && b[0] == "O-1" && b[1] == "O-1", "第 1 张发了 2 次——而且全程 err == nil，没有任何报错")

	fmt.Println()
	if !pass {
		fmt.Println("node_state_demo FAIL")
		os.Exit(1)
	}
	fmt.Println("node_state_demo PASS")
}
