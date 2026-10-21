package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// TestRepro_AskUserMultiVisitCrosstalk 验证 audit Round 3 Bug #1：
// 同节点跨多次"完成访问"（节点 N+1 次成功完成后被再次调度）InterruptID 不串台。
//
// **场景**：BasicAgent ReAct 多轮——tools 节点第 N 轮成功完成后回 model 节点，
// 第 N+1 轮又调 tools 节点（新一次"逻辑访问"）。每次完成访问应有独立 ID。
//
// **测试拓扑**：start → A → B → A → end（条件边让 A 被访问 2 次完成）
// - Visit 1: A 内 ask_user("Q1") 抛 ID_A1 (visit=0, idx=0)
// - Resume 答 ID_A1: A 重跑（同 visit=0）hit 旧答案 → A 完成 → visitCount[A]=1
// - B 跑过（不调 ask_user）→ 路由回 A
// - Visit 2: A 内 ask_user("Q2") 抛 ID_A2 (visit=1, idx=0) ≠ ID_A1
// - Run 3 答 ID_A2: A 重跑（visit=1）hit Q2 答案 → 完成
//
// **修复前**：visit 不持久化或一直是 0 → ID_A1 == ID_A2 → ask_user("Q2") 直接 hit
// 旧答案 ID_A1 → 静默串台。
//
// sanity-revert：去掉 NodeVisits 持久化或 advanceVisitsLocked 的 visit 自增，本测试 fail。
func TestRepro_AskUserMultiVisitCrosstalk(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(agent_v2.NewAskUserTool())

	visitedA := 0
	nodeA := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		visitedA++
		_, err := tk.Call(ctx, agent_v2.AskUserToolName, map[string]any{
			"question": "Q_visit" + string(rune('0'+visitedA)),
		})
		return nil, err
	}
	nodeB := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	}

	// 用 channel "a_completions" 跟踪 A 的成功完成次数让条件边读
	aCompCh := compose_v2.Sum("a_completions")

	completedA := 0
	// 包一层 NodeFunc 让 A 成功完成时把 +1 写 channel
	nodeAWithCount := func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		writes, err := nodeA(ctx, chs)
		if err != nil {
			return writes, err
		}
		completedA++
		return append(writes, compose_v2.Write(aCompCh, 1)), nil
	}

	visitB := 0
	b := compose_v2.NewBuilder().
		AddNode("A", nodeAWithCount).
		AddNode("B", nodeB).
		AddEdge(compose_v2.StartNode, "A").
		AddConditionalEdge("A", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
			// A 第一次成功完成 → B；A 第二次成功完成 → End
			n, _ := compose_v2.Get(chs, aCompCh)
			if n <= 1 {
				return "B", nil
			}
			return compose_v2.EndNode, nil
		}).
		AddConditionalEdge("B", func(_ context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
			visitB++
			return "A", nil
		}).
		WithMaxVisit(20)
	b = compose_v2.WithChannel(b, aCompCh)
	graph, _ := b.Compile()
	_ = completedA

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// Run 1: A 第一次访问，ask_user 抛 ID_A1 (visit=0, idx=0)
	cm1 := compose_v2.NewChannelManager()
	err := compose_v2.Run(ctx, graph, cm1,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("sid_multivisit"),
	)
	ie1, _ := compose_v2.IsInterrupt(err)
	if ie1 == nil {
		t.Fatalf("Run 1 expected interrupt, got %v", err)
	}
	idA1 := ie1.Query.InterruptID
	t.Logf("Run 1 A 第一次访问抛 ID = %s (visitedA=%d)", idA1, visitedA)

	// Run 2: 答 ID_A1 → A 重跑同 visit hit 旧答 → A 成功 → IncrVisit → 走 B → 回 A
	// → A 第二次访问 ask_user 抛 ID_A2 (visit=1, idx=0) ≠ ID_A1
	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, graph, cm2,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("sid_multivisit"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{idA1: schema.UserMessage("ans_visit1")}),
	)
	ie2, _ := compose_v2.IsInterrupt(err)
	if ie2 == nil {
		t.Fatalf("Run 2 expected interrupt at A's second visit, got err=%v visitedA=%d visitB=%d", err, visitedA, visitB)
	}
	idA2 := ie2.Query.InterruptID
	t.Logf("Run 2 A 第二次访问抛 ID = %s (visitedA=%d visitB=%d)", idA2, visitedA, visitB)

	if idA2 == idA1 {
		t.Fatalf("Bug #1 reproduced: A 第二次访问的 InterruptID 与第一次相同 (%s) → 跨完成访问串台\n"+
			"sanity-revert：去掉 NodeVisits 持久化或 advanceVisitsLocked 的 visit 自增，测试 fail",
			idA1)
	}
}

// TestRepro_AgentAsTool_GraphNSIsolation 验证 audit Round 3 Bug #3 修复：
// AgentAsTool 嵌套调用时 push graphNS 让父子 graph 节点 ask_user 不串台。
//
// 之前 bug：父子 BasicAgent 都用同 nodeID="tools"（agent_v2.BasicAgentBuilder 内置），
// 都用空 graphNS → ask_user idx=0 算同 ID → 串台。
//
// 修复：AgentAsTool.Call 入口 push graphNS = parent_ns + "/" + sub_agent_name。
//
// 这里手写父子各自调 ask_user 验证 ID 不同。
func TestRepro_AgentAsTool_GraphNSIsolation(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(agent_v2.NewAskUserTool())

	// 父图节点 "shared_node" 调 ask_user 抛 ID_parent
	parentAskNode := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		_, err := tk.Call(ctx, agent_v2.AskUserToolName, map[string]any{"question": "parent_Q"})
		return nil, err
	}

	// 模拟 sub-agent 的"shared_node"——同样 nodeID 同样 ask_user 调用
	// 通过手动构造一个 sub-agent 嵌入：用 fakeSubAgent 直接复用同名 graph
	subGraph, _ := compose_v2.NewBuilder().
		AddNode("shared_node", parentAskNode).
		AddEdge(compose_v2.StartNode, "shared_node").
		AddEdge("shared_node", compose_v2.EndNode).
		Compile()

	// 父图先调 sub-agent（通过手写 NodeFunc 模拟 AgentAsTool.Call 内部行为）
	// 第二次直接调 ask_user
	step := 0
	parentNodeFunc := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		step++
		if step == 1 {
			// 模拟 AgentAsTool 行为：push graphNS 后跑 sub graph
			subCtx := compose_v2.WithGraphNS(ctx, compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), "sub_agent"))
			subCM := compose_v2.NewChannelManager()
			cper := compose_v2.GetCheckpointer(ctx)
			sid := compose_v2.GetSessionID(ctx)
			runOpts := []compose_v2.RunOption{
				compose_v2.WithRunNS(compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), "sub_agent")),
			}
			if cper != nil {
				runOpts = append(runOpts, compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
			}
			_ = subCtx
			return nil, compose_v2.Run(ctx, subGraph, subCM, runOpts...)
		}
		// 父级第二次直接调 ask_user
		_, err := tk.Call(ctx, agent_v2.AskUserToolName, map[string]any{"question": "parent_direct"})
		return nil, err
	}

	parentGraph, _ := compose_v2.NewBuilder().
		AddNode("shared_node", parentNodeFunc).
		AddEdge(compose_v2.StartNode, "shared_node").
		AddEdge("shared_node", compose_v2.EndNode).
		Compile()

	cper := compose_v2.NewInMemoryCheckpointer()
	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), parentGraph, cm,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("sid_isolation"),
	)
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("expected interrupt from sub-agent ask_user, got %v", err)
	}
	subAskID := ie.Query.InterruptID
	t.Logf("Sub-agent (graphNS='sub_agent', nodeID='shared_node') 抛 ID = %s", subAskID)

	// 验证 sub-agent 的 InterruptID 与父级 "shared_node" 第一次访问的 ID 不同
	// （否则就是 graphNS 没 push）。算父级"shared_node"在 graphNS="" + visit=0 + idx=0 的 ID
	// 手工构造（不走 ctx helper）——本测试要独立算出"父级同名节点同位置"的 ID
	// 来对比，走 helper 就成了自己跟自己比、验不出 graphNS 隔离
	parentAskID := compose_v2.InterruptSite{NodeID: "shared_node"}.InterruptID()
	if subAskID == parentAskID {
		t.Fatalf("Bug #3 reproduced: sub-agent 的 InterruptID 与父级同名节点同位置 ID 相同 (%s)\n"+
			"修复方向：AgentAsTool / 子图入口必须 push graphNS",
			subAskID)
	}
	t.Logf("OK — sub-agent ID (%s) != parent calculated ID (%s)", subAskID, parentAskID)
}
