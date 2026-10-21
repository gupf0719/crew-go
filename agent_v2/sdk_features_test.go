package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// ── HistoryFor cross-agent rewrite ──

// TestHistoryFor_RewritesCrossAgentMessages 验证 HistoryFor：
//   - 自 agent 消息原样返
//   - 跨 agent 的 assistant(content) 改写为 user("For context: [name] said: ...")
//   - 跨 agent 的 assistant(tool_call) 改写为 user("For context: [name] called tool: ...")
//   - 跨 agent 的 tool 消息改写为 user("For context: [name] tool returned result: ...")
func TestHistoryFor_RewritesCrossAgentMessages(t *testing.T) {
	sess := agent_v2.NewRunSession()

	// 自 agent A 的 user 输入 + assistant 响应
	sess.AddEvent(&agent_v2.AgentEvent{
		AgentName: "agentA",
		Output:    &agent_v2.AgentOutput{Message: schema.UserMessage("hi")},
	})
	sess.AddEvent(&agent_v2.AgentEvent{
		AgentName: "agentA",
		Output:    &agent_v2.AgentOutput{Message: schema.AssistantMessage("hello A")},
	})

	// agent B 的 assistant + tool_call + tool_result（跨 agent，应被改写）
	sess.AddEvent(&agent_v2.AgentEvent{
		AgentName: "agentB",
		Output:    &agent_v2.AgentOutput{Message: schema.AssistantMessage("from B")},
	})
	sess.AddEvent(&agent_v2.AgentEvent{
		AgentName: "agentB",
		Output: &agent_v2.AgentOutput{Message: schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "tc_1", Function: schema.FunctionCall{Name: "search", Arguments: `{"q":"x"}`}},
		})},
	})
	sess.AddEvent(&agent_v2.AgentEvent{
		AgentName: "agentB",
		Output:    &agent_v2.AgentOutput{Message: schema.ToolResultMessage("tc_1", "result data")},
	})

	out := sess.HistoryFor("agentA")
	if len(out) != 5 {
		t.Fatalf("expected 5 messages, got %d", len(out))
	}

	// 前两条应原样（自 agent）
	if out[0].Role != schema.RoleUser || out[0].Content != "hi" {
		t.Fatalf("self user msg modified: %+v", out[0])
	}
	if out[1].Role != schema.RoleAssistant || out[1].Content != "hello A" {
		t.Fatalf("self assistant msg modified: %+v", out[1])
	}

	// 第 3 条：B 的 assistant content 应改写为 user("For context: [agentB] said: from B.")
	if out[2].Role != schema.RoleUser {
		t.Fatalf("cross-agent assistant should be rewritten to user role, got %v", out[2].Role)
	}
	if !strings.Contains(out[2].Content, "[agentB] said: from B") {
		t.Fatalf("cross-agent rewrite missing 'said:', got %q", out[2].Content)
	}

	// 第 4 条：B 的 tool_call 应改写为 user("For context: [agentB] called tool: `search` with arguments: ...")
	if out[3].Role != schema.RoleUser {
		t.Fatalf("cross-agent tool_call should be rewritten to user, got %v", out[3].Role)
	}
	if !strings.Contains(out[3].Content, "called tool: `search`") {
		t.Fatalf("cross-agent tool_call rewrite missing 'called tool:', got %q", out[3].Content)
	}

	// 第 5 条：B 的 tool_result 应改写为 user("For context: [agentB] tool returned result: ...")
	if out[4].Role != schema.RoleUser {
		t.Fatalf("cross-agent tool result should be rewritten to user, got %v", out[4].Role)
	}
	if !strings.Contains(out[4].Content, "tool returned result: result data") {
		t.Fatalf("cross-agent tool_result rewrite missing 'returned result:', got %q", out[4].Content)
	}
}

// ── ask_user 内置工具 ──

// TestAskUserTool_FirstCallThrowsInterrupt 验证 ask_user tool 第一次调用抛
// InterruptError 含 Question + Options。
//
// 这里用的是**裸 ctx**（不在任何节点里），所以身份为空——见下方断言。
func TestAskUserTool_FirstCallThrowsInterrupt(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(agent_v2.NewAskUserTool())

	res, err := tk.Call(context.Background(), agent_v2.AskUserToolName, map[string]any{
		"question": "what color?",
		"options":  []any{"red", "blue"},
	})
	if err == nil {
		t.Fatal("expected InterruptError, got nil")
	}
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("expected InterruptError, got %v", err)
	}
	if ie.Query == nil || ie.Query.Question != "what color?" {
		t.Fatalf("Query.Question wrong: %+v", ie.Query)
	}
	if len(ie.Query.Options) != 2 || ie.Query.Options[0] != "red" {
		t.Fatalf("Query.Options wrong: %+v", ie.Query.Options)
	}
	// ⚠️ 这一格原先断言的是「框架应该自动填上」——那时它确实非空，但填的是各维全零
	// hash 出来的一个常量（同 ns 下所有图所有节点共用），业务拿它提问会跨任务串台。
	// 「节点内取到的身份非空」由 compose_v2.TestHookSite_InNodeStillWorks 守。
	if ie.Query.InterruptID != "" {
		t.Fatalf("裸 ctx 下不该有身份，实际 %q", ie.Query.InterruptID)
	}
	if res != nil {
		t.Fatalf("expected nil result on interrupt, got %+v", res)
	}
}

// TestAskUserTool_SecondCallWithResumeContextReturnsAnswer 验证 Resume 时第二次
// 调用从 ResumeContext 取答案返 tool.Success(answer)。
//
// **audit Bug B 修复**：之前测试只判 Run 2 没出错——即使 ask_user 返空字符串也
// 通过。改为节点内显式断言 res.Output == "user_answered_yes"，写入 channel
// 让外部能验证。
func TestAskUserTool_SecondCallWithResumeContextReturnsAnswer(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(agent_v2.NewAskUserTool())

	answerCh := compose_v2.LastValue[string]("ask_test_answer")

	ctx := context.Background()
	b := compose_v2.NewBuilder().
		AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			res, err := tk.Call(ctx, agent_v2.AskUserToolName, map[string]any{"question": "?"})
			if err != nil {
				return nil, err
			}
			// 把 tool 输出写到 channel 让外部断言——而非 silent return nil
			content := ""
			if res != nil {
				if s, ok := res.Output.(string); ok {
					content = s
				}
			}
			return []compose_v2.ChannelWrite{compose_v2.Write(answerCh, content)}, nil
		}).
		AddEdge(compose_v2.StartNode, "ask").
		AddEdge("ask", compose_v2.EndNode)
	b = compose_v2.WithChannel(b, answerCh)
	graph, _ := b.Compile()

	cper := compose_v2.NewInMemoryCheckpointer()
	cm1 := compose_v2.NewChannelManager()
	// Run 1: 触发 InterruptError——抓取 framework 自动填的 InterruptID
	err := compose_v2.Run(ctx, graph, cm1,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("sid_ask"),
	)
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("Run 1 expected interrupt, got %v", err)
	}
	if ie.Query == nil || ie.Query.InterruptID == "" {
		t.Fatalf("Query.InterruptID should be auto-filled, got %+v", ie.Query)
	}

	// Run 2: Resume with answer——按 framework 自动填的 InterruptID 精确配对（不用 sentinel）
	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, graph, cm2,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("sid_ask"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("user_answered_yes")}),
	)
	if err != nil {
		t.Fatalf("Run 2 should succeed, got %v", err)
	}

	// **关键断言**：ask_user 必须真返 "user_answered_yes" 给 caller
	got, _ := compose_v2.Get(cm2.Channels(), answerCh)
	if got != "user_answered_yes" {
		t.Fatalf("ask_user should return resume answer, got %q\n"+
			"sanity-revert：ask_user_tool.go 去掉 GetResumeAnswer 检查，本测试 fail",
			got)
	}
}

// TestAskUserTool_MultiNodeNoCrosstalk 验证 audit Bug #1 修复：**多节点**各调
// ask_user 时 InterruptID 按 nodeID 区分不串台——之前 nodeID 传空字符串 → 同
// graphNS 多节点 idx=0 算同 ID → 答案错配。
//
// sanity-revert：ask_user_tool.go 把 GetCurrentNodeID 改回传 ""，本测试 fail——
// node1 node2 两次抛中断的 InterruptID 会是同一个。
func TestAskUserTool_MultiNodeNoCrosstalk(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(agent_v2.NewAskUserTool())

	answerCh := compose_v2.Append[string]("multi_node_answers")

	makeAskNode := func(label string) compose_v2.NodeFunc {
		return func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			res, err := tk.Call(ctx, agent_v2.AskUserToolName, map[string]any{"question": label})
			if err != nil {
				return nil, err
			}
			ans, _ := res.Output.(string)
			return []compose_v2.ChannelWrite{compose_v2.Write(answerCh, []string{label + ":" + ans})}, nil
		}
	}

	// 拓扑：Start → node_a → node_b → End（顺序，不并行）
	b := compose_v2.NewBuilder().
		AddNode("node_a", makeAskNode("nodeA")).
		AddNode("node_b", makeAskNode("nodeB")).
		AddEdge(compose_v2.StartNode, "node_a").
		AddEdge("node_a", "node_b").
		AddEdge("node_b", compose_v2.EndNode)
	b = compose_v2.WithChannel(b, answerCh)
	graph, _ := b.Compile()

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// Run 1: node_a 抛 InterruptID（含 node_a 维度）
	cm1 := compose_v2.NewChannelManager()
	err := compose_v2.Run(ctx, graph, cm1,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("sid_multi_node"),
	)
	ie1, _ := compose_v2.IsInterrupt(err)
	idA := ie1.Query.InterruptID

	// Run 2: 答 node_a，node_b 抛新 InterruptID（含 node_b 维度）—— 必须不同 ID
	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, graph, cm2,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("sid_multi_node"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{idA: schema.UserMessage("ans_a")}),
	)
	ie2, _ := compose_v2.IsInterrupt(err)
	if ie2 == nil || ie2.Query == nil {
		t.Fatalf("Run 2 expected node_b interrupt, got %v", err)
	}
	idB := ie2.Query.InterruptID
	if idB == idA {
		t.Fatalf("InterruptID for node_b should differ from node_a (got both %q)\n"+
			"sanity-revert：ask_user 改回 nodeID=\"\"，多节点同 idx 算同 ID，本断言 fail",
			idA)
	}

	// Run 3: 同时给两个答案 → 两节点正确取到对应答案
	cm3 := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, graph, cm3,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("sid_multi_node"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			idA: schema.UserMessage("ans_a"),
			idB: schema.UserMessage("ans_b"),
		}),
	)
	if err != nil {
		t.Fatalf("Run 3 should succeed, got %v", err)
	}
	got, _ := compose_v2.Get(cm3.Channels(), answerCh)
	want := []string{"nodeA:ans_a", "nodeB:ans_b"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("multi-node answers crosstalk: expected %v, got %v\n"+
			"sanity-revert：nodeID=\"\" → 两节点同 ID → 答案串台",
			want, got)
	}
}

// TestAskUserTool_MultiPendingNoCrosstalk 验证 audit Bug E 修复：同节点内 N 次
// 调 ask_user 时 InterruptID 按 idx 区分，多 pending 答案配对不串台。
//
// 之前 bug：ask_user 用 GetResumeAnswerSingle 取 map 第一条，第二次 ask_user
// 调用拿到第一次的答案 → 串台。
//
// 修复：framework 注入 per-node interrupt counter；ask_user 用 NextInterruptIndex +
// MakeInterruptID 算稳定 ID（同序号跨 retry/Resume 不变；不同序号不同 ID）。
//
// sanity-revert：ask_user_tool.go 改回 GetResumeAnswerSingle，本测试应 fail——
// 第二次调用拿到错答案。
func TestAskUserTool_MultiPendingNoCrosstalk(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(agent_v2.NewAskUserTool())

	answerCh := compose_v2.Append[string]("multi_ask_answers")

	// 节点内连续两次调 ask_user——第一次抛中断后框架 SaveNS；Resume 后第二次也
	// 抛中断（因为只有一个答案匹配 InterruptID #0，#1 没答案）；第三次 Resume
	// 同时给两个答案 → 两次都正确取到对应答案
	b := compose_v2.NewBuilder().
		AddNode("multi_ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			res1, err := tk.Call(ctx, agent_v2.AskUserToolName, map[string]any{"question": "Q1"})
			if err != nil {
				return nil, err
			}
			ans1, _ := res1.Output.(string)

			res2, err := tk.Call(ctx, agent_v2.AskUserToolName, map[string]any{"question": "Q2"})
			if err != nil {
				return nil, err
			}
			ans2, _ := res2.Output.(string)

			return []compose_v2.ChannelWrite{
				compose_v2.Write(answerCh, []string{ans1, ans2}),
			}, nil
		}).
		AddEdge(compose_v2.StartNode, "multi_ask").
		AddEdge("multi_ask", compose_v2.EndNode)
	b = compose_v2.WithChannel(b, answerCh)
	graph, _ := b.Compile()

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// Run 1: 第一次 ask_user 抛 InterruptID #0
	cm1 := compose_v2.NewChannelManager()
	err := compose_v2.Run(ctx, graph, cm1,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("sid_multi"),
	)
	ie1, isInt := compose_v2.IsInterrupt(err)
	if !isInt || ie1.Query == nil {
		t.Fatalf("Run 1 expected interrupt with Query, got %v", err)
	}
	id0 := ie1.Query.InterruptID

	// Run 2: 答 #0，第二次 ask_user 抛 InterruptID #1（不同 ID 不串台）
	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, graph, cm2,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("sid_multi"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{id0: schema.UserMessage("answer_to_Q1")}),
	)
	ie2, isInt := compose_v2.IsInterrupt(err)
	if !isInt || ie2.Query == nil {
		t.Fatalf("Run 2 expected second interrupt, got %v", err)
	}
	id1 := ie2.Query.InterruptID
	if id1 == id0 {
		t.Fatalf("InterruptID for second ask_user should differ from first (got both %q)\n"+
			"sanity-revert：ask_user 改回 GetResumeAnswerSingle，第二次调用拿第一次答案，本断言 fail",
			id0)
	}

	// Run 3: 同时给两个答案 → 节点跑通，两次 ask_user 都正确取到
	cm3 := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, graph, cm3,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID("sid_multi"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			id0: schema.UserMessage("answer_to_Q1"),
			id1: schema.UserMessage("answer_to_Q2"),
		}),
	)
	if err != nil {
		t.Fatalf("Run 3 should succeed with both answers, got %v", err)
	}

	got, _ := compose_v2.Get(cm3.Channels(), answerCh)
	if len(got) != 2 || got[0] != "answer_to_Q1" || got[1] != "answer_to_Q2" {
		t.Fatalf("multi-pending answers crosstalk: expected [answer_to_Q1 answer_to_Q2], got %v\n"+
			"sanity-revert：ask_user 改回 GetResumeAnswerSingle，两次拿同一答案，本断言 fail",
			got)
	}
}
