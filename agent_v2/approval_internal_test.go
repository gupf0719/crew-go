package agent_v2

import (
	"context"
	"strings"
	"sync"
	"testing"

	"crew-go/schema"
	"crew-go/tool"
)

// TestFrameworkMinted_ByInstanceNotName：「是不是框架工具」看实例上的封闭标记、不看名字——框架构造的每一件
// 都带标记（审批等闸据它豁免）；业务自己写一件同名的不带，照常受审批。曾经按名字判，业务工具恰好叫 respond /
// plan / final_output 就绕过审批。
func TestFrameworkMinted_ByInstanceNotName(t *testing.T) {
	framework := map[string]tool.Tool{
		"ask_user":          NewAskUserTool(),
		"final_output":      frameworkFinalOutput{Tool: newFinalOutputTool(&outputConfig{name: "s", schema: map[string]any{"type": "object"}})},
		"activate_skill":    newSkillTool(nil),
		"run_skill_action":  newSkillActionTool(nil),
		"tool_search":       newToolSearchTool(&ToolSearchConfig{}, nil),
		"transfer_to_agent": markFrameworkTool(newHandoffTransferTool(nil)),
		"route_to":          markFrameworkTool(newRouteTool()),
		"桩":                 markMinted(newRouteToolStub()),
	}
	for what, tl := range framework {
		if !isFrameworkMinted(tl) {
			t.Errorf("框架的 %s 应带标记（审批等闸据它豁免）", what)
		}
	}
	noop := func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("ok"), nil }
	for _, n := range []string{AskUserToolName, FinalOutputToolName, planToolName, respondToolName,
		HandoffTransferToolName, RouteTool, ActivateSkillToolName, "do_thing"} {
		if isFrameworkMinted(tool.NewFuncTool(n, n, map[string]any{"type": "object"}, noop)) {
			t.Errorf("业务自己写的 %q 不该被当成框架工具（应受审批）", n)
		}
	}
	if isFrameworkMinted(nil) || isFrameworkMintedIn(nil, AskUserToolName) {
		t.Error("没有工具时不是框架工具")
	}
}

// TestToolApprovedRule don't-ask-again 规则的两档语义：**这件事** vs **这个工具**。
//
// 核心是空间维度：一条授权的有效期不该长于「用户还能预期到这个动作会发生」的范围。
// 我们的时间维度是「整个 session 且永不失效」，所以空间维度必须收到「这次要做的这件事」
// ——否则一次 `transfer_money(张三, 5000)` 的放行会顺带批准**任意金额转给任意人**。
func TestToolApprovedRule(t *testing.T) {
	call := func(name, args string) schema.ToolCall {
		return schema.ToolCall{ID: "c", Function: schema.FunctionCall{Name: name, Arguments: args}}
	}
	transfer5000 := call("transfer_money", `{"to":"张三","amount":5000}`)
	transfer9999 := call("transfer_money", `{"to":"李四","amount":9999}`)
	other := call("read_file", `{"path":"a.txt"}`)
	cfg := &Config{}

	if isToolApproved(nil, cfg, transfer5000) {
		t.Error("nil session 应返 false 不 panic")
	}

	t.Run("approve_always 只批这件事", func(t *testing.T) {
		s := NewRunSession()
		if isToolApproved(s, cfg, transfer5000) {
			t.Fatal("未标记不应命中")
		}
		markToolApproved(s, cfg, transfer5000, false)
		if !isToolApproved(s, cfg, transfer5000) {
			t.Error("同工具同参数应命中")
		}
		// **承重**：换了参数就是另一件事，必须重新问
		if isToolApproved(s, cfg, transfer9999) {
			t.Error("批了「转张三 5000」不该顺带批准「转李四 9999」——" +
				"这正是把 key 从裸工具名收紧到参数级要挡住的")
		}
		if isToolApproved(s, cfg, other) {
			t.Error("不同工具不该命中")
		}
	})

	t.Run("approve_always_tool 批整个工具", func(t *testing.T) {
		s := NewRunSession()
		markToolApproved(s, cfg, transfer5000, true)
		if !isToolApproved(s, cfg, transfer9999) {
			t.Error("工具级规则应覆盖该工具的任何调用")
		}
		if isToolApproved(s, cfg, other) {
			t.Error("工具级规则不该外溢到别的工具")
		}
	})

	t.Run("业务可插的指纹归一化", func(t *testing.T) {
		// 业务说「同一个收款人算同一件事，金额不算」
		scoped := &Config{ApprovalScope: func(tc schema.ToolCall) string {
			if strings.Contains(tc.Function.Arguments, `"张三"`) {
				return "to=张三"
			}
			return tc.Function.Arguments
		}}
		s := NewRunSession()
		markToolApproved(s, scoped, transfer5000, false)
		same := call("transfer_money", `{"to":"张三","amount":1}`)
		if !isToolApproved(s, scoped, same) {
			t.Error("按业务归一化后应算同一件事")
		}
		if isToolApproved(s, scoped, transfer9999) {
			t.Error("归一化后不同的仍应重新问")
		}
	})

	t.Run("多条规则并存不互相覆盖", func(t *testing.T) {
		s := NewRunSession()
		markToolApproved(s, cfg, transfer5000, false)
		markToolApproved(s, cfg, other, false)
		if !isToolApproved(s, cfg, transfer5000) || !isToolApproved(s, cfg, other) {
			t.Error("二次写入不应覆盖首次（read-modify-write）")
		}
	})
}

// TestCallApprovalFns 「这次调用的审批记录」读写回调里「批过没有」那一对。
//
// 它原先按**稳定 InterruptID** 存 `RunSession` KV。那个 ID 的五个维度
// `(graphNS, nodeID, visit, branchIdx, idx)` **在新的顶层调用里全部重置** ⟹ 任务 B 的
// 第一个工具调用跟任务 A 的算出同一个 ID ⟹ 任务 A 批过的在任务 B 里被静默命中。
//
// 现在它挪进了 tools 节点的**节点私有中断态**（`toolCallMemo.Approved`），
// 由节点注入读写对。跨任务隔离**不再靠任何清除动作**——新任务是新 cp、NodeStates 为空。
// 详见 `toolCallMemo.Approved` 的注释。
func TestCallApprovalFns(t *testing.T) {
	// 没注入时不 panic、恒 false（业务直接调 checkToolApproval 而不经 tools 节点的形态）
	if isCallApproved(context.Background()) {
		t.Error("未注入时应返 false")
	}
	markCallApproved(context.Background()) // 不该 panic

	memo := &toolCallMemo{}
	var mu sync.Mutex
	ctx := withCallApproval(context.Background(), callApprovalFns{
		is: func() bool { mu.Lock(); defer mu.Unlock(); return memo.Approved["k1"] },
		mark: func() {
			mu.Lock()
			defer mu.Unlock()
			if memo.Approved == nil {
				memo.Approved = map[string]bool{}
			}
			memo.Approved["k1"] = true
		},
	})

	if isCallApproved(ctx) {
		t.Error("未标记应未批准")
	}
	markCallApproved(ctx)
	if !isCallApproved(ctx) {
		t.Error("标记后应批准")
	}

	// 承重：跟 don't-ask-again 规则集是两套独立的东西，互不污染
	s := NewRunSession()
	anyCall := schema.ToolCall{ID: "k1", Function: schema.FunctionCall{Name: "k1", Arguments: "{}"}}
	if isToolApproved(s, &Config{}, anyCall) {
		t.Error("「这次调用批过」不该让 don't-ask-again 规则命中")
	}
}

// TestApprovalDecisionOf_WithoutRecordFns：没注入审批记录回调时（不经 tools 节点的形态）不 panic、
// 每次现算；注入后只形成一次、之后重放，重放不看现在的配置。重放的端到端行为（含内容固定）见
// approval_heuristic_replay_test.go。下面的启发式都不 panic，第三个返回值（错误）恒为 nil，故不检查。
func TestApprovalDecisionOf_WithoutRecordFns(t *testing.T) {
	calls := 0
	cfg := &Config{ApprovalHeuristic: func(context.Context, string, map[string]any) ApprovalDecision {
		calls++
		if calls == 1 {
			return ApprovalDecisionAsk
		}
		return ApprovalDecisionAllow
	}}
	if d, on, _ := approvalDecisionOf(context.Background(), cfg, "t", nil); d != ApprovalDecisionAsk || !on {
		t.Fatalf("第一次应得 Ask，实际 %v / %v", d, on)
	}
	if d, _, _ := approvalDecisionOf(context.Background(), cfg, "t", nil); d != ApprovalDecisionAllow || calls != 2 {
		t.Fatalf("没注入记录回调时每次现算：第二次应得 Allow 且启发式被调 2 次，实际 %v / %d 次", d, calls)
	}

	calls = 0
	var recorded *ApprovalDecision
	ctx := withCallApproval(context.Background(), callApprovalFns{
		decided: func() (ApprovalDecision, bool) {
			if recorded == nil {
				return 0, false
			}
			return *recorded, true
		},
		decide: func(d ApprovalDecision) { recorded = &d },
	})
	for i := 0; i < 3; i++ {
		if d, _, _ := approvalDecisionOf(ctx, cfg, "t", nil); d != ApprovalDecisionAsk {
			t.Fatalf("第 %d 次应重放第一次的 Ask，实际 %v", i+1, d)
		}
	}
	if calls != 1 {
		t.Fatalf("注入记录回调后启发式应只被调 1 次，实际 %d 次", calls)
	}
	// 记下的结论不看现在的配置：审批关掉了照样重放
	if d, on, _ := approvalDecisionOf(ctx, &Config{}, "t", nil); d != ApprovalDecisionAsk || !on {
		t.Fatalf("已经形成的结论在审批关掉后应照样重放，实际 %v / %v", d, on)
	}

	// 没配启发式的 ApprovalAsk 也记：结论是 mode 默认
	recorded = nil
	if d, on, _ := approvalDecisionOf(ctx, &Config{ApprovalMode: ApprovalAsk}, "t", nil); d != ApprovalDecisionAsk || !on || recorded == nil || *recorded != ApprovalDecisionAsk {
		t.Fatalf("ApprovalAsk 无启发式应得 Ask 并记下，实际 %v / %v / recorded=%v", d, on, recorded)
	}

	// 审批不生效、也没有记下的结论：不生效、不写记录
	recorded = nil
	if _, on, _ := approvalDecisionOf(ctx, &Config{}, "t", nil); on || recorded != nil {
		t.Fatalf("审批不生效时应报不生效、且不写记录，实际 on=%v / recorded=%v", on, recorded)
	}
}
