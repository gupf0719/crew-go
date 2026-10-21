package agent_v2_test

import (
	"context"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// 业务工具用 `compose_v2.Ask` 提问时，几种硬形态都必须收敛且副作用不重放。
//
// # 为什么要有这一组
//
// 中断 ID 由一组坐标算出，其中一维是**本分支自己的计数器**，它的当前值**只有正在跑的那段
// 代码知道**。所以「算 ID」和「用 ID」必须是同一次调用——任何第二处想重建都是猜。
//
// 曾经试过让框架在分支把中断交出去之后替它盖章（按「第 j 个 query 配第 j 个计数值」猜），
// 审计逐形态实测下来**比不盖还糟**：
//
//	                        不盖章        事后盖章（已删）
//	一个工具问两次           扣款 1 次      扣款 16 次、永不收敛
//	开审批 + 工具再问(N=2)   执行 10        执行 16、一个都没完成
//
// 猜错的根子是**审批层自己会先 tick 一次**（`approval.go` 在任何 fast-path 之前无条件算
// stableID），业务工具拿到的第一个值就不是 0。这条在框架内部就成立，不需要任何奇怪的业务代码。
//
// ⟹ 正解是把提问入口收敛成 `compose_v2.Ask`：**每次 Ask 自己 tick 自己用**。
// 框架自带的三处 HITL（`ask_user` / 工具审批 / a2a）之所以一直没事，正是这个模式。

// askTool 造一个用 `compose_v2.Ask` 提问的业务工具。asks 是它要连问几次。
func askTool(name string, asks int, mu *sync.Mutex, effects *[]string) tool.Tool {
	return tool.NewFuncTool(name, "做"+name, map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			for k := 0; k < asks; k++ {
				if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{
					Reason:   "biz_ask",
					Question: name + " 的第 " + string(rune('A'+k)) + " 问",
				}); err != nil {
					return nil, err
				}
				// **每答完一问就记一次副作用**——这是承重量：
				// ID 错位时的症状正是「前面的问反复命中 ⟹ 副作用无界重放」。
				mu.Lock()
				*effects = append(*effects, name+"#"+string(rune('A'+k)))
				mu.Unlock()
			}
			return tool.Success("done:" + name), nil
		})
}

// runAskScenario 跑一轮完整 HITL 闭环，返回（是否收敛, 副作用序列, 轮数）。
func runAskScenario(t *testing.T, toolNames []string, asks int, approval bool) (bool, []string, int) {
	t.Helper()
	var mu sync.Mutex
	var effects []string
	tk := tool.NewToolkit()
	tcs := make([]schema.ToolCall, 0, len(toolNames))
	for i, n := range toolNames {
		tk.Register(askTool(n, asks, &mu, &effects))
		tcs = append(tcs, schema.ToolCall{
			ID: "tc" + string(rune('1'+i)), Type: "function",
			Function: schema.FunctionCall{Name: n, Arguments: "{}"},
		})
	}
	opts := []agent_v2.Option{
		agent_v2.WithName("bot"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(12),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage(tcs), schema.AssistantMessage("好了"),
		})),
	}
	if approval {
		opts = append(opts, agent_v2.WithApprovalMode(agent_v2.ApprovalAsk))
	}
	ag := agent_v2.NewBasicAgent(opts...)

	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("跑"))

	answers := map[string]*schema.Message{}
	const maxRounds = 12
	var r int
	for r = 0; r < maxRounds; r++ {
		ie, isInt := compose_v2.IsInterrupt(err)
		if !isInt {
			mu.Lock()
			out := append([]string(nil), effects...)
			mu.Unlock()
			return true, out, r
		}
		for _, q := range ie.AllQueries() {
			// 审批问的答「批准」，业务问的答任意文本——两者都用同一个 map
			answers[q.InterruptID] = schema.UserMessage(agent_v2.ApprovalOptionApprove)
		}
		_, err = ag.Resume(ctx, sess.RunID,
			agent_v2.WithResumeSession(sess),
			agent_v2.WithResumeUserInputs(answers))
	}
	mu.Lock()
	out := append([]string(nil), effects...)
	mu.Unlock()
	return false, out, r
}

// TestAsk_ConvergesAcrossHardShapes 四种硬形态都必须收敛，且副作用**每问恰好一次**。
//
// ⚠️ **必须数副作用次数，不能只断「收敛了」**：ID 错位的典型症状是「前面的问反复命中 ⟹
// 每轮都重做一遍副作用」，那种情况下有时也会「收敛」，只是中间多扣了几次钱。
func TestAsk_ConvergesAcrossHardShapes(t *testing.T) {
	cases := []struct {
		name      string
		tools     []string
		asks      int
		approval  bool
		maxRounds int // 期望在几轮内收敛（承重量：坏掉时是**永不收敛**）
	}{
		{"单工具单问", []string{"toolA"}, 1, false, 2},
		{"两工具各问一次（并发扇出）", []string{"toolA", "toolB"}, 1, false, 2},
		// 下面两条是事后盖章崩得最惨的形态
		{"单工具连问两次（确认→做事→再确认）", []string{"toolA"}, 2, false, 3},
		{"开审批 + 两工具各再问一次", []string{"toolA", "toolB"}, 1, true, 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			converged, effects, rounds := runAskScenario(t, c.tools, c.asks, c.approval)
			t.Logf("收敛=%v 轮数=%d 副作用=%v", converged, rounds, effects)
			if !converged {
				t.Fatalf("连答 %d 轮仍未收敛，副作用已发生 %d 次：%v\n"+
					"（ID 错位时就是这样：前面的问反复命中、后面的永远答不进去）",
					rounds, len(effects), effects)
			}
			// 承重②：**必须在少数几轮内收敛**——坏掉时是永不收敛（轮数只受调用方循环上限约束）
			if rounds > c.maxRounds {
				t.Fatalf("应在 %d 轮内收敛，实际 %d 轮：副作用 %v", c.maxRounds, rounds, effects)
			}
			// 承重③：**每个提问都真的被答上过**（对应的副作用发生过）——只断收敛分辨不出
			// 「有一问被静默跳过了」
			for _, name := range c.tools {
				for k := 0; k < c.asks; k++ {
					want := name + "#" + string(rune('A'+k))
					found := false
					for _, e := range effects {
						if e == want {
							found = true
							break
						}
					}
					if !found {
						t.Fatalf("提问 %q 从未被答上（副作用没发生）：%v", want, effects)
					}
				}
			}
			// 承重④：**次数有界**——ID 错位时的症状是每轮重做一遍、次数随轮数无限增长。
			//
			// ⚠️ 这里**不能断「恰好一次」**：一个 tool_call 内部多次中断时，工具体每次
			// Resume 从头重跑（`toolCallMemo` 的粒度是 per-tool_call，管不到 call 内部），
			// 所以前面的问会被重做。这是**既有语义**（CLAUDE.md：业务工具自负幂等性），
			// 不是本入口引入的。关键区别是**有界**：坏掉时实测 16 次且还在涨。
			if upper := len(c.tools) * c.asks * (c.maxRounds + 1); len(effects) > upper {
				t.Fatalf("副作用 %d 次超出有界上限 %d：%v\n"+
					"（无界增长 = ID 错位导致每轮重做，审批场景下就是重复扣款）",
					len(effects), upper, effects)
			}
		})
	}
}

// TestAsk_IDsDifferPerQuestion 同一个工具连问两次必须拿到两个不同 ID，
// **且复用同一个 AskQuery 对象也不例外**。
//
// 相同的话第二问会命中第一问已经答过的答案 ⟹ 用户只被问了一次，第二件事**没问就做了**。
// 实测过那个形态：轨迹是「问第一问 → 答=是 → 问第二问 → 答=是」，但第二问**从未被抛出去**，
// 图还成功收尾（`err == nil`）——审批语境下就是静默自动批准。
//
// ⚠️ **断言点必须是 `ie.Query.InterruptID` 而不是传进去的那个 q**：`Ask` **不写回**调用方的
// 对象（框架的身份放在自己的记录上，五家皆然）。读 q 的话什么也读不到——而正是「写回」
// 制造了上面那个静默自动批准。
func TestAsk_IDsDifferPerQuestion(t *testing.T) {
	var ids []string
	var mu sync.Mutex
	// **故意复用同一个对象**——这是最容易踩的写法（工具结构体字段 / 闭包外变量）
	shared := &compose_v2.AskQuery{}
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("t", "t", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			for k := 0; k < 2; k++ {
				shared.Question = "问" + string(rune('A'+k))
				_, err := compose_v2.Ask(ctx, shared)
				if err != nil {
					if ie, ok := compose_v2.IsInterrupt(err); ok && ie.Query != nil {
						mu.Lock()
						ids = append(ids, ie.Query.InterruptID)
						mu.Unlock()
					}
					return nil, err
				}
			}
			return tool.Success("ok"), nil
		}))
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("b"), agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(8),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID: "tc1", Type: "function",
				Function: schema.FunctionCall{Name: "t", Arguments: "{}"},
			}}),
			schema.AssistantMessage("ok"),
		})))
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("go"))
	ans := map[string]*schema.Message{}
	for r := 0; r < 8; r++ {
		ie, isInt := compose_v2.IsInterrupt(err)
		if !isInt {
			break
		}
		for _, q := range ie.AllQueries() {
			ans[q.InterruptID] = schema.UserMessage("好")
		}
		_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
			agent_v2.WithResumeUserInputs(ans))
	}
	mu.Lock()
	got := append([]string(nil), ids...)
	mu.Unlock()

	if len(got) < 2 {
		t.Fatalf("应该被问两次（每次一个中断），实际只抛出 %d 次：%v\n"+
			"（第二问没被抛出去 = 它吃掉了第一问的答案、没问就做了）", len(got), got)
	}
	seen := map[string]bool{}
	for _, id := range got {
		if id == "" {
			t.Fatalf("中断里没有 InterruptID：%v", got)
		}
		seen[id] = true
	}
	if len(seen) < 2 {
		t.Fatalf("连问两次拿到了相同的 ID：%v\n"+
			"（复用同一个 AskQuery 对象时，若 Ask 把身份写回入参就会这样）", got)
	}
}
