package compose_v2_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestInterruptJoin_Resume_ConvergesNoDeadloop 是复审 graph_loop.go:695 修复健全性的
// 端到端收敛测试（不是 asInterrupt 单元级——interrupt_join_test.go 已覆盖单元）。
//
// 场景精确复刻 695 路径：同一 fan-out 超步内
//   - nodeInt 抛 InterruptError（模拟 ask_user，幂等：答案注入后不再抛）
//   - nodeBad 作为 NodeFunc 成功，但写一个**未注册 channel** → 超步合并点
//     cm.ApplyAll(res.SuccessWrites) 确定性失败（channel 注册是静态的，Resume 重跑必再失败）
//   - graph_loop.go:656 记录中断 "qA"；675→676 ApplyAll 失败；694 res.FirstErr!=nil
//     → return errors.Join(InterruptError, applyErr)
//
// **复审核心问题**：修复把这条路径从"IsInterrupt=false 致命错"变"IsInterrupt=true 可 Resume"。
// 那个"确定性 ApplyAll 失败"会不会在 Resume 重跑整超步时**再失败 → 再 errors.Join → 再被当
// 中断 → 业务反复 Resume 永不收敛的死循环**？
//
// **结论（本测试钉死）**：不会死循环。答案注入后 nodeInt 不再抛中断 → 该超步
// res.FirstErr==nil → 695 路径不再触发（`if res.FirstErr != nil` 为假）→ 走 697
// `return applyErr` 返**裸** ApplyAll 错（非 errors.Join 包 InterruptError）→
// IsInterrupt=false → 业务按标准契约（只在 IsInterrupt=true 时 Resume）停止。收敛。
//
// 换言之：确定性 ApplyAll 失败在 Resume 后**降级为致命错浮现**，而不是伪装成可 Resume 中断。
func TestInterruptJoin_Resume_ConvergesNoDeadloop(t *testing.T) {
	goodCh := compose_v2.LastValue[string]("ijr_good")
	// badCh 故意不 WithChannel 注册 → nodeBad 写它时 ApplyAll phase-1 预校验必失败
	badCh := compose_v2.LastValue[string]("ijr_bad_unregistered")

	var nodeIntAnsweredCount atomic.Int64 // Resume 后 nodeInt 走"已答"分支的次数

	build := func() *compose_v2.Graph {
		b := compose_v2.NewBuilder().
			AddNode("node_int", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				// 幂等 ask_user：答案注入即成功、不再抛中断
				if ans, ok := compose_v2.GetResumeAnswerText(ctx, "qA"); ok && ans != "" {
					nodeIntAnsweredCount.Add(1)
					return []compose_v2.ChannelWrite{compose_v2.Write(goodCh, "answered:"+ans)}, nil
				}
				return nil, &compose_v2.InterruptError{
					Reason: "ask_user",
					Query:  &compose_v2.AskQuery{InterruptID: "qA", Question: "continue?"},
				}
			}).
			AddNode("node_bad", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				// NodeFunc 层成功，但写未注册 channel → 超步 ApplyAll 确定性失败
				return []compose_v2.ChannelWrite{compose_v2.Write(badCh, "x")}, nil
			}).
			// 两条 Start 出边 → 初始 frontier = {node_int, node_bad} 同超步并发
			AddEdge(compose_v2.StartNode, "node_int").
			AddEdge(compose_v2.StartNode, "node_bad").
			AddEdge("node_int", compose_v2.EndNode).
			AddEdge("node_bad", compose_v2.EndNode)
		b = compose_v2.WithChannel(b, goodCh) // 只注册 goodCh，badCh 故意漏
		g, err := b.Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		return g
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	const sid = "ijr_sid"

	// ── Run 1：应命中 695，返回可被 IsInterrupt 穿透的 errors.Join ──
	g1 := build()
	cm1 := compose_v2.NewChannelManager()
	err1 := compose_v2.Run(context.Background(), g1, cm1,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sid),
	)
	if err1 == nil {
		t.Fatal("Run 1 应返回 error（中断 + ApplyAll 失败）")
	}
	ie1, ok1 := compose_v2.IsInterrupt(err1)
	if !ok1 {
		t.Fatalf("Run 1：修复应让 errors.Join(InterruptError, applyErr) 被 IsInterrupt 穿透 = true；\n"+
			"得到 false（= 修复前 bug：HITL 中断被误判致命错，Resume 闭环静默失效）\nerr=%v", err1)
	}
	// 中断 query 必须已 record 进 cp（否则 Resume 无 ID 可配答案）
	var sawQA bool
	for _, q := range ie1.AllQueries() {
		if q.InterruptID == "qA" {
			sawQA = true
		}
	}
	if !sawQA {
		t.Fatalf("Run 1：InterruptError 应含 qA query，got %+v", ie1.AllQueries())
	}

	// ── Run 2（Resume）：注入答案，重跑整超步 ──
	// 关键断言：nodeInt 幂等 → res.FirstErr 变 nil → 695 不再触发 → 返**裸** applyErr
	// → IsInterrupt=false → 收敛（业务停止，不死循环）。
	g2 := build()
	cm2 := compose_v2.NewChannelManager()
	err2 := compose_v2.Run(context.Background(), g2, cm2,
		compose_v2.WithCheckpointer(cper),
		compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			"qA": schema.UserMessage("yes"),
		}),
	)
	if err2 == nil {
		t.Fatal("Run 2：ApplyAll 确定性失败仍在 → 应返回 error（但应是裸致命错，非中断）")
	}
	if _, ok2 := compose_v2.IsInterrupt(err2); ok2 {
		t.Fatalf("Run 2 死循环风险确认：Resume 后仍返回被识别为中断的 error → 业务会反复 Resume 永不收敛。\nerr=%v", err2)
	}
	if !strings.Contains(err2.Error(), "not registered") {
		t.Fatalf("Run 2：应把确定性 ApplyAll 失败作为致命错浮现（含 'not registered'），got %v", err2)
	}
	if got := nodeIntAnsweredCount.Load(); got < 1 {
		t.Fatalf("Run 2：答案应被正确配对投递给 node_int（cp pending query + frontier Resume 正确），"+
			"nodeInt 走'已答'分支次数=%d，应 >=1", got)
	}

	t.Logf("收敛证据：Run1 IsInterrupt=true（fix 生效），Run2 IsInterrupt=false + 含 'not registered'"+
		"（确定性失败降级为致命错浮现，不伪装可 Resume 中断）；nodeInt 已答分支命中 %d 次",
		nodeIntAnsweredCount.Load())
}

// TestInterruptJoin_Resume_NonIdempotentIsBusinessBug 证明真正的"反复 Resume 死循环"
// 只在业务节点**非幂等**（无视答案每次都抛中断）时出现——而这类节点**即便没有
// ApplyAll 失败也会**在重复 Resume 下死循环。故 695 的 errors.Join 修复**不引入新的
// 死循环类**：它只是让本就该可 Resume 的中断被正确识别。
//
// 本测试用 bounded loop（≤5 次）验证：
//   - 幂等节点：1 次 Resume 内 IsInterrupt 转 false（收敛）
//   - 非幂等节点（对照）：始终 IsInterrupt=true（业务契约违规，与本 fix 无关）
func TestInterruptJoin_Resume_NonIdempotentIsBusinessBug(t *testing.T) {
	// 纯中断节点（无 ApplyAll 失败），验证"非幂等 = 天生死循环"与本 fix 正交。
	run := func(idempotent bool) (resumeCount int, converged bool) {
		var answered atomic.Bool
		build := func() *compose_v2.Graph {
			b := compose_v2.NewBuilder().
				AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					if idempotent {
						if _, ok := compose_v2.GetResumeAnswerText(ctx, "q"); ok {
							answered.Store(true)
							return nil, nil
						}
					}
					// 非幂等：无视答案恒抛
					return nil, &compose_v2.InterruptError{
						Query: &compose_v2.AskQuery{InterruptID: "q", Question: "?"},
					}
				}).
				AddEdge(compose_v2.StartNode, "ask").
				AddEdge("ask", compose_v2.EndNode)
			g, _ := b.Compile()
			return g
		}
		cper := compose_v2.NewInMemoryCheckpointer()
		sid := "nonidem_sid"
		cm := compose_v2.NewChannelManager()
		err := compose_v2.Run(context.Background(), build(), cm,
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
		if _, ok := compose_v2.IsInterrupt(err); !ok {
			return 0, true
		}
		// 模拟业务标准契约：IsInterrupt=true 就 Resume，最多 5 次
		for i := 0; i < 5; i++ {
			resumeCount++
			cmR := compose_v2.NewChannelManager()
			err = compose_v2.Run(context.Background(), build(), cmR,
				compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
				compose_v2.WithResumeAnswers(map[string]*schema.Message{"q": schema.UserMessage("yes")}))
			if _, ok := compose_v2.IsInterrupt(err); !ok {
				return resumeCount, true
			}
		}
		return resumeCount, false
	}

	if rc, conv := run(true); !conv || rc > 1 {
		t.Fatalf("幂等节点应 1 次 Resume 内收敛，got resumeCount=%d converged=%v", rc, conv)
	}
	if _, conv := run(false); conv {
		t.Fatal("非幂等节点应始终不收敛（证明死循环是业务契约违规，与 695 fix 正交）")
	}
}
