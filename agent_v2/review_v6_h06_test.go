package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
)

// TestReviewV6_H06_GraphAgentResumeNilCheck 验 review-v6 H-06 fix —
// GraphAgent.Resume 入口加 g.graph / g.outputReader nil 校验, 与 Run 入口对称.
//
// 历史: Run 路径 (line 128-136) 完整校验, Resume 路径只校验 cper. 业务用
// builder NewGraphAgent(nil/.../忘传 WithGraphOutputReader) 直接调 Resume 时,
// 下游 compose_v2.Run(ctx, nil, ...) / g.outputReader(nil) 远 stack panic.
//
// Resume 是公开 API, 业务可独立调用 (无 invariant 强制 Run 先于 Resume).
//
// sanity-revert: 删 H-06 fix 的 nil 校验 block → 测试改为 expect panic,
// 但 nil graph 走到 compose_v2.Run 会远 stack panic 难定位 — 业务踩坑后看到
// stack trace 在 compose_v2 层而非 agent_v2 入口.
func TestReviewV6_H06_GraphAgentResumeNilCheck(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)

	t.Run("nil_graph", func(t *testing.T) {
		// 业务 NewGraphAgent(nil, ...) 错用 — 直接 Resume 应在 agent_v2 层 fail-fast
		// 返业务定位友好的 error, 不退到 compose_v2.Run 内部通用 "graph is nil" 错.
		//
		// audit 揭露 (LOW-1 修): 之前断言 strings.Contains(err.Error(), "graph") 是假绿
		// — compose_v2.Run 已自带 nil 防御返 "graph is nil" (按 graph_loop.go::Run 入口 nil 防御自查),
		// 通用关键词 "graph" 在 fix revert 后仍命中 (CLAUDE.md 5 类炸弹 #4 双层防护遮蔽).
		// 改断言为 H-06 fix 专属字符串 "resume requires graph" 让 sanity-revert 真 fail.
		g := agent_v2.NewGraphAgent(nil, agent_v2.WithGraphAgentName("test"))
		_, err := g.Resume(ctx, "test_sid")
		if err == nil {
			t.Fatal("review-v6 H-06 fix: nil graph 应 fail-fast 返 error, 实际 nil")
		}
		if !strings.Contains(err.Error(), "resume requires graph") {
			t.Errorf("review-v6 H-06 fix: error message 应含 'resume requires graph' (fix 专属), 实际 = %q\n"+
				"sanity-revert: 删 Resume 入口 g.graph nil 校验 → compose_v2.Run 内部返通用 'graph is nil' 错, 不含 fix 专属字符串 → 此处 fail",
				err.Error())
		}
	})

	t.Run("missing_output_reader", func(t *testing.T) {
		// 业务有 graph 但忘传 WithGraphOutputReader — Resume 应 fail-fast
		bld := compose_v2.NewBuilder()
		ch := compose_v2.LastValue[string]("dummy")
		compose_v2.WithChannel(bld, ch)
		bld.AddNode("noop", func(ctx context.Context, ch compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		})
		bld.AddEdge(compose_v2.StartNode, "noop")
		bld.AddEdge("noop", compose_v2.EndNode)
		graph, err := bld.Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}

		g := agent_v2.NewGraphAgent(graph,
			agent_v2.WithGraphAgentName("test"),
			// 故意不传 WithGraphOutputReader
		)
		_, err = g.Resume(ctx, "test_sid")
		if err == nil {
			t.Fatal("review-v6 H-06 fix: 缺 outputReader 应 fail-fast 返 error, 实际 nil")
		}
		if !strings.Contains(err.Error(), "output reader") {
			t.Errorf("review-v6 H-06 fix: error message 应含 'output reader' 让业务定位修, 实际 = %q\n"+
				"sanity-revert: 删 Resume 入口的 nil 校验 → 此处 panic 不 return error",
				err.Error())
		}
	})
}
