package agent_v2_test

import (
	"context"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// h05Hook 记录业务 hook 各 method 被 fire 的次数, 验证 PlanAgent / GraphAgent
// 在 review-v6 H-05 fix 后真 fire OnAgentStart + OnError + OnAgentEnd.
type h05Hook struct {
	hook.NoOpHooks
	startCount atomic.Int32
	endCount   atomic.Int32
	errCount   atomic.Int32
}

func (h *h05Hook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	h.startCount.Add(1)
	return ctx, nil
}

func (h *h05Hook) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	h.endCount.Add(1)
	return ctx
}

func (h *h05Hook) OnError(ctx context.Context, _ *hook.AgentInfo, _ *schema.CommError) context.Context {
	h.errCount.Add(1)
	return ctx
}

// TestReviewV6_H05_PlanAgentHookProtocol 验 review-v6 H-05 fix —
// PlanAgent wrapper 层 Run/Resume 入口 fire OnAgentStart + OnError,
// 不再仅 fire OnAgentEnd (与 7 编排协议对称).
//
// 历史: PlanAgent + GraphAgent 业务 hook 8 method 中多个 silent —
// 业务监控 / SLO / 计费 silently 漏数据.
//
// sanity-revert: 删 PlanAgent.Run 入口 OnAgentStart fire 块 → startCount=0 测试 fail.
func TestReviewV6_H05_PlanAgentHookProtocol(t *testing.T) {
	t.Run("Run_success_fires_start_end", func(t *testing.T) {
		stub := newStubModel([]*schema.Message{schema.AssistantMessage("done")})
		a := agent_v2.NewBasicAgent(agent_v2.WithName("a"), agent_v2.WithModel(stub))

		planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
			if len(executed) == 0 {
				return []agent_v2.PlanStep{{Description: "step1", SubAgentName: "a"}}, "", nil
			}
			return nil, "completed", nil
		}

		h := &h05Hook{}
		pa := agent_v2.NewPlanAgent(planner,
			agent_v2.PlanName("p"),
			agent_v2.WithPlanHooks(h),
		).AddSubAgent("a", a)

		_, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("task"))
		if err != nil {
			t.Fatalf("Run err: %v", err)
		}

		if h.startCount.Load() != 1 {
			t.Errorf("review-v6 H-05 fix: PlanAgent.Run 应 fire OnAgentStart 1 次, 实际 %d", h.startCount.Load())
		}
		if h.endCount.Load() != 1 {
			t.Errorf("PlanAgent.Run 应 fire OnAgentEnd 1 次 (ISSUE-83 已修, 回归保护), 实际 %d", h.endCount.Load())
		}
		if h.errCount.Load() != 0 {
			t.Errorf("PlanAgent.Run success 路径不应 fire OnError, 实际 %d", h.errCount.Load())
		}
	})

	t.Run("Run_planner_error_fires_OnError", func(t *testing.T) {
		// planner 返业务错 → 应 fire OnError (非 InterruptError / 非 ctx.Err)
		planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, _ []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
			return nil, "", &simpleBizErr{msg: "planner business failure"}
		}

		h := &h05Hook{}
		pa := agent_v2.NewPlanAgent(planner,
			agent_v2.PlanName("p"),
			agent_v2.WithPlanHooks(h),
		)

		_, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("task"))
		if err == nil {
			t.Fatal("Run 应返业务错")
		}
		// InterruptError 不算业务错 — 不 fire OnError
		if _, isInt := agent_v2.IsInterrupt(err); isInt {
			t.Fatal("planner 返普通业务错不应被识别为 Interrupt")
		}
		if h.errCount.Load() != 1 {
			t.Errorf("review-v6 H-05 fix: PlanAgent.Run 业务错应 fire OnError 1 次, 实际 %d\n"+
				"sanity-revert: 删 runPlanGraph err 路径的 SafeOnError fire → 0", h.errCount.Load())
		}
		// start 仍应 fire (在 runPlanGraph 之前)
		if h.startCount.Load() != 1 {
			t.Errorf("start 应在 runPlanGraph 之前 fire 1 次, 实际 start=%d", h.startCount.Load())
		}
	})
}

// TestReviewV6_H05_GraphAgentHookProtocol 同款验 GraphAgent 协议补齐.
func TestReviewV6_H05_GraphAgentHookProtocol(t *testing.T) {
	bld := compose_v2.NewBuilder()
	ch := compose_v2.LastValue[string]("dummy")
	compose_v2.WithChannel(bld, ch)
	bld.AddNode("noop", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(ch, "ok")}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "noop")
	bld.AddEdge("noop", compose_v2.EndNode)
	graph, err := bld.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	h := &h05Hook{}
	g := agent_v2.NewGraphAgent(graph,
		agent_v2.WithGraphAgentName("g"),
		agent_v2.WithGraphInputWriter(func(_ *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(ch, "input")}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			v, _ := compose_v2.Get(chs, ch)
			return schema.AssistantMessage(v)
		}),
		agent_v2.WithGraphAgentHooks(h),
	)

	_, runErr := g.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if runErr != nil {
		t.Fatalf("Run err: %v", runErr)
	}

	if h.startCount.Load() != 1 {
		t.Errorf("review-v6 H-05 fix: GraphAgent.Run 应 fire OnAgentStart 1 次, 实际 %d", h.startCount.Load())
	}
	if h.endCount.Load() != 1 {
		t.Errorf("GraphAgent.Run 应 fire OnAgentEnd 1 次, 实际 %d", h.endCount.Load())
	}
}

// simpleBizErr 是普通业务 error (非 InterruptError) — 触发 OnError 路径
type simpleBizErr struct{ msg string }

func (e *simpleBizErr) Error() string { return e.msg }

// h05ConsumeHook 在 OnAgentStart 改写 input.Messages 验证 hook 消费语义
type h05ConsumeHook struct {
	hook.NoOpHooks
	newContent string
}

func (h *h05ConsumeHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	// 业务改写: PII filter / 模板包装 / input 重写场景
	in.Messages = []*schema.Message{schema.UserMessage(h.newContent)}
	return ctx, nil
}

// TestReviewV6_H05_HookConsumeMessagesRewrite 验 H-05 audit LOW-1 fix —
// PlanAgent + GraphAgent fire OnAgentStart 后真消费业务改写的 agentIn.Messages
// (CLAUDE.md Hook 消费语义 + BasicAgent modelNode 协议对称).
//
// sanity-revert: 删 fix 中"消费 hook 改写" block → planner 看到的 original 是
// 原始 input 不是改写后的 content → 测试 fail.
func TestReviewV6_H05_HookConsumeMessagesRewrite(t *testing.T) {
	t.Run("PlanAgent_consumes_rewrite", func(t *testing.T) {
		stub := newStubModel([]*schema.Message{schema.AssistantMessage("done")})
		a := agent_v2.NewBasicAgent(agent_v2.WithName("a"), agent_v2.WithModel(stub))

		// planner 拿 original 字符串 — 应是 hook 改写后的, 不是原始 input
		var capturedOriginal string
		planner := func(_ context.Context, _ *agent_v2.RunSession, original string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
			if len(executed) == 0 {
				capturedOriginal = original
				return []agent_v2.PlanStep{{Description: "step1", SubAgentName: "a"}}, "", nil
			}
			return nil, "done", nil
		}

		h := &h05ConsumeHook{newContent: "REWRITTEN_BY_HOOK"}
		pa := agent_v2.NewPlanAgent(planner,
			agent_v2.PlanName("p"),
			agent_v2.WithPlanHooks(h),
		).AddSubAgent("a", a)

		_, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("ORIGINAL_INPUT"))
		if err != nil {
			t.Fatalf("Run err: %v", err)
		}

		if capturedOriginal != "REWRITTEN_BY_HOOK" {
			t.Errorf("review-v6 H-05 audit LOW-1 fix: PlanAgent 应消费 hook 改写的 agentIn.Messages, planner 看到的 original 应是 'REWRITTEN_BY_HOOK', 实际 = %q\n"+
				"sanity-revert: 删 'effectiveContent := agentIn.Messages[...]' block → planner 看到原始 'ORIGINAL_INPUT' → 此 fail", capturedOriginal)
		}
	})

	t.Run("GraphAgent_consumes_rewrite", func(t *testing.T) {
		bld := compose_v2.NewBuilder()
		ch := compose_v2.LastValue[string]("input_ch")
		compose_v2.WithChannel(bld, ch)

		var capturedInput string
		bld.AddNode("noop", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			v, _ := compose_v2.Get(chs, ch)
			capturedInput = v
			return nil, nil
		})
		bld.AddEdge(compose_v2.StartNode, "noop")
		bld.AddEdge("noop", compose_v2.EndNode)
		graph, err := bld.Compile()
		if err != nil {
			t.Fatalf("compile: %v", err)
		}

		h := &h05ConsumeHook{newContent: "REWRITTEN_BY_HOOK"}
		g := agent_v2.NewGraphAgent(graph,
			agent_v2.WithGraphAgentName("g"),
			agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
				// inputWriter 拿到 hook 改写后的 input.Content
				return []compose_v2.ChannelWrite{compose_v2.Write(ch, m.Content)}
			}),
			agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
				v, _ := compose_v2.Get(chs, ch)
				return schema.AssistantMessage(v)
			}),
			agent_v2.WithGraphAgentHooks(h),
		)

		_, err = g.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("ORIGINAL_INPUT"))
		if err != nil {
			t.Fatalf("Run err: %v", err)
		}

		if capturedInput != "REWRITTEN_BY_HOOK" {
			t.Errorf("review-v6 H-05 audit LOW-1 fix: GraphAgent 应消费 hook 改写的 agentIn.Messages, inputWriter 看到的 input.Content 应是 'REWRITTEN_BY_HOOK', 实际 = %q",
				capturedInput)
		}
	})
}
