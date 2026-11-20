package observability_test

import (
	"context"
	"fmt"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/observability"
	"crew-go/schema"
)

type detachedAgentStart struct {
	hook.NoOpHooks
	child   agent_v2.Agent
	session *agent_v2.RunSession
	reply   string
}

func (h detachedAgentStart) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	events, err := h.child.Run(agent_v2.DetachTask(ctx), h.session, schema.UserMessage("子任务"))
	if err == nil && h.reply != "" && agent_v2.FinalMessage(events).GetContent() != h.reply {
		err = fmt.Errorf("unexpected child reply: %v", agent_v2.FinalMessage(events))
	}
	return ctx, err
}

func TestSameNameDetachedAgentOwnsInvocationSpan(t *testing.T) {
	h := newHarness(t)
	ctx, childOpts := h.obs.Install(t.Context(), agent_v2.WithName("shared"), agent_v2.WithModel(&scriptModel{}))
	childSession, parentSession := agent_v2.NewRunSession(), agent_v2.NewRunSession()
	child := agent_v2.NewBasicAgent(childOpts...)
	parent := agent_v2.NewBasicAgent(agent_v2.WithName("shared"), agent_v2.WithModel(&scriptModel{}),
		agent_v2.WithHooks(h.obs.AgentHooks(detachedAgentStart{child: child, session: childSession, reply: "最终答复：子任务"})))
	events, err := parent.Run(ctx, parentSession, schema.UserMessage("父任务"))
	if err != nil || agent_v2.FinalMessage(events).GetContent() != "最终答复：父任务" {
		t.Fatalf("父任务输出异常: %v，%v", agent_v2.FinalMessage(events), err)
	}
	h.assertAllEnded(t)
	spans := h.spansNamed("invoke_agent shared")
	byRun := make(map[string]sdktrace.ReadOnlySpan)
	for _, span := range spans {
		run, _ := attrOf(span, observability.AttrRunID)
		byRun[run.AsString()] = span
	}
	childSpan, parentSpan := byRun[childSession.RunID], byRun[parentSession.RunID]
	if len(spans) != 2 || childSpan == nil || parentSpan == nil {
		t.Fatalf("父子任务应各自导出调用 span，实际 %d 条（%v）", len(spans), byRun)
	}
	if childSpan.Parent().SpanID() != parentSpan.SpanContext().SpanID() {
		t.Fatal("分离任务丢失 trace 父子关系")
	}
	rm := h.collect(t)
	if n := sumInt(t, rm, observability.MetricAgentActive, nil); n != 0 {
		t.Fatalf("任务完成后在途计数 = %d", n)
	}
}

func TestDetachedGraphWithoutAgentHooksOwnsWorkflowSpan(t *testing.T) {
	h := newHarness(t)
	ctx := agent_v2.WithCtxOrchHooks(t.Context(), h.obs.OrchHooks())
	input := compose_v2.LastValue[string]("child_input")
	builder := compose_v2.NewBuilder().AddNode("independent_work",
		func(_ context.Context, channels compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if got := compose_v2.MustGet(channels, input); got != "子任务" {
				return nil, fmt.Errorf("unexpected child input: %q", got)
			}
			return nil, nil
		}).AddEdge(compose_v2.StartNode, "independent_work").AddEdge("independent_work", compose_v2.EndNode)
	graph, err := compose_v2.WithChannel(builder, input).Compile()
	if err != nil {
		t.Fatal(err)
	}
	session := agent_v2.NewRunSession()
	child := agent_v2.NewGraphAgent(graph, agent_v2.WithGraphAgentName("independent"),
		agent_v2.WithGraphInputWriter(func(message *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(input, message.GetContent())}
		}),
		agent_v2.WithGraphOutputReader(func(channels compose_v2.Channels) *schema.Message {
			return schema.AssistantMessage("收到：" + compose_v2.MustGet(channels, input))
		}))
	parent := agent_v2.NewBasicAgent(agent_v2.WithName("parent"), agent_v2.WithModel(&scriptModel{}),
		agent_v2.WithHooks(h.obs.AgentHooks(detachedAgentStart{child: child, session: session, reply: "收到：子任务"})))
	if _, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("父任务")); err != nil {
		t.Fatal(err)
	}
	h.assertAllEnded(t)
	workflow := h.span(t, "invoke_workflow root")
	node := h.span(t, "node independent_work")
	run, _ := attrOf(workflow, observability.AttrRunID)
	if run.AsString() != session.RunID || node.Parent().SpanID() != workflow.SpanContext().SpanID() ||
		workflow.Parent().SpanID() != h.span(t, "invoke_agent parent").SpanContext().SpanID() {
		t.Fatal("独立图与节点 span 未绑定到各自的运行")
	}
}
