package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/observability"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestExecutor_StreamConsumerStopsExecutionAndCleansCheckpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var sid string
	var executionErr error
	b := compose_v2.NewBuilder().WithName("stop-stream")
	b.AddNode("work", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sid = compose_v2.GetSessionID(ctx)
		compose_v2.GetEventSink(ctx).Send(compose_v2.Event{
			Type: "chunk", Content: "partial", Metadata: map[string]any{"run_id": "model-run"},
		})
		<-ctx.Done()
		executionErr = ctx.Err()
		return nil, errors.New("business failure after stream stopped")
	}).AddEdge(compose_v2.StartNode, "work").AddEdge("work", compose_v2.EndNode)
	graph, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ag := agent_v2.NewGraphAgent(graph, agent_v2.WithGraphAgentName("stop-stream"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return nil }))
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	defer tp.Shutdown(t.Context())
	obs := observability.New(observability.WithTracerProvider(tp),
		observability.WithMeterProvider(metricnoop.NewMeterProvider()))
	e := NewExecutor(ag, WithExecutorObservability(obs), WithOwnerResolver(func(context.Context) string { return "tenant" }))
	execCtx := &a2asrv.ExecutorContext{TaskID: "stop", ContextID: "thread",
		Message: a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("run"))}
	var gotArtifact bool
	for event, err := range e.Execute(ctx, execCtx) {
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := event.(*a2atypes.TaskArtifactUpdateEvent); ok {
			gotArtifact = true
			break
		}
	}
	if !gotArtifact || !errors.Is(executionErr, context.Canceled) || ctx.Err() != nil {
		t.Fatalf("停止迭代应取消实际执行，收到 artifact %v，执行错误 %v，调用方错误 %v", gotArtifact, executionErr, ctx.Err())
	}
	if _, exists := e.tasks[taskKey{owner: "tenant", taskID: execCtx.TaskID}]; exists {
		t.Fatal("已结束的迭代留下任务入口")
	}
	if cp, err := e.cp.LoadNS(ctx, sid, ""); sid == "" || err != nil || cp != nil {
		t.Fatalf("已结束的迭代留下存档，任务 %q 存档 %v 错误 %v", sid, cp, err)
	}
	for _, span := range rec.Ended() {
		if span.Name() == "a2a.server message_stream" {
			for _, attr := range span.Attributes() {
				if attr.Key == "crew.outcome" && attr.Value.AsString() == observability.OutcomeCancelled {
					return
				}
			}
		}
	}
	t.Fatal("A2A server span 应报告实际执行已取消")
}
