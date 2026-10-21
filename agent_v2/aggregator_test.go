package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// TestAggregator_Basic 验证 Aggregator 简化 API。
func TestAggregator_Basic(t *testing.T) {
	a1 := agent_v2.NewBasicAgent(agent_v2.WithName("a1"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("X")})))
	a2 := agent_v2.NewBasicAgent(agent_v2.WithName("a2"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("Y")})))

	agg := func(_ context.Context, m map[string]string) (*schema.Message, error) {
		return schema.AssistantMessage("aggregated[" + m["b1"] + "+" + m["b2"] + "]"), nil
	}

	par := agent_v2.NewParallelAgent(
		agent_v2.WithParallelName("par"),
		agent_v2.WithAggregator(agg),
	).AddBranch("b1", a1).AddBranch("b2", a2)

	events, err := par.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "aggregated[X+Y]") {
		t.Errorf("expect 'aggregated[X+Y]', got %v", final)
	}
}

// TestAggregator_MergerWins 验证 Merger 优先级高于 Aggregator。
func TestAggregator_MergerWins(t *testing.T) {
	a1 := agent_v2.NewBasicAgent(agent_v2.WithName("a1"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("X")})))

	merger := func(_ context.Context, _ []agent_v2.ParallelBranchResult) (*schema.Message, error) {
		return schema.AssistantMessage("MERGER"), nil
	}
	agg := func(_ context.Context, _ map[string]string) (*schema.Message, error) {
		return schema.AssistantMessage("AGGREGATOR"), nil
	}

	par := agent_v2.NewParallelAgent(
		agent_v2.WithParallelName("par"),
		agent_v2.WithParallelMerger(merger),
		agent_v2.WithAggregator(agg), // 应被忽略
	).AddBranch("b1", a1)

	events, err := par.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "MERGER") {
		t.Errorf("expect MERGER (wins over Aggregator), got %v", final)
	}
}
