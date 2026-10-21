package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// TestLoopAgent_MaxIterations 验证 maxIterations 兜底退出：
// stub 每轮返不同文本，循环直到 maxIter 触顶。
func TestLoopAgent_MaxIterations(t *testing.T) {
	stub := newStubModel([]*schema.Message{
		schema.AssistantMessage("r0"),
		schema.AssistantMessage("r1"),
		schema.AssistantMessage("r2"),
	})
	body := agent_v2.NewBasicAgent(agent_v2.WithName("body"), agent_v2.WithModel(stub))

	loop := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("loop"),
		agent_v2.WithLoopMaxIterations(3),
	).WithBody(body)

	events, err := loop.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatal("expected final")
	}
	// 跑满 3 轮 → 最后一轮 output 是 "r2"
	if !strings.Contains(final.Content, "r2") {
		t.Errorf("expect last round output 'r2', got %q", final.Content)
	}
}

// TestLoopAgent_ExitCondition 验证自定义 ExitCondition 提前退出：
// 第 2 轮 cond 返 true → 应在 2 轮后立即退出（不跑到 maxIter=10）。
func TestLoopAgent_ExitCondition(t *testing.T) {
	stub := newStubModel([]*schema.Message{
		schema.AssistantMessage("a"),
		schema.AssistantMessage("STOP"),
		schema.AssistantMessage("never"),
	})
	body := agent_v2.NewBasicAgent(agent_v2.WithName("body"), agent_v2.WithModel(stub))

	cond := func(_ context.Context, lastOut *schema.Message, _ int) (bool, error) {
		return lastOut != nil && strings.Contains(lastOut.Content, "STOP"), nil
	}

	loop := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("loop"),
		agent_v2.WithLoopMaxIterations(10),
		agent_v2.WithLoopExitCondition(cond),
	).WithBody(body)

	events, err := loop.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "STOP") {
		t.Errorf("expect ExitCondition exit on 'STOP', got %v", final)
	}
}

// TestLoopAgent_BreakLoopAnnotation 验证 break_loop annotation 优先级最高。
func TestLoopAgent_BreakLoopAnnotation(t *testing.T) {
	bMsg := schema.AssistantMessage("done")
	if bMsg.Annotations == nil {
		bMsg.Annotations = map[string]any{}
	}
	bMsg.Annotations["break_loop"] = true

	stub := newStubModel([]*schema.Message{
		bMsg,
		schema.AssistantMessage("never"),
	})
	body := agent_v2.NewBasicAgent(agent_v2.WithName("body"), agent_v2.WithModel(stub))

	loop := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("loop"),
		agent_v2.WithLoopMaxIterations(10),
	).WithBody(body)

	events, err := loop.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, "done") {
		t.Errorf("expect break_loop exit on round 1 with 'done', got %v", final)
	}
}

// TestLoopAgent_NoBodyFails 验证 body 必填 fail-fast。
func TestLoopAgent_NoBodyFails(t *testing.T) {
	loop := agent_v2.NewLoopAgent(agent_v2.WithLoopName("loop"))
	_, err := loop.Run(context.Background(), nil, schema.UserMessage("hi"))
	if err == nil || !strings.Contains(err.Error(), "body required") {
		t.Fatalf("expect 'body required' err, got %v", err)
	}
}
