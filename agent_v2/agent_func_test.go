package agent_v2_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

func TestAgentFuncRunsInSequenceAndResumesFailedCall(t *testing.T) {
	transient := errors.New("external service temporarily unavailable")
	wantInput := "原始目标：start\n\n上一步（one）的产出：\nprefix:start"
	var firstCalls, secondCalls int
	first := agent_v2.NewAgentFunc("prefix", "拼接前缀", func(_ context.Context, input string) (string, error) {
		firstCalls++
		return "prefix:" + input, nil
	})
	second := agent_v2.NewAgentFunc("external", "模拟外部服务", func(_ context.Context, input string) (string, error) {
		secondCalls++
		if input != wantInput {
			t.Errorf("外部服务实际收到 %q", input)
		}
		if secondCalls == 1 {
			return "", transient
		}
		return "processed:" + input, nil
	})
	sequence := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("interop")).AddStep("one", first).AddStep("two", second)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	session := agent_v2.NewRunSession()
	if _, err := sequence.Run(ctx, session, schema.UserMessage("start")); !errors.Is(err, transient) {
		t.Fatalf("外部错误链丢失: %v", err)
	}
	events, err := sequence.Resume(ctx, session.RunID)
	if final := agent_v2.FinalMessage(events); err != nil || final == nil || final.Content != "processed:"+wantInput {
		t.Fatalf("续跑产出错误: %v, %v", final, err)
	}
	if firstCalls != 1 || secondCalls != 2 || second.Name() != "external" || second.Description() != "模拟外部服务" {
		t.Fatalf("成员身份或重放边界错误: first=%d second=%d", firstCalls, secondCalls)
	}
}
