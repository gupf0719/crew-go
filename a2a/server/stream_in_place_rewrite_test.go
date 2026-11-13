package server_test

import (
	"context"
	"testing"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

type inPlaceRedactOnEnd struct{ hook.NoOpHooks }

func (inPlaceRedactOnEnd) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	out.Message.Content = "REDACTED"
	delete(out.Message.Annotations, compose_v2.RunIDAnnotationKey)
	return ctx, nil
}
func TestRemoteAgent_InPlaceReplacementHonored(t *testing.T) {
	base := serveAgent(t, agent_v2.NewBasicAgent(agent_v2.WithName("redact-inplace"), agent_v2.WithModel(&streamModel{}), agent_v2.WithHooks(inPlaceRedactOnEnd{})))
	ra, err := a2a.NewRemoteAgent(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	events, err := ra.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatal(err)
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "REDACTED" {
		t.Fatalf("远程调用应收到改写后的答复，实际 %+v", final)
	}
}
