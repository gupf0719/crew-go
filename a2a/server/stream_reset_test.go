package server_test

// 服务端一次模型调用流中途失败、整次重来：失败那次的半截已经进了 artifact。流桥用 append=false 把 artifact 换成剩下的，
// RemoteAgent 认 append=false 按替换处理——对端与宿主拿到的都只有完整的那份，不是「半截 + 完整」（曾经实测 "partial-full"）。

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/a2a"
	"crew-go/a2a/server"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

// flakyStream 第一次流出半截后断开，之后正常。
type flakyStream struct{ calls atomic.Int64 }

func (m *flakyStream) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, errors.New("unused")
}

func (m *flakyStream) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	sr, send := schema.NewStreamReader[*schema.Message](4)
	if m.calls.Add(1) == 1 {
		send(&schema.Message{Role: schema.RoleAssistant, Content: "partial-"})
		schema.SendError(sr, errors.New("connection reset"))
	} else {
		send(&schema.Message{Role: schema.RoleAssistant, Content: "full"})
	}
	schema.CloseSend(sr)
	return sr, nil
}

func (m *flakyStream) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func flakyServedAgent() agent_v2.Agent {
	return agent_v2.NewBasicAgent(agent_v2.WithName("flaky"), agent_v2.WithModel(&flakyStream{}),
		agent_v2.WithMaxRounds(2), agent_v2.WithModelRetry(compose_v2.SimpleRetry(2)))
}

// 裸客户端按 A2A 语义应用 artifact 更新（append=false 替换）：最后的 artifact 只有完整的那份。
func TestServerStreaming_ResetReplacesArtifact(t *testing.T) {
	ag := flakyServedAgent()
	base := serveAgent(t, ag)
	ctx := context.Background()
	client, err := a2aclient.NewFromCard(ctx, server.Card(ag, base))
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}
	var art strings.Builder
	replaced := false
	msg := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("hi"))
	for ev, e := range client.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
		if e != nil {
			t.Fatalf("stream: %v", e)
		}
		v, ok := ev.(*a2atypes.TaskArtifactUpdateEvent)
		if !ok || v.Artifact == nil {
			continue
		}
		if !v.Append && art.Len() > 0 {
			art.Reset()
			replaced = true
		}
		for _, p := range v.Artifact.Parts {
			if p != nil {
				art.WriteString(p.Text())
			}
		}
	}
	if !replaced || art.String() != "full" {
		t.Fatalf("作废那次的半截应被替换掉：replaced=%v artifact=%q", replaced, art.String())
	}
}

// chunkLog 收宿主侧的 chunk / reset_stream。
type chunkLog struct {
	agent_v2.NoOpOrchestrationHooks
	mu     sync.Mutex
	events []agent_v2.Event
}

func (h *chunkLog) OnNodeEvent(ctx context.Context, _ string, ev agent_v2.Event) context.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, ev)
	return ctx
}

// RemoteAgent 作为宿主的编排成员：远端的答复是 "full"；宿主已显示的半截按它转发时的 run_id 撤回。
func TestRemoteAgent_ArtifactReplaceHonored(t *testing.T) {
	base := serveAgent(t, flakyServedAgent())
	ctx := context.Background()
	ra, err := a2a.NewRemoteAgent(ctx, base)
	if err != nil {
		t.Fatalf("NewRemoteAgent: %v", err)
	}
	log := &chunkLog{}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("host")).AddStep("remote", ra)
	events, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"), agent_v2.WithOrchHooks(log))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := agent_v2.FinalMessage(events).Content; got != "full" {
		t.Fatalf("远端的答复应只有完整那份，实际 %q", got)
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	var halfRun string
	resets := map[string]int{}
	for _, ev := range log.events {
		runID, _ := ev.Metadata["run_id"].(string)
		switch {
		case ev.Type == "chunk" && ev.Content == "partial-":
			halfRun = runID
		case ev.Type == "reset_stream":
			resets[runID]++
		}
	}
	if halfRun == "" || resets[halfRun] != 1 {
		t.Fatalf("宿主已显示的半截（run_id %q）应撤回一次，实际 %v", halfRun, resets)
	}
}

// redactOnEnd 在 OnModelEnd 里换上一条新消息（复制、改正文）。
type redactOnEnd struct{ hook.NoOpHooks }

func (redactOnEnd) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	if out.Message != nil {
		cp := *out.Message
		cp.Content = "REDACTED"
		out.Message = &cp
	}
	return ctx, nil
}

// freshRedactOnEnd 换上一条全新的消息（不带框架的标注）。
type freshRedactOnEnd struct{ hook.NoOpHooks }

func (freshRedactOnEnd) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	out.Message = &schema.Message{Role: schema.RoleAssistant, Content: "REDACTED"}
	return ctx, nil
}

// 钩子换上的新消息不带 run_id 标注：改写照样对得上原来那次调用，经 A2A 一跳之后 RemoteAgent 拿到的仍是改写后的正文（曾经
// 流桥丢掉 run_id 为空的改写信号，宿主拿到的是没脱敏的原文，审计实测）。
func TestRemoteAgent_FreshReplacementHonored(t *testing.T) {
	base := serveAgent(t, agent_v2.NewBasicAgent(agent_v2.WithName("redact2"), agent_v2.WithModel(&streamModel{}),
		agent_v2.WithHooks(freshRedactOnEnd{}), agent_v2.WithMaxRounds(2)))
	ctx := context.Background()
	ra, err := a2a.NewRemoteAgent(ctx, base)
	if err != nil {
		t.Fatalf("NewRemoteAgent: %v", err)
	}
	events, err := ra.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := agent_v2.FinalMessage(events).Content; got != "REDACTED" {
		t.Fatalf("经 A2A 一跳之后答复应是改写后的正文，实际 %q", got)
	}
}

// OnModelEnd 钩子改写了答复：流桥把这次调用的片段换成新正文（不是删掉），对端最后的 artifact 是 "REDACTED"。
func TestServerStreaming_HookReplacementReplacesArtifact(t *testing.T) {
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("redact"), agent_v2.WithModel(&streamModel{}),
		agent_v2.WithHooks(redactOnEnd{}), agent_v2.WithMaxRounds(2))
	base := serveAgent(t, ag)
	ctx := context.Background()
	client, err := a2aclient.NewFromCard(ctx, server.Card(ag, base))
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}
	var art strings.Builder
	msg := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("hi"))
	for ev, e := range client.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
		if e != nil {
			t.Fatalf("stream: %v", e)
		}
		v, ok := ev.(*a2atypes.TaskArtifactUpdateEvent)
		if !ok || v.Artifact == nil {
			continue
		}
		if !v.Append {
			art.Reset()
		}
		for _, p := range v.Artifact.Parts {
			if p != nil {
				art.WriteString(p.Text())
			}
		}
	}
	if art.String() != "REDACTED" {
		t.Fatalf("对端最后的 artifact 应是改写后的正文，实际 %q", art.String())
	}
}
