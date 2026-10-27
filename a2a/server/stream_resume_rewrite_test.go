package server_test

// 续跑时改写上一个请求流出的答复：替换当初那个 artifact，不另建一个。

import (
	"context"
	"strings"
	"testing"

	"crew-go/a2a"
	"crew-go/a2a/server"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

type askOnEndResume struct{ hook.NoOpHooks }

func (askOnEndResume) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
	_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "发出去吗"})
	return ctx, err
}

// 第一轮流出原文后问人，续跑时钩子改写了那份答复：任务里只剩改写后的 artifact（曾经续跑另建一个，原文那个永久留在任务里、
// 对端看得到，审计实测）。
func TestServerStreaming_ResumeRewriteReplacesEarlierArtifact(t *testing.T) {
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("rr"), agent_v2.WithModel(&streamModel{}),
		agent_v2.WithHooks(hook.NewChainHooks(redactOnEnd{}, askOnEndResume{})), agent_v2.WithMaxRounds(2))
	base := serveAgent(t, ag)
	ctx := context.Background()
	client, err := a2aclient.NewFromCard(ctx, server.Card(ag, base))
	if err != nil {
		t.Fatal(err)
	}
	arts := map[a2atypes.ArtifactID]*strings.Builder{}
	var order []a2atypes.ArtifactID
	stream := func(msg *a2atypes.Message) turn {
		var out turn
		for ev, e := range client.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
			if e != nil {
				t.Fatalf("stream: %v", e)
			}
			switch v := ev.(type) {
			case *a2atypes.Task:
				out.taskID, out.ctxID, out.state = v.ID, v.ContextID, v.Status.State
				if v.Status.Message != nil {
					out.pending = a2a.PendingQuestionsFromParts(v.Status.Message.Parts)
				}
			case *a2atypes.TaskStatusUpdateEvent:
				out.taskID, out.ctxID, out.state = v.TaskID, v.ContextID, v.Status.State
				if v.Status.Message != nil {
					out.pending = a2a.PendingQuestionsFromParts(v.Status.Message.Parts)
				}
			case *a2atypes.TaskArtifactUpdateEvent:
				b, ok := arts[v.Artifact.ID]
				if !ok {
					b = &strings.Builder{}
					arts[v.Artifact.ID] = b
					order = append(order, v.Artifact.ID)
				}
				if !v.Append {
					b.Reset()
				}
				for _, p := range v.Artifact.Parts {
					if p != nil {
						b.WriteString(p.Text())
					}
				}
			}
		}
		return out
	}
	first := stream(a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("hi")))
	if len(first.pending) != 1 {
		t.Fatalf("fixture 没搭对：第一轮应停在问人上，实际 %s", first.state)
	}
	second := stream(reply(first, a2a.AnswerPart(first.pending[0].InterruptID, nil, "是")))
	task, err := client.GetTask(ctx, &a2atypes.GetTaskRequest{ID: second.taskID})
	if err != nil {
		t.Fatal(err)
	}
	if len(task.Artifacts) == 0 {
		t.Fatalf("任务里应有改写后的 artifact，实际没有")
	}
	for _, a := range task.Artifacts {
		var b strings.Builder
		for _, p := range a.Parts {
			if p != nil {
				b.WriteString(p.Text())
			}
		}
		if b.String() != "REDACTED" {
			t.Errorf("续跑改写后任务里应只有改写后的正文，实际有一个 artifact 是 %q", b.String())
		}
	}
}

type twoRoundModel struct{ tools []*schema.ToolInfo }

func (m *twoRoundModel) WithTools(ts []*schema.ToolInfo) (model.ChatModel, error) {
	cp := *m
	cp.tools = ts
	return &cp, nil
}
func (m *twoRoundModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	panic("unused")
}
func (m *twoRoundModel) Stream(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	sr, send := schema.NewStreamReader[*schema.Message](4)
	hasTool := false
	for _, mm := range msgs {
		if mm.Role == schema.RoleTool {
			hasTool = true
		}
	}
	if hasTool {
		send(schema.AssistantMessage("SECRET"))
	} else {
		tc := schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c1", Type: "function", Function: schema.FunctionCall{Name: "noop", Arguments: "{}"}}})
		tc.Content = "A-"
		send(tc)
	}
	schema.CloseSend(sr)
	return sr, nil
}

type redactSecret struct{ hook.NoOpHooks }

func (redactSecret) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	if out.Message != nil && strings.Contains(out.Message.Content, "SECRET") {
		cp := *out.Message
		cp.Content = "REDACTED"
		out.Message = &cp
	}
	return ctx, nil
}

type askOnFinal struct{ hook.NoOpHooks }

func (askOnFinal) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	if out.Message != nil && len(schema.GetToolCalls(out.Message)) == 0 {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "send?"})
		return ctx, err
	}
	return ctx, nil
}

func probeAgent() agent_v2.Agent {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("noop", "noop", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
		return tool.Success("ok"), nil
	}))
	return agent_v2.NewBasicAgent(agent_v2.WithName("pb"), agent_v2.WithModel(&twoRoundModel{}), agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(hook.NewChainHooks(redactSecret{}, askOnFinal{})), agent_v2.WithMaxRounds(4))
}

// 两轮：第 1 轮流出 "A-" 加一个工具调用，第 2 轮流出 "SECRET"，钩子把它改写成 "REDACTED" 后在最终答复上问人。宿主经
// RemoteAgent 续跑那一轮拿到的只有改写后的答复，宿主前端上 "A-" 只出现一次（曾经一个请求共用一个 artifact，续跑替换上一个
// 请求的那个、整份重发，宿主拿到 "A-REDACTED"，审计实测）。
func TestRemoteAgent_ResumeRewriteOnlyReplacesThatCall(t *testing.T) {
	base := serveAgent(t, probeAgent())
	ctx := context.Background()
	ra, err := a2a.NewRemoteAgent(ctx, base, a2a.WithHumanGate())
	if err != nil {
		t.Fatal(err)
	}
	log := &chunkLog{}
	cp := compose_v2.NewInMemoryCheckpointer()
	cctx := compose_v2.WithCheckpointerCtx(ctx, cp)
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("host")).AddStep("remote", ra)
	sess := agent_v2.NewRunSession()
	_, err = seq.Run(cctx, sess, schema.UserMessage("hi"), agent_v2.WithOrchHooks(log))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("want interrupt, got %v", err)
	}
	events, err := seq.Resume(cctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeOrchHooks(log),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("yes")}))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if got := agent_v2.FinalMessage(events).Content; got != "REDACTED" {
		t.Fatalf("续跑那一轮宿主拿到的答复应只有改写后的正文，实际 %q", got)
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	shown := 0
	for _, ev := range log.events {
		if ev.Type == "chunk" && strings.Contains(ev.Content, "A-") {
			shown++
		}
	}
	if shown != 1 {
		t.Fatalf("第 1 轮的 \"A-\" 在宿主前端应只出现一次，实际 %d 次", shown)
	}
}
