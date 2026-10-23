package agent_v2_test

// 同一条输入经过几层 agent 入口（每层各拷一份）之后，会话历史里仍只记一份：「这条输入本次 Run 记过没有」按消息 ID 认，
// 不按指针；钩子把输入换成没有 ID 的消息时，在采纳那一刻补 ID。
//
// 按指针认时：顶层 GraphAgent 的节点把输入交给 BasicAgent（直接 Run 或经 RunSubAgent），叶子记的是它入口拷的那份，
// 顶层结算时拿自己那份查不到，用户原话记两份；Handoff 的钩子把输入换成没有 ID 的消息，两跳各补一个不同的 ID、各记一份。

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
)

type identityProbeModel struct{ reply string }

func (m *identityProbeModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(m.reply), nil
}
func (m *identityProbeModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *identityProbeModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func countUserWithContent(msgs []*schema.Message, c string) int {
	n := 0
	for _, m := range msgs {
		if m.Role == schema.RoleUser && m.Content == c {
			n++
		}
	}
	return n
}

// 顶层 GraphAgent 的节点直接调 BasicAgent.Run，把 channel 里的输入原样交给它。
func TestInputIdentity_GraphDirectRun(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	leaf := agent_v2.NewBasicAgent(agent_v2.WithName("leaf"), agent_v2.WithModel(&identityProbeModel{reply: "ans"}), agent_v2.WithMaxRounds(2))
	ga := mkEchoGraph(t, "direct", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
		ev, err := leaf.Run(ctx, agent_v2.GetSession(ctx), in)
		if err != nil {
			return nil, err
		}
		return agent_v2.FinalMessage(ev), nil
	})
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_direct")
	if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs := loadHist(t, hs, ctx, "c_direct")
	if n := countUserWithContent(msgs, "原话"); n != 1 {
		t.Errorf("原话应 1 条，实际 %d：%s", n, histDump(msgs))
	}
}

// HandoffAgent 的 OnAgentStart 钩子把输入换成一条没有 ID 的消息（脱敏常见写法）
type redactToIDLessHook struct{ hook.NoOpHooks }

func (redactToIDLessHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	if n := len(in.Messages); n > 0 {
		in.Messages[n-1] = &schema.Message{Role: schema.RoleUser, Content: "REDACTED_MARK"}
	}
	return ctx, nil
}

func TestInputIdentity_HandoffHookReplacesInputWithoutID(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	frontdesk := agent_v2.NewBasicAgent(
		agent_v2.WithName("frontdesk"),
		agent_v2.WithModel(newStubModel([]*schema.Message{transferToolCall("tc_1", "billing")})),
		agent_v2.WithHandoffTransfer(), agent_v2.WithHistory(hs, "tenant-a"), agent_v2.WithMaxRounds(5))
	billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("账单已处理")})),
		agent_v2.WithHistory(hs, "tenant-a"), agent_v2.WithMaxRounds(5))
	ho := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("desk"), agent_v2.WithHandoffInitial("frontdesk"),
		agent_v2.WithHandoffHooks(redactToIDLessHook{}),
	).AddAgent("frontdesk", frontdesk).AddAgent("billing", billing)
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-ho-redact")
	if _, err := ho.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原文")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got, _ := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-ho-redact"})
	if n := countUserWithContent(got, "REDACTED_MARK"); n != 1 {
		t.Errorf("脱敏后的输入应 1 条，实际 %d：%s", n, histDump(got))
	}
}

// 同上，经 RunSubAgent 调（嵌套调用一直交拷贝，改动之前就记两份）。
func TestInputIdentity_GraphRunSubAgentBasic(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	leaf := agent_v2.NewBasicAgent(agent_v2.WithName("leaf"), agent_v2.WithModel(&identityProbeModel{reply: "ans"}), agent_v2.WithMaxRounds(2))
	ga := mkEchoGraph(t, "viasub", func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
		ev, err := agent_v2.RunSubAgent(ctx, "worker", leaf, in)
		if err != nil {
			return nil, err
		}
		return agent_v2.FinalMessage(ev), nil
	})
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_viasub")
	if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs := loadHist(t, hs, ctx, "c_viasub")
	if n := countUserWithContent(msgs, "原话"); n != 1 {
		t.Errorf("原话应 1 条，实际 %d：%s", n, histDump(msgs))
	}
}

// 节点用结构体拷贝派生一条新输入（`d := *in` 保留了原话的 ID，再改内容）交给叶子：派生的那条跟原话共用一个 ID，
// 但不是同一条输入——原话照样记一份。「同一条输入」= 同一个 ID 且内容相同；只看 ID 的话原话被当成记过、历史里
// 一条都不剩（审计实测）。
func TestInputIdentity_StructCopyDerivedInputKeepsOriginal(t *testing.T) {
	for _, viaSub := range []bool{false, true} {
		name := map[bool]string{false: "direct_run", true: "run_subagent"}[viaSub]
		t.Run(name, func(t *testing.T) {
			hs := agent_v2.NewInMemoryHistoryStore()
			leaf := agent_v2.NewBasicAgent(agent_v2.WithName("leaf"), agent_v2.WithModel(&identityProbeModel{reply: "ans"}), agent_v2.WithMaxRounds(2))
			ga := mkEchoGraph(t, "derive_"+name, func(ctx context.Context, in *schema.Message) (*schema.Message, error) {
				d := *in
				d.Content = "派生:" + in.Content
				var ev []*agent_v2.AgentEvent
				var err error
				if viaSub {
					ev, err = agent_v2.RunSubAgent(ctx, "worker", leaf, &d)
				} else {
					ev, err = leaf.Run(ctx, agent_v2.GetSession(ctx), &d)
				}
				if err != nil {
					return nil, err
				}
				return agent_v2.FinalMessage(ev), nil
			})
			ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
			ctx = compose_v2.WithThreadIDCtx(ctx, "c_derive_"+name)
			if _, err := ga.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
				t.Fatalf("Run: %v", err)
			}
			msgs := loadHist(t, hs, ctx, "c_derive_"+name)
			if n := countUserWithContent(msgs, "原话"); n != 1 {
				t.Errorf("原话应 1 条，实际 %d：%s", n, histDump(msgs))
			}
			if n := countUserWithContent(msgs, "派生:原话"); n != 1 {
				t.Errorf("派生的那条应 1 条（叶子记的），实际 %d：%s", n, histDump(msgs))
			}
		})
	}
}
