package agui_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
)

// TestHistory_OwnerFlowsFromRequestContext 端到端钉住：会话历史按**调用方身份**分区，
// 即使业务只在 agui 那层配了 resolver。
//
// 起因是一处不对称：`historyKeyFrom` 里 ConvID 从 `compose_v2.GetThreadID(ctx)`
// **自动流过来**，而 owner 一度只看 `WithHistory` 的显式配置、从不问 `GetOwner`。
// 于是业务接了多租户 resolver（框架逼着配，不配构造期 panic）、又照 godoc 写
// `WithHistory(hs, "prod")` 时，两租户的对话全落进同一个分区——**租户 A 的原话
// 会进租户 B 的 prompt**。
//
// ⚠️ 断言落在「B 的 prompt 里有没有 A 的原话」，不落在「历史条数」上：条数对不上
// 有很多种原因，而泄漏的实质是那句话被喂给了另一个租户的模型。
func TestHistory_OwnerFlowsFromRequestContext(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	cap := &memInjectSpy{}
	h := ownerAwareHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("clerk"),
			agent_v2.WithModel(cap),
			agent_v2.WithHooks(hooks),
			// 业务照 godoc 写的静态 owner——多租户时它是**兜底**，不该盖过调用方身份。
			agent_v2.WithHistory(hs, "prod"),
			agent_v2.WithMaxRounds(2))
	})

	const sharedThread = "thread-both-tenants-use"
	body := func(text string) string {
		return `{"threadId":"` + sharedThread + `","runId":"r","messages":[{"role":"user","content":"` + text + `"}]}`
	}
	postAs(h, "tenant-A", body("我的卡号 4321"))
	cap.reset()
	postAs(h, "tenant-B", body("你好"))

	for _, m := range cap.last {
		if strings.Contains(m.Content, "卡号 4321") {
			t.Fatalf("租户 A 的原话进了租户 B 的 prompt：%q——历史层没按调用方身份分区", m.Content)
		}
	}

	// **正向断言**：A 自己回来时读得到自己的历史（否则「历史整个不工作」也能让上面那条绿）。
	cap.reset()
	postAs(h, "tenant-A", body("刚才我说的卡号是多少"))
	found := false
	for _, m := range cap.last {
		if strings.Contains(m.Content, "卡号 4321") {
			found = true
		}
	}
	if !found {
		t.Error("租户 A 读不到自己上一轮的话——按 owner 分区把合法的历史也切没了")
	}
}

// memInjectSpy 记下最近一次 Generate 收到的完整 messages。
type memInjectSpy struct {
	last []*schema.Message
}

func (s *memInjectSpy) reset() { s.last = nil }

func (s *memInjectSpy) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	s.last = append([]*schema.Message{}, msgs...)
	return schema.AssistantMessage("好的"), nil
}

func (s *memInjectSpy) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	m, err := s.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(m)
	schema.CloseSend(sr)
	return sr, nil
}

func (s *memInjectSpy) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return s, nil }
