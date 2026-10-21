package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// externalAgent 是「不是 BasicAgent 的执行体」——远程 agent / 自研 agent / 适配别的
// 框架的壳都是这个形状：实现 Agent 接口，但对历史层一无所知、跑完什么也不留。
type externalAgent struct{ name string }

func (a *externalAgent) Run(_ context.Context, sess *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	out := schema.AssistantMessage("【外部产出】" + strings.TrimSpace(in.Content))
	sess.AddEvent(agent_v2.NewExitEvent(a.name, out))
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, out)}, nil
}

func (a *externalAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *externalAgent) Name() string        { return a.name }
func (a *externalAgent) Description() string { return "外部 agent" }

// TestHistory_ExternalAgentInOrchestration_NotSilent：编排里有非 BasicAgent 的执行体时，
// 那一步的输入与产出仍要进历史。
//
// 修复前的形状：写入器只在 BasicAgent.Run 里建、写入点也只在它的三个节点里，所以
// `Sequential(外部, 本地)` 跑完历史里**只剩本地那一条产出**——用户原话和外部产出双双
// 缺席，下一轮模型看到一个没有任何前因的结论。不是「少一步」，是整条断链：第一步没人记，
// 第二步的输入是派生输入按判据跳过（而「成分能从历史重算」这个前提此刻不成立）。
//
// **sanity-revert**：把 sequential_agent.go 的 settleHistorySlot 调用删掉，或把
// startHistoryForOrchestration 那段删掉，本测试都会 FAIL 并报缺了哪条。
func TestHistory_ExternalAgentInOrchestration_NotSilent(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	local := agent_v2.NewBasicAgent(
		agent_v2.WithName("localB"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("本地结论")})),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("flow")).
		AddStep("remote", &externalAgent{name: "remoteA"}).
		AddStep("local", local)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-ext")
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("用户原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs, err := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-ext"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var sawUser, sawExternal bool
	for _, m := range msgs {
		if strings.Contains(m.Content, "用户原话") {
			sawUser = true
		}
		if strings.Contains(m.Content, "外部产出") {
			sawExternal = true
		}
	}
	if !sawUser {
		t.Errorf("用户原话没进历史——第一步交给了外部 agent，没人记它\n%s", dumpHist(msgs))
	}
	if !sawExternal {
		t.Errorf("外部 agent 的产出没进历史——下一轮模型看不到这一步做了什么\n%s", dumpHist(msgs))
	}

	// 归属该是编排位置，不是外部 agent 自己的名字
	owners := map[string]bool{}
	for _, m := range msgs {
		if o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string); o != "" {
			owners[o] = true
		}
	}
	if !owners["remote"] {
		t.Errorf("外部那步的归属不是步骤注册名 remote（实际 %v）", owners)
	}
}

// TestHistory_ExternalAgent_NoDoubleRecord：边界补记不能跟 BasicAgent 自己记的重复。
//
// 判据是「这次调用的范围内有没有产生记录」+「这条消息本身记过没有」——BasicAgent 会在
// 节点里写，边界据此跳过。见 settleHistorySlot。
func TestHistory_ExternalAgent_NoDoubleRecord(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	mk := func(reply string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("worker"),
			agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage(reply)})),
			agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("flow")).
		AddStep("s1", mk("产出一")).
		AddStep("s2", mk("产出二"))

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-nodup")
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-nodup"})

	counts := map[string]int{}
	for _, m := range msgs {
		c := strings.TrimSpace(m.Content)
		if c != "" {
			counts[c]++
		}
	}
	for _, want := range []string{"原话", "产出一", "产出二"} {
		if counts[want] > 1 {
			t.Errorf("%q 记了 %d 次——边界补记跟 BasicAgent 自己记的重复了\n%s",
				want, counts[want], dumpHist(msgs))
		}
	}
}
