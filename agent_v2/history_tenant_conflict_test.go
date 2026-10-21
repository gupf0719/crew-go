package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// 同一编排下的子 agent 声明了不同的历史配置时必须报错，不能静默挑一个。
//
// 修复前取的是「第一个配了的」，而 HandoffAgent / PlanAgent 的子是 `map[string]Agent`、
// `for range` 顺序每次都不同 ⟹ **整场对话落到哪个租户每个 Run 随机翻**。跨 Run 更糟：
// Run 1 写租户 A、Run 2 写租户 B、Run 3 读到哪半看运气，而落进 B 的内容本身是 A 的对话。
// 全程没有任何错误信号。
//
// 这里用 owner 不同来构造冲突（store 不同同理）。
func TestHistory_ConflictingTenantConfig_FailsFast(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	mk := func(name, tenant string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName(name),
			agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("ok")})),
			agent_v2.WithHistory(hs, tenant), agent_v2.WithMaxRounds(2))
	}
	ho := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("front"), agent_v2.WithHandoffInitial("a"),
	).AddAgent("a", mk("agentA", "tenantA")).
		AddAgent("b", mk("agentB", "tenantB"))

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-tenant")
	_, err := ho.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话"))
	if err == nil {
		t.Fatalf("子 agent 配了两个不同租户，应 fail-fast；实际成功了——" +
			"那意味着对话被写进了其中一个租户，而是哪个取决于 map 迭代顺序")
	}
	if !strings.Contains(err.Error(), "conflicting history config") {
		t.Errorf("报错该指明是历史配置冲突，实际：%v", err)
	}
}

// 对照组：同一套配置（同 store 同 owner）不受影响——这是绝大多数业务的写法。
//
// 没有这一条，上面那个用例可以靠「任何多子编排都报错」通过，等于把功能测没了。
func TestHistory_SameTenantConfig_Runs(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	mk := func(name string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName(name),
			agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("ok-" + name)})),
			agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	}
	ho := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("front"), agent_v2.WithHandoffInitial("a"),
	).AddAgent("a", mk("agentA")).AddAgent("b", mk("agentB"))

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-same")
	if _, err := ho.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("同一套配置不该报错：%v", err)
	}
	msgs, err := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-same"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(msgs) == 0 {
		t.Errorf("历史是空的——测试无效（配置没生效的话上面那个冲突用例也测不到东西）")
	}
}

// 子列表的顺序必须确定——map 迭代顺序随机会让报错信息（和修复前的租户归属）跨次漂移。
func TestHistory_SubsOrderStable(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	mk := func(name, tenant string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName(name),
			agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("ok")})),
			agent_v2.WithHistory(hs, tenant), agent_v2.WithMaxRounds(2))
	}
	build := func() agent_v2.Agent {
		return agent_v2.NewHandoffAgent(
			agent_v2.WithHandoffName("front"), agent_v2.WithHandoffInitial("m"),
		).AddAgent("m", mk("m", "tA")).
			AddAgent("z", mk("z", "tB")).
			AddAgent("a", mk("a", "tC"))
	}
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-order")
	var first string
	for i := 0; i < 12; i++ {
		_, err := build().Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话"))
		if err == nil {
			t.Fatalf("第 %d 次没报错——三个租户配置互不相同，必须冲突", i)
		}
		if i == 0 {
			first = err.Error()
			continue
		}
		if err.Error() != first {
			t.Fatalf("第 %d 次的报错跟第 1 次不同，说明子列表顺序不稳定：\n  1: %s\n  %d: %s",
				i, first, i, err.Error())
		}
	}
}

// 被 WithoutHistoryCtx 摘除的内部决策 LLM（Routing 的 classifier / Reflect 的
// reflectorAgent）也算历史配置来源。
//
// 它们自己的产出进不了历史，但 `WithHistory(store, owner)` 声明的是**这场会话属于哪个
// store / 哪个 owner**，跟声明者自己写不写无关。不收的后果有两个：编排里只有它是
// BasicAgent 时建不起写入器（边界补记全程 no-op）；以及配置冲突检查漏掉它——同类成员里
// evaluator 会报错、reflectorAgent 静默，两种行为。
func TestHistory_SuppressedMemberIsConfigSource(t *testing.T) {
	// Routing：分支全是外部执行体，只有 classifier 配了历史。
	hs := agent_v2.NewInMemoryHistoryStore()
	classifier := agent_v2.NewBasicAgent(
		agent_v2.WithName("classifier"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("br1")})),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	rt := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("rt"),
		agent_v2.WithRoutingClassifier(classifier),
	).AddBranch("br1", &externalAgent{name: "extBr"})

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-suppressed")
	if _, err := rt.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs, err := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-suppressed"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var found bool
	for _, m := range msgs {
		if strings.Contains(m.Content, "外部产出") {
			found = true
		}
	}
	if !found {
		t.Errorf("分支执行体的产出不在历史里——classifier 没被当成配置来源，"+
			"写入器根本没建起来，边界补记全程 no-op（历史共 %d 条）", len(msgs))
	}
	// classifier 自己的产出必须**不**在历史里（它被 WithoutHistoryCtx 摘除了）。
	for _, m := range msgs {
		if strings.TrimSpace(m.Content) == "br1" {
			t.Errorf("classifier 自己的产出进了历史——WithoutHistoryCtx 没生效")
		}
	}
}
