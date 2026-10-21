package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/store"
)

// TestHandoffActive_OwnerIsolatesTenants 钉住：两个租户递**同一个 threadID** 时，
// 转交状态互不可见。
//
// threadID 来自 wire——任何人都能递给我们，而且就该能（前端刷新重连拿的正是同一个）。
// 没有 owner 维时它同时被当成了授权凭据：实测租户 B 的全新对话被租户 A 的 active
// 直接送进了另一个候选。owner 进 ns（不是加一道闸）之后：同 owner 同 threadID 照常
// 续接，异 owner 读到空、走 initial——不存在 deny 决策，没有合法场景被打断。
//
// ⚠️ 断言落在**租户 B 实际被路由到了谁**，不落在「有没有报错」上——泄漏的实质是
// 「B 被送去了 A 的分支」，那条路上不会有任何错误。
func TestHandoffActive_OwnerIsolatesTenants(t *testing.T) {
	const sharedThreadID = "thread-shared-by-both-tenants"
	st := store.NewInMemoryStore()

	mk := func(name, reply string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName(name),
			agent_v2.WithModel(newScopeStub(schema.AssistantMessage(reply))))
	}
	build := func() *agent_v2.HandoffAgent {
		return agent_v2.NewHandoffAgent(
			agent_v2.WithHandoffName("desk"),
			agent_v2.WithHandoffInitial("billing"),
		).AddAgent("billing", mk("billing", "BILLING 接待")).
			AddAgent("tech", mk("tech", "TECH 接待"))
	}
	runAs := func(owner string) string {
		ctx := store.WithContext(context.Background(), st)
		ctx = compose_v2.WithThreadIDCtx(ctx, sharedThreadID)
		ctx = compose_v2.WithOwner(ctx, owner)
		ev, err := build().Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("你好"))
		if err != nil {
			t.Fatalf("owner=%q Run: %v", owner, err)
		}
		if m := agent_v2.FinalMessage(ev); m != nil {
			return m.Content
		}
		return ""
	}

	// 租户 A：**真跑一次并真转交**（billing 的模型吐 transfer(tech) 工具调用），让
	// active 走生产写入路径落盘。⚠️ 不手动 seed——手动 seed 得选一种 ns 形状，
	// owner 维被拆掉时 seed 读不到，测试会红在「前提不成立」而红不到「泄漏」上，
	// fail 信息指错地方。走生产路径则写读永远同一形状：维在 ⟹ 隔离；维没了 ⟹
	// B 真的继承 A 的转交，泄漏断言原样命中。
	transferring := agent_v2.NewBasicAgent(agent_v2.WithName("billing"),
		agent_v2.WithModel(newScopeStub(
			scopeTransferCall("tcA", "tech"),
			schema.AssistantMessage("BILLING 接待"))),
		agent_v2.WithHandoffTransfer())
	hA := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("desk"),
		agent_v2.WithHandoffInitial("billing"),
	).AddAgent("billing", transferring).
		AddAgent("tech", mk("tech", "TECH 接待"))
	ctxA := store.WithContext(context.Background(), st)
	ctxA = compose_v2.WithThreadIDCtx(ctxA, sharedThreadID)
	ctxA = compose_v2.WithOwner(ctxA, "tenant-A")
	if _, err := hA.Run(ctxA, agent_v2.NewRunSession(), schema.UserMessage("转给技术")); err != nil {
		t.Fatalf("租户 A 转交 Run: %v", err)
	}
	// 前提自证：A 的转交真落盘了——A 自己再进来应直达 tech。**不成立就 Fatal**：
	// fixture 没搭对时后面那条断言恒真（B 本来就会走 initial），测不出泄漏。
	if got := runAs("tenant-A"); !strings.Contains(got, "TECH") {
		t.Fatalf("前提不成立：租户 A 的转交没落盘（拿到 %q）", got)
	}

	// 租户 B：同一个 threadID，全新对话，应落在 initial（billing）。
	if got := runAs("tenant-B"); !strings.Contains(got, "BILLING") {
		t.Errorf("租户 B 递同一个 threadID 就继承了租户 A 的转交状态：拿到 %q，应为 BILLING", got)
	}
}

// TestHandoffActive_NoOwnerKeepsLegacyShape 钉住：没配 owner 时 ns 形状**跟以前完全
// 一致**，不是「多一个空段」。
//
// 多一个空段会让所有既有存档一次性失联——而那种失联是静默的（读不到就当没有，
// 走 initial），业务只会觉得「转交状态偶尔丢」。
func TestHandoffActive_NoOwnerKeepsLegacyShape(t *testing.T) {
	const tid = "t1"
	st := store.NewInMemoryStore()
	// 按**旧形状**（无 owner 段）预置。
	if err := st.Put(context.Background(),
		store.NS("__handoff_active__", "desk", tid), "active", "tech"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	mk := func(name, reply string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName(name),
			agent_v2.WithModel(newScopeStub(schema.AssistantMessage(reply))))
	}
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("desk"),
		agent_v2.WithHandoffInitial("billing"),
	).AddAgent("billing", mk("billing", "BILLING 接待")).
		AddAgent("tech", mk("tech", "TECH 接待"))

	ctx := store.WithContext(context.Background(), st)
	ctx = compose_v2.WithThreadIDCtx(ctx, tid)
	ev, err := h.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("你好"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	m := agent_v2.FinalMessage(ev)
	if m == nil || !strings.Contains(m.Content, "TECH") {
		t.Errorf("没配 owner 时应读得到旧形状的存档（拿到 %v）——ns 形状变了，既有存档全部失联", m)
	}
}
