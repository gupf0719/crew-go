package a2a_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/a2a"
	"crew-go/a2a/registry"
	"crew-go/agent_v2"
	"crew-go/schema"

	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// cardURL 把 httptest base 拼成完整 Agent Card URL（= spec 的 agentCardUrl）。
func cardURL(base string) string { return base + a2asrv.WellKnownAgentCardPath }

func TestDiscoverAndCall(t *testing.T) {
	base := startServer(t, fnExec(completedExec))
	ctx := context.Background()
	dir := registry.NewInMemory()
	if _, err := dir.Register(ctx, registry.DefaultNamespace, registry.ServiceInstance{
		SchemaVersion: registry.CurrentSchemaVersion,
		ServiceName:   "rich",
		InstanceID:    "i1",
		AgentCardURL:  cardURL(base),
		Status:        registry.StatusServing,
		Weight:        registry.WeightPtr(100),
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ra, err := a2a.NewRemoteAgentFromService(ctx, dir, registry.DefaultNamespace, "rich", nil)
	if err != nil {
		t.Fatalf("NewRemoteAgentFromService: %v", err)
	}
	events, err := ra.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "done: hi" {
		t.Fatalf("发现+调用应返 'done: hi', got %v", final)
	}
}

// TestDiscoverFailover：坏实例（死端口 card）+ 好实例并存，发现应 failover 到好的（§8）。
func TestDiscoverFailover(t *testing.T) {
	base := startServer(t, fnExec(completedExec))
	ctx := context.Background()
	dir := registry.NewInMemory()
	mustReg := func(id, card string) {
		if _, err := dir.Register(ctx, registry.DefaultNamespace, registry.ServiceInstance{
			SchemaVersion: registry.CurrentSchemaVersion, ServiceName: "rich", InstanceID: id,
			AgentCardURL: card, Status: registry.StatusServing, Weight: registry.WeightPtr(100),
		}); err != nil {
			t.Fatalf("Register %s: %v", id, err)
		}
	}
	mustReg("bad", "http://127.0.0.1:1/.well-known/agent-card.json") // 死端口
	mustReg("good", cardURL(base))

	ra, err := a2a.NewRemoteAgentFromService(ctx, dir, registry.DefaultNamespace, "rich", registry.NewRoundRobinSelector())
	if err != nil {
		t.Fatalf("有好实例时应 failover 成功, got err: %v", err)
	}
	events, _ := ra.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "done: hi" {
		t.Fatalf("failover 后调用应成功, got %v", final)
	}
}

// TestDiscoverAllBad：只有坏实例 → failover 耗尽 → 返 error（§8 发现层穷举失败才报错）。
func TestDiscoverAllBad(t *testing.T) {
	ctx := context.Background()
	dir := registry.NewInMemory()
	if _, err := dir.Register(ctx, registry.DefaultNamespace, registry.ServiceInstance{
		SchemaVersion: registry.CurrentSchemaVersion, ServiceName: "rich", InstanceID: "bad",
		AgentCardURL: "http://127.0.0.1:1/.well-known/agent-card.json", Status: registry.StatusServing, Weight: registry.WeightPtr(100),
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	_, err := a2a.NewRemoteAgentFromService(ctx, dir, registry.DefaultNamespace, "rich", nil)
	if err == nil {
		t.Fatal("全坏实例应返 error")
	}
	if errors.Is(err, a2a.ErrNoEligibleInstance) {
		t.Fatalf("全坏实例是『拉取失败』非『无实例』——不应命中 ErrNoEligibleInstance（两类失败要可分流）, got %v", err)
	}
}

func TestDiscoverNoInstance(t *testing.T) {
	dir := registry.NewInMemory()
	_, err := a2a.NewRemoteAgentFromService(context.Background(), dir, registry.DefaultNamespace, "ghost", nil)
	if !errors.Is(err, a2a.ErrNoEligibleInstance) {
		t.Fatalf("无实例应返 ErrNoEligibleInstance（业务 errors.Is 分流『等注册』vs『瞬态重试』）, got %v", err)
	}
}
