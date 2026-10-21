package observability_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/observability"
)

// TestInstall_WiresBothChains 钉住 Install **两条链都接上**。
//
// 这是这个入口存在的全部理由：两条链的安装点不在同一处（业务级是构造期 option、
// 编排级是 ctx），所以「只装一半」不报错、不崩，只是 8 个 metric 静默不 fire。
// 实测我们自己的 example 里 4 个有 2 个漏了编排那条。
func TestInstall_WiresBothChains(t *testing.T) {
	obs, err := observability.New(observability.Config{
		Resource: observability.Resource{AppName: "t", EnvName: "test", IP: "1.2.3.4", SDKVersion: "test"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, opts := obs.Install(context.Background(), agent_v2.WithName("x"))

	// ① 编排那条必须已经 push 进 ctx。
	if agent_v2.GetCtxOrchHooks(ctx) == nil {
		t.Error("Install 返回的 ctx 里没有编排 hook——8 个 metric 会静默不 fire")
	}

	// ② 业务那条必须已经追加进 opts。
	cfg := &agent_v2.Config{}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.Hooks == nil {
		t.Fatal("Install 返回的 opts 没配业务 hook")
	}
	if _, isNoOp := cfg.Hooks.(hook.NoOpHooks); isNoOp {
		t.Error("业务 hook 退化成 NoOp 了")
	}
}

// countingHooks 数 OnAgentStart 次数，用来验证业务自己的 hook 没被 Install 顶掉。
type countingHooks struct {
	hook.NoOpHooks
	starts int
}

func (c *countingHooks) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	c.starts++
	return ctx, nil
}

// TestInstall_DoesNotClobberBusinessHooks 钉住「业务自己也配了 hook 时不会被顶掉」。
//
// `WithHooks` 是**覆盖写**（后写赢）。Install 要在业务的 opts 后面追加 obs 的 hook，
// 如果直接用 WithHooks，业务那份会被静默丢掉——业务的审计/计费 hook 一声不响地不再执行。
// 所以 Install 走的是 WithHooksChained。
func TestInstall_DoesNotClobberBusinessHooks(t *testing.T) {
	obs, err := observability.New(observability.Config{
		Resource: observability.Resource{AppName: "t", EnvName: "test", IP: "1.2.3.4", SDKVersion: "test"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	biz := &countingHooks{}

	_, opts := obs.Install(context.Background(),
		agent_v2.WithName("x"),
		agent_v2.WithHooks(biz), // 业务先配了自己的
	)
	cfg := &agent_v2.Config{}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.Hooks == nil {
		t.Fatal("没配上 hook")
	}
	// 直接调一次，看业务那份还在不在链上。
	info := &hook.AgentInfo{Name: "x"}
	if _, err := cfg.Hooks.OnAgentStart(context.Background(), info, &hook.AgentInput{}); err != nil {
		t.Fatalf("OnAgentStart: %v", err)
	}
	if biz.starts != 1 {
		t.Errorf("业务 hook 被 Install 顶掉了（OnAgentStart 跑了 %d 次，期望 1）——"+
			"WithHooks 是覆盖写，追加 obs 时必须走 WithHooksChained", biz.starts)
	}
}
