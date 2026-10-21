package a2a_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"

	"crew-go/a2a"
	"crew-go/a2a/registry"
	"crew-go/a2a/server"
	"crew-go/agent_v2"
	"crew-go/schema"
)

// echoAgent 是测试用的最小 agent_v2.Agent——回显输入。验证 server 适配 + 全互通环
// （无需真 LLM）。
type echoAgent struct{}

func (echoAgent) Name() string        { return "echo_agent" }
func (echoAgent) Description() string { return "echoes the input back" }

func (echoAgent) Run(_ context.Context, sess *agent_v2.RunSession, input *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	ev := agent_v2.NewExitEvent("echo_agent", schema.AssistantMessage("echo: "+input.Content))
	sess.AddEvent(ev)
	return []*agent_v2.AgentEvent{ev}, nil
}

func (echoAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, fmt.Errorf("echo: resume not supported")
}

// TestFullInteropLoop 验证全互通环：crew-go agent →[server 适配] A2A server →[注册] 服务
// 目录 →[发现] →[RemoteAgent 调用]→ 拿回结果。证明"注册+发现"双向打通端到端。
func TestFullInteropLoop(t *testing.T) {
	ctx := context.Background()

	// 1. 把 echoAgent 暴露成 A2A server（in-process，net.Listen 拿端口建 baseURL）。
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	baseURL := "http://" + ln.Addr().String()
	go func() { _ = http.Serve(ln, server.Handler(echoAgent{}, baseURL, testOwnerOpt())) }()
	t.Cleanup(func() { _ = ln.Close() })

	// 2. 注册进服务目录。
	dir := registry.NewInMemory()
	if _, err := dir.Register(ctx, registry.DefaultNamespace, server.Instance("echo-svc", "", "i1", baseURL)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// 3. 发现 + 建 RemoteAgent。
	ra, err := a2a.NewRemoteAgentFromService(ctx, dir, registry.DefaultNamespace, "echo-svc", nil)
	if err != nil {
		t.Fatalf("NewRemoteAgentFromService: %v", err)
	}
	if ra.Name() != "echo_agent" {
		t.Errorf("发现的 agent 名应来自远程 card, got %q", ra.Name())
	}

	// 4. 调用 → 回显。
	events, err := ra.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "echo: hi" {
		t.Fatalf("全互通环应返 'echo: hi', got %v", final)
	}
}
