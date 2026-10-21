package mcp_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/compose_v2"
	"crew-go/mcp"
)

//go:noinline
func mcpInitializeExplodes() { panic("initialize boom") }

// explodingClient 的 Initialize panic——Hub.Start 兜住它。
type explodingClient struct{}

func (explodingClient) Name() string                     { return "exploding" }
func (explodingClient) Initialize(context.Context) error { mcpInitializeExplodes(); return nil }
func (explodingClient) ListTools(context.Context) ([]mcp.ToolSpec, error) {
	return nil, nil
}
func (explodingClient) CallTool(context.Context, string, map[string]any) (*mcp.CallResult, error) {
	return nil, nil
}
func (explodingClient) Close() error { return nil }

// TestHub_StartPanicCarriesStack：业务 client 启动时 panic，Hub.Start 返回的错误上挂着带栈的 compose_v2.PanicError，
// 错误文本不带栈。
func TestHub_StartPanicCarriesStack(t *testing.T) {
	err := mcp.NewHub(mcp.WithClient(explodingClient{})).Start(context.Background())
	if err == nil {
		t.Fatal("该返回错误")
	}
	var pe *compose_v2.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("errors.As 取不到 PanicError：%v", err)
	}
	if !strings.Contains(pe.Stack, "mcp_test.mcpInitializeExplodes") {
		t.Fatalf("栈里没有 panic 发生处：\n%s", pe.Stack)
	}
	if strings.Contains(err.Error(), "goroutine ") {
		t.Errorf("错误文本不该带栈：%q", err.Error())
	}
}
