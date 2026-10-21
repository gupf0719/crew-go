// mcp_demo 演示 mcp 包接入 MCP Server——mcp.Hub 聚合多个 Client 的工具成统一 tool.Toolkit
// 供 BasicAgent 调用。为自包含，这里用 in-process fakeClient 模拟两个 MCP Server；真实场景
// 换成 mcp.NewStdioClient(命令, 参数)（stdio 子进程）或 mcp.NewStreamableHTTPClient(endpoint)
// （HTTP）即可，Hub 聚合 + agent 用法完全一样。
//
// 自包含，直接跑:
//
//	go run ./example/mcp/mcp_demo
package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"crew-go/agent_v2"
	"crew-go/mcp"
	"crew-go/model"
	"crew-go/schema"
)

func main() {
	// 两个 mock MCP Server（in-process）：weather 提供 get_weather，calc 提供 add
	weatherSrv := &fakeClient{
		name: "weather",
		tools: []mcp.ToolSpec{{
			Name:        "get_weather",
			Description: "查询指定城市的天气",
			InputSchema: map[string]any{
				"type":       "object",
				"properties": map[string]any{"city": map[string]any{"type": "string", "description": "城市名"}},
				"required":   []string{"city"},
			},
		}},
		results: map[string]*mcp.CallResult{
			"get_weather": {Content: []mcp.Block{{Type: "text", Text: "北京：晴 12°C"}}},
		},
	}
	calcSrv := &fakeClient{
		name: "calc",
		tools: []mcp.ToolSpec{{
			Name:        "add",
			Description: "两个整数相加",
			InputSchema: map[string]any{
				"type":       "object",
				"properties": map[string]any{"a": map[string]any{"type": "number"}, "b": map[string]any{"type": "number"}},
				"required":   []string{"a", "b"},
			},
		}},
		results: map[string]*mcp.CallResult{
			"add": {Content: []mcp.Block{{Type: "text", Text: "42"}}},
		},
	}

	// Hub 聚合多个 Client → 统一 toolkit（工具 canonical name = mcp_<server>_<tool>，避免多 server 重名）
	hub := mcp.NewHub(mcp.WithClient(weatherSrv), mcp.WithClient(calcSrv))
	if err := hub.Start(context.Background()); err != nil {
		log.Fatalf("Hub Start: %v", err)
	}
	defer func() { _ = hub.Close() }()

	tk := hub.Toolkit()
	fmt.Println("=== Hub 聚合的 MCP 工具（2 个 server → 1 个 toolkit）===")
	for _, t := range tk.List() {
		fmt.Printf("  - %-24s %s\n", t.Name(), t.Description())
	}

	// BasicAgent 直接用聚合后的 MCP toolkit——跟用本地工具无差别
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("mcp_bot"),
		agent_v2.WithModel(&mcpMockModel{}),
		agent_v2.WithToolkit(tk),
	)
	fmt.Println("\n=== BasicAgent 调 MCP 工具（问天气 → 调 mcp_weather_get_weather）===")
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("北京天气怎么样"))
	if err != nil {
		log.Fatalf("Run: %v", err)
	}
	if msg := agent_v2.FinalMessage(events); msg != nil {
		fmt.Printf("  agent> %s\n", msg.Content)
	}
}

// fakeClient 是 in-process mock MCP Client（实现 mcp.Client 接口），替代真 stdio/HTTP server
// 让 demo 自包含。真实场景用 mcp.NewStdioClient / mcp.NewStreamableHTTPClient。
type fakeClient struct {
	name    string
	tools   []mcp.ToolSpec
	results map[string]*mcp.CallResult
}

func (f *fakeClient) Name() string                                        { return f.name }
func (f *fakeClient) Initialize(_ context.Context) error                  { return nil }
func (f *fakeClient) ListTools(_ context.Context) ([]mcp.ToolSpec, error) { return f.tools, nil }
func (f *fakeClient) CallTool(_ context.Context, name string, _ map[string]any) (*mcp.CallResult, error) {
	if r, ok := f.results[name]; ok {
		return r, nil
	}
	return &mcp.CallResult{IsError: true, Content: []mcp.Block{{Type: "text", Text: "not found: " + name}}}, nil
}
func (f *fakeClient) Close() error { return nil }

// mcpMockModel 是 mock LLM：看到"天气"就调 mcp_weather_get_weather（canonical name），有
// tool_result 就总结，否则文本回答（不调真 LLM key）。
type mcpMockModel struct{ tools []*schema.ToolInfo }

func (m *mcpMockModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	cp := *m
	cp.tools = tools
	return &cp, nil
}

func (m *mcpMockModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	// 本轮已有 tool_result → 总结
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == schema.RoleUser {
			break
		}
		if msgs[i].Role == schema.RoleTool {
			return schema.AssistantMessage("[mock] MCP 工具返回：" + msgs[i].Content), nil
		}
	}
	// 否则看 user 问天气 → 调 MCP 工具
	var last string
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == schema.RoleUser {
			last = msgs[i].Content
			break
		}
	}
	if strings.Contains(last, "天气") {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID:       "call_weather_1",
			Type:     "function",
			Function: schema.FunctionCall{Name: "mcp_weather_get_weather", Arguments: `{"city":"北京"}`},
		}}), nil
	}
	return schema.AssistantMessage("[mock] 试试问「北京天气怎么样」"), nil
}

func (m *mcpMockModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	resp, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(resp)
	schema.CloseSend(sr)
	return sr, nil
}
