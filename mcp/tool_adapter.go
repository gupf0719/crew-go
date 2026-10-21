package mcp

import (
	"context"
	"fmt"
	"strings"

	"crew-go/tool"
)

// RemoteTool 把 MCP 远端工具适配为框架标准 tool.Tool。
// Name 是 CanonicalName，对下游（LLM tool schema / Toolkit）隐藏 MCP 细节；
// Call 内部桥接 Client.CallTool，按 CallResult 语义回填 tool.Result。
type RemoteTool struct {
	client      Client
	canonical   string
	sourceName  string
	description string
	schema      map[string]any
}

// NewRemoteTool 构造一个 RemoteTool。一般由 Hub 自动创建，不建议直接调用。
//
// **audit¹⁸ HIGH-2 fix**: isClientNil 检 typed nil Client (协议十八复证).
func NewRemoteTool(client Client, spec ToolSpec) *RemoteTool {
	if isClientNil(client) {
		panic("NewRemoteTool: nil Client (含 Go typed nil interface 陷阱)")
	}
	schema := spec.InputSchema
	if schema == nil {
		schema = map[string]any{"type": "object"}
	}
	return &RemoteTool{
		client:      client,
		canonical:   CanonicalName(client.Name(), spec.Name),
		sourceName:  spec.Name,
		description: spec.Description,
		schema:      schema,
	}
}

func (t *RemoteTool) Name() string                { return t.canonical }
func (t *RemoteTool) Description() string         { return t.description }
func (t *RemoteTool) InputSchema() map[string]any { return t.schema }

// SourceName 返回服务端原始工具名（用于调试/日志）。
func (t *RemoteTool) SourceName() string { return t.sourceName }

// ClientName 返回来源 MCP 客户端的逻辑名。
func (t *RemoteTool) ClientName() string { return t.client.Name() }

// IsRemoteTool 判断一个 tool.Tool 是否来自 MCP（*RemoteTool）。给工具级渐进披露的 Deferred
// 谓词用——`WithToolSearch(&ToolSearchConfig{Deferred: mcp.IsRemoteTool})` 即「延迟披露所有
// MCP 工具」。MCP Hub 聚合多 server 易工具爆炸，是渐进披露的主要场景。用类型断言而非名字前缀
// （CanonicalName 的 "mcp_" 前缀理论上普通工具也能撞）。
func IsRemoteTool(t tool.Tool) bool {
	_, ok := t.(*RemoteTool)
	return ok
}

func (t *RemoteTool) Call(ctx context.Context, args map[string]any) (*tool.Result, error) {
	res, err := t.client.CallTool(ctx, t.sourceName, args)
	if err != nil {
		// transport / 协议层错误：向上传递，reactLoop 会映射为框架级 internal_error。
		return nil, fmt.Errorf("mcp call %s: %w", t.canonical, err)
	}
	if res.IsError {
		msg := FirstText(res.Content)
		if msg == "" {
			msg = "mcp tool call failed"
		}
		return tool.Failure(tool.ErrCodeInternal, msg), nil
	}
	if res.Structured != nil {
		return tool.Success(res.Structured), nil
	}
	return tool.Success(flattenContent(res.Content)), nil
}

// flattenContent 把 MCP content blocks 整理为对 LLM 友好的输出：
//   - 全为 text block：拼接为字符串
//   - 含非 text block：text 拼接后与其余 blocks 打包为 map
func flattenContent(blocks []Block) any {
	if len(blocks) == 0 {
		return ""
	}
	var texts []string
	var others []Block
	for _, b := range blocks {
		if b.Type == "text" {
			texts = append(texts, b.Text)
		} else {
			others = append(others, b)
		}
	}
	joined := strings.Join(texts, "\n\n")
	if len(others) == 0 {
		return joined
	}
	return map[string]any{"text": joined, "blocks": others}
}
