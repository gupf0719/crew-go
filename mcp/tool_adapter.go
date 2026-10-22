package mcp

import (
	"context"
	"strings"

	"crew-go/tool"
)

// RemoteTool 把 MCP 远端工具适配为框架标准 tool.Tool。
// Name 是框架内的名字（默认 CanonicalName，见 WithToolNamer），对下游（LLM tool schema / Toolkit）隐藏 MCP 细节；
// Call 经所属 server 的当前连接调 Client.CallTool，按 CallResult 语义回填 tool.Result。
type RemoteTool struct {
	srv         *server
	canonical   string
	sourceName  string
	description string
	schema      map[string]any
	annotations *ToolAnnotations
	// trustAnnotations：调用方显式信任这个 server 的自述（WithTrustedAnnotations / WithTrustedClient），只有这时才认
	// 它的只读声明，见 ReadOnly。
	trustAnnotations bool
}

// NewRemoteTool 构造一个直接绑在 client 上的 RemoteTool（名字用 CanonicalName）。一般由 Hub 自动创建，
// 不建议直接调用：这样建的工具没有掉线判定与重连，调用失败的处理同 Hub 的（见 Call）。
//
// **audit¹⁸ HIGH-2 fix**: isClientNil 检 typed nil Client (协议十八复证).
func NewRemoteTool(client Client, spec ToolSpec) *RemoteTool {
	if isClientNil(client) {
		panic("NewRemoteTool: nil Client (含 Go typed nil interface 陷阱)")
	}
	s := newServer(nil, client.Name(), client, nil, nil)
	s.client, s.online = client, true
	return newRemoteTool(s, CanonicalName(client.Name(), spec.Name), cloneSpecs([]ToolSpec{spec})[0])
}

func newRemoteTool(s *server, name string, spec ToolSpec) *RemoteTool {
	schema := spec.InputSchema
	if schema == nil {
		schema = map[string]any{"type": "object"}
	}
	return &RemoteTool{
		srv:         s,
		canonical:   name,
		sourceName:  spec.Name,
		description: spec.Description,
		schema:      schema,
		annotations: spec.Annotations.clone(),
		// 信任与否是 server 的配置（WithTrustedAnnotations），NewRemoteTool 直接建的不信任
		trustAnnotations: s.cfg.trustAnnotations,
	}
}

// ReadOnly 实现 tool.ReadOnlyDeclarer：**只有**调用方显式信任这个 server（[WithTrustedAnnotations]）、
// server 声明了 readOnlyHint=true、且没有同时声明 destructiveHint=true 时才返回 true。
//
// 默认不认：MCP 规范明说工具注解是提示、不能凭不受信任的 server 的注解做决定——认了的话，一个 server
// 给写操作标上只读，就绕过了审批的只读放行档。两个声明自相矛盾（只读又有破坏性）时按有副作用处理。
func (t *RemoteTool) ReadOnly() bool {
	a := t.annotations
	if !t.trustAnnotations || a == nil || a.ReadOnlyHint == nil || !*a.ReadOnlyHint {
		return false
	}
	return a.DestructiveHint == nil || !*a.DestructiveHint
}

// Annotations 返回 server 对这个工具的自述（副本；server 没给时为 nil）。业务的审批启发式可以拿它做
// 自己的判断——它是提示、不保证如实，见 [ToolAnnotations]。
func (t *RemoteTool) Annotations() *ToolAnnotations { return t.annotations.clone() }

func (t *RemoteTool) Name() string                { return t.canonical }
func (t *RemoteTool) Description() string         { return t.description }
func (t *RemoteTool) InputSchema() map[string]any { return t.schema }

// SourceName 返回服务端原始工具名（用于调试/日志）。
func (t *RemoteTool) SourceName() string { return t.sourceName }

// ClientName 返回来源 MCP server 的逻辑名。
func (t *RemoteTool) ClientName() string { return t.srv.name }

// IsRemoteTool 判断一个 tool.Tool 是否来自 MCP（*RemoteTool）。给工具级渐进披露的 Deferred
// 谓词用——`WithToolSearch(&ToolSearchConfig{Deferred: mcp.IsRemoteTool})` 即「延迟披露所有
// MCP 工具」。MCP Hub 聚合多 server 易工具爆炸，是渐进披露的主要场景。用类型断言而非名字前缀
// （CanonicalName 的 "mcp_" 前缀理论上普通工具也能撞）。
func IsRemoteTool(t tool.Tool) bool {
	_, ok := t.(*RemoteTool)
	return ok
}

// Call 调远端工具。返回 error 只在调用方 ctx 已结束（取消 / 到期）时；其余失败都是 tool.Failure（模型看到
// is_error）：server 离线 / 连接出错 → ErrCodeServerUnavailable，超过 WithCallTimeout → tool.ErrCodeTimeout，
// 重连后 server 上已没有这个工具 → tool.ErrCodeNotFound，server 报的错（isError、JSON-RPC 错误、空结果）→
// tool.ErrCodeInternal。
func (t *RemoteTool) Call(ctx context.Context, args map[string]any) (*tool.Result, error) {
	return t.srv.call(ctx, t.canonical, t.sourceName, args)
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
