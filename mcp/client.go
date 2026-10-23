// Package mcp 提供 Model Context Protocol 客户端集成。
//
// 通过 Client 接口连接远端 MCP Server（stdio 子进程或 streamable HTTP），
// 将其暴露的工具通过 RemoteTool 适配为框架标准 tool.Tool，
// 再由 Hub 聚合多个 Client 的工具形成统一的 *tool.Toolkit，
// 供 agent.BasicAgent 或任意编排模式复用。
//
// Hub 的工具集在 Start 时定下、之后不变；工具调用的失败（server 掉线、超时、报错）都作为失败结果交给模型，
// 不让 Run 失败；WithPartialStart 让连不上的 server 按快照注册工具、后台重连。见 Hub godoc 与
// doc/v2/mcp.md。
//
// 依赖方向：schema ← tool ← mcp ← agent（mcp 自身不感知 agent）。
package mcp

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
)

// Client 抽象一个 MCP Server 连接。
// 实现需保证 Initialize/Close 幂等，ListTools/CallTool 并发安全。
//
// 生命周期：Initialize → ListTools/CallTool（任意并发）→ Close。
// Close 后 Client 不可再用；如需重连请新建实例（Hub 用 WithClientFactory 注册的 server 掉线后就这么做）。
// StdioClient 的 Close 会终止子进程，属终态；StreamableHTTPClient 的 Close 仅重置状态，
// 但调用方不应依赖此行为，统一视作终态处理。
type Client interface {
	// Name 返回客户端逻辑名，用于生成工具的 canonical name（mcp_<name>_<source>）。
	Name() string
	// Initialize 建立连接、握手协议版本；多次调用仅首次生效。
	Initialize(ctx context.Context) error
	// ListTools 返回远端可用工具列表，实现内部可做缓存。
	ListTools(ctx context.Context) ([]ToolSpec, error)
	// CallTool 调用远端工具，name 为 server 原始名（非 canonical name）。
	//
	// 返回的 error 按「这次调用失败」处理（转成工具失败交给模型，调用方 ctx 已结束时除外）：server 答的 JSON-RPC
	// 错误交出 *RPCError（server 活着，不标掉线）；确定请求没发出时包上 ErrNotSent（模型被告知「没有发出」）；
	// 其余算连接出错（Hub 把 server 标成掉线、后台重连，模型被告知「不确定是否已执行」）。
	// 不要从这里返回控制流信号（中断等）——它们会被当成工具失败。
	CallTool(ctx context.Context, name string, args map[string]any) (*CallResult, error)
	// Close 释放资源（子进程、HTTP 连接、goroutine）；多次调用应安全。
	// Close 后 Client 不可再用，如需重连请新建实例。
	Close() error
}

// ToolSpec 是 MCP 远端工具的元数据。
type ToolSpec struct {
	// Name 服务端原始工具名。
	Name string
	// Description 工具描述，透传给 LLM。
	Description string
	// InputSchema JSON Schema（object 类型），直接作为 tool.Tool.InputSchema 返回。
	InputSchema map[string]any
	// OutputSchema 可选的输出 schema，V1 暂不使用。
	OutputSchema map[string]any
	// Annotations 是 server 对这个工具行为的自述（MCP 的 tool annotations），server 没给时为 nil。
	Annotations *ToolAnnotations
}

// ToolAnnotations 是 MCP server 对工具行为的自述。按 MCP 规范它们**全是提示、不保证如实**：
// 来自不受信任的 server 时不能据此做决定（比如跳过审批）。框架只在调用方显式信任这个 server 时
// （[WithTrustedClient]）才认它的只读声明，见 [RemoteTool.ReadOnly]。
//
// 字段是指针：没给与给了 false 分得开。规范的缺省语义是保守的——没给 readOnlyHint 即不是只读、
// 没给 destructiveHint 即可能有破坏性。
type ToolAnnotations struct {
	Title           string `json:"title,omitempty"`
	ReadOnlyHint    *bool  `json:"readOnlyHint,omitempty"`
	DestructiveHint *bool  `json:"destructiveHint,omitempty"`
	IdempotentHint  *bool  `json:"idempotentHint,omitempty"`
	OpenWorldHint   *bool  `json:"openWorldHint,omitempty"`
}

// clone 深拷贝（指针字段各拷一份）。
func (a *ToolAnnotations) clone() *ToolAnnotations {
	if a == nil {
		return nil
	}
	c := *a
	dup := func(p *bool) *bool {
		if p == nil {
			return nil
		}
		v := *p
		return &v
	}
	c.ReadOnlyHint, c.DestructiveHint = dup(a.ReadOnlyHint), dup(a.DestructiveHint)
	c.IdempotentHint, c.OpenWorldHint = dup(a.IdempotentHint), dup(a.OpenWorldHint)
	return &c
}

// CallResult 是一次 MCP 工具调用的结果。
// 对齐 MCP 协议 tools/call 响应的结构：
//   - IsError=true 表示业务级失败（LLM 可见）
//   - Structured 对应 structuredContent 字段，优先于 Content 暴露
//   - Content 为内容块列表（文本/图像/资源引用），IsError=false 且无 Structured 时使用
type CallResult struct {
	IsError    bool
	Structured any
	Content    []Block
}

// Block 是 MCP 内容块的框架表示。
// Type 取值遵循 MCP 协议："text" | "image" | "audio" | "resource" | "resource_link" 等。
//
// 嵌入资源（resource）的内容摊平到同一层：resource.text → Text，resource.blob → Data，resource.uri → URI，
// resource.mimeType → Mime。资源链接（resource_link）的 uri / name 进 URI / Name。
type Block struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
	Data []byte `json:"data,omitempty"`
	Mime string `json:"mimeType,omitempty"`
	URI  string `json:"uri,omitempty"`
	Name string `json:"name,omitempty"`

	// textResource：这是文本资源（resource.text 有这个键，可能是空串——空文件），不是二进制。
	textResource bool
	// undecodable：data / blob 的 base64 解不了，Data 为空。
	undecodable bool
}

// FirstText 返回 blocks 中首个 text 块的内容，无则空串。
func FirstText(blocks []Block) string {
	for _, b := range blocks {
		if b.Type == "text" && b.Text != "" {
			return b.Text
		}
	}
	return ""
}

// CanonicalName 生成工具在框架内的唯一名：mcp_<sanitize(clientName)>_<sanitize(sourceName)>。
// 加 client 前缀，避免多 Server 工具重名冲突。
// 超过 64 字符时对末尾做 sha1 截断，保证 OpenAI/Qwen tool name 合法性。
func CanonicalName(clientName, sourceName string) string {
	name := "mcp_" + sanitize(clientName) + "_" + sanitize(sourceName)
	if len(name) <= 64 {
		return name
	}
	sum := sha1.Sum([]byte(name))
	return name[:56] + hex.EncodeToString(sum[:4]) // 56 + 8 = 64
}

// sanitize 将非 [A-Za-z0-9_] 字符转义；非 ASCII（如 CJK）用 _%x 保留原始信息，
// 避免多个 CJK-only 工具名 sanitize 后全部变成 "___ " 产生碰撞。
// 空串兜底为 "tool"。
func sanitize(value string) string {
	if value == "" {
		return "tool"
	}
	buf := make([]byte, 0, len(value))
	for _, ch := range value {
		switch {
		case ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9', ch == '_':
			buf = append(buf, byte(ch))
		case ch < 0x80:
			// ASCII 特殊字符替换为下划线
			buf = append(buf, '_')
		default:
			// 非 ASCII（CJK 等）用十六进制编码保留区分度
			buf = append(buf, []byte(fmt.Sprintf("_%x", ch))...)
		}
	}
	return string(buf)
}
