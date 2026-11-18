package mcp

import (
	"context"
	"encoding/base64"
	"fmt"
	"mime"
	"net/http"
	"slices"
	"strings"

	"crew-go/schema"
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
//
// 注册之后又连上拉过清单时，最近一次拉到的那份也要声明只读：注册时那份可能来自快照（上一次运行存下的），server 现在
// 可能已经把它改成有副作用、或去掉了只读声明；工具定义按会话冻结（发给模型的那份不变），审批看的是 server 现在怎么说。
// 任何一份不说只读就不放行，最近的清单里已经没有这个工具时同样不放行。
func (t *RemoteTool) ReadOnly() bool {
	if !t.trustAnnotations || !annotationsReadOnly(t.annotations) {
		return false
	}
	if cur, listed := t.srv.listedAnnotations(t.sourceName); listed && !annotationsReadOnly(cur) {
		return false
	}
	return true
}

// annotationsReadOnly 报一份注解是不是声明了只读（readOnlyHint=true 且没有同时声明 destructiveHint=true）。
func annotationsReadOnly(a *ToolAnnotations) bool {
	if a == nil || a.ReadOnlyHint == nil || !*a.ReadOnlyHint {
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

// adaptToolResult 保留原有文本投影，另带上可透传的内嵌媒体。结构化正文优先，媒体仍按相对顺序保留。
func adaptToolResult(result *CallResult) *tool.Result {
	parts := make([]schema.ToolResultPart, 0, len(result.Content))
	structured := result.Structured != nil
	hasMedia := false
	for _, b := range result.Content {
		part := toolResultPart(b)
		hasMedia = hasMedia || part.Data != ""
		if structured && part.Data == "" {
			continue
		}
		if !structured {
			part.Text = blockText(b, part.Data != "")
		}
		if part.Text != "" || part.Data != "" {
			parts = append(parts, part)
		}
	}
	if structured {
		out := tool.Success(result.Structured)
		if hasMedia {
			out.Parts = append([]schema.ToolResultPart{{Text: tool.OutputText(result.Structured)}}, parts...)
		}
		return out
	}
	out := tool.Success(schema.ToolResultPartsText(parts))
	if hasMedia {
		out.Parts = parts
	}
	return out
}

func toolResultPart(block Block) schema.ToolResultPart {
	if (block.Type != "image" && block.Type != "audio") || len(block.Data) == 0 || block.undecodable {
		return schema.ToolResultPart{}
	}
	if block.Audience != nil && !slices.Contains(block.Audience, "assistant") {
		return schema.ToolResultPart{}
	}
	mediaType, _, err := mime.ParseMediaType(block.Mime)
	if err != nil || !strings.HasPrefix(mediaType, block.Type+"/") {
		return schema.ToolResultPart{}
	}
	if mediaType == "image/jpg" {
		mediaType = "image/jpeg"
	}
	if !matchesMediaHeader(mediaType, block.Data) {
		return schema.ToolResultPart{}
	}
	return schema.ToolResultPart{MediaType: mediaType, Data: base64.StdEncoding.EncodeToString(block.Data)}
}

// matchesMediaHeader 在常用送模格式上挡住明显错标的负载。未知格式仍可供业务读取。
func matchesMediaHeader(mediaType string, data []byte) bool {
	switch mediaType {
	case "image/png", "image/jpeg", "image/gif", "image/webp":
		return http.DetectContentType(data) == mediaType
	case "audio/wav", "audio/x-wav", "audio/wave", "audio/vnd.wave":
		return http.DetectContentType(data) == "audio/wave"
	case "audio/mpeg", "audio/mp3":
		return len(data) >= 10 && http.DetectContentType(data) == "audio/mpeg" ||
			len(data) >= 4 && data[0] == 0xff && data[1]&0xe0 == 0xe0 && data[1]&0x06 != 0
	}
	return true
}

// blockText 是一个内容块交给模型的文本：text 原样；带文本的嵌入资源在正文前标出 URI；资源链接写成一行；
// 其余（图片、音频、二进制资源、不认识的类型）写一行说明：类型、MIME、大小、URI。
func blockText(b Block, attachment bool) string {
	switch {
	case b.Type == "text":
		return b.Text
	case b.Text != "" || b.textResource:
		text := b.Text
		if text == "" {
			text = "（空）"
		}
		if b.URI != "" {
			return fmt.Sprintf("[资源 %s]\n%s", b.URI, text)
		}
		return text
	case b.Type == "resource_link":
		return strings.Join(nonEmpty("[资源链接", b.Name, b.URI), " ") + "]"
	default:
		desc := nonEmpty(b.Type, b.Mime)
		switch {
		case b.undecodable:
			desc = append(desc, "base64 解不了")
		case len(b.Data) > 0:
			desc = append(desc, fmt.Sprintf("%d 字节", len(b.Data)))
		}
		desc = append(desc, nonEmpty(b.URI)...)
		if attachment {
			return "[" + strings.Join(desc, "，") + "：附件]"
		}
		return "[" + strings.Join(desc, "，") + "：二进制内容，没有交给模型]"
	}
}

func nonEmpty(ss ...string) []string {
	out := ss[:0:0]
	for _, s := range ss {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}
