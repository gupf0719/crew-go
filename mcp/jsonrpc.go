package mcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
)

// MCP 协议默认版本 "2025-03-26"。
const defaultProtocolVersion = "2025-03-26"

// rpcRequest 是 JSON-RPC 2.0 请求信封。
// 我方始终发送 int64 类型的 id。
type rpcRequest struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      int64          `json:"id"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params,omitempty"`
}

// ErrNotSent 标记「这次请求肯定没有发到 server」：连接没建起来、请求没写出去、连接早已关闭。Client 实现
// （CallTool）在确定没发出时用 %w 包上它，Hub 据此告诉模型「这次调用没有发出」——没包上的连接错误一律按
// 「不确定是否已执行」说，所以拿不准时别包。内置的 StdioClient / StreamableHTTPClient 都按这条做。
var ErrNotSent = errors.New("mcp: request was not sent")

// RPCError 是 server 答的 JSON-RPC 错误（server 活着，只是这次请求报错）。Client 实现（CallTool）把 server 的
// JSON-RPC 错误响应按它交出（可以 %w 包着），Hub 据此把它当成这次调用的失败、不把 server 标成掉线。
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("mcp rpc error %d: %s", e.Code, e.Message)
}

// badResponseError 是「server 答了，但结果解析不了」：请求已经到了 server（可能已执行），server 也活着。
type badResponseError struct{ err error }

func (e *badResponseError) Error() string { return "bad response: " + e.err.Error() }
func (e *badResponseError) Unwrap() error { return e.err }

// httpStatusError 是 HTTP 非 2xx 且响应体不是 JSON-RPC 错误。body 已截短、抹掉了自己发出的请求头的值。
type httpStatusError struct {
	code int
	body string
}

func (e *httpStatusError) Error() string {
	if e.body == "" {
		return fmt.Sprintf("http status %d", e.code)
	}
	return fmt.Sprintf("http status %d: %s", e.code, e.body)
}

// rpcResponse 是 JSON-RPC 2.0 响应信封；Result 和 Error 互斥。
// ID 使用 json.RawMessage：协议允许 id 为 number | string | null，
// 部分 server 实现会将数字 id 序列化为字符串（如 `"id":"1"`），
// 用 RawMessage 避免 json.Unmarshal 因类型不匹配丢弃整条响应。
type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

// responseIDKey 把响应的 raw id（可能是 number 或 string）规范化为字符串，
// 用于与 pending map 的 key（由 idKey 生成）比对。
// 规则：去除首尾空白后，若带引号则去引号，保持原始数字字符串。
// 例：`1` → "1"，`"1"` → "1"，`"uuid-abc"` → "uuid-abc"。
func responseIDKey(raw json.RawMessage) string {
	s := strings.TrimSpace(string(raw))
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		// 是 JSON string，去掉引号；内容简单不含转义时直接切片
		unquoted, err := strconv.Unquote(s)
		if err == nil {
			return unquoted
		}
		return s[1 : len(s)-1]
	}
	return s
}

// idKey 把我方发出的 int64 id 转为 pending map 的 key。
func idKey(id int64) string {
	return strconv.FormatInt(id, 10)
}

// buildInitializeParams 构造 initialize 请求的 params。
func buildInitializeParams(protocolVersion, clientName, clientVersion string) map[string]any {
	return map[string]any{
		"protocolVersion": protocolVersion,
		"capabilities":    map[string]any{},
		"clientInfo": map[string]any{
			"name":    clientName,
			"version": clientVersion,
		},
	}
}

// maxToolsListPages 是一次 tools/list 最多翻的页数：服务端的 cursor 出了问题（每页都给一个新的）时不至于一直翻下去。
const maxToolsListPages = 1000

// listAllTools 按 tools/list 的分页把工具拉全：带上一页的 nextCursor 再请求，直到它缺省或为空（MCP 规范：cursor 是
// 不透明串，到头时不给）。只取第一页的话，服务端一分页，第二页起的工具静默丢失、模型永远看不到。
//
// 服务端给回同一个 cursor（会一直翻同一页）或翻过 maxToolsListPages 页时报错，不返回半截列表——半截的工具表会让
// 缺的那些工具看起来不存在，比报错更难查。
func listAllTools(ctx context.Context, rpc func(context.Context, string, map[string]any) (json.RawMessage, error), allow []string) ([]ToolSpec, error) {
	var all []ToolSpec
	seen := map[string]bool{}
	params := map[string]any{}
	for page := 0; ; page++ {
		if page >= maxToolsListPages {
			return nil, fmt.Errorf("mcp tools/list: 翻了 %d 页还没到头（服务端的 nextCursor 可能有问题）", maxToolsListPages)
		}
		raw, err := rpc(ctx, "tools/list", params)
		if err != nil {
			return nil, fmt.Errorf("mcp tools/list: %w", err)
		}
		tools, next, err := parseToolsList(raw, allow)
		if err != nil {
			return nil, err
		}
		all = append(all, tools...)
		if next == "" {
			return all, nil
		}
		if seen[next] {
			return nil, fmt.Errorf("mcp tools/list: 服务端第 %d 页给回了已经用过的 cursor %q，再翻会一直重复", page+1, next)
		}
		seen[next] = true
		params = map[string]any{"cursor": next}
	}
}

// parseToolsList 把 tools/list 一页的 raw result 解析为 []ToolSpec 与下一页的 cursor（没有下一页时为空）。
// 对 allow 白名单做服务端原始名过滤；allow 为空表示放行全部。
//
// annotations 逐个工具宽松地解：一个工具的注解形状不对（字段类型错之类）只丢掉它自己的注解（按没有注解处理，
// 于是也不算只读）并告警，不让整份列表解析失败——注解只是提示（MCP 规范），一个写错的提示不该让这个 server
// 的所有工具都用不了。
func parseToolsList(raw json.RawMessage, allow []string) ([]ToolSpec, string, error) {
	var body struct {
		NextCursor string `json:"nextCursor"`
		Tools      []struct {
			Name         string          `json:"name"`
			Description  string          `json:"description"`
			InputSchema  map[string]any  `json:"inputSchema"`
			OutputSchema map[string]any  `json:"outputSchema"`
			Annotations  json.RawMessage `json:"annotations"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, "", fmt.Errorf("decode tools/list: %w", err)
	}
	allowSet := toSet(allow)
	out := make([]ToolSpec, 0, len(body.Tools))
	for _, t := range body.Tools {
		if len(allowSet) > 0 {
			if _, ok := allowSet[t.Name]; !ok {
				continue
			}
		}
		out = append(out, ToolSpec{
			Name:         t.Name,
			Description:  t.Description,
			InputSchema:  t.InputSchema,
			OutputSchema: t.OutputSchema,
			Annotations:  parseToolAnnotations(t.Name, t.Annotations),
		})
	}
	return out, body.NextCursor, nil
}

// parseToolAnnotations 解一个工具的 annotations；没有、是 null、或者形状不对时返回 nil（形状不对时告警）。
func parseToolAnnotations(toolName string, raw json.RawMessage) *ToolAnnotations {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var a ToolAnnotations
	if err := json.Unmarshal(raw, &a); err != nil {
		slog.Warn("mcp: 工具的 annotations 解析失败，按没有注解处理（也不算只读）", logKeyTool, toolName, logKeyErr, err)
		return nil
	}
	return &a
}

// parseCallResult 把 tools/call 的 raw result 解析为 CallResult。
// 协议要点：优先使用 structuredContent；content 是内容块列表。
//
// structuredContent 里的数字解成 json.Number（UseNumber）：解成 float64 的话，超过 2^53 的整数（雪花 ID 之类）
// 交给模型时已经变了样，而且编成 JSON 后看起来仍是一个精确的整数（1234567890123456789 → 1234567890123456800），
// 模型拿它调下一个工具就静默出错。json.Number 编 JSON 时原样输出。
func parseCallResult(raw json.RawMessage) (*CallResult, error) {
	var body struct {
		IsError           bool              `json:"isError"`
		Content           []json.RawMessage `json:"content"`
		StructuredContent json.RawMessage   `json:"structuredContent"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, fmt.Errorf("decode tools/call: %w", err)
	}
	var structured any
	if len(body.StructuredContent) > 0 {
		dec := json.NewDecoder(bytes.NewReader(body.StructuredContent))
		dec.UseNumber()
		if err := dec.Decode(&structured); err != nil {
			return nil, fmt.Errorf("decode tools/call structuredContent: %w", err)
		}
	}
	blocks := make([]Block, 0, len(body.Content))
	for _, item := range body.Content {
		b, err := decodeBlock(item)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, b)
	}
	return &CallResult{
		IsError:    body.IsError,
		Structured: structured,
		Content:    blocks,
	}, nil
}

// decodeBlock 解析单个 content block：text / image / audio / resource（嵌入资源摊平，见 [Block]）/
// resource_link / 未知类型（只留 type 与认得的字段）。
func decodeBlock(raw json.RawMessage) (Block, error) {
	var b struct {
		Type     string `json:"type"`
		Text     string `json:"text"`
		Data     string `json:"data"` // base64
		MimeType string `json:"mimeType"`
		URI      string `json:"uri"`
		Name     string `json:"name"`
		Resource *struct {
			URI      string `json:"uri"`
			MimeType string `json:"mimeType"`
			Text     *string `json:"text"` // 有这个键就是文本资源（空文件的 text 是空串）
			Blob     string  `json:"blob"` // base64
		} `json:"resource"`
	}
	if err := json.Unmarshal(raw, &b); err != nil {
		return Block{}, fmt.Errorf("decode content block: %w", err)
	}
	textResource := false
	if r := b.Resource; r != nil {
		b.URI, b.Data = r.URI, r.Blob
		if r.Text != nil {
			b.Text, textResource = *r.Text, true
		}
		if r.MimeType != "" {
			b.MimeType = r.MimeType
		}
	}
	block := Block{Type: b.Type, Text: b.Text, Mime: b.MimeType, URI: b.URI, Name: b.Name, textResource: textResource}
	if b.Data != "" {
		// 解不了不让整次调用失败：二进制内容本来就不交给模型，解码只为说明里的大小；为它报错的话，同一次结果里的
		// 文本也丢了，还会被报成「可能已执行」引着模型重试有副作用的工具。不规范的 base64（URL-safe、缺 padding）也认。
		block.Data, block.undecodable = decodeBase64(b.Data)
	}
	return block, nil
}

// decodeBase64 按标准、无 padding、URL-safe、URL-safe 无 padding 依次试；都不行时 undecodable 为 true。
func decodeBase64(s string) (data []byte, undecodable bool) {
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if d, err := enc.DecodeString(s); err == nil {
			return d, false
		}
	}
	return nil, true
}

func toSet(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(values))
	for _, v := range values {
		set[v] = struct{}{}
	}
	return set
}
