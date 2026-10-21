package mcp

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
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

// rpcResponse 是 JSON-RPC 2.0 响应信封；Result 和 Error 互斥。
// ID 使用 json.RawMessage：协议允许 id 为 number | string | null，
// 部分 server 实现会将数字 id 序列化为字符串（如 `"id":"1"`），
// 用 RawMessage 避免 json.Unmarshal 因类型不匹配丢弃整条响应。
type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
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

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func (e *rpcError) Error() string {
	return fmt.Sprintf("mcp rpc error %d: %s", e.Code, e.Message)
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

// parseToolsList 把 tools/list 的 raw result 解析为 []ToolSpec。
// 对 allow 白名单做服务端原始名过滤；allow 为空表示放行全部。
func parseToolsList(raw json.RawMessage, allow []string) ([]ToolSpec, error) {
	var body struct {
		Tools []struct {
			Name         string         `json:"name"`
			Description  string         `json:"description"`
			InputSchema  map[string]any `json:"inputSchema"`
			OutputSchema map[string]any `json:"outputSchema"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, fmt.Errorf("decode tools/list: %w", err)
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
		})
	}
	return out, nil
}

// parseCallResult 把 tools/call 的 raw result 解析为 CallResult。
// 协议要点：优先使用 structuredContent；content 是内容块列表。
func parseCallResult(raw json.RawMessage) (*CallResult, error) {
	var body struct {
		IsError           bool              `json:"isError"`
		Content           []json.RawMessage `json:"content"`
		StructuredContent any               `json:"structuredContent"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, fmt.Errorf("decode tools/call: %w", err)
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
		Structured: body.StructuredContent,
		Content:    blocks,
	}, nil
}

// decodeBlock 解析单个 content block，支持 text / image / 未知类型。
func decodeBlock(raw json.RawMessage) (Block, error) {
	var b struct {
		Type     string `json:"type"`
		Text     string `json:"text"`
		Data     string `json:"data"` // base64
		MimeType string `json:"mimeType"`
	}
	if err := json.Unmarshal(raw, &b); err != nil {
		return Block{}, fmt.Errorf("decode content block: %w", err)
	}
	block := Block{Type: b.Type, Text: b.Text, Mime: b.MimeType}
	if b.Data != "" {
		data, err := base64.StdEncoding.DecodeString(b.Data)
		if err != nil {
			return Block{}, fmt.Errorf("decode block base64: %w", err)
		}
		block.Data = data
	}
	return block, nil
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
