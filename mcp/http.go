package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// maxResponseBytes 是一个 JSON-RPC 响应体的上限（同 stdio 的单行上限），超了按「结果解析不了」处理。
const maxResponseBytes = 10 * 1024 * 1024

// maxErrorBodyBytes 是错误信息里带的响应体长度上限：错误会进日志与 Hub.Status。
const maxErrorBodyBytes = 512

// StreamableHTTPClient 通过 streamable HTTP JSON-RPC 对接 MCP Server。
// 每次调用都是独立 POST 请求，适合无状态服务端。
type StreamableHTTPClient struct {
	name            string
	endpoint        string
	httpClient      *http.Client
	protocolVersion string
	clientName      string
	clientVersion   string
	allowTools      []string
	headers         map[string]string

	nextID atomic.Int64

	mu          sync.Mutex
	initialized bool
	cachedTools []ToolSpec
}

// HTTPOption 配置 StreamableHTTPClient。
type HTTPOption func(*StreamableHTTPClient)

// WithHTTPClient 注入自定义 *http.Client，不设置时使用 20s 超时的默认客户端。
func WithHTTPClient(c *http.Client) HTTPOption {
	return func(s *StreamableHTTPClient) { s.httpClient = c }
}

// WithHTTPProtocolVersion 设置 MCP 协议版本。
func WithHTTPProtocolVersion(v string) HTTPOption {
	return func(s *StreamableHTTPClient) { s.protocolVersion = v }
}

// WithHTTPClientInfo 设置 clientInfo.name / version。
func WithHTTPClientInfo(name, version string) HTTPOption {
	return func(s *StreamableHTTPClient) {
		s.clientName = name
		s.clientVersion = version
	}
}

// WithHTTPHeader 追加一个固定请求头（如鉴权 token）。
func WithHTTPHeader(key, value string) HTTPOption {
	return func(s *StreamableHTTPClient) {
		if s.headers == nil {
			s.headers = map[string]string{}
		}
		s.headers[key] = value
	}
}

// WithHTTPAllowTools 设置远端工具白名单（按 server 原始名过滤）。
func WithHTTPAllowTools(names ...string) HTTPOption {
	return func(s *StreamableHTTPClient) {
		s.allowTools = append([]string{}, names...)
	}
}

// NewStreamableHTTPClient 构造 streamable HTTP MCP 客户端。
// name 为逻辑名（用于 CanonicalName），endpoint 为 JSON-RPC POST 地址。
func NewStreamableHTTPClient(name, endpoint string, opts ...HTTPOption) (*StreamableHTTPClient, error) {
	if name == "" {
		return nil, fmt.Errorf("mcp: client name is required")
	}
	if endpoint == "" {
		return nil, fmt.Errorf("mcp: endpoint is required")
	}
	c := &StreamableHTTPClient{
		name:            name,
		endpoint:        endpoint,
		protocolVersion: defaultProtocolVersion,
		clientName:      "crew-mcp",
		clientVersion:   "1.0.0",
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.httpClient == nil {
		c.httpClient = &http.Client{Timeout: 20 * time.Second}
	}
	c.nextID.Store(1)
	return c, nil
}

func (c *StreamableHTTPClient) Name() string { return c.name }

func (c *StreamableHTTPClient) Initialize(ctx context.Context) error {
	// 持锁整段执行，消除 double-checked locking race：
	// rpc() 只使用 httpClient（无锁），不会与 mu 形成死锁。
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialized {
		return nil
	}
	_, err := c.rpc(ctx, "initialize", buildInitializeParams(c.protocolVersion, c.clientName, c.clientVersion))
	if err != nil {
		return fmt.Errorf("mcp initialize: %w", err)
	}
	c.initialized = true
	return nil
}

func (c *StreamableHTTPClient) ListTools(ctx context.Context) ([]ToolSpec, error) {
	if err := c.Initialize(ctx); err != nil {
		return nil, err
	}
	// 持锁整段执行，消除并发重复拉取 tools/list 的 race。
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cachedTools != nil {
		return append([]ToolSpec{}, c.cachedTools...), nil
	}
	raw, err := c.rpc(ctx, "tools/list", map[string]any{})
	if err != nil {
		return nil, fmt.Errorf("mcp tools/list: %w", err)
	}
	tools, err := parseToolsList(raw, c.allowTools)
	if err != nil {
		return nil, err
	}
	c.cachedTools = append([]ToolSpec{}, tools...)
	return append([]ToolSpec{}, c.cachedTools...), nil
}

func (c *StreamableHTTPClient) CallTool(ctx context.Context, name string, args map[string]any) (*CallResult, error) {
	if err := c.Initialize(ctx); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNotSent, err) // 握手没过，tools/call 还没发
	}
	if args == nil {
		args = map[string]any{}
	}
	params := map[string]any{
		"name":      name,
		"arguments": args,
	}
	raw, err := c.rpc(ctx, "tools/call", params)
	if err != nil {
		return nil, fmt.Errorf("mcp tools/call %s: %w", name, err)
	}
	res, err := parseCallResult(raw)
	if err != nil {
		return nil, &badResponseError{fmt.Errorf("mcp tools/call %s: %w", name, err)}
	}
	return res, nil
}

func (c *StreamableHTTPClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.initialized = false
	c.cachedTools = nil
	return nil
}

// rpc 执行一次 JSON-RPC 请求，返回 result 的 raw bytes。
//
// 错误分档（Hub 据此决定告诉模型什么、标不标掉线）：请求没写出去就失败 → 包 ErrNotSent；server 答 JSON-RPC 错误
// （2xx 或非 2xx 响应体里都可能）→ *RPCError；其余非 2xx → httpStatusError；2xx 但响应解析不了 → badResponseError；
// 写出之后连接出错 → 原样（不确定是否已执行）。
func (c *StreamableHTTPClient) rpc(ctx context.Context, method string, params map[string]any) (json.RawMessage, error) {
	req := rpcRequest{
		JSONRPC: "2.0",
		ID:      c.nextID.Add(1),
		Method:  method,
		Params:  params,
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("%w: encode request: %w", ErrNotSent, err)
	}
	// 记下请求有没有完整写出：没写出就失败（连不上、dial 被时限打断……）时 server 肯定没收到。不能从错误形状推断——
	// 请求 ctx 结束时 net/http 交出的是 ctx 的错，错误链上没有 dial 错误。
	var wrote atomic.Bool
	trace := &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if info.Err == nil {
			wrote.Store(true)
		}
	}}
	httpReq, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNotSent, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	for k, v := range c.headers {
		httpReq.Header.Set(k, v)
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		if !wrote.Load() {
			return nil, fmt.Errorf("%w: %w", ErrNotSent, err)
		}
		return nil, err
	}
	defer resp.Body.Close()
	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if resp.StatusCode/100 != 2 {
		// review-v6 M-26 fix: 读 error body 让业务定位错误原因（ReadAll 同样排空 body，连接可复用）。
		// JSON-RPC 错误放在非 2xx 响应体里的 server 也有：解得出来就按 server 的错误交出（它活着）。
		var env rpcResponse
		if json.Unmarshal(respBody, &env) == nil && env.Error != nil {
			return nil, env.Error
		}
		return nil, &httpStatusError{code: resp.StatusCode, body: c.redact(respBody)}
	}
	if readErr != nil {
		return nil, fmt.Errorf("read response: %w", readErr)
	}
	if len(respBody) > maxResponseBytes {
		return nil, &badResponseError{fmt.Errorf("response exceeds %d bytes", maxResponseBytes)}
	}
	var env rpcResponse
	if err := json.Unmarshal(respBody, &env); err != nil {
		return nil, &badResponseError{fmt.Errorf("decode envelope: %w", err)}
	}
	if env.Error != nil {
		return nil, env.Error
	}
	return env.Result, nil
}

// redact 把响应体截短，并抹掉自己发出的请求头的值（有的 server 在错误里回显 Authorization）。
func (c *StreamableHTTPClient) redact(body []byte) string {
	text := string(body)
	for _, v := range c.headers {
		candidates := []string{v}
		if _, tok, ok := strings.Cut(v, " "); ok { // "Bearer <token>" 只回显 token 的也抹
			candidates = append(candidates, tok)
		}
		for _, cand := range candidates {
			if len(cand) >= 6 {
				text = strings.ReplaceAll(text, cand, "<redacted>")
			}
		}
	}
	if len(text) > maxErrorBodyBytes {
		text = text[:maxErrorBodyBytes] + "…"
	}
	return text
}
