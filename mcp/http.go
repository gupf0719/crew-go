package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

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
		return nil, err
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
	return parseCallResult(raw)
}

func (c *StreamableHTTPClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.initialized = false
	c.cachedTools = nil
	return nil
}

// rpc 执行一次 JSON-RPC 请求，返回 result 的 raw bytes。
func (c *StreamableHTTPClient) rpc(ctx context.Context, method string, params map[string]any) (json.RawMessage, error) {
	req := rpcRequest{
		JSONRPC: "2.0",
		ID:      c.nextID.Add(1),
		Method:  method,
		Params:  params,
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	for k, v := range c.headers {
		httpReq.Header.Set(k, v)
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		// review-v6 M-26 fix: 读 error body 让业务定位错误原因 — 之前 io.Copy(io.Discard)
		// 只为连接复用丢了 body 含的 JSON-RPC error message / server 返的具体错信息.
		// 业务调试体验差. ReadAll 同样排空 body 也允许连接复用.
		body, _ := io.ReadAll(resp.Body)
		if len(body) > 0 {
			return nil, fmt.Errorf("http status %d: %s", resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("http status %d", resp.StatusCode)
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	var env rpcResponse
	if err := json.Unmarshal(respBody, &env); err != nil {
		return nil, fmt.Errorf("decode envelope: %w", err)
	}
	if env.Error != nil {
		return nil, env.Error
	}
	return env.Result, nil
}
