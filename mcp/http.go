package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// maxResponseBytes 是一个 JSON-RPC 响应体的上限（同 stdio 的单行上限），超了按「结果解析不了」处理。
const maxResponseBytes = 10 * 1024 * 1024

// maxErrorBodyBytes 是错误信息里带的响应体长度上限：错误会进日志与 Hub.Status。
const maxErrorBodyBytes = 512

// StreamableHTTPClient 通过 streamable HTTP JSON-RPC 对接 MCP Server。
// 支持 JSON 和 SSE 响应。服务端分配会话时，后续请求携带会话标识。
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
	session     *httpSession
	cachedTools []ToolSpec
}

type httpSession struct {
	id      string
	version string
}

const mcpSessionHeader = "Mcp-Session-Id"
const mcpProtocolHeader = "MCP-Protocol-Version"

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
	client := *c.httpClient
	checkRedirect := client.CheckRedirect
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) > 0 {
			original := via[0]
			if req.Method != original.Method || req.URL.Scheme != original.URL.Scheme ||
				!strings.EqualFold(req.URL.Host, original.URL.Host) {
				return http.ErrUseLastResponse
			}
		}
		if checkRedirect != nil {
			return checkRedirect(req, via)
		}
		if len(via) >= 10 {
			return fmt.Errorf("stopped after 10 redirects")
		}
		return nil
	}
	c.httpClient = &client
	c.nextID.Store(1)
	return c, nil
}

func (c *StreamableHTTPClient) Name() string { return c.name }

func (c *StreamableHTTPClient) Initialize(ctx context.Context) error {
	_, err := c.activeSession(ctx)
	return err
}

func (c *StreamableHTTPClient) activeSession(ctx context.Context) (*httpSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.initializeLocked(ctx)
}

func (c *StreamableHTTPClient) initializeLocked(ctx context.Context) (*httpSession, error) {
	if c.session != nil {
		return c.session, nil
	}
	raw, sessionID, err := c.rpc(ctx, nil, "initialize", buildInitializeParams(c.protocolVersion, c.clientName, c.clientVersion))
	for _, b := range []byte(sessionID) {
		if b < 0x21 || b > 0x7e {
			return nil, fmt.Errorf("mcp initialize: invalid session id")
		}
	}
	session := &httpSession{id: sessionID, version: c.protocolVersion}
	defer func() {
		if c.session != session {
			_ = c.terminateSession(session) // 尽力释放握手未完成的远端会话，返回原始初始化错误。
		}
	}()
	if err != nil {
		return nil, fmt.Errorf("mcp initialize: %w", err)
	}
	var result struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("mcp initialize: decode result: %w", err)
	}
	if result.ProtocolVersion == "" {
		result.ProtocolVersion = c.protocolVersion
	}
	switch result.ProtocolVersion {
	case c.protocolVersion, "2025-03-26", "2025-06-18", "2025-11-25":
	default:
		return nil, fmt.Errorf("mcp initialize: unsupported negotiated protocol version %q", c.redact([]byte(result.ProtocolVersion), sessionID))
	}
	session.version = result.ProtocolVersion
	if err := c.initializedNotification(ctx, session); err != nil {
		return nil, fmt.Errorf("mcp notifications/initialized: %w", err)
	}
	c.session = session
	return session, nil
}

func (c *StreamableHTTPClient) ListTools(ctx context.Context) ([]ToolSpec, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	session, err := c.initializeLocked(ctx)
	if err != nil {
		return nil, err
	}
	if c.cachedTools != nil {
		return append([]ToolSpec{}, c.cachedTools...), nil
	}
	tools, err := listAllTools(ctx, func(ctx context.Context, method string, params map[string]any) (json.RawMessage, error) {
		result, _, err := c.rpc(ctx, session, method, params)
		return result, err
	}, c.allowTools)
	if err != nil {
		c.expireSessionLocked(session, err)
		return nil, err
	}
	c.cachedTools = append([]ToolSpec{}, tools...)
	return append([]ToolSpec{}, c.cachedTools...), nil
}

func (c *StreamableHTTPClient) CallTool(ctx context.Context, name string, args map[string]any) (*CallResult, error) {
	session, err := c.activeSession(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNotSent, err) // 握手没过，tools/call 还没发
	}
	if args == nil {
		args = map[string]any{}
	}
	params := map[string]any{
		"name":      name,
		"arguments": args,
	}
	raw, _, err := c.rpc(ctx, session, "tools/call", params)
	if err != nil {
		c.mu.Lock()
		c.expireSessionLocked(session, err)
		c.mu.Unlock()
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
	session := c.session
	c.session = nil
	c.cachedTools = nil
	c.mu.Unlock()
	return c.terminateSession(session)
}

func (c *StreamableHTTPClient) terminateSession(session *httpSession) error {
	if session == nil || session.id == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.endpoint, nil)
	if err != nil {
		return c.redactTransportError(err, session.id)
	}
	c.setRequestHeaders(req, session)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return c.redactTransportError(err, session.id)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 == 2 || resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return &httpStatusError{code: resp.StatusCode}
}

func (c *StreamableHTTPClient) expireSessionLocked(session *httpSession, err error) {
	var status *httpStatusError
	if session == c.session && session.id != "" && errors.As(err, &status) && status.code == http.StatusNotFound {
		c.session = nil
		c.cachedTools = nil
	}
}

func (c *StreamableHTTPClient) setRequestHeaders(req *http.Request, session *httpSession) {
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Accept", "application/json, text/event-stream")
	if req.Method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	if session != nil {
		req.Header.Set(mcpProtocolHeader, session.version)
		req.Header.Del(mcpSessionHeader)
		if session.id != "" {
			req.Header.Set(mcpSessionHeader, session.id)
		}
	} else {
		req.Header.Del(mcpSessionHeader)
		req.Header.Del(mcpProtocolHeader)
	}
}

func (c *StreamableHTTPClient) initializedNotification(ctx context.Context, session *httpSession) error {
	const notification = `{"jsonrpc":"2.0","method":"notifications/initialized"}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, strings.NewReader(notification))
	if err != nil {
		return c.redactTransportError(err, session.id)
	}
	c.setRequestHeaders(req, session)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return c.redactTransportError(err, session.id)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes)) // 尽力提取协议错误文本。
		return &httpStatusError{code: resp.StatusCode, body: c.redact(body, session.id)}
	}
	return nil
}

// rpc 执行一次 JSON-RPC 请求，返回 result 的 raw bytes。
//
// 错误分档（Hub 据此决定告诉模型什么、标不标掉线）：请求没写出去就失败 → 包 ErrNotSent；server 答 JSON-RPC 错误
// （2xx 或非 2xx 响应体里都可能）→ *RPCError；其余非 2xx → httpStatusError；2xx 但响应解析不了 → badResponseError；
// 写出之后连接出错 → 原样（不确定是否已执行）。
func (c *StreamableHTTPClient) rpc(ctx context.Context, session *httpSession, method string, params map[string]any) (json.RawMessage, string, error) {
	req := rpcRequest{
		JSONRPC: "2.0",
		ID:      c.nextID.Add(1),
		Method:  method,
		Params:  params,
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, "", fmt.Errorf("%w: encode request: %w", ErrNotSent, err)
	}
	// 记下请求有没有写出：连请求头都没写出就失败（连不上、dial 被时限打断……）时 server 肯定没收到。不能从错误形状
	// 推断——请求 ctx 结束时 net/http 交出的是 ctx 的错，错误链上没有 dial 错误。也不能只看 WroteRequest：HTTP/2 在 ctx
	// 结束时 Do 立刻返回、不等写请求的 goroutine，WroteRequest 可能在 Do 返回之后才到，而请求其实已经完整发出、执行了。
	// 所以只在「trace 确实生效了（GetConn 到过）、请求头与请求体都没写出」时才算没发出；h2 中止流之后不会再发 DATA 帧，
	// 请求头没写出就什么都没发。拿不准的（自定义 RoundTripper 不走 trace、h1 大请求体写到一半）一律按「不确定」。
	var gotConn, headersOut, wrote atomic.Bool
	trace := &httptrace.ClientTrace{
		GetConn:      func(string) { gotConn.Store(true) },
		WroteHeaders: func() { headersOut.Store(true) },
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				wrote.Store(true)
			}
		},
	}
	httpReq, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, "", fmt.Errorf("%w: %w", ErrNotSent, c.redactTransportError(err))
	}
	c.setRequestHeaders(httpReq, session)
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		if session != nil {
			err = c.redactTransportError(err, session.id)
		} else {
			err = c.redactTransportError(err)
		}
		if gotConn.Load() && !headersOut.Load() && !wrote.Load() {
			return nil, "", fmt.Errorf("%w: %w", ErrNotSent, err)
		}
		return nil, "", err
	}
	defer resp.Body.Close()
	responseSessionID := resp.Header.Get(mcpSessionHeader)
	secrets := []string{responseSessionID}
	if session != nil {
		secrets = append(secrets, session.id)
	}
	if resp.StatusCode/100 != 2 {
		// review-v6 M-26 fix: 读 error body 让业务定位错误原因（ReadAll 同样排空 body，连接可复用）。
		// JSON-RPC 错误放在非 2xx 响应体里的 server 也有：解得出来就按 server 的错误交出（它活着）——鉴权 / 地址 / 会话
		// 不对的（401 / 403 / 404）除外，那几种按状态码分档（标掉线），原文只进日志与 Status。
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1)) // 尽力提取协议错误文本。
		var env rpcResponse
		if json.Unmarshal(respBody, &env) == nil && env.Error != nil {
			switch resp.StatusCode {
			case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
				return nil, "", &httpStatusError{code: resp.StatusCode, body: c.redact([]byte(env.Error.Message), secrets...)}
			}
			return nil, "", c.redactRPC(env.Error, secrets...)
		}
		return nil, "", &httpStatusError{code: resp.StatusCode, body: c.redact(respBody, secrets...)}
	}
	contentType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")) // 缺省时按 JSON 读取。
	env, err := readHTTPResponse(resp.Body, contentType, idKey(req.ID))
	if err != nil {
		return nil, responseSessionID, err
	}
	if env.Error != nil {
		return nil, responseSessionID, c.redactRPC(env.Error, secrets...)
	}
	return env.Result, responseSessionID, nil
}

type redactedTransportError struct {
	message string
	cause   error
}

func (e *redactedTransportError) Error() string { return e.message }
func (e *redactedTransportError) Unwrap() error { return e.cause }

func (c *StreamableHTTPClient) redactTransportError(err error, additional ...string) error {
	var requestError *url.Error
	if !errors.As(err, &requestError) {
		return err
	}
	endpoint, parseErr := url.Parse(requestError.URL)
	address := "MCP endpoint"
	if parseErr == nil {
		endpoint.User, endpoint.RawQuery, endpoint.Fragment = nil, "", ""
		address = c.redact([]byte(endpoint.String()), additional...)
	}
	return &redactedTransportError{
		message: fmt.Sprintf("%s %q: %s", requestError.Op, address, c.redact([]byte(requestError.Err.Error()), additional...)),
		cause:   err,
	}
}

// redactRPC 抹掉 server 的 JSON-RPC 错误信息里的秘密：这段文字会原样交给模型（进 provider、进会话历史）。
func (c *StreamableHTTPClient) redactRPC(e *RPCError, additional ...string) *RPCError {
	cp := *e
	cp.Message = c.redact([]byte(e.Message), additional...)
	return &cp
}

// redact 把响应体截短（不切断多字节字符），并抹掉自己发出的秘密：请求头的值（有的 server 在错误里回显
// Authorization）、endpoint 里的 userinfo、query 值与长的路径段（有的服务把 token 放在 URL 里，404 时回显路径）。
func (c *StreamableHTTPClient) redact(body []byte, additional ...string) string {
	text := string(body)
	for _, secret := range additional {
		if secret != "" {
			text = strings.ReplaceAll(text, secret, "<redacted>")
		}
	}
	for _, cand := range c.secrets() {
		if len(cand) >= 6 {
			text = strings.ReplaceAll(text, cand, "<redacted>")
		}
	}
	if len(text) > maxErrorBodyBytes {
		cut := maxErrorBodyBytes
		for cut > 0 && !utf8.RuneStart(text[cut]) {
			cut--
		}
		text = text[:cut] + "…"
	}
	return strings.ToValidUTF8(text, "�")
}

// secrets 是 redact 要抹掉的值。
func (c *StreamableHTTPClient) secrets() []string {
	var out []string
	for _, v := range c.headers {
		out = append(out, v)
		if _, tok, ok := strings.Cut(v, " "); ok { // "Bearer <token>" 只回显 token 的也抹
			out = append(out, tok)
		}
	}
	if u, err := url.Parse(c.endpoint); err == nil {
		if pw, ok := u.User.Password(); ok {
			out = append(out, pw)
		}
		for _, vs := range u.Query() {
			out = append(out, vs...)
		}
		for _, seg := range strings.Split(u.Path, "/") {
			if len(seg) >= 16 {
				out = append(out, seg)
			}
		}
	}
	return out
}
