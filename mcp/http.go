package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
// 支持 JSON、SSE 响应及通过 [StreamableHTTPClient.Subscribe] 接收独立推送。
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

	mu           sync.Mutex
	session      *httpSession
	initializing *httpInitialization
	generation   uint64
}

type httpInitialization struct {
	done       chan struct{}
	cancel     context.CancelFunc
	generation uint64
	session    *httpSession // 与 err、retry 在关闭 done 前写入。
	err        error
	retry      bool
}

type httpSession struct {
	id         string
	version    string
	generation uint64
	lifetime   context.Context // 独立的会话寿命，仅用于在过期或 Close 时取消这个会话的网络请求
	cancel     context.CancelFunc

	toolsMu     sync.Mutex
	cachedTools []ToolSpec
}

const mcpSessionHeader = "Mcp-Session-Id"
const mcpProtocolHeader = "MCP-Protocol-Version"
const lastEventIDHeader = "Last-Event-ID"

func (s *httpSession) requestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.lifetime, cancel)
	if s.lifetime.Err() != nil {
		cancel()
	}
	return ctx, func() {
		stop()
		cancel()
	}
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
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c.mu.Lock()
		if session := c.session; session != nil {
			c.mu.Unlock()
			return session, nil
		}
		pending := c.initializing
		if pending == nil {
			requestCtx, cancel := context.WithCancel(ctx)
			c.generation++
			pending = &httpInitialization{done: make(chan struct{}), cancel: cancel, generation: c.generation}
			c.initializing = pending
			c.mu.Unlock()
			return c.finishInitialization(requestCtx, pending)
		}
		c.mu.Unlock()
		select {
		case <-pending.done:
			if !pending.retry {
				return pending.session, pending.err
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *StreamableHTTPClient) finishInitialization(ctx context.Context, pending *httpInitialization) (session *httpSession, err error) {
	// 即使自定义传输 panic，也要解开同一握手的等待者，由调用方按自己的边界处理 panic。
	defer func() {
		pending.cancel()
		c.mu.Lock()
		publish := c.initializing == pending
		if publish {
			c.initializing = nil
			if session != nil {
				session.generation = pending.generation
			}
			c.session = session
			pending.session = session
			pending.retry = session == nil // 不继承握手发起方的取消，等待者可重新准入。
			if err != nil {
				pending.err = &httpGenerationError{err, c, pending.generation}
			}
			close(pending.done)
		}
		c.mu.Unlock()
		if !publish && session != nil {
			session.cancel()
			_ = c.terminateSession(session) // Close 时握手刚完成，由完成方清理得到的远端会话。
		}
		session, err = pending.session, pending.err
	}()
	return c.initialize(ctx)
}

func (c *StreamableHTTPClient) initialize(ctx context.Context) (session *httpSession, err error) {
	raw, sessionID, err := c.rpc(ctx, nil, "initialize", buildInitializeParams(c.protocolVersion, c.clientName, c.clientVersion))
	candidate := &httpSession{id: sessionID, version: c.protocolVersion}
	defer func() {
		if session == nil {
			_ = c.terminateSession(candidate) // 尽力释放握手未完成的远端会话，返回原始初始化错误。
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
	candidate.version = result.ProtocolVersion
	if err := c.initializedNotification(ctx, candidate); err != nil {
		return nil, fmt.Errorf("mcp notifications/initialized: %w", err)
	}
	candidate.lifetime, candidate.cancel = context.WithCancel(context.Background())
	return candidate, nil
}

func (c *StreamableHTTPClient) ListTools(ctx context.Context) ([]ToolSpec, error) {
	session, err := c.activeSession(ctx)
	if err != nil {
		return nil, err
	}
	session.toolsMu.Lock()
	defer session.toolsMu.Unlock()
	if err := session.lifetime.Err(); err != nil {
		return nil, err
	}
	if session.cachedTools != nil {
		return append([]ToolSpec{}, session.cachedTools...), nil
	}
	tools, err := listAllTools(ctx, func(ctx context.Context, method string, params map[string]any) (json.RawMessage, error) {
		result, _, err := c.rpc(ctx, session, method, params)
		return result, err
	}, c.allowTools)
	if err != nil {
		c.expireSession(session, err)
		return nil, err
	}
	session.cachedTools = append([]ToolSpec{}, tools...)
	return append([]ToolSpec{}, session.cachedTools...), nil
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
		c.expireSession(session, err)
		cause := fmt.Errorf("mcp tools/call %s: %w", name, err)
		return nil, &httpGenerationError{cause, c, session.generation}
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
	if session != nil {
		session.cancel()
	}
	if pending := c.initializing; pending != nil {
		c.initializing = nil
		pending.err = &httpGenerationError{context.Canceled, c, pending.generation}
		close(pending.done)
		pending.cancel()
	}
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

func (c *StreamableHTTPClient) expireSession(session *httpSession, err error) {
	var status *httpStatusError
	if session.cancel == nil || session.id == "" || !errors.As(err, &status) || status.code != http.StatusNotFound {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if session == c.session {
		c.session = nil
		session.cancel()
	}
}

func (c *StreamableHTTPClient) setRequestHeaders(req *http.Request, session *httpSession) {
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	req.Header.Del(lastEventIDHeader) // 游标只属于一条正在续订的 SSE 流。
	req.Header.Set("Accept", "application/json, text/event-stream")
	if req.Method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Del(mcpSessionHeader)
	req.Header.Del(mcpProtocolHeader)
	if session != nil && session.id != "" {
		req.Header.Set(mcpSessionHeader, session.id)
	}
	if session != nil && session.version != "" {
		req.Header.Set(mcpProtocolHeader, session.version)
	}
}

func (c *StreamableHTTPClient) initializedNotification(ctx context.Context, session *httpSession) error {
	const notification = `{"jsonrpc":"2.0","method":"notifications/initialized"}`
	return c.postSessionMessage(ctx, session, []byte(notification))
}

func (c *StreamableHTTPClient) postSessionMessage(ctx context.Context, session *httpSession, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
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
	if session != nil {
		var release context.CancelFunc
		ctx, release = session.requestContext(ctx)
		defer release()
	}
	if timeout := c.httpClient.Timeout; timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout) // POST 和所有续订共享原有 RPC 时限。
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return nil, "", fmt.Errorf("%w: %w", ErrNotSent, err)
	}
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
	responseSessionID := resp.Header.Get(mcpSessionHeader)
	secrets := []string{responseSessionID}
	if session != nil {
		secrets = append(secrets, session.id)
	}
	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
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
	if session == nil && !validSessionID(responseSessionID) {
		_ = resp.Body.Close() // 返回握手错误，不使用非法 ID 发送后续请求。
		return nil, "", errors.New("invalid session id")
	}
	var env rpcResponse
	if isEventStream(resp) {
		streamSession := session
		if streamSession == nil {
			// 初始化响应也是 SSE。续订可使用服务端已分配的 ID，版本要等响应到齐再协商。
			streamSession = &httpSession{id: responseSessionID}
		}
		env, err = c.readSSEReply(ctx, streamSession, resp.Body, idKey(req.ID))
	} else {
		defer resp.Body.Close()
		env, err = readJSONReply(resp.Body, idKey(req.ID))
	}
	if err != nil {
		if errors.Is(err, ErrNotSent) {
			err = &responseReadError{err} // GET 或应答服务端请求的失败不能证明原 POST 未发送。
		}
		return nil, responseSessionID, err
	}
	if env.Error != nil {
		return nil, responseSessionID, c.redactRPC(env.Error, secrets...)
	}
	return env.Result, responseSessionID, nil
}

func validSessionID(id string) bool {
	for i := 0; i < len(id); i++ {
		if id[i] < 0x21 || id[i] > 0x7e {
			return false
		}
	}
	return true
}

// httpGenerationError 携带这次调用所在 HTTP 会话的代数，供 Hub 丢弃旧会话的迟到失败。
type httpGenerationError struct {
	error
	client     *StreamableHTTPClient
	generation uint64
}

func (e *httpGenerationError) Unwrap() error { return e.error }

// responseReadError 保留响应读取错误的类型，但隔离属于后续请求的 ErrNotSent。
type responseReadError struct{ error }

func (e *responseReadError) Is(target error) bool {
	return target != ErrNotSent && errors.Is(e.error, target)
}

func (e *responseReadError) As(target any) bool { return errors.As(e.error, target) }

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
