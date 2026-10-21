package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
)

// StdioClient 通过子进程 stdio（按行 JSON-RPC）对接 MCP Server。
// 适合本地命令式 MCP（如 @modelcontextprotocol/server-filesystem）。
type StdioClient struct {
	name            string
	command         string
	args            []string
	env             []string
	protocolVersion string
	clientName      string
	clientVersion   string
	allowTools      []string
	closeTimeout    time.Duration

	nextID atomic.Int64

	startOnce sync.Once
	startErr  error

	mu          sync.Mutex
	cmd         *exec.Cmd
	stdin       io.WriteCloser
	stdout      *bufio.Scanner
	writeMu     sync.Mutex                   // 保护 pending map 及 stdin nil 检查
	sendMu      sync.Mutex                   // 串行化 stdin 写入，防止 pipe 满时阻塞 pending 操作
	pending     map[string]chan *rpcResponse // key = idKey(id)，兼容 string/number id
	done        chan struct{}
	initialized bool
	cachedTools []ToolSpec
}

// StdioOption 配置 StdioClient。
type StdioOption func(*StdioClient)

// WithStdioEnv 追加子进程环境变量（格式 "KEY=VAL"），默认继承当前 os.Environ。
func WithStdioEnv(env ...string) StdioOption {
	return func(s *StdioClient) { s.env = append(s.env, env...) }
}

// WithStdioProtocolVersion 设置 MCP 协议版本。
func WithStdioProtocolVersion(v string) StdioOption {
	return func(s *StdioClient) { s.protocolVersion = v }
}

// WithStdioClientInfo 设置 clientInfo.name / version。
func WithStdioClientInfo(name, version string) StdioOption {
	return func(s *StdioClient) {
		s.clientName = name
		s.clientVersion = version
	}
}

// WithStdioAllowTools 设置远端工具白名单（按 server 原始名过滤）。
func WithStdioAllowTools(names ...string) StdioOption {
	return func(s *StdioClient) { s.allowTools = append([]string{}, names...) }
}

// WithStdioCloseTimeout 设置 Close 时等待子进程退出的超时，默认 3s。
func WithStdioCloseTimeout(d time.Duration) StdioOption {
	return func(s *StdioClient) { s.closeTimeout = d }
}

// NewStdioClient 构造 stdio MCP 客户端。调用 Initialize 时才会启动子进程。
func NewStdioClient(name, command string, args []string, opts ...StdioOption) (*StdioClient, error) {
	if name == "" {
		return nil, fmt.Errorf("mcp: client name is required")
	}
	if command == "" {
		return nil, fmt.Errorf("mcp: command is required")
	}
	c := &StdioClient{
		name:            name,
		command:         command,
		args:            append([]string{}, args...),
		protocolVersion: defaultProtocolVersion,
		clientName:      "crew-mcp",
		clientVersion:   "1.0.0",
		closeTimeout:    3 * time.Second,
	}
	for _, opt := range opts {
		opt(c)
	}
	c.nextID.Store(1)
	return c, nil
}

func (c *StdioClient) Name() string { return c.name }

func (c *StdioClient) Initialize(ctx context.Context) error {
	// start() 内部会锁 mu，必须在 startOnce 完成后再持锁，避免死锁。
	c.startOnce.Do(func() { c.startErr = c.start() })
	if c.startErr != nil {
		return c.startErr
	}
	// 持锁整段执行，消除 double-checked locking race：
	// rpc()/notify() 只使用 writeMu，不会与 mu 形成死锁。
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialized {
		return nil
	}
	_, err := c.rpc(ctx, "initialize", buildInitializeParams(c.protocolVersion, c.clientName, c.clientVersion))
	if err != nil {
		return fmt.Errorf("mcp initialize: %w", err)
	}
	// 通知 server 初始化完成（MCP 协议要求）。
	if err := c.notify("notifications/initialized", map[string]any{}); err != nil {
		return fmt.Errorf("mcp notifications/initialized: %w", err)
	}
	c.initialized = true
	return nil
}

func (c *StdioClient) ListTools(ctx context.Context) ([]ToolSpec, error) {
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

func (c *StdioClient) CallTool(ctx context.Context, name string, args map[string]any) (*CallResult, error) {
	if err := c.Initialize(ctx); err != nil {
		return nil, err
	}
	if args == nil {
		args = map[string]any{}
	}
	raw, err := c.rpc(ctx, "tools/call", map[string]any{
		"name":      name,
		"arguments": args,
	})
	if err != nil {
		return nil, fmt.Errorf("mcp tools/call %s: %w", name, err)
	}
	return parseCallResult(raw)
}

// Close 关闭子进程：先关 stdin 触发 server 自然退出，再等待 cmd.Wait；超时则 Kill。
func (c *StdioClient) Close() error {
	c.mu.Lock()
	cmd := c.cmd
	done := c.done
	c.cmd = nil
	c.stdout = nil
	c.initialized = false
	c.cachedTools = nil
	c.mu.Unlock()

	// stdin 用 writeMu（与 rpc/notify 读 stdin 同锁——修 F1 分裂锁 race：原 stdin 写在 mu、
	// rpc/notify 读在 writeMu 两锁不互斥致 data race）。
	c.writeMu.Lock()
	stdin := c.stdin
	c.stdin = nil
	c.writeMu.Unlock()

	if cmd == nil {
		return nil
	}
	if stdin != nil {
		_ = stdin.Close()
	}
	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()
	// review-v6 M-25 fix: go.mod 是 1.22, time.After 在 fire 之前不 GC
	// (pre-1.23 timer 泄漏). 改 NewTimer + Stop 模式让 timer 主动释放.
	timer := time.NewTimer(c.closeTimeout)
	select {
	case <-waitCh:
		timer.Stop()
	case <-timer.C:
		_ = cmd.Process.Kill()
		<-waitCh
	}
	if done != nil {
		<-done
	}
	// 唤醒所有悬挂的请求。
	c.writeMu.Lock()
	for key, ch := range c.pending {
		close(ch)
		delete(c.pending, key)
	}
	c.writeMu.Unlock()
	return nil
}

// start 启动子进程并拉起 stdout 读取 goroutine。
func (c *StdioClient) start() error {
	cmd := exec.Command(c.command, c.args...)
	if len(c.env) > 0 {
		cmd.Env = append(os.Environ(), c.env...)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", c.command, err)
	}

	scanner := bufio.NewScanner(stdout)
	// JSON-RPC 消息可能很大，放宽 buffer 上限到 10MB。
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)

	c.mu.Lock()
	c.cmd = cmd
	c.stdout = scanner
	c.done = make(chan struct{})
	c.mu.Unlock()

	// stdin + pending 用 writeMu（对齐 writeMu godoc "保护 pending map 及 stdin nil 检查"，
	// 与 rpc/notify/Close 的 stdin+pending 访问同锁——修 F1 分裂锁 race：原写在 mu、rpc/notify
	// 读在 writeMu，两锁不互斥。start 是 startOnce 构造期虽 happens-before 后续访问，但统一
	// 锁避免"Initialize 与 Close 并发"时 start(mu) vs Close(writeMu) 写 stdin 的 race）。
	c.writeMu.Lock()
	c.stdin = stdin
	c.pending = make(map[string]chan *rpcResponse)
	c.writeMu.Unlock()

	go c.readLoop(scanner, c.done)
	return nil
}

// readLoop 持续读取 stdout，按 id 派发响应到 pending channel。
func (c *StdioClient) readLoop(scanner *bufio.Scanner, done chan struct{}) {
	defer close(done)
	// **F2 修复**：readLoop 退出（子进程 stdout 关闭/崩溃）后 drain pending——唤醒 in-flight
	// rpc waiters（否则 CallTool(无 deadline ctx) 永久挂到有人 Close 才被救）。defer LIFO：本
	// drain 后注册先跑、close(done) 后跑，故 Close 的 <-done 等到时 pending 已 drain；两处
	// drain 不 double-close（readLoop drain happens-before close(done) happens-before Close 的
	// <-done + drain，顺序不并发，且各自从 pending delete 后才 close）。rpc 侧 <-ch 收到 closed
	// 返 "connection closed"（stdio.go rpc select ok=false 分支）。
	defer func() {
		c.writeMu.Lock()
		for key, ch := range c.pending {
			close(ch)
			delete(c.pending, key)
		}
		c.writeMu.Unlock()
	}()
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		// 可能是响应，也可能是通知/请求（当前仅处理响应）。
		var env rpcResponse
		if err := json.Unmarshal(line, &env); err != nil {
			continue
		}
		key := responseIDKey(env.ID)
		if key == "" || key == "null" {
			// 通知或未匹配响应（无 id），跳过。
			continue
		}
		c.writeMu.Lock()
		ch, ok := c.pending[key]
		if ok {
			delete(c.pending, key)
		}
		c.writeMu.Unlock()
		if ok {
			ch <- &env
			close(ch)
		}
	}
}

// rpc 发送请求并等待响应；ctx 超时会解除等待但请求已写出（对端响应将被丢弃）。
func (c *StdioClient) rpc(ctx context.Context, method string, params map[string]any) (json.RawMessage, error) {
	id := c.nextID.Add(1)
	req := rpcRequest{JSONRPC: "2.0", ID: id, Method: method, Params: params}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	key := idKey(id)
	ch := make(chan *rpcResponse, 1)
	// 注册 pending（短暂持锁，不涉及 I/O）
	c.writeMu.Lock()
	if c.stdin == nil {
		c.writeMu.Unlock()
		return nil, errors.New("mcp stdio: client closed")
	}
	c.pending[key] = ch
	stdin := c.stdin // 持锁期间捕获引用，避免 Close 并发置 nil
	c.writeMu.Unlock()

	// 串行化写入（sendMu 独立于 writeMu，pipe 满阻塞不影响 pending 操作）
	c.sendMu.Lock()
	_, werr := stdin.Write(append(body, '\n'))
	c.sendMu.Unlock()
	if werr != nil {
		c.writeMu.Lock()
		delete(c.pending, key)
		c.writeMu.Unlock()
		return nil, fmt.Errorf("write: %w", werr)
	}

	select {
	case <-ctx.Done():
		c.writeMu.Lock()
		delete(c.pending, key)
		c.writeMu.Unlock()
		return nil, ctx.Err()
	case resp, ok := <-ch:
		if !ok || resp == nil {
			return nil, errors.New("mcp stdio: connection closed")
		}
		if resp.Error != nil {
			return nil, resp.Error
		}
		return resp.Result, nil
	}
}

// notify 发送 JSON-RPC 通知（无 id，不等响应）。
func (c *StdioClient) notify(method string, params map[string]any) error {
	msg := map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	if c.stdin == nil {
		c.writeMu.Unlock()
		return errors.New("mcp stdio: client closed")
	}
	stdin := c.stdin
	c.writeMu.Unlock()

	c.sendMu.Lock()
	_, err = stdin.Write(append(body, '\n'))
	c.sendMu.Unlock()
	return err
}
