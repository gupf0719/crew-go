package mcp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"reflect"
	"sync"
	"time"

	"crew-go/internal/panicerr"
	"crew-go/tool"
)

// ErrCodeServerUnavailable 是 server 连不上 / 掉线时工具调用返回的失败代码（tool.Result.Error.Code）。
// BasicAgent 交给模型的 tool_result 是「[server_unavailable] 该能力暂时不可用（<server> offline）……」。
const ErrCodeServerUnavailable = "server_unavailable"

// ServerOption 配置 Hub 里的一个 server，随 WithClient / WithClientFactory 传入。
type ServerOption func(*serverConfig)

type serverConfig struct {
	snapshot    []ToolSpec
	hasSnapshot bool
	callTimeout time.Duration
	serial      bool
}

// WithToolSnapshot 给这个 server 一份工具清单快照（通常是上次连上时 ServerStatus.Tools 存下来的那份）。
//
// 只在开了 WithPartialStart、Start 时它连不上的情况下用：按快照注册它的工具，让工具集跟它在线时一样，
// 调用这些工具返回「该能力暂时不可用」直到它重连上。Start 时连上了就按它实际报的清单注册，快照不用。
// 传空切片表示「它上次就没有工具」，跟不给快照一样不注册。
func WithToolSnapshot(specs []ToolSpec) ServerOption {
	cp := cloneSpecs(specs)
	return func(c *serverConfig) { c.snapshot, c.hasSnapshot = cp, true }
}

// WithCallTimeout 给这个 server 的每次工具调用一个时限（d <= 0 = 不设，默认）。超过时限返回失败结果
// （tool.ErrCodeTimeout，告诉模型 server 那边可能仍在执行），server 不因此算掉线。开了 WithSerialCalls 时
// 排队等前一个调用的时间也算在内。
//
// ⚠️ StreamableHTTPClient 默认的 *http.Client 自带 20s 超时，比这里设得长的时限要配
// WithHTTPClient 换掉它，否则 20s 时 HTTP 层先报错，按连接出错处理（server 标成掉线）。
func WithCallTimeout(d time.Duration) ServerOption {
	return func(c *serverConfig) { c.callTimeout = d }
}

// WithSerialCalls 让这个 server 的工具调用串行：同一时刻至多一个在执行，其余排队（排队受调用方 ctx 与
// WithCallTimeout 约束）。给操纵硬件的 server 用（摄像头、舵机……）——BasicAgent 一轮里的多个工具调用
// 是并发执行的，没有按工具串行的声明。
//
// 超时的那个调用 server 那边可能还在执行：WithClientFactory 注册的 server 这时按连接不可信处理——标成掉线、
// 关掉旧连接（stdio 的旧子进程随之退出）再重连，排队的调用不会发给还在执行的那个连接。HTTP server 端还在
// 执行的调用管不到，要它自己串行；WithClient 的固定实例不重连，超时后排队的调用照常发出。
//
// （见 doc/v2/mcp.md §5）
func WithSerialCalls() ServerOption {
	return func(c *serverConfig) { c.serial = true }
}

// ServerState 是一个 server 的在线状态。
type ServerState int

const (
	// ServerOffline 连不上或已掉线：它的工具调用直接返回「该能力暂时不可用」，不发给 server。
	ServerOffline ServerState = iota
	// ServerOnline 已连上：工具调用发给 server。
	ServerOnline
)

// String 返回 "online" / "offline"。
func (s ServerState) String() string {
	if s == ServerOnline {
		return "online"
	}
	return "offline"
}

// ServerStatus 是一个 server 某一时刻的状态快照（Hub.Status 与 WithStatusHook 交出的都是拷贝）。
type ServerStatus struct {
	// Name 是 server 的逻辑名（Client.Name / WithClientFactory 的 name）。
	Name string
	// State 是在线状态。
	State ServerState
	// Since 是进入当前状态的时刻。
	Since time.Time
	// Err 是离线的原因（连不上、掉线、Hub 已关闭）；在线时为 nil。
	Err error
	// Tools 是最近一次连上时 server 报的工具清单；从没连上过为 nil。要持久化成下次的快照
	// （WithToolSnapshot）就存它——它可能跟本会话注册的不同（server 中途改了清单）。
	Tools []ToolSpec
	// Registered 是本会话为它注册的工具（server 原始名，按注册顺序）。整个会话不变。
	Registered []string
	// FromSnapshot 为 true 表示 Registered 来自快照：Start 时它连不上。
	FromSnapshot bool
}

// server 是 Hub 里的一个 MCP server：怎么建连接、当前连接、在线状态、本会话为它注册了哪些工具。
// NewRemoteTool 直接建的工具也挂一个（hub 为 nil、始终在线、不重连）。
type server struct {
	hub     *Hub
	name    string
	fixed   Client // WithClient 给的实例；WithClientFactory 注册的为 nil
	factory func(ctx context.Context) (Client, error)
	cfg     serverConfig
	sem     chan struct{} // WithSerialCalls 时容量 1，否则 nil

	mu      sync.Mutex
	client  Client // 当前连接：在线时非 nil；factory 型离线时为 nil，fixed 型始终是那一个实例
	online  bool
	closed  bool
	gen     uint64 // 连接代数：每换一个连接加一，掉线判定只认当前这一代（见 markOffline）
	since   time.Time
	lastErr error
	// listed 是最近一次连上时拉到的清单（拷贝），listedNames 是它的名字集合；从没连上过时都是 nil，
	// 这时不做「工具还在不在」的检查。
	listed      []ToolSpec
	listedNames map[string]struct{}
	// registered / registeredDefs 是本会话为它注册的工具（原始名，按注册顺序）与那时的定义，Start 定下后不变。
	registered     []string
	registeredDefs map[string]ToolSpec
	fromSnapshot   bool
}

func newServer(h *Hub, name string, fixed Client, factory func(context.Context) (Client, error), opts []ServerOption) *server {
	s := &server{hub: h, name: name, fixed: fixed, factory: factory}
	for _, opt := range opts {
		opt(&s.cfg)
	}
	if s.cfg.serial {
		s.sem = make(chan struct{}, 1)
	}
	return s
}

// connect 建一次连接并拉工具清单：factory 型新建实例，fixed 型复用那一个。失败时 factory 建的实例当场关掉
// （fixed 的不关：Start 失败的回滚与 Close 管它）。
func (s *server) connect(ctx context.Context) (c Client, specs []ToolSpec, err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		// 业务自定义 Client（或 factory）panic：转成这次连接失败，不让 goroutine panic 带走整个进程。
		pe := panicerr.New(r)
		err = panicerr.WrapOpaque(s.name+": panic recovered", pe)
		panicerr.Log(ctx, slog.LevelError, "mcp: 连接 server 时兜住 panic，已转成错误", pe, panicerr.KeyError, err.Error())
		if c != nil && s.factory != nil {
			closeQuietly(c)
		}
		c, specs = nil, nil
	}()
	c = s.fixed
	if s.factory != nil {
		if c, err = s.factory(ctx); err != nil {
			return nil, nil, fmt.Errorf("%s: new client: %w", s.name, err)
		}
		if isClientNil(c) {
			return nil, nil, fmt.Errorf("%s: client factory returned a nil client", s.name)
		}
		if got := c.Name(); got != s.name {
			closeQuietly(c)
			return nil, nil, fmt.Errorf("%s: client factory returned a client named %q", s.name, got)
		}
	}
	if err = c.Initialize(ctx); err != nil {
		s.discard(c)
		return nil, nil, fmt.Errorf("%s: initialize: %w", s.name, err)
	}
	if specs, err = c.ListTools(ctx); err != nil {
		s.discard(c)
		return nil, nil, fmt.Errorf("%s: list tools: %w", s.name, err)
	}
	specs = cloneSpecs(specs)
	if err = validateSpecs(specs); err != nil {
		s.discard(c)
		return nil, nil, fmt.Errorf("%s: tools/list: %w", s.name, err)
	}
	return c, specs, nil
}

// discard 关掉一次失败的连接里 factory 新建的实例；fixed 的实例留着下次再试。
func (s *server) discard(c Client) {
	if s.factory != nil {
		closeQuietly(c)
	}
}

// call 执行一次工具调用。返回 error 只有一种情况：调用方的 ctx 已经结束（取消 / 到期）——那是控制流，
// 原样往上交；其余失败（离线、超时、server 报错、连接出错）一律是 tool.Failure，交给模型看。
// BasicAgent 把工具返回的 error 当成整轮失败，一个 server 掉线不该拖垮整个 Run。
func (s *server) call(ctx context.Context, name, source string, args map[string]any) (*tool.Result, error) {
	c, gen, refused := s.admit(source)
	if refused != nil {
		return refused, nil
	}

	callCtx, cancel := ctx, context.CancelFunc(func() {})
	if s.cfg.callTimeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, s.cfg.callTimeout)
	}
	defer cancel()

	if s.sem != nil {
		select {
		case s.sem <- struct{}{}:
			defer func() { <-s.sem }()
		case <-callCtx.Done():
			if ctx.Err() != nil {
				return nil, fmt.Errorf("mcp call %s: %w", name, ctx.Err())
			}
			return tool.Failure(tool.ErrCodeTimeout,
				fmt.Sprintf("等 %s 上一个调用结束超时（%s 内没排上），这次调用没有发出", s.name, s.cfg.callTimeout)), nil
		}
		// 排队期间可能掉线、重连过：按现在的状态再准入一次，别把调用发给已经判掉线（可能还在执行上一个调用）的旧连接。
		if c, gen, refused = s.admit(source); refused != nil {
			return refused, nil
		}
	}

	res, err := c.CallTool(callCtx, source, args)
	if err != nil {
		return s.callFailed(ctx, callCtx, name, gen, err)
	}
	if res == nil {
		return tool.Failure(tool.ErrCodeInternal, fmt.Sprintf("%s 返回了空结果", s.name)), nil
	}
	if res.IsError {
		msg := FirstText(res.Content)
		if msg == "" {
			msg = "mcp tool call failed"
		}
		return tool.Failure(tool.ErrCodeInternal, msg), nil
	}
	if res.Structured != nil {
		return tool.Success(res.Structured), nil
	}
	return tool.Success(flattenContent(res.Content)), nil
}

// admit 按当前状态决定这次调用能不能发出：能发时交出当前连接与它的代数，不能时交出给模型的失败结果。
func (s *server) admit(source string) (Client, uint64, *tool.Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.closed:
		return nil, 0, tool.Failure(ErrCodeServerUnavailable,
			fmt.Sprintf("该能力暂时不可用（%s 已关闭），这次调用没有发出", s.name))
	case !s.online:
		return nil, 0, tool.Failure(ErrCodeServerUnavailable,
			fmt.Sprintf("该能力暂时不可用（%s offline），这次调用没有发出", s.name))
	}
	if s.listedNames != nil {
		if _, ok := s.listedNames[source]; !ok {
			return nil, 0, tool.Failure(tool.ErrCodeNotFound,
				fmt.Sprintf("%s 上已经没有工具 %q（本会话的工具清单在启动时定下，server 之后去掉了它），这次调用没有发出", s.name, source))
		}
	}
	return s.client, s.gen, nil
}

// callFailed 把一次失败的 CallTool 分档：调用方喊停 → error；自己的时限到了 → 超时失败；server 答了
// JSON-RPC 错误 → 普通失败（它活着）；其余算连接出错 → 标掉线、后台重连，返回「该能力暂时不可用」。
func (s *server) callFailed(ctx, callCtx context.Context, name string, gen uint64, err error) (*tool.Result, error) {
	if ctx.Err() != nil {
		return nil, fmt.Errorf("mcp call %s: %w", name, err)
	}
	// 建连接（dial）就失败 = 连不上，哪怕是被时限打断的：按超时处理的话，server 不在时每次调用都要
	// 干等满时限。
	var opErr *net.OpError
	dialFailed := errors.As(err, &opErr) && opErr.Op == "dial"
	if callCtx.Err() != nil && !dialFailed {
		if s.sem != nil && s.factory != nil {
			// 串行的 server：超时的调用那边可能还在执行，连接不可信——标掉线、关旧连接再重连（见 WithSerialCalls）。
			s.markOffline(gen, err)
			return tool.Failure(tool.ErrCodeTimeout,
				fmt.Sprintf("工具调用超时（超过 %s 未返回），已放弃等待；为保证同一时刻只有一个调用在执行，已断开 %s 重连（它那边的执行可能被打断）", s.cfg.callTimeout, s.name)), nil
		}
		return tool.Failure(tool.ErrCodeTimeout,
			fmt.Sprintf("工具调用超时（超过 %s 未返回），已放弃等待；%s 那边可能仍在执行", s.cfg.callTimeout, s.name)), nil
	}
	var rpcErr *rpcError
	if errors.As(err, &rpcErr) {
		return tool.Failure(tool.ErrCodeInternal, rpcErr.Error()), nil
	}
	s.markOffline(gen, err)
	// 原始错误（地址、端口）只进日志与 Status，不进模型看到的文本。连接都没建起来时请求肯定没发出——
	// 说清楚，模型才不会把一个没执行的写操作当成「可能已执行」。
	if dialFailed {
		return tool.Failure(ErrCodeServerUnavailable,
			fmt.Sprintf("该能力暂时不可用（%s 连不上），这次调用没有发出", s.name)), nil
	}
	return tool.Failure(ErrCodeServerUnavailable,
		fmt.Sprintf("该能力暂时不可用（%s 连接出错），这次调用没拿到结果，不确定是否已执行", s.name)), nil
}

// markOffline 在第 gen 代连接上出了连接错误时把 server 标成掉线，并起后台重连。
//
// 只认当前这一代：并发的几个调用同时撞上同一次掉线时只有第一个生效；重连上之后，迟到的旧连接错误不会
// 把新连接标掉。fixed 型（WithClient）不标——它没法新建连接，标了也只能拿同一个实例再试，下一次调用
// 照常发给它。
func (s *server) markOffline(gen uint64, cause error) {
	if s.factory == nil || s.hub == nil {
		return
	}
	s.mu.Lock()
	if s.closed || !s.online || s.gen != gen {
		s.mu.Unlock()
		return
	}
	stale := s.client
	s.client = nil
	s.online = false
	s.since = time.Now()
	s.lastErr = cause
	s.hub.events.push(s.statusLocked())
	// 在 s.mu 里 Add：Close 先在 s.mu 里置 closed 再 Wait，这里要么看到 closed 不起，要么 Add 先于 Wait。
	// 在线时不会有重连循环在跑（循环只在离线时存在、上线即退出），所以每次掉线起一个。
	s.hub.bg.Add(1)
	s.mu.Unlock()
	slog.Warn("mcp: server 掉线，后台重连；期间它的工具返回「该能力暂时不可用」",
		logKeyServer, s.name, logKeyErr, cause.Error())
	go s.reconnectLoop(stale)
}

// reconnectLoop 按退避反复连，连上或 Hub 关闭为止。stale 是掉线的那个连接（factory 型），在这里关：
// 关一个 stdio 连接要等子进程退出（至多几秒），不放在调用路径上。
func (s *server) reconnectLoop(stale Client) {
	h := s.hub
	defer h.bg.Done()
	if stale != nil {
		closeQuietly(stale)
	}
	delay := h.backoffInitial
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for attempt := 1; ; attempt++ {
		select {
		case <-h.done:
			return
		case <-timer.C:
		}
		// 计时器与 done 同时就绪时 select 随机选：再查一次，Close 已开始就不再发起新的尝试。
		if h.isClosed() {
			return
		}
		ctx, cancel := h.attemptCtx(context.Background()) // Close 时被取消
		c, specs, err := s.connect(ctx)
		cancel()
		if err == nil {
			s.goOnline(c, specs)
			return
		}
		delay = min(delay*2, h.backoffMax)
		slog.Debug("mcp: 重连失败，稍后再试", logKeyServer, s.name, logKeyAttempt, attempt,
			logKeyErr, err.Error(), logKeyRetryIn, delay.String())
		timer.Reset(delay)
	}
}

// goOnline 换上新连接、标成在线。Hub 已关闭时关掉这个连接（factory 型）。
func (s *server) goOnline(c Client, specs []ToolSpec) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.discard(c)
		return
	}
	s.gen++
	s.client = c
	s.online = true
	s.since = time.Now()
	s.lastErr = nil
	s.setListedLocked(specs)
	missing, added, changed := s.driftLocked()
	s.hub.events.push(s.statusLocked())
	s.mu.Unlock()
	slog.Info("mcp: server 重连上了", logKeyServer, s.name, logKeyTools, len(specs))
	if len(missing)+len(added)+len(changed) > 0 {
		// 工具集整个会话不变（见 Hub godoc）：去掉的调用时返回「已经没有」，新加的这次不注册，改了定义的仍按
		// 启动时的定义发给模型。下次启动按新清单注册。
		slog.Warn("mcp: server 的工具清单跟本会话注册的不一样了，本会话仍按启动时的工具集",
			logKeyServer, s.name, "missing", missing, "added", added, "changed", changed)
	}
}

// setListedLocked 记下最近一次拉到的清单。调用方持 s.mu。
func (s *server) setListedLocked(specs []ToolSpec) {
	s.listed = specs
	s.listedNames = make(map[string]struct{}, len(specs))
	for _, sp := range specs {
		s.listedNames[sp.Name] = struct{}{}
	}
}

// driftLocked 比较最近一次拉到的清单与本会话注册的工具：注册了但已经没有的、新加的、定义变了的。调用方持 s.mu。
func (s *server) driftLocked() (missing, added, changed []string) {
	reg := make(map[string]struct{}, len(s.registered))
	for _, n := range s.registered {
		reg[n] = struct{}{}
		if _, ok := s.listedNames[n]; !ok {
			missing = append(missing, n)
		}
	}
	for _, sp := range s.listed {
		if _, ok := reg[sp.Name]; !ok {
			added = append(added, sp.Name)
			continue
		}
		if !sameSpec(s.registeredDefs[sp.Name], sp) {
			changed = append(changed, sp.Name)
		}
	}
	return missing, added, changed
}

// statusLocked 拍一份状态快照（深拷贝）。调用方持 s.mu。
func (s *server) statusLocked() ServerStatus {
	st := ServerStatus{
		Name:         s.name,
		Since:        s.since,
		Err:          s.lastErr,
		Tools:        cloneSpecs(s.listed),
		FromSnapshot: s.fromSnapshot,
	}
	if s.online {
		st.State = ServerOnline
	}
	if len(s.registered) > 0 {
		st.Registered = append([]string(nil), s.registered...)
	}
	return st
}

// shutdown 标成已关闭并交出要关的连接（fixed 型交出那个实例，哪怕从没连上过）。
func (s *server) shutdown() Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.client
	if s.fixed != nil {
		c = s.fixed
	}
	s.closed = true
	s.online = false
	s.client = nil
	s.lastErr = errHubClosed
	s.since = time.Now()
	return c
}

// closeQuietly 关一个连接，错误与 panic 只记日志（用在没有调用方等着接错误的地方）。
func closeQuietly(c Client) {
	defer func() {
		if r := recover(); r != nil {
			pe := panicerr.New(r)
			panicerr.Log(context.Background(), slog.LevelError, "mcp: 关闭 client 时兜住 panic", pe,
				panicerr.KeyError, c.Name()+": close panic recovered")
		}
	}()
	if err := c.Close(); err != nil {
		slog.Warn("mcp: 关闭 client 出错", logKeyServer, c.Name(), logKeyErr, err.Error())
	}
}

// validateSpecs 检查一份工具清单：每个工具要有名字，名字不重复（重名的两个工具在框架里会互相顶掉）。
func validateSpecs(specs []ToolSpec) error {
	seen := make(map[string]struct{}, len(specs))
	for i, sp := range specs {
		if sp.Name == "" {
			return fmt.Errorf("tool #%d has no name", i)
		}
		if _, dup := seen[sp.Name]; dup {
			return fmt.Errorf("duplicate tool name %q", sp.Name)
		}
		seen[sp.Name] = struct{}{}
	}
	return nil
}

// sameSpec 判两份定义发给模型时是否一样（描述与参数 schema）。
func sameSpec(a, b ToolSpec) bool {
	return a.Description == b.Description && reflect.DeepEqual(normalizeSchema(a.InputSchema), normalizeSchema(b.InputSchema))
}

// normalizeSchema 把 nil schema 换成注册时用的默认值，免得「没给」与「给了默认值」被当成不同。
func normalizeSchema(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{"type": "object"}
	}
	return m
}

// cloneSpecs 深拷贝一份清单：schema 是 map，浅拷贝的话调用方改它就改到了已注册工具的定义。nil 原样返回 nil。
func cloneSpecs(specs []ToolSpec) []ToolSpec {
	if specs == nil {
		return nil
	}
	out := make([]ToolSpec, len(specs))
	for i, sp := range specs {
		out[i] = ToolSpec{
			Name:         sp.Name,
			Description:  sp.Description,
			InputSchema:  cloneMap(sp.InputSchema),
			OutputSchema: cloneMap(sp.OutputSchema),
		}
	}
	return out
}

func cloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = cloneValue(v)
	}
	return out
}

func cloneValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return cloneMap(x)
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = cloneValue(e)
		}
		return out
	case []string:
		return append([]string(nil), x...)
	default:
		return v
	}
}
