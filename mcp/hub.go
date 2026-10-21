package mcp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"crew-go/internal/panicerr"
	"crew-go/tool"
)

// 日志字段名。
const (
	logKeyServer  = "mcp_server"
	logKeyErr     = "err"
	logKeyTools   = "tools"
	logKeyAttempt = "attempt"
	logKeyRetryIn = "retry_in"
)

// 默认值：重连退避从 1s 翻倍到 60s；每次连接尝试（建实例 + Initialize + ListTools）30s。
const (
	defaultBackoffInitial = time.Second
	defaultBackoffMax     = time.Minute
	defaultConnectTimeout = 30 * time.Second
)

var errHubClosed = errors.New("mcp: hub closed")

// Hub 管理多个 MCP server（各由一个 Client 连接），把它们的工具聚合成一个 *tool.Toolkit。
// 用法是「一次启动、多处复用」：Start 握手并拉工具清单、注册进 Toolkit；Toolkit() 交给 agent；Close 释放全部连接。
//
// # 工具集在 Start 时定下，之后不变
//
// 注册的工具（名字、描述、参数 schema、顺序：按 server 注册顺序，再按 server 报的顺序）只在 Start 时定一次。
// 之后 server 掉线、重连、重连后报的清单变了，都不改 Toolkit——发给模型的工具段在消息之前，它一变，按前缀命中的
// prompt cache 与要求前缀逐字节不变的 thinking 回放（Anthropic）都从头失效。server 的变化只体现在调用结果上：
// 掉线时返回「该能力暂时不可用」，重连后去掉的工具返回「已经没有」，新加的工具下次启动才注册。
//
// # 调用失败交给模型，不让 Run 失败
//
// 工具调用的任何失败——server 掉线、连接出错、超时、server 报错——都是失败结果（tool.Failure，模型看到
// is_error），只有调用方的 ctx 结束时才返回 error。连接出错时 server 标成掉线、后台按退避重连，期间它的工具
// 直接返回 ErrCodeServerUnavailable、不再发给它（不用每次干等超时）。只有 WithClientFactory 注册的 server 会
// 掉线后重建连接：stdio 子进程退了只能新起一个，而 Client 关了不能再用；WithClient 给的实例出错时下一次调用照常
// 发给它。
//
// # 按 server 降级（WithPartialStart）
//
// 默认 Start 全有或全无：任一 server 连不上就整体失败，并关掉已经连上的。开了 WithPartialStart 后，连不上的
// server 不让 Start 失败：给了 WithToolSnapshot 就按快照注册它的工具（工具集跟它在线时一样），没给就不注册
// （本会话没有它的工具，连上后也不补）；它的工具返回「该能力暂时不可用」，后台按退避重连。
//
// 状态用 Status 查，或用 WithStatusHook 按变化收；要让下次启动时连不上的 server 也有工具，就把
// ServerStatus.Tools 存下来，下次作为 WithToolSnapshot 传回。
type Hub struct {
	servers []*server
	toolkit *tool.Toolkit

	partial        bool
	namer          func(server, tool string) string
	connectTimeout time.Duration
	backoffInitial time.Duration
	backoffMax     time.Duration
	statusHook     func(ServerStatus)
	events         *statusQueue // 配了 statusHook 才有

	mu      sync.Mutex // 管 started / closed / tools；Start 全程持有（并发 Start 串行）
	started bool
	closed  bool
	tools   []*RemoteTool

	done chan struct{}  // Close 时关闭：后台重连与状态回调据此退出
	bg   sync.WaitGroup // 后台 goroutine：各 server 的重连循环、状态回调
}

// HubOption 配置 Hub。
type HubOption func(*Hub)

// WithClient 向 Hub 注册一个 Client（一个 server）。可重复调用以加入多个 server；opts 是这个 server 的选项。
//
// 这个实例在 Start 失败的回滚与 Close 时被关掉。开了 WithPartialStart 时它 Start 连不上的话，后台用同一个实例
// 重试 Initialize / ListTools；中途出错不会重建连接——要掉线后自动重连，用 WithClientFactory。
//
// **audit¹⁸ HIGH-1 fix**: isClientNil 检 typed nil Client (协议十八复证).
// 业务 `var c *MyClient; WithClient(c)` typed nil → Hub.ListTools 时 c.ListTools nil deref.
func WithClient(c Client, opts ...ServerOption) HubOption {
	return func(h *Hub) {
		if isClientNil(c) {
			panic("WithClient: nil Client (含 Go typed nil interface 陷阱)")
		}
		h.servers = append(h.servers, newServer(h, c.Name(), c, nil, opts))
	}
}

// WithClientFactory 注册一个可重建连接的 server：Start 时用 newClient 建实例；连不上或中途掉线时关掉旧实例、
// 后台再用它建新的重连。name 是 server 的逻辑名（工具名前缀、快照、状态都按它），newClient 建出的 Client 的
// Name() 必须等于它；newClient 的 ctx 是这次连接尝试的（受 WithConnectTimeout 约束）。
//
//	mcp.WithClientFactory("fs", func(context.Context) (mcp.Client, error) {
//	    return mcp.NewStdioClient("fs", "mcp-server-fs", []string{"/data"})
//	}, mcp.WithCallTimeout(10*time.Second))
func WithClientFactory(name string, newClient func(ctx context.Context) (Client, error), opts ...ServerOption) HubOption {
	return func(h *Hub) {
		if name == "" {
			panic("WithClientFactory: empty server name")
		}
		if newClient == nil {
			panic("WithClientFactory: nil factory")
		}
		h.servers = append(h.servers, newServer(h, name, nil, newClient, opts))
	}
}

// WithPartialStart 开启按 server 降级：Start 不因个别 server 连不上而失败（见 Hub godoc）。调用方 ctx 取消或到期
// 仍让 Start 失败（视为放弃启动）；单个 server 最多等多久用 WithConnectTimeout 定。工具重名之类的确定性错误照样失败。
func WithPartialStart() HubOption {
	return func(h *Hub) { h.partial = true }
}

// WithConnectTimeout 设每次连接尝试（建实例 + Initialize + ListTools，Start 时与后台重连时都是）的时限，默认 30s；
// d <= 0 表示不设（只受调用方 ctx 约束）。开了 WithPartialStart 时，它决定一个连不上的 server 让 Start 等多久。
func WithConnectTimeout(d time.Duration) HubOption {
	return func(h *Hub) { h.connectTimeout = d }
}

// WithReconnectBackoff 设后台重连的退避：第一次等 initial，之后每次翻倍、不超过 max。默认 1s → 60s；
// initial <= 0 用默认值，max 小于 initial 时取 initial。
func WithReconnectBackoff(initial, max time.Duration) HubOption {
	return func(h *Hub) {
		if initial <= 0 {
			initial = defaultBackoffInitial
		}
		if max < initial {
			max = initial
		}
		h.backoffInitial, h.backoffMax = initial, max
	}
}

// WithToolNamer 设工具在框架里的名字（发给模型的工具名）怎么由 server 名与 server 上的原始名生成，默认
// CanonicalName（mcp_<server>_<tool>）。fn 的结果要符合各 provider 对工具名的要求（如 Anthropic / OpenAI
// 都要求 ^[a-zA-Z0-9_-]{1,64}$）；结果为空或两个工具撞名时 Start 报错——同名的两个工具在 Toolkit 里会互相顶掉。
func WithToolNamer(fn func(server, tool string) string) HubOption {
	return func(h *Hub) {
		if fn != nil {
			h.namer = fn
		}
	}
}

// WithStatusHook 设状态回调：Start 成功后每个 server 回调一次初始状态，之后每次上线 / 掉线回调一次，按状态变化的
// 先后顺序逐个调用（在 Hub 自己的一个 goroutine 里，不并发）。用来记日志、上屏、把 ServerStatus.Tools 持久化成
// 下次的快照。fn 要尽快返回（Close 会等它）；在它里面调 Status 没问题；panic 只记日志。Close 之后不再回调。
func WithStatusHook(fn func(ServerStatus)) HubOption {
	return func(h *Hub) { h.statusHook = fn }
}

// NewHub 构造一个空 Hub，使用 With* 选项注入 server。
func NewHub(opts ...HubOption) *Hub {
	h := &Hub{
		toolkit:        tool.NewToolkit(),
		namer:          CanonicalName,
		connectTimeout: defaultConnectTimeout,
		backoffInitial: defaultBackoffInitial,
		backoffMax:     defaultBackoffMax,
		done:           make(chan struct{}),
	}
	for _, opt := range opts {
		opt(h)
	}
	if h.statusHook != nil {
		h.events = &statusQueue{wake: make(chan struct{}, 1)}
	}
	return h
}

// Start 并发连接所有 server（Initialize + ListTools），定下工具集并注册进内部 Toolkit。
//
// 默认任一 server 失败则整体失败，已连上的 Client 会被 Close 回滚；开了 WithPartialStart 时只有调用方 ctx 结束与
// 确定性错误（server 名重复、工具重名、快照不合法）才失败，连不上的 server 按快照注册并后台重连（见 Hub godoc）。
// 失败时可以重试；成功后重复调用是幂等的（工具集已定，不再连）。Close 之后调用返回错误。
func (h *Hub) Start(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	// **F3 修复**：全程持 mu 让并发 Start 串行——第二个阻塞到第一个完成、见 started=true 返回
	// nil（此时 register 已完成）。原 check(顶部) 与 set(末尾) 分两个临界区，并发 Start 都过
	// started==false 门 → 重复 ListTools(冗余 RPC) + 重复 append h.tools + 重复 Register。
	// 不用"乐观置位于首个临界区"——那样第二个 Start 见 true 提前 return 时第一个尚在 register，
	// caller 拿到未 register 完的空 Toolkit。连接 goroutine 只写局部 results 不碰 h.mu、
	// register 在锁内故安全持 mu；失败不置 started(保持 false)允许重试。
	if h.closed {
		return errHubClosed
	}
	if h.started {
		return nil
	}
	if err := h.checkServerNames(); err != nil {
		return err
	}

	type result struct {
		client Client
		specs  []ToolSpec
		err    error
	}
	results := make([]result, len(h.servers))
	var wg sync.WaitGroup
	for i, s := range h.servers {
		wg.Add(1)
		go func(i int, s *server) {
			defer wg.Done()
			actx, cancel := h.attemptCtx(ctx)
			defer cancel()
			c, specs, err := s.connect(actx) // 业务 Client panic 在 connect 里转成 err
			results[i] = result{client: c, specs: specs, err: err}
		}(i, s)
	}
	wg.Wait()

	rollback := func() {
		for i, s := range h.servers {
			if s.fixed != nil {
				closeQuietly(s.fixed)
			} else if results[i].client != nil {
				closeQuietly(results[i].client)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		rollback()
		return fmt.Errorf("mcp: start: %w", err)
	}
	var startErrs []error
	for _, r := range results {
		if r.err != nil {
			startErrs = append(startErrs, r.err)
		}
	}
	if len(startErrs) > 0 && !h.partial {
		rollback()
		return errors.Join(startErrs...)
	}

	// 定工具集：连上的按它报的清单，连不上的按快照（WithPartialStart 下才走到这里）。
	type plan struct {
		specs        []ToolSpec
		fromSnapshot bool
	}
	plans := make([]plan, len(h.servers))
	var tools []*RemoteTool
	owner := make(map[string]string) // 框架内工具名 → server 名，查撞名
	for i, s := range h.servers {
		p := plan{specs: results[i].specs}
		if results[i].err != nil && s.cfg.hasSnapshot {
			if err := validateSpecs(s.cfg.snapshot); err != nil {
				rollback()
				return fmt.Errorf("mcp: %s: tool snapshot: %w", s.name, err)
			}
			p = plan{specs: s.cfg.snapshot, fromSnapshot: true}
		}
		plans[i] = p
		for _, spec := range p.specs {
			name := h.namer(s.name, spec.Name)
			if name == "" {
				rollback()
				return fmt.Errorf("mcp: %s: tool namer returned an empty name for %q", s.name, spec.Name)
			}
			if other, dup := owner[name]; dup {
				rollback()
				return fmt.Errorf("mcp: tool name %q is produced by both %s and %s (one would silently replace the other)", name, other, s.name)
			}
			owner[name] = s.name
			tools = append(tools, newRemoteTool(s, name, spec))
		}
	}

	for _, t := range tools {
		h.toolkit.Register(t)
	}
	h.tools = tools
	now := time.Now()
	for i, s := range h.servers {
		p := plans[i]
		s.mu.Lock()
		s.registered = make([]string, 0, len(p.specs))
		s.registeredDefs = make(map[string]ToolSpec, len(p.specs))
		for _, spec := range p.specs {
			s.registered = append(s.registered, spec.Name)
			s.registeredDefs[spec.Name] = spec
		}
		s.fromSnapshot = p.fromSnapshot
		s.since = now
		if err := results[i].err; err == nil {
			s.client = results[i].client
			s.online = true
			s.gen = 1
			s.setListedLocked(results[i].specs)
		} else {
			s.lastErr = err
			s.reconnecting = true
			h.bg.Add(1)
		}
		h.events.push(s.statusLocked())
		offline := !s.online
		s.mu.Unlock()
		if offline {
			if p.fromSnapshot {
				slog.WarnContext(ctx, "mcp: server 启动时连不上，按快照注册它的工具，后台重连",
					logKeyServer, s.name, logKeyTools, len(p.specs), logKeyErr, results[i].err.Error())
			} else {
				slog.WarnContext(ctx, "mcp: server 启动时连不上且没有快照，本会话不注册它的工具，后台重连",
					logKeyServer, s.name, logKeyErr, results[i].err.Error())
			}
			go s.reconnectLoop(nil)
		}
	}
	if h.events != nil {
		h.bg.Add(1)
		go h.dispatchStatus()
	}
	h.started = true
	return nil
}

// checkServerNames 查 server 名重复：工具名前缀、快照、状态都按名字对应，重名的两个分不开。
func (h *Hub) checkServerNames() error {
	seen := make(map[string]struct{}, len(h.servers))
	for _, s := range h.servers {
		if _, dup := seen[s.name]; dup {
			return fmt.Errorf("mcp: duplicate server name %q", s.name)
		}
		seen[s.name] = struct{}{}
	}
	return nil
}

// attemptCtx 给一次连接尝试派生 ctx（WithConnectTimeout）。
func (h *Hub) attemptCtx(parent context.Context) (context.Context, context.CancelFunc) {
	if h.connectTimeout > 0 {
		return context.WithTimeout(parent, h.connectTimeout)
	}
	return context.WithCancel(parent)
}

// Toolkit 返回聚合后的工具注册表；在 Start 前调用得到空 Toolkit。
func (h *Hub) Toolkit() *tool.Toolkit {
	return h.toolkit
}

// Tools 返回所有已注册的 RemoteTool 视图（注册顺序），便于调试或手动组装。
func (h *Hub) Tools() []*RemoteTool {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*RemoteTool, len(h.tools))
	copy(out, h.tools)
	return out
}

// Status 返回各 server 的状态（按注册顺序）；Start 成功之前返回 nil。Close 之后各 server 都是离线、Err 为
// Hub 已关闭。
func (h *Hub) Status() []ServerStatus {
	h.mu.Lock()
	started := h.started
	h.mu.Unlock()
	if !started {
		return nil
	}
	out := make([]ServerStatus, 0, len(h.servers))
	for _, s := range h.servers {
		s.mu.Lock()
		out = append(out, s.statusLocked())
		s.mu.Unlock()
	}
	return out
}

// Close 停掉后台重连与状态回调，并发关闭所有 Client，用 errors.Join 聚合所有错误。多次调用安全；Close 之后
// 工具调用返回「该能力暂时不可用」。
func (h *Hub) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	h.mu.Unlock()
	close(h.done)

	clients := make([]Client, 0, len(h.servers))
	for _, s := range h.servers {
		if c := s.shutdown(); c != nil {
			clients = append(clients, c)
		}
	}
	// 重连循环里正在进行的尝试被 done 打断；它们建出的连接自己关（见 goOnline）。
	h.bg.Wait()

	errs := make([]error, len(clients))
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func(i int, c Client) {
			defer wg.Done()
			// review-v6 M-21 fix: 业务自定义 Client Close() panic 时 recover 转 err
			defer func() {
				if r := recover(); r != nil {
					pe := panicerr.New(r)
					errs[i] = panicerr.WrapOpaque(c.Name()+": close panic recovered", pe)
					// Close 没有 ctx 参数
					panicerr.Log(context.Background(), slog.LevelError, "mcp: client 关闭兜住 panic，已转成错误", pe, panicerr.KeyError, errs[i].Error())
				}
			}()
			errs[i] = c.Close()
		}(i, c)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// dispatchStatus 按入队顺序逐个回调 statusHook，Close 时退出。
func (h *Hub) dispatchStatus() {
	defer h.bg.Done()
	for {
		select {
		case <-h.done:
			return
		case <-h.events.wake:
		}
		for _, st := range h.events.take() {
			h.callStatusHook(st)
		}
	}
}

func (h *Hub) callStatusHook(st ServerStatus) {
	defer func() {
		if r := recover(); r != nil {
			pe := panicerr.New(r)
			panicerr.Log(context.Background(), slog.LevelError, "mcp: 状态回调 panic，已忽略", pe,
				panicerr.KeyError, st.Name+": status hook panic recovered")
		}
	}()
	h.statusHook(st)
}

// statusQueue 是状态变化的队列：入队在 server 的锁里做（入队顺序 = 状态变化的顺序），回调在 dispatchStatus 里做
// （不持任何锁，回调里调 Status 不会死锁，回调慢也不挡调用路径）。nil 队列（没配回调）入队是空操作。
type statusQueue struct {
	mu    sync.Mutex
	items []ServerStatus
	wake  chan struct{} // 容量 1
}

func (q *statusQueue) push(st ServerStatus) {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.items = append(q.items, st)
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *statusQueue) take() []ServerStatus {
	q.mu.Lock()
	defer q.mu.Unlock()
	items := q.items
	q.items = nil
	return items
}
