package mcp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
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
	timeoutSet     bool // 调用方显式给了 WithConnectTimeout
	backoffInitial time.Duration
	backoffMax     time.Duration
	statusHook     func(ServerStatus)
	events         *statusQueue // 配了 statusHook 才有

	mu      sync.Mutex  // 管 tools；Start 全程持有（并发 Start 串行）
	started atomic.Bool // Start 成功后置位（在 mu 里）；Status 读它不拿锁，Start 进行中也不挡
	tools   []*RemoteTool

	done      chan struct{}  // Close 一开始就关闭：Start 与后台重连的连接尝试据此打断，重连循环、状态回调据此退出
	bg        sync.WaitGroup // 各 server 的重连循环（Close 等它们退出）
	closeOnce sync.Once
	closeDone chan struct{} // 第一个 Close 做完时关闭；并发的 Close 等它
	closeErr  error
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

// WithPartialStart 开启按 server 降级：Start 不因个别 server 连不上而失败（见 Hub godoc）。server 报了重名或没名字的
// 工具也算它连不上。调用方 ctx 取消或到期仍让 Start 失败（视为放弃启动）；单个 server 最多等多久用 WithConnectTimeout
// 定（默认 30s）。确定性错误照样失败：两个工具生成同一个名字、server 名重复、要用的快照不合法。
func WithPartialStart() HubOption {
	return func(h *Hub) { h.partial = true }
}

// WithConnectTimeout 设每次连接尝试（建实例 + Initialize + ListTools）的时限；d <= 0 表示不设（只受调用方 ctx 约束）。
// 不给时：后台重连与 WithPartialStart 的 Start 默认 30s（一个卡住的 server 不能挡住启动），默认（全有或全无）的
// Start 不设——首次运行要下载东西的 stdio server 起得慢，失败了整个 Start 都失败。
func WithConnectTimeout(d time.Duration) HubOption {
	return func(h *Hub) { h.connectTimeout, h.timeoutSet = d, true }
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
// 下次的快照。fn 里调 Status、调工具、调 Close 都可以；panic 只记日志。
//
// Close 之后不再开始新的回调（至多还有一个恰好在 Close 那一刻开始的），也不等正在执行的那个返回（等的话，回调里调
// Close 就是自己等自己）；Close 时还没送出的状态变化不再回调——要持久化最后的清单，Close 之前读一次 Status。
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
		closeDone:      make(chan struct{}),
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
// 确定性错误（server 名重复、工具撞名、要用的快照不合法）才失败，连不上的 server 按快照注册并后台重连（见 Hub godoc）。
// 失败时可以重试，但回滚关掉了 WithClient 给的实例（stdio 的关了就不能再用），只有 WithClientFactory 注册的 server
// 重试时会新建连接。成功后重复调用是幂等的（工具集已定，不再连）。Close 会打断进行中的 Start（它返回错误），Close
// 之后调用返回错误。
func (h *Hub) Start(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	// **F3 修复**：全程持 mu 让并发 Start 串行——第二个阻塞到第一个完成、见 started=true 返回
	// nil（此时 register 已完成）。原 check(顶部) 与 set(末尾) 分两个临界区，并发 Start 都过
	// started==false 门 → 重复 ListTools(冗余 RPC) + 重复 append h.tools + 重复 Register。
	// 不用"乐观置位于首个临界区"——那样第二个 Start 见 true 提前 return 时第一个尚在 register，
	// caller 拿到未 register 完的空 Toolkit。连接 goroutine 只写局部 results 不碰 h.mu、
	// register 在锁内故安全持 mu；失败不置 started(保持 false)允许重试。
	if h.isClosed() {
		return errHubClosed
	}
	if h.started.Load() {
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
			timeout := h.connectTimeout
			if !h.partial && !h.timeoutSet {
				timeout = 0 // 见 WithConnectTimeout
			}
			actx, cancel := h.attemptCtx(ctx, timeout)
			defer cancel()
			c, specs, err := s.connect(actx) // 业务 Client panic 在 connect 里转成 err；Close 打断时 actx 被取消
			results[i] = result{client: c, specs: specs, err: err}
		}(i, s)
	}
	wg.Wait()

	rollback := func() {
		for i, s := range h.servers {
			if s.fixed != nil {
				closeQuietly(s.fixed)
				s.mu.Lock()
				s.fixedClosed = true // Close 时不再关第二次
				s.mu.Unlock()
			} else if results[i].client != nil {
				closeQuietly(results[i].client)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		rollback()
		return fmt.Errorf("mcp: start: %w", err)
	}
	if h.isClosed() {
		rollback()
		return errHubClosed
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
	var reconnect []*server // Start 时连不上的：置 started 之后再起重连循环
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
			s.fixedClosed = false
			s.setListedLocked(results[i].specs)
		} else {
			s.lastErr = err
			h.bg.Add(1) // 在 h.mu 里：Close 拿到 h.mu 之后才 Wait
		}
		h.events.push(s.statusLocked())
		offline := !s.online
		s.mu.Unlock()
		if offline {
			if p.fromSnapshot {
				slog.WarnContext(ctx, "mcp: server 启动时连不上，按快照注册它的工具，后台重连",
					logKeyServer, s.name, logKeyTools, len(p.specs), logKeyErr, results[i].err)
			} else {
				slog.WarnContext(ctx, "mcp: server 启动时连不上且没有快照，本会话不注册它的工具，后台重连",
					logKeyServer, s.name, logKeyErr, results[i].err)
			}
			reconnect = append(reconnect, s)
		}
	}
	// 先置 started 再起后台 goroutine：回调与重连循环里调 Status 要看到已启动（Status 不拿 h.mu）。顺序反了的话第一个
	// 回调偶尔读到 nil（审计探针 3000 次撞到 1–4 次），把快照写空——窗口太窄，没有稳定的回归用例，别改回去。
	h.started.Store(true)
	for _, s := range reconnect {
		go s.reconnectLoop(nil)
	}
	if h.events != nil {
		// 不计入 bg：Close 不等它（回调里调 Close 时等它就是自己等自己），它在 done 关闭后不再开始新的回调。
		go h.dispatchStatus()
	}
	return nil
}

// isClosed 报 Close 是否已经开始。
func (h *Hub) isClosed() bool {
	select {
	case <-h.done:
		return true
	default:
		return false
	}
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

// attemptCtx 给一次连接尝试派生 ctx：timeout > 0 时带时限，Close 时被取消（ctx 不存进 Hub，起一个小 goroutine
// 把 done 转成取消；cancel 之后它就退出）。
func (h *Hub) attemptCtx(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	var ctx context.Context
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, timeout)
	} else {
		ctx, cancel = context.WithCancel(parent)
	}
	go func() {
		select {
		case <-h.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
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
	if !h.started.Load() {
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

// Close 打断进行中的 Start 与重连尝试，停掉后台重连与状态回调，并发关闭所有 Client，用 errors.Join 聚合所有错误。
// 多次调用安全：并发的 Close 等第一个做完，返回同一个结果。Close 之后工具调用返回「该能力暂时不可用」。
func (h *Hub) Close() error {
	first := false
	h.closeOnce.Do(func() {
		first = true
		close(h.done) // 先关：Start 的连接尝试被打断，Start 才会放开 mu
	})
	if !first {
		<-h.closeDone
		return h.closeErr
	}
	defer close(h.closeDone)

	h.mu.Lock() // 等进行中的 Start 收尾（它看到 done 已关闭会回滚）
	h.mu.Unlock()
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
	h.closeErr = errors.Join(errs...)
	return h.closeErr
}

// dispatchStatus 按入队顺序逐个回调 statusHook；Close 之后不再开始新的回调、退出。
func (h *Hub) dispatchStatus() {
	for {
		select {
		case <-h.done:
			return
		case <-h.events.wake:
		}
		for _, st := range h.events.take() {
			if h.isClosed() {
				return
			}
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
