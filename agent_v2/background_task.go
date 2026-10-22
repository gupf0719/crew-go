package agent_v2

// 长时间运行的工具**转入后台**——一次调用跑过 YieldAfter 还没返回，就先给模型一个任务 id、这一轮接着往下走；
// 工具在后台跑完之后，结果在开它的那个 agent 下一次调模型时作为一条通知送进对话。模型也可以用
// [BackgroundTaskToolName] 查询、等待、取消、列出。
//
// # 为什么是「先同步等一会儿，再转后台」
//
// 让模型事先声明「这次放后台」要它预判耗时，猜错两头都亏：快的被放进后台、多绕一轮才拿到结果；慢的同步跑、
// 堵住整轮。先同步等 YieldAfter：跑得快的当场拿到结果（跟没开这个功能一样），真的慢的才换成任务 id。
//
// # 形状
//
//   - **哪些工具**：业务点名（[BackgroundTasksConfig.Eligible]），默认一个都没有。点名的工具**从一开始**就在
//     一个脱离当前执行的 ctx 里跑（不论这次最后有没有转入后台——工具跑到一半没法换 ctx）：不能提问
//     （`compose_v2.Ask`）、不能做转交 / 路由决定、不能用节点私有态、拿不到 session 与会话 ID。需要这些的工具
//     别点名。同步等的那段里调用方的 ctx 被取消（Run 被撤），任务跟着取消、照样等工具返回；转入后台之后不再跟。
//   - **这次请求的环境不给它**：任务可能比请求活得久。编排钩子（两层）换成空的——它们往这次请求的流里写
//     （agui 的 SSE、a2a 的事件），请求结束之后再写就写进了已经返回的连接；checkpointer 也不给——后台里跑的
//     嵌套 agent 中断时会留下一份没人续的存档。调用主体、store 留着。
//   - **谁能转后台**：只有顶层 agent（不在另一个正在执行的任务里的那个——一场对话里面向用户的那个）。编排成员、
//     被当工具调的 agent 照旧同步跑：它们这一次调用结束之后，没有一个确定会再来、又不会跟兄弟撞上的模型调用
//     能接住这个结果。
//   - **作用域**：任务按调用方身份 + 会话（没有会话 ID 时按任务）分，跟转存同一把尺（[conversationScope]）。
//     查询、等待、取消只认当前会话里的任务；结束通知只发给开它的那个 agent。
//   - **通知**：任务结束后，开它的 agent 下一次调模型时（`OnModelStart` 之前），框架往消息流末尾追加一条通知
//     （user 消息，带 [BackgroundTaskNotificationAnnotation]），随这一轮的产出一起写进消息流与会话历史。
//     **送达按「已提交的消息流里看到它」记**：之后的模型节点、收尾节点（return-direct 收尾也经过它）读到的消息流
//     （都是已提交的）里有它就记为送达——那一轮没成（模型调用失败、写入被回滚）就看不到它、下次再发；送达跟
//     哪个任务、哪个 session 对象无关。那一批工具中断、任务随后被放弃时也不算送达（对话没有往下走），下一个任务
//     再发——开了会话历史时通知已经记进历史，下一个任务的 prior 里就有它，同样记为送达、不再重发。模型已经经 background_task 拿到过结果的不再通知。它不是用户说的话：框架挑「用户这一轮
//     的输入」的地方跳过它（[IsBackgroundTaskNotification]）。
//   - **生命周期**：任务归 [BackgroundTaskManager]，跟 Run 无关——Run 结束、续跑、放弃都不影响它；进程退出前
//     [BackgroundTaskManager.Close]。不落盘：进程重启之后任务与结果都没了，模型再查得到「没找到」。续跑时转入
//     后台的那次调用命中工具账、不再执行，模型看到的还是同一个任务 id。
//   - **上限**：同时在跑的任务数有全局与每个会话两道上限；到了上限的调用不转后台、照旧同步等（跟没开这个功能
//     一样，工具超时照常生效）。转入后台的任务最长跑 MaxDuration，超了取消。

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"crew-go/compose_v2"
	"crew-go/internal/panicerr"
	"crew-go/schema"
	"crew-go/tool"
)

// BackgroundTaskToolName 是查询 / 等待 / 取消 / 列出后台任务的框架工具名。
const BackgroundTaskToolName = "background_task"

// BackgroundTaskAnnotation 盖在「已转入后台」的那条 tool_result 上，值是任务 id。
const BackgroundTaskAnnotation = "crew_background_task"

// BackgroundTaskNotificationAnnotation 盖在框架送进对话的任务结束通知上，值是任务 id。
const BackgroundTaskNotificationAnnotation = "crew_background_task_notification"

// IsBackgroundTaskNotification 报这条消息是不是框架送进对话的后台任务结束通知。
//
// 通知是 user 角色、追加在消息流末尾，但它不是用户说的话：凡是**挑一条消息代表「用户这一轮的输入」**的地方都要
// 跳过它。框架以 user 角色追加的提示不只它一类，挑用户输入时用 [IsFrameworkNote]（三类一起认），同
// [IsUnresolvedCallsRecord]。
func IsBackgroundTaskNotification(m *schema.Message) bool {
	if m == nil || m.Annotations == nil {
		return false
	}
	id, _ := m.Annotations[BackgroundTaskNotificationAnnotation].(string)
	return id != ""
}

const (
	defaultBackgroundYieldAfter  = 10 * time.Second
	defaultBackgroundMaxWait     = 10 * time.Minute
	defaultBackgroundWait        = 30 * time.Second
	defaultBackgroundMaxRunning  = 64
	defaultBackgroundMaxPerConv  = 8
	defaultBackgroundMaxDuration = time.Hour
	defaultBackgroundRetention   = time.Hour
	// 一个会话最多留多少个任务（在跑的不丢，超出时从最早转入后台、已经结束了的丢起）；整个管理器多久顺手清一遍过期的。
	backgroundMaxRetainedPerConv = 100
	backgroundSweepEvery         = time.Minute
	backgroundListMax            = 20
	backgroundArgsPreview        = 200
	backgroundErrorPreview       = 2000
	backgroundCancelGrace        = 2 * time.Second
	backgroundSoftCancelPoll     = 200 * time.Millisecond
)

// BackgroundTaskStatus 是后台任务的状态。
type BackgroundTaskStatus string

const (
	// BackgroundTaskRunning：还在跑。
	BackgroundTaskRunning BackgroundTaskStatus = "running"
	// BackgroundTaskCompleted：工具返回了结果——成功，或业务工具声明的失败（tool.Failure，那也是工具给的结果）。
	BackgroundTaskCompleted BackgroundTaskStatus = "completed"
	// BackgroundTaskFailed：工具返回 error、panic，或超过最长运行时间被取消。
	BackgroundTaskFailed BackgroundTaskStatus = "failed"
	// BackgroundTaskCancelled：被模型（background_task 的 cancel）、业务（[BackgroundTaskManager.Cancel]）或
	// [BackgroundTaskManager.Close] 取消。
	BackgroundTaskCancelled BackgroundTaskStatus = "cancelled"
)

// BackgroundTask 是一个后台任务某一刻的快照。
type BackgroundTask struct {
	ID string
	// ToolName / Arguments 是那次工具调用（Arguments 是调用参数的 JSON 原文）。
	ToolName  string
	Arguments string
	// Agent 是开它的 agent；Owner / ConversationID / TaskID 是开它时的调用方身份、会话 ID 与任务（RunID）。
	Agent          string
	Owner          string
	ConversationID string
	TaskID         string

	Status    BackgroundTaskStatus
	StartedAt time.Time
	// EndedAt / Result / Err 在任务结束之后才有：Result / Err 是工具的原始返回。
	EndedAt time.Time
	Result  *tool.Result
	Err     error
}

// BackgroundTaskManagerConfig 配 [NewBackgroundTaskManager]。零值字段用默认值。
type BackgroundTaskManagerConfig struct {
	// MaxRunning 是整个管理器同时在跑的后台任务上限，默认 64。到了上限的调用不转后台，照旧同步等。
	MaxRunning int
	// MaxRunningPerConversation 是一个会话里同时在跑的后台任务上限，默认 8。
	MaxRunningPerConversation int
	// MaxDuration 是一个任务最长跑多久（从工具开始执行算），默认 1 小时；超了取消，任务记为失败。
	MaxDuration time.Duration
	// Retention 是结束了的任务保留多久，默认 1 小时。过期之后查询得到「没找到」，没送达的通知也不再发。
	Retention time.Duration
	// OnDone 在任务结束时调（在后台 goroutine 里调，panic 会被兜住），给业务观测、落库、推送用。
	OnDone func(BackgroundTask)
}

// BackgroundTaskManager 持有后台任务：起、记账、送通知、取消、清理。一个进程建一个，在开了后台任务的 agent
// 之间共用；进程退出前调 [BackgroundTaskManager.Close]。见 [WithBackgroundTasks]。
type BackgroundTaskManager struct {
	cfg BackgroundTaskManagerConfig

	mu        sync.Mutex
	closed    bool
	tasks     map[string]*bgTask   // 已转入后台的，按 id
	byConv    map[string][]*bgTask // 已转入后台的，按会话作用域、开始先后
	running   map[string]int       // 每个会话作用域在跑的数
	total     int                  // 在跑的总数
	live      map[*bgTask]struct{} // 工具还没返回的（含同步等着、没转入后台的），Close 时一并取消
	lastSweep time.Time

	wg sync.WaitGroup
}

// NewBackgroundTaskManager 建一个后台任务管理器。cfg 为 nil 时全用默认值。
func NewBackgroundTaskManager(cfg *BackgroundTaskManagerConfig) *BackgroundTaskManager {
	var c BackgroundTaskManagerConfig
	if cfg != nil {
		c = *cfg
	}
	if c.MaxRunning <= 0 {
		c.MaxRunning = defaultBackgroundMaxRunning
	}
	if c.MaxRunningPerConversation <= 0 {
		c.MaxRunningPerConversation = defaultBackgroundMaxPerConv
	}
	if c.MaxDuration <= 0 {
		c.MaxDuration = defaultBackgroundMaxDuration
	}
	if c.Retention <= 0 {
		c.Retention = defaultBackgroundRetention
	}
	return &BackgroundTaskManager{
		cfg:     c,
		tasks:   map[string]*bgTask{},
		byConv:  map[string][]*bgTask{},
		running: map[string]int{},
		live:    map[*bgTask]struct{}{},
	}
}

// BackgroundTasksConfig 配 [WithBackgroundTasks]。
type BackgroundTasksConfig struct {
	// Manager 持有任务，必填。
	Manager *BackgroundTaskManager
	// Eligible 点名哪些业务工具可以转入后台，必填。点名的工具在一个脱离当前执行的 ctx 里跑（不论最后有没有
	// 转入后台）：不能提问、不能做转交 / 路由决定、不能用节点私有态、拿不到 session、会话 ID、这次请求的编排钩子
	// 与 checkpointer。它可能在请求结束之后还在跑，所以**不能持有这次请求的资源**——按请求构造、往这次请求的流
	// 里写的钩子（比如挂了 agui adapter 的子 agent 包成的工具）别点名。return-direct 的工具也别点名——转入后台之后
	// 它的结果只能作为通知送回，不再是答复。
	//
	// 它的 ctx 截止时间是管理器的 MaxDuration（转入后台之后它还要接着跑）；同步等的那段里工具超时由框架到点
	// 取消 ctx（`ctx.Err()` 是 Canceled 不是 DeadlineExceeded）。完全不看 ctx 的工具停不下来：转入后台之后一直
	// 占着上限里的一个名额，直到它自己返回。
	Eligible func(t tool.Tool) bool
	// YieldAfter 是同步等多久还没返回就转入后台，默认 10 秒。比工具超时（[WithToolTimeout]）短才有意义：
	// 同步等的那段里工具超时照常生效。
	YieldAfter time.Duration
	// MaxWait 是模型用 background_task 等一次最多等多久，默认 10 分钟。
	MaxWait time.Duration
}

func (c *BackgroundTasksConfig) yieldAfter() time.Duration {
	if c.YieldAfter <= 0 {
		return defaultBackgroundYieldAfter
	}
	return c.YieldAfter
}

func (c *BackgroundTasksConfig) maxWait() time.Duration {
	if c.MaxWait <= 0 {
		return defaultBackgroundMaxWait
	}
	return c.MaxWait
}

// WithBackgroundTasks 让长时间运行的工具转入后台（仅 BasicAgent，且只在顶层生效）：点名的工具一次调用跑过
// YieldAfter 还没返回，就先给模型一个任务 id、这一轮接着往下走；它在后台跑完之后，结果在这个 agent 下一次调
// 模型时作为通知送进对话。同时注入 background_task 工具，让模型查询、等待、取消、列出。
//
//	mgr := agent_v2.NewBackgroundTaskManager(nil)
//	defer mgr.Close(context.Background())
//	agent_v2.NewBasicAgent(
//	    agent_v2.WithName("devops"), // 必填：通知按 agent 名发给开它的那个 agent
//	    agent_v2.WithModel(m),
//	    agent_v2.WithToolkit(tk),
//	    agent_v2.WithBackgroundTasks(&agent_v2.BackgroundTasksConfig{
//	        Manager:  mgr,
//	        Eligible: func(t tool.Tool) bool { return t.Name() == "run_tests" },
//	    }),
//	)
//
// 跨 Run 收通知要有会话 ID（`compose_v2.WithThreadIDCtx`）：没有时任务按「任务」分，Run 结束、开了新任务之后
// 就查不到、也收不到了。完整说明见 background_task.go。
func WithBackgroundTasks(cfg *BackgroundTasksConfig) Option {
	return func(c *Config) {
		c.BackgroundTasks = cfg
	}
}

// validateBackgroundTasksConfig 是构造期检查：Manager 与 Eligible 必填。
func validateBackgroundTasksConfig(cfg *Config) {
	bt := cfg.BackgroundTasks
	if bt == nil {
		return
	}
	if bt.Manager == nil {
		panic("agent_v2: WithBackgroundTasks 需要 Manager——用 NewBackgroundTaskManager 建一个，进程退出前 Close")
	}
	if bt.Eligible == nil {
		panic("agent_v2: WithBackgroundTasks 需要 Eligible——哪些工具可以转入后台由业务点名：点名的工具在脱离当前执行的 ctx 里跑，" +
			"不能提问、不能做转交 / 路由决定、拿不到 session")
	}
	if !cfg.nameExplicit {
		panic("agent_v2: WithBackgroundTasks 要求显式 WithName——任务结束通知按 agent 名发给开它的那个 agent，" +
			"自动生成的名字每次构造都不同，按请求构造 agent 时下一次请求永远收不到通知")
	}
	if cfg.Toolkit != nil && cfg.Toolkit.Get(BackgroundTaskToolName) != nil {
		slog.Warn("framework background_task overrides business tool with the same name", "agent", cfg.Name)
	}
}

var (
	errBackgroundCancelledByModel  = errors.New("cancelled by background_task")
	errBackgroundCancelledByCaller = errors.New("cancelled by BackgroundTaskManager.Cancel")
	errBackgroundManagerClosed     = errors.New("background task manager closed")
	errBackgroundMaxDuration       = errors.New("background task exceeded max duration")
	errBackgroundToolTimeout       = errors.New("tool timeout")
)

// bgTask 是一个后台任务（或一次还在同步等、没转入后台的可后台调用）。
type bgTask struct {
	id, toolName, args string
	conv               string // 会话作用域（[conversationScope]）
	agent              string // 开它的 agent（通知发给它）
	owner, convID      string // 开它时的调用方身份、会话 ID（快照用）
	task               string // 开它时的任务（RunID）
	startedAt          time.Time
	cancel             context.CancelCauseFunc
	done               chan struct{} // 没转入后台：工具返回后关；转入了：结局定下后关

	// 以下由 BackgroundTaskManager.mu 保护
	admitted        bool // 转入了后台（进了 tasks / byConv、计入上限）
	returned        bool // 工具返回了
	res             *tool.Result
	err             error
	cause           error // 工具返回时任务 ctx 的取消原因（没取消为 nil）
	cancelRequested bool
	status          BackgroundTaskStatus
	text            string // 结局的正文（交给模型的那段）
	spillID         string // 结果被转存时的取回 id 与说明（盖到通知上，见 ToolResultSpillAnnotation）
	spillInfo       string
	endedAt         time.Time
	delivered       bool // 模型已经看到过结局：已提交的消息流里有它的通知，或经 background_task 交出过
}

func (t *bgTask) snapshotLocked() BackgroundTask {
	return BackgroundTask{
		ID: t.id, ToolName: t.toolName, Arguments: t.args,
		Agent: t.agent, Owner: t.owner, ConversationID: t.convID, TaskID: t.task,
		Status: t.status, StartedAt: t.startedAt, EndedAt: t.endedAt, Result: t.res, Err: t.err,
	}
}

func (t *bgTask) terminal() bool {
	return t.status == BackgroundTaskCompleted || t.status == BackgroundTaskFailed || t.status == BackgroundTaskCancelled
}

// Get 返回任务 id 的快照（业务侧用，不看作用域）。没有或已过期时 ok 为 false。
func (m *BackgroundTaskManager) Get(id string) (BackgroundTask, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.tasks[id]
	if t == nil {
		return BackgroundTask{}, false
	}
	return t.snapshotLocked(), true
}

// Cancel 取消任务 id（业务侧用，不看作用域，比如用户在界面上点了取消）。任务在跑时返回 true。
// 工具不响应 ctx 取消时它会继续跑到返回，那时按它的实际返回记结局。
func (m *BackgroundTaskManager) Cancel(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.tasks[id]
	if t == nil || t.terminal() {
		return false
	}
	t.cancelRequested = true
	t.cancel(errBackgroundCancelledByCaller)
	return true
}

// Close 取消所有还在跑的任务，等它们的工具返回（最多等到 ctx 结束）。之后可后台的调用照旧同步执行、不再转入
// 后台。可以重复调。
func (m *BackgroundTaskManager) Close(ctx context.Context) error {
	m.mu.Lock()
	if !m.closed {
		m.closed = true
		for t := range m.live {
			t.cancelRequested = true
			t.cancel(errBackgroundManagerClosed)
		}
	}
	m.mu.Unlock()
	// 这个 goroutine 等到所有任务的工具返回就退出；Close 等不及返回时它照样会退出
	drained := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(drained)
	}()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("background task manager close: %w", ctx.Err())
	}
}

// ── 作用域 ──

// backgroundScope 是一次 BasicAgent 调用的后台任务作用域，在 Run / Resume 入口定下（见 withBackgroundScope）。
type backgroundScope struct {
	cfg           *BackgroundTasksConfig
	conv          string
	owner, convID string
	agent         string
	task          string
}

type backgroundScopeKey struct{}

// withBackgroundScope 在 BasicAgent 的 Run / Resume 入口放这次调用的后台任务作用域：顶层、开了后台任务时
// 非空，否则放 nil——盖掉从调用方继承来的那份（顶层 agent 的工具里调起的子 agent 不该转后台、不该收父的通知）。
func withBackgroundScope(ctx context.Context, cfg *Config, topLevel bool) context.Context {
	var sc *backgroundScope
	if topLevel && cfg.BackgroundTasks != nil {
		owner, conv := conversationScope(ctx)
		sc = &backgroundScope{
			cfg:    cfg.BackgroundTasks,
			conv:   owner + "/" + conv,
			owner:  compose_v2.GetOwner(ctx),
			convID: compose_v2.GetThreadID(ctx),
			agent:  cfg.Name,
			task:   currentHistoryTask(ctx),
		}
	}
	return context.WithValue(ctx, backgroundScopeKey{}, sc)
}

func backgroundScopeFrom(ctx context.Context) *backgroundScope {
	sc, _ := ctx.Value(backgroundScopeKey{}).(*backgroundScope)
	return sc
}

// backgroundEligible 报这次调用能不能走可后台的路：这一层开了后台任务、派发的是业务工具、业务点了它的名。
func backgroundEligible(sc *backgroundScope, minted bool, t tool.Tool) bool {
	return sc != nil && !minted && t != nil && sc.cfg.Eligible(t)
}

// ── 一次可后台的调用 ──

// bgOutcome 是一次可后台调用的结局：工具在转入后台之前返回了（res / err，跟同步执行一样往下走）、转入了后台
// （task）、或者同步等的那段里超时了（timedOut 是那条上限）。
type bgOutcome struct {
	res      *tool.Result
	err      error
	task     *bgTask
	timedOut time.Duration
}

// call 跑一次可后台的调用。管理器已关闭时 started 为 false，调用方照旧同步执行。
//
// invoke 是这次调用本身（中间件链 + 工具），在后台 goroutine 里以脱离当前执行的 ctx 跑；render 把工具的结果变成
// 交给模型的正文（转存 / 截断与同步执行同一份），任务转入后台、结束之后调。
//
// bgRender 返回交给模型的正文，连同转存后的取回 id 与说明（没转存为空）。
type bgRender func(ctx context.Context, res *tool.Result) bgRendered

type bgRendered struct{ content, spillID, spillInfo string }

func (m *BackgroundTaskManager) call(callCtx context.Context, sc *backgroundScope, tc schema.ToolCall, toolTimeout time.Duration,
	invoke func(c, guard context.Context) (*tool.Result, error), render bgRender) (out bgOutcome, started bool) {
	t := m.start(callCtx, sc, tc, invoke, render)
	if t == nil {
		return bgOutcome{}, false
	}
	return m.await(callCtx, t, sc.cfg.yieldAfter(), toolTimeout), true
}

// start 起工具的 goroutine。任务的 ctx 脱离当前执行（compose_v2.DetachExecution）、去掉这次请求的环境
// （backgroundTaskCtx）、不跟调用方的取消（同步等的那段由 await 代为转达），最长 MaxDuration。
func (m *BackgroundTaskManager) start(callCtx context.Context, sc *backgroundScope, tc schema.ToolCall,
	invoke func(c, guard context.Context) (*tool.Result, error), render bgRender) *bgTask {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	// 转存要按这次调用所在的会话分命名空间（取回工具在同一个会话里按同一把尺找），所以渲染用的是保留了全部值、
	// 只去掉取消的那个；交给工具的是脱离了当前执行的那个。
	keep := context.WithoutCancel(callCtx)
	tctx, stopTimer := context.WithTimeoutCause(backgroundTaskCtx(keep), m.cfg.MaxDuration, errBackgroundMaxDuration)
	tctx, cancel := context.WithCancelCause(tctx)
	t := &bgTask{
		id: newBackgroundTaskID(), toolName: tc.Function.Name, args: tc.Function.Arguments,
		conv: sc.conv, agent: sc.agent, owner: sc.owner, convID: sc.convID, task: sc.task,
		startedAt: time.Now(), cancel: cancel, done: make(chan struct{}),
	}
	m.live[t] = struct{}{}
	m.wg.Add(1)
	m.mu.Unlock()
	go func() {
		defer m.wg.Done()
		defer stopTimer()
		defer cancel(nil)
		res, err := invokeRecovered(tctx, t.toolName, invoke)
		m.finish(keep, t, res, err, context.Cause(tctx), render)
	}()
	return t
}

// backgroundTaskCtx 是交给点名工具的 ctx：脱离当前执行，再把这次请求的环境里会被请求结束之后用到的那几样换掉——
// 编排钩子（两层）换成空的（它们往这次请求的流里写，请求结束之后再写就写进了已经返回的连接），checkpointer
// 去掉（后台里跑的嵌套 agent 中断时会留下一份没人续的存档）。调用主体、store 留着，工具自己要用。
func backgroundTaskCtx(keep context.Context) context.Context {
	ctx := compose_v2.DetachExecution(keep)
	ctx = compose_v2.WithOrchHooks(ctx, compose_v2.NoOpOrchestrationHooks{})
	ctx = WithCtxOrchHooks(ctx, NoOpOrchestrationHooks{})
	return compose_v2.WithCheckpointerCtx(ctx, nil)
}

// invokeRecovered 跑 invoke，把 panic 转成 error——后台 goroutine 里没人兜，不转的话整个进程崩掉。
func invokeRecovered(ctx context.Context, toolName string, invoke func(c, guard context.Context) (*tool.Result, error)) (res *tool.Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			pe := panicerr.New(r)
			res, err = nil, panicerr.Wrap(fmt.Sprintf("tool %q panic recovered", toolName), pe)
			panicerr.Log(ctx, slog.LevelError, "agent_v2: 后台工具 panic，已转成错误", pe, panicerr.KeyError, err.Error())
		}
	}()
	return invoke(ctx, ctx)
}

// await 同步等工具：在 YieldAfter 之内返回就当场交回结果；到点了还没返回、又没到上限，就转入后台。
func (m *BackgroundTaskManager) await(callCtx context.Context, t *bgTask, yieldAfter, toolTimeout time.Duration) bgOutcome {
	yield := time.NewTimer(yieldAfter)
	defer yield.Stop()
	yieldC := yield.C
	var timeoutC <-chan time.Time
	if toolTimeout > 0 {
		tt := time.NewTimer(toolTimeout)
		defer tt.Stop()
		timeoutC = tt.C
	}
	for {
		select {
		case <-t.done:
			return m.syncOutcome(t)
		case <-yieldC:
			yieldC = nil
			admitted, returned := m.admit(t)
			if admitted {
				slog.InfoContext(callCtx, "agent_v2: 工具调用转入后台", "task_id", t.id, "tool", t.toolName, "yield_after", yieldAfter)
				return bgOutcome{task: t}
			}
			if returned {
				<-t.done // 工具刚好返回、done 马上就关
				return m.syncOutcome(t)
			}
			slog.InfoContext(callCtx, "agent_v2: 后台任务已到上限，这次调用照旧同步等", "tool", t.toolName)
		case <-timeoutC:
			// 跟同步执行的工具超时一样：交给工具的 ctx 到点取消，照样等它返回，只在它返回时说一句「超时了」
			t.cancel(errBackgroundToolTimeout)
			<-t.done
			o := m.syncOutcome(t)
			if o.err == nil && o.res != nil && o.res.Success {
				slog.WarnContext(callCtx, "agent_v2: 工具超过 WithToolTimeout 仍返回成功——"+
					"它没有响应 ctx 取消，这次调用实际没有被超时打断（结果已按超时丢弃）",
					"tool", t.toolName, "timeout", toolTimeout)
			}
			return bgOutcome{timedOut: toolTimeout}
		case <-callCtx.Done():
			// 调用方撤了：任务跟着取消，照样等工具返回——跟同步执行一样（续跑时这次调用不在工具账里、会再执行，
			// 不等的话两次执行可能重叠）
			t.cancel(context.Cause(callCtx))
			<-t.done
			return bgOutcome{err: callCtx.Err()}
		}
	}
}

// syncOutcome 取没转入后台的那次调用的返回。超过最长运行时间（同步等着时也算）按超时收尾。
func (m *BackgroundTaskManager) syncOutcome(t *bgTask) bgOutcome {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t.err != nil && errors.Is(t.cause, errBackgroundMaxDuration) {
		return bgOutcome{timedOut: m.cfg.MaxDuration}
	}
	if t.err != nil && errors.Is(t.cause, errBackgroundManagerClosed) {
		return bgOutcome{err: fmt.Errorf("tool %q: %w: %w", t.toolName, errBackgroundManagerClosed, t.err)}
	}
	return bgOutcome{res: t.res, err: t.err}
}

// admit 把还没返回的调用转入后台。工具已经返回时 returned 为 true；管理器关了或到了上限时两者都是 false。
func (m *BackgroundTaskManager) admit(t *bgTask) (admitted, returned bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t.returned {
		return false, true
	}
	now := time.Now()
	m.maybeSweepAllLocked(now)
	if m.closed || m.total >= m.cfg.MaxRunning || m.running[t.conv] >= m.cfg.MaxRunningPerConversation {
		return false, false
	}
	m.sweepConvLocked(t.conv, now)
	t.admitted = true
	t.status = BackgroundTaskRunning
	m.tasks[t.id] = t
	m.byConv[t.conv] = append(m.byConv[t.conv], t)
	m.total++
	m.running[t.conv]++
	return true, false
}

// finish 在工具返回时调：没转入后台的只记下返回、交给 await；转入了的定结局、发 OnDone。
//
// 这里跑的业务代码——渲染（转存走业务的 store）、日志 handler、OnDone——都兜住 panic：后台 goroutine 里没人兜，
// 不兜的话整个进程崩掉（同步执行时同样的 panic 由 runOne 兜住）。结局与计数一定落下、done 一定关。
func (m *BackgroundTaskManager) finish(renderCtx context.Context, t *bgTask, res *tool.Result, err, cause error, render bgRender) {
	m.mu.Lock()
	t.returned = true
	t.res, t.err, t.cause = res, err, cause
	delete(m.live, t)
	admitted := t.admitted
	m.mu.Unlock()
	if !admitted {
		close(t.done)
		return
	}
	// 渲染可能转存（store I/O），放在锁外
	status, text, sp := m.conclude(renderCtx, t.toolName, res, err, cause, render)
	m.mu.Lock()
	if status == BackgroundTaskCompleted && t.cancelRequested {
		text = "（取消请求之后它仍然跑完了，下面是它的结果）\n" + text
	}
	t.status, t.text, t.spillID, t.spillInfo, t.endedAt = status, text, sp.spillID, sp.spillInfo, time.Now()
	m.total--
	if m.running[t.conv]--; m.running[t.conv] <= 0 {
		delete(m.running, t.conv)
	}
	snap := t.snapshotLocked()
	m.mu.Unlock()
	close(t.done)
	m.afterDone(renderCtx, t.id, snap)
}

// conclude 定结局：状态、交给模型的正文、转存后的取回 id。渲染 panic 时记为失败。
func (m *BackgroundTaskManager) conclude(ctx context.Context, toolName string, res *tool.Result, err, cause error,
	render bgRender) (status BackgroundTaskStatus, text string, sp bgRendered) {
	if err != nil {
		switch {
		case errors.Is(cause, errBackgroundCancelledByModel):
			return BackgroundTaskCancelled, "按 background_task 的取消请求取消", sp
		case errors.Is(cause, errBackgroundCancelledByCaller):
			return BackgroundTaskCancelled, "被业务取消", sp
		case errors.Is(cause, errBackgroundManagerClosed):
			return BackgroundTaskCancelled, "后台任务管理器已关闭（服务在停止）", sp
		case errors.Is(cause, errBackgroundMaxDuration):
			return BackgroundTaskFailed, fmt.Sprintf("超过最长运行时间 %s，已取消", m.cfg.MaxDuration), sp
		}
		return BackgroundTaskFailed, truncateWithEllipsis(err.Error(), backgroundErrorPreview), sp
	}
	if res != nil && res.Metadata != nil {
		if rd, ok := res.Metadata[tool.ReturnDirectMetaKey].(bool); ok && rd {
			slog.WarnContext(ctx, "agent_v2: return-direct 工具的结果转入后台之后只能作为通知送回，不再是答复——别把它点进 Eligible",
				"tool", toolName)
		}
		// 转入后台之后结果只是给模型看的信息：去掉 Metadata，return-direct 的豁免不再适用（转存 / 截断照常）
		cp := *res
		cp.Metadata = nil
		res = &cp
	}
	defer func() {
		if r := recover(); r != nil {
			pe := panicerr.New(r)
			status, text, sp = BackgroundTaskFailed, fmt.Sprintf("整理工具结果时出错：%v", r), bgRendered{}
			panicerr.Log(ctx, slog.LevelError, "agent_v2: 后台任务整理结果时 panic，已记为失败", pe, "tool", toolName)
		}
	}()
	sp = render(ctx, res)
	return BackgroundTaskCompleted, sp.content, sp
}

// afterDone 报任务结束：日志与 OnDone。两者都是业务代码（日志 handler 也是），panic 兜住。
func (m *BackgroundTaskManager) afterDone(ctx context.Context, id string, snap BackgroundTask) {
	defer func() {
		if r := recover(); r != nil {
			pe := panicerr.New(r)
			panicerr.Log(ctx, slog.LevelError, "agent_v2: 后台任务结束之后的日志或 OnDone panic，已兜住", pe, "task_id", id)
		}
	}()
	slog.InfoContext(ctx, "agent_v2: 后台任务结束", "task_id", id, "tool", snap.ToolName,
		"status", string(snap.Status), "elapsed", snap.EndedAt.Sub(snap.StartedAt))
	if m.cfg.OnDone != nil {
		m.cfg.OnDone(snap)
	}
}

// ── 清理 ──

// maybeSweepAllLocked 每隔一段时间顺手把所有会话里过期的任务清掉——只按会话清的话，不再有动静的会话里的任务
// 永远留着。
func (m *BackgroundTaskManager) maybeSweepAllLocked(now time.Time) {
	if now.Sub(m.lastSweep) < backgroundSweepEvery {
		return
	}
	m.lastSweep = now
	for conv := range m.byConv {
		m.sweepConvLocked(conv, now)
	}
}

// sweepConvLocked 清掉一个会话里过期的、超出保留个数的结束了的任务（在跑的不动）。
func (m *BackgroundTaskManager) sweepConvLocked(conv string, now time.Time) {
	list := m.byConv[conv]
	kept := make([]*bgTask, 0, len(list))
	for _, t := range list {
		if t.terminal() && now.Sub(t.endedAt) > m.cfg.Retention {
			delete(m.tasks, t.id)
			continue
		}
		kept = append(kept, t)
	}
	// 超出保留个数：从最早的开始丢结束了的
	excess := len(kept) - backgroundMaxRetainedPerConv
	if excess > 0 {
		trimmed := make([]*bgTask, 0, len(kept))
		for _, t := range kept {
			if excess > 0 && t.terminal() {
				delete(m.tasks, t.id)
				excess--
				continue
			}
			trimmed = append(trimmed, t)
		}
		kept = trimmed
	}
	if len(kept) == 0 {
		delete(m.byConv, conv)
		return
	}
	m.byConv[conv] = kept
}

// ── 通知 ──

// backgroundNotes 是模型节点这一轮要送进对话的任务结束通知。msgs 是这一轮开始时已提交的消息流：先按它记下已经送达
// 的（backgroundSeen），再挑出还没送达的。不在顶层、没开后台任务时返回 nil。
func backgroundNotes(ctx context.Context, msgs []*schema.Message) []*schema.Message {
	sc := backgroundScopeFrom(ctx)
	if sc == nil {
		return nil
	}
	return sc.cfg.Manager.notes(sc, msgs)
}

// backgroundSeen 按一份已提交的消息流记下哪些通知已经送达（收尾节点调；模型节点经 backgroundNotes 先调它）。通知在
// 哪一轮送进来，之后那个节点读到的已提交消息流里就有它——那一轮没成（模型调用失败、写入被回滚）时没有，下次再发。
func backgroundSeen(ctx context.Context, msgs []*schema.Message) {
	if sc := backgroundScopeFrom(ctx); sc != nil {
		sc.cfg.Manager.seen(sc, msgs)
	}
}

func (m *BackgroundTaskManager) seen(sc *backgroundScope, msgs []*schema.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seenLocked(sc, msgs)
}

// seenLocked：这个作用域里有结束了、还没送达的任务时才扫一遍消息流。
func (m *BackgroundTaskManager) seenLocked(sc *backgroundScope, msgs []*schema.Message) {
	pending := false
	for _, t := range m.byConv[sc.conv] {
		if t.terminal() && !t.delivered {
			pending = true
			break
		}
	}
	if !pending {
		return
	}
	for id := range notedTaskIDs(msgs) {
		if t := m.tasks[id]; t != nil && t.conv == sc.conv {
			t.delivered = true
		}
	}
}

func (m *BackgroundTaskManager) notes(sc *backgroundScope, msgs []*schema.Message) []*schema.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sweepConvLocked(sc.conv, time.Now())
	m.seenLocked(sc, msgs)
	var due []*bgTask
	for _, t := range m.byConv[sc.conv] {
		if t.agent == sc.agent && t.terminal() && !t.delivered {
			due = append(due, t)
		}
	}
	if len(due) == 0 {
		return nil
	}
	sort.SliceStable(due, func(i, j int) bool { return due[i].endedAt.Before(due[j].endedAt) })
	notes := make([]*schema.Message, 0, len(due))
	for _, t := range due {
		n := schema.UserMessage("[后台任务通知] " + t.terminalTextLocked())
		n.Annotations = map[string]any{BackgroundTaskNotificationAnnotation: t.id}
		if t.spillID != "" {
			n.Annotations[ToolResultSpillAnnotation] = t.spillID
			n.Annotations[toolResultSpillInfoAnnotation] = t.spillInfo
		}
		notes = append(notes, n)
	}
	return notes
}

// notedTaskIDs 收消息流里已有的任务结束通知。
func notedTaskIDs(msgs []*schema.Message) map[string]bool {
	ids := map[string]bool{}
	for _, msg := range msgs {
		if msg == nil || msg.Annotations == nil {
			continue
		}
		if id, ok := msg.Annotations[BackgroundTaskNotificationAnnotation].(string); ok && id != "" {
			ids[id] = true
		}
	}
	return ids
}

// terminalTextLocked 是结局的说明：哪次调用、用时多久、结果是什么。通知与 background_task 用的都是它。
func (t *bgTask) terminalTextLocked() string {
	head := fmt.Sprintf("任务 id=%q（%s，参数 %s）", t.id, t.toolName, truncateWithEllipsis(t.args, backgroundArgsPreview))
	took := t.endedAt.Sub(t.startedAt).Round(time.Second)
	switch t.status {
	case BackgroundTaskCompleted:
		return fmt.Sprintf("%s已完成，用时 %s。结果：\n%s", head, took, t.text)
	case BackgroundTaskFailed:
		return fmt.Sprintf("%s失败了，用时 %s：%s", head, took, t.text)
	default:
		return fmt.Sprintf("%s已取消，用时 %s：%s", head, took, t.text)
	}
}

// backgroundHandleMessage 是转入后台时交给模型的 tool_result：任务 id 与怎么拿结果。
func backgroundHandleMessage(tc schema.ToolCall, t *bgTask, waited time.Duration) *schema.Message {
	content := fmt.Sprintf("【已转入后台】%s 运行超过 %s 仍未结束，已转入后台继续执行，任务 id=%q。\n"+
		"它结束后结果会作为通知自动发给你，在那之前可以先处理别的事、或先回复用户。确实需要它的结果才能往下做时，"+
		"用 %s 等它（action=\"wait\"）；也可以查询（action=\"status\"）或取消（action=\"cancel\"）。",
		tc.Function.Name, waited, t.id, BackgroundTaskToolName)
	msg := schema.ToolResultMessage(tc.ID, content)
	msg.Annotations = map[string]any{BackgroundTaskAnnotation: t.id}
	return msg
}

func newBackgroundTaskID() string {
	var b [8]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read 不会失败（Go 1.24 起失败直接 panic）
	return "bg_" + hex.EncodeToString(b[:])
}

// ── background_task 工具 ──

// newBackgroundTaskTool 是框架铸的查询 / 等待 / 取消 / 列出工具。
func newBackgroundTaskTool() tool.Tool {
	schemaDef := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"action": map[string]any{
				"type": "string", "enum": []string{"status", "wait", "cancel", "list"},
				"description": "status：查一次状态（不等）；wait：等它结束（最多 timeout_seconds 秒）；cancel：取消它；list：列出当前会话里的后台任务",
			},
			"id":              map[string]any{"type": "string", "description": "任务 id（转入后台时给出的那个）；list 不需要"},
			"timeout_seconds": map[string]any{"type": "integer", "description": "wait 最多等多少秒，默认 30"},
		},
		"required": []string{"action"},
	}
	desc := "查询、等待、取消转入后台的工具调用。工具运行太久时会转入后台并给出一个任务 id，它结束后结果会自动作为通知发给你——" +
		"不需要反复用 status 轮询；确实需要它的结果才能往下做时用 wait。"
	return markMinted(tool.NewFuncTool(BackgroundTaskToolName, desc, schemaDef, backgroundTaskAction))
}

// backgroundTaskAction 是 background_task 的实现。参数有问题、任务找不到时返回给模型看的失败结果；ctx 被取消时
// 返回 error（这一轮本来就在停）。
func backgroundTaskAction(ctx context.Context, args map[string]any) (*tool.Result, error) {
	sc := backgroundScopeFrom(ctx)
	if sc == nil {
		return tool.Failure("unavailable", "这里没有开启后台任务"), nil
	}
	m := sc.cfg.Manager
	action, _ := args["action"].(string)
	if action == "list" {
		return &tool.Result{Success: true, Output: m.listText(sc.conv)}, nil
	}
	if action != "status" && action != "wait" && action != "cancel" {
		return tool.Failure("invalid_arguments", fmt.Sprintf("不认识的 action %q——可选 status / wait / cancel / list", action)), nil
	}
	id, _ := args["id"].(string)
	if id == "" {
		return tool.Failure("invalid_arguments", "缺少 id——用转入后台时给出的那个；要看有哪些任务用 action=\"list\""), nil
	}
	t := m.lookup(sc.conv, id)
	if t == nil {
		return tool.Failure("not_found", fmt.Sprintf("没找到任务 id=%q（可能已过期、服务重启后丢失，或不属于当前会话）", id)), nil
	}
	switch action {
	case "wait":
		d := time.Duration(intArg(args, "timeout_seconds", int(defaultBackgroundWait/time.Second))) * time.Second
		if d <= 0 {
			d = defaultBackgroundWait
		}
		if max := sc.cfg.maxWait(); d > max {
			d = max
		}
		if err := waitBackgroundTask(ctx, t, d); err != nil {
			return nil, err
		}
	case "cancel":
		m.requestCancel(t)
		if err := waitBackgroundTask(ctx, t, backgroundCancelGrace); err != nil {
			return nil, err
		}
	}
	return &tool.Result{Success: true, Output: m.describe(t, action == "cancel")}, nil
}

// lookup 按 id 找当前会话作用域里的任务；别的会话的当作没有（不透露它存在）。
func (m *BackgroundTaskManager) lookup(conv, id string) *bgTask {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sweepConvLocked(conv, time.Now())
	t := m.tasks[id]
	if t == nil || t.conv != conv {
		return nil
	}
	return t
}

func (m *BackgroundTaskManager) requestCancel(t *bgTask) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t.terminal() {
		return
	}
	t.cancelRequested = true
	t.cancel(errBackgroundCancelledByModel)
}

// waitBackgroundTask 等任务结束，最多 d。ctx 被取消时返回它的错；软取消触发时提前返回（不报错）。
func waitBackgroundTask(ctx context.Context, t *bgTask, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	var poll <-chan time.Time
	if sc := compose_v2.GetSoftCancel(ctx); sc != nil {
		tk := time.NewTicker(backgroundSoftCancelPoll)
		defer tk.Stop()
		poll = tk.C
	}
	for {
		select {
		case <-t.done:
			return nil
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-poll:
			if triggered, _ := compose_v2.GetSoftCancel(ctx).Triggered(); triggered {
				return nil
			}
		}
	}
}

// describe 是 status / wait / cancel 交给模型的说明。结局已定时一并交出结果，记为已送达（不再通知）。
func (m *BackgroundTaskManager) describe(t *bgTask, cancelling bool) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t.terminal() {
		t.delivered = true
		return t.terminalTextLocked()
	}
	elapsed := time.Since(t.startedAt).Round(time.Second)
	if cancelling {
		return fmt.Sprintf("已请求取消任务 id=%q（%s，已运行 %s），它还没停下（工具可能不响应取消）；结束后结果会通知你。", t.id, t.toolName, elapsed)
	}
	return fmt.Sprintf("任务 id=%q（%s）仍在运行，已运行 %s。它结束后结果会自动通知你；确实需要结果才能往下做时用 action=\"wait\"。",
		t.id, t.toolName, elapsed)
}

// listText 列出当前会话作用域里的任务，最近的在前。
func (m *BackgroundTaskManager) listText(conv string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sweepConvLocked(conv, time.Now())
	list := m.byConv[conv]
	if len(list) == 0 {
		return "当前会话里没有后台任务。"
	}
	var b strings.Builder
	n := len(list)
	if n > backgroundListMax {
		fmt.Fprintf(&b, "当前会话里的后台任务（共 %d 个，列出最近 %d 个）：\n", n, backgroundListMax)
	} else {
		fmt.Fprintf(&b, "当前会话里的后台任务（%d 个）：\n", n)
	}
	now := time.Now()
	for i := n - 1; i >= 0 && n-1-i < backgroundListMax; i-- {
		t := list[i]
		if t.terminal() {
			fmt.Fprintf(&b, "- %s  %s  %s（用时 %s）\n", t.id, t.status, t.toolName, t.endedAt.Sub(t.startedAt).Round(time.Second))
		} else {
			fmt.Fprintf(&b, "- %s  %s  %s（已运行 %s）\n", t.id, t.status, t.toolName, now.Sub(t.startedAt).Round(time.Second))
		}
	}
	return strings.TrimRight(b.String(), "\n")
}
