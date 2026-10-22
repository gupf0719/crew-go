package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"path/filepath"
	"runtime"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/example/envloader"
	"crew-go/example/llmtrace"
	"crew-go/example/otelsetup"
	"crew-go/example/scenarios"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/model/deepseek"
	"crew-go/observability"
	"crew-go/prompt"
	"crew-go/schema"
	"crew-go/skill"
	"crew-go/tool"
)

// BasicAgent 级功能演示 flag（融入 plan_chat_web）——默认全关，不改现有 demo 行为，
// 加 -feat-* 开关按需演示。见 buildChatEntryAgent 里的条件装配。
var (
	featToolFilter     bool // ToolFilter：chat_entry 按 mode 裁工具（normal→ask_*, plan→scenario）
	featPromptTemplate bool // PromptTemplate：chat_entry sysPrompt 参数化模板
	featToolSearch     bool // ToolSearch：scenario 重型工具 deferred，LLM 调 tool_search 揭示
	featApproval       bool // Approval：敏感工具（ask_coder）执行前审批
)

// skillsDir 返本 binary 同目录下 ./skills 绝对路径——`go run` 不论 cwd 都能定位。
func skillsDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "skills"
	}
	return filepath.Join(filepath.Dir(file), "skills")
}

// runSlot 持有一个 HITL 会话的可中断 + 可恢复状态（ISSUE-56 / ISSUE-58）。
//
// 同一 session_id 可被多次 SSE 连接复用：第一次新建 → 跑到一半触发 interrupt →
// runtime save checkpoint 到 cp[sess.RunID] → SSE 断开 → 用户点"继续/改方向/取消"再发
// 一次 GET /api/chat?session_id=xxx&action=...&text=... → 服务端按 action 走 Resume。
//
// **lifecycle 模型** (run status, ISSUE-90)：
// runs sync.Map 完成后**不删** slot, 仅 transition status → 让前端能查 history。
// 后台 sweeper goroutine (-session-ttl-minutes) 周期清 lastActivity 超期 slot 防 OOM。
//
// status 字段 6 状态:
//   - running — 当前在跑 LLM (q-only 新建 / Resume 续传后)
//   - interrupted — InterruptError 挂起, 可 Resume
//   - success — 自然跑完
//   - error — 非 Interrupt 错 (LLM 报错 / ctx 超时等)
//   - cancelled — Resume action=abort 用户主动取消
//
// status / lastActivity 用 mu 保护; handler / goroutine / sweeper 三方并发读写。
//
// resumeAction / resumeText / lastInterruptID / mode 单次串行赋值 (handler 写 →
// goroutine 读 → 不同 Resume 之间 handler 重置), 不需 lock。
type runSlot struct {
	sess      *agent_v2.RunSession
	cp        *compose_v2.InMemoryCheckpointer
	interrupt atomic.Bool

	mu           sync.RWMutex
	status       string    // 6 状态见 godoc; sweeper / handler 读, goroutine / handler 写
	lastActivity time.Time // sweeper 按 TTL 判过期; 每次 transition + Resume handler 触发更新
	createdAt    time.Time // history 时序展示

	// ISSUE-58 HITL：每次 /api/chat 触发 resume 时由 handler 注入，goroutine 透给
	// PlanAgent.Resume（继续/改方向/取消 + 可选文本）。
	resumeAction agent_v2.ResumeAction
	resumeText   string
	// ISSUE-70 Phase 2: lastInterruptID 是 goroutine 在中断 emit 时记录的本次
	// pending interrupt 的稳定 ID。Resume 时框架要求 caller 用 UserInputs map[id]Message
	// 显式配对，而不是单值通道——所以下次 Resume 用此 ID 包成 map。
	// 单 pending 场景下足够；多 pending 需扩为 map[wsID]string 等更复杂结构。
	lastInterruptID string

	// ISSUE-71 Mode 切换：normal / plan，sticky 状态——切了就持续到下次切。
	// chat_entry 重建时用此值装载对应 system prompt（toolkit 始终不变）。
	mode string
}

// 6 状态常量 — run status 生命周期。
// pending 状态 plan_chat_web 不用 (q-only 立即 spawn goroutine 不存在排队期)。
const (
	statusRunning     = "running"
	statusInterrupted = "interrupted"
	statusSuccess     = "success"
	statusError       = "error"
	statusCancelled   = "cancelled"
)

// setStatus 原子 transition status + 刷 lastActivity (sweeper 按 lastActivity 判过期)。
func (s *runSlot) setStatus(newStatus string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = newStatus
	s.lastActivity = time.Now()
}

// getStatus 返当前 status 副本 (handler 判 410 vs Resume / sweeper 判过期用)。
func (s *runSlot) getStatus() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

// touch 仅刷 lastActivity (Resume 续传时即使 status 不变也要重置 TTL 计时)。
func (s *runSlot) touch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastActivity = time.Now()
}

// isFinished 检 status 是否进入终态 (success/error/cancelled)。interrupted 不是终态
// (可 Resume), running 也不是。终态 slot 不可 Resume 必返 410。
func (s *runSlot) isFinished() bool {
	st := s.getStatus()
	return st == statusSuccess || st == statusError || st == statusCancelled
}

// runs 是全局 session_id → *runSlot 注册表。process 重启即丢——单进程 demo 内存
// 模式 (生产前换 Redis 见 ISSUE-90)。完成 slot 不立即 Delete 让前端能查 history,
// 后台 sweeper 按 TTL 清防 OOM。
var runs sync.Map

// requestTimeoutSeconds 是 HTTP 请求 ctx timeout, 0 表示无 timeout (依赖 client
// connection 控制). iter-5-N MED-4 修: 原硬编码 600s 一刀切 — scenario PlanAgent
// 8-step plan × 真 LLM 每 step 10-30s × replan 几轮可超 10min 触超时 cp Pending
// 被 cancel. 改可配 + 默认 0 让业务自决.
var requestTimeoutSeconds int

//go:embed index.html
var indexHTML []byte

// compactionDemoCtxWindow 是一个全局可调的 ModelContextWindow——demo / mock 模式
// 下用低阈值（默认 32000 token）容易看到 AutoCompact 触发；生产改回真实 model 上限。
//
// 通过 -compaction-window 调整：用 1000 让 mock 流轻松触发 AutoCompact。
var compactionDemoCtxWindow = 32000

func main() {
	addr := flag.String("addr", ":8080", "HTTP 监听地址")
	useMock := flag.Bool("mock", false, "使用离线 mock chat model（不调用真 LLM）")
	// iter-5-N audit follow-up: 加 -provider 跟 observability_demo 同款 + 默认 qwen
	// 跟 CLAUDE.md "测试 LLM 默认 qwen.NewTestClient (dashscope, TestModelCandidates 首位,
	// QWEN_API_KEY)" 对齐. deepseek 仍可备选.
	provider := flag.String("provider", "qwen", "真 LLM provider: qwen (默认, dashscope, 需 QWEN_API_KEY) / deepseek (需 DEEPSEEK_API_KEY); 仅 -mock=false 生效")
	modelName := flag.String("model", "", "模型名（仅 -provider deepseek 生效；qwen 走 llmtrace 固定备选链额度耗尽自动切，忽略此项）")
	flag.BoolVar(&featToolFilter, "feat-tool-filter", false, "演示 ToolFilter：chat_entry 按 mode 裁工具（normal→ask_*, plan→scenario）")
	flag.BoolVar(&featPromptTemplate, "feat-prompt-template", false, "演示 PromptTemplate：chat_entry sysPrompt 参数化模板")
	flag.BoolVar(&featToolSearch, "feat-tool-search", false, "演示 ToolSearch：scenario 重型工具 deferred，LLM 调 tool_search 揭示后才可用")
	flag.BoolVar(&featApproval, "feat-approval", false, "演示 Approval：敏感工具（ask_coder）执行前审批（复用 ask_user HITL 中断链）")
	// OpenTelemetry 导出配置：传 OTLP gRPC 地址就把 trace / 指标发给 collector，不传则不导出（本地开发）。
	serviceName := flag.String("obs-service-name", "plan_chat_web", "OTel Resource 的 service.name")
	obsTrace := flag.String("obs-trace-endpoint", "", "OTLP gRPC 地址，如 127.0.0.1:4317（空 = 不导出 trace）")
	obsMetrics := flag.String("obs-metrics-endpoint", "", "OTLP gRPC 地址，如 127.0.0.1:4317（空 = 不导出指标）")
	cwin := flag.Int("compaction-window", 32000, "AutoCompact 模拟 ModelContextWindow（mock demo 调小可看 4 层压缩事件）")
	// TTL `default_ttl` 设计:
	// session 完成后保留 N 分钟让前端能查 history, 超期由后台 sweeper 清防 OOM。
	// 默认 60min (demo 短跑友好); 0 = 永不过期 (不启 sweeper); 生产用 24h/7d 见 ISSUE-90。
	ttlMinutes := flag.Int("session-ttl-minutes", 60, "已完成 session 保留时长 (分钟); 0 = 永不过期 (不启 sweeper)")
	sweepIntervalMinutes := flag.Int("sweep-interval-minutes", 5, "TTL sweeper 周期 (分钟); 仅 -session-ttl-minutes > 0 时生效")
	reqTimeout := flag.Int("request-timeout-seconds", 0, "HTTP 请求 ctx timeout (秒); 0 = 无 timeout (依赖 client connection 控制, 推荐 — 真 LLM Plan-Execute 长跑可超 10min)")
	flag.Parse()
	requestTimeoutSeconds = *reqTimeout
	compactionDemoCtxWindow = *cwin

	envloader.Load()

	var chatModel model.ChatModel
	if *useMock {
		chatModel = newMockModel()
		log.Printf("[plan_chat_web] mock 模式：不调用外部 LLM")
	} else {
		switch *provider {
		case "qwen":
			if os.Getenv("QWEN_API_KEY") == "" {
				log.Fatal("QWEN_API_KEY 未设置 (.env 加 QWEN_API_KEY=xxx); export 后再运行, 或用 -mock 跑离线 demo, 或 -provider deepseek 切 deepseek")
			}
			chatModel, _ = llmtrace.NewQwenFallback() // qwen 备选链（候选取 qwen.TestModelCandidates），额度耗尽自动切
			log.Printf("[plan_chat_web] qwen 模式 + llmtrace fallback 备选链（额度耗尽自动切；-model 对 qwen 链不生效，deepseek 分支才用）")
		case "deepseek":
			apiKey := os.Getenv("DEEPSEEK_API_KEY")
			if apiKey == "" {
				log.Fatal("DEEPSEEK_API_KEY 未设置 (.env 加 DEEPSEEK_API_KEY=xxx); export 后再运行, 或用 -mock 跑离线 demo, 或 -provider qwen 切 qwen")
			}
			chatModel = deepseek.NewClient(apiKey, *modelName)
			log.Printf("[plan_chat_web] deepseek 模式 (model=%q)", *modelName)
		default:
			log.Fatalf("未知 -provider %q (支持 qwen / deepseek)", *provider)
		}
	}

	// 应用配 OTel SDK（导出器 / Resource），observability 用全局 provider。装配失败不挡业务：照常跑，只是不导出。
	if shutdownOTel, err := otelsetup.Setup(context.Background(), otelsetup.Config{
		ServiceName: *serviceName, TraceEndpoint: *obsTrace, MetricsEndpoint: *obsMetrics, Insecure: true,
	}); err != nil {
		log.Printf("[plan_chat_web] OTel SDK 装配失败（业务照常跑，不导出）: %v", err)
	} else {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = shutdownOTel(ctx)
		}()
		log.Printf("[plan_chat_web] OTel OK (service.name=%s, trace=%q, metrics=%q)", *serviceName, *obsTrace, *obsMetrics)
	}
	// demo 打开内容采集，在 trace 里能看到模型 / 工具的输入输出；生产按是否含个人信息决定，配 WithRedactPatterns 脱敏。
	obs := observability.New(observability.WithContentRecording(true))

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})
	mux.HandleFunc("/api/chat", makeChatHandler(chatModel, obs))
	mux.HandleFunc("/api/interrupt", interruptHandler)
	// ISSUE-71 后 /api/scenarios 已删——chat_entry 自己路由 tool，前端不需要 dropdown

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	// 启 TTL sweeper goroutine (-session-ttl-minutes > 0 时)。
	// 周期 Range runs.sync.Map, 删除 lastActivity 超期 + isFinished() 的 slot。
	// running / interrupted 状态即使久未活动也不删 (业务可能挂着等 Resume)。
	if *ttlMinutes > 0 {
		ttl := time.Duration(*ttlMinutes) * time.Minute
		interval := time.Duration(*sweepIntervalMinutes) * time.Minute
		go runSweeper(ttl, interval)
		log.Printf("[plan_chat_web] session TTL=%s, sweeper 周期=%s", ttl, interval)
	} else {
		log.Printf("[plan_chat_web] session TTL=0 永不过期 (sweeper 关闭, 长跑注意 OOM)")
	}
	log.Printf("[plan_chat_web] 启动 http://localhost%s", *addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// runSweeper 后台 goroutine: 周期清 runs 中 TTL 过期且 isFinished 的 slot。
// running / interrupted slot 即使久未活动也不删 (业务挂着等 Resume), 防误清正在
// 用的 session。
//
// 生命周期跟 process 一致 (demo 单进程不需要显式 stop)。
func runSweeper(ttl, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		cleared := 0
		runs.Range(func(k, v any) bool {
			slot := v.(*runSlot)
			if !slot.isFinished() {
				return true // 跳 running/interrupted, 业务可能挂着等 Resume
			}
			slot.mu.RLock()
			expired := now.Sub(slot.lastActivity) > ttl
			slot.mu.RUnlock()
			if expired {
				runs.Delete(k)
				cleared++
			}
			return true
		})
		if cleared > 0 {
			log.Printf("[plan_chat_web] sweeper 清 %d 过期 session", cleared)
		}
	}
}

// makeChatHandler 为每个请求构造独立的 sink + hooks（每次连接重建以绑定新 sink）；
// session_id 维度复用 sess + cp + interrupt flag（runSlot），支持 HITL 中断 + 恢复。
//
// 入参（query string 二选一）：
//   - q=...           新建会话，从初始 input 启动
//   - session_id=...  恢复已挂起会话，从 checkpoint 续跑
func makeChatHandler(chatModel model.ChatModel, obs *observability.Observability) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
		q := strings.TrimSpace(r.URL.Query().Get("q"))

		var slot *runSlot
		isResume := false
		switch {
		case sessionID != "":
			v, ok := runs.Load(sessionID)
			if !ok {
				// thread 不存在或已 sweeper 清掉
				http.Error(w, "session not found", http.StatusNotFound)
				return
			}
			slot = v.(*runSlot)
			// 终态 (success/error/cancelled) 不可 Resume — 返 410 含具体 status 让前端
			// 区分 status 字段语义。interrupted 状态可
			// Resume 走下面正常路径。
			if slot.isFinished() {
				http.Error(w, "session already completed (status="+slot.getStatus()+")", http.StatusGone)
				return
			}
			slot.touch()                // 刷 lastActivity 防 sweeper 在 Resume 路径误清
			slot.interrupt.Store(false) // 清挂起标志，避免 hook 立即再次抛
			// ISSUE-58 HITL：解析 action / text 注入到 slot，供 runOrResume 透给 Resume。
			slot.resumeAction = parseResumeAction(strings.TrimSpace(r.URL.Query().Get("action")))
			slot.resumeText = strings.TrimSpace(r.URL.Query().Get("text"))
			// Mode 切换：sticky，URL 显式带 mode 时更新；否则保持 slot 当前 mode
			if newMode := strings.TrimSpace(r.URL.Query().Get("mode")); newMode == "normal" || newMode == "plan" {
				slot.mode = newMode
			}
			isResume = true
		case q != "":
			// ISSUE-71 接口合并后 chat_entry BasicAgent 作顶层入口。Mode 由 query 决定，
			// 默认 normal；切 mode 不重置 session，sticky 持续到下次切。
			mode := strings.TrimSpace(r.URL.Query().Get("mode"))
			if mode != "plan" {
				mode = "normal"
			}
			now := time.Now()
			slot = &runSlot{
				sess:         agent_v2.NewRunSession(),
				cp:           compose_v2.NewInMemoryCheckpointer(),
				mode:         mode,
				status:       statusRunning,
				createdAt:    now,
				lastActivity: now,
			}
			runs.Store(slot.sess.RunID, slot)
		default:
			http.Error(w, "missing q (new session) or session_id (resume)", http.StatusBadRequest)
			return
		}

		sink := newEventSink()
		orchSSE := &orchAdapter{sink: sink}
		observer := &hookAdapter{sink: sink}
		intHook := &interruptHook{flag: &slot.interrupt}

		// 业务钩子（SSE 推送 + interrupt）交给 observability 包在里面：两条都要接，漏接 OrchHooks 时节点 / 工作流
		// span 与压缩、存档、扇出等框架指标都不记。别自己用 NewChainHooks 串——业务钩子开始一侧失败（比如
		// interruptHook 抛中断）时，包在里面 observability 才能当场收掉刚开的 span。
		var hooks hook.Hooks = obs.AgentHooks(observer, intHook)
		var orch agent_v2.OrchestrationHooks = obs.OrchHooks(orchSSE)

		var wg sync.WaitGroup
		wg.Add(1)
		// iter-5-N MED-4 修: 原硬编码 ctx 600s timeout 跟 scenario PlanAgent 长跑 (>10min)
		// 冲突让 cp Pending silent cancel. 改 -request-timeout-seconds 可配, 默认 0 = 无
		// timeout 依赖 client connection 控制 (业务真生产 client/CDN 自管 timeout).
		var (
			ctx    = r.Context()
			cancel = func() {}
		)
		if requestTimeoutSeconds > 0 {
			ctx, cancel = context.WithTimeout(r.Context(), time.Duration(requestTimeoutSeconds)*time.Second)
		}
		defer cancel()
		ctx = compose_v2.WithCheckpointerCtx(ctx, slot.cp)

		// 每个 HTTP 请求（Run / Resume / Abort 各一次）一个 trace：在这里开请求 span，agent 的 span 挂在
		// 它下面；同一个任务的多次请求（Run 与续跑）靠 crew.run.id 关联（session_id 就是 RunID）。trace ID
		// 推给前端，方便到后端里搜。没配 SDK 时 span 是 no-op、trace ID 为空，不推。
		ctx = compose_v2.WithSessionIDCtx(ctx, slot.sess.RunID)
		ctx, reqSpan := obs.Tracer().Start(ctx, "plan_chat_web "+r.URL.Path)
		defer reqSpan.End()
		if traceID := observability.TraceIDFromContext(ctx); traceID != "" {
			sink.send("trace_started", map[string]any{
				"session_id": slot.sess.RunID,
				"trace_id":   traceID,
			})
		}

		go func() {
			defer wg.Done()
			defer sink.close()
			runOrResume(ctx, chatModel, slot, q, isResume, sink, orch, hooks)
		}()

		writeSSE(w, r, sink)
		wg.Wait()
	}
}

// parseResumeAction 把 query 参数 action 映射到 ResumeAction 枚举（ISSUE-58）。
// 未识别值默认 Continue——保持向后兼容（老前端不传 action 时不报错）。
func parseResumeAction(s string) agent_v2.ResumeAction {
	switch s {
	case "replan":
		return agent_v2.ResumeActionReplan
	case "abort":
		return agent_v2.ResumeActionAbort
	default:
		return agent_v2.ResumeActionContinue
	}
}

// interruptHandler 触发指定 session 的挂起（ISSUE-56）。请求成功只代表 flag 已设；
// 真正抛 InterruptError + save checkpoint 在下一轮 hook 触发，前端通过 SSE
// 上的 `interrupted` 事件得到确认。
func interruptHandler(w http.ResponseWriter, r *http.Request) {
	sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
	if sessionID == "" {
		http.Error(w, "missing session_id", http.StatusBadRequest)
		return
	}
	v, ok := runs.Load(sessionID)
	if !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	slot := v.(*runSlot)
	if slot.isFinished() {
		http.Error(w, "session already completed (status="+slot.getStatus()+")", http.StatusGone)
		return
	}
	slot.interrupt.Store(true)
	w.WriteHeader(http.StatusOK)
}

// runOrResume 是 chat handler goroutine 主体（ISSUE-56）：每次连接重建 PlanAgent +
// sub-agents（绑当前 sink 的 hooks），按 isResume 走 Run / Resume，识别
// *InterruptError 推 `interrupted` 事件。完成后**不删** slot、只改 status（让前端查得到
// history），真正的回收交给 TTL sweeper——见包头 lifecycle 那段。
func runOrResume(
	ctx context.Context,
	m model.ChatModel,
	slot *runSlot,
	userInput string,
	isResume bool,
	sink *eventSink,
	orch agent_v2.OrchestrationHooks,
	hooks hook.Hooks,
) {
	ag := buildChatEntryAgent(m, hooks, sink, slot.mode)

	if isResume {
		sink.send("resume", map[string]any{"session_id": slot.sess.RunID, "mode": slot.mode})
	} else {
		sink.send("user_input", map[string]any{
			"q":          userInput,
			"session_id": slot.sess.RunID,
			"mode":       slot.mode,
		})
	}

	var (
		err    error
		events []*agent_v2.AgentEvent
	)
	if isResume {
		// ISSUE-70 Phase 2: 单值 UserInput 已删，Resume 必须用 UserInputs map 按
		// InterruptID 配对答案。slot.lastInterruptID 由上次中断时记录。
		var userInputs map[string]*schema.Message
		if slot.resumeText != "" && slot.lastInterruptID != "" {
			userInputs = map[string]*schema.Message{
				slot.lastInterruptID: schema.UserMessage(slot.resumeText),
			}
		}
		// **同一句话走两条通道，是本 demo 有意的**：它既是给那个提问的答案（寻址），
		// 也是「用户这轮说了什么」——planner 要据此调整剩余步骤（非寻址）。
		//
		// 拆分之前这半是**偶然**得到的：planner 用「取 map 里任意一条」的读法顺手
		// 捞到了它。那个读法同时也能捞到**别人的**答案，所以被删了。现在要 planner
		// 看见就得显式给——调用方本来就知道自己的意图，这是它该表的态。
		events, err = ag.Resume(ctx, slot.sess.RunID,
			agent_v2.WithResumeOrchHooks(orch),
			agent_v2.WithResumeAction(slot.resumeAction),
			agent_v2.WithResumeUserInputs(userInputs),
			agent_v2.WithResumeUserText(slot.resumeText),
			agent_v2.WithResumeSession(slot.sess), // ISSUE-72 cross-audit: 续传 sess 让续跑事件累积到 slot.sess
		)
	} else {
		events, err = ag.Run(ctx, slot.sess, schema.UserMessage(userInput), agent_v2.WithOrchHooks(orch))
	}

	if err != nil {
		var ie *compose_v2.InterruptError
		if errors.As(err, &ie) {
			// 框架已经 save 了 checkpoint 到 slot.cp[slot.sess.RunID]；slot 留在 runs
			// 里 status=interrupted 等续跑 (TTL sweeper 不清 interrupted 状态)。
			// ISSUE-70 Phase 2: 记录本次中断 ID 让后续 Resume 按 ID 配对答案
			//
			// **单 query 简化**（本 example hook-based interrupt 单一 Query）：
			// 直接读 ie.Query。**生产业务遇到 ISSUE-81 toolsFn fanout 多 tc 同时
			// 抛 Interrupt 场景必须改 for q := range ie.AllQueries() pattern**——
			// 见 doc/v2/hitl-resume.md §2.1 / §2.2 业务统一 accessor 协议。
			if ie.Query != nil && ie.Query.InterruptID != "" {
				slot.lastInterruptID = ie.Query.InterruptID
			}
			slot.setStatus(statusInterrupted)
			payload := map[string]any{
				"session_id": slot.sess.RunID,
				"status":     statusInterrupted,
				"reason":     ie.Reason,
				"data":       ie.Data,
				"checkpoint": summarizeCheckpoint(ctx, slot.cp, slot.sess.RunID),
			}
			if ie.Query != nil {
				payload["query"] = ie.Query
			}
			sink.send("interrupted", payload)
			sink.send("done", map[string]any{"interrupted": true, "session_id": slot.sess.RunID, "status": statusInterrupted})
			return
		}
		// 非 Interrupt 错 → 终态 error (sweeper TTL 到清 slot)
		slot.setStatus(statusError)
		sink.send("error", map[string]any{"message": err.Error(), "status": statusError})
		sink.send("done", map[string]any{"status": statusError})
		return
	}

	// ISSUE-58 HITL：Abort 路径——**所有 9 编排** (含 chat_entry 的 BasicAgent) 走
	// `commonResumePrep::ResumeActionAbort` 共享 helper 返 (nil, nil) + 触发
	// DeleteSubtree + EmitCpCleanupHelper (agent_v2/resume_helpers.go:62-78).
	// 显式发 aborted 事件让前端清状态，区别于"自然跑完"。终态 cancelled (sweeper 清)。
	if isResume && slot.resumeAction == agent_v2.ResumeActionAbort {
		slot.setStatus(statusCancelled)
		sink.send("aborted", map[string]any{"session_id": slot.sess.RunID, "status": statusCancelled})
		sink.send("done", map[string]any{"aborted": true, "session_id": slot.sess.RunID, "status": statusCancelled})
		return
	}

	// 跑完了——终态 success (slot 留 runs 让前端查 history; sweeper TTL 到清)
	slot.setStatus(statusSuccess)

	if msg := agent_v2.FinalMessage(events); msg != nil {
		data := map[string]any{"content": msg.Content}
		if rid, ok := msg.Annotations["run_id"].(string); ok && rid != "" {
			data["run_id"] = rid
		}
		sink.send("final", data)
	}
	sink.send("done", map[string]any{
		"history_size": len(slot.sess.History()),
		"status":       statusSuccess,
		"session_id":   slot.sess.RunID,
	})
}

// buildChatEntryAgent 构造 chat_entry BasicAgent，按 mode 装载不同 system prompt。
//
// **toolkit 始终不变**（含全部 26 tools = 18 sub-agent + 8 scenario PlanAgent），
// mode 切换只换 system prompt——LLM 靠 prompt 自觉遵守约束。这跟 Claude 的 plan
// mode / normal mode 切换设计同款。
//
// **toolkit 不变的好处**：tool_call 历史在 mode 切换后仍有效（不会成幽灵引用），
// 同对话框可 inline 对比两种 mode 效果。
//
// mode:
//   - "normal"（默认）：LLM 自由调任意 tool 完成任务（轻量 sub-agent 直调 + 重型 scenario）
//   - "plan"：LLM 优先调 8 scenario PlanAgent tool（演示 Plan-Execute 范式）
func buildChatEntryAgent(m model.ChatModel, hooks hook.Hooks, sink *eventSink, mode string) *agent_v2.BasicAgent {
	tk := tool.NewToolkit()

	// 8 个 sub-agent 直挂——Normal mode 下 LLM 调单步专家不启 PlanAgent
	for _, sa := range buildSubAgents(m, hooks) {
		tk.Register(agent_v2.AgentAsTool(sa.agent, agent_v2.WithAgentToolDescription(sa.description)))
	}

	// 8 scenario PlanAgent 包成 tool——Plan mode 下 LLM 调结构化流程
	for _, name := range scenarios.Names() {
		scn, ok := scenarios.Build(name, m, hooks, compactionExtraOpts(m)...)
		if !ok {
			continue
		}
		plannerAgent := agent_v2.NewBasicAgent(
			agent_v2.WithName("planner_"+name),
			agent_v2.WithSystemPrompt(scn.PlannerPrompt),
			agent_v2.WithModel(m),
			agent_v2.WithHooks(hooks),
			agent_v2.WithReactiveCompact(&agent_v2.ReactiveCompactConfig{Enabled: true, MaxAttempts: 3}),
		)
		pa := agent_v2.NewPlanAgent(
			newWebPlanner(plannerAgent, sink, scn.SubAgents),
			agent_v2.PlanName(name),
			agent_v2.WithSubAgents(scn.SubAgents),
			agent_v2.WithExecInputBuilder(makeWebExecInput(sink)),
		).SetExecutor(scn.Fallback)
		// scenario tool 名加 start_ 前缀区分 sub-agent 的 ask_，让 LLM 看 tool name 区分轻重
		tk.Register(agent_v2.AgentAsTool(pa, agent_v2.WithAgentToolDescription("[scenario] "+scn.Description)))
	}

	// 按 mode 装载 system prompt
	systemPrompt := chatEntrySystemPromptNormal
	if mode == "plan" {
		systemPrompt = chatEntrySystemPromptPlan
	}

	extraOpts := compactionExtraOpts(m)
	baseOpts := []agent_v2.Option{
		agent_v2.WithName("chat_entry"),
		agent_v2.WithDescription("聊天入口 agent，按 mode 装不同 system prompt 引导 LLM 调对应工具"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(hooks),
		agent_v2.WithMaxRounds(20),
	}
	// PromptTemplate（feat）：参数化 sysPrompt（注入 mode + 工具数）替代硬编码字符串。
	if featPromptTemplate {
		tmpl := prompt.FromMessages(prompt.System(
			"你是聊天入口助手（{{.Mode}} mode，当前可用工具 {{.ToolCount}} 个）。简单查询直接答；" +
				"复杂任务调对应工具（ask_* 单步专家 / scenario 重型流程）。"))
		baseOpts = append(baseOpts, agent_v2.WithPromptTemplate(tmpl,
			func(ctx context.Context, _ *schema.Message) map[string]any {
				return map[string]any{"Mode": mode, "ToolCount": len(tk.List())}
			}))
	} else {
		baseOpts = append(baseOpts, agent_v2.WithSystemPrompt(systemPrompt))
	}
	// ToolFilter（feat）：按 mode 裁工具——normal 只暴露 ask_*（单步专家），plan 只暴露 scenario
	// （非 ask_）。框架工具（ask_user/final_output/tool_search）恒豁免、不受白名单影响。
	if featToolFilter {
		baseOpts = append(baseOpts, agent_v2.WithToolFilter(
			func(ctx context.Context, _ *schema.Message) []string {
				var allow []string
				for _, t := range tk.List() {
					isSub := strings.HasPrefix(t.Name(), "ask_")
					if (mode == "plan") != isSub { // plan→非 ask_，normal→ask_
						allow = append(allow, t.Name())
					}
				}
				return allow
			}))
	}
	// ToolSearch（feat）：scenario 重型工具 deferred，chat_entry 初始只看 ask_* + tool_search，
	// 想用 scenario 先检索揭示。框架工具由 collectDeferred 自动豁免（不会被 defer）。
	if featToolSearch {
		baseOpts = append(baseOpts, agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{
			Enabled:  true,
			Deferred: func(t tool.Tool) bool { return !strings.HasPrefix(t.Name(), "ask_") },
		}))
	}
	// Approval（feat）：敏感工具 ask_coder 执行前强制审批（抛 InterruptError 走 ask_user 同款
	// cp 挂起 + Resume 链）；其他工具交回默认（Auto 放行）。演示审批不必全工具中断。
	if featApproval {
		baseOpts = append(baseOpts, agent_v2.WithApprovalHeuristic(
			func(ctx context.Context, toolName string, args map[string]any) agent_v2.ApprovalDecision {
				if toolName == "ask_coder" {
					return agent_v2.ApprovalDecisionAsk // 敏感工具强制审批（即使默认 Auto）
				}
				return agent_v2.ApprovalDecisionDefault // 其他交回默认放行
			}))
	}
	// 加载 example/orchestration/plan_chat_web/skills/ 下的 SKILL.md (chinese-poem / bug-triage /
	// api-design / deep-research). 失败 (skill 目录缺 / 解析错) silent skip — 不阻塞
	// demo 启动.
	if box, err := skill.NewDefaultSkillBox(skillsDir(),
		skill.WithSkillBoxName("plan_chat_web-skills"),
	); err == nil && len(box.ListSkills()) > 0 {
		baseOpts = append(baseOpts,
			agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
			// deep-research SKILL.md 标 context: fork + agent: deep_researcher
			// → 框架 spawn 独立 sub-agent 跑 query (cp 物理分离 + 父对话不污染).
			agent_v2.WithSkillSubAgentResolver(makeDeepResearcherResolver(m, hooks)),
		)
	}
	return agent_v2.NewBasicAgent(append(baseOpts, extraOpts...)...)
}

// makeDeepResearcherResolver 返一个 SkillSubAgentResolver: 按 fork skill 的
// `agent: <name>` frontmatter 路由到对应 sub-agent. 未知 name 返 error 让框架
// fallback 默认 BasicAgent + SKILL.md body 作 sysPrompt (默认路径).
func makeDeepResearcherResolver(m model.ChatModel, hooks hook.Hooks) func(name string) (agent_v2.Agent, error) {
	deepResearcher := agent_v2.NewBasicAgent(
		agent_v2.WithName("deep_researcher"),
		agent_v2.WithDescription("深度调研 sub-agent (fork 模式, cp 物理分离 + 父对话不污染)"),
		agent_v2.WithSystemPrompt(
			`你是深度调研专家. 收到 query 后按 4 维展开 (现状/对比/取舍/风险) 给可执行建议. `+
				`结构化 markdown + 引用具体数字/名词避免泛泛而谈.`),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(hooks),
		agent_v2.WithMaxRounds(3),
	)
	return func(name string) (agent_v2.Agent, error) {
		if name == "deep_researcher" {
			return deepResearcher, nil
		}
		return nil, fmt.Errorf("unknown sub-agent: %s", name)
	}
}

// chatEntrySystemPromptNormal 是 Normal mode 的 system prompt（默认 mode）。
// LLM 自由调任意 tool 完成任务，对简单查询直调单步专家（ask_xxx），不启重型流程。
const chatEntrySystemPromptNormal = `你是聊天入口助手（Normal mode）。

可用工具分三类（按 tool name 区分，按 tool description 选）：
- ask_xxx 系列（轻量单步专家）：翻译 / 摘要 / 写代码 / 查资料 / 审稿 / 对比 / 写结论
- start_xxx 系列（重型 Plan-Execute 流程）：完整任务流水线（写完整博客、代码 review 循环、故障诊断、预算规划等）
- activate_skill 工具（专项技能加持）：调 activate_skill({"skill":"<name>","query":"<可选>"}) 激活 skill
  研发类:
  - code-review: 代码审查 (功能/性能/可读/安全 4 维 + patch 建议)
  - sql-query: SQL 查询构造 (自然语言 → SQL + 索引建议)
  - regex-builder: 正则表达式构造 / 解读 (含拆解 + 测试 case)
  - git-commit-msg: Git commit message (Conventional Commits 格式)
  - bug-triage: bug 优先级评定（P0-P3 矩阵 + 修复顺序）
  - api-design: REST/gRPC/GraphQL API 设计（资源建模 + 错误码 + 版本演进）
  产品/运营类:
  - pr-faq: PR/FAQ 立项文档 (Amazon working backwards)
  - user-story: 用户故事 (As a / I want / So that + AC)
  - meeting-summary: 会议纪要 (议题 + 决议 + 行动项)
  数据/分析:
  - data-viz: 数据可视化推荐 (图表选型 + matplotlib/echarts 代码)
  生活/通用:
  - travel-plan: 旅行规划 (按天行程 + 餐饮住宿 + 预算)
  - learning-path: 学习路径规划 (分阶段 + 资源 + 里程碑)
  - chinese-poem: 写古风诗词（绝句/律诗/词牌，严格格律）
  专业辅助:
  - interview-prep: 面试准备 (高频题 + STAR 模板 + 公司针对性)
  深度调研 (fork 模式):
  - deep-research: 深度调研（spawn 独立 sub-agent 跑 query 返答案，cp 物理分离不污染父对话）— **必传 query 字段**

调用策略：
- 简单 / 闲聊问题 → 直接回答，不调工具
- 单步任务（翻译一句、查一个资料、审一段文字）→ 调对应 ask_xxx
- 复杂多步任务（写完整博客 / 代码 review / 故障诊断 / ...）→ 调 start_xxx
- 专项技能任务（写古诗 / bug 排优 / 设计 API）→ 调 activate_skill 拿专家 prompt 再答
- 跨多步组合任务 → 自由组合 ask_xxx + skill（如：先 ask_researcher 查 → 再 activate_skill chinese-poem 写诗）
- 工具返回结果后总结或追问，不要无脑调全部

注意：你可以一次调多个 tool（并行）也可以多轮调用。按任务实际需要决定。`

// chatEntrySystemPromptPlan 是 Plan mode 的 system prompt。
// 强制 LLM 优先选用 8 scenario PlanAgent tool，演示 Plan-Execute 范式。
const chatEntrySystemPromptPlan = `你是聊天入口助手（Plan mode）。

**当前为 Plan mode**：复杂任务一律走结构化 Plan-Execute 范式（PlanAgent 内部
plan↔exec 循环 + 多 sub-agent 协作），不要用单步 ask_xxx 工具凑合。

可用工具分两类：
- start_xxx 系列（**Plan mode 优先选这个**）：scenario PlanAgent 包装的完整流水线
- ask_xxx 系列（仅简单查询用）：翻译 / 摘要 等纯单步任务

调用策略：
- 简单 / 闲聊问题 → 直接回答
- 单步纯文本任务（翻译一句、压一段文字）→ 可以调 ask_xxx
- 一切多步 / 结构化任务 → 必须调 start_xxx（即使你觉得 ask_xxx 组合也能做）
- 工具返回结果后总结

差异说明（Plan mode vs Normal mode）：
- Normal mode 下你会自由组合 ask_xxx
- Plan mode 下你**优先**调 start_xxx，让内部 PlanAgent 接管多步规划——这是
  框架 Plan-Execute 范式 demo，让用户看到 PlanAgent 内部的 plan↔exec 循环。`

// subAgentMeta 是 sub-agent 直挂工具的元数据。
type subAgentMeta struct {
	agent       agent_v2.Agent
	description string
}

// buildSubAgents 构造 8 个 distinct sub-agent，每个独立的 BasicAgent + 4 层压缩配置。
// 这些 sub-agent 是 Normal mode 下的「单步专家工具」——LLM 直调它们做单一任务，
// 不启 scenario PlanAgent 流程。tool name 用 ask_<role> 命名让 LLM 区分轻重。
func buildSubAgents(m model.ChatModel, hooks hook.Hooks) []subAgentMeta {
	extra := compactionExtraOpts(m)
	mk := func(name, prompt, desc string, tk *tool.Toolkit, extraOpts ...agent_v2.Option) subAgentMeta {
		opts := []agent_v2.Option{
			agent_v2.WithName("ask_" + name),
			agent_v2.WithDescription(desc),
			agent_v2.WithSystemPrompt(prompt),
			agent_v2.WithModel(m),
			agent_v2.WithHooks(hooks),
		}
		if tk != nil {
			opts = append(opts, agent_v2.WithToolkit(tk))
		}
		opts = append(opts, extra...)
		opts = append(opts, extraOpts...)
		return subAgentMeta{
			agent:       agent_v2.NewBasicAgent(opts...),
			description: desc,
		}
	}

	// 「查资料」型：含 mock search 工具
	searchTool := tool.NewFuncTool(
		"search",
		"根据关键词检索资料",
		map[string]any{
			"type":       "object",
			"properties": map[string]any{"query": map[string]any{"type": "string"}},
			"required":   []string{"query"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			q, _ := args["query"].(string)
			return tool.Success(fmt.Sprintf("[mock search] %q 的要点：A/B/C", q)), nil
		},
	)
	searchTk := tool.NewToolkit()
	searchTk.Register(searchTool)

	return []subAgentMeta{
		mk("researcher",
			"你是资料调研员。收到子任务后必要时调用 search 工具，最终输出结构化的资料要点。",
			"[单步] 查资料、列事实——必要时调 search 工具",
			searchTk),
		mk("writer",
			"你是文案作者。基于已有资料要点，写出流畅的段落或教程章节。",
			"[单步] 整理资料、写流畅的段落或教程章节",
			nil),
		mk("critic",
			"你是审稿人。指出前序产出在事实、逻辑、结构上的问题，给出改进建议。",
			"[单步] 审查内容输出改进建议（独立调用，非 code_review 循环）",
			nil),
		mk("coder",
			"你是代码工程师。基于上下文生成简短可运行的示例代码并配少量注释，不要过度解释。",
			"[单步] 写代码示例——单次生成，不带循环修复",
			nil),
		mk("translator",
			"你是翻译员。把输入文本翻译为目标语言，保留术语原貌。无目标语言时默认中英互译。",
			"[单步] 中英互译——单句 / 单段直译",
			nil),
		mk("summarizer",
			"你是摘要专家。把上文压成 3-5 条 bullet，保留关键事实和决策点，去掉冗余。",
			"[单步] 摘要/提纯——把冗长资料压缩成 3-5 个关键 bullet",
			nil),
		mk("comparator",
			"你是对比分析员。把上文里的多份内容并排对照，按维度列差异和共性，输出表格或要点。",
			"[单步] 对比分析——把两份/多份内容并排对照输出差异表",
			nil),
		mk("concluder",
			"你是结论撰写者。基于已有内容给出 1-3 句话的最终结论。",
			"[单步] 写 1-3 句话最终结论（能/不能/部分能 + 关键原因）",
			nil),
		// 结构化输出 sub-agent（WithOutputSchema）——从文本抽取人物信息为 typed JSON。
		// 嵌套在 chat_entry 下（AgentAsTool）：final_output 收尾的结构化 JSON 作 tool_result
		// 传回 chat_entry（真实嵌套结构化场景）。SSE 流里可见其 final_output 工具调用。
		mk("extractor",
			"你是信息抽取专家。从用户给的文本里抽取人物的结构化信息（姓名 / 年龄 / 城市），"+
				"必须调 final_output 工具提交，不要自由文本回答。",
			"[单步][结构化] 把文本里的人物信息抽成结构化 JSON（name/age/city），供程序消费",
			nil,
			agent_v2.WithOutputSchema("person_info", map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name": map[string]any{"type": "string", "description": "姓名"},
					"age":  map[string]any{"type": "integer", "description": "年龄"},
					"city": map[string]any{"type": "string", "description": "所在城市"},
				},
				"required": []string{"name", "age", "city"},
			}),
		),
	}
}

// compactionExtraOpts 构造透给 scenarios.Build 的 4 层压缩配置（ISSUE-65~68）——
// 所有 sub-agent 启用，让 SSE 事件流里能看到压缩触发：研究员调多次 search 时
// MicroCompact 清旧 tool_result；长历史触发 AutoCompact 摘要；prompt-too-long
// 兜底走 ReactiveCompact。
func compactionExtraOpts(m model.ChatModel) []agent_v2.Option {
	microCfg := &agent_v2.MicroCompactConfig{
		Enabled:    true,
		KeepRecent: 3,
	}
	// 缩放 summary buffer 让低阈值 demo 仍能正确触发——AutoCompactConfig
	// 默认 SummaryMaxTokens=20K + SummaryBufferTokens=13K，直接用 demo 的小
	// ContextWindow（如 300）会让 threshold = 300-20K-13K 负数 → 永不触发。
	// 按 ContextWindow 比例缩放：分别取 1/5 和 1/8。
	summaryMax := compactionDemoCtxWindow / 5
	if summaryMax < 50 {
		summaryMax = 50
	}
	summaryBuf := compactionDemoCtxWindow / 8
	if summaryBuf < 30 {
		summaryBuf = 30
	}
	// 同理缩放成本守卫的下限——默认 5000 是照 ~200K 窗口定的（约 1/40），直接用会让
	// demo 的可压缩区永远低于它 ⟹ 守卫在调摘要之前早退 ⟹ **压缩静默不发生**，而 demo
	// 还在打印「触发了」。低于 1 要夹住：该字段 <= 0 会被当成「没配」取回默认 5000。
	minCompactable := compactionDemoCtxWindow / 40
	if minCompactable < 1 {
		minCompactable = 1
	}
	autoCfg := &agent_v2.AutoCompactConfig{
		Enabled:              true,
		Model:                m,
		ModelContextWindow:   compactionDemoCtxWindow,
		SummaryMaxTokens:     summaryMax,
		SummaryBufferTokens:  summaryBuf,
		MinCompactableTokens: minCompactable,
		ReinjectRecentFiles:  true,
	}
	reactiveCfg := &agent_v2.ReactiveCompactConfig{
		Enabled:     true,
		MaxAttempts: 3,
	}
	return []agent_v2.Option{
		agent_v2.WithMicroCompact(microCfg),
		agent_v2.WithAutoCompact(autoCfg),
		agent_v2.WithReactiveCompact(reactiveCfg),
	}
}

// makeScenariosHandler / scenario dropdown 已删（ISSUE-71 chat_entry 模式）——
// chat_entry BasicAgent 内部 toolkit 注册 8 scenario 包成 AgentAsTool，LLM 看
// 用户输入决定调哪个，前端无需感知 scenario。

// agentMeta 是 webPlanner 内部用的 sub-agent 元数据视图（ISSUE-72 R7）。
// 与 LLMPlanner.subAgentMeta 形态一致——这是同一种"interface contract pattern"
// 的两次实现：一次在框架（LLMPlanner），一次在 example（webPlanner）。
type agentMeta struct {
	Name        string
	Description string
}

// extractAgentMetas 从 subAgents map 抽取 metas 并字典序稳定排序.
// 排序保证 prompt 输出顺序稳定，便于断言与 LLM cache.
//
// **iter-5-N P2 修**: 原用 bubble sort O(n²), 改 sort.Slice O(n log n). 8 agent
// 实测差异微但代码意图更清.
func extractAgentMetas(subAgents map[string]agent_v2.Agent) []agentMeta {
	metas := make([]agentMeta, 0, len(subAgents))
	for name, ag := range subAgents {
		desc := ""
		if ag != nil {
			desc = ag.Description()
		}
		metas = append(metas, agentMeta{Name: name, Description: desc})
	}
	sort.Slice(metas, func(i, j int) bool {
		return metas[i].Name < metas[j].Name
	})
	return metas
}

// newWebPlanner 与 plan_chat/main.go 的 newJSONPlanner 逻辑等价，仅把打印改为
// sink.send。prevRemaining 由闭包捕获，用于计算 replan diff。
//
// ISSUE-72 R7：subAgents map 透传进来抽 Description() 注入 prompt，让 planner
// 看到"可选 agent 列表"——加 / 删专家时只动 scenario builder，无需改 PlannerPrompt。
func newWebPlanner(plannerAgent agent_v2.Agent, sink *eventSink, subAgents map[string]agent_v2.Agent) agent_v2.Planner {
	metas := extractAgentMetas(subAgents)
	var prevRemaining []agent_v2.PlanStep
	return func(ctx context.Context, sess *agent_v2.RunSession, input string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		// ISSUE-58 HITL: Resume 注入的 user 插话喂回 Planner prompt。
		// 走**非寻址通道**：planner 不在节点里、没有自己的 InterruptID，它要的是「用户这轮说了什么」。
		var resumeUserMsg string
		if text, ok := compose_v2.GetResumeUserText(ctx); ok {
			resumeUserMsg = text
		}
		prompt := buildReplanPrompt(input, executed, metas, resumeUserMsg)
		events, err := plannerAgent.Run(ctx, sess, schema.UserMessage(prompt))
		if err != nil {
			return nil, "", fmt.Errorf("planner: %w", err)
		}
		out := agent_v2.FinalMessage(events)
		if out == nil {
			return nil, "", fmt.Errorf("planner: empty output")
		}
		raw := extractJSONArray(out.Content)
		var items []struct {
			Desc  string            `json:"desc"`
			Agent string            `json:"agent"`
			Refs  []json.RawMessage `json:"refs"`
		}
		if err := json.Unmarshal([]byte(raw), &items); err != nil {
			return nil, "", fmt.Errorf("planner: parse json %q: %w", raw, err)
		}
		steps := make([]agent_v2.PlanStep, 0, len(items))
		for _, it := range items {
			// ISSUE-54：闭包不再 resolve；只把 LLM 输出 refs 塞进 RefSpec，
			// 解析在 PlanAgent.execNode 即将执行该 step 时进行。
			refs := parseRawRefs(it.Refs, sink)
			steps = append(steps, agent_v2.PlanStep{
				Description:  it.Desc,
				SubAgentName: it.Agent,
				InputRefs:    refs,
			})
		}

		data := map[string]any{
			"executed": len(executed),
			"curr":     planToWire(steps),
			"prev":     planToWire(prevRemaining),
		}
		switch {
		case len(executed) == 0:
			data["kind"] = "initial"
		case len(steps) == 0:
			data["kind"] = "done"
		default:
			data["kind"] = "replan"
		}
		sink.send("plan", data)

		prevRemaining = steps
		if len(steps) == 0 {
			return nil, "", nil
		}
		return steps, "", nil
	}
}

// makeWebExecInput 克隆 plan_chat 的 makeExecInputBuilder，trace 打印改为 sink.send。
//
// ISSUE-54：refs 已由 PlanAgent.execNode 通过 ResolveInputRefs 解析为 0-based 下标，
// 这里直接消费 resolvedRefs 即可，不再触碰 RefSpec。
func makeWebExecInput(sink *eventSink) agent_v2.ExecInputBuilder {
	return func(
		_ context.Context,
		original string,
		executed []agent_v2.ExecutedStep,
		current agent_v2.PlanStep,
		resolvedRefs []int,
	) (*schema.Message, error) {
		stepIdx := len(executed)
		target := current.SubAgentName
		if target == "" {
			target = "(default)"
		}
		sink.send("step_dispatch", map[string]any{
			"idx":   stepIdx,
			"agent": target,
			"desc":  current.Description,
			"refs":  resolvedRefs,
		})

		var b strings.Builder
		fmt.Fprintf(&b, "原始目标：%s\n\n你的任务：%s", original, current.Description)
		for _, idx := range resolvedRefs {
			if idx < 0 || idx >= len(executed) {
				continue
			}
			e := executed[idx]
			fmt.Fprintf(&b, "\n\n步骤 %d（%s）的产出：\n%s", idx+1, e.Step, e.Result)
		}
		return schema.UserMessage(b.String()), nil
	}
}

// ── 以下 3 个辅助函数与 plan_chat/main.go 内同名函数逻辑一致，独立拷贝以免包耦合 ──

func buildReplanPrompt(original string, executed []agent_v2.ExecutedStep, metas []agentMeta, resumeUserMsg string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "原始目标：%s\n\n", original)
	// ISSUE-72 R7：把可选 agent 列表（含 description）注入 prompt——避免在 system
	// prompt 里写死，加专家只动 buildPlanAgent
	if len(metas) > 0 {
		b.WriteString("可选 agent（按目标和已完成步骤选最合适的）：\n")
		for _, m := range metas {
			if m.Description == "" {
				fmt.Fprintf(&b, "  - %s\n", m.Name)
			} else {
				fmt.Fprintf(&b, "  - %s: %s\n", m.Name, m.Description)
			}
		}
		b.WriteString("\n")
	}
	if len(executed) == 0 {
		b.WriteString("已完成步骤：（无，请给出初始计划）\n\n")
	} else {
		fmt.Fprintf(&b, "已完成 %d 步（refs 中可用 0-based [N] 或 agent 名引用）：\n", len(executed))
		for i, e := range executed {
			fmt.Fprintf(&b, "  [%d] (%s) %s\n      产出摘要：%s\n", i, e.Agent, e.Step, preview(e.Result, 200))
		}
		b.WriteString("\n")
	}
	// ISSUE-58 HITL：用户中途插话（Resume Replan/Continue 注入），Planner 应据此调整剩余步骤。
	if resumeUserMsg != "" {
		fmt.Fprintf(&b, "用户补充指示（请据此调整剩余步骤）：%s\n\n", resumeUserMsg)
	}
	b.WriteString("请输出剩余步骤的 JSON 数组（若已完成请输出 []）。")
	return b.String()
}

// parseRawRefs 把 LLM 输出 refs（int 或 string）转成 []agent_v2.RefSpec。
// 不做 executed 关联校验（ISSUE-54）；解析在 PlanAgent.execNode 即将执行该 step 时进行。
func parseRawRefs(raw []json.RawMessage, sink *eventSink) []agent_v2.RefSpec {
	if len(raw) == 0 {
		return nil
	}
	out := make([]agent_v2.RefSpec, 0, len(raw))
	for i, r := range raw {
		var asInt int
		if err := json.Unmarshal(r, &asInt); err == nil {
			out = append(out, agent_v2.IndexRef(asInt))
			continue
		}
		var asStr string
		if err := json.Unmarshal(r, &asStr); err == nil {
			out = append(out, agent_v2.AgentRef(asStr))
			continue
		}
		sink.send("warn", map[string]any{
			"msg": fmt.Sprintf("refs[%d] (%s) 既不是 int 也不是 string，跳过", i, string(r)),
		})
	}
	return out
}

func extractJSONArray(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "```") {
		s = strings.TrimPrefix(s, "```json")
		s = strings.TrimPrefix(s, "```")
		if idx := strings.LastIndex(s, "```"); idx >= 0 {
			s = s[:idx]
		}
		s = strings.TrimSpace(s)
	}
	lb := strings.Index(s, "[")
	rb := strings.LastIndex(s, "]")
	if lb >= 0 && rb > lb {
		return s[lb : rb+1]
	}
	return s
}

// planToWire 把 PlanStep 压成简洁 JSON 格式（只保留 desc / agent / refs），
// 方便前端 diff 对比。
func planToWire(steps []agent_v2.PlanStep) []map[string]any {
	out := make([]map[string]any, 0, len(steps))
	for _, s := range steps {
		out = append(out, map[string]any{
			"desc":  s.Description,
			"agent": s.SubAgentName,
			"refs":  s.InputRefs,
		})
	}
	return out
}

// summarizeCheckpoint 把 Checkpointer 里指定 sessionID 的 checkpoint 提取关键字段
// 暴露给前端，方便用户在 SSE 上看到"挂起时框架到底存了啥"——便于审计 HITL 是否
// 真的有持久化、续跑能从哪个状态接力。
//
// 提取的字段：
//   - id                  checkpoint ID = sessionID
//   - created_at          UnixNano，前端格式化为时间
//   - frontier            框架将从哪个图节点续跑
//   - status              CheckpointStatus (Pending / Unset)
//   - pending_interrupts  HITL 待答 query (iter-5-N MED-5 修): 含 InterruptID +
//     Question + Options + Reason — 业务用户需要看到 "我应该答什么" + Resume 时
//     用 InterruptID 配对答案
//
// 加载失败 / metadata 缺字段时静默跳过对应字段——这是 best-effort 展示，不要因为
// 摘要构造失败而阻断 SSE 推送 interrupted 事件。
func summarizeCheckpoint(ctx context.Context, cp compose_v2.Checkpointer, sessionID string) map[string]any {
	saved, err := cp.LoadNS(ctx, sessionID, "")
	if err != nil || saved == nil {
		return nil
	}
	out := map[string]any{
		"id":         saved.ID,
		"created_at": saved.CreatedAt,
		"frontier":   saved.CurrentFrontier,
		"status":     saved.Status.String(),
	}
	// iter-5-N MED-5: 加 PendingInterrupts 摘要 — HITL 关键信息让业务看到 "等待
	// 答 Q1: <Question>". 框架 5 维稳定 InterruptID + Question + Options + Reason
	// 都暴露给前端.
	if len(saved.PendingInterrupts) > 0 {
		queries := make([]map[string]any, 0, len(saved.PendingInterrupts))
		for _, q := range saved.PendingInterrupts {
			if q == nil {
				continue
			}
			item := map[string]any{
				"interrupt_id": q.InterruptID,
				"question":     q.Question,
			}
			if len(q.Options) > 0 {
				item["options"] = q.Options
			}
			if q.Reason != "" {
				item["reason"] = q.Reason
			}
			queries = append(queries, item)
		}
		out["pending_interrupts"] = queries
	}
	return out
}
