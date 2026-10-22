package agent_v2

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"crew-go/compose_v2"
	"crew-go/internal/panicerr"
	"crew-go/model"
	"crew-go/schema"
)

// AutoCompact 自动压缩（ISSUE-67）
//
// 上下文压缩 4 层中的第 2 层——主动触发 LLM 摘要早期对话，输出 9 段结构化
// summary 替代原始消息。是 4 层中最复杂的一层，承担主要 token 控制工作。
//
// 触发：BasicAgent makeModelFn 入口（MicroCompact 之后、LLM 调用之前）；
//       估算 token >= Threshold 且断路器未跳。
// 算法：1）调用 cfg.Model 生成 summary（单次 LLM 调用），2）后处理删 <analysis>
// 提取 <summary>，3）替换 messages = [system, memoryInjected, summary,
// reinject 文件（如启用）, tail（触发后保留的最近消息）]。

// AutoCompactConfig 控制 AutoCompact 行为。
type AutoCompactConfig struct {
	// Enabled 默认 false——需显式 WithAutoCompact + 配 Model。
	Enabled bool

	// Model 摘要用 LLM。可与主 Model 不同（推荐用便宜 model，如 GPT-4o-mini / Qwen-Turbo）。
	// nil 时 AutoCompact 退化为 no-op（即使 Enabled=true）。
	Model model.ChatModel

	// ModelContextWindow 主 Model 的上下文窗口大小（tokens）。必填。
	// 阈值 = ModelContextWindow - SummaryMaxTokens - SummaryBufferTokens。
	ModelContextWindow int

	// SummaryMaxTokens 摘要输出预留 token 数。<= 0 时使用默认值 20_000。它也是摘要的上限：摘要模型写的那部分
	// 超过它判压缩无效（回退原 messages + 计入断路器）——阈值是按「摘要不超过它」留的空间。这个数写在摘要请求里；
	// 框架附在摘要后面的取回索引不算。
	SummaryMaxTokens int

	// SummaryBufferTokens 距硬上限的安全缓冲。<= 0 时使用默认值 13_000。
	SummaryBufferTokens int

	// MaxConsecutiveFailures 断路器阈值；连续失败 N 次后跳过。<= 0 时使用默认值 3。
	MaxConsecutiveFailures int

	// MinCompactableTokens 可压缩区（early）里上一份摘要之外的新内容最小 token 数；低于此值静默跳过不调 LLM。
	// <= 0 时使用默认值 5_000。增量摘要时（early 里有上一份摘要）上一份摘要不算可压缩量——它已经压缩过，
	// 见 autoCompactRun 里算 freshTokens 的那段。
	//
	// **为什么需要**：触发阈值算的是整个对话，early 却可能远小于它——典型是单个
	// 巨大当前轮（一次超大 tool_result）把总量顶过阈值，可压缩的历史其实没多少。
	// 此时摘要省不下几个 token，却要付一次完整 LLM 调用。
	//
	// 跳过**不计断路器**——"没东西可压"不是失败。残留：阈值下一轮仍会触发，但每次都
	// **不调 LLM、不烧断路器**，严格优于修复前每轮一次无效调用。
	//
	// ⚠️ **它不是「零成本」**（这里曾这么写）：早退点在压缩事件的 Before emit 与 After
	// defer **之后**，所以跳过的那些轮照样各发一对事件 ⟹ 接了 observability 的业务会看到
	// `crew.compactions{crew.outcome=success}` 每轮 +1、ratio 恒 1.0。见 `doc/code/todo.md` 同名条目。
	MinCompactableTokens int

	// MaxReductionRatio 摘要相对原文的最大占比；摘要 token > early * 此比例时判定
	// 压缩无效，回退原 messages + 计入断路器。<= 0 时使用默认值 0.8。增量摘要时（early 里有上一份摘要）
	// 上限是「上一份摘要 + 新内容 * 此比例」：上一份摘要原样保留、新内容压到这个比例以内都算有效。
	//
	// **为什么需要**：LLM 摘要可能失控——不遵循指令原样复述、甚至比原文更长。没有
	// 这层校验时框架照单全收：白花一次调用，还把上下文搞得更大，下轮更容易再次触发
	// 形成恶性循环。计入断路器同 empty summary block——底层调用成功 ≠ 业务成功。
	//
	// 设 **> 1.0** 才等于放弃此校验（恰好 1.0 时 limit == earlyTokens，判据是严格大于，
	// 所以「摘要比原文更长」——最该拦的那种——仍会被拒）。仅在业务确知摘要必然有效时使用。
	MaxReductionRatio float64

	// CustomInstructions 附加摘要指令（追加到 9 段提示词后）。
	CustomInstructions string

	// Language 提示词语言；默认 CN。
	Language AutoCompactLanguage

	// SystemPromptOverride 摘要 Call 的 system prompt 自定义；空时从原 messages 取首个 system。
	SystemPromptOverride string

	// 文件重注入（压缩后丢失文件内容时，重新塞回最近读过的若干文件）
	ReinjectRecentFiles   bool // 默认 false——业务必须显式 true 启用（reinjectRecentFiles() 直接 return 字段值）
	ReinjectMaxFiles      int  // 默认 5
	ReinjectPerFileBudget int  // 默认 5_000 tokens
	ReinjectTotalBudget   int  // 默认 50_000 tokens

	// per-session breaker registry（ISSUE-72 修跨 session 共享单例 breaker bug）
	//
	// **bug 历史**：旧版 `breaker *compose_v2.CircuitBreaker` 是 NewBasicAgent 构造期
	// 单实例，**所有 session 共享**——session A 的 quota burst trip 后 session B
	// 也被禁用 AutoCompact，跨业务隔离破坏。修复为 per-session lazy 实例。
	//
	// 设计：sync.Map[sessionID]*sessionBreakerEntry。每个 entry 含独立 breaker
	// + 独立 callMu。callMu 串行化 LLM 摘要调用避免 fan-out N 分支 thundering
	// herd（Bug U）——保护范围限本 session 内的 fan-out。
	//
	// breaker 不持久化（per-process latency 优化，不属于业务状态），隔离粒度从
	// "per-Agent 单例"改为"per-session 实例"——既避免共享污染，又不引入持久化复杂度。
	//
	// breakers 用 *sync.Map（不是值）——AutoCompactConfig 在 WithAutoCompact 内
	// 走 `copied := *cfg` 浅拷贝，*sync.Map 共享同一实例，避免 sync.Map 值拷贝
	// 的 lock 复制问题。WithAutoCompact 显式 `copied.breakers = &sync.Map{}`。
	breakers           *sync.Map // key=sessionID(string), value=*sessionBreakerEntry
	breakerMaxFailures int       // lazy 创建用
}

// sessionBreakerEntry 是 AutoCompactConfig.breakers 的 value 类型。
// 每 session 独立持有 breaker + callSem。
//
// **audit Round 6 Bug 6 修复**：callSem 用 1-buffer chan struct{} 实现 ctx-aware
// 互斥（取代旧 sync.Mutex）——sync.Mutex.Lock 不响应 ctx cancel，fan-out N 个
// worker 共享同一 cfg 时 worker 1 持锁调长 LLM（如 5min），caller ctx cancel
// 后 worker 2-N 仍卡 Lock 等到锁自然释放，goroutine 泄漏 + 延迟 cancellation。
//
// channel-based semaphore 让 acquireCallSlot 用 select { case sem<-{}; case <-ctx.Done() }
// 实现 ctx-aware acquire，cancel 时立即退出。
type sessionBreakerEntry struct {
	breaker *compose_v2.CircuitBreaker
	callSem chan struct{} // 1-buffer semaphore；acquire 用 select + ctx
}

// acquireCallSlot 用 ctx-aware 模式获取 callSem。失败返 ctx.Err()。
func (e *sessionBreakerEntry) acquireCallSlot(ctx context.Context) error {
	select {
	case e.callSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// releaseCallSlot 释放 callSem。defer 友好。
func (e *sessionBreakerEntry) releaseCallSlot() {
	<-e.callSem
}

// getOrCreateBreakerEntry 按 sessionID lazy 创建/返回 breaker entry。
//
// sessionID 为空（无 session 上下文，如裸调 AutoCompactRun 不带 ctx session）：
// 用 "" 作 key fallback——所有"无 session"调用共享一个 entry，仍然实现"有
// session 时跨 session 隔离"的核心目标。
//
// breakers nil 防御：用户白盒构造 cfg 没走 WithAutoCompact 注入时，breakers 为
// nil。返回 nil entry，shouldTrigger / AutoCompactRun 检测 nil 走无 breaker 路径
// （等同旧实现 cfg.breaker==nil）。
func (cfg *AutoCompactConfig) getOrCreateBreakerEntry(sessionID string) *sessionBreakerEntry {
	if cfg == nil || cfg.breakers == nil {
		return nil
	}
	if v, ok := cfg.breakers.Load(sessionID); ok {
		return v.(*sessionBreakerEntry)
	}
	failures := cfg.breakerMaxFailures
	if failures <= 0 {
		failures = defaultAutoCompactMaxFailures
	}
	e := &sessionBreakerEntry{
		breaker: compose_v2.NewCircuitBreaker(failures),
		callSem: make(chan struct{}, 1),
	}
	actual, _ := cfg.breakers.LoadOrStore(sessionID, e)
	return actual.(*sessionBreakerEntry)
}

// errBreakerTripped 是内部 sentinel——AutoCompact 检测到 breaker 已 trip 时
// 用此 error 在闭包内传出，外层 errors.Is 识别后做"安静跳过"（不报错）。
var errBreakerTripped = errors.New("auto compact: breaker tripped, skipping")

const (
	defaultAutoCompactSummaryMaxTokens      = 20_000
	defaultAutoCompactSummaryBufferTokens   = 13_000
	defaultAutoCompactMaxFailures           = 3
	defaultAutoCompactReinjectMaxFiles      = 5
	defaultAutoCompactReinjectPerFileBudget = 5_000
	defaultAutoCompactReinjectTotalBudget   = 50_000
	defaultAutoCompactPlaceholderRole       = "assistant"
	defaultAutoCompactMinCompactableTokens  = 5_000
	defaultAutoCompactMaxReductionRatio     = 0.8
)

// StoreInjectedAnnotation 是 buildInitialMessages 注入 store.Search 消息时打的
// marker。AutoCompact 在 early 中识别并跳过这些消息（不放入摘要 LLM 的 input），
// 避免每个 Run 都重复 summarize 同一份 StoreMemory（下个 Run buildInitialMessages
// 又会重新注入）。修 Bug V。
//
// 常量值字面量保留 "memory_injected" 是历史兼容（Memory 接口时代沿用），改了会
// 破坏跨版本读 session/cp 时的 marker 匹配——只改 godoc 不改字面量。
const StoreInjectedAnnotation = "memory_injected"

// AutoCompactSummaryAnnotation 是 AutoCompact 在 summary 消息上打的 marker。
// 用途：
//   - ReactiveCompact 的 emergencyDropOldestGroups 用此 marker 识别"这是上层
//     AutoCompact 产出的浓缩历史"，删消息时优先保留
//   - compose_v2.SelfHistoryCompacted 跨 Run 复用 session 时按此 marker 截断早期
//     已被 summary 覆盖的事件（Bug T）
//
// 别名 CompactionSummaryAnnotationKey——保持值同步，agent 与 compose 双向可见。
const AutoCompactSummaryAnnotation = CompactionSummaryAnnotationKey

// CleanupSession 是业务侧主动清理一个 sessionID 的 breaker entry —
// review-v6 M-22 fix: breakers map 永不自动清理, 长跑服务 + 高 session 流转率
// 时 distinct RunID 累积让 map 缓慢膨胀 (含 *CircuitBreaker + chan struct{}
// semaphore, 每 session 几 KB).
//
// 业务最佳实践: sweeper 扫已超时 session, 调 `cfg.CleanupSession(sid)` 主动清.
func (cfg *AutoCompactConfig) CleanupSession(sessionID string) {
	if cfg == nil || cfg.breakers == nil || sessionID == "" {
		return
	}
	cfg.breakers.Delete(sessionID)
}

// Threshold 返回触发 AutoCompact 的 token 阈值。
func (cfg *AutoCompactConfig) Threshold() int {
	smax := cfg.summaryMaxTokens()
	sbuf := cfg.SummaryBufferTokens
	if sbuf <= 0 {
		sbuf = defaultAutoCompactSummaryBufferTokens
	}
	return cfg.ModelContextWindow - smax - sbuf
}

// shouldTrigger 判定是否应该触发 AutoCompact。
//
// entry 由 caller 通过 getOrCreateBreakerEntry(sessionID) 获取并传入；nil 时
// 走无 breaker 路径（等同旧 cfg.breaker==nil 行为）。
//
// **token 估算只算非 system messages**（综合 audit H2 fix）：AutoCompactRun
// 内部 `sys = messages[:sysEnd]` 不动 system，sys 进 token 估算却不被压缩 →
// adaptive 模式注入大 SKILL.md body 进 system 时会持续超阈值反复触发摘要无效，
// 业务白付 N 倍 LLM 调用费 + 断路器最终跳。
//
// 改算策略：跳过 system 段，只算 conversation 部分 token，跟 AutoCompactRun
// 实际能压缩的范围对齐。
func (cfg *AutoCompactConfig) shouldTrigger(entry *sessionBreakerEntry, messages []*schema.Message) bool {
	if cfg == nil || !cfg.Enabled || cfg.Model == nil || cfg.ModelContextWindow <= 0 {
		return false
	}
	if entry != nil && !entry.breaker.Allow() {
		return false
	}
	threshold := cfg.Threshold()
	if threshold <= 0 {
		return false
	}
	// 跳过 leading system messages——sys 不被 AutoCompactRun 压缩，估算时算上会反复无效触发。
	//
	// ⚠️ 原先这条注释举的例子是「adaptive 注入的大 SKILL.md body」，**那个例子现在不成立**：
	// skill body 拼进 sysPrompt，而 sysPrompt 已经不在 messages 里（走
	// `BasicAgentPreambleOutput` 的 System 那条流）⟹ 它压根不进本函数的估算。
	// 顺带说明为什么当初那个例子会失效：判据是**位置**，配了 `WithPromptTemplate` 的
	// few-shot 之后 sysPrompt 就不在前导块里了，于是 120k 字符的 skill body 被算进阈值——
	// 实测同一段短对话，前导时 false、非前导时 true。
	convStart := 0
	for convStart < len(messages) && messages[convStart] != nil && messages[convStart].Role == schema.RoleSystem {
		convStart++
	}
	// ⚠️ 上面只跳过**前导**块，中段的 system 还在 messages[convStart:] 里，由下面的
	// 逐条过滤接手——它们同样不被压缩（`groupContainsProtectedMarker` 整组保护），
	// 算进阈值会让「大 system + 短对话」反复无效触发摘要。
	// **R22 audit LOW-1 + R29 视角 2 LOW-1 + offload 孤儿 fix 连带**：跳带 SkillBody /
	// StoreInjected / **MicroCompactOffloaded** marker 的 messages——
	// AutoCompactRun splitProtectedGroups 整组保留 skill body + offloaded 段不压缩 +
	// per-msg filter 抽 StoreMemory 注入不进 summary input，估算时算上会让大 skill body /
	// 大 StoreMemory / 多 offloaded 占位符的场景反复无效触发摘要 →
	// 摘要无效（这些段不变）→ 业务付 3-5 次无效 LLM 调用 → 断路器最终跳。原理同跳 sys。
	// **offloaded 必须同步跳**（孤儿 fix 连带）：offloaded 从"逐条抽进 summary"改为整组
	// 保留不压缩后，若估算仍算它，多个 offloaded 组会把 current 顶到恒超阈值反复空触发。
	convMsgs := make([]*schema.Message, 0, len(messages)-convStart)
	for _, m := range messages[convStart:] {
		if m != nil && m.Role == schema.RoleSystem {
			continue // 中段 system：受保护、不被压缩 ⟹ 不进阈值估算（同前导块）
		}
		if m != nil && m.Annotations != nil {
			if v, ok := m.Annotations[SkillBodyAnnotation].(bool); ok && v {
				continue
			}
			if v, ok := m.Annotations[StoreInjectedAnnotation].(bool); ok && v {
				continue
			}
			if v, ok := m.Annotations[MicroCompactOffloadedAnnotation].(bool); ok && v {
				continue
			}
		}
		convMsgs = append(convMsgs, m)
	}
	// 用 EstimateMessageTokens（纯字符估）而非 EstimateTokens：后者优先读 LLM 返回的
	// usage InputTokens（= 全 prompt，含上面刚过滤掉的 skill/store 段），会把"只算可压缩
	// 切片"的过滤架空——大 skill body 下 current 恒为全量、反复空触发摘要烧断路器。字符估
	// 只看传入的 convMsgs 内容，与过滤后的可压缩切片一致（eino summarization 增量也用字符估，
	// 精度足够 gate 阈值决策）。
	current := compose_v2.EstimateMessageTokens(convMsgs)
	return current >= threshold
}

// AutoCompactRun 在阈值触发时跑摘要压缩，返回压缩后 messages。
//
// 不触发 / 断路器跳 / model nil 时直接返回原 messages。压缩过程中 model 报错时
// 记录失败并返回原 messages + error（caller 决定是否继续）。
//
// **panic 防御**：cfg.Model.Generate 是用户提供（自定义 ChatModel 实现），
// 可能 panic（buggy SDK / nil deref 等）。AutoCompact 是 best-effort 优化，
// panic 不应该拖垮整个 Run——defer recover 把 panic 转成 error，breaker 计入
// 一次失败，主链路继续走原 messages。
//
// **关键不变量**：保留触发点之后的"当前 round"消息——典型场景是用户最新输入
// + 当前轮次的 tool_call/result 链（modelNode 入口跑 AutoCompact 时 gs.Messages
// 末尾可能是 user 新提问，也可能是 tool_result 等待下一轮 LLM）。把这些 summarize
// 掉会让 LLM 失去当前上下文。
//
// 算法：用 GroupMessagesByAPIRound 按 API 轮次分组，保留 **最后 1 组**作为 tail，
// 早于 tail 的所有组作为 early 走摘要。这统一处理了：
//
//   - "user → 模型应答" 单轮：tail = [user, ...] 保留
//   - "user → tool_call → tool_result" 当前轮：tail = [user, asst_tc, tool_res] 保留
//   - 单 user 历史 + 多 agent 轮（BasicAgent 典型）：每个 LLM 调用一组，
//     tail = 最近一组，early = 之前所有组——能正确触发摘要
func AutoCompactRun(ctx context.Context, cfg *AutoCompactConfig, messages []*schema.Message) (out []*schema.Message, err error) {
	return autoCompactRun(ctx, cfg, messages, nil)
}

// compactCut 是 BasicAgent 交给本轮压缩的上下文（见 compaction_memo.go）。
//
// 套用了上一次的压缩结果时，messages 的前 headLen 条是那次的输出（前导 system、长期记忆、受保护的组、
// 摘要、文件重注入），不是原始消息流——压缩对输入的几条默认假设在它上面都不成立：「最后一条 user 是用户的
// 最新提问」（摘要与重注入都是 user 角色）、文件重注入要从原始消息流倒扫。所以这里显式告诉压缩 Head 在哪：
// Head 与之后新增的部分分开分组，尾部只在新增部分里找，上一次的重注入丢掉重算。
type compactCut struct {
	headLen  int               // messages 的前 headLen 条是上一次压缩的输出；0 = 没套用
	original []*schema.Message // 套用之前、MicroCompact 之前的原始消息流——文件重注入从这里倒扫；nil = 用 messages
	cleared  map[string]bool   // MicroCompact 在 original 上会清掉的 tool_use_id（microCompactToClear）：重注入跳过它们
	tailLen  int               // 输出：真做了一次摘要时尾部原样保留的条数（输入的后缀），否则保持 -1
}

// autoCompactRun 是 [AutoCompactRun] 的本体。cut 非 nil 时按它处理上一次的压缩结果，并在**真做了一次
// 摘要**（输出里有新的摘要消息）时写 cut.tailLen；其余所有出口（没触发、跳过、失败）都不写。
func autoCompactRun(ctx context.Context, cfg *AutoCompactConfig, messages []*schema.Message, cut *compactCut) (out []*schema.Message, err error) {
	// 取 per-session breaker entry——sessionID 来自 ctx 注入的 RunSession（修
	// ISSUE-72 跨 session 共享单例 breaker bug）。无 session 上下文时用 "" 作 key。
	var sessionID string
	if sess := GetSession(ctx); sess != nil {
		sessionID = sess.RunID
	}
	entry := cfg.getOrCreateBreakerEntry(sessionID)

	// panic 防御——cfg.Model 是用户实现，buggy SDK 可能 panic
	defer func() {
		if r := recover(); r != nil {
			out = messages
			pe := panicerr.New(r)
			err = panicerr.WrapOpaque("auto compact: recovered panic", pe)
			// 调用方对这类失败只记一条 WARN 就继续用原消息，栈只能在这里记下
			panicerr.Log(ctx, slog.LevelError, "agent_v2: AutoCompact 摘要 panic，已转成错误", pe)
			if entry != nil {
				entry.breaker.RecordFailure()
			}
		}
	}()

	if !cfg.shouldTrigger(entry, messages) {
		return messages, nil
	}

	// emit Before event — 仅在 trigger 命中后 fire (不在 shouldTrigger
	// 跳过路径 fire).
	//
	// Sub-5d audit fix: 测 inputTokens + startNanos 让 After event 自带 InputTokens +
	// DurationMs (OTel self-contained — observability 端 fire metric 单 event 即得完整数据).
	var inputTokens int
	var compactStartNanos int64
	if hasEventSink(ctx) {
		inputTokens = compose_v2.EstimateTokens(messages)
		compactStartNanos = time.Now().UnixNano()
		EmitCompactionEvent(ctx, &CompactionAction{
			Layer: CompactionLayerAuto,
			Type:  CompactionActionBefore,
			Before: &CompactionBeforeAction{
				Messages:    messages,
				InputTokens: inputTokens,
				Trigger:     "threshold",
			},
		})
		// emit After event via defer — 覆盖所有退出路径 (含 panic / error / 成功).
		// 注: defer 注册在 Before fire 之后, 跟 panic recover defer LIFO 关系:
		//   After defer 后注册 → 先跑; panic recover 先注册 → 后跑.
		//   After defer 内读 named return out/err 拿到 panic recover 已经设的值.
		defer func() {
			afterAction := &CompactionAfterAction{
				Messages:     out,
				Kept:         len(out),
				Dropped:      len(messages) - len(out),
				InputTokens:  inputTokens,
				OutputTokens: compose_v2.EstimateTokens(out),
				DurationMs:   (time.Now().UnixNano() - compactStartNanos) / 1e6,
			}
			if err != nil {
				afterAction.Err = err.Error()
			}
			EmitCompactionEvent(ctx, &CompactionAction{
				Layer: CompactionLayerAuto,
				Type:  CompactionActionAfter,
				After: afterAction,
			})
		}()
	}

	// 1）隔离前导 system（不动）。messages 元素可能为 nil（防御性），nil 不算 system，扫描终止。
	//
	// ⚠️ 同 `emergencyDropOldestGroups`：**这里没有 agent 自己的 system prompt**，
	// 它走 `BasicAgentPreambleOutput` 的 System 那条流、不进本函数看得到的 messages。
	// 剩下的是长期记忆注入与调用方自己塞的 system。
	sysEnd := 0
	for sysEnd < len(messages) && messages[sysEnd] != nil && messages[sysEnd].Role == schema.RoleSystem {
		sysEnd++
	}
	sys := messages[:sysEnd]
	conv := messages[sysEnd:]
	if len(conv) == 0 {
		return messages, nil
	}

	// 2）按 API 轮次分组，定 tail 边界：
	//    - 优先：含最近 user 消息的那组及之后所有组——保留用户最新提问的完整上下文
	//    - 兜底：若 user 在第 1 组（典型 ReAct：1 user + N agent rounds），用最后 1 组作 tail
	//      这种情况下用户消息会被 summarize 进 summary（trade-off：换取压缩能力）
	var groups [][]*schema.Message
	var tailStartIdx int
	if headInConv := headLenInConv(cut, sysEnd, len(conv)); headInConv > 0 {
		// 套用了上一次的压缩结果：Head 与新增部分分开分组——合在一起分的话，Head 末尾那条摘要（user 角色）
		// 会把新增的 assistant / tool 消息吞进它的组；尾部只在新增部分里找（Head 里的摘要、重注入不是用户的
		// 最新提问）；上一次的重注入丢掉，下面从原始消息流重新倒扫。
		headGroups := dropReinjectionGroups(compose_v2.GroupMessagesByAPIRound(conv[:headInConv]))
		newGroups := compose_v2.GroupMessagesByAPIRound(conv[headInConv:])
		if len(newGroups) == 0 {
			return messages, nil
		}
		t := lastUserGroupIndex(newGroups)
		if t < 0 {
			t = len(newGroups) - 1 // 新增部分里没有用户消息（同一次 Run 的 ReAct 轮）：最后 1 组作 tail
		}
		groups = append(headGroups, newGroups...)
		tailStartIdx = len(headGroups) + t
	} else {
		groups = compose_v2.GroupMessagesByAPIRound(conv)
		if len(groups) <= 1 {
			return messages, nil
		}
		tailStartIdx = lastUserGroupIndex(groups)
		if tailStartIdx <= 0 {
			// user 在第 1 组或没有 user → 兜底用最后 1 组作 tail
			tailStartIdx = len(groups) - 1
		}
	}
	if tailStartIdx <= 0 {
		return messages, nil
	}
	// **ISSUE-76 fix**：buildInitialMessages 注入 store.Search 让 GroupMessagesByAPIRound
	// 把 StoreMemory items（无 runID）独成 group 0，UserInput 落 group 1 → lastUserGroupIndex
	// 返 1 不触发 fallback → earlyGroups=[[storeMem]] → per-msg filter 抽空 → early=[] →
	// "AutoCompact 永不调 LLM 摘要" silent 失效。
	//
	// 修法：检测 [0..tailStartIdx) 内所有 group 是否全是 protected (storeMem/skill body/
	// skill body marker) — 是则 effectively user 在第 1 个真实组，reset 走 fallback
	// 让最后 1 组作 tail，前面真实 ReAct 轮组进 early summarize。
	if headLenInConv(cut, sysEnd, len(conv)) == 0 && tailStartIdx > 0 && allProtectedGroups(groups[:tailStartIdx]) {
		tailStartIdx = len(groups) - 1
	}
	// 末尾是刚接进消息流、模型还没回应的插话 / 闸门反馈（user 消息，各自单独成组）：tail 从它们前面那一轮
	// 算起。插话说的是刚才那批工具结果（「只看 2026 年的」），闸门反馈说的是刚才那条答复（「漏了价格」）——
	// 只按「最后一条 user 所在的组」起算，那一轮就被摘要掉，模型只能对着摘要回应。退到 0 时整段都是 tail、
	// 这一轮不压（同只有一组时）。放在上面那条重置之后：先往前带的话，前面只剩受保护组时又会被它重置回最后
	// 一组（只剩插话），那一轮照样被摘要掉（审计实测）。
	for tailStartIdx > 0 && allPendingUserInput(groups[tailStartIdx]) {
		tailStartIdx--
	}
	earlyGroups := groups[:tailStartIdx]
	tailGroups := groups[tailStartIdx:]
	tail := compose_v2.FlattenGroups(tailGroups)

	// P1.C + offload 孤儿修复: 按整组保留含 SkillBodyAnnotation **或**
	// MicroCompactOffloadedAnnotation 的 group——两者都在 tool_result（RoleTool）里，
	// 单独抽 tool_result 会 orphan 配对的 assistant tool_call message（OpenAI 422）。
	// 整组保留虽然多带些 business tool_call 但保 pair 完整，是 token 经济性 vs API
	// 合法性的合理 trade-off。
	//
	// 算法：earlyGroups 分流为 protectedGroups（保留）+ summarizeGroups（送 summary input）。
	// protectedGroups 的所有消息保留在最终输出（在 memory 后、summary 前）。
	protectedGroupsMsgs, summarizeGroupsMsgs := splitProtectedGroups(earlyGroups)
	earlyRaw := summarizeGroupsMsgs

	// 过滤 memory_injected 消息——这些消息每个 Run buildInitialMessages 都会
	// 重新注入，summarize 进 summary 是浪费且会污染下次摘要质量。修 Bug V。
	// 提取出来的 memory 不进 summary input；保留在最终输出中（拼回原位置——
	// memory 在 sys 之后、其它消息之前）。
	var memoryInjected []*schema.Message
	early := make([]*schema.Message, 0, len(earlyRaw))
	for _, m := range earlyRaw {
		if m != nil && m.Annotations != nil {
			if v, ok := m.Annotations[StoreInjectedAnnotation].(bool); ok && v {
				memoryInjected = append(memoryInjected, m)
				continue
			}
			// offloaded 占位符（MicroCompactOffloadedAnnotation）**不在此逐条抽**——它是
			// RoleTool，逐条抽会把 tool_result 从其 API 轮剥离：配对 assistant tool_call
			// 留在 early 进 summary input 成悬空（摘要调用 400）+ 占位符拼回输出成孤儿
			// tool_result（主调用 422，audit 实证）。已改走 splitProtectedGroups 整组保留
			// （与 skill body 同款），earlyRaw 到不了 offloaded 消息。原「preview 不进
			// summary 防失真」目标由整组保留同样兑现。
		}
		early = append(early, m)
	}
	if len(early) == 0 {
		// 所有 early 都是 memory / protected（skill body / offloaded 整组）→
		// 无内容可压缩，但仍要拼回保留的 sys/memory/protected
		out = make([]*schema.Message, 0, len(sys)+len(memoryInjected)+len(protectedGroupsMsgs)+len(tail))
		out = append(out, sys...)
		out = append(out, memoryInjected...)
		out = append(out, protectedGroupsMsgs...)
		out = append(out, tail...)
		return out, nil
	}

	// 2.5）可压缩区太小 → 不值得调 LLM，静默跳过。
	//
	// 用 EstimateMessageTokens（纯字符估）而非 EstimateTokens：理由同 shouldTrigger——
	// 后者优先读 LLM usage InputTokens（= 全 prompt），会把"只算 early 切片"架空。
	//
	// 不计断路器——"没东西可压"不是失败，跟 errBreakerTripped 一样静默返回原
	// messages。注意**不能**走 len(early)==0 那条拼装路径：early 非空，丢弃它会
	// 真丢消息。
	//
	// 上一份摘要（增量摘要时它在 early 里：套用了备忘，或 prior 从压缩边界读起）已经是压缩过的，可压缩量与下面的
	// 缩减比都只按它之外的新内容算。按整个 early 算的话，上一份摘要越长、两次触发之间的新内容越少（触发看的是整段
	// 输入，上一份摘要也在里面），合法的增量摘要越难小于 early 的 MaxReductionRatio 倍：被反复判成无效、每次白调
	// 一次 LLM，连续几次就让断路器跳闸（实测 220 轮里摘要被调 135 次、64 次被判无效）。
	//
	// 只认最新的那一份：框架自己的路径上可压缩区里至多一份（备忘的 Head、本 agent 压缩边界的替代历史）；业务自己
	// 写进来几份时，更早的那几份算新内容——把它们合进这一次的摘要能压小。
	earlyTokens := compose_v2.EstimateMessageTokens(early)
	prevSummaryTokens := 0
	for i := len(early) - 1; i >= 0; i-- {
		if isCompactSummary(early[i]) {
			prevSummaryTokens = compose_v2.EstimateMessageTokens(early[i : i+1])
			break
		}
	}
	freshTokens := earlyTokens - prevSummaryTokens
	if freshTokens < cfg.minCompactableTokens() {
		return messages, nil
	}

	// 3）构造 Call 1 input：[system?, ...early, user(extraction prompt)]
	//
	// 没配 SystemPromptOverride 时回落「agent 自己的 system prompt」——但 sys 是
	// **一整块前导 system**，不是「agent 的 persona」，两者只在「前导块恰好只有它一条」
	// 时相等。开了长期记忆注入就不相等了：注入的记忆也是 system、也在前导块里，
	// 不配 WithSystemPrompt 时它就是 sys[0] ⟹ **用户的长期记忆被当成摘要模型的
	// system prompt 发出去**（实测：摘要 input 首条 = `system: 用户的信用卡尾号 4321…`）。
	//
	// 所以按 marker 跳过注入的记忆再取——它是这个块里唯一**标得出来**的非-persona 成员。
	// 一条都不剩时不填，摘要调用就没有 system prompt：那比发错一条强，也跟
	// 「摘要调用自带一份独立 prompt」那档做法等价。
	systemPrompt := cfg.SystemPromptOverride
	if systemPrompt == "" {
		systemPrompt = firstNonInjectedSystem(sys)
	}

	callInput := make([]*schema.Message, 0, len(early)+2)
	if systemPrompt != "" {
		callInput = append(callInput, schema.SystemMessage(systemPrompt))
	}
	callInput = append(callInput, early...)
	callInput = append(callInput, schema.UserMessage(buildExtractionPrompt(cfg)))

	// 4）调摘要 LLM——串行化保护断路器对并发场景生效（Bug U）
	// entry 由 getOrCreateBreakerEntry 返回，nil 时走无锁退化（白盒测试 / 用户
	// 直接构造 cfg 路径）。defer 保证所有早退路径都释放锁，避免遗漏。
	//
	// **audit Round 6 Bug 6 修复**：用 ctx-aware acquireCallSlot 取代 sync.Mutex.Lock，
	// caller ctx cancel 时 worker 立即退出不卡等长 LLM 完成。
	resp, err := func() (*schema.Message, error) {
		if entry != nil {
			if acqErr := entry.acquireCallSlot(ctx); acqErr != nil {
				return nil, acqErr
			}
			defer entry.releaseCallSlot()
		}
		// 重新检查 breaker——锁等待期间可能已被其它分支调用 trip
		if entry != nil && !entry.breaker.Allow() {
			return nil, errBreakerTripped // sentinel
		}
		// **R30 视角C MED-1 fix**：summarize Model 是纯压缩工具，不应读"用户 Resume
		// 答案"——业务自定义 cfg.AutoCompact.Model 实现内若调 GetResumeAnswerSingle（已删）
		// 会误命中为 sub-agent ask_user 准备的答案当成"摘要 LLM 特殊指令"。strip
		// ResumeContext 对齐 R14 audit 对 PlanAgent fork mode 隔离思路。
		summarizeCtx := compose_v2.StripResumeContext(ctx)
		r, e := cfg.Model.Generate(summarizeCtx, callInput)
		// emit Generate event — primary phase attempt=1 (AutoCompactRun
		// 内部不 retry, retry 由 caller modelFn 包围). 失败 attempt 也 emit 让业务可观测.
		if hasEventSink(ctx) {
			generateAction := &CompactionGenerateAction{
				Attempt:  1,
				Phase:    CompactionGeneratePhasePrimary,
				Response: r,
			}
			if e != nil {
				generateAction.Err = e.Error()
			}
			EmitCompactionEvent(ctx, &CompactionAction{
				Layer:    CompactionLayerAuto,
				Type:     CompactionActionGenerate,
				Generate: generateAction,
			})
		}
		// **R23 audit HIGH-1 fix（关键扩展）**：control flow error 不算 breaker
		// failure——InterruptError / ctx.Err() 是业务/用户主动控制流，AutoCompact
		// 本身没有"失败"。原计入 breaker → 3 次 Interrupt 后 breaker 跳 → AutoCompact
		// 在该 sess 内永久失效，严重时业务后续 Run 全部 silent 不压缩 → ctx 爆。
		if e != nil && entry != nil {
			if _, isInt := compose_v2.IsInterrupt(e); !isInt && ctx.Err() == nil {
				entry.breaker.RecordFailure()
			}
		}
		return r, e
	}()
	if errors.Is(err, errBreakerTripped) {
		return messages, nil // 安静跳过，行为等同 shouldTrigger 失败
	}
	if err != nil {
		// **R23 audit HIGH-1 fix**：control flow error 必须 propagate（与 R14
		// runForkMode + R5 selectSkillAdaptive 同协议）——业务自定义 cfg.Model 实现
		// 可能在 prompt 长度超限 / 内容审查不通过 / 用户取消等场景抛 InterruptError /
		// ctx.Err()。原 wrap 成 "auto compact: summarize: %w" 后 caller 用 `_` 吞掉
		// 整个 error → 框架不存 cp / 父不挂起 / Resume 找不到入口 → HITL 链断。
		// 改：先判 IsInterrupt + ctx.Err() 直接 propagate；其他业务错才 wrap。
		if _, isInt := compose_v2.IsInterrupt(err); isInt {
			return messages, err
		}
		if ctx.Err() != nil {
			return messages, err
		}
		return messages, fmt.Errorf("auto compact: summarize: %w", err)
	}
	// 防御 nil resp——某些 model 实现错误时不报 err 直接返回 nil（违反契约但实际见过）
	if resp == nil {
		if entry != nil {
			entry.breaker.RecordFailure()
		}
		return messages, errors.New("auto compact: model returned nil response")
	}

	summary := formatCompactSummary(resp.Content)
	if summary == "" {
		// 没解析到 <summary> → LLM 没遵循指令——按失败计入断路器
		// （如果连续 N 次提示词不生效，跳过避免持续浪费 quota）
		if entry != nil {
			entry.breaker.RecordFailure()
		}
		return messages, errors.New("auto compact: empty summary block")
	}

	// summary 消息打 AutoCompactSummaryAnnotation marker，让 ReactiveCompact 后续
	// emergency drop 时识别并保护——丢 summary 等于丢早期所有压缩历史。
	// 提前到此处构造：下面的缩减比校验要按**实际写回的消息**量，而非裸 summary 串。
	summaryMsg := schema.UserMessage("# Previous Conversation Summary\n\n" + summary)
	summaryMsg.Annotations = map[string]any{AutoCompactSummaryAnnotation: true}
	// 摘要模型写的那部分有多大——SummaryMaxTokens 只比它（见下面那道判定），下面附的取回索引不算。
	writtenTokens := compose_v2.EstimateMessageTokens([]*schema.Message{summaryMsg})
	// 被摘要掉的消息里带取回 id 的（转存过的工具结果、上一份摘要附的索引）：摘要不保留 id，而全文还在 store 里——
	// 在摘要后面附一份索引，模型照样取得回（见 tool_spill.go 的 spillIndex）。记在标注上，下一次增量摘要接着带。
	if idx := spillIndex(early); len(idx) > 0 {
		summaryMsg.Content += "\n\n" + spillIndexText(idx)
		summaryMsg.Annotations[spillIndexAnnotation] = strings.Join(idx, "\n")
	}

	// 摘要没比原文小够 → 压缩无效。LLM 可能不遵循指令原样复述甚至更长；照单全收
	// 会白花一次调用还把上下文搞更大，下轮更容易再触发形成恶性循环。
	// 按失败计断路器——底层 Generate 成功 ≠ 业务成功（同 empty summary block）。
	//
	// 上限是「上一份摘要 + 新内容的 MaxReductionRatio 倍」（没有上一份摘要时就是 early 的那么多倍）：上一份摘要
	// 原样保留、新内容压到这个比例以内都算有效，把新内容原样抄进去的照样被拦下。
	summaryTokens := compose_v2.EstimateMessageTokens([]*schema.Message{summaryMsg})
	if limit := prevSummaryTokens + int(float64(freshTokens)*cfg.maxReductionRatio()); summaryTokens > limit {
		if entry != nil {
			entry.breaker.RecordFailure()
		}
		return messages, fmt.Errorf("auto compact: insufficient reduction: summary %d tokens > limit %d "+
			"(previous summary %d tokens + new %d tokens * %.2f)",
			summaryTokens, limit, prevSummaryTokens, freshTokens, cfg.maxReductionRatio())
	}
	// 摘要本身放不进阈值公式给它留的预算（SummaryMaxTokens）时同样判无效：Threshold() 是按「摘要不超过它」留的
	// 空间，超过了压完照样在阈值之上、下一轮又触发。上面的比例上限没有绝对量——把上一份摘要原样保留、只追加
	// 新内容的模型会让摘要一轮轮变长，每次都按比例算有效（审计实测 600 轮里 515 次「有效」压缩、466 次压完仍在
	// 阈值之上，摘要涨到窗口的三倍）。超出预算就必须真压，压不下来按失败计、交给断路器。
	//
	// 只比摘要模型写的那部分（标题 + <summary> 块，预算写在摘要请求里，见 buildExtractionPrompt）：后面附的取回索引
	// 是框架加的、模型控制不了（最多 50 条，约 1100 token），算进去的话 SummaryMaxTokens 配得小时，可压缩区里转存过的
	// 工具结果多到一定程度，一句话的摘要也判无效、每轮判无效、断路器跳闸，这个会话的压缩就停了（审计实测）。
	if smax := cfg.summaryMaxTokens(); writtenTokens > smax {
		if entry != nil {
			entry.breaker.RecordFailure()
		}
		return messages, fmt.Errorf("auto compact: summary %d tokens exceeds SummaryMaxTokens %d", writtenTokens, smax)
	}

	// 真实 success（有效 summary + 缩减达标）才计 breaker
	if entry != nil {
		entry.breaker.RecordSuccess()
	}

	// 5）文件重注入（best-effort）
	//
	// **ISSUE-72 Bug 5 修复**：从**入参 messages 原始 snapshot** 提取，不从 early
	// 提取。理由：early 是上次 AutoCompact 跑过后的产物（user "# Recently Read
	// Files" message + summary 占位符），第二次 AutoCompact 时上次 reinject 的
	// 文件路径已被压扁进 summary，再从 early 倒扫找 Read tool_result 找不到 →
	// reinjected 为空 → 文件链断了。
	//
	// 从原始 contextMsgs 倒扫，不从压缩结果回扫——保证多轮压缩仍能找到原始文件证据。
	var reinjected []*schema.Message
	if cfg.reinjectRecentFiles() {
		// 从入参 messages 倒扫（含 sys + 全部历史）—— extractRecentFiles 内部
		// 已通过 idToPath 双 pass 扫 assistant tool_call + tool_result，
		// 重复扫 sys/已 reinject 的 user message 不会误识别（非 Read tool_call
		// 不会进 idToPath）。
		// 从原始消息流（套用备忘之前、MicroCompact 之前）倒扫：套用了上一次的结果时，更早读过的文件已在摘要里。
		// 原始流里的结果没经过 MicroCompact，它会清掉的那些（cut.cleared，同一个判据 microCompactToClear）不注回
		// 原文——跟没套用备忘时、从 MicroCompact 之后的输入倒扫看到的是同一批。
		src, skip := messages, map[string]bool(nil)
		if cut != nil && cut.original != nil {
			src, skip = cut.original, cut.cleared
		}
		files := extractRecentFiles(src, skip,
			cfg.reinjectMaxFiles(),
			cfg.reinjectPerFileBudget(),
			cfg.reinjectTotalBudget())
		if len(files) > 0 {
			reinjected = []*schema.Message{buildReinjectionMessage(files)}
		}
	}

	// 6）重组 messages = [sys, summary, reinject, ...tail]
	//    tail 保留用户当前提问 + 任何未完成的当前轮消息
	//    summaryMsg 已在上面的缩减比校验前构造（含 marker）——校验要按**实际写回的
	//    消息**量算，而不是裸 summary 串。
	out = make([]*schema.Message, 0, len(sys)+len(memoryInjected)+len(protectedGroupsMsgs)+1+len(reinjected)+len(tail))
	out = append(out, sys...)
	out = append(out, memoryInjected...)      // 拼回 memory（在 sys 之后，summary 之前）
	out = append(out, protectedGroupsMsgs...) // P1.C + offload: 拼回整组保留（skill body / offloaded 按原组间序，在 summary 前）——offloaded 占位符带着配对 assistant tool_call，LLM 可看到 [offloaded:tc_x] 决定是否 Read 重读
	out = append(out, summaryMsg)
	out = append(out, reinjected...)
	out = append(out, tail...)
	if cut != nil {
		cut.tailLen = len(tail)
	}
	return out, nil
}

// headLenInConv 把 cut.headLen 换算成 conv（去掉前导 system 之后）里的下标；没套用、或 Head 全是前导
// system 时返回 0。
func headLenInConv(cut *compactCut, sysEnd, convLen int) int {
	if cut == nil || cut.headLen <= sysEnd {
		return 0
	}
	if h := cut.headLen - sysEnd; h < convLen {
		return h
	}
	return 0
}

// dropReinjectionGroups 去掉上一次压缩的文件重注入（它在 Head 里自成一组）——这次重新从原始消息流倒扫。
func dropReinjectionGroups(groups [][]*schema.Message) [][]*schema.Message {
	out := groups[:0:0]
	for _, g := range groups {
		if len(g) == 1 && g[0] != nil && g[0].Annotations != nil {
			if v, ok := g[0].Annotations[autoCompactReinjectAnnotation].(bool); ok && v {
				continue
			}
		}
		out = append(out, g)
	}
	return out
}

// autoCompactReinjectAnnotation 标「这条是压缩后的文件重注入」——下一次压缩据它丢掉旧的、重新倒扫。
const autoCompactReinjectAnnotation = "crew_autocompact_reinject"

// splitProtectedGroups 按 group 分流 earlyGroups 为「含保护 marker 的整组保留」和
// 「不含的进 summary input」。返回两段 flatten 后的 messages（保 group 内序 + 组间
// 原相对顺序）。保护 marker 两类：
//   - SkillBodyAnnotation（P1.C）：skill body 在 tool_result 里
//   - MicroCompactOffloadedAnnotation（audit 孤儿修复）：offload 占位符也是 tool_result——
//     曾经逐条抽进独立 offloadedRefs 槽，把 RoleTool 从其 API 轮剥离：配对 assistant
//     tool_call 留在 summary input 成悬空（真实摘要调用 400）+ 占位符拼回输出成孤儿
//     tool_result（主调用 422）。整组保留同时兑现原「preview 不进 summary 防失真 +
//     占位符跨 compact 保鲜（LLM 可 Read 重读全文）」目标。
//
// **why 整组保留**：tool_result 必须有配对 assistant tool_call message——单独抽
// tool_result 会 orphan tc.ID。group 是 GroupMessagesByAPIRound 按 API 轮分组，
// 同组内 tool_call ↔ tool_result pair 完整。
//
// **副作用 trade-off**：含保护 marker 的整组会保留所有消息（业务 tool_call / 中间
// assistant 等也跟着保留）。token 上多花一些，换 pair 合法性。
func splitProtectedGroups(earlyGroups [][]*schema.Message) (protectedMsgs, summarizeMsgs []*schema.Message) {
	for _, g := range earlyGroups {
		if groupContainsProtectedMarker(g) {
			protectedMsgs = append(protectedMsgs, g...)
		} else {
			summarizeMsgs = append(summarizeMsgs, g...)
		}
	}
	return protectedMsgs, summarizeMsgs
}

// groupContainsProtectedMarker 检查组内是否含整组保留 marker（SkillBodyAnnotation /
// MicroCompactOffloadedAnnotation）的 message。
func groupContainsProtectedMarker(group []*schema.Message) bool {
	// 组里含 system ⟹ 这组带着一条指令消息，摘要掉等于把调用方的指令改写成散文。
	// 跟 `isProtectedGroup` 同一条理由，见那处 godoc（含「为什么判据写成『含』而不是
	// 『全是』」，以及那条理由在分组改成「system 单独成组」之后变成了什么）。
	if groupContainsSystem(group) {
		return true
	}
	for _, m := range group {
		if m == nil || m.Annotations == nil {
			continue
		}
		if v, ok := m.Annotations[SkillBodyAnnotation].(bool); ok && v {
			return true
		}
		if v, ok := m.Annotations[MicroCompactOffloadedAnnotation].(bool); ok && v {
			return true
		}
	}
	return false
}

// lastUserGroupIndex 返回 groups 中含最后一条 user 消息的组索引；找不到返回 -1。
// AutoCompact 用此确定 tail 边界——从含 user 的组起作为 tail（保留用户提问 +
// 之后所有 agent 轮次完整上下文）。
func lastUserGroupIndex(groups [][]*schema.Message) int {
	for i := len(groups) - 1; i >= 0; i-- {
		for _, m := range groups[i] {
			// 框架以 user 角色追加的提示（后台任务通知、重复调用提醒、inline skill 正文，见 IsFrameworkNote）不是用户说的
			// 话：它们排在用户这一轮的提问之后，按它们定尾部会把用户刚问的话连同这一轮的工具往返摘进摘要（实测）。跳过，
			// 尾部跟没有这些提示时一样。
			if m != nil && m.Role == schema.RoleUser && !IsFrameworkNote(m) {
				return i
			}
		}
	}
	return -1
}

// allPendingUserInput 报 g 是不是整组都是模型还没回应的、框架接进消息流的用户输入（运行中插话 / 闸门反馈，
// 见 isPendingUserInput）。空组不算。
func allPendingUserInput(g []*schema.Message) bool {
	n := 0
	for _, m := range g {
		if m == nil {
			continue
		}
		if !isPendingUserInput(m) {
			return false
		}
		n++
	}
	return n > 0
}

// allProtectedGroups 判 groups 内每个 group 是否全是 protected message——即每条
// message 都带 StoreInjectedAnnotation / SkillBodyAnnotation 之一。用于 ISSUE-76 fix：检测 lastUserGroupIndex 找到的"user group"前面所有
// group 都是框架元信息（mem/skill marker）时走 fallback 让 early 真实 ReAct 轮组
// 进 summary input。
//
// 空 groups 返 true（向量空满足"全是" trivially）；空 group 也算 protected
// （无可压缩内容）。
func allProtectedGroups(groups [][]*schema.Message) bool {
	for _, g := range groups {
		for _, m := range g {
			if m == nil {
				continue
			}
			if m.Annotations == nil {
				return false
			}
			isProtected := false
			if v, ok := m.Annotations[StoreInjectedAnnotation].(bool); ok && v {
				isProtected = true
			}
			if !isProtected {
				if v, ok := m.Annotations[SkillBodyAnnotation].(bool); ok && v {
					isProtected = true
				}
			}
			if !isProtected {
				return false
			}
		}
	}
	return true
}

// buildExtractionPrompt 拼前 preamble + 主体 + 摘要长度预算 + 自定义 instructions + 后 trailer。
//
// 长度预算是 SummaryMaxTokens：摘要超过它判无效（见 autoCompactRun），模型得知道这个数，不然照 9 段提示词写出的
// 摘要在它配得小时每次都超、每次白调一次。
func buildExtractionPrompt(cfg *AutoCompactConfig) string {
	preamble, trailer, mainPrompt := selectPromptByLanguage(cfg.Language)
	var sb strings.Builder
	sb.Grow(len(preamble) + len(mainPrompt) + len(trailer) + len(cfg.CustomInstructions) + 160)
	sb.WriteString(preamble)
	sb.WriteString("\n\n")
	sb.WriteString(mainPrompt)
	sb.WriteString("\n\n")
	if cfg.Language == AutoCompactLangEN {
		fmt.Fprintf(&sb, "Keep the <summary> block within about %d tokens — a longer summary is discarded and the conversation is kept as is.", cfg.summaryMaxTokens())
	} else {
		fmt.Fprintf(&sb, "<summary> 块控制在约 %d token 以内——超出时这次摘要作废、对话原样保留。", cfg.summaryMaxTokens())
	}
	if cfg.CustomInstructions != "" {
		sb.WriteString("\n\n## Compact Instructions\n")
		sb.WriteString(cfg.CustomInstructions)
	}
	sb.WriteString("\n\n")
	sb.WriteString(trailer)
	return sb.String()
}

func selectPromptByLanguage(lang AutoCompactLanguage) (preamble, trailer, mainPrompt string) {
	if lang == AutoCompactLangEN {
		return noToolsPreambleEN, NoToolsTrailerEN, MainExtractionPromptEN
	}
	return noToolsPreambleCN, NoToolsTrailerCN, MainExtractionPromptCN
}

// analysisRe 匹配 <analysis>...</analysis>（s 标志：. 匹配换行）
var analysisRe = regexp.MustCompile(`(?s)<analysis>.*?</analysis>`)

// summaryRe 匹配 <summary>...</summary> 提取内部内容
var summaryRe = regexp.MustCompile(`(?s)<summary>(.*?)</summary>`)

// formatCompactSummary 后处理 LLM 摘要输出：删 <analysis> 块，提取 <summary> 内容。
//
// 严格策略：找不到 <summary> 标签 → 返回空串（caller 当失败处理）。
// 旧策略"找不到就返回 trim 后全文"会让 LLM 不遵循格式时仍被当成功，注入
// 杂乱思考过程到 messages 当 summary——质量持续低且 breaker 不会跳。
func formatCompactSummary(raw string) string {
	out := analysisRe.ReplaceAllString(raw, "")
	if m := summaryRe.FindStringSubmatch(out); len(m) == 2 {
		return strings.TrimSpace(m[1])
	}
	return ""
}

// reinjectedFile 是从历史 tool_result 重新提取的文件内容片段。
type reinjectedFile struct {
	Path    string
	Content string
}

// extractRecentFiles 倒序扫消息找最近读过的文件——通过 Read tool 的 tool_result 识别。
//
// 规则：
//   - 倒序扫描 messages，找 role=tool 的 tool_result
//   - 通过对应 assistant 消息的 tool_call.Function.Name == "Read" 识别 Read 调用
//   - 收集最近 maxFiles 个不重复 path
//   - 单文件超 perFileBudget 截断
//   - 总 token 超 totalBudget 停止
//
// path 提取：从 assistant tool_call.Function.Arguments 解析 JSON 取 "path" 字段。
// 简化处理：直接字符串匹配 `"path":"..."`，避免引入 json 解析依赖。
//
// skip 是要跳过的 tool_use_id（MicroCompact 会清掉的那些，见 microCompactToClear）；nil 不跳。
func extractRecentFiles(messages []*schema.Message, skip map[string]bool, maxFiles, perFileBudget, totalBudget int) []reinjectedFile {
	if maxFiles <= 0 || perFileBudget <= 0 || totalBudget <= 0 {
		return nil
	}
	// 先建 tool_call_id → path 映射
	idToPath := map[string]string{}
	for _, m := range messages {
		if m == nil || m.Role != schema.RoleAssistant {
			continue
		}
		for _, tc := range schema.GetToolCalls(m) {
			if tc.Function.Name != "Read" {
				continue
			}
			if path := extractPathFromArgs(tc.Function.Arguments); path != "" {
				idToPath[tc.ID] = path
			}
		}
	}

	seen := map[string]bool{}
	var out []reinjectedFile
	totalTokens := 0
	// 倒序扫 tool_result
	for i := len(messages) - 1; i >= 0; i-- {
		m := messages[i]
		if m == nil || m.Role != schema.RoleTool {
			continue
		}
		id := schema.GetToolResultID(m)
		path, ok := idToPath[id]
		if !ok || seen[path] {
			continue
		}
		seen[path] = true
		if skip[id] {
			continue // MicroCompact 会清掉的
		}
		// 产生时就转存过的：正文是转存说明 + 预览，不是文件内容
		if sid, _ := m.Annotations[ToolResultSpillAnnotation].(string); sid != "" {
			continue
		}

		// 关键过滤：MicroCompact 已清掉/已 offload 的 tool_result 不能再注入——优先按
		// MicroCompactClearedAnnotation / MicroCompactOffloadedAnnotation marker
		// 识别（用户自定义 Placeholder 时也准）。
		// 兼容老消息无 annotation 时用占位符字符串兜底（cleared 字面量 + offloaded 前缀）。
		//
		// **HIGH-2 audit fix**：offloaded marker 跟 cleared 同款防护 — 之前 R17 audit
		// MED-2 给 SkillBody/SkillListing 做"跨层 marker 一致防护"漏给 offloaded 配套.
		// 不加 → AutoCompact reinjectRecentFiles 把 "[offloaded:tc_xxx] preview: ..."
		// 占位符当真 Read 文件内容收进 reinject, LLM 看到假文件失去 reinject 价值
		// (比 cleared 更糟 — cleared 明示"已清", offloaded preview 假装真内容).
		if m.Annotations != nil {
			if v, ok := m.Annotations[MicroCompactClearedAnnotation].(bool); ok && v {
				continue
			}
			if v, ok := m.Annotations[MicroCompactOffloadedAnnotation].(bool); ok && v {
				continue
			}
		}

		content := m.Content
		if content == "" && len(m.ContentBlocks) > 0 {
			for _, cb := range m.ContentBlocks {
				if cb != nil && cb.Toolresult != nil {
					content = cb.Toolresult.Content
					break
				}
			}
		}
		if content == defaultMicroCompactPlaceholder {
			continue
		}
		// HIGH-2 audit fix: 字符串兜底同步 — offloaded 占位符 "[offloaded:...]" 前缀
		// 兼容老消息无 annotation 时 (e.g. cp 回滚到 HIGH-2 commit 前的 message).
		if strings.HasPrefix(content, "[offloaded:") {
			continue
		}

		// per-file 截断
		approxTokens := len(content) / 4
		if approxTokens > perFileBudget {
			cutChars := perFileBudget * 4
			if cutChars > 0 && cutChars < len(content) {
				content = content[:cutChars] + "\n... [truncated for re-injection budget]"
			}
			approxTokens = perFileBudget
		}
		if totalTokens+approxTokens > totalBudget {
			break
		}
		totalTokens += approxTokens
		out = append(out, reinjectedFile{Path: path, Content: content})
		if len(out) >= maxFiles {
			break
		}
	}
	return out
}

// pathArgRe 从 `{"path":"..."}` 风格 JSON 中提取 path 字符串值。
// 简化处理：正则匹配 `"path":"..."` 字面量；不严格解析 JSON。
var pathArgRe = regexp.MustCompile(`"path"\s*:\s*"([^"]+)"`)

func extractPathFromArgs(args string) string {
	if m := pathArgRe.FindStringSubmatch(args); len(m) == 2 {
		return m[1]
	}
	return ""
}

// buildReinjectionMessage 把重注入文件拼成单条 user 消息。
func buildReinjectionMessage(files []reinjectedFile) *schema.Message {
	var sb strings.Builder
	sb.WriteString("# Recently Read Files (re-injected after compaction)\n\n")
	for _, f := range files {
		sb.WriteString("## ")
		sb.WriteString(f.Path)
		sb.WriteString("\n```\n")
		sb.WriteString(f.Content)
		sb.WriteString("\n```\n\n")
	}
	m := schema.UserMessage(sb.String())
	m.Annotations = map[string]any{autoCompactReinjectAnnotation: true}
	return m
}

// reinjectRecentFiles / reinject* 取默认值（cfg 字段未设时）。
func (cfg *AutoCompactConfig) reinjectRecentFiles() bool {
	// 任何 reinject* 字段被显式设为 0 也视作"使用默认"——简化语义：
	// 整体一个开关 ReinjectRecentFiles=true（默认 true）。
	// 由于 bool 默认值是 false，构造时即使不显式设也希望默认开启——
	// 用户必须显式 ReinjectRecentFiles: true 以启用。明确比隐式好。
	return cfg.ReinjectRecentFiles
}

func (cfg *AutoCompactConfig) reinjectMaxFiles() int {
	if cfg.ReinjectMaxFiles <= 0 {
		return defaultAutoCompactReinjectMaxFiles
	}
	return cfg.ReinjectMaxFiles
}

func (cfg *AutoCompactConfig) reinjectPerFileBudget() int {
	if cfg.ReinjectPerFileBudget <= 0 {
		return defaultAutoCompactReinjectPerFileBudget
	}
	return cfg.ReinjectPerFileBudget
}

func (cfg *AutoCompactConfig) reinjectTotalBudget() int {
	if cfg.ReinjectTotalBudget <= 0 {
		return defaultAutoCompactReinjectTotalBudget
	}
	return cfg.ReinjectTotalBudget
}

// firstNonInjectedSystem 从**前导 system 块**里挑出第一条不是长期记忆注入的，取其正文。
//
// 用途只有一个：给摘要 LLM 配 system prompt 时回落到「agent 自己的 persona」。
// 判据是 [StoreInjectedAnnotation]——注入的记忆是**用户的数据**，把它当成
// 「你是什么样的助手」发给摘要模型既跑题又外泄。
//
// 全被跳过时返空串（调用方据此不填 system prompt）。这是有意的：宁可不给，
// 也别给一条**内容是用户隐私**的。
func firstNonInjectedSystem(sys []*schema.Message) string {
	for _, m := range sys {
		if m == nil {
			continue
		}
		if v, ok := m.Annotations[StoreInjectedAnnotation].(bool); ok && v {
			continue
		}
		return m.Content
	}
	return ""
}

// summaryMaxTokens 是摘要的预算：阈值公式按「摘要不超过它」留出空间，压出来的摘要超过它就判无效。
func (cfg *AutoCompactConfig) summaryMaxTokens() int {
	if cfg.SummaryMaxTokens <= 0 {
		return defaultAutoCompactSummaryMaxTokens
	}
	return cfg.SummaryMaxTokens
}

func (cfg *AutoCompactConfig) minCompactableTokens() int {
	if cfg.MinCompactableTokens <= 0 {
		return defaultAutoCompactMinCompactableTokens
	}
	return cfg.MinCompactableTokens
}

func (cfg *AutoCompactConfig) maxReductionRatio() float64 {
	if cfg.MaxReductionRatio <= 0 {
		return defaultAutoCompactMaxReductionRatio
	}
	return cfg.MaxReductionRatio
}
