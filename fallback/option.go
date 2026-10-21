package fallback

import (
	"context"
	"time"

	"crew-go/compose_v2"
)

const (
	// defaultBreakerMaxFailures 每候选默认连续失败 3 次后熔断。
	defaultBreakerMaxFailures = 3
	// defaultBreakerCooldown 熔断后默认 5 分钟 cooldown，到期半开探测自愈。
	defaultBreakerCooldown = 5 * time.Minute
)

// config 是 New 的内部配置，经 Option 填充。
type config struct {
	policy             FailoverPolicy
	retry              PerModelRetry
	breakerMaxFailures int
	breakerCooldown    time.Duration
	onSwitch           func(ctx context.Context, fromIdx, toIdx int, err error)
}

func newConfig() *config {
	return &config{
		policy:             DefaultFailoverPolicy{},
		breakerMaxFailures: defaultBreakerMaxFailures,
		breakerCooldown:    defaultBreakerCooldown,
	}
}

// Option 配置 FallbackChatModel。
type Option func(*config)

// WithFailoverPolicy 自定义错误分类；默认 DefaultFailoverPolicy。传 nil 忽略（保留默认）。
//
// **控制流信号无需也无法分类**：InterruptError / ctx 取消由框架层 guard 在自定义
// policy 之前无条件豁免（不切换，见 guardedPolicy；在当前候选上也不重试，重试判断自己先问 IsRetryFutile）——
// 自定义 policy 只需关心"哪些真实 API 错误换候选有用"，对控制流返 true 也不会生效（防漏写豁免吞中断）。
// policy 返回 false 只表示不换候选，不表示不重试，见 [FailoverPolicy]。
func WithFailoverPolicy(p FailoverPolicy) Option {
	return func(c *config) {
		if p != nil {
			c.policy = p
		}
	}
}

// PerModelRetry 是「在切到下一个候选之前，先在当前候选上重试几次」的参数。
//
// # ⚠️ 先看这个乘积再填数
//
// 单次模型调用的最坏真实请求数 = **候选数 × (1 + MaxRetries)**。而 fallback 通常又被
// 编排层的成员重试包着（`WithXxxRetryConfig`），那一层再乘一遍：
//
//	最坏总请求 = (1 + 编排 MaxRetries) × 候选数 × (1 + PerModelRetry.MaxRetries)
//
// 实测：3 候选 + `PerModelRetry{MaxRetries: 2}` + 编排 `SimpleRetry(3)` = **27 次真实
// LLM 请求**（单 fallback 调用 9 次 × 外层 3 轮）。三个数字分别填在三个不同的地方、由
// 可能是三拨人写的代码持有，**乘起来没有任何一处会告诉你**。
//
// 断路器**压不住这个最坏值**，见 [WithBreaker] 的 godoc——它数的单位跟你以为的不一样。
//
// # 重试哪些错
//
// 只重试会自己恢复的错误：5xx、429、408、网络错误与不认识的错误（按时间窗口的配额——「每分钟请求数超了」——是限流，
// 照样重试）。计费类额度用完（账户额度 / 余额用完、免费额度用完且只允许免费额度）、其余 4xx、声明了
// [compose_v2.NonRetryable] 的错误不重试——换不换下一个候选另由 [FailoverPolicy] 判断（额度用完不重试、但会换）。
//
// # 要等很久时先推迟
//
// 服务端用 Retry-After 要求等超过 60 秒、且 [FailoverPolicy] 允许换候选时，不在这个候选上等：先推迟它、去试别的候选；
// 别的候选都没成时回来，等到它能再请求时把没用完的重试用完（每个候选的请求数仍不超过 1 + MaxRetries）。挑候选只有
// 一条规则：每次挑此刻能请求的里排序最靠前的，都要等时等最早能请求的那个。要等的时间（Retry-After 或退避）等完已经
// 到了调用方的期限时不等：这次的错误交给 [FailoverPolicy]，换下一个候选或返回。[FailoverPolicy] 不允许换、又等得完
// 时照旧等。见 tryMembers / callMemberWithRetry。
//
// # 为什么不复用 compose_v2.RetryConfig、也不带判据字段
//
// 那个类型带着一个判据字段（`ShouldRetry`）。复用它的代价实测过：业务照那个字段的文档传了谓词说「别重试」，
// 谓词**一次都没被咨询**，副作用照样重放三次，而两边都不报错。文档限定作用域只能让人别踩，拆开类型才能让人
// **踩不到**——这里的参数表里根本没有判据字段，重试哪些错是本包固定的分类（shouldRetryMember）；业务自己的
// 错误不该重试时实现 [compose_v2.NonRetryable]。
type PerModelRetry struct {
	// MaxRetries 当前候选上的最大重试次数；0 = 不重试，失败直接切下一个候选。
	MaxRetries int
	// Backoff 是重试之间的等待策略。**这一层真的需要退避**——它重试的是**同一个**
	// 候选上的瞬时故障（5xx / 网络），立刻重发大概率撞上同一个故障窗口。
	// （切换到**别的**候选那条路不等待：候选列表本身就是上界。）
	Backoff compose_v2.BackoffPolicy
}

// WithPerModelRetry 设每个候选切换前的重试策略；零值（默认）= 不重试，直接切。
// 只重试会自己恢复的错误（5xx / 网络 / 408 / 429）；计费类额度用完与其余 4xx 不重试；Retry-After 超过 60 秒时先推迟
// 这个候选、去试别的，别的都没成时回来；等完已过调用方期限的不等（见 [PerModelRetry]）。
func WithPerModelRetry(r PerModelRetry) Option {
	return func(c *config) { c.retry = r }
}

// WithBreaker 设每候选断路器参数；默认 maxFailures=3、cooldown=5min。
//
// # ⚠️ maxFailures 数的是 sweep，不是请求
//
// 一次 sweep（对该候选的一整轮尝试，含 [PerModelRetry] 的内层重试）**只记一次失败**。
// 所以跳闸前实际付出的真实请求数是 `maxFailures × (1 + PerModelRetry.MaxRetries)`。
//
// 实测：默认 `maxFailures=3` + `PerModelRetry{MaxRetries: 2}` ⟹ 该候选跳闸前发了
// **9 次**真实请求（sweep 1/2/3 各 3 次），第 4 次 sweep 起才是 0。
//
// 这个计数粒度是有意的——「这个候选不健康」的判据本来就该是「它连重试都救不回来」，
// 而不是「它失败过一次」。写在这儿是因为**两个数字乘起来才是账单**，而它们分属两个 option。
//
// maxFailures 是 **best-effort 计数阈值，非硬上限**：断路器是计数型（Allow 与
// RecordFailure 分离），并发 burst 下首个失败窗口内多个请求会在 trip 前同时通过
// Allow()，主候选 overshoot 到 ~并发度才熔断。这对结果无影响（都正确兜底切换），但
// 收费/限流主候选在该窗口会多打几次。要精确截断需 compose_v2 层断路器加 singleflight
// （独立任务）；当前接受 overshoot——并发限流是 model 层责任（对齐 ReactiveCompact 取舍）。
func WithBreaker(maxFailures int, cooldown time.Duration) Option {
	return func(c *config) {
		c.breakerMaxFailures = maxFailures
		c.breakerCooldown = cooldown
	}
}

// WithFallbackHook 注册切换观测回调——每次从一个候选换到另一个候选去请求时触发：toIdx 是接下来真正去请求的
// 那个（通常是 fromIdx+1；跳过熔断中的、或回到之前推迟的候选时不是），err 是 fromIdx 那次的失败原因。用于打日志 / 上报 metric；切换发生在 model 接口层，早于
// agent/orch hook 触发时机，故走独立回调而非塞进 hook 体系（model-fallback.md §11.4）。
//
// ⚠️ 多个并发 Generate/Stream 同时切换时本回调会被**并发调用**——回调内访问共享状态
// （如累加 slice/map）需自行加锁，框架不串行化。
func WithFallbackHook(fn func(ctx context.Context, fromIdx, toIdx int, err error)) Option {
	return func(c *config) { c.onSwitch = fn }
}
