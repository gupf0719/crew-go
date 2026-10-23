package compose_v2

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// BackoffPolicy 是**纯退避参数**——「等多久再试」，不含「要不要试」。
//
// # 为什么单独一个类型
//
// 「隔多久重试」这件事有多个使用者（编排层的成员重试、模型层的单候选重试），而
// 「这个错该不该重试」**不是**：那个判断只在编排层有意义，模型故障切换那一层用的
// 是另一个判据（「该重试」在那里恰好等于「该切换」）。
//
// 把两者放进同一个结构体的后果实测过：`fallback` 只读退避字段、不读判据，而判据
// 字段的文档写着「必填，不给就 panic」——业务照文档给了，谓词**一次都没被咨询**，
// 副作用照样重放。契约在第二个使用者身上是假的。
//
// 拆开之后那句假话无处可说：只需要退避的地方拿到的类型里**根本没有判据字段**。
type BackoffPolicy struct {
	// InitialDelay 首次重试前等待时间；0 表示立即重试（无退避）。
	InitialDelay time.Duration

	// MaxDelay 单次等待上限；0 表示不限制。
	MaxDelay time.Duration

	// Multiplier 每次重试后等待时间的倍数，建议 2.0。
	// ≤ 1.0 时退化为固定间隔（等于 InitialDelay）。
	Multiplier float64

	// Jitter 开启 ±25% 随机抖动，防 Thunder Herd（多协程同时重试集中爆发）。
	Jitter bool
}

// RetryConfig 配置重试策略（含可选指数退避）。
//
// 零值（`RetryConfig{}`）等价于 SimpleRetry(0)：只执行一次不重试。
type RetryConfig struct {
	// MaxRetries 最大重试次数；0 表示不重试（共执行 1 次）。
	MaxRetries int

	// RateLimitMaxRetries 是**限流（HTTP 429）专用**的重试上限，独立于 MaxRetries。
	// <= 0 表示不单独限制，退回用 MaxRetries。
	//
	// **为什么要独立**：限流的等待是秒到分钟级——服务端给了 `Retry-After` 时我们
	// 照它等（见 RunWithRetry），没给也是指数退避。用 5xx / 网络抖动那套毫秒级的
	// MaxRetries 去乘这个等待时长，一次限流就能把整个 Run 拖很久。
	//
	// **语义是「429 自己重试了几次」**——独立计数器，只在遇到 429 时自增，不受其他
	// 错误消耗的次数影响。两个上限同时生效（谁先到谁停）：
	//
	//	MaxRetries=5, RateLimitMaxRetries=2 遇到 503,503,429,429,429
	//	→ 第 3/4 次的 429 各消耗一次限流预算，第 5 次的 429 超限退出（总 attempt 才 4）
	//
	// 若改成「共享 attempt 比更低的上限」，上例第 3 次 429 就因 attempt 已到 2 直接
	// 放弃、限流一次都没重试——而 `503→429`（provider 过载被重试打压后转限流）恰是常见
	// 的生产退化路径。
	//
	// 设得比 MaxRetries 大时自然不生效（总预算先耗尽）。
	//
	// ⚠️ **它属于 RetryConfig 不属于 [BackoffPolicy]**：那个类型是「等多久再试」，
	// 而这是「试几次」。放错地方的后果是 `fallback.PerModelRetry.Backoff` 会跟着长出
	// 一个**静默不生效**的字段——那正是当初把两个类型拆开要消灭的形态。
	RateLimitMaxRetries int

	// BackoffPolicy 是退避参数。**嵌入**——`cfg.InitialDelay` 这类访问照旧可用。
	BackoffPolicy

	// ShouldRetry 判定「这个错该不该重试」。**MaxRetries > 0 时必须给**（不给在装配期
	// panic，见 ValidateRetryConfig）；MaxRetries == 0 时不重试，本字段无意义。
	//
	// （这句约束在**每一个**吃 RetryConfig 的地方都成立——只需要退避参数的使用者拿的是
	// [BackoffPolicy]，那个类型里根本没有本字段。曾经不是这样：`fallback` 吃整个
	// RetryConfig 却只读退避那半，于是这句「必须给」在它身上是假的，业务照着给了、
	// 谓词一次没被咨询、副作用照样重放。拆开类型之后那句假话无处可说。）
	//
	// # 为什么逼你给
	//
	// 重试的对象是**整个成员调用**——里面可能有扣款、发消息、多次模型调用。重试一次
	// 就把它们全部重放一次。所以「这个错该不该重试」这个问题，答错的代价是副作用重复。
	//
	// 而**框架答不上这个问题**：它在这一层看到的只是 `error`，分不出「网络抖了一下」
	// 和「参数非法，重跑一万次也一样」——后者在 Go 里的写法就是普通的 `fmt.Errorf`，
	// 跟前者长得一模一样。曾经的默认是「没标特殊哨兵就重试」，于是一次确定性的协议
	// 违规被跑了 4 次、副作用重放 4 次，而 err 全程只是一句普通错误。
	//
	// **配重试的那个人答得上**——他刚决定要重试自己的成员，他知道那个成员会出什么错、
	// 哪些重跑有救。这正是「守卫合法，当且仅当调用方能用它手上已有的信息满足它」。
	//
	// 想省事就用 [SimpleRetry] / [ExponentialBackoff]，它们填的是框架的保守谓词
	// [IsRetryable]——那是**显式选择接受这个默认**，而不是忘了想。
	//
	// ⚠️ **那个默认仍然偏宽，用它之前先想一下**。它排除四类：控制流信号、确定性的
	// provider HTTP 失败（400/401/403/404/422）、自己实现了 [NonRetryable] 的、终局失败。
	// 剩下的**一律重试**——而「剩下的」里就包含**你自己的确定性业务错**：一个
	// `fmt.Errorf("参数非法")` 跟一个「网络抖了一下」在框架眼里长得一模一样，它不会替
	// 你猜。你的成员要是有这类失败，就别用默认，自己给谓词。
	//
	// （那道 HTTP 闸是 2026-09-21 才加的。在那之前这里写着「确定性 4xx 也会被重试 N 次
	// 白费配额」，`fallback` 包当年正是为此**绕开**了 `RunWithRetry` 自建判据——那个
	// 理由现已失效，但它不收敛另有理由，见 `fallback/fallback.go` 的 callMemberWithRetry。）
	ShouldRetry func(error) bool
}

// ValidateRetryConfig 在**装配期**检查重试配置：要重试就得说清楚重试什么。
//
// `cfg.MaxRetries > 0` 且 `cfg.ShouldRetry == nil` 时 **panic**（配置错误当场炸，
// 与本仓其他「必填、无默认、只有业务答得上」的守卫同档，如 AddStep 的名字校验）；
// 其余情形无副作用返回。optionName 会进 panic 正文，用于指认是哪个配置出的问题。
//
// 由各编排的 `WithXxxRetryConfig` 调用——放在装配期而不是执行期，是为了让配错的人
// 在第一次 Run 之前就知道，而不是等到某个错误真的发生、副作用已经重放过一轮。
func ValidateRetryConfig(cfg RetryConfig, optionName string) {
	if cfg.MaxRetries > 0 && cfg.ShouldRetry == nil {
		panic(fmt.Sprintf("%s: 配了 MaxRetries=%d 就必须给 ShouldRetry——"+
			"重试会把整个成员调用连同它的副作用重放一遍，而框架在这一层分不出"+
			"「网络抖了一下」和「参数非法重跑也一样」（两者都是普通 error）。"+
			"想保留你现在的退避参数就加一行 cfg.ShouldRetry = compose_v2.IsRetryable；"+
			"不在意退避细节就整个换成 compose_v2.SimpleRetry(n) / ExponentialBackoff(...)",
			optionName, cfg.MaxRetries))
	}
}

// SimpleRetry 返回无退避的重试配置：失败立即重试，判据用框架的保守谓词 [IsRetryable]。
//
// 它是 [RetryConfig.ShouldRetry] 那条必填约束的省事出路——传它就等于**显式选择接受**
// 那个默认，而不是忘了想。⚠️ 那个默认排除四类：控制流信号（[IsControlFlowSignal]——它认的是
// **错误链上**有没有 ctx 错，后端自己的超时也算，见 `doc/code/todo.md` 同名条目）、终局失败
// （`ErrRunAborted` / `ErrCheckpointNotResumable`）、自己实现 [NonRetryable] 声明不重试的错、
// 以及**确定性的 provider HTTP 失败**（400/401/403/404/422，见 isDeterministicHTTPFailure）。
func SimpleRetry(n int) RetryConfig {
	return RetryConfig{MaxRetries: n, ShouldRetry: IsRetryable}
}

// ExponentialBackoff 返回带指数退避和抖动的重试配置。
//
// 典型用法：
//
//	cfg := compose_v2.ExponentialBackoff(3, 200*time.Millisecond, 10*time.Second)
func ExponentialBackoff(n int, initial, max time.Duration) RetryConfig {
	return RetryConfig{
		MaxRetries: n,
		BackoffPolicy: BackoffPolicy{
			InitialDelay: initial,
			MaxDelay:     max,
			Multiplier:   2.0,
			Jitter:       true,
		},
		ShouldRetry: IsRetryable,
	}
}

// BackoffDelay 计算第 attempt 次重试（从 1 开始）前应等待的时间。
//
// attempt == 0 不应调用此函数（首次执行不需要 delay）。
func BackoffDelay(cfg BackoffPolicy, attempt int) time.Duration {
	if cfg.InitialDelay <= 0 {
		return 0
	}
	multiplier := cfg.Multiplier
	if multiplier <= 1.0 {
		multiplier = 1.0
	}
	d := float64(cfg.InitialDelay) * math.Pow(multiplier, float64(attempt-1))
	if cfg.MaxDelay > 0 && d > float64(cfg.MaxDelay) {
		d = float64(cfg.MaxDelay)
	}
	if cfg.Jitter && d > 0 {
		jitter := d * 0.25 * (2*rand.Float64() - 1)
		d += jitter
		if d < 0 {
			d = 0
		}
	}
	return time.Duration(d)
}

// RetryAfterProvider 是**错误**可选实现的接口——携带服务端明示的重试等待时长
// （HTTP `Retry-After` 头）。
//
// **接口定义在使用方**：retry 层不 import model 包，靠此接口识别；`model.APIError`
// 实现它。业务自定义 error 类型实现同一方法即可接管退避时长。
type RetryAfterProvider interface {
	// RetryAfterDelay 返回服务端要求的等待时长；0 表示未提供。
	RetryAfterDelay() time.Duration
}

// RetryAfterFromError 沿 Unwrap 链找首个实现 [RetryAfterProvider] **且返正值**的错误，
// 未找到返 0。遍历序同 [errors.As]（深度优先前序，多错误树按 `Unwrap() []error` 的成员序）。
//
// ⚠️ **「返 0 的实现者不算数，继续往下找」是这里的要点**，所以它不能写成一次
// `errors.As` 加一个后置的 `d > 0` 判断——`errors.As` 命中第一个可赋值的错误就停了。
// 那么写有两个静默失败形态（都实测过，都 `err == nil`）：
//
//   - 业务按上面 [RetryAfterProvider] 的邀请包了一层自己的错误类型，在「我没意见」时
//     返 0 ⟹ 被它包住的 `model.APIError` 里服务端明说的 30s 再也取不到，退避悄悄退回瞎猜
//   - `errors.Join` 的多错误树上取值**随成员顺序翻**：`Join(RA=0, RA=30s)` 返 0、
//     `Join(RA=30s, RA=0)` 返 30s。而本仓确实在产多错误树（graph_loop 的超步聚合等）
//
// 这里不复用 `errors.As` 的 `As(any) bool` 定制钩子——本接口是纯方法集匹配，
// 直接类型断言即可；自己实现了 `As(any) bool` 的错误类型不在这条路的语义里。
func RetryAfterFromError(err error) time.Duration {
	for err != nil {
		if p, ok := err.(RetryAfterProvider); ok {
			if d := p.RetryAfterDelay(); d > 0 {
				return d
			}
		}
		switch x := err.(type) {
		case interface{ Unwrap() error }:
			err = x.Unwrap()
		case interface{ Unwrap() []error }:
			for _, e := range x.Unwrap() {
				if d := RetryAfterFromError(e); d > 0 {
					return d
				}
			}
			return 0
		default:
			return 0
		}
	}
	return 0
}

// HTTPStatusProvider 是**错误**可选实现的接口——携带 provider 返回的 HTTP 状态码。
//
// **接口定义在使用方**：retry 层不 import model 包（会成依赖环），靠此接口识别；
// `model.APIError` 实现它。业务自定义 error 类型实现同一方法即可参与重试分类。
//
// 约定 0 表示非 HTTP 层错误（网络中断 / 超时 / 解析失败）——那类**值得重试**。
type HTTPStatusProvider interface {
	// HTTPStatus 返回 provider 的 HTTP 状态码；0 表示非 HTTP 错误。
	HTTPStatus() int
}

// httpStatusFromError 沿 Unwrap 链找首个**非零**的 HTTP 状态码，没有则返 0。
// 遍历序同 [errors.As]（深度优先前序，多错误树按 `Unwrap() []error` 的成员序）。
//
// ⚠️ **「返 0 的实现者不算数，继续往下找」跟 [RetryAfterFromError] 同款，理由也同款**：
// 写成一次 `errors.As` 的话，业务按 [HTTPStatusProvider] 的邀请包一层自己的错误类型、
// 在「这不是 HTTP 错」时返 0，就会把被它包住的 `model.APIError` 里的 400 / 429 整个
// 遮掉——确定性失败重新被重试 N 次、限流重新跟 5xx 抢同一份预算，且全程 err == nil。
// `errors.Join` 的多错误树上还会随成员顺序翻。
//
// **只返 int 不返 (int, bool)**：两个调用点问的都是「是不是某个具体状态码」
// （`== 429` / `isDeterministicHTTPFailure`），而那两个谓词对 0 都返 false ——
// 「找到了但是 0」和「没找到」在这儿本来就是同一个答案，多一个 bool 只会让人以为
// 它们能区分。
func httpStatusFromError(err error) int {
	for err != nil {
		if p, ok := err.(HTTPStatusProvider); ok {
			if code := p.HTTPStatus(); code != 0 {
				return code
			}
		}
		switch x := err.(type) {
		case interface{ Unwrap() error }:
			err = x.Unwrap()
		case interface{ Unwrap() []error }:
			for _, e := range x.Unwrap() {
				if code := httpStatusFromError(e); code != 0 {
					return code
				}
			}
			return 0
		default:
			return 0
		}
	}
	return 0
}

// IsRateLimited 判断 err 是否限流（HTTP 429）。
//
// 限流的等待是**秒到分钟级**（还常常照服务端 `Retry-After` 指示等），跟 5xx / 网络
// 抖动那种毫秒级重试不是一回事——`RetryConfig.RateLimitMaxRetries` 据此给它更低的
// 独立重试上限，避免一次限流把整个 Run 拖很久。
func IsRateLimited(err error) bool {
	return httpStatusFromError(err) == 429
}

// SleepWithContext 在 ctx 未取消时睡眠 d 时长。
//
// ctx 取消则立即返回 ctx.Err()——retry 循环用此函数才能在 cancel 时及时退出。
func SleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// IsControlFlowSignal 判断 err 是否执行协议的控制流信号——不是失败：
//   - *InterruptError —— HITL 挂起。重试 = 白跑 N 轮后还是抛它；切换候选 = 吞掉
//     挂起信号（cp 不存、父不挂起、Resume 无入口，HITL 链断）；计失败 = 污染断路器
//   - context.Canceled / context.DeadlineExceeded —— 错误链上出现过 ctx 错
//
// ⚠️ **最后那一档回答的不是「我的 ctx 死了吗」**，是「这条错误链上有没有出现过某个
// ctx 错」。两者不是一回事：业务后端内部一个已超时的子请求（`http.Client{Timeout}` /
// `net.Dialer{Timeout}` / 它自己用 `context.WithTimeout` 造的那个 ctx）产出的错**同样匹配**，而
// caller 的 ctx 还活着。
//
// 所以本谓词只适合回答「**这个错要不要原样往上冒**」——过度包含无害，内层 ctx 超时
// 本来就该冒。要回答「**要不要再做一次**」请用 [IsRetryFutile]，它问 ctx 不问 error。
// 混用的后果实测过：给模型客户端配 `http.Client{Timeout}`（标准做法）⟹ provider 变慢
// 时 fallback **永不切换**，正是它最该发挥作用的那个场景。
//
// **它是标签，不是重试判据**："什么是控制流信号"在此定义一次，给出口的挂起判定（shouldStayPending）、span
// 状态这类「这个错是什么」的地方用。决定「要不要再做一次」的都不调它：[RunWithRetry] 的守卫与 fallback 用
// [IsRetryFutile]（问调用方 ctx），默认谓词 [IsRetryable] 只认按错误本身就说明了的那几个信号。
// ⚠️ **fallback 层也不消费它**：`fallback/policy.go` 的框架 guard 与
// `DefaultFailoverPolicy` 第 1 分类都改调了 [IsRetryFutile]，而那个函数自带一份哨兵
// 清单 + `ctx.Err()`、**不调用本函数**。所以往这里加第四个哨兵**到不了 fallback**——
// 两处都要改。
//
// 不含 ErrSoftCanceled：软取消只在超步边界由 graph_loop 抛，不出现在 model/tool
// 调用错误路径（retry/fallback 包裹的层面见不到它）；若未来出现此类路径再纳入。
func IsControlFlowSignal(err error) bool {
	if err == nil {
		return false
	}
	var ie *InterruptError
	if errors.As(err, &ie) {
		return true
	}
	// 陈旧写回：**重试是有害的**——它的语义是"另一个执行者已经推进了这个会话，你这次
	// 的账没记上"。重跑不会让账记上（对方还在那儿），只会把有副作用的节点再跑一遍。
	// 实测过后果：编排 retry 静默重跑已成功的子 agent，扣款做了两次而业务拿到 err=nil。
	if errors.Is(err, ErrCheckpointStale) {
		return true
	}
	// 软取消：跟 ctx 取消**同族**，只是优雅版（当前超步跑完再停 + 存档）。业务在
	// 另一个 goroutine 明确说了"停下来"，重跑就是不听——而且它已经存档了，重跑会从
	// 断点 Resume 再推进一个超步，每次 retry 多跑一步真实工作（LLM 调用、工具副作用），
	// 然后又撞上同一个已触发的信号。用户点停，框架反而多跑几步。
	if errors.Is(err, ErrSoftCanceled) {
		return true
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// IsRetryFutile 判断「再做一次（重试 / 换候选）**有没有可能有用**」——没有则返 true。
//
// 它跟 [IsControlFlowSignal] 的差别**只在 ctx 那一档，而那一档是承重的**：
//
//   - 三个**框架自铸的哨兵**（*InterruptError / ErrCheckpointStale / ErrSoftCanceled）
//     照旧按 error 判——它们只可能由框架产出，第三方后端伪造不出来。
//   - **ctx 那一档改成问 `ctx.Err()`**，因为 `context.Canceled` / `DeadlineExceeded`
//     是**全生态每个网络库都在产**的东西。「错误链上有 ctx 错」跟「我的 ctx 死了」是
//     两回事，而只有后者才意味着「再做一次必然立即再失败」。
//
// ⚠️ 为什么不能反过来「只看 ctx」把三个哨兵那半也删掉：**软取消不动 ctx**（它在超步
// 边界优雅停、`ctx.Err()` 恒 nil），只看 ctx 的话业务点了停、框架还会重试。
//
// 判别式（这条值得推广）：**`errors.Is(err, context.X)` 可以用来「贴标签 / 报状态 /
// 打 span」，不能用来「决定要不要再做一次」。** 前者错了只是标签不准，后者错了是功能
// 静默失效。
func IsRetryFutile(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	var ie *InterruptError
	if errors.As(err, &ie) {
		return true
	}
	if errors.Is(err, ErrCheckpointStale) || errors.Is(err, ErrSoftCanceled) {
		return true
	}
	// **问 ctx，不问 error。** ctx 为 nil 时退回 error 判据——调用方没给 ctx 就没有
	// 归属信息可用，此时宁可保守（不重试）也不要把一个真的 caller 取消当成可重试。
	if ctx == nil {
		return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
	}
	return ctx.Err() != nil
}

// IsRetryable 判断错误是否值得重试。
//
// 四类不重试（对应函数体的四道闸）：
//
//   - **按错误本身就认得出的控制流信号**——中断（要等答案）、[ErrCheckpointStale]（重跑只会再造一份副作用）、
//     [ErrSoftCanceled]（业务说了停）。**调用方的 ctx 结束了这一档不在这里判**：错误链上有
//     `context.DeadlineExceeded` / `Canceled` 不等于调用方的 ctx 死了——pgx、`http.Client{Timeout}` 这类后端自己的
//     超时也是这个形状，按错误判会让它们一律不重试（曾经如此，实测只调 1 次）。调用方的 ctx 归 [RunWithRetry] 在调
//     谓词之前用 [IsRetryFutile] 判；**自己写重试循环时也先问 `IsRetryFutile(ctx, err)`**，再问本函数
//
//   - **确定性的 provider HTTP 失败**——同样的请求重发必然同样失败，重试只是浪费
//     配额 + 拖长 Run。经 [HTTPStatusProvider] 接口按状态码判：400 参数错 / 401 鉴权
//     失败 / 403 无权限 / 404 模型不存在 / 422 请求体不合法（见 isDeterministicHTTPFailure）。
//     其余照旧重试：0（网络 / 超时 / 解析失败，接入点问题下次可能就好）/ 408 / 409 /
//     429 限流 / 5xx provider 故障 / **未知状态码保守重试**。
//
//   - **自己声明了 [NonRetryable] 的**——谁知道答案谁在自己的定义处声明（预算耗尽类
//     走这条）
//
//   - **终局失败**（ErrRunAborted）——框架已判定本次 Run 不可恢复**并清掉了 cp
//     子树**。重跑不是"再试一次"，是**从头再来一遍**：cp 没了所以 fresh run，
//     所有副作用重放，然后撞上同一个确定性的坏数据再 abort。
//
//     框架**按"重跑必然同样失败"处置**它的每一个来源（见 errors.go 的 ErrRunAborted
//     godoc）——注意这是**契约不是事实断言**：中断请求形状非法确实是代码 bug、重跑必然
//     同样非法；而 cond 的 `CondFailAbort` 只是**默认档**，它自己的 godoc 写的是"确定性
//     **居多**"，上游若调 LLM（planner / classifier）重跑很可能就好了。那种业务该显式
//     声明 `CondFailRerunFrom`——**选了默认档就等于声明"这次别重试"**，框架照此办理。
//
// 其它错误（model API 临时故障 / tool 网络错等）走 retry 路径。
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	// 只认错误本身就说明了的信号；ctx 那一档问的是调用方的 ctx，见上面 godoc
	var ie *InterruptError
	if errors.As(err, &ie) || errors.Is(err, ErrCheckpointStale) || errors.Is(err, ErrSoftCanceled) {
		return false
	}
	// provider 用标准状态码明说了「这个请求本身有问题」时，别再发一遍。
	//
	// ⚠️ 这道闸跟下面几道的**先后不影响结果**——它们全都只会返 false，没有哪一道能
	// 把另一道否掉（[NonRetryable] 只声明「别重试」，声明不了「这个请求可以重试」）。
	// 放这儿纯是为了跟上面的控制流闸挨着：两者都是「读一个明确信号」，而下面几道是
	// 「框架对自己抛的错的处置」。
	if isDeterministicHTTPFailure(httpStatusFromError(err)) {
		return false
	}

	// 终局失败**不进 IsControlFlowSignal**：那个函数回答的是"这是不是控制流信号"，
	// 而 abort 是一次失败的结局，不是信号。两个概念都能让"不该重试"成立，但混成
	// 一个会让 fallback 层跟着承担它不需要的语义（⚠️ fallback 今天消费的是
	// [IsRetryFutile] 不是本函数，这条理由是历史的；但这个区分本身仍成立）。
	//
	// `ErrCheckpointNotResumable` 同理放这儿：存档里没有可执行的东西，重跑必然拿到
	// **同一份档**。漏掉它的话，配了 retry 的编排会把一次确定性的输入问题重试到次数
	// 用满（而每次都要读一遍存储）。
	// **预算耗尽类：框架自己抛的、定义上已经做满工作的错。**
	//
	// 判据不是「重跑必然一样」——那句话字面上是假的（LLM 非确定，同样输入第二次可能
	// 更少轮次答完），一个反例就能推翻它。**正确的轴是「失败的这一次已经做下了什么」**：
	// 一次撞满 `maxVisit`（= maxRounds+1）的 ReAct 恰恰是**做下最多东西的那种失败**
	// ——N 次 LLM 调用 + 全部工具副作用。重放代价已知是最大值，而收益是「也许这次少绕
	// 几圈」。实测：配 `SimpleRetry(3)` 时一次触顶被完整跑 **4 次**。
	//
	// 两个抛出点都覆盖：ReAct 深度触顶，以及 **fan-out 宽度失控**（业务终止条件写错
	// 导致的指数爆炸——那个更该一次就停）。
	//
	// ⚠️ **框架只排除自己抛的这一类，以及 provider 用标准状态码明说了的那一类。**
	// 后者不是「猜」——401 / 400 / 404 是 HTTP 定死的语义，读它跟读一个哨兵没区别，
	// 所以上面那道 HTTP 闸合法。但**不透明的业务错框架仍然不猜**：一个
	// `fmt.Errorf("参数非法")` 跟一个「网络抖了一下」长得一模一样，实测同样被跑 4 次，
	// 而它们的重放代价框架不知道——那正是
	// [RetryConfig.ShouldRetry] 被设成**必填**、且它的 godoc 明写「那个默认确实偏宽」
	// 的原因。想收窄业务错就自己写谓词，别指望这个保守默认。
	// **一次接口断言，不是逐个哨兵。** 谁知道答案谁在自己的定义处声明——见
	// [NonRetryable] 的 godoc（那里记着这个清单是怎么被四次事故一条条逼出来的，
	// 以及为什么接口跨得过分层而哨兵跨不过）。
	var nr NonRetryable
	if errors.As(err, &nr) {
		return false
	}
	return !errors.Is(err, ErrRunAborted) && !errors.Is(err, ErrCheckpointNotResumable)
}

// isDeterministicHTTPFailure 判断状态码是否代表「重发必然同样失败」。
//
// 只列**确定性**的：请求本身有问题（参数 / 鉴权 / 目标不存在 / 体不合法），换个时间
// 重发结果一样。**不含** 408（超时，可能是瞬时）/ 409（冲突，可能已被解决）/ 429
// （限流，等一会儿就好）——那些值得重试。
//
// 未知 4xx 保守判**可重试**：宁可多试一次，也不要把一个其实瞬时的错误钉死成永久失败。
func isDeterministicHTTPFailure(code int) bool {
	switch code {
	case 400, 401, 403, 404, 422:
		return true
	default:
		return false
	}
}

// RunWithRetry 用 cfg 包装 fn 调用——失败后过了框架那道 guard、cfg.ShouldRetry 也放行时，
// 等一段时间再试。
//
// 行为：
//  1. 第 1 次直接调 fn
//  2. 失败时先过框架那道 guard：控制流信号（[IsRetryFutile]：中断 / 调用方 ctx 已死 /
//     软取消 / 陈旧写回）、终局失败（[ErrRunAborted]）、坏存档（[ErrCheckpointNotResumable]）、
//     档没写成（[ErrCheckpointNotPersisted]）→ 立即返。这道 guard 在业务谓词**之前**，谓词写成
//     什么都吞不掉它们
//  3. 再问 cfg.ShouldRetry：谓词否决 → 立即返。默认谓词 [IsRetryable]（[SimpleRetry] /
//     [ExponentialBackoff] 带的就是它）比 guard 多排除两类：确定性的 HTTP 失败、[NonRetryable]（外加
//     guard 已经管了的那几个按错误本身认的信号）。它**不按错误链上的 ctx 错判**：调用方的 ctx 死没死由
//     上面的 guard 问 ctx，后端自己的超时（pgx、`http.Client{Timeout}`）在调用方还活着时照常重试。
//     **自定义谓词要自己排除**确定性的失败，返 true 的话它们照样会被重试
//  4. 限流失败（[IsRateLimited]）另有独立计数：`RateLimitMaxRetries > 0` 且限流失败的
//     次数超过它 → 立即返
//  5. 过了上面几关且 attempt < MaxRetries → 等待后重试：服务端给了 Retry-After
//     （[RetryAfterFromError]）就照它等（不受 `MaxDelay` 约束），否则等 [BackoffDelay]
//  6. attempt == MaxRetries → 返最后一次失败的 error
//
// ⚠️ **直接调本函数是唯一绕过装配期校验的路径**：`cfg.MaxRetries > 0` 却没给
// `ShouldRetry` 时，第 3 步恒否决 ⟹ **只跑 1 次、不重试、不报错**。经编排的
// `WithXxxRetryConfig` 配置时这种组合在装配期就被 [ValidateRetryConfig] 拦下；
// 直接用本函数的话，自己记得给谓词（省事就传 [SimpleRetry] / [ExponentialBackoff]）。
//
// # 重试的是同一次调用
//
// 第 2 次尝试起：清掉「新开」信号，fn 里的嵌套调用按自己的存档续跑、只重跑失败的部分；
// 嵌套调用的次序编号（[NextSubCallIndex]）退回进入重试那一刻的值，所以输入不变的调用算出
// 跟第一次尝试相同的身份——第一次尝试里已完成的拿回记下的结果、不再执行，失败的续上第一次
// 尝试的存档。重试时换了输入的调用是另一次调用、从头跑。
//
// 所以「拿到嵌套调用的产出、校验不合格就重来」**别用本函数**：已完成的调用在每次尝试里原样
// 拿回，重试预算全部白花（实测三次尝试拿回同一份坏产出）。那是「要一份新产出」，不是「把失败
// 的部分再试一次」——在循环里调（同一份输入的第 k 次就是第 k 次调用，各自执行），或者把校验放到
// 条件边上声明 [CondFailRerunFrom]（那一档要求节点重新产出：点名为产出的记录随之作废——
// [Rerunnable]，嵌套调用上是 `agent_v2.WithRerunnable`）。
//
// fn 里直接提的问题（[Ask]、[NextAnswerSlot]）同理：提问序号也退回进入重试那一刻的值，每次尝试按
// 同样次序问同样的问题、算出同样的 ID。前一次尝试已经兑付的答案（自填 ID 的 [GetResumeAnswer] 也算），
// 后一次尝试在同一个位置照样拿到，不重问，也不被当成「落在已兑付的位上」：答案账本按尝试分层，后一次
// 尝试看不见前面几次记的兑付，本函数返回时各次尝试记下的一并记进外层。编排按 retryCfg 重试成员时，
// 成员在自己的图之外（agent 级钩子、自研 agent 的 Run 体）问的问题也是这样。
//
// 前提跟续跑一样：每次尝试在同一个位置问的是同一个问题。提问之前算出来的东西要是每次尝试都可能
// 不一样（fn 里直接调模型的产出、直接跑的嵌套图重新生成的内容），先记下来（[SaveStepResult]）再拿它提问，不然后一次尝试问的是
// 新内容、拿到的却是对旧内容的回答。续跑时节点也是从头执行到这一问，这个前提在那里早就要满足。
//
// fn 里直接跑的嵌套图（[Run]）不在此列：它在失败那次尝试里已经跑完时，存档随成功清掉，后一次尝试从头
// 再跑它——一次重生，内容可能重新生成，里面的提问换新身份（见 [Run] 里「嵌套图跑完之后节点又失败」那条），
// 用户被问到的是新内容，对旧内容的回答不会兑付给它。不想让它重跑（副作用、重问）：跑完就用 [SaveStepResult]
// 记下、后面的尝试跳过它（上行信号的处理见 [Run] 里同一条），或者经 `agent_v2.RunSubAgent` 调（已完成的调用有
// 记录，重试时直接拿回）。
//
// **手写的循环没有这些**：循环里每次调用都算新调用、从头跑，每次提问都换一个 ID 重问。
//
// 退回作用于 ctx 上的整个计数器：fn 跟自起的 goroutine 共用同一个计数器、一边重试一边并发提问时，
// 别处的序号会被一起退回（那种写法谁拿到几号取决于调度，续跑时本来就对不上）。要并发请走框架的
// fan-out，每个分支、每个 tool_call 各有自己的计数器。
//
// # 失败的尝试交不出它没做成的那部分决定
//
// 每次尝试做出的上行信号（转交给谁、走哪条分支，见 [CaptureSignals]）只在那次尝试成功时交给
// 外层；失败那次尝试里**没完成、没提交**的部分做出的随它一起作废。已经做成的那部分跟它的产出
// 一样留下：下一次尝试拿回第一次尝试里已完成的嵌套调用、续上被调方已提交的步骤时，那些决定
// 随记录 / 存档一起交出（同一次调用，见上一节）——不是「第一次尝试的一切都不算」。
//
// 泛型版本——T 为 fn 返回值类型（*schema.Message / string / 自定义 struct 等）。
//
// **InterruptError**：永不重试，第一次抛出直接返——HITL 控制流不能被吃掉。
//
// **ctx cancel**：调用方 ctx 已取消 / 超时时，fn 的失败在第 2 步直接返（[IsRetryFutile]
// 按调用方 ctx 判，不按错误链）；等待期间 ctx 取消时返 ctx 的错。
func RunWithRetry[T any](ctx context.Context, cfg RetryConfig, fn func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	// audit²³ HIGH-1 fix: nil fn 检 (协议二十三复证). 业务传 typed nil func → fn(ctx) 远 stack nil deref.
	if fn == nil {
		return zero, fmt.Errorf("RunWithRetry: nil fn")
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	// 「新开」信号与重试的关系见循环内 attempt == 1 那段注释——那行加过、回退过、
	// 又以「身份维护航」的形态第二次落地，历史别删（防第三个人再走中间那条错路）。
	retryCtx := ctx
	// rlAttempts 只数限流（429）失败——独立于总 attempt，见下方退出判定。
	rlAttempts := 0
	// 重试是**同一次调用**再做一遍：每次尝试里的嵌套调用都要从同一个编号数起（见
	// NextSubCallIndex），才会落回第一次尝试的存档槽、只重跑失败的部分。不退回的话第 2 次
	// 尝试里的调用被当成「第 1 次调用」，落到新槽、捡不到第一次尝试的失败存档，成员里
	// 已经成功的步骤连同副作用一起重放。
	callMark := markSubCalls(ctx)
	// 提问同理：每次尝试从同一个序号数起，同一个问题算出同一个 ID。只退回序号还不够——第一次尝试
	// 已经把那一位记成兑付过了，第二次尝试取同一个 ID 会被当成「已兑付」、随后的提问落在已兑付的位上，
	// 终局失败。所以兑付也按尝试分层：后一次尝试看不见前面几次记的，收尾时一并记进外层（见 retryLedger）。
	// 不退回的话第二次尝试的提问换了 ID、用户被重问；累积传答案时，重问拿到的回答在下一次续跑里落到
	// 后面那一问的位置上，后一问被前一问的答案静默满足（实测）。
	askMark := markAskSlots(ctx)
	// 不重试时只有一次尝试，不分层：兑付直接记进外层，跟没有重试这一层时一样
	var ledger *retryLedger
	if cfg.MaxRetries > 0 {
		ledger = newRetryLedger(ctx)
	}
	defer ledger.settle()
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if attempt >= 1 {
			callMark.rewind()
			askMark.rewind()
		}
		if attempt == 1 {
			// 第 2 次尝试起清「新开」信号：重试不是新任务，成员该按自己的存档续跑、
			// 只重跑失败的部分（实测不清时成员内部已成功那步连副作用一起重放）。
			//
			// ⚠️ **这行第一次加的时候引发过跨任务泄漏、回退过**（见上面那段注释的旧文）：
			// 上个被放弃任务的挂起档会被本任务的第 2 次尝试捡走。现在 auto-load 闸有了
			// **身份维**（[Checkpoint.BelongsToTask]——顶层任务代号沿 ctx 传、档上带
			// TaskID，不等不捡），跨任务残留被身份挡住，清信号只放行**本任务**的进度。
			// 哨兵两头钉：TestRetry_CrossTaskIsolationHolds（隔离）+
			// TestRetry_Granularity（不重放）。
			retryCtx = context.WithValue(ctx, freshRunCtxKey{}, false)
		}
		// 每次尝试是一段独立的执行：它做出的上行信号（转交 / 路由这类交给上方的决定，见
		// signal.go）只在它成功时交给外层，失败那次尝试里没做成的部分做出的随它作废——不然第一次
		// 尝试转交了之后失败、第二次成功且不转交，结果照样转走了（实测）。第一次尝试里已完成的
		// 嵌套调用在下一次尝试拿回时，它的决定随记录一起再交出。
		var out T
		sigs, err := CaptureSignals(ledger.begin(retryCtx), func(c context.Context) error {
			var e error
			out, e = fn(c)
			return e
		})
		if err == nil {
			EmitSignals(ctx, sigs)
			return out, nil
		}
		// **控制流信号与终局失败恒不重试**——它们不是「失败了再试试」，中断要等答案、
		// abort 的档已经清了。这道 guard 在业务谓词**之前**，业务写错谓词也吞不掉中断。
		//
		// **用 [IsRetryFutile] 不用 [IsControlFlowSignal]**：ctx 归属这件事业务谓词
		// 不该也没法自己判（它只拿到 err）。放框架这一层，业务写什么谓词都不会因为
		// 「错误链上碰巧有个别人的 ctx 错」把自己的重试关掉——那正是 `http.Client{Timeout}`
		// 这种标准配法会产出的形状。
		// 档没写成（ErrCheckpointNotPersisted）同样恒不重试：多数时候它带着 ErrRunAborted，但槽属于别人（身份
		// 冲突）那种不带，只靠类型上的 NonRetryable——那只在默认谓词里生效，业务写个宽松谓词就会把嵌套执行
		// 连同副作用重跑（实测 4 次）。
		if IsRetryFutile(ctx, err) || errors.Is(err, ErrRunAborted) ||
			errors.Is(err, ErrCheckpointNotResumable) || errors.Is(err, ErrCheckpointNotPersisted) {
			return out, err
		}
		// 业务谓词：只有配重试的人答得上「这个错重跑有没有救」。没给谓词而 MaxRetries>0
		// 的配置在装配期已被 ValidateRetryConfig 拦下；这里的 nil 只可能来自绕过那道
		// 校验的路径（如直接调本函数），保守处理成不重试。
		if cfg.ShouldRetry == nil || !cfg.ShouldRetry(err) {
			return out, err
		}
		// 限流（429）用**独立计数器**——它的等待是秒到分钟级（还常照服务端 Retry-After
		// 等），跟 5xx / 网络抖动的毫秒级重试不是一回事，共用 MaxRetries 会让一次限流把
		// 整个 Run 拖很久。
		//
		// **必须独立计数而不是「共享 attempt 比更低的上限」**：后者在混合错误序列下算的
		// 是别人消耗的次数。`MaxRetries=5, RateLimitMaxRetries=2` 遇到 `503, 503, 429`
		// 时，第 3 次的 429 因 attempt 已到 2 直接放弃——**429 一次都没重试**。而
		// `503→429`（provider 过载被重试打压后转限流）是常见的生产退化路径。
		if IsRateLimited(err) {
			rlAttempts++
		}
		if cfg.RateLimitMaxRetries > 0 && rlAttempts > cfg.RateLimitMaxRetries {
			return out, err // 限流预算耗尽
		}
		if attempt >= cfg.MaxRetries {
			// 总预算耗尽——返当时的 (out, err)（out 通常 zero）
			return out, err
		}
		// 等待 BackoffDelay 后重试（attempt+1 次将开始）。
		//
		// 服务端明示 `Retry-After` 时**以它为准**——它是权威的，我们猜的指数退避要么
		// 太早（再吃一次 429，部分 provider 还会因此延长封禁）要么太晚（白等）。
		//
		// **有意不受 MaxDelay 约束**：那个字段管的是指数退避别失控增长，不是用来推翻
		// 服务端指令。外层边界交给 ctx——SleepWithContext 在 ctx 取消 / 超时时立即返回，
		// 所以业务想给「最多等多久」兜底就设 ctx deadline（长跑场景本来就该设）。
		delay := BackoffDelay(cfg.BackoffPolicy, attempt+1)
		if ra := RetryAfterFromError(err); ra > 0 {
			delay = ra
		}
		if err := SleepWithContext(ctx, delay); err != nil {
			return zero, err
		}
	}
	return zero, nil // unreachable
}

// errCondRerunFrom 标记「cond 报错走了 CondFailRerunFrom 档」。
//
// 不导出：业务判「这次要不要 Resume」看的是有没有存档（`ErrNoPendingCheckpoint`），
// 不需要认这个 sentinel。它只在框架内部用来定存档的档位。
var errCondRerunFrom = errors.New("cond failed with rerun-from policy")

// shouldStayPending 报这个错该不该让存档留在「等自动恢复」那一档。
//
// **判据是框架对这个错的处置意图，不是错本身的类型**——这是它跟
// `IsControlFlowSignal` 的区别，也是当初漏掉 `CondFailRerunFrom` 的原因：
//
//   - 控制流信号（中断 / ctx 取消 / 软取消 / 陈旧写回）本来就该等恢复
//   - `CondFailRerunFrom` 返出去的是**业务错**（cond 报错的原因原样带着），
//     按类型判它就是「失败现场」；但框架对它的处置写在 policy 名字里——
//     存着，等 Resume 重跑 from。那正是 Pending 的定义
//
// 只看类型的话，声明了 `CondFailRerunFrom` 的图会被标成失败现场。后果**不是**续跑断掉
// （2026-08-13 起续跑闸门按内容判（`HasResumableWork`）、不读 Status），而是它在 `ListPending`
// （业务查「谁在等答案」的入口）里列不出来。
//
// `ErrCheckpointNotResumable` 也归这一档：**坏的是那份存档，不是这次执行**。
// 报它的那一层通常守得住自己的现场（守卫在 `Restore` 之前、出口早退不写）——⚠️ 那有
// 个前提「cm 是空的」，调用方先 Restore 过就不成立，完整说明见
// [ErrCheckpointNotResumable] godoc，别单独把它当结构保证。但**祖先层**
// 各自还要写自己的档——按类型判它们会一路盖成失败现场，于是业务修好 backend 之后
// 这些档在 `ListPending` 里一个都列不出来（续跑本身不受影响，见上）。
//
// 实测过这个形态（⚠️ 这段实测做于 2026-08-13 撤掉 Status 闸门**之前**——那时 Status 决定捡不捡，
// 今天闸门按内容判，父档即便标成 failed 也续得上；留着是因为它仍然说明「按类型盖章」会把
// 挂起现场标错）：Sequential 套 BasicAgent，只让子 ns 的 backend 丢 `CurrentFrontier`
// ⟹ 子档完好、父档 `pending → failed`；**只把父档的 Status 掰回 pending，HITL 闭环立刻
// 跑通**——数据自始至终是好的，不可续跑 100% 来自那一次盖章。
func shouldStayPending(err error) bool {
	return IsControlFlowSignal(err) || errors.Is(err, errCondRerunFrom) ||
		errors.Is(err, ErrCheckpointNotResumable)
}

// LeavesResumableCheckpoint 报「这次退出**留没留下一份之后能续上的存档**」。
//
// 判据：**err 非 nil 且不是终局失败**。成功不留档（cleanupOnSuccess 清）；终局失败
// （`ErrRunAborted`）主动清子树；其余任何退出——中断、软取消、坏存档、**普通业务错**
// ——都留档，而引擎「捡不捡」按内容判（`Checkpoint.HasResumableWork`）⟹ 那些档
// **全都续得上**（失败档 Resume = 重试失败分支，成功兄弟不重放）。
//
// ⚠️ **它曾经 = `shouldStayPending`（普通业务错返 false），现在不是了**。那时失败档
// 被 auto-load 的档位 gate 挡住、确实续不上；gate 撤掉（判据改按内容）后失败档也可续，
// 这个谓词的真值表必须跟上——否则它的两个消费者（`agent_v2` 的历史落点）会在
// 「业务错退出、之后被 Resume 重试成功」这条链上落成**有答无问**：用户那句话没被
// 结算进历史，重试成功后历史里只有 assistant 的答复（实测过）。
// `shouldStayPending` 仍留在引擎出口写**档位**（那是报告位，Status 还在），两者从此
// **不是同一个判据**——一个答「留没留档」，一个答「档位标什么」。
//
// **给谁用**：跨包的框架层（`agent_v2`）需要按「这次退出还有没有下文」分档做事，而不能
// 靠枚举错类型。典型是 GraphAgent 的历史落点：它必须在**退出前**把用户那句话记进历史，
// 因为 Resume 入口拿不到原始 input。
//
// **过度近似是良性的**：入口 fail-fast（如 GraphID 冲突）也返 true，但那类退出没有
// 消费输入，历史层的跨 Run 守卫会防 dup——宁多结算一条用户原话，不落「有答无问」。出口档
// 没写成（[ErrCheckpointNotPersisted]）多数时候带着 ErrRunAborted、返 false；槽属于别人的
// 那种不带、返 true，同属这一档。
//
// **业务一般不需要它**：想知道「该不该 Resume」直接看 `IsInterrupt`（要答案）或
// `errors.Is(err, ErrRunAborted)`（终局失败）更贴近意图。
func LeavesResumableCheckpoint(err error) bool {
	return err != nil && !errors.Is(err, ErrRunAborted)
}
