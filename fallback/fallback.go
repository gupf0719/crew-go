// Package fallback 提供 model.ChatModel 的故障切换装饰器 FallbackChatModel。
//
// 持有一个有序候选 model 列表，主候选故障时按序自动切到下一个，对调用方与全部
// 编排透明（实现 model.ChatModel，可直接注入 agent_v2.WithModel）。
//
// # 包位置
//
// 本包 import compose_v2（复用 RetryConfig / BackoffDelay / CircuitBreaker /
// InterruptError 原语）+ model + schema，放在 compose_v2/agent_v2 层，**不放 model 包**：
// model 是低层叶子（仅依赖 schema），让它反向依赖编排引擎 compose_v2 会反转分层；
// 且 compose_v2 一旦 import model（分层本就允许）即成环。故独立成包，谁都不 import 它。
//
// 完整设计：doc/v2/model-fallback.md。
package fallback

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// ErrAllMembersUnavailable 在所有候选都处于熔断 cooldown（无可尝试候选）时返回。
// 业务可 errors.Is 区分"全员熔断（稍后自愈）"与"真实 API 错误"。
var ErrAllMembersUnavailable = errors.New("fallback: all members unavailable (circuit breakers open)")

// FallbackChatModel 按优先级顺序在多个候选 model 间做故障切换，实现 model.ChatModel。
//
// 构造后字段只读（无运行时 mutation）——并发 Generate/Stream 安全；可变的故障计数
// 封装在每个候选独立的 CircuitBreaker（自带锁）内。
type FallbackChatModel struct {
	members  []*fallbackMember
	policy   FailoverPolicy
	retry    PerModelRetry
	onSwitch func(ctx context.Context, fromIdx, toIdx int, err error)
}

// fallbackMember 包装一个候选 model 及其独立断路器——某候选连续失败只 trip 它自己，
// 不波及其他候选（对齐 autocompact.go 跨实例共享单例 breaker 的教训）。gate 是这个候选「服务端说 T 之前别来」
// 的共享门（见 tryMembers 第 1 条）；WithTools 派生的模型跟断路器一样复用同一个指针。
type fallbackMember struct {
	model   model.ChatModel
	breaker *compose_v2.CircuitBreaker
	gate    *retryGate
}

// retryGate 是一个候选「服务端说 T 之前别来」的共享状态：任何一次调用收到超过 memberRetryAfterLimit 的
// Retry-After 时设下，所有调用在 T 之前都不请求它。
type retryGate struct {
	mu        sync.Mutex
	notBefore time.Time
	err       error // 设下这道门的那次错误（带 Retry-After）：等不到 T 的调用拿它当这个候选的失败原因
}

// set 把门推到 until（只往后推，不往前拉：最晚的那个 T 才是「T 之前都会被拒」的界，见 tryMembers 第 1 条）。
func (g *retryGate) set(until time.Time, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if until.After(g.notBefore) {
		g.notBefore, g.err = until, err
	}
}

// closedUntil 返回门开的时刻与设门的错误；门已经开了（或从没设过）时 closed 为 false。
func (g *retryGate) closedUntil(now time.Time) (until time.Time, err error, closed bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.notBefore.After(now) {
		return time.Time{}, nil, false
	}
	return g.notBefore, g.err, true
}

// gateClosedError 是被门挡住、这次调用没请求过的候选的错误（第 1 条）。RetryAfterDelay 是门还要关多久，不是设门时
// 服务端给的原值——外层照原值等会多等一整个 Retry-After；错误链上取得到设门的那次错误（状态码、正文）。
type gateClosedError struct {
	until time.Time
	left  time.Duration // 出错那一刻门还要关多久：错误文本用它，别随打印时刻变
	cause error
}

func (e *gateClosedError) Error() string {
	return fmt.Sprintf("fallback: member is rate limited for another %v: %v", e.left.Round(time.Millisecond), e.cause)
}

func (e *gateClosedError) Unwrap() error { return e.cause }

// RetryAfterDelay 返回门还要关多久；门已经开了时返回 1 纳秒（现在就能再试）——返回 0 的话
// compose_v2.RetryAfterFromError 会接着往下找、取到设门时的原值。
func (e *gateClosedError) RetryAfterDelay() time.Duration {
	if d := time.Until(e.until); d > 0 {
		return d
	}
	return time.Nanosecond
}

// New 构造故障切换装饰器。models 按优先级排序——models[0] 为主，其余按序备用。
// 空列表或含 nil model（含 typed-nil）时 panic（fail-fast，对齐框架 nil 防御纪律）。
func New(models []model.ChatModel, opts ...Option) *FallbackChatModel {
	if len(models) == 0 {
		panic("fallback: New requires at least one model")
	}
	cfg := newConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}
	members := make([]*fallbackMember, 0, len(models))
	for i, m := range models {
		if isNilModel(m) {
			panic(fmt.Sprintf("fallback: model at index %d is nil", i))
		}
		members = append(members, &fallbackMember{
			model:   m,
			breaker: compose_v2.NewCircuitBreakerWithCooldown(cfg.breakerMaxFailures, cfg.breakerCooldown),
			gate:    &retryGate{},
		})
	}
	// 框架层控制流 guard：构造期包一次，切换判断（tryMembers 切换、callMemberWithRetry 推迟）自动豁免
	// interrupt/ctx——业务自定义 policy 漏写豁免也不吞中断（见 guardedPolicy godoc）。重试判断不读 policy，
	// shouldRetryMember 自己先问 IsRetryFutile。WithTools 重组时复制的是已包装的 f.policy，不会二次包装。
	return &FallbackChatModel{members: members, policy: guardedPolicy{inner: cfg.policy}, retry: cfg.retry, onSwitch: cfg.onSwitch}
}

// errNilMessage 是候选的 Generate 返回 nil 消息、又没有报错时的错误：按候选故障处理（照常重试、
// 切换），Generate 与 Stream 两条路同一个判据。
var errNilMessage = errors.New("fallback: member Generate returned nil message")

// generateMember 调候选的 Generate，nil 消息按 errNilMessage 报错。
func generateMember(ctx context.Context, m model.ChatModel, messages []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	msg, err := m.Generate(ctx, messages, opts...)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, errNilMessage
	}
	return msg, nil
}

// Generate 按序在候选间故障切换调用非流式 API。
func (f *FallbackChatModel) Generate(ctx context.Context, messages []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	return tryMembers(ctx, f, func(ctx context.Context, m model.ChatModel) (*schema.Message, error) {
		return generateMember(ctx, m, messages, opts...)
	})
}

// Stream 按序在候选间故障切换建立流式调用。
//
// **仅建流阶段切换**（model-fallback.md §6）：一旦某候选 Stream 成功返回 reader、
// chunk 开始投递，就不再切换（下游已收到半截输出无法干净回切）；流中途的错误照常
// 向下游传递。建流阶段同步返回 error（如 HTTP 非 200 在拿到首 chunk 前）才触发切换。
//
// **不支持流式的候选**（Stream 返回 model.ErrStreamUnsupported，或 nil reader 且没有错误）
// 就地用它的 Generate，结果包成只有一项的流——轮到哪个候选就用哪个，顺序不倒退。把
// ErrStreamUnsupported 交回调用方的话，调用方改调 Generate 会从第一个候选重新走：前面的候选
// 刚失败过，立刻又被打一次（不看 Retry-After，断路器一次调用记两次失败）。只有一项的流，
// BasicAgent 整条采用那一项、不做增量拼接，拿到的跟直接调 Generate 一样（OnModelEndStream
// 钩子改了片段数时照常拼接）。
func (f *FallbackChatModel) Stream(ctx context.Context, messages []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return tryMembers(ctx, f, func(ctx context.Context, m model.ChatModel) (*schema.StreamReader[*schema.Message], error) {
		sr, err := m.Stream(ctx, messages, opts...)
		if !errors.Is(err, model.ErrStreamUnsupported) && (err != nil || sr != nil) {
			return sr, err
		}
		msg, err := generateMember(ctx, m, messages, opts...)
		if err != nil {
			return nil, err
		}
		return schema.NewSingleChunkReader(msg), nil
	})
}

// WithTools 对每个候选绑定工具并重组新的 FallbackChatModel。
//
// **关键**（model-fallback.md §7.1）：绑定后的新候选**复用同一批 breaker 指针**——
// 否则 BasicAgent 每轮 toolsFn 都 WithTools 一次、每次得到全新断路器，故障计数永远
// 清零，断路器形同虚设。副作用：由同一 FallbackChatModel 派生的多个模型共享熔断状态
// （同候选的故障计数全局生效）——这是有意设计。
func (f *FallbackChatModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	bound := make([]*fallbackMember, len(f.members))
	for i, mem := range f.members {
		bm, err := mem.model.WithTools(tools)
		if err != nil {
			return nil, fmt.Errorf("fallback: member %d WithTools: %w", i, err)
		}
		bound[i] = &fallbackMember{model: bm, breaker: mem.breaker, gate: mem.gate} // 复用 breaker 与门的指针
	}
	return &FallbackChatModel{members: bound, policy: f.policy, retry: f.retry, onSwitch: f.onSwitch}, nil
}

// tryMembers 是 Generate/Stream 共享的切换骨架：在候选间挑着试，跳过熔断中的，成功即返；失败按 policy 决定换下一个
// 还是立即抛。
//
// **挑候选只有一条规则**：每次挑「此刻能请求的」候选里排序最靠前的——没试过的，或之前推迟、现在到了能再请求的时刻的；
// 都要等时，等最早能再请求的那个。候选要求等很久时先推迟它、去试别的，**推迟不是放弃**：别的候选都没成、期限也够时
// 回来，把它没用完的重试用完。曾经换走之后不再回来：后面的候选失败时，调用方给的时间明明够、第一个候选 61 秒后就能用，
// 结果立刻失败，或者去等两个 Retry-After 里更长的那个（审计实测）。
//
// 四条不变量（推迟改变了「先等这个候选、再试下一个」的次序，下面几条保证它不比原地等更差）：
//
//  1. **服务端说「T 之前别来」，对所有调用一视同仁**：任何一次调用收到超过 memberRetryAfterLimit 的 Retry-After（且切换
//     判据允许换候选）时设下这个候选的共享门（retryGate），T 之前任何调用都不请求它。碰到它时断路器闭合就当推迟（先试
//     别的，都没成且期限够时在 T 回来）；断路器已经断开（熔断中，含半开）就当熔断跳过——探测名额在设门的那个调用手里，
//     原地等的话别的调用同样拿不到名额、当场跳过；等不到 T 的也跳过。这次调用没请求过它时，它的错误是「门还要关多久」
//     （gateClosedError：Retry-After 是剩余时间，链上取得到设门的那次错误）；请求过的以自己拿到的错为准。被门挡住、改去
//     请求别的候选，照常发切换回调与 failover 事件（from 是被门挡住的候选）。**门不是失败**：不记断路器（候选是好的，
//     只是忙；这次请求占着半开的探测名额时放掉）。**门以服务端说的 T 为准**：几次设门取最晚的 T，T 之前不因为别的
//     信号提前开——门关着时没有新请求发出去，期间能看到的成功只可能来自门设下之前在途、限流生效前就被放行的请求，
//     说明不了候选恢复了；并发的几个 429 给出不同的 T 往往对应不同的限额（请求数、token 数），最晚的那个才是「T 之前
//     都会被拒」的界。代价是服务端给的 T 偏保守时这段时间用不上它（2026-10-20 定）。曾经推迟当场记一次失败：并发的调用一多，候选就被这些推迟熔断，之后到达的调用在
//     断路器那里直接跳过它、不推迟也不回来，别的候选也失败时立刻失败（审计实测 20 个并发调用只成功 3 个）；更早推迟
//     时不记、回来时才算，别的请求在 T 之前照样一个个打到它（审计实测 15 个并发请求全部打到半开的候选）。
//  2. **真失败当场记、一次调用里一个候选至多记一次**：候选的最终失败（不是设门的那种）在这次调用不再用它时记。调用方
//     在原地等的途中取消不算候选的失败、不记。
//  3. **回到推迟的候选是这次尝试的继续，不再经过断路器**：跟原地重试一样——等完服务端要求的时间就请求，不因为别的
//     请求在此期间把它熔断而作罢（回来那次没占探测名额，报「换候选没用」时也不放名额；失败照常记、成功照常
//     RecordSuccess）。回来之前先看门：门被别的调用推后了就跟着推后。曾经回来时重新问断路器：等满 Retry-After
//     之后被别的请求记的失败挡住、一次没请求就失败（审计实测）。
//  4. **候选 i 报「换候选没用」（切换判据为假、又不是控制流）时，排在 i 后面的不再试**（没试过的、推迟着的都算），
//     **排在 i 前面推迟着的照样回去**；最终失败时返回排得最靠前的那个「换候选没用」的错——跟原地等一样：原地等的话
//     先等前面的候选，它成功就成功，失败了才轮到后面的、碰到这个错就返回。曾经直接返回（第一个候选 100 毫秒后就能
//     用，调用以备用候选的 401 结束）；后来不看先后，会去等排在 i 后面的推迟候选、返回时间上第一个「换候选没用」的
//     错（上下文超限被换成后面候选的 401，ReactiveCompact 认不出）（都是审计实测）。
//
// 全部候选失败时返**排得最靠前的那个候选**的错误（model-fallback.md §11.7：主 model 的诊断价值最高，用户最关心主候选
// 为何失败；第 4 条的情形除外），但有候选的失败还值得再来一次（compose_v2.IsRetryable）时返排得最靠前的那个——交出的错误
// 的可重试性要等于整次调用的，主候选额度用完、备用瞬时 503 时外层照常重试；全部熔断（没有一个候选请求过、也没有一个被门
// 挡住）时返 ErrAllMembersUnavailable。
//
// （Go 方法不能带类型参数，故为接收 f 的包级泛型函数。）
func tryMembers[T any](ctx context.Context, f *FallbackChatModel, call func(context.Context, model.ChatModel) (T, error)) (T, error) {
	var zero T
	var noSwitchErr error
	states := make([]memberState, len(f.members))
	last := -1 // 上一个请求过（或被门挡住）的候选：换到别的候选时，切换回调与 failover 事件的 from
	var lastErr error
	// exhausted 是这次调用里在本层把重试用完的候选的标记（有的话）。交出的最终错误照样带上它：一次调用就是这一层的一次
	// 失败，只看被选中报告的那个候选会让标记丢掉——主候选额度用完不重试、备用重试用完，交出的是主候选的错，外层又整轮
	// 重做，备用被请求 9 次（审计实测）。
	var exhausted *compose_v2.RetriesExhaustedError
	finalErr := func(err error) error {
		if exhausted == nil || compose_v2.IsRetryFutile(ctx, err) {
			return err
		}
		return compose_v2.MarkRetriesExhausted(err, exhausted.Attempts)
	}
	for {
		i, wait, ok := nextMember(states, time.Now())
		if !ok {
			break
		}
		if wait > 0 {
			if err := compose_v2.SleepWithContext(ctx, wait); err != nil {
				return zero, err
			}
		}
		mem := f.members[i]
		st := &states[i]
		// 门（第 1 条）：没试过的、回访的都先看——T 之前不请求它
		if until, gateErr, closed := mem.gate.closedUntil(time.Now()); closed {
			if !st.requested {
				st.err = &gateClosedError{until: until, left: time.Until(until), cause: gateErr}
				if last < 0 {
					last, lastErr = i, st.err // 被门挡住、改去请求别的候选也是一次切换
				}
			}
			switch {
			case exceedsDeadline(ctx, time.Until(until)):
				st.done = true // 等不到 T：这次调用里不再回来
			case !st.requested && mem.breaker.Tripped():
				st.done = true // 熔断中：探测名额在设门的那个调用手里，按熔断跳过
			default:
				st.deferred, st.readyAt = true, until
			}
			continue
		}
		// 回到推迟的候选是这次尝试的继续（第 3 条）：不再经过断路器，也没有占它的探测名额。只被门挡过、这次调用还没
		// 请求过的不算继续，照常经过断路器——不然它因真失败熔断时，T 一到所有调用一起绕过断路器去请求它
		revisit := st.deferred && st.requested
		if !revisit && !mem.breaker.Allow() {
			st.done = true // 熔断中：这次调用里不再回来
			continue
		}
		if last >= 0 && last != i {
			if f.onSwitch != nil {
				f.onSwitch(ctx, last, i, lastErr)
			}
			// sink-gated：业务接了 observability 自动采集 failover metric，没接零开销（event.go）
			emitFailoverEvent(ctx, last, i, lastErr)
		}
		mayDefer := func(err error) bool { return f.policy.ShouldFailover(ctx, err) }
		out, err, d, gated := callMemberWithRetry(ctx, f.retry, mem, st.nextAttempt, mayDefer, call)
		if err == nil {
			st.done = true
			mem.breaker.RecordSuccess()
			return out, nil
		}
		st.requested, st.err = true, err // 自己拿到的错为准
		last, lastErr = i, err
		if re := (*compose_v2.RetriesExhaustedError)(nil); exhausted == nil && errors.As(err, &re) {
			exhausted = re
		}
		if gated {
			// 设了门（第 1 条）：不是失败，不记断路器；这次请求占着半开的探测名额时放掉（回访的那次没占）
			if !revisit {
				mem.breaker.ClearProbe()
			}
			if d != nil {
				st.deferred, st.readyAt, st.nextAttempt = true, d.readyAt, d.attempt
			} else {
				st.done = true // 重试用完 / 等不到 T
			}
			continue
		}
		st.done = true
		if !f.policy.ShouldFailover(ctx, err) {
			// 控制流 / 4xx：不切换（不计 breaker——非 member 瞬时故障）。**但须清 half-open probing**：若本次
			// 是 half-open 探测（Allow 置了 probing），非切换返回既不 RecordSuccess 也不 RecordFailure，probing
			// 泄漏 true → 候选被额外排除到 probeAt+cooldown 才自愈。回访的那次没有经过 Allow，不碰别人的名额。
			if !revisit {
				mem.breaker.ClearProbe()
			}
			// 换候选没用（第 4 条）：排在它后面的不再试，前面推迟着的照样回去
			skipAfter(states, i)
			if compose_v2.IsRetryFutile(ctx, err) || !anyDeferred(states) {
				return zero, finalErr(err)
			}
			noSwitchErr = err // 此后再报的只会出自排得更靠前的候选：后者覆盖前者
			continue
		}
		recordMemberFailure(ctx, i, mem, err)
	}
	if noSwitchErr != nil {
		return zero, finalErr(noSwitchErr)
	}
	// 交出的错误决定调用方下一步做什么，它的可重试性要等于整次调用的：有候选的失败还值得再来一次（瞬时 5xx、限流）时，
	// 交出排得最靠前的那个；都不值得时交出排得最靠前的候选的错误。曾经一律交出排得最靠前的：主候选额度用完（不可重试）、
	// 备用瞬时 503 时外层不再重试整次调用，备用本来能恢复（审计实测）。被跳过的那些原因在切换事件与日志里。
	var first error
	for _, st := range states {
		if st.err == nil {
			continue
		}
		if first == nil {
			first = st.err
		}
		// 值得再来一次 = 错误可重试，且它要求等的（Retry-After、门还要关多久）在调用方期限之内：这次调用里已经判过等不到的
		// 候选交出去，外层会照它的 Retry-After 一直睡到期限，真正的原因还被换成期限到了（审计实测）
		if compose_v2.IsRetryable(st.err) && !exceedsDeadline(ctx, compose_v2.RetryAfterFromError(st.err)) {
			return zero, finalErr(st.err)
		}
	}
	if first != nil {
		// 没有一个候选值得再来一次：交出的错误也不能让外层再来。排得最靠前的那个本身可能可重试、只是这次等不到（Retry-After
		// 超过调用方期限），原样交出去外层会照它睡到期限（审计实测）——补上「这一层已经用完」的标记，错误文本不变。框架的重试
		// 守卫认这个标记，业务谓词放得再宽也不再重试
		if compose_v2.IsRetryable(first) {
			failed := 0
			for _, st := range states {
				if st.err != nil {
					failed++
				}
			}
			return zero, compose_v2.MarkRetriesExhausted(finalErr(first), max(failed, 2))
		}
		return zero, finalErr(first)
	}
	return zero, ErrAllMembersUnavailable
}

// recordMemberFailure 给候选记一次失败；断路器 closed→open 的边缘 sink-gated emit member.tripped（同 failover
// 零开销门）。
func recordMemberFailure(ctx context.Context, i int, mem *fallbackMember, err error) {
	if mem.breaker.RecordFailure() {
		emitMemberTrippedEvent(ctx, i, err)
	}
}

// anyDeferred 判断这次调用里还有没有推迟着、等着回去请求的候选。
func anyDeferred(states []memberState) bool {
	for _, st := range states {
		if st.deferred && !st.done {
			return true
		}
	}
	return false
}

// skipAfter 把排在 i 后面的候选（没试过的、推迟着的）标成这次调用里不再试（第 4 条）。
func skipAfter(states []memberState, i int) {
	for j := i + 1; j < len(states); j++ {
		states[j].done = true
	}
}

// memberState 是一次调用里一个候选的状态：没试过（零值）、推迟中（readyAt 之后能再请求，从 nextAttempt 接着重试）、
// 或已有结局（done：成功、最终失败、熔断中跳过、等不到门开、不再试）。requested 表示这次调用请求过它（推迟的是这次
// 尝试的继续，回来时不经过断路器）；只被门挡过的为假。err 是它在这次调用里的失败原因：请求过的是自己拿到的最后一个
// 错，只被门挡过的是门的错（gateClosedError）。
type memberState struct {
	done        bool
	deferred    bool
	requested   bool
	readyAt     time.Time
	nextAttempt int
	err         error
}

// nextMember 挑下一个要请求的候选：此刻能请求的（没试过的，或推迟到期的）里排序最靠前的，wait 为 0；没有的话挑推迟中
// 最早到期的那个，wait 是还要等多久。都有结局了返回 ok=false。
func nextMember(states []memberState, now time.Time) (idx int, wait time.Duration, ok bool) {
	earliest := -1
	for i, st := range states {
		switch {
		case st.done:
		case !st.deferred, !st.readyAt.After(now):
			return i, 0, true
		case earliest < 0 || st.readyAt.Before(states[earliest].readyAt):
			earliest = i
		}
	}
	if earliest < 0 {
		return 0, 0, false
	}
	return earliest, states[earliest].readyAt.Sub(now), true
}

// exceedsDeadline 判断等 d 之后是不是已经到了调用方的期限（等完也来不及再请求）。
func exceedsDeadline(ctx context.Context, d time.Duration) bool {
	deadline, ok := ctx.Deadline()
	return ok && time.Until(deadline) <= d
}

// memberRetryAfterLimit：服务端要求的等待超过它时，不在当前候选上等：给它设门、先推迟它、去试别的候选（见
// callMemberWithRetry）。跟 OpenAI 的 Go SDK 只认一分钟以内的 Retry-After、pi-ai 超过 60 秒直接报错一致。是变量只为
// 测试里能调小。
var memberRetryAfterLimit = 60 * time.Second

// deferral 是一个候选被推迟时还没用完的尝试：最早什么时候能再请求、回来时从第几次尝试接着来。
type deferral struct {
	readyAt time.Time
	attempt int
}

// callMemberWithRetry 在单个候选上重试会自己恢复的故障（从第 startAttempt 次尝试起），返回最后一次的错误交给
// tryMembers 决定切不切；要等很久时返回推迟（deferral 非 nil）。gated 为真表示最后那次响应设了门（tryMembers 不把它
// 记成断路器失败，见第 1 条）。
//
// **「在这个候选上再试」与「换下一个候选」是两个判断**：前者由 shouldRetryMember 答（只重试会自己恢复的
// 错误），后者由 [FailoverPolicy] 答（tryMembers 里）。同一个错两边的答案可以相反——计费类额度用完在这里不该
// 重试（等多久都不会恢复）、在切换层该换（换资源池改变了条件）。曾经两者共用 ShouldFailover 一个判据，
// 额度耗尽先在同一个候选上按 MaxRetries 重试、白等退避才换（dashscope 403 实测 3 次请求、600 毫秒）。
//
// **要等的时间（服务端的 Retry-After，没给时是退避）**：Retry-After 超过 memberRetryAfterLimit、且切换判据允许换候选
// （mayDefer）时，先给这个候选设门（不管这次调用等不等得到，别的调用都该知道）；重试用完、或等完已经到了调用方的
// 期限时不等，返回这次的错误；设了门、等得到时返回推迟，交给 tryMembers 先试别的候选、回来时从下一次尝试接着来——
// 这不推翻服务端的指令，T 之前没有调用再请求这个候选。其余照旧等（Retry-After 不受 MaxDelay 约束，外层边界交给
// ctx）。曾经照着 Retry-After 干等（429 + 10 分钟），调用方的超时先到，后面的候选一次都没被调用；后来只在超过 60 秒、
// 且后面有断路器此刻放行的候选时换，等待比调用方剩下的时间还长（5 秒对 300 毫秒）照样干等到超时，切换判据说这个错
// 不换时又既不等也不换（都是审计实测）。
//
// **刻意不用 compose_v2.RunWithRetry**：它在业务谓词之前有一道编排层拥有的 guard（IsRetryFutile /
// ErrRunAborted / ErrCheckpointNotResumable），按「成员 Run 重试」的语境演化；收敛到它，将来任何人往那道
// guard 里加编排哨兵都会静默改变这里的行为。能共享的已经共享：BackoffDelay / SleepWithContext 两个纯计算
// 退避原语，参数走 [compose_v2.BackoffPolicy]。参数类型为什么不共享见 [PerModelRetry]。
func callMemberWithRetry[T any](ctx context.Context, retry PerModelRetry, mem *fallbackMember, startAttempt int, mayDefer func(error) bool, call func(context.Context, model.ChatModel) (T, error)) (out T, err error, d *deferral, gated bool) {
	maxRetries := retry.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	for attempt := startAttempt; attempt <= maxRetries; attempt++ {
		out, err = call(ctx, mem.model)
		if err == nil {
			return out, nil, nil, false
		}
		if !shouldRetryMember(ctx, err) {
			return out, err, nil, false // 不重试：交给 tryMembers 按切换判据决定换下一个还是直接返回
		}
		delay := compose_v2.BackoffDelay(retry.Backoff, attempt+1)
		ra := compose_v2.RetryAfterFromError(err)
		if ra > 0 {
			// 服务端明示 Retry-After 时以它为准（权威值优于猜的退避，不受 MaxDelay 约束，外层边界交给 ctx）
			delay = ra
		}
		gated = ra > memberRetryAfterLimit && mayDefer(err)
		if gated {
			mem.gate.set(time.Now().Add(ra), err) // T 之前所有调用都不请求它（第 1 条）
		}
		if attempt == maxRetries {
			// 重试用完：带上「已在这一层重试过」的标记，上面各层（BasicAgent 的模型调用重试、编排成员重试）不再重试它
			return out, compose_v2.MarkRetriesExhausted(err, attempt+1), nil, gated
		}
		if exceedsDeadline(ctx, delay) {
			return out, err, nil, gated // 等完也到期了：不等
		}
		if gated {
			return out, err, &deferral{readyAt: time.Now().Add(delay), attempt: attempt + 1}, true
		}
		if serr := compose_v2.SleepWithContext(ctx, delay); serr != nil {
			return out, serr, nil, false // ctx 取消 / 超时——立即返
		}
	}
	return out, err, nil, false
}

// isNilModel 检测 nil 与 typed-nil（业务传 var m *X; New([]{m}) 时 m 非 nil interface
// 但底层指针 nil——漏检会在远端 m.Generate() 处 panic 难定位）。
func isNilModel(m model.ChatModel) bool {
	if m == nil {
		return true
	}
	v := reflect.ValueOf(m)
	switch v.Kind() {
	// 与框架 isFrameworkNil 协议同款的可空 Kind 集（含 UnsafePointer 对齐）。
	case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan, reflect.UnsafePointer:
		return v.IsNil()
	default:
		return false
	}
}

// ModelName 返回这次调用起手请求的那个候选的模型名（满足 model.ModelNamer）。
//
// 是尽力而为的预估：并发调用共用一个 fallback 时，到真正请求那一刻状态可能已经变了（别的调用先占了探测名额）；
// 中途切到别的候选时也只报起手那个。实际回答的模型看响应里的模型名（gen_ai.response.model）与 failover 事件。
// 候选没实现 model.ModelNamer 时返回空串。只读，不占探测名额、不影响放行决策。
func (f *FallbackChatModel) ModelName() string {
	if f == nil {
		return ""
	}
	i := f.startIndex(time.Now())
	if i < 0 {
		return "" // 一个候选都没有：没有可报的模型名
	}
	if n, ok := f.members[i].model.(model.ModelNamer); ok {
		return n.ModelName()
	}
	return ""
}

// startIndex 预估下一次调用会起手请求哪个候选，**跟 tryMembers 走同一套选法**：[nextMember] 挑、门与断路器筛，
// 筛掉的按 tryMembers 的同一套分档置 done / deferred 再挑下一个。
//
// # 为什么必须复用 nextMember 而不是自己写一遍
//
// 这里曾经自己写过一个循环（「按顺序第一个门开着且断路器放行的，都不满足时取第一个」），它**漏掉了选法里
// 「都要等时等最早能再请求的那个」这一档**：首选 Retry-After=30m、备选 2m 时它报首选，而 tryMembers 睡 2 分钟
// 之后起手请求的是**备选**。在那之前还错过一轮——只看 breaker.Tripped()，门关着时报首选、cooldown 过后的半开
// 探测期报备选，两种都跟实际请求的对不上（`806c3a3` 修的就是它）。
//
// **同一条判据被审出两个反例，就不该再补第三个分支**（CLAUDE.md「必须与 X 保持一致 = 设计债的自白」）：选法只
// 该有一份，放在 nextMember 里，这里跟着它走。tryMembers 的挑法以后再改，这里自动跟上。
//
// 与 tryMembers 的唯一差别是**不判调用方期限**（exceedsDeadline）：那需要 ctx，而 model.ModelNamer 不带 ctx。
// 期限很紧时 tryMembers 会把推迟中的候选全标 done、一个都不请求，所以分叉恒为「报了一个谁也没请求的候选」，
// 那次调用注定失败、span 上带着错，此时报「最早能用的那个」无害。
//
// # ⚠️ 终止性依赖「两处同钟」——改这里之前先读这段
//
// 循环靠「每次迭代把一个候选移出『此刻可请求集』」终止：门关着时置 deferred 且 readyAt = until，而
// [gate.closedUntil] 报 closed 的充要条件就是 until > now，所以下一次 nextMember 不会再以 wait==0 返回它。
// **这条推理成立的前提是 closedUntil 与 nextMember 拿的是同一个 now**（两次比较的操作数完全相同，于是必然
// 同答案；跟单调钟 / 墙钟 / 时钟回拨都无关）。
//
// 把 `nextMember(states, now)` 改成 `nextMember(states, time.Now())`（看着像「用最新时间更准」）会**死循环**：
// 某个候选的门在取 now 与读活钟之间刚好到期 ⟹ nextMember 给 wait==0、closedUntil(冻结的 now) 仍报 closed
// ⟹ 反复 defer。审计实测：既有全部 fallback 用例照样全绿，而 ModelName 在每次模型调用前被 observability
// 调用 ⟹ 整个 Run 卡死且出不来（活钟只会越过越远）。
//
// tryMembers **不等价**：它两处都用活钟、同向单调，所以安全。安全的是「两处同钟」，不是「冻结」或「活钟」
// 本身——混用才死。
func (f *FallbackChatModel) startIndex(now time.Time) int {
	// 一个候选都没有 ⟹ **没有下标可报**，返 -1 让调用方显式判。
	//
	// ⚠️ 这和下面「全都有结局了」那条 return 0 **不是同一件事**，所以不能共用一个返回值：全熔断时报首选
	// 仍然有意义（cooldown 过后下一次调用会先去半开探测它，那就是真正起手请求的那个），而候选为空时
	// `f.members[0]` 直接越界。曾经两者都返 0，越界风险全押在调用方的 len 守卫上——第二个调用方出现时
	// 就是 index out of range。
	//
	// len 的判据也只留这一份：ModelName 不再自己判（那是同一个判据的第二份拷贝，CLAUDE.md
	// 「必须与 X 保持一致」那条规约）。
	if len(f.members) == 0 {
		return -1
	}
	states := make([]memberState, len(f.members))
	for {
		i, wait, ok := nextMember(states, now)
		if !ok {
			return 0 // 全都有结局了（全熔断）：tryMembers 此时返 ErrAllMembersUnavailable，报首选
		}
		if wait > 0 {
			return i // 都要等：tryMembers 睡完 wait 之后起手请求的就是它
		}
		mem := f.members[i]
		if until, _, closed := mem.gate.closedUntil(now); closed {
			// 同 tryMembers：熔断中时探测名额在设门的那个调用手里，按熔断跳过；否则推迟到 T。
			if mem.breaker.Tripped() {
				states[i].done = true
			} else {
				states[i].deferred, states[i].readyAt = true, until
			}
			continue
		}
		if !mem.breaker.WouldAllow() {
			states[i].done = true
			continue
		}
		return i
	}
}

// StructuredCapability 让 FallbackChatModel 满足 `model.StructuredCapable`——
// 返回**所有成员能力的交集**。
//
// # 为什么必须是交集而不是首个成员
//
// 装饰器随时可能把请求切到任一成员上。若按首成员报"支持 tool_choice=required"，
// 而备选里有个不支持的，切过去那次请求就带着它不认的参数 → 直接失败，且这是
// fallback 本该兜住的场景反被它自己制造出来。取交集 = "所有成员都担保得起的能力"。
//
// **不实现这个接口的后果**（本方法补的正是它）：`agent_v2` 用类型断言探能力
// （`structured_output.go` 的 `m.(model.StructuredCapable)`），装饰器不实现就返 false
// → 保守降级、不下 `tool_choice=required`。结构化输出仍能跑，但**丢了"强制经工具
// 收尾"这层保证**，退化成靠模型自觉——静默且难察觉（`example/harness/
// structured_smoke` 就踩在这上面：它用 WithOutputSchema + 本装饰器，场景全过靠的是
// 模型自觉遵守）。
//
// 成员不实现 `StructuredCapable` 时按**全 false** 计入交集（保守）：未知能力不能
// 假定支持。`DefaultMode` 取首个成员的（agent 层当前不消费它，见其字段 godoc）。
func (f *FallbackChatModel) StructuredCapability(modelName string) model.StructuredCapability {
	if f == nil || len(f.members) == 0 {
		return model.StructuredCapability{}
	}
	var out model.StructuredCapability
	for i, mem := range f.members {
		sc, ok := mem.model.(model.StructuredCapable)
		if !ok {
			// 成员不自报能力 ⟹ 交集立即塌成全 false（保守，不猜）
			return model.StructuredCapability{}
		}
		cur := sc.StructuredCapability(modelName)
		if i == 0 {
			out = cur
			continue
		}
		out.SupportsJSONSchema = out.SupportsJSONSchema && cur.SupportsJSONSchema
		out.SupportsJSONObject = out.SupportsJSONObject && cur.SupportsJSONObject
		out.SupportsToolChoiceRequired = out.SupportsToolChoiceRequired && cur.SupportsToolChoiceRequired
		out.SupportsStrictTool = out.SupportsStrictTool && cur.SupportsStrictTool
	}
	return out
}
