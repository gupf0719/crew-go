package fallback

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// 要等很久的候选先推迟、去试别的候选；推迟不是放弃——别的候选都没成时回来，把它没用完的重试用完（见 tryMembers）。
// 白盒：把 60 秒的阈值调到 50 毫秒，不用真等一分钟。

// seqModel 按第几次调用决定成败：respond 返回 nil 表示成功（答复 "<name> ok"）。
type seqModel struct {
	name    string
	respond func(n int) error
	mu      sync.Mutex
	calls   int
}

func (m *seqModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	n := m.calls
	m.calls++
	m.mu.Unlock()
	if err := m.respond(n); err != nil {
		return nil, err
	}
	return schema.AssistantMessage(m.name + " ok"), nil
}

func (m *seqModel) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(msg), nil
}

func (m *seqModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *seqModel) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func setRetryAfterLimit(t *testing.T, d time.Duration) {
	t.Helper()
	old := memberRetryAfterLimit
	memberRetryAfterLimit = d
	t.Cleanup(func() { memberRetryAfterLimit = old })
}

// rateLimited 是一次 429，要求等 ra。
func rateLimited(ra time.Duration) error {
	return &model.APIError{Provider: "p", StatusCode: 429, RetryAfter: ra}
}

// firstThenOK：第一次按 err 失败，之后成功。
func firstThenOK(err error) func(int) error {
	return func(n int) error {
		if n == 0 {
			return err
		}
		return nil
	}
}

func always(err error) func(int) error { return func(int) error { return err } }

func generate(t *testing.T, f *FallbackChatModel, timeout time.Duration) (*schema.Message, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return f.Generate(ctx, []*schema.Message{schema.UserMessage("q")})
}

// 第一个候选推迟（要等 100 毫秒，超过阈值），第二个一直 503：回到第一个，等到能请求时把重试用完、成功。曾经换走
// 之后不再回来，调用方给的时间明明够，结果立刻失败（审计实测）。
func TestDefer_RevisitedWhenOthersFail(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: firstThenOK(rateLimited(100 * time.Millisecond))}
	second := &seqModel{name: "b", respond: always(&model.APIError{Provider: "p", StatusCode: 503})}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}))
	start := time.Now()
	out, err := generate(t, f, 2*time.Second)
	if err != nil || out.Content != "a ok" {
		t.Fatalf("第二个候选失败后应回到第一个、等到能请求时成功：out=%v err=%v", out, err)
	}
	if d := time.Since(start); d < 90*time.Millisecond {
		t.Fatalf("回来之前应等到 Retry-After 到期，只等了 %v", d)
	}
	if a, b := first.count(), second.count(); a != 2 || b != 2 {
		t.Fatalf("第一个候选应请求 2 次（推迟前 1 次、回来 1 次）、第二个 2 次（1 + 重试 1），实际 %d / %d", a, b)
	}
}

// 第二个（最后一个）候选也要等很久、而且比第一个久：先回到更早能请求的第一个，不在更久的那个上干等。曾经最后一个
// 候选照 Retry-After 等，等的是两个 Retry-After 里更长的那个（审计实测）。
func TestDefer_WaitsForEarliestNotLongest(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: firstThenOK(rateLimited(100 * time.Millisecond))}
	second := &seqModel{name: "b", respond: always(rateLimited(10 * time.Minute))}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}))
	out, err := generate(t, f, 2*time.Second)
	if err != nil || out.Content != "a ok" {
		t.Fatalf("应回到更早能请求的第一个候选并成功：out=%v err=%v", out, err)
	}
	if b := second.count(); b != 1 {
		t.Fatalf("第二个候选要等 10 分钟，应只请求 1 次（推迟），实际 %d", b)
	}
}

// 后面的候选此刻都请求不了（熔断中）：推迟的候选马上成为最早能请求的那个，照样回来等、重试——不会既没等也没换。
// 曾经判断「后面能换」与真去换之间探测名额被并发请求占掉时，直接返回了限流错误（审计实测）。
func TestDefer_NothingElseToTryStillWaits(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: firstThenOK(rateLimited(100 * time.Millisecond))}
	second := &seqModel{name: "b", respond: always(nil)}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}), WithBreaker(1, time.Hour))
	f.members[1].breaker.RecordFailure() // 第二个候选熔断，这次调用里请求不了
	out, err := generate(t, f, 2*time.Second)
	if err != nil || out.Content != "a ok" {
		t.Fatalf("没有别的候选可试时应等到第一个能请求并成功：out=%v err=%v", out, err)
	}
	if b := second.count(); b != 0 {
		t.Fatalf("熔断中的候选不该被请求，实际 %d 次", b)
	}
}

// noSwitch429 是不在 429 上换候选的切换判据（备用候选贵，宁可等）。
type noSwitch429 struct{}

func (noSwitch429) ShouldFailover(_ context.Context, err error) bool {
	var apiErr *model.APIError
	return !(errors.As(err, &apiErr) && apiErr.StatusCode == 429)
}

// 切换判据说这个错不换候选：要等很久也只在原候选上等（等得完的话），不推迟、不去请求别的候选。曾经「后面有能换的」
// 不看切换判据，Retry-After 超过阈值就直接返回，既没等也没换（审计实测）。
func TestDefer_PolicyNoSwitchWaitsInPlace(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: firstThenOK(rateLimited(100 * time.Millisecond))}
	second := &seqModel{name: "b", respond: always(nil)}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}), WithFailoverPolicy(noSwitch429{}))
	out, err := generate(t, f, 2*time.Second)
	if err != nil || out.Content != "a ok" {
		t.Fatalf("切换判据不换 429 时应在原候选上等完重试：out=%v err=%v", out, err)
	}
	if b := second.count(); b != 0 {
		t.Fatalf("切换判据不换时不该请求别的候选，实际 %d 次", b)
	}
}

// 挑候选按「此刻能请求的里排序最靠前的」：推迟的第一个候选到期时，优先于还没试过、排在后面的候选。
func TestDefer_ReadyEarlierMemberPreferred(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: firstThenOK(rateLimited(60 * time.Millisecond))}
	second := &seqModel{name: "b", respond: func(int) error {
		time.Sleep(120 * time.Millisecond) // 慢慢失败：失败时第一个候选已经到期
		return &model.APIError{Provider: "p", StatusCode: 503}
	}}
	third := &seqModel{name: "c", respond: always(nil)}
	f := New([]model.ChatModel{first, second, third}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}))
	out, err := generate(t, f, 2*time.Second)
	if err != nil || out.Content != "a ok" {
		t.Fatalf("到期的第一个候选应排在没试过的第三个前面：out=%v err=%v", out, err)
	}
	if c := third.count(); c != 0 {
		t.Fatalf("第一个候选成功了，第三个不该被请求，实际 %d 次", c)
	}
}

// 门（第 1 条）：第一次调用收到长 Retry-After 设下门，之后的调用 T 之前不请求它；门不是失败，不记断路器。
func TestGate_LaterCallsSkipUntilT(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: always(rateLimited(time.Minute))}
	second := &seqModel{name: "b", respond: always(nil)}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}), WithBreaker(1, time.Hour))
	// 期限要长于 Retry-After：等完已过期限的不推迟，那样测不到推迟这条路
	if out, err := generate(t, f, 2*time.Minute); err != nil || out.Content != "b ok" {
		t.Fatalf("第一次调用应由第二个候选答复：out=%v err=%v", out, err)
	}
	if out, err := generate(t, f, 2*time.Second); err != nil || out.Content != "b ok" {
		t.Fatalf("第二次调用应直接由第二个候选答复：out=%v err=%v", out, err)
	}
	if a := first.count(); a != 1 {
		t.Fatalf("T 之前不该再请求第一个候选，实际 %d 次", a)
	}
	if f.members[0].breaker.Tripped() {
		t.Fatal("门不是失败，不该记断路器（maxFailures=1 时一记就熔断）")
	}
}

// 半开的候选回 429 设门：放掉它占着的探测名额、不记失败；这次调用回来时请求它，成功后恢复。
func TestDefer_HalfOpenMemberRevisitable(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	var mode sync.Map
	mode.Store("a", "fail")
	first := &seqModel{name: "a", respond: func(n int) error {
		m, _ := mode.Load("a")
		switch m.(string) {
		case "fail":
			return &model.APIError{Provider: "p", StatusCode: 503}
		case "ra":
			mode.Store("a", "ok")
			return rateLimited(100 * time.Millisecond)
		default:
			return nil
		}
	}}
	second := &seqModel{name: "b", respond: always(&model.APIError{Provider: "p", StatusCode: 503})}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}), WithBreaker(1, 300*time.Millisecond))
	_, _ = generate(t, f, 2*time.Second) // 两个候选都失败、都熔断
	time.Sleep(320 * time.Millisecond)   // 冷却过去：都进半开
	mode.Store("a", "ra")
	out, err := generate(t, f, 2*time.Second)
	if err != nil || out.Content != "a ok" {
		t.Fatalf("半开的候选推迟后应能回来成功：out=%v err=%v", out, err)
	}
	if f.members[0].breaker.Tripped() {
		t.Fatal("回来成功之后第一个候选应恢复（不再熔断）")
	}
}

// 门当场生效（第 1 条）：这次调用还在别的候选上的时候，别的请求已经看到门、T 之前不再打它；门也不记断路器。曾经推迟
// 时不记、调用结束时补记，这次调用跑完之前别的请求照样一个个打到它（审计实测 15 个并发请求全部打到半开的候选）。
func TestGate_VisibleToConcurrentCalls(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: always(rateLimited(time.Minute))}
	entered := make(chan struct{})
	release := make(chan struct{})
	second := &seqModel{name: "b", respond: func(n int) error {
		if n == 0 {
			close(entered)
			<-release // 第一次调用停在第二个候选上
		}
		return nil
	}}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}), WithBreaker(1, time.Hour))
	done := make(chan error, 1)
	go func() {
		_, err := generate(t, f, 2*time.Minute)
		done <- err
	}()
	<-entered
	// 第一次调用给第一个候选设了门、正在第二个候选上：另一个请求不该再打第一个候选
	if out, err := generate(t, f, 2*time.Second); err != nil || out.Content != "b ok" {
		t.Fatalf("另一个请求应由第二个候选答复：out=%v err=%v", out, err)
	}
	if a := first.count(); a != 1 {
		t.Fatalf("门应当场生效、别的请求 T 之前不再打它，实际第一个候选被请求 %d 次", a)
	}
	if f.members[0].breaker.Tripped() {
		t.Fatal("门不是失败，第一个候选不该熔断")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("第一次调用应由第二个候选答复：%v", err)
	}
}

// 设门的响应不算失败（第 1 条），回来那次又设门也不算：maxFailures=1 时第一个候选照样不熔断；第二个候选的 503 照常记。
func TestGate_NotRecordedAsFailure(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: always(rateLimited(80 * time.Millisecond))}
	second := &seqModel{name: "b", respond: always(&model.APIError{Provider: "p", StatusCode: 503})}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}), WithBreaker(1, time.Hour))
	if _, err := generate(t, f, 2*time.Second); err == nil {
		t.Fatal("两个候选都失败，应返回错误")
	}
	if a := first.count(); a != 2 {
		t.Fatalf("前置：第一个候选应推迟一次、回来一次，实际请求 %d 次", a)
	}
	if f.members[0].breaker.Tripped() {
		t.Fatal("设门的响应不是失败，第一个候选不该熔断")
	}
	if !f.members[1].breaker.Tripped() {
		t.Fatal("第二个候选的 503 应照常记失败（maxFailures=1 时熔断）")
	}
}

// 回到推迟的候选不经过断路器（第 3 条）：等它的时候别的请求让它真失败、熔断了，这次调用等完照样请求它。曾经回来时
// 重新问断路器，等满 Retry-After 之后被挡住、一次没请求就失败（审计实测）。
func TestDefer_RevisitIgnoresBreakerTrippedByOthers(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: firstThenOK(rateLimited(150 * time.Millisecond))}
	entered := make(chan struct{})
	release := make(chan struct{})
	second := &seqModel{name: "b", respond: func(n int) error {
		if n == 0 {
			close(entered)
			<-release
		}
		return &model.APIError{Provider: "p", StatusCode: 503}
	}}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}), WithBreaker(1, time.Hour))
	type result struct {
		out *schema.Message
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := generate(t, f, 2*time.Minute)
		done <- result{out, err}
	}()
	<-entered
	f.members[0].breaker.RecordFailure() // 别的请求在此期间让第一个候选真失败、熔断（maxFailures=1）
	close(release)
	r := <-done
	if r.err != nil || r.out.Content != "a ok" {
		t.Fatalf("等完 Retry-After 应照样请求第一个候选并成功：out=%v err=%v", r.out, r.err)
	}
}

// 只被门挡过、这次调用还没请求过的候选，门开之后照常经过断路器（第 3 条）：它在此期间因真失败熔断时，不去请求它。不然
// T 一到，所有被门挡过的调用一起绕过断路器去请求一个熔断中的候选。
func TestGate_GateOnlyMemberGoesThroughBreaker(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: always(rateLimited(100 * time.Millisecond))}
	second := &seqModel{name: "b", respond: always(&model.APIError{Provider: "p", StatusCode: 503})}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 0}), WithBreaker(1, time.Hour))
	_, _ = generate(t, f, 60*time.Millisecond) // 第一次调用设下门（重试为 0，不回来）
	if a := first.count(); a != 1 {
		t.Fatalf("前置：第一次调用应请求第一个候选 1 次，实际 %d 次", a)
	}
	f.members[0].breaker.RecordFailure() // 别的请求让它真失败、熔断
	// 第二次调用：T 之前被门挡住（不请求），门开之后经过断路器——熔断中，不请求
	if _, err := generate(t, f, 2*time.Second); err == nil {
		t.Fatal("两个候选都不可用，应返回错误")
	}
	if a := first.count(); a != 1 {
		t.Fatalf("熔断中的第一个候选在门开之后不该被请求，实际共请求 %d 次", a)
	}
}

// 推迟期间试的候选报「换候选没用」（第 4 条）：排在它后面没试过的不再试，排在前面推迟着的照样回去请求。曾经直接返回：
// 第一个候选 100 毫秒后就能用，调用以备用候选的 401 结束（审计实测）。
func TestDefer_NoSwitchErrorDuringDeferralRevisitsDeferred(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: firstThenOK(rateLimited(100 * time.Millisecond))}
	second := &seqModel{name: "b", respond: always(&model.APIError{Provider: "p", StatusCode: 401, Body: "invalid api key"})}
	third := &seqModel{name: "c", respond: always(nil)}
	f := New([]model.ChatModel{first, second, third}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}))
	out, err := generate(t, f, 2*time.Second)
	if err != nil || out.Content != "a ok" {
		t.Fatalf("推迟着的第一个候选应照样回去请求并成功：out=%v err=%v", out, err)
	}
	if c := third.count(); c != 0 {
		t.Fatalf("报了「换候选没用」之后不该再试排在后面的候选，第三个候选被请求 %d 次", c)
	}
}

// 同上，推迟着的候选回来也失败：返回那个「换候选没用」的错误（原地等的话交给调用方的也是它），不是第一个候选的 429。
func TestDefer_NoSwitchErrorReturnedWhenDeferredFailsToo(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: always(rateLimited(100 * time.Millisecond))}
	second := &seqModel{name: "b", respond: always(&model.APIError{Provider: "p", StatusCode: 401, Body: "invalid api key"})}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}))
	_, err := generate(t, f, 2*time.Second)
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 401 {
		t.Fatalf("应返回那个「换候选没用」的错误（401），实际 %v", err)
	}
	if a := first.count(); a != 2 {
		t.Fatalf("推迟着的第一个候选应回去请求一次，实际共请求 %d 次", a)
	}
}

// 回到推迟的候选报「换候选没用」时，排在它后面推迟着的不再回去（第 4 条）：原地等会在第一个候选的 401 处返回。曾经不看
// 先后，又等到第二个候选到期、请求它一次才返回同一个 401（审计实测）。
func TestDefer_NoSwitchStopsLaterDeferred(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: func(n int) error {
		if n == 0 {
			return rateLimited(60 * time.Millisecond)
		}
		return &model.APIError{Provider: "p", StatusCode: 401, Body: "invalid api key"}
	}}
	second := &seqModel{name: "b", respond: func(n int) error {
		if n == 0 {
			return rateLimited(400 * time.Millisecond)
		}
		return &model.APIError{Provider: "p", StatusCode: 503}
	}}
	third := &seqModel{name: "c", respond: always(&model.APIError{Provider: "p", StatusCode: 503})}
	f := New([]model.ChatModel{first, second, third}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}))
	start := time.Now()
	_, err := generate(t, f, 2*time.Second)
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 401 {
		t.Fatalf("应返回第一个候选的 401：%v", err)
	}
	if b := second.count(); b != 1 {
		t.Fatalf("第一个候选报了「换候选没用」，排在它后面的第二个候选不该再回去请求，实际请求 %d 次", b)
	}
	if d := time.Since(start); d > 300*time.Millisecond {
		t.Fatalf("不该等到第二个候选到期（400 毫秒）才返回，实际 %v", d)
	}
}

// 两个候选都推迟、第三个报「换候选没用」（401）；第一个回来报上下文超限（也是换候选没用）：返回排得最靠前的那个——
// 上下文超限，ReactiveCompact 才认得出（第 4 条）。曾经返回时间上第一个「换候选没用」的错，即第三个候选的 401（审计
// 实测）。
func TestDefer_NoSwitchReturnsEarliestIndex(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	cwe := &model.APIError{Provider: "p", Err: model.ErrContextWindowExceeded}
	first := &seqModel{name: "a", respond: func(n int) error {
		if n == 0 {
			return rateLimited(100 * time.Millisecond)
		}
		return cwe
	}}
	second := &seqModel{name: "b", respond: func(n int) error {
		if n == 0 {
			return rateLimited(150 * time.Millisecond)
		}
		return &model.APIError{Provider: "p", StatusCode: 503}
	}}
	third := &seqModel{name: "c", respond: always(&model.APIError{Provider: "p", StatusCode: 401, Body: "invalid api key"})}
	f := New([]model.ChatModel{first, second, third}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}))
	_, err := generate(t, f, 2*time.Second)
	if !errors.Is(err, model.ErrContextWindowExceeded) {
		t.Fatalf("应返回第一个候选的上下文超限：%v", err)
	}
}

// 后报的「换候选没用」覆盖先报的（第 4 条）：第三个报 401 之后，先到期回来的第二个报上下文超限，第一个回来 503。原地等
// 的次序是第一个（503）→ 第二个（上下文超限，返回），所以返回的是上下文超限，不是先报出来的 401。
func TestDefer_NoSwitchOverwrittenByEarlierMember(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: func(n int) error {
		if n == 0 {
			return rateLimited(150 * time.Millisecond)
		}
		return &model.APIError{Provider: "p", StatusCode: 503}
	}}
	second := &seqModel{name: "b", respond: func(n int) error {
		if n == 0 {
			return rateLimited(60 * time.Millisecond)
		}
		return &model.APIError{Provider: "p", Err: model.ErrContextWindowExceeded}
	}}
	third := &seqModel{name: "c", respond: always(&model.APIError{Provider: "p", StatusCode: 401, Body: "invalid api key"})}
	f := New([]model.ChatModel{first, second, third}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}))
	_, err := generate(t, f, 2*time.Second)
	if !errors.Is(err, model.ErrContextWindowExceeded) {
		t.Fatalf("应返回排得更靠前的第二个候选的上下文超限，不是第三个候选的 401：%v", err)
	}
	if a := first.count(); a != 2 {
		t.Fatalf("排在最前的第一个候选应回去请求一次，实际共请求 %d 次", a)
	}
}

// 回到推迟的候选时没有占它的探测名额：回来那次报「换候选没用」时不放名额，不然别的请求正占着的探测名额被放掉，又一个
// 请求能同时去探测。时序：第一次调用请求它、收到 429 设门（T=150 毫秒）并推迟；它在此期间因真失败熔断（冷却 100 毫秒）；
// 门开、冷却过后另一个请求占住探测名额；第一次调用回来，那一次报 400 → 第三个请求来时探测名额仍被占着（占着不到一个
// 冷却期，不算过期），不该再去探测。
func TestDefer_RevisitDoesNotReleaseOthersProbe(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	yEntered := make(chan struct{})
	yRelease := make(chan struct{})
	first := &seqModel{name: "a", respond: func(n int) error {
		switch n {
		case 0:
			return rateLimited(150 * time.Millisecond) // 第一次调用：设门、推迟
		case 1:
			close(yEntered) // 另一个请求拿到探测名额、停在这里
			<-yRelease
			return nil
		case 2:
			return &model.APIError{Provider: "p", StatusCode: 400, Body: "bad request"} // 第一次调用回来：换候选没用
		default:
			return nil
		}
	}}
	xEntered := make(chan struct{})
	xRelease := make(chan struct{})
	second := &seqModel{name: "b", respond: func(n int) error {
		if n == 0 {
			close(xEntered)
			<-xRelease
			// 换候选没用：不记断路器，推迟着的第一个候选照样回去请求
			return &model.APIError{Provider: "p", StatusCode: 400, Body: "bad request"}
		}
		return nil
	}}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}), WithBreaker(1, 100*time.Millisecond))
	xDone := make(chan error, 1)
	go func() {
		_, err := generate(t, f, 2*time.Second)
		xDone <- err
	}()
	<-xEntered
	f.members[0].breaker.RecordFailure() // 第一个候选在此期间因真失败熔断（冷却 100 毫秒）
	time.Sleep(160 * time.Millisecond)   // 门开（150 毫秒）、冷却过去：第一个候选半开
	yDone := make(chan error, 1)
	go func() {
		_, err := generate(t, f, 2*time.Second)
		yDone <- err
	}()
	<-yEntered // 另一个请求占着探测名额
	close(xRelease)
	if err := <-xDone; err == nil {
		t.Fatal("前置：第一次调用应以「换候选没用」的错误结束")
	}
	if a := first.count(); a != 3 {
		t.Fatalf("前置：第一次调用应回来请求过第一个候选，实际第一个候选被请求 %d 次（应为 3）", a)
	}
	// 第三个请求：探测名额仍被另一个请求占着，不该再去探测第一个候选
	if out, err := generate(t, f, 2*time.Second); err != nil || out.Content != "b ok" {
		t.Fatalf("第三个请求应跳过第一个候选、由第二个候选答复：out=%v err=%v", out, err)
	}
	if a := first.count(); a != 3 {
		t.Fatalf("第三个请求不该同时去探测第一个候选，第一个候选被请求 %d 次（应为 3）", a)
	}
	close(yRelease)
	if err := <-yDone; err != nil {
		t.Fatalf("占着探测名额的请求应成功：%v", err)
	}
}

// 并发（第 1 条）：20 个调用陆续到达，第一个候选在开头 100 毫秒里对所有请求回 429 + Retry-After 100 毫秒、之后成功，第二个
// 一直 503。所有调用都该成功：T 之前被门挡住的调用推迟它，T 到了回来。曾经推迟当场记失败，候选被这些推迟熔断，之后到达
// 的调用在断路器那里直接跳过它，20 个只成功 3 个（审计实测）。
func TestGate_ConcurrentCallsAllSucceed(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	start := time.Now()
	first := &seqModel{name: "a", respond: func(int) error {
		if time.Since(start) < 100*time.Millisecond {
			return rateLimited(100 * time.Millisecond)
		}
		return nil
	}}
	second := &seqModel{name: "b", respond: func(int) error {
		time.Sleep(10 * time.Millisecond)
		return &model.APIError{Provider: "p", StatusCode: 503}
	}}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failed []error
	for k := 0; k < 20; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := generate(t, f, 2*time.Second); err != nil {
				mu.Lock()
				failed = append(failed, err)
				mu.Unlock()
			}
		}()
		time.Sleep(2 * time.Millisecond)
	}
	wg.Wait()
	if len(failed) > 0 {
		t.Fatalf("%d 个调用失败（期限 2 秒，第一个候选 100 毫秒后就能用），例如：%v", len(failed), failed[0])
	}
	if f.members[0].breaker.Tripped() {
		t.Fatal("门不是失败，第一个候选不该熔断")
	}
}

// 等不到 T 的调用不请求被门挡住的候选；别的候选也失败时直接返回设门的那个 429（带 Retry-After），不等，也不报
// ErrAllMembersUnavailable。
func TestGate_BeyondDeadlineReturnsGateError(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: always(rateLimited(time.Minute))}
	second := &seqModel{name: "b", respond: func(n int) error {
		if n == 0 {
			return nil
		}
		return &model.APIError{Provider: "p", StatusCode: 503}
	}}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}))
	if out, err := generate(t, f, 2*time.Minute); err != nil || out.Content != "b ok" {
		t.Fatalf("前置：第一次调用应由第二个候选答复：out=%v err=%v", out, err)
	}
	start := time.Now()
	_, err := generate(t, f, 200*time.Millisecond)
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 429 {
		t.Fatalf("应返回设门的那个 429：%v", err)
	}
	if a := first.count(); a != 1 {
		t.Fatalf("等不到 T 的调用不该请求第一个候选，实际共请求 %d 次", a)
	}
	if d := time.Since(start); d > 150*time.Millisecond {
		t.Fatalf("等不到 T 不该等，实际用了 %v", d)
	}
}

// 半开的候选回 429 设门时放掉它占着的探测名额（第 1 条）：这次调用由别的候选答复、不回来，门开之后新来的调用照样能去
// 探测它。不放的话名额要占满一个冷却期，候选明明能用也被跳过。
func TestGate_ReleasesProbe(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: firstThenOK(rateLimited(60 * time.Millisecond))}
	second := &seqModel{name: "b", respond: always(nil)}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}), WithBreaker(1, 100*time.Millisecond))
	f.members[0].breaker.RecordFailure() // 第一个候选熔断
	time.Sleep(110 * time.Millisecond)   // 冷却过去：半开
	if out, err := generate(t, f, 30*time.Millisecond); err != nil || out.Content != "b ok" {
		t.Fatalf("前置：第一次调用（拿探测名额请求第一个候选、收到 429 设门）应由第二个候选答复：out=%v err=%v", out, err)
	}
	time.Sleep(70 * time.Millisecond) // 门开；探测名额要是没放，还要再占约 30 毫秒
	if out, err := generate(t, f, 2*time.Second); err != nil || out.Content != "a ok" {
		t.Fatalf("门开之后应能去探测第一个候选并成功：out=%v err=%v", out, err)
	}
}

// 被门挡住、等不到 T 的调用返回的 Retry-After 是门还要关多久，不是设门时的原值（第 1 条）：外层照原值等会多等一整个
// Retry-After（审计实测原值 1 秒、门只剩 99 毫秒，外层睡了 1 秒）。
func TestGate_BlockedCallReportsRemainingRetryAfter(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: always(rateLimited(300 * time.Millisecond))}
	second := &seqModel{name: "b", respond: func(n int) error {
		if n == 0 {
			return nil
		}
		return &model.APIError{Provider: "p", StatusCode: 503}
	}}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}))
	if out, err := generate(t, f, 2*time.Second); err != nil || out.Content != "b ok" {
		t.Fatalf("前置：第一次调用应由第二个候选答复：out=%v err=%v", out, err)
	}
	time.Sleep(200 * time.Millisecond) // 门还剩约 100 毫秒
	_, err := generate(t, f, 50*time.Millisecond)
	ra := compose_v2.RetryAfterFromError(err)
	if ra <= 0 || ra > 150*time.Millisecond {
		t.Fatalf("Retry-After 应是门还要关多久（约 100 毫秒），实际 %v（err=%v）", ra, err)
	}
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 429 {
		t.Fatalf("错误链上应取得到设门的那个 429：%v", err)
	}
}

// 门开之后这次调用自己请求了它、拿到 503：以自己拿到的错为准，不是别的调用设门时的那个 429（第 1 条）。曾经返回过时的
// 429 和它的 Retry-After，外层照着再等一轮，而门早就开了（审计实测）。
func TestGate_OwnErrorSupersedesGateError(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: func(n int) error {
		if n == 0 {
			return rateLimited(150 * time.Millisecond) // 第一次调用设门
		}
		return &model.APIError{Provider: "p", StatusCode: 503}
	}}
	second := &seqModel{name: "b", respond: func(n int) error {
		if n == 0 {
			return nil
		}
		return &model.APIError{Provider: "p", StatusCode: 502}
	}}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}))
	if out, err := generate(t, f, 2*time.Second); err != nil || out.Content != "b ok" {
		t.Fatalf("前置：第一次调用应由第二个候选答复：out=%v err=%v", out, err)
	}
	// 第二次调用：第一个候选被门挡住（推迟），第二个候选 502；门开之后回来请求第一个候选，两次都 503
	_, err := generate(t, f, 2*time.Second)
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 503 {
		t.Fatalf("应返回第一个候选自己回的 503，不是设门时的 429：%v", err)
	}
	if a := first.count(); a != 3 {
		t.Fatalf("第一个候选应被请求 3 次（设门 1 次、第二次调用回来 2 次），实际 %d 次", a)
	}
}

// 熔断中（半开）的候选被门挡住时按熔断跳过，不等到 T（第 1 条）：探测名额在设门的那个调用手里，原地等的话别的调用同样
// 拿不到名额、当场跳过。曾经被门挡过的调用都等到 T，再一起去问断路器，只有一个拿到探测，其余等完才失败（审计实测
// 20 个调用 18 个等到 T 之后失败）。
func TestGate_TrippedMemberSkippedNotWaited(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: firstThenOK(rateLimited(300 * time.Millisecond))}
	second := &seqModel{name: "b", respond: always(&model.APIError{Provider: "p", StatusCode: 503})}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 0}), WithBreaker(1, 50*time.Millisecond))
	f.members[0].breaker.RecordFailure() // 第一个候选熔断
	time.Sleep(60 * time.Millisecond)    // 冷却过去：半开
	// 第一次调用拿探测名额请求它，收到 429 设门（期限短，不回来）
	_, _ = generate(t, f, 20*time.Millisecond)
	if a := first.count(); a != 1 {
		t.Fatalf("前置：第一次调用应请求第一个候选 1 次，实际 %d 次", a)
	}
	start := time.Now()
	_, err := generate(t, f, 2*time.Second)
	if err == nil {
		t.Fatal("第一个候选熔断中、第二个 503，应返回错误")
	}
	if d := time.Since(start); d > 150*time.Millisecond {
		t.Fatalf("熔断中的候选被门挡住应当场跳过，不该等到 T（300 毫秒），实际用了 %v", d)
	}
	if a := first.count(); a != 1 {
		t.Fatalf("熔断中的第一个候选不该被请求，实际共请求 %d 次", a)
	}
}

// 被门挡住、改去请求别的候选也是一次切换：照常发切换回调（第 1 条）。曾经门关着的整段时间里只有设门那一次切换，看板上
// 看不出流量都在备用候选上（审计实测 10 次调用只有 1 次切换）。
func TestGate_DivertEmitsSwitch(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: always(rateLimited(time.Minute))}
	second := &seqModel{name: "b", respond: always(nil)}
	var mu sync.Mutex
	var switches []string
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}),
		WithFallbackHook(func(_ context.Context, from, to int, _ error) {
			mu.Lock()
			defer mu.Unlock()
			switches = append(switches, fmt.Sprintf("%d->%d", from, to))
		}))
	for k := 0; k < 4; k++ {
		if out, err := generate(t, f, 2*time.Minute); err != nil || out.Content != "b ok" {
			t.Fatalf("第 %d 次调用应由第二个候选答复：out=%v err=%v", k+1, out, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(switches) != 4 {
		t.Fatalf("4 次调用都从第一个候选切到第二个，应有 4 次切换回调，实际 %v", switches)
	}
	if a := first.count(); a != 1 {
		t.Fatalf("门关着时不该再请求第一个候选，实际 %d 次", a)
	}
}

// 回到推迟的候选又收到长 Retry-After：门往后推，这次调用（期限够）再推迟一次，回来成功。
func TestGate_ReGatedOnRevisit(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: func(n int) error {
		if n < 2 {
			return rateLimited(100 * time.Millisecond)
		}
		return nil
	}}
	second := &seqModel{name: "b", respond: always(&model.APIError{Provider: "p", StatusCode: 503})}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 2}))
	start := time.Now()
	out, err := generate(t, f, 2*time.Second)
	if err != nil || out.Content != "a ok" {
		t.Fatalf("应在第二次门开之后成功：out=%v err=%v", out, err)
	}
	if a := first.count(); a != 3 {
		t.Fatalf("第一个候选应请求 3 次（设门、回来又设门、回来成功），实际 %d 次", a)
	}
	if d := time.Since(start); d < 190*time.Millisecond {
		t.Fatalf("应等满两个 Retry-After，只用了 %v", d)
	}
}

// WithTools 派生的模型跟原模型共用门：经原模型的调用设下门之后，经派生模型的调用 T 之前也不请求它。
func TestGate_SharedWithWithTools(t *testing.T) {
	setRetryAfterLimit(t, 50*time.Millisecond)
	first := &seqModel{name: "a", respond: always(rateLimited(time.Minute))}
	second := &seqModel{name: "b", respond: always(nil)}
	f := New([]model.ChatModel{first, second}, WithPerModelRetry(PerModelRetry{MaxRetries: 1}))
	bound, err := f.WithTools(nil)
	if err != nil {
		t.Fatal(err)
	}
	if out, err := generate(t, f, 2*time.Minute); err != nil || out.Content != "b ok" {
		t.Fatalf("前置：第一次调用应由第二个候选答复：out=%v err=%v", out, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if out, err := bound.Generate(ctx, []*schema.Message{schema.UserMessage("q")}); err != nil || out.Content != "b ok" {
		t.Fatalf("经派生模型的调用应由第二个候选答复：out=%v err=%v", out, err)
	}
	if a := first.count(); a != 1 {
		t.Fatalf("派生模型 T 之前不该请求第一个候选，实际共请求 %d 次", a)
	}
}
