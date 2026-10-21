package fallback_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/fallback"
	"crew-go/model"
	"crew-go/schema"
)

// stubModel 是可编程的 model.ChatModel 测试桩——按调用次序返回预设响应，并记录
// 调用次数 + 捕获每次输入（CLAUDE.md：mock 必须读 input 做断言，否则等价单测）。
type stubModel struct {
	name string

	mu       sync.Mutex
	calls    int
	captured [][]*schema.Message
	tools    []*schema.ToolInfo

	// respond 决定第 n 次调用（从 0 起）返回什么；nil 时恒成功。
	respond func(n int, in []*schema.Message) (*schema.Message, error)
}

func (s *stubModel) record(in []*schema.Message) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.calls
	s.calls++
	cp := make([]*schema.Message, len(in))
	copy(cp, in)
	s.captured = append(s.captured, cp)
	return n
}

func (s *stubModel) Generate(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	n := s.record(in)
	if s.respond != nil {
		if msg, err := s.respond(n, in); err != nil {
			return nil, err
		} else if msg != nil {
			return msg, nil
		}
	}
	return &schema.Message{Role: schema.RoleAssistant, Content: s.name + " ok"}, nil
}

func (s *stubModel) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	n := s.record(in)
	if s.respond != nil {
		if _, err := s.respond(n, in); err != nil {
			return nil, err // 建流阶段失败
		}
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(&schema.Message{Role: schema.RoleAssistant, Content: s.name + " stream"})
	schema.CloseSend(sr)
	return sr, nil
}

func (s *stubModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	s.mu.Lock()
	s.tools = tools
	s.mu.Unlock()
	return s, nil // 测试桩复用同一指针，方便断言绑定后的调用计数
}

func (s *stubModel) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *stubModel) lastInput() []*schema.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.captured) == 0 {
		return nil
	}
	return s.captured[len(s.captured)-1]
}

func msgs() []*schema.Message { return []*schema.Message{{Role: schema.RoleUser, Content: "q"}} }

func apiErr(code int) error { return &model.APIError{Provider: "stub", StatusCode: code, Body: "boom"} }
func netErr(e error) error  { return &model.APIError{Provider: "stub", Err: e} }

func failWith(err error) func(int, []*schema.Message) (*schema.Message, error) {
	return func(int, []*schema.Message) (*schema.Message, error) { return nil, err }
}

// TestFallback_PrimarySuccess_NoSwitch 主候选成功时不碰备用。
func TestFallback_PrimarySuccess_NoSwitch(t *testing.T) {
	primary := &stubModel{name: "primary"}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup})

	out, err := fb.Generate(context.Background(), msgs())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.Content, "primary") {
		t.Fatalf("got %q, want primary", out.Content)
	}
	if backup.callCount() != 0 {
		t.Fatalf("backup 不该被调用，calls=%d", backup.callCount())
	}
}

// TestFallback_SwitchOn503 主候选 503 → 切备用成功；并断言备用收到原输入（mock 读 input）。
func TestFallback_SwitchOn503(t *testing.T) {
	primary := &stubModel{name: "primary", respond: failWith(apiErr(503))}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup})

	out, err := fb.Generate(context.Background(), msgs())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.Content, "backup") {
		t.Fatalf("got %q, want backup", out.Content)
	}
	if primary.callCount() != 1 || backup.callCount() != 1 {
		t.Fatalf("calls primary=%d backup=%d, want 1,1", primary.callCount(), backup.callCount())
	}
	if in := backup.lastInput(); len(in) != 1 || in[0].Content != "q" {
		t.Fatalf("备用未收到正确输入: %+v", in)
	}
}

// TestFallback_NoSwitchOn400 4xx（非 408/429）不切，立即抛。
func TestFallback_NoSwitchOn400(t *testing.T) {
	primary := &stubModel{name: "primary", respond: failWith(apiErr(400))}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup})

	_, err := fb.Generate(context.Background(), msgs())
	var ae *model.APIError
	if !errors.As(err, &ae) || ae.StatusCode != 400 {
		t.Fatalf("want 400 APIError, got %v", err)
	}
	if backup.callCount() != 0 {
		t.Fatalf("400 不该切，backup calls=%d", backup.callCount())
	}
}

// TestFallback_NoSwitchOnInterrupt HITL 控制流信号不切（否则吞中断）。
func TestFallback_NoSwitchOnInterrupt(t *testing.T) {
	ie := &compose_v2.InterruptError{Reason: "hitl"}
	primary := &stubModel{name: "primary", respond: failWith(ie)}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup})

	_, err := fb.Generate(context.Background(), msgs())
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("want InterruptError, got %v", err)
	}
	if backup.callCount() != 0 {
		t.Fatalf("interrupt 不该切，backup calls=%d", backup.callCount())
	}
}

// badPolicy 模拟业务自定义 policy 漏写控制流豁免——恒判"该切换"。真实事故形态：
// 业务经 WithFailoverPolicy 注入只按 HTTP 状态码分类的 policy，不知道
// InterruptError / ctx 取消是框架控制流协议（框架内部知识，不该要求业务复述）。
type badPolicy struct{}

func (badPolicy) ShouldFailover(context.Context, error) bool { return true }

// TestFallback_GuardOverridesCustomPolicy_Interrupt 框架层控制流 guard 不可被业务
// policy 覆盖——badPolicy 把 interrupt 误判"该切"，guard 必须拦住：不重试（primary
// 恰 1 次）、不切换（backup 零调用）、原样返回 InterruptError（HITL 信号完整）。
//
// sanity-revert: 还原 New() 里 guardedPolicy 包装（f.policy = cfg.policy 直赋）
// → badPolicy 生效，interrupt 被切到 backup 吞掉 → 本测 fail 精确指向"中断被吞掉"；
// 去掉 shouldRetryMember 开头的 IsRetryFutile → interrupt 在同一个候选上被重试（重试
// 判断不读 policy，自己先问 IsRetryFutile）→ 本测 fail 指向"interrupt 不该在 retry 层重试"。夹具必须用 badPolicy 而非默认 policy——默认
// DefaultFailoverPolicy 自带豁免会遮蔽 guard 层（R9 T3 双层防护遮蔽教训）。
func TestFallback_GuardOverridesCustomPolicy_Interrupt(t *testing.T) {
	ie := &compose_v2.InterruptError{Reason: "hitl"}
	primary := &stubModel{name: "primary", respond: failWith(ie)}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup},
		fallback.WithFailoverPolicy(badPolicy{}),
		fallback.WithPerModelRetry(fallback.PerModelRetry{MaxRetries: 2})) // 同时验证重试决策点也被 guard

	_, err := fb.Generate(context.Background(), msgs())
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("want InterruptError（信号完整传出），got %v", err)
	}
	if got := primary.callCount(); got != 1 {
		t.Fatalf("interrupt 不该在 retry 层重试，primary calls=%d want 1", got)
	}
	if got := backup.callCount(); got != 0 {
		t.Fatalf("interrupt 不该切换（吞中断），backup calls=%d want 0", got)
	}
}

// TestFallback_GuardOverridesCustomPolicy_CtxCanceled caller 自己的 ctx 死了时框架层
// 豁免——badPolicy 误判也不重试不切换（切了立即再失败）。
//
// ⚠️ **2026-08-26 改：原来传的是 `context.Background()`（活的 ctx）**，而断言写着
// 「ctx 已死」。那时判据是「错误链上有没有 ctx 错」，所以传活 ctx 也过——**测试和它
// 自己的注释说的不是一件事**。判据换成问 `ctx.Err()` 之后，要测「ctx 已死」就得真的
// 给一个死的。活 ctx + 后端内部 ctx 错该切，由 TestFallback_BackendCtxErr_StillSwitches 钉。
func TestFallback_GuardOverridesCustomPolicy_CtxCanceled(t *testing.T) {
	primary := &stubModel{name: "primary", respond: failWith(fmt.Errorf("call: %w", context.Canceled))}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup},
		fallback.WithFailoverPolicy(badPolicy{}),
		fallback.WithPerModelRetry(fallback.PerModelRetry{MaxRetries: 2}))

	deadCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := fb.Generate(deadCtx, msgs())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if got := primary.callCount(); got != 1 {
		t.Fatalf("ctx 取消不该重试，primary calls=%d want 1", got)
	}
	if got := backup.callCount(); got != 0 {
		t.Fatalf("ctx 取消不该切换，backup calls=%d want 0", got)
	}
}

// TestFallback_AllFail_ReturnsFirstError 全失败抛**第一个**（主候选）错误（§11.7）。
func TestFallback_AllFail_ReturnsFirstError(t *testing.T) {
	primary := &stubModel{name: "primary", respond: failWith(&model.APIError{Provider: "primaryprov", StatusCode: 503})}
	backup := &stubModel{name: "backup", respond: failWith(&model.APIError{Provider: "backupprov", StatusCode: 503})}
	fb := fallback.New([]model.ChatModel{primary, backup})

	_, err := fb.Generate(context.Background(), msgs())
	var ae *model.APIError
	if !errors.As(err, &ae) {
		t.Fatalf("want APIError, got %v", err)
	}
	if ae.Provider != "primaryprov" {
		t.Fatalf("want 主候选错误(primaryprov)，got provider %q", ae.Provider)
	}
}

// TestFallback_PerMemberRetry 配置 per-model 重试：503 在主候选重试到耗尽再切。
func TestFallback_PerMemberRetry(t *testing.T) {
	primary := &stubModel{name: "primary", respond: failWith(apiErr(503))}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup}, fallback.WithPerModelRetry(fallback.PerModelRetry{MaxRetries: 2}))

	_, err := fb.Generate(context.Background(), msgs())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if primary.callCount() != 3 {
		t.Fatalf("主候选应重试到 3 次(1+2)，got %d", primary.callCount())
	}
	if backup.callCount() != 1 {
		t.Fatalf("backup calls=%d, want 1", backup.callCount())
	}
}

// TestFallback_NoRetryOn400 关键：4xx 不该被重试（区别于 RunWithRetry 的粗粒度 IsRetryable）。
func TestFallback_NoRetryOn400(t *testing.T) {
	primary := &stubModel{name: "primary", respond: failWith(apiErr(400))}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup}, fallback.WithPerModelRetry(fallback.PerModelRetry{MaxRetries: 2}))

	_, err := fb.Generate(context.Background(), msgs())
	var ae *model.APIError
	if !errors.As(err, &ae) || ae.StatusCode != 400 {
		t.Fatalf("want 400, got %v", err)
	}
	if primary.callCount() != 1 {
		t.Fatalf("400 不该重试，主候选 calls=%d want 1", primary.callCount())
	}
	if backup.callCount() != 0 {
		t.Fatalf("400 不该切，backup calls=%d", backup.callCount())
	}
}

// TestFallback_OnSwitchHook 切换回调触发一次，from/to 正确。
func TestFallback_OnSwitchHook(t *testing.T) {
	var mu sync.Mutex
	var switches []string
	primary := &stubModel{name: "primary", respond: failWith(apiErr(503))}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup}, fallback.WithFallbackHook(func(ctx context.Context, from, to int, err error) {
		mu.Lock()
		switches = append(switches, fmt.Sprintf("%d->%d", from, to))
		mu.Unlock()
	}))

	_, _ = fb.Generate(context.Background(), msgs())
	if len(switches) != 1 || switches[0] != "0->1" {
		t.Fatalf("switches=%v, want [0->1]", switches)
	}
}

// TestFallback_AllCooldown_Sentinel 所有候选熔断后返哨兵 ErrAllMembersUnavailable。
func TestFallback_AllCooldown_Sentinel(t *testing.T) {
	primary := &stubModel{name: "primary", respond: failWith(apiErr(503))}
	backup := &stubModel{name: "backup", respond: failWith(apiErr(503))}
	fb := fallback.New([]model.ChatModel{primary, backup}, fallback.WithBreaker(1, time.Hour))

	// 第 1 次：两候选各失败 1 次 → 都 trip（maxFailures=1）
	_, _ = fb.Generate(context.Background(), msgs())
	// 第 2 次：两 breaker 都 open → 无候选可试
	_, err := fb.Generate(context.Background(), msgs())
	if !errors.Is(err, fallback.ErrAllMembersUnavailable) {
		t.Fatalf("want ErrAllMembersUnavailable, got %v", err)
	}
	if primary.callCount() != 1 || backup.callCount() != 1 {
		t.Fatalf("第 2 次应全跳过；calls primary=%d backup=%d want 1,1", primary.callCount(), backup.callCount())
	}
}

// TestFallback_WithTools_ReusesBreaker §7.1 关键不变量：WithTools 后复用 breaker，
// 已 trip 的候选仍被跳过（否则断路器形同虚设）。
func TestFallback_WithTools_ReusesBreaker(t *testing.T) {
	primary := &stubModel{name: "primary", respond: failWith(apiErr(503))}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup}, fallback.WithBreaker(1, time.Hour))

	// 触发主候选失败 1 次 → 其 breaker trip
	_, _ = fb.Generate(context.Background(), msgs())
	if primary.callCount() != 1 {
		t.Fatalf("primary calls=%d want 1", primary.callCount())
	}

	bound, err := fb.WithTools([]*schema.ToolInfo{{Name: "t"}})
	if err != nil {
		t.Fatalf("WithTools: %v", err)
	}
	// 新 model 上再调：若 breaker 复用，主候选 trip 中被跳过 → calls 不增；否则会再调
	_, _ = bound.Generate(context.Background(), msgs())
	if primary.callCount() != 1 {
		t.Fatalf("WithTools 后 breaker 未复用：主候选又被调用 calls=%d want 1", primary.callCount())
	}
	if backup.callCount() != 2 {
		t.Fatalf("backup 应两次都承接，calls=%d want 2", backup.callCount())
	}
}

// TestFallback_Stream_SwitchOnBuildError 流式建流阶段失败也切换。
func TestFallback_Stream_SwitchOnBuildError(t *testing.T) {
	primary := &stubModel{name: "primary", respond: failWith(apiErr(503))}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup})

	sr, err := fb.Stream(context.Background(), msgs())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	msg, rerr := sr.Recv(context.Background())
	if rerr != nil {
		t.Fatalf("recv: %v", rerr)
	}
	if !strings.Contains(msg.Content, "backup") {
		t.Fatalf("got %q, want backup stream", msg.Content)
	}
	if primary.callCount() != 1 || backup.callCount() != 1 {
		t.Fatalf("calls primary=%d backup=%d, want 1,1", primary.callCount(), backup.callCount())
	}
}

// TestFallback_NetworkError_Switch StatusCode==0（网络错）触发切换。
func TestFallback_NetworkError_Switch(t *testing.T) {
	primary := &stubModel{name: "primary", respond: failWith(netErr(errors.New("connection refused")))}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup})

	out, err := fb.Generate(context.Background(), msgs())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(out.Content, "backup") {
		t.Fatalf("got %q, want backup", out.Content)
	}
}

// TestFallback_UnknownError_Switch 非 APIError 的未知错误（如 provider 空响应 / 解码
// 失败冒泡的裸 error）走 DefaultFailoverPolicy 的"保守切换"分支——锁定 default 行为防
// 回归（防未来误把 policy default 改成 return false）。
func TestFallback_UnknownError_Switch(t *testing.T) {
	primary := &stubModel{name: "primary", respond: failWith(errors.New("decode failed: unexpected EOF"))}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup})

	out, err := fb.Generate(context.Background(), msgs())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out == nil || !strings.Contains(out.Content, "backup") {
		t.Fatalf("未知错误应保守切换到备用，got %v", out)
	}
}

// TestFallback_CtxCancel_NoSwitch **caller 自己的** ctx 取消时不切换。
//
// ⚠️ 2026-08-26 改：原来传 `context.Background()`（活的），测的其实是「错误里有 ctx 错」
// 而不是「我的 ctx 死了」。见下面的姊妹用例。
func TestFallback_CtxCancel_NoSwitch(t *testing.T) {
	primary := &stubModel{name: "primary", respond: failWith(netErr(context.Canceled))}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup})

	deadCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := fb.Generate(deadCtx, msgs())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if backup.callCount() != 0 {
		t.Fatalf("caller ctx 已死不该切，backup calls=%d", backup.callCount())
	}
}

// TestFallback_BackendCtxErr_StillSwitches 钉住上一条的**反面**：错误链上有 ctx 错、
// 而 **caller 的 ctx 还活着**时，照切。
//
// 这才是 fallback 存在的那个场景——`http.Client{Timeout}`（标准配法）在 provider 变慢时
// 产出的错就匹配 `DeadlineExceeded`。判据若写成「错误链上有没有 ctx 错」，fallback 在这里
// **永不工作**，而它正是为这个场景存在的。
func TestFallback_BackendCtxErr_StillSwitches(t *testing.T) {
	for _, e := range []error{context.Canceled, context.DeadlineExceeded} {
		primary := &stubModel{name: "primary", respond: failWith(netErr(e))}
		backup := &stubModel{name: "backup"}
		fb := fallback.New([]model.ChatModel{primary, backup})

		out, err := fb.Generate(context.Background(), msgs()) // caller ctx 全程健康
		if err != nil {
			t.Errorf("%v: 该切到备用，却返错：%v", e, err)
			continue
		}
		if out == nil || !strings.Contains(out.Content, "backup") {
			t.Errorf("%v: 该切到备用，got %v", e, out)
		}
		if backup.callCount() != 1 {
			t.Errorf("%v: 备用该被调 1 次，实际 %d——caller ctx 活着，"+
				"这个 ctx 错来自后端内部，换个候选很可能就好了", e, backup.callCount())
		}
	}
}

// TestFallback_New_PanicEmpty 空候选列表 fail-fast。
func TestFallback_New_PanicEmpty(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("空列表应 panic")
		}
	}()
	fallback.New(nil)
}

// TestFallback_New_PanicNilModel typed-nil 候选 fail-fast（reflect 检底层指针 nil）。
func TestFallback_New_PanicNilModel(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("nil model 应 panic")
		}
	}()
	var typedNil *stubModel
	fallback.New([]model.ChatModel{typedNil})
}

// TestFallback_Concurrent_SharedBreaker 并发 Generate 共享 breaker——run with -race。
// maxFailures=100 让主候选在 N 次并发失败内不 trip，全部切到备用；验证无 data race +
// 共享态（CircuitBreaker / 计数）线程安全。
func TestFallback_Concurrent_SharedBreaker(t *testing.T) {
	primary := &stubModel{name: "primary", respond: failWith(apiErr(503))}
	backup := &stubModel{name: "backup"}
	fb := fallback.New([]model.ChatModel{primary, backup}, fallback.WithBreaker(100, time.Hour))

	const N = 16
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // 屏障：尽量制造真正并发
			out, err := fb.Generate(context.Background(), msgs())
			if err != nil || out == nil || !strings.Contains(out.Content, "backup") {
				t.Errorf("并发 generate 失败: out=%v err=%v", out, err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if backup.callCount() != N {
		t.Fatalf("备用应承接全部 %d 次，got %d", N, backup.callCount())
	}
}

// TestFallback_PerMemberRetry_HonorsRetryAfter 钉住 fallback 的 per-member 重试也按
// 服务端明示的 `Retry-After` 退避，而不是按自己猜的指数退避。
//
// **为什么这条必须独立存在**：引入 Retry-After 的那一批只测了 `compose_v2.RunWithRetry`，
// 而 fallback 有**自己的一份重试循环**（参数类型都是另一个 `PerModelRetry`）。两处各接
// 各的，漏掉一处不会有任何信号——退避照常发生，只是等错了时长。
//
// **测试设计**：把 InitialDelay 配成 5s（远大于服务端给的 30ms）。接上了就 30ms 走完，
// 没接上就要等秒级。断言选在**耗时**上，因为这个 fix 唯一改变的就是那个量：
// 调用次数、最终结果、错误都不受影响，拿它们做断言在 fix 前后一模一样。
func TestFallback_PerMemberRetry_HonorsRetryAfter(t *testing.T) {
	rateLimited := &model.APIError{
		Provider:   "stub",
		StatusCode: 429,
		Body:       "rate limited",
		RetryAfter: 30 * time.Millisecond,
	}
	// 第一次限流、第二次成功：必须走到「重试前睡一觉」那一步，断言才承重。
	primary := &stubModel{name: "primary", respond: func(n int, _ []*schema.Message) (*schema.Message, error) {
		if n == 0 {
			return nil, rateLimited
		}
		return schema.AssistantMessage("ok"), nil
	}}
	backup := &stubModel{name: "backup"}

	fb := fallback.New([]model.ChatModel{primary, backup},
		fallback.WithPerModelRetry(fallback.PerModelRetry{
			MaxRetries: 1,
			Backoff: compose_v2.BackoffPolicy{
				InitialDelay: 5 * time.Second, // 故意远大于 Retry-After
				MaxDelay:     10 * time.Second,
				Multiplier:   2.0,
			},
		}))

	start := time.Now()
	out, err := fb.Generate(context.Background(), msgs())
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("重试后应成功，got %v", err)
	}
	if out == nil || out.Content != "ok" {
		t.Fatalf("应拿到重试那次的结果，got %v", out)
	}
	if got := primary.callCount(); got != 2 {
		t.Fatalf("primary 应被调 2 次（限流 1 次 + 重试 1 次），got %d", got)
	}
	if got := backup.callCount(); got != 0 {
		t.Fatalf("重试成功就不该切候选，backup calls=%d want 0", got)
	}
	if elapsed > time.Second {
		t.Errorf("等了 %v——服务端明示 30ms 却按自己的 InitialDelay=5s 退避了，"+
			"说明 fallback 这条重试循环没接 Retry-After", elapsed)
	}
}
