package hook_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crew-go/hook"
	"crew-go/modules/common"
	specModel "crew-go/modules/model"
	"crew-go/schema"
)

// ────────────────────────────────────────────────────────────
// 测试工具：recorderHook / fakeStream
// ────────────────────────────────────────────────────────────

// recorderHook 按"id:method" 追加日志，便于断言调用顺序和短路语义；
// 各方法错误可独立注入。OnModelEndStream 通过 wrapWith 把输入 stream
// 包装成一个新的 fakeStream（若配置），便于验证包装链与 Close 链。
type recorderHook struct {
	id  string
	log *[]string

	agentStartErr     error
	modelStartErr     error
	modelEndErr       error
	modelEndStreamErr error
	toolStartErr      error
	toolEndErr        error

	// 若非 nil，则 OnModelEndStream 返回的新 stream 的 id
	wrapAs string

	// 若非空，OnAgentStart 在 input.Extra[mutateKey]=id 写入
	mutateKey string
}

func (r *recorderHook) record(m string) {
	*r.log = append(*r.log, r.id+":"+m)
}

func (r *recorderHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, input *hook.AgentInput) (context.Context, error) {
	r.record("start")
	if r.mutateKey != "" {
		if input.Extra == nil {
			input.Extra = map[string]any{}
		}
		input.Extra[r.mutateKey] = r.id
	}
	return ctx, r.agentStartErr
}

func (r *recorderHook) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	r.record("end")
	return ctx
}

func (r *recorderHook) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	r.record("modelStart")
	return ctx, r.modelStartErr
}

func (r *recorderHook) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
	r.record("modelEnd")
	return ctx, r.modelEndErr
}

func (r *recorderHook) OnModelEndStream(ctx context.Context, _ *hook.AgentInfo, sr specModel.MessageStreamReader) (context.Context, specModel.MessageStreamReader, error) {
	r.record("modelStream")
	if r.modelEndStreamErr != nil {
		return ctx, nil, r.modelEndStreamErr
	}
	if r.wrapAs != "" {
		if fs, ok := sr.(*fakeStream); ok {
			return ctx, &fakeStream{id: r.wrapAs, closes: fs.closes}, nil
		}
		// 非 fakeStream 的兜底：原样返回
	}
	return ctx, sr, nil
}

func (r *recorderHook) OnToolStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ToolInput) (context.Context, error) {
	r.record("toolStart")
	return ctx, r.toolStartErr
}

func (r *recorderHook) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ToolOutput) (context.Context, error) {
	r.record("toolEnd")
	return ctx, r.toolEndErr
}

func (r *recorderHook) OnError(ctx context.Context, _ *hook.AgentInfo, _ *schema.CommError) context.Context {
	r.record("error")
	return ctx
}

// fakeStream 是 specModel.MessageStreamReader 的最小实现，
// 跟踪 Close 调用顺序（写入共享的 closes slice）。
type fakeStream struct {
	id     string
	closes *[]string
}

func (f *fakeStream) Recv(_ context.Context) (*common.Message, error) {
	return nil, errors.New("fakeStream.Recv 未使用")
}

func (f *fakeStream) Close() error {
	*f.closes = append(*f.closes, f.id)
	return nil
}

// ────────────────────────────────────────────────────────────
// NoOpHooks
// ────────────────────────────────────────────────────────────

func TestNoOpHooks_AllMethodsReturnNil(t *testing.T) {
	h := hook.NoOpHooks{}
	ctx := context.Background()
	info := &hook.AgentInfo{Name: "x", Type: "base"}

	if _, err := h.OnAgentStart(ctx, info, &hook.AgentInput{}); err != nil {
		t.Errorf("OnAgentStart err=%v, want nil", err)
	}
	h.OnAgentEnd(ctx, info, &hook.AgentOutput{}) // 无返回
	if _, err := h.OnModelStart(ctx, info, &schema.ModelInput{}); err != nil {
		t.Errorf("OnModelStart err=%v, want nil", err)
	}
	if _, err := h.OnModelEnd(ctx, info, &schema.ModelOutput{}); err != nil {
		t.Errorf("OnModelEnd err=%v, want nil", err)
	}

	// OnModelEndStream 必须原样返回 sr
	sr := &fakeStream{id: "orig", closes: &[]string{}}
	_, got, err := h.OnModelEndStream(ctx, info, sr)
	if err != nil {
		t.Errorf("OnModelEndStream err=%v, want nil", err)
	}
	if got != specModel.MessageStreamReader(sr) {
		t.Errorf("OnModelEndStream 应原样返回 sr，实际返回 %v", got)
	}

	if _, err := h.OnToolStart(ctx, info, &schema.ToolInput{}); err != nil {
		t.Errorf("OnToolStart err=%v, want nil", err)
	}
	if _, err := h.OnToolEnd(ctx, info, &schema.ToolOutput{}); err != nil {
		t.Errorf("OnToolEnd err=%v, want nil", err)
	}
	h.OnError(ctx, info, &schema.CommError{Code: "x"}) // 无返回
}

// ────────────────────────────────────────────────────────────
// ChainHooks — 顺序 / 短路 / Mutation
// ────────────────────────────────────────────────────────────

func TestChainHooks_Empty_NoPanic(t *testing.T) {
	c := hook.NewChainHooks()
	ctx := context.Background()
	info := &hook.AgentInfo{}

	if _, err := c.OnAgentStart(ctx, info, &hook.AgentInput{}); err != nil {
		t.Errorf("空链 OnAgentStart err=%v, want nil", err)
	}
	c.OnAgentEnd(ctx, info, &hook.AgentOutput{})

	if _, err := c.OnModelStart(ctx, info, &schema.ModelInput{}); err != nil {
		t.Errorf("空链 OnModelStart err=%v, want nil", err)
	}
	if _, err := c.OnModelEnd(ctx, info, &schema.ModelOutput{}); err != nil {
		t.Errorf("空链 OnModelEnd err=%v, want nil", err)
	}
	sr := &fakeStream{id: "orig", closes: &[]string{}}
	_, got, err := c.OnModelEndStream(ctx, info, sr)
	if err != nil {
		t.Errorf("空链 OnModelEndStream err=%v, want nil", err)
	}
	if got != specModel.MessageStreamReader(sr) {
		t.Errorf("空链 OnModelEndStream 应原样返回 sr")
	}

	if _, err := c.OnToolStart(ctx, info, &schema.ToolInput{}); err != nil {
		t.Errorf("空链 OnToolStart err=%v, want nil", err)
	}
	if _, err := c.OnToolEnd(ctx, info, &schema.ToolOutput{}); err != nil {
		t.Errorf("空链 OnToolEnd err=%v, want nil", err)
	}
	c.OnError(ctx, info, &schema.CommError{})
}

// TestChainHooks_OrderPreserved 验证各非短路方法按注册顺序调用。
func TestChainHooks_OrderPreserved(t *testing.T) {
	log := []string{}
	a := &recorderHook{id: "a", log: &log}
	b := &recorderHook{id: "b", log: &log}
	c := &recorderHook{id: "c", log: &log}

	chain := hook.NewChainHooks(a, b, c)
	ctx := context.Background()
	info := &hook.AgentInfo{}

	_, _ = chain.OnAgentStart(ctx, info, &hook.AgentInput{})
	chain.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	_, _ = chain.OnModelStart(ctx, info, &schema.ModelInput{})
	_, _ = chain.OnModelEnd(ctx, info, &schema.ModelOutput{})
	_, _ = chain.OnToolStart(ctx, info, &schema.ToolInput{})
	_, _ = chain.OnToolEnd(ctx, info, &schema.ToolOutput{})
	chain.OnError(ctx, info, &schema.CommError{})

	want := []string{
		"a:start", "b:start", "c:start",
		"a:end", "b:end", "c:end",
		"a:modelStart", "b:modelStart", "c:modelStart",
		"a:modelEnd", "b:modelEnd", "c:modelEnd",
		"a:toolStart", "b:toolStart", "c:toolStart",
		"a:toolEnd", "b:toolEnd", "c:toolEnd",
		"a:error", "b:error", "c:error",
	}
	assertEqualStringSlices(t, log, want)
}

// TestChainHooks_ShortCircuitOnError 验证返 error 的方法一旦出错立即中断，
// 表驱动覆盖所有支持短路的方法。
func TestChainHooks_ShortCircuitOnError(t *testing.T) {
	injected := errors.New("boom")

	cases := []struct {
		name   string
		inject func(h *recorderHook)
		call   func(c *hook.ChainHooks) error
		want   []string // 期望的调用记录（b 报错，c 不应被调）
	}{
		{
			name:   "OnAgentStart",
			inject: func(h *recorderHook) { h.agentStartErr = injected },
			call: func(c *hook.ChainHooks) error {
				_, err := c.OnAgentStart(context.Background(), &hook.AgentInfo{}, &hook.AgentInput{})
				return err
			},
			want: []string{"a:start", "b:start"},
		},
		{
			name:   "OnModelStart",
			inject: func(h *recorderHook) { h.modelStartErr = injected },
			call: func(c *hook.ChainHooks) error {
				_, err := c.OnModelStart(context.Background(), &hook.AgentInfo{}, &schema.ModelInput{})
				return err
			},
			want: []string{"a:modelStart", "b:modelStart"},
		},
		{
			name:   "OnModelEnd",
			inject: func(h *recorderHook) { h.modelEndErr = injected },
			call: func(c *hook.ChainHooks) error {
				_, err := c.OnModelEnd(context.Background(), &hook.AgentInfo{}, &schema.ModelOutput{})
				return err
			},
			want: []string{"a:modelEnd", "b:modelEnd"},
		},
		{
			name:   "OnToolStart",
			inject: func(h *recorderHook) { h.toolStartErr = injected },
			call: func(c *hook.ChainHooks) error {
				_, err := c.OnToolStart(context.Background(), &hook.AgentInfo{}, &schema.ToolInput{})
				return err
			},
			want: []string{"a:toolStart", "b:toolStart"},
		},
		{
			name:   "OnToolEnd",
			inject: func(h *recorderHook) { h.toolEndErr = injected },
			call: func(c *hook.ChainHooks) error {
				_, err := c.OnToolEnd(context.Background(), &hook.AgentInfo{}, &schema.ToolOutput{})
				return err
			},
			want: []string{"a:toolEnd", "b:toolEnd"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log := []string{}
			a := &recorderHook{id: "a", log: &log}
			b := &recorderHook{id: "b", log: &log}
			cc := &recorderHook{id: "c", log: &log}
			tc.inject(b)

			chain := hook.NewChainHooks(a, b, cc)
			err := tc.call(chain)
			if !errors.Is(err, injected) {
				t.Fatalf("err=%v, want %v", err, injected)
			}
			assertEqualStringSlices(t, log, tc.want)
		})
	}
}

// TestChainHooks_OnAgentEnd_NoShortCircuit：OnAgentEnd 无 error，
// 任一 hook 都不能阻断后续调用。
func TestChainHooks_OnAgentEnd_NoShortCircuit(t *testing.T) {
	log := []string{}
	a := &recorderHook{id: "a", log: &log}
	b := &recorderHook{id: "b", log: &log}
	c := &recorderHook{id: "c", log: &log}

	chain := hook.NewChainHooks(a, b, c)
	chain.OnAgentEnd(context.Background(), &hook.AgentInfo{}, &hook.AgentOutput{})
	assertEqualStringSlices(t, log, []string{"a:end", "b:end", "c:end"})
}

// TestChainHooks_OnError_NoShortCircuit：OnError 同样必须全部调完。
func TestChainHooks_OnError_NoShortCircuit(t *testing.T) {
	log := []string{}
	a := &recorderHook{id: "a", log: &log}
	b := &recorderHook{id: "b", log: &log}
	c := &recorderHook{id: "c", log: &log}

	chain := hook.NewChainHooks(a, b, c)
	chain.OnError(context.Background(), &hook.AgentInfo{}, &schema.CommError{})
	assertEqualStringSlices(t, log, []string{"a:error", "b:error", "c:error"})
}

// TestChainHooks_MutationPropagation 验证 hook 对 input 的改写会被
// 后续 hook 看到（"所有 hook 视为可改写"契约）。
func TestChainHooks_MutationPropagation(t *testing.T) {
	log := []string{}
	a := &recorderHook{id: "a", log: &log, mutateKey: "k"}
	b := &recorderHook{id: "b", log: &log, mutateKey: "k"}

	chain := hook.NewChainHooks(a, b)
	input := &hook.AgentInput{}
	if _, err := chain.OnAgentStart(context.Background(), &hook.AgentInfo{}, input); err != nil {
		t.Fatalf("OnAgentStart err=%v", err)
	}
	// 最后一个写入者 b 应覆盖前者 a
	got, _ := input.Extra["k"].(string)
	if got != "b" {
		t.Errorf("input.Extra[k]=%q, want b（后写覆盖）", got)
	}
}

// ────────────────────────────────────────────────────────────
// ChainHooks.OnModelEndStream — wrap 链 + 出错逆序 Close
// ────────────────────────────────────────────────────────────

// TestChainHooks_OnModelEndStream_WrapChain 验证正常链式包装：
// 返回的 stream 是最后一个 hook 的 wrap 结果，原始 stream 未 Close。
func TestChainHooks_OnModelEndStream_WrapChain(t *testing.T) {
	closes := []string{}
	log := []string{}
	a := &recorderHook{id: "a", log: &log, wrapAs: "wa"}
	b := &recorderHook{id: "b", log: &log, wrapAs: "wb"}
	c := &recorderHook{id: "c", log: &log, wrapAs: "wc"}

	chain := hook.NewChainHooks(a, b, c)
	orig := &fakeStream{id: "orig", closes: &closes}

	_, got, err := chain.OnModelEndStream(context.Background(), &hook.AgentInfo{}, orig)
	if err != nil {
		t.Fatalf("OnModelEndStream err=%v", err)
	}
	fs, ok := got.(*fakeStream)
	if !ok || fs.id != "wc" {
		t.Errorf("最终 stream id=%v, want wc", got)
	}
	// 正常路径不 Close 任何 stream
	if len(closes) != 0 {
		t.Errorf("正常路径不应 Close 任何 stream，实际 closes=%v", closes)
	}
	assertEqualStringSlices(t, log, []string{"a:modelStream", "b:modelStream", "c:modelStream"})
}

// TestChainHooks_OnModelEndStream_ErrorClosesStack 验证中间 hook 报错时：
//  1. 已 wrap 的 stream 按逆序 Close（w1 在 w2 之后——即 w2 先）
//  2. 原始 stream 最后 Close
//  3. 出错 hook 本次的 wrap 产出（若有）不加入栈、不被 Close
//  4. 报错后续 hook 不被调用
func TestChainHooks_OnModelEndStream_ErrorClosesStack(t *testing.T) {
	closes := []string{}
	log := []string{}
	a := &recorderHook{id: "a", log: &log, wrapAs: "w1"}
	b := &recorderHook{id: "b", log: &log, wrapAs: "w2"}
	// c 报错；wrapAs 即便设置也不会生效（err 先于 wrapped 返回）
	c := &recorderHook{
		id:                "c",
		log:               &log,
		wrapAs:            "w3",
		modelEndStreamErr: errors.New("stream boom"),
	}
	d := &recorderHook{id: "d", log: &log, wrapAs: "w4"}

	chain := hook.NewChainHooks(a, b, c, d)
	orig := &fakeStream{id: "orig", closes: &closes}

	_, got, err := chain.OnModelEndStream(context.Background(), &hook.AgentInfo{}, orig)
	if err == nil {
		t.Fatalf("期望错误，实际 err=nil")
	}
	if got != nil {
		t.Errorf("出错时应返回 nil stream，实际 %v", got)
	}
	// d 不应被调
	assertEqualStringSlices(t, log, []string{"a:modelStream", "b:modelStream", "c:modelStream"})

	// closes 顺序：w2, w1, orig（逆序关栈 + 原始最后）
	if want := []string{"w2", "w1", "orig"}; !equalStringSlices(closes, want) {
		t.Errorf("Close 顺序=%v, want %v", closes, want)
	}
}

// ────────────────────────────────────────────────────────────
// Panic 隔离测试（audit Round 7 Bug 2 修过的核心特性）
// ────────────────────────────────────────────────────────────

// panickyHook 在指定 method 上 panic——验证 ChainHooks safe 包装。
type panickyHook struct {
	id              string
	log             *[]string
	panicOnEnd      bool
	panicOnError    bool
	panicOnModelEnd bool
	panicOnStream   bool
}

func (p *panickyHook) record(m string) { *p.log = append(*p.log, p.id+":"+m) }

func (p *panickyHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	p.record("start")
	return ctx, nil
}
func (p *panickyHook) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentOutput) context.Context {
	p.record("end")
	if p.panicOnEnd {
		panic("boom-end")
	}
	return ctx
}
func (p *panickyHook) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	p.record("modelStart")
	return ctx, nil
}
func (p *panickyHook) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
	p.record("modelEnd")
	if p.panicOnModelEnd {
		panic("boom-modelEnd")
	}
	return ctx, nil
}
func (p *panickyHook) OnModelEndStream(ctx context.Context, _ *hook.AgentInfo, sr specModel.MessageStreamReader) (context.Context, specModel.MessageStreamReader, error) {
	p.record("modelStream")
	if p.panicOnStream {
		panic("boom-stream")
	}
	return ctx, sr, nil
}
func (p *panickyHook) OnToolStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ToolInput) (context.Context, error) {
	p.record("toolStart")
	return ctx, nil
}
func (p *panickyHook) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ToolOutput) (context.Context, error) {
	p.record("toolEnd")
	return ctx, nil
}
func (p *panickyHook) OnError(ctx context.Context, _ *hook.AgentInfo, _ *schema.CommError) context.Context {
	p.record("err")
	if p.panicOnError {
		panic("boom-error")
	}
	return ctx
}

// TestNoOpHooks_EmbeddingPattern —— 业务嵌入 NoOpHooks 只覆盖关心的回调。
// 验证嵌入 + 部分 override 不破坏其他默认行为。
func TestNoOpHooks_EmbeddingPattern(t *testing.T) {
	type myHooks struct {
		hook.NoOpHooks
		modelEndCalls int
	}
	// 业务只覆盖 OnModelEnd——其他全用 NoOp 默认 nil
	customH := &struct {
		myHooks
	}{}
	customH.modelEndCalls = 0

	// 编译期验证 customH 满足 hook.Hooks 接口（嵌入实现所有方法）
	var _ hook.Hooks = customH

	// 调用未覆盖方法不应 panic
	if _, err := customH.OnAgentStart(context.Background(), &hook.AgentInfo{}, &hook.AgentInput{}); err != nil {
		t.Errorf("NoOp OnAgentStart 应返 nil，实际 err=%v", err)
	}
	if _, err := customH.OnToolEnd(context.Background(), &hook.AgentInfo{}, &schema.ToolOutput{}); err != nil {
		t.Errorf("NoOp OnToolEnd 应返 nil，实际 err=%v", err)
	}
}

// (audit² 删 SafeOnAgentEnd helper + 跟 eino 对齐 framework Run 入口 outer defer recover 兜底 panic,
// 原 TestSafeOnAgentEnd_StandalonePanicRecover 已随 helper 删除)
// ────────────────────────────────────────────────────────────
// 辅助断言
// ────────────────────────────────────────────────────────────

func assertEqualStringSlices(t *testing.T, got, want []string) {
	t.Helper()
	if !equalStringSlices(got, want) {
		t.Errorf("got=%v\nwant=%v", got, want)
	}
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ─────────────── audit² Phase 1.5 回归测试 ───────────────
// 三类测试守护新 ctx-based 协议:
// 1. ctx propagation: hook A push 的 ctx 让 hook B 拿到 (跟 eino Handler 协议同款)
// 2. ChainHooks 错误路径返 newCtx (业务 hook A 已 push span 但中途 fail 时让 caller 拿到 ctx 做 cleanup)
// 3. panic 冒泡协议: chain 内 panic 直接冒泡, framework outer recover 兜底转 wrapped err

// ctxKey 测试用 ctx value key
type ctxKey struct{ id string }

// pushCtxHook 在指定 method 内 push ctx value, 让下游 hook / caller 拿到.
type pushCtxHook struct {
	hook.NoOpHooks
	pushOnStart      string
	pushOnModelStart string
}

func (h *pushCtxHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if h.pushOnStart != "" {
		ctx = context.WithValue(ctx, ctxKey{"start"}, h.pushOnStart)
	}
	return ctx, nil
}

func (h *pushCtxHook) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	if h.pushOnModelStart != "" {
		ctx = context.WithValue(ctx, ctxKey{"model"}, h.pushOnModelStart)
	}
	return ctx, nil
}

// TestChainHooks_CtxPropagation_AcrossHooks 验 ctx 在 chain 内 hook 间链式串.
// hook A push ctx value → hook B 在同一 method 拿到 (chain 内 propagation).
//
// sanity-revert: ChainHooks.OnAgentStart 内删 `ctx = newCtx` (用初始 ctx 调下一个 hook)
// → hook B 拿不到 hook A push 的 value → fail.
func TestChainHooks_CtxPropagation_AcrossHooks(t *testing.T) {
	pusher := &pushCtxHook{pushOnStart: "pushed-by-A"}
	var captured any
	captureFn := &captureMidChainHook{}
	captureFn.onStart = func(ctx context.Context) {
		captured = ctx.Value(ctxKey{"start"})
	}
	chain := hook.NewChainHooks(pusher, captureFn)
	ctx := context.Background()
	_, err := chain.OnAgentStart(ctx, &hook.AgentInfo{}, &hook.AgentInput{})
	if err != nil {
		t.Fatalf("OnAgentStart err: %v", err)
	}
	if captured != "pushed-by-A" {
		t.Errorf("audit² ctx propagation regression: hook B 应拿到 hook A push 的 value, got %v\n"+
			"sanity-revert: ChainHooks.OnAgentStart 删 ctx=newCtx → 此测 fail", captured)
	}
}

// TestChainHooks_CtxPropagation_OnModelStart 同上，但守 **OnModelStart** 那条链。
//
// # 为什么单独一条
//
// `pushCtxHook` 早就有 `pushOnModelStart` 字段和读取逻辑，**但全仓从没有任何地方
// 设过它**——于是 `ChainHooks.OnModelStart` 的 ctx 串链零覆盖：把那句 `ctx = newCtx`
// 拿掉，`go test ./... -count=1` **全仓全绿**（实测）。
//
// 这是「fixture 上摆着开关、从没被拨过」那一类假绿：开关看着像有覆盖，实际那条
// 生产路径没人守。同类的 `OnAgentStart` 有上面那条守着，`OnModelStart` 一直裸着。
//
// 业务侧的实际后果：多 hook chain 下（如 observability + 业务自定义），前一个 hook
// 往 ctx 里 push 的东西后一个拿不到——而 Hooks godoc 写明链里每个 hook 返回的 ctx 交给下一个
// hook。
//
// sanity-revert：`ChainHooks.OnModelStart` 里删 `ctx = newCtx` → 本用例红。
func TestChainHooks_CtxPropagation_OnModelStart(t *testing.T) {
	pusher := &pushCtxHook{pushOnModelStart: "pushed-by-A"}
	var captured any
	captureFn := &captureMidChainHook{}
	captureFn.onModelStart = func(ctx context.Context) {
		captured = ctx.Value(ctxKey{"model"})
	}
	chain := hook.NewChainHooks(pusher, captureFn)
	if _, err := chain.OnModelStart(context.Background(), &hook.AgentInfo{}, &schema.ModelInput{}); err != nil {
		t.Fatalf("OnModelStart err: %v", err)
	}
	if captured != "pushed-by-A" {
		t.Errorf("ctx 串链断了：hook B 该拿到 hook A 在 OnModelStart push 的 value，got %v\n"+
			"  多 hook chain 下前一个 hook 的 ctx 改动会被丢弃", captured)
	}
}

// captureMidChainHook 是 chain 内中间 hook, 在自定义闭包内捕获 ctx — 用于
// 验 chain 内前面 hook 的 ctx push 真传给本 hook.
type captureMidChainHook struct {
	hook.NoOpHooks
	onStart      func(ctx context.Context)
	onModelStart func(ctx context.Context)
}

func (h *captureMidChainHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if h.onStart != nil {
		h.onStart(ctx)
	}
	return ctx, nil
}

func (h *captureMidChainHook) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	if h.onModelStart != nil {
		h.onModelStart(ctx)
	}
	return ctx, nil
}

// TestChainHooks_PanicBubblesUp 验 chain 内单 hook panic **冒泡到 caller**
// (跟 eino fail-fast 哲学对齐, 不 silent isolate).
//
// sanity-revert: ChainHooks 加回 per-handler safeCallVoid recover → panic 被
// silent 吞 → 测试 expect panic 但没 panic → fail.
func TestChainHooks_PanicBubblesUp(t *testing.T) {
	panicker := &panickyHook{id: "p", log: &[]string{}, panicOnEnd: true}
	chain := hook.NewChainHooks(panicker)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("audit² panic 协议: chain 内 hook panic 应冒泡, framework outer recover 兜底\n" +
				"实际没 panic — 说明 chain 内仍有 per-handler isolation (跟 eino 协议偏离)")
		}
	}()
	chain.OnAgentEnd(context.Background(), &hook.AgentInfo{}, &hook.AgentOutput{})
}

// TestChainHooks_ErrorPath_ReturnsInputCtx 验 audit³ B2 协议: chain 内 hook A
// 返 (newCtx, err) 时 caller 拿到的是入参 ctx (不含 hook A push 的 span).
//
// **协议理由**: hook 返 err 即业务声明失败, 此时 propagate newCtx 让 caller 误以为
// hook 部分成功 — 跨 hook 链行为不一致 (有的 hook 返 (nil, err) 有的返 (newCtx, err)).
// 简化为"错误路径回入参 ctx"让 caller pattern A 自洽. OTel cleanup 由业务 hook 自己
// `defer span.End()` 负责, 不靠 caller propagate newCtx.
//
// sanity-revert: ChainHooks 错误路径改回 `if newCtx != nil return newCtx` → 此测 fail.
// spanCtxKey 包级 key — Go ctx.Value 按 type 比较, 不同 scope declare 的同名 struct 算不同 type.
type spanCtxKey struct{}

func TestChainHooks_ErrorPath_ReturnsInputCtx(t *testing.T) {
	pushAndFail := &spanPushAndFailHook{spanName: "spanA", failErr: fmt.Errorf("validate fail")}
	chain := hook.NewChainHooks(pushAndFail)
	ctx := context.Background()
	gotCtx, err := chain.OnAgentStart(ctx, &hook.AgentInfo{}, &hook.AgentInput{})
	if err == nil {
		t.Fatal("expect err from pushAndFailHook")
	}
	// audit³ B2: 错误路径回入参 ctx — hook A push 的 span 不该传给 caller
	if span := gotCtx.Value(spanCtxKey{}); span != nil {
		t.Errorf("audit³ B2 协议违反: 错误路径应回入参 ctx (不含 hook push 的值), got %v\n"+
			"sanity-revert: ChainHooks 错误路径改回 nil-guard newCtx propagate → 此测 fail", span)
	}
}

// TestChainHooks_ErrorPath_MultiHook_NoLeakHookAcx 验 audit⁴ B MED-2: 单 hook
// 测试不够 — 真正考验 B2 "inCtx" 协议是 2 hook 场景: hook A success push span,
// hook B fail. 没 inCtx 设计 (只是 `return ctx, err` 不重置) 会让 caller 拿到
// 含 hook A span 的 ctx — caller 误以为 hook 链部分成功 + OTel cleanup 错位.
//
// sanity-revert: 删 ChainHooks 6 method 顶部 `inCtx := ctx` 改回 `return ctx, err`
// → hook A span 泄漏给 caller → 此测 fail.
type spanPushOnlyHook struct {
	hook.NoOpHooks
	spanName string
}

func (h *spanPushOnlyHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	return context.WithValue(ctx, spanCtxKey{}, h.spanName), nil
}

type plainFailHook struct {
	hook.NoOpHooks
	failErr error
}

func (h *plainFailHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	// 不 push 任何值, 直接 fail — verify caller 拿到的也不含 hook A 的值
	return ctx, h.failErr
}

func TestChainHooks_ErrorPath_MultiHook_NoLeakHookACtx(t *testing.T) {
	hookA := &spanPushOnlyHook{spanName: "leaked-spanA"}
	hookB := &plainFailHook{failErr: fmt.Errorf("validate fail")}
	chain := hook.NewChainHooks(hookA, hookB)
	ctx := context.Background()
	gotCtx, err := chain.OnAgentStart(ctx, &hook.AgentInfo{}, &hook.AgentInput{})
	if err == nil {
		t.Fatal("expect err from hookB")
	}
	// audit⁴ B MED-2: 即便 hookA 成功 push 了 span, hookB fail 时 caller 应拿
	// 入参 ctx (不含 hookA span). 单 hook 测漏掉了这种"链内 partial mutation"风险.
	if span := gotCtx.Value(spanCtxKey{}); span != nil {
		t.Errorf("audit⁴ B MED-2 协议违反: 2 hook 链 hook B fail 时 caller 不应拿到 hook A push 的 span, got %v\n"+
			"sanity-revert: ChainHooks 6 method 删 `inCtx := ctx` 改回 `return ctx, err` → 此测 fail", span)
	}
}

// spanPushAndFailHook 模拟业务 OTel: push span 然后 throw err.
type spanPushAndFailHook struct {
	hook.NoOpHooks
	spanName string
	failErr  error
}

func (h *spanPushAndFailHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	ctx = context.WithValue(ctx, spanCtxKey{}, h.spanName)
	return ctx, h.failErr
}
