// Package llmtrace 给真 LLM smoke 提供「哪个模型真服务 + token 消耗」追踪——fallback 链
// 会掩盖实际服务的模型，本包用 model 包装记录名字 + hook 记录 token，smoke 结束打印透明可核。
package llmtrace

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"crew-go/fallback"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/model/qwen"
	"crew-go/schema"
)

// Tracer 记录 fallback 链里哪个模型真服务（每轮）+ 哪个报错触发 failover + 累计 token。
type Tracer struct {
	mu       sync.Mutex
	served   map[string]int // model name → 成功服务次数（轮数）
	failed   map[string]int // model name → 报错次数（触发 failover）
	inTok    int
	outTok   int
	totalTok int
}

func (t *Tracer) recordServe(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.served == nil {
		t.served = map[string]int{}
	}
	t.served[name]++
}

func (t *Tracer) recordFail(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.failed == nil {
		t.failed = map[string]int{}
	}
	t.failed[name]++
}

func (t *Tracer) addTokens(u *schema.MessageUsage) {
	if u == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inTok += int(u.InputTokens)
	t.outTok += int(u.OutputTokens)
	t.totalTok += int(u.TotalTokens)
}

// Report 返回一行可打印的服务模型 + token 汇总。
func (t *Tracer) Report() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var b strings.Builder
	b.WriteString("服务模型: ")
	b.WriteString(fmtCounts(t.served))
	if len(t.failed) > 0 {
		b.WriteString(" | failover(报错): ")
		b.WriteString(fmtCounts(t.failed))
	}
	fmt.Fprintf(&b, " | token: in %d + out %d = %d", t.inTok, t.outTok, t.totalTok)
	return b.String()
}

func fmtCounts(m map[string]int) string {
	if len(m) == 0 {
		return "(无)"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s×%d", k, m[k])
	}
	return strings.Join(parts, " ")
}

// Hooks 返回累计 token 的 hook（OnModelEnd 读 ModelOutput.Usage，对 Generate / Stream 都准）。
func (t *Tracer) Hooks() hook.Hooks { return &usageHook{t: t} }

type usageHook struct {
	hook.NoOpHooks
	t *Tracer
}

func (h *usageHook) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	if out != nil {
		h.t.addTokens(out.Usage)
	}
	return ctx, nil
}

// tracked 包一个 model，记录它服务/报错到 Tracer（用于在 fallback 链里识别实际服务的成员）。
type tracked struct {
	name  string
	inner model.ChatModel
	t     *Tracer
}

func (m *tracked) Generate(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	resp, err := m.inner.Generate(ctx, msgs, opts...)
	if err != nil {
		m.t.recordFail(m.name)
		return nil, err
	}
	m.t.recordServe(m.name)
	return resp, nil
}

func (m *tracked) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	sr, err := m.inner.Stream(ctx, msgs, opts...)
	if err != nil {
		m.t.recordFail(m.name)
		return nil, err
	}
	m.t.recordServe(m.name)
	return sr, nil
}

// ModelName 转发里面那个客户端的模型名（满足 model.ModelNamer）。
func (m *tracked) ModelName() string {
	if n, ok := m.inner.(model.ModelNamer); ok {
		return n.ModelName()
	}
	return m.name
}

func (m *tracked) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	bound, err := m.inner.WithTools(tools)
	if err != nil {
		return nil, err
	}
	return &tracked{name: m.name, inner: bound, t: m.t}, nil
}

// alwaysFailover：测试用激进策略，任何非 ctx 取消错误都换下一个模型（含 quota 400/429）。
type alwaysFailover struct{}

func (alwaysFailover) ShouldFailover(_ context.Context, err error) bool {
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// NewQwenFallback 建带模型追踪的 qwen fallback 链（额度满/限流/欠费自动切下一个）。
// 返回 model + Tracer——smoke 结束 `fmt.Println(tr.Report())` 打印实际服务的模型 +
// token，业务把 `tr.Hooks()` 接进 WithHooks（多 hook 用 hook.ChainHooks 串）。
//
// 候选直接取 `qwen.TestModelCandidates`（带日期的固定快照在前）——**不在这里写死
// 模型名**：额度按模型耗尽，写死的链会整条烂掉（原先 4 个候选里 3 个已耗尽）。
func NewQwenFallback() (model.ChatModel, *Tracer) {
	names := qwen.TestModelCandidates
	tr := &Tracer{}
	models := make([]model.ChatModel, len(names))
	for i, n := range names {
		models[i] = &tracked{name: n, inner: qwen.NewTestClient(n), t: tr}
	}
	return fallback.New(models, fallback.WithFailoverPolicy(alwaysFailover{})), tr
}
