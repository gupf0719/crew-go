package agent_v2_test

import (
	"context"
	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// putRecorder 记每次 Put 的 (ns, key)，用来数「写了几次、落了几条」。
type putRecorder struct {
	store.Store
	mu   sync.Mutex
	puts []string
}

func (p *putRecorder) Put(ctx context.Context, ns store.Namespace, key string, v any, opts ...store.PutOption) error {
	p.mu.Lock()
	p.puts = append(p.puts, ns.String()+"|"+key)
	p.mu.Unlock()
	return p.Store.Put(ctx, ns, key, v, opts...)
}

func (p *putRecorder) distinct() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	seen := map[string]bool{}
	for _, k := range p.puts {
		seen[k] = true
	}
	return len(seen)
}

// TestMemoryKey_SameWriterCalledTwiceDoesNotOverwrite 钉住**同一写入者在一次任务里被调
// 多次时不撞键**。
//
// 键一度是 `<RunID>_assistant_<session 事件序号>`。那个序号**不是我们的**——它数当前
// session 的事件条数，而 AgentAsTool 每次调用**各自新建 child session**，序号都从头数
// ⟹ 键相同 ⟹ **后写静默覆盖先写**。
//
// ⚠️ **fixture 必须是「同一个 Run 里调两次」**。第一版写成「两次独立 Run」——那是两个
// 不同的 RunID，键怎么都不会撞，用例**恒绿**。是 sanity-revert 才发现的：回退到序号键时
// 它照样过，红的是另一条。空转的断言比没有断言更糟，因为它看起来在守着。
//
// 承重的量是**落下来几条**，不是 Put 调了几次——覆盖发生在存储层，调用次数不变。
func TestMemoryKey_SameWriterCalledTwiceDoesNotOverwrite(t *testing.T) {
	base := store.NS("mem")
	rec := &putRecorder{Store: store.NewInMemoryStore()}
	ctx := store.WithContext(context.Background(), rec)

	// 子 agent 每次答不同内容；它自己配 StoreMemory ⟹ 每次调用都写一条
	child := agent_v2.NewBasicAgent(
		agent_v2.WithName("child"), agent_v2.WithModel(&countingAnswerStub{}),
		agent_v2.WithStoreMemory(base, &agent_v2.StoreMemoryConfig{}),
		agent_v2.WithMaxRounds(2))
	kit := tool.NewToolkit()
	kit.Register(agent_v2.AgentAsTool(child, agent_v2.WithAgentToolDescription("问子专家")))

	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"), agent_v2.WithModel(&callChildTwiceStub{}),
		agent_v2.WithToolkit(kit), agent_v2.WithMaxRounds(6))
	if _, err := parent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("问两次")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	rec.mu.Lock()
	puts := append([]string(nil), rec.puts...)
	rec.mu.Unlock()
	childPuts := 0
	for _, k := range puts {
		if strings.Contains(k, "child") {
			childPuts++
		}
	}
	if childPuts < 2 {
		t.Fatalf("fixture 没搭对：子 agent 该被调两次各写一条，实际 child 相关 Put %d 次（%v）",
			childPuts, puts)
	}
	if n := rec.distinct(); n != len(puts) {
		t.Errorf("%d 次 Put 只落了 %d 个不同 (ns,key)——撞键了，后写静默覆盖先写。puts=%v",
			len(puts), n, puts)
	}
}

// TestMemoryKey_SameContentIsIdempotent 钉住**同内容同键**——这条守的是 Resume 幂等。
//
// ⚠️ 这不是「顺带的好处」，是**约束**：commitFinal 在 Resume 时会重跑（Put 抛中断时
// ExitEvent 不 add、Resume 重跑 Put 成功后才 add）。换成随机键（uuid）会把「N≥2 才错」
// 换成**「每次 Resume 都多写一条」**——从偶发变必然。所以键必须是内容的函数。
func TestMemoryKey_SameContentIsIdempotent(t *testing.T) {
	base := store.NS("mem")
	rec := &putRecorder{Store: store.NewInMemoryStore()}
	ctx := store.WithContext(context.Background(), rec)
	sess := agent_v2.NewRunSession()

	// 同一个 session 里跑两次、模型给同样的答复 ⟹ 同键 ⟹ 只该落一条
	for i := 0; i < 2; i++ {
		ag := agent_v2.NewBasicAgent(
			agent_v2.WithName("m"), agent_v2.WithModel(&fixedAnswerStub{answer: "同一条结论"}),
			agent_v2.WithStoreMemory(base, &agent_v2.StoreMemoryConfig{}),
			agent_v2.WithMaxRounds(2))
		if _, err := ag.Run(ctx, sess, schema.UserMessage("问题")); err != nil {
			t.Fatalf("Run %d: %v", i, err)
		}
	}
	if n := rec.distinct(); n != 1 {
		t.Errorf("同一 session、同样内容该落 1 个 (ns,key)，实际 %d 个（puts=%v）——"+
			"键要是含随机/递增维，Resume 重跑就会每次多写一条", n, rec.puts)
	}
}

type fixedAnswerStub struct{ answer string }

func (f *fixedAnswerStub) Generate(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(f.answer), nil
}
func (f *fixedAnswerStub) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (f *fixedAnswerStub) WithTools(t []*schema.ToolInfo) (model.ChatModel, error) { return f, nil }

// countingAnswerStub 每次调用答不同内容（子 agent 用）。
type countingAnswerStub struct {
	mu sync.Mutex
	n  int
}

func (c *countingAnswerStub) Generate(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	c.mu.Lock()
	c.n++
	n := c.n
	c.mu.Unlock()
	return schema.AssistantMessage(fmt.Sprintf("子专家的第 %d 条结论", n)), nil
}
func (c *countingAnswerStub) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (c *countingAnswerStub) WithTools(t []*schema.ToolInfo) (model.ChatModel, error) { return c, nil }

// callChildTwiceStub 父 agent：连调两次子工具，然后收尾。
type callChildTwiceStub struct {
	mu    sync.Mutex
	calls int
}

func (p *callChildTwiceStub) Generate(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	p.mu.Lock()
	p.calls++
	n := p.calls
	p.mu.Unlock()
	if n <= 2 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID:       fmt.Sprintf("tc%d", n),
			Function: schema.FunctionCall{Name: "child", Arguments: `{"input":"go"}`},
		}}), nil
	}
	return schema.AssistantMessage("父的最终答复"), nil
}
func (p *callChildTwiceStub) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (p *callChildTwiceStub) WithTools(t []*schema.ToolInfo) (model.ChatModel, error) { return p, nil }
