package agent_v2_test

// MicroCompact 的 offload 跨轮去重哨兵。
//
// MicroCompact 是「每轮重扫历史挑旧的清」的形态（`KeepRecent` 是按新旧淘汰，「旧」
// 相对全局位置定义、产生时不可知，只能重扫）。重扫本身免费，但 `OffloadEnabled` 给
// 这条免费路径挂上了**远程写**——而消息上的「已 offload」marker 活不到下一轮
// （它设在 clone 上，clone 不写回 channel），所以不拦的话 `store.Put` 次数随轮数
// **平方**增长。修复前实测：5 轮里该 offload 的 4 条，Put 了 10 次。

import (
	"context"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

// countingStore 数 Put 次数并记 key 序列。
type countingStore struct {
	store.Store
	mu   sync.Mutex
	keys []string
}

func (c *countingStore) Put(ctx context.Context, ns store.Namespace, key string, val any, opts ...store.PutOption) error {
	c.mu.Lock()
	c.keys = append(c.keys, key)
	c.mu.Unlock()
	return c.Store.Put(ctx, ns, key, val, opts...)
}

func (c *countingStore) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.keys))
	copy(out, c.keys)
	return out
}

func TestMicroCompactOffload_NoRepeatedPutAcrossRounds(t *testing.T) {
	cs := &countingStore{Store: store.NewInMemoryStore()}
	readTool := tool.NewFuncTool("read_file", "读", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Success(strings.Repeat("Y", 300)), nil
		})
	tk := tool.NewToolkit()
	tk.Register(readTool)

	rd := func(id string) *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: id, Function: schema.FunctionCall{Name: "read_file", Arguments: `{}`}}})
	}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("offAg"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			rd("o1"), rd("o2"), rd("o3"), rd("o4"), rd("o5"),
			schema.AssistantMessage("done"),
		})),
		agent_v2.WithToolkit(tk),
		agent_v2.WithStore(cs),
		agent_v2.WithMicroCompact(&agent_v2.MicroCompactConfig{
			Enabled: true, KeepRecent: 1, CompactableTools: []string{"read_file"},
			OffloadEnabled: true,
		}),
		agent_v2.WithMaxRounds(10))

	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := cs.snapshot()
	counts := map[string]int{}
	for _, k := range got {
		counts[k]++
	}
	if len(counts) == 0 {
		t.Fatal("前提不成立：一次 offload 都没发生，本测试测的是空气")
	}
	for k, n := range counts {
		if n != 1 {
			t.Errorf("key %q 被 Put 了 %d 次，应恰 1 次\n"+
				"  全序列 %v\n"+
				"  >1 = 跨轮去重没生效：每轮重扫历史都把同一条旧 tool_result 挑出来重写一遍，\n"+
				"  次数随轮数平方增长（远程 store 上就是每轮 N 次多余远程写）", k, n, got)
		}
	}
	t.Logf("Put 序列 = %v（%d 个 key 各 1 次）", got, len(counts))
}

// TestMicroCompactOffload_DedupIsPerRun 去重集合不跨 Run——同 sid 第二次 Run 照常卸载。
//
// 这条是上一条的反面：去重范围必须**恰好**是一次 Run。挂到跨 Run 复用的地方
// （比如 MicroCompactConfig，它在 agent 构造期建一次）会让另一个 Run 里撞了
// tool_call ID 的消息被**错误跳过卸载**——那是真数据丢失，比重复写坏得多。
func TestMicroCompactOffload_DedupIsPerRun(t *testing.T) {
	cs := &countingStore{Store: store.NewInMemoryStore()}
	readTool := tool.NewFuncTool("read_file", "读", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Success(strings.Repeat("Z", 300)), nil
		})
	tk := tool.NewToolkit()
	tk.Register(readTool)
	rd := func(id string) *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: id, Function: schema.FunctionCall{Name: "read_file", Arguments: `{}`}}})
	}
	mk := func() *agent_v2.BasicAgent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("offAg2"),
			agent_v2.WithModel(newStubModel([]*schema.Message{
				rd("same_1"), rd("same_2"), schema.AssistantMessage("done"),
			})),
			agent_v2.WithToolkit(tk), agent_v2.WithStore(cs),
			agent_v2.WithMicroCompact(&agent_v2.MicroCompactConfig{
				Enabled: true, KeepRecent: 1, CompactableTools: []string{"read_file"},
				OffloadEnabled: true,
			}),
			agent_v2.WithMaxRounds(6))
	}

	// **两个独立 Run 用同一个 tool_call ID**（模拟撞 ID）
	if _, err := mk().Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("run1")); err != nil {
		t.Fatalf("Run1: %v", err)
	}
	afterRun1 := len(cs.snapshot())
	if afterRun1 == 0 {
		t.Fatal("前提不成立：Run1 没有 offload")
	}
	if _, err := mk().Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("run2")); err != nil {
		t.Fatalf("Run2: %v", err)
	}
	afterRun2 := len(cs.snapshot())

	if afterRun2 == afterRun1 {
		t.Errorf("Run2 一次 offload 都没发生（Put 总数停在 %d）——去重集合跨 Run 复用了。\n"+
			"  两个 Run 撞了同一个 tool_call ID 时，Run2 的内容会被**错误跳过卸载** ⟹ 真数据丢失。\n"+
			"  去重范围必须恰好是一次 Run（Put 序列 %v）", afterRun1, cs.snapshot())
	}
	t.Logf("Run1 后 %d 次 Put，Run2 后 %d 次（每 Run 各卸各的）", afterRun1, afterRun2)
}
