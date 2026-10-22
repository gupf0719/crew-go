package agent_v2_test

// 开了转存时工具账（tools 节点私有态）在每个调用成功那一刻记下的是转存前的全文，而节点私有态在一次 Run 里不设清除点——
// 这一批完成之后要把账里那几条换成处理之后的那份，否则每一轮的全文都留在内存里直到 Run 结束。

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

// discardSpillStore 只记 Put 次数、不存内容：store 自己不留全文，量出来的就只剩框架留下的。
type discardSpillStore struct {
	store.Store
	mu   sync.Mutex
	puts int
}

func (s *discardSpillStore) Put(context.Context, store.Namespace, string, any, ...store.PutOption) error {
	s.mu.Lock()
	s.puts++
	s.mu.Unlock()
	return nil
}

func (s *discardSpillStore) Get(context.Context, store.Namespace, string) (*store.Item, error) {
	return nil, nil
}

func heapInUse() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// bigRoundsModel 前 n 轮各调一次 big，第 n+1 轮收尾；第 1 轮与收尾那轮各量一次堆。
type bigRoundsModel struct {
	n, calls   int
	base, peak uint64
	maxSeen    int
}

func (m *bigRoundsModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.calls++
	for _, msg := range msgs {
		if msg != nil && len(msg.Content) > m.maxSeen {
			m.maxSeen = len(msg.Content)
		}
	}
	if m.calls == 1 {
		m.base = heapInUse()
	}
	if m.calls <= m.n {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: fmt.Sprintf("b%d", m.calls), Type: "function",
			Function: schema.FunctionCall{Name: "big", Arguments: fmt.Sprintf(`{"i":%d}`, m.calls)}}}), nil
	}
	m.peak = heapInUse()
	return schema.AssistantMessage("done"), nil
}

func (m *bigRoundsModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *bigRoundsModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestToolResultSpill_MemoDoesNotRetainFullOutput：20 轮、每轮 2MB 的工具输出，开了转存（store 不留内容）。模型每轮
// 看到的都是预览；Run 结束前框架自己留在内存里的不该随轮数 × 2MB 增长（工具账没整理时多占约 80MB）。
func TestToolResultSpill_MemoDoesNotRetainFullOutput(t *testing.T) {
	const rounds, size = 20, 2 << 20
	st := &discardSpillStore{}
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("big", "大输出", map[string]any{"type": "object"},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: fmt.Sprintf("%v:", args["i"]) + strings.Repeat("y", size)}, nil
		}))
	m := &bigRoundsModel{n: rounds}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("mem"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(rounds+5),
		agent_v2.WithToolResultSpill(&agent_v2.ToolResultSpillConfig{Threshold: 1000, PreviewBytes: 300, Store: st}))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatal(err)
	}
	if st.puts != rounds {
		t.Fatalf("fixture 没搭对：应转存 %d 次，实际 %d", rounds, st.puts)
	}
	if m.maxSeen > 5000 {
		t.Fatalf("fixture 没搭对：模型看到的消息最长 %d 字节，转存没生效", m.maxSeen)
	}
	growth := int64(m.peak) - int64(m.base)
	if growth > 16<<20 {
		t.Fatalf("Run 进行中框架留在内存里的随轮数增长：多占 %.1f MB（%d 轮 × %.1f MB 输出）——转存前的全文留在每一轮的工具账里",
			float64(growth)/(1<<20), rounds, float64(size)/(1<<20))
	}
	t.Logf("收尾那轮比第 1 轮多占堆 %.1f MB（%d 轮 × %.1f MB 输出）", float64(growth)/(1<<20), rounds, float64(size)/(1<<20))
}
