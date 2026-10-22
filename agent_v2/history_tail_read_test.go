package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// tailPromptModel：输入最后一条是工具结果就收尾，否则发一个工具调用；记下每次调用看到的消息（角色 + 正文）。
type tailPromptModel struct {
	mu      sync.Mutex
	seq     int
	prompts []string
}

func (m *tailPromptModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var b strings.Builder
	for _, x := range msgs {
		fmt.Fprintf(&b, "[%s]%s|", x.Role, x.Content)
	}
	m.prompts = append(m.prompts, b.String())
	if msgs[len(msgs)-1].Role == schema.RoleTool {
		return schema.AssistantMessage(fmt.Sprintf("答复 %d", m.seq)), nil
	}
	m.seq++
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: fmt.Sprintf("c%d", m.seq), Type: "function",
		Function: schema.FunctionCall{Name: "lookup", Arguments: `{}`}}}), nil
}
func (m *tailPromptModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *tailPromptModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *tailPromptModel) last(n int) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.prompts[len(m.prompts)-n:]...)
}

// wholeStore 模拟只读尾部之前的后端行为：不管 Limit，每次都把整场会话返回去。框架拿到的第一页就是
// 全部，于是它在整个视图上截——这正是改动之前的结果，拿来当对照。
type wholeStore struct {
	inner *agent_v2.InMemoryHistoryStore
	msgs  atomic.Int64
}

func (s *wholeStore) Append(ctx context.Context, k agent_v2.HistoryKey, m []*schema.Message) error {
	return s.inner.Append(ctx, k, m)
}
func (s *wholeStore) Load(ctx context.Context, k agent_v2.HistoryKey, _ agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	p, err := s.inner.Load(ctx, k, agent_v2.HistoryQuery{})
	s.msgs.Add(int64(len(p.Messages)))
	return p, err
}

type pagedStore struct {
	inner *agent_v2.InMemoryHistoryStore
	msgs  atomic.Int64
}

func (s *pagedStore) Append(ctx context.Context, k agent_v2.HistoryKey, m []*schema.Message) error {
	return s.inner.Append(ctx, k, m)
}
func (s *pagedStore) Load(ctx context.Context, k agent_v2.HistoryKey, q agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	p, err := s.inner.Load(ctx, k, q)
	s.msgs.Add(int64(len(p.Messages)))
	return p, err
}

func lookupToolkit() *tool.Toolkit {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("lookup", "查", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: "查到的结果"}, nil
		}))
	return tk
}

// TestHistory_TailRead_SamePromptAsWholeRead：长会话里，只读尾部时模型收到的 prompt 跟「后端每次都
// 返回整场会话」时**逐条相同**，而读回的条数少一个数量级。
//
// 两个后端里是同一份历史（同一批真实 Run 攒出来的），再各跑一个 Run 比较模型看到的东西。单 agent 与
// 三步顺序编排（SelfOnly 视图，三个 agent 的消息交错在一条流上）各一遍。
func TestHistory_TailRead_SamePromptAsWholeRead(t *testing.T) {
	type topo struct {
		name  string
		build func(hs agent_v2.HistoryStore, m model.ChatModel) agent_v2.Agent
		calls int // 最后一个 Run 里模型被调几次
	}
	basic := func(name string, hs agent_v2.HistoryStore, m model.ChatModel) *agent_v2.BasicAgent {
		return agent_v2.NewBasicAgent(agent_v2.WithName(name), agent_v2.WithModel(m),
			agent_v2.WithToolkit(lookupToolkit()), agent_v2.WithHistory(hs, "t"))
	}
	topos := []topo{
		{"single", func(hs agent_v2.HistoryStore, m model.ChatModel) agent_v2.Agent { return basic("solo", hs, m) }, 2},
		{"seq3", func(hs agent_v2.HistoryStore, m model.ChatModel) agent_v2.Agent {
			s := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"))
			s.AddStep("s1", basic("a1", hs, m))
			s.AddStep("s2", basic("a2", hs, m))
			s.AddStep("s3", basic("a3", hs, m))
			return s
		}, 6},
	}
	for _, tp := range topos {
		t.Run(tp.name, func(t *testing.T) {
			ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-tail")
			key := agent_v2.HistoryKey{Owner: "t", ConvID: "conv-tail"}
			src := agent_v2.NewInMemoryHistoryStore()
			builder := tp.build(src, &tailPromptModel{})
			for i := 0; i < 150; i++ {
				if _, err := builder.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(fmt.Sprintf("问题 %d", i))); err != nil {
					t.Fatal(err)
				}
			}
			all, _ := agent_v2.LoadAllHistory(ctx, src, key)

			run := func(hs agent_v2.HistoryStore) []string {
				m := &tailPromptModel{seq: 100000}
				if _, err := tp.build(hs, m).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("最后一问")); err != nil {
					t.Fatal(err)
				}
				return m.last(tp.calls)
			}
			paged := &pagedStore{inner: agent_v2.NewInMemoryHistoryStore()}
			whole := &wholeStore{inner: agent_v2.NewInMemoryHistoryStore()}
			_ = paged.inner.Append(ctx, key, all)
			_ = whole.inner.Append(ctx, key, all)

			got, want := run(paged), run(whole)
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("第 %d 次模型调用看到的 prompt 不同\n只读尾部: %s\n读整场:   %s", i, got[i], want[i])
				}
			}
			if p, w := paged.msgs.Load(), whole.msgs.Load(); p*5 > w {
				t.Fatalf("会话 %d 条：只读尾部读回 %d 条，读整场 %d 条——没省下来", len(all), p, w)
			} else {
				t.Logf("会话 %d 条：只读尾部读回 %d 条，读整场 %d 条", len(all), p, w)
			}
		})
	}
}
