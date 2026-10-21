package agent_v2_test

// 渐进披露的揭示集按任务分：上一个任务检索解锁的工具，下一个任务一开始看不见、也调不了。

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// boundRecordingModel 记下每次调用时绑着哪些工具，按「第几次调用」给出回复。
type boundRecordingModel struct {
	mu     sync.Mutex
	bound  []string
	seen   [][]string
	script func(n int) *schema.Message
}

func (m *boundRecordingModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	n := len(m.seen)
	m.seen = append(m.seen, append([]string(nil), m.bound...))
	m.mu.Unlock()
	return m.script(n), nil
}

func (m *boundRecordingModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *boundRecordingModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	names := make([]string, 0, len(tools))
	for _, t := range tools {
		names = append(names, t.Name)
	}
	m.mu.Lock()
	m.bound = names
	m.mu.Unlock()
	return m, nil
}

func (m *boundRecordingModel) boundAt(n int) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n >= len(m.seen) {
		return nil
	}
	return m.seen[n]
}

func oneCall(name, args string) *schema.Message {
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c", Function: schema.FunctionCall{Name: name, Arguments: args}}})
}

// TestToolSearch_RevealedSetPerTask：任务 A 检索解锁 lookup 并调用它；同一个 session 开任务 B（NextTask 换任务，或者
// 复用同一个 RunID 再跑一次顶层 Run），B 第一轮的 bind list 里没有 lookup，B 直接调它被执行闸拒掉、函数体不执行。
// 曾经揭示集挂在会话上只按位置分，A 解锁的工具 B 一开始就绑着、直接调也照样执行（审计实测）。
func TestToolSearch_RevealedSetPerTask(t *testing.T) {
	for _, nextTask := range []bool{true, false} {
		var lookups atomic.Int32
		kit := tool.NewToolkit()
		kit.Register(tool.NewFuncTool("lookup", "查资料", map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) {
				lookups.Add(1)
				return tool.Success("资料"), nil
			}))
		newAgent := func(m model.ChatModel) agent_v2.Agent {
			return agent_v2.NewBasicAgent(agent_v2.WithName("per_task"), agent_v2.WithModel(m), agent_v2.WithToolkit(kit),
				agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true,
					Deferred: func(tl tool.Tool) bool { return tl.Name() == "lookup" }}),
				agent_v2.WithMaxRounds(5))
		}
		sess := agent_v2.NewRunSession()

		mA := &boundRecordingModel{script: func(n int) *schema.Message {
			switch n {
			case 0:
				return oneCall("tool_search", `{"query":"lookup"}`)
			case 1:
				return oneCall("lookup", "{}")
			}
			return schema.AssistantMessage("A 完成")
		}}
		if _, err := newAgent(mA).Run(context.Background(), sess, schema.UserMessage("查")); err != nil {
			t.Fatalf("任务 A：%v", err)
		}
		if !slices.Contains(mA.boundAt(1), "lookup") || lookups.Load() != 1 {
			t.Fatalf("fixture 没搭对：任务 A 检索之后应绑着 lookup 并执行一次，实际绑定 %v、执行 %d 次", mA.boundAt(1), lookups.Load())
		}

		if nextTask {
			sess.NextTask()
		}
		mB := &boundRecordingModel{script: func(n int) *schema.Message {
			if n == 0 {
				return oneCall("lookup", "{}") // 没检索就直接调
			}
			return schema.AssistantMessage("B 完成")
		}}
		if _, err := newAgent(mB).Run(context.Background(), sess, schema.UserMessage("你好")); err != nil {
			t.Fatalf("任务 B（NextTask=%v）：%v", nextTask, err)
		}
		if slices.Contains(mB.boundAt(0), "lookup") {
			t.Errorf("NextTask=%v：任务 B 第一轮不该绑着上一个任务解锁的 lookup，实际 %v", nextTask, mB.boundAt(0))
		}
		if n := lookups.Load(); n != 1 {
			t.Errorf("NextTask=%v：任务 B 没检索就调 lookup 应被拒、不执行，实际累计执行 %d 次（A 那 1 次之外又执行了）", nextTask, n)
		}
	}
}
