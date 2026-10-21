package agent_v2_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// barrierModel 在 Generate 第一次被调时撞屏障——强制两个并发 sub-agent 的 transfer
// 写在同一时刻发生（不被 Go scheduler 自然串行化），真实复现 thundering-herd。
// 撞完屏障后返回预设的 transfer tool_call。
type barrierModel struct {
	barrier   *sync.WaitGroup // 两个 model 都 Done 后才放行
	once      sync.Once
	transfer  *schema.Message
	postReply *schema.Message // transfer 后该 active sub-agent 不会再被调（return-direct）；占位
	calls     int
	mu        sync.Mutex
}

func (m *barrierModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	m.calls++
	n := m.calls
	m.mu.Unlock()
	if n == 1 {
		// 第一次调用：撞屏障强制并发，然后发 transfer
		m.once.Do(func() { m.barrier.Done() })
		m.barrier.Wait() // 等对方也到这——两 transfer 写几乎同时
		return m.transfer, nil
	}
	return m.postReply, nil
}
func (m *barrierModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *barrierModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// twoToolCallModel 外层 BasicAgent 的 model：一轮同时 emit 两个 tool_call
// （调 AgentAsTool(HandoffA) + AgentAsTool(HandoffB)）→ 触发 ISSUE-81 N>=2 fanout。
type twoToolCallModel struct {
	toolA, toolB string // 两个 AgentAsTool 的工具名（= HandoffAgent.Name()）
	calls        int
	mu           sync.Mutex
}

func (m *twoToolCallModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	m.calls++
	n := m.calls
	m.mu.Unlock()
	if n == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "outer_tc_0", Function: schema.FunctionCall{Name: m.toolA, Arguments: `{"input":"go A"}`}},
			{ID: "outer_tc_1", Function: schema.FunctionCall{Name: m.toolB, Arguments: `{"input":"go B"}`}},
		}), nil
	}
	return schema.AssistantMessage("done"), nil
}
func (m *twoToolCallModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *twoToolCallModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// recordingTool 记录被调时的 input——验证 active sub-agent 转交后的目标确实被接管。
// 用作各 HandoffAgent 的转交目标 agent 的可观测探针（通过 sub-agent 名区分）。
type captureBox struct {
	mu      sync.Mutex
	visited map[string]int // agent name -> 被激活次数
}

func (c *captureBox) mark(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.visited == nil {
		c.visited = map[string]int{}
	}
	c.visited[name]++
}

// markerModel 接管后回复，并通过 captureBox 记录自己被激活——区分串台。
type markerModel struct {
	name string
	box  *captureBox
}

func (m *markerModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.box.mark(m.name)
	return schema.AssistantMessage(m.name + " handled"), nil
}
func (m *markerModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *markerModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// buildHandoff 造一个 HandoffAgent：initial agent 调 transfer 转给 target agent。
// front 的 model 撞共享屏障强制并发。
func buildHandoff(name, frontName, targetName string, barrier *sync.WaitGroup, box *captureBox) *agent_v2.HandoffAgent {
	front := agent_v2.NewBasicAgent(
		agent_v2.WithName(frontName),
		agent_v2.WithModel(&barrierModel{
			barrier: barrier,
			transfer: schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID:       "transfer_" + frontName,
				Function: schema.FunctionCall{Name: agent_v2.HandoffTransferToolName, Arguments: fmt.Sprintf(`{"agent_name":%q}`, targetName)},
			}}),
			postReply: schema.AssistantMessage("front noop"),
		}),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithMaxRounds(5),
	)
	target := agent_v2.NewBasicAgent(
		agent_v2.WithName(targetName),
		agent_v2.WithModel(&markerModel{name: targetName, box: box}),
		agent_v2.WithMaxRounds(5),
	)
	return agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName(name),
		agent_v2.WithHandoffDescription(name+" handoff"),
		agent_v2.WithHandoffInitial(frontName),
	).AddAgent(frontName, front).AddAgent(targetName, target)
}

// TestHandoff_ConcurrentAgentAsTool_PendingKeyCollision 端到端：外层 BasicAgent 一轮
// 并发调两个 AgentAsTool(HandoffAgent)（ISSUE-81 fanout），两个 HandoffAgent 的转交
// 必须各归各的。
//
// 正确行为：HandoffA 应转交给 targetA（targetA 被激活），HandoffB 应转交给 targetB。
// 曾经的 bug 表现：两个实例的转交信号写进**共享的 session values map**（半共享 child
// sess，ISSUE-79），键只按位置算 ⟹ 互覆 ⟹ 至少一个 HandoffAgent 转交错位。
// 现在信号是各自那次调用交上来的上行信号（按收件人取），共享容器本身没有了。
//
// ⚠️ **必须 -race 跑才可靠复现**：WaitGroup 屏障强制两 transfer 几乎同时，但不带 -race
// 时 Go scheduler 可能让一个 HandoffAgent 完整跑完再跑另一个 → 单次假绿。
// 确定性那半由白盒 TestSideSignal_ConcurrentJoinIsDeterministic 承担（屏障强制并发、跑 300 轮）。
func TestHandoff_ConcurrentAgentAsTool_PendingKeyCollision(t *testing.T) {
	box := &captureBox{}
	var barrier sync.WaitGroup
	barrier.Add(2) // 两个 front 的 barrierModel 各 Done 一次

	handoffA := buildHandoff("handoffA", "frontA", "targetA", &barrier, box)
	handoffB := buildHandoff("handoffB", "frontB", "targetB", &barrier, box)

	outer := agent_v2.NewBasicAgent(
		agent_v2.WithName("outer"),
		agent_v2.WithModel(&twoToolCallModel{toolA: "handoffA", toolB: "handoffB"}),
		agent_v2.WithToolkit(func() *tool.Toolkit {
			tk := tool.NewToolkit()
			tk.Register(agent_v2.AgentAsTool(handoffA))
			tk.Register(agent_v2.AgentAsTool(handoffB))
			return tk
		}()),
		agent_v2.WithMaxRounds(5),
	)

	_, err := outer.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("dispatch both"))
	if err != nil {
		t.Fatalf("outer.Run error: %v", err)
	}

	box.mu.Lock()
	defer box.mu.Unlock()
	t.Logf("activated targets: %v", box.visited)

	// 正确行为：targetA 和 targetB 各被激活恰好 1 次（两 HandoffAgent 各自正确转交）。
	if box.visited["targetA"] != 1 || box.visited["targetB"] != 1 {
		t.Errorf("转交串台/丢失：期望 targetA=1 targetB=1，实际 %v —— pending key 在共享 values map 上碰撞", box.visited)
	}
}
