package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// breakLoopMockAgent 每次 Run 输出带 break_loop annotation——模拟 body 主动 break。
type breakLoopMockAgent struct {
	name  string
	calls *int
}

func (a *breakLoopMockAgent) Run(_ context.Context, _ *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	*a.calls++
	msg := schema.AssistantMessage("inner-done")
	agent_v2.SetBreakLoop(msg) // 用新 helper（验证 #1 常量化 + key 收口）
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, msg)}, nil
}
func (a *breakLoopMockAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *breakLoopMockAgent) Name() string        { return a.name }
func (a *breakLoopMockAgent) Description() string { return "" }

// TestLoop_NestedBreakLoop_NoLeak 验证（audit Loop review MED-2 疑似 bug）：内层 LoopAgent
// 的 break_loop annotation 不得泄漏到外层 LoopAgent 误触发外层提前退出。
//
// 构造：内层 LoopAgent(body=每轮打 break_loop 的 mock，1 轮即 break) 嵌进外层
// LoopAgent(maxIter=3, 无 exitCond)。外层无退出条件应跑满 maxIter=3 轮 → 内层被调 3 次。
// 若内层 final 带的 break_loop annotation 泄漏到外层 lastOut → 外层第 1 轮 exitCheck 误命中
// → 外层提前退 → 内层只调 1 次。
func TestLoop_NestedBreakLoop_NoLeak(t *testing.T) {
	calls := 0
	body := &breakLoopMockAgent{name: "worker", calls: &calls}
	inner := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("inner"),
		agent_v2.WithLoopMaxIterations(5),
	).WithBody(body)
	outer := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("outer"),
		agent_v2.WithLoopMaxIterations(3),
	).WithBody(inner)

	_, err := outer.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go"))
	if err != nil {
		t.Fatalf("outer.Run: %v", err)
	}

	if calls != 3 {
		t.Errorf("嵌套 break_loop 泄漏：worker 期望调 3 次（外层 maxIter=3 无 exitCond 应跑满），实际 %d —— 内层 final 的 break_loop annotation 泄漏到外层 lastOut 误触发外层提前退出（audit MED-2）", calls)
	}
}
