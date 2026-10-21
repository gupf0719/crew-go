package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// nonZeroSizeAgent 是非零大小的 stub——空结构体在 Go 里两个实例可能共享地址，
// 测"同实例"必须用有字段的类型，否则测的是地址巧合不是实例身份。
type nonZeroSizeAgent struct{ tag string }

func (a *nonZeroSizeAgent) Run(context.Context, *agent_v2.RunSession, *schema.Message, ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.tag, schema.AssistantMessage("done:"+a.tag))}, nil
}

func (a *nonZeroSizeAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *nonZeroSizeAgent) Name() string        { return a.tag }
func (a *nonZeroSizeAgent) Description() string { return "stub" }

// TestParallelAgent_SameInstanceMultiBranch_Allowed：同一个 agent 实例可以挂多个分支。
//
// 曾经拦过（历史归属按 agent 名标时，同实例的分支归属会撞、产出合并成一条流）。归属改用
// **分支注册名**后两个分支天然分属 "br1"/"br2"，限制撤销——根因修掉了就不该再用限制掩盖。
// best-of-N 采样正是这个形状。
//
// 归属真的分开由 history_slot_test.go 的端到端用例钉住；这里只钉"构造不再 panic"。
func TestParallelAgent_SameInstanceMultiBranch_Allowed(t *testing.T) {
	shared := &nonZeroSizeAgent{tag: "worker"}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("同实例多分支不该再被拦（归属已改用分支注册名），实际 panic: %v", r)
		}
	}()
	agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("br1", shared).
		AddBranch("br2", shared)
}

// TestParallelAgent_DistinctInstances_SameType_Allowed：**不同实例**即使类型相同、
// 名字相同也放行——拦的是"同一个对象出现在两处"，不是"长得一样"。
//
// 这条同时钉住零大小类型不被误判：空结构体 stub 的两个实例可能共享地址，若判定只看
// 指针相等就会把它们误当同一个（实测踩过：一个用 `struct{}` 的既有回归测试因此炸掉）。
func TestParallelAgent_DistinctInstances_SameType_Allowed(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("两个独立实例不该被拦，实际 panic: %v", r)
		}
	}()

	// 有字段的类型：两个实例地址必然不同
	agent_v2.NewParallelAgent(agent_v2.WithParallelName("p1")).
		AddBranch("br1", &nonZeroSizeAgent{tag: "w"}).
		AddBranch("br2", &nonZeroSizeAgent{tag: "w"})

	// 零大小类型：两个实例可能共享地址，同样不该被拦
	agent_v2.NewParallelAgent(agent_v2.WithParallelName("p2")).
		AddBranch("br1", &zeroSizeStubAgent{}).
		AddBranch("br2", &zeroSizeStubAgent{})
}

type zeroSizeStubAgent struct{}

func (a *zeroSizeStubAgent) Run(context.Context, *agent_v2.RunSession, *schema.Message, ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

func (a *zeroSizeStubAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *zeroSizeStubAgent) Name() string        { return "zero" }
func (a *zeroSizeStubAgent) Description() string { return "stub" }
