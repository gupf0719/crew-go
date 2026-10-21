package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// TestR31_ParallelAgent_AddBranch_DuplicateNamePanic 复现 R31 视角 A HIGH-1：
//
// 同名 branch 静默丢失——AddBranch 不去重，Compile 时 compose_v2.AddNode(NodeID(
// "worker_"+name), fn) 是 map[id]=fn 赋值，第二个同名 worker 的 fn 静默覆盖第一个；
// 同时上层 branchFn 用 `JoinNS(parentNS, name)` push graphNS，同名 branch 共用同
// childNS 让 cp 互覆 (HITL Resume 永久 deadlock)。
//
// fix 后：AddBranch 检测重复名 fail-fast panic。
func TestR31_ParallelAgent_AddBranch_DuplicateNamePanic(t *testing.T) {
	stub1 := newStubModel([]*schema.Message{schema.AssistantMessage("res1")})
	stub2 := newStubModel([]*schema.Message{schema.AssistantMessage("res2")})

	a1 := agent_v2.NewBasicAgent(agent_v2.WithName("a1"), agent_v2.WithModel(stub1))
	a2 := agent_v2.NewBasicAgent(agent_v2.WithName("a2"), agent_v2.WithModel(stub2))

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for duplicate branch name 'worker'")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected panic string, got %T: %v", r, r)
		}
		if !strings.Contains(msg, "duplicate branch name") {
			t.Fatalf("expected 'duplicate branch name' in panic msg, got %q", msg)
		}
		if !strings.Contains(msg, `"worker"`) {
			t.Fatalf("expected branch name 'worker' in panic msg, got %q", msg)
		}
	}()

	agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("worker", a1).
		AddBranch("worker", a2) // 应 panic
}

// TestR31_ComposeParallelBuilder_AddBranch_DuplicateNamePanic 验证 compose_v2
// 底层 agent_v2.ParallelAgentBuilder.AddBranch 同样 fail-fast——业务直接用低阶 API 时也防。
func TestR31_ComposeParallelBuilder_AddBranch_DuplicateNamePanic(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for duplicate branch name 'work'")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected panic string, got %T: %v", r, r)
		}
		if !strings.Contains(msg, "duplicate branch name") {
			t.Fatalf("expected 'duplicate branch name' in panic msg, got %q", msg)
		}
	}()

	noop := func(_ context.Context, _ *schema.Message) (*schema.Message, error) {
		return schema.AssistantMessage(""), nil
	}
	agent_v2.NewParallelAgentBuilder().
		WithName("p").
		AddBranch("work", noop).
		AddBranch("work", noop) // 应 panic
}
