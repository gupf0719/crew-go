package server

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// approveOnceModel 第一轮调 name 这个工具，看到工具结果后给出最终答复。
type approveOnceModel struct{ name string }

func (m *approveOnceModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for _, ms := range msgs {
		if ms.Role == schema.RoleTool {
			return schema.AssistantMessage("done"), nil
		}
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c1", Type: "function",
		Function: schema.FunctionCall{Name: m.name, Arguments: `{}`}}}), nil
}

func (m *approveOnceModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(msg), nil
}

func (m *approveOnceModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// 请重答的那次续发期间来了 Cancel（有在途执行时 Cancel 只打标记、把清理留给在途的那次）：请重答之后要接手清理——删
// 条目、清存档——且不再发 input-required（Cancel 已经发了 canceled）。曾经直接放掉 busy：条目、存档、观测会话都留着，
// 之后的续发拿到「已取消」，再来的 Cancel 被 SDK 短路（task 已是 canceled），再也没有路径清掉它们（审计实测）。
//
// 时序是确定的：在 e.mu 里看到这次续发占着 busy 时打上 Cancel 的标记——持锁期间它放不掉 busy，于是放 busy 那一刻
// 一定看得到标记。窗口靠一条很长的认不出的审批答复撑开（解析它要几毫秒）；没赶上就再续发一次。
func TestReask_CancelDuringReaskCleansUp(t *testing.T) {
	runs := 0
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("refund", "refund", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { runs++; return tool.Success("ok"), nil }))
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("approver"), agent_v2.WithModel(&approveOnceModel{name: "refund"}),
		agent_v2.WithToolkit(tk), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), agent_v2.WithMaxRounds(4))
	e := NewExecutor(ag, WithOwnerResolver(func(context.Context) string { return "" }))
	ctx := context.Background()
	const tid = a2atypes.TaskID("t-reask-cancel")
	k := taskKey{taskID: tid}

	start := &a2asrv.ExecutorContext{TaskID: tid, ContextID: "c-1",
		Message: a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("go"))}
	if got := lastStatus(drain(e.Execute(ctx, start))); got != a2atypes.TaskStateInputRequired {
		t.Fatalf("前置：应停在审批上，got %s", got)
	}
	e.mu.Lock()
	st := e.tasks[k]
	sid := st.sess.RunID
	e.mu.Unlock()

	// 一条认不出的审批答复（长只是为了把占 busy 到放 busy 之间撑开，答复本身照样是「认不出」⟹ 请重答）
	big := strings.Repeat("x", 8<<20)
	resume := func(text string) *a2asrv.ExecutorContext {
		return &a2asrv.ExecutorContext{TaskID: tid, ContextID: "c-1", StoredTask: &a2atypes.Task{ID: tid, ContextID: "c-1"},
			Message: a2atypes.NewMessageForTask(a2atypes.MessageRoleUser, a2atypes.TaskInfo{TaskID: tid, ContextID: "c-1"}, a2atypes.NewTextPart(text))}
	}
	var evs []a2atypes.Event
	caught := false
	for attempt := 0; attempt < 20 && !caught; attempt++ {
		done := make(chan []a2atypes.Event, 1)
		go func() { done <- drain(e.Execute(ctx, resume(big))) }()
		for !caught {
			e.mu.Lock()
			if st.busy {
				// Cancel 在有在途执行时做的就是这一件（见 Cancel：busy 分支只打标记，停在途 run 的 cancelRun 此时为 nil）
				st.canceled = true
				caught = true
			}
			e.mu.Unlock()
			if caught {
				break
			}
			select {
			case evs = <-done: // 没赶上：这次已经请重答完了，再来一次
			default:
				runtime.Gosched()
				continue
			}
			break
		}
		if caught {
			evs = <-done
		}
	}
	if !caught {
		t.Fatal("20 次续发都没赶上占着 busy 的窗口——测试没有生效")
	}

	if got := lastStatus(evs); got != a2atypes.TaskStateUnspecified {
		t.Fatalf("已取消的任务不该再发状态（Cancel 发过 canceled 了），实际发了 %s", got)
	}
	e.mu.Lock()
	_, present := e.tasks[k]
	e.mu.Unlock()
	if present {
		t.Fatal("请重答期间被取消：任务条目应已删除")
	}
	if exists, err := e.cp.ExistsNS(ctx, sid, ""); err != nil || exists {
		t.Fatalf("请重答期间被取消：存档应已清（exists=%v err=%v）", exists, err)
	}
	if got := lastStatus(drain(e.Execute(ctx, resume("approve")))); got != a2atypes.TaskStateFailed {
		t.Fatalf("取消之后再续发应失败（没有可续的状态），实际 %s", got)
	}
	if runs != 0 {
		t.Fatalf("全程工具不该执行，实际 %d 次", runs)
	}
}
