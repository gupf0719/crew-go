package agent_v2

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// lookupLoopModel 按输入决定回复（读输入而不是按调用次数背剧本，同一个模型桩能跑任意多个任务）：这一轮的用户消息
// 之后还没检索过就检索 lookup；检索过、lookup 调了不到 3 次就再调一次（同样参数）；否则收尾。顺带数每一轮输入里
// 的重复调用提醒。
type lookupLoopModel struct {
	mu        sync.Mutex
	reminders int
}

func (m *lookupLoopModel) Generate(_ context.Context, in []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	searched, lookups := false, 0
	for i := len(in) - 1; i >= 0; i-- {
		msg := in[i]
		if msg.Annotations[RepeatToolReminderAnnotation] != nil {
			m.reminders++
			continue
		}
		if msg.Role == schema.RoleUser {
			break // 这一轮的用户消息：再往前是上一个任务的
		}
		for _, tc := range schema.GetToolCalls(msg) {
			switch tc.Function.Name {
			case toolSearchToolName:
				searched = true
			case "lookup":
				lookups++
			}
		}
	}
	switch {
	case !searched:
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "s", Type: "function",
			Function: schema.FunctionCall{Name: toolSearchToolName, Arguments: `{"query":"lookup"}`}}}), nil
	case lookups < 3:
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "l", Type: "function",
			Function: schema.FunctionCall{Name: "lookup", Arguments: `{"q":1}`}}}), nil
	}
	return schema.AssistantMessage("完成"), nil
}

func (m *lookupLoopModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *lookupLoopModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestTaskState_SessionKVStaysEmpty：同一个 session 连跑几个任务（每个任务都检索解锁 lookup、再用同样参数连调
// 3 次触发重复调用提醒），会话 KV 上一个键都不留——解锁集、这一批的解锁记录、重复调用计数链都在任务自己的状态里，
// approve_always 没用过时也不写。曾经每个任务每条调用流留 3 个键、换任务时 approve_always 还写一个空键，长期复用
// 一个 session 的聊天循环里随消息数线性增长（审计实测 [4 8 12 16 20]）。
//
// 正向断言同时钉住功能真的在干活：每个任务 lookup 都执行了 3 次（解锁生效），每个任务都收到了提醒（计数生效）——
// 只断言「没有键」的话，两个功能都静默不工作时这条照样绿。
func TestTaskState_SessionKVStaysEmpty(t *testing.T) {
	var lookups atomic.Int32
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("lookup", "查资料 lookup", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			lookups.Add(1)
			return tool.Success("资料"), nil
		}))
	m := &lookupLoopModel{}
	ag := NewBasicAgent(WithName("kv_growth"), WithModel(m), WithToolkit(kit),
		WithToolSearch(&ToolSearchConfig{Enabled: true, Deferred: func(tl tool.Tool) bool { return tl.Name() == "lookup" }}),
		WithRepeatToolReminder(&RepeatToolConfig{Enabled: true, Thresholds: []int{3}}),
		WithMaxRounds(8))
	sess := NewRunSession()
	for i := 1; i <= 5; i++ {
		if i > 1 {
			sess.NextTask()
		}
		m.mu.Lock()
		m.reminders = 0
		m.mu.Unlock()
		if _, err := ag.Run(context.Background(), sess, schema.UserMessage("查")); err != nil {
			t.Fatalf("任务 %d：%v", i, err)
		}
		if got := lookups.Load(); got != int32(3*i) {
			t.Fatalf("fixture 没搭对：任务 %d 之后 lookup 应累计执行 %d 次（每个任务解锁后调 3 次），实际 %d", i, 3*i, got)
		}
		// 第 3 次调用之后的那一轮（收尾那轮）输入里有一条提醒。
		if m.reminders != 1 {
			t.Fatalf("fixture 没搭对：任务 %d 的收尾那轮应看到 1 条提醒，实际 %d", i, m.reminders)
		}
		sess.ensureValues()
		sess.valuesMu.RLock()
		keys := make([]string, 0, len(sess.values))
		for k := range sess.values {
			keys = append(keys, k)
		}
		sess.valuesMu.RUnlock()
		if len(keys) != 0 {
			t.Fatalf("任务 %d 之后会话 KV 上有 %d 个键：%q", i, len(keys), keys)
		}
	}
}

// TestToolsGate_IgnoresSameBatchReveals：执行闸只认这一批开始前已提交的解锁集，同一批里检索已经记下的名字不放行。
// 直接调 toolsFn、事先把 lookup 记进这一批的解锁记录（相当于同一批里排在前面、已经跑完的那次检索），不靠并发调度去
// 碰那个时序——黑盒那条（TestToolSearch_SameBatchCallStaysLocked）在闸读错了那份时也多半是绿的，因为检索通常还没跑完。
// 对照组：已提交的解锁集里有 lookup 时照常执行，证明拒绝不是别的原因。
func TestToolsGate_IgnoresSameBatchReveals(t *testing.T) {
	var lookups atomic.Int32
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("lookup", "查资料", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			lookups.Add(1)
			return tool.Success("资料"), nil
		}))
	a := NewBasicAgent(WithName("gate"), WithModel(&lookupLoopModel{}), WithToolkit(kit),
		WithToolSearch(&ToolSearchConfig{Enabled: true, Deferred: func(tl tool.Tool) bool { return tl.Name() == "lookup" }}))
	toolsFn := a.makeToolsFn(memberGrants{}, a.composeDispatchToolkit(memberGrants{}, false))
	call := []schema.ToolCall{{ID: "l", Type: "function", Function: schema.FunctionCall{Name: "lookup", Arguments: `{}`}}}

	turn := &revealTurn{}
	turn.add("lookup")
	ctx := withRevealTurn(withRevealedFromChannel(context.Background(), nil), turn)
	results, err := toolsFn(ctx, call, []context.Context{ctx})
	if err != nil {
		t.Fatal(err)
	}
	if n := lookups.Load(); n != 0 {
		t.Errorf("同一批里刚记下的解锁被闸放行了：lookup 执行了 %d 次", n)
	}
	if len(results) == 0 || !isFailedToolResult(results[0]) {
		t.Errorf("应回一条未解锁的失败结果，实际 %+v", results)
	}

	ctx = withRevealTurn(withRevealedFromChannel(context.Background(), []string{"lookup"}), &revealTurn{})
	if _, err := toolsFn(ctx, call, []context.Context{ctx}); err != nil {
		t.Fatal(err)
	}
	if n := lookups.Load(); n != 1 {
		t.Errorf("对照组：已提交的解锁集里有 lookup 时应执行 1 次，实际 %d", n)
	}
}
