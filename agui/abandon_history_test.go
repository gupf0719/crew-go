package agui_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// transferModel：用户说「转账」就发起 transfer 调用，别的一律直接答复。
type transferModel struct{}

func (transferModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if last := msgs[len(msgs)-1]; last.Role == schema.RoleUser && strings.Contains(last.Content, "转账") {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c1", Type: "function",
			Function: schema.FunctionCall{Name: "transfer", Arguments: `{"amount":5000}`}}}), nil
	}
	return schema.AssistantMessage("好的"), nil
}
func (transferModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m transferModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// 同一个 thread 来了新 run、旧会话还挂在审批上（用户没答、直接说了别的）：旧挂起被作废，这次 Run 在入口给它悬空的 transfer
// 调用补一条结果（agent_v2.WithAbandonedTasks），转账不执行。曾经只删存档——挂起被作废了，会话历史里永久留着一条再也不会
// 有结果的调用（调研：业内凡写进持久历史的合成结果都在作废的这一刻写）。
//
// 工厂返回顶层的自研壳（入口 EnterRun、再把 opts 透传给框架 agent）时一样：壳盖的是这次 Run 自己的任务章，不算嵌套（审计实测：
// 曾按「ctx 上有没有任务章」判，这种壳每次顶掉挂起都 RUN_ERROR、悬空的调用永远补不上）。
func TestHandler_OverwrittenPendingClosesDanglingCall(t *testing.T) {
	for _, viaShell := range []bool{false, true} {
		t.Run(map[bool]string{false: "BasicAgent", true: "EnterRun 自研壳"}[viaShell], func(t *testing.T) {
			hs := agent_v2.NewInMemoryHistoryStore()
			var executed atomic.Int32
			tk := tool.NewToolkit()
			tk.Register(tool.NewFuncTool("transfer", "转账", map[string]any{"type": "object"},
				func(context.Context, map[string]any) (*tool.Result, error) {
					executed.Add(1)
					return tool.Success("已转账"), nil
				}))
			h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
				var ag agent_v2.Agent = agent_v2.NewBasicAgent(agent_v2.WithName("x"), agent_v2.WithModel(transferModel{}),
					agent_v2.WithToolkit(tk), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
					agent_v2.WithHistory(hs, "tenant"), agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(5))
				if viaShell {
					ag = &forwardingShell{inner: ag}
				}
				return ag
			}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()))
			// 历史按调用方的 owner 分（agui 把解析出的 owner 推给下游）
			key := agent_v2.HistoryKey{Owner: "test-owner", ConvID: "t1"}

			if iid := findInterruptID(parseSSE(postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"role":"user","content":"转账 5000"}]}`).Body.String())); iid == "" {
				t.Fatal("fixture 没搭对：转账应停在审批上")
			}
			before, err := agent_v2.LoadAllHistory(context.Background(), hs, key)
			if err != nil {
				t.Fatal(err)
			}
			if !hasToolCall(before, "c1") || hasToolResult(before, "c1") {
				t.Fatalf("fixture 没搭对：挂起时历史里应有 transfer 调用、没有结果，实际 %d 条", len(before))
			}

			body := postAGUI(h, `{"threadId":"t1","runId":"r2","messages":[{"role":"user","content":"先别转了，查下余额"}]}`).Body.String()
			if strings.Contains(body, "RUN_ERROR") {
				t.Fatalf("顶掉挂起的新 run 不该报错：%s", body)
			}

			after, err := agent_v2.LoadAllHistory(context.Background(), hs, key)
			if err != nil {
				t.Fatal(err)
			}
			if !hasToolResult(after, "c1") {
				t.Fatalf("挂起被新 run 覆盖之后，历史里那条 transfer 调用仍然没有结果（%d 条）", len(after))
			}
			if n := executed.Load(); n != 0 {
				t.Fatalf("被放弃的转账不该执行，实际执行 %d 次", n)
			}
		})
	}
}

// forwardingShell 是按 agent_v2.EnterRun 文档写的自研 agent：入口 EnterRun，再把调用连同 opts 透传给框架 agent。
type forwardingShell struct{ inner agent_v2.Agent }

func (s *forwardingShell) Name() string        { return "shell" }
func (s *forwardingShell) Description() string { return "透传壳" }
func (s *forwardingShell) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	ctx, sess, err := agent_v2.EnterRun(ctx, sess, s.Name())
	if err != nil {
		return nil, err
	}
	return s.inner.Run(ctx, sess, in, agent_v2.RegrantRouteTo(opts, s, s.inner)...)
}
func (s *forwardingShell) Resume(ctx context.Context, sid string, opts ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return s.inner.Resume(ctx, sid, opts...)
}

func hasToolResult(msgs []*schema.Message, callID string) bool {
	for _, m := range msgs {
		if m.Role == schema.RoleTool && schema.GetToolResultID(m) == callID {
			return true
		}
	}
	return false
}

func hasToolCall(msgs []*schema.Message, callID string) bool {
	for _, m := range msgs {
		for _, tc := range schema.GetToolCalls(m) {
			if tc.ID == callID {
				return true
			}
		}
	}
	return false
}
