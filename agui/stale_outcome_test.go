package agui_test

// `ErrCheckpointStale` 走到 agui 时必须被识别成并发冲突，**不能当正常中断**。
//
// 守卫用双 `%w` 保留了原始 `InterruptError`，所以 interrupt+stale 会满足
// `IsInterrupt`。若照常发 `RUN_INTERRUPTED` 并留 entry 等续发：那个中断的 cp
// 根本没落盘，前端答完发回来，框架从 cp 载入的是另一个执行者的状态 → 重新问一遍
// 已经答过的问题 → 来回不收敛（实测过 q1→答→q2→答→又问 q1 的死循环）。

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// staleAgent 直接返回守卫会产出的两种错误形态。
type staleAgent struct {
	withInterrupt bool // true = 双 %w（interrupt+stale）；false = 纯 stale
}

func (a *staleAgent) Name() string        { return "stale_agent" }
func (a *staleAgent) Description() string { return "returns ErrCheckpointStale" }

func (a *staleAgent) staleErr() error {
	// 复刻 graph_loop 的文案：内部 session / ns / epoch 都在里面，正是不该外泄的
	inner := fmt.Errorf("%w (session=sess-uuid-1234 ns=sub#42 op=save: 基线 aabbccdd，槽里已是 eeff0011)",
		compose_v2.ErrCheckpointStale)
	if !a.withInterrupt {
		return inner
	}
	ie := &compose_v2.InterruptError{
		Reason: "ask",
		Query:  &compose_v2.AskQuery{InterruptID: "q1", Question: "你最喜欢什么颜色？"},
	}
	return fmt.Errorf("%w (cp 未落盘: %w)", ie, inner)
}

func (a *staleAgent) Run(context.Context, *agent_v2.RunSession, *schema.Message, ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	return nil, a.staleErr()
}

func (a *staleAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, a.staleErr()
}

func TestAGUI_StaleCheckpoint_NotTreatedAsInterrupt(t *testing.T) {
	for _, tc := range []struct {
		name          string
		withInterrupt bool
	}{
		{"双 %w（interrupt+stale）——绝不能当正常中断留 entry 等续发", true},
		{"纯 stale", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := ownedHandler(func(hook.Hooks) agent_v2.Agent {
				return &staleAgent{withInterrupt: tc.withInterrupt}
			})
			rec := postAGUI(h, `{"threadId":"t-stale","runId":"r1","messages":[{"role":"user","content":"hi"}]}`)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %q", rec.Code, rec.Body.String())
			}
			events := parseSSE(rec.Body.String())

			var sawError, sawInterrupted bool
			var code, msg string
			for _, ev := range events {
				switch ev["type"] {
				case "RUN_ERROR":
					sawError = true
					code, _ = ev["code"].(string)
					msg, _ = ev["message"].(string)
				case "RUN_INTERRUPTED":
					sawInterrupted = true
				case "RUN_FINISHED":
					// 中断也可能表达成 RUN_FINISHED{outcome:{type:"interrupt"}}
					if oc, ok := ev["outcome"].(map[string]any); ok && oc["type"] == "interrupt" {
						sawInterrupted = true
					}
				}
			}
			if sawInterrupted {
				t.Error("不能发 RUN_INTERRUPTED——那个中断的 cp 没落盘，前端续发会拿到" +
					"另一个执行者的状态、重新被问已答过的问题，来回不收敛")
			}
			if !sawError {
				t.Fatalf("应发 RUN_ERROR, events=%v", events)
			}
			if code != "checkpoint_stale" {
				t.Errorf("code 应为 checkpoint_stale（前端要能把并发冲突与其它错误区分开）, got %q", code)
			}
			// 内部信息不能外泄给前端
			for _, leak := range []string{"sess-uuid-1234", "sub#42", "aabbccdd", "eeff0011", "基线"} {
				if strings.Contains(msg, leak) {
					t.Errorf("message 泄漏内部信息 %q: %s", leak, msg)
				}
			}
		})
	}
}
