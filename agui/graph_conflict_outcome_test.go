package agui_test

// `ErrGraphIDConflict` 走到 agui 时要有专属 code、文案脱敏，且不能当正常中断留着等续发。
//
// 它重跑必然撞同一个槽（框架在 IsRetryable 里排掉了它），落进通用兜底的 `agent_error`
// （「可能是瞬时的，原样重来是合理选项」）会让前端去重试；它的文案里有 sid / ns / 两张图的
// GraphID，不该推给浏览器。引擎在中断出口写档失败时返回「中断 + 写档原因」双 %w，那份中断的
// 存档没写成——跟 stale 同理，照常发 RUN_INTERRUPTED 前端答完也续不上。

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

type graphConflictAgent struct{ withInterrupt bool }

func (a *graphConflictAgent) Name() string        { return "conflict_agent" }
func (a *graphConflictAgent) Description() string { return "returns ErrGraphIDConflict" }

func (a *graphConflictAgent) err() error {
	// 复刻 SaveNS 的文案：sid / ns / 两张图的 GraphID 都在里面
	conflict := fmt.Errorf("%w: sid=%q ns=%q existing=%q new=%q",
		compose_v2.ErrGraphIDConflict, "sess-secret-77", "sub#9", "graph-A", "graph-B")
	if !a.withInterrupt {
		return conflict
	}
	ie := &compose_v2.InterruptError{Reason: "ask",
		Query: &compose_v2.AskQuery{InterruptID: "q1", Question: "继续吗？"}}
	return fmt.Errorf("%w (root cp save failed: %w)", ie, conflict) // 引擎出口的形状
}

func (a *graphConflictAgent) Run(context.Context, *agent_v2.RunSession, *schema.Message, ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	return nil, a.err()
}

func (a *graphConflictAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, a.err()
}

func TestAGUI_GraphIDConflict_DedicatedCodeAndSanitized(t *testing.T) {
	for _, tc := range []struct {
		name          string
		withInterrupt bool
	}{
		{"纯身份冲突", false},
		{"中断 + 写档撞身份冲突——不能当正常中断", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := ownedHandler(func(hook.Hooks) agent_v2.Agent { return &graphConflictAgent{withInterrupt: tc.withInterrupt} })
			rec := postAGUI(h, `{"threadId":"t-conflict","runId":"r1","messages":[{"role":"user","content":"hi"}]}`)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %q", rec.Code, rec.Body.String())
			}
			var code, msg string
			var sawError, sawInterrupted bool
			for _, ev := range parseSSE(rec.Body.String()) {
				switch ev["type"] {
				case "RUN_ERROR":
					sawError = true
					code, _ = ev["code"].(string)
					msg, _ = ev["message"].(string)
				case "RUN_INTERRUPTED":
					sawInterrupted = true
				case "RUN_FINISHED":
					if oc, ok := ev["outcome"].(map[string]any); ok && oc["type"] == "interrupt" {
						sawInterrupted = true
					}
				}
			}
			if sawInterrupted {
				t.Error("不能当正常中断——那份中断的存档没写成，前端答完也续不上")
			}
			if !sawError || code != "graph_id_conflict" {
				t.Errorf("应发 RUN_ERROR、code=graph_id_conflict（别让前端当成可重试的 agent_error），实际 error=%v code=%q", sawError, code)
			}
			for _, leak := range []string{"sess-secret-77", "sub#9", "graph-A", "graph-B"} {
				if strings.Contains(msg, leak) {
					t.Errorf("message 泄漏内部信息 %q: %s", leak, msg)
				}
			}
		})
	}
}
