package agui_test

import (
	"context"
	"encoding/json"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

type legacyDeliveryRollback struct {
	agent_v2.NoOpOrchestrationHooks
	once atomic.Bool
}

func (h *legacyDeliveryRollback) OnNodeEnd(ctx context.Context, id string, err error) context.Context {
	if id == "tools" && err == nil && h.once.CompareAndSwap(false, true) {
		runtime.Goexit()
	}
	return ctx
}

type legacyDeliveryBranch struct {
	agent_v2.Agent
	rollback *legacyDeliveryRollback
}

func (b legacyDeliveryBranch) Run(ctx context.Context, sess *agent_v2.RunSession, input *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	hooks := agent_v2.NewChainOrchHooks(agent_v2.GetCtxOrchHooks(ctx), b.rollback)
	return b.Agent.Run(ctx, sess, input, append(opts, agent_v2.WithOrchHooks(hooks))...)
}

type legacyDeliveryOtherBranch struct {
	hook.Hooks
	sid *atomic.Value
}

func (h legacyDeliveryOtherBranch) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	h.sid.Store(compose_v2.GetSessionID(ctx))
	_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "continue other branch?"})
	return ctx, err
}

func TestToolResult_ResumeFinalizedDisplay(t *testing.T) {
	const raw = "legacy public result"
	const detail = " private model context"
	for _, tc := range []struct {
		name              string
		want              string
		legacy, projected bool
	}{
		{name: "legacy_ambiguous", legacy: true, want: ""},
		{name: "legacy_projection", legacy: true, projected: true, want: "[redacted]"},
		{name: "recorded_final_result", want: raw + detail},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sid atomic.Value
			var bodyCalls atomic.Int32
			rollback := &legacyDeliveryRollback{}
			cp := compose_v2.NewInMemoryCheckpointer()
			h := ownedHandler(func(ui hook.Hooks) agent_v2.Agent {
				tk := tool.NewToolkit()
				tk.Register(tool.NewFuncTool("lookup", "lookup", nil, func(context.Context, map[string]any) (*tool.Result, error) {
					bodyCalls.Add(1)
					return tool.Success(raw), nil
				}))
				display := ui
				if tc.projected {
					display = displayToolResult{Hooks: ui}
				}
				main := agent_v2.NewBasicAgent(agent_v2.WithName("lookup"), agent_v2.WithToolkit(tk),
					agent_v2.WithModel(displayedToolModel{wantResult: raw + detail}),
					agent_v2.WithHooks(hook.NewChainHooks(display, addModelToolDetail{detail: detail})))
				other := agent_v2.NewBasicAgent(agent_v2.WithName("other"), agent_v2.WithModel(genOnly{}),
					agent_v2.WithHooks(legacyDeliveryOtherBranch{Hooks: ui, sid: &sid}))
				return agent_v2.NewParallelAgent(agent_v2.WithParallelName("tool-result-rollback")).
					AddBranch("lookup", legacyDeliveryBranch{Agent: main, rollback: rollback}).AddBranch("other", other)
			}, agui.WithCheckpointer(cp))
			first := parseSSE(postAGUI(h, `{"threadId":"legacy-final","runId":"r1","messages":[{"role":"user","content":"run"}]}`).Body.String())
			id := findInterruptID(first)
			var callID string
			for _, event := range first {
				if event["type"] == "TOOL_CALL_START" {
					callID, _ = event["toolCallId"].(string)
				}
			}
			if id == "" || callID == "" || !rollback.once.Load() || bodyCalls.Load() != 1 ||
				contains(eventTypes(first), "TOOL_CALL_RESULT") {
				t.Fatalf("应保存提交前作废的工具定稿，并展示兄弟节点的问题，事件 %v", eventTypes(first))
			}
			sessionID := sid.Load().(string)
			saved, err := cp.LoadNS(t.Context(), sessionID, "lookup")
			if err != nil || saved == nil {
				t.Fatalf("读取工具节点存档失败，%v", err)
			}
			wire, err := json.Marshal(saved)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(wire), `"final":true`) || !strings.Contains(string(wire), raw+detail) {
				t.Fatalf("存档应包含实际定稿和模型内容，%s", wire)
			}
			if tc.legacy {
				// 旧版明确投影已记录正文，但默认提交结果和等值展示副本共用无标记记录。
				legacy := strings.ReplaceAll(string(wire), `"delivery":{"use_final_result":true}`, `"delivery":{}`)
				want := `"delivery":{}`
				if tc.projected {
					want = `"delivery":{"projected":true`
				}
				if !strings.Contains(legacy, want) {
					t.Fatalf("存档缺少预期的旧展示记录，%s", legacy)
				}
				var old compose_v2.Checkpoint
				if err := json.Unmarshal([]byte(legacy), &old); err != nil {
					t.Fatal(err)
				}
				if err := cp.SaveNS(t.Context(), sessionID, "lookup", &old); err != nil {
					t.Fatal(err)
				}
			}
			last := resumeEntries(t, h, "legacy-final", "r2", resolved(id, "continue"))
			var resultIDs, results []string
			for _, event := range last {
				if event["type"] == "TOOL_CALL_RESULT" {
					resultIDs = append(resultIDs, event["toolCallId"].(string))
					results = append(results, event["content"].(string))
				}
			}
			if outcomeType(last) != "success" || bodyCalls.Load() != 1 || contains(eventTypes(last), "TOOL_CALL_START") ||
				len(resultIDs) != 1 || resultIDs[0] != callID || results[0] != tc.want {
				t.Fatalf("续跑应按原 ID 发一次可展示的结果，实际 %q/%v，期望 %q，实际执行 %d 次，事件 %v",
					resultIDs, results, tc.want, bodyCalls.Load(), eventTypes(last))
			}
		})
	}
}
