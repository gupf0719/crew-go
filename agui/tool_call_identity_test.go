package agui_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model/openai"
	"crew-go/schema"
	"crew-go/tool"
)

func TestToolCallIdentity_StreamedIndicesAndApprovalResume(t *testing.T) {
	var requests, executions atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Messages []struct {
				Role      string `json:"role"`
				ToolCalls []struct {
					ID       string `json:"id"`
					Function struct {
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
				ToolCallID string `json:"tool_call_id"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Errorf("解码模型请求：%v", err)
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		if requests.Add(1) == 1 {
			_, _ = io.WriteString(w, `data: {"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"shared","type":"function","function":{"name":"write_sample","arguments":"{\"name\":\""}},{"index":1,"id":"shared","type":"function","function":{"name":"write_sample","arguments":"{\"name\":\""}}]}}]}`+"\n\n"+
				`data: {"choices":[{"delta":{"tool_calls":[{"index":1,"function":{"arguments":"B\"}"}}]}}]}`+"\n\n"+
				`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"A\"}"}}]},"finish_reason":"tool_calls"}]}`+"\n\n"+
				"data: [DONE]\n\n")
			return
		}
		var calls, results int
		for _, m := range input.Messages {
			for _, tc := range m.ToolCalls {
				calls++
				if tc.ID != "shared" || (tc.Function.Arguments != `{"name":"A"}` && tc.Function.Arguments != `{"name":"B"}`) {
					t.Errorf("送模声明应保留 provider ID 和原参数，实际 %+v", tc)
				}
			}
			if m.Role == "tool" {
				results++
				if m.ToolCallID != "shared" {
					t.Errorf("送模结果应以 provider ID 配对，实际 %q", m.ToolCallID)
				}
			}
		}
		if calls != 2 || results != 2 {
			t.Errorf("续跑后应送回两条调用及两条结果，实际调用 %d 条、结果 %d 条", calls, results)
		}
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(provider.Close)
	model := openai.NewClient("test-key", "test-model", openai.WithBaseURL(provider.URL), openai.WithHTTPClient(provider.Client()))
	handler := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("write_sample", "写入样本", map[string]any{"type": "object"},
			func(_ context.Context, args map[string]any) (*tool.Result, error) {
				executions.Add(1)
				return tool.Success("已写入 " + args["name"].(string)), nil
			}))
		return agent_v2.NewBasicAgent(agent_v2.WithName("write_samples"), agent_v2.WithModel(model),
			agent_v2.WithToolkit(tk), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), agent_v2.WithHooks(hooks))
	}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()))
	first := parseSSE(postAGUI(handler, `{"threadId":"samples","runId":"first","messages":[{"role":"user","content":"写两个样本"}]}`).Body.String())
	var starts []string
	args := map[string]string{}
	ends := map[string]int{}
	for _, ev := range first {
		id, _ := ev["toolCallId"].(string)
		switch ev["type"] {
		case "TOOL_CALL_START":
			starts = append(starts, id)
		case "TOOL_CALL_ARGS":
			args[id], _ = ev["delta"].(string)
		case "TOOL_CALL_END":
			ends[id]++
		}
	}
	if len(starts) != 2 || starts[0] == "" || starts[1] == "" || starts[0] == starts[1] ||
		args[starts[0]] != `{"name":"A"}` || args[starts[1]] != `{"name":"B"}` || ends[starts[0]] != 1 || ends[starts[1]] != 1 {
		t.Fatalf("两个流式位置应分别发出完整参数与结束事件，实际 start=%q args=%q end=%v", starts, args, ends)
	}
	interrupts := interruptsIn(first)
	if len(interrupts) != 2 || executions.Load() != 0 {
		t.Fatalf("工具应在各自审批前挂起，实际中断 %v、执行 %d 次", interrupts, executions.Load())
	}
	var answers [2]string
	for i, q := range interrupts {
		answers[i], _ = q["id"].(string)
		question, _ := q["message"].(string)
		if answers[i] == "" || q["reason"] != "tool_call" || q["toolCallId"] != starts[i] || !strings.Contains(question, args[starts[i]]) {
			t.Fatalf("审批应指向同一个调用及其参数，第 %d 条为 %v", i+1, q)
		}
	}
	resumed := resumeEntries(t, handler, "samples", "resume", resolved(answers[1], true), resolved(answers[0], false))
	results := map[string]string{}
	for _, ev := range resumed {
		switch ev["type"] {
		case "TOOL_CALL_START", "TOOL_CALL_ARGS":
			t.Errorf("续跑不应重新发送已展示的调用，实际 %v", ev)
		case "TOOL_CALL_RESULT":
			id, _ := ev["toolCallId"].(string)
			results[id], _ = ev["content"].(string)
		}
	}
	if outcomeType(resumed) != "success" || executions.Load() != 1 || requests.Load() != 2 ||
		len(results) != 2 || !strings.HasPrefix(results[starts[0]], "[tool_denied]") || results[starts[1]] != "已写入 B" {
		t.Fatalf("只应执行批准的调用并按原身份发结果，执行 %d 次、模型 %d 次，结果 %q，事件 %v",
			executions.Load(), requests.Load(), results, eventTypes(resumed))
	}
}

func TestToolCallIdentity_ParallelBranches(t *testing.T) {
	ready := make(chan struct{})
	var arrivals atomic.Int32
	handler := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("lookup", "查询", map[string]any{"type": "object"},
			func(ctx context.Context, args map[string]any) (*tool.Result, error) {
				if arrivals.Add(1) == 2 {
					close(ready)
				}
				ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				defer cancel()
				select {
				case <-ready:
					return tool.Success("结果 " + args["branch"].(string)), nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}))
		branch := func(name string) *agent_v2.BasicAgent {
			m := &scriptStream{script: func(n int, send func(*schema.Message)) error {
				if n == 0 {
					send(schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "shared", Function: schema.FunctionCall{
						Name: "lookup", Arguments: `{"branch":"` + name + `"}`,
					}}}))
				} else {
					send(schema.AssistantMessage("完成 " + name))
				}
				return nil
			}}
			return agent_v2.NewBasicAgent(agent_v2.WithName(name), agent_v2.WithModel(m), agent_v2.WithToolkit(tk), agent_v2.WithHooks(hooks))
		}
		return agent_v2.NewParallelAgent(agent_v2.WithParallelName("parallel")).AddBranch("a", branch("A")).AddBranch("b", branch("B"))
	})
	events := parseSSE(postAGUI(handler, `{"threadId":"parallel","runId":"first","messages":[{"role":"user","content":"查询"}]}`).Body.String())
	args, results := map[string]string{}, map[string]string{}
	for _, ev := range events {
		id, _ := ev["toolCallId"].(string)
		switch ev["type"] {
		case "TOOL_CALL_ARGS":
			args[id], _ = ev["delta"].(string)
		case "TOOL_CALL_RESULT":
			results[id], _ = ev["content"].(string)
		}
	}
	if arrivals.Load() != 2 || outcomeType(events) != "success" || len(args) != 2 || len(results) != 2 {
		t.Fatalf("并行分支应各执行一次并发出独立结果，参数 %q，结果 %q，事件 %v", args, results, eventTypes(events))
	}
	for id, raw := range args {
		var input struct{ Branch string }
		if err := json.Unmarshal([]byte(raw), &input); err != nil || id == "" || results[id] != "结果 "+input.Branch {
			t.Errorf("分支调用 %q 的参数 %s 与结果 %q 不配对，err=%v", id, raw, results[id], err)
		}
	}
}

type toolQuestionHook struct{ hook.NoOpHooks }

func (toolQuestionHook) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	return ctx, askAboutToolCall(ctx, in.CallId)
}

func askAboutToolCall(ctx context.Context, providerID string) error {
	_, err := compose_v2.Ask(compose_v2.EnterPhase(ctx, "confirm-lookup"), &compose_v2.AskQuery{
		Question: "继续查询？", Reason: "tool_call", ToolCallID: providerID,
	})
	return err
}

type missedToolStartWriter struct{ *httptest.ResponseRecorder }

func (w missedToolStartWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte(`"type":"TOOL_CALL_START"`)) {
		return 0, errors.New("could not write tool start")
	}
	return w.ResponseRecorder.Write(p)
}

func TestToolCallIdentity_CustomQuestionMatchesToolInstance(t *testing.T) {
	for _, tc := range []struct {
		name      string
		inHook    bool
		missStart bool
	}{{name: "tool"}, {name: "hook", inHook: true}, {name: "missed_start", missStart: true}} {
		t.Run(tc.name, func(t *testing.T) {
			var executions atomic.Int32
			handler := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
				tk := tool.NewToolkit()
				tk.Register(tool.NewFuncTool("lookup", "查询", map[string]any{"type": "object"},
					func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
						if !tc.inHook {
							if err := askAboutToolCall(ctx, compose_v2.GetCurrentToolCallID(ctx)); err != nil {
								return nil, err
							}
						}
						executions.Add(1)
						return tool.Success("查询结果"), nil
					}))
				if tc.inHook {
					hooks = hook.NewChainHooks(hooks, toolQuestionHook{})
				}
				return agent_v2.NewBasicAgent(agent_v2.WithName("lookup_agent"), agent_v2.WithToolkit(tk),
					agent_v2.WithHooks(hooks), agent_v2.WithModel(&callToolsOnce{names: []string{"lookup"}}))
			}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()))
			rec := httptest.NewRecorder()
			var writer http.ResponseWriter = rec
			if tc.missStart {
				writer = missedToolStartWriter{rec}
			}
			handler.ServeHTTP(writer, httptest.NewRequest(http.MethodPost, "/agui", strings.NewReader(
				`{"threadId":"lookup","runId":"first","messages":[{"role":"user","content":"查询"}]}`)))
			first := parseSSE(rec.Body.String())
			var shownID string
			for _, ev := range first {
				if ev["type"] == "TOOL_CALL_START" {
					shownID, _ = ev["toolCallId"].(string)
				}
			}
			questions := interruptsIn(first)
			if len(questions) != 1 || executions.Load() != 0 {
				t.Fatalf("自定义提问应在执行前挂起，实际 %v", questions)
			}
			callID, _ := questions[0]["toolCallId"].(string)
			if callID == "" || callID == "c1" || (!tc.missStart && shownID != callID) || (tc.missStart && shownID != "") {
				t.Fatalf("自定义提问应关联框架实例，实际 call=%q start=%q", callID, shownID)
			}
			qid, _ := questions[0]["id"].(string)
			events := resumeEntries(t, handler, "lookup", "resume", resolved(qid, "确认"))
			wantStarts := 0
			if tc.missStart {
				wantStarts = 1
			}
			starts, args := 0, 0
			matched := false
			for _, ev := range events {
				switch ev["type"] {
				case "TOOL_CALL_START":
					starts++
					if ev["toolCallId"] != callID {
						t.Errorf("补发的调用应沿用 %q，实际 %v", callID, ev)
					}
				case "TOOL_CALL_ARGS":
					args++
				case "TOOL_CALL_RESULT":
					matched = ev["toolCallId"] == callID && ev["content"] == "查询结果"
				}
			}
			if outcomeType(events) != "success" || !matched || executions.Load() != 1 || starts != wantStarts || args != wantStarts {
				t.Fatalf("续跑应补发 %d 组调用事件并执行一次，实际 %v", wantStarts, events)
			}
		})
	}
}
