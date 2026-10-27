package agui_test

// 流中途失败的那次模型调用，它已交出的片段要从前端的对话里撤回。AG-UI 的标准办法是 MESSAGES_SNAPSHOT：消费方丢掉快照里
// 没有的消息。曾经只发自定义事件 crew.reset_stream，标准前端忽略它，半截那条一直留在对话里（实测 ["partial-","full"]
// 成了两条消息）。这里用一个按 AG-UI 1.0 规则应用事件的最小消费方，断言前端最后看到的对话。

import (
	"context"
	"encoding/json"
	"errors"
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

// clientMsg 是最小消费方手上的一条消息。
type clientMsg struct {
	id, role, content string
	toolCalls         []string
}

// applyAGUI 按 AG-UI 1.0 的规则把事件应用到消息列表（只覆盖本测试用到的事件）：TEXT_MESSAGE_* 按 messageId 建 / 追加；
// TOOL_CALL_START 没带 parentMessageId 时建 id = toolCallId 的 assistant 消息（官方客户端 resolveOrCreateAssistantMessage）；
// TOOL_CALL_RESULT 建 tool 消息；MESSAGES_SNAPSHOT 同 id 就地替换、没见过的追加、快照里没有的丢掉。
func applyAGUI(start []clientMsg, events []map[string]any) []clientMsg {
	msgs := append([]clientMsg(nil), start...)
	find := func(id string) int {
		for i, m := range msgs {
			if m.id == id {
				return i
			}
		}
		return -1
	}
	for _, ev := range events {
		switch ev["type"] {
		case "TEXT_MESSAGE_START":
			id, _ := ev["messageId"].(string)
			if find(id) < 0 {
				msgs = append(msgs, clientMsg{id: id, role: "assistant"})
			}
		case "TEXT_MESSAGE_CONTENT":
			id, _ := ev["messageId"].(string)
			d, _ := ev["delta"].(string)
			if i := find(id); i >= 0 {
				msgs[i].content += d
			}
		case "TOOL_CALL_START":
			id, _ := ev["toolCallId"].(string)
			if find(id) < 0 {
				msgs = append(msgs, clientMsg{id: id, role: "assistant", toolCalls: []string{id}})
			}
		case "TOOL_CALL_RESULT":
			id, _ := ev["messageId"].(string)
			c, _ := ev["content"].(string)
			msgs = append(msgs, clientMsg{id: id, role: "tool", content: c})
		case "MESSAGES_SNAPSHOT":
			snap, _ := ev["messages"].([]any)
			seen := map[string]bool{}
			for _, raw := range snap {
				sm, _ := raw.(map[string]any)
				id, _ := sm["id"].(string)
				role, _ := sm["role"].(string)
				content, _ := sm["content"].(string)
				seen[id] = true
				m := clientMsg{id: id, role: role, content: content}
				if tcs, ok := sm["toolCalls"].([]any); ok {
					for _, tc := range tcs {
						tcm, _ := tc.(map[string]any)
						tid, _ := tcm["id"].(string)
						m.toolCalls = append(m.toolCalls, tid)
					}
				}
				if i := find(id); i >= 0 {
					msgs[i] = m
				} else {
					msgs = append(msgs, m)
				}
			}
			kept := msgs[:0]
			for _, m := range msgs {
				if seen[m.id] {
					kept = append(kept, m)
				}
			}
			msgs = kept
		}
	}
	return msgs
}

// scriptStream 第 n 次 Stream（从 0 数）交给 script。
type scriptStream struct {
	calls  atomic.Int64
	script func(n int, send func(*schema.Message)) error
}

func (m *scriptStream) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, errors.New("unused")
}

func (m *scriptStream) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	n := int(m.calls.Add(1) - 1)
	sr, send := schema.NewStreamReader[*schema.Message](8)
	if err := m.script(n, send); err != nil {
		schema.SendError(sr, err)
	}
	schema.CloseSend(sr)
	return sr, nil
}

func (m *scriptStream) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func text(s string) *schema.Message { return &schema.Message{Role: schema.RoleAssistant, Content: s} }

var errReset = errors.New("connection reset")

// 文本流出半截后断开，成员重试成功：前端最后看到的对话只有用户那句和完整的答复。
func TestResetSnapshot_HalfMessageRetracted(t *testing.T) {
	m := &scriptStream{script: func(n int, send func(*schema.Message)) error {
		if n == 0 {
			send(text("partial-"))
			return errReset
		}
		send(text("full"))
		return nil
	}}
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		step := agent_v2.NewBasicAgent(agent_v2.WithName("s"), agent_v2.WithModel(m), agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(2))
		return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
			agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(2))).AddStep("s1", step)
	})
	start := []clientMsg{{id: "u1", role: "user", content: "go"}}
	rec := postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"id":"u1","role":"user","content":"go"}]}`)
	got := applyAGUI(start, parseSSE(rec.Body.String()))
	if len(got) != 2 || got[0].id != "u1" || got[0].content != "go" || got[1].content != "full" {
		t.Fatalf("前端最后应只有用户那句和完整答复，实际 %+v", got)
	}
}

// 作废发生在工具调用之后：快照要把这个 run 已经交出的工具调用与结果原样带回，只丢半截那条。
func TestResetSnapshot_KeepsEarlierToolCall(t *testing.T) {
	m := &scriptStream{script: func(n int, send func(*schema.Message)) error {
		switch n {
		case 0:
			send(&schema.Message{Role: schema.RoleAssistant, ContentBlocks: []*schema.ContentBlock{{Type: "tool_call",
				Toolcall: &schema.ToolcallContentBlock{Id: "call_1", Name: "lookup", Arguments: `{"q":"x"}`}}}})
			return nil
		case 1:
			send(text("partial-"))
			return errReset
		default:
			send(text("full"))
			return nil
		}
	}}
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("lookup", "查", map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("找到了"), nil }))
		return agent_v2.NewBasicAgent(agent_v2.WithName("b"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
			agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(4), agent_v2.WithModelRetry(compose_v2.SimpleRetry(2)))
	})
	start := []clientMsg{{id: "u1", role: "user", content: "go"}}
	rec := postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"id":"u1","role":"user","content":"go"}]}`)
	got := applyAGUI(start, parseSSE(rec.Body.String()))
	if len(got) != 4 {
		t.Fatalf("应是 用户 / 工具调用 / 工具结果 / 完整答复 四条，实际 %+v", got)
	}
	if got[1].id != "call_1" || len(got[1].toolCalls) != 1 || got[2].role != "tool" || got[2].content != "找到了" || got[3].content != "full" {
		t.Fatalf("快照应保住工具调用与结果、只丢半截那条，实际 %+v", got)
	}
}

// 成员的一次尝试被从头再跑取代：那次尝试里已经完整流完的模型调用（文本 "A" + 工具调用 call_1 + 结果）一起撤回，前端最后只有
// 重来那次的一份。曾经只有流中途失败的那次调用会撤回，前端多出一份第一次尝试的答复与工具调用（审计实测 8 条，应为 5 条）。
func TestResetSnapshot_DiscardedAttemptRetracted(t *testing.T) {
	callMsg := func() *schema.Message {
		return &schema.Message{Role: schema.RoleAssistant, Content: "A", ContentBlocks: []*schema.ContentBlock{{Type: "tool_call",
			Toolcall: &schema.ToolcallContentBlock{Id: "call_1", Name: "lookup", Arguments: `{}`}}}}
	}
	m := &scriptStream{script: func(n int, send func(*schema.Message)) error {
		switch n {
		case 0, 2:
			send(callMsg())
		case 1:
			// 什么都没流出就失败：这次调用自己没有可撤的，作废的是整次尝试。报 400——模型调用层不重试它，交给成员重试从头再来
			return &model.APIError{Provider: "stub", StatusCode: 400, Body: "bad"}
		case 3:
			// 重来之后再有一次流中途失败（模型调用层重来）：第二次快照里要带着重来那次的 call_1——它跟作废那次同一个 ID
			send(text("half-"))
			return errReset
		default:
			send(text("full"))
		}
		return nil
	}}
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("lookup", "查", map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("找到了"), nil }))
		step := agent_v2.NewBasicAgent(agent_v2.WithName("s"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
			agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(4), agent_v2.WithModelRetry(compose_v2.SimpleRetry(2)))
		return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
			agent_v2.WithSequentialRetryConfig(compose_v2.RetryConfig{MaxRetries: 2, ShouldRetry: func(error) bool { return true }})).
			AddStep("s1", step)
	})
	rec := postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"id":"u1","role":"user","content":"go"}]}`)
	got := applyAGUI([]clientMsg{{id: "u1", role: "user", content: "go"}}, parseSSE(rec.Body.String()))
	var texts []string
	tools := 0
	for _, cm := range got {
		switch {
		case cm.role == "tool":
			tools++
		case cm.content != "" && cm.role == "assistant":
			texts = append(texts, cm.content)
		}
	}
	if len(got) != 5 || tools != 1 || len(texts) != 2 || texts[0] != "A" || texts[1] != "full" {
		t.Fatalf("前端最后应只有 用户 / A / call_1 / 结果 / full 各一条，实际 %+v", got)
	}
}

// redactHook 在 OnModelEnd 里换上一条新消息（复制一份、改正文，标注照留）——业务做脱敏的典型写法。
type redactHook struct{ hook.NoOpHooks }

func (redactHook) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	if out.Message != nil && len(schema.GetToolCalls(out.Message)) == 0 {
		cp := *out.Message
		cp.Content = "REDACTED"
		out.Message = &cp
	}
	return ctx, nil
}

// genOnly 只支持 Generate（非流式）：答复由 agui 在 OnModelEnd 里整条补发。
type genOnly struct{}

func (genOnly) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return text("secret"), nil
}
func (genOnly) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (genOnly) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return genOnly{}, nil }

// OnModelEnd 钩子换掉了答复：前端最后看到的是新正文，不是整条消失（曾经 agui 把「替换」当「作废」，答复从标准前端整条
// 消失，审计实测三种组合全是只剩用户那句）。
func TestResetSnapshot_HookReplacedAnswerShown(t *testing.T) {
	for _, tc := range []struct {
		name        string
		streaming   bool
		bizBeforeUI bool
	}{
		{"流式/适配层在前", true, false},
		{"流式/业务钩子在前", true, true},
		{"非流式/业务钩子在前", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var m model.ChatModel = genOnly{}
			if tc.streaming {
				m = &scriptStream{script: func(_ int, send func(*schema.Message)) error { send(text("secret")); return nil }}
			}
			h := ownedHandler(func(ui hook.Hooks) agent_v2.Agent {
				chain := hook.NewChainHooks(ui, redactHook{})
				if tc.bizBeforeUI {
					chain = hook.NewChainHooks(redactHook{}, ui)
				}
				return agent_v2.NewBasicAgent(agent_v2.WithName("b"), agent_v2.WithModel(m), agent_v2.WithHooks(chain), agent_v2.WithMaxRounds(2))
			})
			rec := postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"id":"u1","role":"user","content":"go"}]}`)
			got := applyAGUI([]clientMsg{{id: "u1", role: "user", content: "go"}}, parseSSE(rec.Body.String()))
			if len(got) != 2 || got[1].content != "REDACTED" {
				t.Fatalf("前端最后应是用户那句加改写后的答复，实际 %+v", got)
			}
		})
	}
}

// freshRedactHook 在 OnModelEnd 里换上一条全新的消息（不带框架的标注）——业务最自然的写法。
type freshRedactHook struct{ hook.NoOpHooks }

func (freshRedactHook) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	if out.Message != nil && len(schema.GetToolCalls(out.Message)) == 0 {
		out.Message = &schema.Message{Role: schema.RoleAssistant, Content: "REDACTED"}
	}
	return ctx, nil
}

// 钩子换上的新消息不带 run_id 标注时，改写照样对得上原来那条：前端最后只有改写后的正文（曾经原文留着、另多一条，审计实测）。
func TestResetSnapshot_FreshMessageReplacement(t *testing.T) {
	for _, bizFirst := range []bool{false, true} {
		m := &scriptStream{script: func(_ int, send func(*schema.Message)) error { send(text("secret")); return nil }}
		h := ownedHandler(func(ui hook.Hooks) agent_v2.Agent {
			chain := hook.NewChainHooks(ui, freshRedactHook{})
			if bizFirst {
				chain = hook.NewChainHooks(freshRedactHook{}, ui)
			}
			return agent_v2.NewBasicAgent(agent_v2.WithName("b"), agent_v2.WithModel(m), agent_v2.WithHooks(chain), agent_v2.WithMaxRounds(2))
		})
		rec := postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"id":"u1","role":"user","content":"go"}]}`)
		got := applyAGUI([]clientMsg{{id: "u1", role: "user", content: "go"}}, parseSSE(rec.Body.String()))
		if len(got) != 2 || got[1].content != "REDACTED" {
			t.Fatalf("bizFirst=%v：前端最后应是用户那句加改写后的答复，实际 %+v", bizFirst, got)
		}
	}
}

// askOnEnd 在 OnModelEnd 里问一次人（第一遍中断，续跑拿到答案放行）。
type askOnEnd struct{ hook.NoOpHooks }

func (askOnEnd) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
	_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "发出去吗"})
	return ctx, err
}

// 第一遍改写的信号因为排在后面的钩子问人而没发出，前端留着原文；续跑时同一条答复再改写一次，那条消息已经在请求带来的对话
// 里——改那条原文、发快照，不是重新流式发送（那在客户端是追加，曾经得到 "secretREDACTED"，审计实测）。
func TestResetSnapshot_ResumeRewriteOfShownMessage(t *testing.T) {
	m := &scriptStream{script: func(_ int, send func(*schema.Message)) error { send(text("secret")); return nil }}
	h := ownedHandler(func(ui hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("b"), agent_v2.WithModel(m),
			agent_v2.WithHooks(hook.NewChainHooks(ui, redactHook{}, askOnEnd{})), agent_v2.WithMaxRounds(2))
	}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()))
	start := []clientMsg{{id: "u1", role: "user", content: "go"}}
	ev1 := parseSSE(postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"id":"u1","role":"user","content":"go"}]}`).Body.String())
	iid := findInterruptID(ev1)
	if iid == "" {
		t.Fatal("第一遍应停在问人上")
	}
	after1 := applyAGUI(start, ev1)
	var msgs []map[string]any
	for _, cm := range after1 {
		msgs = append(msgs, map[string]any{"id": cm.id, "role": cm.role, "content": cm.content})
	}
	body, _ := json.Marshal(map[string]any{"threadId": "t1", "runId": "r2", "messages": msgs,
		"resume": []map[string]any{{"interruptId": iid, "status": "resolved", "payload": "是"}}})
	got := applyAGUI(after1, parseSSE(postAGUI(h, string(body)).Body.String()))
	for _, cm := range got {
		if strings.Contains(cm.content, "secret") {
			t.Fatalf("续跑改写之后前端不该还留着原文，实际 %+v", got)
		}
	}
	if len(got) != 2 || got[1].content != "REDACTED" {
		t.Fatalf("前端最后应是用户那句加改写后的答复，实际 %+v", got)
	}
}

// 作废那次尝试的工具调用是 call_x，重来那次换成 call_y：call_x 与它的结果都要从前端消失（工具调用本身的撤回，不靠同 ID 去重）。
func TestResetSnapshot_DiscardedToolCallGone(t *testing.T) {
	call := func(id string) *schema.Message {
		return &schema.Message{Role: schema.RoleAssistant, ContentBlocks: []*schema.ContentBlock{{Type: "tool_call",
			Toolcall: &schema.ToolcallContentBlock{Id: id, Name: "lookup", Arguments: `{}`}}}}
	}
	m := &scriptStream{script: func(n int, send func(*schema.Message)) error {
		switch n {
		case 0:
			send(call("call_x"))
		case 1:
			return &model.APIError{Provider: "stub", StatusCode: 400, Body: "bad"}
		case 2:
			send(call("call_y"))
		default:
			send(text("full"))
		}
		return nil
	}}
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("lookup", "查", map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("r"), nil }))
		step := agent_v2.NewBasicAgent(agent_v2.WithName("s"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
			agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(4))
		return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
			agent_v2.WithSequentialRetryConfig(compose_v2.RetryConfig{MaxRetries: 2, ShouldRetry: func(error) bool { return true }})).
			AddStep("s1", step)
	})
	rec := postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"id":"u1","role":"user","content":"go"}]}`)
	got := applyAGUI([]clientMsg{{id: "u1", role: "user", content: "go"}}, parseSSE(rec.Body.String()))
	tools := 0
	for _, cm := range got {
		if cm.id == "call_x" {
			t.Fatalf("作废那次的工具调用 call_x 不该留在前端，实际 %+v", got)
		}
		if cm.role == "tool" {
			tools++
		}
	}
	if tools != 1 || got[len(got)-1].content != "full" {
		t.Fatalf("应只剩 call_y 的一个结果、最后是完整答复，实际 %+v", got)
	}
}

// 顺序编排两步都用 call_0：s1 成功提交，s2 的第一次尝试作废、从头再跑。撤回只认产出它的那次模型调用，s1 已经提交的工具调用
// 与结果保住。曾经按 tool_call ID 认，s1 的工具调用与结果被一起撤掉（审计实测）。
func TestResetSnapshot_DiscardOnlyOwnToolCalls(t *testing.T) {
	call := func(q string) *schema.Message {
		return &schema.Message{Role: schema.RoleAssistant, ContentBlocks: []*schema.ContentBlock{{Type: "tool_call",
			Toolcall: &schema.ToolcallContentBlock{Id: "call_0", Name: "lookup", Arguments: `{"q":"` + q + `"}`}}}}
	}
	s1m := &scriptStream{script: func(n int, send func(*schema.Message)) error {
		if n == 0 {
			send(call("a1"))
		} else {
			send(text("done1"))
		}
		return nil
	}}
	s2m := &scriptStream{script: func(n int, send func(*schema.Message)) error {
		switch n {
		case 0, 2:
			send(call("a2"))
		case 1:
			return &model.APIError{Provider: "stub", StatusCode: 400, Body: "bad"}
		default:
			send(text("done2"))
		}
		return nil
	}}
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("lookup", "查", map[string]any{"type": "object"},
			func(_ context.Context, a map[string]any) (*tool.Result, error) {
				q, _ := a["q"].(string)
				return tool.Success("R-" + q), nil
			}))
		mk := func(name string, m model.ChatModel) agent_v2.Agent {
			return agent_v2.NewBasicAgent(agent_v2.WithName(name), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
				agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(4))
		}
		return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
			agent_v2.WithSequentialRetryConfig(compose_v2.RetryConfig{MaxRetries: 2, ShouldRetry: func(error) bool { return true }})).
			AddStep("s1", mk("s1", s1m)).AddStep("s2", mk("s2", s2m))
	})
	rec := postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"id":"u1","role":"user","content":"go"}]}`)
	got := applyAGUI([]clientMsg{{id: "u1", role: "user", content: "go"}}, parseSSE(rec.Body.String()))
	var results, texts []string
	for _, cm := range got {
		switch {
		case cm.role == "tool":
			results = append(results, cm.content)
		case cm.role == "assistant" && cm.content != "":
			texts = append(texts, cm.content)
		}
	}
	if len(results) != 2 || results[0] != "R-a1" || results[1] != "R-a2" || len(texts) != 2 {
		t.Fatalf("s1 已提交的结果要保住、s2 只留重来那次的：期望结果 [R-a1 R-a2]、两条答复，实际 %+v", got)
	}
}

// provider 在一个 run 里复用 tool_call ID（两轮都是 call_0）之后作废一次：两次工具结果在线上与快照里都是各自的一条，不拼成
// 一条（审计实测曾是 "RaRb"）。
func TestResetSnapshot_ReusedToolCallID(t *testing.T) {
	call := func(q string) *schema.Message {
		return &schema.Message{Role: schema.RoleAssistant, ContentBlocks: []*schema.ContentBlock{{Type: "tool_call",
			Toolcall: &schema.ToolcallContentBlock{Id: "call_0", Name: "lookup", Arguments: `{"q":"` + q + `"}`}}}}
	}
	m := &scriptStream{script: func(n int, send func(*schema.Message)) error {
		switch n {
		case 0:
			send(call("a"))
		case 1:
			send(call("b"))
		case 2:
			send(text("partial-"))
			return errReset
		default:
			send(text("full"))
		}
		return nil
	}}
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("lookup", "查", map[string]any{"type": "object"},
			func(_ context.Context, a map[string]any) (*tool.Result, error) {
				q, _ := a["q"].(string)
				return tool.Success("R" + q), nil
			}))
		return agent_v2.NewBasicAgent(agent_v2.WithName("b"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
			agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(5), agent_v2.WithModelRetry(compose_v2.SimpleRetry(2)))
	})
	rec := postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"id":"u1","role":"user","content":"go"}]}`)
	events := parseSSE(rec.Body.String())
	resultIDs := map[string]bool{}
	for _, ev := range events {
		if ev["type"] == "TOOL_CALL_RESULT" {
			id, _ := ev["messageId"].(string)
			resultIDs[id] = true
		}
	}
	if len(resultIDs) != 2 {
		t.Fatalf("两次工具结果的 messageId 应各不相同，实际 %v", resultIDs)
	}
	got := applyAGUI([]clientMsg{{id: "u1", role: "user", content: "go"}}, events)
	var results []string
	for _, cm := range got {
		if cm.role == "tool" {
			results = append(results, cm.content)
		}
	}
	if len(results) != 2 || results[0] != "Ra" || results[1] != "Rb" || got[len(got)-1].content != "full" {
		t.Fatalf("快照之后两次结果应各是一条（Ra、Rb），最后是完整答复，实际 %+v", got)
	}
}

// 成员的一次尝试里有两次模型调用流完，之后这次尝试被从头再跑取代：两次调用各来一个撤回，适配层只发一份快照（攒到下一个
// 事件之前），前端最后的对话照样正确。曾经每个撤回各发一份重述整场对话的快照，长对话下发送量近似平方增长。
func TestResetSnapshot_DiscardedAttemptOneSnapshot(t *testing.T) {
	callMsg := func(s, id string) *schema.Message {
		return &schema.Message{Role: schema.RoleAssistant, Content: s, ContentBlocks: []*schema.ContentBlock{{Type: "tool_call",
			Toolcall: &schema.ToolcallContentBlock{Id: id, Name: "lookup", Arguments: `{}`}}}}
	}
	m := &scriptStream{script: func(n int, send func(*schema.Message)) error {
		switch n {
		case 0:
			send(callMsg("A", "call_1"))
		case 1:
			send(callMsg("B", "call_2"))
		case 2:
			return &model.APIError{Provider: "stub", StatusCode: 400, Body: "bad"} // 交给成员重试从头再来
		default:
			send(text("full"))
		}
		return nil
	}}
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("lookup", "查", map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("找到了"), nil }))
		step := agent_v2.NewBasicAgent(agent_v2.WithName("s"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
			agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(4))
		return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
			agent_v2.WithSequentialRetryConfig(compose_v2.RetryConfig{MaxRetries: 2, ShouldRetry: func(error) bool { return true }})).
			AddStep("s1", step)
	})
	events := parseSSE(postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"id":"u1","role":"user","content":"go"}]}`).Body.String())
	snapshots, resets := 0, 0
	for _, ev := range events {
		switch {
		case ev["type"] == "MESSAGES_SNAPSHOT":
			snapshots++
		case ev["type"] == "CUSTOM" && ev["name"] == agui.CustomEventResetStream:
			resets++
		}
	}
	// 撤回至少两次（什么都没流出就失败的那次调用也登记过 run_id，一并撤，无害）
	if resets < 2 || snapshots != 1 {
		t.Fatalf("流完的两次模型调用都要撤、只发一份快照，实际撤回 %d 次、快照 %d 份", resets, snapshots)
	}
	got := applyAGUI([]clientMsg{{id: "u1", role: "user", content: "go"}}, events)
	if len(got) != 2 || got[1].content != "full" {
		t.Fatalf("前端最后应只有 用户 / full，实际 %+v", got)
	}
}

// streamingNodeAgent 是一个单节点 GraphAgent：节点经 EventSink 依次发 X 的片段、Y 的片段、撤回 Y、再发 X 的片段（X 在
// 撤回 Y 的时候还在流）。
func streamingNodeAgent(t *testing.T) agent_v2.Agent {
	t.Helper()
	b := compose_v2.NewBuilder()
	out := compose_v2.LastValue[string]("out")
	compose_v2.WithChannel(b, out)
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		sink := compose_v2.GetEventSink(ctx)
		chunk := func(id, c string) {
			sink.Send(compose_v2.Event{Type: "chunk", Content: c, Metadata: map[string]any{"run_id": id}})
		}
		chunk("X", "x1")
		chunk("Y", "y1")
		sink.Send(compose_v2.Event{Type: "reset_stream", Content: "discard", Metadata: map[string]any{"run_id": "Y"}})
		chunk("X", "x2")
		return []compose_v2.ChannelWrite{compose_v2.Write(out, "done")}, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("g"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			o, _ := compose_v2.Get(chs, out)
			return schema.AssistantMessage(o)
		}))
}

// 撤回时另一条消息还在流：快照等它收尾再发，不落在它的 START 与 END 之间（落在中间时官方客户端的事件压缩回放把快照排到
// 那条流之后，回放出来那条消息只剩快照那一刻的半截）；最后照样发了一份，Y 不在前端。
func TestResetSnapshot_NotInsideOpenStream(t *testing.T) {
	h := ownedHandler(func(hook.Hooks) agent_v2.Agent { return streamingNodeAgent(t) })
	events := parseSSE(postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"id":"u1","role":"user","content":"go"}]}`).Body.String())
	open, snapshots := false, 0
	for _, ev := range events {
		switch ev["type"] {
		case "TEXT_MESSAGE_START":
			if ev["messageId"] == "X" {
				open = true
			}
		case "TEXT_MESSAGE_END":
			if ev["messageId"] == "X" {
				open = false
			}
		case "MESSAGES_SNAPSHOT":
			snapshots++
			if open {
				t.Fatalf("快照落在了 X 的流中间：%v", events)
			}
		}
	}
	if snapshots != 1 {
		t.Fatalf("撤回之后应发一份快照，实际 %d 份", snapshots)
	}
	got := applyAGUI([]clientMsg{{id: "u1", role: "user", content: "go"}}, events)
	for _, cm := range got {
		if cm.id == "Y" {
			t.Fatalf("被撤回的 Y 不该留在前端：%+v", got)
		}
	}
}
