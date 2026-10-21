package observability

import (
	"context"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/prompt"
	"crew-go/schema"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// findAttrInt 在 span attribute 里取整型值（既有 findAttr 把 int 渲染成 rune 串，不适合比数）。
func findAttrInt(s *tracepb.Span, key string) (int64, bool) {
	for _, a := range s.Attributes {
		if a.Key != key {
			continue
		}
		if iv, ok := a.Value.Value.(*commonpb.AnyValue_IntValue); ok {
			return iv.IntValue, true
		}
	}
	return 0, false
}

// TestOnModelStart_MessagesCountIncludesPreamble 钉住「`gen_ai.request.messages_count`
// 数的是这次请求的**全部**消息，不只是被压缩的那半」。
//
// `schema.ModelInput` 把 agent 的前言（system prompt / 提示词模板渲染结果，含 skill 正文）
// 拆成了跟 `Messages` 并列的 `Preamble`。只数 `Messages` 会**静默少报**，而少报的正是
// 「这个 agent 是什么」那几条——运维在 trace 上看不出请求变小是因为压缩生效了，
// 还是因为 agent 的定义压根没送出去。
//
// 走**真 span**（真 BasicAgent + 真 exporter 捕获）而不是在测试里重算一遍表达式——
// 重算的话 revert 生产代码这条也不会红。
func TestOnModelStart_MessagesCountIncludesPreamble(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs := newTestObs(t, addr)
	defer obs.Close()

	// 前言 3 条（模板 system + few-shot 一对）+ sysPrompt 1 条 = 4；对话 1 条 user。
	tmpl := prompt.FromMessages(
		prompt.System("TEMPLATE SYSTEM"),
		prompt.User("fewshot q"),
		prompt.Assistant("fewshot a"),
	)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("count_agent"),
		agent_v2.WithModel(&capAssembly{}),
		agent_v2.WithSystemPrompt("REAL SYSPROMPT"),
		agent_v2.WithPromptTemplate(tmpl, nil),
		agent_v2.WithHooks(obs.AgentHooks()),
		agent_v2.WithMaxRounds(2),
	)
	ctx := compose_v2.WithSessionIDCtx(context.Background(), "count_sid")
	ctx = agent_v2.WithCtxOrchHooks(ctx, obs.OrchHooks())
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("用户问题")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	obs.CleanupSession("count_sid")
	time.Sleep(500 * time.Millisecond)

	sp := findSpanByOperation(srv.Spans(), "chat")
	if sp == nil {
		t.Fatalf("没抓到 model 的 chat span，ops=%v", spanOps(srv.Spans()))
	}
	got, ok := findAttrInt(sp, "gen_ai.request.messages_count")
	if !ok {
		t.Fatal("chat span 上没有 gen_ai.request.messages_count")
	}
	// 前言 4（TEMPLATE SYSTEM + fewshot q + fewshot a + REAL SYSPROMPT）+ 对话 1（user）。
	const want = 5
	if got != want {
		t.Errorf("messages_count 应为 %d（前言 4 + 对话 1），实际 %d——"+
			"数漏了前言那半，trace 上会看不出 agent 的定义有没有送出去", want, got)
	}
}

// TestOnModelStart_PromptEventCoversPreamble 钉住「prompt event 的 messages_count 与
// gen_ai_content 也覆盖前言」。
//
// `gen_ai_content` 是**承载 prompt 正文的唯一 key**（spec 给它独立的 1MB 预算）。前言
// 拆成独立字段之后只采 `Messages` 的话，agent 的 system prompt / few-shot / skill 正文
// **一个字都不上报**。
//
// ⚠️ **这条必须跟 span attr 那条一起存在**：曾经只把 span attr 补上，结果同一个 span 上
// attr 报 2、event 报 1、`gen_ai_content` 里干脆没有 system prompt——**自相矛盾的上报
// 比一致地少报更难排查**。两处共用一份 `promptMsgs`，这条钉的就是「别再各算各的」。
func TestOnModelStart_PromptEventCoversPreamble(t *testing.T) {
	srv, addr, stop := startCaptureServer(t)
	defer stop()
	obs, _ := New(Config{
		Resource:             Resource{AppName: "a", EnvName: "t", IP: "1.1.1.1", SDKVersion: "v"},
		TraceEndpoint:        addr,
		TraceExportInterval:  100 * time.Millisecond,
		TraceExportBatchSize: 100,
		RecordMessageContent: true,
	})
	defer obs.Close()

	ctx := compose_v2.WithGraphNS(context.Background(), "g")
	orch := obs.OrchHooks()
	ah := obs.AgentHooks()
	_, _ = orch.OnGraphStart(ctx)
	info := &hook.AgentInfo{Name: "ag"}
	_, _ = ah.OnAgentStart(ctx, info, &hook.AgentInput{})
	_, _ = ah.OnModelStart(ctx, info, &schema.ModelInput{
		Preamble: []*schema.Message{schema.SystemMessage("PREAMBLE-SYSPROMPT-xyz")},
		Messages: []*schema.Message{schema.UserMessage("对话内容-abc")},
	})
	ah.OnAgentEnd(ctx, info, &hook.AgentOutput{})
	orch.OnGraphEnd(ctx, nil)
	time.Sleep(500 * time.Millisecond)

	chat := findSpanByOp(srv.Spans(), "chat")
	if chat == nil {
		t.Fatal("没找到 chat span")
	}
	var content string
	var count int64 = -1
	for _, e := range chat.Events {
		if e.Name != "prompt" {
			continue
		}
		for _, a := range e.Attributes {
			switch a.Key {
			case "gen_ai_content":
				if sv, ok := a.Value.Value.(*commonpb.AnyValue_StringValue); ok {
					content = sv.StringValue
				}
			case "messages_count":
				if iv, ok := a.Value.Value.(*commonpb.AnyValue_IntValue); ok {
					count = iv.IntValue
				}
			}
		}
	}
	if count != 2 {
		t.Errorf("prompt event 的 messages_count 应为 2（前言 1 + 对话 1），实际 %d", count)
	}
	if !strings.Contains(content, "PREAMBLE-SYSPROMPT-xyz") {
		t.Errorf("gen_ai_content 里没有 agent 的 system prompt——它是 prompt 正文的唯一承载 key，"+
			"漏了它 trace 上就看不出 agent 的定义。实际 %q", content)
	}
	if !strings.Contains(content, "对话内容-abc") {
		t.Errorf("gen_ai_content 里没有对话内容：%q", content)
	}
}
