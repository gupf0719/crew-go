//go:build integration

// anthropic_smoke 用真 Claude API 验 model/anthropic（单独用、以及接在 BasicAgent 上）：
//
//   - G1 preserved thinking：prefix_mismatch_behavior 设成 "error"，多轮对话里回放 thinking 块
//     （含并行工具调用、失败的工具结果、中途的 system 消息），断言没有 400、响应里没有
//     prefix_binding_mismatch；并断言确实回放过 thinking 块——一次都没回放的话这个用例什么也没证明。
//   - G2 prompt cache：同前缀的第二轮请求命中 cache；两次请求的 tools 序列化逐字节相同。
//   - G4 并行工具：模型一轮调两个工具时，两个 tool_result 发在同一条 user 消息里。
//
// 默认 go test ./... 不编译（integration tag）。跑法：
//
//	go test -tags=integration ./example/model/anthropic_smoke -v
//
// 需要 ANTHROPIC_API_KEY（项目根 .env 或环境变量），没有时 Skip。模型默认 claude-haiku-5-5（省钱：
// 测试用 Haiku、最多 Sonnet），ANTHROPIC_SMOKE_MODEL 可换。全部用例 prefix_mismatch_behavior="error"、effort=low，合计十几次请求。
package anthropic_smoke_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/example/envloader"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/model/anthropic"
	"crew-go/schema"
	"crew-go/tool"

	"github.com/anthropics/anthropic-sdk-go/option"
)

// recorder 是记下每个请求体的 http.RoundTripper。
type recorder struct {
	mu     sync.Mutex
	bodies [][]byte
}

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		raw, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		_ = req.Body.Close() // 已读完，换成内存副本
		req.Body = io.NopCloser(bytes.NewReader(raw))
		r.mu.Lock()
		r.bodies = append(r.bodies, raw)
		r.mu.Unlock()
	}
	return http.DefaultTransport.RoundTrip(req)
}

func (r *recorder) requests(t *testing.T) []map[string]any {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]map[string]any, 0, len(r.bodies))
	for _, raw := range r.bodies {
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("请求体不是 JSON: %v", err)
		}
		out = append(out, m)
	}
	return out
}

// newClient 建一个记请求体的 Client；没有 key 时 Skip。
func newClient(t *testing.T, opts ...anthropic.ClientOption) (*anthropic.Client, *recorder) {
	t.Helper()
	envloader.Load()
	key := os.Getenv("ANTHROPIC_API_KEY")
	if key == "" {
		t.Skip("ANTHROPIC_API_KEY 未设（.env 或环境变量）")
	}
	rec := &recorder{}
	base := []anthropic.ClientOption{
		anthropic.WithHTTPClient(&http.Client{Transport: rec}),
		anthropic.WithEffort(anthropic.EffortLow),
		anthropic.WithMaxTokens(8000),
		// 全部用例在 error 档下跑：回放的历史有任何改动都直接 400，而不是被静默丢掉
		anthropic.WithPrefixMismatch(anthropic.PrefixMismatchError),
		// 冒烟验的是行为，网络抖动交给 SDK 重试（实测本机带宽被别的设备占满时，连接偶发被断开、
		// 报 EOF）。重试发的是同一个请求，不改变前缀。
		anthropic.WithRequestOptions(option.WithMaxRetries(2)),
	}
	model := os.Getenv("ANTHROPIC_SMOKE_MODEL")
	if model == "" {
		model = "claude-haiku-5-5"
	}
	return anthropic.NewClient(key, model, append(base, opts...)...), rec
}

// replayedThinking 数请求里回放的 thinking / redacted_thinking 块。
func replayedThinking(reqs []map[string]any) int {
	n := 0
	for _, req := range reqs {
		msgs, _ := req["messages"].([]any)
		for _, m := range msgs {
			mm, _ := m.(map[string]any)
			if mm["role"] != "assistant" {
				continue
			}
			blocks, _ := mm["content"].([]any)
			for _, b := range blocks {
				if bm, _ := b.(map[string]any); bm["type"] == "thinking" || bm["type"] == "redacted_thinking" {
					n++
				}
			}
		}
	}
	return n
}

// checkNoPrefixMismatch 断言响应里没有 prefix_binding_mismatch。
func checkNoPrefixMismatch(t *testing.T, msg *schema.Message) {
	t.Helper()
	for _, x := range anthropic.InputTransformations(msg) {
		if x.Reason == "prefix_binding_mismatch" {
			t.Errorf("API 报告前缀被改过：%+v——回放的历史跟上一轮发出的不一致", x)
		}
	}
}

// strictSchema 是一个 strict 兼容的单字符串参数 schema。
func strictSchema(field string) map[string]any {
	return map[string]any{
		"type":                 "object",
		"properties":           map[string]any{field: map[string]any{"type": "string"}},
		"required":             []any{field},
		"additionalProperties": false,
	}
}

// modelEnds 记下每次模型调用的最终消息（流式聚合之后）。
type modelEnds struct {
	hook.NoOpHooks
	mu   sync.Mutex
	msgs []*schema.Message
}

func (h *modelEnds) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if out != nil && out.Message != nil {
		h.msgs = append(h.msgs, out.Message)
	}
	return ctx, nil
}

// TestSmoke_G1_BasicAgentMultiTurn：BasicAgent + 会话历史跨两次 Run，工具里一次失败；error 模式下
// 历史的任何改写都会让请求 400。
func TestSmoke_G1_BasicAgentMultiTurn(t *testing.T) {
	// effort 用 high：查天气这种简单任务在 low 下模型常常不思考，整个过程没有 thinking 块可回放，
	// 用例就什么也没证明（实测 low 下 0 个、high 下 5 次响应里 2 次带 thinking）。
	c, rec := newClient(t, anthropic.WithThinkingDisplay(anthropic.DisplaySummarized),
		anthropic.WithEffort(anthropic.EffortHigh))

	var mu sync.Mutex
	shanghaiCalls := 0
	weather := tool.NewFuncTool("get_weather", "查询一个城市现在的天气", strictSchema("city"),
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			city := fmt.Sprint(args["city"])
			if strings.Contains(city, "上海") || strings.EqualFold(city, "shanghai") {
				mu.Lock()
				shanghaiCalls++
				first := shanghaiCalls == 1
				mu.Unlock()
				if first {
					return tool.Failure("upstream_timeout", "上海气象站超时，请重试"), nil
				}
				return tool.Success("上海：小雨 18℃"), nil
			}
			return tool.Success(city + "：晴 22℃"), nil
		})
	clock := tool.NewFuncTool("get_time", "查询一个城市现在的当地时间", strictSchema("city"),
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			return tool.Success(fmt.Sprint(args["city"]) + " 现在 14:05"), nil
		})
	tk := tool.NewToolkit()
	tk.Register(weather)
	tk.Register(clock)

	ends := &modelEnds{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("smoke_g1"),
		agent_v2.WithModel(c),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(ends),
		agent_v2.WithSystemPrompt("你是天气助手。需要信息时调用工具；多个城市的查询放在同一轮里一起调用。工具报错时可以重试一次。"),
		agent_v2.WithHistory(agent_v2.NewInMemoryHistoryStore(), "smoke"),
		agent_v2.WithMaxRounds(6),
	)
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "anthropic-smoke-g1")
	for i, q := range []string{"同时查一下北京和上海现在的天气。", "那北京现在几点？用一句话回答。"} {
		ev, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(q))
		if err != nil {
			t.Fatalf("第 %d 次 Run: %v", i+1, err)
		}
		t.Logf("第 %d 次 Run 答：%s", i+1, agent_v2.FinalMessage(ev).Content)
	}
	for _, m := range ends.msgs {
		checkNoPrefixMismatch(t, m)
	}
	if n := replayedThinking(rec.requests(t)); n == 0 {
		t.Fatalf("整个过程没有回放过 thinking 块——这次运行证明不了回放正确（模型可能没思考，换高一点的 effort 再跑）")
	} else {
		t.Logf("回放 thinking 块共 %d 次", n)
	}
}

// TestSmoke_G1_MidConversationSystem：直接调模型，历史里有非前导的 system 消息，流式与非流式交替；
// error 模式下没有 400、没有前缀不一致。
func TestSmoke_G1_MidConversationSystem(t *testing.T) {
	c, rec := newClient(t, anthropic.WithThinkingDisplay(anthropic.DisplaySummarized))
	ctx := context.Background()
	history := []*schema.Message{
		schema.SystemMessage("你是一个简洁的助手。"),
		schema.UserMessage("一个三角形三边是 3、4、5，它是什么三角形？先想清楚再答。"),
	}
	turn := func(stream bool) {
		t.Helper()
		var msg *schema.Message
		if stream {
			reader, err := c.Stream(ctx, history)
			if err != nil {
				t.Fatalf("Stream: %v", err)
			}
			msg = aggregate(t, reader)
		} else {
			var err error
			if msg, err = c.Generate(ctx, history); err != nil {
				t.Fatalf("Generate: %v", err)
			}
		}
		checkNoPrefixMismatch(t, msg)
		history = append(history, msg)
	}
	turn(true)
	history = append(history, schema.SystemMessage("用户偏好：之后的回答都控制在一句话以内。"),
		schema.UserMessage("把它的面积也算一下。"))
	turn(false)
	history = append(history, schema.UserMessage("周长呢？"))
	turn(true)
	if n := replayedThinking(rec.requests(t)); n == 0 {
		t.Fatalf("没有回放过 thinking 块，这次运行证明不了回放正确")
	}
}

// aggregate 读完流并按 agent_v2 的规则聚合（文本拼接、工具调用、标注、元数据）。
func aggregate(t *testing.T, reader *schema.StreamReader[*schema.Message]) *schema.Message {
	t.Helper()
	defer reader.Close()
	out := &schema.Message{Role: schema.RoleAssistant}
	var text, reasoning strings.Builder
	for {
		ch, err := reader.Recv(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("流出错: %v", err)
		}
		text.WriteString(ch.Content)
		reasoning.WriteString(ch.ReasoningContent)
		out.ContentBlocks = append(out.ContentBlocks, ch.ContentBlocks...)
		for k, v := range ch.Annotations {
			if out.Annotations == nil {
				out.Annotations = map[string]any{}
			}
			out.Annotations[k] = v
		}
		if ch.Metadata != nil {
			out.Metadata = ch.Metadata
		}
	}
	out.Content, out.ReasoningContent = text.String(), reasoning.String()
	return out
}

// TestSmoke_G2_PromptCache：第二轮请求（前缀 = 第一轮请求 + 第一轮回答）命中 cache；两次请求的 tools
// 序列化逐字节相同。
func TestSmoke_G2_PromptCache(t *testing.T) {
	c, rec := newClient(t)
	var rules strings.Builder
	for i := 1; i <= 300; i++ { // 撑过最小可缓存长度
		fmt.Fprintf(&rules, "规则 %d：处理第 %d 类工单时，先确认工单号、再确认负责人、最后记录处理时间。\n", i, i)
	}
	tools := []*schema.ToolInfo{
		{Name: "zeta_lookup", Desc: "查工单", Params: strictSchema("id")},
		{Name: "alpha_assign", Desc: "分配工单", Params: strictSchema("owner")},
	}
	bound, err := c.WithTools(tools)
	if err != nil {
		t.Fatalf("WithTools: %v", err)
	}
	ctx := context.Background()
	history := []*schema.Message{schema.SystemMessage(rules.String()), schema.UserMessage("第 7 条规则说了什么？一句话。")}
	first, err := bound.Generate(ctx, history, model.WithToolChoice(schema.ToolChoiceNone))
	if err != nil {
		t.Fatalf("第一轮: %v", err)
	}
	history = append(history, first, schema.UserMessage("第 8 条呢？一句话。"))
	second, err := bound.Generate(ctx, history, model.WithToolChoice(schema.ToolChoiceNone))
	if err != nil {
		t.Fatalf("第二轮: %v", err)
	}
	u := second.Metadata.Usage
	t.Logf("第二轮用量：%+v", *u)
	if u.CachedInputTokens <= 0 {
		t.Errorf("第二轮没命中 cache：%+v", *u)
	}
	reqs := rec.requests(t)
	if len(reqs) != 2 {
		t.Fatalf("发了 %d 次请求", len(reqs))
	}
	a, _ := json.Marshal(reqs[0]["tools"])
	b, _ := json.Marshal(reqs[1]["tools"])
	if !bytes.Equal(a, b) {
		t.Errorf("两次请求的 tools 不同：\n%s\n%s", a, b)
	}
}

// TestSmoke_G4_ParallelTools：模型一轮调两个工具；回填时两个 tool_result 在同一条 user 消息里，
// 第二轮请求成功。
func TestSmoke_G4_ParallelTools(t *testing.T) {
	c, rec := newClient(t)
	weather := &schema.ToolInfo{Name: "get_weather", Desc: "查询一个城市现在的天气", Params: strictSchema("city"),
		Extra: map[string]any{anthropic.StrictToolExtraKey: true}}
	bound, err := c.WithTools([]*schema.ToolInfo{weather})
	if err != nil {
		t.Fatalf("WithTools: %v", err)
	}
	ctx := context.Background()
	history := []*schema.Message{
		schema.SystemMessage("需要信息时调用工具。多个独立的查询必须在同一轮里并行调用。"),
		schema.UserMessage("北京和上海现在天气怎样？两个城市在同一轮里并行查询。"),
	}
	first, err := bound.Generate(ctx, history)
	if err != nil {
		t.Fatalf("第一轮: %v", err)
	}
	calls := schema.GetToolCalls(first)
	if len(calls) < 2 {
		t.Fatalf("模型这一轮只调了 %d 个工具（期望并行调两个）：%q", len(calls), first.Content)
	}
	history = append(history, first)
	for _, tc := range calls {
		history = append(history, &schema.Message{Role: schema.RoleTool, Content: "晴 20℃", ContentBlocks: []*schema.ContentBlock{{
			Type: "tool_result", Toolresult: &schema.ToolresultContentBlock{ToolCallId: tc.ID, Content: "晴 20℃"},
		}}})
	}
	if _, err := bound.Generate(ctx, history); err != nil {
		t.Fatalf("第二轮: %v", err)
	}
	reqs := rec.requests(t)
	msgs, _ := reqs[len(reqs)-1]["messages"].([]any)
	last, _ := msgs[len(msgs)-1].(map[string]any)
	blocks, _ := last["content"].([]any)
	n := 0
	for _, b := range blocks {
		if bm, _ := b.(map[string]any); bm["type"] == "tool_result" {
			n++
		}
	}
	if last["role"] != "user" || n != len(calls) {
		t.Errorf("最后一条消息 role=%v、tool_result %d 个，期望 user、%d 个", last["role"], n, len(calls))
	}
}
